//! Stateless parsing / scaling helpers used across the Lighter connector.
//!
//! These were originally associated functions on `LighterConnector` but
//! none of them touch `&self`, so they live more naturally as free
//! functions. The parent module already holds all the imports they need.

use super::MAX_DECIMAL_PRECISION;
use crate::dex_connector::string_to_decimal;
use crate::dex_request::DexError;
use crate::{CanceledOrder, FilledOrder, OrderSide};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};
use serde_json::Value;

/// Recover the numeric `order_index` Lighter uses internally from one of
/// the two order-id formats we surface: a plain numeric string, or a
/// legacy `trigger-{ts}-{market}` style identifier (kept for backwards
/// compatibility with the trigger-order id format that pre-dates the
/// numeric CID change).
pub(super) fn parse_cancel_order_index(order_id: &str) -> Option<i64> {
    if let Ok(idx) = order_id.parse::<i64>() {
        return Some(idx);
    }

    let stripped = order_id.strip_prefix("trigger-")?;
    let (timestamp, _) = stripped.split_once('-')?;
    timestamp.parse::<i64>().ok()
}

/// `10^decimals`, clamped to the protocol-supported precision.
pub(super) fn ten_pow(decimals: u32) -> u64 {
    let safe_decimals = decimals.min(MAX_DECIMAL_PRECISION);
    if safe_decimals != decimals {
        log::warn!(
            "Decimal precision {} exceeds supported max {}, clamping",
            decimals,
            MAX_DECIMAL_PRECISION
        );
    }
    10u64.pow(safe_decimals)
}

/// Scale a `Decimal` into a `u64` integer with the given number of
/// decimal places, applying the requested rounding. `context` is folded
/// into the error message so callers don't need to wrap the result.
pub(super) fn scale_decimal_to_u64(
    value: Decimal,
    decimals: u32,
    rounding: RoundingStrategy,
    context: &str,
) -> Result<u64, DexError> {
    let safe_decimals = decimals.min(MAX_DECIMAL_PRECISION);
    if safe_decimals != decimals {
        log::warn!(
            "{} decimals {} exceed supported max {}, clamping",
            context,
            decimals,
            MAX_DECIMAL_PRECISION
        );
    }

    let multiplier = Decimal::new(10i64.pow(safe_decimals), 0);
    let rounded = value.round_dp_with_strategy(safe_decimals, rounding);
    (rounded * multiplier).to_u64().ok_or_else(|| {
        DexError::Other(format!(
            "Invalid {} value {} after scaling to {} decimals",
            context, value, safe_decimals
        ))
    })
}

pub(super) fn scale_decimal_to_u32(
    value: Decimal,
    decimals: u32,
    rounding: RoundingStrategy,
    context: &str,
) -> Result<u32, DexError> {
    let scaled = scale_decimal_to_u64(value, decimals, rounding, context)?;
    if scaled > u64::from(u32::MAX) {
        return Err(DexError::Other(format!(
            "Scaled {} value {} exceeds u32 maximum",
            context, scaled
        )));
    }
    Ok(scaled as u32)
}

pub(super) fn calculate_min_tick(price: Decimal, sz_decimals: u32, is_spot: bool) -> Decimal {
    let price_str = price.to_string();
    let integer_part = price_str.split('.').next().unwrap_or("");
    let integer_digits = if integer_part == "0" {
        0
    } else {
        integer_part.len()
    };

    let scale_by_sig: u32 = if integer_digits >= 5 {
        0
    } else {
        (5 - integer_digits) as u32
    };

    let max_decimals: u32 = if is_spot { 8u32 } else { 6u32 };
    let scale_by_dec: u32 = max_decimals.saturating_sub(sz_decimals);
    let scale: u32 = scale_by_sig.min(scale_by_dec);

    Decimal::new(1, scale)
}

pub(super) fn value_to_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(s) => string_to_decimal(Some(s.clone())).ok(),
        Value::Number(n) => string_to_decimal(Some(n.to_string())).ok(),
        _ => None,
    }
}

pub(super) fn map_side(side: Option<&str>) -> Option<OrderSide> {
    let s = side?;
    let normalized = s.trim().to_lowercase();
    match normalized.as_str() {
        "buy" | "bid" | "long" => Some(OrderSide::Long),
        "sell" | "ask" | "short" => Some(OrderSide::Short),
        _ => None,
    }
}

pub(super) fn parse_filled_order(data: &Value, account_id: u64) -> Result<FilledOrder, DexError> {
    let order_id = data
        .get("order_id")
        .or_else(|| data.get("orderId"))
        .or_else(|| data.get("oid"))
        .or_else(|| data.get("client_order_id"))
        .or_else(|| data.get("clientOrderId"))
        .or_else(|| data.get("client_order_index"))
        .or_else(|| data.get("clientOrderIndex"))
        .and_then(|v| {
            v.as_str()
                .map(|s| s.to_string())
                .or_else(|| v.as_u64().map(|n| n.to_string()))
        });

    let order_id =
        order_id.ok_or_else(|| DexError::Other("Missing order_id in fill".to_string()))?;

    let trade_id = data
        .get("trade_id")
        .or_else(|| data.get("tradeId"))
        .or_else(|| data.get("id"))
        .and_then(|v| {
            v.as_str()
                .map(|s| s.to_string())
                .or_else(|| v.as_u64().map(|n| n.to_string()))
        })
        .unwrap_or_else(|| "0".to_string());

    let filled_size = data
        .get("size")
        .or_else(|| data.get("filled_size"))
        .or_else(|| data.get("filledSize"))
        .or_else(|| data.get("base_amount"))
        .or_else(|| data.get("baseAmount"))
        .and_then(value_to_decimal)
        .ok_or_else(|| DexError::Other("Missing filled size in fill".to_string()))?;

    let filled_price = data
        .get("price")
        .or_else(|| data.get("fill_price"))
        .or_else(|| data.get("fillPrice"))
        .and_then(value_to_decimal);

    let filled_side = if let Some(side) = data.get("side").and_then(|v| v.as_str()) {
        match side.to_ascii_lowercase().as_str() {
            "buy" | "long" => Some(OrderSide::Long),
            "sell" | "short" => Some(OrderSide::Short),
            _ => None,
        }
    } else if let Some(is_ask) = data.get("is_ask").and_then(|v| v.as_bool()) {
        if is_ask {
            Some(OrderSide::Short)
        } else {
            Some(OrderSide::Long)
        }
    } else if let (Some(ask_account_id), Some(bid_account_id)) = (
        data.get("ask_account_id").and_then(|v| v.as_u64()),
        data.get("bid_account_id").and_then(|v| v.as_u64()),
    ) {
        if account_id == ask_account_id {
            Some(OrderSide::Short)
        } else if account_id == bid_account_id {
            Some(OrderSide::Long)
        } else {
            None
        }
    } else {
        None
    };

    let filled_value = filled_price.map(|price| filled_size * price);

    Ok(FilledOrder {
        order_id,
        is_rejected: false,
        trade_id,
        filled_side,
        filled_size: Some(filled_size),
        filled_value,
        filled_fee: None,
        filled_ts_ms: None,
    })
}

pub(super) fn parse_canceled_order(data: &Value) -> Result<CanceledOrder, DexError> {
    Ok(CanceledOrder {
        order_id: data
            .get("order_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        canceled_timestamp: data.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0),
    })
}

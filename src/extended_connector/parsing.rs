//! Stateless helpers used across the Extended connector: serde
//! deserializers, default fee constant, balance/position model
//! converters, and symbol canonicalization.

use crate::{BalanceResponse, PositionSnapshot};
use rust_decimal::Decimal;
use serde::de::{self, Deserializer, Visitor};

use super::PositionModel;

pub(super) fn default_taker_fee() -> Decimal {
    Decimal::new(5, 4)
}

pub(super) fn deserialize_i64_from_string_or_number<'de, D>(
    deserializer: D,
) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    struct I64Visitor;

    impl<'de> Visitor<'de> for I64Visitor {
        type Value = i64;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("an integer or a string containing an integer")
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
            Ok(value)
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            i64::try_from(value).map_err(|_| E::custom("value overflows i64"))
        }

        fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(value.round() as i64)
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            value
                .parse::<i64>()
                .map_err(|_| E::custom("invalid integer string"))
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_str(&value)
        }
    }

    deserializer.deserialize_any(I64Visitor)
}

pub(super) fn copy_balance(balance: &BalanceResponse) -> BalanceResponse {
    BalanceResponse {
        equity: balance.equity,
        balance: balance.balance,
        position_entry_price: balance.position_entry_price,
        position_sign: balance.position_sign,
    }
}

/// Strip Extended's `-USD` / `-USDT` suffix to get the canonical token
/// symbol the rest of the bot keys caches by. Extended's REST/WS use
/// the market name ("BTC-USD") while callers across pairtrade/slow-mm
/// pass the bare token ("BTC"). Keying caches by the market name caused
/// `get_filled_orders("BTC")` to miss every WS-streamed fill
/// (bot-strategy#179, observed 2026-04-23 Tokyo) because they were
/// stored under "BTC-USD".
pub(super) fn normalize_symbol(market_or_symbol: &str) -> String {
    match market_or_symbol.split_once('-') {
        Some((base, _)) => base.to_string(),
        None => market_or_symbol.to_string(),
    }
}

pub(super) fn position_snapshot_from_model(position: PositionModel) -> Option<PositionSnapshot> {
    if let Some(status) = position.status.as_ref() {
        if status != "OPENED" {
            return None;
        }
    }
    let sign = match position.side.as_str() {
        "LONG" | "BUY" => 1,
        "SHORT" | "SELL" => -1,
        _ => 0,
    };
    if sign == 0 {
        return None;
    }
    let size = position.size.abs();
    if size <= Decimal::ZERO {
        return None;
    }
    Some(PositionSnapshot {
        symbol: normalize_symbol(&position.market),
        size,
        sign,
        entry_price: position.open_price,
    })
}

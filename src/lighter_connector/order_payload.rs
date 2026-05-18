//! Pure (FFI-free) helpers extracted from `create_order_native_with_type`
//! and `create_order_native_with_trigger` so the side/price/expiry/tpsl
//! mapping is unit-testable without bringing up the Lighter Go SDK.
//! See bot-strategy#390.
//!
//! The top-level `create_order_native_*` fns orchestrate
//!   client_order_index → nonce → [`OrderPayload`] → FFI signer → POST.
//! Everything in this module is the first arrow: it takes raw caller
//! args + a sampled `now_ms` and returns a fully-resolved payload that
//! the signing path (`call_go_sign_create_order`) can consume directly.

use super::market_cache::MarketInfo;
use super::{
    DEFAULT_PRICE_DECIMALS, DEFAULT_SIZE_DECIMALS, MAX_DECIMAL_PRECISION, ORDER_TYPE_IOC,
    ORDER_TYPE_TRIGGER, TIF_IOC,
};
use crate::CreateOrderResponse;
use rust_decimal::Decimal;
use std::collections::HashMap;

/// Decimal precision used to format the response amounts. Resolved from
/// the market metadata cache, with defaults when the cache hasn't been
/// populated yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct OrderDecimals {
    pub(super) price_decimals: u32,
    pub(super) size_decimals: u32,
}

impl OrderDecimals {
    pub(super) fn resolve(by_id: &HashMap<u32, MarketInfo>, market_id: u32) -> Self {
        if let Some(info) = by_id.get(&market_id) {
            Self {
                price_decimals: info.price_decimals.min(MAX_DECIMAL_PRECISION),
                size_decimals: info.size_decimals.min(MAX_DECIMAL_PRECISION),
            }
        } else {
            Self {
                price_decimals: DEFAULT_PRICE_DECIMALS.min(MAX_DECIMAL_PRECISION),
                size_decimals: DEFAULT_SIZE_DECIMALS.min(MAX_DECIMAL_PRECISION),
            }
        }
    }
}

/// Resolved arguments for `call_go_sign_create_order`. Construct via the
/// `build_*` helpers below; never assemble fields by hand at call sites.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct OrderPayload {
    pub(super) market_id: u32,
    pub(super) client_order_index: u64,
    pub(super) base_amount: u64,
    pub(super) price: u64,
    pub(super) side: u32,
    pub(super) order_type: u32,
    pub(super) time_in_force: u32,
    pub(super) reduce_only: bool,
    pub(super) trigger_price: u64,
    pub(super) order_expiry_ms: i64,
    pub(super) nonce: u64,
}

/// Build payload for a non-trigger order.
///
/// `expiry_secs` is honored only for non-IOC / non-immediate-TIF orders;
/// IOC / immediate TIF get `0` (NilOrderExpiry), matching the Go SDK
/// contract. The default for resting (GTC-style) orders is 24h, and
/// `now_ms` is sampled by the caller so the helper stays pure.
#[allow(clippy::too_many_arguments)] // mirrors create_order_native_with_type signature.
pub(super) fn build_order_payload_type_only(
    market_id: u32,
    side: u32,
    tif: u32,
    base_amount: u64,
    price: u64,
    client_order_index: u64,
    order_type: u32,
    reduce_only: bool,
    expiry_secs: Option<u64>,
    nonce: u64,
    now_ms: i64,
) -> OrderPayload {
    let is_immediate_tif = tif == TIF_IOC;
    let order_expiry_ms = if order_type == ORDER_TYPE_IOC || is_immediate_tif {
        0i64
    } else {
        let expiry_duration_ms = expiry_secs
            .map(|s| s.saturating_mul(1_000))
            .unwrap_or(24 * 60 * 60 * 1_000);
        now_ms + expiry_duration_ms as i64
    };
    OrderPayload {
        market_id,
        client_order_index,
        base_amount,
        price,
        side,
        order_type,
        time_in_force: tif,
        reduce_only,
        trigger_price: 0,
        order_expiry_ms,
        nonce,
    }
}

/// Build payload for a trigger (SL/TP) order.
///
/// Trigger-kind order types (2/3/4/5) enforce a 60s minimum expiry per
/// the Go SDK's `MinOrderExpiry >= 1` rule, and default to 28 days when
/// the caller doesn't supply one. Non-trigger types fall back to the
/// 24h default.
#[allow(clippy::too_many_arguments)] // mirrors create_order_native_with_trigger signature.
pub(super) fn build_order_payload_trigger(
    market_id: u32,
    side: u32,
    tif: u32,
    base_amount: u64,
    price: u64,
    trigger_price: u64,
    client_order_index: u64,
    order_type: u32,
    reduce_only: bool,
    expiry_secs: Option<u64>,
    nonce: u64,
    now_ms: i64,
) -> OrderPayload {
    let is_trigger_kind =
        order_type == ORDER_TYPE_TRIGGER || order_type == 4 || order_type == 3 || order_type == 5;
    let expiry_duration_ms: u64 = if is_trigger_kind {
        expiry_secs
            .map(|s| std::cmp::max(60, s).saturating_mul(1_000))
            .unwrap_or(28 * 24 * 60 * 60 * 1_000)
    } else {
        expiry_secs
            .map(|s| s.saturating_mul(1_000))
            .unwrap_or(24 * 60 * 60 * 1_000)
    };
    OrderPayload {
        market_id,
        client_order_index,
        base_amount,
        price,
        side,
        order_type,
        time_in_force: tif,
        reduce_only,
        trigger_price,
        order_expiry_ms: now_ms + expiry_duration_ms as i64,
        nonce,
    }
}

/// Form-urlencoded body for `POST /api/v1/sendTx`. Matches the Go SDK's
/// wire format (tx_type=14, no `price_protection`).
pub(super) fn build_send_tx_form(tx_info: &str) -> String {
    format!(
        "tx_type=14&tx_info={}&price_protection=false",
        urlencoding::encode(tx_info)
    )
}

/// Build the success response after `sendTx` accepts the order. Uses the
/// `client_order_index` as `order_id` (Lighter's tracking convention) and
/// stamps `ordered_price` / `ordered_size` from the same payload that was
/// signed.
pub(super) fn build_create_order_response(
    payload: &OrderPayload,
    decimals: OrderDecimals,
) -> CreateOrderResponse {
    let order_id = payload.client_order_index.to_string();
    CreateOrderResponse {
        order_id,
        exchange_order_id: None,
        ordered_price: i64::try_from(payload.price)
            .ok()
            .map(|p| Decimal::new(p, decimals.price_decimals))
            .unwrap_or(Decimal::ZERO),
        ordered_size: i64::try_from(payload.base_amount)
            .ok()
            .map(|b| Decimal::new(b, decimals.size_decimals))
            .unwrap_or(Decimal::ZERO),
        client_order_id: Some(payload.client_order_index.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW_MS: i64 = 1_700_000_000_000;

    fn market_info(price_decimals: u32, size_decimals: u32) -> MarketInfo {
        MarketInfo {
            canonical_symbol: "BTC".to_string(),
            market_id: 1,
            price_decimals,
            size_decimals,
            min_order: None,
        }
    }

    #[test]
    fn order_decimals_uses_defaults_when_market_id_missing() {
        let by_id: HashMap<u32, MarketInfo> = HashMap::new();
        let d = OrderDecimals::resolve(&by_id, 42);
        assert_eq!(
            d,
            OrderDecimals {
                price_decimals: DEFAULT_PRICE_DECIMALS,
                size_decimals: DEFAULT_SIZE_DECIMALS,
            }
        );
    }

    #[test]
    fn order_decimals_caps_at_max_precision() {
        let mut by_id = HashMap::new();
        by_id.insert(1, market_info(99, 99));
        let d = OrderDecimals::resolve(&by_id, 1);
        assert_eq!(d.price_decimals, MAX_DECIMAL_PRECISION);
        assert_eq!(d.size_decimals, MAX_DECIMAL_PRECISION);
    }

    #[test]
    fn order_decimals_passes_through_in_range_values() {
        let mut by_id = HashMap::new();
        by_id.insert(1, market_info(2, 6));
        let d = OrderDecimals::resolve(&by_id, 1);
        assert_eq!(d.price_decimals, 2);
        assert_eq!(d.size_decimals, 6);
    }

    #[test]
    fn type_only_ioc_order_type_yields_nil_expiry() {
        let p = build_order_payload_type_only(
            1,
            1,
            1,
            1_000,
            100,
            42,
            ORDER_TYPE_IOC,
            false,
            Some(60),
            7,
            NOW_MS,
        );
        assert_eq!(p.order_expiry_ms, 0);
        assert_eq!(p.trigger_price, 0);
        assert_eq!(p.nonce, 7);
    }

    #[test]
    fn type_only_immediate_tif_yields_nil_expiry_regardless_of_order_type() {
        let p = build_order_payload_type_only(
            1,
            1,
            TIF_IOC,
            1_000,
            100,
            42,
            0,
            false,
            Some(60),
            7,
            NOW_MS,
        );
        assert_eq!(p.order_expiry_ms, 0);
    }

    #[test]
    fn type_only_gtc_uses_supplied_expiry_seconds() {
        let p =
            build_order_payload_type_only(1, 1, 1, 1_000, 100, 42, 0, false, Some(45), 7, NOW_MS);
        assert_eq!(p.order_expiry_ms, NOW_MS + 45_000);
    }

    #[test]
    fn type_only_gtc_defaults_to_24h_when_no_expiry_supplied() {
        let p = build_order_payload_type_only(1, 1, 1, 1_000, 100, 42, 0, false, None, 7, NOW_MS);
        assert_eq!(p.order_expiry_ms, NOW_MS + 24 * 60 * 60 * 1_000);
    }

    #[test]
    fn type_only_carries_reduce_only_flag() {
        let p = build_order_payload_type_only(1, 1, 1, 1_000, 100, 42, 0, true, None, 7, NOW_MS);
        assert!(p.reduce_only);
    }

    #[test]
    fn trigger_type_2_enforces_60s_minimum_expiry() {
        let p = build_order_payload_trigger(
            1,
            SIDE_BUY_TEST,
            1,
            1_000,
            100,
            120,
            42,
            ORDER_TYPE_TRIGGER,
            true,
            Some(10),
            7,
            NOW_MS,
        );
        assert_eq!(p.order_expiry_ms, NOW_MS + 60_000);
        assert_eq!(p.trigger_price, 120);
    }

    #[test]
    fn trigger_type_2_defaults_to_28_days_when_no_expiry_supplied() {
        let p = build_order_payload_trigger(
            1,
            SIDE_BUY_TEST,
            1,
            1_000,
            100,
            120,
            42,
            ORDER_TYPE_TRIGGER,
            true,
            None,
            7,
            NOW_MS,
        );
        assert_eq!(p.order_expiry_ms, NOW_MS + 28 * 24 * 60 * 60 * 1_000);
    }

    #[test]
    fn trigger_kinds_3_4_5_share_trigger_expiry_rules() {
        for kind in [3u32, 4, 5] {
            let p = build_order_payload_trigger(
                1,
                SIDE_BUY_TEST,
                1,
                1_000,
                100,
                120,
                42,
                kind,
                true,
                Some(10),
                7,
                NOW_MS,
            );
            assert_eq!(
                p.order_expiry_ms,
                NOW_MS + 60_000,
                "order_type={kind} should clamp to 60s min"
            );
        }
    }

    #[test]
    fn trigger_non_trigger_order_type_uses_24h_default() {
        let p = build_order_payload_trigger(
            1,
            SIDE_BUY_TEST,
            1,
            1_000,
            100,
            120,
            42,
            0, // not a trigger kind
            false,
            None,
            7,
            NOW_MS,
        );
        assert_eq!(p.order_expiry_ms, NOW_MS + 24 * 60 * 60 * 1_000);
    }

    #[test]
    fn send_tx_form_is_url_encoded_with_tx_type_14() {
        let body = build_send_tx_form(r#"{"foo":"bar"}"#);
        assert!(body.starts_with("tx_type=14&tx_info="));
        assert!(body.ends_with("&price_protection=false"));
        assert!(body.contains("%7B%22foo%22%3A%22bar%22%7D"));
    }

    #[test]
    fn response_uses_client_order_index_as_order_id_and_scales_amounts() {
        let payload = OrderPayload {
            market_id: 1,
            client_order_index: 42,
            base_amount: 12_345,
            price: 67_890,
            side: 1,
            order_type: 0,
            time_in_force: 1,
            reduce_only: false,
            trigger_price: 0,
            order_expiry_ms: NOW_MS,
            nonce: 7,
        };
        let decimals = OrderDecimals {
            price_decimals: 2,
            size_decimals: 5,
        };
        let resp = build_create_order_response(&payload, decimals);
        assert_eq!(resp.order_id, "42");
        assert_eq!(resp.client_order_id.as_deref(), Some("42"));
        assert_eq!(resp.ordered_price, Decimal::new(67_890, 2));
        assert_eq!(resp.ordered_size, Decimal::new(12_345, 5));
        assert!(resp.exchange_order_id.is_none());
    }

    const SIDE_BUY_TEST: u32 = 1;
}

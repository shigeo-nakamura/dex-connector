//! Lighter protocol constants and pure order-parameter helpers.

use rust_decimal::Decimal;

// Lighter Protocol Order Type constants
pub(super) const ORDER_TYPE_LIMIT: u32 = 0;
pub(super) const ORDER_TYPE_IOC: u32 = 1;
pub(super) const ORDER_TYPE_TRIGGER: u32 = 2;
// Note: Lighter reuses enum values for certain order-type/TIF combinations.

// Lighter Protocol Side constants (order direction, not position direction)
pub(super) const SIDE_SELL: u32 = 0; // Sell order (close long positions, open short positions)
pub(super) const SIDE_BUY: u32 = 1; // Buy order (close short positions, open long positions)

// Lighter Protocol Time-in-Force constants (aligned with Go SDK)
pub(super) const TIF_IOC: u32 = 0; // Immediate-or-Cancel
pub(super) const TIF_GTT: u32 = 1; // Good-Till-Time
pub(super) const TIF_POST_ONLY: u32 = 2; // Post-Only (behaves like GTT but rejects immediate fills)

// Lighter Protocol scaling defaults (will be overridden by market metadata)
pub(super) const DEFAULT_PRICE_DECIMALS: u32 = 1;
pub(super) const DEFAULT_SIZE_DECIMALS: u32 = 5;
pub(super) const MAX_DECIMAL_PRECISION: u32 = 9;
pub(super) const DEFAULT_ORDERBOOK_STALE_SECS: u64 = 15;

/// Resolve the cross-DexConnector `spread` parameter to a Lighter TIF +
/// adjusted price. Pulled out of `create_order` so the
/// `spread = -2 → TIF_POST_ONLY` mapping (used by xvenue-arb's
/// maker-on-Lighter redesign — bot-strategy#309 / #317) is unit
/// testable without a network round trip.
///
/// Convention (matches Extended; see `extended_connector::mod.rs`):
/// - `Some(s)` with `s >= 0`: tick-based price adjustment, no TIF
///   override. Final price = `price + s * tick_size`. TIF stays at
///   `default_tif`.
/// - `Some(-1)`: caller asked for IOC, but Lighter doesn't honor IOC
///   on resting limits — degrade to GTT and keep the price unchanged.
/// - `Some(-2)`: post-only. TIF flips to `TIF_POST_ONLY`; the venue
///   rejects a cross instead of executing as taker.
/// - `Some(other_negative)`: invalid sentinel; warn + fall back to
///   `default_tif` (no price adjustment).
/// - `None`: no spread, no TIF override. Price unchanged, TIF stays
///   at `default_tif`.
pub(super) fn resolve_spread_to_tif_and_price(
    price: Decimal,
    spread: Option<i64>,
    tick_size: Decimal,
    default_tif: u32,
) -> (Decimal, u32) {
    match spread {
        Some(s) if s < 0 => {
            let tif = match s {
                -1 => TIF_GTT,       // Treat IOC override as resting GTT on Lighter
                -2 => TIF_POST_ONLY, // Post-only (resting limit)
                _ => {
                    log::warn!("Invalid TIF spread value: {}, using default", s);
                    default_tif
                }
            };
            (price, tif) // No price adjustment for TIF sentinels
        }
        Some(s) => {
            let spread_amount = Decimal::from(s) * tick_size;
            (price + spread_amount, default_tif)
        }
        None => (price, default_tif),
    }
}

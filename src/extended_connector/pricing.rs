//! Pure pricing / rounding helpers for Extended order placement.
//! Stateless free functions — no `&self` dependency on
//! `ExtendedConnector`. Co-located with the unit tests that exercise
//! tick-scale invariants.

use rust_decimal::{Decimal, RoundingStrategy};

use crate::dex_request::DexError;
use crate::OrderSide;

use super::models::MarketModel;

#[allow(dead_code)]
pub(super) fn is_invalid_price_error(err: &DexError) -> bool {
    match err {
        DexError::ServerResponse(message) => message.to_lowercase().contains("invalid price"),
        _ => false,
    }
}

pub(super) fn round_to_step(value: Decimal, step: Decimal, rounding: RoundingStrategy) -> Decimal {
    if step <= Decimal::ZERO {
        return value;
    }
    let steps = (value / step).round_dp_with_strategy(0, rounding);
    let rounded = steps * step;
    let step_scale = step.scale();
    rounded.round_dp_with_strategy(step_scale, RoundingStrategy::ToZero)
}

pub(super) fn round_size_for_market(
    size: Decimal,
    market: &MarketModel,
) -> Result<Decimal, DexError> {
    let mut rounded = round_to_step(
        size,
        market.trading_config.min_order_size_change,
        RoundingStrategy::ToNegativeInfinity,
    );
    let asset_precision = market.asset_precision.max(0).try_into().unwrap_or(0);
    rounded = rounded.round_dp_with_strategy(asset_precision, RoundingStrategy::ToZero);
    if rounded < market.trading_config.min_order_size {
        return Err(DexError::InvalidInput {
            field: "size".to_string(),
            value: format!(
                "{} below min {} for {}",
                rounded, market.trading_config.min_order_size, market.name
            ),
        });
    }
    Ok(rounded)
}

pub(super) fn round_price_for_market(
    price: Decimal,
    market: &MarketModel,
    side: OrderSide,
) -> Decimal {
    let tick = market.trading_config.min_price_change;
    let raw_floor = market.trading_config.limit_price_floor;
    let raw_cap = market.trading_config.limit_price_cap;
    let idx = market.market_stats.index_price;

    // Interpret floor/cap as % bands when <= 1.0, otherwise absolute prices.
    let (floor_px, cap_px) = if raw_cap > Decimal::ZERO && raw_cap <= Decimal::ONE {
        let floor_pct = if raw_floor > Decimal::ZERO {
            raw_floor
        } else {
            Decimal::ZERO
        };
        let cap_pct = raw_cap;
        (
            if floor_pct > Decimal::ZERO {
                idx * (Decimal::ONE - floor_pct)
            } else {
                Decimal::ZERO
            },
            idx * (Decimal::ONE + cap_pct),
        )
    } else {
        (
            if raw_floor > Decimal::ZERO {
                raw_floor
            } else {
                Decimal::ZERO
            },
            if raw_cap > Decimal::ZERO {
                raw_cap
            } else {
                Decimal::ZERO
            },
        )
    };

    log::debug!(
        "[round_price_for_market] raw_price={} tick={} floor_px={} cap_px={} raw_floor={} raw_cap={} idx={} side={:?}",
        price,
        tick,
        floor_px,
        cap_px,
        raw_floor,
        raw_cap,
        idx,
        side
    );

    let mut bounded = price;
    if cap_px > Decimal::ZERO && bounded > cap_px {
        bounded = cap_px;
    }
    if floor_px > Decimal::ZERO && bounded < floor_px {
        bounded = floor_px;
    }

    let rounding = match side {
        OrderSide::Long => RoundingStrategy::ToNegativeInfinity,
        OrderSide::Short => RoundingStrategy::ToPositiveInfinity,
    };
    let mut rounded = round_to_step(bounded, tick, rounding);
    if floor_px > Decimal::ZERO && rounded < floor_px {
        rounded = round_to_step(floor_px, tick, RoundingStrategy::ToPositiveInfinity);
    }
    if cap_px > Decimal::ZERO && rounded > cap_px {
        rounded = round_to_step(cap_px, tick, RoundingStrategy::ToNegativeInfinity);
    }

    if tick > Decimal::ZERO && rounded < tick {
        rounded = tick;
    }

    let final_price = clamp_positive_price(rounded, tick, floor_px);
    log::debug!(
        "[round_price_for_market] final_price={} bounded={} rounded={} tick={} floor_px={} cap_px={}",
        final_price,
        bounded,
        rounded,
        tick,
        floor_px,
        cap_px
    );
    final_price
}

pub(super) fn round_price_for_market_aggressive(
    price: Decimal,
    market: &MarketModel,
    side: OrderSide,
) -> Decimal {
    let tick = market.trading_config.min_price_change;
    let raw_floor = market.trading_config.limit_price_floor;
    let raw_cap = market.trading_config.limit_price_cap;
    let idx = market.market_stats.index_price;

    // Interpret floor/cap as % bands when <= 1.0, otherwise absolute prices.
    let (floor_px, cap_px) = if raw_cap > Decimal::ZERO && raw_cap <= Decimal::ONE {
        let floor_pct = if raw_floor > Decimal::ZERO {
            raw_floor
        } else {
            Decimal::ZERO
        };
        let cap_pct = raw_cap;
        (
            if floor_pct > Decimal::ZERO {
                idx * (Decimal::ONE - floor_pct)
            } else {
                Decimal::ZERO
            },
            idx * (Decimal::ONE + cap_pct),
        )
    } else {
        (
            if raw_floor > Decimal::ZERO {
                raw_floor
            } else {
                Decimal::ZERO
            },
            if raw_cap > Decimal::ZERO {
                raw_cap
            } else {
                Decimal::ZERO
            },
        )
    };

    let mut bounded = price;
    if cap_px > Decimal::ZERO && bounded > cap_px {
        bounded = cap_px;
    }
    if floor_px > Decimal::ZERO && bounded < floor_px {
        bounded = floor_px;
    }

    let rounding = match side {
        OrderSide::Long => RoundingStrategy::ToPositiveInfinity,
        OrderSide::Short => RoundingStrategy::ToNegativeInfinity,
    };
    let mut rounded = round_to_step(bounded, tick, rounding);
    if floor_px > Decimal::ZERO && rounded < floor_px {
        rounded = round_to_step(floor_px, tick, RoundingStrategy::ToPositiveInfinity);
    }
    if cap_px > Decimal::ZERO && rounded > cap_px {
        rounded = round_to_step(cap_px, tick, RoundingStrategy::ToNegativeInfinity);
    }

    if tick > Decimal::ZERO && rounded < tick {
        rounded = tick;
    }

    clamp_positive_price(rounded, tick, floor_px)
}

pub(super) fn apply_close_slippage_bps(price: Decimal, bps: u32, side: OrderSide) -> Decimal {
    if bps == 0 {
        return price;
    }
    let adj = Decimal::from(bps) / Decimal::new(10_000, 0);
    match side {
        OrderSide::Long => price * (Decimal::ONE + adj),
        OrderSide::Short => price * (Decimal::ONE - adj),
    }
}

pub(super) fn clamp_positive_price(price: Decimal, tick: Decimal, floor: Decimal) -> Decimal {
    if price > Decimal::ZERO {
        return price;
    }
    if floor > Decimal::ZERO {
        return floor;
    }
    if tick > Decimal::ZERO {
        return tick;
    }
    Decimal::ONE
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OrderSide;
    use std::str::FromStr;

    use super::super::models::{
        L2ConfigModel, MarketModel, MarketStatsModel, TradingConfigModel,
    };

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    fn sample_market(min_price_change: Decimal) -> MarketModel {
        MarketModel {
            name: "TEST-USD".to_string(),
            asset_name: "TEST".to_string(),
            asset_precision: 6,
            collateral_asset_name: "USD".to_string(),
            collateral_asset_precision: 6,
            active: true,
            market_stats: MarketStatsModel {
                daily_volume: Decimal::ZERO,
                daily_volume_base: Decimal::ZERO,
                daily_price_change: Decimal::ZERO,
                daily_low: Decimal::ZERO,
                daily_high: Decimal::ZERO,
                last_price: Decimal::ZERO,
                ask_price: Decimal::ZERO,
                bid_price: Decimal::ZERO,
                mark_price: Decimal::ZERO,
                index_price: Decimal::ZERO,
                funding_rate: Decimal::ZERO,
                next_funding_rate: 0,
                open_interest: Decimal::ZERO,
                open_interest_base: Decimal::ZERO,
            },
            trading_config: TradingConfigModel {
                min_order_size: dec("0.001"),
                min_order_size_change: dec("0.001"),
                min_price_change,
                max_market_order_value: Decimal::ZERO,
                max_limit_order_value: Decimal::ZERO,
                max_position_value: Decimal::ZERO,
                max_leverage: Decimal::ZERO,
                max_num_orders: 0,
                limit_price_cap: Decimal::ZERO,
                limit_price_floor: Decimal::ZERO,
            },
            l2_config: L2ConfigModel {
                l2_type: "stark".to_string(),
                collateral_id: "0".to_string(),
                collateral_resolution: 1,
                synthetic_id: "0".to_string(),
                synthetic_resolution: 1,
            },
        }
    }

    #[test]
    fn round_to_step_preserves_step_scale() {
        let step = dec("0.10");
        let rounded = round_to_step(dec("1.234"), step, RoundingStrategy::ToNegativeInfinity);
        assert_eq!(rounded, dec("1.20"));
        assert_eq!(rounded.scale(), step.scale());
    }

    #[test]
    fn round_price_for_market_preserves_tick_scale() {
        let market = sample_market(dec("0.10"));
        let rounded = round_price_for_market(dec("100.123"), &market, OrderSide::Long);
        assert_eq!(
            rounded.scale(),
            market.trading_config.min_price_change.scale()
        );
    }

    #[test]
    fn round_price_for_market_clamps_to_tick_when_zero() {
        let market = sample_market(dec("0.05"));
        let rounded = round_price_for_market(Decimal::ZERO, &market, OrderSide::Long);
        assert_eq!(rounded, dec("0.05"));
    }
}

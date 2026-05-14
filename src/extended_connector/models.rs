//! Wire-protocol model structs deserialized from Extended REST / WS
//! responses. Mostly internal to the connector — most fields are
//! `pub(super)` (visible to mod.rs and sibling sub-modules) so they can
//! be read across pricing / order-placement / state code without being
//! exposed on the crate API.
//!
//! `#[allow(dead_code)]` on individual structs preserves fields that
//! arrive on the wire but aren't read today — useful when the wire shape
//! changes and we want to bring up a new field without first noisily
//! adding it to the struct.

use rust_decimal::Decimal;
use serde::Deserialize;

use super::parsing::deserialize_i64_from_string_or_number;

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
pub(super) struct MarketStatsModel {
    pub(super) daily_volume: Decimal,
    pub(super) daily_volume_base: Decimal,
    pub(super) daily_price_change: Decimal,
    pub(super) daily_low: Decimal,
    pub(super) daily_high: Decimal,
    pub(super) last_price: Decimal,
    pub(super) ask_price: Decimal,
    pub(super) bid_price: Decimal,
    pub(super) mark_price: Decimal,
    pub(super) index_price: Decimal,
    pub(super) funding_rate: Decimal,
    pub(super) next_funding_rate: i64,
    pub(super) open_interest: Decimal,
    pub(super) open_interest_base: Decimal,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
pub(super) struct TradingConfigModel {
    pub(super) min_order_size: Decimal,
    pub(super) min_order_size_change: Decimal,
    pub(super) min_price_change: Decimal,
    pub(super) max_market_order_value: Decimal,
    pub(super) max_limit_order_value: Decimal,
    pub(super) max_position_value: Decimal,
    pub(super) max_leverage: Decimal,
    #[serde(deserialize_with = "deserialize_i64_from_string_or_number")]
    pub(super) max_num_orders: i64,
    pub(super) limit_price_cap: Decimal,
    pub(super) limit_price_floor: Decimal,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
pub(super) struct L2ConfigModel {
    #[serde(rename = "type")]
    pub(super) l2_type: String,
    pub(super) collateral_id: String,
    pub(super) collateral_resolution: i64,
    pub(super) synthetic_id: String,
    pub(super) synthetic_resolution: i64,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
pub(super) struct MarketModel {
    pub(super) name: String,
    pub(super) asset_name: String,
    pub(super) asset_precision: i64,
    pub(super) collateral_asset_name: String,
    pub(super) collateral_asset_precision: i64,
    pub(super) active: bool,
    pub(super) market_stats: MarketStatsModel,
    pub(super) trading_config: TradingConfigModel,
    pub(super) l2_config: L2ConfigModel,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct OrderbookQuantityModel {
    pub(super) qty: Decimal,
    pub(super) price: Decimal,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
pub(super) struct OrderbookUpdateModel {
    pub(super) market: String,
    pub(super) bid: Vec<OrderbookQuantityModel>,
    pub(super) ask: Vec<OrderbookQuantityModel>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
pub(super) struct BalanceModel {
    pub(super) collateral_name: String,
    pub(super) balance: Decimal,
    pub(super) equity: Decimal,
    pub(super) available_for_trade: Decimal,
    pub(super) available_for_withdrawal: Decimal,
    pub(super) unrealised_pnl: Decimal,
    pub(super) initial_margin: Decimal,
    pub(super) margin_ratio: Decimal,
    pub(super) updated_time: i64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
pub(super) struct OpenOrderModel {
    pub(super) id: i64,
    pub(super) account_id: i64,
    pub(super) external_id: String,
    pub(super) market: String,
    #[serde(rename = "type")]
    pub(super) order_type: String,
    pub(super) side: String,
    pub(super) status: String,
    pub(super) status_reason: Option<String>,
    // TPSL standalone orders have no main `price` field in the response
    // (price=0 is sent on the wire but the server omits it). Accept either.
    #[serde(default)]
    pub(super) price: Option<Decimal>,
    pub(super) average_price: Option<Decimal>,
    pub(super) qty: Decimal,
    pub(super) filled_qty: Option<Decimal>,
    pub(super) reduce_only: bool,
    pub(super) post_only: bool,
    pub(super) created_time: i64,
    pub(super) updated_time: i64,
    pub(super) expiry_time: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
pub(super) struct AccountTradeModel {
    pub(super) id: i64,
    pub(super) account_id: i64,
    pub(super) market: String,
    pub(super) order_id: i64,
    pub(super) side: String,
    pub(super) price: Decimal,
    pub(super) qty: Decimal,
    pub(super) value: Decimal,
    pub(super) fee: Decimal,
    pub(super) is_taker: bool,
    pub(super) trade_type: String,
    pub(super) created_time: i64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
pub(super) struct PositionModel {
    pub(super) market: String,
    pub(super) side: String,
    pub(super) size: Decimal,
    pub(super) open_price: Option<Decimal>,
    pub(super) status: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
pub(super) struct PlacedOrderModel {
    pub(super) id: i64,
    pub(super) external_id: String,
}

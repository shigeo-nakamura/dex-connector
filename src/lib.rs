use std::fmt;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

mod dex_connector;
mod dex_request;
mod dex_websocket;
mod hyperliquid_connector;
#[cfg(feature = "lighter-sdk")]
pub mod lighter_connector;

pub use dex_connector::DexConnector;
pub use dex_request::DexError;
pub use hyperliquid_connector::*;
#[cfg(feature = "lighter-sdk")]
pub use lighter_connector::*;

#[derive(Debug, Clone, Copy, PartialEq, Default, Deserialize)]
pub enum OrderSide {
    #[default]
    Long,
    Short,
}

impl fmt::Display for OrderSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderSide::Long => write!(f, "long"),
            OrderSide::Short => write!(f, "short"),
        }
    }
}

#[derive(Deserialize, Debug, Default)]
pub struct CommonErrorResponse {
    pub message: Option<String>,
}

#[derive(Deserialize, Debug, Default)]
pub struct TickerResponse {
    pub symbol: String,
    pub price: Decimal,
    pub min_tick: Option<Decimal>,
    pub min_order: Option<Decimal>,
    pub volume: Option<Decimal>,
    pub num_trades: Option<u64>,
    pub open_interest: Option<Decimal>,
    pub funding_rate: Option<Decimal>,
    pub oracle_price: Option<Decimal>,
}

#[derive(Deserialize, Clone, Debug, Default)]
pub struct FilledOrder {
    pub order_id: String,
    pub is_rejected: bool,
    pub trade_id: String,
    pub filled_side: Option<OrderSide>,
    pub filled_size: Option<Decimal>,
    pub filled_value: Option<Decimal>,
    pub filled_fee: Option<Decimal>,
}

#[derive(Deserialize, Debug, Default)]
pub struct FilledOrdersResponse {
    pub orders: Vec<FilledOrder>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CanceledOrder {
    pub order_id: String,
    pub canceled_timestamp: u64,
}

#[derive(Debug, Default, Deserialize)]
pub struct CanceledOrdersResponse {
    pub orders: Vec<CanceledOrder>,
}

#[derive(Deserialize, Debug, Default)]
pub struct BalanceResponse {
    pub equity: Decimal,
    pub balance: Decimal,
}

#[derive(Deserialize, Debug, Default)]
pub struct CreateOrderResponse {
    pub order_id: String,
    pub ordered_price: Decimal,
    pub ordered_size: Decimal,
}

#[derive(Deserialize, Debug, Default, Clone)]
pub struct LastTrade {
    pub price: Decimal,
}

#[derive(Deserialize, Debug, Default)]
pub struct LastTradesResponse {
    pub trades: Vec<LastTrade>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Trigger {
    /// If true, executes as a market order once triggered; otherwise limit.
    pub is_market: bool,
    /// The price at which the trigger fires.
    pub trigger_px: String,
    /// “tp” for take‐profit or “sl” for stop‐loss.
    pub tpsl: String,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TpSl {
    /// Take‐Profit
    Tp,
    /// Stop‐Loss
    Sl,
}

use std::fmt;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

mod dex_connector;
mod dex_request;
mod dex_websocket;
mod extended_connector;
mod hyperliquid_connector;
#[cfg(feature = "lighter-sdk")]
pub mod lighter_connector;

pub use dex_connector::DexConnector;
pub use dex_request::DexError;
pub use extended_connector::*;
pub use hyperliquid_connector::*;
#[cfg(feature = "lighter-sdk")]
pub use lighter_connector::*;

#[derive(Debug, Clone, Copy, PartialEq, Default, Deserialize)]
pub enum OrderSide {
    #[default]
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, PartialEq, Default, Deserialize)]
pub enum TriggerOrderStyle {
    #[default]
    Market,
    Limit,
    MarketWithSlippageControl,
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
    pub size_decimals: Option<u32>,
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

#[derive(Debug, Clone, Deserialize)]
pub struct OpenOrder {
    pub order_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub size: Decimal,
    pub price: Decimal,
    pub status: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct OpenOrdersResponse {
    pub orders: Vec<OpenOrder>,
}

#[derive(Deserialize, Debug, Default)]
pub struct BalanceResponse {
    pub equity: Decimal,
    pub balance: Decimal,
    pub position_entry_price: Option<Decimal>,
    pub position_sign: Option<i32>,
}

#[derive(Debug, Clone, Default)]
pub struct PositionSnapshot {
    pub symbol: String,
    pub size: Decimal,
    pub sign: i32,
    pub entry_price: Option<Decimal>,
}

#[derive(Debug, Default)]
pub struct CombinedBalanceResponse {
    pub usd_balance: Decimal,
    pub token_balances: std::collections::HashMap<String, BalanceResponse>,
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
    pub size: Option<Decimal>,
    /// Optional taker side if provided by the venue
    pub side: Option<OrderSide>,
}

#[derive(Deserialize, Debug, Default)]
pub struct LastTradesResponse {
    pub trades: Vec<LastTrade>,
}

#[derive(Debug, Clone, Default)]
pub struct OrderBookLevel {
    pub price: Decimal,
    pub size: Decimal,
}

#[derive(Debug, Clone, Default)]
pub struct OrderBookSnapshot {
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
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

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TpSl {
    /// Take‐Profit
    Tp,
    /// Stop‐Loss
    Sl,
}

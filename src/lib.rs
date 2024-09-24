use std::fmt;

use rust_decimal::Decimal;
use serde::Deserialize;

mod dex_connector;
mod dex_request;
mod dex_websocket;
mod hyperliquid_connector;

pub use dex_connector::DexConnector;
pub use dex_request::DexError;
pub use hyperliquid_connector::*;

#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
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

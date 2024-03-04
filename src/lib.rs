use std::fmt;

use rust_decimal::Decimal;
use serde::Deserialize;

mod dex_connector;
mod dex_request;
mod dex_websocket;
mod hyperliquid_connector;
mod rabbitx_connector;

pub use dex_connector::DexConnector;
pub use dex_request::DexError;
pub use hyperliquid_connector::*;
pub use rabbitx_connector::*;

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
}

#[derive(Deserialize, Debug, Default)]
pub struct FilledOrder {
    pub order_id: String,
    pub trade_id: String,
    pub filled_side: OrderSide,
    pub filled_size: Decimal,
    pub filled_value: Decimal,
    pub filled_fee: Decimal,
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

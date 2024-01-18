use serde::Deserialize;

mod dex_connector;
mod dex_request;
mod dex_websocket;
mod rabbitx_connector;

pub use dex_connector::DexConnector;
pub use dex_request::DexError;
pub use rabbitx_connector::*;

#[derive(Deserialize, Debug, Default)]
pub struct CommonErrorResponse {
    pub message: Option<String>,
}

#[derive(Deserialize, Debug, Default)]
pub struct TickerResponse {
    pub symbol: Option<String>,
    pub price: Option<String>,
}

#[derive(Deserialize, Debug, Default)]
pub struct FilledOrder {
    pub order_id: Option<String>,
    pub filled_size: Option<String>,
    pub filled_value: Option<String>,
    pub filled_fee: Option<String>,
}

#[derive(Deserialize, Debug, Default)]
pub struct FilledOrdersResponse {
    pub orders: Vec<FilledOrder>,
}

#[derive(Deserialize, Debug, Default)]
pub struct BalanceResponse {
    pub equity: Option<String>,
    pub balance: Option<String>,
}

#[derive(Deserialize, Debug, Default)]
pub struct CreateOrderResponse {
    pub order_id: Option<String>,
}

#[derive(Deserialize, Debug, Default)]
pub struct DefaultResponse {}

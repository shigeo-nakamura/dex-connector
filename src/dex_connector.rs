use crate::{
    dex_request::DexError, BalanceResponse, CreateOrderResponse, DefaultResponse,
    FilledOrdersResponse, TickerResponse,
};
use async_trait::async_trait;

#[async_trait]
pub trait DexConnector: Send + Sync {
    async fn get_ticker(&self, symbol: &str) -> Result<TickerResponse, DexError>;

    async fn get_filled_orders(
        &self,
        dex: &str,
        symbol: &str,
    ) -> Result<FilledOrdersResponse, DexError>;

    async fn get_balance(&self) -> Result<BalanceResponse, DexError>;

    async fn clear_filled_order(
        &self,
        symbol: &str,
        order_id: &str,
    ) -> Result<DefaultResponse, DexError>;

    async fn create_order(
        &self,
        symbol: &str,
        size: &str,
        side: &str,
        price: Option<String>,
    ) -> Result<CreateOrderResponse, DexError>;

    async fn cancel_order(&self, order_id: &str) -> Result<DefaultResponse, DexError>;

    async fn close_all_positions(
        &self,
        symbol: Option<String>,
    ) -> Result<DefaultResponse, DexError>;
}

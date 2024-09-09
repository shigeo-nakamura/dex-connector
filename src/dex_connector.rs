use crate::{
    dex_request::DexError, BalanceResponse, CreateOrderResponse, FilledOrdersResponse, OrderSide,
    TickerResponse,
};
use async_trait::async_trait;
use debot_utils::parse_to_decimal;
use lazy_static::lazy_static;
use rust_decimal::Decimal;

lazy_static! {
    static ref DEFAULT_SLIPPAGE: Decimal = Decimal::new(5, 2);
}

pub fn string_to_decimal(string_value: Option<String>) -> Result<Decimal, DexError> {
    match string_value {
        Some(value) => match parse_to_decimal(&value) {
            Ok(v) => return Ok(v),
            Err(_) => return Err(DexError::Other(format!("Invalid value: {}", value))),
        },
        None => return Err(DexError::Other("Value is None".to_owned())),
    }
}

pub fn slippage_price(price: Decimal, is_buy: bool) -> Decimal {
    if is_buy {
        price * (Decimal::new(1, 0) + *DEFAULT_SLIPPAGE)
    } else {
        price * (Decimal::new(1, 0) - *DEFAULT_SLIPPAGE)
    }
}

#[async_trait]
pub trait DexConnector: Send + Sync {
    async fn start(&self) -> Result<(), DexError>;

    async fn stop(&self) -> Result<(), DexError>;

    async fn restart(&self) -> Result<(), DexError>;

    async fn set_leverage(&self, symbol: &str, leverage: u32) -> Result<(), DexError>;

    async fn get_ticker(
        &self,
        symbol: &str,
        test_price: Option<Decimal>,
    ) -> Result<TickerResponse, DexError>;

    async fn get_filled_orders(&self, symbol: &str) -> Result<FilledOrdersResponse, DexError>;

    async fn get_balance(&self) -> Result<BalanceResponse, DexError>;

    async fn clear_filled_order(&self, symbol: &str, order_id: &str) -> Result<(), DexError>;

    async fn clear_all_filled_order(&self) -> Result<(), DexError>;

    async fn create_order(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        price: Option<Decimal>,
        spread: Option<i64>,
    ) -> Result<CreateOrderResponse, DexError>;

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<(), DexError>;

    async fn cancel_all_orders(&self, symbol: Option<String>) -> Result<(), DexError>;

    async fn close_all_positions(&self, symbol: Option<String>) -> Result<(), DexError>;

    async fn clear_last_trades(&self, symbol: &str) -> Result<(), DexError>;
}

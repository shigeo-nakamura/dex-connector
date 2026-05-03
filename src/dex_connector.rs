use crate::{
    dex_request::DexError, BalanceResponse, CanceledOrdersResponse, CombinedBalanceResponse,
    CreateOrderResponse, FilledOrdersResponse, LastTradesResponse, OpenOrdersResponse,
    OrderBookSnapshot, OrderSide, PositionSnapshot, PriceUpdate, TickerResponse, TpSl,
    TriggerOrderStyle,
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

    async fn restart(&self, max_retries: i32) -> Result<(), DexError>;

    async fn set_leverage(&self, symbol: &str, leverage: u32) -> Result<(), DexError>;

    async fn get_ticker(
        &self,
        symbol: &str,
        test_price: Option<Decimal>,
    ) -> Result<TickerResponse, DexError>;

    async fn get_filled_orders(&self, symbol: &str) -> Result<FilledOrdersResponse, DexError>;

    async fn get_canceled_orders(&self, symbol: &str) -> Result<CanceledOrdersResponse, DexError>;

    async fn get_open_orders(&self, symbol: &str) -> Result<OpenOrdersResponse, DexError>;

    async fn get_balance(&self, symbol: Option<&str>) -> Result<BalanceResponse, DexError>;

    async fn get_combined_balance(&self) -> Result<CombinedBalanceResponse, DexError>;

    async fn get_positions(&self) -> Result<Vec<PositionSnapshot>, DexError>;

    async fn get_last_trades(&self, symbol: &str) -> Result<LastTradesResponse, DexError>;
    async fn get_order_book(
        &self,
        symbol: &str,
        depth: usize,
    ) -> Result<OrderBookSnapshot, DexError>;

    async fn clear_filled_order(&self, symbol: &str, trade_id: &str) -> Result<(), DexError>;
    async fn clear_all_filled_orders(&self) -> Result<(), DexError>;

    async fn clear_canceled_order(&self, symbol: &str, order_id: &str) -> Result<(), DexError>;
    async fn clear_all_canceled_orders(&self) -> Result<(), DexError>;

    async fn create_order(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        price: Option<Decimal>,
        spread: Option<i64>,
        reduce_only: bool,
        expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError>;

    async fn create_advanced_trigger_order(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        trigger_px: Decimal,
        limit_px: Option<Decimal>,
        order_style: TriggerOrderStyle,
        slippage_bps: Option<u32>,
        tpsl: TpSl,
        reduce_only: bool,
        expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError>;

    /// Place a taker order with IOC time-in-force.
    ///
    /// `create_order` historically submits with `time_in_force=GTT` (1 h
    /// resting LIMIT) regardless of the caller's intent. For genuine
    /// taker semantics — "cross the book now, accept partial fill,
    /// cancel any unmatched residual immediately" — callers need this
    /// dedicated entry point so the venue actually receives an IOC.
    ///
    /// Implementations should:
    /// - Read top of book (REST fallback if WS empty).
    /// - Place a LIMIT IOC at touch ± 1 tick (aggressive) ± `slippage_bps`
    ///   so the order crosses on the first opposing level even if the
    ///   book moves a few ticks between read and submit.
    /// - Set `post_only=false`, `reduce_only=<param>`,
    ///   `time_in_force=IOC` so the venue terminates the order on first
    ///   match (filled or zero-fill cancel) within ~ms.
    ///
    /// Default impl returns `DexError::Other(...)` so this is opt-in
    /// per connector. bot-strategy#302 — Extended impl reuses the IOC
    /// path that `close_all_positions` already exercises.
    async fn create_order_taker_ioc(
        &self,
        _symbol: &str,
        _size: Decimal,
        _side: OrderSide,
        _slippage_bps: u32,
        _reduce_only: bool,
    ) -> Result<CreateOrderResponse, DexError> {
        Err(DexError::Other(
            "create_order_taker_ioc not implemented for this connector".into(),
        ))
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<(), DexError>;

    async fn cancel_all_orders(&self, symbol: Option<String>) -> Result<(), DexError>;

    async fn cancel_orders(
        &self,
        symbol: Option<String>,
        order_ids: Vec<String>,
    ) -> Result<(), DexError>;

    async fn close_all_positions(&self, symbol: Option<String>) -> Result<(), DexError>;

    async fn clear_last_trades(&self, symbol: &str) -> Result<(), DexError>;

    async fn is_upcoming_maintenance(&self, hours_ahead: i64) -> bool;

    async fn sign_evm_65b(&self, message: &str) -> Result<String, DexError>;

    async fn sign_evm_65b_with_eip191(&self, message: &str) -> Result<String, DexError>;

    /// Subscribe to real-time price updates from WebSocket order book changes.
    /// Returns a broadcast receiver that yields PriceUpdate on each OB update.
    /// Default implementation returns an error (unsupported).
    fn subscribe_price_updates(
        &self,
    ) -> Result<tokio::sync::broadcast::Receiver<PriceUpdate>, DexError> {
        Err(DexError::Other(
            "subscribe_price_updates not supported by this connector".to_string(),
        ))
    }
}

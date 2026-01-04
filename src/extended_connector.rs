use crate::{
    dex_connector::{slippage_price, DexConnector},
    dex_request::{DexError, DexRequest, HttpMethod},
    BalanceResponse, CanceledOrder, CanceledOrdersResponse, CombinedBalanceResponse,
    CreateOrderResponse, FilledOrder, FilledOrdersResponse, LastTrade, LastTradesResponse,
    OpenOrder, OpenOrdersResponse, OrderBookLevel, OrderBookSnapshot, OrderSide, TickerResponse,
    TpSl, TriggerOrderStyle,
};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use rust_crypto_lib_base::{get_order_hash, sign_message};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
use serde_json::json;
use starknet::core::types::Felt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

const MAINNET_API_BASE: &str = "https://api.starknet.extended.exchange/api/v1";
const TESTNET_API_BASE: &str = "https://api.starknet.sepolia.extended.exchange/api/v1";

const DOMAIN_NAME: &str = "Perpetuals";
const DOMAIN_VERSION: &str = "v0";
const DOMAIN_REVISION: &str = "1";
const MAINNET_CHAIN_ID: &str = "SN_MAIN";
const TESTNET_CHAIN_ID: &str = "SN_SEPOLIA";

fn default_taker_fee() -> Decimal {
    Decimal::new(5, 4)
}

#[derive(Debug, Clone, Copy)]
enum ExtendedEnvironment {
    Mainnet,
    Testnet,
}

impl ExtendedEnvironment {
    fn chain_id(&self) -> &'static str {
        match self {
            ExtendedEnvironment::Mainnet => MAINNET_CHAIN_ID,
            ExtendedEnvironment::Testnet => TESTNET_CHAIN_ID,
        }
    }

    fn api_base(&self) -> &'static str {
        match self {
            ExtendedEnvironment::Mainnet => MAINNET_API_BASE,
            ExtendedEnvironment::Testnet => TESTNET_API_BASE,
        }
    }
}

#[derive(Clone)]
struct ExtendedApi {
    request: DexRequest,
    api_key: String,
}

impl ExtendedApi {
    async fn new(api_base: String, api_key: String) -> Result<Self, DexError> {
        Ok(Self {
            request: DexRequest::new(api_base).await?,
            api_key,
        })
    }

    async fn get<T>(&self, path: String, authed: bool) -> Result<T, DexError>
    where
        T: serde::de::DeserializeOwned,
    {
        self.send(HttpMethod::Get, path, authed, None::<serde_json::Value>)
            .await
    }

    async fn post<T, U>(&self, path: String, payload: U, authed: bool) -> Result<T, DexError>
    where
        T: serde::de::DeserializeOwned,
        U: Serialize,
    {
        self.send(HttpMethod::Post, path, authed, Some(payload))
            .await
    }

    async fn delete<T>(&self, path: String, authed: bool) -> Result<T, DexError>
    where
        T: serde::de::DeserializeOwned,
    {
        self.send(HttpMethod::Delete, path, authed, None::<serde_json::Value>)
            .await
    }

    async fn patch<T, U>(&self, path: String, payload: U, authed: bool) -> Result<T, DexError>
    where
        T: serde::de::DeserializeOwned,
        U: Serialize,
    {
        self.send(HttpMethod::Patch, path, authed, Some(payload))
            .await
    }

    async fn send<T, U>(
        &self,
        method: HttpMethod,
        path: String,
        authed: bool,
        payload: Option<U>,
    ) -> Result<T, DexError>
    where
        T: serde::de::DeserializeOwned,
        U: Serialize,
    {
        let mut headers = HashMap::new();
        if authed {
            headers.insert("X-Api-Key".to_string(), self.api_key.clone());
        }

        let json_payload = match payload {
            Some(body) => serde_json::to_string(&body)
                .map_err(|e| DexError::Other(format!("Failed to serialize payload: {}", e)))?,
            None => String::new(),
        };

        let response: WrappedApiResponse<T> = self
            .request
            .handle_request::<WrappedApiResponse<T>, serde_json::Value>(
                method,
                path,
                &headers,
                json_payload,
            )
            .await?;

        if response.status != ResponseStatus::Ok || response.error.is_some() {
            let message = response
                .error
                .as_ref()
                .map(|err| err.message.clone())
                .unwrap_or_else(|| "Extended API error".to_string());
            return Err(DexError::ServerResponse(message));
        }

        response
            .data
            .ok_or_else(|| DexError::Other("Extended API returned empty data".to_string()))
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
enum ResponseStatus {
    Ok,
    Error,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
struct ResponseError {
    code: i64,
    message: String,
    debug_info: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
struct WrappedApiResponse<T> {
    status: ResponseStatus,
    data: Option<T>,
    error: Option<ResponseError>,
    pagination: Option<Pagination>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
struct Pagination {
    cursor: Option<i64>,
    count: i64,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
struct MarketStatsModel {
    daily_volume: Decimal,
    daily_volume_base: Decimal,
    daily_price_change: Decimal,
    daily_low: Decimal,
    daily_high: Decimal,
    last_price: Decimal,
    ask_price: Decimal,
    bid_price: Decimal,
    mark_price: Decimal,
    index_price: Decimal,
    funding_rate: Decimal,
    next_funding_rate: i64,
    open_interest: Decimal,
    open_interest_base: Decimal,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
struct TradingConfigModel {
    min_order_size: Decimal,
    min_order_size_change: Decimal,
    min_price_change: Decimal,
    max_market_order_value: Decimal,
    max_limit_order_value: Decimal,
    max_position_value: Decimal,
    max_leverage: Decimal,
    max_num_orders: i64,
    limit_price_cap: Decimal,
    limit_price_floor: Decimal,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
struct L2ConfigModel {
    #[serde(rename = "type")]
    l2_type: String,
    collateral_id: String,
    collateral_resolution: i64,
    synthetic_id: String,
    synthetic_resolution: i64,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
struct MarketModel {
    name: String,
    asset_name: String,
    asset_precision: i64,
    collateral_asset_name: String,
    collateral_asset_precision: i64,
    active: bool,
    market_stats: MarketStatsModel,
    trading_config: TradingConfigModel,
    l2_config: L2ConfigModel,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderbookQuantityModel {
    qty: Decimal,
    price: Decimal,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
struct OrderbookUpdateModel {
    market: String,
    bid: Vec<OrderbookQuantityModel>,
    ask: Vec<OrderbookQuantityModel>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
struct BalanceModel {
    collateral_name: String,
    balance: Decimal,
    equity: Decimal,
    available_for_trade: Decimal,
    available_for_withdrawal: Decimal,
    unrealised_pnl: Decimal,
    initial_margin: Decimal,
    margin_ratio: Decimal,
    updated_time: i64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
struct OpenOrderModel {
    id: i64,
    account_id: i64,
    external_id: String,
    market: String,
    #[serde(rename = "type")]
    order_type: String,
    side: String,
    status: String,
    status_reason: Option<String>,
    price: Decimal,
    average_price: Option<Decimal>,
    qty: Decimal,
    filled_qty: Option<Decimal>,
    reduce_only: bool,
    post_only: bool,
    created_time: i64,
    updated_time: i64,
    expiry_time: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
struct AccountTradeModel {
    id: i64,
    account_id: i64,
    market: String,
    order_id: i64,
    side: String,
    price: Decimal,
    qty: Decimal,
    value: Decimal,
    fee: Decimal,
    is_taker: bool,
    trade_type: String,
    created_time: i64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
struct PlacedOrderModel {
    id: i64,
    external_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SettlementSignatureModel {
    r: String,
    s: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StarkSettlementModel {
    signature: SettlementSignatureModel,
    stark_key: String,
    collateral_position: Decimal,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StarkDebuggingOrderAmountsModel {
    collateral_amount: Decimal,
    fee_amount: Decimal,
    synthetic_amount: Decimal,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct NewOrderModel {
    id: String,
    market: String,
    #[serde(rename = "type")]
    order_type: String,
    side: String,
    qty: Decimal,
    price: Decimal,
    reduce_only: bool,
    post_only: bool,
    time_in_force: String,
    expiry_epoch_millis: i64,
    fee: Decimal,
    self_trade_protection_level: String,
    nonce: Decimal,
    cancel_id: Option<String>,
    settlement: StarkSettlementModel,
    tp_sl_type: Option<String>,
    take_profit: Option<serde_json::Value>,
    stop_loss: Option<serde_json::Value>,
    debugging_amounts: Option<StarkDebuggingOrderAmountsModel>,
    #[serde(rename = "builderFee")]
    builder_fee: Option<Decimal>,
    #[serde(rename = "builderId")]
    builder_id: Option<i64>,
}

#[allow(dead_code)]
struct SettlementData {
    stark_synthetic_amount: i64,
    stark_collateral_amount: i64,
    stark_fee_amount: u64,
    fee_rate: Decimal,
    order_hash: Felt,
    settlement: StarkSettlementModel,
    debugging_amounts: StarkDebuggingOrderAmountsModel,
}

pub struct ExtendedConnector {
    api: ExtendedApi,
    public_key: String,
    private_key: String,
    vault: u64,
    env: ExtendedEnvironment,
    market_cache: Arc<RwLock<HashMap<String, MarketModel>>>,
    filled_orders: Arc<RwLock<HashMap<String, Vec<FilledOrder>>>>,
    canceled_orders: Arc<RwLock<HashMap<String, Vec<CanceledOrder>>>>,
    last_trades: Arc<RwLock<HashMap<String, Vec<LastTrade>>>>,
}

impl ExtendedConnector {
    pub async fn new(
        api_key: String,
        public_key: String,
        private_key: String,
        vault: u64,
        base_url: Option<String>,
    ) -> Result<Self, DexError> {
        let env = base_url
            .as_deref()
            .map(Self::infer_environment)
            .unwrap_or(ExtendedEnvironment::Mainnet);
        let api_base = base_url.unwrap_or_else(|| env.api_base().to_string());
        let api = ExtendedApi::new(api_base, api_key).await?;

        Ok(Self {
            api,
            public_key,
            private_key,
            vault,
            env,
            market_cache: Arc::new(RwLock::new(HashMap::new())),
            filled_orders: Arc::new(RwLock::new(HashMap::new())),
            canceled_orders: Arc::new(RwLock::new(HashMap::new())),
            last_trades: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    fn infer_environment(base_url: &str) -> ExtendedEnvironment {
        let lowered = base_url.to_lowercase();
        if lowered.contains("sepolia") || lowered.contains("testnet") {
            ExtendedEnvironment::Testnet
        } else {
            ExtendedEnvironment::Mainnet
        }
    }

    fn domain_chain_id(&self) -> &'static str {
        self.env.chain_id()
    }

    async fn get_market(&self, symbol: &str) -> Result<MarketModel, DexError> {
        {
            let cache = self.market_cache.read().await;
            if let Some(market) = cache.get(symbol) {
                return Ok(market.clone());
            }
        }

        let path = build_query(
            "/info/markets",
            vec![("market".to_string(), symbol.to_string())],
        );
        let markets: Vec<MarketModel> = self.api.get(path, false).await?;
        let market = markets
            .into_iter()
            .find(|m| m.name == symbol)
            .ok_or_else(|| DexError::Other(format!("Market not found: {}", symbol)))?;

        let mut cache = self.market_cache.write().await;
        cache.insert(symbol.to_string(), market.clone());
        Ok(market)
    }

    fn parse_private_key(&self) -> Result<Felt, DexError> {
        Felt::from_hex(&self.private_key)
            .map_err(|e| DexError::Other(format!("Invalid private key hex: {}", e)))
    }

    fn parse_public_key(&self) -> Result<Felt, DexError> {
        Felt::from_hex(&self.public_key)
            .map_err(|e| DexError::Other(format!("Invalid public key hex: {}", e)))
    }

    fn to_epoch_millis(value: DateTime<Utc>) -> i64 {
        let secs = value.timestamp();
        let nanos = value.timestamp_subsec_nanos() as i64;
        let extra_ms = if nanos % 1_000_000 == 0 { 0 } else { 1 };
        secs * 1000 + (nanos / 1_000_000) + extra_ms
    }

    fn settlement_expiration_secs(value: DateTime<Utc>) -> i64 {
        let expiry = value + Duration::days(14);
        let secs = expiry.timestamp();
        let nanos = expiry.timestamp_subsec_nanos() as i64;
        secs + if nanos == 0 { 0 } else { 1 }
    }

    fn to_stark_amount(
        value: Decimal,
        resolution: i64,
        rounding: RoundingStrategy,
    ) -> Result<i64, DexError> {
        let scaled = value
            * Decimal::from_i64(resolution).ok_or_else(|| {
                DexError::Other("Invalid resolution for amount conversion".to_string())
            })?;
        let rounded = scaled.round_dp_with_strategy(0, rounding);
        rounded
            .to_i64()
            .ok_or_else(|| DexError::Other("Failed to convert amount to i64".to_string()))
    }

    fn compute_settlement(
        &self,
        market: &MarketModel,
        side: &str,
        synthetic_amount: Decimal,
        price: Decimal,
        expire_time: DateTime<Utc>,
        nonce: u64,
    ) -> Result<SettlementData, DexError> {
        let is_buying = side == "BUY";
        let synthetic_resolution = market.l2_config.synthetic_resolution;
        let collateral_resolution = market.l2_config.collateral_resolution;

        let collateral_amount = synthetic_amount * price;
        let total_fee_rate = default_taker_fee();
        let fee_amount = total_fee_rate * collateral_amount;

        let rounding_main = if is_buying {
            RoundingStrategy::ToPositiveInfinity
        } else {
            RoundingStrategy::ToNegativeInfinity
        };
        let rounding_fee = RoundingStrategy::ToPositiveInfinity;

        let mut stark_synthetic_amount =
            Self::to_stark_amount(synthetic_amount, synthetic_resolution, rounding_main)?;
        let mut stark_collateral_amount =
            Self::to_stark_amount(collateral_amount, collateral_resolution, rounding_main)?;
        let stark_fee_amount =
            Self::to_stark_amount(fee_amount, collateral_resolution, rounding_fee)? as u64;

        if is_buying {
            stark_collateral_amount = -stark_collateral_amount;
        } else {
            stark_synthetic_amount = -stark_synthetic_amount;
        }

        let expiration_secs = Self::settlement_expiration_secs(expire_time);
        let order_hash = get_order_hash(
            self.vault.to_string(),
            market.l2_config.synthetic_id.clone(),
            stark_synthetic_amount.to_string(),
            market.l2_config.collateral_id.clone(),
            stark_collateral_amount.to_string(),
            market.l2_config.collateral_id.clone(),
            stark_fee_amount.to_string(),
            expiration_secs.to_string(),
            nonce.to_string(),
            self.public_key.clone(),
            DOMAIN_NAME.to_string(),
            DOMAIN_VERSION.to_string(),
            self.domain_chain_id().to_string(),
            DOMAIN_REVISION.to_string(),
        )
        .map_err(|e| DexError::Other(format!("Failed to compute order hash: {}", e)))?;

        let private_key = self.parse_private_key()?;
        let signature = sign_message(&order_hash, &private_key)
            .map_err(|e| DexError::Other(format!("Failed to sign order: {}", e)))?;

        let settlement = StarkSettlementModel {
            signature: SettlementSignatureModel {
                r: signature.r.to_hex_string(),
                s: signature.s.to_hex_string(),
            },
            stark_key: self.parse_public_key()?.to_hex_string(),
            collateral_position: Decimal::from(self.vault),
        };

        let debugging_amounts = StarkDebuggingOrderAmountsModel {
            collateral_amount: Decimal::from(stark_collateral_amount),
            fee_amount: Decimal::from(stark_fee_amount),
            synthetic_amount: Decimal::from(stark_synthetic_amount),
        };

        Ok(SettlementData {
            stark_synthetic_amount,
            stark_collateral_amount,
            stark_fee_amount,
            fee_rate: total_fee_rate,
            order_hash,
            settlement,
            debugging_amounts,
        })
    }
}

#[async_trait]
impl DexConnector for ExtendedConnector {
    async fn start(&self) -> Result<(), DexError> {
        Ok(())
    }

    async fn stop(&self) -> Result<(), DexError> {
        Ok(())
    }

    async fn restart(&self, _max_retries: i32) -> Result<(), DexError> {
        Ok(())
    }

    async fn set_leverage(&self, symbol: &str, leverage: u32) -> Result<(), DexError> {
        let payload = json!({
            "market": symbol,
            "leverage": leverage,
        });
        let _response: EmptyResponse = self
            .api
            .patch("/user/leverage".to_string(), payload, true)
            .await?;
        Ok(())
    }

    async fn get_ticker(
        &self,
        symbol: &str,
        _test_price: Option<Decimal>,
    ) -> Result<TickerResponse, DexError> {
        let market = self.get_market(symbol).await?;
        Ok(TickerResponse {
            symbol: market.name.clone(),
            price: market.market_stats.last_price,
            min_tick: Some(market.trading_config.min_price_change),
            min_order: Some(market.trading_config.min_order_size),
            volume: Some(market.market_stats.daily_volume),
            num_trades: None,
            open_interest: Some(market.market_stats.open_interest),
            funding_rate: Some(market.market_stats.funding_rate),
            oracle_price: Some(market.market_stats.index_price),
        })
    }

    async fn get_filled_orders(&self, symbol: &str) -> Result<FilledOrdersResponse, DexError> {
        let path = build_query(
            "/user/trades",
            vec![("market".to_string(), symbol.to_string())],
        );
        let trades: Vec<AccountTradeModel> = self.api.get(path, true).await?;
        let orders = trades
            .into_iter()
            .map(|trade| FilledOrder {
                order_id: trade.order_id.to_string(),
                is_rejected: false,
                trade_id: trade.id.to_string(),
                filled_side: match trade.side.as_str() {
                    "BUY" => Some(OrderSide::Long),
                    "SELL" => Some(OrderSide::Short),
                    _ => None,
                },
                filled_size: Some(trade.qty),
                filled_value: Some(trade.value),
                filled_fee: Some(trade.fee),
            })
            .collect::<Vec<_>>();

        let mut cache = self.filled_orders.write().await;
        cache.insert(symbol.to_string(), orders.clone());
        Ok(FilledOrdersResponse { orders })
    }

    async fn get_canceled_orders(&self, symbol: &str) -> Result<CanceledOrdersResponse, DexError> {
        let path = build_query(
            "/user/orders/history",
            vec![("market".to_string(), symbol.to_string())],
        );
        let orders_history: Vec<OpenOrderModel> = self.api.get(path, true).await?;
        let orders = orders_history
            .into_iter()
            .filter(|order| order.status == "CANCELLED")
            .map(|order| CanceledOrder {
                order_id: order.external_id,
                canceled_timestamp: order.updated_time as u64,
            })
            .collect::<Vec<_>>();

        let mut cache = self.canceled_orders.write().await;
        cache.insert(symbol.to_string(), orders.clone());
        Ok(CanceledOrdersResponse { orders })
    }

    async fn get_open_orders(&self, symbol: &str) -> Result<OpenOrdersResponse, DexError> {
        let path = build_query(
            "/user/orders",
            vec![("market".to_string(), symbol.to_string())],
        );
        let open_orders: Vec<OpenOrderModel> = self.api.get(path, true).await?;
        let orders = open_orders
            .into_iter()
            .map(|order| OpenOrder {
                order_id: order.external_id.clone(),
                symbol: order.market.clone(),
                side: if order.side == "BUY" {
                    OrderSide::Long
                } else {
                    OrderSide::Short
                },
                size: order.qty,
                price: order.price,
                status: order.status,
            })
            .collect::<Vec<_>>();
        Ok(OpenOrdersResponse { orders })
    }

    async fn get_balance(&self, symbol: Option<&str>) -> Result<BalanceResponse, DexError> {
        let balance: BalanceModel = self.api.get("/user/balance".to_string(), true).await?;
        if let Some(symbol) = symbol {
            if symbol != balance.collateral_name {
                return Err(DexError::Other(format!(
                    "Unsupported balance symbol {} (collateral: {})",
                    symbol, balance.collateral_name
                )));
            }
        }
        Ok(BalanceResponse {
            equity: balance.equity,
            balance: balance.balance,
            position_entry_price: None,
            position_sign: None,
        })
    }

    async fn get_combined_balance(&self) -> Result<CombinedBalanceResponse, DexError> {
        let balance: BalanceModel = self.api.get("/user/balance".to_string(), true).await?;
        let mut token_balances = HashMap::new();
        token_balances.insert(
            balance.collateral_name.clone(),
            BalanceResponse {
                equity: balance.equity,
                balance: balance.balance,
                position_entry_price: None,
                position_sign: None,
            },
        );
        Ok(CombinedBalanceResponse {
            usd_balance: balance.equity,
            token_balances,
        })
    }

    async fn get_last_trades(&self, symbol: &str) -> Result<LastTradesResponse, DexError> {
        let path = build_query(
            "/user/trades",
            vec![("market".to_string(), symbol.to_string())],
        );
        let trades: Vec<AccountTradeModel> = self.api.get(path, true).await?;
        let last_trades = trades
            .into_iter()
            .map(|trade| LastTrade {
                price: trade.price,
                size: Some(trade.qty),
                side: match trade.side.as_str() {
                    "BUY" => Some(OrderSide::Long),
                    "SELL" => Some(OrderSide::Short),
                    _ => None,
                },
            })
            .collect::<Vec<_>>();

        let mut cache = self.last_trades.write().await;
        cache.insert(symbol.to_string(), last_trades.clone());
        Ok(LastTradesResponse {
            trades: last_trades,
        })
    }

    async fn get_order_book(
        &self,
        symbol: &str,
        depth: usize,
    ) -> Result<OrderBookSnapshot, DexError> {
        let path = format!("/info/markets/{}/orderbook", symbol);
        let snapshot: OrderbookUpdateModel = self.api.get(path, false).await?;
        let bids = snapshot
            .bid
            .into_iter()
            .take(depth)
            .map(|level| OrderBookLevel {
                price: level.price,
                size: level.qty,
            })
            .collect::<Vec<_>>();
        let asks = snapshot
            .ask
            .into_iter()
            .take(depth)
            .map(|level| OrderBookLevel {
                price: level.price,
                size: level.qty,
            })
            .collect::<Vec<_>>();
        Ok(OrderBookSnapshot { bids, asks })
    }

    async fn clear_filled_order(&self, symbol: &str, trade_id: &str) -> Result<(), DexError> {
        let mut filled_orders = self.filled_orders.write().await;
        if let Some(orders) = filled_orders.get_mut(symbol) {
            let initial_len = orders.len();
            orders.retain(|order| order.trade_id != trade_id);
            if orders.len() < initial_len {
                Ok(())
            } else {
                Err(DexError::Other(format!(
                    "Trade ID {} not found for symbol {}",
                    trade_id, symbol
                )))
            }
        } else {
            Err(DexError::Other(format!(
                "No filled orders found for symbol {}",
                symbol
            )))
        }
    }

    async fn clear_all_filled_orders(&self) -> Result<(), DexError> {
        let mut filled_orders = self.filled_orders.write().await;
        filled_orders.clear();
        Ok(())
    }

    async fn clear_canceled_order(&self, symbol: &str, order_id: &str) -> Result<(), DexError> {
        let mut canceled_orders = self.canceled_orders.write().await;
        if let Some(orders) = canceled_orders.get_mut(symbol) {
            let initial_len = orders.len();
            orders.retain(|order| order.order_id != order_id);
            if orders.len() < initial_len {
                Ok(())
            } else {
                Err(DexError::Other(format!(
                    "Order ID {} not found for symbol {}",
                    order_id, symbol
                )))
            }
        } else {
            Err(DexError::Other(format!(
                "No canceled orders found for symbol {}",
                symbol
            )))
        }
    }

    async fn clear_all_canceled_orders(&self) -> Result<(), DexError> {
        let mut canceled_orders = self.canceled_orders.write().await;
        canceled_orders.clear();
        Ok(())
    }

    async fn create_order(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        price: Option<Decimal>,
        _spread: Option<i64>,
        expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        let order_price = match price {
            Some(price) => price,
            None => {
                let ticker = self.get_ticker(symbol, None).await?;
                slippage_price(ticker.price, side == OrderSide::Long)
            }
        };

        let expire_time = match expiry_secs {
            Some(secs) => Utc::now() + Duration::seconds(secs as i64),
            None => Utc::now() + Duration::hours(1),
        };

        let nonce = rand::random::<u32>() as u64;
        let market = self.get_market(symbol).await?;
        let side_str = match side {
            OrderSide::Long => "BUY",
            OrderSide::Short => "SELL",
        };

        let settlement =
            self.compute_settlement(&market, side_str, size, order_price, expire_time, nonce)?;

        let order_id = format!("dex-{}", settlement.order_hash.to_hex_string());

        let order = NewOrderModel {
            id: order_id.clone(),
            market: market.name.clone(),
            order_type: "LIMIT".to_string(),
            side: side_str.to_string(),
            qty: size,
            price: order_price,
            reduce_only: false,
            post_only: false,
            time_in_force: "GTT".to_string(),
            expiry_epoch_millis: Self::to_epoch_millis(expire_time),
            fee: settlement.fee_rate,
            self_trade_protection_level: "ACCOUNT".to_string(),
            nonce: Decimal::from(nonce),
            cancel_id: None,
            settlement: settlement.settlement,
            tp_sl_type: None,
            take_profit: None,
            stop_loss: None,
            debugging_amounts: Some(settlement.debugging_amounts),
            builder_fee: None,
            builder_id: None,
        };

        let response: PlacedOrderModel = self
            .api
            .post("/user/order".to_string(), order, true)
            .await?;

        Ok(CreateOrderResponse {
            order_id: response.external_id,
            ordered_price: order_price,
            ordered_size: size,
        })
    }

    async fn create_advanced_trigger_order(
        &self,
        _symbol: &str,
        _size: Decimal,
        _side: OrderSide,
        _trigger_px: Decimal,
        _limit_px: Option<Decimal>,
        _order_style: TriggerOrderStyle,
        _slippage_bps: Option<u32>,
        _tpsl: TpSl,
        _reduce_only: bool,
        _expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        Err(DexError::Other(
            "Advanced trigger orders not supported for Extended".to_string(),
        ))
    }

    async fn cancel_order(&self, _symbol: &str, order_id: &str) -> Result<(), DexError> {
        let path = build_query(
            "/user/order",
            vec![("externalId".to_string(), order_id.to_string())],
        );
        let _response: EmptyResponse = self.api.delete(path, true).await?;
        Ok(())
    }

    async fn cancel_all_orders(&self, symbol: Option<String>) -> Result<(), DexError> {
        let payload = match symbol {
            Some(symbol) => json!({
                "markets": vec![symbol],
                "cancelAll": false,
            }),
            None => json!({
                "cancelAll": true,
            }),
        };
        let _response: EmptyResponse = self
            .api
            .post("/user/order/massCancel".to_string(), payload, true)
            .await?;
        Ok(())
    }

    async fn cancel_orders(
        &self,
        _symbol: Option<String>,
        order_ids: Vec<String>,
    ) -> Result<(), DexError> {
        let payload = json!({
            "externalOrderIds": order_ids,
        });
        let _response: EmptyResponse = self
            .api
            .post("/user/order/massCancel".to_string(), payload, true)
            .await?;
        Ok(())
    }

    async fn close_all_positions(&self, _symbol: Option<String>) -> Result<(), DexError> {
        Err(DexError::Other(
            "close_all_positions not supported for Extended".to_string(),
        ))
    }

    async fn clear_last_trades(&self, symbol: &str) -> Result<(), DexError> {
        let mut last_trades = self.last_trades.write().await;
        if last_trades.remove(symbol).is_some() {
            Ok(())
        } else {
            Err(DexError::Other(format!(
                "No last trades found for symbol {}",
                symbol
            )))
        }
    }

    async fn is_upcoming_maintenance(&self, _hours_ahead: i64) -> bool {
        false
    }

    async fn sign_evm_65b(&self, _message: &str) -> Result<String, DexError> {
        Err(DexError::Other(
            "sign_evm_65b not supported for Extended".to_string(),
        ))
    }

    async fn sign_evm_65b_with_eip191(&self, _message: &str) -> Result<String, DexError> {
        Err(DexError::Other(
            "sign_evm_65b_with_eip191 not supported for Extended".to_string(),
        ))
    }
}

#[derive(Debug, Deserialize)]
struct EmptyResponse {}

pub async fn create_extended_connector(
    api_key: String,
    public_key: String,
    private_key: String,
    vault: u64,
    base_url: Option<String>,
) -> Result<Box<dyn DexConnector>, DexError> {
    let connector =
        ExtendedConnector::new(api_key, public_key, private_key, vault, base_url).await?;
    Ok(Box::new(connector))
}

fn build_query(base: &str, params: Vec<(String, String)>) -> String {
    if params.is_empty() {
        return base.to_string();
    }
    let mut parts = Vec::new();
    for (key, value) in params {
        let encoded = format!(
            "{}={}",
            urlencoding::encode(&key),
            urlencoding::encode(&value)
        );
        parts.push(encoded);
    }
    format!("{}?{}", base, parts.join("&"))
}

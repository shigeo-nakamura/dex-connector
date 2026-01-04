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
use futures::StreamExt;
use rust_crypto_lib_base::{get_order_hash, sign_message};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
use serde_json::json;
use starknet::core::types::Felt;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;

const MAINNET_API_BASE: &str = "https://api.starknet.extended.exchange/api/v1";
const TESTNET_API_BASE: &str = "https://api.starknet.sepolia.extended.exchange/api/v1";

const DOMAIN_NAME: &str = "Perpetuals";
const DOMAIN_VERSION: &str = "v0";
const DOMAIN_REVISION: &str = "1";
const MAINNET_CHAIN_ID: &str = "SN_MAIN";
const TESTNET_CHAIN_ID: &str = "SN_SEPOLIA";
const DEFAULT_ORDERBOOK_DEPTH: usize = 50;

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
    websocket_url: Option<String>,
    tracked_symbols: Vec<String>,
    market_cache: Arc<RwLock<HashMap<String, MarketModel>>>,
    order_book_cache: Arc<RwLock<HashMap<String, OrderBookSnapshot>>>,
    balance_cache: Arc<RwLock<Option<BalanceResponse>>>,
    open_orders_cache: Arc<RwLock<HashMap<String, Vec<OpenOrder>>>>,
    filled_orders: Arc<RwLock<HashMap<String, Vec<FilledOrder>>>>,
    canceled_orders: Arc<RwLock<HashMap<String, Vec<CanceledOrder>>>>,
    last_trades: Arc<RwLock<HashMap<String, Vec<LastTrade>>>>,
    ws_started: AtomicBool,
    ws_tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl ExtendedConnector {
    pub async fn new(
        api_key: String,
        public_key: String,
        private_key: String,
        vault: u64,
        base_url: Option<String>,
        websocket_url: Option<String>,
        tracked_symbols: Vec<String>,
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
            websocket_url,
            tracked_symbols,
            market_cache: Arc::new(RwLock::new(HashMap::new())),
            order_book_cache: Arc::new(RwLock::new(HashMap::new())),
            balance_cache: Arc::new(RwLock::new(None)),
            open_orders_cache: Arc::new(RwLock::new(HashMap::new())),
            filled_orders: Arc::new(RwLock::new(HashMap::new())),
            canceled_orders: Arc::new(RwLock::new(HashMap::new())),
            last_trades: Arc::new(RwLock::new(HashMap::new())),
            ws_started: AtomicBool::new(false),
            ws_tasks: Mutex::new(Vec::new()),
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

    fn build_ws_url(&self, path: &str) -> Option<String> {
        let base = self.websocket_url.as_ref()?;
        let base = base.trim_end_matches('/');
        Some(format!("{base}{path}"))
    }

    async fn spawn_ws_tasks(&self) {
        if self.websocket_url.is_none() {
            return;
        }
        if self.ws_started.swap(true, Ordering::SeqCst) {
            return;
        }

        let mut handles = self.ws_tasks.lock().await;
        let symbols = self.tracked_symbols.clone();

        for symbol in symbols.iter() {
            let orderbook_path = format!("/orderbooks/{symbol}?depth={DEFAULT_ORDERBOOK_DEPTH}");
            if let Some(url) = self.build_ws_url(&orderbook_path) {
                let order_book_cache = Arc::clone(&self.order_book_cache);
                let symbol = symbol.clone();
                handles.push(tokio::spawn(async move {
                    loop {
                        if let Err(err) = stream_orderbooks(&url, &symbol, &order_book_cache).await
                        {
                            log::warn!("orderbook stream error: {err}");
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    }
                }));
            }

            let trades_path = format!("/publicTrades/{symbol}");
            if let Some(url) = self.build_ws_url(&trades_path) {
                let last_trades = Arc::clone(&self.last_trades);
                let symbol = symbol.clone();
                handles.push(tokio::spawn(async move {
                    loop {
                        if let Err(err) = stream_trades(&url, &symbol, &last_trades).await {
                            log::warn!("public trades stream error: {err}");
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    }
                }));
            }
        }

        if let Some(url) = self.build_ws_url("/account") {
            let api_key = self.api.api_key.clone();
            let balance_cache = Arc::clone(&self.balance_cache);
            let open_orders_cache = Arc::clone(&self.open_orders_cache);
            let filled_orders = Arc::clone(&self.filled_orders);
            handles.push(tokio::spawn(async move {
                loop {
                    if let Err(err) = stream_account(
                        &url,
                        &api_key,
                        &balance_cache,
                        &open_orders_cache,
                        &filled_orders,
                    )
                    .await
                    {
                        log::warn!("account stream error: {err}");
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            }));
        }
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

fn copy_balance(balance: &BalanceResponse) -> BalanceResponse {
    BalanceResponse {
        equity: balance.equity,
        balance: balance.balance,
        position_entry_price: balance.position_entry_price,
        position_sign: balance.position_sign,
    }
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
struct WrappedStreamResponse<T> {
    #[serde(rename = "type")]
    msg_type: Option<String>,
    data: Option<T>,
    error: Option<String>,
    ts: i64,
    seq: i64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct StreamOrderbookQuantity {
    #[serde(alias = "q")]
    qty: Decimal,
    #[serde(alias = "p")]
    price: Decimal,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct StreamOrderbookUpdate {
    #[serde(alias = "m")]
    market: String,
    #[serde(alias = "b")]
    bid: Vec<StreamOrderbookQuantity>,
    #[serde(alias = "a")]
    ask: Vec<StreamOrderbookQuantity>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct StreamTradeModel {
    #[serde(alias = "i")]
    id: i64,
    #[serde(alias = "m")]
    market: String,
    #[serde(alias = "S")]
    side: String,
    #[serde(alias = "T")]
    timestamp: i64,
    #[serde(alias = "p")]
    price: Decimal,
    #[serde(alias = "q")]
    qty: Decimal,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountStreamData {
    orders: Option<Vec<OpenOrderModel>>,
    trades: Option<Vec<AccountTradeModel>>,
    balance: Option<BalanceModel>,
}

async fn connect_ws(
    url: &str,
    api_key: Option<&str>,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    DexError,
> {
    let mut request = url
        .into_client_request()
        .map_err(|e| DexError::Other(format!("Invalid websocket url: {e}")))?;
    {
        let headers = request.headers_mut();
        headers.insert("User-Agent", HeaderValue::from_static("dex-connector"));
        if let Some(key) = api_key {
            headers.insert(
                "X-Api-Key",
                HeaderValue::from_str(key)
                    .map_err(|e| DexError::Other(format!("Invalid API key header: {e}")))?,
            );
        }
    }
    let (stream, _) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(|e| DexError::Other(format!("Websocket connect failed: {e}")))?;
    Ok(stream)
}

async fn stream_orderbooks(
    url: &str,
    symbol: &str,
    order_book_cache: &Arc<RwLock<HashMap<String, OrderBookSnapshot>>>,
) -> Result<(), DexError> {
    let mut ws = connect_ws(url, None).await?;
    while let Some(message) = ws.next().await {
        let message = message.map_err(|e| DexError::Other(format!("ws error: {e}")))?;
        if !message.is_text() {
            continue;
        }
        let payload: WrappedStreamResponse<StreamOrderbookUpdate> =
            serde_json::from_str(message.to_text().unwrap_or(""))
                .map_err(|e| DexError::Other(format!("orderbook parse error: {e}")))?;
        if let Some(update) = payload.data {
            let bids = update
                .bid
                .into_iter()
                .map(|level| OrderBookLevel {
                    price: level.price,
                    size: level.qty,
                })
                .collect::<Vec<_>>();
            let asks = update
                .ask
                .into_iter()
                .map(|level| OrderBookLevel {
                    price: level.price,
                    size: level.qty,
                })
                .collect::<Vec<_>>();
            let mut cache = order_book_cache.write().await;
            cache.insert(symbol.to_string(), OrderBookSnapshot { bids, asks });
        }
    }
    Ok(())
}

async fn stream_trades(
    url: &str,
    symbol: &str,
    last_trades: &Arc<RwLock<HashMap<String, Vec<LastTrade>>>>,
) -> Result<(), DexError> {
    let mut ws = connect_ws(url, None).await?;
    while let Some(message) = ws.next().await {
        let message = message.map_err(|e| DexError::Other(format!("ws error: {e}")))?;
        if !message.is_text() {
            continue;
        }
        let payload: WrappedStreamResponse<Vec<StreamTradeModel>> =
            serde_json::from_str(message.to_text().unwrap_or(""))
                .map_err(|e| DexError::Other(format!("trade parse error: {e}")))?;
        if let Some(trades) = payload.data {
            let mut mapped = Vec::new();
            for trade in trades {
                let side = match trade.side.as_str() {
                    "BUY" => Some(OrderSide::Long),
                    "SELL" => Some(OrderSide::Short),
                    _ => None,
                };
                mapped.push(LastTrade {
                    price: trade.price,
                    size: Some(trade.qty),
                    side,
                });
            }
            let mut cache = last_trades.write().await;
            let entry = cache.entry(symbol.to_string()).or_default();
            entry.extend(mapped);
            if entry.len() > 200 {
                let excess = entry.len() - 200;
                entry.drain(0..excess);
            }
        }
    }
    Ok(())
}

async fn stream_account(
    url: &str,
    api_key: &str,
    balance_cache: &Arc<RwLock<Option<BalanceResponse>>>,
    open_orders_cache: &Arc<RwLock<HashMap<String, Vec<OpenOrder>>>>,
    filled_orders: &Arc<RwLock<HashMap<String, Vec<FilledOrder>>>>,
) -> Result<(), DexError> {
    let mut ws = connect_ws(url, Some(api_key)).await?;
    while let Some(message) = ws.next().await {
        let message = message.map_err(|e| DexError::Other(format!("ws error: {e}")))?;
        if !message.is_text() {
            continue;
        }
        let payload: WrappedStreamResponse<AccountStreamData> =
            serde_json::from_str(message.to_text().unwrap_or(""))
                .map_err(|e| DexError::Other(format!("account parse error: {e}")))?;
        if let Some(data) = payload.data {
            if let Some(balance) = data.balance {
                let mut cache = balance_cache.write().await;
                *cache = Some(BalanceResponse {
                    equity: balance.equity,
                    balance: balance.balance,
                    position_entry_price: None,
                    position_sign: None,
                });
            }

            if let Some(orders) = data.orders {
                let mut cache = open_orders_cache.write().await;
                cache.clear();
                for order in orders {
                    let entry = cache.entry(order.market.clone()).or_default();
                    entry.push(OpenOrder {
                        order_id: order.external_id.clone(),
                        symbol: order.market.clone(),
                        side: if order.side == "BUY" {
                            OrderSide::Long
                        } else {
                            OrderSide::Short
                        },
                        size: order.qty,
                        price: order.price,
                        status: order.status.clone(),
                    });
                }
            }

            if let Some(trades) = data.trades {
                let mut cache = filled_orders.write().await;
                for trade in trades {
                    let entry = cache.entry(trade.market.clone()).or_default();
                    entry.push(FilledOrder {
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
                    });
                }
            }
        }
    }
    Ok(())
}

#[async_trait]
impl DexConnector for ExtendedConnector {
    async fn start(&self) -> Result<(), DexError> {
        self.spawn_ws_tasks().await;
        Ok(())
    }

    async fn stop(&self) -> Result<(), DexError> {
        let mut handles = self.ws_tasks.lock().await;
        for handle in handles.drain(..) {
            handle.abort();
        }
        self.ws_started.store(false, Ordering::SeqCst);
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
        let price = if self.websocket_url.is_some() {
            let trades = self.last_trades.read().await;
            if let Some(items) = trades.get(symbol).and_then(|v| v.last()) {
                items.price
            } else {
                let orderbooks = self.order_book_cache.read().await;
                if let Some(snapshot) = orderbooks.get(symbol) {
                    let bid = snapshot.bids.first().map(|v| v.price);
                    let ask = snapshot.asks.first().map(|v| v.price);
                    match (bid, ask) {
                        (Some(bid), Some(ask)) => (bid + ask) / Decimal::new(2, 0),
                        (Some(bid), None) => bid,
                        (None, Some(ask)) => ask,
                        _ => {
                            return Err(DexError::Other(
                                "ticker unavailable: waiting for websocket data".to_string(),
                            ))
                        }
                    }
                } else {
                    return Err(DexError::Other(
                        "ticker unavailable: waiting for websocket data".to_string(),
                    ));
                }
            }
        } else {
            market.market_stats.last_price
        };
        Ok(TickerResponse {
            symbol: market.name.clone(),
            price,
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
        if self.websocket_url.is_some() {
            let cache = self.filled_orders.read().await;
            let orders = cache.get(symbol).cloned().ok_or_else(|| {
                DexError::Other("filled orders unavailable: waiting for websocket data".to_string())
            })?;
            Ok(FilledOrdersResponse { orders })
        } else {
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
        let orders = if self.websocket_url.is_some() {
            let cache = self.open_orders_cache.read().await;
            cache.get(symbol).cloned().unwrap_or_else(|| Vec::new())
        } else {
            let path = build_query(
                "/user/orders",
                vec![("market".to_string(), symbol.to_string())],
            );
            let open_orders: Vec<OpenOrderModel> = self.api.get(path, true).await?;
            open_orders
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
                .collect::<Vec<_>>()
        };
        Ok(OpenOrdersResponse { orders })
    }

    async fn get_balance(&self, symbol: Option<&str>) -> Result<BalanceResponse, DexError> {
        let balance = if self.websocket_url.is_some() {
            let cache = self.balance_cache.read().await;
            cache.as_ref().map(copy_balance).ok_or_else(|| {
                DexError::Other("balance unavailable: waiting for websocket data".to_string())
            })?
        } else {
            let balance: BalanceModel = self.api.get("/user/balance".to_string(), true).await?;
            if let Some(symbol) = symbol {
                if symbol != balance.collateral_name {
                    return Err(DexError::Other(format!(
                        "Unsupported balance symbol {} (collateral: {})",
                        symbol, balance.collateral_name
                    )));
                }
            }
            BalanceResponse {
                equity: balance.equity,
                balance: balance.balance,
                position_entry_price: None,
                position_sign: None,
            }
        };
        if symbol.is_some() && self.websocket_url.is_some() {
            // Collateral symbol isn't supplied in account stream payload.
        }
        Ok(balance)
    }

    async fn get_combined_balance(&self) -> Result<CombinedBalanceResponse, DexError> {
        let balance = if self.websocket_url.is_some() {
            let cache = self.balance_cache.read().await;
            cache.as_ref().map(copy_balance).ok_or_else(|| {
                DexError::Other("balance unavailable: waiting for websocket data".to_string())
            })?
        } else {
            let balance: BalanceModel = self.api.get("/user/balance".to_string(), true).await?;
            BalanceResponse {
                equity: balance.equity,
                balance: balance.balance,
                position_entry_price: None,
                position_sign: None,
            }
        };
        let mut token_balances = HashMap::new();
        token_balances.insert(
            "USD".to_string(),
            BalanceResponse {
                equity: balance.equity,
                balance: balance.balance,
                position_entry_price: balance.position_entry_price,
                position_sign: balance.position_sign,
            },
        );
        Ok(CombinedBalanceResponse {
            usd_balance: balance.equity,
            token_balances,
        })
    }

    async fn get_last_trades(&self, symbol: &str) -> Result<LastTradesResponse, DexError> {
        if self.websocket_url.is_some() {
            let cache = self.last_trades.read().await;
            let trades = cache.get(symbol).cloned().ok_or_else(|| {
                DexError::Other("last trades unavailable: waiting for websocket data".to_string())
            })?;
            Ok(LastTradesResponse { trades })
        } else {
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
    }

    async fn get_order_book(
        &self,
        symbol: &str,
        depth: usize,
    ) -> Result<OrderBookSnapshot, DexError> {
        if self.websocket_url.is_some() {
            let cache = self.order_book_cache.read().await;
            if let Some(snapshot) = cache.get(symbol) {
                let bids = snapshot.bids.iter().take(depth).cloned().collect();
                let asks = snapshot.asks.iter().take(depth).cloned().collect();
                Ok(OrderBookSnapshot { bids, asks })
            } else {
                Err(DexError::Other(
                    "order book unavailable: waiting for websocket data".to_string(),
                ))
            }
        } else {
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
    websocket_url: Option<String>,
    tracked_symbols: Vec<String>,
) -> Result<Box<dyn DexConnector>, DexError> {
    let connector = ExtendedConnector::new(
        api_key,
        public_key,
        private_key,
        vault,
        base_url,
        websocket_url,
        tracked_symbols,
    )
    .await?;
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

use crate::{
    dex_connector::{slippage_price, string_to_decimal, DexConnector},
    dex_request::{DexError, DexRequest, HttpMethod},
    dex_websocket::DexWebSocket,
    BalanceResponse, CanceledOrder, CanceledOrdersResponse, CreateOrderResponse, FilledOrder,
    FilledOrdersResponse, OrderSide, TickerResponse,
};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use debot_utils::parse_to_decimal;
use ethers::{signers::LocalWallet, types::H160};
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use hyperliquid_rust_sdk_fork::{
    BaseUrl, ClientCancelRequest, ClientLimit, ClientOrder, ClientOrderRequest, ExchangeClient,
    ExchangeDataStatus, ExchangeResponseStatus,
};
use reqwest::Client;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    net::TcpStream,
    select,
    signal::unix::{signal, SignalKind},
    sync::{Mutex, RwLock},
    task::JoinHandle,
    time::sleep,
};
use tokio_tungstenite::{tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream};

struct Config {
    evm_wallet_address: String,
    symbol_list: Vec<String>,
}

// --- Spot metadata support ---
#[derive(Deserialize, Debug)]
struct SpotMetaToken {
    #[serde(rename = "name")]
    _name: String,
    #[serde(rename = "szDecimals")]
    _sz_decimals: u32,
    #[serde(rename = "weiDecimals")]
    _wei_decimals: u32,
    #[serde(rename = "index")]
    _index: usize,
}

#[derive(Deserialize, Debug, Clone)]
struct SpotMetaUniverse {
    #[serde(rename = "name")]
    name: String,
    #[serde(rename = "tokens")]
    _tokens: Vec<usize>,
    #[serde(rename = "index")]
    index: usize,
}

#[derive(Deserialize, Debug)]
struct SpotMetaResponse {
    #[serde(rename = "tokens")]
    _tokens: Vec<SpotMetaToken>,
    #[serde(rename = "universe")]
    universe: Vec<SpotMetaUniverse>,
}

#[derive(Serialize, Debug)]
struct InfoRequest<'a> {
    #[serde(rename = "type")]
    req_type: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    user: Option<&'a str>,
}

#[derive(Debug)]
struct TradeResult {
    pub filled_side: OrderSide,
    pub filled_size: Decimal,
    pub filled_value: Decimal,
    pub filled_fee: Decimal,
    order_id: String,
}

#[derive(Debug, Clone)]
pub struct CancelEvent {
    pub order_id: String,
    pub timestamp: u64,
}

#[derive(Default)]
struct DynamicMarketInfo {
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub market_price: Option<Decimal>,
    pub min_tick: Option<Decimal>,
    pub volume: Option<Decimal>,
    pub num_trades: Option<u64>,
    pub open_interest: Option<Decimal>,
    pub funding_rate: Option<Decimal>,
    pub oracle_price: Option<Decimal>,
}

struct StaticMarketInfo {
    pub decimals: u32,
    pub _max_leverage: u32,
}

#[derive(Clone)]
#[allow(dead_code)]
struct MaintenanceInfo {
    next_start: Option<DateTime<Utc>>,
    fetched_at: DateTime<Utc>,
}

#[derive(Deserialize, Debug)]
pub struct OrderUpdateDetail {
    pub coin: String,
    #[serde(rename = "oid")]
    pub oid: u64,
}

#[derive(Deserialize, Debug)]
pub struct OrderUpdate {
    pub order: OrderUpdateDetail,
    pub status: String,
    #[serde(rename = "statusTimestamp")]
    pub status_timestamp: u64,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct WsLevel {
    px: String,
    sz: String,
    n: u64,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct WsBbo {
    coin: String,
    time: u64,
    bbo: [Option<WsLevel>; 2], // [bestBid?, bestAsk?]
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct WsBook {
    coin: String,
    time: u64,
    levels: [Vec<WsLevel>; 2], // [bids, asks]
}

pub struct HyperliquidConnector {
    config: Config,
    request: DexRequest,
    web_socket: DexWebSocket,
    running: Arc<AtomicBool>,
    read_socket: Arc<Mutex<Option<SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>>>,
    write_socket:
        Arc<Mutex<Option<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>>,
    task_handle_read_message: Arc<Mutex<Option<JoinHandle<()>>>>,
    task_handle_read_sigterm: Arc<Mutex<Option<JoinHandle<()>>>>,
    // 1st key = symbol, 2nd key = order_id
    trade_results: Arc<RwLock<HashMap<String, HashMap<String, TradeResult>>>>,
    canceled_results: Arc<RwLock<HashMap<String, HashMap<String, CancelEvent>>>>,
    dynamic_market_info: Arc<RwLock<HashMap<String, DynamicMarketInfo>>>,
    static_market_info: HashMap<String, StaticMarketInfo>,
    spot_index_map: HashMap<String, usize>,
    spot_reverse_map: Arc<HashMap<usize, String>>,
    exchange_client: ExchangeClient,
    maintenance: Arc<RwLock<MaintenanceInfo>>,
}

#[derive(Debug)]
struct WebSocketMessage {
    _channel: String,
    data: WebSocketData,
}

#[derive(Debug)]
enum WebSocketData {
    AllMidsData(AllMidsData),
    UserFillsData(UserFillsData),
    CandleData(CandleData),
    ActiveAssetCtxData(ActiveAssetCtxData),
    OrderUpdatesData(Vec<OrderUpdate>),
    Bbo(WsBbo),
    L2Book(WsBook),
}

#[derive(Deserialize, Debug)]
struct AllMidsData {
    mids: HashMap<String, String>,
}

#[allow(dead_code, non_snake_case)]
#[derive(Deserialize, Debug)]
struct CandleData {
    t: u64,     // Open time (milliseconds)
    T: u64,     // Close time (milliseconds)
    s: String,  // Symbol
    i: String,  // Interval
    o: Decimal, // Open price
    c: Decimal, // Close price
    h: Decimal, // High price
    l: Decimal, // Low price
    v: Decimal, // Volume
    n: u64,     // Number of trades
}

#[derive(Deserialize, Debug)]
pub struct ActiveAssetCtxData {
    pub coin: String,       // The asset symbol (e.g., BTC-USD)
    pub ctx: PerpsAssetCtx, // The asset context containing market details
}

#[allow(dead_code, non_snake_case)]
#[derive(Deserialize, Debug)]
pub struct PerpsAssetCtx {
    pub dayNtlVlm: Decimal,     // Daily notional volume
    pub prevDayPx: Decimal,     // Previous day's price
    pub markPx: Decimal,        // Mark price
    pub midPx: Option<Decimal>, // Mid price (optional)
    pub funding: Decimal,       // Funding rate
    pub openInterest: Decimal,  // Open interest
    pub oraclePx: Decimal,      // Oracle price
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UserFillsData {
    pub user: String,
    pub fills: Vec<Fill>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Fill {
    pub coin: String,
    pub px: Decimal,
    pub sz: Decimal,
    pub side: String,
    pub dir: String,
    #[serde(rename = "closedPnl")]
    pub closed_pnl: Decimal,
    pub oid: u64,
    pub tid: u64,
    pub fee: Decimal,
}

impl<'de> Deserialize<'de> for WebSocketMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            channel: String,
            data: serde_json::Value,
        }
        let helper = Helper::deserialize(deserializer)?;
        let data = match helper.channel.as_str() {
            "allMids" => AllMidsData::deserialize(helper.data)
                .map(WebSocketData::AllMidsData)
                .map_err(serde::de::Error::custom)?,
            "userFills" => UserFillsData::deserialize(helper.data)
                .map(WebSocketData::UserFillsData)
                .map_err(serde::de::Error::custom)?,
            "orderUpdates" => Vec::<OrderUpdate>::deserialize(helper.data)
                .map(WebSocketData::OrderUpdatesData)
                .map_err(serde::de::Error::custom)?,
            "candle" => CandleData::deserialize(helper.data)
                .map(WebSocketData::CandleData)
                .map_err(serde::de::Error::custom)?,
            "activeAssetCtx" => ActiveAssetCtxData::deserialize(helper.data)
                .map(WebSocketData::ActiveAssetCtxData)
                .map_err(serde::de::Error::custom)?,
            "bbo" => WsBbo::deserialize(helper.data)
                .map(WebSocketData::Bbo)
                .map_err(serde::de::Error::custom)?,
            "l2Book" => WsBook::deserialize(helper.data)
                .map(WebSocketData::L2Book)
                .map_err(serde::de::Error::custom)?,
            _ => return Err(serde::de::Error::custom("unknown channel type")),
        };
        Ok(WebSocketMessage {
            _channel: helper.channel,
            data,
        })
    }
}

impl HyperliquidConnector {
    pub async fn new(
        rest_endpoint: &str,
        web_socket_endpoint: &str,
        private_key: &str,
        evm_wallet_address: &str,
        vault_address: Option<String>,
        use_agent: bool,
        agent_name: Option<String>,
        symbol_list: &[&str],
    ) -> Result<Self, DexError> {
        let request = DexRequest::new(rest_endpoint.to_owned()).await?;
        let web_socket = DexWebSocket::new(web_socket_endpoint.to_owned());

        let evm_wallet_address = vault_address
            .clone()
            .unwrap_or_else(|| evm_wallet_address.into());
        let config = Config {
            evm_wallet_address,
            symbol_list: symbol_list.iter().map(|s| s.to_string()).collect(),
        };

        let vault_address: Option<H160> = vault_address
            .as_deref()
            .and_then(|v| H160::from_str(v).ok());

        let mut local_wallet: LocalWallet = private_key.parse().unwrap();

        if use_agent {
            let ec_tmp =
                ExchangeClient::new(None, local_wallet, Some(BaseUrl::Mainnet), None, None)
                    .await
                    .map_err(|e| DexError::Other(e.to_string()))?;

            let (pk, resp) = ec_tmp
                .approve_agent(None, agent_name)
                .await
                .map_err(|e| DexError::Other(e.to_string()))?;
            log::info!("Agent approved: {resp:?}");

            local_wallet = pk.parse().unwrap();
        }

        let exchange_client = ExchangeClient::new(
            None,
            local_wallet,
            Some(BaseUrl::Mainnet),
            None,
            vault_address,
        )
        .await
        .map_err(|e| DexError::Other(e.to_string()))?;

        let mut instance = HyperliquidConnector {
            config,
            request,
            web_socket,
            trade_results: Arc::new(RwLock::new(HashMap::new())),
            canceled_results: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(AtomicBool::new(false)),
            read_socket: Arc::new(Mutex::new(None)),
            write_socket: Arc::new(Mutex::new(None)),
            task_handle_read_message: Arc::new(Mutex::new(None)),
            task_handle_read_sigterm: Arc::new(Mutex::new(None)),
            dynamic_market_info: Arc::new(RwLock::new(HashMap::new())),
            static_market_info: HashMap::new(),
            spot_index_map: HashMap::new(),
            spot_reverse_map: Arc::new(HashMap::new()),
            exchange_client,
            maintenance: Arc::new(RwLock::new(MaintenanceInfo {
                next_start: None,
                fetched_at: Utc::now() - ChronoDuration::hours(1),
            })),
        };

        instance.spawn_maintenance_watcher();

        instance.retrive_market_metadata().await?;

        let info_payload = serde_json::to_string(&InfoRequest {
            req_type: "spotMeta",
            user: None,
        })
        .map_err(|e| DexError::Other(e.to_string()))?;

        let spot_meta: SpotMetaResponse = instance
            .request
            .handle_request::<SpotMetaResponse, InfoRequest<'_>>(
                HttpMethod::Post,
                "/info".into(),
                &HashMap::new(),
                info_payload,
            )
            .await?;

        // index → token_name
        let token_name_map: HashMap<usize, String> = spot_meta
            ._tokens
            .iter()
            .map(|t| (t._index, t._name.clone()))
            .collect();

        let mut idx_from_pair = HashMap::<String, usize>::new();
        let mut pair_from_idx = HashMap::<usize, String>::new();

        for uni in &spot_meta.universe {
            let pair = if !uni.name.starts_with('@') {
                uni.name.clone()
            } else if uni._tokens.len() == 2 {
                format!(
                    "{}/{}",
                    token_name_map.get(&uni._tokens[0]).unwrap_or(&"?".into()),
                    token_name_map.get(&uni._tokens[1]).unwrap_or(&"?".into())
                )
            } else {
                log::warn!(
                    "universe idx {} has unexpected token vec {:?}",
                    uni.index,
                    uni._tokens
                );
                uni.name.clone()
            };

            idx_from_pair.insert(pair.clone(), uni.index);
            pair_from_idx.insert(uni.index, pair);
        }

        instance.spot_index_map = idx_from_pair;
        instance.spot_reverse_map = Arc::new(pair_from_idx);

        {
            let token_decimals: HashMap<String, u32> = spot_meta
                ._tokens
                .iter()
                .map(|t| (t._name.clone(), t._sz_decimals))
                .collect();

            let mut sm = std::mem::take(&mut instance.static_market_info);

            for uni in &spot_meta.universe {
                let pair = if !uni.name.starts_with('@') {
                    uni.name.clone()
                } else if uni._tokens.len() == 2 {
                    format!(
                        "{}/{}",
                        spot_meta._tokens[uni._tokens[0]]._name,
                        spot_meta._tokens[uni._tokens[1]]._name
                    )
                } else {
                    uni.name.clone()
                };

                let base = pair.split('/').next().unwrap();
                let decimals = *token_decimals.get(base).unwrap_or(&0);

                sm.insert(
                    pair.clone(),
                    StaticMarketInfo {
                        decimals,
                        _max_leverage: 0,
                    },
                );
            }

            instance.static_market_info = sm;
        }

        Ok(instance)
    }

    fn spawn_maintenance_watcher(&self) {
        let cache = self.maintenance.clone();
        tokio::spawn(async move {
            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .expect("reqwest client");

            loop {
                if let Ok(res) = client
                    .get("https://hyperliquid.statuspage.io/api/v2/scheduled-maintenances/upcoming.json")
                    .send()
                    .await
                {
                    if let Ok(json) = res.json::<Value>().await {
                        let next = json
                            .get("scheduled_maintenances")
                            .and_then(|v| v.get(0))
                            .and_then(|v| v.get("scheduled_for"))
                            .and_then(|v| v.as_str())
                            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                            .map(|dt| dt.with_timezone(&Utc));

                        *cache.write().await = MaintenanceInfo {
                            next_start: next,
                            fetched_at: Utc::now(),
                        };
                    }
                }
                sleep(Duration::from_secs(600)).await;
            }
        });
    }

    pub async fn start_web_socket(&self) -> Result<(), DexError> {
        log::info!("start_web_socket");

        let (write, read) = self
            .web_socket
            .clone()
            .connect()
            .await
            .map_err(|_| DexError::Other("Failed to connect to WebSocket".to_string()))?;

        {
            let mut read_lock = self.read_socket.lock().await;
            *read_lock = Some(read);
        }
        {
            let mut write_lock = self.write_socket.lock().await;
            *write_lock = Some(write);
        }

        self.running.store(true, Ordering::SeqCst);
        self.subscribe_to_channels(&self.config.evm_wallet_address)
            .await?;

        let running = self.running.clone();
        let read_sock = self.read_socket.clone();
        let write_sock = self.write_socket.clone();
        let dmi = self.dynamic_market_info.clone();
        let trs = self.trade_results.clone();
        let rev_map = self.spot_reverse_map.clone();
        let crs = self.canceled_results.clone();

        let reader_handle = tokio::spawn(async move {
            let mut idle_counter = 0;
            while running.load(Ordering::SeqCst) {
                if let Some(stream) = read_sock.lock().await.as_mut() {
                    tokio::select! {
                        msg = stream.next() => match msg {
                            Some(Ok(Message::Text(txt))) => {
                                idle_counter = 0;
                                if txt == "{}" {
                                    if let Some(w) = write_sock.lock().await.as_mut() {
                                        let _ = w.send(Message::Text(txt)).await;
                                    }
                                } else {
                                    if let Err(e) = HyperliquidConnector::handle_websocket_message(
                                        Message::Text(txt),
                                        dmi.clone(),
                                        trs.clone(),
                                        rev_map.clone(),
                                        crs.clone(),
                                    ).await {
                                        log::error!("WebSocket handler error: {:?}", e);
                                        break;
                                    }
                                }
                            }
                            Some(Ok(_)) => {
                            }
                            Some(Err(err)) => {
                                log::error!("WebSocket read error: {:?}", err);
                                break;
                            }
                            None => {
                                log::info!("WebSocket stream closed");
                                break;
                            }
                        },
                        _ = tokio::time::sleep(Duration::from_secs(10)) => {
                            idle_counter += 1;
                            if idle_counter >= 10 {
                                log::error!("No WebSocket messages for 100s, shutting down reader");
                                break;
                            }
                        }
                    }
                }
            }
            running.store(false, Ordering::SeqCst);
            log::info!("WebSocket reader task ended");
        });
        *self.task_handle_read_message.lock().await = Some(reader_handle);

        let running_for_sig = self.running.clone();
        let sig_handle = tokio::spawn(async move {
            let mut sigterm =
                signal(SignalKind::terminate()).expect("Failed to bind SIGTERM handler");
            loop {
                select! {
                    _ = sigterm.recv() => {
                        log::info!("SIGTERM received, stopping WebSocket");
                        running_for_sig.store(false, Ordering::SeqCst);
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        if !running_for_sig.load(Ordering::SeqCst) {
                            break;
                        }
                    }
                }
            }
        });
        *self.task_handle_read_sigterm.lock().await = Some(sig_handle);

        Ok(())
    }

    pub async fn stop_web_socket(&self) -> Result<(), DexError> {
        log::info!("stop_web_socket");
        self.running.store(false, Ordering::SeqCst);

        {
            let mut write_guard = self.write_socket.lock().await;
            if let Some(write_socket) = write_guard.as_mut() {
                if let Err(e) = write_socket.send(Message::Close(None)).await {
                    log::error!("Failed to send WebSocket close message: {:?}", e);
                }
            }
            *write_guard = None;
        }

        {
            let mut read_guard = self.read_socket.lock().await;
            *read_guard = None;
        }

        if let Some(handle) = self.task_handle_read_message.lock().await.take() {
            let _ = handle.await;
        }

        if let Some(handle) = self.task_handle_read_sigterm.lock().await.take() {
            let _ = handle.await;
        }

        drop(self.web_socket.clone());

        Ok(())
    }

    async fn subscribe_to_channels(&self, user_address: &str) -> Result<(), DexError> {
        let all_mids_subscription = serde_json::json!({
            "method": "subscribe",
            "subscription": {
                "type": "allMids"
            }
        })
        .to_string();

        let user_fills_subscription = serde_json::json!({
            "method": "subscribe",
            "subscription": {
                "type": "userFills",
                "user": user_address
            }
        })
        .to_string();

        let order_updates_subscription = serde_json::json!({
            "method": "subscribe",
            "subscription": {
                "type": "orderUpdates",
                "user": user_address
            }
        })
        .to_string();

        let mut write_socket_lock = self.write_socket.lock().await;

        if let Some(write_socket) = write_socket_lock.as_mut() {
            if let Err(e) = write_socket
                .send(Message::Text(all_mids_subscription))
                .await
            {
                return Err(DexError::WebSocketError(format!(
                    "Failed to subscribe to allMids: {}",
                    e
                )));
            }

            if let Err(e) = write_socket
                .send(Message::Text(order_updates_subscription))
                .await
            {
                return Err(DexError::WebSocketError(format!(
                    "Failed to subscribe to userFills: {}",
                    e
                )));
            }

            if let Err(e) = write_socket
                .send(Message::Text(user_fills_subscription))
                .await
            {
                return Err(DexError::WebSocketError(format!(
                    "Failed to subscribe to userFills: {}",
                    e
                )));
            }

            for symbol in &self.config.symbol_list {
                let coin = resolve_coin(symbol, &self.spot_index_map);
                let candle_subscription = serde_json::json!({
                    "method": "subscribe",
                    "subscription": {
                        "type": "candle",
                        "coin": coin,
                        "interval": "1m"
                    }
                })
                .to_string();
                if let Err(e) = write_socket.send(Message::Text(candle_subscription)).await {
                    return Err(DexError::WebSocketError(format!(
                        "Failed to subscribe to candle for {}: {}",
                        symbol, e
                    )));
                }

                let active_asset_ctx_subscription = serde_json::json!({
                    "method": "subscribe",
                    "subscription": {
                        "type": "activeAssetCtx",
                        "coin": coin,
                    }
                })
                .to_string();
                if let Err(e) = write_socket
                    .send(Message::Text(active_asset_ctx_subscription))
                    .await
                {
                    return Err(DexError::WebSocketError(format!(
                        "Failed to subscribe to activeAssetCtx: {}",
                        e
                    )));
                }

                let bbo_subscription = serde_json::json!({
                    "method": "subscribe",
                    "subscription": {
                        "type": "bbo",
                        "coin": coin
                    }
                })
                .to_string();
                if let Err(e) = write_socket.send(Message::Text(bbo_subscription)).await {
                    return Err(DexError::WebSocketError(format!(
                        "Failed to subscribe to bbo: {}",
                        e
                    )));
                }

                let l2_subscription = serde_json::json!({
                    "method": "subscribe",
                    "subscription": {
                        "type": "l2Book",
                        "coin": coin
                    }
                })
                .to_string();
                if let Err(e) = write_socket.send(Message::Text(l2_subscription)).await {
                    return Err(DexError::WebSocketError(format!(
                        "Failed to subscribe to l2: {}",
                        e
                    )));
                }
            }
        } else {
            return Err(DexError::WebSocketError(
                "Write socket is not available".to_string(),
            ));
        }

        Ok(())
    }

    async fn handle_websocket_message(
        msg: Message,
        dynamic_market_info: Arc<RwLock<HashMap<String, DynamicMarketInfo>>>,
        trade_results: Arc<RwLock<HashMap<String, HashMap<String, TradeResult>>>>,
        spot_reverse_map: Arc<HashMap<usize, String>>,
        canceled_results: Arc<RwLock<HashMap<String, HashMap<String, CancelEvent>>>>,
    ) -> Result<(), DexError> {
        if let Message::Text(text) = msg {
            for line in text.split('\n') {
                if line.is_empty() {
                    continue;
                }
                if let Ok(message) = serde_json::from_str::<WebSocketMessage>(line) {
                    match message.data {
                        WebSocketData::AllMidsData(ref data) => {
                            Self::process_all_mids_message(
                                data,
                                dynamic_market_info.clone(),
                                spot_reverse_map.clone(),
                            )
                            .await;
                        }
                        WebSocketData::CandleData(ref data) => {
                            Self::process_candle_message(
                                data,
                                dynamic_market_info.clone(),
                                spot_reverse_map.clone(),
                            )
                            .await;
                        }
                        WebSocketData::UserFillsData(ref data) => {
                            Self::process_account_data(data, trade_results.clone()).await;
                        }
                        WebSocketData::ActiveAssetCtxData(ref data) => {
                            Self::process_active_asset_ctx_message(
                                data,
                                dynamic_market_info.clone(),
                                spot_reverse_map.clone(),
                            )
                            .await;
                        }
                        WebSocketData::OrderUpdatesData(ref orders) => {
                            Self::process_order_updates_message(orders, canceled_results.clone())
                                .await;
                        }
                        WebSocketData::Bbo(bbo) => {
                            let key = format!("{}-USD", bbo.coin);
                            let mut info_map = dynamic_market_info.write().await;
                            let info = info_map.entry(key).or_default();
                            info.best_bid = bbo
                                .bbo
                                .get(0)
                                .and_then(|lvl| lvl.as_ref())
                                .map(|l| Decimal::from_str(&l.px).unwrap());
                            info.best_ask = bbo
                                .bbo
                                .get(1)
                                .and_then(|lvl| lvl.as_ref())
                                .map(|l| Decimal::from_str(&l.px).unwrap());
                        }
                        WebSocketData::L2Book(book) => {
                            let key = format!("{}-USD", book.coin);
                            let mut info_map = dynamic_market_info.write().await;
                            let info = info_map.entry(key).or_default();
                            info.best_bid = book.levels[0]
                                .get(0)
                                .map(|lvl| Decimal::from_str(&lvl.px).unwrap());
                            info.best_ask = book.levels[1]
                                .get(0)
                                .map(|lvl| Decimal::from_str(&lvl.px).unwrap());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn process_order_updates_message(
        orders: &[OrderUpdate],
        canceled_results: Arc<RwLock<HashMap<String, HashMap<String, CancelEvent>>>>,
    ) {
        for upd in orders.iter().filter(|o| o.status == "canceled") {
            let symbol = if upd.order.coin.contains('/') || upd.order.coin.contains('-') {
                upd.order.coin.clone()
            } else {
                format!("{}-USD", upd.order.coin)
            };
            let evt = CancelEvent {
                order_id: upd.order.oid.to_string(),
                timestamp: upd.status_timestamp,
            };
            canceled_results
                .write()
                .await
                .entry(symbol)
                .or_default()
                .insert(evt.order_id.clone(), evt);
        }
    }

    async fn process_all_mids_message(
        mids_data: &AllMidsData,
        dynamic_market_info: Arc<RwLock<HashMap<String, DynamicMarketInfo>>>,
        spot_reverse_map: Arc<HashMap<usize, String>>,
    ) {
        for (raw_coin, mid_price_str) in &mids_data.mids {
            let coin = if let Some(stripped) = raw_coin.strip_prefix('@') {
                stripped
                    .parse::<usize>()
                    .ok()
                    .and_then(|idx| spot_reverse_map.get(&idx).cloned())
                    .unwrap_or_else(|| {
                        log::debug!(
                            "in spot_reverse_map {} is missing (@{})",
                            raw_coin,
                            stripped
                        );
                        raw_coin.clone()
                    })
            } else {
                raw_coin.clone()
            };

            let market_key = if coin.contains('/') || coin.contains('-') {
                coin.clone() // Spot: UBTC/USDC,  etc.
            } else {
                format!("{}-USD", coin) // Perp: BTC-USD, etc.
            };

            if let Ok(mid) = string_to_decimal(Some(mid_price_str.clone())) {
                let mut guard = dynamic_market_info.write().await;
                let info = guard.entry(market_key.clone()).or_default();
                if info.min_tick.is_none() {
                    info.min_tick = Some(Self::calculate_min_tick(mid));
                }
                info.market_price = Some(mid);
            }
        }
    }

    async fn process_candle_message(
        candle: &CandleData,
        dynamic_market_info: Arc<RwLock<HashMap<String, DynamicMarketInfo>>>,
        spot_reverse_map: Arc<HashMap<usize, String>>,
    ) {
        let coin = if let Some(stripped) = candle.s.strip_prefix('@') {
            stripped
                .parse::<usize>()
                .ok()
                .and_then(|idx| spot_reverse_map.get(&idx).cloned())
                .unwrap_or_else(|| {
                    log::debug!(
                        "in spot_reverse_map: {} is missing (@{})",
                        candle.s,
                        stripped
                    );
                    candle.s.clone()
                })
        } else {
            candle.s.clone()
        };

        let market_key = if coin.contains('/') || coin.contains('-') {
            coin.clone()
        } else {
            format!("{}-USD", coin)
        };

        let mut guard = dynamic_market_info.write().await;
        let info = guard.entry(market_key.clone()).or_default();
        info.volume = Some(candle.v);
        info.num_trades = Some(candle.n);
    }

    async fn process_active_asset_ctx_message(
        asset_data: &ActiveAssetCtxData,
        dynamic_market_info: Arc<RwLock<HashMap<String, DynamicMarketInfo>>>,
        spot_reverse_map: Arc<HashMap<usize, String>>,
    ) {
        let coin = if let Some(stripped) = asset_data.coin.strip_prefix('@') {
            stripped
                .parse::<usize>()
                .ok()
                .and_then(|idx| spot_reverse_map.get(&idx).cloned())
                .unwrap_or_else(|| {
                    log::debug!(
                        "in spot_reverse_map {} is missing (@{})",
                        asset_data.coin,
                        stripped
                    );
                    asset_data.coin.clone()
                })
        } else {
            asset_data.coin.clone()
        };

        let market_key = if coin.contains('/') || coin.contains('-') {
            coin.clone()
        } else {
            format!("{}-USD", coin)
        };

        let mut guard = dynamic_market_info.write().await;
        let info = guard
            .entry(market_key.clone())
            .or_insert_with(DynamicMarketInfo::default);
        info.funding_rate = Some(asset_data.ctx.funding);
        info.open_interest = Some(asset_data.ctx.openInterest);
        info.oracle_price = Some(asset_data.ctx.oraclePx);
    }

    async fn process_account_data(
        data: &UserFillsData,
        trade_results: Arc<RwLock<HashMap<String, HashMap<String, TradeResult>>>>,
    ) {
        for fill in &data.fills {
            log::debug!("{:?}", fill);

            let filled_side = if fill.side == "A" {
                OrderSide::Short
            } else {
                OrderSide::Long
            };

            let filled_size = fill.sz;
            let filled_price = fill.px;
            let filled_value = filled_size * filled_price;
            let filled_fee = fill.fee;
            let order_id = fill.oid;
            let trade_id = fill.tid;

            let market_id = if fill.coin.contains('/') || fill.coin.contains('-') {
                fill.coin.clone()
            } else {
                format!("{}-USD", fill.coin)
            };

            let trade_result = TradeResult {
                filled_side,
                filled_size,
                filled_value,
                filled_fee,
                order_id: order_id.to_string(),
            };

            let mut trade_results_guard = trade_results.write().await;
            trade_results_guard
                .entry(market_id.clone())
                .or_default()
                .insert(trade_id.to_string(), trade_result);
        }
    }
}

#[derive(Serialize, Debug, Clone)]
struct HyperliquidDefaultPayload {
    r#type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    user: Option<String>,
}

#[derive(Deserialize, Debug)]
struct HyperliquidRetrieveUserStateResponse {
    #[serde(rename = "marginSummary")]
    margin_summary: Option<HyperliquidMarginSummary>,
}
#[derive(Deserialize, Debug)]
struct HyperliquidMarginSummary {
    #[serde(rename = "accountValue")]
    account_value: String,
    #[serde(rename = "totalRawUsd")]
    total_rawusd: String,
}

#[derive(Deserialize, Debug)]
struct HyperliquidRetriveUserOpenOrder {
    coin: String,
    oid: u64,
}

#[derive(Deserialize, Debug)]
struct HyperliquidRetriveUserPositionResponse {
    #[serde(rename = "assetPositions")]
    asset_positions: Vec<HyperliquidRetriveUserPositionResponseBody>,
}
#[derive(Deserialize, Debug)]
struct HyperliquidRetriveUserPositionResponseBody {
    position: HyperliquidRetriveUserPosition,
}
#[derive(Deserialize, Debug)]
struct HyperliquidRetriveUserPosition {
    coin: String,
    szi: Decimal,
}

#[derive(Deserialize, Debug)]
struct HyperliquidRetriveMarketMetadataResponse {
    universe: Vec<HyperliquidRetriveMarketMetadata>,
}
#[derive(Deserialize, Debug)]
struct HyperliquidRetriveMarketMetadata {
    name: String,
    #[serde(rename = "szDecimals")]
    decimals: u32,
    #[serde(rename = "maxLeverage")]
    max_leverage: u32,
}

#[derive(Deserialize, Debug)]
struct HyperliquidSpotBalanceResponse {
    balances: Vec<HyperliquidSpotBalance>,
}

#[derive(Deserialize, Debug)]
struct HyperliquidSpotBalance {
    coin: String,
    total: String,
}

#[async_trait]
impl DexConnector for HyperliquidConnector {
    async fn start(&self) -> Result<(), DexError> {
        self.start_web_socket().await?;
        sleep(Duration::from_secs(5)).await;
        Ok(())
    }

    async fn stop(&self) -> Result<(), DexError> {
        self.stop_web_socket().await?;
        Ok(())
    }

    async fn restart(&self, max_retries: i32) -> Result<(), DexError> {
        log::info!("Restarting WebSocket connection...");

        let mut retry_count = 0;
        let mut backoff_delay = Duration::from_secs(1);

        while retry_count < max_retries {
            if let Err(e) = self.stop_web_socket().await {
                log::error!(
                    "Failed to stop WebSocket on attempt {}: {:?}",
                    retry_count + 1,
                    e
                );
            } else {
                log::info!(
                    "Successfully stopped WebSocket on attempt {}.",
                    retry_count + 1
                );
            }

            sleep(backoff_delay).await;

            match self.start_web_socket().await {
                Ok(_) => {
                    log::info!(
                        "Successfully started WebSocket on attempt {}.",
                        retry_count + 1
                    );
                    return Ok(());
                }
                Err(e) => {
                    log::error!(
                        "Failed to start WebSocket on attempt {}: {:?}",
                        retry_count + 1,
                        e
                    );
                    retry_count += 1;
                    backoff_delay *= 2; // Exponential backoff
                }
            }
        }

        log::error!(
            "Failed to restart WebSocket after {} attempts.",
            max_retries
        );
        Err(DexError::Other(format!(
            "Failed to restart WebSocket after {} attempts.",
            max_retries
        )))
    }

    async fn set_leverage(&self, symbol: &str, leverage: u32) -> Result<(), DexError> {
        let asset = Self::extract_asset_name(symbol);
        self.exchange_client
            .update_leverage(leverage, asset, false, None)
            .await
            .map_err(|e| DexError::Other(e.to_string()))?;
        Ok(())
    }

    async fn get_ticker(
        &self,
        symbol: &str,
        _test_price: Option<Decimal>,
    ) -> Result<TickerResponse, DexError> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(DexError::NoConnection);
        }

        let dynamic_info_guard = self.dynamic_market_info.read().await;
        let dynamic_info = dynamic_info_guard
            .get(symbol)
            .ok_or_else(|| DexError::Other("No dynamic market info available".to_string()))?;
        let price = dynamic_info
            .market_price
            .ok_or_else(|| DexError::Other("No price available".to_string()))?;
        let min_tick = dynamic_info.min_tick;
        let volume = dynamic_info.volume;
        let num_trades = dynamic_info.num_trades;
        let funding_rate = dynamic_info.funding_rate;
        let open_interest = dynamic_info.open_interest;
        let oracle_price = dynamic_info.oracle_price;

        Ok(TickerResponse {
            symbol: symbol.to_owned(),
            price,
            min_tick,
            min_order: None,
            volume,
            num_trades,
            funding_rate,
            open_interest,
            oracle_price,
        })
    }

    async fn get_filled_orders(&self, symbol: &str) -> Result<FilledOrdersResponse, DexError> {
        let mut response: Vec<FilledOrder> = vec![];
        let trade_results_guard = self.trade_results.read().await;
        let orders = match trade_results_guard.get(symbol) {
            Some(v) => v,
            None => return Ok(FilledOrdersResponse::default()),
        };
        for (trade_id, order) in orders.iter() {
            let filled_order = FilledOrder {
                order_id: order.order_id.clone(),
                trade_id: trade_id.clone(),
                is_rejected: false,
                filled_side: Some(order.filled_side.clone()),
                filled_size: Some(order.filled_size),
                filled_fee: Some(order.filled_fee),
                filled_value: Some(order.filled_value),
            };
            response.push(filled_order);
        }

        Ok(FilledOrdersResponse { orders: response })
    }

    async fn get_canceled_orders(&self, symbol: &str) -> Result<CanceledOrdersResponse, DexError> {
        let mut resp = Vec::new();
        let guard = self.canceled_results.read().await;
        if let Some(map) = guard.get(symbol) {
            for (_, evt) in map.iter() {
                resp.push(CanceledOrder {
                    order_id: evt.order_id.clone(),
                    canceled_timestamp: evt.timestamp,
                });
            }
        }
        Ok(CanceledOrdersResponse { orders: resp })
    }

    async fn get_balance(&self, symbol: Option<&str>) -> Result<BalanceResponse, DexError> {
        if let Some(pair) = symbol {
            // "UBTC/USDC" → "UBTC"
            let base_coin = pair.split('/').next().unwrap_or(pair);

            let spot_action = HyperliquidDefaultPayload {
                r#type: "spotClearinghouseState".into(),
                user: Some(self.config.evm_wallet_address.clone()),
            };
            let spot_res: HyperliquidSpotBalanceResponse = self
                .handle_request_with_action("/info".into(), &spot_action)
                .await?;

            let mut usdc_total = Decimal::ZERO;
            let mut base_total = Decimal::ZERO;
            for b in &spot_res.balances {
                match b.coin.as_str() {
                    "USDC" => usdc_total = parse_to_decimal(&b.total)?,
                    c if c == base_coin => base_total = parse_to_decimal(&b.total)?,
                    _ => {}
                }
            }

            let price_key = pair.to_string();
            let px = self
                .get_market_price(&price_key)
                .await
                .unwrap_or(Decimal::ZERO);

            let equity = base_total * px + usdc_total;
            let balance = usdc_total;

            return Ok(BalanceResponse { equity, balance });
        }

        let request_url = "/info";
        let action = HyperliquidDefaultPayload {
            r#type: "clearinghouseState".into(),
            user: Some(self.config.evm_wallet_address.clone()),
        };
        let res = self
            .handle_request_with_action::<HyperliquidRetrieveUserStateResponse, _>(
                request_url.into(),
                &action,
            )
            .await?;

        if let Some(summary) = res.margin_summary {
            let equity = parse_to_decimal(&summary.account_value)?;
            let balance = parse_to_decimal(&summary.total_rawusd)?;
            Ok(BalanceResponse { equity, balance })
        } else {
            Err(DexError::Other("Unknown error".into()))
        }
    }

    async fn clear_filled_order(&self, symbol: &str, order_id: &str) -> Result<(), DexError> {
        let mut trade_results_guard = self.trade_results.write().await;

        if let Some(orders) = trade_results_guard.get_mut(symbol) {
            if orders.contains_key(order_id) {
                orders.remove(order_id);
            } else {
                return Err(DexError::Other(format!(
                    "filled order(order_id:{}({})) does not exist",
                    order_id, symbol
                )));
            }
        } else {
            return Err(DexError::Other(format!(
                "filled order(symbol:{}({})) does not exist",
                symbol, order_id
            )));
        }

        Ok(())
    }

    async fn clear_all_filled_orders(&self) -> Result<(), DexError> {
        let mut trade_results_guard = self.trade_results.write().await;
        trade_results_guard.clear();
        Ok(())
    }

    async fn clear_canceled_order(&self, symbol: &str, order_id: &str) -> Result<(), DexError> {
        let mut guard = self.canceled_results.write().await;
        if let Some(map) = guard.get_mut(symbol) {
            if map.remove(order_id).is_some() {
                return Ok(());
            }
        }
        Err(DexError::Other(format!(
            "canceled order {} for {} not found",
            order_id, symbol
        )))
    }

    async fn clear_all_canceled_orders(&self) -> Result<(), DexError> {
        self.canceled_results.write().await.clear();
        Ok(())
    }

    async fn create_order(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        price: Option<Decimal>,
        spread: Option<i64>,
    ) -> Result<CreateOrderResponse, DexError> {
        let (price, time_in_force) = match price {
            Some(v) => {
                if v.is_zero() {
                    let map = self.dynamic_market_info.read().await;
                    let info = map
                        .get(symbol)
                        .ok_or_else(|| DexError::Other(format!("No market info for {}", symbol)))?;
                    let bid = info
                        .best_bid
                        .ok_or_else(|| DexError::Other("No best_bid".into()))?;
                    let ask = info
                        .best_ask
                        .ok_or_else(|| DexError::Other("No best_ask".into()))?;
                    let tick = info
                        .min_tick
                        .ok_or_else(|| DexError::Other("No min_tick".into()))?;
                    let spread = Decimal::from(spread.unwrap_or(1));
                    log::debug!(
                        "bid = {}, ask = {}, tick = {}, spread = {}",
                        bid,
                        ask,
                        tick,
                        spread
                    );
                    let calc = if side == OrderSide::Long {
                        bid - tick * spread
                    } else {
                        ask + tick * spread
                    };
                    (calc, "Alo")
                } else {
                    (v, "Alo")
                }
            }
            None => {
                let price = self.get_worst_price(symbol, &side).await?;
                (price, "Ioc")
            }
        };

        let dynamic_market_info_guard = self.dynamic_market_info.read().await;
        let market_info = dynamic_market_info_guard
            .get(symbol)
            .ok_or_else(|| DexError::Other("Market info not found".to_string()))?;
        let min_tick = market_info
            .min_tick
            .ok_or_else(|| DexError::Other("Min tick not set for market".to_string()))?;

        let rounded_price = Self::round_price(price, min_tick, side.clone(), spread);
        let rounded_size = self.floor_size(size, symbol);

        log::debug!("{}, {}({}), {}", symbol, rounded_price, price, rounded_size,);

        let asset = resolve_coin(symbol, &self.spot_index_map);

        let order = ClientOrderRequest {
            asset,
            is_buy: side == OrderSide::Long,
            reduce_only: false,
            limit_px: rounded_price
                .to_f64()
                .ok_or_else(|| DexError::Other("Conversion to f64 failed".to_string()))?,
            sz: rounded_size
                .to_f64()
                .ok_or_else(|| DexError::Other("Conversion to f64 failed".to_string()))?,
            cloid: None,
            order_type: ClientOrder::Limit(ClientLimit {
                tif: time_in_force.to_string(),
            }),
        };

        let res = self
            .exchange_client
            .order(order, None)
            .await
            .map_err(|e| DexError::Other(e.to_string()))?;

        let res = match res {
            ExchangeResponseStatus::Ok(exchange_response) => exchange_response,
            ExchangeResponseStatus::Err(e) => return Err(DexError::ServerResponse(e.to_string())),
        };
        let status = res.data.unwrap().statuses[0].clone();
        let order_id = match status {
            ExchangeDataStatus::Filled(order) => order.oid,
            ExchangeDataStatus::Resting(order) => order.oid,
            _ => {
                return Err(DexError::ServerResponse(
                    "Unknown ExchangeDataStaus".to_owned(),
                ))
            }
        };

        Ok(CreateOrderResponse {
            order_id: order_id.to_string(),
            ordered_price: rounded_price,
            ordered_size: rounded_size,
        })
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<(), DexError> {
        let asset = resolve_coin(symbol, &self.spot_index_map);
        let cancel = ClientCancelRequest {
            asset,
            oid: u64::from_str(order_id).unwrap_or_default(),
        };

        self.exchange_client
            .cancel(cancel, None)
            .await
            .map_err(|e| DexError::Other(e.to_string()))?;

        Ok(())
    }

    async fn cancel_all_orders(&self, symbol: Option<String>) -> Result<(), DexError> {
        let open_orders = self.get_orders().await?;
        let order_ids: Vec<String> = open_orders
            .iter()
            .filter_map(|order| {
                let idx_opt = order.coin.strip_prefix('@').and_then(|s| s.parse::<usize>().ok());
                let external_sym = idx_opt
                    .and_then(|idx| self.spot_reverse_map.get(&idx).cloned())
                    .unwrap_or_else(|| format!("{}-USD", order.coin));

                    log::debug!(
                        "cancel_all_orders: raw coin = {}, idx = {:?}, external_sym = {:?}, target = {:?}",
                        order.coin, idx_opt, external_sym, symbol
                    );

                if symbol.as_deref().map_or(true, |s| s == &external_sym) {
                    Some(order.oid.to_string())
                } else {
                    None
                }
            })
            .collect();
        self.cancel_orders(symbol, order_ids).await
    }

    async fn cancel_orders(
        &self,
        symbol: Option<String>,
        order_ids: Vec<String>,
    ) -> Result<(), DexError> {
        let open_orders = self.get_orders().await?;
        let mut cancels = Vec::new();

        for order in open_orders {
            let idx_opt = order
                .coin
                .strip_prefix('@')
                .and_then(|s| s.parse::<usize>().ok());
            let external_sym = idx_opt
                .and_then(|idx| self.spot_reverse_map.get(&idx).cloned())
                .unwrap_or_else(|| format!("{}-USD", order.coin));

            log::debug!(
                    "cancel_orders: raw coin = {}, idx = {:?}, external_sym = {:?}, requested_ids = {:?}",
                    order.coin, idx_opt, external_sym, order_ids
                );

            if symbol.as_deref().map_or(true, |s| s == &external_sym)
                && order_ids.contains(&order.oid.to_string())
            {
                let asset = resolve_coin(&external_sym, &self.spot_index_map);
                cancels.push(ClientCancelRequest {
                    asset,
                    oid: order.oid,
                });
            }
        }

        if !cancels.is_empty() {
            self.exchange_client
                .bulk_cancel(cancels, None)
                .await
                .map_err(|e| DexError::Other(e.to_string()))?;
        }
        Ok(())
    }

    async fn close_all_positions(&self, symbol: Option<String>) -> Result<(), DexError> {
        let open_positions = self.get_positions().await?;
        for p in open_positions {
            let position = p.position;
            let idx_opt = position
                .coin
                .strip_prefix('@')
                .and_then(|s| s.parse::<usize>().ok());
            let external_sym = idx_opt
                .and_then(|idx| self.spot_reverse_map.get(&idx).cloned())
                .unwrap_or_else(|| format!("{}-USD", position.coin));
            if symbol.as_deref().map_or(true, |s| s == &external_sym) {
                let reversed_side = if position.szi.is_sign_negative() {
                    OrderSide::Long
                } else {
                    OrderSide::Short
                };
                let size = position.szi.abs();
                let _ = self
                    .create_order(&external_sym, size, reversed_side, None, None)
                    .await;
            }
        }
        Ok(())
    }

    async fn clear_last_trades(&self, _symbol: &str) -> Result<(), DexError> {
        Ok(())
    }

    async fn check_upcoming_maintenance(&self) -> Result<(), DexError> {
        let info = self.maintenance.read().await;
        if let Some(start) = info.next_start {
            if start - Utc::now() <= ChronoDuration::hours(2) {
                return Err(DexError::UpcomingMaintenance);
            }
        }
        Ok(())
    }
}

impl HyperliquidConnector {
    async fn handle_request_with_action<T, U>(
        &self,
        request_url: String,
        action: &U,
    ) -> Result<T, DexError>
    where
        T: for<'de> Deserialize<'de>,
        U: Serialize + std::fmt::Debug + Clone,
    {
        let json_payload =
            serde_json::to_value(action).map_err(|e| DexError::Other(e.to_string()))?;

        log::debug!("json_payload = {:?}", json_payload);

        self.request
            .handle_request::<T, U>(
                HttpMethod::Post,
                request_url,
                &HashMap::new(),
                json_payload.to_string(),
            )
            .await
            .map_err(|e| DexError::Other(e.to_string()))
    }

    async fn get_positions(
        &self,
    ) -> Result<Vec<HyperliquidRetriveUserPositionResponseBody>, DexError> {
        let request_url = "/info";
        let action = HyperliquidDefaultPayload {
            r#type: "clearinghouseState".to_owned(),
            user: Some(self.config.evm_wallet_address.clone()),
        };
        let res: HyperliquidRetriveUserPositionResponse = self
            .handle_request_with_action::<HyperliquidRetriveUserPositionResponse, HyperliquidDefaultPayload>(
                request_url.to_string(),
                &action,
            )
            .await?;

        Ok(res.asset_positions)
    }

    async fn get_orders(&self) -> Result<Vec<HyperliquidRetriveUserOpenOrder>, DexError> {
        let request_url = "/info";
        let action = HyperliquidDefaultPayload {
            r#type: "openOrders".to_owned(),
            user: Some(self.config.evm_wallet_address.clone()),
        };
        let res: Vec<HyperliquidRetriveUserOpenOrder> = self
            .handle_request_with_action::<Vec<HyperliquidRetriveUserOpenOrder>, HyperliquidDefaultPayload>(
                request_url.to_string(),
                &action,
            )
            .await?;

        Ok(res)
    }

    async fn retrive_market_metadata(&mut self) -> Result<(), DexError> {
        let request_url = "/info";
        let action = HyperliquidDefaultPayload {
            r#type: "meta".to_owned(),
            user: None,
        };
        let res = self
            .handle_request_with_action::<HyperliquidRetriveMarketMetadataResponse, HyperliquidDefaultPayload>(
                request_url.to_string(),
                &action,
            )
            .await?;

        let mut static_market_info_update = HashMap::new();
        for metadata in res.universe.into_iter() {
            let market_id = format!("{}-USD", metadata.name);
            static_market_info_update.insert(
                market_id,
                StaticMarketInfo {
                    decimals: metadata.decimals,
                    _max_leverage: metadata.max_leverage,
                },
            );
        }

        self.static_market_info = static_market_info_update;

        Ok(())
    }

    async fn get_worst_price(&self, symbol: &str, side: &OrderSide) -> Result<Decimal, DexError> {
        let market_price = self.get_market_price(symbol).await?;

        let worst_price = slippage_price(market_price, *side == OrderSide::Long);
        Ok(worst_price)
    }

    async fn get_market_price(&self, symbol: &str) -> Result<Decimal, DexError> {
        let market_info_guard = self.dynamic_market_info.read().await;
        match market_info_guard.get(symbol) {
            Some(v) => match v.market_price {
                Some(price) => Ok(price),
                None => Err(DexError::Other("Price is None".to_string())),
            },
            None => Err(DexError::Other("No price available".to_string())),
        }
    }

    fn calculate_min_tick(price: Decimal) -> Decimal {
        let price_str = price.to_string();
        let parts: Vec<&str> = price_str.split('.').collect();
        let integer_part = parts[0];

        if integer_part.len() >= 5 {
            return Decimal::ONE;
        }

        let scale = 5 - integer_part.len();

        Decimal::new(1, scale as u32)
    }

    fn round_price(
        price: Decimal,
        min_tick: Decimal,
        order_side: OrderSide,
        spread: Option<i64>,
    ) -> Decimal {
        if min_tick.is_zero() {
            log::error!("round_price: min_tick is zero");
            return price;
        }
        let spread = match spread {
            Some(v) => Decimal::new(v, 0),
            None => Decimal::ZERO,
        };

        match order_side {
            OrderSide::Long => (price / min_tick - spread).floor() * min_tick,
            OrderSide::Short => (price / min_tick + spread).ceil() * min_tick,
        }
    }

    fn floor_size(&self, size: Decimal, symbol: &str) -> Decimal {
        let decimals = match self.static_market_info.get(symbol) {
            Some(v) => v.decimals,
            None => {
                log::error!("symbol meta is not available: {}", symbol);
                return size;
            }
        };

        size.round_dp(decimals)
    }

    fn extract_asset_name(symbol: &str) -> &str {
        symbol.split('-').next().unwrap_or(symbol)
    }
}

fn resolve_coin(sym: &str, map: &HashMap<String, usize>) -> String {
    if sym.contains('/') {
        // ---- Spot ----
        match map.get(sym) {
            Some(idx) => format!("@{}", idx),
            None => {
                log::warn!("resolve_coin: {} is not in spot_index_map", sym);
                sym.to_string()
            }
        }
    } else if let Some(base) = sym.strip_suffix("-USD") {
        // ---- Perp ----
        base.to_string()
    } else {
        sym.to_string()
    }
}

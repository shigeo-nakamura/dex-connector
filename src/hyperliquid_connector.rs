use crate::{
    dex_connector::{slippage_price, string_to_decimal, DexConnector},
    dex_request::{DexError, DexRequest, HttpMethod},
    dex_websocket::DexWebSocket,
    BalanceResponse, CreateOrderResponse, FilledOrder, FilledOrdersResponse, OrderSide,
    TickerResponse,
};
use ::serde::{Deserialize, Serialize};
use async_trait::async_trait;
use debot_utils::parse_to_decimal;
use ethers::{
    signers::{LocalWallet, Signer},
    types::H160,
};
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use hyperliquid_rust_sdk::{
    BaseUrl, ClientCancelRequest, ClientLimit, ClientOrder, ClientOrderRequest, ExchangeClient,
    ExchangeDataStatus, ExchangeResponseStatus,
};
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::signal::unix::SignalKind;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio::{net::TcpStream, task::JoinHandle};
use tokio::{select, signal::unix::signal};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;

struct Config {
    evm_wallet_address: String,
    symbol_list: Vec<String>,
}

#[derive(Debug)]
struct TradeResult {
    pub filled_side: OrderSide,
    pub filled_size: Decimal,
    pub filled_value: Decimal,
    pub filled_fee: Decimal,
    order_id: String,
}

#[derive(Default)]
struct DynamicMarketInfo {
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
    // key = symbol
    dynamic_market_info: Arc<RwLock<HashMap<String, DynamicMarketInfo>>>,
    static_market_info: HashMap<String, StaticMarketInfo>,
    exchange_client: ExchangeClient,
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
            "allMids" => {
                let mids_data = AllMidsData::deserialize(helper.data)
                    .map(WebSocketData::AllMidsData)
                    .map_err(serde::de::Error::custom)?;
                mids_data
            }
            "userFills" => {
                let fills_data = UserFillsData::deserialize(helper.data)
                    .map(WebSocketData::UserFillsData)
                    .map_err(serde::de::Error::custom)?;
                fills_data
            }
            "candle" => {
                let candle_data = CandleData::deserialize(helper.data)
                    .map(WebSocketData::CandleData)
                    .map_err(serde::de::Error::custom)?;
                candle_data
            }
            "activeAssetCtx" => {
                let active_asset_ctx_data = ActiveAssetCtxData::deserialize(helper.data)
                    .map(WebSocketData::ActiveAssetCtxData)
                    .map_err(serde::de::Error::custom)?;
                active_asset_ctx_data
            }
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
        symbol_list: &[&str],
    ) -> Result<Self, DexError> {
        let request = DexRequest::new(rest_endpoint.to_owned()).await?;
        let web_socket = DexWebSocket::new(web_socket_endpoint.to_owned());

        let evm_wallet_address = match vault_address.clone() {
            Some(v) => v.to_owned(),
            None => evm_wallet_address.to_owned(),
        };

        let config = Config {
            evm_wallet_address,
            symbol_list: symbol_list
                .iter()
                .map(|&s| s.strip_suffix("-USD").unwrap_or(s).to_string())
                .collect(),
        };

        let vault_address: Option<H160> = match vault_address {
            Some(v) => H160::from_str(&v).ok(),
            None => None,
        };

        let mut local_wallet: LocalWallet = private_key.parse().unwrap();

        if use_agent {
            let exchange_client =
                ExchangeClient::new(None, local_wallet, Some(BaseUrl::Mainnet), None, None)
                    .await
                    .unwrap();

            let (private_key, response) = exchange_client.approve_agent(None).await.unwrap();
            log::info!("Agent creation response: {response:?}");

            local_wallet = private_key.parse().unwrap();
            local_wallet.address();
            log::info!("Agent address: {:?}", local_wallet.address());
        }

        let exchange_client = ExchangeClient::new(
            None,
            local_wallet,
            Some(BaseUrl::Mainnet),
            None,
            vault_address,
        )
        .await
        .unwrap();

        let mut instance = HyperliquidConnector {
            config,
            request,
            web_socket,
            trade_results: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(AtomicBool::new(false)),
            read_socket: Arc::new(Mutex::new(None)),
            write_socket: Arc::new(Mutex::new(None)),
            task_handle_read_message: Arc::new(Mutex::new(None)),
            task_handle_read_sigterm: Arc::new(Mutex::new(None)),
            dynamic_market_info: Arc::new(RwLock::new(HashMap::new())),
            static_market_info: HashMap::new(),
            exchange_client,
        };

        instance.retrive_market_metadata().await?;

        Ok(instance)
    }

    pub async fn start_web_socket(&self) -> Result<(), DexError> {
        log::info!("start_web_socket");

        // Establish socket connection
        let web_socket = self.web_socket.clone();
        let (write, read) = match web_socket.connect().await {
            Ok((write, read)) => (write, read),
            Err(_) => {
                return Err(DexError::Other(
                    "Failed to connect to WebSocket".to_string(),
                ))
            }
        };
        let mut read_socket_lock = self.read_socket.lock().await;
        *read_socket_lock = Some(read);
        drop(read_socket_lock);

        let mut write_socket_lock = self.write_socket.lock().await;
        *write_socket_lock = Some(write);
        drop(write_socket_lock);

        self.running.store(true, Ordering::SeqCst);

        // Subscribe channels
        self.subscribe_to_channels(&self.config.evm_wallet_address)
            .await
            .unwrap();
        log::debug!("subscription is done");

        // Create a message recevie thread
        let running_clone = self.running.clone();
        let read_clone = self.read_socket.clone();
        let write_clone = self.write_socket.clone();
        let dynamic_market_info_clone = self.dynamic_market_info.clone();
        let trade_results_clone = self.trade_results.clone();
        let handle = tokio::spawn(async move {
            log::debug!("WebSocket message handling task started");

            let mut message_counter = 0;
            while running_clone.load(Ordering::SeqCst) {
                let mut read_guard = read_clone.lock().await;
                if let Some(read_stream) = read_guard.as_mut() {
                    tokio::select! {
                        message = read_stream.next() => match message {
                            Some(Ok(msg)) => {
                                message_counter = 0;
                                if msg == "{}".into() {
                                    if let Some(write_socket) = write_clone.lock().await.as_mut() {
                                        if let Err(e) = write_socket.send(Message::Text(msg.to_string())).await {
                                            log::error!("Failed to send message: {}", e);
                                            break;
                                        }
                                    } else {
                                        log::error!("Write socket is not available");
                                        break;
                                    }
                                    log::trace!("Responsed to the ping")
                                } else {
                                    log::trace!("Received message: {:?}", msg);
                                    if let Err(e) = Self::handle_websocket_message(
                                        msg,
                                        dynamic_market_info_clone.clone(),
                                        trade_results_clone.clone(),
                                    )
                                    .await
                                    {
                                        log::error!("Error handling WebSocket message: {:?}", e);
                                        break;
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                log::error!("Failed to read: {:?}", e);
                                break;
                            }
                            None => {
                                log::info!("WebSocket stream ended");
                                break;
                            }
                        },
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(10)) => {
                            if !running_clone.load(Ordering::SeqCst) {
                                log::info!("Running flag changed, shutting down...");
                                break;
                            }
                            message_counter += 1;
                            if message_counter >= 10 {
                                log::error!("No message has been received for some time");
                                break;
                            }
                        },
                    }
                }
            }

            running_clone.store(false, Ordering::SeqCst);
            log::info!("WebSocket message handling task ended");
        });
        let mut task_handle = self.task_handle_read_message.lock().await;
        *task_handle = Some(handle);

        // Create a SITERM wait thread
        let mut sigterm =
            signal(SignalKind::terminate()).expect("Failed to create SIGTERM listener");
        let running_clone = self.running.clone();
        let handle = tokio::spawn(async move {
            log::debug!("SIGTERM handling task started");
            loop {
                select! {
                    _ = sigterm.recv() => {
                        log::info!("SIGTERM received, shutting down...");
                        running_clone.store(false, Ordering::SeqCst);
                        break;
                    },
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                        if !running_clone.load(Ordering::SeqCst) {
                            log::info!("Running flag changed, shutting down...");
                            break;
                        }
                    },
                }
            }
        });
        let mut task_handle = self.task_handle_read_sigterm.lock().await;
        *task_handle = Some(handle);

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
                .send(Message::Text(user_fills_subscription))
                .await
            {
                return Err(DexError::WebSocketError(format!(
                    "Failed to subscribe to userFills: {}",
                    e
                )));
            }

            for symbol in &self.config.symbol_list {
                let candle_subscription = serde_json::json!({
                    "method": "subscribe",
                    "subscription": {
                        "type": "candle",
                        "coin": symbol,
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
                        "coin": symbol,
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
    ) -> Result<(), DexError> {
        match msg {
            Message::Text(text) => {
                for line in text.split('\n') {
                    if line.is_empty() {
                        continue;
                    }

                    if let Ok(message) = serde_json::from_str::<WebSocketMessage>(line) {
                        match &message.data {
                            WebSocketData::AllMidsData(data) => {
                                Self::process_all_mids_message(data, dynamic_market_info.clone())
                                    .await;
                            }
                            WebSocketData::CandleData(data) => {
                                Self::process_candle_message(data, dynamic_market_info.clone())
                                    .await;
                            }
                            WebSocketData::UserFillsData(data) => {
                                Self::process_account_data(data, trade_results.clone()).await;
                            }
                            WebSocketData::ActiveAssetCtxData(data) => {
                                Self::process_active_asset_ctx_message(
                                    data,
                                    dynamic_market_info.clone(),
                                )
                                .await;
                            }
                        }
                    }
                }
            }
            _ => {
                log::warn!("Message is empty");
            }
        }
        Ok(())
    }

    async fn process_all_mids_message(
        mids_data: &AllMidsData,
        dynamic_market_info: Arc<RwLock<HashMap<String, DynamicMarketInfo>>>,
    ) {
        for (symbol, mid_price_str) in mids_data.mids.iter() {
            match string_to_decimal(Some(mid_price_str.to_string())) {
                Ok(mid_price) => {
                    let market_id = format!("{}-USD", symbol);

                    log::trace!("{} mid price = {:?}", market_id, mid_price);

                    let mut dynamic_market_info_guard = dynamic_market_info.write().await;

                    let market_info = dynamic_market_info_guard
                        .entry(market_id.to_string())
                        .or_insert_with(DynamicMarketInfo::default);

                    if market_info.min_tick.is_none() {
                        let min_tick = Self::calculate_min_tick(mid_price);
                        market_info.min_tick = Some(min_tick);
                    }

                    market_info.market_price = Some(mid_price);
                }
                Err(e) => log::error!("Failed to parse mid price for symbol: {}: {:?}", symbol, e),
            }
        }
    }

    async fn process_candle_message(
        candle_data: &CandleData,
        dynamic_market_info: Arc<RwLock<HashMap<String, DynamicMarketInfo>>>,
    ) {
        let symbol = &candle_data.s;
        let volume = candle_data.v;
        let num_trades = candle_data.n;
        log::debug!(
            "Candle update for {}: volume = {}, num_trades = {}",
            symbol,
            volume,
            num_trades
        );

        let market_id = format!("{}-USD", symbol);

        let mut dynamic_market_info_guard = dynamic_market_info.write().await;

        let market_info = dynamic_market_info_guard
            .entry(market_id.to_string())
            .or_insert_with(DynamicMarketInfo::default);

        market_info.volume = Some(volume);
        market_info.num_trades = Some(num_trades);
    }

    async fn process_active_asset_ctx_message(
        asset_data: &ActiveAssetCtxData,
        dynamic_market_info: Arc<RwLock<HashMap<String, DynamicMarketInfo>>>,
    ) {
        let symbol = &asset_data.coin;
        let funding_rate = asset_data.ctx.funding;
        let open_interest = asset_data.ctx.openInterest;
        let oracle_price = asset_data.ctx.oraclePx;

        log::debug!(
            "Asset CTX update for {}: funding_rate = {}, open_intereset = {}, oracle_price = {}",
            symbol,
            funding_rate,
            open_interest,
            oracle_price,
        );

        let market_id = format!("{}-USD", symbol);

        let mut dynamic_market_info_guard = dynamic_market_info.write().await;

        let market_info = dynamic_market_info_guard
            .entry(market_id.to_string())
            .or_insert_with(DynamicMarketInfo::default);

        market_info.funding_rate = Some(funding_rate);
        market_info.open_interest = Some(open_interest);
        market_info.oracle_price = Some(oracle_price);
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
            let market_id = format!("{}-USD", fill.coin);

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

    async fn get_balance(&self) -> Result<BalanceResponse, DexError> {
        let request_url = "/info";
        let action = HyperliquidDefaultPayload {
            r#type: "clearinghouseState".to_owned(),
            user: Some(self.config.evm_wallet_address.clone()),
        };
        let res = self
            .handle_request_with_action::<HyperliquidRetrieveUserStateResponse, HyperliquidDefaultPayload>(
                request_url.to_string(),
                &action,
            )
            .await?;

        if let Some(summary) = res.margin_summary {
            let equity = match parse_to_decimal(&summary.account_value) {
                Ok(v) => v,
                Err(e) => return Err(DexError::Other(format!("acount_equity: {:?}", e))),
            };

            let balance = match parse_to_decimal(&summary.total_rawusd) {
                Ok(v) => v,
                Err(e) => return Err(DexError::Other(format!("balance: {:?}", e))),
            };

            Ok(BalanceResponse {
                equity: equity,
                balance: balance,
            })
        } else {
            return Err(DexError::Other(String::from("Unknown error")));
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

    async fn clear_all_filled_order(&self) -> Result<(), DexError> {
        let mut trade_results_guard = self.trade_results.write().await;
        trade_results_guard.clear();
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
            Some(v) => (v, "Alo"),
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

        let asset = Self::extract_asset_name(symbol).to_owned();

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
        let asset = Self::extract_asset_name(symbol).to_owned();
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
            .filter(|order| {
                symbol.as_deref() == Some(&format!("{}-USD", order.coin)) || symbol.is_none()
            })
            .map(|order| order.oid.to_string())
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
            let order_symbol = format!("{}-USD", order.coin);
            if (symbol.as_deref() == Some(&order_symbol) || symbol.is_none())
                && order_ids.contains(&order.oid.to_string())
            {
                cancels.push(ClientCancelRequest {
                    asset: Self::extract_asset_name(&order_symbol).to_owned(),
                    oid: order.oid,
                });
            }
        }

        if !cancels.is_empty() {
            if let Err(e) = self.exchange_client.bulk_cancel(cancels, None).await {
                log::error!("cancel_orders: Failed to cancel orders: {:?}", e);
            }
        }

        Ok(())
    }

    async fn close_all_positions(&self, symbol: Option<String>) -> Result<(), DexError> {
        let open_positions = self.get_positions().await?;

        for p in open_positions {
            let position = p.position;
            let order_symbol = format!("{}-USD", position.coin);
            if symbol.as_deref() == Some(&order_symbol) || symbol.is_none() {
                let reversed_side = if position.szi.is_sign_negative() {
                    OrderSide::Long
                } else {
                    OrderSide::Short
                };
                let size = position.szi.abs();

                if let Err(e) = self
                    .create_order(&order_symbol, size, reversed_side, None, None)
                    .await
                {
                    log::error!("close_all_positions: {:?}", e);
                }
            }
        }

        Ok(())
    }

    async fn clear_last_trades(&self, _symbol: &str) -> Result<(), DexError> {
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

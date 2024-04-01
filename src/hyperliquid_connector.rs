use crate::{
    dex_connector::{slippage_price, string_to_decimal, DexConnector},
    dex_request::{DexError, DexRequest, HttpMethod},
    dex_websocket::DexWebSocket,
    BalanceResponse, CreateOrderResponse, FilledOrder, FilledOrdersResponse, LastTradeResponse,
    OrderSide, TickerResponse,
};
use ::serde::{Deserialize, Serialize};
use async_trait::async_trait;
use debot_utils::parse_to_decimal;
use ecdsa::SigningKey;
use ethers::{prelude::Signer, types::transaction::eip712::EIP712Domain};
use ethers::{
    prelude::*,
    types::transaction::eip712::{Eip712DomainType, TypedData, Types},
};
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use generic_array::GenericArray;
use k256::Secp256k1;
use k256::SecretKey;
use rust_decimal::Decimal;
use serde::de::Error as SerdeError;
use serde::de::Visitor;
use serde::ser::SerializeStruct;
use serde_json::json;
use serde_json::Value;
use sha3::{Digest, Keccak256};
use std::fmt;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
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
    market_ids: Vec<String>,
    chain_id: u64,
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
    pub last_trade_price: Option<Decimal>,
    pub market_price: Option<Decimal>,
}

struct StaticMarketInfo {
    pub decimals: u32,
    pub max_leverage: u32,
    pub asset_index: u32,
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
    nonce_history: Arc<Mutex<VecDeque<u64>>>,
    wallet: Wallet<SigningKey<Secp256k1>>,
}

#[derive(Debug)]
struct WebSocketMessage {
    channel: String,
    data: WebSocketData,
}

#[derive(Debug)]
enum WebSocketData {
    AllMidsData(AllMidsData),
    UserFillsData(UserFillsData),
}

#[derive(Deserialize, Debug)]
struct AllMidsData {
    mids: HashMap<String, String>,
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
            _ => return Err(serde::de::Error::custom("unknown channel type")),
        };

        Ok(WebSocketMessage {
            channel: helper.channel,
            data,
        })
    }
}

impl HyperliquidConnector {
    pub async fn new(
        rest_endpoint: &str,
        web_socket_endpoint: &str,
        agent_private_key: &str,
        evm_wallet_address: &str,
        market_ids: &[String],
    ) -> Result<Self, DexError> {
        let request = DexRequest::new(rest_endpoint.to_owned()).await?;
        let web_socket = DexWebSocket::new(web_socket_endpoint.to_owned());
        let config = Config {
            evm_wallet_address: evm_wallet_address.to_owned(),
            market_ids: market_ids.to_vec(),
            chain_id: 1337,
        };

        let private_key_bytes = hex::decode(agent_private_key)
            .map_err(|e| DexError::Other(format!("Failed to decode private key: {}", e)))?;

        let private_key_bytes = GenericArray::from_slice(&private_key_bytes);

        let secret_key = SecretKey::from_bytes(private_key_bytes)
            .map_err(|e| DexError::Other(format!("Failed to create secret key: {}", e)))?;

        let signing_key = SigningKey::from(&secret_key);

        let wallet = Wallet::from(signing_key);
        log::debug!("wallet = {:?}", wallet);

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
            nonce_history: Arc::new(Mutex::new(VecDeque::with_capacity(20))),
            wallet,
            dynamic_market_info: Arc::new(RwLock::new(HashMap::new())),
            static_market_info: HashMap::new(),
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

        if let Some(handle) = self.task_handle_read_message.lock().await.take() {
            let _ = handle.await;
        }

        if let Some(handle) = self.task_handle_read_sigterm.lock().await.take() {
            let _ = handle.await;
        }

        {
            let mut write_guard = self.write_socket.lock().await;
            *write_guard = None;

            let mut read_guard = self.read_socket.lock().await;
            *read_guard = None;
        }

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
                            WebSocketData::UserFillsData(data) => {
                                Self::process_account_data(data, trade_results.clone()).await;
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

                    market_info.market_price = Some(mid_price);
                }
                Err(e) => log::error!("Failed to parse mid price for symbol: {}: {:?}", symbol, e),
            }
        }
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

impl HyperliquidCommonResponse {
    fn is_success(&self) -> Result<(), DexError> {
        match self.status.as_str() {
            "ok" => Ok(()),
            "err" => match &self.response {
                HyperliquidResponse::Err(err_msg) => Err(DexError::Other(err_msg.clone())),
                HyperliquidResponse::Ok(_) => {
                    Err(DexError::Other("Unexpected success response".to_owned()))
                }
            },
            _ => Err(DexError::Other("Unknown status".to_owned())),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct HyperliquidCommonResponse {
    status: String,
    response: HyperliquidResponse,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum HyperliquidResponse {
    Ok(ResponseType),
    Err(String),
}

#[derive(Serialize, Deserialize, Debug)]
struct ResponseType {
    #[serde(rename = "type")]
    response_type: String,
}
#[derive(Deserialize, Debug)]
struct HyperliquidCommonResponseData {
    statuses: Vec<HyperliquidCommonResponseStatus>,
}
#[derive(Deserialize, Debug)]
struct HyperliquidCommonResponseStatus {
    error: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
struct HyperliquidUpdateLeveragePayload {
    r#type: String,
    asset: u32,
    #[serde(rename = "isCross")]
    is_cross: bool,
    leverage: u32,
}
#[derive(Deserialize, Debug)]
struct HyperliquidUpdateLeverageResponse {
    status: String,
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

#[derive(Serialize, Debug, Clone)]
struct HyperliquidCreateOrderPayload {
    r#type: String,
    orders: Vec<HyperliquidOrder>,
    grouping: String,
}
impl Serialize for HyperliquidOrder {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HyperliquidOrder", 6)?;
        state.serialize_field("a", &self.asset)?;
        state.serialize_field("b", &self.is_buy)?;
        state.serialize_field("p", &self.limit_px.to_string())?;
        state.serialize_field("s", &self.sz.to_string())?;
        state.serialize_field("r", &self.reduce_only)?;
        state.serialize_field("t", &self.order_type)?;
        state.end()
    }
}
#[derive(Debug, Clone)]
struct HyperliquidOrder {
    asset: u32,
    is_buy: bool,
    limit_px: Decimal,
    sz: Decimal,
    reduce_only: bool,
    order_type: HyperliquidOrderType,
}
impl HyperliquidOrderType {
    fn new(tif_type: &str) -> Self {
        let limit_order_type = HyperliquidLimitOrderType {
            tif: tif_type.to_owned(),
        };
        Self {
            limit: limit_order_type,
        }
    }
}
#[derive(Serialize, Debug, Clone)]
struct HyperliquidOrderType {
    limit: HyperliquidLimitOrderType,
}
#[derive(Serialize, Debug, Clone)]
struct HyperliquidLimitOrderType {
    tif: String,
}

impl HyperliquidCreateOrderResponse {
    fn is_success(&self) -> Result<HyperliquidOrderStatus, DexError> {
        if self.status == "ok" {
            match &self.response.data.statuses[0] {
                HyperliquidOrderOrErrorResponse::ErrorResponse(err) => {
                    Err(DexError::Other(err.error.clone()))
                }
                HyperliquidOrderOrErrorResponse::OrderStatus(status) => Ok(status.clone()),
            }
        } else {
            Err(DexError::Other("Order creation failed".to_owned()))
        }
    }
}
#[derive(Deserialize, Debug)]
struct HyperliquidCreateOrderResponse {
    status: String,
    response: HyperliquidOrderResponseBody,
}
#[derive(Deserialize, Debug, Clone)]
struct HyperliquidOrderResponseBody {
    r#type: String,
    data: HyperliquidOrderResponseData,
}

#[derive(Deserialize, Debug, Clone)]
struct HyperliquidOrderResponseData {
    #[serde(deserialize_with = "deserialize_order_or_error")]
    statuses: Vec<HyperliquidOrderOrErrorResponse>,
}
fn deserialize_order_or_error<'de, D>(
    deserializer: D,
) -> Result<Vec<HyperliquidOrderOrErrorResponse>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct OrderOrErrorResponseVisitor;

    impl<'de> Visitor<'de> for OrderOrErrorResponseVisitor {
        type Value = Vec<HyperliquidOrderOrErrorResponse>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a list of order statuses or errors")
        }

        fn visit_seq<S>(self, mut seq: S) -> Result<Self::Value, S::Error>
        where
            S: serde::de::SeqAccess<'de>,
        {
            let mut items = Vec::new();

            while let Some(value) = seq.next_element::<Value>()? {
                let item = if value.get("resting").is_some() || value.get("filled").is_some() {
                    serde_json::from_value(value)
                        .map(HyperliquidOrderOrErrorResponse::OrderStatus)
                        .map_err(SerdeError::custom)?
                } else {
                    serde_json::from_value(value)
                        .map(HyperliquidOrderOrErrorResponse::ErrorResponse)
                        .map_err(SerdeError::custom)?
                };
                items.push(item);
            }

            Ok(items)
        }
    }

    deserializer.deserialize_seq(OrderOrErrorResponseVisitor)
}
#[derive(Deserialize, Debug, Clone)]
#[serde(untagged)]
enum HyperliquidOrderOrErrorResponse {
    OrderStatus(HyperliquidOrderStatus),
    ErrorResponse(HyperliquidErrorResponse),
}
#[derive(Deserialize, Debug, Clone)]
struct HyperliquidErrorResponse {
    error: String,
}
#[derive(Deserialize, Debug, Clone)]
struct HyperliquidOrderStatus {
    resting: Option<HyperliquidOrderStatusDetail>,
    filled: Option<HyperliquidOrderStatusDetail>,
}
#[derive(Deserialize, Debug, Clone)]
struct HyperliquidOrderStatusDetail {
    oid: u64,
}
impl HyperliquidOrderStatus {
    fn get_oid(&self) -> Option<u64> {
        self.resting
            .as_ref()
            .map(|d| d.oid)
            .or_else(|| self.filled.as_ref().map(|d| d.oid))
    }
}

#[derive(Serialize, Debug, Clone)]
struct HyperliquidCancelOrderPayload {
    r#type: String,
    cancels: Vec<HyperliquidCancelOrder>,
}
#[derive(Serialize, Debug, Clone)]
struct HyperliquidCancelOrder {
    a: u32,
    o: u64,
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

#[derive(Serialize, Debug)]
struct HyperliquidRetrieveUserFillsPayload {
    r#type: String,
    user: String,
    #[serde(rename = "startTime")]
    start_time: u128,
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
    #[serde(rename = "onlyIsolated")]
    only_isolated: bool,
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

    async fn set_leverage(&self, symbol: &str, leverage: u32) -> Result<(), DexError> {
        let request_url = "/exchange";
        let asset = self.get_asset_index(symbol)?;
        let action = HyperliquidUpdateLeveragePayload {
            r#type: "updateLeverage".to_owned(),
            asset,
            is_cross: false,
            leverage,
        };

        let res = self
            .handle_request_with_action::<HyperliquidCommonResponse, HyperliquidUpdateLeveragePayload>(
                request_url.to_string(),
                &action,
                true,
            )
            .await?;

        res.is_success()
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerResponse, DexError> {
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

        Ok(TickerResponse {
            symbol: symbol.to_owned(),
            price,
            min_tick: None,
            min_order: None,
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
                false,
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

    async fn create_order(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        price: Option<Decimal>,
    ) -> Result<CreateOrderResponse, DexError> {
        let request_url = "/exchange";
        let (price, time_in_force) = match price {
            Some(v) => (v, "Alo"),
            None => {
                let price = self.get_worst_price(symbol, &side).await?;
                (price, "Ioc")
            }
        };

        let rounded_price = Self::round_price(price, side.clone());
        let rounded_size = self.floor_size(size, symbol);

        log::debug!("{}, {}({}), {}", symbol, rounded_price, price, rounded_size,);

        if rounded_price * rounded_size <= Decimal::new(10, 0) {
            return Ok(CreateOrderResponse {
                order_id: String::new(),
                ordered_price: Decimal::new(0, 0),
                ordered_size: Decimal::new(0, 0),
            });
        }

        let asset = self.get_asset_index(symbol)?;

        let order = HyperliquidOrder {
            asset,
            is_buy: side == OrderSide::Long,
            limit_px: rounded_price,
            sz: rounded_size,
            reduce_only: false,
            order_type: HyperliquidOrderType::new(time_in_force),
        };

        let action = HyperliquidCreateOrderPayload {
            r#type: "order".to_string(),
            grouping: "na".to_string(),
            orders: vec![order],
        };

        let res = self
            .handle_request_with_action::<HyperliquidCreateOrderResponse, HyperliquidCreateOrderPayload>(
                request_url.to_string(),
                &action,
                true,
            )
            .await?;

        let order_status = res.is_success()?;

        let order_id = match order_status.get_oid() {
            Some(v) => v,
            None => {
                return Err(DexError::Other("order_id is unknown".to_string()));
            }
        };

        Ok(CreateOrderResponse {
            order_id: order_id.to_string(),
            ordered_price: rounded_price,
            ordered_size: rounded_size,
        })
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<(), DexError> {
        let request_url = "/exchange";
        let asset = self.get_asset_index(symbol)?;
        let action = HyperliquidCancelOrderPayload {
            r#type: "cancel".to_owned(),
            cancels: vec![HyperliquidCancelOrder {
                a: asset,
                o: u64::from_str(order_id).unwrap_or_default(),
            }],
        };

        let res = self
            .handle_request_with_action::<HyperliquidCommonResponse, HyperliquidCancelOrderPayload>(
                request_url.to_string(),
                &action,
                true,
            )
            .await?;

        res.is_success()
    }

    async fn cancel_all_orders(&self, symbol: Option<String>) -> Result<(), DexError> {
        let open_orders = self.get_orders().await?;
        for order in open_orders {
            let order_symbol = format!("{}-USD", order.coin);
            if symbol.as_deref() == Some(&order_symbol) || symbol.is_none() {
                if let Err(e) = self
                    .cancel_order(&order_symbol, &order.oid.to_string())
                    .await
                {
                    log::error!("cancel_all_orders: {:?}", e);
                }
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
                    .create_order(&order_symbol, size, reversed_side, None)
                    .await
                {
                    log::error!("close_all_positions: {:?}", e);
                }
            }
        }

        Ok(())
    }

    async fn get_last_trades(&self, _symbol: &str) -> Result<LastTradeResponse, DexError> {
        Ok(LastTradeResponse::default())
    }

    async fn clear_last_trades(&self, _symbol: &str) -> Result<(), DexError> {
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct PhantomAgent {
    source: String,
    connection_id: Vec<u8>,
}

impl PhantomAgent {
    fn new(hash: &[u8], is_mainnet: bool) -> Self {
        Self {
            source: if is_mainnet {
                "a".to_string()
            } else {
                "b".to_string()
            },
            connection_id: hash.to_vec(),
        }
    }
}

impl HyperliquidConnector {
    fn action_hash<A: Serialize + std::fmt::Debug>(
        action: &A,
        vault_address: Option<&str>,
        nonce: u64,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        action
            .serialize(&mut rmp_serde::Serializer::new(&mut buf).with_struct_map())
            .unwrap();

        buf.extend_from_slice(&nonce.to_be_bytes());

        if let Some(address) = vault_address {
            buf.push(1);
            buf.extend_from_slice(&hex::decode(address.trim_start_matches("0x")).unwrap());
        } else {
            buf.push(0);
        }

        let mut hasher = Keccak256::new();
        hasher.update(&buf);
        hasher.finalize().to_vec()
    }

    async fn sign_l1_action<A: Serialize + std::fmt::Debug>(
        &self,
        action: &A,
        nonce: u64,
        vault_address: Option<&str>,
        is_mainnet: bool,
    ) -> Result<Signature, Box<dyn std::error::Error + Send + Sync>> {
        let hash = Self::action_hash(action, vault_address, nonce);
        let phantom_agent = PhantomAgent::new(&hash, is_mainnet);
        let connection_id_hex = phantom_agent
            .connection_id
            .iter()
            .map(|byte| format!("{:02x}", byte))
            .collect::<String>();
        let phantom_agent_value = serde_json::to_value(phantom_agent)?;

        let mut types = Types::new();
        types.insert(
            "EIP712Domain".to_string(),
            vec![
                Eip712DomainType {
                    name: "name".to_string(),
                    r#type: "string".to_string(),
                },
                Eip712DomainType {
                    name: "version".to_string(),
                    r#type: "string".to_string(),
                },
                Eip712DomainType {
                    name: "chainId".to_string(),
                    r#type: "uint256".to_string(),
                },
                Eip712DomainType {
                    name: "verifyingContract".to_string(),
                    r#type: "address".to_string(),
                },
            ],
        );
        types.insert(
            "Agent".to_string(),
            vec![
                Eip712DomainType {
                    name: "source".to_string(),
                    r#type: "string".to_string(),
                },
                Eip712DomainType {
                    name: "connectionId".to_string(),
                    r#type: "bytes32".to_string(),
                },
            ],
        );
        log::debug!("EIP712 types: {:?}", types);

        let domain = EIP712Domain {
            name: Some("Exchange".to_string()),
            version: Some("1".to_string()),
            chain_id: Some(self.config.chain_id.into()),
            verifying_contract: Some("0x0000000000000000000000000000000000000000".parse()?),
            salt: None,
        };

        let typed_data = TypedData {
            types,
            domain,
            primary_type: "Agent".to_string(),
            message: BTreeMap::from([
                ("source".to_string(), phantom_agent_value["source"].clone()),
                (
                    "connectionId".to_string(),
                    Value::String(format!("0x{}", connection_id_hex)),
                ),
            ]),
        };
        log::debug!("EIP712 TypedData prepared for signing: {:?}", typed_data);

        let signature = self
            .wallet
            .sign_typed_data(&typed_data)
            .await
            .map_err(|e| DexError::Other(format!("Failed to sign typed data: {}", e)))?;

        log::debug!("Signature obtained: {:?}", signature);

        Ok(signature)
    }

    async fn generate_nonce(&self) -> u64 {
        let mut nonce_history = self.nonce_history.lock().await;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;

        let mut nonce = now;
        while nonce_history.contains(&nonce) {
            nonce += 1;
        }

        if nonce_history.len() == nonce_history.capacity() {
            nonce_history.pop_front();
        }
        nonce_history.push_back(nonce);

        nonce
    }

    // Add a new parameter `modify_payload` to determine how to handle the payload
    async fn handle_request_with_action<T, U>(
        &self,
        request_url: String,
        action: &U,
        modify_payload: bool, // New parameter to control payload modification
    ) -> Result<T, DexError>
    where
        T: for<'de> Deserialize<'de>,
        U: Serialize + std::fmt::Debug + Clone,
    {
        if modify_payload {
            let nonce = self.generate_nonce().await;
            let signature = self
                .sign_l1_action(action, nonce, None, true)
                .await
                .map_err(|e| DexError::Other(e.to_string()))?;

            let json_payload = json!({
                "action": action,
                "nonce": nonce,
                "signature": signature,
                "vaultAddress": Option::<String>::None,
            });

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
        } else {
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
                false,
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
                false,
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
                false,
            )
            .await?;

        let mut static_market_info_update = HashMap::new();
        for (index, metadata) in res.universe.into_iter().enumerate() {
            let market_id = format!("{}-USD", metadata.name);
            static_market_info_update.insert(
                market_id,
                StaticMarketInfo {
                    decimals: metadata.decimals,
                    max_leverage: metadata.max_leverage,
                    asset_index: index as u32,
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

    fn get_asset_index(&self, symbol: &str) -> Result<u32, DexError> {
        match self.static_market_info.get(symbol) {
            Some(info) => Ok(info.asset_index),
            None => Err(DexError::Other(format!("Unknown symbol: {}", symbol))),
        }
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

    fn round_price(price: Decimal, order_side: OrderSide) -> Decimal {
        let price_str = price.to_string();
        let mut parts = price_str.split('.');
        let integer_part = parts.next().unwrap_or("0");
        let decimal_part = parts.next().unwrap_or("");

        if decimal_part.len() <= 6 && integer_part.len() + decimal_part.len() <= 5 {
            return Self::adjust_by_tick(price, order_side, integer_part, decimal_part);
        }

        let trimmed_decimal_part = &decimal_part[0..decimal_part.len().min(6)];

        if integer_part.len() >= 5 {
            let rounded = integer_part[0..5].parse::<Decimal>().unwrap();
            return Self::adjust_by_tick(rounded, order_side, integer_part, "");
        }

        if integer_part.len() + trimmed_decimal_part.len() > 5 {
            let total_digits = 5 - integer_part.len();
            let final_decimal_part = &trimmed_decimal_part[0..total_digits.min(6)];
            let rounded_str = format!("{}.{}", integer_part, final_decimal_part);
            let rounded = Decimal::from_str(&rounded_str).unwrap();
            return Self::adjust_by_tick(rounded, order_side, integer_part, final_decimal_part);
        }

        Self::adjust_by_tick(price, order_side, integer_part, decimal_part)
    }

    fn adjust_by_tick(
        price: Decimal,
        order_side: OrderSide,
        integer_part: &str,
        decimal_part: &str,
    ) -> Decimal {
        let adjustment = Decimal::new(1, (integer_part.len() + decimal_part.len()) as u32);
        match order_side {
            OrderSide::Long => price - adjustment,
            OrderSide::Short => price + adjustment,
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
}

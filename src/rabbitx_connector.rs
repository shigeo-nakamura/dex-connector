use crate::{
    dex_connector::{string_to_decimal, DexConnector},
    dex_request::{DexError, DexRequest, HttpMethod},
    dex_websocket::DexWebSocket,
    BalanceResponse, CreateOrderResponse, FilledOrder, FilledOrdersResponse, OrderSide,
    TickerResponse,
};
use async_trait::async_trait;
use debot_utils::{parse_to_decimal, serialize_decimal_as_f64};
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use hmac::{Hmac, Mac};
use lazy_static::lazy_static;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
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
    profile_id: String,
    api_key: String,
    // public_jwt: String,
    refresh_token: String,
    secret: String,
    private_jwt: Arc<Mutex<String>>,
    market_ids: Vec<String>,
}

struct TradeResult {
    pub order_id: String,
    pub filled_side: OrderSide,
    pub filled_size: Decimal,
    pub filled_value: Decimal,
    pub filled_fee: Decimal,
}

struct MarketInfo {
    pub last_trade_price: Option<Decimal>,
    pub market_price: Option<Decimal>,
    pub min_order: Option<Decimal>,
    pub min_tick: Option<Decimal>,
}

pub struct RabbitxConnector {
    config: Config,
    request: DexRequest,
    web_socket: DexWebSocket,
    running: Arc<AtomicBool>,
    read_socket: Arc<Mutex<Option<SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>>>,
    task_handle_read_message: Arc<Mutex<Option<JoinHandle<()>>>>,
    task_handle_read_sigterm: Arc<Mutex<Option<JoinHandle<()>>>>,
    // 1st key = symbol, 2nd key = order_id
    trade_results: Arc<RwLock<HashMap<String, HashMap<String, TradeResult>>>>,
    // key = symbol
    market_info: Arc<RwLock<HashMap<String, MarketInfo>>>,
}

#[derive(Deserialize, Debug)]
struct WebSocketMessage {
    push: Option<PushData>,
}

#[derive(Deserialize, Debug)]
struct PushData {
    channel: String,
    #[serde(rename = "pub")]
    pub_data: PubData,
}

#[derive(Deserialize, Debug)]
struct PubData {
    data: MarketData,
}

#[derive(Deserialize, Debug)]
struct MarketData {
    id: Option<String>,
    min_tick: Option<String>,
    min_order: Option<String>,
    last_trade_price: Option<String>,
    market_price: Option<String>,
}

#[derive(Deserialize, Debug)]
struct Fill {
    order_id: String,
    side: String,
    price: String,
    size: String,
    market_id: String,
    fee: String,
    trade_id: String,
}

// #[derive(Deserialize, Debug)]
// struct Order {
//     id: String,
//     market_id: String,
//     status: String,
// }

#[derive(Deserialize, Debug)]
struct AccountData {
    fills: Vec<Fill>,
    // orders: Vec<Order>,
}

#[derive(Deserialize, Debug)]
struct AccountPubData {
    data: AccountData,
}

#[derive(Deserialize, Debug)]
struct AccountPushData {
    channel: String,
    #[serde(rename = "pub")]
    pub_data: AccountPubData,
}

#[derive(Deserialize, Debug)]
struct AccountWebSocketMessage {
    push: Option<AccountPushData>,
}

impl RabbitxConnector {
    pub async fn new(
        rest_endpoint: &str,
        web_socket_endpoint: &str,
        profile_id: &str,
        api_key: &str,
        _public_jwt: &str,
        refresh_token: &str,
        secret: &str,
        private_jwt: &str,
        market_ids: &[String],
    ) -> Result<Self, DexError> {
        let request = DexRequest::new(rest_endpoint.to_owned()).await?;
        let web_socket = DexWebSocket::new(web_socket_endpoint.to_owned());
        let config = Config {
            profile_id: profile_id.to_owned(),
            api_key: api_key.to_owned(),
            refresh_token: refresh_token.to_owned(),
            secret: secret.to_owned(),
            private_jwt: Arc::new(Mutex::new(private_jwt.to_owned())),
            market_ids: market_ids.to_vec(),
        };
        Ok(RabbitxConnector {
            config,
            request,
            web_socket,
            trade_results: Arc::new(RwLock::new(HashMap::new())),
            market_info: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(AtomicBool::new(false)),
            read_socket: Arc::new(Mutex::new(None)),
            task_handle_read_message: Arc::new(Mutex::new(None)),
            task_handle_read_sigterm: Arc::new(Mutex::new(None)),
        })
    }

    pub async fn start_web_socket(&self) -> Result<(), DexError> {
        log::info!("start_web_socket");

        // Update the token
        let new_token = self.update_token().await?;
        let mut token_lock = self.config.private_jwt.lock().await;
        *token_lock = new_token;
        drop(token_lock);

        // Establish socket connection
        let web_socket = self.web_socket.clone();
        let (mut write, read) = match web_socket.connect().await {
            Ok((write, read)) => (write, read),
            Err(_) => {
                return Err(DexError::Other(
                    "Failed to connect to WebSocket".to_string(),
                ))
            }
        };
        let mut read_lock = self.read_socket.lock().await;
        *read_lock = Some(read);

        self.running.store(true, Ordering::SeqCst);

        // Send an authentication message
        let auth_message = Message::Text(format!(
            r#"{{ "connect": {{ "token": "{}", "name": "js" }}, "id": 1 }}"#,
            self.config.private_jwt.lock().await,
        ));
        write.send(auth_message).await.unwrap();
        log::debug!("authentication is done");

        // Subscribe channels
        self.subscribe_to_channels(&mut write, &self.config.market_ids)
            .await
            .unwrap();
        log::debug!("subscription is done");

        // Create a message recevie thread
        let running_clone = self.running.clone();
        let read_clone = self.read_socket.clone();
        let write_clone = Arc::new(Mutex::new(write));
        let market_info_clone = self.market_info.clone();
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
                                    write_clone
                                        .lock()
                                        .await
                                        .send(Message::Text(msg.to_string()))
                                        .await
                                        .unwrap();
                                    log::trace!("Responsed to the ping")
                                } else {
                                    log::trace!("Received message: {:?}", msg);
                                    if let Err(e) = Self::handle_websocket_message(
                                        msg,
                                        market_info_clone.clone(),
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
        Ok(())
    }

    async fn subscribe_to_channels(
        &self,
        socket: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        market_ids: &[String],
    ) -> Result<(), DexError> {
        let mut channels = Vec::new();

        for market_id in market_ids {
            // channels.push(format!("orderbook:{}", market_id));
            // channels.push(format!("trade:{}", market_id));
            channels.push(format!("market:{}", market_id));
        }

        channels.push(format!("account@{}", self.config.profile_id));

        for (idx, channel) in channels.iter().enumerate() {
            let data = serde_json::json!({
                "subscribe": {
                    "channel": channel,
                    "name": "js",
                },
                "id": idx + 1
            });
            socket.send(Message::Text(data.to_string())).await.unwrap();
        }

        Ok(())
    }

    async fn handle_websocket_message(
        msg: Message,
        market_info: Arc<RwLock<HashMap<String, MarketInfo>>>,
        trade_results: Arc<RwLock<HashMap<String, HashMap<String, TradeResult>>>>,
    ) -> Result<(), DexError> {
        match msg {
            Message::Text(text) => {
                for line in text.split('\n') {
                    if line.is_empty() {
                        continue;
                    }

                    if let Ok(account_message) =
                        serde_json::from_str::<AccountWebSocketMessage>(line)
                    {
                        if let Some(account_push_data) = account_message.push {
                            if account_push_data.channel.starts_with("account@") {
                                Self::process_account_data(
                                    &account_push_data.pub_data.data,
                                    trade_results.clone(),
                                )
                                .await;
                            }
                        }
                    } else if let Ok(market_message) =
                        serde_json::from_str::<WebSocketMessage>(line)
                    {
                        if let Some(push_data) = market_message.push {
                            if push_data.channel.starts_with("market:") {
                                let market_id = match push_data.pub_data.data.id {
                                    Some(v) => v,
                                    None => return Ok(()),
                                };
                                let last_trade_price =
                                    string_to_decimal(push_data.pub_data.data.last_trade_price)
                                        .ok();
                                let market_price =
                                    string_to_decimal(push_data.pub_data.data.market_price).ok();
                                let min_order =
                                    string_to_decimal(push_data.pub_data.data.min_order).ok();
                                let min_tick =
                                    string_to_decimal(push_data.pub_data.data.min_tick).ok();

                                let mut market_info_guard = market_info.write().await;
                                let market_info_entry = market_info_guard
                                    .entry(market_id.to_owned())
                                    .or_insert_with(|| MarketInfo {
                                        last_trade_price: None,
                                        market_price: None,
                                        min_order: None,
                                        min_tick: None,
                                    });

                                log::trace!("last_trade_price = {:?}", last_trade_price);

                                if last_trade_price.is_some() {
                                    market_info_entry.last_trade_price = last_trade_price;
                                }

                                if market_price.is_some() {
                                    market_info_entry.market_price = market_price;
                                }

                                if min_order.is_some() {
                                    market_info_entry.min_order = min_order;
                                }
                                if min_tick.is_some() {
                                    market_info_entry.min_tick = min_tick;
                                }
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

    async fn process_account_data(
        data: &AccountData,
        trade_results: Arc<RwLock<HashMap<String, HashMap<String, TradeResult>>>>,
    ) {
        for fill in &data.fills {
            log::debug!("fill: {:?}", fill);

            let filled_price = match parse_to_decimal(&fill.price) {
                Ok(v) => v,
                Err(_) => {
                    log::error!("Invalid filled_price: {}", fill.price);
                    return;
                }
            };

            let filled_side = match fill.side.as_str() {
                "long" => OrderSide::Long,
                "short" => OrderSide::Short,
                _ => return,
            };

            let filled_size = match parse_to_decimal(&fill.size) {
                Ok(v) => v,
                Err(_) => {
                    log::error!("Invalid filled_size: {}", fill.size);
                    return;
                }
            };

            let filled_fee = match parse_to_decimal(&fill.fee) {
                Ok(v) => -v,
                Err(_) => {
                    log::error!("Invalid filled_fee: {}", fill.fee);
                    return;
                }
            };

            let filled_value = filled_price * filled_size;

            let trade_result = TradeResult {
                filled_side,
                filled_size,
                filled_value,
                filled_fee,
                order_id: fill.order_id.clone(),
            };

            // let order_id_suffix = fill.order_id.split('@').nth(1).unwrap_or(&fill.order_id);

            let mut trade_results_guard = trade_results.write().await;
            trade_results_guard
                .entry(fill.market_id.clone())
                .or_default()
                .insert(fill.trade_id.to_owned(), trade_result);
        }
    }
}

#[derive(Serialize, Debug)]
struct RabbitxDefaultPayload {}

#[derive(Deserialize, Debug)]
struct RabbitxCommonResponse {
    success: bool,
    error: String,
}

#[derive(Serialize, Debug)]
struct RabbitxAccountLeveragePayload {
    market_id: String,
    leverage: u32,
    method: String,
    path: String,
}

#[derive(Deserialize, Debug)]
struct RabbitxAccountResult {
    account_equity: String,
    balance: String,
}

#[derive(Deserialize, Debug)]
struct RabbitxAccountResponse {
    success: bool,
    error: String,
    result: Vec<RabbitxAccountResult>,
}

#[derive(Serialize, Debug)]
struct RabbitxCreateOrderPayload {
    market_id: String,
    #[serde(serialize_with = "serialize_decimal_as_f64")]
    price: Decimal,
    side: String,
    #[serde(serialize_with = "serialize_decimal_as_f64")]
    size: Decimal,
    r#type: String,
    time_in_force: String,
    method: String,
    path: String,
}

#[derive(Deserialize, Debug)]
struct RabbitxOrderResult {
    id: String,
}

#[derive(Deserialize, Debug)]
struct RabbitxOrderResponse {
    success: bool,
    error: String,
    result: Vec<RabbitxOrderResult>,
}

#[derive(Serialize, Debug)]
struct RabbitxCancelOrderPayload {
    order_id: String,
    market_id: String,
    method: String,
    path: String,
}

#[derive(Serialize, Debug)]
struct RabbitxCancelAllOrderPayload {
    method: String,
    path: String,
}

#[derive(Deserialize, Debug)]
struct RabbitxPositionsResult {
    market_id: String,
    side: String,
    size: String,
}

#[derive(Deserialize, Debug)]
struct RabbitxPositionsResponse {
    success: bool,
    error: String,
    result: Vec<RabbitxPositionsResult>,
}

#[derive(Deserialize, Debug)]
struct RabbitxOrdersResult {
    id: String,
    market_id: String,
    status: String,
}

#[derive(Deserialize, Debug)]
struct RabbitxOrdersResponse {
    success: bool,
    error: String,
    result: Vec<RabbitxOrdersResult>,
}

#[derive(Serialize, Debug)]
struct RabbitxUpdateTokenPayload {
    is_client: bool,
    refresh_token: String,
    method: String,
    path: String,
}

#[derive(Deserialize, Debug)]
struct RabbitxUpdateTokenResult {
    jwt: String,
}

#[derive(Deserialize, Debug)]
struct RabbitxUpdateTokenResponse {
    success: bool,
    error: String,
    result: Vec<RabbitxUpdateTokenResult>,
}

lazy_static! {
    static ref PRICE_DISCOUNT_RATIO: Decimal = Decimal::new(1, 1);
}

#[async_trait]
impl DexConnector for RabbitxConnector {
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
        let request_url = "/account/leverage";
        let payload = RabbitxAccountLeveragePayload {
            market_id: symbol.to_string(),
            leverage,
            method: String::from("PUT"),
            path: String::from(request_url),
        };

        let res = self
            .handle_request_with_auth::<RabbitxCommonResponse, RabbitxAccountLeveragePayload>(
                HttpMethod::Put,
                request_url.to_string(),
                Some(&payload),
            )
            .await?;

        if res.success {
            Ok(())
        } else {
            Err(DexError::Other(res.error))
        }
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerResponse, DexError> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(DexError::NoConnection);
        }

        let market_info_guard = self.market_info.read().await;
        let price;
        let min_tick;
        let min_order;
        match market_info_guard.get(symbol) {
            Some(v) => {
                match v.market_price {
                    Some(v) => price = v,
                    None => return Err(DexError::Other("No price available".to_string())),
                }
                match v.min_tick {
                    Some(v) => min_tick = v,
                    None => return Err(DexError::Other("No min_tick available".to_string())),
                }
                match v.min_order {
                    Some(v) => min_order = v,
                    None => return Err(DexError::Other("No min_order available".to_string())),
                }
            }
            None => return Err(DexError::Other("No market info available".to_string())),
        };

        Ok(TickerResponse {
            symbol: symbol.to_owned(),
            price,
            min_tick,
            min_order,
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
                filled_side: order.filled_side.clone(),
                filled_size: order.filled_size,
                filled_fee: order.filled_fee,
                filled_value: order.filled_value,
            };
            response.push(filled_order);
        }

        Ok(FilledOrdersResponse { orders: response })
    }

    async fn get_balance(&self) -> Result<BalanceResponse, DexError> {
        let request_url = "/account";
        let res = self
            .handle_request_with_auth::<RabbitxAccountResponse, RabbitxDefaultPayload>(
                HttpMethod::Get,
                request_url.to_string(),
                None,
            )
            .await?;

        if res.success {
            let equity = match parse_to_decimal(&res.result[0].account_equity) {
                Ok(v) => v,
                Err(e) => return Err(DexError::Other(format!("acount_equity: {:?}", e))),
            };

            let balance = match parse_to_decimal(&res.result[0].balance) {
                Ok(v) => v,
                Err(e) => return Err(DexError::Other(format!("balance: {:?}", e))),
            };

            Ok(BalanceResponse {
                equity: equity,
                balance: balance,
            })
        } else {
            Err(DexError::Other(res.error))
        }
    }

    async fn clear_filled_order(&self, symbol: &str, trade_id: &str) -> Result<(), DexError> {
        let mut trade_results_guard = self.trade_results.write().await;

        if let Some(orders) = trade_results_guard.get_mut(symbol) {
            if orders.contains_key(trade_id) {
                orders.remove(trade_id);
            } else {
                return Err(DexError::Other(format!(
                    "filled order(trade_id:{}({})) does not exist",
                    trade_id, symbol
                )));
            }
        } else {
            return Err(DexError::Other(format!(
                "filled order(symbol:{}({})) does not exist",
                symbol, trade_id
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
        let request_url = "/orders";
        let (price, r#type, time_in_force) = match price {
            Some(v) => (v, "limit", "post_only"),
            None => {
                let price = self.get_worst_price(symbol, &side).await?;
                (price, "market", "good_till_cancel")
            }
        };
        let side_str = format!("{}", side);

        // round down the price and size
        let rounded_price;
        let rounded_size;
        {
            let market_info_guard = self.market_info.read().await;
            let (min_tick, min_order) = match market_info_guard.get(symbol) {
                Some(v) => (v.min_tick, v.min_order),
                None => return Err(DexError::Other("No price available".to_string())),
            };
            let min_tick = match min_tick {
                Some(v) => v,
                None => return Err(DexError::Other("No min_tick available".to_string())),
            };
            let min_order = match min_order {
                Some(v) => v,
                None => return Err(DexError::Other("No min_order available".to_string())),
            };

            rounded_price = self.round_price(price, min_tick, side);
            rounded_size = self.round_size(size, min_order);

            log::debug!(
                "{}, {}, {:?}({}), {:?}({})",
                symbol,
                price,
                rounded_price,
                min_tick,
                rounded_size,
                min_order
            );
        }

        if rounded_size.is_zero() {
            return Ok(CreateOrderResponse {
                order_id: String::new(),
                ordered_price: Decimal::new(0, 0),
                ordered_size: Decimal::new(0, 0),
            });
        }

        let payload = RabbitxCreateOrderPayload {
            market_id: symbol.to_string(),
            price: rounded_price,
            side: side_str,
            size: rounded_size,
            r#type: r#type.to_string(),
            time_in_force: time_in_force.to_string(),
            method: String::from("POST"),
            path: String::from(request_url),
        };

        let res = self
            .handle_request_with_auth::<RabbitxOrderResponse, RabbitxCreateOrderPayload>(
                HttpMethod::Post,
                request_url.to_string(),
                Some(&payload),
            )
            .await?;

        if res.success {
            Ok(CreateOrderResponse {
                order_id: res.result[0].id.to_owned(),
                ordered_price: rounded_price,
                ordered_size: rounded_size,
            })
        } else {
            Err(DexError::Other(res.error))
        }
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<(), DexError> {
        let request_url = "/orders";
        let payload = RabbitxCancelOrderPayload {
            order_id: order_id.to_string(),
            market_id: symbol.to_string(),
            method: String::from("DELETE"),
            path: String::from(request_url),
        };

        let res = self
            .handle_request_with_auth::<RabbitxCommonResponse, RabbitxCancelOrderPayload>(
                HttpMethod::Delete,
                request_url.to_string(),
                Some(&payload),
            )
            .await?;

        if res.success {
            Ok(())
        } else {
            Err(DexError::Other(res.error))
        }
    }

    async fn cancel_all_orders(&self, symbol: Option<String>) -> Result<(), DexError> {
        match symbol {
            Some(market_id) => {
                let current_orders = self.get_orders().await?;
                for order in current_orders {
                    if market_id != order.market_id {
                        continue;
                    }
                    if order.status != "open" {
                        continue;
                    }
                    if let Err(e) = self.cancel_order(&order.market_id, &order.id).await {
                        log::error!("cancel_all_orders: {:?}", e);
                    }
                }
            }
            None => {
                let request_url = "/orders/cancel_all";
                let payload = RabbitxCancelAllOrderPayload {
                    method: String::from("DELETE"),
                    path: String::from(request_url),
                };

                self
                    .handle_request_with_auth::<RabbitxCommonResponse, RabbitxCancelAllOrderPayload>(
                        HttpMethod::Delete,
                        request_url.to_string(),
                        Some(&payload),
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn close_all_positions(&self, symbol: Option<String>) -> Result<(), DexError> {
        let current_positions = self.get_positions().await?;

        for position in current_positions {
            if let Some(market_id) = symbol.clone() {
                if market_id != position.market_id {
                    continue;
                }
            }

            log::info!(
                "close_all_posiiton: {}, {}, {}",
                position.market_id,
                position.side,
                position.size
            );

            let position_size = match parse_to_decimal(&position.size) {
                Ok(v) => v,
                Err(e) => {
                    return Err(DexError::Other(format!("{:?}", e)));
                }
            };

            let reversed_side = if position.side == "long" {
                OrderSide::Short
            } else {
                OrderSide::Long
            };
            if let Err(e) = self
                .create_order(&position.market_id, position_size, reversed_side, None)
                .await
            {
                log::error!("close_all_positions: {:?}", e);
            }
        }

        Ok(())
    }
}

impl RabbitxConnector {
    async fn add_auth_headers(&self, json_payload: &str) -> HashMap<String, String> {
        let payload_map: HashMap<String, Value> =
            serde_json::from_str(json_payload).unwrap_or_default();

        let mut sorted_keys = payload_map.keys().collect::<Vec<&String>>();
        sorted_keys.sort();
        let sorted_payload = sorted_keys
            .iter()
            .map(|k| {
                let v = payload_map.get(*k).unwrap();
                format!("{}={}", k, v.to_string().trim_matches('"'))
            })
            .collect::<Vec<String>>()
            .join("");

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .checked_add(Duration::new(5, 0)) // add 5 seconds
            .expect("Failed to add offset to timestamp")
            .as_secs()
            .to_string();
        let message = format!("{}{}", sorted_payload, timestamp);

        let mut hasher = Sha256::new();
        hasher.update(message.as_bytes());
        let message_hash = hasher.finalize();

        let secret_key = hex::decode(&self.config.secret).expect("Invalid hex string");
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&secret_key).expect("HMAC can take key of any size");
        mac.update(&message_hash);
        let signature = format!("0x{}", hex::encode(mac.finalize().into_bytes()));

        let mut headers = HashMap::new();
        headers.insert("RBT-SIGNATURE".to_string(), signature);
        headers.insert("RBT-API-KEY".to_string(), self.config.api_key.clone());
        headers.insert("RBT-TS".to_string(), timestamp);
        headers
    }

    async fn handle_request_with_auth<T, U>(
        &self,
        method: HttpMethod,
        request_url: String,
        payload: Option<&U>,
    ) -> Result<T, DexError>
    where
        T: for<'de> Deserialize<'de>,
        U: Serialize,
    {
        let payload_str = if let Some(p) = payload {
            serde_json::to_string(p).unwrap()
        } else {
            "".to_string()
        };

        let auth_headers = self.add_auth_headers(&payload_str).await;
        self.request
            .handle_request::<T, U>(method, request_url, &auth_headers, payload_str)
            .await
    }

    async fn get_positions(&self) -> Result<Vec<RabbitxPositionsResult>, DexError> {
        let request_url = "/positions";
        let res = self
            .handle_request_with_auth::<RabbitxPositionsResponse, RabbitxDefaultPayload>(
                HttpMethod::Get,
                request_url.to_string(),
                None,
            )
            .await?;

        if res.success {
            Ok(res.result)
        } else {
            Err(DexError::Other(res.error))
        }
    }

    async fn get_orders(&self) -> Result<Vec<RabbitxOrdersResult>, DexError> {
        let request_url = "/orders";
        let res = self
            .handle_request_with_auth::<RabbitxOrdersResponse, RabbitxDefaultPayload>(
                HttpMethod::Get,
                request_url.to_string(),
                None,
            )
            .await?;

        if res.success {
            Ok(res.result)
        } else {
            Err(DexError::Other(res.error))
        }
    }

    async fn get_worst_price(&self, symbol: &str, side: &OrderSide) -> Result<Decimal, DexError> {
        let market_info_guard = self.market_info.read().await;
        let last_price = match market_info_guard.get(symbol) {
            Some(v) => match v.market_price {
                Some(v) => v,
                None => return Err(DexError::Other("Price is None".to_string())),
            },
            None => return Err(DexError::Other("No price available".to_string())),
        };

        let worst_price = if *side == OrderSide::Long {
            last_price * (Decimal::new(1, 0) + *PRICE_DISCOUNT_RATIO)
        } else {
            last_price * (Decimal::new(1, 0) - *PRICE_DISCOUNT_RATIO)
        };

        Ok(worst_price)
    }

    async fn update_token(&self) -> Result<String, DexError> {
        let request_url = "/jwt";
        let payload = RabbitxUpdateTokenPayload {
            is_client: false,
            refresh_token: self.config.private_jwt.lock().await.to_owned(),
            method: String::from("POST"),
            path: String::from(request_url),
        };
        let res = self
            .handle_request_with_auth::<RabbitxUpdateTokenResponse, RabbitxUpdateTokenPayload>(
                HttpMethod::Post,
                request_url.to_string(),
                Some(&payload),
            )
            .await?;

        if res.success {
            log::info!("Updated token successfully");
            Ok(res.result[0].jwt.to_owned())
        } else {
            Err(DexError::Other(res.error))
        }
    }
}
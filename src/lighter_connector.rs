use crate::{
    dex_connector::{string_to_decimal, DexConnector},
    dex_request::{DexError, HttpMethod},
    dex_websocket::DexWebSocket,
    BalanceResponse, CanceledOrder, CanceledOrdersResponse, CreateOrderResponse, FilledOrder,
    FilledOrdersResponse, OrderSide, TickerResponse, TpSl,
};
use async_trait::async_trait;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::RwLock, time::sleep};
use tokio_tungstenite::tungstenite::protocol::Message;

#[derive(Clone)]
pub struct LighterConnector {
    api_key: String,
    private_key: String,
    base_url: String,
    websocket_url: String,
    client: Client,
    filled_orders: Arc<RwLock<HashMap<String, Vec<FilledOrder>>>>,
    canceled_orders: Arc<RwLock<HashMap<String, Vec<CanceledOrder>>>>,
    is_running: Arc<AtomicBool>,
    ws: Option<DexWebSocket>,
}

#[derive(Deserialize, Debug)]
struct LighterOrderbookResponse {
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
    #[serde(rename = "lastPrice")]
    last_price: Option<String>,
    #[serde(rename = "markPrice")]
    mark_price: Option<String>,
}

#[derive(Deserialize, Debug)]
struct LighterAccountResponse {
    #[serde(rename = "totalEquity")]
    total_equity: String,
    #[serde(rename = "availableBalance")]
    available_balance: String,
}

#[derive(Serialize, Debug)]
struct LighterOrderRequest {
    #[serde(rename = "ticker")]
    ticker: String,
    #[serde(rename = "amount")]
    amount: String,
    #[serde(rename = "price")]
    price: Option<String>,
    #[serde(rename = "orderType")]
    order_type: String,
    #[serde(rename = "timeInForce")]
    time_in_force: String,
}

#[derive(Deserialize, Debug)]
struct LighterOrderResponse {
    #[serde(rename = "orderId")]
    order_id: String,
    #[serde(rename = "price")]
    price: String,
    #[serde(rename = "amount")]
    amount: String,
}

#[derive(Deserialize, Debug)]
struct LighterTradeResponse {
    #[serde(rename = "orderId")]
    order_id: String,
    #[serde(rename = "tradeId")]
    trade_id: String,
    #[serde(rename = "side")]
    side: String,
    #[serde(rename = "size")]
    size: String,
    #[serde(rename = "price")]
    price: String,
    #[serde(rename = "fee")]
    fee: String,
    #[serde(rename = "timestamp")]
    _timestamp: u64,
}

impl LighterConnector {
    pub fn new(
        api_key: String,
        private_key: String,
        base_url: String,
        websocket_url: String,
    ) -> Result<Self, DexError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| DexError::Other(format!("Failed to create HTTP client: {}", e)))?;

        Ok(LighterConnector {
            api_key,
            private_key,
            base_url,
            websocket_url: websocket_url.clone(),
            client,
            filled_orders: Arc::new(RwLock::new(HashMap::new())),
            canceled_orders: Arc::new(RwLock::new(HashMap::new())),
            is_running: Arc::new(AtomicBool::new(false)),
            ws: Some(DexWebSocket::new(websocket_url)),
        })
    }

    async fn sign_request(&self, payload: &str, timestamp: u64) -> Result<String, DexError> {
        use secp256k1::{Message, Secp256k1, SecretKey};
        use sha3::{Digest, Keccak256};

        let secp = Secp256k1::new();
        let secret_key = SecretKey::from_slice(
            &hex::decode(&self.private_key)
                .map_err(|e| DexError::Other(format!("Invalid private key format: {}", e)))?,
        )
        .map_err(|e| DexError::Other(format!("Invalid private key: {}", e)))?;

        let message_string = format!("{}{}", payload, timestamp);
        let mut hasher = Keccak256::new();
        hasher.update(message_string.as_bytes());
        let hash = hasher.finalize();

        let message = Message::from_digest_slice(&hash)
            .map_err(|e| DexError::Other(format!("Failed to create message: {}", e)))?;

        let signature = secp.sign_ecdsa(&message, &secret_key);
        Ok(hex::encode(signature.serialize_compact()))
    }

    async fn make_request<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        method: HttpMethod,
        payload: Option<&str>,
    ) -> Result<T, DexError> {
        let url = format!("{}{}", self.base_url, endpoint);
        let timestamp = Utc::now().timestamp_millis() as u64;

        let payload_str = payload.unwrap_or("");
        let signature = self.sign_request(payload_str, timestamp).await?;

        let mut request = match method {
            HttpMethod::Get => self.client.get(&url),
            HttpMethod::Post => self.client.post(&url),
            HttpMethod::Delete => self.client.delete(&url),
            HttpMethod::Put => self.client.put(&url),
        };

        request = request
            .header("X-API-KEY", &self.api_key)
            .header("X-TIMESTAMP", timestamp.to_string())
            .header("X-SIGNATURE", signature)
            .header("Content-Type", "application/json");

        if let Some(body) = payload {
            request = request.body(body.to_string());
        }

        let response = request
            .send()
            .await
            .map_err(|e| DexError::Other(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(DexError::Other(format!(
                "API request failed with status {}: {}",
                status, text
            )));
        }

        let response_text = response
            .text()
            .await
            .map_err(|e| DexError::Other(format!("Failed to read response: {}", e)))?;

        serde_json::from_str(&response_text)
            .map_err(|e| DexError::Other(format!("Failed to parse response: {}", e)))
    }
}

#[async_trait]
impl DexConnector for LighterConnector {
    async fn start(&self) -> Result<(), DexError> {
        self.is_running.store(true, Ordering::SeqCst);

        if let Some(ws) = &self.ws {
            let ws_clone = ws.clone();
            let filled_orders = self.filled_orders.clone();
            let canceled_orders = self.canceled_orders.clone();
            let is_running = self.is_running.clone();

            tokio::spawn(async move {
                loop {
                    if !is_running.load(Ordering::SeqCst) {
                        break;
                    }

                    match ws_clone.connect().await {
                        Ok((mut ws_sender, mut ws_receiver)) => {
                            log::info!("Connected to Lighter WebSocket");

                            // Subscribe to user data stream
                            let subscribe_msg = serde_json::json!({
                                "id": 1,
                                "method": "subscribe",
                                "params": ["user.orders", "user.trades"]
                            });

                            if let Err(e) = ws_sender
                                .send(Message::Text(subscribe_msg.to_string()))
                                .await
                            {
                                log::error!("Failed to send subscription message: {}", e);
                                sleep(Duration::from_secs(5)).await;
                                continue;
                            }

                            // Listen for messages
                            while is_running.load(Ordering::SeqCst) {
                                match tokio::time::timeout(
                                    Duration::from_secs(30),
                                    ws_receiver.next(),
                                )
                                .await
                                {
                                    Ok(Some(Ok(message))) => {
                                        if let Err(e) = LighterConnector::handle_websocket_message(
                                            message,
                                            filled_orders.clone(),
                                            canceled_orders.clone(),
                                        )
                                        .await
                                        {
                                            log::error!("Error handling WebSocket message: {}", e);
                                        }
                                    }
                                    Ok(Some(Err(e))) => {
                                        log::error!("WebSocket error: {}", e);
                                        break;
                                    }
                                    Ok(None) => {
                                        log::warn!("WebSocket stream ended");
                                        break;
                                    }
                                    Err(_) => {
                                        log::warn!("WebSocket timeout, sending ping");
                                        if let Err(e) = ws_sender.send(Message::Ping(vec![])).await
                                        {
                                            log::error!("Failed to send ping: {}", e);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        Err(_) => {
                            log::error!(
                                "Failed to connect to Lighter WebSocket, retrying in 5 seconds"
                            );
                            sleep(Duration::from_secs(5)).await;
                        }
                    }
                }
                log::info!("Lighter WebSocket task ended");
            });
        }

        log::info!("Lighter connector started");
        Ok(())
    }

    async fn stop(&self) -> Result<(), DexError> {
        self.is_running.store(false, Ordering::SeqCst);
        log::info!("Lighter connector stopped");
        Ok(())
    }

    async fn restart(&self, _max_retries: i32) -> Result<(), DexError> {
        self.stop().await?;
        sleep(Duration::from_secs(1)).await;
        self.start().await
    }

    async fn set_leverage(&self, symbol: &str, leverage: u32) -> Result<(), DexError> {
        // Note: Lighter does not currently support dynamic leverage setting via API
        // Leverage is typically set at the account level or per order
        log::warn!(
            "Set leverage API not available for Lighter - leverage for {} requested: {}",
            symbol,
            leverage
        );
        Ok(())
    }

    async fn get_ticker(
        &self,
        symbol: &str,
        test_price: Option<Decimal>,
    ) -> Result<TickerResponse, DexError> {
        if let Some(price) = test_price {
            return Ok(TickerResponse {
                symbol: symbol.to_string(),
                price,
                min_tick: Some(Decimal::new(1, 4)),
                min_order: Some(Decimal::new(1, 1)),
                volume: None,
                num_trades: None,
                open_interest: None,
                funding_rate: None,
                oracle_price: None,
            });
        }

        let endpoint = format!("/v1/orderbook/{}", symbol);
        let orderbook: LighterOrderbookResponse =
            self.make_request(&endpoint, HttpMethod::Get, None).await?;

        let price = if let Some(last_price) = orderbook.last_price {
            string_to_decimal(Some(last_price))?
        } else if let Some(mark_price) = orderbook.mark_price {
            string_to_decimal(Some(mark_price))?
        } else if !orderbook.bids.is_empty() && !orderbook.asks.is_empty() {
            let bid = string_to_decimal(Some(orderbook.bids[0][0].clone()))?;
            let ask = string_to_decimal(Some(orderbook.asks[0][0].clone()))?;
            (bid + ask) / Decimal::new(2, 0)
        } else {
            return Err(DexError::Other("No price data available".to_string()));
        };

        Ok(TickerResponse {
            symbol: symbol.to_string(),
            price,
            min_tick: Some(Decimal::new(1, 4)),
            min_order: Some(Decimal::new(1, 1)),
            volume: None,
            num_trades: None,
            open_interest: None,
            funding_rate: None,
            oracle_price: None,
        })
    }

    async fn get_filled_orders(&self, symbol: &str) -> Result<FilledOrdersResponse, DexError> {
        let endpoint = format!("/v1/trades/{}", symbol);
        let trades: Vec<LighterTradeResponse> =
            self.make_request(&endpoint, HttpMethod::Get, None).await?;

        let mut orders = Vec::new();
        for trade in trades {
            let side = match trade.side.as_str() {
                "buy" | "long" => Some(OrderSide::Long),
                "sell" | "short" => Some(OrderSide::Short),
                _ => None,
            };

            orders.push(FilledOrder {
                order_id: trade.order_id,
                is_rejected: false,
                trade_id: trade.trade_id,
                filled_side: side,
                filled_size: string_to_decimal(Some(trade.size)).ok(),
                filled_value: string_to_decimal(Some(trade.price)).ok(),
                filled_fee: string_to_decimal(Some(trade.fee)).ok(),
            });
        }

        let mut filled_orders = self.filled_orders.write().await;
        filled_orders.insert(symbol.to_string(), orders.clone());

        Ok(FilledOrdersResponse { orders })
    }

    async fn get_canceled_orders(&self, symbol: &str) -> Result<CanceledOrdersResponse, DexError> {
        let canceled_orders = self.canceled_orders.read().await;
        let orders = canceled_orders.get(symbol).cloned().unwrap_or_default();
        Ok(CanceledOrdersResponse { orders })
    }

    async fn get_balance(&self, _symbol: Option<&str>) -> Result<BalanceResponse, DexError> {
        let account: LighterAccountResponse = self
            .make_request("/v1/account", HttpMethod::Get, None)
            .await?;

        let equity = string_to_decimal(Some(account.total_equity))?;
        let balance = string_to_decimal(Some(account.available_balance))?;

        Ok(BalanceResponse { equity, balance })
    }

    async fn clear_filled_order(&self, symbol: &str, trade_id: &str) -> Result<(), DexError> {
        let mut filled_orders = self.filled_orders.write().await;
        if let Some(orders) = filled_orders.get_mut(symbol) {
            orders.retain(|order| order.trade_id != trade_id);
        }
        Ok(())
    }

    async fn clear_all_filled_orders(&self) -> Result<(), DexError> {
        let mut filled_orders = self.filled_orders.write().await;
        filled_orders.clear();
        Ok(())
    }

    async fn clear_canceled_order(&self, symbol: &str, order_id: &str) -> Result<(), DexError> {
        let mut canceled_orders = self.canceled_orders.write().await;
        if let Some(orders) = canceled_orders.get_mut(symbol) {
            orders.retain(|order| order.order_id != order_id);
        }
        Ok(())
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
    ) -> Result<CreateOrderResponse, DexError> {
        let order_type = if price.is_some() { "limit" } else { "market" };
        let amount = if side == OrderSide::Short {
            -size
        } else {
            size
        }
        .to_string();

        let order_request = LighterOrderRequest {
            ticker: symbol.to_string(),
            amount,
            price: price.map(|p| p.to_string()),
            order_type: order_type.to_string(),
            time_in_force: "GTC".to_string(),
        };

        let payload = serde_json::to_string(&order_request)
            .map_err(|e| DexError::Other(format!("Failed to serialize order: {}", e)))?;

        let response: LighterOrderResponse = self
            .make_request("/v1/order", HttpMethod::Post, Some(&payload))
            .await?;

        Ok(CreateOrderResponse {
            order_id: response.order_id,
            ordered_price: string_to_decimal(Some(response.price))?,
            ordered_size: string_to_decimal(Some(response.amount))?,
        })
    }

    async fn create_trigger_order(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        trigger_px: Decimal,
        is_market: bool,
        tpsl: TpSl,
    ) -> Result<CreateOrderResponse, DexError> {
        let order_type = match (tpsl, is_market) {
            (TpSl::Tp, true) => "take_profit_market",
            (TpSl::Tp, false) => "take_profit",
            (TpSl::Sl, true) => "stop_loss_market",
            (TpSl::Sl, false) => "stop_loss",
        };

        let amount = if side == OrderSide::Short {
            -size
        } else {
            size
        }
        .to_string();

        let payload = serde_json::json!({
            "ticker": symbol,
            "amount": amount,
            "triggerPrice": trigger_px.to_string(),
            "orderType": order_type,
            "timeInForce": "GTC"
        })
        .to_string();

        let response: LighterOrderResponse = self
            .make_request("/v1/trigger-order", HttpMethod::Post, Some(&payload))
            .await?;

        Ok(CreateOrderResponse {
            order_id: response.order_id,
            ordered_price: string_to_decimal(Some(response.price))?,
            ordered_size: string_to_decimal(Some(response.amount))?,
        })
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<(), DexError> {
        let payload = serde_json::json!({
            "ticker": symbol,
            "orderId": order_id
        })
        .to_string();

        let _: Value = self
            .make_request("/v1/cancel-order", HttpMethod::Delete, Some(&payload))
            .await?;

        let mut canceled_orders = self.canceled_orders.write().await;
        let orders = canceled_orders
            .entry(symbol.to_string())
            .or_insert_with(Vec::new);
        orders.push(CanceledOrder {
            order_id: order_id.to_string(),
            canceled_timestamp: Utc::now().timestamp_millis() as u64,
        });

        Ok(())
    }

    async fn cancel_all_orders(&self, symbol: Option<String>) -> Result<(), DexError> {
        let payload = if let Some(sym) = symbol {
            serde_json::json!({ "ticker": sym }).to_string()
        } else {
            "{}".to_string()
        };

        let _: Value = self
            .make_request("/v1/cancel-all-orders", HttpMethod::Delete, Some(&payload))
            .await?;
        Ok(())
    }

    async fn cancel_orders(
        &self,
        symbol: Option<String>,
        order_ids: Vec<String>,
    ) -> Result<(), DexError> {
        for order_id in order_ids {
            if let Some(ref sym) = symbol {
                self.cancel_order(sym, &order_id).await?;
            }
        }
        Ok(())
    }

    async fn close_all_positions(&self, symbol: Option<String>) -> Result<(), DexError> {
        let payload = if let Some(sym) = symbol {
            serde_json::json!({ "ticker": sym }).to_string()
        } else {
            "{}".to_string()
        };

        let _: Value = self
            .make_request("/v1/close-all-positions", HttpMethod::Post, Some(&payload))
            .await?;
        Ok(())
    }

    async fn clear_last_trades(&self, symbol: &str) -> Result<(), DexError> {
        let mut filled_orders = self.filled_orders.write().await;
        filled_orders.remove(symbol);
        Ok(())
    }

    async fn is_upcoming_maintenance(&self) -> bool {
        false
    }
}

impl LighterConnector {
    async fn handle_websocket_message(
        message: Message,
        filled_orders: Arc<RwLock<HashMap<String, Vec<FilledOrder>>>>,
        canceled_orders: Arc<RwLock<HashMap<String, Vec<CanceledOrder>>>>,
    ) -> Result<(), DexError> {
        if let Message::Text(text) = message {
            if let Ok(data) = serde_json::from_str::<Value>(&text) {
                log::debug!("Received WebSocket message: {}", text);

                // Handle different message types based on Lighter's WebSocket format
                if let Some(msg_type) = data.get("type").and_then(|v| v.as_str()) {
                    match msg_type {
                        "trade" => {
                            if let Some(trade_data) = data.get("data") {
                                Self::process_trade_message(trade_data, filled_orders).await?;
                            }
                        }
                        "order" => {
                            if let Some(order_data) = data.get("data") {
                                Self::process_order_message(order_data, canceled_orders).await?;
                            }
                        }
                        _ => {
                            log::debug!("Unknown message type: {}", msg_type);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn process_trade_message(
        trade_data: &Value,
        filled_orders: Arc<RwLock<HashMap<String, Vec<FilledOrder>>>>,
    ) -> Result<(), DexError> {
        if let (Some(symbol), Some(price), Some(quantity), Some(side)) = (
            trade_data.get("symbol").and_then(|v| v.as_str()),
            trade_data.get("price").and_then(|v| v.as_str()),
            trade_data.get("quantity").and_then(|v| v.as_str()),
            trade_data.get("side").and_then(|v| v.as_str()),
        ) {
            let filled_order = FilledOrder {
                order_id: trade_data
                    .get("orderId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string(),
                is_rejected: false,
                trade_id: trade_data
                    .get("tradeId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string(),
                filled_side: Some(if side == "buy" {
                    OrderSide::Long
                } else {
                    OrderSide::Short
                }),
                filled_size: string_to_decimal(Some(quantity.to_string())).ok(),
                filled_value: trade_data
                    .get("value")
                    .and_then(|v| v.as_str())
                    .and_then(|s| string_to_decimal(Some(s.to_string())).ok()),
                filled_fee: string_to_decimal(Some(
                    trade_data
                        .get("fee")
                        .and_then(|v| v.as_str())
                        .unwrap_or("0")
                        .to_string(),
                ))
                .ok(),
            };

            let mut orders = filled_orders.write().await;
            orders
                .entry(symbol.to_string())
                .or_insert_with(Vec::new)
                .push(filled_order);
        }
        Ok(())
    }

    async fn process_order_message(
        order_data: &Value,
        canceled_orders: Arc<RwLock<HashMap<String, Vec<CanceledOrder>>>>,
    ) -> Result<(), DexError> {
        if let Some(status) = order_data.get("status").and_then(|v| v.as_str()) {
            if status == "cancelled" {
                if let (Some(order_id), Some(symbol)) = (
                    order_data.get("orderId").and_then(|v| v.as_str()),
                    order_data.get("symbol").and_then(|v| v.as_str()),
                ) {
                    let canceled_order = CanceledOrder {
                        order_id: order_id.to_string(),
                        canceled_timestamp: order_data
                            .get("timestamp")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0),
                    };

                    let mut orders = canceled_orders.write().await;
                    orders
                        .entry(symbol.to_string())
                        .or_insert_with(Vec::new)
                        .push(canceled_order);
                }
            }
        }
        Ok(())
    }
}

pub fn create_lighter_connector(
    api_key: String,
    private_key: String,
    base_url: String,
    websocket_url: String,
) -> Result<Box<dyn DexConnector>, DexError> {
    let connector = LighterConnector::new(api_key, private_key, base_url, websocket_url)?;
    Ok(Box::new(connector))
}

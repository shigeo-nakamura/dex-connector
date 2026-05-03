//! Extended WebSocket layer: connection lifecycle (`connect_ws`),
//! per-stream supervisor state (`WsStreamState`), the four stream
//! readers (orderbook / trades / account / fallback_price helper),
//! and the SNAPSHOT/DELTA orderbook frame applier (also used by
//! mod.rs tests).
//!
//! Stream-error logging keeps the same `[ws ...]` prefixes the rest
//! of the codebase (and error-watch #168) was keying off.

use super::{
    normalize_symbol, position_snapshot_from_model, AccountTradeModel, BalanceModel, MarketModel,
    OpenOrderModel, OrderBookCacheEntry, PositionModel,
};
use crate::dex_request::DexError;
use crate::{
    BalanceResponse, FilledOrder, LastTrade, OpenOrder, OrderSide, PositionSnapshot,
};
use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Instant;
use tokio::sync::RwLock;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;

static WS_CONN_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WrappedStreamResponse<T> {
    #[serde(rename = "type")]
    pub(super) msg_type: Option<String>,
    pub(super) data: Option<T>,
    #[allow(dead_code)]
    error: Option<String>,
    pub(super) ts: i64,
    #[allow(dead_code)]
    seq: i64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct StreamOrderbookQuantity {
    // On SNAPSHOT frames `q` is the absolute quantity at that level.
    // On DELTA frames `q` is the *change* in quantity (can be negative),
    // and `c` is the new cumulative quantity the exchange already computed.
    // Prefer `c` when present so we never have to do delta arithmetic
    // ourselves.
    #[serde(alias = "q")]
    qty: Decimal,
    #[serde(alias = "p")]
    price: Decimal,
    #[serde(alias = "c", default)]
    cumulative: Option<Decimal>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub(super) struct StreamOrderbookUpdate {
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
    positions: Option<Vec<PositionModel>>,
}

pub(super) struct WsStreamState {
    pub(super) conn_id: u64,
    started_at: Instant,
    last_msg_at: Instant,
    last_ping_at: Option<Instant>,
    last_pong_at: Option<Instant>,
    messages_seen: u64,
}

impl WsStreamState {
    pub(super) fn new(conn_id: u64) -> Self {
        let now = Instant::now();
        Self {
            conn_id,
            started_at: now,
            last_msg_at: now,
            last_ping_at: None,
            last_pong_at: None,
            messages_seen: 0,
        }
    }

    fn on_message(&mut self) {
        self.messages_seen = self.messages_seen.saturating_add(1);
        self.last_msg_at = Instant::now();
    }

    fn on_ping(&mut self) {
        self.last_ping_at = Some(Instant::now());
    }

    fn on_pong(&mut self) {
        self.last_pong_at = Some(Instant::now());
    }

    fn context(&self, stream: &str, symbol: Option<&str>, url: &str) -> String {
        let now = Instant::now();
        let age_secs = now.duration_since(self.started_at).as_secs();
        let idle_secs = now.duration_since(self.last_msg_at).as_secs();
        let last_ping_age = self.last_ping_at.map(|ts| now.duration_since(ts).as_secs());
        let last_pong_age = self.last_pong_at.map(|ts| now.duration_since(ts).as_secs());
        format!(
            "stream={stream} symbol={} url={url} conn={} age={}s idle={}s messages={} last_ping_age={:?} last_pong_age={:?}",
            symbol.unwrap_or("-"),
            self.conn_id,
            age_secs,
            idle_secs,
            self.messages_seen,
            last_ping_age,
            last_pong_age
        )
    }
}

pub(super) fn next_ws_conn_id() -> u64 {
    WS_CONN_ID.fetch_add(1, Ordering::SeqCst)
}

fn log_ws_error_detail(
    stream: &str,
    symbol: Option<&str>,
    conn_id: u64,
    err: &tokio_tungstenite::tungstenite::Error,
) {
    let symbol = symbol.unwrap_or("-");
    match err {
        tokio_tungstenite::tungstenite::Error::Protocol(protocol_err) => {
            log::debug!(
                "WebSocket protocol error detail: {:?} (stream={}, symbol={}, conn={})",
                protocol_err,
                stream,
                symbol,
                conn_id
            );
        }
        tokio_tungstenite::tungstenite::Error::Io(io_err) => {
            log::debug!(
                "WebSocket IO error detail: kind={:?}, error={} (stream={}, symbol={}, conn={})",
                io_err.kind(),
                io_err,
                stream,
                symbol,
                conn_id
            );
        }
        tokio_tungstenite::tungstenite::Error::Tls(tls_err) => {
            log::debug!(
                "WebSocket TLS error detail: {:?} (stream={}, symbol={}, conn={})",
                tls_err,
                stream,
                symbol,
                conn_id
            );
        }
        _ => {
            log::debug!(
                "WebSocket error detail: {:?} (stream={}, symbol={}, conn={})",
                err,
                stream,
                symbol,
                conn_id
            );
        }
    }
}

pub(super) async fn connect_ws(
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
        headers.insert("User-Agent", HeaderValue::from_static("Mozilla/5.0"));
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

// Extended sends SNAPSHOT on (re)connect and DELTA frames in between.
// Treating every frame as a full snapshot wipes the book each delta,
// leaving bid/ask empty. We rebuild on SNAPSHOT and apply level-by-level
// (qty==0 → remove, else set) on DELTA.
pub(super) fn apply_orderbook_frame(
    msg_type: &str,
    update: StreamOrderbookUpdate,
    publish_ts_ms: i64,
    entry: &mut OrderBookCacheEntry,
) {
    if msg_type == "SNAPSHOT" {
        entry.bids.clear();
        entry.asks.clear();
    }
    for level in update.bid {
        let absolute = level.cumulative.unwrap_or(level.qty);
        if absolute.is_zero() {
            entry.bids.remove(&level.price);
        } else {
            entry.bids.insert(level.price, absolute);
        }
    }
    for level in update.ask {
        let absolute = level.cumulative.unwrap_or(level.qty);
        if absolute.is_zero() {
            entry.asks.remove(&level.price);
        } else {
            entry.asks.insert(level.price, absolute);
        }
    }
    entry.updated_at = Instant::now();
    entry.last_publish_ts_ms = Some(publish_ts_ms);
}

pub(super) async fn stream_orderbooks(
    url: &str,
    symbol: &str,
    order_book_cache: &Arc<RwLock<HashMap<String, OrderBookCacheEntry>>>,
) -> Result<(), DexError> {
    let conn_id = next_ws_conn_id();
    let mut ws = match connect_ws(url, None).await {
        Ok(ws) => ws,
        Err(err) => {
            // Don't wipe the cache on connect failure — the existing entry
            // (if any) will age out of `get_cached_order_book` via the 5 s
            // `ORDERBOOK_STALE_AFTER` gate. Wiping eagerly just forces
            // callers to see "unavailable" during the reconnect window
            // even though the spawn loop is back up within ~2 s.
            return Err(DexError::Other(format!(
                "ws connect error: {err} (stream=orderbook symbol={symbol} url={url} conn={conn_id})"
            )));
        }
    };
    let mut ws_state = WsStreamState::new(conn_id);
    log::debug!(
        "WebSocket connected ({})",
        ws_state.context("orderbook", Some(symbol), url)
    );
    let result = loop {
        let message = match ws.next().await {
            Some(Ok(message)) => {
                ws_state.on_message();
                message
            }
            Some(Err(err)) => {
                log_ws_error_detail("orderbook", Some(symbol), ws_state.conn_id, &err);
                break Err(DexError::Other(format!(
                    "ws error: {err} ({})",
                    ws_state.context("orderbook", Some(symbol), url)
                )));
            }
            None => break Ok(()),
        };
        match message {
            tokio_tungstenite::tungstenite::Message::Ping(payload) => {
                ws_state.on_ping();
                if let Err(err) = ws
                    .send(tokio_tungstenite::tungstenite::Message::Pong(payload))
                    .await
                {
                    log_ws_error_detail("orderbook", Some(symbol), ws_state.conn_id, &err);
                    break Err(DexError::Other(format!(
                        "ws error: {err} ({})",
                        ws_state.context("orderbook", Some(symbol), url)
                    )));
                }
                ws_state.on_pong();
                continue;
            }
            tokio_tungstenite::tungstenite::Message::Pong(_) => {
                ws_state.on_pong();
                continue;
            }
            tokio_tungstenite::tungstenite::Message::Close(frame) => {
                log::debug!(
                    "WebSocket close frame received: {:?} ({})",
                    frame,
                    ws_state.context("orderbook", Some(symbol), url)
                );
                break Ok(());
            }
            _ => {}
        }
        if !message.is_text() {
            continue;
        }
        let payload: WrappedStreamResponse<StreamOrderbookUpdate> =
            match serde_json::from_str(message.to_text().unwrap_or("")) {
                Ok(payload) => payload,
                Err(err) => break Err(DexError::Other(format!("orderbook parse error: {err}"))),
            };
        let Some(update) = payload.data else {
            continue;
        };
        let msg_type = payload.msg_type.as_deref().unwrap_or("").to_string();
        let mut cache = order_book_cache.write().await;
        let entry = cache
            .entry(symbol.to_string())
            .or_insert_with(OrderBookCacheEntry::default);
        apply_orderbook_frame(&msg_type, update, payload.ts, entry);
    };
    log::debug!(
        "WebSocket stream ended ({})",
        ws_state.context("orderbook", Some(symbol), url)
    );
    // Leave the cached book intact across stream restarts. Extended closes
    // idle (and even active) connections periodically; spawn_ws_tasks
    // reconnects after a 2 s sleep and the new SNAPSHOT lands ~100 ms
    // later. Keeping the cache lets get_cached_order_book's staleness
    // gate (ORDERBOOK_STALE_AFTER = 5 s) decide when callers should see
    // "unavailable" — same behaviour as today for real outages, silent
    // for the common transient-reconnect case. See bot-strategy#141.
    result
}

pub(super) async fn fallback_price(
    last_trades: &Arc<RwLock<HashMap<String, Vec<LastTrade>>>>,
    symbol: &str,
    market: &MarketModel,
) -> Result<Decimal, DexError> {
    // Last resort when we cannot read either a live WS book or a REST book:
    // use the latest cached trade if we have one, else the startup-time
    // `last_price` from /info/markets. Both are stale in different ways but
    // beat failing the call outright — callers always expect a number.
    let trades = last_trades.read().await;
    if let Some(items) = trades.get(symbol).and_then(|v| v.last()) {
        Ok(items.price)
    } else if market.market_stats.last_price > Decimal::ZERO {
        Ok(market.market_stats.last_price)
    } else {
        Err(DexError::Other(
            "ticker unavailable: no orderbook, trade cache, or market stats".to_string(),
        ))
    }
}

pub(super) async fn stream_trades(
    url: &str,
    symbol: &str,
    last_trades: &Arc<RwLock<HashMap<String, Vec<LastTrade>>>>,
) -> Result<(), DexError> {
    let conn_id = next_ws_conn_id();
    let mut ws = match connect_ws(url, None).await {
        Ok(ws) => ws,
        Err(err) => {
            return Err(DexError::Other(format!(
                "ws connect error: {err} (stream=trades symbol={symbol} url={url} conn={conn_id})"
            )));
        }
    };
    let mut ws_state = WsStreamState::new(conn_id);
    log::debug!(
        "WebSocket connected ({})",
        ws_state.context("trades", Some(symbol), url)
    );
    while let Some(message) = ws.next().await {
        let message = match message {
            Ok(message) => {
                ws_state.on_message();
                message
            }
            Err(err) => {
                log_ws_error_detail("trades", Some(symbol), ws_state.conn_id, &err);
                return Err(DexError::Other(format!(
                    "ws error: {err} ({})",
                    ws_state.context("trades", Some(symbol), url)
                )));
            }
        };
        match message {
            tokio_tungstenite::tungstenite::Message::Ping(payload) => {
                ws_state.on_ping();
                if let Err(err) = ws
                    .send(tokio_tungstenite::tungstenite::Message::Pong(payload))
                    .await
                {
                    log_ws_error_detail("trades", Some(symbol), ws_state.conn_id, &err);
                    return Err(DexError::Other(format!(
                        "ws error: {err} ({})",
                        ws_state.context("trades", Some(symbol), url)
                    )));
                }
                ws_state.on_pong();
                continue;
            }
            tokio_tungstenite::tungstenite::Message::Pong(_) => {
                ws_state.on_pong();
                continue;
            }
            tokio_tungstenite::tungstenite::Message::Close(frame) => {
                log::debug!(
                    "WebSocket close frame received: {:?} ({})",
                    frame,
                    ws_state.context("trades", Some(symbol), url)
                );
                break;
            }
            _ => {}
        }
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
    log::debug!(
        "WebSocket stream ended ({})",
        ws_state.context("trades", Some(symbol), url)
    );
    Ok(())
}

pub(super) async fn stream_account(
    url: &str,
    api_key: &str,
    balance_cache: &Arc<RwLock<Option<BalanceResponse>>>,
    open_orders_cache: &Arc<RwLock<HashMap<String, Vec<OpenOrder>>>>,
    order_id_map: &Arc<RwLock<HashMap<i64, String>>>,
    filled_orders: &Arc<RwLock<HashMap<String, Vec<FilledOrder>>>>,
    positions_cache: &Arc<RwLock<Option<Vec<PositionSnapshot>>>>,
) -> Result<(), DexError> {
    let conn_id = next_ws_conn_id();
    let mut ws = match connect_ws(url, Some(api_key)).await {
        Ok(ws) => ws,
        Err(err) => {
            if err.to_string().contains("401 Unauthorized") {
                return Err(DexError::ApiKeyRegistrationRequired);
            }
            return Err(DexError::Other(format!(
                "ws connect error: {err} (stream=account url={url} conn={conn_id})"
            )));
        }
    };
    let mut ws_state = WsStreamState::new(conn_id);
    log::debug!(
        "WebSocket connected ({})",
        ws_state.context("account", None, url)
    );
    let mut logged_once = false;
    while let Some(message) = ws.next().await {
        let message = match message {
            Ok(message) => {
                ws_state.on_message();
                message
            }
            Err(err) => {
                log_ws_error_detail("account", None, ws_state.conn_id, &err);
                return Err(DexError::Other(format!(
                    "ws error: {err} ({})",
                    ws_state.context("account", None, url)
                )));
            }
        };
        match message {
            tokio_tungstenite::tungstenite::Message::Ping(payload) => {
                ws_state.on_ping();
                if let Err(err) = ws
                    .send(tokio_tungstenite::tungstenite::Message::Pong(payload))
                    .await
                {
                    log_ws_error_detail("account", None, ws_state.conn_id, &err);
                    return Err(DexError::Other(format!(
                        "ws error: {err} ({})",
                        ws_state.context("account", None, url)
                    )));
                }
                ws_state.on_pong();
                continue;
            }
            tokio_tungstenite::tungstenite::Message::Pong(_) => {
                ws_state.on_pong();
                continue;
            }
            tokio_tungstenite::tungstenite::Message::Close(frame) => {
                log::debug!(
                    "WebSocket close frame received: {:?} ({})",
                    frame,
                    ws_state.context("account", None, url)
                );
                break;
            }
            _ => {}
        }
        if !message.is_text() {
            continue;
        }
        let payload: WrappedStreamResponse<AccountStreamData> =
            serde_json::from_str(message.to_text().unwrap_or(""))
                .map_err(|e| DexError::Other(format!("account parse error: {e}")))?;
        if let Some(data) = payload.data {
            if !logged_once {
                let orders_len = data.orders.as_ref().map(|v| v.len()).unwrap_or(0);
                let trades_len = data.trades.as_ref().map(|v| v.len()).unwrap_or(0);
                let has_balance = data.balance.is_some();
                log::debug!(
                    "account stream update received: balance={}, orders={}, trades={}",
                    has_balance,
                    orders_len,
                    trades_len
                );
                logged_once = true;
            }
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
                {
                    let mut map = order_id_map.write().await;
                    for order in orders.iter() {
                        map.insert(order.id, order.external_id.clone());
                    }
                }
                let mut cache = open_orders_cache.write().await;
                cache.clear();
                for order in orders {
                    let entry = cache
                        .entry(normalize_symbol(&order.market))
                        .or_default();
                    entry.push(OpenOrder {
                        order_id: order.external_id.clone(),
                        symbol: order.market.clone(),
                        side: if order.side == "BUY" {
                            OrderSide::Long
                        } else {
                            OrderSide::Short
                        },
                        size: order.qty,
                        price: order.price.unwrap_or(Decimal::ZERO),
                        status: order.status.clone(),
                    });
                }
            }

            if let Some(trades) = data.trades {
                let mut mapped_trades = Vec::new();
                {
                    let map = order_id_map.read().await;
                    for trade in trades {
                        let order_id = map
                            .get(&trade.order_id)
                            .cloned()
                            .unwrap_or_else(|| trade.order_id.to_string());
                        mapped_trades.push((trade, order_id));
                    }
                }
                let mut cache = filled_orders.write().await;
                for (trade, order_id) in mapped_trades {
                    let entry = cache
                        .entry(normalize_symbol(&trade.market))
                        .or_default();
                    entry.push(FilledOrder {
                        order_id,
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
                        filled_ts_ms: Some(trade.created_time),
                    });
                }
            }

            if let Some(positions) = data.positions {
                let mut positions_map: HashMap<String, PositionSnapshot> = HashMap::new();
                for position in positions {
                    if let Some(snapshot) = position_snapshot_from_model(position) {
                        positions_map.insert(snapshot.symbol.clone(), snapshot);
                    }
                }
                let mut cache = positions_cache.write().await;
                *cache = Some(positions_map.into_values().collect());
            }
        }
    }
    log::debug!(
        "WebSocket stream ended ({})",
        ws_state.context("account", None, url)
    );
    Ok(())
}

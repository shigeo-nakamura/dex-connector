use super::models::WsBookEnvelope;
use super::{now_ms, BookCache, TrackedMarket};
use crate::ws_reconnect::WsReconnectPolicy;
use crate::{DexError, PriceUpdate};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;

/// Maximum interval between any inbound WS frame before the stream is
/// treated as silently stalled and force-reconnected via the spawn-loop.
/// Without this, a server or network that goes half-open without a
/// close/error frame leaves `ws.next().await` pending forever, so
/// `subscribe_price_updates` silently stops producing prices even though
/// `websocket_loop`'s reconnect policy never gets a chance to run
/// (bot-strategy#749 review). Mirrors the Extended connector's stall
/// watchdog (`extended_connector::ws::WS_STALL_TIMEOUT`).
const WS_STALL_TIMEOUT: Duration = Duration::from_secs(60);

pub(super) async fn websocket_loop(
    websocket_url: String,
    tracked_markets: Vec<TrackedMarket>,
    books: BookCache,
    price_update_tx: broadcast::Sender<PriceUpdate>,
) {
    let reconnect_policy = WsReconnectPolicy::lighter();
    let mut attempt = 0u32;

    loop {
        let connected_at = Instant::now();
        let result = run_ws_once(&websocket_url, &tracked_markets, &books, &price_update_tx).await;

        clear_books(&books).await;
        let elapsed = connected_at.elapsed().as_secs();
        if reconnect_policy.should_reset_attempt(elapsed) {
            attempt = 0;
        } else {
            attempt = attempt.saturating_add(1);
        }

        match result {
            Ok(()) => log::warn!(
                "[arcus] WebSocket ended without an error; reconnecting (attempt={attempt})"
            ),
            Err(err) => log::warn!(
                "[arcus] WebSocket disconnected: {err}; reconnecting (attempt={attempt})"
            ),
        }
        reconnect_policy.wait(attempt).await;
    }
}

pub(super) async fn run_ws_once(
    websocket_url: &str,
    tracked_markets: &[TrackedMarket],
    books: &BookCache,
    price_update_tx: &broadcast::Sender<PriceUpdate>,
) -> Result<(), DexError> {
    run_ws_once_with_timeout(
        websocket_url,
        tracked_markets,
        books,
        price_update_tx,
        WS_STALL_TIMEOUT,
    )
    .await
}

async fn run_ws_once_with_timeout(
    websocket_url: &str,
    tracked_markets: &[TrackedMarket],
    books: &BookCache,
    price_update_tx: &broadcast::Sender<PriceUpdate>,
    stall_timeout: Duration,
) -> Result<(), DexError> {
    let (mut ws, _) = tokio_tungstenite::connect_async(websocket_url)
        .await
        .map_err(|err| DexError::WebSocketError(format!("Arcus connect failed: {err}")))?;

    for tracked in tracked_markets {
        let subscribe = json!({
            "type": "subscribe",
            "channel": "l2OrderbookUpdates",
            "id": tracked.market,
            "nLevels": 100,
            "snapshot": true
        });
        ws.send(Message::Text(subscribe.to_string()))
            .await
            .map_err(|err| {
                DexError::WebSocketError(format!(
                    "Arcus subscribe failed market={}: {err}",
                    tracked.market
                ))
            })?;
    }

    // Track elapsed time since the last *order-book* message rather than
    // since any inbound frame. A connection can stay alive indefinitely on
    // Ping/Pong keepalives alone while `l2OrderbookUpdates` has silently
    // stopped; resetting the watchdog on every frame would mask exactly the
    // stall this timeout exists to catch (bot-strategy#749 review).
    let mut last_book_msg = Instant::now();
    loop {
        let remaining = stall_timeout.saturating_sub(last_book_msg.elapsed());
        let message = match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(message)) => message,
            Ok(None) => break,
            Err(_) => {
                return Err(DexError::WebSocketError(format!(
                    "Arcus WebSocket stalled: no order-book message within {}s",
                    stall_timeout.as_secs()
                )));
            }
        };
        match message.map_err(|err| DexError::WebSocketError(err.to_string()))? {
            Message::Text(text) => {
                if handle_text(&text, tracked_markets, books, price_update_tx).await? {
                    last_book_msg = Instant::now();
                }
            }
            Message::Ping(payload) => {
                ws.send(Message::Pong(payload))
                    .await
                    .map_err(|err| DexError::WebSocketError(err.to_string()))?;
            }
            Message::Close(frame) => {
                return Err(DexError::WebSocketError(format!(
                    "Arcus server closed connection: {frame:?}"
                )));
            }
            Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }

    Err(DexError::WebSocketError(
        "Arcus WebSocket stream ended".to_string(),
    ))
}

/// Returns `Ok(true)` when `text` was an `l2Orderbook*` channel message,
/// so the caller can distinguish a live order-book feed from other frames
/// (errors, unrelated channels) for stall-watchdog purposes.
async fn handle_text(
    text: &str,
    tracked_markets: &[TrackedMarket],
    books: &BookCache,
    price_update_tx: &broadcast::Sender<PriceUpdate>,
) -> Result<bool, DexError> {
    let value: Value = serde_json::from_str(text)?;
    let kind = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if kind == "error" {
        return Err(DexError::WebSocketError(format!(
            "Arcus WebSocket error frame: {text}"
        )));
    }

    let channel = value
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !channel.starts_with("l2Orderbook") {
        return Ok(false);
    }

    let envelope: WsBookEnvelope = serde_json::from_value(value)?;
    let Some(contents) = envelope.contents else {
        return Ok(true);
    };
    let market = super::normalize_market(&envelope.id);
    let output_symbol = tracked_markets
        .iter()
        .find(|tracked| tracked.market == market)
        .map(|tracked| tracked.output_symbol.clone())
        .unwrap_or_else(|| market.clone());
    let received_ms = now_ms();

    let top = {
        let mut cache = books.write().await;
        let state = cache.entry(market.clone()).or_default();
        match envelope.kind.as_str() {
            "subscribed" => state.replace_from_ws(&market, &contents, received_ms)?,
            "channel_data" => state.apply_delta(&market, &contents, received_ms)?,
            _ => None,
        }
    };

    if let Some(top) = top {
        let _ = price_update_tx.send(PriceUpdate {
            symbol: output_symbol,
            mid_price: top.mid,
            best_bid: top.best_bid,
            best_ask: top.best_ask,
            timestamp: top.timestamp_ms,
        });
    }
    Ok(true)
}

async fn clear_books(books: &BookCache) {
    for state in books.write().await.values_mut() {
        state.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arcus_connector::book::BookState;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio::sync::RwLock;
    use tokio_tungstenite::accept_async;

    #[tokio::test]
    async fn reconnect_snapshot_recovers_after_sequence_gap() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Arcus fake WS");
        let url = format!("ws://{}", listener.local_addr().expect("listener address"));

        let server = tokio::spawn(async move {
            for frames in [
                vec![
                    json!({
                        "type": "subscribed",
                        "channel": "l2OrderbookUpdates",
                        "id": "BTC-USD",
                        "contents": {
                            "bids": [["99", "1"]],
                            "asks": [["101", "1"]],
                            "lastSequenceId": 10,
                            "globalSequenceId": 100,
                            "timestamp": 1_784_880_000_000_000u64
                        }
                    }),
                    json!({
                        "type": "channel_data",
                        "channel": "l2OrderbookUpdates",
                        "id": "BTC-USD",
                        "contents": {
                            "bids": [["100", "1"]],
                            "asks": [],
                            "lastSequenceId": 12,
                            "globalSequenceId": 102
                        }
                    }),
                ],
                vec![
                    json!({
                        "type": "subscribed",
                        "channel": "l2OrderbookUpdates",
                        "id": "BTC-USD",
                        "contents": {
                            "bids": [["100", "2"]],
                            "asks": [["102", "3"]],
                            "lastSequenceId": 20,
                            "globalSequenceId": 200,
                            "timestamp": 1_784_880_001_000_000u64
                        }
                    }),
                    json!({
                        "type": "channel_data",
                        "channel": "l2OrderbookUpdates",
                        "id": "BTC-USD",
                        "contents": {
                            "bids": [["101", "4"]],
                            "asks": [],
                            "lastSequenceId": 21,
                            "globalSequenceId": 201
                        }
                    }),
                ],
            ] {
                let (stream, _) = listener.accept().await.expect("accept Arcus client");
                let mut ws = accept_async(stream).await.expect("upgrade Arcus WS");
                let subscription = ws
                    .next()
                    .await
                    .expect("subscription frame")
                    .expect("valid subscription frame")
                    .into_text()
                    .expect("text subscription");
                let subscription: Value =
                    serde_json::from_str(&subscription).expect("subscription JSON");
                assert_eq!(subscription["channel"], "l2OrderbookUpdates");
                assert_eq!(subscription["id"], "BTC-USD");

                for frame in frames {
                    ws.send(Message::Text(frame.to_string()))
                        .await
                        .expect("send Arcus fake frame");
                }
                let _ = ws.send(Message::Close(None)).await;
            }
        });

        let tracked = vec![TrackedMarket {
            market: "BTC-USD".to_string(),
            output_symbol: "BTC".to_string(),
        }];
        let books: BookCache = Arc::new(RwLock::new(HashMap::<String, BookState>::new()));
        let (tx, mut rx) = broadcast::channel(16);

        let first = run_ws_once(&url, &tracked, &books, &tx).await;
        assert!(first
            .expect_err("sequence gap must restart the connection")
            .to_string()
            .contains("sequence gap"));
        assert!(!books
            .read()
            .await
            .get("BTC-USD")
            .expect("book state")
            .is_ready());

        clear_books(&books).await;
        let _ = run_ws_once(&url, &tracked, &books, &tx).await;
        server.await.expect("Arcus fake server");

        let state = books.read().await;
        let book = state.get("BTC-USD").expect("recovered BTC book");
        assert!(book.is_ready());
        assert_eq!(book.last_sequence_id(), 21);
        let top = book.top().expect("recovered top");
        assert_eq!(top.best_bid.to_string(), "101");
        assert_eq!(top.best_ask.to_string(), "102");

        let mut last = rx.recv().await.expect("first price update");
        while let Ok(next) = rx.try_recv() {
            last = next;
        }
        assert_eq!(last.symbol, "BTC");
        assert_eq!(last.best_bid.to_string(), "101");
        assert_eq!(last.best_ask.to_string(), "102");
    }

    #[tokio::test(start_paused = true)]
    async fn stall_timeout_fires_on_silent_stream() {
        // Reproduces the half-open-connection pattern flagged in the
        // bot-strategy#749 review: a stream that never yields anything
        // (server stops sending without a close/error frame). Without a
        // stall watchdog, `ws.next().await` would hang forever and
        // `websocket_loop`'s reconnect path would never run.
        let mut silent = futures::stream::pending::<Result<Message, ()>>();
        let result = tokio::time::timeout(WS_STALL_TIMEOUT, silent.next()).await;
        assert!(
            result.is_err(),
            "expected stall timeout to fire on a silent stream, got {:?}",
            result.is_ok()
        );
    }

    #[tokio::test]
    async fn stall_timeout_fires_despite_ping_only_keepalive() {
        // PR #41 review: a connection that stays technically alive via
        // Ping/Pong keepalives while `l2OrderbookUpdates` has silently
        // stopped must still be treated as stalled. Resetting the watchdog
        // on every inbound frame (instead of only order-book messages)
        // would mask this and never reconnect.
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake Arcus ping-only server");
        let url = format!("ws://{}", listener.local_addr().expect("listener addr"));

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept client");
            let mut ws = accept_async(stream).await.expect("upgrade WS");
            // Consume the subscribe frame, then send only Pings — never an
            // order-book message — until the client gives up.
            let _ = ws.next().await;
            loop {
                if ws.send(Message::Ping(vec![])).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });

        let tracked = vec![TrackedMarket {
            market: "BTC-USD".to_string(),
            output_symbol: "BTC".to_string(),
        }];
        let books: BookCache = Arc::new(RwLock::new(HashMap::<String, BookState>::new()));
        let (tx, _rx) = broadcast::channel(16);

        let result =
            run_ws_once_with_timeout(&url, &tracked, &books, &tx, Duration::from_millis(150)).await;

        assert!(
            result
                .expect_err("ping-only keepalive must not suppress the stall watchdog")
                .to_string()
                .contains("stalled"),
            "expected a stall error"
        );
        server.abort();
    }
}

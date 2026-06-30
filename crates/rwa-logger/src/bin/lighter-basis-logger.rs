//! Lighter spot/perp basis logger (bot-strategy#573 / #663).
//!
//! Consumes Lighter public WebSocket `order_book/{id}` and `market_stats/{id}`
//! streams and writes spot/perp basis JSONL without using Lighter REST.

use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;

use rwa_logger::resolve_poll_secs;

const DEFAULT_LIGHTER_WS_URL: &str = "wss://mainnet.zklighter.elliot.ai/stream";
const DEFAULT_ORDERBOOK_LIMIT: u64 = 100;
const MAX_ORDERBOOK_LIMIT: u64 = 250;
const DEFAULT_DEPTH_BPS: f64 = 25.0;
/// Lighter drops a WS client that sends no frame within ~2 minutes. The basis
/// loop only replies to server pings otherwise, so send a proactive control Ping
/// on this cadence (mirrors the live connector's 20s heartbeat).
const LIGHTER_WS_PING_SECS: u64 = 20;
/// If no inbound frame (data, server ping, or pong to our keepalive) arrives
/// within this window the socket is treated as dead and reconnected. A healthy
/// socket answers our 20s ping even when the market is quiet, so this fires only
/// on a genuinely dead connection — not on a legitimately idle order book (which
/// Lighter leaves un-updated until it actually changes).
const LIGHTER_WS_IDLE_TIMEOUT_SECS: u64 = 60;

#[derive(Clone, Debug, PartialEq, Eq)]
struct BasisMarket {
    label: String,
    spot_market_id: u64,
    perp_market_id: u64,
}

#[derive(Clone, Debug, PartialEq)]
struct BookSummary {
    bid: f64,
    ask: f64,
    mid: f64,
    spread_bps: f64,
    depth_bps: f64,
    depth_usd: f64,
}

#[derive(Clone, Debug)]
struct BookLevel {
    price: f64,
    size: f64,
}

#[derive(Clone, Debug, Default)]
struct BookState {
    bids: HashMap<String, BookLevel>,
    asks: HashMap<String, BookLevel>,
    updated_at: Option<Instant>,
    /// Last applied matching-engine `nonce`. Used to detect dropped delta frames:
    /// a continuous update has `begin_nonce == last_nonce`. (Lighter's `offset`
    /// is API-server tied and not contiguous, so it cannot be used here.)
    last_nonce: Option<u64>,
}

impl BookState {
    /// Re-baseline from a fresh `subscribed/order_book` snapshot.
    fn reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.updated_at = None;
        self.last_nonce = None;
    }

    /// True when an incremental `update/order_book` does not continue from the
    /// last applied nonce, i.e. a frame was dropped and the book is now stale.
    /// Returns false before a baseline exists or when nonce fields are absent.
    fn is_gap(&self, order_book: &Value) -> bool {
        match (self.last_nonce, ob_u64(order_book, "begin_nonce")) {
            (Some(last), Some(begin)) => begin != last,
            _ => false,
        }
    }

    fn apply(&mut self, order_book: &Value) {
        apply_side(&mut self.bids, order_book.get("bids"));
        apply_side(&mut self.asks, order_book.get("asks"));
        self.updated_at = Some(Instant::now());
        if let Some(nonce) = ob_u64(order_book, "nonce") {
            self.last_nonce = Some(nonce);
        }
    }
}

fn ob_u64(order_book: &Value, key: &str) -> Option<u64> {
    match order_book.get(key) {
        Some(Value::Number(n)) => n.as_u64(),
        Some(Value::String(s)) => s.parse::<u64>().ok(),
        _ => None,
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let markets_spec = match std::env::var("LIGHTER_BASIS_MARKETS") {
        Ok(v) => v,
        Err(_) => {
            log::error!(
                "LIGHTER_BASIS_MARKETS is required (label:spot_market_id:perp_market_id,...)"
            );
            std::process::exit(2);
        }
    };
    let markets = match parse_basis_markets(&markets_spec) {
        Ok(v) => v,
        Err(e) => {
            log::error!("failed to parse LIGHTER_BASIS_MARKETS: {e}");
            std::process::exit(2);
        }
    };
    let log_dir = std::env::var("LIGHTER_BASIS_LOG_DIR").unwrap_or_else(|_| ".".to_string());
    let ws_url =
        std::env::var("LIGHTER_WS_URL").unwrap_or_else(|_| DEFAULT_LIGHTER_WS_URL.to_string());
    let poll_secs = resolve_poll_secs(std::env::var("LIGHTER_BASIS_POLL_SECS").ok().as_deref());
    let orderbook_limit = resolve_orderbook_limit(
        std::env::var("LIGHTER_BASIS_ORDERBOOK_LIMIT")
            .ok()
            .as_deref(),
    );
    let depth_bps = resolve_depth_bps(std::env::var("LIGHTER_BASIS_DEPTH_BPS").ok().as_deref());

    log::info!(
        "lighter-basis-logger starting: {} market pair(s), poll={}s, limit={}, depth={}bps, ws={}, dir={}",
        markets.len(), poll_secs, orderbook_limit, depth_bps, ws_url, log_dir
    );

    let running = Arc::new(AtomicBool::new(true));
    {
        let r = running.clone();
        let _ = ctrlc::set_handler(move || {
            log::info!("shutdown signal received, stopping after current tick");
            r.store(false, Ordering::SeqCst);
        });
    }

    while running.load(Ordering::SeqCst) {
        if let Err(e) = run_ws_loop(
            &ws_url,
            &log_dir,
            &markets,
            poll_secs,
            orderbook_limit,
            depth_bps,
            Arc::clone(&running),
        )
        .await
        {
            if running.load(Ordering::SeqCst) {
                log::warn!("lighter basis WS loop ended: {e}; reconnecting in 2s");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }

    log::info!("lighter-basis-logger stopped");
}

async fn run_ws_loop(
    ws_url: &str,
    log_dir: &str,
    markets: &[BasisMarket],
    poll_secs: u64,
    orderbook_limit: u64,
    depth_bps: f64,
    running: Arc<AtomicBool>,
) -> Result<(), String> {
    let (mut ws, _resp) = tokio_tungstenite::connect_async(ws_url)
        .await
        .map_err(|e| format!("connect failed: {e}"))?;
    log::info!("Lighter basis WS connected: {ws_url}");

    let subscriptions = subscriptions_for(markets);
    // Subscribe immediately after the handshake. The production connector
    // (src/lighter_connector/ws.rs) and the Lighter WS reference both send
    // subscribe frames directly; gating on an undocumented `type:"connected"`
    // welcome would leave subscriptions unsent if the server never emits one.
    for sub in &subscriptions {
        ws.send(Message::Text(sub.to_string()))
            .await
            .map_err(|e| format!("subscribe failed: {e}"))?;
    }
    log::info!(
        "sent {} Lighter basis WS subscriptions",
        subscriptions.len()
    );
    let mut books: HashMap<u64, BookState> = HashMap::new();
    let mut funding: HashMap<u64, f64> = HashMap::new();
    let mut last_write = Instant::now()
        .checked_sub(Duration::from_secs(poll_secs))
        .unwrap_or_else(Instant::now);
    let mut last_ping = Instant::now();
    let idle_timeout = Duration::from_secs(LIGHTER_WS_IDLE_TIMEOUT_SECS);
    // Tracks the last frame of ANY kind, including the pong to our keepalive.
    // This is deliberate: a legitimately quiet order book sends no data frames,
    // so data-silence is not a stall signal — only total socket silence (no pong
    // either) is. That avoids reconnecting a healthy but idle market (e.g. LIT).
    let mut last_inbound = Instant::now();

    while running.load(Ordering::SeqCst) {
        if last_write.elapsed() >= Duration::from_secs(poll_secs) {
            write_tick(
                log_dir,
                markets,
                &books,
                orderbook_limit,
                depth_bps,
                &funding,
            );
            last_write = Instant::now();
        }

        if last_ping.elapsed() >= Duration::from_secs(LIGHTER_WS_PING_SECS) {
            // Proactive client keepalive (the 1s read timeout below guarantees we
            // reach this at least once per second to honour the cadence).
            ws.send(Message::Ping(Vec::new()))
                .await
                .map_err(|e| format!("client ping failed: {e}"))?;
            last_ping = Instant::now();
        }

        // Reconnect only when the socket goes fully silent (no data AND no pong),
        // which means it is dead; a quiet-but-live book still pongs.
        if last_inbound.elapsed() > idle_timeout {
            return Err(format!(
                "no inbound frame for {}s; reconnecting",
                last_inbound.elapsed().as_secs()
            ));
        }

        let next = match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(next) => next,
            Err(_) => continue,
        };
        let Some(next) = next else {
            return Err("stream closed".to_string());
        };
        let msg = next.map_err(|e| format!("ws read failed: {e}"))?;
        last_inbound = Instant::now();
        match msg {
            Message::Ping(payload) => {
                ws.send(Message::Pong(payload))
                    .await
                    .map_err(|e| format!("pong failed: {e}"))?;
            }
            Message::Pong(_) => {}
            Message::Close(frame) => return Err(format!("close frame: {frame:?}")),
            Message::Text(text) => {
                let value: Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(e) => {
                        log::warn!("non-JSON WS text ignored: {e}");
                        continue;
                    }
                };
                let msg_type = value.get("type").and_then(|v| v.as_str()).unwrap_or("");
                if msg_type == "connected" {
                    // Optional welcome frame; subscriptions were already sent after
                    // the handshake, so nothing to do here.
                    log::debug!("Lighter basis WS connected frame");
                    continue;
                }
                if msg_type == "ping" {
                    ws.send(Message::Text(json!({"type": "pong"}).to_string()))
                        .await
                        .map_err(|e| format!("app pong failed: {e}"))?;
                    continue;
                }
                if let Some(err) = value.get("error") {
                    log::warn!("Lighter basis WS error frame: {err}");
                    continue;
                }
                match msg_type {
                    "subscribed/order_book" => {
                        // Full snapshot: replace the book and re-baseline the nonce.
                        if let (Some(market_id), Some(order_book)) =
                            (channel_market_id(&value), value.get("order_book"))
                        {
                            let state = books.entry(market_id).or_default();
                            state.reset();
                            state.apply(order_book);
                        }
                    }
                    "update/order_book" => {
                        if let (Some(market_id), Some(order_book)) =
                            (channel_market_id(&value), value.get("order_book"))
                        {
                            let state = books.entry(market_id).or_default();
                            if state.is_gap(order_book) {
                                // A dropped delta leaves stale price levels in the
                                // book; reconnect so the next subscription delivers
                                // a fresh snapshot rather than emitting corrupt rows.
                                return Err(format!(
                                    "order_book nonce gap on market {market_id}; reconnecting for snapshot"
                                ));
                            }
                            state.apply(order_book);
                        }
                    }
                    "subscribed/market_stats" | "update/market_stats" => {
                        if let (Some(market_id), Some(rate)) =
                            (channel_market_id(&value), market_stats_funding_rate(&value))
                        {
                            funding.insert(market_id, rate);
                        }
                    }
                    _ => log::trace!("ignored Lighter basis WS type={msg_type}"),
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn subscriptions_for(markets: &[BasisMarket]) -> Vec<Value> {
    let mut book_ids = HashSet::new();
    let mut stats_ids = HashSet::new();
    for market in markets {
        book_ids.insert(market.spot_market_id);
        book_ids.insert(market.perp_market_id);
        stats_ids.insert(market.perp_market_id);
    }
    let mut book_ids: Vec<_> = book_ids.into_iter().collect();
    book_ids.sort_unstable();
    let mut stats_ids: Vec<_> = stats_ids.into_iter().collect();
    stats_ids.sort_unstable();

    let mut out = Vec::new();
    for id in book_ids {
        out.push(json!({"type": "subscribe", "channel": format!("order_book/{id}")}));
    }
    for id in stats_ids {
        out.push(json!({"type": "subscribe", "channel": format!("market_stats/{id}")}));
    }
    out
}

fn parse_basis_markets(spec: &str) -> Result<Vec<BasisMarket>, String> {
    let mut out = Vec::new();
    for entry in spec.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let parts: Vec<_> = entry.split(':').map(str::trim).collect();
        if parts.len() != 3 {
            return Err(format!(
                "entry '{entry}' is not label:spot_market_id:perp_market_id"
            ));
        }
        if parts.iter().any(|p| p.is_empty()) {
            return Err(format!("entry '{entry}' has an empty field"));
        }
        let spot_market_id = parts[1]
            .parse::<u64>()
            .map_err(|_| format!("entry '{entry}' has invalid spot market id"))?;
        let perp_market_id = parts[2]
            .parse::<u64>()
            .map_err(|_| format!("entry '{entry}' has invalid perp market id"))?;
        out.push(BasisMarket {
            label: parts[0].to_string(),
            spot_market_id,
            perp_market_id,
        });
    }
    if out.is_empty() {
        return Err("parsed to zero entries".to_string());
    }
    Ok(out)
}

fn resolve_orderbook_limit(raw: Option<&str>) -> u64 {
    let parsed = raw
        .and_then(|s| match s.trim().parse::<u64>() {
            Ok(n) => Some(n),
            Err(_) => {
                log::warn!(
                    "LIGHTER_BASIS_ORDERBOOK_LIMIT='{s}' is invalid, using {DEFAULT_ORDERBOOK_LIMIT}"
                );
                None
            }
        })
        .unwrap_or(DEFAULT_ORDERBOOK_LIMIT);
    parsed.clamp(1, MAX_ORDERBOOK_LIMIT)
}

/// Age in whole seconds of a leg's cached order book, or None if no snapshot has
/// arrived yet. Surfaced per leg so downstream can judge staleness itself — a
/// quiet book is still the latest valid book, so we never drop it from the row.
fn book_age_secs(state: &BookState, now: Instant) -> Option<u64> {
    state
        .updated_at
        .map(|t| now.saturating_duration_since(t).as_secs())
}

fn resolve_depth_bps(raw: Option<&str>) -> f64 {
    match raw {
        None => DEFAULT_DEPTH_BPS,
        Some(s) => match s.trim().parse::<f64>() {
            Ok(n) if n.is_finite() && n > 0.0 => n,
            _ => {
                log::warn!("LIGHTER_BASIS_DEPTH_BPS='{s}' is invalid, using {DEFAULT_DEPTH_BPS}");
                DEFAULT_DEPTH_BPS
            }
        },
    }
}

fn fnum(node: &Value, key: &str) -> Option<f64> {
    match node.get(key) {
        Some(Value::String(s)) if !s.is_empty() => s.parse::<f64>().ok(),
        Some(Value::Number(n)) => n.as_f64(),
        _ => None,
    }
}

fn channel_market_id(message: &Value) -> Option<u64> {
    message
        .get("channel")
        .and_then(|v| v.as_str())
        .and_then(|channel| channel.rsplit(['/', ':']).next())
        .and_then(|s| s.parse::<u64>().ok())
}

fn market_stats_funding_rate(message: &Value) -> Option<f64> {
    let raw = message
        .get("market_stats")
        .and_then(|s| s.get("funding_rate"))
        .and_then(value_as_f64)?;
    Some(raw / 100.0)
}

fn apply_side(dst: &mut HashMap<String, BookLevel>, raw: Option<&Value>) {
    let Some(rows) = raw.and_then(|v| v.as_array()) else {
        return;
    };
    for row in rows {
        let Some((key, price)) = row_price_key(row) else {
            continue;
        };
        let Some(size) = row_size(row) else {
            continue;
        };
        if !price.is_finite() || !size.is_finite() {
            continue;
        }
        if size <= 0.0 {
            dst.remove(&key);
        } else {
            dst.insert(key, BookLevel { price, size });
        }
    }
}

fn row_price_key(row: &Value) -> Option<(String, f64)> {
    match row {
        Value::Object(_) => row
            .get("price")
            .or_else(|| row.get("p"))
            .and_then(|v| match v {
                Value::String(s) if !s.is_empty() => {
                    s.parse::<f64>().ok().map(|price| (s.clone(), price))
                }
                Value::Number(n) => n.as_f64().map(|price| (n.to_string(), price)),
                _ => None,
            }),
        Value::Array(a) => a.first().and_then(|v| match v {
            Value::String(s) if !s.is_empty() => {
                s.parse::<f64>().ok().map(|price| (s.clone(), price))
            }
            Value::Number(n) => n.as_f64().map(|price| (n.to_string(), price)),
            _ => None,
        }),
        _ => None,
    }
}

fn row_size(row: &Value) -> Option<f64> {
    match row {
        Value::Object(_) => fnum(row, "size")
            .or_else(|| fnum(row, "remaining_base_amount"))
            .or_else(|| fnum(row, "amount"))
            .or_else(|| fnum(row, "q")),
        Value::Array(a) => a.get(1).and_then(value_as_f64),
        _ => None,
    }
}

fn value_as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) if !s.is_empty() => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn summarize_state(
    state: &BookState,
    depth_bps: f64,
    orderbook_limit: u64,
) -> Result<BookSummary, String> {
    let mut bids: Vec<_> = state.bids.values().collect();
    let mut asks: Vec<_> = state.asks.values().collect();
    bids.sort_by(|a, b| {
        b.price
            .partial_cmp(&a.price)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    asks.sort_by(|a, b| {
        a.price
            .partial_cmp(&b.price)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    bids.truncate(orderbook_limit as usize);
    asks.truncate(orderbook_limit as usize);

    let bid = bids
        .first()
        .map(|level| level.price)
        .filter(|p| p.is_finite() && *p > 0.0)
        .ok_or_else(|| "missing bid".to_string())?;
    let ask = asks
        .first()
        .map(|level| level.price)
        .filter(|p| p.is_finite() && *p > 0.0)
        .ok_or_else(|| "missing ask".to_string())?;
    if ask < bid {
        return Err(format!("crossed book bid={bid} ask={ask}"));
    }

    let mid = (bid + ask) / 2.0;
    let spread_bps = (ask - bid) / mid * 10_000.0;
    let lo = mid * (1.0 - depth_bps / 10_000.0);
    let hi = mid * (1.0 + depth_bps / 10_000.0);
    let mut depth_usd = 0.0;
    for level in bids {
        if level.price >= lo && level.size > 0.0 {
            depth_usd += level.price * level.size;
        }
    }
    for level in asks {
        if level.price <= hi && level.size > 0.0 {
            depth_usd += level.price * level.size;
        }
    }

    Ok(BookSummary {
        bid,
        ask,
        mid,
        spread_bps,
        depth_bps,
        depth_usd,
    })
}

fn book_json(book: &BookSummary) -> Value {
    json!({
        "bid": book.bid,
        "ask": book.ask,
        "mid": book.mid,
        "spread_bps": book.spread_bps,
        "depth_bps": book.depth_bps,
        "depth_usd": book.depth_usd,
    })
}

fn write_tick(
    log_dir: &str,
    markets: &[BasisMarket],
    books: &HashMap<u64, BookState>,
    orderbook_limit: u64,
    depth_bps: f64,
    funding: &HashMap<u64, f64>,
) {
    let now = chrono::Utc::now();
    let now_instant = Instant::now();
    let path = format!("{log_dir}/lighter_basis_{}.jsonl", now.format("%Y%m%d"));
    let mut file = match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            log::error!("cannot open {path}: {e}");
            return;
        }
    };
    let ts = now.to_rfc3339();

    for market in markets {
        let mut error = None;
        // The cached book is the latest known book even if the market has been
        // quiet (Lighter only pushes order_book frames on change), so summarize
        // it whenever present and expose its age rather than dropping the leg.
        let spot_state = books.get(&market.spot_market_id);
        let perp_state = books.get(&market.perp_market_id);
        let spot_age_secs = spot_state.and_then(|s| book_age_secs(s, now_instant));
        let perp_age_secs = perp_state.and_then(|s| book_age_secs(s, now_instant));

        let spot = match spot_state
            .ok_or_else(|| "spot: waiting for websocket data".to_string())
            .and_then(|state| {
                summarize_state(state, depth_bps, orderbook_limit).map_err(|e| format!("spot: {e}"))
            }) {
            Ok(summary) => Some(summary),
            Err(e) => {
                error = Some(e);
                None
            }
        };
        let perp = match perp_state
            .ok_or_else(|| "perp: waiting for websocket data".to_string())
            .and_then(|state| {
                summarize_state(state, depth_bps, orderbook_limit).map_err(|e| format!("perp: {e}"))
            }) {
            Ok(summary) => Some(summary),
            Err(e) => {
                let prefix = error.take().map(|s| format!("{s}; ")).unwrap_or_default();
                error = Some(format!("{prefix}{e}"));
                None
            }
        };

        let basis_bps = match (&spot, &perp) {
            (Some(s), Some(p)) if s.mid > 0.0 => Some((p.mid - s.mid) / s.mid * 10_000.0),
            _ => None,
        };
        let funding_rate = funding.get(&market.perp_market_id).copied();
        let funding_bps_per_day = funding_rate.map(|r| r * 24.0 * 10_000.0);

        let line = json!({
            "ts": ts,
            "label": market.label,
            "spot_market_id": market.spot_market_id,
            "perp_market_id": market.perp_market_id,
            "spot": spot.as_ref().map(book_json),
            "perp": perp.as_ref().map(book_json),
            "spot_age_secs": spot_age_secs,
            "perp_age_secs": perp_age_secs,
            "perp_minus_spot_bps": basis_bps,
            "perp_funding_rate": funding_rate,
            "perp_funding_bps_per_day": funding_bps_per_day,
            "orderbook_limit": orderbook_limit,
            "source": "lighter_ws",
            "error": error,
        });
        if let Err(e) = writeln!(file, "{line}") {
            log::error!("write failed for {}: {e}", market.label);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_basis_markets_accepts_triples() {
        assert_eq!(
            parse_basis_markets("ETH:2048:0, LIT:2049:120").unwrap(),
            vec![
                BasisMarket {
                    label: "ETH".to_string(),
                    spot_market_id: 2048,
                    perp_market_id: 0,
                },
                BasisMarket {
                    label: "LIT".to_string(),
                    spot_market_id: 2049,
                    perp_market_id: 120,
                },
            ]
        );
    }

    #[test]
    fn parse_basis_markets_rejects_bad_entries() {
        assert!(parse_basis_markets("").is_err());
        assert!(parse_basis_markets("ETH:2048").is_err());
        assert!(parse_basis_markets("ETH:x:0").is_err());
    }

    #[test]
    fn subscriptions_skip_spot_market_stats() {
        let subs = subscriptions_for(&[BasisMarket {
            label: "ETH".to_string(),
            spot_market_id: 2048,
            perp_market_id: 0,
        }]);
        let channels: Vec<_> = subs
            .iter()
            .filter_map(|s| s.get("channel").and_then(|v| v.as_str()))
            .collect();
        assert_eq!(
            channels,
            vec!["order_book/0", "order_book/2048", "market_stats/0"]
        );
    }

    #[test]
    fn summarize_state_handles_lighter_ws_shape() {
        let mut state = BookState::default();
        state.apply(&json!({
            "bids": [
                {"price": "99.0", "size": "2.0"},
                {"price": "100.0", "size": "1.5"}
            ],
            "asks": [
                {"price": "101.0", "size": "1.0"},
                {"price": "102.0", "size": "2.0"}
            ]
        }));
        let summary = summarize_state(&state, 100.0, 100).unwrap();
        assert_eq!(summary.bid, 100.0);
        assert_eq!(summary.ask, 101.0);
        assert_eq!(summary.mid, 100.5);
        assert!((summary.spread_bps - 99.50248756218906).abs() < 1e-9);
        assert_eq!(summary.depth_usd, 251.0);
    }

    #[test]
    fn is_gap_detects_dropped_delta() {
        let mut state = BookState::default();
        // Snapshot establishes the nonce baseline.
        state.apply(&json!({"bids": [], "asks": [], "begin_nonce": 10, "nonce": 12}));
        // Continuous delta: begin_nonce matches the previous nonce.
        assert!(!state.is_gap(&json!({"begin_nonce": 12, "nonce": 15})));
        state.apply(&json!({"bids": [], "asks": [], "begin_nonce": 12, "nonce": 15}));
        // Gapped delta: begin_nonce (16) skips ahead of the last nonce (15).
        assert!(state.is_gap(&json!({"begin_nonce": 16, "nonce": 18})));
    }

    #[test]
    fn is_gap_false_without_baseline_or_nonce_fields() {
        let state = BookState::default();
        // No baseline yet.
        assert!(!state.is_gap(&json!({"begin_nonce": 5, "nonce": 7})));
        // Baseline present but the frame omits nonce fields → cannot judge, allow.
        let mut state = BookState::default();
        state.apply(&json!({"bids": [], "asks": [], "nonce": 7}));
        assert!(!state.is_gap(&json!({"bids": [], "asks": []})));
    }

    #[test]
    fn book_age_secs_none_until_first_apply() {
        let now = Instant::now();
        // No snapshot yet.
        assert_eq!(book_age_secs(&BookState::default(), now), None);
        // After apply, age is measured from updated_at (~0s here).
        let mut fresh = BookState::default();
        fresh.apply(&json!({"bids": [], "asks": []}));
        assert_eq!(book_age_secs(&fresh, now), Some(0));
        // A book updated 120s ago reports its age, and is NOT dropped.
        let mut quiet = BookState::default();
        quiet.updated_at = now.checked_sub(Duration::from_secs(120));
        assert_eq!(book_age_secs(&quiet, now), Some(120));
    }

    #[test]
    fn ob_u64_parses_number_and_string() {
        assert_eq!(ob_u64(&json!({"nonce": 42}), "nonce"), Some(42));
        assert_eq!(ob_u64(&json!({"nonce": "42"}), "nonce"), Some(42));
        assert_eq!(ob_u64(&json!({"nonce": "x"}), "nonce"), None);
        assert_eq!(ob_u64(&json!({}), "nonce"), None);
    }

    #[test]
    fn apply_side_removes_zero_size_delta() {
        let mut state = BookState::default();
        state.apply(&json!({"bids": [{"price": "10.0", "size": "3.0"}], "asks": []}));
        state.apply(&json!({"bids": [{"price": "10.0", "size": "0"}], "asks": []}));
        assert!(state.bids.is_empty());
    }

    #[test]
    fn market_stats_funding_rate_normalizes_pct_per_hour() {
        let msg = json!({
            "channel": "market_stats:0",
            "type": "update/market_stats",
            "market_stats": {"funding_rate": "0.0007"}
        });
        assert_eq!(market_stats_funding_rate(&msg), Some(0.000007));
    }
}

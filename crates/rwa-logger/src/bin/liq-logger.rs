//! Liquidation event logger PoC (bot-strategy#571 / #663).
//!
//! Records confirmed liquidation/deleverage trades. Lighter is consumed from
//! the public `trade/{market_id}` WebSocket stream so this logger does not spend
//! the live bot's Lighter REST budget. Extended remains on its public REST
//! recent-trades endpoint because that venue has a separate rate-limit domain.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;

use rwa_logger::{parse_pairs, resolve_poll_secs};

const DEFAULT_LIGHTER_WS_URL: &str = "wss://mainnet.zklighter.elliot.ai/stream";
const DEFAULT_EXTENDED_BASE: &str = "https://api.starknet.extended.exchange/api/v1";
const DEFAULT_FETCH_LIMIT: u64 = 100;
const DEFAULT_SEEN_CAP: usize = 20_000;
const USER_AGENT: &str = "rwa-logger/0.1 (+bot-strategy#571/#663)";
/// Lighter drops a WS client that sends no frame within ~2 minutes. The live
/// connector sends a control Ping every 20s (src/lighter_connector/ws.rs); mirror
/// that here so a quiet trade stream is not disconnected between liquidations.
const LIGHTER_WS_PING_SECS: u64 = 20;
/// If no inbound frame (trade, server ping, or pong to our keepalive) arrives
/// within this window the socket is treated as read-stalled and reconnected. A
/// healthy connection answers our 20s ping, so 3 missed cycles means it is dead.
const LIGHTER_WS_IDLE_TIMEOUT_SECS: u64 = 60;
/// Serializes appends to the shared daily `liq_*.jsonl`: the Lighter WS task and
/// the Extended blocking poller both reach `write_liq` concurrently, and their
/// `writeln!` calls would otherwise interleave and corrupt JSONL rows.
static LIQ_WRITE_LOCK: Mutex<()> = Mutex::new(());

fn fnum(node: &Value, key: &str) -> Option<f64> {
    match node.get(key) {
        Some(Value::String(s)) if !s.is_empty() => s.parse::<f64>().ok(),
        Some(Value::Number(n)) => n.as_f64(),
        _ => None,
    }
}

fn lighter_id(t: &Value) -> String {
    t.get("trade_id_str")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| t.get("trade_id").map(|v| v.to_string()))
        .unwrap_or_default()
}

fn extended_id(t: &Value) -> String {
    t.get("i")
        .map(|v| match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        })
        .unwrap_or_default()
}

struct LiqTrade {
    label: String,
    market: String,
    trade_id: String,
    kind: String,
    side: Option<String>,
    price: Option<f64>,
    size: Option<f64>,
    usd: Option<f64>,
    trade_ts_ms: Option<i64>,
    raw: Value,
}

fn lighter_liqs_from_ws(
    msg: &Value,
    labels_by_market: &HashMap<String, String>,
) -> (Vec<LiqTrade>, Option<String>) {
    let market = msg
        .get("channel")
        .and_then(|v| v.as_str())
        .and_then(|channel| channel.rsplit(['/', ':']).next())
        .map(str::to_string);
    let Some(market) = market else {
        return (Vec::new(), None);
    };
    let label = labels_by_market
        .get(&market)
        .cloned()
        .unwrap_or_else(|| market.clone());

    let mut out = Vec::new();
    append_lighter_liqs_from_array(
        msg.get("liquidation_trades").and_then(|v| v.as_array()),
        &label,
        &market,
        true,
        &mut out,
    );
    append_lighter_liqs_from_array(
        msg.get("trades").and_then(|v| v.as_array()),
        &label,
        &market,
        false,
        &mut out,
    );
    (out, Some(market))
}

fn append_lighter_liqs_from_array(
    rows: Option<&Vec<Value>>,
    label: &str,
    market: &str,
    liquidation_feed: bool,
    out: &mut Vec<LiqTrade>,
) {
    let Some(rows) = rows else {
        return;
    };
    for row in rows {
        let trade = row.get("trade").unwrap_or(row);
        let kind = trade
            .get("type")
            .or_else(|| row.get("type"))
            .and_then(|v| v.as_str())
            .unwrap_or(if liquidation_feed {
                "liquidation"
            } else {
                "trade"
            });
        if !liquidation_feed && kind == "trade" {
            continue;
        }
        let side = trade
            .get("is_maker_ask")
            .and_then(|v| v.as_bool())
            .map(|maker_ask| if maker_ask { "buy" } else { "sell" }.to_string());
        out.push(LiqTrade {
            label: label.to_string(),
            market: market.to_string(),
            trade_id: lighter_id(trade),
            kind: kind.to_string(),
            side,
            price: fnum(trade, "price"),
            size: fnum(trade, "size"),
            usd: fnum(trade, "usd_amount"),
            trade_ts_ms: trade.get("timestamp").and_then(|v| v.as_i64()),
            raw: row.clone(),
        });
    }
}

fn extended_liqs(body: &Value, label: &str, market: &str) -> (Vec<LiqTrade>, Vec<String>) {
    let arr = body.get("data").and_then(|d| d.as_array());
    let mut out = Vec::new();
    let mut page_ids = Vec::new();
    if let Some(a) = arr {
        for t in a {
            let id = extended_id(t);
            page_ids.push(id.clone());
            let tt = t.get("tT").and_then(|v| v.as_str()).unwrap_or("TRADE");
            if tt == "TRADE" {
                continue;
            }
            let side = t
                .get("S")
                .and_then(|v| v.as_str())
                .map(|s| s.to_lowercase());
            out.push(LiqTrade {
                label: label.to_string(),
                market: market.to_string(),
                trade_id: id,
                kind: tt.to_lowercase(),
                side,
                price: fnum(t, "p"),
                size: fnum(t, "q"),
                usd: None,
                trade_ts_ms: t.get("T").and_then(|v| v.as_i64()),
                raw: t.clone(),
            });
        }
    }
    (out, page_ids)
}

struct SeenRing {
    set: HashSet<String>,
    order: VecDeque<String>,
    cap: usize,
}

impl SeenRing {
    fn new(cap: usize) -> Self {
        SeenRing {
            set: HashSet::new(),
            order: VecDeque::new(),
            cap: cap.max(1),
        }
    }

    fn insert(&mut self, id: &str) -> bool {
        if self.set.contains(id) {
            return false;
        }
        self.set.insert(id.to_string());
        self.order.push_back(id.to_string());
        if self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            }
        }
        true
    }

    #[cfg(test)]
    fn contains(&self, id: &str) -> bool {
        self.set.contains(id)
    }
}

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let lighter_spec = std::env::var("LIGHTER_LIQ_MARKETS").unwrap_or_default();
    let extended_spec = std::env::var("EXTENDED_LIQ_MARKETS").unwrap_or_default();
    let lighter_markets = parse_or_empty("LIGHTER_LIQ_MARKETS", &lighter_spec);
    let extended_markets = parse_or_empty("EXTENDED_LIQ_MARKETS", &extended_spec);
    if lighter_markets.is_empty() && extended_markets.is_empty() {
        log::error!(
            "at least one of LIGHTER_LIQ_MARKETS / EXTENDED_LIQ_MARKETS must be a non-empty label:market spec"
        );
        std::process::exit(2);
    }

    let log_dir = std::env::var("LIQ_LOG_DIR").unwrap_or_else(|_| ".".to_string());
    let poll_secs = resolve_poll_secs(std::env::var("LIQ_POLL_SECS").ok().as_deref());
    let limit = std::env::var("LIQ_FETCH_LIMIT")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(DEFAULT_FETCH_LIMIT);
    let lighter_ws_url =
        std::env::var("LIGHTER_WS_URL").unwrap_or_else(|_| DEFAULT_LIGHTER_WS_URL.to_string());
    let extended_base =
        std::env::var("EXTENDED_BASE_URL").unwrap_or_else(|_| DEFAULT_EXTENDED_BASE.to_string());
    let seen_cap = std::env::var("LIQ_SEEN_CAP")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(DEFAULT_SEEN_CAP);

    log::info!(
        "liq-logger starting: lighter_ws={} market(s), extended_rest={} market(s), poll={}s, limit={}, dir={}",
        lighter_markets.len(),
        extended_markets.len(),
        poll_secs,
        limit,
        log_dir
    );

    let running = Arc::new(AtomicBool::new(true));
    {
        let r = running.clone();
        let _ = ctrlc::set_handler(move || {
            log::info!("shutdown signal received, stopping after current tick");
            r.store(false, Ordering::SeqCst);
        });
    }

    let total_liq = Arc::new(AtomicU64::new(0));
    let lighter_ok = Arc::new(AtomicUsize::new(0));
    let extended_ok = Arc::new(AtomicUsize::new(0));

    let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // Single heartbeat source, independent of which collectors are enabled, so a
    // Lighter-only config still emits `[HEARTBEAT]` (verify-liq-logger.sh needs
    // it) even though Lighter liquidations are rare.
    tasks.push(tokio::spawn(heartbeat_task(
        poll_secs,
        Arc::clone(&running),
        Arc::clone(&lighter_ok),
        lighter_markets.len(),
        Arc::clone(&extended_ok),
        extended_markets.len(),
        Arc::clone(&total_liq),
    )));

    if !lighter_markets.is_empty() {
        tasks.push(tokio::spawn(lighter_ws_task(
            lighter_ws_url,
            log_dir.clone(),
            lighter_markets.clone(),
            seen_cap,
            Arc::clone(&running),
            Arc::clone(&lighter_ok),
            Arc::clone(&total_liq),
        )));
    }

    if !extended_markets.is_empty() {
        // Extended polling uses a blocking HTTP client and std::thread::sleep.
        // Run it on a dedicated blocking thread (not an async worker) so it can
        // never monopolize the runtime and starve the spawned Lighter WS task on
        // a single-vCPU host.
        let running = Arc::clone(&running);
        let extended_ok = Arc::clone(&extended_ok);
        let total_liq = Arc::clone(&total_liq);
        let log_dir = log_dir.clone();
        tasks.push(tokio::task::spawn_blocking(move || {
            let client = match reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(10))
                .user_agent(USER_AGENT)
                .build()
            {
                Ok(c) => c,
                Err(e) => {
                    log::error!("failed to build http client: {e}");
                    std::process::exit(1);
                }
            };
            extended_poll_loop(
                client,
                extended_markets,
                extended_base,
                log_dir,
                limit,
                poll_secs,
                seen_cap,
                running,
                extended_ok,
                total_liq,
            );
        }));
    }

    for task in tasks {
        let _ = task.await;
    }

    log::info!(
        "liq-logger stopped (total_liq={})",
        total_liq.load(Ordering::SeqCst)
    );
}

/// Periodic `[HEARTBEAT]` emitter. Runs regardless of which collectors are
/// enabled so a Lighter-only logger (where liquidations are rare) still produces
/// the heartbeat line that `deploy/verify-liq-logger.sh` polls for.
#[allow(clippy::too_many_arguments)]
async fn heartbeat_task(
    poll_secs: u64,
    running: Arc<AtomicBool>,
    lighter_ok: Arc<AtomicUsize>,
    lighter_total: usize,
    extended_ok: Arc<AtomicUsize>,
    extended_total: usize,
    total_liq: Arc<AtomicU64>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(poll_secs.max(1)));
    let mut last_reported_total = 0u64;
    while running.load(Ordering::SeqCst) {
        interval.tick().await;
        if !running.load(Ordering::SeqCst) {
            break;
        }
        let total_now = total_liq.load(Ordering::SeqCst);
        let new_since_last = total_now.saturating_sub(last_reported_total);
        last_reported_total = total_now;
        log::info!(
            "[HEARTBEAT] poll ok: lighter={}/{}, extended={}/{}, new_liq={}, total_liq={}",
            lighter_ok.load(Ordering::SeqCst),
            lighter_total,
            extended_ok.load(Ordering::SeqCst),
            extended_total,
            new_since_last,
            total_now
        );
    }
}

/// Blocking Extended recent-trades poll loop. Runs on a dedicated blocking
/// thread (see `main`) so its `reqwest::blocking` calls and `thread::sleep` do
/// not occupy an async worker.
#[allow(clippy::too_many_arguments)]
fn extended_poll_loop(
    client: reqwest::blocking::Client,
    markets: Vec<(String, String)>,
    base: String,
    log_dir: String,
    limit: u64,
    poll_secs: u64,
    seen_cap: usize,
    running: Arc<AtomicBool>,
    extended_ok: Arc<AtomicUsize>,
    total_liq: Arc<AtomicU64>,
) {
    let mut extended_seen = SeenRing::new(seen_cap);
    let mut extended_prev: HashMap<String, HashSet<String>> = HashMap::new();

    while running.load(Ordering::SeqCst) {
        let mut new_this_cycle: u64 = 0;
        let mut ok_this_cycle = 0usize;

        for (label, market) in &markets {
            if !running.load(Ordering::SeqCst) {
                break;
            }
            let url = format!(
                "{}/info/markets/{}/trades",
                base.trim_end_matches('/'),
                market
            );
            match client
                .get(&url)
                .query(&[("limit", &limit.to_string())])
                .send()
            {
                Ok(resp) => {
                    let status = resp.status();
                    match resp.json::<Value>() {
                        Ok(body) if status.is_success() => {
                            ok_this_cycle += 1;
                            let (liqs, page_ids) = extended_liqs(&body, label, market);
                            new_this_cycle += record_page(
                                &log_dir,
                                "extended",
                                &liqs,
                                &page_ids,
                                limit,
                                &mut extended_seen,
                                &mut extended_prev,
                                market,
                                label,
                            );
                        }
                        Ok(body) => log::warn!("extended {label}: trades {status}: {body}"),
                        Err(e) => log::warn!("extended {label}: {status}, body not JSON: {e}"),
                    }
                }
                Err(e) => log::warn!("extended {label}: request failed: {e}"),
            }
        }

        if new_this_cycle > 0 {
            total_liq.fetch_add(new_this_cycle, Ordering::SeqCst);
        }
        // Publish this cycle's health for the heartbeat task to report.
        extended_ok.store(ok_this_cycle, Ordering::SeqCst);

        let mut slept = 0u64;
        while slept < poll_secs && running.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_secs(1));
            slept += 1;
        }
    }
}

async fn lighter_ws_task(
    ws_url: String,
    log_dir: String,
    markets: Vec<(String, String)>,
    seen_cap: usize,
    running: Arc<AtomicBool>,
    ok_count: Arc<AtomicUsize>,
    total_liq: Arc<AtomicU64>,
) {
    let labels_by_market: HashMap<String, String> = markets
        .iter()
        .map(|(label, market)| (market.clone(), label.clone()))
        .collect();
    let mut seen = SeenRing::new(seen_cap);
    while running.load(Ordering::SeqCst) {
        ok_count.store(0, Ordering::SeqCst);
        match lighter_ws_once(
            &ws_url,
            &log_dir,
            &markets,
            &labels_by_market,
            &mut seen,
            &running,
            &ok_count,
            &total_liq,
        )
        .await
        {
            Ok(()) => {}
            Err(e) => {
                if running.load(Ordering::SeqCst) {
                    log::warn!("lighter WS stream ended: {e}; reconnecting in 2s");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn lighter_ws_once(
    ws_url: &str,
    log_dir: &str,
    markets: &[(String, String)],
    labels_by_market: &HashMap<String, String>,
    seen: &mut SeenRing,
    running: &Arc<AtomicBool>,
    ok_count: &Arc<AtomicUsize>,
    total_liq: &Arc<AtomicU64>,
) -> Result<(), String> {
    let (mut ws, _resp) = tokio_tungstenite::connect_async(ws_url)
        .await
        .map_err(|e| format!("connect failed: {e}"))?;
    log::info!("Lighter liquidation WS connected: {ws_url}");
    // Subscribe immediately after the handshake. The production connector
    // (src/lighter_connector/ws.rs) and the Lighter WS reference both send
    // subscribe frames directly; gating on an undocumented `type:"connected"`
    // welcome would leave subscriptions unsent if the server never emits one.
    for (_label, market) in markets {
        let sub = json!({"type": "subscribe", "channel": format!("trade/{market}")});
        ws.send(Message::Text(sub.to_string()))
            .await
            .map_err(|e| format!("subscribe failed: {e}"))?;
    }
    log::info!("sent {} Lighter trade WS subscriptions", markets.len());
    let mut acknowledged = HashSet::new();
    let mut ping_interval = tokio::time::interval(Duration::from_secs(LIGHTER_WS_PING_SECS));
    ping_interval.tick().await; // consume the immediate first tick
    let idle_timeout = Duration::from_secs(LIGHTER_WS_IDLE_TIMEOUT_SECS);
    let mut last_inbound = tokio::time::Instant::now();

    while running.load(Ordering::SeqCst) {
        let next = tokio::select! {
            _ = ping_interval.tick() => {
                // A read-stalled (half-open) socket would otherwise ping forever
                // without ever reading a frame; bail so the outer loop reconnects.
                if last_inbound.elapsed() > idle_timeout {
                    return Err(format!(
                        "no inbound frame for {}s; reconnecting",
                        last_inbound.elapsed().as_secs()
                    ));
                }
                // Proactive client keepalive: the liquidation stream is often quiet
                // for minutes, so we cannot rely on reacting to a server ping.
                ws.send(Message::Ping(Vec::new()))
                    .await
                    .map_err(|e| format!("client ping failed: {e}"))?;
                continue;
            }
            next = ws.next() => next,
        };
        let Some(next) = next else {
            return Err("stream closed".to_string());
        };
        let msg = next.map_err(|e| format!("ws read failed: {e}"))?;
        last_inbound = tokio::time::Instant::now();
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
                        log::warn!("non-JSON Lighter trade WS text ignored: {e}");
                        continue;
                    }
                };
                let msg_type = value.get("type").and_then(|v| v.as_str()).unwrap_or("");
                if msg_type == "connected" {
                    // Optional welcome frame; subscriptions were already sent after
                    // the handshake, so nothing to do here.
                    log::debug!("Lighter trade WS connected frame");
                    continue;
                }
                if msg_type == "ping" {
                    ws.send(Message::Text(json!({"type": "pong"}).to_string()))
                        .await
                        .map_err(|e| format!("app pong failed: {e}"))?;
                    continue;
                }
                if let Some(err) = value.get("error") {
                    log::warn!("Lighter trade WS error frame: {err}");
                    continue;
                }
                if msg_type != "subscribed/trade" && msg_type != "update/trade" {
                    log::trace!("ignored Lighter trade WS type={msg_type}");
                    continue;
                }
                let (liqs, market) = lighter_liqs_from_ws(&value, labels_by_market);
                if let Some(market) = market {
                    if acknowledged.insert(market) {
                        ok_count.store(acknowledged.len(), Ordering::SeqCst);
                    }
                }
                let written = record_stream(log_dir, "lighter", &liqs, seen);
                if written > 0 {
                    total_liq.fetch_add(written, Ordering::SeqCst);
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn parse_or_empty(name: &str, spec: &str) -> Vec<(String, String)> {
    if spec.trim().is_empty() {
        return Vec::new();
    }
    match parse_pairs(spec) {
        Ok(v) => v,
        Err(e) => {
            log::error!("failed to parse {name}: {e}");
            std::process::exit(2);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn record_page(
    log_dir: &str,
    venue: &str,
    liqs: &[LiqTrade],
    page_ids: &[String],
    limit: u64,
    seen: &mut SeenRing,
    prev_page_ids: &mut HashMap<String, HashSet<String>>,
    market: &str,
    label: &str,
) -> u64 {
    let cur: HashSet<String> = page_ids.iter().cloned().collect();
    if page_ids.len() as u64 >= limit {
        if let Some(prev) = prev_page_ids.get(market) {
            if !prev.is_empty() && prev.is_disjoint(&cur) {
                log::warn!(
                    "{venue} {label}: full page ({}) with no overlap vs previous poll; liquidations may have been skipped",
                    page_ids.len()
                );
            }
        }
    }
    prev_page_ids.insert(market.to_string(), cur);
    record_stream(log_dir, venue, liqs, seen)
}

fn record_stream(log_dir: &str, venue: &str, liqs: &[LiqTrade], seen: &mut SeenRing) -> u64 {
    let mut written = 0u64;
    for t in liqs {
        if t.trade_id.is_empty() {
            log::warn!(
                "{venue} {}: liquidation row with empty trade_id, skipping dedup",
                t.label
            );
            continue;
        }
        let key = format!("{}:{}", t.market, t.trade_id);
        if !seen.insert(&key) {
            continue;
        }
        write_liq(log_dir, venue, t);
        written += 1;
    }
    written
}

fn write_liq(log_dir: &str, venue: &str, t: &LiqTrade) {
    let now = chrono::Utc::now();
    let path = format!("{}/liq_{}.jsonl", log_dir, now.format("%Y%m%d"));
    let line = json!({
        "ts": now.to_rfc3339(),
        "venue": venue,
        "label": t.label,
        "market": t.market,
        "trade_id": t.trade_id,
        "type": t.kind,
        "side": t.side,
        "price": t.price,
        "size": t.size,
        "usd": t.usd,
        "trade_ts_ms": t.trade_ts_ms,
        "raw": t.raw,
    });
    {
        // Hold the lock across open+append so concurrent writers (Lighter WS task
        // + Extended blocking poller) cannot interleave rows in the daily file.
        let _guard = LIQ_WRITE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mut file = match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(f) => f,
            Err(e) => {
                log::error!("cannot open {path}: {e}");
                return;
            }
        };
        if let Err(e) = writeln!(file, "{line}") {
            log::error!("write failed for {venue} {}: {e}", t.label);
        }
    }
    log::info!(
        "LIQUIDATION {venue} {} {} price={:?} size={:?} side={:?}",
        t.label,
        t.kind,
        t.price,
        t.size,
        t.side
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn lighter_ws_extracts_non_trade_rows() {
        let msg = json!({
            "type": "update/trade",
            "channel": "trade:1",
            "trades": [
                {"trade_id_str": "1", "type": "trade"},
                {"trade_id_str": "2", "type": "liquidation", "price": "10", "size": "3"}
            ],
            "liquidation_trades": []
        });
        let labels = HashMap::from([("1".to_string(), "BTC".to_string())]);
        let (rows, market) = lighter_liqs_from_ws(&msg, &labels);
        assert_eq!(market.as_deref(), Some("1"));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].label, "BTC");
        assert_eq!(rows[0].trade_id, "2");
        assert_eq!(rows[0].kind, "liquidation");
    }

    #[test]
    fn extended_filters_only_liquidations() {
        let body = json!({"status":"OK","data":[
            {"i":10,"m":"BTC-USD","S":"BUY","tT":"TRADE","T":1781541009022i64,"p":"67111","q":"0.1"},
            {"i":11,"m":"BTC-USD","S":"SELL","tT":"LIQUIDATION","T":1781541009999i64,"p":"67000","q":"0.5"},
            {"i":12,"m":"BTC-USD","S":"BUY","tT":"DELEVERAGE","T":1781541010000i64,"p":"66900","q":"0.2"}
        ]});
        let (liqs, page_ids) = extended_liqs(&body, "BTC", "BTC-USD");
        assert_eq!(page_ids, vec!["10", "11", "12"]);
        assert_eq!(liqs.len(), 2);
        assert_eq!(liqs[0].trade_id, "11");
        assert_eq!(liqs[0].kind, "liquidation");
        assert_eq!(liqs[0].side.as_deref(), Some("sell"));
        assert_eq!(liqs[0].price, Some(67000.0));
        assert_eq!(liqs[1].kind, "deleverage");
    }

    #[test]
    fn extended_empty_data_is_no_liqs() {
        let body = json!({"status":"OK","data":[]});
        let (liqs, page_ids) = extended_liqs(&body, "BTC", "BTC-USD");
        assert!(page_ids.is_empty());
        assert!(liqs.is_empty());
    }

    #[test]
    fn seen_ring_dedupes_and_evicts() {
        let mut r = SeenRing::new(2);
        assert!(r.insert("a"));
        assert!(!r.insert("a"));
        assert!(r.insert("b"));
        assert!(r.insert("c"));
        assert!(!r.contains("a"));
        assert!(r.contains("b"));
        assert!(r.contains("c"));
        assert!(r.insert("a"));
    }

    #[test]
    fn saturation_tracks_full_turnover_even_with_no_liquidations() {
        let dir = std::env::temp_dir();
        let dir = dir.to_str().unwrap();
        let mut seen = SeenRing::new(100);
        let mut prev: HashMap<String, HashSet<String>> = HashMap::new();
        let limit = 3u64;

        let w0 = record_page(
            dir,
            "lighter",
            &[],
            &id_vec(&["1", "2", "3"]),
            limit,
            &mut seen,
            &mut prev,
            "1",
            "BTC",
        );
        assert_eq!(w0, 0);
        assert_eq!(prev.get("1").unwrap().len(), 3);

        let w1 = record_page(
            dir,
            "lighter",
            &[],
            &id_vec(&["7", "8", "9"]),
            limit,
            &mut seen,
            &mut prev,
            "1",
            "BTC",
        );
        assert_eq!(w1, 0);
        assert_eq!(
            prev.get("1").unwrap(),
            &id_vec(&["7", "8", "9"]).into_iter().collect()
        );
    }

    #[test]
    fn dedup_is_market_scoped() {
        let dir = std::env::temp_dir();
        let dir = dir.to_str().unwrap();
        let mut seen = SeenRing::new(100);

        let liq = |market: &str| LiqTrade {
            label: "L".into(),
            market: market.into(),
            trade_id: "5".into(),
            kind: "liquidation".into(),
            side: None,
            price: Some(1.0),
            size: Some(1.0),
            usd: None,
            trade_ts_ms: Some(1),
            raw: Value::Null,
        };

        let a = record_stream(dir, "lighter", &[liq("0")], &mut seen);
        let b = record_stream(dir, "lighter", &[liq("1")], &mut seen);
        let c = record_stream(dir, "lighter", &[liq("0")], &mut seen);
        assert_eq!(a, 1);
        assert_eq!(b, 1);
        assert_eq!(c, 0);
    }

    #[test]
    fn fnum_handles_string_and_number() {
        let v = json!({"s":"67191.4","n":42.5,"empty":""});
        assert_eq!(fnum(&v, "s"), Some(67191.4));
        assert_eq!(fnum(&v, "n"), Some(42.5));
        assert_eq!(fnum(&v, "empty"), None);
        assert_eq!(fnum(&v, "missing"), None);
    }

    fn id_vec(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }
}

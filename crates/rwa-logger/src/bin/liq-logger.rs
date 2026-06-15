//! Liquidation event logger PoC (bot-strategy#571 — true-liquidation fade).
//!
//! Phase 0 (read-only) collector that records CONFIRMED liquidation/deleverage
//! trades on Lighter and Extended, so the "fade the overshoot of a real
//! liquidation cascade" idea (#571) can be characterized offline. It does NOT
//! place orders and needs no auth/signing.
//!
//! Why this is different from the rejected spread-derived cascade-fade
//! (project: cascade-fade-rejected): the trigger here is the venue's own
//! liquidation TAG on each trade, not a price/spread move. A degraded-book
//! artifact (a fake 1000s-bps move from a thin book) cannot enter this dataset
//! unless the venue itself tagged the trade as a liquidation. The first thing
//! #571 must learn is simply the EVENT FREQUENCY on BTC/ETH — if real
//! liquidations are only a handful per day, there is no statistics to build and
//! the idea is NO-GO. This logger answers that cheaply.
//!
//! ## Mechanism (REST poll, no WS/SDK — keeps the arm64 deploy build light)
//!
//! Both venues expose the liquidation marker on their public recent-trades REST.
//! Lighter `GET {base}/api/v1/recentTrades?market_id={id}&limit={n}` returns
//! `trades[].type` in {trade, liquidation, deleverage, market-settlement};
//! anything other than "trade" is captured. Extended
//! `GET {base}/info/markets/{market}/trades?limit={n}` returns `data[].tT` in
//! {TRADE, LIQUIDATION, DELEVERAGE}; anything other than "TRADE" is captured.
//!
//! Each new (non-normal) trade is de-duplicated by (venue, trade_id) against a
//! bounded seen-set and appended as one JSONL line. Normal trades are dropped.
//!
//! ### Sampling caveat (logged, never silent)
//! recent-trades is a sliding window, so on a very high-volume market a burst
//! between two polls could push liquidations out of the window unseen. When a
//! poll returns a full `limit` page AND none of those trades were already seen
//! (i.e. the window fully turned over between polls), we WARN that liquidations
//! may have been skipped — the resulting counts are then a lower bound. Lower
//! `LIQ_POLL_SECS` / raise `LIQ_FETCH_LIMIT` if this fires often.
//!
//! ## Env vars
//! At least one of the two market specs must be non-empty:
//! - `LIGHTER_LIQ_MARKETS`  — comma-separated `label:market_id` (e.g. `BTC:1,ETH:0`)
//! - `EXTENDED_LIQ_MARKETS` — comma-separated `label:market` (e.g. `BTC:BTC-USD,ETH:ETH-USD`)
//! - `LIQ_LOG_DIR`     (default `.`)    — output dir for daily JSONL
//! - `LIQ_POLL_SECS`   (default `5`)    — poll cadence (clamped to >=1s)
//! - `LIQ_FETCH_LIMIT` (default `100`)  — recent-trades page size per market
//!   (Lighter caps this at 100; larger values are clamped for the Lighter leg)
//! - `LIGHTER_BASE_URL`  (default `https://mainnet.zklighter.elliot.ai`)
//! - `EXTENDED_BASE_URL` (default `https://api.starknet.extended.exchange/api/v1`)
//! - `LIQ_SEEN_CAP`    (default `20000`) — per-venue de-dup ring size
//!
//! ## Output
//! `<LIQ_LOG_DIR>/liq_YYYYMMDD.jsonl`, one line per NEW liquidation trade:
//! `{"ts":..,"venue":"lighter","label":"BTC","market":"1","trade_id":"..",
//!   "type":"liquidation","side":"buy","price":..,"size":..,"usd":..,
//!   "trade_ts_ms":..,"raw":{..}}`

use std::collections::{HashSet, VecDeque};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use rwa_logger::{parse_pairs, resolve_poll_secs};

const DEFAULT_LIGHTER_BASE: &str = "https://mainnet.zklighter.elliot.ai";
const DEFAULT_EXTENDED_BASE: &str = "https://api.starknet.extended.exchange/api/v1";
const DEFAULT_FETCH_LIMIT: u64 = 100;
const DEFAULT_SEEN_CAP: usize = 20_000;
/// Lighter `recentTrades` rejects `limit > 100` with code 20001, so the
/// per-poll page is clamped to this for the Lighter leg regardless of config.
const LIGHTER_MAX_LIMIT: u64 = 100;
/// Extended's WAF returns 403 to requests with no User-Agent (reqwest sends
/// none by default), so the client sets an explicit one.
const USER_AGENT: &str = "rwa-logger/0.1 (+bot-strategy#571)";

/// Parse a numeric field that may arrive as a JSON string ("67191.4") or a
/// JSON number. Empty string -> None.
fn fnum(node: &Value, key: &str) -> Option<f64> {
    match node.get(key) {
        Some(Value::String(s)) if !s.is_empty() => s.parse::<f64>().ok(),
        Some(Value::Number(n)) => n.as_f64(),
        _ => None,
    }
}

/// A liquidation trade normalized across venues, ready to serialize.
struct LiqTrade {
    label: String,
    market: String,
    trade_id: String,
    kind: String, // venue's tag, lowercased: liquidation | deleverage | market-settlement
    side: Option<String>,
    price: Option<f64>,
    size: Option<f64>,
    usd: Option<f64>,
    trade_ts_ms: Option<i64>,
    raw: Value,
}

/// Extract liquidation trades from a Lighter `recentTrades` body. Returns
/// (liq_trades, total_rows_in_page). A row is a liquidation iff its `type` is
/// present and not "trade".
fn lighter_liqs(body: &Value, label: &str, market: &str) -> (Vec<LiqTrade>, usize) {
    let arr = body.get("trades").and_then(|t| t.as_array());
    let rows = arr.map(|a| a.len()).unwrap_or(0);
    let mut out = Vec::new();
    if let Some(a) = arr {
        for t in a {
            let kind = t.get("type").and_then(|v| v.as_str()).unwrap_or("trade");
            if kind == "trade" {
                continue;
            }
            // Taker side: if the maker sat on the ask, the taker bought.
            let side = t
                .get("is_maker_ask")
                .and_then(|v| v.as_bool())
                .map(|maker_ask| if maker_ask { "buy" } else { "sell" }.to_string());
            let id = t
                .get("trade_id_str")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .or_else(|| t.get("trade_id").map(|v| v.to_string()))
                .unwrap_or_default();
            out.push(LiqTrade {
                label: label.to_string(),
                market: market.to_string(),
                trade_id: id,
                kind: kind.to_string(),
                side,
                price: fnum(t, "price"),
                size: fnum(t, "size"),
                usd: fnum(t, "usd_amount"),
                trade_ts_ms: t.get("timestamp").and_then(|v| v.as_i64()),
                raw: t.clone(),
            });
        }
    }
    (out, rows)
}

/// Extract liquidation trades from an Extended `trades` body. A row is a
/// liquidation iff its `tT` is present and not "TRADE".
fn extended_liqs(body: &Value, label: &str, market: &str) -> (Vec<LiqTrade>, usize) {
    let arr = body.get("data").and_then(|d| d.as_array());
    let rows = arr.map(|a| a.len()).unwrap_or(0);
    let mut out = Vec::new();
    if let Some(a) = arr {
        for t in a {
            let tt = t.get("tT").and_then(|v| v.as_str()).unwrap_or("TRADE");
            if tt == "TRADE" {
                continue;
            }
            let side = t
                .get("S")
                .and_then(|v| v.as_str())
                .map(|s| s.to_lowercase());
            let id = t
                .get("i")
                .map(|v| match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .unwrap_or_default();
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
    (out, rows)
}

/// Bounded de-dup set: remembers up to `cap` recent ids, evicting oldest first.
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
    /// Insert id; returns true if it was NEW (not seen before).
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
    fn contains(&self, id: &str) -> bool {
        self.set.contains(id)
    }
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let lighter_spec = std::env::var("LIGHTER_LIQ_MARKETS").unwrap_or_default();
    let extended_spec = std::env::var("EXTENDED_LIQ_MARKETS").unwrap_or_default();
    let lighter_markets = parse_or_empty("LIGHTER_LIQ_MARKETS", &lighter_spec);
    let extended_markets = parse_or_empty("EXTENDED_LIQ_MARKETS", &extended_spec);
    if lighter_markets.is_empty() && extended_markets.is_empty() {
        log::error!(
            "at least one of LIGHTER_LIQ_MARKETS / EXTENDED_LIQ_MARKETS must be a non-empty \
             label:market spec"
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
    let lighter_base =
        std::env::var("LIGHTER_BASE_URL").unwrap_or_else(|_| DEFAULT_LIGHTER_BASE.to_string());
    let extended_base =
        std::env::var("EXTENDED_BASE_URL").unwrap_or_else(|_| DEFAULT_EXTENDED_BASE.to_string());
    let seen_cap = std::env::var("LIQ_SEEN_CAP")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(DEFAULT_SEEN_CAP);

    log::info!(
        "liq-logger starting: lighter={} market(s), extended={} market(s), poll={}s, limit={}, dir={}",
        lighter_markets.len(),
        extended_markets.len(),
        poll_secs,
        limit,
        log_dir
    );

    let lighter_limit = limit.min(LIGHTER_MAX_LIMIT);

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

    let running = Arc::new(AtomicBool::new(true));
    {
        let r = running.clone();
        let _ = ctrlc::set_handler(move || {
            log::info!("shutdown signal received, stopping after current tick");
            r.store(false, Ordering::SeqCst);
        });
    }

    let mut lighter_seen = SeenRing::new(seen_cap);
    let mut extended_seen = SeenRing::new(seen_cap);
    let mut total_liq: u64 = 0;

    while running.load(Ordering::SeqCst) {
        let mut new_this_cycle: u64 = 0;
        let mut lighter_ok = 0usize;
        let mut extended_ok = 0usize;

        for (label, market_id) in &lighter_markets {
            if !running.load(Ordering::SeqCst) {
                break;
            }
            let url = format!("{}/api/v1/recentTrades", lighter_base.trim_end_matches('/'));
            match client
                .get(&url)
                .query(&[
                    ("market_id", market_id.as_str()),
                    ("limit", &lighter_limit.to_string()),
                ])
                .send()
            {
                Ok(resp) => {
                    let status = resp.status();
                    match resp.json::<Value>() {
                        Ok(body) if status.is_success() => {
                            lighter_ok += 1;
                            let (liqs, rows) = lighter_liqs(&body, label, market_id);
                            new_this_cycle += record(
                                &log_dir,
                                "lighter",
                                &liqs,
                                rows,
                                lighter_limit,
                                &mut lighter_seen,
                                label,
                            );
                        }
                        Ok(body) => log::warn!("lighter {label}: recentTrades {status}: {body}"),
                        Err(e) => log::warn!("lighter {label}: {status}, body not JSON: {e}"),
                    }
                }
                Err(e) => log::warn!("lighter {label}: request failed: {e}"),
            }
        }

        for (label, market) in &extended_markets {
            if !running.load(Ordering::SeqCst) {
                break;
            }
            let url = format!(
                "{}/info/markets/{}/trades",
                extended_base.trim_end_matches('/'),
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
                            extended_ok += 1;
                            let (liqs, rows) = extended_liqs(&body, label, market);
                            new_this_cycle += record(
                                &log_dir,
                                "extended",
                                &liqs,
                                rows,
                                limit,
                                &mut extended_seen,
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

        total_liq += new_this_cycle;
        // Heartbeat: lets a deploy verify confirm liveness without waiting for a
        // (rare) liquidation to occur — assert both venues poll OK, not rows.
        log::info!(
            "[HEARTBEAT] poll ok: lighter={}/{}, extended={}/{}, new_liq={}, total_liq={}",
            lighter_ok,
            lighter_markets.len(),
            extended_ok,
            extended_markets.len(),
            new_this_cycle,
            total_liq
        );

        let mut slept = 0u64;
        while slept < poll_secs && running.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_secs(1));
            slept += 1;
        }
    }

    log::info!("liq-logger stopped (total_liq={total_liq})");
}

/// Parse a spec that is allowed to be empty (returns []), erroring out the
/// process only on a malformed non-empty spec.
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

/// Append the NEW (not-yet-seen) liquidations to today's JSONL and emit the
/// sliding-window saturation warning. Returns the count of new rows written.
fn record(
    log_dir: &str,
    venue: &str,
    liqs: &[LiqTrade],
    page_rows: usize,
    limit: u64,
    seen: &mut SeenRing,
    label: &str,
) -> u64 {
    // Saturation check uses ALL page ids, not just liquidations: a full page
    // whose every row is new means the window fully turned over since last poll.
    let any_already_seen = liqs.iter().any(|t| seen.contains(&t.trade_id));
    if page_rows as u64 >= limit && !liqs.is_empty() && !any_already_seen {
        log::warn!(
            "{venue} {label}: full page ({page_rows}) with no overlap — liquidations may have \
             been skipped between polls; lower LIQ_POLL_SECS or raise LIQ_FETCH_LIMIT"
        );
    }

    let mut written = 0u64;
    for t in liqs {
        if t.trade_id.is_empty() {
            log::warn!("{venue} {label}: liquidation row with empty trade_id, skipping dedup");
            continue;
        }
        if !seen.insert(&t.trade_id) {
            continue; // already logged
        }
        write_liq(log_dir, venue, t);
        written += 1;
    }
    written
}

fn write_liq(log_dir: &str, venue: &str, t: &LiqTrade) {
    let now = chrono::Utc::now();
    let path = format!("{}/liq_{}.jsonl", log_dir, now.format("%Y%m%d"));
    let mut file = match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            log::error!("cannot open {path}: {e}");
            return;
        }
    };
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
    if let Err(e) = writeln!(file, "{line}") {
        log::error!("write failed for {venue} {}: {e}", t.label);
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

    #[test]
    fn lighter_filters_only_liquidations() {
        let body = json!({"code":200,"trades":[
            {"trade_id_str":"1","type":"trade","price":"100","size":"1","usd_amount":"100","is_maker_ask":true,"timestamp":1781541099583i64},
            {"trade_id_str":"2","type":"liquidation","price":"99","size":"2","usd_amount":"198","is_maker_ask":false,"timestamp":1781541099999i64},
            {"trade_id_str":"3","type":"deleverage","price":"98","size":"3","usd_amount":"294","is_maker_ask":true,"timestamp":1781541100000i64}
        ]});
        let (liqs, rows) = lighter_liqs(&body, "BTC", "1");
        assert_eq!(rows, 3);
        assert_eq!(liqs.len(), 2);
        assert_eq!(liqs[0].trade_id, "2");
        assert_eq!(liqs[0].kind, "liquidation");
        assert_eq!(liqs[0].side.as_deref(), Some("sell")); // is_maker_ask=false -> taker sold
        assert_eq!(liqs[0].price, Some(99.0));
        assert_eq!(liqs[1].kind, "deleverage");
        assert_eq!(liqs[1].side.as_deref(), Some("buy")); // is_maker_ask=true -> taker bought
    }

    #[test]
    fn extended_filters_only_liquidations() {
        let body = json!({"status":"OK","data":[
            {"i":10,"m":"BTC-USD","S":"BUY","tT":"TRADE","T":1781541009022i64,"p":"67111","q":"0.1"},
            {"i":11,"m":"BTC-USD","S":"SELL","tT":"LIQUIDATION","T":1781541009999i64,"p":"67000","q":"0.5"},
            {"i":12,"m":"BTC-USD","S":"BUY","tT":"DELEVERAGE","T":1781541010000i64,"p":"66900","q":"0.2"}
        ]});
        let (liqs, rows) = extended_liqs(&body, "BTC", "BTC-USD");
        assert_eq!(rows, 3);
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
        let (liqs, rows) = extended_liqs(&body, "BTC", "BTC-USD");
        assert_eq!(rows, 0);
        assert!(liqs.is_empty());
    }

    #[test]
    fn seen_ring_dedupes_and_evicts() {
        let mut r = SeenRing::new(2);
        assert!(r.insert("a")); // new
        assert!(!r.insert("a")); // dup
        assert!(r.insert("b"));
        assert!(r.insert("c")); // evicts "a"
        assert!(!r.contains("a"));
        assert!(r.contains("b"));
        assert!(r.contains("c"));
        assert!(r.insert("a")); // "a" is new again after eviction
    }

    #[test]
    fn fnum_handles_string_and_number() {
        let v = json!({"s":"67191.4","n":42.5,"empty":""});
        assert_eq!(fnum(&v, "s"), Some(67191.4));
        assert_eq!(fnum(&v, "n"), Some(42.5));
        assert_eq!(fnum(&v, "empty"), None);
        assert_eq!(fnum(&v, "missing"), None);
    }
}

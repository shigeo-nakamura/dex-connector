//! Lighter spot/perp basis logger (bot-strategy#573).
//!
//! Phase 0 read-only collector for the narrow single-venue spot/perp surface
//! found on Lighter. It polls public order books for configured spot/perp
//! market-id pairs, joins the current perp funding rate, and writes one compact
//! JSONL row per symbol per tick. It does NOT place orders and needs no auth.
//!
//! ## Env vars
//! - `LIGHTER_BASIS_MARKETS` - comma-separated `label:spot_market_id:perp_market_id`
//!                            entries, e.g. `ETH:2048:0,LIT:2049:120`.
//! - `LIGHTER_BASIS_LOG_DIR` (default `.`) - output directory.
//! - `LIGHTER_BASIS_POLL_SECS` (default `10`) - poll cadence.
//! - `LIGHTER_BASIS_ORDERBOOK_LIMIT` (default `100`, max `250`).
//! - `LIGHTER_BASIS_DEPTH_BPS` (default `25`) - depth window around mid.
//! - `LIGHTER_BASE_URL` (default `https://mainnet.zklighter.elliot.ai`).
//!
//! ## Output
//! `<LIGHTER_BASIS_LOG_DIR>/lighter_basis_YYYYMMDD.jsonl`, one row per pair:
//! `perp_minus_spot_bps` is positive when the perp mid is rich versus spot.

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use reqwest::StatusCode;
use serde_json::{json, Value};

use rwa_logger::resolve_poll_secs;

const DEFAULT_LIGHTER_BASE: &str = "https://mainnet.zklighter.elliot.ai";
const DEFAULT_ORDERBOOK_LIMIT: u64 = 100;
const MAX_ORDERBOOK_LIMIT: u64 = 250;
const DEFAULT_DEPTH_BPS: f64 = 25.0;
const USER_AGENT: &str = "lighter-basis-logger/0.1 (+bot-strategy#573)";

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

fn main() {
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
    let base_url =
        std::env::var("LIGHTER_BASE_URL").unwrap_or_else(|_| DEFAULT_LIGHTER_BASE.to_string());
    let poll_secs = resolve_poll_secs(std::env::var("LIGHTER_BASIS_POLL_SECS").ok().as_deref());
    let orderbook_limit = resolve_orderbook_limit(
        std::env::var("LIGHTER_BASIS_ORDERBOOK_LIMIT")
            .ok()
            .as_deref(),
    );
    let depth_bps = resolve_depth_bps(std::env::var("LIGHTER_BASIS_DEPTH_BPS").ok().as_deref());

    log::info!(
        "lighter-basis-logger starting: {} market pair(s), poll={}s, limit={}, depth={}bps, base={}, dir={}",
        markets.len(), poll_secs, orderbook_limit, depth_bps, base_url, log_dir
    );

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

    while running.load(Ordering::SeqCst) {
        let funding = fetch_funding_rates(&client, &base_url);
        write_tick(
            &client,
            &log_dir,
            &base_url,
            &markets,
            orderbook_limit,
            depth_bps,
            funding.as_ref(),
        );

        let mut slept = 0u64;
        while slept < poll_secs && running.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_secs(1));
            slept += 1;
        }
    }

    log::info!("lighter-basis-logger stopped");
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

fn market_id_value(node: &Value) -> Option<u64> {
    match node.get("market_id") {
        Some(Value::Number(n)) => n.as_u64(),
        Some(Value::String(s)) => s.parse::<u64>().ok(),
        _ => None,
    }
}

fn fetch_json(
    client: &reqwest::blocking::Client,
    url: &str,
    context: &str,
) -> Result<Value, String> {
    let resp = client
        .get(url)
        .send()
        .map_err(|e| format!("{context} request failed: {e}"))?;
    let status = resp.status();
    let body = resp
        .json::<Value>()
        .map_err(|e| format!("{context} {status}, body not JSON: {e}"))?;
    if status == StatusCode::OK {
        Ok(body)
    } else {
        Err(format!("{context} returned {status}: {body}"))
    }
}

fn fetch_order_book(
    client: &reqwest::blocking::Client,
    base_url: &str,
    market_id: u64,
    limit: u64,
) -> Result<Value, String> {
    let url = format!("{base_url}/api/v1/orderBookOrders?market_id={market_id}&limit={limit}");
    fetch_json(
        client,
        &url,
        &format!("orderBookOrders market_id={market_id}"),
    )
}

fn fetch_funding_rates(
    client: &reqwest::blocking::Client,
    base_url: &str,
) -> Option<HashMap<u64, Value>> {
    let url = format!("{base_url}/api/v1/funding-rates");
    let body = match fetch_json(client, &url, "funding-rates") {
        Ok(body) => body,
        Err(e) => {
            log::warn!("{e}");
            return None;
        }
    };
    let mut out = HashMap::new();
    if let Some(rows) = body.get("funding_rates").and_then(|v| v.as_array()) {
        for row in rows {
            if let Some(market_id) = market_id_value(row) {
                out.insert(market_id, row.clone());
            }
        }
    }
    Some(out)
}

fn order_rows<'a>(body: &'a Value, side: &str) -> Vec<&'a Value> {
    let src = body
        .get("order_book")
        .filter(|v| v.is_object())
        .unwrap_or(body);
    src.get(side)
        .and_then(|v| v.as_array())
        .map(|a| a.iter().collect())
        .unwrap_or_default()
}

fn row_price(row: &Value) -> Option<f64> {
    match row {
        Value::Object(_) => fnum(row, "price").or_else(|| fnum(row, "p")),
        Value::Array(a) => a.first().and_then(value_as_f64),
        _ => None,
    }
}

fn row_size(row: &Value) -> Option<f64> {
    match row {
        Value::Object(_) => fnum(row, "remaining_base_amount")
            .or_else(|| fnum(row, "size"))
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

fn summarize_book(body: &Value, depth_bps: f64) -> Result<BookSummary, String> {
    let bids = order_rows(body, "bids");
    let asks = order_rows(body, "asks");
    let bid = bids
        .iter()
        .filter_map(|r| row_price(r))
        .filter(|p| p.is_finite() && *p > 0.0)
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .ok_or_else(|| "missing bid".to_string())?;
    let ask = asks
        .iter()
        .filter_map(|r| row_price(r))
        .filter(|p| p.is_finite() && *p > 0.0)
        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .ok_or_else(|| "missing ask".to_string())?;
    if ask < bid {
        return Err(format!("crossed book bid={bid} ask={ask}"));
    }

    let mid = (bid + ask) / 2.0;
    let spread_bps = (ask - bid) / mid * 10_000.0;
    let lo = mid * (1.0 - depth_bps / 10_000.0);
    let hi = mid * (1.0 + depth_bps / 10_000.0);
    let mut depth_usd = 0.0;
    for row in bids {
        if let (Some(p), Some(s)) = (row_price(row), row_size(row)) {
            if p >= lo && p.is_finite() && s.is_finite() && s > 0.0 {
                depth_usd += p * s;
            }
        }
    }
    for row in asks {
        if let (Some(p), Some(s)) = (row_price(row), row_size(row)) {
            if p <= hi && p.is_finite() && s.is_finite() && s > 0.0 {
                depth_usd += p * s;
            }
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

fn funding_rate_for(funding: Option<&HashMap<u64, Value>>, market_id: u64) -> Option<f64> {
    funding
        .and_then(|m| m.get(&market_id))
        .and_then(|row| fnum(row, "rate"))
}

fn write_tick(
    client: &reqwest::blocking::Client,
    log_dir: &str,
    base_url: &str,
    markets: &[BasisMarket],
    orderbook_limit: u64,
    depth_bps: f64,
    funding: Option<&HashMap<u64, Value>>,
) {
    let now = chrono::Utc::now();
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
        let mut spot = None;
        let mut perp = None;

        match fetch_order_book(client, base_url, market.spot_market_id, orderbook_limit)
            .and_then(|body| summarize_book(&body, depth_bps))
        {
            Ok(summary) => spot = Some(summary),
            Err(e) => {
                log::warn!(
                    "{} spot market {}: {e}",
                    market.label,
                    market.spot_market_id
                );
                error = Some(format!("spot: {e}"));
            }
        }

        match fetch_order_book(client, base_url, market.perp_market_id, orderbook_limit)
            .and_then(|body| summarize_book(&body, depth_bps))
        {
            Ok(summary) => perp = Some(summary),
            Err(e) => {
                log::warn!(
                    "{} perp market {}: {e}",
                    market.label,
                    market.perp_market_id
                );
                let prefix = error.take().map(|s| format!("{s}; ")).unwrap_or_default();
                error = Some(format!("{prefix}perp: {e}"));
            }
        }

        let basis_bps = match (&spot, &perp) {
            (Some(s), Some(p)) if s.mid > 0.0 => Some((p.mid - s.mid) / s.mid * 10_000.0),
            _ => None,
        };
        let funding_rate = funding_rate_for(funding, market.perp_market_id);
        let funding_bps_per_day = funding_rate.map(|r| r * 24.0 * 10_000.0);

        let line = json!({
            "ts": ts,
            "label": market.label,
            "spot_market_id": market.spot_market_id,
            "perp_market_id": market.perp_market_id,
            "spot": spot.as_ref().map(book_json),
            "perp": perp.as_ref().map(book_json),
            "perp_minus_spot_bps": basis_bps,
            "perp_funding_rate": funding_rate,
            "perp_funding_bps_per_day": funding_bps_per_day,
            "orderbook_limit": orderbook_limit,
            "source": "lighter",
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
    fn summarize_book_handles_lighter_shape() {
        let body = json!({
            "bids": [
                {"price": "99.0", "remaining_base_amount": "2.0"},
                {"price": "100.0", "remaining_base_amount": "1.5"}
            ],
            "asks": [
                {"price": "101.0", "remaining_base_amount": "1.0"},
                {"price": "102.0", "remaining_base_amount": "2.0"}
            ]
        });
        let summary = summarize_book(&body, 100.0).unwrap();
        assert_eq!(summary.bid, 100.0);
        assert_eq!(summary.ask, 101.0);
        assert_eq!(summary.mid, 100.5);
        assert!((summary.spread_bps - 99.50248756218906).abs() < 1e-9);
        assert_eq!(summary.depth_usd, 251.0);
    }

    #[test]
    fn summarize_book_handles_nested_and_array_rows() {
        let body = json!({
            "order_book": {
                "bids": [["10.0", "3.0"]],
                "asks": [["10.2", "4.0"]]
            }
        });
        let summary = summarize_book(&body, 200.0).unwrap();
        assert_eq!(summary.bid, 10.0);
        assert_eq!(summary.ask, 10.2);
        assert_eq!(summary.depth_usd, 70.8);
    }

    #[test]
    fn funding_rate_for_uses_market_id() {
        let mut m = HashMap::new();
        m.insert(0, json!({"market_id": 0, "rate": 0.000048}));
        assert_eq!(funding_rate_for(Some(&m), 0), Some(0.000048));
        assert_eq!(funding_rate_for(Some(&m), 1), None);
    }
}

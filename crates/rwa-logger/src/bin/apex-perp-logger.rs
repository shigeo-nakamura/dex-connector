//! ApeX RWA perp ticker logger PoC (bot-strategy#574, for #563/#566).
//!
//! Phase 0 (read-only) collector for the RWA-perp side of the basis/carry
//! strategies. It does NOT place orders and needs no auth/signing — it polls
//! ApeX Omni's public `ticker` endpoint per symbol and appends one JSONL line
//! per symbol per tick, capturing funding rate, mark/index/last price and open
//! interest. Pairs with `rwa-spot-logger` (xStocks spot) so the perp-vs-spot
//! basis (#563) and funding carry (#566) can be characterized offline.
//!
//! Standalone binary in the lightweight `rwa-logger` crate (no DEX SDK deps).
//!
//! ## Env vars
//! - `APEX_SYMBOLS`  (required) — comma-separated `label:tickerSymbol` pairs.
//!                   NOTE the ApeX *ticker* endpoint wants the **dash-less**
//!                   form, e.g. `TSLA:TSLAUSDT,NVDA:NVDAUSDT`.
//! - `APEX_LOG_DIR`  (default `.`)   — output dir for daily JSONL
//! - `APEX_TICKER_URL` (default `https://omni.apex.exchange/api/v3/ticker`)
//! - `APEX_POLL_SECS` (default `5`) — poll cadence (clamped to >=1s)
//!
//! ## Output
//! `<APEX_LOG_DIR>/apex_perp_YYYYMMDD.jsonl`, one line per symbol per tick:
//! `{"ts":..,"label":"TSLA","symbol":"TSLAUSDT","funding_rate":..,"mark_price":..,
//!   "index_price":..,"last_price":..,"open_interest":..,"next_funding_time":..,
//!   "predicted_funding_rate":..,"source":"apex","raw":{..}}`
//!
//! Numeric fields are null (logged as a warn) when the endpoint returns an
//! empty `data` array or a blank string for that field, so gaps are visible.

use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use rwa_logger::{parse_pairs, resolve_poll_secs};

const DEFAULT_TICKER_URL: &str = "https://omni.apex.exchange/api/v3/ticker";

/// Parse an ApeX numeric field that arrives as a (possibly empty) JSON string,
/// e.g. `"0.0000125"` -> Some(..), `""` -> None.
fn fnum(node: &Value, key: &str) -> Option<f64> {
    match node.get(key) {
        Some(Value::String(s)) if !s.is_empty() => s.parse::<f64>().ok(),
        Some(Value::Number(n)) => n.as_f64(),
        _ => None,
    }
}

/// Pull the single ticker object out of ApeX's `{ "data": [ {..} ] }` envelope.
fn ticker_obj(body: &Value) -> Option<&Value> {
    body.get("data")?.as_array()?.first()
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let symbols_spec = match std::env::var("APEX_SYMBOLS") {
        Ok(v) => v,
        Err(_) => {
            log::error!("APEX_SYMBOLS is required (comma-separated label:tickerSymbol pairs)");
            std::process::exit(2);
        }
    };
    let symbols = match parse_pairs(&symbols_spec) {
        Ok(s) => s,
        Err(e) => {
            log::error!("failed to parse APEX_SYMBOLS: {e}");
            std::process::exit(2);
        }
    };
    let log_dir = std::env::var("APEX_LOG_DIR").unwrap_or_else(|_| ".".to_string());
    let url = std::env::var("APEX_TICKER_URL").unwrap_or_else(|_| DEFAULT_TICKER_URL.to_string());
    let poll_secs = resolve_poll_secs(std::env::var("APEX_POLL_SECS").ok().as_deref());

    log::info!(
        "apex-perp-logger starting: {} symbol(s), poll={}s, url={}, dir={}",
        symbols.len(),
        poll_secs,
        url,
        log_dir
    );

    let client = match reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
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
        for (label, symbol) in &symbols {
            if !running.load(Ordering::SeqCst) {
                break;
            }
            match client.get(&url).query(&[("symbol", symbol)]).send() {
                Ok(resp) => {
                    let status = resp.status();
                    match resp.json::<Value>() {
                        Ok(body) if status.is_success() => {
                            write_tick(&log_dir, label, symbol, &body)
                        }
                        Ok(body) => log::warn!("{label}: ticker returned {status}: {body}"),
                        Err(e) => log::warn!("{label}: ticker {status}, body not JSON: {e}"),
                    }
                }
                Err(e) => log::warn!("{label}: ticker request failed: {e}"),
            }
        }

        // Sleep in short slices so Ctrl-C is responsive.
        let mut slept = 0u64;
        while slept < poll_secs && running.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_secs(1));
            slept += 1;
        }
    }

    log::info!("apex-perp-logger stopped");
}

fn write_tick(log_dir: &str, label: &str, symbol: &str, body: &Value) {
    let now = chrono::Utc::now();
    let path = format!("{}/apex_perp_{}.jsonl", log_dir, now.format("%Y%m%d"));
    let mut file = match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            log::error!("cannot open {path}: {e}");
            return;
        }
    };

    let obj = ticker_obj(body);
    if obj.is_none() {
        log::warn!("no ticker data for {label} ({symbol}) this tick");
    }
    let funding_rate = obj.and_then(|o| fnum(o, "fundingRate"));
    let line = json!({
        "ts": now.to_rfc3339(),
        "label": label,
        "symbol": symbol,
        "funding_rate": funding_rate,
        "predicted_funding_rate": obj.and_then(|o| fnum(o, "predictedFundingRate")),
        "mark_price": obj.and_then(|o| fnum(o, "markPrice")),
        "index_price": obj.and_then(|o| fnum(o, "indexPrice")),
        "oracle_price": obj.and_then(|o| fnum(o, "oraclePrice")),
        "last_price": obj.and_then(|o| fnum(o, "lastPrice")),
        "open_interest": obj.and_then(|o| fnum(o, "openInterest")),
        "volume_24h": obj.and_then(|o| fnum(o, "volume24h")),
        "next_funding_time": obj.and_then(|o| o.get("nextFundingTime")).cloned().unwrap_or(Value::Null),
        "source": "apex",
        "raw": obj.cloned().unwrap_or(Value::Null),
    });
    if let Err(e) = writeln!(file, "{line}") {
        log::error!("write failed for {label}: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample() -> Value {
        json!({ "data": [ {
            "fundingRate": "0.0000125",
            "indexPrice": "409.14",
            "markPrice": "409.08",
            "lastPrice": "408.94",
            "oraclePrice": "",
            "openInterest": "72.04",
            "predictedFundingRate": "0.0000125",
            "nextFundingTime": "2026-06-15T14:00:00Z",
            "symbol": "TSLAUSDT"
        } ], "timeCost": 1 })
    }

    #[test]
    fn ticker_obj_extracts_first() {
        let b = sample();
        assert_eq!(ticker_obj(&b).unwrap().get("symbol").unwrap(), "TSLAUSDT");
    }

    #[test]
    fn ticker_obj_empty_data_is_none() {
        let b = json!({ "data": [], "timeCost": 1 });
        assert!(ticker_obj(&b).is_none());
    }

    #[test]
    fn fnum_parses_string_number() {
        let b = sample();
        let o = ticker_obj(&b).unwrap();
        assert_eq!(fnum(o, "fundingRate"), Some(0.0000125));
        assert_eq!(fnum(o, "indexPrice"), Some(409.14));
    }

    #[test]
    fn fnum_blank_string_is_none() {
        let b = sample();
        let o = ticker_obj(&b).unwrap();
        assert_eq!(fnum(o, "oraclePrice"), None);
        assert_eq!(fnum(o, "missingKey"), None);
    }
}

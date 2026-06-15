//! RWA spot price logger PoC (bot-strategy#574).
//!
//! Phase 0 (read-only) enabler for the Tier-1 RWA strategies
//! (#562 stock-hours gap, #564 multi-issuer convergence, #568 sector
//! stat-arb). It does NOT place orders — it only polls a price source
//! (Jupiter by default) for a set of tokenized-stock SPL mints and
//! appends one JSONL line per token per tick, mirroring the
//! `market_data_*.jsonl` cadence used by pairtrade so the dumps can be
//! replayed/analyzed the same way.
//!
//! Standalone binary, no feature flags: uses only base deps
//! (reqwest blocking + serde_json + chrono).
//!
//! ## Env vars
//! - `RWA_TOKENS`     (required) — comma-separated `label:mint` pairs,
//!                    e.g. `TSLAx:Xsa…,SPCX:Abc…,SPCXx:Def…`
//! - `RWA_LOG_DIR`    (default `.`)   — output directory for daily JSONL
//! - `RWA_JUP_PRICE_URL` (default `https://lite-api.jup.ag/price/v3`)
//!                    — price endpoint; `ids=<mints>` is appended as query
//! - `RWA_POLL_SECS`  (default `5`)   — poll cadence in seconds
//!
//! ## Output
//! `<RWA_LOG_DIR>/rwa_spot_YYYYMMDD.jsonl`, one line per token per tick:
//! `{"ts":"<rfc3339>","label":"TSLAx","mint":"Xsa…","usd_price":412.3,"source":"jupiter","raw":{…}}`
//!
//! `usd_price` is null when the source omits/!parses the token that tick
//! (logged as a warn) so gaps are visible rather than silently dropped.

use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use rwa_logger::{parse_pairs, resolve_poll_secs};

const DEFAULT_PRICE_URL: &str = "https://lite-api.jup.ag/price/v3";

/// Extract a USD price for `mint` from a Jupiter price response, tolerating
/// both the v3 flat shape (`{ "<mint>": { "usdPrice": <num> } }`) and the
/// v2 nested shape (`{ "data": { "<mint>": { "price": "<num|str>" } } }`).
fn extract_price(body: &Value, mint: &str) -> Option<f64> {
    let node = body
        .get(mint)
        .or_else(|| body.get("data").and_then(|d| d.get(mint)))?;
    // v3: usdPrice (number). v2: price (string or number).
    let raw = node.get("usdPrice").or_else(|| node.get("price"))?;
    match raw {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let tokens_spec = match std::env::var("RWA_TOKENS") {
        Ok(v) => v,
        Err(_) => {
            log::error!("RWA_TOKENS is required (comma-separated label:mint pairs)");
            std::process::exit(2);
        }
    };
    let tokens = match parse_pairs(&tokens_spec) {
        Ok(t) => t,
        Err(e) => {
            log::error!("failed to parse RWA_TOKENS: {e}");
            std::process::exit(2);
        }
    };
    let log_dir = std::env::var("RWA_LOG_DIR").unwrap_or_else(|_| ".".to_string());
    let url = std::env::var("RWA_JUP_PRICE_URL").unwrap_or_else(|_| DEFAULT_PRICE_URL.to_string());
    let poll_secs = resolve_poll_secs(std::env::var("RWA_POLL_SECS").ok().as_deref());

    let ids = tokens
        .iter()
        .map(|(_, m)| m.as_str())
        .collect::<Vec<_>>()
        .join(",");

    log::info!(
        "rwa-spot-logger starting: {} token(s), poll={}s, url={}, dir={}",
        tokens.len(),
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
        match client.get(&url).query(&[("ids", &ids)]).send() {
            Ok(resp) => {
                let status = resp.status();
                match resp.json::<Value>() {
                    Ok(body) if status.is_success() => write_tick(&log_dir, &tokens, &body),
                    Ok(body) => log::warn!("price endpoint returned {status}: {body}"),
                    Err(e) => log::warn!("price endpoint {status}, body not JSON: {e}"),
                }
            }
            Err(e) => log::warn!("price request failed: {e}"),
        }

        // Sleep in short slices so Ctrl-C is responsive.
        let mut slept = 0u64;
        while slept < poll_secs && running.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_secs(1));
            slept += 1;
        }
    }

    log::info!("rwa-spot-logger stopped");
}

fn write_tick(log_dir: &str, tokens: &[(String, String)], body: &Value) {
    let now = chrono::Utc::now();
    let path = format!("{}/rwa_spot_{}.jsonl", log_dir, now.format("%Y%m%d"));
    let mut file = match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            log::error!("cannot open {path}: {e}");
            return;
        }
    };
    let ts = now.to_rfc3339();
    for (label, mint) in tokens {
        let price = extract_price(body, mint);
        if price.is_none() {
            log::warn!("no price for {label} ({mint}) this tick");
        }
        let raw = body
            .get(mint)
            .or_else(|| body.get("data").and_then(|d| d.get(mint)))
            .cloned()
            .unwrap_or(Value::Null);
        let line = json!({
            "ts": ts,
            "label": label,
            "mint": mint,
            "usd_price": price,
            "source": "jupiter",
            "raw": raw,
        });
        if let Err(e) = writeln!(file, "{line}") {
            log::error!("write failed for {label}: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_price_v3_flat() {
        let body = json!({ "MintA": { "usdPrice": 412.3, "decimals": 8 } });
        assert_eq!(extract_price(&body, "MintA"), Some(412.3));
    }

    #[test]
    fn extract_price_v2_nested_string() {
        let body = json!({ "data": { "MintA": { "id": "MintA", "price": "99.5" } } });
        assert_eq!(extract_price(&body, "MintA"), Some(99.5));
    }

    #[test]
    fn extract_price_missing() {
        let body = json!({ "MintB": { "usdPrice": 1.0 } });
        assert_eq!(extract_price(&body, "MintA"), None);
    }
}

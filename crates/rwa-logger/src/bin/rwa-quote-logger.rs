//! RWA executable quote-status logger (bot-strategy#592).
//!
//! Read-only companion to `rwa-spot-logger`: it probes Jupiter Quote API for
//! tokenized-stock SPL mints and writes whether each token is actually
//! swap-routable at small notionals. This separates price-API dislocations from
//! executable opportunities and catches `NO_ROUTE -> ROUTABLE` route-birth
//! events.
//!
//! ## Env vars
//! - `RWA_QUOTE_TOKENS` — comma-separated `label:mint:decimals` entries.
//! - `RWA_QUOTE_NOTIONALS_USD` (default `25,100,500`) — probe sizes.
//! - `RWA_QUOTE_LOG_DIR` (default `.`) — output directory.
//! - `RWA_QUOTE_POLL_SECS` (default `120`).
//! - `RWA_QUOTE_REQUEST_GAP_MS` (default `250`) — gap between Quote API calls.
//! - `RWA_JUP_QUOTE_URL` (default `https://lite-api.jup.ag/swap/v1/quote`).
//! - `RWA_JUP_PRICE_URL` (default `https://lite-api.jup.ag/price/v3`).
//! - `JUPITER_API_KEY` (optional) — sent as `x-api-key` if present.
//!
//! ## Output
//! `<RWA_QUOTE_LOG_DIR>/rwa_quote_YYYYMMDD.jsonl`, one row per token/notional:
//! `quote_status` is `ROUTABLE` only when both sell and buy quotes route.

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::StatusCode;
use serde_json::{json, Value};

const DEFAULT_PRICE_URL: &str = "https://lite-api.jup.ag/price/v3";
const DEFAULT_QUOTE_URL: &str = "https://lite-api.jup.ag/swap/v1/quote";
const DEFAULT_NOTIONALS_USD: &str = "25,100,500";
const DEFAULT_REQUEST_GAP_MS: u64 = 250;
const DEFAULT_QUOTE_POLL_SECS: u64 = 120;
const MIN_QUOTE_POLL_SECS: u64 = 60;
const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const USDC_DECIMALS: u32 = 6;

#[derive(Clone, Debug, PartialEq, Eq)]
struct QuoteToken {
    label: String,
    mint: String,
    decimals: u32,
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let tokens_spec = match std::env::var("RWA_QUOTE_TOKENS") {
        Ok(v) => v,
        Err(_) => {
            log::error!("RWA_QUOTE_TOKENS is required (label:mint:decimals,...)");
            std::process::exit(2);
        }
    };
    let tokens = match parse_quote_tokens(&tokens_spec) {
        Ok(v) => v,
        Err(e) => {
            log::error!("failed to parse RWA_QUOTE_TOKENS: {e}");
            std::process::exit(2);
        }
    };
    let notionals = match parse_notional_usd(
        &std::env::var("RWA_QUOTE_NOTIONALS_USD")
            .unwrap_or_else(|_| DEFAULT_NOTIONALS_USD.to_string()),
    ) {
        Ok(v) => v,
        Err(e) => {
            log::error!("failed to parse RWA_QUOTE_NOTIONALS_USD: {e}");
            std::process::exit(2);
        }
    };

    let log_dir = std::env::var("RWA_QUOTE_LOG_DIR").unwrap_or_else(|_| ".".to_string());
    let price_url =
        std::env::var("RWA_JUP_PRICE_URL").unwrap_or_else(|_| DEFAULT_PRICE_URL.to_string());
    let quote_url =
        std::env::var("RWA_JUP_QUOTE_URL").unwrap_or_else(|_| DEFAULT_QUOTE_URL.to_string());
    let poll_secs = resolve_min_u64_env(
        std::env::var("RWA_QUOTE_POLL_SECS").ok().as_deref(),
        DEFAULT_QUOTE_POLL_SECS,
        MIN_QUOTE_POLL_SECS,
        "RWA_QUOTE_POLL_SECS",
    );
    let request_gap_ms = resolve_u64_env(
        std::env::var("RWA_QUOTE_REQUEST_GAP_MS").ok().as_deref(),
        DEFAULT_REQUEST_GAP_MS,
        "RWA_QUOTE_REQUEST_GAP_MS",
    );
    let api_key = std::env::var("JUPITER_API_KEY").unwrap_or_default();

    log::info!(
        "rwa-quote-logger starting: {} token(s), {} notional(s), poll={}s, request_gap={}ms, dir={}",
        tokens.len(),
        notionals.len(),
        poll_secs,
        request_gap_ms,
        log_dir
    );

    let client = match reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(12))
        .user_agent("rwa-quote-logger/0.1 (+bot-strategy#592)")
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

    let mut last_status: HashMap<String, String> = HashMap::new();
    while running.load(Ordering::SeqCst) {
        let price_body = fetch_price_body(&client, &price_url, &tokens);
        write_tick(
            &client,
            &log_dir,
            &quote_url,
            &api_key,
            &tokens,
            &notionals,
            price_body.as_ref(),
            Duration::from_millis(request_gap_ms),
            &running,
            &mut last_status,
        );

        let mut slept = 0u64;
        while slept < poll_secs && running.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_secs(1));
            slept += 1;
        }
    }

    log::info!("rwa-quote-logger stopped");
}

fn parse_quote_tokens(spec: &str) -> Result<Vec<QuoteToken>, String> {
    let mut out = Vec::new();
    for entry in spec.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let parts: Vec<_> = entry.split(':').map(str::trim).collect();
        if parts.len() != 3 {
            return Err(format!("entry '{entry}' is not label:mint:decimals"));
        }
        if parts[0].is_empty() || parts[1].is_empty() || parts[2].is_empty() {
            return Err(format!("entry '{entry}' has an empty field"));
        }
        let decimals = parts[2]
            .parse::<u32>()
            .map_err(|_| format!("entry '{entry}' has invalid decimals"))?;
        if decimals > 18 {
            return Err(format!("entry '{entry}' decimals too large"));
        }
        out.push(QuoteToken {
            label: parts[0].to_string(),
            mint: parts[1].to_string(),
            decimals,
        });
    }
    if out.is_empty() {
        return Err("parsed to zero entries".to_string());
    }
    Ok(out)
}

fn parse_notional_usd(spec: &str) -> Result<Vec<f64>, String> {
    let mut out = Vec::new();
    for entry in spec.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let n = entry
            .parse::<f64>()
            .map_err(|_| format!("invalid notional '{entry}'"))?;
        if !n.is_finite() || n <= 0.0 {
            return Err(format!("notional '{entry}' must be finite and > 0"));
        }
        out.push(n);
    }
    if out.is_empty() {
        return Err("parsed to zero notionals".to_string());
    }
    Ok(out)
}

fn resolve_u64_env(raw: Option<&str>, default: u64, name: &str) -> u64 {
    match raw {
        None => default,
        Some(s) => match s.trim().parse::<u64>() {
            Ok(n) => n,
            Err(_) => {
                log::warn!("{name}='{s}' is not a valid u64, using default {default}");
                default
            }
        },
    }
}

fn resolve_min_u64_env(raw: Option<&str>, default: u64, min: u64, name: &str) -> u64 {
    let n = resolve_u64_env(raw, default, name);
    if n < min {
        log::warn!("{name}={n} below minimum, clamping to {min}");
        min
    } else {
        n
    }
}

fn fetch_price_body(
    client: &reqwest::blocking::Client,
    price_url: &str,
    tokens: &[QuoteToken],
) -> Option<Value> {
    let ids = tokens
        .iter()
        .map(|t| t.mint.as_str())
        .collect::<Vec<_>>()
        .join(",");
    match client.get(price_url).query(&[("ids", ids)]).send() {
        Ok(resp) => {
            let status = resp.status();
            match resp.json::<Value>() {
                Ok(body) if status.is_success() => Some(body),
                Ok(body) => {
                    log::warn!("price endpoint returned {status}: {body}");
                    None
                }
                Err(e) => {
                    log::warn!("price endpoint {status}, body not JSON: {e}");
                    None
                }
            }
        }
        Err(e) => {
            log::warn!("price request failed: {e}");
            None
        }
    }
}

fn extract_price(body: &Value, mint: &str) -> Option<f64> {
    let node = body
        .get(mint)
        .or_else(|| body.get("data").and_then(|d| d.get(mint)))?;
    let raw = node.get("usdPrice").or_else(|| node.get("price"))?;
    match raw {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn write_tick(
    client: &reqwest::blocking::Client,
    log_dir: &str,
    quote_url: &str,
    api_key: &str,
    tokens: &[QuoteToken],
    notionals: &[f64],
    price_body: Option<&Value>,
    request_gap: Duration,
    running: &AtomicBool,
    last_status: &mut HashMap<String, String>,
) {
    let now = chrono::Utc::now();
    let path = format!("{}/rwa_quote_{}.jsonl", log_dir, now.format("%Y%m%d"));
    let mut file = match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            log::error!("cannot open {path}: {e}");
            return;
        }
    };
    let ts = now.to_rfc3339();

    for token in tokens {
        let usd_price = price_body.and_then(|body| extract_price(body, &token.mint));
        if usd_price.is_none() {
            log::warn!("no price for {} ({}) this tick", token.label, token.mint);
        }

        for notional in notionals {
            let sell_amount =
                usd_price.and_then(|p| raw_token_amount(*notional, p, token.decimals));
            let sell = match sell_amount {
                Some(amount) => quote_side(
                    client,
                    quote_url,
                    api_key,
                    &token.mint,
                    USDC_MINT,
                    amount,
                    USDC_DECIMALS,
                ),
                None => missing_price_side("missing token price for sell amount"),
            };
            sleep_request_gap(request_gap, running);

            let buy_amount = raw_usdc_amount(*notional);
            let buy = quote_side(
                client,
                quote_url,
                api_key,
                USDC_MINT,
                &token.mint,
                buy_amount,
                token.decimals,
            );
            sleep_request_gap(request_gap, running);

            let sell_price = sell
                .get("out_ui")
                .and_then(Value::as_f64)
                .zip(sell_amount)
                .map(|(out_usdc, in_raw)| out_usdc / raw_to_ui(in_raw, token.decimals));
            let buy_price = buy
                .get("out_ui")
                .and_then(Value::as_f64)
                .filter(|out_token| *out_token > 0.0)
                .map(|out_token| notional / out_token);
            let spread_bps = buy_price
                .zip(sell_price)
                .and_then(|(buy_px, sell_px)| spread_bps(buy_px, sell_px));
            let quote_status = merge_quote_status(
                status_of(&sell).unwrap_or("ERROR"),
                status_of(&buy).unwrap_or("ERROR"),
            );
            let key = format!("{}:{notional}", token.label);
            let prev = last_status.get(&key).cloned();
            let changed = prev.as_deref().is_some_and(|p| p != quote_status);
            last_status.insert(key, quote_status.to_string());

            let line = json!({
                "ts": ts,
                "label": token.label,
                "mint": token.mint,
                "source": "jupiter_quote",
                "notional_usd": notional,
                "usd_price": usd_price,
                "quote_status": quote_status,
                "prev_quote_status": prev,
                "quote_status_changed": changed,
                "sell_price": sell_price,
                "buy_price": buy_price,
                "spread_bps": spread_bps,
                "sell": sell,
                "buy": buy,
            });
            if let Err(e) = writeln!(file, "{line}") {
                log::error!("write failed for {} @ ${notional}: {e}", token.label);
            }
        }
    }
}

fn sleep_request_gap(gap: Duration, running: &AtomicBool) {
    if gap.is_zero() || !running.load(Ordering::SeqCst) {
        return;
    }
    std::thread::sleep(gap);
}

fn raw_usdc_amount(notional: f64) -> u64 {
    (notional * 10_f64.powi(USDC_DECIMALS as i32)).round() as u64
}

fn raw_token_amount(notional: f64, price: f64, decimals: u32) -> Option<u64> {
    if !notional.is_finite() || !price.is_finite() || price <= 0.0 {
        return None;
    }
    let raw = (notional / price) * 10_f64.powi(decimals as i32);
    if raw.is_finite() && raw >= 1.0 && raw <= u64::MAX as f64 {
        Some(raw.round() as u64)
    } else {
        None
    }
}

fn raw_to_ui(raw: u64, decimals: u32) -> f64 {
    raw as f64 / 10_f64.powi(decimals as i32)
}

fn spread_bps(buy_price: f64, sell_price: f64) -> Option<f64> {
    let mid = (buy_price + sell_price) / 2.0;
    if mid > 0.0 {
        Some((buy_price - sell_price) / mid * 10_000.0)
    } else {
        None
    }
}

fn quote_side(
    client: &reqwest::blocking::Client,
    quote_url: &str,
    api_key: &str,
    input_mint: &str,
    output_mint: &str,
    amount: u64,
    output_decimals: u32,
) -> Value {
    let started = Instant::now();
    let mut req = client.get(quote_url).query(&[
        ("inputMint", input_mint.to_string()),
        ("outputMint", output_mint.to_string()),
        ("amount", amount.to_string()),
        ("slippageBps", "50".to_string()),
    ]);
    if !api_key.trim().is_empty() {
        req = req.header("x-api-key", api_key.trim());
    }

    match req.send() {
        Ok(resp) => {
            let status = resp.status();
            let text = match resp.text() {
                Ok(t) => t,
                Err(e) => {
                    return json!({
                        "status": "ERROR",
                        "http_status": status.as_u16(),
                        "latency_ms": elapsed_ms(started),
                        "error": format!("body read failed: {e}"),
                    });
                }
            };
            let body = serde_json::from_str::<Value>(&text).ok();
            let quote_status = classify_quote_status(status, body.as_ref(), &text);
            let out_amount = body
                .as_ref()
                .and_then(|b| b.get("outAmount"))
                .and_then(parse_u64_value);
            let out_ui = out_amount.map(|raw| raw_to_ui(raw, output_decimals));
            json!({
                "status": quote_status,
                "http_status": status.as_u16(),
                "latency_ms": elapsed_ms(started),
                "input_mint": input_mint,
                "output_mint": output_mint,
                "in_amount": amount.to_string(),
                "out_amount": out_amount.map(|v| v.to_string()),
                "out_ui": out_ui,
                "price_impact_pct": body.as_ref().and_then(|b| b.get("priceImpactPct")).cloned(),
                "context_slot": body.as_ref().and_then(|b| b.get("contextSlot")).cloned(),
                "time_taken": body.as_ref().and_then(|b| b.get("timeTaken")).cloned(),
                "route": body.as_ref().map(first_route_labels).unwrap_or_default(),
                "error_code": extract_error_code(body.as_ref()),
                "error": extract_error(body.as_ref()).or_else(|| {
                    if quote_status == "ERROR" { Some(text.chars().take(240).collect::<String>()) } else { None }
                }),
            })
        }
        Err(e) => json!({
            "status": "ERROR",
            "latency_ms": elapsed_ms(started),
            "input_mint": input_mint,
            "output_mint": output_mint,
            "in_amount": amount.to_string(),
            "error": e.to_string(),
        }),
    }
}

fn missing_price_side(error: &str) -> Value {
    json!({
        "status": "ERROR",
        "error": error,
    })
}

fn elapsed_ms(started: Instant) -> f64 {
    (started.elapsed().as_secs_f64() * 1000.0 * 100.0).round() / 100.0
}

fn classify_quote_status(status: StatusCode, body: Option<&Value>, text: &str) -> &'static str {
    if status == StatusCode::TOO_MANY_REQUESTS {
        return "RATE_LIMITED";
    }
    if is_no_route(body, text) {
        return "NO_ROUTE";
    }
    if status.is_success()
        && body
            .and_then(|b| b.get("outAmount"))
            .and_then(parse_u64_value)
            .is_some()
    {
        return "ROUTABLE";
    }
    "ERROR"
}

fn is_no_route(body: Option<&Value>, text: &str) -> bool {
    extract_error_code(body)
        .as_deref()
        .is_some_and(|c| c.eq_ignore_ascii_case("NO_ROUTES_FOUND"))
        || text.contains("NO_ROUTES_FOUND")
}

fn parse_u64_value(v: &Value) -> Option<u64> {
    match v {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.parse::<u64>().ok(),
        _ => None,
    }
}

fn extract_error_code(body: Option<&Value>) -> Option<String> {
    body.and_then(|b| b.get("errorCode").or_else(|| b.get("code")))
        .and_then(|v| match v {
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            _ => None,
        })
}

fn extract_error(body: Option<&Value>) -> Option<String> {
    body.and_then(|b| b.get("error").or_else(|| b.get("message")))
        .and_then(|v| match v {
            Value::String(s) => Some(s.clone()),
            _ => None,
        })
}

fn status_of(side: &Value) -> Option<&str> {
    side.get("status").and_then(Value::as_str)
}

fn merge_quote_status(sell: &str, buy: &str) -> &'static str {
    if sell == "ROUTABLE" && buy == "ROUTABLE" {
        "ROUTABLE"
    } else if sell == "RATE_LIMITED" || buy == "RATE_LIMITED" {
        "RATE_LIMITED"
    } else if sell == "NO_ROUTE" && buy == "NO_ROUTE" {
        "NO_ROUTE"
    } else if sell == "ROUTABLE" || buy == "ROUTABLE" {
        "PARTIAL_ROUTABLE"
    } else {
        "ERROR"
    }
}

fn first_route_labels(quote: &Value) -> Vec<String> {
    quote
        .get("routePlan")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|leg| {
            let info = leg.get("swapInfo")?;
            let label = info.get("label")?.as_str()?;
            match leg.get("percent").filter(|pct| !pct.is_null()) {
                Some(pct) => Some(format!("{label}:{pct}")),
                None => Some(label.to_string()),
            }
        })
        .take(6)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_quote_tokens_ok() {
        let got = parse_quote_tokens("SPCX:MintA:6, SPCXx:MintB:8").unwrap();
        assert_eq!(
            got,
            vec![
                QuoteToken {
                    label: "SPCX".to_string(),
                    mint: "MintA".to_string(),
                    decimals: 6
                },
                QuoteToken {
                    label: "SPCXx".to_string(),
                    mint: "MintB".to_string(),
                    decimals: 8
                }
            ]
        );
    }

    #[test]
    fn parse_quote_tokens_rejects_bad_entries() {
        assert!(parse_quote_tokens("").is_err());
        assert!(parse_quote_tokens("SPCX:Mint").is_err());
        assert!(parse_quote_tokens("SPCX:Mint:not-num").is_err());
        assert!(parse_quote_tokens("SPCX:Mint:30").is_err());
    }

    #[test]
    fn parse_notional_usd_ok() {
        assert_eq!(
            parse_notional_usd("25, 100,500").unwrap(),
            vec![25.0, 100.0, 500.0]
        );
    }

    #[test]
    fn resolve_min_u64_env_defaults_and_clamps() {
        assert_eq!(
            resolve_min_u64_env(None, DEFAULT_QUOTE_POLL_SECS, MIN_QUOTE_POLL_SECS, "x"),
            DEFAULT_QUOTE_POLL_SECS
        );
        assert_eq!(
            resolve_min_u64_env(
                Some("abc"),
                DEFAULT_QUOTE_POLL_SECS,
                MIN_QUOTE_POLL_SECS,
                "x"
            ),
            DEFAULT_QUOTE_POLL_SECS
        );
        assert_eq!(
            resolve_min_u64_env(Some("1"), DEFAULT_QUOTE_POLL_SECS, MIN_QUOTE_POLL_SECS, "x"),
            MIN_QUOTE_POLL_SECS
        );
        assert_eq!(
            resolve_min_u64_env(
                Some("90"),
                DEFAULT_QUOTE_POLL_SECS,
                MIN_QUOTE_POLL_SECS,
                "x"
            ),
            90
        );
    }

    #[test]
    fn classify_quote_statuses() {
        assert_eq!(
            classify_quote_status(StatusCode::OK, Some(&json!({"outAmount":"123"})), ""),
            "ROUTABLE"
        );
        assert_eq!(
            classify_quote_status(
                StatusCode::BAD_REQUEST,
                Some(&json!({"errorCode":"NO_ROUTES_FOUND"})),
                ""
            ),
            "NO_ROUTE"
        );
        assert_eq!(
            classify_quote_status(StatusCode::TOO_MANY_REQUESTS, Some(&json!({})), ""),
            "RATE_LIMITED"
        );
    }

    #[test]
    fn merge_statuses() {
        assert_eq!(merge_quote_status("ROUTABLE", "ROUTABLE"), "ROUTABLE");
        assert_eq!(merge_quote_status("NO_ROUTE", "NO_ROUTE"), "NO_ROUTE");
        assert_eq!(
            merge_quote_status("RATE_LIMITED", "NO_ROUTE"),
            "RATE_LIMITED"
        );
        assert_eq!(
            merge_quote_status("ROUTABLE", "NO_ROUTE"),
            "PARTIAL_ROUTABLE"
        );
    }

    #[test]
    fn extract_price_shapes() {
        let flat = json!({"MintA":{"usdPrice": 10.5}});
        let nested = json!({"data":{"MintA":{"price": "9.25"}}});
        assert_eq!(extract_price(&flat, "MintA"), Some(10.5));
        assert_eq!(extract_price(&nested, "MintA"), Some(9.25));
    }

    #[test]
    fn raw_amounts() {
        assert_eq!(raw_usdc_amount(25.0), 25_000_000);
        assert_eq!(raw_token_amount(25.0, 200.0, 8), Some(12_500_000));
        assert_eq!(raw_token_amount(25.0, 0.0, 8), None);
    }

    #[test]
    fn route_labels_include_percent_when_present() {
        let q = json!({
            "routePlan": [
                {"percent": 70, "swapInfo": {"label": "Byreal"}},
                {"swapInfo": {"label": "GoonFi"}}
            ]
        });
        assert_eq!(first_route_labels(&q), vec!["Byreal:70", "GoonFi"]);
    }
}

//! RTT benchmark for Extended Exchange (bot-strategy#60 Phase 3b).
//!
//! Measures:
//!   - REST baseline: GET /info/markets
//!   - Order place ack (signed StarkNet SNIP-12 + server roundtrip)
//!   - Order cancel ack
//!
//! Runs each N times and reports min / p50 / p90 / p99 / max. Each cycle
//! places a post-only limit 50% below mid and cancels immediately, so
//! fill risk is effectively zero.
//!
//! Env vars: same as extended-spike (.env). Additional:
//!   RTT_N              number of cycles (default 20)
//!   RTT_SYMBOL         market (default BTC-USD)
//!   RTT_OFFSET_PCT     price offset (default 50)

use std::env;
use std::time::Instant;

use dex_connector::{create_extended_connector, OrderSide};
use rust_decimal::Decimal;

fn require(var: &str) -> String {
    env::var(var).unwrap_or_else(|_| panic!("{} must be set", var))
}

fn optional(var: &str) -> Option<String> {
    env::var(var).ok().filter(|v| !v.is_empty())
}

async fn decrypt_private_key() -> String {
    let encrypted = require("EXTENDED_PRIVATE_KEY");
    let data_key = require("ENCRYPTED_DATA_KEY").replace(' ', "");
    let bytes = debot_utils::decrypt_data_with_kms(&data_key, encrypted, true)
        .await
        .expect("KMS decrypt failed");
    String::from_utf8(bytes).expect("decrypted key not utf8")
}

fn pct(sorted_ms: &[f64], p: f64) -> f64 {
    if sorted_ms.is_empty() {
        return 0.0;
    }
    let idx = ((sorted_ms.len() as f64 - 1.0) * p).round() as usize;
    sorted_ms[idx.min(sorted_ms.len() - 1)]
}

fn report(label: &str, mut samples: Vec<f64>) {
    if samples.is_empty() {
        println!("  {:<20} n=0 (all failed)", label);
        return;
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let sum: f64 = samples.iter().sum();
    let mean = sum / samples.len() as f64;
    println!(
        "  {:<20} n={:2} min={:6.1} p50={:6.1} p90={:6.1} p99={:6.1} max={:6.1} mean={:6.1}  (ms)",
        label,
        samples.len(),
        samples.first().copied().unwrap_or(0.0),
        pct(&samples, 0.50),
        pct(&samples, 0.90),
        pct(&samples, 0.99),
        samples.last().copied().unwrap_or(0.0),
        mean,
    );
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();

    let api_key = require("EXTENDED_API_KEY");
    let public_key = require("EXTENDED_PUBLIC_KEY");
    let vault: u64 = require("EXTENDED_VAULT").parse().expect("u64 vault");
    let private_key = decrypt_private_key().await;
    let symbol = env::var("RTT_SYMBOL").unwrap_or_else(|_| "BTC-USD".into());
    let offset_pct: Decimal = env::var("RTT_OFFSET_PCT")
        .unwrap_or_else(|_| "50".into())
        .parse()
        .expect("decimal");
    let n: usize = env::var("RTT_N")
        .unwrap_or_else(|_| "20".into())
        .parse()
        .expect("usize");

    println!("── Extended RTT benchmark ──");
    println!("host symbol={} n={} offset={}%", symbol, n, offset_pct);

    let connector = create_extended_connector(
        api_key,
        public_key,
        private_key,
        vault,
        optional("REST_ENDPOINT"),
        optional("WEB_SOCKET_ENDPOINT"),
        vec![symbol.clone()],
    )
    .await
    .expect("connector init");
    connector.start().await.ok();
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Orderbook-driven probe price (re-computed each cycle isn't needed; the
    // market won't move 50% during the run).
    let ob = connector
        .get_order_book(&symbol, 1)
        .await
        .expect("orderbook");
    let bid = ob.bids.first().map(|l| l.price).unwrap_or_default();
    let ask = ob.asks.first().map(|l| l.price).unwrap_or_default();
    let mid = (bid + ask) / Decimal::from(2);
    let probe_price = mid * (Decimal::from(100) - offset_pct) / Decimal::from(100);
    let ticker = connector.get_ticker(&symbol, None).await.expect("ticker");
    let min_size = ticker.min_order.unwrap_or(Decimal::new(1, 4));
    println!(
        "  mid={} probe_price={} size={}  (per cycle notional ≈ ${})",
        mid,
        probe_price,
        min_size,
        probe_price * min_size
    );

    // Note: the DexConnector trait's read methods all serve from WS caches
    // once start() has run, so we can't benchmark REST latency through the
    // trait here. place/cancel are what matter for trading anyway.

    println!("\n[1] place + cancel cycle (signed) …");
    let mut place_rtt = Vec::with_capacity(n);
    let mut cancel_rtt = Vec::with_capacity(n);
    let mut failures = 0usize;

    for i in 0..n {
        let t_place = Instant::now();
        let placed = connector
            .create_order(&symbol, min_size, OrderSide::Long, Some(probe_price), Some(-2), false, None)
            .await;
        let place_ms = t_place.elapsed().as_secs_f64() * 1000.0;

        match placed {
            Ok(order) => {
                place_rtt.push(place_ms);
                let t_cancel = Instant::now();
                match connector.cancel_order(&symbol, &order.order_id).await {
                    Ok(_) => cancel_rtt.push(t_cancel.elapsed().as_secs_f64() * 1000.0),
                    Err(e) => {
                        failures += 1;
                        eprintln!("  cycle {} cancel FAILED: {:?}", i, e);
                    }
                }
            }
            Err(e) => {
                failures += 1;
                eprintln!("  cycle {} place FAILED: {:?}", i, e);
            }
        }

        // Tiny throttle to avoid hammering 1000 req/min headroom
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    println!("\n── Results ──");
    report("place_rtt", place_rtt);
    report("cancel_rtt", cancel_rtt);
    if failures > 0 {
        println!("  failures: {}", failures);
    }

    let _ = connector.stop().await;
}

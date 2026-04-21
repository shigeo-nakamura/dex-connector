//! Phase 1 signature spike for Extended Exchange (bot-strategy#60).
//!
//! Two-pass UX:
//!   pass 1  — loads .env, decrypts private key via KMS, builds connector
//!             (this already exercises `/info/markets` auth), fetches
//!             orderbook, prints the probe order it *would* place, then
//!             exits. No order placement.
//!   pass 2  — same as pass 1, then places a post-only limit far from mid
//!             and immediately cancels. Gated on `SPIKE_CONFIRM=yes` to
//!             prevent accidental orders.

use std::env;

use dex_connector::{create_extended_connector, OrderSide};
use rust_decimal::Decimal;

fn require(var: &str) -> String {
    env::var(var).unwrap_or_else(|_| panic!("{} must be set in .env / env", var))
}

fn optional(var: &str) -> Option<String> {
    env::var(var).ok().filter(|v| !v.is_empty())
}

async fn wait_for<F, Fut>(
    timeout: std::time::Duration,
    mut probe: F,
) -> Option<std::time::Duration>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<()>>,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if probe().await.is_some() {
            return Some(start.elapsed());
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    None
}

async fn decrypt_private_key() -> String {
    let encrypted = require("EXTENDED_PRIVATE_KEY");
    let data_key = require("ENCRYPTED_DATA_KEY").replace(' ', "");
    let bytes = debot_utils::decrypt_data_with_kms(&data_key, encrypted, true)
        .await
        .expect("KMS decrypt of EXTENDED_PRIVATE_KEY failed");
    String::from_utf8(bytes).expect("decrypted key is not utf8")
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let api_key = require("EXTENDED_API_KEY");
    let public_key = require("EXTENDED_PUBLIC_KEY");
    let vault: u64 = require("EXTENDED_VAULT")
        .parse()
        .expect("EXTENDED_VAULT must be u64");
    let symbol = env::var("SPIKE_SYMBOL").unwrap_or_else(|_| "BTC-USD".into());
    let side_str = env::var("SPIKE_SIDE").unwrap_or_else(|_| "buy".into());
    let offset_pct: Decimal = env::var("SPIKE_OFFSET_PCT")
        .unwrap_or_else(|_| "50".into())
        .parse()
        .expect("SPIKE_OFFSET_PCT must be a decimal");
    let confirm = env::var("SPIKE_CONFIRM").unwrap_or_default() == "yes";
    let side = match side_str.to_lowercase().as_str() {
        "buy" | "long" => OrderSide::Long,
        "sell" | "short" => OrderSide::Short,
        other => panic!("SPIKE_SIDE must be buy/sell (got {})", other),
    };

    println!("── Phase 1 spike ──");
    println!("symbol           = {}", symbol);
    println!("side             = {:?}", side);
    println!("offset_pct       = {}%", offset_pct);
    println!("vault            = {}", vault);
    println!("public_key       = {}…", &public_key[..public_key.len().min(12)]);

    println!("\n[1/6] decrypting EXTENDED_PRIVATE_KEY via KMS…");
    let private_key = decrypt_private_key().await;
    println!("      ok (len={})", private_key.len());

    println!("\n[2/6] building ExtendedConnector (also hits /info/markets with X-Api-Key)…");
    let base_url = optional("REST_ENDPOINT");
    let ws_url = optional("WEB_SOCKET_ENDPOINT");
    let connector = create_extended_connector(
        api_key,
        public_key,
        private_key,
        vault,
        base_url,
        ws_url,
        vec![symbol.clone()],
    )
    .await
    .expect("create_extended_connector failed (auth / markets fetch)");
    println!("      ok — auth + markets fetch passed");

    println!("\n[3/6] start() — WS + account stream…");
    connector.start().await.expect("start() failed");
    // Give WS a moment to populate the order book
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    println!("      ok");

    println!("\n[4/6] fetching orderbook for {}…", symbol);
    let ob = connector
        .get_order_book(&symbol, 5)
        .await
        .expect("get_order_book failed");
    let best_bid = ob.bids.first().map(|l| l.price).unwrap_or_default();
    let best_ask = ob.asks.first().map(|l| l.price).unwrap_or_default();
    let mid = if !best_bid.is_zero() && !best_ask.is_zero() {
        (best_bid + best_ask) / Decimal::from(2)
    } else {
        panic!("empty orderbook for {}", symbol);
    };
    println!("      bid={} ask={} mid={}", best_bid, best_ask, mid);

    let ticker = connector
        .get_ticker(&symbol, None)
        .await
        .expect("get_ticker failed");
    let min_size = ticker.min_order.unwrap_or(Decimal::new(1, 4));
    println!(
        "      min_size={:?} tick={:?}",
        ticker.min_order, ticker.min_tick
    );

    let hundred = Decimal::from(100);
    let probe_price = match side {
        OrderSide::Long => mid * (hundred - offset_pct) / hundred,
        OrderSide::Short => mid * (hundred + offset_pct) / hundred,
    };
    println!(
        "\n[5/6] planned order: {:?} {} @ {} (mid={}, offset={}%)",
        side, min_size, probe_price, mid, offset_pct
    );

    if !confirm {
        println!("\n[6/6] SPIKE_CONFIRM!=yes → dry mode, not placing. Re-run with");
        println!("      SPIKE_CONFIRM=yes to place + cancel.");
        let _ = connector.stop().await;
        return;
    }

    println!("\n[6/6] placing post-only limit…");
    let resp = connector
        .create_order(
            &symbol,
            min_size,
            side,
            Some(probe_price),
            Some(-2), // post-only
            false,
            None,
        )
        .await;
    match resp {
        Ok(order) => {
            println!(
                "      ok — order_id={} exch_id={:?} px={} sz={}",
                order.order_id, order.exchange_order_id, order.ordered_price, order.ordered_size
            );

            // Phase 3a: WS account-stream regression. Cache should be populated
            // via the /account WS push, not a REST call triggered by our read.
            println!("\n[ws-check] polling get_open_orders for up to 5s…");
            let appeared = wait_for(std::time::Duration::from_secs(5), || async {
                let open = connector.get_open_orders(&symbol).await.ok()?;
                open.orders
                    .iter()
                    .any(|o| o.order_id == order.order_id)
                    .then_some(())
            })
            .await;
            match appeared {
                Some(elapsed) => println!(
                    "      ✅ order appeared in open_orders cache after {:?}",
                    elapsed
                ),
                None => println!(
                    "      ⚠️  order NOT seen in open_orders cache within 5s (WS account \
                     stream may be down or slow — REST fallback will still work)"
                ),
            }

            println!("\n[ws-check] cancelling…");
            match connector.cancel_order(&symbol, &order.order_id).await {
                Ok(_) => println!("      cancel ack ok"),
                Err(e) => eprintln!("      cancel FAILED: {:?}", e),
            }

            println!("\n[ws-check] polling for cancel to propagate (up to 5s)…");
            let gone = wait_for(std::time::Duration::from_secs(5), || async {
                let open = connector.get_open_orders(&symbol).await.ok()?;
                (!open.orders.iter().any(|o| o.order_id == order.order_id)).then_some(())
            })
            .await;
            match gone {
                Some(elapsed) => println!(
                    "      ✅ order gone from open_orders cache after {:?}",
                    elapsed
                ),
                None => println!("      ⚠️  order still in open_orders after 5s"),
            }

            // Surface final account-stream-driven views
            let filled = connector
                .get_filled_orders(&symbol)
                .await
                .map(|f| f.orders.len())
                .unwrap_or(0);
            let canceled = connector
                .get_canceled_orders(&symbol)
                .await
                .map(|c| c.orders.len())
                .unwrap_or(0);
            let positions = connector
                .get_positions()
                .await
                .map(|p| p.len())
                .unwrap_or(0);
            // Extended collateral is USD (market symbol like "BTC-USD" is not valid here)
            let balance = connector
                .get_balance(Some("USD"))
                .await
                .map(|b| format!("equity={} bal={}", b.equity, b.balance))
                .unwrap_or_else(|e| format!("err: {:?}", e));
            println!(
                "\n[ws-check] account views — filled={} canceled={} positions={} balance=[{}]",
                filled, canceled, positions, balance
            );
        }
        Err(e) => {
            eprintln!("      place FAILED: {:?}", e);
            eprintln!("      → this is the Phase 1 go/no-go signal. If the error");
            eprintln!("        mentions signature / StarknetDomain, starknet-rs v0.17");
            eprintln!("        likely needs bumping.");
        }
    }

    let _ = connector.stop().await;
    println!("\ndone.");
}

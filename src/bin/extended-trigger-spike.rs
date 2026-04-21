//! Verify Extended `create_advanced_trigger_order` end-to-end.
//!
//! Extended's server rejects standalone TPSL orders when there is no
//! position to reduce (error 1137), so the full test is:
//!
//!   1. Open a tiny long position via a crossing limit BUY
//!   2. Wait for the position to appear in the /account WS cache
//!   3. Place a reduce-only SL trigger with slippage control, far below mid
//!   4. Verify it appears in open_orders
//!   5. Cancel the SL
//!   6. Close the position via a crossing limit SELL
//!
//! Minimal notional per pass: ~$7.58 of BTC. Fees ~\$0.02.
//!
//! Gated by `TRIGGER_CONFIRM=yes`. First run is dry and prints the plan.

use std::env;

use dex_connector::{create_extended_connector, DexConnector, OrderSide, TpSl, TriggerOrderStyle};
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

async fn wait_until<F, Fut>(
    label: &str,
    timeout: std::time::Duration,
    mut probe: F,
) -> Option<std::time::Duration>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if probe().await {
            println!("  ✅ {} after {:?}", label, start.elapsed());
            return Some(start.elapsed());
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    println!("  ⚠️  {} not observed within {:?}", label, timeout);
    None
}

async fn has_open_position(conn: &Box<dyn DexConnector>, symbol: &str) -> bool {
    conn.get_positions()
        .await
        .map(|ps| {
            ps.iter()
                .any(|p| p.symbol == symbol && p.size.abs() > Decimal::ZERO)
        })
        .unwrap_or(false)
}

async fn open_orders_contains(
    conn: &Box<dyn DexConnector>,
    symbol: &str,
    order_id: &str,
) -> bool {
    conn.get_open_orders(symbol)
        .await
        .map(|o| o.orders.iter().any(|x| x.order_id == order_id))
        .unwrap_or(false)
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();

    let api_key = require("EXTENDED_API_KEY");
    let public_key = require("EXTENDED_PUBLIC_KEY");
    let vault: u64 = require("EXTENDED_VAULT").parse().expect("u64");
    let private_key = decrypt_private_key().await;

    let symbol = env::var("SPIKE_SYMBOL").unwrap_or_else(|_| "BTC-USD".into());
    let sl_offset_pct: Decimal = env::var("SL_OFFSET_PCT")
        .unwrap_or_else(|_| "5".into()) // within BTC-USD's limit_price_floor/cap
        .parse()
        .expect("decimal");
    let slippage_bps: u32 = env::var("SPIKE_SLIPPAGE_BPS")
        .unwrap_or_else(|_| "500".into())
        .parse()
        .expect("u32");
    let confirm = env::var("TRIGGER_CONFIRM").unwrap_or_default() == "yes";

    println!("── Extended trigger-order end-to-end spike ──");
    println!("symbol={} sl_offset_pct={}% slippage_bps={}", symbol, sl_offset_pct, slippage_bps);

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

    // Clean any stale dangling orders from previous runs so they can't
    // contaminate the fill path below.
    if let Ok(existing) = connector.get_open_orders(&symbol).await {
        if !existing.orders.is_empty() {
            println!(
                "  [cleanup] cancelling {} stale open order(s) before fresh run",
                existing.orders.len()
            );
            for o in &existing.orders {
                let _ = connector.cancel_order(&symbol, &o.order_id).await;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }

    let ob = connector.get_order_book(&symbol, 1).await.expect("ob");
    let best_bid = ob.bids.first().map(|l| l.price).unwrap_or_default();
    let best_ask = ob.asks.first().map(|l| l.price).unwrap_or_default();
    let mid = (best_bid + best_ask) / Decimal::from(2);
    let ticker = connector.get_ticker(&symbol, None).await.expect("ticker");
    let min_size = ticker.min_order.unwrap_or(Decimal::new(1, 4));

    let trigger_px = mid * (Decimal::from(100) - sl_offset_pct) / Decimal::from(100);

    println!(
        "  bid={} ask={} mid={} size={}",
        best_bid, best_ask, mid, min_size
    );
    println!("  planned SL trigger={} (close via SELL, slippage {} bps)", trigger_px, slippage_bps);

    if !confirm {
        println!("\nTRIGGER_CONFIRM!=yes → dry only. Re-run with TRIGGER_CONFIRM=yes.");
        let _ = connector.stop().await;
        return;
    }

    // ── 1. Open position ──
    // Placing at exactly best_ask can sit behind existing asks without
    // crossing. Bump 0.5% above to guarantee a fill regardless of tick size.
    let entry_px = best_ask * Decimal::new(1005, 3);
    println!(
        "\n[1/6] opening tiny long via aggressive limit BUY @ {} (best_ask={})…",
        entry_px, best_ask
    );
    let entry = connector
        .create_order(&symbol, min_size, OrderSide::Long, Some(entry_px), None, false, None)
        .await
        .expect("entry order failed");
    println!("  ok — order_id={}", entry.order_id);

    println!("\n[2/6] waiting for position to appear…");
    let got_pos = wait_until(
        "position opened",
        std::time::Duration::from_secs(10),
        || async { has_open_position(&connector, &symbol).await },
    )
    .await
    .is_some();
    if !got_pos {
        eprintln!("  ❌ no position observed — aborting (check balance / fill status)");
        let _ = connector.stop().await;
        std::process::exit(1);
    }

    // ── 2. Place SL ──
    println!("\n[3/6] placing SL trigger order (reduce_only, 28d expiry)…");
    let sl = match connector
        .create_advanced_trigger_order(
            &symbol,
            min_size,
            OrderSide::Short, // close a long by selling
            trigger_px,
            None,
            TriggerOrderStyle::MarketWithSlippageControl,
            Some(slippage_bps),
            TpSl::Sl,
            true,
            None,
        )
        .await
    {
        Ok(r) => {
            println!(
                "  ok — order_id={} exch_id={:?} ordered_px={}",
                r.order_id, r.exchange_order_id, r.ordered_price
            );
            r
        }
        Err(e) => {
            eprintln!("  ❌ SL place FAILED: {:?}", e);
            eprintln!("  → unwinding position before exit");
            let _ = connector
                .create_order(&symbol, min_size, OrderSide::Short, Some(best_bid), None, true, None)
                .await;
            let _ = connector.stop().await;
            std::process::exit(1);
        }
    };

    println!("\n[4/6] waiting for SL to appear in open_orders cache…");
    wait_until(
        "SL in open_orders",
        std::time::Duration::from_secs(5),
        || async { open_orders_contains(&connector, &symbol, &sl.order_id).await },
    )
    .await;

    // ── 3. Cancel SL ──
    println!("\n[5/6] cancelling SL…");
    match connector.cancel_order(&symbol, &sl.order_id).await {
        Ok(_) => println!("  cancel ok"),
        Err(e) => eprintln!("  cancel FAILED: {:?}", e),
    }
    wait_until(
        "SL removed from open_orders",
        std::time::Duration::from_secs(5),
        || async { !open_orders_contains(&connector, &symbol, &sl.order_id).await },
    )
    .await;

    // ── 4. Close position ──
    let close_px = best_bid * Decimal::new(995, 3); // 0.5% below bid, guaranteed cross
    println!(
        "\n[6/6] closing position via aggressive limit SELL @ {} (best_bid={})…",
        close_px, best_bid
    );
    match connector
        .create_order(&symbol, min_size, OrderSide::Short, Some(close_px), None, true, None)
        .await
    {
        Ok(c) => println!("  ok — close order_id={}", c.order_id),
        Err(e) => eprintln!("  close FAILED: {:?}", e),
    }
    wait_until(
        "position closed",
        std::time::Duration::from_secs(10),
        || async { !has_open_position(&connector, &symbol).await },
    )
    .await;

    let bal = connector.get_balance(Some("USD")).await.ok();
    println!("\nfinal balance: {:?}", bal.map(|b| (b.equity, b.balance)));

    let _ = connector.stop().await;
    println!("done.");
}

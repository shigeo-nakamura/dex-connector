//! Lighter post-only verification spike (bot-strategy#317).
//!
//! Two-pass UX mirroring `extended-spike.rs`:
//!   pass 1  — loads .env, decrypts API keys via KMS (or accepts plain
//!             keys for testing), builds the Lighter connector,
//!             prints the probe order it *would* place, then exits. No
//!             order placement.
//!   pass 2  — same as pass 1, then places a post-only limit far from
//!             mid and immediately cancels. Gated on `SPIKE_CONFIRM=yes`
//!             to prevent accidental orders.
//!
//! ## What this verifies
//!
//! 1. `create_order(symbol, qty, side, Some(price), Some(-2), false, None)`
//!    is honored as TIF_POST_ONLY by the Lighter connector
//!    (lighter_connector::mod.rs:2849-2850 maps -2 → TIF_POST_ONLY).
//! 2. The placed order shows up in `get_open_orders` (i.e. it's
//!    actually resting on the book, not crossed).
//! 3. `cancel_order` removes the resting order cleanly.
//!
//! Without this verification, the xvenue-arb maker-on-Lighter redesign
//! (#309 step 6) cannot flip from DRY_RUN to live with confidence.
//!
//! ## Required env vars
//!
//! Either:
//! - `LIGHTER_PLAIN_PUBLIC_API_KEY` + `LIGHTER_PLAIN_PRIVATE_API_KEY`
//!   (testing only; not used in production)
//!
//! Or:
//! - `LIGHTER_PUBLIC_API_KEY` (KMS-encrypted)
//! - `LIGHTER_PRIVATE_API_KEY` (KMS-encrypted)
//! - `ENCRYPTED_DATA_KEY` (KMS data key)
//!
//! Always required:
//! - `LIGHTER_API_KEY_INDEX` (u32)
//! - `LIGHTER_ACCOUNT_INDEX` (u64)
//!
//! Optional:
//! - `LIGHTER_EVM_WALLET_PRIVATE_KEY` (KMS-encrypted, may be needed
//!   for some account flows)
//! - `REST_ENDPOINT` / `WEB_SOCKET_ENDPOINT` (default to mainnet)
//! - `SPIKE_SYMBOL` (default "ETH")
//! - `SPIKE_SIDE` (default "buy")
//! - `SPIKE_OFFSET_PCT` (default "1.0" — 1% below/above mid so the
//!   post-only doesn't cross)
//! - `SPIKE_NOTIONAL_USD` (default "50" — \$50 notional probe size,
//!   matching the #309 Phase 1 cap)
//! - `SPIKE_CONFIRM` ("yes" to actually place; otherwise dry mode)

use std::env;

use dex_connector::lighter_connector::create_lighter_connector;
use dex_connector::{LighterConnectorConfig, OrderSide};
use rust_decimal::Decimal;

fn require(var: &str) -> String {
    env::var(var).unwrap_or_else(|_| panic!("{} must be set in .env / env", var))
}

fn optional(var: &str) -> Option<String> {
    env::var(var).ok().filter(|v| !v.is_empty())
}

async fn wait_for<F, Fut>(timeout: std::time::Duration, mut probe: F) -> Option<std::time::Duration>
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

async fn decrypt_kms(encrypted: &str) -> String {
    let data_key = require("ENCRYPTED_DATA_KEY").replace(' ', "");
    let bytes = debot_utils::decrypt_data_with_kms(&data_key, encrypted.to_string(), true)
        .await
        .expect("KMS decrypt failed");
    String::from_utf8(bytes).expect("decrypted value is not utf8")
}

async fn resolve_keys() -> (String, String, Option<String>) {
    // Prefer plain keys if both are set (testing path); otherwise
    // decrypt the KMS-encrypted production keys.
    let plain_pub = optional("LIGHTER_PLAIN_PUBLIC_API_KEY");
    let plain_priv = optional("LIGHTER_PLAIN_PRIVATE_API_KEY");
    let api_key_public;
    let api_private_key;

    if let (Some(pp), Some(pv)) = (plain_pub, plain_priv) {
        println!("      using PLAIN keys (testing mode)");
        api_key_public = pp;
        api_private_key = pv;
    } else {
        let enc_pub = require("LIGHTER_PUBLIC_API_KEY");
        let enc_priv = require("LIGHTER_PRIVATE_API_KEY");
        println!("      decrypting via KMS…");
        api_key_public = decrypt_kms(&enc_pub).await;
        api_private_key = decrypt_kms(&enc_priv).await;
    }

    let evm_wallet = if let Some(enc_evm) = optional("LIGHTER_EVM_WALLET_PRIVATE_KEY") {
        Some(decrypt_kms(&enc_evm).await)
    } else {
        None
    };

    (api_key_public, api_private_key, evm_wallet)
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let api_key_index: u32 = require("LIGHTER_API_KEY_INDEX")
        .parse()
        .expect("LIGHTER_API_KEY_INDEX must be u32");
    let account_index: u64 = require("LIGHTER_ACCOUNT_INDEX")
        .parse()
        .expect("LIGHTER_ACCOUNT_INDEX must be u64");

    let symbol = env::var("SPIKE_SYMBOL").unwrap_or_else(|_| "ETH".into());
    let side_str = env::var("SPIKE_SIDE").unwrap_or_else(|_| "buy".into());
    let offset_pct: Decimal = env::var("SPIKE_OFFSET_PCT")
        .unwrap_or_else(|_| "1.0".into())
        .parse()
        .expect("SPIKE_OFFSET_PCT must be a decimal");
    let notional_usd: Decimal = env::var("SPIKE_NOTIONAL_USD")
        .unwrap_or_else(|_| "50".into())
        .parse()
        .expect("SPIKE_NOTIONAL_USD must be a decimal");
    let confirm = env::var("SPIKE_CONFIRM").unwrap_or_default() == "yes";
    let side = match side_str.to_lowercase().as_str() {
        "buy" | "long" => OrderSide::Long,
        "sell" | "short" => OrderSide::Short,
        other => panic!("SPIKE_SIDE must be buy/sell (got {})", other),
    };

    println!("── #317 Lighter post-only spike ──");
    println!("symbol            = {}", symbol);
    println!("side              = {:?}", side);
    println!("offset_pct        = {}%", offset_pct);
    println!("notional_usd      = ${}", notional_usd);
    println!("api_key_index     = {}", api_key_index);
    println!("account_index     = {}", account_index);

    println!("\n[1/6] resolving keys…");
    let (api_key_public, api_private_key_hex, evm_wallet_private_key) = resolve_keys().await;
    println!(
        "      ok (api_key_public.len={}, api_private_key.len={}, evm_present={})",
        api_key_public.len(),
        api_private_key_hex.len(),
        evm_wallet_private_key.is_some()
    );

    println!("\n[2/6] building LighterConnector…");
    let base_url = optional("REST_ENDPOINT")
        .unwrap_or_else(|| "https://mainnet.zklighter.elliot.ai/".to_string());
    let websocket_url = optional("WEB_SOCKET_ENDPOINT")
        .unwrap_or_else(|| "wss://mainnet.zklighter.elliot.ai/stream".to_string());
    let cfg = LighterConnectorConfig {
        api_key_public,
        api_key_index,
        api_private_key_hex,
        evm_wallet_private_key,
        account_index,
        base_url,
        websocket_url,
        tracked_symbols: vec![symbol.clone()],
        ob_stale_secs: None,
    };
    let connector = create_lighter_connector(cfg).expect("create_lighter_connector failed");
    println!("      ok");

    println!("\n[3/6] start() — WS + account stream…");
    connector.start().await.expect("start() failed");
    // Give WS a moment to populate the order book.
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
    println!(
        "      min_size={:?} tick={:?}",
        ticker.min_order, ticker.min_tick
    );

    // Convert notional → qty using current mid.
    let mut qty = (notional_usd / mid).round_dp(8);
    if let Some(min_order) = ticker.min_order {
        if qty < min_order {
            println!(
                "      qty {} < min_order {} → bumping to min_order",
                qty, min_order
            );
            qty = min_order;
        }
    }

    let hundred = Decimal::from(100);
    // Park the post-only price away from touch so it doesn't accidentally
    // cross. For Long (buy) we sit BELOW best_bid; for Short (sell) we
    // sit ABOVE best_ask.
    let probe_price = match side {
        OrderSide::Long => best_bid * (hundred - offset_pct) / hundred,
        OrderSide::Short => best_ask * (hundred + offset_pct) / hundred,
    };
    let probe_price = probe_price.round_dp(2);
    println!(
        "\n[5/6] planned post_only order: {:?} qty={} @ {} (mid={}, offset={}%)",
        side, qty, probe_price, mid, offset_pct
    );

    if !confirm {
        println!("\n[6/6] SPIKE_CONFIRM!=yes → dry mode, not placing. Re-run with");
        println!("      SPIKE_CONFIRM=yes to place + cancel.");
        let _ = connector.stop().await;
        return;
    }

    println!("\n[6/6] placing post_only limit (spread=-2 → TIF_POST_ONLY)…");
    let resp = connector
        .create_order(
            &symbol,
            qty,
            side,
            Some(probe_price),
            Some(-2), // post_only
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

            println!("\n[verify] polling get_open_orders for up to 5s…");
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
                    "      ✅ post_only resting: order appeared in open_orders cache after {:?}",
                    elapsed
                ),
                None => {
                    eprintln!(
                        "      ⚠️  order NOT seen in open_orders within 5s — \
                         the order may have crossed (= NOT post_only behavior!) \
                         or the WS account stream is lagging."
                    );
                    eprintln!("      Inspect manually before assuming the spike passed.");
                }
            }

            println!("\n[verify] cancelling…");
            match connector.cancel_order(&symbol, &order.order_id).await {
                Ok(_) => println!("      cancel ack ok"),
                Err(e) => eprintln!("      cancel FAILED: {:?}", e),
            }

            println!("\n[verify] polling for cancel to propagate (up to 5s)…");
            let gone = wait_for(std::time::Duration::from_secs(5), || async {
                let open = connector.get_open_orders(&symbol).await.ok()?;
                (!open.orders.iter().any(|o| o.order_id == order.order_id)).then_some(())
            })
            .await;
            match gone {
                Some(elapsed) => println!(
                    "      ✅ cancel propagated: order gone from open_orders after {:?}",
                    elapsed
                ),
                None => eprintln!(
                    "      ⚠️  order still in open_orders after 5s — cancel may not have landed"
                ),
            }

            // Snapshot account-level views for completeness.
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
            println!(
                "\n[verify] account views — filled={} canceled={} positions={}",
                filled, canceled, positions
            );

            println!(
                "\n[verify] post_only sanity check: filled count should be 0 \
                      (post_only that crosses would be rejected by Lighter rather \
                      than executing as taker; if you see filled>0 here, investigate)."
            );
        }
        Err(e) => {
            eprintln!("      place FAILED: {:?}", e);
            eprintln!("      → this is the #317 go/no-go signal. If the error mentions");
            eprintln!("        TIF / order_type / signature, the spread=-2 mapping in");
            eprintln!("        lighter_connector::mod.rs may be broken; inspect");
            eprintln!("        TIF_POST_ONLY (= 2) and the create_order limit-order branch.");
        }
    }

    let _ = connector.stop().await;
    println!("\ndone.");
}

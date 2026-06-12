#![cfg(feature = "lighter-sdk")]

// Lighter Protocol Order Type constants
const ORDER_TYPE_LIMIT: u32 = 0;
const ORDER_TYPE_IOC: u32 = 1;
const ORDER_TYPE_TRIGGER: u32 = 2;
// Note: Lighter reuses enum values for certain order-type/TIF combinations.

// Lighter Protocol Side constants (order direction, not position direction)
const SIDE_SELL: u32 = 0; // Sell order (close long positions, open short positions)
const SIDE_BUY: u32 = 1; // Buy order (close short positions, open long positions)

// Lighter Protocol Time-in-Force constants (aligned with Go SDK)
const TIF_IOC: u32 = 0; // Immediate-or-Cancel
const TIF_GTT: u32 = 1; // Good-Till-Time
const TIF_POST_ONLY: u32 = 2; // Post-Only (behaves like GTT but rejects immediate fills)

// Lighter Protocol scaling defaults (will be overridden by market metadata)
const DEFAULT_PRICE_DECIMALS: u32 = 1;
const DEFAULT_SIZE_DECIMALS: u32 = 5;
const MAX_DECIMAL_PRECISION: u32 = 9;
const DEFAULT_ORDERBOOK_STALE_SECS: u64 = 15;

/// Resolve the cross-DexConnector `spread` parameter to a Lighter TIF +
/// adjusted price. Pulled out of `create_order` so the
/// `spread = -2 → TIF_POST_ONLY` mapping (used by xvenue-arb's
/// maker-on-Lighter redesign — bot-strategy#309 / #317) is unit
/// testable without a network round trip.
///
/// Convention (matches Extended; see `extended_connector::mod.rs`):
/// - `Some(s)` with `s >= 0`: tick-based price adjustment, no TIF
///   override. Final price = `price + s * tick_size`. TIF stays at
///   `default_tif`.
/// - `Some(-1)`: caller asked for IOC, but Lighter doesn't honor IOC
///   on resting limits — degrade to GTT and keep the price unchanged.
/// - `Some(-2)`: post-only. TIF flips to `TIF_POST_ONLY`; the venue
///   rejects a cross instead of executing as taker.
/// - `Some(other_negative)`: invalid sentinel; warn + fall back to
///   `default_tif` (no price adjustment).
/// - `None`: no spread, no TIF override. Price unchanged, TIF stays
///   at `default_tif`.
fn resolve_spread_to_tif_and_price(
    price: Decimal,
    spread: Option<i64>,
    tick_size: Decimal,
    default_tif: u32,
) -> (Decimal, u32) {
    match spread {
        Some(s) if s < 0 => {
            let tif = match s {
                -1 => TIF_GTT,       // Treat IOC override as resting GTT on Lighter
                -2 => TIF_POST_ONLY, // Post-only (resting limit)
                _ => {
                    log::warn!("Invalid TIF spread value: {}, using default", s);
                    default_tif
                }
            };
            (price, tif) // No price adjustment for TIF sentinels
        }
        Some(s) => {
            let spread_amount = Decimal::from(s) * tick_size;
            (price + spread_amount, default_tif)
        }
        None => (price, default_tif),
    }
}

/// Configuration for creating a LighterConnector.
#[derive(Debug, Clone)]
pub struct LighterConnectorConfig {
    pub api_key_public: String,
    pub api_key_index: u32,
    pub api_private_key_hex: String,
    pub evm_wallet_private_key: Option<String>,
    pub account_index: u64,
    pub base_url: String,
    pub websocket_url: String,
    pub tracked_symbols: Vec<String>,
    /// Seconds before a cached order book snapshot is considered stale.
    /// Default: 15 seconds.
    pub ob_stale_secs: Option<u64>,
}

use crate::{
    dex_connector::{string_to_decimal, DexConnector},
    dex_request::DexError,
    BalanceResponse, CanceledOrder, CanceledOrdersResponse, CombinedBalanceResponse,
    CreateOrderResponse, FilledOrder, FilledOrdersResponse, LastTrade, LastTradesResponse,
    OpenOrder, OpenOrdersResponse, OrderBookLevel, OrderBookSnapshot, OrderSide, PositionSnapshot,
    SpotAssetBalance, TickerResponse, TpSl, TriggerOrderStyle,
};
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use rust_decimal::{prelude::FromStr, RoundingStrategy};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

/// Determine buy/sell direction for SL/TP orders based on position direction
/// Returns true for buy orders, false for sell orders
/// - Long position SL/TP = Sell order (close position) = false
/// - Short position SL/TP = Buy order (close position) = true
fn is_buy_for_tpsl(position_side: OrderSide) -> bool {
    matches!(position_side, OrderSide::Short)
}

fn normalize_symbol(symbol: &str) -> String {
    let upper = symbol.trim().to_ascii_uppercase();

    // Spot symbols contain "/" (e.g. "LIT/USDC") — preserve as-is
    if upper.contains('/') {
        return upper;
    }

    // Perp symbols: strip common suffixes to get canonical name
    let mut normalized = upper
        .replace("-PERP", "")
        .replace("_PERP", "")
        .replace(".PERP", "")
        .replace("-USD", "")
        .replace("_USD", "")
        .replace("-USDC", "")
        .replace("_USDC", "");
    if normalized.ends_with("-PERP") {
        normalized = normalized.trim_end_matches("-PERP").to_string();
    }
    normalized
}

// Cryptographic imports
use libc::{c_int, c_longlong};
use std::ffi::{CStr, CString};
use tokio::time::sleep;

mod account;
mod constructor;
mod dex_impl;
mod ffi;
mod lifecycle;
mod maintenance;
mod market_cache;
mod models;
mod order_payload;
mod orders;
mod outage_detector;
mod parsing;
mod rest;
mod signing;
mod ticker;
mod ws;
use ffi::{parse_signed_tx_response, CheckClient, CreateClient, SignChangePubKey};
pub use ffi::{SignedTxResponse, StrOrErr};
use maintenance::{
    fetch_next_maintenance_window_with, maintenance_ttl_mins, maintenance_within_window,
    MaintenanceInfo, MAINTENANCE_BACKOFF_429_MINS, MAINTENANCE_BACKOFF_OTHER_MINS,
};
#[cfg(test)]
use market_cache::MarketInfo;
use market_cache::{MarketCache, MARKET_CACHE, MARKET_CACHE_INIT_LOCK};
use models::{
    ApiKeyResponse, LighterAccountInfo, LighterAccountResponse, LighterFundingRates,
    LighterOrderBookCacheEntry, LighterOrderBookDetailsResponse, LighterOrderBooksResponse,
    LighterTradesResponse,
};
use outage_detector::{OutageDetector, OutageSignal, OutageTransition};
use parsing::{
    calculate_min_tick, map_side, parse_cancel_order_index, scale_decimal_to_u32,
    scale_decimal_to_u64,
};
use rest::track_api_call;

/// Bundled account-side cache shared between REST (`get_balance` /
/// `get_positions`) and WS (`handle_account_update` / fill detection).
///
/// All three fields used to live behind their own `Arc<RwLock<...>>`.
/// A WS fill (bot-strategy#155 / #239) has to invalidate `balance` and
/// keep `collateral` consistent while positions update; bundling them
/// under one lock makes that invalidation atomic and removes one variant
/// of the lock-ordering hazard the bot-strategy#392 audit surfaced.
#[derive(Default)]
struct AccountState {
    positions: Vec<PositionSnapshot>,
    /// `(response, fetched_at)` — `Instant` so callers can expire
    /// entries beyond `BALANCE_CACHE_TTL_SECS`. See bot-strategy#155.
    balance: Option<(BalanceResponse, Instant)>,
    /// Latest known `assets[USDC].margin_balance` (the perp sub-account
    /// collateral). Seeded from REST `/account` + WS
    /// `subscribed/account_all`, then combined with the live
    /// `positions[*].unrealized_pnl` from `update/account_all` to derive
    /// mark-to-market equity without further REST. Invalidated by
    /// WS-fill (the next REST refresh reseeds it). See bot-strategy#239.
    collateral: Option<Decimal>,
}

#[derive(Clone)]
pub struct LighterConnector {
    api_key_public: String,      // X-API-KEY header (from Lighter UI)
    api_key_index: u32,          // api_key_index query param
    api_private_key_hex: String, // API private key for signing (40-byte)
    #[cfg(feature = "lighter-sdk")]
    evm_wallet_private_key: Option<String>, // EVM wallet private key for API key registration
    account_index: u64,          // account_index query param
    base_url: String,
    websocket_url: String,
    _l1_address: String, // derived from wallet for logging purposes
    client: Client,
    filled_orders: Arc<RwLock<HashMap<String, Vec<FilledOrder>>>>,
    canceled_orders: Arc<RwLock<HashMap<String, Vec<CanceledOrder>>>>,
    // Cache for API key data to avoid repeated requests
    cached_server_pubkey: Arc<tokio::sync::RwLock<Option<(String, std::time::Instant)>>>,
    // Latched after the first successful Go-SDK CheckClient() call so that
    // subsequent create_go_client() invocations skip the /api/v1/apikeys REST
    // probe. The API key doesn't change during process lifetime; re-validating
    // on every sendTx let partial-fill reissue bursts 429 the wallet's short
    // window. See bot-strategy#144.
    api_key_validated: Arc<AtomicBool>,
    is_running: Arc<AtomicBool>,
    // Auto-cleanup management
    cleanup_started: Arc<AtomicBool>,
    cleanup_handle: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
    // Background refresher for the Lighter status-page maintenance feed
    // (see bot-strategy#160). One spawn per connector instance; the loop
    // exits when `is_running` flips false.
    maintenance_refresher_started: Arc<AtomicBool>,
    // WebSocket data storage
    current_price: Arc<RwLock<HashMap<String, (Decimal, u64)>>>, // symbol -> (price, timestamp)
    order_book: Arc<RwLock<HashMap<u32, LighterOrderBookCacheEntry>>>,
    maintenance: Arc<RwLock<MaintenanceInfo>>,
    outage_detector: Arc<std::sync::Mutex<OutageDetector>>,
    // WebSocket-based order tracking (no API calls)
    cached_open_orders: Arc<RwLock<HashMap<String, Vec<OpenOrder>>>>, // symbol -> orders
    /// Bundled positions + balance + collateral cache (bot-strategy#392).
    /// One lock so WS-fill invalidation of `balance` and reseed of
    /// `collateral` are atomic with the positions update that motivated
    /// them. See `AccountState`.
    account_state: Arc<RwLock<AccountState>>,
    positions_ready: Arc<AtomicBool>,
    // Connection epoch counter for race detection
    connection_epoch: Arc<AtomicU64>,
    // Market metadata cache for symbol↔market_id resolution
    market_cache: Arc<RwLock<MarketCache>>,
    // Serialize refresh attempts to avoid thundering herd on orderBookDetails
    market_cache_init_lock: Arc<tokio::sync::Mutex<()>>,
    // Symbols requested by caller (for order book subscription)
    tracked_symbols: Vec<String>,
    nonce_cache: Arc<tokio::sync::Mutex<Option<NonceCache>>>,
    nonce_cache_ttl: Duration,
    ob_stale_after: Duration,
    // Per-market funding rate fed by the `market_stats/{market_id}` WS channel
    // (bot-strategy#162). Key: market_id. Value: the `funding_rate` field of
    // the WS payload (the rate at the most recent funding settlement — same
    // semantics the strategy consumed from `/funding-rates` REST previously).
    // Cold-start is an empty map; callers handle the missing-entry case by
    // falling back to `None`, which matches the prior REST error path.
    funding_rate_cache: Arc<RwLock<HashMap<u32, Decimal>>>,
    // Broadcast sender for real-time price updates from WS OB changes
    price_update_tx: tokio::sync::broadcast::Sender<crate::PriceUpdate>,
    // Host-shared weight-based rate limiter (bot-strategy#79). Routes every
    // Lighter REST call through the sidecar daemon (or an in-process fallback
    // bucket) so the per-IP 60k weight/min ceiling is respected even when
    // multiple bots on the same host burst simultaneously after a WS reconnect.
    rate_limiter: crate::lighter_ratelimit::RateLimitClient,
}

#[derive(Clone, Debug)]
struct NonceCache {
    next_nonce: u64,
    last_refresh: Instant,
}

// Lighter-specific cryptographic structures

pub fn create_lighter_connector(
    config: LighterConnectorConfig,
) -> Result<Box<dyn DexConnector>, DexError> {
    let connector = LighterConnector::new(config)?;
    Ok(Box::new(connector))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn parses_plain_numeric_order_id_for_cancel() {
        assert_eq!(parse_cancel_order_index("12345"), Some(12345));
    }

    #[test]
    fn parses_trigger_style_order_id_for_cancel() {
        assert_eq!(
            parse_cancel_order_index("trigger-1762471376097-1"),
            Some(1762471376097)
        );
    }

    #[test]
    fn returns_none_for_unknown_cancel_format() {
        assert_eq!(parse_cancel_order_index("unknown-id"), None);
    }

    #[test]
    fn client_order_id_parsed_as_client_order_index() {
        // When a valid numeric string is provided as client_order_id,
        // it should be used as client_order_index (not the timestamp).
        let cid = Some("1234567890123".to_string());
        let timestamp = 9999999999999u64;
        let client_order_index = cid
            .as_deref()
            .and_then(|id| id.parse::<u64>().ok())
            .unwrap_or(timestamp);
        assert_eq!(client_order_index, 1234567890123u64);
    }

    #[test]
    fn client_order_id_falls_back_to_timestamp() {
        // When client_order_id is None, client_order_index should be the timestamp.
        let cid: Option<String> = None;
        let timestamp = 9999999999999u64;
        let client_order_index = cid
            .as_deref()
            .and_then(|id| id.parse::<u64>().ok())
            .unwrap_or(timestamp);
        assert_eq!(client_order_index, timestamp);
    }

    /// bot-strategy#317: post-only spread sentinel (-2) must map to
    /// TIF_POST_ONLY without adjusting the price. Locks in the
    /// xvenue-arb maker-on-Lighter contract so future refactors of
    /// `create_order` don't silently regress the mapping.
    #[test]
    fn spread_minus_two_maps_to_tif_post_only() {
        let price = Decimal::new(2000, 0);
        let tick = Decimal::new(1, 1); // 0.1
        let (out_price, tif) = resolve_spread_to_tif_and_price(price, Some(-2), tick, TIF_GTT);
        assert_eq!(out_price, price, "post-only must NOT adjust the price");
        assert_eq!(tif, TIF_POST_ONLY);
    }

    /// `Some(-1)` is the IOC sentinel; Lighter degrades it to GTT
    /// (lighter_connector cannot rest IOC orders) — keep that
    /// behaviour wired so a caller asking for IOC doesn't silently
    /// land as something else.
    #[test]
    fn spread_minus_one_degrades_ioc_to_gtt() {
        let price = Decimal::new(2000, 0);
        let tick = Decimal::new(1, 1);
        let (out_price, tif) = resolve_spread_to_tif_and_price(price, Some(-1), tick, TIF_GTT);
        assert_eq!(out_price, price);
        assert_eq!(tif, TIF_GTT);
    }

    /// Other negative spreads are invalid sentinels — fall back to the
    /// caller's default TIF without adjusting price.
    #[test]
    fn unknown_negative_spread_falls_back_to_default_tif() {
        let price = Decimal::new(2000, 0);
        let tick = Decimal::new(1, 1);
        let (out_price, tif) = resolve_spread_to_tif_and_price(price, Some(-7), tick, TIF_IOC);
        assert_eq!(out_price, price);
        assert_eq!(tif, TIF_IOC);
    }

    /// Non-negative spread values are tick-based price adjustments;
    /// TIF stays at the caller's default.
    #[test]
    fn positive_spread_adjusts_price_by_tick_size() {
        let price = Decimal::new(2000, 0);
        let tick = Decimal::new(1, 1); // 0.1
        let (out_price, tif) = resolve_spread_to_tif_and_price(price, Some(3), tick, TIF_GTT);
        assert_eq!(out_price, Decimal::new(20003, 1)); // 2000.3
        assert_eq!(tif, TIF_GTT);
    }

    #[test]
    fn no_spread_passes_through_default_tif_and_price() {
        let price = Decimal::new(2000, 0);
        let tick = Decimal::new(1, 1);
        let (out_price, tif) = resolve_spread_to_tif_and_price(price, None, tick, TIF_GTT);
        assert_eq!(out_price, price);
        assert_eq!(tif, TIF_GTT);
    }

    #[test]
    fn trigger_order_id_is_numeric_and_cancellable() {
        // After CID change, trigger orders return numeric client_order_index
        // instead of "trigger-{ts}-{market}". Verify cancel parsing still works.
        let trigger_order_id = "1762471376097"; // numeric CID
        assert_eq!(
            parse_cancel_order_index(trigger_order_id),
            Some(1762471376097)
        );
    }

    #[tokio::test]
    async fn test_get_open_orders() {
        // Skip test if environment variables are not set
        let api_key_public = match env::var("LIGHTER_PLAIN_PUBLIC_API_KEY") {
            Ok(key) => key,
            Err(_) => {
                println!("Skipping test - LIGHTER_PLAIN_PUBLIC_API_KEY not set");
                return;
            }
        };

        let base_url = env::var("LIGHTER_BASE_URL")
            .unwrap_or_else(|_| "https://mainnet.zklighter.elliot.ai".to_string());

        let account_index = env::var("LIGHTER_ACCOUNT_INDEX")
            .unwrap_or_else(|_| "0".to_string())
            .parse::<u64>()
            .unwrap_or(0);

        // Create connector using the proper constructor
        let connector = match LighterConnector::new(LighterConnectorConfig {
            api_key_public,
            api_key_index: 0,
            api_private_key_hex: "dummy_private_key".to_string(),
            evm_wallet_private_key: None,
            account_index,
            base_url,
            websocket_url: "dummy_websocket_url".to_string(),
            tracked_symbols: Vec::new(),
            ob_stale_secs: None,
        }) {
            Ok(c) => c,
            Err(e) => {
                println!("Failed to create connector: {}", e);
                return;
            }
        };

        // Test get_open_orders
        match connector.get_open_orders("BTC").await {
            Ok(response) => {
                println!(
                    "✅ get_open_orders success: {} orders found",
                    response.orders.len()
                );
                for (i, order) in response.orders.iter().enumerate() {
                    println!("  Order {}: {}", i, order.order_id);
                }
            }
            Err(e) => {
                println!("❌ get_open_orders failed: {}", e);
                panic!("get_open_orders test failed: {}", e);
            }
        }
    }

    // bot-strategy#155: event-sourced equity tracking. An update that carries
    // explicit totals must land in `account_state.balance` as
    // (response, fetched_at) so TTL-aware readers can expire it. Verifies
    // the tuple shape and non-zero timestamp (which matters for
    // try_read_cached_balance staleness checks).
    #[tokio::test]
    async fn account_update_with_explicit_totals_populates_cache_with_timestamp() {
        use serde_json::json;
        use std::str::FromStr;
        use std::sync::atomic::AtomicBool;

        let filled_orders = Arc::new(RwLock::new(HashMap::new()));
        let canceled_orders = Arc::new(RwLock::new(HashMap::new()));
        let cached_open_orders = Arc::new(RwLock::new(HashMap::new()));
        let account_state = Arc::new(RwLock::new(AccountState::default()));
        let positions_ready = Arc::new(AtomicBool::new(false));
        let market_cache = Arc::new(RwLock::new(MarketCache::default()));

        let data = json!({
            "total_asset_value": "999.04",
            "available_balance": "800.00"
        });

        let before = Instant::now();
        LighterConnector::handle_account_update(
            &data,
            "update/account_all",
            &filled_orders,
            &canceled_orders,
            &cached_open_orders,
            &account_state,
            &positions_ready,
            1,
            &market_cache,
            "BTC",
        )
        .await;

        let guard = account_state.read().await;
        let (resp, fetched_at) = guard
            .balance
            .as_ref()
            .expect("explicit totals must populate cache");
        assert_eq!(resp.equity, Decimal::from_str("999.04").unwrap());
        assert_eq!(resp.balance, Decimal::from_str("800.00").unwrap());
        assert!(*fetched_at >= before, "fetched_at must be recent");
    }

    // bot-strategy#155: a WS fill must invalidate `account_state.balance`
    // so the next get_balance(None) goes back to REST and picks up the
    // post-fill realized P&L + fees. We simulate a pre-populated cache,
    // deliver a trades payload, and verify the cache is cleared.
    #[tokio::test]
    async fn ws_fill_invalidates_balance_cache() {
        use serde_json::json;
        use std::str::FromStr;
        use std::sync::atomic::AtomicBool;

        let filled_orders = Arc::new(RwLock::new(HashMap::new()));
        let canceled_orders = Arc::new(RwLock::new(HashMap::new()));
        let cached_open_orders = Arc::new(RwLock::new(HashMap::new()));
        let positions_ready = Arc::new(AtomicBool::new(false));
        let account_state = Arc::new(RwLock::new(AccountState {
            positions: Vec::new(),
            balance: Some((
                BalanceResponse {
                    equity: Decimal::from_str("500.0").unwrap(),
                    balance: Decimal::from_str("500.0").unwrap(),
                    position_entry_price: None,
                    position_sign: None,
                },
                Instant::now(),
            )),
            collateral: None,
        }));

        // Seed market_cache so the fill's market_id=0 resolves to a symbol
        // and the trade parse path engages (otherwise pending_inserts stays
        // empty and we wouldn't exercise the invalidation).
        let market_cache = Arc::new(RwLock::new({
            let mut mc = MarketCache::default();
            mc.by_id.insert(
                0,
                MarketInfo {
                    canonical_symbol: "ETH".to_string(),
                    market_id: 0,
                    price_decimals: 2,
                    size_decimals: 4,
                    min_order: Some(Decimal::from_str("0.01").unwrap()),
                },
            );
            mc
        }));

        let data = json!({
            "account": 522842,
            "trades": {
                "0": [{
                    "ask_id": 1u64,
                    "bid_id": 2u64,
                    "ask_account_id": 522842u64,
                    "bid_account_id": 999u64,
                    "size": "0.5",
                    "price": "3500.00",
                    "trade_id": 42u64
                }]
            },
            "type": "update/account_all"
        });

        LighterConnector::handle_account_update(
            &data,
            "update/account_all",
            &filled_orders,
            &canceled_orders,
            &cached_open_orders,
            &account_state,
            &positions_ready,
            522842,
            &market_cache,
            "ETH",
        )
        .await;

        assert!(
            account_state.read().await.balance.is_none(),
            "WS fill must invalidate balance_cache so the next get_balance fetches fresh equity"
        );
    }

    // bot-strategy#239: subscribed/account_all carries assets[USDC].margin_balance
    // and positions; equity must be derived as collateral + sum(unrealized_pnl).
    // A subsequent update/account_all carries assets:null but updated positions
    // and must keep the previously-cached collateral while refreshing the
    // unrealized-pnl portion of the equity.
    #[tokio::test]
    async fn ws_subscribed_then_update_derives_perp_equity_from_margin_balance_and_pnl() {
        use serde_json::json;
        use std::str::FromStr;
        use std::sync::atomic::AtomicBool;

        let filled_orders = Arc::new(RwLock::new(HashMap::new()));
        let canceled_orders = Arc::new(RwLock::new(HashMap::new()));
        let cached_open_orders = Arc::new(RwLock::new(HashMap::new()));
        let positions_ready = Arc::new(AtomicBool::new(false));
        let account_state = Arc::new(RwLock::new(AccountState::default()));
        let market_cache = Arc::new(RwLock::new(MarketCache::default()));

        // Fixture mirrors Frankfurt 2026-04-25 00:11 UTC REST response (one
        // sub-account, two open positions): collateral 998.96368343036,
        // unrealized_pnl ETH=0.052700 + BTC=0.127140 = 0.179840,
        // total_asset_value 999.1455 — matches to within rounding.
        let subscribed = json!({
            "account": 281474976624818u64,
            "assets": {
                "3": {
                    "asset_id": 3,
                    "balance": "0.000000",
                    "locked_balance": "0.000000",
                    "margin_balance": "998.96368343036",
                    "margin_mode": "disabled",
                    "symbol": "USDC"
                }
            },
            "positions": {
                "0": {
                    "market_id": 0,
                    "symbol": "ETH",
                    "position": "0.0850",
                    "sign": -1,
                    "open_order_count": 0,
                    "avg_entry_price": "2313.40",
                    "unrealized_pnl": "0.052700"
                },
                "1": {
                    "market_id": 1,
                    "symbol": "BTC",
                    "position": "0.00260",
                    "sign": 1,
                    "open_order_count": 0,
                    "avg_entry_price": "77358.5",
                    "unrealized_pnl": "0.127140"
                }
            },
            "type": "subscribed/account_all"
        });

        LighterConnector::handle_account_update(
            &subscribed,
            "subscribed/account_all",
            &filled_orders,
            &canceled_orders,
            &cached_open_orders,
            &account_state,
            &positions_ready,
            281474976624818,
            &market_cache,
            "ETH",
        )
        .await;

        let collateral = Decimal::from_str("998.96368343036").unwrap();
        let pnl_sum = Decimal::from_str("0.179840").unwrap();
        let expected_equity = collateral + pnl_sum;

        {
            let guard = account_state.read().await;
            assert_eq!(
                guard.collateral,
                Some(collateral),
                "subscribed snapshot must seed account_state.collateral from assets[USDC].margin_balance"
            );
            let (resp, _) = guard
                .balance
                .as_ref()
                .expect("subscribed snapshot must populate account_state.balance");
            assert_eq!(resp.balance, collateral, "balance reflects raw collateral");
            assert_eq!(
                resp.equity, expected_equity,
                "equity = collateral + sum(unrealized_pnl)"
            );
        }

        // Now deliver an update with assets:null but updated positions
        // (mark-to-market drift). Equity must update using the SAME
        // account_state.collateral combined with the new pnl_sum.
        let update = json!({
            "account": 281474976624818u64,
            "assets": null,
            "positions": {
                "0": {
                    "market_id": 0,
                    "symbol": "ETH",
                    "position": "0.0850",
                    "sign": -1,
                    "open_order_count": 0,
                    "avg_entry_price": "2313.40",
                    "unrealized_pnl": "1.000000"
                },
                "1": {
                    "market_id": 1,
                    "symbol": "BTC",
                    "position": "0.00260",
                    "sign": 1,
                    "open_order_count": 0,
                    "avg_entry_price": "77358.5",
                    "unrealized_pnl": "2.000000"
                }
            },
            "type": "update/account_all"
        });

        LighterConnector::handle_account_update(
            &update,
            "update/account_all",
            &filled_orders,
            &canceled_orders,
            &cached_open_orders,
            &account_state,
            &positions_ready,
            281474976624818,
            &market_cache,
            "ETH",
        )
        .await;

        let new_pnl_sum = Decimal::from_str("3.000000").unwrap();
        let new_expected_equity = collateral + new_pnl_sum;
        let guard = account_state.read().await;
        assert_eq!(
            guard.collateral,
            Some(collateral),
            "update with assets:null must NOT reset account_state.collateral"
        );
        let (resp, _) = guard
            .balance
            .as_ref()
            .expect("update must refresh account_state.balance");
        assert_eq!(resp.balance, collateral);
        assert_eq!(resp.equity, new_expected_equity);
    }

    // bot-strategy#162: a `market_stats` WS push with a `funding_rate` field
    // must populate the per-market cache keyed by the market_id parsed out of
    // the channel string. Both delimiter shapes the server uses (`/` on
    // subscribe, `:` on push) must resolve to the same market_id.
    //
    // bot-strategy#414: the wire value is percent-per-hour and the cache
    // stores fraction-per-hour, so every assertion below uses `wire / 100`.
    #[tokio::test]
    async fn market_stats_push_populates_funding_rate_cache() {
        use serde_json::json;
        use std::str::FromStr;

        let cache: Arc<RwLock<HashMap<u32, Decimal>>> = Arc::new(RwLock::new(HashMap::new()));

        let push_with_colon = json!({
            "channel": "market_stats:1",
            "type": "update/market_stats",
            "market_stats": {
                "symbol": "BTC",
                "market_id": 1,
                "current_funding_rate": "-0.0032",
                "funding_rate": "-0.0017",
                "funding_timestamp": 1776967200000u64
            }
        });
        LighterConnector::handle_market_stats_update(&push_with_colon, &cache).await;
        assert_eq!(
            cache.read().await.get(&1).copied(),
            Some(Decimal::from_str("-0.000017").unwrap()),
            "push with ':' delimiter must store fraction-per-hour (wire pct/h / 100)"
        );

        let sub_with_slash = json!({
            "channel": "market_stats/2",
            "type": "subscribed/market_stats",
            "market_stats": {
                "symbol": "ETH",
                "market_id": 2,
                "funding_rate": "0.00001"
            }
        });
        LighterConnector::handle_market_stats_update(&sub_with_slash, &cache).await;
        assert_eq!(
            cache.read().await.get(&2).copied(),
            Some(Decimal::from_str("0.0000001").unwrap()),
            "subscribed push with '/' delimiter must also resolve and cache (fraction/h)"
        );

        // A later push for market_id=1 must overwrite (most recent wins).
        let overwrite = json!({
            "channel": "market_stats:1",
            "type": "update/market_stats",
            "market_stats": {
                "symbol": "BTC",
                "market_id": 1,
                "funding_rate": "0.00005"
            }
        });
        LighterConnector::handle_market_stats_update(&overwrite, &cache).await;
        assert_eq!(
            cache.read().await.get(&1).copied(),
            Some(Decimal::from_str("0.0000005").unwrap()),
            "later push must replace the stored value (fraction/h)"
        );
    }

    // bot-strategy#414: regression for the wire-to-cache scale conversion.
    // Anchor on the actual 2026-05-15 BTC observation — wire pushed
    // "0.0007" (percent/h), CSV settled 7e-6 fraction/h — so any future
    // refactor that drops the /100 will fail this test before reaching prod.
    #[tokio::test]
    async fn market_stats_funding_rate_is_normalized_to_fraction_per_hour() {
        use serde_json::json;
        use std::str::FromStr;

        let cache: Arc<RwLock<HashMap<u32, Decimal>>> = Arc::new(RwLock::new(HashMap::new()));
        let push = json!({
            "channel": "market_stats:1",
            "type": "update/market_stats",
            "market_stats": {
                "symbol": "BTC",
                "market_id": 1,
                "funding_rate": "0.0007"
            }
        });
        LighterConnector::handle_market_stats_update(&push, &cache).await;
        let cached = cache
            .read()
            .await
            .get(&1)
            .copied()
            .expect("cache populated");
        assert_eq!(
            cached,
            Decimal::from_str("0.000007").unwrap(),
            "0.0007 pct/h must normalize to 7e-6 fraction/h to match CSV-settled rate"
        );
    }

    // bot-strategy#162: malformed / partial pushes must not poison the cache
    // nor panic. A missing market_id or funding_rate field is skipped silently.
    #[tokio::test]
    async fn market_stats_missing_fields_are_skipped() {
        use serde_json::json;

        let cache: Arc<RwLock<HashMap<u32, Decimal>>> = Arc::new(RwLock::new(HashMap::new()));

        // Channel without a parseable trailing id.
        let bad_channel = json!({
            "channel": "market_stats/",
            "type": "update/market_stats",
            "market_stats": {"funding_rate": "0.001"}
        });
        LighterConnector::handle_market_stats_update(&bad_channel, &cache).await;
        assert!(
            cache.read().await.is_empty(),
            "bad channel must not populate"
        );

        // Payload missing funding_rate entirely.
        let missing_rate = json!({
            "channel": "market_stats:3",
            "type": "update/market_stats",
            "market_stats": {"symbol": "SOL", "market_id": 3}
        });
        LighterConnector::handle_market_stats_update(&missing_rate, &cache).await;
        assert!(
            cache.read().await.is_empty(),
            "missing funding_rate must not populate"
        );

        // Unparseable rate string.
        let bad_rate = json!({
            "channel": "market_stats:4",
            "type": "update/market_stats",
            "market_stats": {"funding_rate": "not-a-number"}
        });
        LighterConnector::handle_market_stats_update(&bad_rate, &cache).await;
        assert!(cache.read().await.is_empty(), "bad rate must not populate");
    }
}

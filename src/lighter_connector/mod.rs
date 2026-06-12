#![cfg(feature = "lighter-sdk")]

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
mod protocol;
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
use market_cache::{MarketCache, MARKET_CACHE, MARKET_CACHE_INIT_LOCK};
use models::{
    ApiKeyResponse, LighterAccountInfo, LighterAccountResponse, LighterFundingRates,
    LighterOrderBookCacheEntry, LighterOrderBookDetailsResponse, LighterOrderBooksResponse,
    LighterTradesResponse,
};
use outage_detector::{OutageDetector, OutageSignal, OutageTransition};
use parsing::{
    calculate_min_tick, map_side, normalize_symbol, parse_cancel_order_index, scale_decimal_to_u32,
    scale_decimal_to_u64,
};
use protocol::{
    resolve_spread_to_tif_and_price, DEFAULT_ORDERBOOK_STALE_SECS, DEFAULT_PRICE_DECIMALS,
    DEFAULT_SIZE_DECIMALS, MAX_DECIMAL_PRECISION, ORDER_TYPE_IOC, ORDER_TYPE_LIMIT,
    ORDER_TYPE_TRIGGER, SIDE_BUY, SIDE_SELL, TIF_GTT, TIF_IOC, TIF_POST_ONLY,
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
mod tests;

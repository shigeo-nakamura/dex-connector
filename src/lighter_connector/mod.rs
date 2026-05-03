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
/// Lighter operates an official Better Stack status page that exposes a
/// public RSS feed of incidents and scheduled maintenance. We fetch this
/// instead of the trading API's `/api/v1/announcement` endpoint, which is
/// rate-limited and WAF-protected (see bot-strategy#15, #32). The RSS feed
/// is on an independent CDN so it does not share rate limits with the
/// trading endpoints.
const LIGHTER_STATUS_FEED_URL: &str = "https://status.lighter.xyz/feed.rss";
/// Default cache TTL for the maintenance check. Override at runtime via the
/// `LIGHTER_MAINTENANCE_TTL_MINS` env var. The status page CDN tolerates
/// frequent polling, so we refresh aggressively to minimize detection
/// latency for newly announced maintenance windows.
const DEFAULT_MAINTENANCE_CACHE_TTL_MINS: i64 = 5;
/// Backoff after a 429 response. The status page CDN should not rate-limit
/// us in practice, but if it does, back off conservatively.
const MAINTENANCE_BACKOFF_429_MINS: i64 = 15;
/// Backoff after a non-429 fetch error (network, parse, etc.). Short
/// because transient network errors are expected to clear quickly.
const MAINTENANCE_BACKOFF_OTHER_MINS: i64 = 5;
const DEFAULT_ORDERBOOK_STALE_SECS: u64 = 15;

/// Read the maintenance check TTL from `LIGHTER_MAINTENANCE_TTL_MINS`.
/// Falls back to `DEFAULT_MAINTENANCE_CACHE_TTL_MINS` if the env var is
/// missing, empty, or unparseable. Negative or zero values are treated as
/// invalid and the default is used (a zero TTL would cause every call to
/// hit the network).
fn maintenance_ttl_mins() -> i64 {
    match std::env::var("LIGHTER_MAINTENANCE_TTL_MINS") {
        Ok(s) => match s.trim().parse::<i64>() {
            Ok(v) if v > 0 => v,
            _ => DEFAULT_MAINTENANCE_CACHE_TTL_MINS,
        },
        Err(_) => DEFAULT_MAINTENANCE_CACHE_TTL_MINS,
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
    dex_request::{DexError, HttpMethod},
    dex_websocket::DexWebSocket,
    BalanceResponse, CanceledOrder, CanceledOrdersResponse, CombinedBalanceResponse, SpotAssetBalance,
    CreateOrderResponse, FilledOrder, FilledOrdersResponse, LastTrade, LastTradesResponse,
    OpenOrder, OpenOrdersResponse, OrderBookLevel, OrderBookSnapshot, OrderSide, PositionSnapshot,
    TickerResponse, TpSl, TriggerOrderStyle,
};
use async_trait::async_trait;
use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveDate, Utc};
use lazy_static::lazy_static;
use regex::Regex;
use reqwest::Client;
use rust_decimal::Decimal;
use rust_decimal::{prelude::FromStr, RoundingStrategy};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    convert::TryFrom,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
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

struct MaintenanceInfo {
    next_start: Option<DateTime<Utc>>,
    last_checked: Option<DateTime<Utc>>,
}

/// One `<item>` extracted from the Lighter status RSS feed.
#[derive(Debug, Clone)]
struct LighterRssItem {
    title: String,
    description: String,
    pub_date: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
struct MarketInfo {
    canonical_symbol: String,
    market_id: u32,
    price_decimals: u32,
    size_decimals: u32,
    min_order: Option<Decimal>,
}

#[derive(Default, Debug)]
struct MarketCache {
    by_symbol: HashMap<String, MarketInfo>,
    by_id: HashMap<u32, MarketInfo>,
}

// Priority message system for WebSocket sending
#[derive(Debug)]
enum OutboundMessage {
    Control(tokio_tungstenite::tungstenite::Message), // High priority: Pong, Close
}

impl OutboundMessage {
    fn into_message(self) -> tokio_tungstenite::tungstenite::Message {
        match self {
            OutboundMessage::Control(msg) => msg,
        }
    }

    fn is_pong(&self) -> bool {
        match self {
            OutboundMessage::Control(tokio_tungstenite::tungstenite::Message::Pong(_)) => true,
            _ => false,
        }
    }
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
use secp256k1::{Message, Secp256k1};
use sha3::{Digest, Keccak256};
use std::ffi::{CStr, CString};
use tokio::time::sleep;
use tokio_tungstenite;

mod ffi;
mod parsing;
pub use ffi::{SignedTxResponse, StrOrErr};
use ffi::{
    parse_signed_tx_response, CheckClient, CreateClient, SignCancelOrder, SignChangePubKey,
    SignCreateOrder,
};
use parsing::{
    calculate_min_tick, map_side, parse_cancel_order_index, parse_canceled_order,
    parse_filled_order, scale_decimal_to_u32, scale_decimal_to_u64, ten_pow, value_to_decimal,
};

/// Global API call counter for monitoring Lighter Protocol rate limits
static API_CALL_COUNTER: AtomicU64 = AtomicU64::new(0);
static API_CALL_TRACKER: std::sync::LazyLock<Mutex<Vec<(Instant, String)>>> =
    std::sync::LazyLock::new(|| Mutex::new(Vec::new()));

/// Process-wide market metadata cache shared across all LighterConnector
/// instances on the same host. Market metadata (`orderBookDetails`,
/// `funding-rates`) is identical across credentials on the same base_url,
/// so multi-instance pairtrade only needs to fetch it ONCE per process
/// instead of N times (one per sub-account). See bot-strategy#135.
static MARKET_CACHE: std::sync::LazyLock<Arc<RwLock<MarketCache>>> =
    std::sync::LazyLock::new(|| Arc::new(RwLock::new(MarketCache::default())));
static MARKET_CACHE_INIT_LOCK: std::sync::LazyLock<Arc<tokio::sync::Mutex<()>>> =
    std::sync::LazyLock::new(|| Arc::new(tokio::sync::Mutex::new(())));

/// Process-wide caches for the get_ticker helper endpoints. Same reasoning
/// as MARKET_CACHE: the data is a global exchange state (not per-account),
/// so all LighterConnector instances in the process should share one copy.
/// Without this, each instance's first get_ticker call misses its own
/// cache and fires a REST, producing an N-way burst at startup. See
/// bot-strategy#135 follow-up.
static CACHED_EXCHANGE_STATS: std::sync::LazyLock<
    Arc<RwLock<Option<(LighterExchangeStats, Instant)>>>,
> = std::sync::LazyLock::new(|| Arc::new(RwLock::new(None)));
/// Track and log API calls for rate limit monitoring
fn track_api_call(endpoint: &str, method: &str) {
    let call_count = API_CALL_COUNTER.fetch_add(1, Ordering::SeqCst) + 1;
    let now = Instant::now();

    // Clean old entries (older than 60 seconds)
    {
        let mut tracker = API_CALL_TRACKER.lock().unwrap();
        tracker.retain(|(time, _)| now.duration_since(*time) < Duration::from_secs(60));
        tracker.push((now, format!("{} {}", method, endpoint)));

        let recent_calls = tracker.len();
        log::info!(
            "[API_TRACKER] #{} {} {} | Recent calls (60s): {} | Rate: {:.1}/min",
            call_count,
            method,
            endpoint,
            recent_calls,
            recent_calls as f64
        );

        // Warn if approaching rate limit
        if recent_calls > 45 {
            log::info!(
                "[API_TRACKER] ⚠️  Approaching rate limit: {}/60 calls in last 60s",
                recent_calls
            );
        }
    }
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
    _ws: Option<DexWebSocket>, // Reserved for future WebSocket implementation
    // WebSocket data storage
    current_price: Arc<RwLock<HashMap<String, (Decimal, u64)>>>, // symbol -> (price, timestamp)
    current_volume: Arc<RwLock<Option<Decimal>>>,
    order_book: Arc<RwLock<HashMap<u32, LighterOrderBookCacheEntry>>>,
    maintenance: Arc<RwLock<MaintenanceInfo>>,
    // WebSocket-based order tracking (no API calls)
    cached_open_orders: Arc<RwLock<HashMap<String, Vec<OpenOrder>>>>, // symbol -> orders
    cached_positions: Arc<RwLock<Vec<PositionSnapshot>>>,
    positions_ready: Arc<AtomicBool>,
    // (response, fetched_at) — Instant so we can expire entries beyond the
    // BALANCE_CACHE_TTL_SECS window. See bot-strategy#155.
    balance_cache: Arc<RwLock<Option<(BalanceResponse, Instant)>>>,
    // Latest known `assets[USDC].margin_balance` (the perp sub-account
    // collateral). Seeded from REST `/account` and from the initial
    // `subscribed/account_all` WS snapshot, then combined with the live
    // `positions[*].unrealized_pnl` from `update/account_all` to derive a
    // mark-to-market equity without further REST calls. Invalidated by
    // WS-fill (see balance_cache invalidation) so the next REST refresh
    // reseeds it. See bot-strategy#239.
    cached_collateral: Arc<RwLock<Option<Decimal>>>,
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
    // Cached exchange stats to reduce REST API calls
    cached_exchange_stats: Arc<RwLock<Option<(LighterExchangeStats, Instant)>>>,
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

#[derive(Deserialize, Debug, Clone)]
struct LighterOrderBook {
    bids: Vec<LighterOrderBookEntry>,
    asks: Vec<LighterOrderBookEntry>,
}

#[derive(Debug, Clone)]
struct LighterOrderBookCacheEntry {
    order_book: LighterOrderBook,
    updated_at: Instant,
}

#[derive(Deserialize, Debug, Clone)]
struct LighterOrderBookEntry {
    price: String,
    size: String,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct LighterAccountResponse {
    code: i32,
    total: i32,
    accounts: Vec<LighterAccountInfo>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct LighterAccountInfo {
    account_index: i64,
    #[serde(default)]
    l1_address: String,
    available_balance: String,
    collateral: String,
    total_asset_value: String,
    positions: Vec<LighterPosition>,
    #[serde(default)]
    assets: Vec<LighterAsset>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct LighterAsset {
    symbol: String,
    asset_id: u32,
    balance: String,
    locked_balance: String,
    #[serde(default)]
    margin_balance: String,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct LighterPosition {
    market_id: u8,
    symbol: String,
    position: String,
    sign: i8,
    open_order_count: u32,
    avg_entry_price: String,
    #[serde(default)]
    unrealized_pnl: String,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct LighterTradesResponse {
    code: i32,
    trades: Vec<LighterTrade>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct LighterTrade {
    trade_id: u64,
    price: String,
    size: String,
    usd_amount: String,
    market_id: u8,
    #[serde(default)]
    side: Option<String>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct LighterExchangeStats {
    code: i32,
    order_book_stats: Vec<LighterOrderBookStats>,
    daily_usd_volume: f64,
    daily_trades_count: u32,
}

impl Clone for LighterExchangeStats {
    fn clone(&self) -> Self {
        Self {
            code: self.code,
            order_book_stats: self.order_book_stats.clone(),
            daily_usd_volume: self.daily_usd_volume,
            daily_trades_count: self.daily_trades_count,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
#[allow(dead_code)]
struct LighterOrderBookStats {
    symbol: String,
    last_trade_price: f64,
    daily_trades_count: u32,
    daily_base_token_volume: f64,
    daily_quote_token_volume: f64,
    daily_price_change: f64,
}

#[derive(Deserialize, Debug, Clone)]
#[allow(dead_code)]
struct LighterFundingRates {
    code: i32,
    funding_rates: Vec<LighterFundingRate>,
}

#[derive(Deserialize, Debug, Clone)]
#[allow(dead_code)]
struct LighterFundingRate {
    market_id: u32,
    exchange: String,
    symbol: String,
    rate: f64,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct LighterOrderBookDetailsResponse {
    code: i32,
    #[serde(rename = "order_book_details")]
    order_book_details: Vec<LighterOrderBookDetail>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct LighterOrderBookDetail {
    market_id: u32,
    symbol: String,
    #[serde(rename = "min_base_amount")]
    min_base_amount: Option<String>,
    #[serde(rename = "supported_price_decimals")]
    supported_price_decimals: Option<u32>,
    #[serde(rename = "supported_size_decimals")]
    supported_size_decimals: Option<u32>,
}

#[derive(Deserialize, Debug)]
struct LighterOrderBooksResponse {
    #[serde(rename = "order_books")]
    order_books: Vec<LighterOrderBookMeta>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct LighterOrderBookMeta {
    market_id: u32,
    symbol: String,
    #[serde(rename = "min_base_amount")]
    min_base_amount: Option<String>,
    #[serde(rename = "supported_price_decimals")]
    supported_price_decimals: Option<u32>,
    #[serde(rename = "supported_size_decimals")]
    supported_size_decimals: Option<u32>,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct LighterNonceResponse {
    nonce: u64,
}

#[derive(Deserialize, Debug)]
struct ApiKeyInfo {
    #[serde(rename = "account_index")]
    #[allow(dead_code)]
    account_index: u64,
    #[serde(rename = "api_key_index")]
    #[allow(dead_code)]
    api_key_index: u32,
    #[allow(dead_code)]
    nonce: u32,
    #[serde(rename = "public_key")]
    public_key: String,
}

#[derive(Deserialize, Debug)]
struct ApiKeyResponse {
    #[allow(dead_code)]
    code: u32,
    #[serde(rename = "api_keys")]
    api_keys: Vec<ApiKeyInfo>,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct LighterOrderResponse {
    order_id: String,
    price: String,
    amount: String,
}

#[allow(dead_code)]
#[derive(Serialize, Debug)]
struct LighterTx {
    tx_type: String,
    ticker: String,
    amount: String,
    price: Option<String>,
    order_type: String,
    time_in_force: String,
}

#[allow(dead_code)]
#[derive(Serialize, Debug)]
struct LighterSignedEnvelope {
    sig: String,
    nonce: u64,
    tx: LighterTx,
}

// Lighter-specific cryptographic structures

impl LighterConnector {
    fn announcement_mentions_downtime(title: &str, content: &str) -> bool {
        let keywords = ["maintenance", "upgrade", "downtime"];
        let lower_title = title.to_ascii_lowercase();
        let lower_content = content.to_ascii_lowercase();

        keywords
            .iter()
            .any(|keyword| lower_title.contains(keyword) || lower_content.contains(keyword))
    }

    fn parse_datetime_from_text(
        title: &str,
        content: &str,
        pub_date_hint: Option<DateTime<Utc>>,
    ) -> Option<DateTime<Utc>> {
        // Examples from announcements:
        // - "We're doing a network upgrade on Sunday November 30th at 12PM UTC."
        // - "November 23, 2025 at 12:00 PM UTC"
        lazy_static! {
            static ref DT_RE: Regex = Regex::new(
                r"(?ix)
                (January|February|March|April|May|June|July|August|September|October|November|December)
                \s+
                (\d{1,2})       # day
                (?:st|nd|rd|th)? # optional ordinal suffix
                (?:,?\s*(\d{4}))? # optional year
                \s+at\s+
                (\d{1,2})       # hour
                (?::(\d{2}))?   # optional minutes
                \s*(AM|PM)?     # optional meridiem (default to 24h if missing)
                \s*UTC
                "
            )
            .unwrap();
        }

        let haystack = format!("{} {}", title, content);
        let caps = DT_RE.captures(&haystack)?;
        let month_str = caps.get(1)?.as_str();
        let day: u32 = caps.get(2)?.as_str().parse().ok()?;
        // When the announcement text omits the year, derive it from the
        // RSS pub_date if available (the year of publication is almost
        // always the year the event occurs in for Lighter status posts).
        // Falling back to "current year" is dangerous because old RSS
        // items get reinterpreted as future events. See bot-strategy#32.
        let (year, year_was_inferred) = if let Some(m) = caps.get(3) {
            (m.as_str().parse().ok()?, false)
        } else if let Some(hint) = pub_date_hint {
            (hint.year(), true)
        } else {
            (Utc::now().year(), true)
        };
        let hour_raw: u32 = caps.get(4)?.as_str().parse().ok()?;
        let minute: u32 = caps
            .get(5)
            .map(|m| m.as_str().parse().unwrap_or(0))
            .unwrap_or(0);
        let meridiem = caps.get(6).map(|m| m.as_str().to_ascii_uppercase());

        let month = match month_str.to_ascii_lowercase().as_str() {
            "january" => 1,
            "february" => 2,
            "march" => 3,
            "april" => 4,
            "may" => 5,
            "june" => 6,
            "july" => 7,
            "august" => 8,
            "september" => 9,
            "october" => 10,
            "november" => 11,
            "december" => 12,
            _ => return None,
        };

        let hour_24 = match meridiem.as_deref() {
            Some("AM") => {
                if hour_raw == 12 {
                    0
                } else {
                    hour_raw
                }
            }
            Some("PM") => {
                if hour_raw == 12 {
                    12
                } else {
                    hour_raw + 12
                }
            }
            None => hour_raw, // assume already 24h format
            _ => return None,
        };

        let naive = NaiveDate::from_ymd_opt(year, month, day)?.and_hms_opt(hour_24, minute, 0)?;
        let dt = DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);

        // Year-rollover heuristic: only apply when the year was inferred
        // *without* a pub_date hint (i.e. fall-back to current-year). With
        // a pub_date hint we trust the publication year — bumping a past
        // date to "next year" would falsely turn a stale RSS item into a
        // future event, which is exactly the bug from bot-strategy#32.
        if year_was_inferred && pub_date_hint.is_none() && dt < Utc::now() {
            let naive_next =
                NaiveDate::from_ymd_opt(year + 1, month, day)?.and_hms_opt(hour_24, minute, 0)?;
            return Some(DateTime::<Utc>::from_naive_utc_and_offset(naive_next, Utc));
        }

        Some(dt)
    }

    fn maintenance_within_window(
        next_start: Option<DateTime<Utc>>,
        now: &DateTime<Utc>,
        hours_ahead: i64,
    ) -> bool {
        if let Some(start) = next_start {
            let horizon = ChronoDuration::hours(hours_ahead.max(0));
            // Lighter advertises typical downtimes of "under 15 minutes".
            // 30min gives a 2x safety margin without locking the bot out
            // of trading for an excessive period after a short upgrade.
            let active_window = ChronoDuration::minutes(30);
            let upcoming = *now <= start && (start - *now) <= horizon;
            let active = *now >= start && (*now - start) <= active_window;
            return upcoming || active;
        }
        false
    }

    /// Decode minimal HTML/XML entities that appear in Better Stack RSS
    /// `<description>` text. We don't need a full HTML decoder — only the
    /// five XML entities plus a couple of common numeric escapes.
    fn decode_xml_entities(s: &str) -> String {
        s.replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&#39;", "'")
            .replace("&apos;", "'")
            .replace("&amp;", "&")
    }

    /// Parse a Better Stack RSS feed body into a list of items. We use
    /// regex rather than a full XML parser because the feed schema is
    /// trivial (flat `<item>` blocks with no nesting we care about) and
    /// we already depend on `regex`. This avoids pulling in `quick-xml`.
    fn parse_lighter_rss(body: &str) -> Vec<LighterRssItem> {
        lazy_static! {
            static ref ITEM_RE: Regex = Regex::new(r"(?s)<item>(.*?)</item>").unwrap();
            static ref TITLE_RE: Regex = Regex::new(r"(?s)<title>(.*?)</title>").unwrap();
            static ref DESC_RE: Regex = Regex::new(r"(?s)<description>(.*?)</description>").unwrap();
            static ref DATE_RE: Regex = Regex::new(r"(?s)<pubDate>(.*?)</pubDate>").unwrap();
        }

        ITEM_RE
            .captures_iter(body)
            .filter_map(|item_caps| {
                let inner = item_caps.get(1)?.as_str();
                let title = TITLE_RE
                    .captures(inner)
                    .and_then(|c| c.get(1))
                    .map(|m| Self::decode_xml_entities(m.as_str().trim()))
                    .unwrap_or_default();
                let description = DESC_RE
                    .captures(inner)
                    .and_then(|c| c.get(1))
                    .map(|m| Self::decode_xml_entities(m.as_str().trim()))
                    .unwrap_or_default();
                let pub_date = DATE_RE
                    .captures(inner)
                    .and_then(|c| c.get(1))
                    .and_then(|m| {
                        DateTime::parse_from_rfc2822(m.as_str().trim())
                            .ok()
                            .map(|dt| dt.with_timezone(&Utc))
                    });

                if title.is_empty() && description.is_empty() {
                    None
                } else {
                    Some(LighterRssItem {
                        title,
                        description,
                        pub_date,
                    })
                }
            })
            .collect()
    }

    async fn fetch_next_maintenance_window_with(
        client: &Client,
    ) -> Result<Option<DateTime<Utc>>, DexError> {
        let url = LIGHTER_STATUS_FEED_URL;

        let response = client.get(url).send().await.map_err(|e| {
            DexError::Other(format!("Failed to fetch Lighter status feed: {}", e))
        })?;

        if !response.status().is_success() {
            return Err(DexError::Other(format!(
                "Lighter status feed returned HTTP {}",
                response.status()
            )));
        }

        let body = response.text().await.map_err(|e| {
            DexError::Other(format!("Failed to read Lighter status feed body: {}", e))
        })?;

        let items = Self::parse_lighter_rss(&body);
        let now = Utc::now();

        log::info!(
            "Lighter maintenance fetch ok: url={} items={}",
            url,
            items.len()
        );

        let mut upcoming: Vec<DateTime<Utc>> = items
            .into_iter()
            .filter(|item| Self::announcement_mentions_downtime(&item.title, &item.description))
            .filter_map(|item| {
                if let Some(dt) =
                    Self::parse_datetime_from_text(&item.title, &item.description, item.pub_date)
                {
                    if dt >= now {
                        return Some(dt);
                    }
                    log::debug!(
                        "Lighter maintenance parse: parsed datetime {} already past, skipping (title={})",
                        dt,
                        item.title
                    );
                }

                // Fall back to pubDate. RSS pubDate is *publication* time, not
                // event time, so this only catches cases where the body text
                // didn't parse — better to err on the side of warning the bot
                // about a maintenance window than to silently drop it.
                match item.pub_date {
                    Some(dt) if dt >= now => Some(dt),
                    Some(dt) => {
                        log::debug!(
                            "Lighter maintenance skip past pub_date: pub_date={} now={} title={}",
                            dt,
                            now,
                            item.title
                        );
                        None
                    }
                    None => {
                        log::debug!(
                            "Lighter maintenance parse: no pub_date and no parseable text, title={}",
                            item.title
                        );
                        None
                    }
                }
            })
            .collect();

        upcoming.sort();
        let next = upcoming.into_iter().next();

        log::info!(
            "Lighter maintenance result: next_start={:?} now={}",
            next,
            now
        );

        Ok(next)
    }

    async fn refresh_market_cache(&self) -> Result<(), DexError> {
        // If a WAF cooldown is already engaged, short-circuit before issuing
        // any of the 3 REST calls below. Each of those calls would otherwise
        // fail-fast and emit its own WARN, turning one cooldown into ~3
        // WARN lines per attempt. See bot-strategy#128.
        if let Some(remaining) = crate::lighter_waf_cooldown::cooldown_remaining() {
            let deadline = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64 + remaining.as_secs() as i64)
                .unwrap_or(0);
            return Err(DexError::Other(format!(
                "Lighter WAF cooldown active until unix={} (rate-limited)",
                deadline
            )));
        }

        let mut cache = MarketCache::default();
        let mut detail_decimals: HashMap<u32, (u32, u32)> = HashMap::new();

        match self.get_order_book_details().await {
            Ok(details) => {
                for detail in details.order_book_details {
                    let normalized = normalize_symbol(&detail.symbol);
                    if normalized.is_empty() {
                        continue;
                    }

                    let raw_price_decimals = detail.supported_price_decimals.unwrap_or_else(|| {
                        log::warn!(
                            "Missing supported_price_decimals for market_id={}, using default {}",
                            detail.market_id,
                            DEFAULT_PRICE_DECIMALS
                        );
                        DEFAULT_PRICE_DECIMALS
                    });
                    let price_decimals = raw_price_decimals.min(MAX_DECIMAL_PRECISION);
                    if price_decimals != raw_price_decimals {
                        log::warn!(
                            "supported_price_decimals {} for market_id={} exceeds max {}, clamping",
                            raw_price_decimals,
                            detail.market_id,
                            MAX_DECIMAL_PRECISION
                        );
                    }

                    let raw_size_decimals = detail.supported_size_decimals.unwrap_or_else(|| {
                        log::warn!(
                            "Missing supported_size_decimals for market_id={}, using default {}",
                            detail.market_id,
                            DEFAULT_SIZE_DECIMALS
                        );
                        DEFAULT_SIZE_DECIMALS
                    });
                    let size_decimals = raw_size_decimals.min(MAX_DECIMAL_PRECISION);
                    if size_decimals != raw_size_decimals {
                        log::warn!(
                            "supported_size_decimals {} for market_id={} exceeds max {}, clamping",
                            raw_size_decimals,
                            detail.market_id,
                            MAX_DECIMAL_PRECISION
                        );
                    }

                    detail_decimals.insert(detail.market_id, (price_decimals, size_decimals));

                    let min_order = detail
                        .min_base_amount
                        .as_deref()
                        .and_then(|v| Decimal::from_str(v).ok());
                    let info = MarketInfo {
                        canonical_symbol: normalized.clone(),
                        market_id: detail.market_id,
                        price_decimals,
                        size_decimals,
                        min_order,
                    };
                    log::info!(
                        "[MARKET_INFO] Loaded market {}: price_decimals={}, size_decimals={}, min_order={:?}",
                        normalized,
                        price_decimals,
                        size_decimals,
                        min_order
                    );

                    cache.by_symbol.insert(normalized.clone(), info.clone());
                    cache.by_id.insert(detail.market_id, info);
                }
            }
            Err(err) => {
                log::warn!(
                    "Failed to fetch order book details for market cache: {}. Falling back to funding rates only",
                    err
                );
            }
        }

        // Also load spot markets from /api/v1/orderBooks (orderBookDetails only has perps).
        // Perps-only bots (pairtrade, slow-mm) can set LIGHTER_SKIP_SPOT_MARKETS=1 to avoid
        // this call — firing it right after the heavy orderBookDetails response was the
        // single biggest trigger of startup 429 / WAF cooldown (bot-strategy#128 follow-up).
        let skip_spot = std::env::var("LIGHTER_SKIP_SPOT_MARKETS")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if !skip_spot {
            match self.get_order_books_all().await {
                Ok(books) => {
                    for book in books.order_books {
                        let normalized = normalize_symbol(&book.symbol);
                        if normalized.is_empty() || cache.by_symbol.contains_key(&normalized) {
                            continue;
                        }
                        let price_decimals = book.supported_price_decimals
                            .unwrap_or(DEFAULT_PRICE_DECIMALS)
                            .min(MAX_DECIMAL_PRECISION);
                        let size_decimals = book.supported_size_decimals
                            .unwrap_or(DEFAULT_SIZE_DECIMALS)
                            .min(MAX_DECIMAL_PRECISION);
                        let min_order = book.min_base_amount
                            .as_deref()
                            .and_then(|v| Decimal::from_str(v).ok());
                        let info = MarketInfo {
                            canonical_symbol: normalized.clone(),
                            market_id: book.market_id,
                            price_decimals,
                            size_decimals,
                            min_order,
                        };
                        log::info!(
                            "[MARKET_INFO] Loaded market {} (spot): price_decimals={}, size_decimals={}, min_order={:?}",
                            normalized, price_decimals, size_decimals, min_order
                        );
                        detail_decimals.insert(book.market_id, (price_decimals, size_decimals));
                        cache.by_symbol.insert(normalized.clone(), info.clone());
                        cache.by_id.insert(book.market_id, info);
                    }
                }
                Err(err) => {
                    log::warn!("Failed to fetch orderBooks for spot markets: {}", err);
                }
            }
        }

        let funding = self.get_funding_rates().await?;

        for entry in funding.funding_rates {
            let normalized = normalize_symbol(&entry.symbol);
            if normalized.is_empty() {
                continue;
            }

            let (price_decimals, size_decimals) = detail_decimals
                .get(&entry.market_id)
                .copied()
                .unwrap_or((DEFAULT_PRICE_DECIMALS, DEFAULT_SIZE_DECIMALS));

            if !cache.by_symbol.contains_key(&normalized) {
                let info = MarketInfo {
                    canonical_symbol: normalized.clone(),
                    market_id: entry.market_id,
                    price_decimals,
                    size_decimals,
                    min_order: None,
                };
                cache.by_symbol.insert(normalized.clone(), info.clone());
                cache.by_id.insert(entry.market_id, info);
            } else if !cache.by_id.contains_key(&entry.market_id) {
                if let Some(existing) = cache.by_symbol.get(&normalized) {
                    cache.by_id.insert(entry.market_id, existing.clone());
                }
            }
        }

        if cache.by_symbol.is_empty() {
            return Err(DexError::Other(
                "Unable to populate Lighter market metadata cache".to_string(),
            ));
        }

        *self.market_cache.write().await = cache;
        Ok(())
    }

    async fn has_market_metadata(&self) -> bool {
        let cache = self.market_cache.read().await;
        !cache.by_symbol.is_empty()
    }

    async fn ensure_market_metadata_loaded(&self) -> Result<(), DexError> {
        if self.has_market_metadata().await {
            return Ok(());
        }

        let _lock = self.market_cache_init_lock.lock().await;
        if self.has_market_metadata().await {
            return Ok(());
        }

        // 10 attempts with exp backoff capped at 60s gives ~8 minutes total
        // wait window. A 5-attempt / 31s cap was shorter than Lighter WAF
        // cooldowns (can exceed 60s), so startup panicked into a systemd
        // crash loop. See bot-strategy#85 priority-2.
        const MAX_ATTEMPTS: u32 = 10;
        let mut attempt: u32 = 0;
        let mut backoff = Duration::from_secs(1);

        loop {
            // If a WAF cooldown is already active, sleep until it expires
            // (plus a small buffer) instead of burning exp-backoff retries
            // that all fail fast and spam WARN lines. Each failed attempt
            // otherwise produces ~3 WARN lines from refresh_market_cache's
            // inner REST calls. See bot-strategy#128.
            if let Some(remaining) = crate::lighter_waf_cooldown::cooldown_remaining() {
                let wait = remaining + Duration::from_secs(1);
                log::info!(
                    "[MARKET_CACHE] Lighter WAF cooldown active, deferring market metadata fetch for {}s",
                    wait.as_secs()
                );
                sleep(wait).await;
                continue;
            }

            attempt += 1;
            match self.refresh_market_cache().await {
                Ok(_) => {
                    log::info!(
                        "📊 [MARKET_CACHE] Loaded market metadata successfully (attempt {})",
                        attempt
                    );
                    return Ok(());
                }
                Err(err) => {
                    log::warn!(
                        "Failed to refresh market metadata (attempt {}): {}",
                        attempt,
                        err
                    );

                    if attempt >= MAX_ATTEMPTS {
                        return Err(err);
                    }

                    log::info!(
                        "Retrying market metadata fetch in {}s (next attempt {}/{})",
                        backoff.as_secs(),
                        attempt + 1,
                        MAX_ATTEMPTS
                    );
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                }
            }
        }
    }

    async fn resolve_market_info(&self, symbol: &str) -> Result<MarketInfo, DexError> {
        let normalized = normalize_symbol(symbol);
        {
            let cache = self.market_cache.read().await;
            if let Some(info) = cache.by_symbol.get(&normalized) {
                return Ok(info.clone());
            }
        }

        self.ensure_market_metadata_loaded().await?;
        let cache = self.market_cache.read().await;
        cache
            .by_symbol
            .get(&normalized)
            .cloned()
            .ok_or_else(|| DexError::Other(format!("Unknown symbol: {}", symbol)))
    }

    /// Initialize Go client
    #[cfg(feature = "lighter-sdk")]
    async fn create_go_client(&self) -> Result<(), DexError> {
        unsafe {
            let url = CString::new(self.base_url.as_str())
                .map_err(|e| DexError::Other(format!("Invalid URL: {}", e)))?;

            // Use API private key directly (should be 40 bytes / 80 hex chars)
            let private_key_hex = self
                .api_private_key_hex
                .strip_prefix("0x")
                .unwrap_or(&self.api_private_key_hex);

            if private_key_hex.len() != 80 {
                return Err(DexError::Other(format!(
                    "API private key must be 40 bytes (80 hex chars), got: {}",
                    private_key_hex.len()
                )));
            }

            let private_key = CString::new(private_key_hex)
                .map_err(|e| DexError::Other(format!("Invalid private key: {}", e)))?;

            let result = CreateClient(
                url.as_ptr(),
                private_key.as_ptr(),
                304, // chain_id = 304 for mainnet (same as Python SDK)
                self.api_key_index as c_int,
                self.account_index as c_longlong,
            );

            if !result.is_null() {
                let error_cstr = CStr::from_ptr(result);
                let error_msg = error_cstr.to_string_lossy().to_string();
                libc::free(result as *mut libc::c_void);
                return Err(DexError::Other(format!(
                    "CreateClient error: {}",
                    error_msg
                )));
            }

            // Skip the Go-SDK CheckClient() call (which hits /api/v1/apikeys)
            // once we've already validated the key. CreateClient above is
            // memory-only in the Go SDK so repeating it on every sendTx is
            // cheap, but CheckClient's REST probe would otherwise burst the
            // wallet's short-window rate-limit during partial-fill reissue
            // storms. See bot-strategy#144.
            if self.api_key_validated.load(Ordering::Relaxed) {
                return Ok(());
            }

            // Verify the API key is properly registered with Lighter
            let check_result = CheckClient(
                self.api_key_index as c_int,
                self.account_index as c_longlong,
            );

            if !check_result.is_null() {
                let error_cstr = CStr::from_ptr(check_result);
                let error_msg = error_cstr.to_string_lossy().to_string();
                libc::free(check_result as *mut libc::c_void);

                // Go SDK CheckClient translates *any* error from
                // /api/v1/apikeys into "key not registered". Under WAF/429
                // pressure the endpoint legitimately returns 429 → classify
                // as transient RateLimited instead of permanent auth failure
                // so callers do not drop into the re-registration path.
                // See bot-strategy#85 priority-2.
                //
                // On rate-limit: engage the host-shared cooldown (so other
                // callers / next restart see the real state via #148's
                // pre-check) and log at WARN, not ERROR — a transient 429 is
                // not an auth failure and must not feed `auto-error` issue
                // generators. See bot-strategy#151.
                let is_rate_limited = error_msg.contains("Too Many Requests")
                    || error_msg.contains("\"code\":23000");

                if is_rate_limited {
                    log::warn!(
                        "API key validation hit rate limit (transient): {}",
                        error_msg
                    );
                    let cooldown = crate::lighter_waf_cooldown::engage_cooldown(
                        crate::lighter_waf_cooldown::RateLimitSource::ApiRateLimit,
                    );
                    let until = Utc::now().timestamp() + cooldown.as_secs() as i64;
                    return Err(DexError::RateLimited { until_unix: until });
                }

                log::error!("API key validation failed: {}", error_msg);

                // Parse the error message to extract key details
                if error_msg.contains("ownPubKey:") && error_msg.contains("PublicKey:") {
                    if let Some(own_start) = error_msg.find("ownPubKey: ") {
                        if let Some(own_end) = error_msg[own_start + 11..].find(" ") {
                            let own_key = &error_msg[own_start + 11..own_start + 11 + own_end];
                            log::error!(
                                "  Our derived public key (first 8): {}",
                                &own_key[..std::cmp::min(8, own_key.len())]
                            );
                            log::error!(
                                "  Our derived public key (last 8): {}",
                                &own_key[std::cmp::max(0, own_key.len().saturating_sub(8))..]
                            );
                        }
                    }
                    if let Some(resp_start) = error_msg.find("PublicKey:") {
                        if let Some(resp_end) = error_msg[resp_start + 10..].find("}") {
                            let resp_key = &error_msg[resp_start + 10..resp_start + 10 + resp_end];
                            log::error!(
                                "  Server expected public key (first 8): {}",
                                &resp_key[..std::cmp::min(8, resp_key.len())]
                            );
                            log::error!(
                                "  Server expected public key (last 8): {}",
                                &resp_key[std::cmp::max(0, resp_key.len().saturating_sub(8))..]
                            );
                        }
                    }
                }
                // If we have the EVM wallet key, try to update the API key
                #[cfg(feature = "lighter-sdk")]
                if self.evm_wallet_private_key.is_some() {
                    return Err(DexError::ApiKeyRegistrationRequired);
                } else {
                    return Err(DexError::Other(format!(
                        "API key validation failed: {}",
                        error_msg
                    )));
                }

                #[cfg(not(feature = "lighter-sdk"))]
                return Err(DexError::Other(format!(
                    "API key validation failed: {}",
                    error_msg
                )));
            }

            // Latch: subsequent create_go_client() calls skip the /apikeys
            // probe. See bot-strategy#144.
            self.api_key_validated.store(true, Ordering::Relaxed);
            Ok(())
        }
    }

    /// Start auto-cleanup background task for filled orders
    /// Removes orders older than specified duration to prevent memory bloat
    pub fn start_auto_cleanup(&self, cleanup_interval_hours: u64) {
        if self.cleanup_started.swap(true, Ordering::SeqCst) {
            log::debug!("[AUTO_CLEANUP] already started; ignoring.");
            return;
        }

        log::info!(
            "[AUTO_CLEANUP] Starting background task (interval: {}h)",
            cleanup_interval_hours
        );

        let filled_orders = Arc::clone(&self.filled_orders);
        let canceled_orders = Arc::clone(&self.canceled_orders);
        let is_running = Arc::clone(&self.is_running);
        let cleanup_started = Arc::clone(&self.cleanup_started);
        let cleanup_handle = Arc::clone(&self.cleanup_handle);

        let handle = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(cleanup_interval_hours * 3600));
            // Skip the first immediate tick to delay initial cleanup
            interval.tick().await;

            while is_running.load(Ordering::Relaxed) {
                interval.tick().await;

                let mut filled_removed = 0usize;
                let mut canceled_removed = 0usize;

                // Clean up filled orders - simple approach since FilledOrder doesn't have timestamp
                {
                    let mut filled = filled_orders.write().await;
                    for (symbol, orders) in filled.iter_mut() {
                        const KEEP_FILLED_PER_SYMBOL: usize = 50;
                        if orders.len() > KEEP_FILLED_PER_SYMBOL {
                            let remove_count = orders.len() - KEEP_FILLED_PER_SYMBOL;
                            // Assumes first elements are oldest (order insertion maintains chronological order)
                            orders.drain(0..remove_count);
                            filled_removed += remove_count;
                            log::debug!(
                                "🗑️ [AUTO_CLEANUP] Removed {} old filled orders for {} (kept {})",
                                remove_count,
                                symbol,
                                KEEP_FILLED_PER_SYMBOL
                            );
                        }
                    }
                    // Remove empty symbol entries
                    filled.retain(|_, orders| !orders.is_empty());
                }

                // Clean up canceled orders older than 24 hours
                {
                    let mut canceled = canceled_orders.write().await;
                    // NOTE: canceled_timestamp is seconds since epoch (not milliseconds)
                    let cutoff_secs = (Utc::now() - ChronoDuration::hours(24)).timestamp() as u64;

                    for (symbol, orders) in canceled.iter_mut() {
                        let initial_len = orders.len();
                        // Keep orders newer than 24 hours (timestamp > cutoff means newer)
                        orders.retain(|order| order.canceled_timestamp > cutoff_secs);
                        let removed = initial_len.saturating_sub(orders.len());
                        canceled_removed += removed;

                        if removed > 0 {
                            log::debug!(
                                "🗑️ [AUTO_CLEANUP] Removed {} old canceled orders for {}",
                                removed,
                                symbol
                            );
                        }
                    }
                    // Remove empty symbol entries
                    canceled.retain(|_, orders| !orders.is_empty());
                }

                let total_removed = filled_removed + canceled_removed;
                if total_removed > 0 {
                    log::info!(
                        "🗑️ [AUTO_CLEANUP] removed total={} (filled={}, canceled={})",
                        total_removed,
                        filled_removed,
                        canceled_removed
                    );
                }
            }

            // Cleanup on exit: reset state for potential restart
            cleanup_started.store(false, Ordering::SeqCst);
            let mut guard = cleanup_handle.lock().await;
            *guard = None;
            log::info!("🛑 [AUTO_CLEANUP] task exited, ready for restart");
        });

        // Store the handle using async context
        let cleanup_handle_for_storage = Arc::clone(&self.cleanup_handle);
        tokio::spawn(async move {
            let mut guard = cleanup_handle_for_storage.lock().await;
            *guard = Some(handle);
        });
    }

    /// Start the background refresher for the Lighter status-page maintenance
    /// feed. The previous design awaited `fetch_next_maintenance_window`
    /// inline from `is_upcoming_maintenance` (which is on the strategy's hot
    /// `step()` path), so a slow status.lighter.xyz response — that endpoint
    /// is a single-IP backend with no Anycast/CDN failover — could blow the
    /// 7.5s STEP_OVERRUN budget or, with default `connect_timeout(5s)`, leak
    /// a WARN line. Move the fetch off the hot path entirely. See
    /// bot-strategy#160.
    pub fn start_maintenance_refresher(&self) {
        // Operator kill-switch: if maintenance handling is disabled, never
        // spawn the task. `is_upcoming_maintenance` returns false when the
        // cache is empty, which matches the disabled semantics.
        if matches!(
            std::env::var("LIGHTER_MAINTENANCE_DISABLED").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE")
        ) {
            log::info!(
                "[MAINTENANCE] LIGHTER_MAINTENANCE_DISABLED set, skipping refresher spawn"
            );
            return;
        }

        if self.maintenance_refresher_started.swap(true, Ordering::SeqCst) {
            log::debug!("[MAINTENANCE] refresher already started; ignoring.");
            return;
        }

        // Dedicated client with tight timeouts. status.lighter.xyz is a
        // third-party CDN we don't control, so cap the worst-case stall at
        // a few seconds rather than the trading-client's 15s budget. Connect
        // timeout is the long pole — `error trying to connect: operation
        // timed out` was the failure mode observed on 2026-04-22 17:23.
        let client = match Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(5))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                log::warn!(
                    "[MAINTENANCE] failed to build refresher HTTP client: {} — refresher disabled",
                    e
                );
                self.maintenance_refresher_started
                    .store(false, Ordering::SeqCst);
                return;
            }
        };

        let maintenance = Arc::clone(&self.maintenance);
        let is_running = Arc::clone(&self.is_running);

        tokio::spawn(async move {
            log::info!("[MAINTENANCE] background refresher started");
            // Small jitter (0-30s) on first iteration so co-located bots
            // don't all hit the status page CDN at the exact same instant
            // post-restart. Per-iteration sleep handles the steady-state
            // staggering implicitly via wall-clock drift.
            let initial_jitter = Duration::from_secs(rand::random::<u64>() % 31);
            tokio::time::sleep(initial_jitter).await;

            while is_running.load(Ordering::Relaxed) {
                let ttl_mins = maintenance_ttl_mins();
                let backoff_mins = match Self::fetch_next_maintenance_window_with(&client).await {
                    Ok(next_start) => {
                        let now = Utc::now();
                        let mut info = maintenance.write().await;
                        info.next_start = next_start;
                        info.last_checked = Some(now);
                        ttl_mins
                    }
                    Err(err) => {
                        let err_str = format!("{:?}", err);
                        let backoff = if err_str.contains("429") {
                            ttl_mins.max(MAINTENANCE_BACKOFF_429_MINS)
                        } else {
                            MAINTENANCE_BACKOFF_OTHER_MINS
                        };
                        log::warn!(
                            "[MAINTENANCE] refresh failed (backing off {}min): {:?}",
                            backoff,
                            err
                        );
                        backoff
                    }
                };

                let sleep_secs =
                    (backoff_mins.max(1) as u64).saturating_mul(60);
                let mut remaining = sleep_secs;
                // Wake every 5s so a stop() flipping `is_running` doesn't
                // wait the full TTL before the task exits.
                while remaining > 0 && is_running.load(Ordering::Relaxed) {
                    let chunk = remaining.min(5);
                    tokio::time::sleep(Duration::from_secs(chunk)).await;
                    remaining = remaining.saturating_sub(chunk);
                }
            }

            log::info!("[MAINTENANCE] background refresher exited");
        });
    }

    /// Initialize Go client (disabled when lighter-sdk feature is not enabled)
    #[cfg(not(feature = "lighter-sdk"))]
    async fn create_go_client(&self) -> Result<(), DexError> {
        Err(DexError::Other(
            "Lighter Go SDK not available. Build with --features lighter-sdk to enable."
                .to_string(),
        ))
    }

    /// Call Go shared library to generate signature for CreateOrder transaction
    #[cfg(feature = "lighter-sdk")]
    async fn call_go_sign_create_order(
        &self,
        market_index: i32,
        client_order_index: i64,
        base_amount: i64,
        price: i32,
        is_ask: i32,
        order_type: i32,
        time_in_force: i32,
        reduce_only: i32,
        trigger_price: i32,
        order_expiry: i64,
        nonce: i64,
    ) -> Result<String, DexError> {
        // First create the client
        self.create_go_client().await?;

        unsafe {
            let result = SignCreateOrder(
                market_index,
                client_order_index,
                base_amount,
                price,
                is_ask,
                order_type,
                time_in_force,
                reduce_only,
                trigger_price,
                order_expiry,
                nonce,
                self.api_key_index as c_int,
                self.account_index as c_longlong,
            );

            let (tx_info, _) = parse_signed_tx_response(result)?;
            Ok(tx_info)
        }
    }

    /// Call Go shared library to generate signature (disabled when lighter-sdk feature is not enabled)
    #[cfg(not(feature = "lighter-sdk"))]
    async fn call_go_sign_create_order(
        &self,
        _market_index: i32,
        _client_order_index: i64,
        _base_amount: i64,
        _price: i32,
        _is_ask: i32,
        _order_type: i32,
        _time_in_force: i32,
        _reduce_only: i32,
        _trigger_price: i32,
        _order_expiry: i64,
        _nonce: i64,
    ) -> Result<String, DexError> {
        Err(DexError::Other(
            "Lighter Go SDK not available. Build with --features lighter-sdk to enable."
                .to_string(),
        ))
    }

    /// Call Go shared library to sign a cancel order transaction
    #[cfg(feature = "lighter-sdk")]
    async fn call_go_sign_cancel_order(
        &self,
        market_index: i32,
        order_index: i64,
        nonce: i64,
    ) -> Result<String, DexError> {
        self.create_go_client().await?;

        unsafe {
            let result = SignCancelOrder(
                market_index,
                order_index,
                nonce,
                self.api_key_index as c_int,
                self.account_index as c_longlong,
            );

            let (tx_info, _) = parse_signed_tx_response(result)?;
            Ok(tx_info)
        }
    }

    #[cfg(not(feature = "lighter-sdk"))]
    async fn call_go_sign_cancel_order(
        &self,
        _market_index: i32,
        _order_index: i64,
        _nonce: i64,
    ) -> Result<String, DexError> {
        Err(DexError::Other(
            "Lighter Go SDK not available. Build with --features lighter-sdk to enable."
                .to_string(),
        ))
    }

    #[cfg(feature = "lighter-sdk")]
    pub fn new(config: LighterConnectorConfig) -> Result<Self, DexError> {
        let l1_address = "N/A".to_string();
        let ob_stale_secs = config.ob_stale_secs.unwrap_or(DEFAULT_ORDERBOOK_STALE_SECS);

        log::debug!(
            "Creating LighterConnector with API key index: {}, account: {}, ob_stale={}s",
            config.api_key_index,
            config.account_index,
            ob_stale_secs
        );

        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| DexError::Other(format!("Failed to build HTTP client: {}", e)))?;

        Ok(Self {
            api_key_public: config.api_key_public,
            api_key_index: config.api_key_index,
            api_private_key_hex: config.api_private_key_hex,
            evm_wallet_private_key: config.evm_wallet_private_key,
            account_index: config.account_index,
            base_url: config.base_url.clone(),
            websocket_url: config.websocket_url.clone(),
            _l1_address: l1_address,
            client,
            filled_orders: Arc::new(RwLock::new(HashMap::new())),
            canceled_orders: Arc::new(RwLock::new(HashMap::new())),
            cached_server_pubkey: Arc::new(tokio::sync::RwLock::new(None)),
            api_key_validated: Arc::new(AtomicBool::new(false)),
            is_running: Arc::new(AtomicBool::new(false)),
            cleanup_started: Arc::new(AtomicBool::new(false)),
            cleanup_handle: Arc::new(tokio::sync::Mutex::new(None)),
            maintenance_refresher_started: Arc::new(AtomicBool::new(false)),
            _ws: Some(DexWebSocket::new(config.websocket_url)),
            current_price: Arc::new(RwLock::new(HashMap::new())),
            current_volume: Arc::new(RwLock::new(None)),
            order_book: Arc::new(RwLock::new(HashMap::new())),
            maintenance: Arc::new(RwLock::new(MaintenanceInfo {
                next_start: None,
                last_checked: None,
            })),
            cached_open_orders: Arc::new(RwLock::new(HashMap::new())),
            cached_positions: Arc::new(RwLock::new(Vec::new())),
            positions_ready: Arc::new(AtomicBool::new(false)),
            balance_cache: Arc::new(RwLock::new(None)),
            cached_collateral: Arc::new(RwLock::new(None)),
            // Connection epoch counter for race detection
            connection_epoch: Arc::new(AtomicU64::new(0)),
            market_cache: Arc::clone(&MARKET_CACHE),
            market_cache_init_lock: Arc::clone(&MARKET_CACHE_INIT_LOCK),
            tracked_symbols: config.tracked_symbols,
            nonce_cache: Arc::new(tokio::sync::Mutex::new(None)),
            nonce_cache_ttl: Duration::from_secs(30),
            ob_stale_after: Duration::from_secs(ob_stale_secs),
            cached_exchange_stats: Arc::clone(&CACHED_EXCHANGE_STATS),
            funding_rate_cache: Arc::new(RwLock::new(HashMap::new())),
            price_update_tx: tokio::sync::broadcast::channel(128).0,
            rate_limiter: crate::lighter_ratelimit::RateLimitClient::from_env(),
        })
    }

    #[cfg(not(feature = "lighter-sdk"))]
    pub fn new(config: LighterConnectorConfig) -> Result<Self, DexError> {
        let l1_address = "N/A".to_string();
        let ob_stale_secs = config.ob_stale_secs.unwrap_or(DEFAULT_ORDERBOOK_STALE_SECS);

        log::debug!(
            "Creating LighterConnector with API key index: {}, account: {}, ob_stale={}s",
            config.api_key_index,
            config.account_index,
            ob_stale_secs
        );

        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| DexError::Other(format!("Failed to build HTTP client: {}", e)))?;

        Ok(Self {
            api_key_public: config.api_key_public,
            api_key_index: config.api_key_index,
            api_private_key_hex: config.api_private_key_hex,
            account_index: config.account_index,
            base_url: config.base_url.clone(),
            websocket_url: config.websocket_url.clone(),
            _l1_address: l1_address,
            client,
            filled_orders: Arc::new(RwLock::new(HashMap::new())),
            canceled_orders: Arc::new(RwLock::new(HashMap::new())),
            cached_server_pubkey: Arc::new(tokio::sync::RwLock::new(None)),
            api_key_validated: Arc::new(AtomicBool::new(false)),
            is_running: Arc::new(AtomicBool::new(false)),
            cleanup_started: Arc::new(AtomicBool::new(false)),
            cleanup_handle: Arc::new(tokio::sync::Mutex::new(None)),
            maintenance_refresher_started: Arc::new(AtomicBool::new(false)),
            _ws: Some(DexWebSocket::new(config.websocket_url)),
            current_price: Arc::new(RwLock::new(HashMap::new())),
            current_volume: Arc::new(RwLock::new(None)),
            order_book: Arc::new(RwLock::new(HashMap::new())),
            maintenance: Arc::new(RwLock::new(MaintenanceInfo {
                next_start: None,
                last_checked: None,
            })),
            cached_open_orders: Arc::new(RwLock::new(HashMap::new())),
            cached_positions: Arc::new(RwLock::new(Vec::new())),
            positions_ready: Arc::new(AtomicBool::new(false)),
            balance_cache: Arc::new(RwLock::new(None)),
            cached_collateral: Arc::new(RwLock::new(None)),
            connection_epoch: Arc::new(AtomicU64::new(0)),
            market_cache: Arc::clone(&MARKET_CACHE),
            market_cache_init_lock: Arc::clone(&MARKET_CACHE_INIT_LOCK),
            tracked_symbols: config.tracked_symbols,
            nonce_cache: Arc::new(tokio::sync::Mutex::new(None)),
            nonce_cache_ttl: Duration::from_secs(30),
            ob_stale_after: Duration::from_secs(ob_stale_secs),
            cached_exchange_stats: Arc::clone(&CACHED_EXCHANGE_STATS),
            funding_rate_cache: Arc::new(RwLock::new(HashMap::new())),
            price_update_tx: tokio::sync::broadcast::channel(128).0,
            rate_limiter: crate::lighter_ratelimit::RateLimitClient::from_env(),
        })
    }

    /// Get server public key with caching to reduce API calls
    async fn get_server_public_key_cached(&self) -> Result<String, DexError> {
        // Check cache first (valid for 5 minutes)
        {
            let cache = self.cached_server_pubkey.read().await;
            if let Some((pubkey, timestamp)) = &*cache {
                if timestamp.elapsed() < std::time::Duration::from_secs(300) {
                    log::debug!("[API_CACHE] Using cached server public key, no API call needed");
                    return Ok(pubkey.clone());
                }
            }
        }

        // Cache miss or expired - fetch from API
        let endpoint = format!(
            "/api/v1/apikeys?account_index={}&api_key_index={}",
            self.account_index, self.api_key_index
        );
        log::debug!("Getting server public key from: {}", endpoint);
        let response: ApiKeyResponse = self
            .make_request(&endpoint, crate::dex_request::HttpMethod::Get, None)
            .await?;

        if response.api_keys.is_empty() {
            return Err(DexError::Other("No API keys found on server".to_string()));
        }

        let server_pubkey = response.api_keys[0].public_key.clone();

        // Update cache
        {
            let mut cache = self.cached_server_pubkey.write().await;
            *cache = Some((server_pubkey.clone(), std::time::Instant::now()));
        }

        Ok(server_pubkey)
    }

    async fn create_order_native_with_type(
        &self,
        market_id: u32,
        side: u32,
        tif: u32,
        base_amount: u64,
        price: u64,
        client_order_id: Option<String>,
        order_type: u32,
        reduce_only: bool,
        expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        let timestamp = chrono::Utc::now().timestamp_millis() as u64;
        let client_order_index = client_order_id
            .as_deref()
            .and_then(|id| id.parse::<u64>().ok())
            .unwrap_or(timestamp);
        let nonce = self.get_nonce().await?;

        let (price_scale, size_scale, price_decimals, size_decimals) = {
            let cache = self.market_cache.read().await;
            if let Some(info) = cache.by_id.get(&market_id) {
                (
                    ten_pow(info.price_decimals),
                    ten_pow(info.size_decimals),
                    info.price_decimals.min(MAX_DECIMAL_PRECISION),
                    info.size_decimals.min(MAX_DECIMAL_PRECISION),
                )
            } else {
                (
                    ten_pow(DEFAULT_PRICE_DECIMALS),
                    ten_pow(DEFAULT_SIZE_DECIMALS),
                    DEFAULT_PRICE_DECIMALS.min(MAX_DECIMAL_PRECISION),
                    DEFAULT_SIZE_DECIMALS.min(MAX_DECIMAL_PRECISION),
                )
            }
        };

        let approx_price = if price_scale > 0 {
            price as f64 / price_scale as f64
        } else {
            price as f64
        };

        let approx_size = if size_scale > 0 {
            base_amount as f64 / size_scale as f64
        } else {
            base_amount as f64
        };

        log::debug!(
            "Creating native order: market_id={}, side={}, base_amount={}, price={}, approx_price={}, approx_size={}, price_decimals={}, size_decimals={}",
            market_id,
            side,
            base_amount,
            price,
            approx_price,
            approx_size,
            price_decimals,
            size_decimals
        );

        let order_type_param = order_type as u64; // Use passed order type
        let time_in_force = tif as u64; // Use passed time in force
        let reduce_only_param = if reduce_only { 1u64 } else { 0u64 };
        let trigger_price = 0u64; // no trigger
                                  // For market or immediate TIF orders, use 0 (NilOrderExpiry). For GTC orders, use future timestamp
        let is_immediate_tif = time_in_force == u64::from(TIF_IOC);
        let order_expiry = if order_type == ORDER_TYPE_IOC || is_immediate_tif {
            0i64 // NilOrderExpiry for immediate-or-cancel / fill-or-kill orders
        } else {
            // For GTC limit orders, use passed expiry_secs or default to 24 hours
            let expiry_duration_ms = if let Some(expiry_secs) = expiry_secs {
                expiry_secs * 1000 // Convert seconds to milliseconds
            } else {
                24 * 60 * 60 * 1000 // Default 24 hours in milliseconds
            };

            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            now_ms + (expiry_duration_ms as i64)
        };

        // Use actual parameters passed to function instead of hardcoded test values
        let actual_market_id = market_id as u64;
        let actual_base_amount = base_amount;
        let actual_price = price;
        let actual_side = side as u64;

        let _tx_data = [
            actual_market_id,    // market_index - actual parameter
            client_order_index,  // client_order_index
            actual_base_amount,  // base_amount - actual parameter
            actual_price,        // price - actual parameter
            actual_side,         // is_ask - actual parameter
            order_type_param,    // order_type
            time_in_force,       // time_in_force
            reduce_only_param,   // reduce_only
            trigger_price,       // trigger_price
            order_expiry as u64, // order_expiry
            nonce,               // nonce - use actual API nonce
        ];

        // Extract private key bytes (40 bytes for Goldilocks quintic extension)
        let private_key_hex = self
            .api_private_key_hex
            .strip_prefix("0x")
            .unwrap_or(&self.api_private_key_hex);
        let private_key_bytes = hex::decode(private_key_hex)
            .map_err(|e| DexError::Other(format!("Invalid private key hex: {}", e)))?;

        // Lighter uses 40-byte private keys for Goldilocks quintic extension
        let mut key_bytes = [0u8; 40];
        let copy_len = std::cmp::min(private_key_bytes.len(), 40);
        key_bytes[..copy_len].copy_from_slice(&private_key_bytes[..copy_len]);

        // Call Go shared library to generate signature dynamically
        let go_result = self
            .call_go_sign_create_order(
                actual_market_id as i32,
                client_order_index as i64,
                actual_base_amount as i64,
                actual_price as i32,
                actual_side as i32,
                order_type_param as i32,
                time_in_force as i32,
                reduce_only_param as i32,
                trigger_price as i32,
                order_expiry as i64,
                nonce as i64,
            )
            .await?;

        log::debug!("=== GO SDK RESULT ===");
        log::debug!("Go SDK JSON: {}", go_result);

        // Use the exact JSON from Go SDK - it already contains everything correctly

        // Use the complete transaction JSON from Go SDK directly
        let tx_info = go_result;

        // Send to Lighter API using application/x-www-form-urlencoded format (same as Go SDK)
        let form_data = format!(
            "tx_type=14&tx_info={}&price_protection=false",
            urlencoding::encode(&tx_info)
        );

        log::debug!("=== REQUEST DEBUG ===");
        log::debug!("Timestamp: {}", timestamp);
        log::debug!("TX Info JSON: {}", tx_info);
        log::debug!("Form data: {}", form_data);

        track_api_call("POST /api/v1/sendTx", "POST");

        // Order submission path — wait up to 5s for budget rather than drop
        // the order. A small queued delay is vastly preferable to a missed
        // trade. See bot-strategy#79.
        self.acquire_rest_budget(
            "/api/v1/sendTx",
            crate::lighter_ratelimit::AcquirePolicy::Wait { max_ms: 5_000 },
        )
        .await?;

        // Use form-urlencoded format same as Go SDK, without X-API-KEY header
        let response = self
            .client
            .post(&format!("{}/api/v1/sendTx", self.base_url))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(form_data)
            .send()
            .await
            .map_err(|e| DexError::Other(format!("HTTP request failed: {}", e)))?;

        let status = response.status();
        let response_text = response
            .text()
            .await
            .map_err(|e| DexError::Other(format!("Failed to read response: {}", e)))?;

        log::debug!(
            "Native order response: HTTP {}, Body: {}",
            status,
            response_text
        );

        if status.is_success() {
            log::debug!("Native order submitted successfully!");
            // Use client_order_index as order_id for tracking
            let order_id = client_order_index.to_string();

            log::debug!(
                "Created order: order_id={}, client_order_index={}, side={}, size={}",
                order_id,
                client_order_index,
                side,
                i64::try_from(base_amount)
                    .ok()
                    .map(|b| Decimal::new(b, size_decimals))
                    .unwrap_or_else(|| Decimal::ZERO)
            );

            Ok(CreateOrderResponse {
                order_id,
                exchange_order_id: None,
                ordered_price: i64::try_from(price)
                    .ok()
                    .map(|p| Decimal::new(p, price_decimals))
                    .unwrap_or_else(|| Decimal::ZERO),
                ordered_size: i64::try_from(base_amount)
                    .ok()
                    .map(|b| Decimal::new(b, size_decimals))
                    .unwrap_or_else(|| Decimal::ZERO),
                client_order_id: Some(client_order_index.to_string()),
            })
        } else {
            self.invalidate_nonce_cache().await;
            Err(DexError::Other(format!(
                "Order failed: HTTP {}, {}",
                status, response_text
            )))
        }
    }

    async fn create_order_native_with_trigger(
        &self,
        market_id: u32,
        side: u32,
        tif: u32,
        base_amount: u64,
        price: u64,
        trigger_price: u64,
        client_order_id: Option<String>,
        order_type: u32,
        reduce_only: bool,
        expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        let timestamp = chrono::Utc::now().timestamp_millis() as u64;
        let client_order_index = client_order_id
            .as_deref()
            .and_then(|id| id.parse::<u64>().ok())
            .unwrap_or(timestamp);
        let nonce = self.get_nonce().await?;

        let (price_scale, size_scale, price_decimals, size_decimals) = {
            let cache = self.market_cache.read().await;
            if let Some(info) = cache.by_id.get(&market_id) {
                (
                    ten_pow(info.price_decimals),
                    ten_pow(info.size_decimals),
                    info.price_decimals.min(MAX_DECIMAL_PRECISION),
                    info.size_decimals.min(MAX_DECIMAL_PRECISION),
                )
            } else {
                (
                    ten_pow(DEFAULT_PRICE_DECIMALS),
                    ten_pow(DEFAULT_SIZE_DECIMALS),
                    DEFAULT_PRICE_DECIMALS.min(MAX_DECIMAL_PRECISION),
                    DEFAULT_SIZE_DECIMALS.min(MAX_DECIMAL_PRECISION),
                )
            }
        };

        let approx_price = if price_scale > 0 {
            price as f64 / price_scale as f64
        } else {
            price as f64
        };
        let approx_trigger = if price_scale > 0 {
            trigger_price as f64 / price_scale as f64
        } else {
            trigger_price as f64
        };
        let approx_size = if size_scale > 0 {
            base_amount as f64 / size_scale as f64
        } else {
            base_amount as f64
        };

        log::debug!(
            "Creating trigger order: market_id={}, side={}, base_amount={}, price={}, trigger_price={}, order_type={}, approx_price={}, approx_trigger={}, approx_size={}",
            market_id,
            side,
            base_amount,
            price,
            trigger_price,
            order_type,
            approx_price,
            approx_trigger,
            approx_size
        );

        let order_type_param = order_type as u64;
        let time_in_force = tif as u64;
        let reduce_only_param = if reduce_only { 1u64 } else { 0u64 };
        let trigger_price_param = trigger_price;

        // For trigger orders, use passed expiry_secs or default to 28 days
        let order_expiry = if order_type == ORDER_TYPE_TRIGGER
            || order_type == 4
            || order_type == 3
            || order_type == 5
        {
            // Use passed expiry_secs or default to 28 days for trigger orders
            let expiry_duration_ms = if let Some(expiry_secs) = expiry_secs {
                // Minimum 60 seconds for trigger orders as Go SDK requires MinOrderExpiry >= 1
                let min_expiry_secs = 60;
                std::cmp::max(min_expiry_secs, expiry_secs) * 1000
            } else {
                28 * 24 * 60 * 60 * 1000 // Default 28 days in milliseconds
            };
            (chrono::Utc::now().timestamp_millis() as u64 + expiry_duration_ms) as i64
        } else {
            // For regular orders, use passed expiry_secs or default
            let expiry_duration_ms = if let Some(expiry_secs) = expiry_secs {
                expiry_secs * 1000
            } else {
                24 * 60 * 60 * 1000 // Default 24 hours in milliseconds
            };
            (chrono::Utc::now().timestamp_millis() as u64 + expiry_duration_ms) as i64
        };

        let actual_market_id = market_id as u64;
        let actual_base_amount = base_amount;
        let actual_price = price;
        let actual_side = side as u64;

        // Extract private key bytes
        let private_key_hex = self
            .api_private_key_hex
            .strip_prefix("0x")
            .unwrap_or(&self.api_private_key_hex);
        let private_key_bytes = hex::decode(private_key_hex)
            .map_err(|e| DexError::Other(format!("Invalid private key hex: {}", e)))?;

        let mut key_bytes = [0u8; 40];
        let copy_len = std::cmp::min(private_key_bytes.len(), 40);
        key_bytes[..copy_len].copy_from_slice(&private_key_bytes[..copy_len]);

        // Call Go shared library for trigger order signature
        let go_result = self
            .call_go_sign_create_order(
                actual_market_id as i32,
                client_order_index as i64,
                actual_base_amount as i64,
                actual_price as i32,
                actual_side as i32,
                order_type_param as i32,
                time_in_force as i32,
                reduce_only_param as i32,
                trigger_price_param as i32,
                order_expiry,
                nonce as i64,
            )
            .await;

        let signature = match go_result {
            Ok(sig) => sig,
            Err(e) => {
                log::error!("Failed to sign trigger order via Go SDK: {}", e);
                return Err(DexError::Other(format!(
                    "Signature generation failed: {}",
                    e
                )));
            }
        };

        // Use same form-urlencoded format as regular orders
        let form_data = format!(
            "tx_type=14&tx_info={}&price_protection=false",
            urlencoding::encode(&signature)
        );

        log::debug!("Trigger order form data: {}", form_data);

        let client = &self.client;
        let url = format!("{}/api/v1/sendTx", self.base_url);

        // Protective orders (SL/TP) must not be dropped — wait for budget.
        self.acquire_rest_budget(
            "/api/v1/sendTx",
            crate::lighter_ratelimit::AcquirePolicy::Wait { max_ms: 5_000 },
        )
        .await?;

        let response = client
            .post(&url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(form_data)
            .send()
            .await
            .map_err(|e| DexError::Other(e.to_string()))?;

        let status = response.status();
        let response_text = response
            .text()
            .await
            .map_err(|e| DexError::Other(e.to_string()))?;

        log::debug!("Trigger order response: HTTP {}, {}", status, response_text);

        if status.is_success() {
            let order_id = client_order_index.to_string();
            log::info!(
                "✅ [TRIGGER_ORDER] Successfully created trigger order: {} (type={}, trigger_price={})",
                order_id,
                order_type,
                trigger_price_param
            );

            Ok(CreateOrderResponse {
                order_id,
                exchange_order_id: None,
                ordered_price: i64::try_from(price)
                    .ok()
                    .map(|p| Decimal::new(p, price_decimals))
                    .unwrap_or_else(|| Decimal::ZERO),
                ordered_size: i64::try_from(base_amount)
                    .ok()
                    .map(|b| Decimal::new(b, size_decimals))
                    .unwrap_or_else(|| Decimal::ZERO),
                client_order_id: Some(client_order_index.to_string()),
            })
        } else {
            self.invalidate_nonce_cache().await;
            Err(DexError::Other(format!(
                "Trigger order failed: HTTP {}, {}",
                status, response_text
            )))
        }
    }

    #[allow(dead_code)]
    async fn send_order_via_sdk(
        &self,
        market_id: u32,
        side: u32,
        tif: u32,
        base_amount: u64,
        price: u64,
        client_order_id: Option<String>,
    ) -> Result<CreateOrderResponse, DexError> {
        let timestamp = chrono::Utc::now().timestamp_millis() as u64;
        let client_id = client_order_id.unwrap_or_else(|| format!("rust-order-{}", timestamp));

        let (price_decimals, size_decimals) = {
            let cache = self.market_cache.read().await;
            if let Some(info) = cache.by_id.get(&market_id) {
                (
                    info.price_decimals.min(MAX_DECIMAL_PRECISION),
                    info.size_decimals.min(MAX_DECIMAL_PRECISION),
                )
            } else {
                (
                    DEFAULT_PRICE_DECIMALS.min(MAX_DECIMAL_PRECISION),
                    DEFAULT_SIZE_DECIMALS.min(MAX_DECIMAL_PRECISION),
                )
            }
        };
        let price_scale = ten_pow(price_decimals);
        let size_scale = ten_pow(size_decimals);

        let approx_price = if price_scale > 0 {
            price as f64 / price_scale as f64
        } else {
            price as f64
        };
        let approx_size = if size_scale > 0 {
            base_amount as f64 / size_scale as f64
        } else {
            base_amount as f64
        };

        log::debug!(
            "Delegating order to Python SDK: market_id={}, side={}, base_amount={}, price={}, approx_price={}, approx_size={}",
            market_id,
            side,
            base_amount,
            price,
            approx_price,
            approx_size
        );

        let output = std::process::Command::new("./venv/bin/python")
            .arg("sdk_send_order.py")
            .arg(&format!("--market-id={}", market_id))
            .arg(&format!("--side={}", side))
            .arg(&format!("--tif={}", tif))
            .arg(&format!("--base-amt={}", base_amount))
            .arg(&format!("--price={}", price))
            .arg(&format!("--client-id={}", client_id))
            .env("LIGHTER_ACCOUNT_INDEX", &self.account_index.to_string())
            .env("LIGHTER_API_KEY_INDEX", &self.api_key_index.to_string())
            .env("LIGHTER_PRIVATE_API_KEY", &self.api_private_key_hex)
            .current_dir(".")
            .output()
            .map_err(|e| DexError::Other(format!("Failed to execute SDK script: {}", e)))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        if !output.status.success() {
            log::error!("SDK delegation failed. stderr: {}", stderr);
            return Err(DexError::Other(format!("SDK execution failed: {}", stderr)));
        }

        if !stderr.is_empty() {
            log::warn!("SDK delegation warnings: {}", stderr);
        }

        // Parse JSON response
        let response: serde_json::Value = serde_json::from_str(&stdout)
            .map_err(|e| DexError::Other(format!("Failed to parse SDK response: {}", e)))?;

        if let Some(true) = response.get("success").and_then(|v| v.as_bool()) {
            log::debug!("Order successfully sent via SDK");

            let order_id = response
                .get("tx_hash")
                .and_then(|v| v.as_str())
                .unwrap_or(&client_id)
                .to_string();

            Ok(CreateOrderResponse {
                order_id,
                exchange_order_id: None,
                ordered_price: i64::try_from(price)
                    .ok()
                    .map(|p| Decimal::new(p, price_decimals))
                    .unwrap_or_else(|| Decimal::ZERO),
                ordered_size: i64::try_from(base_amount)
                    .ok()
                    .map(|b| Decimal::new(b, size_decimals))
                    .unwrap_or_else(|| Decimal::ZERO),
                client_order_id: None,
            })
        } else if let Some(error) = response.get("error") {
            log::error!("SDK order failed: {}", error);
            Err(DexError::Other(format!("SDK order error: {}", error)))
        } else {
            Err(DexError::Other(
                "Unexpected SDK response format".to_string(),
            ))
        }
    }

    #[allow(dead_code)]
    async fn get_nonce(&self) -> Result<u64, DexError> {
        self.get_nonce_with_key(&self.api_key_public).await
    }

    async fn get_nonce_with_key(&self, api_key: &str) -> Result<u64, DexError> {
        if api_key == self.api_key_public {
            // Cache-hit fast path: lock only for cache read/write, never across REST I/O.
            // Holding the lock across `fetch_nonce().await` can deadlock the runtime if
            // the REST call stalls under Lighter WAF/429 (see bot-strategy#85).
            {
                let mut cache = self.nonce_cache.lock().await;
                if let Some(state) = cache.as_mut() {
                    if state.last_refresh.elapsed() <= self.nonce_cache_ttl {
                        let nonce = state.next_nonce;
                        state.next_nonce = state.next_nonce.saturating_add(1);
                        return Ok(nonce);
                    }
                }
            }

            let nonce = self.fetch_nonce(api_key).await?;

            let mut cache = self.nonce_cache.lock().await;
            *cache = Some(NonceCache {
                next_nonce: nonce.saturating_add(1),
                last_refresh: Instant::now(),
            });
            return Ok(nonce);
        }
        self.fetch_nonce(api_key).await
    }

    async fn fetch_nonce(&self, api_key: &str) -> Result<u64, DexError> {
        let url = format!(
            "{}/api/v1/nextNonce?account_index={}&api_key_index={}",
            self.base_url, self.account_index, self.api_key_index
        );

        log::debug!("Getting nonce from: {}", url);
        log::debug!("Using API key: {}", api_key);

        // Track API call
        track_api_call("/api/v1/nextNonce", "GET");

        // Nonce feeds straight into sendTx, so we must not shed — wait up to
        // 3s for budget rather than fail the signing path. See bot-strategy#79.
        self.acquire_rest_budget(
            "/api/v1/nextNonce",
            crate::lighter_ratelimit::AcquirePolicy::Wait { max_ms: 3_000 },
        )
        .await?;

        let response = self
            .client
            .get(&url)
            .header("X-API-KEY", api_key)
            .send()
            .await
            .map_err(|e| DexError::Other(format!("Failed to get nonce: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response
                .text()
                .await
                .unwrap_or_else(|_| "Failed to read error response".to_string());
            log::error!(
                "Nonce request failed: HTTP {}, Body: {}",
                status,
                error_body
            );
            return Err(DexError::Other(format!(
                "Failed to get nonce: HTTP {}, Body: {}",
                status, error_body
            )));
        }

        let nonce_response: LighterNonceResponse = response
            .json()
            .await
            .map_err(|e| DexError::Other(format!("Failed to parse nonce response: {}", e)))?;

        Ok(nonce_response.nonce)
    }

    async fn invalidate_nonce_cache(&self) {
        let mut cache = self.nonce_cache.lock().await;
        *cache = None;
    }

    /// Discover account_index by querying the API with wallet_address and api_key_index.
    /// Fetches all accounts for the wallet's l1_address, then probes each account's
    /// `/api/v1/apikeys` endpoint to find the one matching our `api_key_index`.
    pub async fn discover_account_index(&self, wallet_address: &str) -> Result<u64, DexError> {
        log::info!(
            "Discovering account_index for api_key_index={} wallet={}...",
            self.api_key_index,
            wallet_address
        );

        // Fetch all accounts for this wallet address
        let accounts_url = format!(
            "{}/api/v1/account?by=l1_address&value={}",
            self.base_url, wallet_address
        );

        let response = self
            .client
            .get(&accounts_url)
            .header("X-API-KEY", &self.api_key_public)
            .send()
            .await
            .map_err(|e| {
                DexError::Other(format!("Failed to query accounts for discovery: {}", e))
            })?;

        let status = response.status();
        let body = response.text().await.map_err(|e| {
            DexError::Other(format!("Failed to read accounts response: {}", e))
        })?;

        if !status.is_success() {
            return Err(DexError::Other(format!(
                "Accounts API returned HTTP {}: {}",
                status, body
            )));
        }

        let account_resp: LighterAccountResponse =
            serde_json::from_str(&body).map_err(|e| {
                DexError::Other(format!("Failed to parse accounts response: {}", e))
            })?;

        if account_resp.accounts.is_empty() {
            return Err(DexError::Other(format!(
                "No accounts found for wallet {}",
                wallet_address
            )));
        }

        // Probe each account's apikeys to find the one with our api_key_index.
        // Track whether any probe returned a transient error (429 / code 23000)
        // so we can distinguish \"key legitimately not here\" from \"Lighter was
        // rate-limiting us and the answer is meaningless\". See bot-strategy#120.
        let mut saw_transient = false;
        for account in &account_resp.accounts {
            let acct_idx = account.account_index as u64;
            let probe_url = format!(
                "{}/api/v1/apikeys?account_index={}&api_key_index={}",
                self.base_url, acct_idx, self.api_key_index
            );

            let probe_resp = self
                .client
                .get(&probe_url)
                .header("X-API-KEY", &self.api_key_public)
                .send()
                .await;

            match probe_resp {
                Ok(resp) => {
                    let status = resp.status();
                    if status.as_u16() == 429 {
                        saw_transient = true;
                        continue;
                    }
                    if let Ok(text) = resp.text().await {
                        if text.contains("\"code\":23000") || text.contains("Too Many Requests") {
                            saw_transient = true;
                            continue;
                        }
                        if let Ok(apikey_resp) = serde_json::from_str::<ApiKeyResponse>(&text) {
                            if apikey_resp.code == 200 && !apikey_resp.api_keys.is_empty() {
                                log::info!(
                                    "Discovered account_index={} for api_key_index={}",
                                    acct_idx,
                                    self.api_key_index
                                );
                                return Ok(acct_idx);
                            }
                        }
                    }
                }
                Err(_) => {
                    // Network / connection errors during a WAF episode are
                    // indistinguishable from a transient rate-limit from the
                    // caller's point of view; fold them into the same bucket.
                    saw_transient = true;
                }
            }
        }

        if saw_transient {
            let until = Utc::now().timestamp() + 30;
            return Err(DexError::RateLimited { until_unix: until });
        }

        Err(DexError::Other(format!(
            "Could not find account for api_key_index={} in {} accounts for wallet {}. \
             Set LIGHTER_ACCOUNT_INDEX manually.",
            self.api_key_index,
            account_resp.accounts.len(),
            wallet_address
        )))
    }

    async fn get_server_public_key(&self) -> Result<String, DexError> {
        // Use cached version to reduce API calls
        self.get_server_public_key_cached().await
    }

    /// Direct REST GET helper that performs the same WAF cooldown gating as
    /// `make_request()`, for the few call sites that bypass the generic helper
    /// and want the raw response body. Returns `Err(DexError::RateLimited)` if
    /// the host-shared cooldown is active or the response itself is a WAF
    /// challenge. See bot-strategy#35.
    async fn fetch_text_with_waf_guard(
        &self,
        url: &str,
        err_label: &str,
    ) -> Result<String, DexError> {
        // Log the call through API_TRACKER so `/recentTrades`, `/funding-rates`,
        // `/orderBookDetails`, etc. show up in the 60s rolling window alongside
        // `/account`. Without this they fire silently and any 429 they trigger
        // appears in journalctl as a cooldown engagement with no visible
        // preceding request.
        track_api_call(err_label, "GET");

        if let Some(remaining) = crate::lighter_waf_cooldown::cooldown_remaining() {
            return Err(DexError::RateLimited {
                until_unix: chrono::Utc::now().timestamp() + remaining.as_secs() as i64,
            });
        }

        let response = self
            .client
            .get(url)
            .header("X-API-KEY", &self.api_key_public)
            .send()
            .await
            .map_err(|e| DexError::Other(format!("{}: {}", err_label, e)))?;

        let status = response.status();
        let headers = response.headers().clone();

        if let Some(src) = crate::lighter_waf_cooldown::classify_rate_limit(status, &headers) {
            let dur = crate::lighter_waf_cooldown::engage_cooldown(src);
            return Err(DexError::RateLimited {
                until_unix: chrono::Utc::now().timestamp() + dur.as_secs() as i64,
            });
        }

        let response_text = response
            .text()
            .await
            .map_err(|e| DexError::Other(format!("{} read body: {}", err_label, e)))?;

        if !status.is_success() {
            if let Some(src) =
                crate::lighter_waf_cooldown::classify_rate_limit_body(status, &response_text)
            {
                let dur = crate::lighter_waf_cooldown::engage_cooldown(src);
                return Err(DexError::RateLimited {
                    until_unix: chrono::Utc::now().timestamp() + dur.as_secs() as i64,
                });
            }
            return Err(DexError::Other(format!(
                "{} HTTP {}: {}",
                err_label, status, response_text
            )));
        }

        Ok(response_text)
    }

    /// Acquire host-shared rate-limit budget for an upcoming REST call.
    /// Returns `Err(DexError::RateLimited)` on shed so the caller can fall
    /// back to cached data or skip the cycle. See bot-strategy#79.
    async fn acquire_rest_budget(
        &self,
        endpoint: &str,
        policy: crate::lighter_ratelimit::AcquirePolicy,
    ) -> Result<(), DexError> {
        let weight = crate::lighter_ratelimit::weights::weight_for(endpoint);
        match self
            .rate_limiter
            .acquire(weight, policy, Some(endpoint))
            .await
        {
            crate::lighter_ratelimit::AcquireOutcome::Granted { .. } => Ok(()),
            crate::lighter_ratelimit::AcquireOutcome::Shed => Err(DexError::RateLimited {
                until_unix: chrono::Utc::now().timestamp() + 2,
            }),
        }
    }

    async fn make_request<T>(
        &self,
        endpoint: &str,
        method: HttpMethod,
        body: Option<&str>,
    ) -> Result<T, DexError>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        // Track API call
        let method_str = match method {
            HttpMethod::Get => "GET",
            HttpMethod::Post => "POST",
            HttpMethod::Put => "PUT",
            HttpMethod::Patch => "PATCH",
            HttpMethod::Delete => "DELETE",
        };
        track_api_call(endpoint, method_str);

        // Fail fast if a host-shared WAF cooldown is active. Sending the
        // request would only refresh the WAF rolling window. See bot-strategy#35.
        if let Some(remaining) = crate::lighter_waf_cooldown::cooldown_remaining() {
            return Err(DexError::RateLimited {
                until_unix: chrono::Utc::now().timestamp() + remaining.as_secs() as i64,
            });
        }

        // Proactive per-IP rate limit. Shed policy by default for generic
        // reads: callers that need delivery guarantees (sendTx, nonce) go
        // through dedicated code paths with Wait policy. See bot-strategy#79.
        self.acquire_rest_budget(endpoint, crate::lighter_ratelimit::AcquirePolicy::Shed)
            .await?;

        let url = format!("{}{}", self.base_url, endpoint);

        let mut request = match method {
            HttpMethod::Get => self.client.get(&url),
            HttpMethod::Post => self.client.post(&url),
            HttpMethod::Put => self.client.put(&url),
            HttpMethod::Patch => self.client.patch(&url),
            HttpMethod::Delete => self.client.delete(&url),
        };

        request = request.header("X-API-KEY", &self.api_key_public);

        if let Some(body_content) = body {
            request = request
                .header("Content-Type", "application/json")
                .body(body_content.to_string());
        }

        let response = request
            .send()
            .await
            .map_err(|e| DexError::Other(format!("Request failed: {}", e)))?;

        let status = response.status();
        let headers = response.headers().clone();

        // Detect WAF CAPTCHA / 429 / bare-405 and engage the host-shared
        // cooldown so all bots on this IP back off in lockstep.
        if let Some(src) = crate::lighter_waf_cooldown::classify_rate_limit(status, &headers) {
            let dur = crate::lighter_waf_cooldown::engage_cooldown(src);
            return Err(DexError::RateLimited {
                until_unix: chrono::Utc::now().timestamp() + dur.as_secs() as i64,
            });
        }

        if !status.is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            // Defensive body-based detection (in case headers were stripped).
            if let Some(src) =
                crate::lighter_waf_cooldown::classify_rate_limit_body(status, &error_text)
            {
                let dur = crate::lighter_waf_cooldown::engage_cooldown(src);
                return Err(DexError::RateLimited {
                    until_unix: chrono::Utc::now().timestamp() + dur.as_secs() as i64,
                });
            }
            return Err(DexError::Other(format!("HTTP {}: {}", status, error_text)));
        }

        response
            .json()
            .await
            .map_err(|e| DexError::Other(format!("Failed to parse response: {}", e)))
    }

    #[cfg(feature = "lighter-sdk")]
    async fn register_api_key(
        &self,
        evm_private_key: &str,
        go_public_key: &str,
        server_public_key: &str,
    ) -> Result<(), String> {
        log::debug!(
            "Attempting ChangePubKey: server='{}' -> local='{}'",
            server_public_key,
            go_public_key
        );

        // Get next nonce using server-registered public key (not our new key)
        let nonce = self
            .get_nonce_with_key(server_public_key)
            .await
            .map_err(|e| format!("Failed to get nonce: {:?}", e))?;
        log::debug!("Got nonce for ChangePubKey: {}", nonce);

        // Use the Go-derived public key
        let new_pubkey = if go_public_key.starts_with("0x") {
            go_public_key.to_string()
        } else {
            format!("0x{}", go_public_key)
        };
        log::debug!("New public key to register: {}", new_pubkey);

        // Use SignChangePubKey from the lighter-go library
        let sign_result = unsafe {
            SignChangePubKey(
                std::ffi::CString::new(new_pubkey.clone()).unwrap().as_ptr(),
                nonce as c_longlong,
                self.api_key_index as c_int,
                self.account_index as c_longlong,
            )
        };

        let (tx_info_str, message_to_sign_opt) =
            unsafe { parse_signed_tx_response(sign_result) }
                .map_err(|e| format!("Failed to sign ChangePubKey: {}", e))?;
        log::debug!("SignChangePubKey result: {}", tx_info_str);

        // Parse the tx_info JSON
        let mut tx_info: serde_json::Value = serde_json::from_str(&tx_info_str)
            .map_err(|e| format!("Failed to parse tx_info: {}", e))?;

        let message_to_sign = if let Some(msg) = message_to_sign_opt {
            msg
        } else {
            tx_info["MessageToSign"]
                .as_str()
                .ok_or("MessageToSign not found in tx_info")?
                .to_string()
        };
        log::debug!("MessageToSign: {}", message_to_sign);

        // Remove MessageToSign from tx_info as per Python SDK implementation
        tx_info.as_object_mut().unwrap().remove("MessageToSign");

        // Sign the message with EVM key using local signing (EIP-191)
        let evm_signature = self
            .sign_message_with_lighter_go_evm(evm_private_key, &message_to_sign)
            .await?;
        log::debug!("EVM signature: {}", evm_signature);

        // Compare expected vs actual L1 address for debugging
        if let Ok(recovered_addr) =
            self.recover_address_from_signature(&message_to_sign, &evm_signature)
        {
            if let Ok(expected_addr) = self.get_account_l1_address().await {
                log::debug!("L1 Address Comparison:");
                log::debug!(
                    "  Expected (account {}): {}",
                    self.account_index,
                    expected_addr
                );
                log::debug!("  Recovered from EVM sig: {}", recovered_addr);
                if expected_addr.to_lowercase() == recovered_addr.to_lowercase() {
                    log::debug!("  ✓ Addresses match - signature should be valid");
                } else {
                    log::error!("  ✗ Addresses MISMATCH - signature will fail validation");
                    log::error!("  This explains the L1 signature failure (code 21504)");
                }
            } else {
                log::warn!("Could not retrieve expected L1 address for comparison");
            }
        }

        // Add L1Sig field with the EVM signature (Python SDK uses L1Sig, not evmSignature)
        tx_info["L1Sig"] = serde_json::Value::String(evm_signature);

        // Send the ChangePubKey request
        let response = self.send_change_api_key_request(&tx_info.to_string()).await;

        match response {
            Ok(_v) => {
                let srv_short = &server_public_key[..8];
                let srv_end = &server_public_key[server_public_key.len() - 8..];
                let new_short = &new_pubkey.trim_start_matches("0x")[..8];
                let new_end_start = new_pubkey.len().saturating_sub(10); // account for "0x"
                let new_end = &new_pubkey[new_end_start..];

                log::debug!(
                    "ChangePubKey succeeded (account={}, index={}). Server public key updated from {}…{} to {}…{}",
                    self.account_index,
                    self.api_key_index,
                    srv_short, srv_end,
                    new_short, new_end
                );
                Ok(())
            }
            Err(e) => {
                let srv_short = &server_public_key[..8];
                let srv_end = &server_public_key[server_public_key.len() - 8..];

                log::error!(
                    "ChangePubKey failed (account={}, index={}) -> {}. Server key remains {}…{}",
                    self.account_index,
                    self.api_key_index,
                    e,
                    srv_short,
                    srv_end
                );
                Err(e)
            }
        }
    }

    #[cfg(not(feature = "lighter-sdk"))]
    async fn register_api_key(
        &self,
        _evm_private_key: &str,
        _go_public_key: &str,
        _server_public_key: &str,
    ) -> Result<(), String> {
        Err("API key registration requires lighter-sdk feature".to_string())
    }

    #[cfg(feature = "lighter-sdk")]
    async fn sign_message_with_lighter_go_evm(
        &self,
        evm_private_key: &str,
        message: &str,
    ) -> Result<String, String> {
        use ethers::signers::{LocalWallet, Signer};
        use std::str::FromStr;

        log::debug!("Using local EVM signer (ethers) for ChangePubKey signature");

        let cleaned_key = evm_private_key
            .strip_prefix("0x")
            .unwrap_or(evm_private_key);
        let wallet = LocalWallet::from_str(cleaned_key)
            .map_err(|e| format!("Invalid private key: {}", e))?;

        let signature = wallet
            .sign_message(message.as_bytes())
            .await
            .map_err(|e| format!("Signing failed: {}", e))?;

        // Ensure 0x prefix is present (Lighter expects 0x-prefixed signatures)
        let signature_with_prefix = format!("0x{}", signature);

        // Check if v value needs to be adjusted from {0,1} to {27,28}
        if signature_with_prefix.len() == 132 {
            // "0x" + 130 hex chars = 132
            let mut sig_bytes = hex::decode(&signature_with_prefix[2..])
                .map_err(|e| format!("Failed to decode signature hex: {}", e))?;

            if sig_bytes.len() == 65 {
                // Check if v is 0 or 1, and convert to 27 or 28
                if sig_bytes[64] == 0 {
                    log::debug!("Converting v from 0 to 27");
                    sig_bytes[64] = 27;
                } else if sig_bytes[64] == 1 {
                    log::debug!("Converting v from 1 to 28");
                    sig_bytes[64] = 28;
                }

                let corrected_signature = format!("0x{}", hex::encode(sig_bytes));
                log::debug!("EVM signature v-corrected: {}", corrected_signature);

                // Log recovered address for debugging
                if let Ok(recovered_addr) =
                    self.recover_address_from_signature(message, &corrected_signature)
                {
                    log::debug!("EVM signature recovery check - Address: {}", recovered_addr);
                } else {
                    log::warn!("Failed to recover address from EVM signature for verification");
                }

                return Ok(corrected_signature);
            }
        }

        Ok(signature_with_prefix)
    }

    #[cfg(not(feature = "lighter-sdk"))]
    async fn sign_message_with_lighter_go_evm(
        &self,
        _evm_private_key: &str,
        _message: &str,
    ) -> Result<String, String> {
        Err("EVM signing with lighter-go requires lighter-sdk feature".to_string())
    }

    /// Get account details to find the L1 address for this account
    async fn get_account_l1_address(&self) -> Result<String, DexError> {
        let url = format!(
            "{}/api/v1/account?account_index={}",
            self.base_url, self.account_index
        );
        log::debug!("Getting account details from: {}", url);

        let response = self
            .client
            .get(&url)
            .header("X-API-KEY", &self.api_key_public)
            .send()
            .await
            .map_err(|e| DexError::Other(format!("Failed to get account details: {}", e)))?;

        let status = response.status();
        let response_text = response
            .text()
            .await
            .map_err(|e| DexError::Other(format!("Failed to read response: {}", e)))?;

        log::debug!(
            "Account details response: HTTP {}, Body: {}",
            status,
            response_text
        );

        if !status.is_success() {
            return Err(DexError::Other(format!(
                "HTTP {}: {}",
                status, response_text
            )));
        }

        // Parse the response to extract L1 address
        let account_data: serde_json::Value = serde_json::from_str(&response_text)
            .map_err(|e| DexError::Other(format!("Failed to parse account response: {}", e)))?;

        // Look for l1Address field
        if let Some(l1_address) = account_data.get("l1Address").and_then(|v| v.as_str()) {
            Ok(l1_address.to_string())
        } else {
            Err(DexError::Other(
                "l1Address not found in account response".to_string(),
            ))
        }
    }

    /// Recover the EVM address from a signature for debugging purposes
    fn recover_address_from_signature(
        &self,
        message: &str,
        signature: &str,
    ) -> Result<String, String> {
        // Remove 0x prefix if present
        let signature_hex = signature.strip_prefix("0x").unwrap_or(signature);

        // Decode the signature
        let signature_bytes = hex::decode(signature_hex)
            .map_err(|e| format!("Failed to decode signature hex: {}", e))?;

        if signature_bytes.len() != 65 {
            return Err(format!(
                "Invalid signature length: {} (expected 65)",
                signature_bytes.len()
            ));
        }

        // Split signature into r, s, v components
        let _r = &signature_bytes[0..32];
        let _s = &signature_bytes[32..64];
        let v = signature_bytes[64];

        // Convert v to recovery id (0 or 1)
        let recovery_id = match v {
            27 => 0,
            28 => 1,
            0 | 1 => v, // Already in correct format
            _ => return Err(format!("Invalid v value: {}", v)),
        };

        // Create the message hash (EIP-191 personal_sign format)
        let prefix = format!("\x19Ethereum Signed Message:\n{}", message.len());
        let mut hasher = Keccak256::new();
        hasher.update(prefix.as_bytes());
        hasher.update(message.as_bytes());
        let message_hash = hasher.finalize();

        // Create secp256k1 objects
        let secp = Secp256k1::new();
        let message_obj = Message::from_digest_slice(&message_hash)
            .map_err(|e| format!("Invalid message hash: {}", e))?;

        // Create recoverable signature
        let recoverable_sig = secp256k1::ecdsa::RecoverableSignature::from_compact(
            &signature_bytes[0..64],
            secp256k1::ecdsa::RecoveryId::from_i32(recovery_id as i32)
                .map_err(|e| format!("Invalid recovery id: {}", e))?,
        )
        .map_err(|e| format!("Failed to create recoverable signature: {}", e))?;

        // Recover the public key
        let public_key = secp
            .recover_ecdsa(&message_obj, &recoverable_sig)
            .map_err(|e| format!("Failed to recover public key: {}", e))?;

        // Convert public key to address
        let public_key_bytes = public_key.serialize_uncompressed();
        let mut hasher = Keccak256::new();
        hasher.update(&public_key_bytes[1..]); // Skip the 0x04 prefix
        let hash = hasher.finalize();

        // Take the last 20 bytes and format as address
        let address = format!("0x{}", hex::encode(&hash[12..]));

        Ok(address)
    }

    async fn send_change_api_key_request(
        &self,
        tx_info: &str,
    ) -> Result<serde_json::Value, String> {
        let base_url = self.base_url.trim_end_matches('/');
        let url = format!("{}/api/v1/sendTx", base_url);

        let form_data = [
            ("tx_type", "8"), // TX_TYPE_CHANGE_PUB_KEY = 8 (correct value from Go SDK)
            ("tx_info", tx_info),
        ];

        log::debug!("Sending change API key request to: {}", url);
        log::debug!("Form data: {:?}", form_data);

        track_api_call("POST /api/v1/sendTx (change_api_key)", "POST");

        let response = self
            .client
            .post(&url)
            .form(&form_data)
            .send()
            .await
            .map_err(|e| format!("Failed to send request: {}", e))?;

        let status = response.status();
        let response_text = response
            .text()
            .await
            .map_err(|e| format!("Failed to read response: {}", e))?;

        log::debug!(
            "Change API key response: HTTP {}, Body: {}",
            status,
            response_text
        );

        if !status.is_success() {
            return Err(format!("HTTP {}: {}", status, response_text));
        }

        serde_json::from_str(&response_text)
            .map_err(|e| format!("Failed to parse response JSON: {}", e))
    }
}

#[async_trait]
impl DexConnector for LighterConnector {
    async fn start(&self) -> Result<(), DexError> {
        self.is_running.store(true, Ordering::SeqCst);
        log::debug!(
            "Lighter connector started with WebSocket: {}",
            self.websocket_url
        );

        // If a prior process (or earlier call on this host) engaged a WAF /
        // API rate-limit cooldown, wait it out before the first REST call of
        // start() rather than burning a budget slot on a doomed request and
        // re-learning the cooldown from a fresh 429. See bot-strategy#148.
        if let Some(remaining) =
            crate::lighter_waf_cooldown::pending_cooldown_wait("LighterConnector::start")
        {
            tokio::time::sleep(remaining).await;
        }

        // Initialize the Go client and validate API key
        #[cfg(feature = "lighter-sdk")]
        {
            match self.create_go_client().await {
                Ok(()) => {}
                Err(DexError::ApiKeyRegistrationRequired) => {
                    #[cfg(feature = "lighter-sdk")]
                    if let Some(evm_key) = &self.evm_wallet_private_key {
                        let go_key = self.get_go_pubkey_from_check().map_err(|e| {
                            DexError::Other(format!(
                                "Failed to derive Go public key from CheckClient: {}",
                                e
                            ))
                        })?;

                        log::debug!("API key registration required. Attempting to register...");

                        // Get server public key for ChangePubKey
                        let server_pubkey = self.get_server_public_key().await.map_err(|e| {
                            DexError::Other(format!("Failed to get server public key: {}", e))
                        })?;

                        self.register_api_key(evm_key, &go_key, &server_pubkey)
                            .await
                            .map_err(|e| {
                                DexError::Other(format!("API key registration failed: {}", e))
                            })?;

                        // Retry validation after registration
                        self.create_go_client().await?;
                    } else {
                        return Err(DexError::ApiKeyRegistrationRequired);
                    }

                    #[cfg(not(feature = "lighter-sdk"))]
                    return Err(DexError::ApiKeyRegistrationRequired);
                }
                Err(e) => return Err(e),
            }
        }

        // Stagger startup across bot instances to reduce WAF pressure.
        {
            use rand::Rng;
            let startup_jitter_secs: u64 = std::env::var("LIGHTER_STARTUP_JITTER_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30);
            if startup_jitter_secs > 0 {
                let jitter = rand::thread_rng().gen_range(0..=startup_jitter_secs);
                log::info!(
                    "Startup jitter: sleeping {}s (max {}s)",
                    jitter,
                    startup_jitter_secs
                );
                tokio::time::sleep(std::time::Duration::from_secs(jitter)).await;
            }
        }

        // Preload market metadata once at startup to prevent repeated REST calls later.
        self.ensure_market_metadata_loaded().await?;

        // Start WebSocket connection
        self.start_websocket().await?;

        // Start auto-cleanup background task (every 6 hours)
        self.start_auto_cleanup(6);
        log::info!("🗑️ [AUTO_CLEANUP] Started background cleanup task (every 6 hours)");

        // Start the maintenance feed refresher off the strategy hot path
        // so a slow status.lighter.xyz response can never blow STEP_OVERRUN
        // (bot-strategy#160).
        self.start_maintenance_refresher();

        Ok(())
    }

    async fn stop(&self) -> Result<(), DexError> {
        self.is_running.store(false, Ordering::SeqCst);

        // Optionally abort cleanup task for immediate shutdown
        if let Some(handle) = self.cleanup_handle.lock().await.take() {
            handle.abort();
            self.cleanup_started.store(false, Ordering::SeqCst);
            log::debug!("🛑 [AUTO_CLEANUP] task forcibly aborted on stop");
        }

        log::debug!("Lighter connector stopped");
        Ok(())
    }

    async fn restart(&self, _max_retries: i32) -> Result<(), DexError> {
        self.stop().await?;
        sleep(Duration::from_secs(1)).await;
        self.start().await
    }

    async fn set_leverage(&self, _symbol: &str, _leverage: u32) -> Result<(), DexError> {
        log::debug!("Leverage setting not implemented for Lighter");
        Ok(())
    }

    async fn get_ticker(
        &self,
        symbol: &str,
        test_price: Option<Decimal>,
    ) -> Result<TickerResponse, DexError> {
        if let Some(price) = test_price {
            let min_tick = calculate_min_tick(price, DEFAULT_PRICE_DECIMALS, false);
            return Ok(TickerResponse {
                symbol: symbol.to_string(),
                price,
                min_tick: Some(min_tick),
                min_order: None,
                size_decimals: None,
                volume: Some(Decimal::ZERO),
                num_trades: None,
                open_interest: None,
                funding_rate: None,
                oracle_price: None,
                exchange_ts: None,
            });
        }

        let market_info = self.resolve_market_info(symbol).await?;
        let canonical_symbol = market_info.canonical_symbol.clone();
        let min_order = market_info.min_order.clone();

        // exchangeStats is dead data — populates TickerResponse.volume /
        // num_trades which no downstream consumer (pairtrade, slow-mm) reads.
        // Skipping the REST removes one mainnet GET per get_ticker miss and
        // cuts a chunk of the periodic 429 trigger. See bot-strategy#128
        // follow-up.
        let stats_data: Option<LighterExchangeStats> = None;
        // Funding rate is fed by the `market_stats/{market_id}` WS channel
        // (bot-strategy#162). Cold-start before the first WS push returns None,
        // matching the prior REST error-path fallback. The switch also moves
        // us from the accidental binance rate (first `.find()` hit in the REST
        // list) to Lighter's own rate — behavior change is immaterial under
        // production config (`funding_entry_z_scale=0`, `net_funding_min_per_hour=-0.01`).
        let funding_rate_from_ws = self
            .funding_rate_cache
            .read()
            .await
            .get(&market_info.market_id)
            .copied();

        // Try to get price from WebSocket first, but check if it's recent
        if let Some((ws_price, price_timestamp)) = self
            .current_price
            .read()
            .await
            .get(&canonical_symbol)
            .copied()
        {
            let current_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            // Check if WebSocket price is stale (older than 30 seconds).
            // `price_timestamp` is now ms (bot-strategy#274 / #276).
            let price_age_ms = current_time.saturating_sub(price_timestamp);
            if price_age_ms > 30_000 {
                log::warn!(
                    "WebSocket price is stale ({}ms old), falling back to REST API",
                    price_age_ms
                );
                // Fall through to REST API fallback below
            } else {
                let min_tick =
                    calculate_min_tick(ws_price, market_info.price_decimals, false);

                let (volume, num_trades) = if let Some(stats) = &stats_data {
                    if let Some(market_stats) = stats
                        .order_book_stats
                        .iter()
                        .find(|s| normalize_symbol(&s.symbol) == canonical_symbol)
                    {
                        (
                            Some(
                                Decimal::from_f64_retain(market_stats.daily_base_token_volume)
                                    .unwrap_or(Decimal::ZERO),
                            ),
                            Some(market_stats.daily_trades_count as u64),
                        )
                    } else {
                        (Some(Decimal::ZERO), None)
                    }
                } else {
                    (Some(Decimal::ZERO), None)
                };

                let funding_rate = funding_rate_from_ws;

                log::trace!(
                    "Using WebSocket price with API stats: price={}, volume={:?}, trades={:?}",
                    ws_price,
                    volume,
                    num_trades
                );

                return Ok(TickerResponse {
                    symbol: symbol.to_string(),
                    price: ws_price,
                    min_tick: Some(min_tick),
                    min_order: min_order.clone(),
                    size_decimals: Some(market_info.size_decimals),
                    volume,
                    num_trades,
                    open_interest: None,
                    funding_rate,
                    oracle_price: None,
                    exchange_ts: Some(price_timestamp),
                });
            }
        }

        // Fallback to REST API if WebSocket data is not available.
        // Routed through `fetch_text_with_waf_guard` so the first 429 engages
        // `lighter_waf_cooldown` and subsequent calls within the cooldown
        // window shed locally without sending HTTP. Without this gating the
        // bot's own retry storm extends Lighter's lockout (bot-strategy#281).
        log::warn!("WebSocket data not available, falling back to REST API");

        let market_id = market_info.market_id;
        let endpoint = format!("/api/v1/recentTrades?market_id={}&limit=100", market_id);
        let url = format!("{}{}", self.base_url, endpoint);

        let response_text = self
            .fetch_text_with_waf_guard(&url, "recentTrades")
            .await?;

        log::trace!("Trades API response: {}", response_text);

        let trades_response: LighterTradesResponse = serde_json::from_str(&response_text)
            .map_err(|e| DexError::Other(format!("Failed to parse response: {}", e)))?;

        let price = if let Some(trade) = trades_response.trades.first() {
            string_to_decimal(Some(trade.price.clone()))?
        } else {
            // Fallback to default if no trades found
            Decimal::new(50000, 0)
        };

        let min_tick = calculate_min_tick(price, market_info.price_decimals, false);

        // Funding rate comes from the WS cache populated by market_stats.
        let funding_rate = funding_rate_from_ws;

        // Use stats data if available, otherwise fallback to trades data
        let (volume, num_trades) = if let Some(stats) = &stats_data {
            if let Some(market_stats) = stats
                .order_book_stats
                .iter()
                .find(|s| normalize_symbol(&s.symbol) == canonical_symbol)
            {
                (
                    Some(
                        Decimal::from_f64_retain(market_stats.daily_base_token_volume)
                            .unwrap_or(Decimal::ZERO),
                    ),
                    Some(market_stats.daily_trades_count as u64),
                )
            } else {
                // Fallback to trades data
                let volume = trades_response
                    .trades
                    .iter()
                    .map(|trade| string_to_decimal(Some(trade.size.clone())))
                    .collect::<Result<Vec<_>, _>>()?
                    .iter()
                    .sum();
                (Some(volume), Some(trades_response.trades.len() as u64))
            }
        } else {
            // Fallback to trades data
            let volume = trades_response
                .trades
                .iter()
                .map(|trade| string_to_decimal(Some(trade.size.clone())))
                .collect::<Result<Vec<_>, _>>()?
                .iter()
                .sum();
            (Some(volume), Some(trades_response.trades.len() as u64))
        };

        Ok(TickerResponse {
            symbol: symbol.to_string(),
            price,
            min_tick: Some(min_tick),
            min_order,
            size_decimals: Some(market_info.size_decimals),
            volume,
            num_trades,
            open_interest: None,
            funding_rate,
            oracle_price: None,
            // REST fallback path: no exchange ts available, leave as None so
            // callers know to fall back to local clock for bucketing.
            exchange_ts: None,
        })
    }

    async fn get_filled_orders(&self, symbol: &str) -> Result<FilledOrdersResponse, DexError> {
        let orders = self.filled_orders.read().await;
        let normalized = normalize_symbol(symbol);
        let symbol_orders = orders
            .get(symbol)
            .or_else(|| orders.get(&normalized))
            .cloned()
            .unwrap_or_default();

        Ok(FilledOrdersResponse {
            orders: symbol_orders,
        })
    }

    async fn get_canceled_orders(&self, symbol: &str) -> Result<CanceledOrdersResponse, DexError> {
        let orders = self.canceled_orders.read().await;
        let symbol_orders = orders.get(symbol).cloned().unwrap_or_default();

        Ok(CanceledOrdersResponse {
            orders: symbol_orders,
        })
    }

    async fn get_balance(&self, symbol: Option<&str>) -> Result<BalanceResponse, DexError> {
        if let Some(cached) = self.try_read_cached_balance(symbol).await {
            return Ok(cached);
        }

        // Cache miss (or stale past BALANCE_CACHE_TTL_SECS, or invalidated
        // by a recent WS fill). Go direct to REST — Lighter's `account_all`
        // WS channel does not publish `total_asset_value` /
        // `available_balance` for perp sub-accounts, so there is no WS
        // warmup path that would populate this cache; the previous 10s
        // warmup was always a no-op that forced step() over the 5s tick
        // and caused STEP_OVERRUN on every equity refresh. See
        // bot-strategy#155 (event-sourced equity tracking).

        let endpoint = format!("/api/v1/account?by=index&value={}", self.account_index);

        // First, get the raw response text for debugging
        let url = format!("{}{}", self.base_url, endpoint);
        log::info!(
            "get_balance called for symbol: {:?}, requesting URL: {}",
            symbol,
            url
        );

        // Lighter occasionally 429s the head of a balance-refresh burst even
        // when our per-URL rate is ~1/min. Its short-window per-wallet throttle
        // is shared across all endpoints, so a concurrent order-placement or
        // nextNonce call can push the instantaneous rate over the edge. Retry
        // a couple of times with backoff so a transient 429 doesn't blank out
        // the equity observation for a full 5-minute cache cycle. See
        // bot-strategy#142.
        const BALANCE_RETRY_BACKOFF_MS: &[u64] = &[2_000, 5_000];

        let mut attempt: usize = 0;
        let (status, response_text) = loop {
            // Gate through the shared 60s/60000-weight bucket before each
            // attempt. Wait policy with a short cap — headroom is huge in
            // normal ops so this rarely blocks, but it keeps the sidecar's
            // view of outbound traffic complete and paces bursts. The retry
            // below still handles Lighter's short-window throttle that the
            // sidecar does not model. See bot-strategy#167.
            self.acquire_rest_budget(
                &endpoint,
                crate::lighter_ratelimit::AcquirePolicy::Wait { max_ms: 1_000 },
            )
            .await?;
            track_api_call(&endpoint, "GET");
            let response = self
                .client
                .get(&url)
                .header("X-API-KEY", &self.api_key_public)
                .send()
                .await
                .map_err(|e| DexError::Other(format!("Request failed: {}", e)))?;

            let status = response.status();
            let response_text = response
                .text()
                .await
                .map_err(|e| DexError::Other(format!("Failed to read response: {}", e)))?;

            if status != reqwest::StatusCode::TOO_MANY_REQUESTS
                || attempt >= BALANCE_RETRY_BACKOFF_MS.len()
            {
                break (status, response_text);
            }
            let backoff_ms = BALANCE_RETRY_BACKOFF_MS[attempt];
            // Lighter's per-wallet short-window throttle is shared across
            // endpoints and the first 429 is recovered transparently by the
            // backoff retry below; logging WARN on every attempt 1 hit
            // pollutes error-watch (bot-strategy#213, #227). Escalate only
            // when the throttle persists into attempt 2+, which signals real
            // sustained pressure that warrants attention.
            if attempt == 0 {
                log::info!(
                    "get_balance: HTTP 429 from Lighter (attempt {}/{}), retrying after {}ms",
                    attempt + 1,
                    BALANCE_RETRY_BACKOFF_MS.len() + 1,
                    backoff_ms
                );
            } else {
                log::warn!(
                    "get_balance: HTTP 429 from Lighter (attempt {}/{}), retrying after {}ms",
                    attempt + 1,
                    BALANCE_RETRY_BACKOFF_MS.len() + 1,
                    backoff_ms
                );
            }
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            attempt += 1;
        };

        log::info!(
            "Account API response (status: {}): {}",
            status,
            response_text
        );

        if !status.is_success() {
            return Err(DexError::Other(format!(
                "HTTP {}: {}",
                status, response_text
            )));
        }

        let account_response: LighterAccountResponse = serde_json::from_str(&response_text)
            .map_err(|e| DexError::Other(format!("Failed to parse response: {}", e)))?;

        if account_response.accounts.is_empty() {
            return Err(DexError::Other("No account found".to_string()));
        }

        let account = &account_response.accounts[0];

        // Debug log account information
        log::info!("Account balance info:");
        log::info!("  - Account Index: {}", account.account_index);
        log::info!("  - Available Balance: {} USD", account.available_balance);
        log::info!("  - Collateral: {} USD", account.collateral);
        log::info!("  - Total Asset Value: {} USD", account.total_asset_value);
        log::info!("  - Positions count: {}", account.positions.len());

        // Debug log all positions
        for (i, position) in account.positions.iter().enumerate() {
            log::info!(
                "  Position [{}]: market_id={}, symbol={}, position={}, sign={}",
                i,
                position.market_id,
                position.symbol,
                position.position,
                position.sign
            );
        }

        // If symbol is specified, look for that specific token position
        if let Some(token_symbol) = symbol {
            log::trace!("Looking for position with symbol: {}", token_symbol);

            // Find position for the specific token
            for position in &account.positions {
                if position.symbol == token_symbol {
                    log::trace!(
                        "✓ Found position for {}: {} (sign: {})",
                        token_symbol,
                        position.position,
                        position.sign
                    );
                    let position_decimal = string_to_decimal(Some(position.position.clone()))?;
                    let entry_price = string_to_decimal(Some(position.avg_entry_price.clone()))?;
                    return Ok(BalanceResponse {
                        equity: position_decimal,
                        balance: position_decimal,
                        position_entry_price: Some(entry_price),
                        position_sign: Some(position.sign.into()),
                    });
                }
            }

            // If token not found in positions, return zero
            log::trace!("✗ No position found for {}, returning zero", token_symbol);
            return Ok(BalanceResponse {
                equity: rust_decimal::Decimal::ZERO,
                balance: rust_decimal::Decimal::ZERO,
                position_entry_price: None,
                position_sign: None,
            });
        }

        // If no symbol specified, return account-level balances (USD)
        log::trace!("No symbol specified, returning account-level USD balances");
        let total_asset_value = string_to_decimal(Some(account.total_asset_value.clone()))?;
        let available_balance = string_to_decimal(Some(account.available_balance.clone()))?;

        log::info!(
            "Account balances: total_asset_value={}, available_balance={}",
            total_asset_value,
            available_balance
        );

        let response = BalanceResponse {
            equity: total_asset_value,  // Total account value in USD
            balance: available_balance, // Available balance in USD
            position_entry_price: None, // Account-level call doesn't have position info
            position_sign: None,
        };

        // Populate the cache with this authoritative REST value so subsequent
        // get_balance(None) calls within BALANCE_CACHE_TTL_SECS return from
        // memory. A WS fill invalidates eagerly (handle_account_update) so
        // realized P&L / fees are picked up on the next caller. See
        // bot-strategy#155 (event-sourced equity tracking).
        {
            let mut cache = self.balance_cache.write().await;
            *cache = Some((response.clone(), Instant::now()));
        }

        // Seed cached_collateral from REST so subsequent WS-only updates can
        // recompute equity = collateral + sum(unrealized_pnl) without another
        // REST. Use assets[USDC].margin_balance from the response when
        // present; this matches `total_asset_value` minus
        // sum(positions.unrealized_pnl). See bot-strategy#239.
        if let Some(usdc) = account
            .assets
            .iter()
            .find(|a| a.symbol == "USDC")
            .and_then(|a| string_to_decimal(Some(a.margin_balance.clone())).ok())
        {
            let mut collateral = self.cached_collateral.write().await;
            *collateral = Some(usdc);
        }

        Ok(response)
    }

    async fn get_combined_balance(&self) -> Result<CombinedBalanceResponse, DexError> {
        let cached = {
            let cache = self.balance_cache.read().await;
            cache.as_ref().and_then(|(balance, fetched_at)| {
                if fetched_at.elapsed() >= Duration::from_secs(Self::BALANCE_CACHE_TTL_SECS) {
                    return None;
                }
                Some(BalanceResponse {
                    equity: balance.equity,
                    balance: balance.balance,
                    position_entry_price: balance.position_entry_price,
                    position_sign: balance.position_sign,
                })
            })
        };
        if let Some(balance) = cached {
            let mut token_balances = HashMap::new();
            token_balances.insert(
                "USD".to_string(),
                BalanceResponse {
                    equity: balance.equity,
                    balance: balance.balance,
                    position_entry_price: balance.position_entry_price,
                    position_sign: balance.position_sign,
                },
            );
            return Ok(CombinedBalanceResponse {
                usd_balance: balance.equity,
                total_asset_value: balance.equity,
                token_balances,
                spot_assets: Vec::new(),
            });
        }

        let endpoint = format!("/api/v1/account?by=index&value={}", self.account_index);
        let url = format!("{}{}", self.base_url, endpoint);

        log::info!("get_combined_balance called, requesting URL: {}", url);

        let response = self
            .client
            .get(&url)
            .header("X-API-KEY", &self.api_key_public)
            .send()
            .await
            .map_err(|e| DexError::Other(format!("Request failed: {}", e)))?;

        let status = response.status();
        let response_text = response
            .text()
            .await
            .map_err(|e| DexError::Other(format!("Failed to read response: {}", e)))?;

        if !status.is_success() {
            return Err(DexError::Other(format!(
                "HTTP {}: {}",
                status, response_text
            )));
        }

        let account_response: LighterAccountResponse = serde_json::from_str(&response_text)
            .map_err(|e| DexError::Other(format!("Failed to parse response: {}", e)))?;

        if account_response.accounts.is_empty() {
            return Err(DexError::Other("No account found".to_string()));
        }

        let account = &account_response.accounts[0];

        // Extract USD balance and total asset value
        let usd_balance = string_to_decimal(Some(account.available_balance.clone()))?;
        let total_asset_value = string_to_decimal(Some(account.total_asset_value.clone()))?;

        // Extract all token balances
        let mut token_balances = std::collections::HashMap::new();
        for position in &account.positions {
            let position_decimal = string_to_decimal(Some(position.position.clone()))?;
            let entry_price = string_to_decimal(Some(position.avg_entry_price.clone()))?;

            token_balances.insert(
                position.symbol.clone(),
                BalanceResponse {
                    equity: position_decimal,
                    balance: position_decimal,
                    position_entry_price: Some(entry_price),
                    position_sign: Some(position.sign.into()),
                },
            );
        }

        // Extract spot asset balances
        let mut spot_assets = Vec::new();
        for asset in &account.assets {
            let balance = string_to_decimal(Some(asset.balance.clone())).unwrap_or_default();
            let locked = string_to_decimal(Some(asset.locked_balance.clone())).unwrap_or_default();
            spot_assets.push(SpotAssetBalance {
                symbol: asset.symbol.clone(),
                balance,
                locked_balance: locked,
            });
        }

        log::debug!(
            "Combined balance: USD={}, total_asset_value={}, tokens={} positions, {} spot assets",
            usd_balance,
            total_asset_value,
            token_balances.len(),
            spot_assets.len()
        );

        Ok(CombinedBalanceResponse {
            usd_balance,
            total_asset_value,
            token_balances,
            spot_assets,
        })
    }

    async fn get_positions(&self) -> Result<Vec<PositionSnapshot>, DexError> {
        if !self.positions_ready.load(Ordering::SeqCst) {
            return Err(DexError::Other(
                "positions not ready from websocket".to_string(),
            ));
        }
        let positions_guard = self.cached_positions.read().await;
        Ok(positions_guard.clone())
    }

    async fn get_open_orders(&self, symbol: &str) -> Result<OpenOrdersResponse, DexError> {
        log::debug!(
            "[WS_ORDER_TRACKING] get_open_orders called for symbol: {} (WebSocket-only)",
            symbol
        );

        // Return WebSocket-tracked orders only (no API fallback)
        let orders_guard = self.cached_open_orders.read().await;
        let orders = orders_guard.get(symbol).cloned().unwrap_or_default();

        log::debug!(
            "[WS_ORDER_TRACKING] Returning {} orders for {} from WebSocket tracking",
            orders.len(),
            symbol
        );

        Ok(OpenOrdersResponse { orders })
    }

    async fn get_last_trades(&self, symbol: &str) -> Result<LastTradesResponse, DexError> {
        // Get market_id for the symbol
        let market_id = self.resolve_market_info(symbol).await?.market_id;

        // Query recent trades
        let endpoint = format!("/api/v1/recentTrades?market_id={}&limit=10", market_id);

        let url = format!("{}{}", self.base_url, endpoint);
        let response_text = self
            .fetch_text_with_waf_guard(&url, "recentTrades")
            .await?;

        log::debug!("Last trades API response: {}", response_text);

        let trades_response: LighterTradesResponse = serde_json::from_str(&response_text)
            .map_err(|e| DexError::Other(format!("Failed to parse response: {}", e)))?;

        let trades = trades_response
            .trades
            .into_iter()
            .map(|t| LastTrade {
                price: string_to_decimal(Some(t.price)).unwrap_or_default(),
                size: string_to_decimal(Some(t.size)).ok(),
                side: map_side(t.side.as_deref()),
            })
            .collect();

        Ok(LastTradesResponse { trades })
    }

    async fn get_order_book(
        &self,
        symbol: &str,
        depth: usize,
    ) -> Result<OrderBookSnapshot, DexError> {
        let market_id = self.resolve_market_info(symbol).await?.market_id;
        let (ob, updated_at) = {
            let ob_guard = self.order_book.read().await;
            if let Some(entry) = ob_guard.get(&market_id) {
                (entry.order_book.clone(), entry.updated_at)
            } else {
                log::debug!(
                    "order book snapshot unavailable for {} (market_id={}, cached_entries={})",
                    symbol,
                    market_id,
                    ob_guard.len()
                );
                return Err(DexError::Other(
                    "order book snapshot unavailable (no recent update)".to_string(),
                ));
            }
        };
        if updated_at.elapsed() > self.ob_stale_after {
            // Don't remove stale entry from cache — it may be needed as a
            // baseline for delta merges after WS reconnection. The entry will
            // be replaced once a fresh update arrives. See: dex-connector#2
            return Err(DexError::Other(
                "order book snapshot unavailable (no recent update)".to_string(),
            ));
        }
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        for entry in ob.bids.iter().take(depth) {
            if let (Ok(price), Ok(size)) = (
                string_to_decimal(Some(entry.price.clone())),
                string_to_decimal(Some(entry.size.clone())),
            ) {
                bids.push(OrderBookLevel { price, size });
            }
        }
        for entry in ob.asks.iter().take(depth) {
            if let (Ok(price), Ok(size)) = (
                string_to_decimal(Some(entry.price.clone())),
                string_to_decimal(Some(entry.size.clone())),
            ) {
                asks.push(OrderBookLevel { price, size });
            }
        }
        Ok(OrderBookSnapshot { bids, asks })
    }

    async fn clear_filled_order(&self, symbol: &str, trade_id: &str) -> Result<(), DexError> {
        let mut filled_orders = self.filled_orders.write().await;
        if let Some(orders) = filled_orders.get_mut(symbol) {
            let initial_len = orders.len();
            orders.retain(|order| order.trade_id != trade_id);
            if orders.len() < initial_len {
                log::debug!(
                    "🗑️ [CLEAR_FILL] Removed trade_id {} for {}",
                    trade_id,
                    symbol
                );
                Ok(())
            } else {
                Err(DexError::Other(format!(
                    "Trade ID {} not found for symbol {}",
                    trade_id, symbol
                )))
            }
        } else {
            Err(DexError::Other(format!(
                "No filled orders found for symbol {}",
                symbol
            )))
        }
    }

    async fn clear_all_filled_orders(&self) -> Result<(), DexError> {
        let mut filled_orders = self.filled_orders.write().await;
        let total_cleared = filled_orders.values().map(|v| v.len()).sum::<usize>();
        filled_orders.clear();
        log::info!(
            "🗑️ [CLEAR_ALL_FILLS] Cleared {} filled orders across all symbols",
            total_cleared
        );
        Ok(())
    }

    async fn clear_canceled_order(&self, _symbol: &str, _order_id: &str) -> Result<(), DexError> {
        Err(DexError::Other(
            "clear_canceled_order not supported for Lighter - canceled orders are streamed via WebSocket only".to_string()
        ))
    }

    async fn clear_all_canceled_orders(&self) -> Result<(), DexError> {
        Err(DexError::Other(
            "clear_all_canceled_orders not supported for Lighter - canceled orders are streamed via WebSocket only".to_string()
        ))
    }

    async fn create_order(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        price: Option<Decimal>,
        _spread: Option<i64>,
        reduce_only: bool,
        expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        // Resolve market metadata for symbol
        let market_info = self.resolve_market_info(symbol).await?;
        let market_id = market_info.market_id;

        // Convert side: Long=0(BUY), Short=1(SELL) for Lighter API
        let side_value = match side {
            OrderSide::Long => 0,
            OrderSide::Short => 1,
        };

        // Convert time-in-force: 0=IOC, 1=GTT, 2=PostOnly
        // Use spread parameter to specify TIF when negative values:
        // spread >= 0: normal spread adjustment
        // spread = -1: Request IOC (degraded to GTT on Lighter)
        // spread = -2: Post-only order
        let default_tif = TIF_GTT;

        let price_decimals = market_info.price_decimals;
        let size_decimals = market_info.size_decimals;

        // Convert amounts to Lighter's scaled integers using market metadata
        let size_abs = size.abs();
        let mut base_amount = scale_decimal_to_u64(
            size_abs,
            size_decimals,
            RoundingStrategy::ToZero,
            "base amount",
        )?;

        if base_amount == 0 && size_abs > Decimal::ZERO {
            log::debug!(
                "Rounded base amount to zero for size {} (decimals {}), forcing minimum base_amount=1",
                size_abs,
                size_decimals
            );
            base_amount = 1;
        }

        let (price_value, order_type, tif) = if let Some(p) = price {
            // Handle spread parameter: negative values for TIF, positive for price adjustment
            let (final_price, order_tif) = if let Some(spread_ticks) = _spread {
                if spread_ticks < 0 {
                    // Negative spread values specify TIF
                    let tif_value = match spread_ticks {
                        -1 => TIF_GTT,       // Treat IOC override as resting GTT on Lighter
                        -2 => TIF_POST_ONLY, // Post-only (resting limit)
                        _ => {
                            log::warn!("Invalid TIF spread value: {}, using GTT", spread_ticks);
                            default_tif
                        }
                    };
                    (p, tif_value) // No price adjustment for TIF orders
                } else {
                    // Positive spread values adjust price (original behavior)
                    let tick_decimals = price_decimals.min(MAX_DECIMAL_PRECISION);
                    if tick_decimals != price_decimals {
                        log::warn!(
                            "Price decimals {} exceed supported max {}, clamping for spread adjustment",
                            price_decimals,
                            MAX_DECIMAL_PRECISION
                        );
                    }
                    let tick_size = Decimal::new(1, tick_decimals);
                    let spread_amount = Decimal::from(spread_ticks) * tick_size;
                    (p + spread_amount, default_tif)
                }
            } else {
                (p, default_tif)
            };

            // Limit order
            let price_u32 = scale_decimal_to_u32(
                final_price,
                price_decimals,
                RoundingStrategy::MidpointAwayFromZero,
                "price",
            )?;
            let price_val = u64::from(price_u32);

            let tif_name = match order_tif {
                v if v == TIF_IOC => "IOC",
                v if v == TIF_GTT => "GTT",
                v if v == TIF_POST_ONLY => "POST_ONLY",
                _ => "UNKNOWN",
            };

            log::debug!("Creating limit order: side={}, original_price={}, spread_param={:?}, final_price={}, TIF={} ({}), scaled_price={}, size={}, scaled_base_amount={}",
                side_value, p, _spread, final_price, order_tif, tif_name, price_val, size_abs, base_amount);
            (price_val, ORDER_TYPE_LIMIT, order_tif)
        } else {
            // Market order - get current price and set protection price
            let ticker = self.get_ticker(symbol, None).await?;
            let current_price = ticker.price;

            // Set protection price with large buffer for market orders
            let protection_price = if side_value == 1 {
                // SELL
                current_price * Decimal::new(800, 3) // 20% below market (protection price)
            } else {
                // BUY
                current_price * Decimal::new(1200, 3) // 20% above market (protection price)
            };

            let price_u32 = scale_decimal_to_u32(
                protection_price,
                price_decimals,
                RoundingStrategy::MidpointAwayFromZero,
                "protection price",
            )?;
            let price_val = u64::from(price_u32);

            log::debug!(
                "Market order: current_price={}, protection_price={}, side={}, price_decimals={}, size_decimals={}",
                current_price,
                protection_price,
                side_value,
                price_decimals,
                size_decimals
            );

            (price_val, ORDER_TYPE_IOC, TIF_IOC) // Market orders use IOC semantics
        };

        // Use native Rust implementation for Lighter signatures
        let cid = chrono::Utc::now().timestamp_millis().to_string();
        let result = self
            .create_order_native_with_type(
                market_id,
                side_value,
                tif,
                base_amount,
                price_value,
                Some(cid),
                order_type,
                reduce_only,
                expiry_secs,
            )
            .await;

        // Update order tracking if order creation was successful
        if let Ok(ref response) = result {
            let actual_price = price.unwrap_or(Decimal::ZERO);
            self.update_order_tracking_after_create(
                symbol,
                &response.order_id,
                side,
                size,
                actual_price,
            )
            .await;
        }

        result
    }

    async fn create_advanced_trigger_order(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        trigger_px: Decimal,
        limit_px: Option<Decimal>,
        order_style: TriggerOrderStyle,
        slippage_bps: Option<u32>,
        tpsl: TpSl,
        reduce_only: bool,
        expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        log::info!(
            "🎯 [ADVANCED_TRIGGER_ORDER] Creating {} order for {}: style={:?}, trigger={}, limit={:?}, slippage_bps={:?}",
            match tpsl { TpSl::Sl => "stop loss", TpSl::Tp => "take profit" },
            symbol,
            order_style,
            trigger_px,
            limit_px,
            slippage_bps
        );

        let market_info = self.resolve_market_info(symbol).await?;
        let market_id = market_info.market_id;

        let side_value = if is_buy_for_tpsl(side) {
            SIDE_BUY
        } else {
            SIDE_SELL
        };

        let (is_market, final_limit_price, order_type) = match order_style {
            TriggerOrderStyle::Market => {
                let order_type = match tpsl {
                    TpSl::Sl => 2, // StopLossOrder
                    TpSl::Tp => 4, // TakeProfitOrder
                };
                (true, trigger_px, order_type)
            }
            TriggerOrderStyle::MarketWithSlippageControl => {
                if let Some(slippage) = slippage_bps {
                    let slippage_factor = Decimal::new(slippage as i64, 4);
                    // "Market equivalent with slippage control" means we prioritize execution over price
                    // Always adjust towards the WORSE direction for guaranteed execution
                    let adjusted_price = match (side, tpsl) {
                        (OrderSide::Long, TpSl::Sl) => {
                            // Long Stop Loss (sell): worse price for selling = lower price
                            trigger_px * (Decimal::ONE - slippage_factor)
                        }
                        (OrderSide::Short, TpSl::Sl) => {
                            // Short Stop Loss (buy): worse price for buying = higher price
                            trigger_px * (Decimal::ONE + slippage_factor)
                        }
                        (OrderSide::Long, TpSl::Tp) => {
                            // Long Take Profit executed as market (sell): worse price = lower price
                            trigger_px * (Decimal::ONE - slippage_factor)
                        }
                        (OrderSide::Short, TpSl::Tp) => {
                            // Short Take Profit executed as market (buy): worse price = higher price
                            trigger_px * (Decimal::ONE + slippage_factor)
                        }
                    };
                    let order_type = match tpsl {
                        TpSl::Sl => 3, // StopLossLimitOrder with slippage control
                        TpSl::Tp => 5, // TakeProfitLimitOrder with slippage control
                    };
                    (false, adjusted_price, order_type)
                } else {
                    // Fallback to pure market
                    let order_type = match tpsl {
                        TpSl::Sl => 2,
                        TpSl::Tp => 4,
                    };
                    (true, trigger_px, order_type)
                }
            }
            TriggerOrderStyle::Limit => {
                let limit_price = limit_px.ok_or_else(|| {
                    DexError::Other("limit_px required for Limit order style".into())
                })?;

                // Validate limit price vs trigger price for the order type
                // The validation should be based on order execution direction, not position side
                match (side, tpsl) {
                    (OrderSide::Long, TpSl::Sl) => {
                        // Buy stop loss: limit should be >= trigger (worse price for buying)
                        if limit_price < trigger_px {
                            return Err(DexError::Other(
                                "For Buy Stop Loss, limit_px must be >= trigger_px".into(),
                            ));
                        }
                    }
                    (OrderSide::Short, TpSl::Sl) => {
                        // Sell stop loss: limit should be <= trigger (worse price for selling)
                        if limit_price > trigger_px {
                            return Err(DexError::Other(
                                "For Sell Stop Loss, limit_px must be <= trigger_px".into(),
                            ));
                        }
                    }
                    (OrderSide::Long, TpSl::Tp) => {
                        // Buy take profit: limit should be <= trigger (better price for buying)
                        if limit_price > trigger_px {
                            return Err(DexError::Other(
                                "For Buy Take Profit, limit_px must be <= trigger_px".into(),
                            ));
                        }
                    }
                    (OrderSide::Short, TpSl::Tp) => {
                        // Sell take profit: limit should be >= trigger (better price for selling)
                        if limit_price < trigger_px {
                            return Err(DexError::Other(
                                "For Sell Take Profit, limit_px must be >= trigger_px".into(),
                            ));
                        }
                    }
                }

                let order_type = match tpsl {
                    TpSl::Sl => 3, // StopLossLimitOrder
                    TpSl::Tp => 5, // TakeProfitLimitOrder
                };
                (false, limit_price, order_type)
            }
        };

        // Convert to native units with proper error handling
        let size_abs = size.abs();
        let mut base_amount = scale_decimal_to_u64(
            size_abs,
            market_info.size_decimals,
            RoundingStrategy::ToZero,
            "trigger order base amount",
        )?;
        if base_amount == 0 && size_abs > Decimal::ZERO {
            log::debug!(
                "Rounded trigger order base amount to zero for size {}, forcing minimum base_amount=1",
                size_abs
            );
            base_amount = 1;
        }

        let trigger_price_native = u64::from(scale_decimal_to_u32(
            trigger_px,
            market_info.price_decimals,
            RoundingStrategy::MidpointAwayFromZero,
            "trigger price",
        )?);

        let execution_price_native = if is_market {
            0 // Market orders: server ignores execution_price, use 0 for clarity
        } else {
            u64::from(scale_decimal_to_u32(
                final_limit_price,
                market_info.price_decimals,
                RoundingStrategy::MidpointAwayFromZero,
                "execution price",
            )?)
        };

        // Set TimeInForce based on order type (using global protocol constants)
        let time_in_force = if is_market { TIF_IOC } else { TIF_GTT };

        log::debug!(
            "Creating trigger order: market_id={}, side={}, base_amount={}, price={}, trigger_price={}, order_type={}",
            market_id, side_value, base_amount, execution_price_native, trigger_price_native, order_type
        );

        let cid = chrono::Utc::now().timestamp_millis().to_string();
        let result = self
            .create_order_native_with_trigger(
                market_id,
                side_value,
                time_in_force,
                base_amount,
                execution_price_native,
                trigger_price_native,
                Some(cid),
                order_type,
                reduce_only,
                expiry_secs,
            )
            .await;

        if let Ok(ref response) = result {
            let tracked_price = response.ordered_price;
            self.update_order_tracking_after_create(
                symbol,
                &response.order_id,
                side,
                size,
                tracked_price,
            )
            .await;
        }

        result
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<(), DexError> {
        let market_info = self.resolve_market_info(symbol).await?;

        let order_index = match parse_cancel_order_index(order_id) {
            Some(idx) => idx,
            None => {
                log::warn!(
                    "[CANCEL_ORDER] Unable to derive numeric index from order_id '{}'. Skipping cancel request.",
                    order_id
                );
                return Ok(());
            }
        };

        let nonce = self.get_nonce().await? as i64;
        let tx_json = self
            .call_go_sign_cancel_order(market_info.market_id as i32, order_index, nonce)
            .await?;

        let form_data = format!(
            "tx_type=15&tx_info={}&price_protection=false",
            urlencoding::encode(&tx_json)
        );

        track_api_call("POST /api/v1/sendTx (cancel_order)", "POST");

        // Cancel is on the risk-reduction path — wait for budget.
        self.acquire_rest_budget(
            "/api/v1/sendTx",
            crate::lighter_ratelimit::AcquirePolicy::Wait { max_ms: 5_000 },
        )
        .await?;

        let response = self
            .client
            .post(&format!("{}/api/v1/sendTx", self.base_url))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(form_data)
            .send()
            .await
            .map_err(|e| DexError::Other(format!("HTTP request failed: {}", e)))?;

        let status = response.status();
        let response_text = response
            .text()
            .await
            .map_err(|e| DexError::Other(format!("Failed to read response: {}", e)))?;

        if !status.is_success() {
            self.invalidate_nonce_cache().await;
            return Err(DexError::Other(format!(
                "Cancel order failed: HTTP {}, {}",
                status, response_text
            )));
        }

        self.update_order_tracking_after_cancel(symbol, order_id)
            .await;

        log::info!(
            "[CANCEL_ORDER] Successfully cancelled order {} for {}",
            order_id,
            symbol
        );

        Ok(())
    }

    async fn cancel_all_orders(&self, symbol: Option<String>) -> Result<(), DexError> {
        let targets: Vec<(String, Vec<String>)> = {
            let orders_guard = self.cached_open_orders.read().await;
            match symbol {
                Some(sym) => {
                    let ids = orders_guard
                        .get(&sym)
                        .map(|orders| orders.iter().map(|o| o.order_id.clone()).collect())
                        .unwrap_or_default();
                    vec![(sym, ids)]
                }
                None => orders_guard
                    .iter()
                    .map(|(sym, orders)| {
                        (
                            sym.clone(),
                            orders.iter().map(|o| o.order_id.clone()).collect(),
                        )
                    })
                    .collect(),
            }
        };

        let mut last_err: Option<DexError> = None;
        for (sym, ids) in targets {
            for order_id in ids {
                if let Err(e) = self.cancel_order(&sym, &order_id).await {
                    log::error!(
                        "[CANCEL_ORDER] Failed to cancel order {} for {}: {}",
                        order_id,
                        sym,
                        e
                    );
                    last_err = Some(e);
                }
            }
        }

        if let Some(err) = last_err {
            Err(err)
        } else {
            Ok(())
        }
    }

    async fn cancel_orders(
        &self,
        symbol: Option<String>,
        order_ids: Vec<String>,
    ) -> Result<(), DexError> {
        let symbol = match symbol {
            Some(sym) => sym,
            None => {
                return Err(DexError::Other(
                    "cancel_orders requires a symbol on Lighter".to_string(),
                ))
            }
        };

        if order_ids.is_empty() {
            return Ok(());
        }

        let mut last_err: Option<DexError> = None;
        for order_id in order_ids {
            if let Err(e) = self.cancel_order(&symbol, &order_id).await {
                log::error!(
                    "[CANCEL_ORDER] Failed to cancel order {} for {}: {}",
                    order_id,
                    symbol,
                    e
                );
                last_err = Some(e);
            }
        }

        if let Some(err) = last_err {
            Err(err)
        } else {
            Ok(())
        }
    }

    async fn close_all_positions(&self, symbol: Option<String>) -> Result<(), DexError> {
        // Get current account info to check positions
        let endpoint = format!("/api/v1/account?by=index&value={}", self.account_index);

        let url = format!("{}{}", self.base_url, endpoint);
        let response = self
            .client
            .get(&url)
            .header("X-API-KEY", &self.api_key_public)
            .send()
            .await
            .map_err(|e| DexError::Other(format!("Request failed: {}", e)))?;

        let status = response.status();
        let response_text = response
            .text()
            .await
            .map_err(|e| DexError::Other(format!("Failed to read response: {}", e)))?;

        if !status.is_success() {
            return Err(DexError::Other(format!(
                "HTTP {}: {}",
                status, response_text
            )));
        }

        let account_response: LighterAccountResponse = serde_json::from_str(&response_text)
            .map_err(|e| DexError::Other(format!("Failed to parse response: {}", e)))?;

        if account_response.accounts.is_empty() {
            return Err(DexError::Other("No account found".to_string()));
        }

        let account = &account_response.accounts[0];

        // Check if there are any open positions (position != "0.00000")
        let mut has_positions = false;
        for position in &account.positions {
            if let Some(ref sym) = symbol {
                if position.symbol != *sym {
                    continue;
                }
            }
            if let Ok(pos_size) = position.position.parse::<f64>() {
                if pos_size.abs() > 0.0 {
                    has_positions = true;
                    log::info!(
                        "Found open position: market_id={}, symbol={}, size={}",
                        position.market_id,
                        position.symbol,
                        position.position
                    );
                }
            }
        }

        if !has_positions {
            log::info!("No open positions found (threshold: > 0.0), nothing to close");
            // Log all positions for debugging
            for position in &account.positions {
                if let Ok(pos_size) = position.position.parse::<f64>() {
                    if pos_size.abs() > 0.0 {
                        log::debug!("Small position below threshold: market_id={}, symbol={}, size={} (abs: {})",
                                   position.market_id, position.symbol, position.position, pos_size.abs());
                    }
                }
            }
            return Ok(());
        }

        // Close each open position by placing market orders in opposite direction
        for position in &account.positions {
            if let Some(ref sym) = symbol {
                if position.symbol != *sym {
                    continue;
                }
            }
            if let Ok(pos_size) = position.position.parse::<f64>() {
                if pos_size.abs() > 0.0 {
                    log::info!(
                        "Closing position: market_id={}, symbol={}, size={}, sign={}",
                        position.market_id,
                        position.symbol,
                        position.position,
                        position.sign
                    );

                    // Determine order side (opposite to current position)
                    let order_side = if position.sign > 0 {
                        // Currently long, so sell to close
                        1 // Ask/Sell
                    } else {
                        // Currently short, so buy to close
                        0 // Bid/Buy
                    };

                    let market_id = position.market_id;
                    let market_info = match self.resolve_market_info(&position.symbol).await {
                        Ok(info) => info,
                        Err(err) => {
                            log::warn!(
                                "Failed to resolve market info for {} (market_id={}): {}. Skipping position close",
                                position.symbol,
                                market_id,
                                err
                            );
                            continue;
                        }
                    };

                    // Use rust_decimal for precise conversion to avoid floating point errors
                    let pos_decimal =
                        rust_decimal::Decimal::from_str(&position.position.replace('-', ""))
                            .unwrap_or_else(|_| {
                                // Fallback: convert to string first then parse
                                let pos_str = format!("{:.8}", pos_size.abs());
                                rust_decimal::Decimal::from_str(&pos_str)
                                    .unwrap_or(rust_decimal::Decimal::ZERO)
                            });
                    let mut base_amount = match scale_decimal_to_u64(
                        pos_decimal,
                        market_info.size_decimals,
                        RoundingStrategy::ToZero,
                        "position base amount",
                    ) {
                        Ok(value) => value,
                        Err(err) => {
                            log::warn!(
                                "Failed to scale position size {} with {} decimals: {}. Falling back to default decimals {}",
                                pos_decimal,
                                market_info.size_decimals,
                                err,
                                DEFAULT_SIZE_DECIMALS
                            );
                            scale_decimal_to_u64(
                                pos_decimal,
                                DEFAULT_SIZE_DECIMALS,
                                RoundingStrategy::ToZero,
                                "fallback position base amount",
                            )
                            .unwrap_or(0)
                        }
                    };

                    // Ensure minimum of 1 unit for very small positions
                    if base_amount == 0 && pos_size.abs() > 0.0 {
                        base_amount = 1;
                        log::debug!(
                            "Position too small for conversion, using minimum base_amount=1"
                        );
                    }

                    log::debug!(
                        "Converting position {} to base_amount: {} (original: {}, decimal: {})",
                        position.position,
                        base_amount,
                        pos_size,
                        pos_decimal
                    );

                    // Create reduce-only market order to close position (requires less margin)
                    log::info!(
                        "Placing reduce-only market order to close position: market_id={}, side={}, size={}",
                        market_id, order_side, pos_decimal
                    );

                    // Get current price for market order using ticker data
                    let ticker_price = match self.get_ticker(&position.symbol, None).await {
                        Ok(ticker) => ticker.price,
                        Err(e) => {
                            log::warn!(
                                "Failed to fetch ticker for {} while closing position: {}. Using fallback price",
                                position.symbol,
                                e
                            );
                            rust_decimal::Decimal::new(50000, 0)
                        }
                    };

                    let protection_price = if order_side == 1 {
                        // Sell: set low protection price
                        ticker_price * rust_decimal::Decimal::new(700, 3) // 30% below market
                    } else {
                        // Buy: set high protection price
                        ticker_price * rust_decimal::Decimal::new(1300, 3) // 30% above market
                    };

                    let current_price = match scale_decimal_to_u32(
                        protection_price,
                        market_info.price_decimals,
                        RoundingStrategy::MidpointAwayFromZero,
                        "close-position protection price",
                    ) {
                        Ok(value) => u64::from(value),
                        Err(err) => {
                            log::warn!(
                                "Failed to scale protection price {} with {} decimals: {}. Using fallback 0",
                                protection_price,
                                market_info.price_decimals,
                                err
                            );
                            0
                        }
                    };

                    // Create reduce-only market order directly
                    match self
                        .create_order_native_with_type(
                            market_id as u32,
                            order_side as u32,
                            0, // IOC time in force
                            base_amount,
                            current_price,
                            None,
                            1,    // Market order type
                            true, // reduce_only=true for position closing (prevents overshooting)
                            None, // No expiry for position closing
                        )
                        .await
                    {
                        Ok(response) => {
                            log::info!(
                                "Successfully submitted reduce-only close order for {} position in market {}: Order ID {}",
                                position.symbol,
                                market_id,
                                response.order_id
                            );
                        }
                        Err(e) => {
                            log::error!("Failed to close position in market {}: {}", market_id, e);
                            return Err(e);
                        }
                    }
                }
            }
        }

        log::info!("All position close orders submitted successfully");
        Ok(())
    }

    async fn clear_last_trades(&self, _symbol: &str) -> Result<(), DexError> {
        Ok(())
    }

    async fn is_upcoming_maintenance(&self, hours_ahead: i64) -> bool {
        // Operator kill-switch. When `LIGHTER_MAINTENANCE_DISABLED=1` is set,
        // never report an upcoming maintenance window. The background
        // refresher (see `start_maintenance_refresher`) honors the same env
        // var and skips spawning, so the cache stays empty and this branch
        // is the only thing producing a result. See bot-strategy#32.
        if matches!(
            std::env::var("LIGHTER_MAINTENANCE_DISABLED").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE")
        ) {
            return false;
        }

        // Pure cache read. The actual REST fetch happens off the hot path
        // in the background refresher task spawned at start(). If the cache
        // is empty (refresher hasn't completed its first iteration yet, or
        // every fetch since startup has failed) we treat that as "no
        // upcoming maintenance" — the same default behavior the previous
        // inline-fetch design returned on first call. See bot-strategy#160.
        let now = Utc::now();
        let cached_start = {
            let info = self.maintenance.read().await;
            info.next_start.clone()
        };
        let res = Self::maintenance_within_window(cached_start.clone(), &now, hours_ahead);
        log::debug!(
            "Lighter maintenance check (cached): start={:?} now={} result={}",
            cached_start,
            now,
            res
        );
        res
    }

    async fn sign_evm_65b(&self, message: &str) -> Result<String, DexError> {
        use ethers::signers::{LocalWallet, Signer};
        use std::str::FromStr;

        let private_key = self
            .evm_wallet_private_key
            .as_ref()
            .ok_or_else(|| DexError::Other("EVM wallet private key not set".to_string()))?;
        let cleaned_key = private_key.strip_prefix("0x").unwrap_or(private_key);
        let wallet = LocalWallet::from_str(cleaned_key)
            .map_err(|e| DexError::Other(format!("Invalid private key: {}", e)))?;

        let signature = wallet
            .sign_message(message.as_bytes())
            .await
            .map_err(|e| DexError::Other(format!("Signing failed: {}", e)))?;

        Ok(format!("0x{}", signature))
    }

    async fn sign_evm_65b_with_eip191(&self, message: &str) -> Result<String, DexError> {
        // EIP-191 adds the prefix "\x19Ethereum Signed Message:\n" + message.len() + message
        let prefixed = format!("\x19Ethereum Signed Message:\n{}{}", message.len(), message);
        self.sign_evm_65b(&prefixed).await
    }

    fn subscribe_price_updates(
        &self,
    ) -> Result<tokio::sync::broadcast::Receiver<crate::PriceUpdate>, DexError> {
        Ok(self.price_update_tx.subscribe())
    }
}

impl LighterConnector {
    #[cfg(feature = "lighter-sdk")]
    fn extract_own_pubkey_from_error(error_msg: &str) -> Option<String> {
        let marker = "ownPubKey:";
        let start = error_msg.find(marker)?;
        let after = error_msg[start + marker.len()..].trim_start();
        let end = after.find(' ').unwrap_or(after.len());
        if end == 0 {
            None
        } else {
            Some(after[..end].to_string())
        }
    }

    #[cfg(feature = "lighter-sdk")]
    fn get_go_pubkey_from_check(&self) -> Result<String, DexError> {
        unsafe {
            let result = CheckClient(
                self.api_key_index as c_int,
                self.account_index as c_longlong,
            );

            if result.is_null() {
                return Err(DexError::Other(
                    "CheckClient succeeded; no pubkey mismatch detected".to_string(),
                ));
            }

            let error_cstr = CStr::from_ptr(result);
            let error_msg = error_cstr.to_string_lossy().to_string();
            libc::free(result as *mut libc::c_void);

            Self::extract_own_pubkey_from_error(&error_msg).ok_or_else(|| {
                DexError::Other(format!(
                    "Failed to extract public key from CheckClient error: {}",
                    error_msg
                ))
            })
        }
    }
}

impl LighterConnector {
    /// Update order tracking after order creation
    async fn update_order_tracking_after_create(
        &self,
        symbol: &str,
        order_id: &str,
        side: OrderSide,
        size: Decimal,
        price: Decimal,
    ) {
        let mut orders_guard = self.cached_open_orders.write().await;
        let orders = orders_guard
            .entry(symbol.to_string())
            .or_insert_with(Vec::new);

        let new_order = OpenOrder {
            order_id: order_id.to_string(),
            symbol: symbol.to_string(),
            side,
            size,
            price,
            status: "open".to_string(),
        };

        orders.push(new_order);

        log::debug!(
            "[WS_ORDER_TRACKING] Added order {} to tracking for {} (total: {} orders)",
            order_id,
            symbol,
            orders.len()
        );
    }

    /// Update order tracking after order cancellation
    #[allow(dead_code)]
    async fn update_order_tracking_after_cancel(&self, symbol: &str, order_id: &str) {
        let mut orders_guard = self.cached_open_orders.write().await;
        if let Some(orders) = orders_guard.get_mut(symbol) {
            orders.retain(|order| order.order_id != order_id);
            log::debug!(
                "[WS_ORDER_TRACKING] Removed order {} from tracking for {} (remaining: {} orders)",
                order_id,
                symbol,
                orders.len()
            );
        }
    }

    async fn remove_tracked_order(
        cached_open_orders: &Arc<RwLock<HashMap<String, Vec<OpenOrder>>>>,
        symbol: &str,
        order_id: &str,
    ) {
        let normalized = normalize_symbol(symbol);
        let mut keys = vec![symbol.to_string()];
        if normalized != symbol {
            keys.push(normalized);
        }

        let mut orders_guard = cached_open_orders.write().await;
        for key in keys {
            if let Some(orders) = orders_guard.get_mut(&key) {
                let before = orders.len();
                orders.retain(|order| order.order_id != order_id);
                if before != orders.len() {
                    log::debug!(
                        "[WS_ORDER_TRACKING] Removed order {} from tracking for {} (remaining: {} orders)",
                        order_id,
                        key,
                        orders.len()
                    );
                }
            }
        }
    }
}

impl LighterConnector {
    /// TTL for cached exchange stats / funding rates.
    // Funding rates are paid every 8h on Lighter and the entry gate uses a
    // very loose threshold (-0.01/hour in prod, with funding_entry_z_scale=0
    // so the value never modulates entry threshold). The data is effectively
    // a soft signal where 6h staleness is indistinguishable from 1h. The
    // hourly fetch was the periodic trigger of the host-shared Lighter rate
    // limit cooldown observed 2026-04-22 18:33 UTC: a single isolated GET to
    // /funding-rates returned 429 and engaged the global cooldown for 103s,
    // pausing all REST including order placement. Drop the call cadence 6x
    // by raising TTL from 1h to 6h. See bot-strategy#128 follow-up and
    // bot-strategy#161.
    const STATS_CACHE_TTL_SECS: u64 = 21_600;

    /// TTL for the cached `/account` equity snapshot.
    ///
    /// Lighter's WS `account_all` does not publish `total_asset_value` for
    /// perp sub-accounts, so we treat REST `/account` as the source of
    /// truth, cached here to avoid hitting it on every step(). The value
    /// changes only via (a) trade fills, which the WS stream notifies us
    /// about and which explicitly invalidate this cache, (b) funding every
    /// 8h, and (c) manual deposits/withdrawals. 5 minutes caps funding
    /// drift at roughly one rate-tick and matches pairtrade's
    /// `EQUITY_REFRESH_CACHE_SECS`. See bot-strategy#155.
    const BALANCE_CACHE_TTL_SECS: u64 = 300;

    /// Snapshot read of WS-fed balance/position caches for `get_balance`.
    /// Returns `None` if the WS has not yet delivered an account update; the
    /// caller then decides whether to wait (see bot-strategy#148) or fall
    /// back to REST.
    async fn try_read_cached_balance(
        &self,
        symbol: Option<&str>,
    ) -> Option<BalanceResponse> {
        if let Some(token_symbol) = symbol {
            let positions = self.cached_positions.read().await;
            if let Some(pos) = positions.iter().find(|p| p.symbol == token_symbol) {
                return Some(BalanceResponse {
                    equity: pos.size,
                    balance: pos.size,
                    position_entry_price: pos.entry_price,
                    position_sign: Some(pos.sign),
                });
            }
            let has_ws_balance = self
                .balance_cache
                .read()
                .await
                .as_ref()
                .map(|(_, fetched_at)| {
                    fetched_at.elapsed() < Duration::from_secs(Self::BALANCE_CACHE_TTL_SECS)
                })
                .unwrap_or(false);
            if !positions.is_empty() || has_ws_balance {
                return Some(BalanceResponse {
                    equity: Decimal::ZERO,
                    balance: Decimal::ZERO,
                    position_entry_price: None,
                    position_sign: None,
                });
            }
            None
        } else {
            let cache = self.balance_cache.read().await;
            cache.as_ref().and_then(|(balance, fetched_at)| {
                if fetched_at.elapsed() >= Duration::from_secs(Self::BALANCE_CACHE_TTL_SECS) {
                    return None;
                }
                Some(BalanceResponse {
                    equity: balance.equity,
                    balance: balance.balance,
                    position_entry_price: balance.position_entry_price,
                    position_sign: balance.position_sign,
                })
            })
        }
    }

    #[allow(dead_code)]
    async fn get_exchange_stats_cached(&self) -> Result<LighterExchangeStats, DexError> {
        {
            let cache = self.cached_exchange_stats.read().await;
            if let Some((stats, ts)) = &*cache {
                if ts.elapsed() < Duration::from_secs(Self::STATS_CACHE_TTL_SECS) {
                    return Ok(stats.clone());
                }
            }
        }
        let stats = self.get_exchange_stats().await?;
        {
            let mut cache = self.cached_exchange_stats.write().await;
            *cache = Some((stats.clone(), Instant::now()));
        }
        Ok(stats)
    }

    #[allow(dead_code)]
    async fn get_exchange_stats(&self) -> Result<LighterExchangeStats, DexError> {
        let url = format!("{}/api/v1/exchangeStats", self.base_url);
        let response_text = self
            .fetch_text_with_waf_guard(&url, "exchangeStats")
            .await?;
        log::trace!("Exchange stats response: {}", response_text);
        serde_json::from_str(&response_text)
            .map_err(|e| DexError::Other(format!("Failed to parse exchange stats: {}", e)))
    }

    async fn get_order_book_details(&self) -> Result<LighterOrderBookDetailsResponse, DexError> {
        let url = format!("{}/api/v1/orderBookDetails", self.base_url);
        let response_text = self
            .fetch_text_with_waf_guard(&url, "orderBookDetails")
            .await?;
        log::trace!("Order book details response: {}", response_text);
        serde_json::from_str(&response_text)
            .map_err(|e| DexError::Other(format!("Failed to parse order book details: {}", e)))
    }

    async fn get_order_books_all(&self) -> Result<LighterOrderBooksResponse, DexError> {
        let url = format!("{}/api/v1/orderBooks?filter=all", self.base_url);
        let response_text = self
            .fetch_text_with_waf_guard(&url, "orderBooks")
            .await?;
        serde_json::from_str(&response_text)
            .map_err(|e| DexError::Other(format!("Failed to parse orderBooks: {}", e)))
    }

    async fn get_funding_rates(&self) -> Result<LighterFundingRates, DexError> {
        let url = format!("{}/api/v1/funding-rates", self.base_url);
        let response_text = self
            .fetch_text_with_waf_guard(&url, "funding-rates")
            .await?;
        log::trace!("Funding rates response: {}", response_text);
        serde_json::from_str(&response_text)
            .map_err(|e| DexError::Other(format!("Failed to parse funding rates: {}", e)))
    }

    async fn start_websocket(&self) -> Result<(), DexError> {
        let ws_url = self
            .websocket_url
            .replace("https://", "wss://")
            .replace("http://", "ws://");

        log::info!("Connecting to WebSocket: {}", ws_url);

        if self._ws.is_none() {
            return Err(DexError::Other("WebSocket not initialized".to_string()));
        }

        let primary_market_id = if let Some(symbol) = self.tracked_symbols.first() {
            match self.resolve_market_info(symbol).await {
                Ok(info) => info.market_id,
                Err(e) => {
                    log::warn!(
                        "Failed to resolve primary symbol '{}' for WS order book subscription: {}. Falling back to market_id=1",
                        symbol,
                        e
                    );
                    1
                }
            }
        } else {
            1
        };

        // Clone necessary data for the WebSocket task
        let current_price = self.current_price.clone();
        let current_volume = self.current_volume.clone();
        let order_book = self.order_book.clone();
        let filled_orders = self.filled_orders.clone();
        let canceled_orders = self.canceled_orders.clone();
        let cached_positions = self.cached_positions.clone();
        let cached_open_orders = self.cached_open_orders.clone();
        let positions_ready = self.positions_ready.clone();
        let balance_cache = self.balance_cache.clone();
        let cached_collateral = self.cached_collateral.clone();
        let funding_rate_cache = self.funding_rate_cache.clone();
        let is_running = self.is_running.clone();
        let connection_epoch = self.connection_epoch.clone();
        let account_index = self.account_index;
        let price_update_tx = self.price_update_tx.clone();
        let market_cache = Arc::clone(&self.market_cache);
        let mut orderbook_market_ids: Vec<u32> = Vec::new();
        for sym in &self.tracked_symbols {
            match self.resolve_market_info(sym).await {
                Ok(info) => {
                    if !orderbook_market_ids.contains(&info.market_id) {
                        orderbook_market_ids.push(info.market_id);
                    }
                }
                Err(e) => {
                    log::warn!(
                        "Failed to resolve symbol '{}' for WS order book subscription: {}",
                        sym,
                        e
                    );
                }
            }
        }
        if orderbook_market_ids.is_empty() {
            orderbook_market_ids.push(primary_market_id);
        }
        let default_symbol = self
            .tracked_symbols
            .first()
            .cloned()
            .unwrap_or_else(|| "BTC".to_string());

        // Spawn WebSocket handler task with reconnection logic
        let ws_url_clone = ws_url.clone();
        tokio::spawn(async move {
            use rand::Rng;

            const BACKOFF_MAX_SECS: u64 = 60;
            const BACKOFF_BASE: f64 = 1.5;

            let mut reconnect_attempt = 0u32;
            let mut last_reconnect_time = std::time::SystemTime::now();

            async fn reconnect_backoff(attempt: u32) {
                let pow = BACKOFF_BASE.powi(attempt.min(12) as i32);
                let base_secs = (pow as f64).min(BACKOFF_MAX_SECS as f64);
                let jitter_ms: i64 = rand::thread_rng().gen_range(0..=250);
                let dur = std::time::Duration::from_secs_f64(base_secs)
                    + std::time::Duration::from_millis(jitter_ms as u64);

                log::debug!(
                    "Reconnect backoff: attempt={}, delay={:.1}s",
                    attempt,
                    dur.as_secs_f64()
                );
                tokio::time::sleep(dur).await;
            }

            loop {
                if !is_running.load(Ordering::SeqCst) {
                    log::info!("WebSocket task stopping due to is_running flag");
                    break;
                }

                // Reset attempt counter if enough time has passed since last reconnect
                let now = std::time::SystemTime::now();
                if let Ok(elapsed) = now.duration_since(last_reconnect_time) {
                    if elapsed.as_secs() > 300 {
                        // 5 minutes
                        reconnect_attempt = 0;
                        log::debug!("Reset reconnect attempt counter after successful period");
                    }
                }

                // Respect host-shared WAF cooldown before reconnecting.
                if let Some(remaining) = crate::lighter_waf_cooldown::cooldown_remaining() {
                    log::info!(
                        "WAF cooldown active ({:.0}s remaining), delaying WS reconnect",
                        remaining.as_secs_f64()
                    );
                    tokio::time::sleep(remaining).await;
                }

                if reconnect_attempt > 0 {
                    reconnect_backoff(reconnect_attempt).await;
                } else {
                    // Stagger initial reconnects across bot instances. Default
                    // widened from 15s to 30s (bot-strategy#121): during the
                    // 2026-04-20 cascade (#78) the 15s window was still tight
                    // enough that 4 bots reconnected within ~5s and bursted the
                    // REST refetch past Lighter's per-IP ceiling. 30s spreads
                    // the same traffic over a window twice as wide, halving
                    // peak weight/s while staying well inside Lighter's 60s
                    // rolling window.
                    let reconnect_jitter_secs: u64 =
                        std::env::var("LIGHTER_RECONNECT_JITTER_SECS")
                            .ok()
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(30);
                    if reconnect_jitter_secs > 0 {
                        let jitter =
                            rand::thread_rng().gen_range(0..=reconnect_jitter_secs);
                        log::info!(
                            "Reconnect jitter: sleeping {}s (max {}s)",
                            jitter,
                            reconnect_jitter_secs
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(jitter)).await;
                    }
                }

                log::info!(
                    "Attempting WebSocket connection to: {} (attempt: {})",
                    ws_url_clone,
                    reconnect_attempt + 1
                );
                last_reconnect_time = now;

                // Try to establish connection with optimized configuration
                use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
                let mut config = WebSocketConfig::default();

                // Optimize WebSocket configuration for low latency
                config.max_message_size = Some(64 * 1024 * 1024); // 64MB
                config.max_frame_size = Some(16 * 1024 * 1024); // 16MB
                config.write_buffer_size = 128 * 1024; // 128KB write buffer
                config.max_write_buffer_size = 1024 * 1024; // 1MB max write buffer

                let connection_result = tokio_tungstenite::connect_async_with_config(
                    &ws_url_clone,
                    Some(config),
                    false,
                )
                .await;

                match connection_result {
                    Ok((mut ws_stream, response)) => {
                        let headers = response.headers();
                        let header_value = |name: &str| {
                            headers
                                .get(name)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or("-")
                        };
                        log::info!(
                            "WebSocket handshake ok: status={}, server={}, upgrade={}, connection={}, extensions={}",
                            response.status(),
                            header_value("server"),
                            header_value("upgrade"),
                            header_value("connection"),
                            header_value("sec-websocket-extensions"),
                        );
                        log::debug!("WebSocket handshake headers: {:?}", headers);

                        // Increment connection epoch for race detection
                        let current_epoch = connection_epoch.fetch_add(1, Ordering::SeqCst) + 1;

                        // Extract connection information for logging
                        let (local_addr, peer_addr, stream_kind) = match ws_stream.get_ref() {
                            tokio_tungstenite::MaybeTlsStream::Rustls(tls_stream) => {
                                let tcp_stream = tls_stream.get_ref().0;

                                // Apply TCP optimizations for TLS connection
                                if let Err(e) = tcp_stream.set_nodelay(true) {
                                    log::debug!("Failed to set TCP_NODELAY on TLS: {}", e);
                                }

                                match (tcp_stream.local_addr(), tcp_stream.peer_addr()) {
                                    (Ok(local), Ok(peer)) => (local, peer, "rustls"),
                                    _ => {
                                        log::debug!(
                                            "Failed to get TLS socket addresses for epoch {}",
                                            current_epoch
                                        );
                                        (
                                            "0.0.0.0:0".parse().unwrap(),
                                            "0.0.0.0:0".parse().unwrap(),
                                            "rustls",
                                        )
                                    }
                                }
                            }
                            tokio_tungstenite::MaybeTlsStream::NativeTls(tls_stream) => {
                                let tcp_stream = tls_stream.get_ref().get_ref().get_ref();

                                // Apply TCP optimizations for native TLS connection
                                if let Err(e) = tcp_stream.set_nodelay(true) {
                                    log::debug!("Failed to set TCP_NODELAY on native TLS: {}", e);
                                }

                                match (tcp_stream.local_addr(), tcp_stream.peer_addr()) {
                                    (Ok(local), Ok(peer)) => (local, peer, "native-tls"),
                                    _ => {
                                        log::debug!(
                                            "Failed to get native TLS socket addresses for epoch {}",
                                            current_epoch
                                        );
                                        (
                                            "0.0.0.0:0".parse().unwrap(),
                                            "0.0.0.0:0".parse().unwrap(),
                                            "native-tls",
                                        )
                                    }
                                }
                            }
                            tokio_tungstenite::MaybeTlsStream::Plain(tcp_stream) => {
                                if let Err(e) = tcp_stream.set_nodelay(true) {
                                    log::debug!("Failed to set TCP_NODELAY on plain WS: {}", e);
                                }
                                match (tcp_stream.local_addr(), tcp_stream.peer_addr()) {
                                    (Ok(local), Ok(peer)) => (local, peer, "plain"),
                                    _ => {
                                        log::debug!(
                                            "Failed to get plain socket addresses for epoch {}",
                                            current_epoch
                                        );
                                        (
                                            "0.0.0.0:0".parse().unwrap(),
                                            "0.0.0.0:0".parse().unwrap(),
                                            "plain",
                                        )
                                    }
                                }
                            }
                            other => {
                                log::debug!(
                                    "Unsupported WebSocket stream type {:?} for epoch {}",
                                    other,
                                    current_epoch
                                );
                                (
                                    "0.0.0.0:0".parse().unwrap(),
                                    "0.0.0.0:0".parse().unwrap(),
                                    "unknown",
                                )
                            }
                        };

                        let epoch_prefix = format!("[{:03}]", current_epoch);
                        let conn_label = format!(
                            "{} {} -> {} ({})",
                            epoch_prefix, local_addr, peer_addr, stream_kind
                        );

                        log::info!(
                            "{} WebSocket connected successfully: {} -> {} ({})",
                            epoch_prefix,
                            local_addr,
                            peer_addr,
                            stream_kind
                        );

                        // Send subscription messages
                        let subscribe_account = serde_json::json!({
                            "type": "subscribe",
                            "channel": format!("account_all/{}", account_index)
                        });

                        for market_id in &orderbook_market_ids {
                            let subscribe_orderbook = serde_json::json!({
                                "type": "subscribe",
                                "channel": format!("order_book/{}", market_id)
                            });
                            log::info!(
                                "🔗 [WS_DEBUG] Sending orderbook subscription: {}",
                                subscribe_orderbook
                            );

                            if let Err(e) = ws_stream
                                .send(tokio_tungstenite::tungstenite::Message::Text(
                                    subscribe_orderbook.to_string(),
                                ))
                                .await
                            {
                                log::error!(
                                    "Failed to send orderbook subscription for {}: {}",
                                    market_id,
                                    e
                                );
                                continue;
                            }
                        }

                        // Subscribe to market_stats for every tracked market so the
                        // funding_rate field arrives over WS and get_ticker no longer
                        // needs the `/funding-rates` REST poll. See bot-strategy#162.
                        for market_id in &orderbook_market_ids {
                            let subscribe_market_stats = serde_json::json!({
                                "type": "subscribe",
                                "channel": format!("market_stats/{}", market_id)
                            });
                            if let Err(e) = ws_stream
                                .send(tokio_tungstenite::tungstenite::Message::Text(
                                    subscribe_market_stats.to_string(),
                                ))
                                .await
                            {
                                log::error!(
                                    "Failed to send market_stats subscription for {}: {}",
                                    market_id,
                                    e
                                );
                                continue;
                            }
                        }

                        if let Err(e) = ws_stream
                            .send(tokio_tungstenite::tungstenite::Message::Text(
                                subscribe_account.to_string(),
                            ))
                            .await
                        {
                            log::error!("Failed to send account subscription: {}", e);
                            continue;
                        }

                        log::info!("WebSocket subscriptions sent successfully");

                        // Single-task pump architecture: no split(), unified read/write with select!
                        use futures::sink::SinkExt;
                        use futures::stream::StreamExt;

                        // Split stream for shared access but use channels for coordination
                        let (write, mut read) = ws_stream.split();

                        // Shared writer protected by Arc<Mutex>
                        let ws_writer_arc = Arc::new(tokio::sync::Mutex::new(write));
                        let ws_writer_for_reader = ws_writer_arc.clone();

                        // Control channel for high-priority messages (ping/pong)
                        let (tx_ctrl, mut rx_ctrl) =
                            tokio::sync::mpsc::channel::<OutboundMessage>(32);
                        let tx_ctrl_for_reader = tx_ctrl.clone();
                        let connection_alive = Arc::new(AtomicBool::new(true));

                        // Create unified writer task with priority handling
                        let writer_is_running = is_running.clone();
                        let writer_connection_alive = connection_alive.clone();
                        let ws_writer_for_task = ws_writer_arc.clone();
                        let writer_conn_label = conn_label.clone();
                        let writer_task = tokio::spawn(async move {
                            loop {
                                if !writer_is_running.load(Ordering::SeqCst)
                                    || !writer_connection_alive.load(Ordering::SeqCst)
                                {
                                    break;
                                }

                                // Handle control messages
                                let (msg, _) = tokio::select! {
                                    // High priority channel (control messages like Pong)
                                    Some(msg) = rx_ctrl.recv() => (msg, true),
                                    else => break,
                                };

                                let send_start = std::time::Instant::now();
                                let is_pong = msg.is_pong();

                                // Use shared writer with short-lived lock
                                let mut ws_write = ws_writer_for_task.lock().await;
                                if let Err(e) = ws_write.send(msg.into_message()).await {
                                    log::error!(
                                        "WebSocket send failed: {:?} (conn={})",
                                        e,
                                        writer_conn_label
                                    );
                                    break;
                                }

                                let send_duration = send_start.elapsed();

                                if is_pong {
                                    let latency_ms = send_duration.as_millis();
                                    if latency_ms > 100 {
                                        log::debug!("High pong send latency: {}ms", latency_ms);
                                    }
                                }
                            }

                            log::debug!("WebSocket writer task terminated");
                        });

                        // Heartbeat strategy: WebSocket control frame ping-pong only
                        // Server requires at least one frame every 2 minutes to keep connection alive.
                        // We send a control Ping every 20s which is well within that limit.
                        const IDLE_PING_SECS: u64 = 20; // Client ping interval
                        // Pong timeout: tolerate transient RTT spikes (cross-region WS to AWS
                        // can briefly stall under congestion). Combined with the 5s check
                        // granularity and the 2-strike rule below, a real dead connection is
                        // still detected within ~40s, while one-shot packet loss no longer
                        // forces a reconnect (bot-strategy#47).
                        const PONG_TIMEOUT_SECS: u64 = 15;
                        const PONG_MAX_MISSES: u32 = 2; // Require N consecutive misses before reconnect
                        const HEARTBEAT_CHECK_SECS: u64 = 5; // Check interval for heartbeat logic

                        fn get_pong_payload(ping_payload: &[u8]) -> Vec<u8> {
                            // Always echo for strict server compliance
                            ping_payload.to_vec()
                        }

                        use parking_lot::Mutex;
                        use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64};
                        use std::time::{SystemTime, UNIX_EPOCH};

                        fn now_secs() -> u64 {
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs()
                        }

                        let last_rx = std::sync::Arc::new(AtomicU64::new(now_secs()));
                        let last_tx = std::sync::Arc::new(AtomicU64::new(now_secs()));
                        let pending_client_ping = std::sync::Arc::new(AtomicBool::new(false));
                        // Track ping send time independently of last_tx so concurrent
                        // app-level outbound traffic can't reset the pong timeout window.
                        let ping_sent_at = std::sync::Arc::new(AtomicU64::new(0));
                        let pong_miss_count = std::sync::Arc::new(AtomicU32::new(0));
                        let last_client_ping_payload =
                            std::sync::Arc::new(Mutex::new(Vec::<u8>::new()));

                        // Create ping/heartbeat task with priority channel access
                        let ping_is_running = is_running.clone();
                        let ping_connection_alive = connection_alive.clone();
                        let ping_last_tx = last_tx.clone();
                        let ping_pending_client_ping = pending_client_ping.clone();
                        let ping_sent_at_clone = ping_sent_at.clone();
                        let pong_miss_count_clone = pong_miss_count.clone();
                        let ping_last_client_ping_payload = last_client_ping_payload.clone();
                        let ping_tx_ctrl = tx_ctrl.clone();
                        let ping_conn_label = conn_label.clone();

                        let ping_task = tokio::spawn(async move {
                            let mut heartbeat_interval = tokio::time::interval(
                                std::time::Duration::from_secs(HEARTBEAT_CHECK_SECS),
                            );
                            heartbeat_interval
                                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                            loop {
                                tokio::select! {
                                    // Handle heartbeat check interval
                                    _ = heartbeat_interval.tick() => {
                                        if !ping_is_running.load(Ordering::SeqCst)
                                            || !ping_connection_alive.load(Ordering::SeqCst)
                                        {
                                            break;
                                        }

                                        let now = now_secs();
                                        let idle_tx = now.saturating_sub(ping_last_tx.load(Ordering::SeqCst));

                                        // Send WebSocket control Ping regularly (every 20s)
                                        // Server requires at least one frame every 2 minutes
                                        if !ping_pending_client_ping.load(Ordering::SeqCst)
                                            && idle_tx >= IDLE_PING_SECS
                                        {
                                            let payload: [u8; 8] = (now as u64).to_be_bytes();
                                            *ping_last_client_ping_payload.lock() = payload.to_vec();

                                            let ping_msg = OutboundMessage::Control(
                                                tokio_tungstenite::tungstenite::Message::Ping(payload.to_vec())
                                            );
                                            if let Err(e) = ping_tx_ctrl.send(ping_msg).await {
                                                log::warn!(
                                                    "Failed to send client ping: {:?} (conn={})",
                                                    e,
                                                    ping_conn_label
                                                );
                                                break;
                                            }

                                            ping_pending_client_ping.store(true, Ordering::SeqCst);
                                            ping_sent_at_clone.store(now, Ordering::SeqCst);
                                            ping_last_tx.store(now, Ordering::SeqCst);
                                            log::debug!(
                                                "Sent client ping (conn={}, payload={:?})",
                                                ping_conn_label,
                                                payload
                                            );
                                        }

                                        // Check for control frame pong timeout
                                        if ping_pending_client_ping.load(Ordering::SeqCst) {
                                            let sent_at = ping_sent_at_clone.load(Ordering::SeqCst);
                                            let waited = now.saturating_sub(sent_at);
                                            if waited >= PONG_TIMEOUT_SECS {
                                                let misses = pong_miss_count_clone
                                                    .fetch_add(1, Ordering::SeqCst)
                                                    + 1;
                                                if misses < PONG_MAX_MISSES {
                                                    log::info!(
                                                        "Pong missed ({}s, {}/{} strikes), retrying (conn={})",
                                                        waited,
                                                        misses,
                                                        PONG_MAX_MISSES,
                                                        ping_conn_label,
                                                    );
                                                    // Allow a fresh ping next tick instead of reconnecting
                                                    ping_pending_client_ping
                                                        .store(false, Ordering::SeqCst);
                                                } else {
                                                    log::warn!(
                                                        "Pong timeout ({}s, {} strikes), reconnecting (conn={})",
                                                        waited,
                                                        misses,
                                                        ping_conn_label,
                                                    );
                                                    let close_msg = OutboundMessage::Control(
                                                        tokio_tungstenite::tungstenite::Message::Close(None)
                                                    );
                                                    let _ = ping_tx_ctrl.send(close_msg).await;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            log::debug!("Heartbeat task ended");
                        });

                        // Handle messages in this connection with performance tracking
                        log::debug!("Starting WebSocket message handling loop");

                        while let Some(message) = read.next().await {
                            if !is_running.load(Ordering::SeqCst) {
                                log::info!("WebSocket stopping due to is_running flag");
                                break;
                            }

                            match message {
                                Ok(message) => match message {
                                    tokio_tungstenite::tungstenite::Message::Text(text) => {
                                        let msg_start = std::time::Instant::now();
                                        // Update last received timestamp for any text message
                                        let now = now_secs();
                                        last_rx.store(now, Ordering::SeqCst);

                                        if let Ok(parsed) = serde_json::from_str::<Value>(&text) {
                                            let msg_type = parsed
                                                .get("type")
                                                .and_then(|t| t.as_str())
                                                .unwrap_or("");
                                            let channel = parsed
                                                .get("channel")
                                                .and_then(|c| c.as_str())
                                                .unwrap_or("");
                                            let offset = parsed
                                                .get("offset")
                                                .and_then(|o| o.as_i64())
                                                .unwrap_or_default();
                                            log::trace!(
                                                "WebSocket msg type={} channel={} offset={}",
                                                msg_type,
                                                channel,
                                                offset
                                            );
                                            // Skip application-layer ping/pong messages (server no longer sends them)
                                            if let Some(msg_type) =
                                                parsed.get("type").and_then(|t| t.as_str())
                                            {
                                                if msg_type == "ping" || msg_type == "pong" {
                                                    log::debug!(
                                                        "Ignoring application-layer {} (conn={})",
                                                        msg_type,
                                                        conn_label
                                                    );
                                                    continue;
                                                }
                                            }

                                            Self::handle_websocket_message(
                                                parsed,
                                                &current_price,
                                                &current_volume,
                                                &order_book,
                                                &filled_orders,
                                                &canceled_orders,
                                                &cached_open_orders,
                                                &cached_positions,
                                                &positions_ready,
                                                &balance_cache,
                                                &cached_collateral,
                                                &funding_rate_cache,
                                                account_index,
                                                &market_cache,
                                                default_symbol.as_str(),
                                                &price_update_tx,
                                            )
                                            .await;

                                            let total_duration = msg_start.elapsed();

                                            // Log slow messages only
                                            if total_duration.as_millis() > 10 {
                                                log::debug!(
                                                    "Slow message processing: {}ms (len={})",
                                                    total_duration.as_millis(),
                                                    text.len()
                                                );
                                            }
                                        } else {
                                            log::debug!(
                                                "Failed to parse WebSocket message as JSON (len={}): {}",
                                                text.len(),
                                                text
                                            );
                                        }
                                    }
                                    tokio_tungstenite::tungstenite::Message::Ping(payload) => {
                                        // Server ping -> immediate manual pong response
                                        log::debug!(
                                            "Received control ping: {} bytes (conn={})",
                                            payload.len(),
                                            conn_label
                                        );
                                        let now = now_secs();
                                        last_rx.store(now, Ordering::SeqCst);

                                        // Echo pong payload for server compliance
                                        let pong_payload = get_pong_payload(&payload);

                                        // Get current epoch for race detection logging
                                        let current_epoch = connection_epoch.load(Ordering::SeqCst);

                                        // Direct pong send - bypass writer task for immediate response
                                        if let Ok(mut ws_write) = ws_writer_for_reader.try_lock() {
                                            if let Err(e) = ws_write
                                                .send(
                                                    tokio_tungstenite::tungstenite::Message::Pong(
                                                        pong_payload.clone(),
                                                    ),
                                                )
                                                .await
                                            {
                                                log::error!(
                                                    "🚨 [CRITICAL] Failed to send pong directly: {:?} (conn={})",
                                                    e,
                                                    conn_label
                                                );
                                                break;
                                            }
                                            if let Err(e) =
                                                futures::SinkExt::flush(&mut *ws_write).await
                                            {
                                                log::error!(
                                                    "🚨 [CRITICAL] Failed to flush after pong: {:?} (conn={})",
                                                    e,
                                                    conn_label
                                                );
                                                break;
                                            }
                                            last_tx.store(now, Ordering::SeqCst);

                                            // Verify same connection epoch for race detection
                                            let pong_epoch =
                                                connection_epoch.load(Ordering::SeqCst);

                                            if pong_epoch != current_epoch {
                                                log::error!(
                                                    "Pong epoch mismatch: ping={} pong={}",
                                                    current_epoch,
                                                    pong_epoch
                                                );
                                            }
                                        } else {
                                            // Fallback to try_lock with retry for up to 200ms
                                            let mut pong_sent = false;
                                            for retry in 0..4 {
                                                tokio::time::sleep(
                                                    std::time::Duration::from_millis(50),
                                                )
                                                .await;
                                                if let Ok(mut ws_write) =
                                                    ws_writer_for_reader.try_lock()
                                                {
                                                    if let Err(e) = ws_write.send(tokio_tungstenite::tungstenite::Message::Pong(pong_payload.clone())).await {
                                                        log::error!(
                                                            "Failed to send pong on retry {}: {:?} (conn={})",
                                                            retry,
                                                            e,
                                                            conn_label
                                                        );
                                                        break;
                                                    }
                                                    if let Err(e) =
                                                        futures::SinkExt::flush(&mut *ws_write)
                                                            .await
                                                    {
                                                        log::error!(
                                                            "Failed to flush after pong retry {}: {:?} (conn={})",
                                                            retry,
                                                            e,
                                                            conn_label
                                                        );
                                                        break;
                                                    }
                                                    last_tx.store(now, Ordering::SeqCst);

                                                    pong_sent = true;
                                                    break;
                                                }
                                            }
                                            if !pong_sent {
                                                log::warn!(
                                                    "Pong timeout, closing connection (conn={})",
                                                    conn_label
                                                );
                                                // Proactively close to trigger reconnect before server timeout
                                                let mut ws_write =
                                                    ws_writer_for_reader.lock().await;
                                                let _ = ws_write.close().await;
                                                break;
                                            }
                                        }
                                    }
                                    tokio_tungstenite::tungstenite::Message::Pong(payload) => {
                                        // Pong response - check if it matches our client ping
                                        let now = now_secs();
                                        last_rx.store(now, Ordering::SeqCst);

                                        let expected_payload =
                                            last_client_ping_payload.lock().clone();
                                        if !expected_payload.is_empty()
                                            && payload == expected_payload
                                        {
                                            pending_client_ping.store(false, Ordering::SeqCst);
                                            pong_miss_count.store(0, Ordering::SeqCst);
                                            log::debug!(
                                                "Received control pong (conn={}, payload={:?})",
                                                conn_label,
                                                payload
                                            );
                                        } else if expected_payload.is_empty() {
                                            log::debug!(
                                                "Received unsolicited control pong (conn={}, payload={:?})",
                                                conn_label,
                                                payload
                                            );
                                        } else {
                                            log::warn!(
                                                "Control pong payload mismatch (conn={}, expected={:?}, got={:?})",
                                                conn_label,
                                                expected_payload,
                                                payload
                                            );
                                        }
                                    }
                                    tokio_tungstenite::tungstenite::Message::Close(frame) => {
                                        let now = now_secs();
                                        let idle_rx =
                                            now.saturating_sub(last_rx.load(Ordering::SeqCst));
                                        let idle_tx =
                                            now.saturating_sub(last_tx.load(Ordering::SeqCst));
                                        log::warn!(
                                            "WebSocket close frame received: {:?} (conn={}, idle_rx={}s, idle_tx={}s, pending_client_ping={})",
                                            frame,
                                            conn_label,
                                            idle_rx,
                                            idle_tx,
                                            pending_client_ping.load(Ordering::SeqCst),
                                        );
                                        break;
                                    }
                                    tokio_tungstenite::tungstenite::Message::Binary(data) => {
                                        log::debug!(
                                            "Received binary WebSocket message: {} bytes",
                                            data.len()
                                        );
                                    }
                                    tokio_tungstenite::tungstenite::Message::Frame(_) => {
                                        log::trace!("Received raw WebSocket frame");
                                    }
                                },
                                Err(e) => {
                                    let now = now_secs();
                                    let last_rx_at = last_rx.load(Ordering::SeqCst);
                                    let last_tx_at = last_tx.load(Ordering::SeqCst);
                                    // Protocol errors (e.g. ResetWithoutClosingHandshake from the
                                    // upstream Lighter/CloudFront edge) are bursty, correlated
                                    // across services, and auto-recover via reconnection. Demote
                                    // to INFO so they do not pollute error_summary and trigger
                                    // false alerts in the error-watch workflow
                                    // (shigeo-nakamura/bot-strategy#49). IO/TLS errors indicate
                                    // genuinely unusual conditions and stay at ERROR.
                                    let is_protocol = matches!(
                                        &e,
                                        tokio_tungstenite::tungstenite::Error::Protocol(_)
                                    );
                                    let is_benign_io = matches!(
                                        &e,
                                        tokio_tungstenite::tungstenite::Error::Io(io_err)
                                            if matches!(
                                                io_err.kind(),
                                                std::io::ErrorKind::ConnectionReset
                                                    | std::io::ErrorKind::UnexpectedEof
                                                    | std::io::ErrorKind::BrokenPipe
                                            )
                                    );
                                    if is_protocol {
                                        log::info!(
                                            "WebSocket error: {} (type: {:?}) (conn={}, idle_rx={}s, idle_tx={}s, pending_client_ping={}). Will attempt reconnection.",
                                            e,
                                            std::any::type_name_of_val(&e),
                                            conn_label,
                                            now.saturating_sub(last_rx_at),
                                            now.saturating_sub(last_tx_at),
                                            pending_client_ping.load(Ordering::SeqCst),
                                        );
                                    } else if is_benign_io {
                                        log::warn!(
                                            "WebSocket error: {} (type: {:?}) (conn={}, idle_rx={}s, idle_tx={}s, pending_client_ping={}). Will attempt reconnection.",
                                            e,
                                            std::any::type_name_of_val(&e),
                                            conn_label,
                                            now.saturating_sub(last_rx_at),
                                            now.saturating_sub(last_tx_at),
                                            pending_client_ping.load(Ordering::SeqCst),
                                        );
                                    } else {
                                        log::error!(
                                            "WebSocket error: {} (type: {:?}) (conn={}, idle_rx={}s, idle_tx={}s, pending_client_ping={}). Will attempt reconnection.",
                                            e,
                                            std::any::type_name_of_val(&e),
                                            conn_label,
                                            now.saturating_sub(last_rx_at),
                                            now.saturating_sub(last_tx_at),
                                            pending_client_ping.load(Ordering::SeqCst),
                                        );
                                    }
                                    match &e {
                                        tokio_tungstenite::tungstenite::Error::Protocol(
                                            protocol_err,
                                        ) => {
                                            log::info!(
                                                "WebSocket protocol error detail: {:?} (conn={})",
                                                protocol_err,
                                                conn_label
                                            );
                                        }
                                        tokio_tungstenite::tungstenite::Error::Io(io_err) => {
                                            if is_benign_io {
                                                log::warn!(
                                                    "WebSocket IO error detail: kind={:?}, error={} (conn={})",
                                                    io_err.kind(),
                                                    io_err,
                                                    conn_label
                                                );
                                            } else {
                                                log::error!(
                                                    "WebSocket IO error detail: kind={:?}, error={} (conn={})",
                                                    io_err.kind(),
                                                    io_err,
                                                    conn_label
                                                );
                                            }
                                        }
                                        tokio_tungstenite::tungstenite::Error::Tls(tls_err) => {
                                            log::error!(
                                                "WebSocket TLS error detail: {:?} (conn={})",
                                                tls_err,
                                                conn_label
                                            );
                                        }
                                        _ => {}
                                    }
                                    break; // Break inner loop to attempt reconnection
                                }
                            }
                        }

                        connection_alive.store(false, Ordering::SeqCst);
                        drop(tx_ctrl_for_reader);
                        drop(tx_ctrl);
                        writer_task.abort();
                        ping_task.abort();

                        // NOTE: Do NOT clear order book cache here.
                        // Clearing causes "order book snapshot unavailable" errors
                        // during the gap between reconnection and the first OB update.
                        // The staleness check (ob_stale_after) will naturally expire
                        // stale entries, and the reconnected WS will replace them
                        // with fresh data once the first update arrives.
                        // See: dex-connector#2
                        log::info!(
                            "WebSocket disconnected (conn={}), keeping OB cache (will expire via staleness check)",
                            conn_label
                        );

                        reconnect_attempt += 1;
                    }
                    Err(e) => {
                        reconnect_attempt += 1;

                        // Check for 429 Too Many Requests
                        let error_str = e.to_string();
                        if error_str.contains("429") || error_str.contains("Too Many Requests") {
                            log::error!(
                                "WebSocket connection failed with rate limit (429): {}. Attempt: {}. Using exponential backoff.",
                                e, reconnect_attempt
                            );
                        } else {
                            log::error!(
                                "Failed to connect to WebSocket: {}. Attempt: {}. Will retry with backoff.",
                                e, reconnect_attempt
                            );
                        }
                    }
                }
            }

            log::info!("WebSocket task ended");
        });

        Ok(())
    }

    async fn handle_websocket_message(
        message: Value,
        current_price: &Arc<RwLock<HashMap<String, (Decimal, u64)>>>,
        current_volume: &Arc<RwLock<Option<Decimal>>>,
        order_book: &Arc<RwLock<HashMap<u32, LighterOrderBookCacheEntry>>>,
        filled_orders: &Arc<RwLock<HashMap<String, Vec<FilledOrder>>>>,
        canceled_orders: &Arc<RwLock<HashMap<String, Vec<CanceledOrder>>>>,
        cached_open_orders: &Arc<RwLock<HashMap<String, Vec<OpenOrder>>>>,
        cached_positions: &Arc<RwLock<Vec<PositionSnapshot>>>,
        positions_ready: &Arc<AtomicBool>,
        balance_cache: &Arc<RwLock<Option<(BalanceResponse, Instant)>>>,
        cached_collateral: &Arc<RwLock<Option<Decimal>>>,
        funding_rate_cache: &Arc<RwLock<HashMap<u32, Decimal>>>,
        account_index: u64,
        market_cache: &Arc<RwLock<MarketCache>>,
        default_symbol: &str,
        price_update_tx: &tokio::sync::broadcast::Sender<crate::PriceUpdate>,
    ) {
        let msg_type = message.get("type").and_then(|t| t.as_str()).unwrap_or("");

        match msg_type {
            "subscribed/order_book" | "update/order_book" => {
                if let Some(order_book_data) = message.get("order_book") {
                    if let Ok(ob) =
                        serde_json::from_value::<LighterOrderBook>(order_book_data.clone())
                    {
                        // Resolve symbol for this order book update using the channel hint (order_book/<market_id>)
                        let channel = message
                            .get("channel")
                            .and_then(|c| c.as_str())
                            .unwrap_or_default();
                        // Server sometimes returns channel as "order_book:1" instead of "order_book/1"
                        let market_id = channel
                            .rsplit(|c| c == '/' || c == ':')
                            .next()
                            .and_then(|id| id.parse::<u32>().ok());
                        let symbol = match market_id {
                            Some(market_id) => {
                                let cache = market_cache.read().await;
                                cache
                                    .by_id
                                    .get(&market_id)
                                    .map(|info| info.canonical_symbol.clone())
                            }
                            None => None,
                        };
                        let symbol = match symbol {
                            Some(symbol) => symbol,
                            None => {
                                if let Some(market_id) = market_id {
                                    log::warn!(
                                        "[WS] order_book update missing symbol for market_id={} (channel='{}'); skipping",
                                        market_id,
                                        channel
                                    );
                                } else {
                                    log::warn!(
                                        "[WS] order_book update missing market_id (channel='{}'); skipping",
                                        channel
                                    );
                                }
                                return;
                            }
                        };
                        log::debug!(
                            "[WS_OB] channel='{}' market_id={:?} resolved_symbol={}",
                            channel,
                            market_id,
                            symbol
                        );

                        // Update current price from best bid/ask
                        if let (Some(best_bid), Some(best_ask)) = (ob.bids.first(), ob.asks.first())
                        {
                            if let (Ok(bid_price), Ok(ask_price)) = (
                                string_to_decimal(Some(best_bid.price.clone())),
                                string_to_decimal(Some(best_ask.price.clone())),
                            ) {
                                let mid_price = (bid_price + ask_price) / Decimal::from(2);

                                // Cross-symbol contamination guard: if we already have a
                                // known price for this symbol, reject OB updates where
                                // bid/ask deviates more than 50% from it.
                                let is_contaminated = {
                                    let prices = current_price.read().await;
                                    if let Some(&(known_price, _)) = prices.get(&symbol) {
                                        if known_price > Decimal::ZERO {
                                            let ratio = if mid_price > known_price {
                                                mid_price / known_price
                                            } else {
                                                known_price / mid_price
                                            };
                                            ratio > Decimal::new(15, 1) // 1.5x = 50% deviation
                                        } else {
                                            false
                                        }
                                    } else {
                                        false
                                    }
                                };

                                if is_contaminated {
                                    log::info!(
                                        "[WS_OB] REJECTED contaminated OB for {} (market_id={:?}): \
                                         bid={} ask={} mid={} deviates >50% from known price",
                                        symbol,
                                        market_id,
                                        bid_price,
                                        ask_price,
                                        mid_price
                                    );
                                    return;
                                }

                                // Prefer the exchange-side `last_updated_at` (microseconds since
                                // epoch) so that all bots observing the same WS feed share an
                                // identical timestamp for the same update. Required for the
                                // multi-bot bar-alignment fix (pairtrade#4). Stored in ms so
                                // within-second orderings are preserved for bar bucketing
                                // (bot-strategy#274 / #276).
                                let exchange_ts_ms = message
                                    .get("last_updated_at")
                                    .and_then(|v| v.as_u64())
                                    .map(|us| us / 1_000);
                                let timestamp = exchange_ts_ms.unwrap_or_else(|| {
                                    std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_millis() as u64
                                });
                                log::debug!(
                                    "[WS_OB] {} best_bid={} best_ask={} mid={} ts_ms={}{}",
                                    symbol,
                                    bid_price,
                                    ask_price,
                                    mid_price,
                                    timestamp,
                                    if exchange_ts_ms.is_some() { " (exchange)" } else { " (local)" }
                                );
                                current_price
                                    .write()
                                    .await
                                    .insert(symbol.clone(), (mid_price, timestamp));
                                // Broadcast real-time price update to subscribers
                                let _ = price_update_tx.send(crate::PriceUpdate {
                                    symbol: symbol.clone(),
                                    mid_price,
                                    best_bid: bid_price,
                                    best_ask: ask_price,
                                    timestamp,
                                });
                                log::trace!(
                                    "Updated price from WebSocket: {} at {} ({})",
                                    mid_price,
                                    timestamp,
                                    symbol
                                );
                            }
                        }

                        // Calculate volume from order book
                        let total_volume: Decimal = ob
                            .bids
                            .iter()
                            .chain(ob.asks.iter())
                            .filter_map(|entry| string_to_decimal(Some(entry.size.clone())).ok())
                            .sum();
                        *current_volume.write().await = Some(total_volume);

                        if let Some(market_id) = market_id {
                            {
                                let mut ob_guard = order_book.write().await;
                                // For delta updates, merge with existing cache:
                                // keep the existing side if the new update has no entries for it
                                let merged_ob = if let Some(existing) = ob_guard.get(&market_id) {
                                    let merged_bids = if ob.bids.is_empty() {
                                        existing.order_book.bids.clone()
                                    } else {
                                        ob.bids
                                    };
                                    let merged_asks = if ob.asks.is_empty() {
                                        existing.order_book.asks.clone()
                                    } else {
                                        ob.asks
                                    };
                                    LighterOrderBook {
                                        bids: merged_bids,
                                        asks: merged_asks,
                                    }
                                } else {
                                    ob
                                };
                                ob_guard.insert(
                                    market_id,
                                    LighterOrderBookCacheEntry {
                                        order_book: merged_ob,
                                        updated_at: Instant::now(),
                                    },
                                );
                            }
                            log::debug!(
                                "[WS_OB] cached order book for {} (market_id={}, channel='{}')",
                                symbol,
                                market_id,
                                channel
                            );
                        }
                    }
                }
            }
            "subscribed/market_stats" | "update/market_stats" => {
                Self::handle_market_stats_update(&message, funding_rate_cache).await;
            }
            "subscribed/account_all" | "update/account_all" => {
                log::trace!(
                    "Received account message: type={}, message={:?}",
                    msg_type,
                    message
                );
                // Handle account updates (filled/canceled orders)
                // For Lighter DEX, the data is directly in the message, not in a 'data' field
                Self::handle_account_update(
                    &message,
                    msg_type,
                    filled_orders,
                    canceled_orders,
                    cached_open_orders,
                    cached_positions,
                    positions_ready,
                    balance_cache,
                    cached_collateral,
                    account_index as u64,
                    market_cache,
                    default_symbol,
                )
                .await;
            }
            _ => {
                log::trace!("Unhandled WebSocket message type: {}", msg_type);
            }
        }
    }

    /// Extract the per-market funding rate from a `market_stats/{id}` push and
    /// store it in the shared cache. Lighter's payload carries both
    /// `funding_rate` (realized at the most recent funding_timestamp) and
    /// `current_funding_rate` (running estimate for the next payment); we keep
    /// the realized value to match what `/funding-rates` REST exposed and what
    /// the strategy is calibrated against. See bot-strategy#162.
    async fn handle_market_stats_update(
        message: &Value,
        funding_rate_cache: &Arc<RwLock<HashMap<u32, Decimal>>>,
    ) {
        let channel = message
            .get("channel")
            .and_then(|c| c.as_str())
            .unwrap_or_default();
        // The server accepts `market_stats/<id>` on subscribe but delivers
        // `market_stats:<id>` on the push, matching the order_book pattern.
        let market_id = channel
            .rsplit(|c| c == '/' || c == ':')
            .next()
            .and_then(|id| id.parse::<u32>().ok());
        let Some(market_id) = market_id else {
            log::warn!(
                "[WS] market_stats update missing market_id (channel='{}'); skipping",
                channel
            );
            return;
        };
        let rate_str = message
            .get("market_stats")
            .and_then(|s| s.get("funding_rate"))
            .and_then(|v| v.as_str());
        let Some(rate_str) = rate_str else {
            log::trace!(
                "[WS] market_stats for market_id={} has no funding_rate field (channel='{}')",
                market_id,
                channel
            );
            return;
        };
        match string_to_decimal(Some(rate_str.to_string())) {
            Ok(rate) => {
                funding_rate_cache
                    .write()
                    .await
                    .insert(market_id, rate);
                log::trace!(
                    "[WS_FUNDING] market_id={} funding_rate={}",
                    market_id,
                    rate
                );
            }
            Err(e) => {
                log::warn!(
                    "[WS] failed to parse funding_rate='{}' for market_id={}: {}",
                    rate_str,
                    market_id,
                    e
                );
            }
        }
    }

    async fn handle_account_update(
        data: &Value,
        msg_type: &str,
        filled_orders: &Arc<RwLock<HashMap<String, Vec<FilledOrder>>>>,
        canceled_orders: &Arc<RwLock<HashMap<String, Vec<CanceledOrder>>>>,
        cached_open_orders: &Arc<RwLock<HashMap<String, Vec<OpenOrder>>>>,
        cached_positions: &Arc<RwLock<Vec<PositionSnapshot>>>,
        positions_ready: &Arc<AtomicBool>,
        balance_cache: &Arc<RwLock<Option<(BalanceResponse, Instant)>>>,
        cached_collateral: &Arc<RwLock<Option<Decimal>>>,
        account_id: u64,
        market_cache: &Arc<RwLock<MarketCache>>,
        default_symbol: &str,
    ) {
        positions_ready.store(true, Ordering::SeqCst);
        log::trace!("handle_account_update called with data: {:?}", data);
        if std::env::var("LIGHTER_WS_ACCOUNT_DUMP").ok().as_deref() == Some("1") {
            log::info!("[WS_ACCOUNT_DUMP] {}", data.to_string());
        }

        // Handle positions update (can be array or object). Track unrealized_pnl
        // alongside the snapshot fields so we can re-derive perp equity from
        // WS without hitting REST (bot-strategy#239).
        let positions_vals: Option<Vec<Value>> =
            if let Some(arr) = data.get("positions").and_then(|p| p.as_array()) {
                Some(arr.clone())
            } else if let Some(map) = data.get("positions").and_then(|p| p.as_object()) {
                Some(map.values().cloned().collect())
            } else {
                None
            };
        // None when no positions field was carried in this message;
        // Some(sum) — possibly zero — when the message did carry positions.
        // The distinction matters: a missing positions field must NOT cause
        // us to treat unrealized_pnl as 0 (which would corrupt the cached
        // equity); a present-but-all-flat positions field correctly means 0.
        let mut sum_unrealized_pnl: Option<Decimal> = None;
        if let Some(vals) = positions_vals {
            let allow_empty = msg_type == "subscribed/account_all";
            if vals.is_empty() && !allow_empty {
                log::debug!(
                    "[WS_ACCOUNT_DUMP] Skipping empty positions update for {}",
                    msg_type
                );
            } else {
                let mut updates: Vec<(String, Option<PositionSnapshot>)> = Vec::new();
                let mut acc = Decimal::ZERO;
                for pos_val in vals {
                    if let Ok(position) = serde_json::from_value::<LighterPosition>(pos_val) {
                        if let Ok(pnl) = Decimal::from_str(&position.unrealized_pnl) {
                            acc += pnl;
                        }
                        let size = match Decimal::from_str(&position.position) {
                            Ok(s) => s.abs(),
                            Err(_) => Decimal::ZERO,
                        };
                        if size.is_zero() {
                            updates.push((position.symbol.clone(), None));
                            continue;
                        }
                        let entry_price = Decimal::from_str(&position.avg_entry_price).ok();
                        updates.push((
                            position.symbol.clone(),
                            Some(PositionSnapshot {
                                symbol: position.symbol,
                                size,
                                sign: position.sign as i32,
                                entry_price,
                            }),
                        ));
                    }
                }
                sum_unrealized_pnl = Some(acc);
                if msg_type == "subscribed/account_all" {
                    let mut cache = cached_positions.write().await;
                    *cache = updates.into_iter().filter_map(|(_, snap)| snap).collect();
                    log::info!("Updated cached positions: {} positions", cache.len());
                } else {
                    let mut cache = cached_positions.write().await;
                    let mut map: HashMap<String, PositionSnapshot> = cache
                        .iter()
                        .cloned()
                        .map(|p| (p.symbol.clone(), p))
                        .collect();
                    for (symbol, maybe_snap) in updates {
                        match maybe_snap {
                            Some(snap) => {
                                map.insert(symbol, snap);
                            }
                            None => {
                                map.remove(&symbol);
                            }
                        }
                    }
                    *cache = map.into_values().collect();
                    log::info!("Merged cached positions: {} positions", cache.len());
                }
            }
        }

        // Equity derivation for perp sub-accounts (bot-strategy#239).
        //
        // Lighter does not publish `total_asset_value` / `available_balance`
        // for perp sub-accounts on either snapshot or update messages
        // (verified empirically; the original direct-field code below was
        // dead for these accounts). The aggregate is reconstructible as
        //   equity = assets[USDC].margin_balance + sum(positions.unrealized_pnl)
        // matching REST `total_asset_value` to within rounding (~$0.002 on
        // $1000 in measured fixtures).
        //
        // `subscribed/account_all` carries assets, so margin_balance gets
        // refreshed on (re)subscribe and after fills (which trigger a REST
        // refresh that reseeds cached_collateral). `update/account_all` ships
        // `assets:null` but does carry positions, so we recompute equity
        // from the latest unrealized_pnl combined with the previously cached
        // collateral. Fall back to the legacy direct-totals path if Lighter
        // ever populates them on the parent account or returns to the older
        // schema.
        let direct_total = data
            .get("total_asset_value")
            .and_then(value_to_decimal);
        let direct_available = data
            .get("available_balance")
            .and_then(value_to_decimal);

        let usdc_margin_balance = data
            .get("assets")
            .and_then(|a| a.as_object())
            .and_then(|map| {
                map.values().find_map(|asset| {
                    let symbol = asset.get("symbol").and_then(|s| s.as_str())?;
                    if symbol != "USDC" {
                        return None;
                    }
                    asset
                        .get("margin_balance")
                        .and_then(value_to_decimal)
                })
            });
        if let Some(mb) = usdc_margin_balance {
            let mut collateral = cached_collateral.write().await;
            *collateral = Some(mb);
        }

        let derived_equity = if let Some(pnl_sum) = sum_unrealized_pnl {
            let collateral = cached_collateral.read().await;
            collateral.map(|c| (c, c + pnl_sum))
        } else {
            None
        };

        if direct_total.is_some() || direct_available.is_some() {
            let equity = direct_total
                .or(direct_available)
                .unwrap_or(Decimal::ZERO);
            let balance = direct_available.or(direct_total).unwrap_or(equity);
            let mut cache = balance_cache.write().await;
            *cache = Some((
                BalanceResponse {
                    equity,
                    balance,
                    position_entry_price: None,
                    position_sign: None,
                },
                Instant::now(),
            ));
            log::debug!(
                "Updated cached balance from WS direct totals: equity={}, balance={}",
                equity,
                balance
            );
        } else if let Some((collateral, equity)) = derived_equity {
            let mut cache = balance_cache.write().await;
            *cache = Some((
                BalanceResponse {
                    equity,
                    balance: collateral,
                    position_entry_price: None,
                    position_sign: None,
                },
                Instant::now(),
            ));
            log::debug!(
                "Updated cached balance from WS derivation: collateral={} +unrealized_pnl_sum={} =equity={}",
                collateral,
                equity - collateral,
                equity
            );
        } else {
            log::debug!(
                "Skipping WS balance update: no direct totals and no derivation inputs (collateral_cached={}, positions_in_msg={})",
                cached_collateral.read().await.is_some(),
                sum_unrealized_pnl.is_some(),
            );
        }

        // Handle filled orders - try both 'fills' and 'trades' fields
        if let Some(fills) = data.get("fills").and_then(|f| f.as_array()) {
            log::info!(
                "✅ [FILL_DETECTION] Found {} fills in account update",
                fills.len()
            );
            let default_symbol = default_symbol.to_string();
            let mut filled_map = filled_orders.write().await;
            for fill in fills {
                log::debug!("🔍 [FILL_DETECTION] Processing fill: {:?}", fill);
                if let Ok(filled_order) = parse_filled_order(fill, account_id) {
                    let order_id = filled_order.order_id.clone();
                    log::info!("✅ [FILL_DETECTION] Added filled order: order_id={}, size={:?}, value={:?}",
                              filled_order.order_id, filled_order.filled_size, filled_order.filled_value);
                    filled_map
                        .entry(default_symbol.clone())
                        .or_insert_with(Vec::new)
                        .push(filled_order);
                    Self::remove_tracked_order(cached_open_orders, &default_symbol, &order_id)
                        .await;
                } else {
                    log::debug!("Failed to parse filled order: {:?}", fill);
                }
            }
        }

        // Process 'trades' field according to Lighter API specification
        if let Some(trades) = data.get("trades").and_then(|t| t.as_object()) {
            log::info!("✅ [FILL_DETECTION] Found trades object in account update");

            let mut pending_inserts: Vec<(String, FilledOrder)> = Vec::new();

            for (market_id, trade_array) in trades {
                let market_id_num = match market_id.parse::<u32>() {
                    Ok(id) => id,
                    Err(_) => {
                        log::warn!(
                            "[FILL_DETECTION] Unable to parse market_id '{}' as u32",
                            market_id
                        );
                        continue;
                    }
                };

                let market_symbol = {
                    let cache = market_cache.read().await;
                    cache
                        .by_id
                        .get(&market_id_num)
                        .map(|info| info.canonical_symbol.clone())
                };

                let market_symbol = match market_symbol {
                    Some(symbol) => symbol,
                    None => {
                        log::warn!(
                            "[FILL_DETECTION] Market cache missing entry for market_id {}",
                            market_id_num
                        );
                        continue;
                    }
                };

                if let Some(trades_array) = trade_array.as_array() {
                    for trade in trades_array {
                        if let (
                            Some(ask_id),
                            Some(bid_id),
                            Some(ask_account_id),
                            Some(_bid_account_id),
                            Some(size_str),
                            Some(price_str),
                        ) = (
                            trade.get("ask_id").and_then(|v| v.as_u64()),
                            trade.get("bid_id").and_then(|v| v.as_u64()),
                            trade.get("ask_account_id").and_then(|v| v.as_u64()),
                            trade.get("bid_account_id").and_then(|v| v.as_u64()),
                            trade.get("size").and_then(|v| v.as_str()),
                            trade.get("price").and_then(|v| v.as_str()),
                        ) {
                            let is_ask = account_id == ask_account_id;
                            let client_id = if is_ask {
                                trade.get("ask_client_id").and_then(|v| v.as_u64())
                            } else {
                                trade.get("bid_client_id").and_then(|v| v.as_u64())
                            };
                            let order_id =
                                client_id.unwrap_or_else(|| if is_ask { ask_id } else { bid_id });

                            log::info!(
                                "✅ [FILL_DETECTION] Trade detected: order_id={}, size={}, price={}, market_id={}",
                                order_id, size_str, price_str, market_id_num
                            );

                            if let (Ok(size), Ok(price)) = (
                                size_str.parse::<rust_decimal::Decimal>(),
                                price_str.parse::<rust_decimal::Decimal>(),
                            ) {
                                let filled_order = FilledOrder {
                                    order_id: order_id.to_string(),
                                    is_rejected: false,
                                    trade_id: trade
                                        .get("trade_id")
                                        .and_then(|v| v.as_u64())
                                        .unwrap_or(0)
                                        .to_string(),
                                    filled_side: if is_ask {
                                        Some(OrderSide::Short)
                                    } else {
                                        Some(OrderSide::Long)
                                    },
                                    filled_size: Some(size),
                                    filled_value: Some(size * price),
                                    filled_fee: None,
                                    filled_ts_ms: None,
                                };

                                pending_inserts.push((market_symbol.clone(), filled_order));
                                log::info!(
                                    "✅ [FILL_DETECTION] Added filled order from trade: order_id={}",
                                    order_id
                                );
                                Self::remove_tracked_order(
                                    cached_open_orders,
                                    &market_symbol,
                                    &order_id.to_string(),
                                )
                                .await;
                            }
                        }
                    }
                }
            }

            let had_fills = !pending_inserts.is_empty();
            if had_fills {
                let mut filled_map = filled_orders.write().await;
                for (symbol_key, filled_order) in pending_inserts {
                    filled_map
                        .entry(symbol_key)
                        .or_insert_with(Vec::new)
                        .push(filled_order);
                }
            }

            // Event-sourced equity: a fill changes realized PnL + fees in a
            // way that's not surfaced by the `account_all` WS snapshot for
            // perp sub-accounts (Lighter only publishes per-market state
            // there, not aggregate collateral). Invalidate the cache so the
            // next get_balance(None) falls through to a fresh REST fetch
            // rather than returning pre-fill equity for up to a full TTL
            // window. See bot-strategy#155.
            if had_fills {
                let mut cache = balance_cache.write().await;
                *cache = None;
                log::debug!("Invalidated balance_cache after WS fill");
            }
        } else {
            log::trace!("No 'fills' array or 'trades' object found in account data");
        }

        // Handle canceled orders
        if let Some(cancels) = data.get("cancels").and_then(|c| c.as_array()) {
            let default_symbol = default_symbol.to_string();
            let mut canceled_map = canceled_orders.write().await;
            for cancel in cancels {
                if let Ok(canceled_order) = parse_canceled_order(cancel) {
                    canceled_map
                        .entry(default_symbol.clone())
                        .or_insert_with(Vec::new)
                        .push(canceled_order);
                }
            }
        }
    }

}

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
        assert_eq!(
            parse_cancel_order_index("12345"),
            Some(12345)
        );
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
        assert_eq!(
            parse_cancel_order_index("unknown-id"),
            None
        );
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
    // explicit totals must land in balance_cache as (response, fetched_at) so
    // TTL-aware readers can expire it. Verifies the tuple shape and non-zero
    // timestamp (which matters for try_read_cached_balance staleness checks).
    #[tokio::test]
    async fn account_update_with_explicit_totals_populates_cache_with_timestamp() {
        use serde_json::json;
        use std::str::FromStr;
        use std::sync::atomic::AtomicBool;

        let filled_orders = Arc::new(RwLock::new(HashMap::new()));
        let canceled_orders = Arc::new(RwLock::new(HashMap::new()));
        let cached_open_orders = Arc::new(RwLock::new(HashMap::new()));
        let cached_positions = Arc::new(RwLock::new(Vec::new()));
        let positions_ready = Arc::new(AtomicBool::new(false));
        let balance_cache: Arc<RwLock<Option<(BalanceResponse, Instant)>>> =
            Arc::new(RwLock::new(None));
        let cached_collateral: Arc<RwLock<Option<Decimal>>> = Arc::new(RwLock::new(None));
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
            &cached_positions,
            &positions_ready,
            &balance_cache,
            &cached_collateral,
            1,
            &market_cache,
            "BTC",
        )
        .await;

        let guard = balance_cache.read().await;
        let (resp, fetched_at) = guard
            .as_ref()
            .expect("explicit totals must populate cache");
        assert_eq!(resp.equity, Decimal::from_str("999.04").unwrap());
        assert_eq!(resp.balance, Decimal::from_str("800.00").unwrap());
        assert!(*fetched_at >= before, "fetched_at must be recent");
    }

    // bot-strategy#155: a WS fill must invalidate balance_cache so the next
    // get_balance(None) goes back to REST and picks up the post-fill
    // realized P&L + fees. We simulate a pre-populated cache, deliver a
    // trades payload, and verify the cache is cleared.
    #[tokio::test]
    async fn ws_fill_invalidates_balance_cache() {
        use serde_json::json;
        use std::str::FromStr;
        use std::sync::atomic::AtomicBool;

        let filled_orders = Arc::new(RwLock::new(HashMap::new()));
        let canceled_orders = Arc::new(RwLock::new(HashMap::new()));
        let cached_open_orders = Arc::new(RwLock::new(HashMap::new()));
        let cached_positions = Arc::new(RwLock::new(Vec::new()));
        let positions_ready = Arc::new(AtomicBool::new(false));
        let balance_cache: Arc<RwLock<Option<(BalanceResponse, Instant)>>> =
            Arc::new(RwLock::new(Some((
                BalanceResponse {
                    equity: Decimal::from_str("500.0").unwrap(),
                    balance: Decimal::from_str("500.0").unwrap(),
                    position_entry_price: None,
                    position_sign: None,
                },
                Instant::now(),
            ))));
        let cached_collateral: Arc<RwLock<Option<Decimal>>> = Arc::new(RwLock::new(None));

        // Seed market_cache so the fill's market_id=0 resolves to a symbol
        // and the trade parse path engages (otherwise pending_inserts stays
        // empty and we wouldn't exercise the invalidation).
        let market_cache = Arc::new(RwLock::new({
            let mut mc = MarketCache::default();
            mc.by_id.insert(
                0,
                crate::lighter_connector::MarketInfo {
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
            &cached_positions,
            &positions_ready,
            &balance_cache,
            &cached_collateral,
            522842,
            &market_cache,
            "ETH",
        )
        .await;

        assert!(
            balance_cache.read().await.is_none(),
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
        let cached_positions = Arc::new(RwLock::new(Vec::new()));
        let positions_ready = Arc::new(AtomicBool::new(false));
        let balance_cache: Arc<RwLock<Option<(BalanceResponse, Instant)>>> =
            Arc::new(RwLock::new(None));
        let cached_collateral: Arc<RwLock<Option<Decimal>>> = Arc::new(RwLock::new(None));
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
            &cached_positions,
            &positions_ready,
            &balance_cache,
            &cached_collateral,
            281474976624818,
            &market_cache,
            "ETH",
        )
        .await;

        let collateral = Decimal::from_str("998.96368343036").unwrap();
        let pnl_sum = Decimal::from_str("0.179840").unwrap();
        let expected_equity = collateral + pnl_sum;

        assert_eq!(
            *cached_collateral.read().await,
            Some(collateral),
            "subscribed snapshot must seed cached_collateral from assets[USDC].margin_balance"
        );
        let guard = balance_cache.read().await;
        let (resp, _) = guard.as_ref().expect("subscribed snapshot must populate balance_cache");
        assert_eq!(resp.balance, collateral, "balance reflects raw collateral");
        assert_eq!(resp.equity, expected_equity, "equity = collateral + sum(unrealized_pnl)");
        drop(guard);

        // Now deliver an update with assets:null but updated positions
        // (mark-to-market drift). Equity must update using the SAME
        // cached_collateral combined with the new pnl_sum.
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
            &cached_positions,
            &positions_ready,
            &balance_cache,
            &cached_collateral,
            281474976624818,
            &market_cache,
            "ETH",
        )
        .await;

        let new_pnl_sum = Decimal::from_str("3.000000").unwrap();
        let new_expected_equity = collateral + new_pnl_sum;
        assert_eq!(
            *cached_collateral.read().await,
            Some(collateral),
            "update with assets:null must NOT reset cached_collateral"
        );
        let guard = balance_cache.read().await;
        let (resp, _) = guard.as_ref().expect("update must refresh balance_cache");
        assert_eq!(resp.balance, collateral);
        assert_eq!(resp.equity, new_expected_equity);
    }

    // bot-strategy#162: a `market_stats` WS push with a `funding_rate` field
    // must populate the per-market cache keyed by the market_id parsed out of
    // the channel string. Both delimiter shapes the server uses (`/` on
    // subscribe, `:` on push) must resolve to the same market_id.
    #[tokio::test]
    async fn market_stats_push_populates_funding_rate_cache() {
        use serde_json::json;
        use std::str::FromStr;

        let cache: Arc<RwLock<HashMap<u32, Decimal>>> =
            Arc::new(RwLock::new(HashMap::new()));

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
            Some(Decimal::from_str("-0.0017").unwrap()),
            "push with ':' delimiter must store the realized funding_rate"
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
            Some(Decimal::from_str("0.00001").unwrap()),
            "subscribed push with '/' delimiter must also resolve and cache"
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
            Some(Decimal::from_str("0.00005").unwrap()),
            "later push must replace the stored value"
        );
    }

    // bot-strategy#162: malformed / partial pushes must not poison the cache
    // nor panic. A missing market_id or funding_rate field is skipped silently.
    #[tokio::test]
    async fn market_stats_missing_fields_are_skipped() {
        use serde_json::json;

        let cache: Arc<RwLock<HashMap<u32, Decimal>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // Channel without a parseable trailing id.
        let bad_channel = json!({
            "channel": "market_stats/",
            "type": "update/market_stats",
            "market_stats": {"funding_rate": "0.001"}
        });
        LighterConnector::handle_market_stats_update(&bad_channel, &cache).await;
        assert!(cache.read().await.is_empty(), "bad channel must not populate");

        // Payload missing funding_rate entirely.
        let missing_rate = json!({
            "channel": "market_stats:3",
            "type": "update/market_stats",
            "market_stats": {"symbol": "SOL", "market_id": 3}
        });
        LighterConnector::handle_market_stats_update(&missing_rate, &cache).await;
        assert!(cache.read().await.is_empty(), "missing funding_rate must not populate");

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

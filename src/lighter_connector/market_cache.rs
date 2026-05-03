//! Process-wide Lighter market metadata cache (`/orderBookDetails` /
//! `/orderBooks` / `/funding-rates`).
//!
//! Market metadata is identical across credentials on the same base_url,
//! so multi-instance pairtrade only needs to fetch it once per process
//! rather than once per sub-account. The shared `MARKET_CACHE` static
//! plus `MARKET_CACHE_INIT_LOCK` guard ensure a single fetch per process
//! lifetime even under concurrent constructor calls. See bot-strategy#135.

use super::{
    LighterConnector, LighterExchangeStats, DEFAULT_PRICE_DECIMALS, DEFAULT_SIZE_DECIMALS,
    MAX_DECIMAL_PRECISION,
};
use crate::dex_request::DexError;
use rust_decimal::{prelude::FromStr, Decimal};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::sleep;

#[derive(Clone, Debug)]
pub(super) struct MarketInfo {
    pub(super) canonical_symbol: String,
    pub(super) market_id: u32,
    pub(super) price_decimals: u32,
    pub(super) size_decimals: u32,
    pub(super) min_order: Option<Decimal>,
}

#[derive(Default, Debug)]
pub(super) struct MarketCache {
    pub(super) by_symbol: HashMap<String, MarketInfo>,
    pub(super) by_id: HashMap<u32, MarketInfo>,
}

/// Process-wide market metadata cache shared across all LighterConnector
/// instances on the same host. See module-level docs.
pub(super) static MARKET_CACHE: std::sync::LazyLock<Arc<RwLock<MarketCache>>> =
    std::sync::LazyLock::new(|| Arc::new(RwLock::new(MarketCache::default())));

pub(super) static MARKET_CACHE_INIT_LOCK: std::sync::LazyLock<Arc<tokio::sync::Mutex<()>>> =
    std::sync::LazyLock::new(|| Arc::new(tokio::sync::Mutex::new(())));

/// Process-wide cache for the `/exchange-stats` ticker helper. Same
/// reasoning as `MARKET_CACHE`: the data is global exchange state, not
/// per-account, so all instances share one copy. Without this, each
/// instance's first `get_ticker` call misses its own cache and fires a
/// REST, producing an N-way burst at startup. See bot-strategy#135
/// follow-up.
pub(super) static CACHED_EXCHANGE_STATS: std::sync::LazyLock<
    Arc<RwLock<Option<(LighterExchangeStats, Instant)>>>,
> = std::sync::LazyLock::new(|| Arc::new(RwLock::new(None)));

impl LighterConnector {
    pub(super) async fn refresh_market_cache(&self) -> Result<(), DexError> {
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
                    let normalized = super::normalize_symbol(&detail.symbol);
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
                        let normalized = super::normalize_symbol(&book.symbol);
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
            let normalized = super::normalize_symbol(&entry.symbol);
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

    pub(super) async fn has_market_metadata(&self) -> bool {
        let cache = self.market_cache.read().await;
        !cache.by_symbol.is_empty()
    }

    pub(super) async fn ensure_market_metadata_loaded(&self) -> Result<(), DexError> {
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

    pub(super) async fn resolve_market_info(&self, symbol: &str) -> Result<MarketInfo, DexError> {
        let normalized = super::normalize_symbol(symbol);
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
}

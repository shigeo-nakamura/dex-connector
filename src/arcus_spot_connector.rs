//! Read-only Arcus Spot router client.
//!
//! This module intentionally exposes only public metadata, indicative-price,
//! pre-sign quote, reference-price, and finalized-indexer GETs. It can decode
//! and validate EIP-712 payloads returned by `GET /v1/quote`, but has no
//! wallet, approval, signing, submission, or status-mutation surface. Arcus
//! Spot is inventory-funded and does not fit the leveraged-perpetual
//! [crate::DexConnector] contract, so the client remains a separate API.

mod indexer;
mod recorder;
mod signable_quote;

pub use indexer::*;
pub use recorder::*;
pub use signable_quote::*;

use chrono::{DateTime, Utc};
use ethers::types::{Address, U256};
use reqwest::{
    header::{HeaderValue, RETRY_AFTER},
    Client, StatusCode, Url,
};
use rust_decimal::Decimal;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use thiserror::Error;
use tokio::sync::{Mutex, OnceCell, RwLock};

const DEFAULT_ROUTER_BASE_URL: &str = "https://router.spot.arcus.xyz";
const DEFAULT_META_BASE_URL: &str = "https://api.arcus.xyz";
const DEFAULT_INDEXER_BASE_URL: &str = "https://indexer.spot.arcus.xyz";
const DEFAULT_CHAIN_ID: u64 = 4663;

/// Configuration for public Arcus Spot GET requests.
///
/// Time values are integers so the config can be embedded directly in caller
/// YAML/JSON without custom duration serializers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ArcusSpotConfig {
    pub router_base_url: String,
    pub meta_base_url: String,
    pub indexer_base_url: String,
    pub chain_id: u64,
    pub request_timeout_ms: u64,
    pub min_request_interval_ms: u64,
    pub max_attempts: u32,
    pub retry_base_delay_ms: u64,
    pub max_retry_delay_ms: u64,
    pub user_agent: String,
    /// Permit2 `spender` addresses this deployment recognizes as genuine
    /// venue settlement contracts on `chain_id`, hex-encoded
    /// (case-insensitive), keyed by lowercase venue name (e.g. `"arcus"`,
    /// `"rialto"`).
    ///
    /// A signable quote's Permit2 signature authorizes this address to pull
    /// the sell token; the spender is also responsible for enforcing the
    /// signed witness's semantics once it executes. Comparing it only against
    /// other fields in the same (attacker-influenceable) response, as the
    /// venue-specific checks already do, cannot detect a compromised or
    /// misconfigured router substituting an unrelated contract, so this map
    /// is checked as an independent, deployer-controlled source of truth.
    /// It is keyed per venue rather than one shared set: a response labeled
    /// `"arcus"` must use the Arcus deployment's own spender, not any
    /// address trusted for a different venue, since a compromised or
    /// misconfigured router could otherwise mislabel a quote naming one
    /// venue's spender while claiming another venue's settlement semantics.
    /// Left empty (the default), every quote is refused rather than treated
    /// as validated on an unverified spender: operators MUST populate this
    /// with the real per-chain venue deployment addresses before consuming
    /// this module's output as signing evidence.
    pub trusted_permit2_spenders: BTreeMap<String, Vec<String>>,
    /// Deployer-controlled per-chain token symbol → address pin, hex-encoded
    /// (case-insensitive), keyed by uppercase symbol (see `normalize_symbol`).
    ///
    /// `signable_quote_by_symbol` resolves `sellToken`/`buyToken` addresses
    /// from the router's own `v1/tokens` list (via `verified_token`), and a
    /// signable quote's Permit2 signature authorizes pulling exactly the
    /// returned sell-token address. A compromised or misconfigured router
    /// could map a requested symbol to a different, valuable token while
    /// still marking it `verified`, so this independent, deployer-controlled
    /// pin is required before that router-supplied address is trusted as
    /// signing evidence. Left empty (the default), every signable quote is
    /// refused rather than treated as validated on an unpinned address:
    /// operators MUST populate this with the real per-chain token addresses
    /// before consuming `signable_quote_by_symbol`'s output as signing
    /// evidence. Read-only lookups (`verified_token`, `indicative_price*`,
    /// `refresh_tokens`) do not consult this map.
    pub trusted_token_addresses: BTreeMap<String, String>,
}

impl Default for ArcusSpotConfig {
    fn default() -> Self {
        Self {
            router_base_url: DEFAULT_ROUTER_BASE_URL.to_string(),
            meta_base_url: DEFAULT_META_BASE_URL.to_string(),
            indexer_base_url: DEFAULT_INDEXER_BASE_URL.to_string(),
            chain_id: DEFAULT_CHAIN_ID,
            request_timeout_ms: 30_000,
            min_request_interval_ms: 250,
            max_attempts: 3,
            retry_base_delay_ms: 500,
            max_retry_delay_ms: 30_000,
            user_agent: format!(
                "dex-connector/{}/arcus-spot-readonly",
                env!("CARGO_PKG_VERSION")
            ),
            trusted_permit2_spenders: BTreeMap::new(),
            trusted_token_addresses: BTreeMap::new(),
        }
    }
}

/// Failure category suitable for recorder/status metrics.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArcusSpotFailureClass {
    InvalidConfig,
    RateLimited,
    Http,
    Timeout,
    Transport,
    InvalidJson,
    ResponseValidation,
    MissingMetadata,
}

impl fmt::Display for ArcusSpotFailureClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::InvalidConfig => "invalid_config",
            Self::RateLimited => "rate_limited",
            Self::Http => "http",
            Self::Timeout => "timeout",
            Self::Transport => "transport",
            Self::InvalidJson => "invalid_json",
            Self::ResponseValidation => "response_validation",
            Self::MissingMetadata => "missing_metadata",
        };
        formatter.write_str(label)
    }
}

#[derive(Debug, Error)]
pub enum ArcusSpotError {
    #[error("invalid Arcus Spot configuration: {0}")]
    InvalidConfig(String),
    #[error(
        "Arcus Spot HTTP {status} from {endpoint} after {attempts} attempt(s) ({classification}, retryable={retryable}): {body}"
    )]
    Http {
        endpoint: String,
        status: u16,
        classification: ArcusSpotFailureClass,
        retryable: bool,
        attempts: u32,
        retry_after_ms: Option<u64>,
        body: String,
    },
    #[error(
        "Arcus Spot transport failure from {endpoint} after {attempts} attempt(s) ({classification}, retryable={retryable}): {source}"
    )]
    Transport {
        endpoint: String,
        classification: ArcusSpotFailureClass,
        retryable: bool,
        attempts: u32,
        #[source]
        source: reqwest::Error,
    },
    #[error("invalid Arcus Spot JSON from {endpoint} after {attempts} attempt(s): {source}")]
    InvalidJson {
        endpoint: String,
        attempts: u32,
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid Arcus Spot response: {0}")]
    InvalidResponse(String),
    #[error("verified Arcus Spot token not found for chain {chain_id}: {symbol}")]
    TokenNotFound { chain_id: u64, symbol: String },
}

impl ArcusSpotError {
    pub fn classification(&self) -> ArcusSpotFailureClass {
        match self {
            Self::InvalidConfig(_) => ArcusSpotFailureClass::InvalidConfig,
            Self::Http { classification, .. } | Self::Transport { classification, .. } => {
                *classification
            }
            Self::InvalidJson { .. } => ArcusSpotFailureClass::InvalidJson,
            Self::InvalidResponse(_) => ArcusSpotFailureClass::ResponseValidation,
            Self::TokenNotFound { .. } => ArcusSpotFailureClass::MissingMetadata,
        }
    }

    pub fn retryable(&self) -> bool {
        match self {
            Self::Http { retryable, .. } | Self::Transport { retryable, .. } => *retryable,
            _ => false,
        }
    }
}

/// Request/receipt timing wrapped around a validated public response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArcusSpotObservation<T> {
    pub payload: T,
    pub requested_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub latency_ms: u64,
    pub attempts: u32,
}

impl<T> ArcusSpotObservation<T> {
    pub fn map<U>(self, mapper: impl FnOnce(T) -> U) -> ArcusSpotObservation<U> {
        ArcusSpotObservation {
            payload: mapper(self.payload),
            requested_at: self.requested_at,
            received_at: self.received_at,
            latency_ms: self.latency_ms,
            attempts: self.attempts,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ArcusSpotToken {
    pub chain_id: u64,
    pub symbol: String,
    pub name: String,
    pub address: String,
    pub decimals: u32,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub verified: bool,
    #[serde(default)]
    pub wrapped_token_address: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ArcusSpotVenueQuote {
    pub venue: String,
    pub buy_amount: String,
    pub sell_amount: String,
    #[serde(default)]
    pub fees: Vec<Value>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArcusSpotPriceResponse {
    pub recommended: String,
    #[serde(rename = "all")]
    pub quotes: Vec<ArcusSpotVenueQuote>,
    #[serde(default)]
    pub errors: Vec<Value>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl ArcusSpotPriceResponse {
    /// Return the quote selected by the router without discarding alternatives.
    pub fn recommended_quote(&self) -> Result<&ArcusSpotVenueQuote, ArcusSpotError> {
        self.quotes
            .iter()
            .find(|quote| quote.venue == self.recommended)
            .ok_or_else(|| {
                ArcusSpotError::InvalidResponse(format!(
                    "recommended venue {:?} is absent from all quotes",
                    self.recommended
                ))
            })
    }

    fn validate(&self, requested_sell_amount: U256) -> Result<(), ArcusSpotError> {
        if self.recommended.trim().is_empty() {
            return Err(ArcusSpotError::InvalidResponse(
                "price response has an empty recommended venue".to_string(),
            ));
        }
        if self.quotes.is_empty() {
            return Err(ArcusSpotError::InvalidResponse(
                "price response has no venue quotes".to_string(),
            ));
        }
        for quote in &self.quotes {
            if quote.venue.trim().is_empty() {
                return Err(ArcusSpotError::InvalidResponse(
                    "price response contains an empty venue".to_string(),
                ));
            }
            let buy_amount = parse_raw_amount("buyAmount", &quote.buy_amount)?;
            if buy_amount.is_zero() {
                return Err(ArcusSpotError::InvalidResponse(format!(
                    "venue {} returned a zero buyAmount",
                    quote.venue
                )));
            }
            let sell_amount = parse_raw_amount("sellAmount", &quote.sell_amount)?;
            if sell_amount != requested_sell_amount {
                return Err(ArcusSpotError::InvalidResponse(format!(
                    "venue {} echoed sellAmount {} but {} was requested",
                    quote.venue, quote.sell_amount, requested_sell_amount
                )));
            }
        }
        self.recommended_quote()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ArcusSpotReferenceQuote {
    pub price: Option<Decimal>,
    #[serde(default)]
    pub change_24h: Option<Decimal>,
    #[serde(default)]
    pub change_percent_24h: Option<Decimal>,
    #[serde(default)]
    pub volume_24h: Option<Decimal>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ArcusSpotOverviewEntry {
    pub ticker: String,
    pub contract_address: String,
    pub name: String,
    pub category: String,
    pub quote: ArcusSpotReferenceQuote,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Evidence envelope retaining the request tokens and every venue response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArcusSpotRouteObservation {
    pub chain_id: u64,
    pub sell_symbol: String,
    pub buy_symbol: String,
    pub sell_token: String,
    pub buy_token: String,
    pub sell_amount: String,
    pub response: ArcusSpotObservation<ArcusSpotPriceResponse>,
}

#[derive(Debug, Default)]
struct TokenCache {
    initialized: bool,
    by_symbol: HashMap<String, ArcusSpotToken>,
}

/// `pace()`'s two timestamps, guarded by one lock so they're always read
/// and updated as a single atomic step — see `ArcusSpotClientInner::pace_state`.
struct PaceState {
    last_sent_at: Instant,
    rate_limit_until: Instant,
}

struct ArcusSpotClientInner {
    config: ArcusSpotConfig,
    router_base_url: Url,
    meta_base_url: Url,
    indexer_base_url: Url,
    http: Client,
    /// Both pacing timestamps behind a single lock, checked and updated as
    /// one atomic step. `pace()` re-reads them fresh on every iteration —
    /// including right before actually sending — rather than sleeping
    /// against a pre-computed target, so it can't burst even if the runtime
    /// stalls long enough to wake several queued callers at once. A single
    /// `Mutex` (not two) closes a check-then-claim race: reading
    /// `rate_limit_until` and claiming `last_sent_at` under separate locks
    /// left a window where another caller's `record_retry_after` could land
    /// in between, so a caller could dispatch on a floor value that was
    /// already stale by the time it claimed its slot (Codex review).
    pace_state: Mutex<PaceState>,
    token_cache: RwLock<TokenCache>,
    /// Coalesces the first `refresh_tokens` call triggered from
    /// `verified_token` so concurrent lookups on an uninitialized cache
    /// share one in-flight metadata GET instead of each firing their own.
    /// `OnceCell` (not a `Mutex`) so no guard is held across the refresh
    /// `.await` — see bot-strategy#391 in clippy.toml.
    token_refresh_once: OnceCell<()>,
}

/// Cloneable, process-local Arcus Spot read-only client.
#[derive(Clone)]
pub struct ArcusSpotClient {
    inner: Arc<ArcusSpotClientInner>,
}

impl ArcusSpotClient {
    pub fn new(config: ArcusSpotConfig) -> Result<Self, ArcusSpotError> {
        validate_config(&config)?;
        let router_base_url = parse_base_url("router_base_url", &config.router_base_url)?;
        let meta_base_url = parse_base_url("meta_base_url", &config.meta_base_url)?;
        let indexer_base_url = parse_base_url("indexer_base_url", &config.indexer_base_url)?;
        let http = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .user_agent(config.user_agent.clone())
            .build()
            .map_err(|error| {
                ArcusSpotError::InvalidConfig(format!("could not build HTTP client: {error}"))
            })?;
        let now = Instant::now();
        // Backdated by the pacing interval so the very first request isn't
        // delayed waiting out an interval against a same-instant baseline.
        let last_sent_at = now
            .checked_sub(Duration::from_millis(config.min_request_interval_ms))
            .unwrap_or(now);
        Ok(Self {
            inner: Arc::new(ArcusSpotClientInner {
                config,
                router_base_url,
                meta_base_url,
                indexer_base_url,
                http,
                pace_state: Mutex::new(PaceState {
                    last_sent_at,
                    rate_limit_until: now,
                }),
                token_cache: RwLock::new(TokenCache::default()),
                token_refresh_once: OnceCell::new(),
            }),
        })
    }

    pub fn config(&self) -> &ArcusSpotConfig {
        &self.inner.config
    }

    /// Fetch and cache only server-verified tokens for the configured chain.
    pub async fn refresh_tokens(
        &self,
    ) -> Result<ArcusSpotObservation<Vec<ArcusSpotToken>>, ArcusSpotError> {
        let observation: ArcusSpotObservation<Vec<ArcusSpotToken>> = self
            .get_json(&self.inner.router_base_url, "v1/tokens", &[])
            .await?;
        let mut verified = Vec::new();
        let mut by_symbol = HashMap::new();
        for token in &observation.payload {
            if token.chain_id != self.inner.config.chain_id || !token.verified {
                continue;
            }
            validate_token(token)?;
            let key = normalize_symbol(&token.symbol);
            if by_symbol.insert(key.clone(), token.clone()).is_some() {
                return Err(ArcusSpotError::InvalidResponse(format!(
                    "duplicate verified token symbol for chain {}: {}",
                    self.inner.config.chain_id, key
                )));
            }
            verified.push(token.clone());
        }
        if verified.is_empty() {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "token list contains no verified tokens for chain {}",
                self.inner.config.chain_id
            )));
        }
        *self.inner.token_cache.write().await = TokenCache {
            initialized: true,
            by_symbol,
        };
        Ok(observation.map(|_| verified))
    }

    /// Resolve a verified token from the process-local cache, refreshing once
    /// on first use.
    pub async fn verified_token(&self, symbol: &str) -> Result<ArcusSpotToken, ArcusSpotError> {
        let key = normalize_symbol(symbol);
        if key.is_empty() {
            return Err(ArcusSpotError::TokenNotFound {
                chain_id: self.inner.config.chain_id,
                symbol: symbol.to_string(),
            });
        }
        let (initialized, cached) = {
            let cache = self.inner.token_cache.read().await;
            (cache.initialized, cache.by_symbol.get(&key).cloned())
        };
        if let Some(token) = cached {
            return Ok(token);
        }
        if !initialized {
            // Coalesce the initial refresh: `OnceCell::get_or_try_init` runs
            // the closure at most once and lets every concurrent caller
            // await that same in-flight future, without us holding a
            // MutexGuard/RwLockGuard across an await point ourselves
            // (bot-strategy#391 forbids that pattern — see clippy.toml).
            // Without this, N concurrent callers on a cold cache each fire
            // their own metadata GET, and a redundant request can fail even
            // after another one already succeeded. A failed attempt leaves
            // the cell empty so the next call retries.
            self.inner
                .token_refresh_once
                .get_or_try_init(|| async { self.refresh_tokens().await.map(|_| ()) })
                .await?;
            if let Some(token) = self
                .inner
                .token_cache
                .read()
                .await
                .by_symbol
                .get(&key)
                .cloned()
            {
                return Ok(token);
            }
        }
        Err(ArcusSpotError::TokenNotFound {
            chain_id: self.inner.config.chain_id,
            symbol: key,
        })
    }

    /// Read a direct indicative route by verified token symbols.
    pub async fn indicative_price_by_symbol(
        &self,
        sell_symbol: &str,
        buy_symbol: &str,
        sell_amount: &str,
    ) -> Result<ArcusSpotRouteObservation, ArcusSpotError> {
        let sell = self.verified_token(sell_symbol).await?;
        let buy = self.verified_token(buy_symbol).await?;
        self.indicative_price(&sell, &buy, sell_amount).await
    }

    /// Read a direct indicative route while preserving all venue quotes, fees,
    /// errors, and receipt timing.
    async fn indicative_price(
        &self,
        sell: &ArcusSpotToken,
        buy: &ArcusSpotToken,
        sell_amount: &str,
    ) -> Result<ArcusSpotRouteObservation, ArcusSpotError> {
        validate_token(sell)?;
        validate_token(buy)?;
        if sell.chain_id != self.inner.config.chain_id || buy.chain_id != self.inner.config.chain_id
        {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "route token chain mismatch: expected {}, got sell={} buy={}",
                self.inner.config.chain_id, sell.chain_id, buy.chain_id
            )));
        }
        if sell.address.eq_ignore_ascii_case(&buy.address) {
            return Err(ArcusSpotError::InvalidResponse(
                "sell and buy token addresses are identical".to_string(),
            ));
        }
        let requested_sell_amount = parse_raw_amount("sellAmount", sell_amount)?;
        if requested_sell_amount.is_zero() {
            return Err(ArcusSpotError::InvalidResponse(
                "sellAmount must be greater than zero".to_string(),
            ));
        }
        let query = vec![
            ("chainId", self.inner.config.chain_id.to_string()),
            ("sellToken", sell.address.clone()),
            ("buyToken", buy.address.clone()),
            ("sellAmount", sell_amount.to_string()),
        ];
        let response: ArcusSpotObservation<ArcusSpotPriceResponse> = self
            .get_json(&self.inner.router_base_url, "v1/price", &query)
            .await?;
        response.payload.validate(requested_sell_amount)?;
        Ok(ArcusSpotRouteObservation {
            chain_id: self.inner.config.chain_id,
            sell_symbol: sell.symbol.clone(),
            buy_symbol: buy.symbol.clone(),
            sell_token: sell.address.clone(),
            buy_token: buy.address.clone(),
            sell_amount: sell_amount.to_string(),
            response,
        })
    }

    /// Fetch the public Arcus Spot overview used as a reference-price source.
    pub async fn reference_overview(
        &self,
    ) -> Result<ArcusSpotObservation<Vec<ArcusSpotOverviewEntry>>, ArcusSpotError> {
        self.get_json(&self.inner.meta_base_url, "v1/api-meta/spot/overview", &[])
            .await
    }

    /// Fetch and cross-check the reference-price row for a verified symbol.
    pub async fn reference_price_by_symbol(
        &self,
        symbol: &str,
    ) -> Result<ArcusSpotObservation<ArcusSpotOverviewEntry>, ArcusSpotError> {
        let token = self.verified_token(symbol).await?;
        self.reference_price_for(&token).await
    }

    /// Fetch and cross-check the reference-price row for a verified token.
    async fn reference_price_for(
        &self,
        token: &ArcusSpotToken,
    ) -> Result<ArcusSpotObservation<ArcusSpotOverviewEntry>, ArcusSpotError> {
        validate_token(token)?;
        let observation = self.reference_overview().await?;
        let entry = observation
            .payload
            .iter()
            .find(|entry| entry.ticker.eq_ignore_ascii_case(&token.symbol))
            .cloned()
            .ok_or_else(|| ArcusSpotError::TokenNotFound {
                chain_id: token.chain_id,
                symbol: token.symbol.clone(),
            })?;
        if !entry.contract_address.eq_ignore_ascii_case(&token.address) {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "reference address mismatch for {}: token-list={} overview={}",
                token.symbol, token.address, entry.contract_address
            )));
        }
        let price = entry.quote.price.ok_or_else(|| {
            ArcusSpotError::InvalidResponse(format!("reference price for {} is null", token.symbol))
        })?;
        if price <= Decimal::ZERO {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "reference price for {} is not positive: {}",
                token.symbol, price
            )));
        }
        Ok(observation.map(|_| entry))
    }

    /// Wait until it's this caller's turn, then atomically claim the slot.
    ///
    /// Every iteration re-derives the earliest allowed send time from fresh
    /// reads of `last_sent_at` and `rate_limit_until` — never from a
    /// pre-computed target carried across a sleep. That's what makes this
    /// immune to the whole class of races earlier versions of this fix
    /// chased individually: a 429 cooldown recorded mid-sleep, a due-now
    /// slot that skipped the cooldown check entirely, and (Codex review)
    /// several queued callers all waking into a burst if the runtime stalls
    /// long enough to blow past multiple callers' reserved times at once.
    /// Whichever caller wins the lock first when its own `now >= earliest`
    /// claims that slot by advancing `last_sent_at`; every other caller —
    /// no matter how many woke up at once — recomputes `earliest` against
    /// the now-later `last_sent_at` and waits again, so the interval is
    /// enforced against reality at the moment of the send decision, not
    /// against a stale plan made before sleeping.
    async fn pace(&self) {
        let interval = Duration::from_millis(self.inner.config.min_request_interval_ms);
        loop {
            let now = Instant::now();
            let wait = {
                let mut state = self.inner.pace_state.lock().await;
                let earliest = std::cmp::max(state.last_sent_at + interval, state.rate_limit_until);
                if now >= earliest {
                    state.last_sent_at = now;
                    None
                } else {
                    Some(earliest - now)
                }
            };
            match wait {
                None => return,
                Some(wait) => tokio::time::sleep(wait).await,
            }
        }
    }

    async fn get_json<T: DeserializeOwned>(
        &self,
        base_url: &Url,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<ArcusSpotObservation<T>, ArcusSpotError> {
        let endpoint = base_url.join(path).map_err(|error| {
            ArcusSpotError::InvalidConfig(format!("invalid endpoint path {path:?}: {error}"))
        })?;
        for attempt in 1..=self.inner.config.max_attempts {
            self.pace().await;
            let requested_at = Utc::now();
            let started = Instant::now();
            let response = match self
                .inner
                .http
                .get(endpoint.clone())
                .query(query)
                .send()
                .await
            {
                Ok(response) => response,
                Err(source) => {
                    let classification = if source.is_timeout() {
                        ArcusSpotFailureClass::Timeout
                    } else {
                        ArcusSpotFailureClass::Transport
                    };
                    let retryable = is_retryable_transport_error(&source);
                    if retryable && attempt < self.inner.config.max_attempts {
                        self.sleep_before_retry(attempt, None).await;
                        continue;
                    }
                    return Err(ArcusSpotError::Transport {
                        endpoint: endpoint.to_string(),
                        classification,
                        retryable,
                        attempts: attempt,
                        source,
                    });
                }
            };
            let status = response.status();
            let retry_after =
                retry_after_duration(response.headers().get(RETRY_AFTER), SystemTime::now());
            if let Some(delay) = retry_after {
                // A quota-wide cooldown applies to every cloned client
                // sharing this `Arc`, not just this call's own retry —
                // record it in the shared pacing gate immediately, even on
                // a terminal attempt that won't retry itself, so `pace()`
                // holds off other/later callers too (Codex review).
                self.record_retry_after(delay).await;
            }
            let body = match response.bytes().await {
                Ok(body) => body,
                Err(source) => {
                    // Headers already delivered a definitive status; prefer
                    // that over reclassifying this as a generic transport
                    // failure. A non-retryable status (e.g. 404) must not
                    // spend the retry budget just because the body then
                    // failed to read, and the reported error should surface
                    // the real HTTP status instead of a misleading
                    // `retryable: true` transport failure (Codex review).
                    if !status.is_success() {
                        let retryable = is_retryable_status(status);
                        if retryable && attempt < self.inner.config.max_attempts {
                            self.sleep_before_retry(attempt, retry_after).await;
                            continue;
                        }
                        return Err(ArcusSpotError::Http {
                            endpoint: endpoint.to_string(),
                            status: status.as_u16(),
                            classification: if status == StatusCode::TOO_MANY_REQUESTS {
                                ArcusSpotFailureClass::RateLimited
                            } else {
                                ArcusSpotFailureClass::Http
                            },
                            retryable,
                            attempts: attempt,
                            retry_after_ms: retry_after.map(duration_millis_u64),
                            body: format!("<body unavailable: {source}>"),
                        });
                    }
                    // A successful status but a body that still failed to
                    // read (truncated / connection dropped mid-stream) has
                    // no status-based signal to lean on — a genuine
                    // transport-level failure.
                    let classification = if source.is_timeout() {
                        ArcusSpotFailureClass::Timeout
                    } else {
                        ArcusSpotFailureClass::Transport
                    };
                    if attempt < self.inner.config.max_attempts {
                        self.sleep_before_retry(attempt, retry_after).await;
                        continue;
                    }
                    return Err(ArcusSpotError::Transport {
                        endpoint: endpoint.to_string(),
                        classification,
                        retryable: true,
                        attempts: attempt,
                        source,
                    });
                }
            };
            if !status.is_success() {
                let retryable = is_retryable_status(status);
                if retryable && attempt < self.inner.config.max_attempts {
                    self.sleep_before_retry(attempt, retry_after).await;
                    continue;
                }
                return Err(ArcusSpotError::Http {
                    endpoint: endpoint.to_string(),
                    status: status.as_u16(),
                    classification: if status == StatusCode::TOO_MANY_REQUESTS {
                        ArcusSpotFailureClass::RateLimited
                    } else {
                        ArcusSpotFailureClass::Http
                    },
                    retryable,
                    attempts: attempt,
                    retry_after_ms: retry_after.map(duration_millis_u64),
                    body: truncated_body(&body),
                });
            }
            let payload =
                serde_json::from_slice(&body).map_err(|source| ArcusSpotError::InvalidJson {
                    endpoint: endpoint.to_string(),
                    attempts: attempt,
                    source,
                })?;
            return Ok(ArcusSpotObservation {
                payload,
                requested_at,
                received_at: Utc::now(),
                latency_ms: duration_millis_u64(started.elapsed()),
                attempts: attempt,
            });
        }
        unreachable!("Arcus Spot request loop validates max_attempts >= 1")
    }

    /// Push the shared pacing gate out to at least `now + delay` (bounded by
    /// `max_retry_delay_ms`) so every future `pace()` call — on this client
    /// or any of its clones — waits out a server-advertised, quota-wide
    /// cooldown instead of only the caller that happened to observe it.
    async fn record_retry_after(&self, delay: Duration) {
        let bounded = std::cmp::min(
            delay,
            Duration::from_millis(self.inner.config.max_retry_delay_ms),
        );
        let resume_at = Instant::now() + bounded;
        let mut state = self.inner.pace_state.lock().await;
        if resume_at > state.rate_limit_until {
            state.rate_limit_until = resume_at;
        }
    }

    async fn sleep_before_retry(&self, attempt: u32, retry_after: Option<Duration>) {
        let exponent = attempt.saturating_sub(1).min(20);
        let multiplier = 1_u32 << exponent;
        let backoff =
            Duration::from_millis(self.inner.config.retry_base_delay_ms).saturating_mul(multiplier);
        let requested_delay = std::cmp::max(backoff, retry_after.unwrap_or(Duration::ZERO));
        let delay = std::cmp::min(
            requested_delay,
            Duration::from_millis(self.inner.config.max_retry_delay_ms),
        );
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
    }
}

fn validate_config(config: &ArcusSpotConfig) -> Result<(), ArcusSpotError> {
    if config.chain_id == 0 {
        return Err(ArcusSpotError::InvalidConfig(
            "chain_id must be greater than zero".to_string(),
        ));
    }
    if config.request_timeout_ms == 0 {
        return Err(ArcusSpotError::InvalidConfig(
            "request_timeout_ms must be greater than zero".to_string(),
        ));
    }
    if !(1..=10).contains(&config.max_attempts) {
        return Err(ArcusSpotError::InvalidConfig(
            "max_attempts must be between 1 and 10".to_string(),
        ));
    }
    if config.user_agent.trim().is_empty() {
        return Err(ArcusSpotError::InvalidConfig(
            "user_agent must not be empty".to_string(),
        ));
    }
    Ok(())
}

fn parse_base_url(field: &str, raw: &str) -> Result<Url, ArcusSpotError> {
    let mut url = Url::parse(raw)
        .map_err(|error| ArcusSpotError::InvalidConfig(format!("{field}: {error}")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(ArcusSpotError::InvalidConfig(format!(
            "{field} must use http or https"
        )));
    }
    if url.cannot_be_a_base() || url.query().is_some() || url.fragment().is_some() {
        return Err(ArcusSpotError::InvalidConfig(format!(
            "{field} must be a base URL without query or fragment"
        )));
    }
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    Ok(url)
}

fn normalize_symbol(symbol: &str) -> String {
    symbol.trim().to_ascii_uppercase()
}

fn validate_token(token: &ArcusSpotToken) -> Result<(), ArcusSpotError> {
    if !token.verified {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "token {} is not server-verified",
            token.symbol
        )));
    }
    if normalize_symbol(&token.symbol).is_empty() {
        return Err(ArcusSpotError::InvalidResponse(
            "token has an empty symbol".to_string(),
        ));
    }
    if token.decimals > u8::MAX as u32 {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "token {} has invalid decimals {}",
            token.symbol, token.decimals
        )));
    }
    Address::from_str(&token.address).map_err(|error| {
        ArcusSpotError::InvalidResponse(format!(
            "token {} has invalid address {}: {}",
            token.symbol, token.address, error
        ))
    })?;
    if let Some(wrapped) = &token.wrapped_token_address {
        Address::from_str(wrapped).map_err(|error| {
            ArcusSpotError::InvalidResponse(format!(
                "token {} has invalid wrappedTokenAddress {}: {}",
                token.symbol, wrapped, error
            ))
        })?;
    }
    Ok(())
}

fn parse_raw_amount(field: &str, raw: &str) -> Result<U256, ArcusSpotError> {
    U256::from_dec_str(raw).map_err(|error| {
        ArcusSpotError::InvalidResponse(format!("{field} is not a uint256 decimal: {raw}: {error}"))
    })
}

fn is_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

/// Distinguish a transient transport failure (worth retrying) from a
/// permanent one. A redirect-policy failure (e.g. a looping custom or
/// proxied endpoint) or a malformed request will fail identically on every
/// attempt, so retrying just burns the configured attempt budget and, on
/// the final attempt, would otherwise mislabel the terminal error as
/// `retryable: true` for callers that honor `ArcusSpotError::retryable()`.
///
/// `is_connect()` only covers failures during connection establishment; an
/// already-established connection dropped before headers arrive, or a reset
/// HTTP/2 stream, surfaces as `is_request()`/`is_body()` instead and is just
/// as transient — excluding them left `get_json` giving up after one attempt
/// on an ordinary dropped connection even with attempts to spare (Codex
/// review). `is_decode()` (malformed response encoding) and `is_builder()` /
/// `is_redirect()` remain non-retryable: those fail identically every time.
fn is_retryable_transport_error(source: &reqwest::Error) -> bool {
    source.is_timeout() || source.is_connect() || source.is_request() || source.is_body()
}

fn retry_after_duration(value: Option<&HeaderValue>, now: SystemTime) -> Option<Duration> {
    let raw = value?.to_str().ok()?.trim();
    if let Ok(seconds) = raw.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let deadline = DateTime::parse_from_rfc2822(raw).ok()?.with_timezone(&Utc);
    let now: DateTime<Utc> = now.into();
    if deadline <= now {
        return Some(Duration::ZERO);
    }
    (deadline - now).to_std().ok()
}

fn duration_millis_u64(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

fn truncated_body(body: &[u8]) -> String {
    let body = String::from_utf8_lossy(body);
    let mut chars = body.chars();
    let prefix: String = chars.by_ref().take(512).collect();
    if chars.next().is_some() {
        format!("{prefix}…")
    } else {
        prefix
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    const TOKEN_FIXTURE: &str = include_str!("arcus_spot_connector/fixtures/tokens_nvda_amd.json");
    const PRICE_FIXTURE: &str = include_str!("arcus_spot_connector/fixtures/price_nvda_amd.json");
    const OVERVIEW_FIXTURE: &str =
        include_str!("arcus_spot_connector/fixtures/overview_nvda_amd.json");

    #[test]
    fn token_fixture_captures_verified_and_wrapped_metadata() {
        let tokens: Vec<ArcusSpotToken> = serde_json::from_str(TOKEN_FIXTURE).unwrap();
        assert_eq!(tokens.len(), 2);
        assert!(tokens.iter().all(|token| token.verified));
        assert!(tokens
            .iter()
            .all(|token| token.wrapped_token_address.is_some()));
        for token in &tokens {
            validate_token(token).unwrap();
        }
    }

    #[test]
    fn price_fixture_preserves_all_venues_and_validates_echoed_amount() {
        let response: ArcusSpotPriceResponse = serde_json::from_str(PRICE_FIXTURE).unwrap();
        response
            .validate(U256::from_dec_str("24183796856106408").unwrap())
            .unwrap();
        assert_eq!(response.quotes.len(), 3);
        assert_eq!(response.recommended_quote().unwrap().venue, "arcus");
        assert!(response.quotes.iter().all(|quote| quote.fees.is_empty()));
        assert!(response.errors.is_empty());
    }

    #[test]
    fn price_models_preserve_itemized_fees_and_venue_errors() {
        let response: ArcusSpotPriceResponse = serde_json::from_value(serde_json::json!({
            "recommended": "venue-a",
            "all": [{
                "venue": "venue-a",
                "buyAmount": "99",
                "sellAmount": "100",
                "fees": [{"type": "protocol", "amount": "1"}],
                "routeId": "kept"
            }],
            "errors": [{"venue": "venue-b", "message": "unavailable"}],
            "requestId": "also-kept"
        }))
        .unwrap();
        response.validate(U256::from(100_u64)).unwrap();
        assert_eq!(response.quotes[0].fees[0]["amount"], "1");
        assert_eq!(response.quotes[0].extra["routeId"], "kept");
        assert_eq!(response.errors[0]["venue"], "venue-b");
        assert_eq!(response.extra["requestId"], "also-kept");
    }

    #[test]
    fn overview_fixture_uses_decimal_reference_prices() {
        let rows: Vec<ArcusSpotOverviewEntry> = serde_json::from_str(OVERVIEW_FIXTURE).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].ticker, "NVDA");
        assert!(rows
            .iter()
            .all(|row| row.quote.price.is_some_and(|price| price > Decimal::ZERO)));
    }

    #[test]
    fn retry_after_supports_seconds_and_http_dates() {
        let seconds = HeaderValue::from_static("7");
        assert_eq!(
            retry_after_duration(Some(&seconds), SystemTime::UNIX_EPOCH),
            Some(Duration::from_secs(7))
        );
        let date = HeaderValue::from_static("Thu, 01 Jan 1970 00:00:10 GMT");
        assert_eq!(
            retry_after_duration(Some(&date), SystemTime::UNIX_EPOCH),
            Some(Duration::from_secs(10))
        );
    }

    #[test]
    fn invalid_config_is_rejected_before_network_use() {
        let config = ArcusSpotConfig {
            max_attempts: 0,
            ..ArcusSpotConfig::default()
        };
        let error = ArcusSpotClient::new(config).err().unwrap();
        assert_eq!(error.classification(), ArcusSpotFailureClass::InvalidConfig);
        assert!(!error.retryable());
    }

    async fn spawn_http_sequence(responses: Vec<String>) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            for response in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 4096];
                let _ = socket.read(&mut request).await.unwrap();
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (format!("http://{address}"), task)
    }

    /// Same as `spawn_http_sequence`, but also records each accepted
    /// connection's arrival `Instant` so a test can assert on the real
    /// spacing between requests that actually reached the server.
    async fn spawn_logged_http_sequence(
        responses: Vec<String>,
    ) -> (
        String,
        std::sync::Arc<std::sync::Mutex<Vec<Instant>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let log_for_task = log.clone();
        let task = tokio::spawn(async move {
            for response in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                log_for_task.lock().unwrap().push(Instant::now());
                let mut request = [0_u8; 4096];
                let _ = socket.read(&mut request).await.unwrap();
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (format!("http://{address}"), log, task)
    }

    #[tokio::test]
    async fn public_get_retries_429_and_records_attempt_count() {
        let success = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            TOKEN_FIXTURE.len(),
            TOKEN_FIXTURE
        );
        let (base_url, server) = spawn_http_sequence(vec![
            "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string(),
            success,
        ])
        .await;
        let client = ArcusSpotClient::new(ArcusSpotConfig {
            router_base_url: base_url.clone(),
            meta_base_url: base_url,
            min_request_interval_ms: 0,
            max_attempts: 2,
            retry_base_delay_ms: 0,
            max_retry_delay_ms: 0,
            ..ArcusSpotConfig::default()
        })
        .unwrap();
        let observation = client.refresh_tokens().await.unwrap();
        assert_eq!(observation.attempts, 2);
        assert_eq!(observation.payload.len(), 2);
        assert_eq!(client.verified_token("nvda").await.unwrap().symbol, "NVDA");
        server.await.unwrap();
    }

    // Codex review on PR #43: an already-established connection dropped
    // before headers arrive (or a reset HTTP/2 stream) surfaces from reqwest
    // as `is_request()`/`is_body()`, not `is_connect()` — the first version
    // of `is_retryable_transport_error` only checked timeout/connect, so a
    // plain dropped connection (accepted, then closed with no response
    // bytes) stopped `get_json` after one attempt even with attempts left.
    #[tokio::test]
    async fn get_json_retries_a_dropped_connection() {
        let success = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            TOKEN_FIXTURE.len(),
            TOKEN_FIXTURE
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            // First connection: accept and read the request, then drop the
            // socket without writing any response bytes.
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = socket.read(&mut request).await.unwrap();
            drop(socket);

            // Second connection: respond normally.
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = socket.read(&mut request).await.unwrap();
            socket.write_all(success.as_bytes()).await.unwrap();
        });

        let base_url = format!("http://{address}");
        let client = ArcusSpotClient::new(ArcusSpotConfig {
            router_base_url: base_url.clone(),
            meta_base_url: base_url,
            min_request_interval_ms: 0,
            max_attempts: 2,
            retry_base_delay_ms: 0,
            max_retry_delay_ms: 0,
            ..ArcusSpotConfig::default()
        })
        .unwrap();

        let observation = client.refresh_tokens().await.unwrap();
        assert_eq!(
            observation.attempts, 2,
            "a dropped connection must be retried, not treated as a permanent failure"
        );
        assert_eq!(observation.payload.len(), 2);
        server.await.unwrap();
    }

    // Codex review on PR #43: headers already delivered a definitive,
    // non-retryable status (404) before the body failed to read (truncated:
    // Content-Length claims more than is actually sent). The body-read error
    // alone looks like a generic transient transport failure, but the
    // status is authoritative and must win — report the 404, not a
    // retryable transport error, and don't burn the attempt budget on it.
    #[tokio::test]
    async fn body_read_failure_after_non_retryable_status_reports_that_status() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = socket.read(&mut request).await.unwrap();
            socket
                .write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 100\r\nConnection: close\r\n\r\nshort",
                )
                .await
                .unwrap();
        });

        let base_url = format!("http://{address}");
        let client = ArcusSpotClient::new(ArcusSpotConfig {
            router_base_url: base_url.clone(),
            meta_base_url: base_url,
            min_request_interval_ms: 0,
            max_attempts: 3,
            retry_base_delay_ms: 0,
            max_retry_delay_ms: 0,
            ..ArcusSpotConfig::default()
        })
        .unwrap();

        let error = client.refresh_tokens().await.unwrap_err();
        server.await.unwrap();

        match error {
            ArcusSpotError::Http {
                status,
                retryable,
                attempts,
                ..
            } => {
                assert_eq!(status, 404);
                assert!(
                    !retryable,
                    "a 404 must not be reported as retryable just because its body failed to read"
                );
                assert_eq!(
                    attempts, 1,
                    "a non-retryable status must fail immediately, not exhaust attempts on the body-read error"
                );
            }
            other => panic!("expected Http error with status 404, got {other:?}"),
        }
    }

    // Codex review on PR #43: concurrent lookups on a cold cache each used to
    // fire their own `refresh_tokens` call. The mock server below serves
    // exactly one `/v1/tokens` response; if more than one request reached it
    // concurrently, only the first caller would get a response and the
    // others would time out waiting on a connection nobody answers.
    #[tokio::test]
    async fn concurrent_verified_token_lookups_share_one_refresh() {
        let success = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            TOKEN_FIXTURE.len(),
            TOKEN_FIXTURE
        );
        let (base_url, server) = spawn_http_sequence(vec![success]).await;
        let client = ArcusSpotClient::new(ArcusSpotConfig {
            router_base_url: base_url.clone(),
            meta_base_url: base_url,
            min_request_interval_ms: 0,
            max_attempts: 1,
            retry_base_delay_ms: 0,
            max_retry_delay_ms: 0,
            request_timeout_ms: 2_000,
            ..ArcusSpotConfig::default()
        })
        .unwrap();

        let (nvda, amd, nvda_again) = tokio::join!(
            client.verified_token("nvda"),
            client.verified_token("amd"),
            client.verified_token("nvda"),
        );
        assert_eq!(nvda.unwrap().symbol, "NVDA");
        assert_eq!(amd.unwrap().symbol, "AMD");
        assert_eq!(nvda_again.unwrap().symbol, "NVDA");
        server.await.unwrap();
    }

    // Codex review on PR #43: a caller already queued in pace() (reserved a
    // future send slot and is asleep waiting for it) must recheck the
    // shared gate after waking, not send on its original reservation. Two
    // clones race here with min_request_interval_ms=300: tokio::join! polls
    // client_a first, whose pace() resolves immediately (nothing queued
    // yet) so it sends before client_b's pace() call, which reserves a
    // ~300ms-out slot and sleeps. client_a's request gets a 429 with a
    // 1s Retry-After well within that 300ms, pushing the shared gate out
    // via record_retry_after; client_b must wake, see the extended gate,
    // and keep waiting instead of sending at its stale ~300ms mark.
    #[tokio::test]
    async fn queued_caller_rechecks_cooldown_after_waking() {
        let success = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            TOKEN_FIXTURE.len(),
            TOKEN_FIXTURE
        );
        let (base_url, log, server) = spawn_logged_http_sequence(vec![
            "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string(),
            success,
        ])
        .await;

        let client_a = ArcusSpotClient::new(ArcusSpotConfig {
            router_base_url: base_url.clone(),
            meta_base_url: base_url,
            min_request_interval_ms: 300,
            max_attempts: 1,
            retry_base_delay_ms: 0,
            max_retry_delay_ms: 5_000,
            ..ArcusSpotConfig::default()
        })
        .unwrap();
        let client_b = client_a.clone();

        let (first, second) = tokio::join!(client_a.refresh_tokens(), client_b.refresh_tokens());
        assert!(
            first.is_err(),
            "tokio::join!'s poll order sends client_a's request first, which must hit the 429"
        );
        assert_eq!(second.unwrap().payload.len(), 2);

        server.await.unwrap();
        let timestamps = log.lock().unwrap().clone();
        assert_eq!(
            timestamps.len(),
            2,
            "expected exactly two requests at the server"
        );
        let gap = timestamps[1].duration_since(timestamps[0]);
        assert!(
            gap >= Duration::from_millis(900),
            "the queued second caller must wait out the 429 cooldown observed by the first \
             instead of sending at its original ~300ms reservation, waited only {:?}",
            gap
        );
    }

    // Codex review on PR #43: with a 200ms interval and a 1s cooldown, two
    // callers originally queued at ~200ms and ~400ms (both before the
    // cooldown floor) must not both collapse onto the same ~1s wake time —
    // they must re-claim FIFO slots after the floor and stay interval-spaced
    // relative to each other, not just relative to the caller that hit the
    // 429.
    #[tokio::test]
    async fn queued_callers_stay_interval_spaced_after_a_shared_cooldown() {
        let success = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            TOKEN_FIXTURE.len(),
            TOKEN_FIXTURE
        );
        let (base_url, log, server) = spawn_logged_http_sequence(vec![
            "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string(),
            success.clone(),
            success,
        ])
        .await;

        let client = ArcusSpotClient::new(ArcusSpotConfig {
            router_base_url: base_url.clone(),
            meta_base_url: base_url,
            min_request_interval_ms: 200,
            max_attempts: 1,
            retry_base_delay_ms: 0,
            max_retry_delay_ms: 5_000,
            ..ArcusSpotConfig::default()
        })
        .unwrap();
        let a = client.clone();
        let b = client.clone();
        let c = client.clone();

        let (first, second, third) =
            tokio::join!(a.refresh_tokens(), b.refresh_tokens(), c.refresh_tokens());
        assert!(
            first.is_err(),
            "tokio::join!'s poll order sends the first caller's request first, which must hit the 429"
        );
        second.unwrap();
        third.unwrap();

        server.await.unwrap();
        let timestamps = log.lock().unwrap().clone();
        assert_eq!(
            timestamps.len(),
            3,
            "expected exactly three requests at the server"
        );
        let cooldown_gap = timestamps[1].duration_since(timestamps[0]);
        assert!(
            cooldown_gap >= Duration::from_millis(900),
            "the second request must wait out the cooldown observed by the first, waited only {:?}",
            cooldown_gap
        );
        let queued_gap = timestamps[2].duration_since(timestamps[1]);
        assert!(
            queued_gap >= Duration::from_millis(170) && queued_gap <= Duration::from_millis(280),
            "the two callers queued behind the cooldown must remain ~200ms interval-spaced \
             relative to each other, not collapse onto the same wake time; gap was {:?}",
            queued_gap
        );
    }

    // Codex review on PR #43: an earlier version of pace() only consulted
    // rate_limit_until after actually sleeping, so a caller whose slot
    // happened to already be due-now returned immediately without ever
    // checking the floor. pace() now reads rate_limit_until fresh on every
    // iteration, including the very first, so this can't happen by
    // construction — verified directly here by forcing the floor into the
    // future and asserting pace() still waits it out.
    #[tokio::test]
    async fn pace_checks_the_cooldown_even_on_an_immediate_slot() {
        let client = ArcusSpotClient::new(ArcusSpotConfig {
            min_request_interval_ms: 0,
            ..ArcusSpotConfig::default()
        })
        .unwrap();
        let cooldown = Duration::from_millis(500);
        client.inner.pace_state.lock().await.rate_limit_until = Instant::now() + cooldown;

        let started = Instant::now();
        client.pace().await;
        assert!(
            started.elapsed() >= Duration::from_millis(450),
            "pace() must wait out an active rate-limit floor even when its own FIFO \
             reservation is already due-now, waited only {:?}",
            started.elapsed()
        );
    }

    // Codex review on PR #43: the first recheck-the-gate fix mixed the FIFO
    // reservation counter with the 429 cooldown floor in one variable. With
    // no 429 ever involved, three concurrent callers each advance that same
    // counter, so a caller queued behind the others read the counter's tail
    // (left by callers queued *after* it) as if it were a cooldown extension
    // and waited for it — collapsing distinct slots into a burst, or waiting
    // far longer than min_request_interval_ms. Requests must land roughly
    // interval-apart, neither bursted together nor needlessly delayed.
    #[tokio::test]
    async fn concurrent_pacing_preserves_distinct_fifo_slots_without_a_cooldown() {
        let success = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            TOKEN_FIXTURE.len(),
            TOKEN_FIXTURE
        );
        let (base_url, log, server) =
            spawn_logged_http_sequence(vec![success.clone(), success.clone(), success]).await;

        let client = ArcusSpotClient::new(ArcusSpotConfig {
            router_base_url: base_url.clone(),
            meta_base_url: base_url,
            min_request_interval_ms: 200,
            max_attempts: 1,
            retry_base_delay_ms: 0,
            max_retry_delay_ms: 5_000,
            ..ArcusSpotConfig::default()
        })
        .unwrap();
        let a = client.clone();
        let b = client.clone();
        let c = client.clone();

        let (r1, r2, r3) = tokio::join!(a.refresh_tokens(), b.refresh_tokens(), c.refresh_tokens());
        r1.unwrap();
        r2.unwrap();
        r3.unwrap();

        server.await.unwrap();
        let timestamps = log.lock().unwrap().clone();
        assert_eq!(
            timestamps.len(),
            3,
            "expected exactly three requests at the server"
        );
        for (i, window) in timestamps.windows(2).enumerate() {
            let gap = window[1].duration_since(window[0]);
            assert!(
                gap >= Duration::from_millis(170) && gap <= Duration::from_millis(280),
                "request {} arrived {:?} after the previous one; expected ~200ms FIFO \
                 spacing, not a burst (too small) or an inflated wait (too large)",
                i + 2,
                gap
            );
        }
    }

    // Codex review on PR #43: a quota-wide 429 Retry-After previously only
    // delayed the caller that received it (via sleep_before_retry); other
    // callers sharing the same client kept using the unmodified pacing gate
    // and could send during the advertised cooldown. max_attempts=1 means
    // the first call surfaces the 429 immediately without retrying itself,
    // isolating the shared pacing state as the only thing that can delay
    // the second, independent call.
    #[tokio::test]
    async fn rate_limit_cooldown_applies_to_next_caller_even_without_its_own_retry() {
        let success = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            TOKEN_FIXTURE.len(),
            TOKEN_FIXTURE
        );
        let (base_url, server) = spawn_http_sequence(vec![
            "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string(),
            success,
        ])
        .await;
        let client = ArcusSpotClient::new(ArcusSpotConfig {
            router_base_url: base_url.clone(),
            meta_base_url: base_url,
            min_request_interval_ms: 0,
            max_attempts: 1,
            retry_base_delay_ms: 0,
            max_retry_delay_ms: 5_000,
            ..ArcusSpotConfig::default()
        })
        .unwrap();

        let first = client.refresh_tokens().await;
        assert!(
            first.is_err(),
            "a single-attempt client must surface the 429 instead of retrying itself"
        );

        let started = Instant::now();
        let second = client.refresh_tokens().await.unwrap();
        assert!(
            started.elapsed() >= Duration::from_millis(900),
            "a later independent call must still respect the quota-wide Retry-After cooldown, waited only {:?}",
            started.elapsed()
        );
        assert_eq!(second.payload.len(), 2);
        server.await.unwrap();
    }

    // Codex review on PR #43: a redirect-policy failure (e.g. a looping
    // custom or proxied endpoint) previously retried unconditionally and
    // reported `retryable: true` even on the terminal error, wasting the
    // whole attempt budget on a failure that repeats identically every time.
    // The server below always redirects back to itself, which reqwest's
    // default policy eventually refuses to keep following (`is_redirect()`);
    // that must fail the outer retry loop on the first attempt.
    #[tokio::test]
    async fn permanent_transport_error_is_not_retried() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let base_url = format!("http://{address}");
        let redirect_target = base_url.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let mut request = [0_u8; 4096];
                let _ = socket.read(&mut request).await;
                let response = format!(
                    "HTTP/1.1 302 Found\r\nLocation: {redirect_target}/\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });

        let client = ArcusSpotClient::new(ArcusSpotConfig {
            router_base_url: base_url.clone(),
            meta_base_url: base_url,
            min_request_interval_ms: 0,
            max_attempts: 5,
            retry_base_delay_ms: 0,
            max_retry_delay_ms: 0,
            ..ArcusSpotConfig::default()
        })
        .unwrap();

        let error = client.refresh_tokens().await.unwrap_err();
        server.abort();

        assert!(
            !error.retryable(),
            "a redirect-policy failure must not be marked retryable: {error}"
        );
        match error {
            ArcusSpotError::Transport {
                classification,
                attempts,
                ..
            } => {
                assert_eq!(classification, ArcusSpotFailureClass::Transport);
                assert_eq!(
                    attempts, 1,
                    "a permanent transport error must fail on the first attempt, not exhaust retries"
                );
            }
            other => panic!("expected Transport error, got {other:?}"),
        }
    }

    #[tokio::test]
    #[ignore = "read-only public Arcus API smoke; requires network"]
    async fn public_arcus_spot_read_only_smoke() {
        let client = ArcusSpotClient::new(ArcusSpotConfig::default()).unwrap();
        let tokens = client.refresh_tokens().await.unwrap();
        assert!(tokens.payload.iter().any(|token| token.symbol == "NVDA"));
        let route = client
            .indicative_price_by_symbol("NVDA", "AMD", "1000000000000000")
            .await
            .unwrap();
        assert!(!route.response.payload.quotes.is_empty());
        let reverse_amount = route
            .response
            .payload
            .recommended_quote()
            .unwrap()
            .buy_amount
            .clone();
        let reverse = client
            .indicative_price_by_symbol("AMD", "NVDA", &reverse_amount)
            .await
            .unwrap();
        assert!(!reverse.response.payload.quotes.is_empty());
        let reference = client.reference_price_by_symbol("NVDA").await.unwrap();
        assert!(reference
            .payload
            .quote
            .price
            .is_some_and(|price| price > Decimal::ZERO));
    }
}

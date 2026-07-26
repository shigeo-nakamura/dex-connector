//! Strictly read-only Arcus Spot router client.
//!
//! This module intentionally exposes only public metadata, indicative-price,
//! and reference-price GETs. It has no wallet, approval, signing, firm-quote,
//! submission, or status-mutation surface. Arcus Spot is inventory-funded and
//! does not fit the leveraged-perpetual [crate::DexConnector] contract, so
//! the P0 client remains a separate API.

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
use tokio::sync::{Mutex, RwLock};

const DEFAULT_ROUTER_BASE_URL: &str = "https://router.spot.arcus.xyz";
const DEFAULT_META_BASE_URL: &str = "https://api.arcus.xyz";
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
    pub chain_id: u64,
    pub request_timeout_ms: u64,
    pub min_request_interval_ms: u64,
    pub max_attempts: u32,
    pub retry_base_delay_ms: u64,
    pub max_retry_delay_ms: u64,
    pub user_agent: String,
}

impl Default for ArcusSpotConfig {
    fn default() -> Self {
        Self {
            router_base_url: DEFAULT_ROUTER_BASE_URL.to_string(),
            meta_base_url: DEFAULT_META_BASE_URL.to_string(),
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

struct ArcusSpotClientInner {
    config: ArcusSpotConfig,
    router_base_url: Url,
    meta_base_url: Url,
    http: Client,
    next_request_at: Mutex<Instant>,
    token_cache: RwLock<TokenCache>,
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
        let http = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .user_agent(config.user_agent.clone())
            .build()
            .map_err(|error| {
                ArcusSpotError::InvalidConfig(format!("could not build HTTP client: {error}"))
            })?;
        Ok(Self {
            inner: Arc::new(ArcusSpotClientInner {
                config,
                router_base_url,
                meta_base_url,
                http,
                next_request_at: Mutex::new(Instant::now()),
                token_cache: RwLock::new(TokenCache::default()),
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
            self.refresh_tokens().await?;
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

    async fn pace(&self) {
        let interval = Duration::from_millis(self.inner.config.min_request_interval_ms);
        let wait = {
            let mut next_request_at = self.inner.next_request_at.lock().await;
            let now = Instant::now();
            let scheduled_at = std::cmp::max(*next_request_at, now);
            *next_request_at = scheduled_at + interval;
            scheduled_at.saturating_duration_since(now)
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
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
                    if attempt < self.inner.config.max_attempts {
                        self.sleep_before_retry(attempt, None).await;
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
            let status = response.status();
            let retry_after =
                retry_after_duration(response.headers().get(RETRY_AFTER), SystemTime::now());
            let body = match response.bytes().await {
                Ok(body) => body,
                Err(source) => {
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

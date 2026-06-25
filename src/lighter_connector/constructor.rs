//! `LighterConnector` construction and field initialization.
//!
//! Keeping constructor wiring out of account/auth helpers makes future
//! module-boundary work easier without changing the public constructor.

use super::*;

impl LighterConnector {
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
            .map_err(|e| DexError::Transient(format!("Failed to build HTTP client: {}", e)))?;

        Ok(Self {
            api_key_public: config.api_key_public,
            api_key_index: config.api_key_index,
            api_private_key_hex: config.api_private_key_hex,
            // EVM wallet key only exists on the struct under the lighter-sdk
            // feature (used for API-key registration via the Go SDK); the
            // stub build omits the field entirely.
            #[cfg(feature = "lighter-sdk")]
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
            current_price: Arc::new(RwLock::new(HashMap::new())),
            order_book: Arc::new(RwLock::new(HashMap::new())),
            maintenance: Arc::new(RwLock::new(MaintenanceInfo {
                next_start: None,
                last_checked: None,
            })),
            outage_detector: Arc::new(std::sync::Mutex::new(OutageDetector::default())),
            cached_open_orders: Arc::new(RwLock::new(HashMap::new())),
            account_state: Arc::new(RwLock::new(AccountState::default())),
            positions_ready: Arc::new(AtomicBool::new(false)),
            // Connection epoch counter for race detection
            connection_epoch: Arc::new(AtomicU64::new(0)),
            market_cache: Arc::clone(&MARKET_CACHE),
            market_cache_init_lock: Arc::clone(&MARKET_CACHE_INIT_LOCK),
            tracked_symbols: config.tracked_symbols,
            nonce_cache: Arc::new(tokio::sync::Mutex::new(None)),
            nonce_cache_ttl: Duration::from_secs(30),
            ob_stale_after: Duration::from_secs(ob_stale_secs),
            funding_rate_cache: Arc::new(RwLock::new(HashMap::new())),
            price_update_tx: tokio::sync::broadcast::channel(128).0,
            ws_timing: WsTimingConfig::default(),
            rate_limiter: crate::lighter_ratelimit::RateLimitClient::from_env(),
        })
    }
}

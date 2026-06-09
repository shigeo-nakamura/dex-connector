//! Authentication, account discovery, API-key registration, connector
//! lifecycle (auto-cleanup / maintenance refresher / outage tracking),
//! and the cached `/account` balance helpers for `LighterConnector`.
//!
//! These are private `impl LighterConnector` blocks split out of `mod.rs`
//! (bot-strategy#501). They read connector state and feed the REST /
//! Go-SDK signing paths. The public `impl DexConnector` trait surface
//! lives in `dex_impl.rs`; per-order helpers live in `orders.rs`.

use super::*;

impl LighterConnector {
    /// Initialize Go client
    #[cfg(feature = "lighter-sdk")]
    pub(super) async fn create_go_client(&self) -> Result<(), DexError> {
        unsafe {
            let url = CString::new(self.base_url.as_str())
                .map_err(|e| DexError::Permanent(format!("Invalid URL: {}", e)))?;

            // Use API private key directly (should be 40 bytes / 80 hex chars)
            let private_key_hex = self
                .api_private_key_hex
                .strip_prefix("0x")
                .unwrap_or(&self.api_private_key_hex);

            if private_key_hex.len() != 80 {
                return Err(DexError::InvalidInput {
                    field: "api_private_key_hex".to_string(),
                    value: format!(
                        "must be 40 bytes (80 hex chars), got: {}",
                        private_key_hex.len()
                    ),
                });
            }

            let private_key = CString::new(private_key_hex)
                .map_err(|e| DexError::Permanent(format!("Invalid private key: {}", e)))?;

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
                return Err(DexError::Transient(format!(
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
                let is_rate_limited =
                    error_msg.contains("Too Many Requests") || error_msg.contains("\"code\":23000");

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
                    return Err(DexError::Transient(format!(
                        "API key validation failed: {}",
                        error_msg
                    )));
                }

                #[cfg(not(feature = "lighter-sdk"))]
                return Err(DexError::Transient(format!(
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

    pub(super) fn record_outage_failure(&self, signal: OutageSignal) {
        let transition = {
            let mut detector = self
                .outage_detector
                .lock()
                .expect("outage detector poisoned");
            detector.record_failure(signal, Instant::now())
        };
        if let Some(OutageTransition::Latched { reason }) = transition {
            log::warn!(
                "[MAINTENANCE] Lighter degraded-mode latched via observed {} failures",
                reason
            );
        }
    }

    pub(super) fn record_outage_success(&self, signal: OutageSignal) {
        let transition = {
            let mut detector = self
                .outage_detector
                .lock()
                .expect("outage detector poisoned");
            detector.record_success(signal, Instant::now())
        };
        if let Some(OutageTransition::Cleared { reason }) = transition {
            log::info!(
                "[MAINTENANCE] Lighter degraded-mode cleared after {} recovery",
                reason
            );
        }
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
            log::info!("[MAINTENANCE] LIGHTER_MAINTENANCE_DISABLED set, skipping refresher spawn");
            return;
        }

        if self
            .maintenance_refresher_started
            .swap(true, Ordering::SeqCst)
        {
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
                let backoff_mins = match fetch_next_maintenance_window_with(&client).await {
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

                let sleep_secs = (backoff_mins.max(1) as u64).saturating_mul(60);
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
    pub(super) async fn create_go_client(&self) -> Result<(), DexError> {
        Err(DexError::Transient(
            "Lighter Go SDK not available. Build with --features lighter-sdk to enable."
                .to_string(),
        ))
    }

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
            current_volume: Arc::new(RwLock::new(None)),
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
            return Err(DexError::Transient(
                "No API keys found on server".to_string(),
            ));
        }

        let server_pubkey = response.api_keys[0].public_key.clone();

        // Update cache
        {
            let mut cache = self.cached_server_pubkey.write().await;
            *cache = Some((server_pubkey.clone(), std::time::Instant::now()));
        }

        Ok(server_pubkey)
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
                DexError::Transient(format!("Failed to query accounts for discovery: {}", e))
            })?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| DexError::Transient(format!("Failed to read accounts response: {}", e)))?;

        if !status.is_success() {
            return Err(DexError::Transient(format!(
                "Accounts API returned HTTP {}: {}",
                status, body
            )));
        }

        let account_resp: LighterAccountResponse = serde_json::from_str(&body).map_err(|e| {
            DexError::Transient(format!("Failed to parse accounts response: {}", e))
        })?;

        if account_resp.accounts.is_empty() {
            return Err(DexError::Transient(format!(
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

        Err(DexError::Transient(format!(
            "Could not find account for api_key_index={} in {} accounts for wallet {}. \
             Set LIGHTER_ACCOUNT_INDEX manually.",
            self.api_key_index,
            account_resp.accounts.len(),
            wallet_address
        )))
    }

    pub(super) async fn get_server_public_key(&self) -> Result<String, DexError> {
        // Use cached version to reduce API calls
        self.get_server_public_key_cached().await
    }

    #[cfg(feature = "lighter-sdk")]
    pub(super) async fn register_api_key(
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
    pub(super) async fn register_api_key(
        &self,
        _evm_private_key: &str,
        _go_public_key: &str,
        _server_public_key: &str,
    ) -> Result<(), String> {
        Err("API key registration requires lighter-sdk feature".to_string())
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
            .map_err(|e| DexError::Transient(format!("Failed to get account details: {}", e)))?;

        let status = response.status();
        let response_text = response
            .text()
            .await
            .map_err(|e| DexError::Transient(format!("Failed to read response: {}", e)))?;

        log::debug!(
            "Account details response: HTTP {}, Body: {}",
            status,
            response_text
        );

        if !status.is_success() {
            return Err(DexError::Transient(format!(
                "HTTP {}: {}",
                status, response_text
            )));
        }

        // Parse the response to extract L1 address
        let account_data: serde_json::Value = serde_json::from_str(&response_text)
            .map_err(|e| DexError::Transient(format!("Failed to parse account response: {}", e)))?;

        // Look for l1Address field
        if let Some(l1_address) = account_data.get("l1Address").and_then(|v| v.as_str()) {
            Ok(l1_address.to_string())
        } else {
            Err(DexError::Transient(
                "l1Address not found in account response".to_string(),
            ))
        }
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

impl LighterConnector {
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
    pub(super) const BALANCE_CACHE_TTL_SECS: u64 = 300;

    /// Fetch the `/account?by=index` snapshot via REST and return the first
    /// (and only) account.
    ///
    /// Shared by `get_balance` and `get_combined_balance` (bot-strategy#501)
    /// so both go through one consistent path: the host-shared rate-limit
    /// bucket gate plus the 429 retry/backoff that recovers Lighter's
    /// per-wallet short-window throttle (bot-strategy#142). Previously only
    /// `get_balance` had this gating; `get_combined_balance` issued a bare
    /// `client.get`, so a balance-refresh burst could 429 it with no retry.
    pub(super) async fn fetch_account_via_rest(&self) -> Result<LighterAccountInfo, DexError> {
        let endpoint = format!("/api/v1/account?by=index&value={}", self.account_index);
        let url = format!("{}{}", self.base_url, endpoint);

        // Lighter occasionally 429s the head of a balance-refresh burst even
        // when our per-URL rate is ~1/min. Its short-window per-wallet throttle
        // is shared across all endpoints, so a concurrent order-placement or
        // nextNonce call can push the instantaneous rate over the edge. Retry
        // a couple of times with backoff so a transient 429 doesn't blank out
        // the equity observation for a full cache cycle. See bot-strategy#142.
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
                .map_err(|e| DexError::Transient(format!("Request failed: {}", e)))?;

            let status = response.status();
            let response_text = response
                .text()
                .await
                .map_err(|e| DexError::Transient(format!("Failed to read response: {}", e)))?;

            if status != reqwest::StatusCode::TOO_MANY_REQUESTS
                || attempt >= BALANCE_RETRY_BACKOFF_MS.len()
            {
                break (status, response_text);
            }
            let backoff_ms = BALANCE_RETRY_BACKOFF_MS[attempt];
            // The first 429 is recovered transparently by the backoff retry;
            // logging WARN on every attempt 1 hit pollutes error-watch
            // (bot-strategy#213, #227). Escalate only when the throttle
            // persists into attempt 2+, which signals sustained pressure.
            if attempt == 0 {
                log::info!(
                    "fetch_account: HTTP 429 from Lighter (attempt {}/{}), retrying after {}ms",
                    attempt + 1,
                    BALANCE_RETRY_BACKOFF_MS.len() + 1,
                    backoff_ms
                );
            } else {
                log::warn!(
                    "fetch_account: HTTP 429 from Lighter (attempt {}/{}), retrying after {}ms",
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
            return Err(DexError::Transient(format!(
                "HTTP {}: {}",
                status, response_text
            )));
        }

        let account_response: LighterAccountResponse = serde_json::from_str(&response_text)
            .map_err(|e| DexError::Transient(format!("Failed to parse response: {}", e)))?;

        account_response
            .accounts
            .into_iter()
            .next()
            .ok_or_else(|| DexError::Transient("No account found".to_string()))
    }

    /// Snapshot read of WS-fed balance/position caches for `get_balance`.
    /// Returns `None` if the WS has not yet delivered an account update; the
    /// caller then decides whether to wait (see bot-strategy#148) or fall
    /// back to REST.
    ///
    /// bot-strategy#392: positions + balance now share `account_state`, so
    /// a single read guard covers both lookups instead of acquiring two
    /// locks back-to-back (which #391 already kept from overlapping by
    /// scoping each guard).
    pub(super) async fn try_read_cached_balance(
        &self,
        symbol: Option<&str>,
    ) -> Option<BalanceResponse> {
        let state = self.account_state.read().await;
        if let Some(token_symbol) = symbol {
            let matched = state
                .positions
                .iter()
                .find(|p| p.symbol == token_symbol)
                .map(|pos| BalanceResponse {
                    equity: pos.size,
                    balance: pos.size,
                    position_entry_price: pos.entry_price,
                    position_sign: Some(pos.sign),
                });
            if matched.is_some() {
                return matched;
            }
            let positions_empty = state.positions.is_empty();
            let has_ws_balance = state
                .balance
                .as_ref()
                .map(|(_, fetched_at)| {
                    fetched_at.elapsed() < Duration::from_secs(Self::BALANCE_CACHE_TTL_SECS)
                })
                .unwrap_or(false);
            if !positions_empty || has_ws_balance {
                return Some(BalanceResponse {
                    equity: Decimal::ZERO,
                    balance: Decimal::ZERO,
                    position_entry_price: None,
                    position_sign: None,
                });
            }
            None
        } else {
            state.balance.as_ref().and_then(|(balance, fetched_at)| {
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

    pub(super) async fn get_order_book_details(
        &self,
    ) -> Result<LighterOrderBookDetailsResponse, DexError> {
        let url = format!("{}/api/v1/orderBookDetails", self.base_url);
        let response_text = self
            .fetch_text_with_waf_guard(&url, "orderBookDetails")
            .await?;
        log::trace!("Order book details response: {}", response_text);
        serde_json::from_str(&response_text)
            .map_err(|e| DexError::Transient(format!("Failed to parse order book details: {}", e)))
    }

    pub(super) async fn get_order_books_all(&self) -> Result<LighterOrderBooksResponse, DexError> {
        let url = format!("{}/api/v1/orderBooks?filter=all", self.base_url);
        let response_text = self.fetch_text_with_waf_guard(&url, "orderBooks").await?;
        serde_json::from_str(&response_text)
            .map_err(|e| DexError::Transient(format!("Failed to parse orderBooks: {}", e)))
    }

    pub(super) async fn get_funding_rates(&self) -> Result<LighterFundingRates, DexError> {
        let url = format!("{}/api/v1/funding-rates", self.base_url);
        let response_text = self
            .fetch_text_with_waf_guard(&url, "funding-rates")
            .await?;
        log::trace!("Funding rates response: {}", response_text);
        serde_json::from_str(&response_text)
            .map_err(|e| DexError::Transient(format!("Failed to parse funding rates: {}", e)))
    }
}

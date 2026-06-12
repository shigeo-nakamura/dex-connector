//! Authentication, account discovery, API-key registration, and the
//! cached `/account` balance helpers for `LighterConnector`.
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

    /// Initialize Go client (disabled when lighter-sdk feature is not enabled)
    #[cfg(not(feature = "lighter-sdk"))]
    pub(super) async fn create_go_client(&self) -> Result<(), DexError> {
        Err(DexError::Transient(
            "Lighter Go SDK not available. Build with --features lighter-sdk to enable."
                .to_string(),
        ))
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

/// Account-state read helpers backing the `DexConnector` balance / position
/// trait methods. Extracted from `dex_impl.rs` (bot-strategy#501 item 1):
/// `get_balance` / `get_combined_balance` / `get_positions` now delegate
/// into these. Behaviour is unchanged from the prior in-`dex_impl.rs`
/// implementations.
impl LighterConnector {
    pub(super) async fn fetch_balance(
        &self,
        symbol: Option<&str>,
    ) -> Result<BalanceResponse, DexError> {
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
        log::info!("get_balance called for symbol: {:?}", symbol);

        let account = self.fetch_account_via_rest().await?;
        let account = &account;

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
        //
        // bot-strategy#392: balance + collateral now share a single
        // RwLock so the seed is atomic — readers cannot observe the new
        // balance against a stale collateral.
        let collateral_seed = account
            .assets
            .iter()
            .find(|a| a.symbol == "USDC")
            .and_then(|a| string_to_decimal(Some(a.margin_balance.clone())).ok());
        {
            let mut state = self.account_state.write().await;
            state.balance = Some((response.clone(), Instant::now()));
            // Seed collateral from REST so subsequent WS-only updates can
            // recompute equity = collateral + sum(unrealized_pnl) without
            // another REST. Use assets[USDC].margin_balance when present;
            // matches `total_asset_value` minus sum(positions.unrealized_pnl).
            // See bot-strategy#239.
            if let Some(usdc) = collateral_seed {
                state.collateral = Some(usdc);
            }
        }

        Ok(response)
    }

    pub(super) async fn fetch_combined_balance(&self) -> Result<CombinedBalanceResponse, DexError> {
        let cached = {
            let state = self.account_state.read().await;
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

        log::info!("get_combined_balance called");

        let account = self.fetch_account_via_rest().await?;
        let account = &account;

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

    pub(super) async fn fetch_positions(&self) -> Result<Vec<PositionSnapshot>, DexError> {
        if !self.positions_ready.load(Ordering::SeqCst) {
            return Err(DexError::Transient(
                "positions not ready from websocket".to_string(),
            ));
        }
        let state = self.account_state.read().await;
        Ok(state.positions.clone())
    }
}

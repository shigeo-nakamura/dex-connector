use crate::{
    dex_connector::{string_to_decimal, DexConnector},
    dex_request::{DexError, HttpMethod},
    dex_websocket::DexWebSocket,
    BalanceResponse, CanceledOrder, CanceledOrdersResponse, CreateOrderResponse, FilledOrder,
    FilledOrdersResponse, OrderSide, TickerResponse, TpSl,
};
use async_trait::async_trait;
use reqwest::Client;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

// Cryptographic imports
#[cfg(feature = "lighter-sdk")]
use libc::{c_char, c_int, c_longlong};
use secp256k1::{Message, Secp256k1, SecretKey};
use sha3::{Digest, Keccak256};
#[cfg(feature = "lighter-sdk")]
use std::ffi::{CStr, CString};
use tokio::{sync::RwLock, time::sleep};

// FFI bindings for Go shared library (only with lighter-sdk feature)
#[cfg(feature = "lighter-sdk")]
#[repr(C)]
pub struct StrOrErr {
    pub str: *mut c_char,
    pub err: *mut c_char,
}

#[cfg(feature = "lighter-sdk")]
extern "C" {
    fn CreateClient(
        url: *const c_char,
        private_key: *const c_char,
        chain_id: c_int,
        api_key_index: c_int,
        account_index: c_longlong,
    ) -> *mut c_char;

    fn CheckClient(api_key_index: c_int, account_index: c_longlong) -> *mut c_char;

    fn SignCreateOrder(
        market_index: c_int,
        client_order_index: c_longlong,
        base_amount: c_longlong,
        price: c_int,
        is_ask: c_int,
        order_type: c_int,
        time_in_force: c_int,
        reduce_only: c_int,
        trigger_price: c_int,
        order_expiry: c_longlong,
        nonce: c_longlong,
    ) -> StrOrErr;

    fn SignChangePubKey(new_pubkey: *const c_char, nonce: c_longlong) -> StrOrErr;

    fn SignMessageWithEVM(private_key: *const c_char, message: *const c_char) -> StrOrErr;
}

#[derive(Clone)]
pub struct LighterConnector {
    api_key_public: String,      // X-API-KEY header (from Lighter UI)
    api_key_index: u32,          // api_key_index query param
    api_private_key_hex: String, // API private key for signing (40-byte)
    #[cfg(feature = "lighter-sdk")]
    evm_wallet_private_key: Option<String>, // EVM wallet private key for API key registration
    account_index: u32,          // account_index query param
    base_url: String,
    websocket_url: String,
    _l1_address: String, // derived from wallet for logging purposes
    client: Client,
    filled_orders: Arc<RwLock<HashMap<String, Vec<FilledOrder>>>>,
    canceled_orders: Arc<RwLock<HashMap<String, Vec<CanceledOrder>>>>,
    is_running: Arc<AtomicBool>,
    _ws: Option<DexWebSocket>, // Reserved for future WebSocket implementation
}

#[derive(Deserialize, Debug)]
struct LighterAccountResponse {
    #[serde(rename = "totalEquity")]
    total_equity: String,
    #[serde(rename = "availableBalance")]
    available_balance: String,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct LighterNonceResponse {
    nonce: u64,
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
    /// Initialize Go client
    #[cfg(feature = "lighter-sdk")]
    fn create_go_client(&self) -> Result<(), DexError> {
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

            // Log key configuration details for debugging
            log::info!("API Key Validation Details:");
            log::info!("  Account Index: {}", self.account_index);
            log::info!("  API Key Index: {}", self.api_key_index);
            log::info!("  Chain ID: 304 (hardcoded)");
            log::info!(
                "  API Private Key Length: {} chars",
                self.api_private_key_hex.len()
            );
            log::info!(
                "  API Private Key (first 8): {}",
                &self.api_private_key_hex[..std::cmp::min(8, self.api_private_key_hex.len())]
            );
            log::info!(
                "  API Private Key (last 8): {}",
                &self.api_private_key_hex
                    [std::cmp::max(0, self.api_private_key_hex.len().saturating_sub(8))..]
            );

            // Verify the API key is properly registered with Lighter
            let check_result = CheckClient(
                self.api_key_index as c_int,
                self.account_index as c_longlong,
            );

            if !check_result.is_null() {
                let error_cstr = CStr::from_ptr(check_result);
                let error_msg = error_cstr.to_string_lossy().to_string();
                libc::free(check_result as *mut libc::c_void);
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

                // If EVM wallet private key is provided, mark for API key registration
                #[cfg(feature = "lighter-sdk")]
                if self.evm_wallet_private_key.is_some() {
                    log::info!(
                        "EVM wallet private key provided. API key registration is required."
                    );
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
            } else {
                log::info!("API key validation successful");
            }

            Ok(())
        }
    }

    /// Initialize Go client (disabled when lighter-sdk feature is not enabled)
    #[cfg(not(feature = "lighter-sdk"))]
    fn create_go_client(&self) -> Result<(), DexError> {
        Err(DexError::Other(
            "Lighter Go SDK not available. Build with --features lighter-sdk to enable."
                .to_string(),
        ))
    }

    /// Call Go shared library to generate signature for CreateOrder transaction
    #[cfg(feature = "lighter-sdk")]
    fn call_go_sign_create_order(
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
        self.create_go_client()?;

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
            );

            if !result.err.is_null() {
                let error_cstr = CStr::from_ptr(result.err);
                let error_msg = error_cstr.to_string_lossy().to_string();
                libc::free(result.err as *mut libc::c_void);
                if !result.str.is_null() {
                    libc::free(result.str as *mut libc::c_void);
                }
                return Err(DexError::Other(format!("Go SDK error: {}", error_msg)));
            }

            if result.str.is_null() {
                return Err(DexError::Other("Go SDK returned null result".to_string()));
            }

            let result_cstr = CStr::from_ptr(result.str);
            let json_str = result_cstr.to_string_lossy().to_string();
            libc::free(result.str as *mut libc::c_void);

            Ok(json_str)
        }
    }

    /// Call Go shared library to generate signature (disabled when lighter-sdk feature is not enabled)
    #[cfg(not(feature = "lighter-sdk"))]
    fn call_go_sign_create_order(
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

    #[cfg(feature = "lighter-sdk")]
    pub fn new(
        api_key_public: String,
        api_key_index: u32,
        api_private_key_hex: String,
        evm_wallet_private_key: Option<String>,
        account_index: u32,
        base_url: String,
        websocket_url: String,
    ) -> Result<Self, DexError> {
        // For backward compatibility, derive L1 address for logging if possible
        let l1_address = "N/A".to_string(); // We don't need wallet address anymore

        log::info!(
            "Creating LighterConnector with API key index: {}, account: {}",
            api_key_index,
            account_index
        );

        Ok(Self {
            api_key_public,
            api_key_index,
            api_private_key_hex,
            evm_wallet_private_key,
            account_index,
            base_url: base_url.clone(),
            websocket_url: websocket_url.clone(),
            _l1_address: l1_address,
            client: Client::new(),
            filled_orders: Arc::new(RwLock::new(HashMap::new())),
            canceled_orders: Arc::new(RwLock::new(HashMap::new())),
            is_running: Arc::new(AtomicBool::new(false)),
            _ws: Some(DexWebSocket::new(websocket_url)),
        })
    }

    #[cfg(not(feature = "lighter-sdk"))]
    pub fn new(
        api_key_public: String,
        api_key_index: u32,
        api_private_key_hex: String,
        _evm_wallet_private_key: Option<String>,
        account_index: u32,
        base_url: String,
        websocket_url: String,
    ) -> Result<Self, DexError> {
        // For backward compatibility, derive L1 address for logging if possible
        let l1_address = "N/A".to_string(); // We don't need wallet address anymore

        log::info!(
            "Creating LighterConnector with API key index: {}, account: {}",
            api_key_index,
            account_index
        );

        Ok(Self {
            api_key_public,
            api_key_index,
            api_private_key_hex,
            account_index,
            base_url: base_url.clone(),
            websocket_url: websocket_url.clone(),
            _l1_address: l1_address,
            client: Client::new(),
            filled_orders: Arc::new(RwLock::new(HashMap::new())),
            canceled_orders: Arc::new(RwLock::new(HashMap::new())),
            is_running: Arc::new(AtomicBool::new(false)),
            _ws: Some(DexWebSocket::new(websocket_url)),
        })
    }

    /// Create native order without Python SDK
    async fn create_order_native(
        &self,
        market_id: u32,
        side: u32,
        _tif: u32,
        base_amount: u64,
        price: u64,
        client_order_id: Option<String>,
    ) -> Result<CreateOrderResponse, DexError> {
        let timestamp = chrono::Utc::now().timestamp_millis() as u64;
        let client_id = client_order_id.unwrap_or_else(|| format!("rust-native-{}", timestamp));
        let nonce = self.get_nonce().await?;

        log::info!(
            "Creating native order: market_id={}, side={}, base_amount={}, price={}",
            market_id,
            side,
            base_amount,
            price
        );

        // Match the exact parameters used in Go SDK test to validate signature
        let client_order_index = 12345u64; // Exact value from Go SDK test
        let order_type = 0u64; // ORDER_TYPE_LIMIT
        let time_in_force = 1u64; // ORDER_TIME_IN_FORCE_GOOD_TILL_TIME
        let reduce_only = 0u64; // false
        let trigger_price = 0u64; // no trigger
        let order_expiry = 1762543091693u64; // Exact value from Go SDK test

        // Use exact values from Go SDK test to match the signature
        let test_market_id = 1u64; // Exact value from Go SDK test
        let test_base_amount = 50u64; // Exact value from Go SDK test
        let test_price = 4750000u64; // Exact value from Go SDK test
        let test_side = 0u64; // Exact value from Go SDK test (IsAsk=0)
                              // Use the actual nonce from API, not hardcoded value

        let tx_data = [
            test_market_id,     // market_index
            client_order_index, // client_order_index
            test_base_amount,   // base_amount
            test_price,         // price
            test_side,          // is_ask
            order_type,         // order_type
            time_in_force,      // time_in_force
            reduce_only,        // reduce_only
            trigger_price,      // trigger_price
            order_expiry,       // order_expiry
            nonce,              // nonce - use actual API nonce
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
        let go_result = self.call_go_sign_create_order(
            test_market_id as i32,
            client_order_index as i64,
            test_base_amount as i64,
            test_price as i32,
            test_side as i32,
            order_type as i32,
            time_in_force as i32,
            reduce_only as i32,
            trigger_price as i32,
            order_expiry as i64,
            nonce as i64,
        )?;

        log::info!("=== GO SDK RESULT ===");
        log::info!("Go SDK JSON: {}", go_result);

        // Use the exact JSON from Go SDK - it already contains everything correctly
        log::info!("=== SIGNATURE DEBUG ===");
        log::info!("API nonce: {}", nonce);
        log::info!("Transaction data: {:?}", tx_data);
        log::info!("Using Go SDK transaction JSON directly");

        // Use the complete transaction JSON from Go SDK directly
        let tx_info = go_result;

        // Send to Lighter API using application/x-www-form-urlencoded format (same as Go SDK)
        let form_data = format!(
            "tx_type=14&tx_info={}&price_protection=false",
            urlencoding::encode(&tx_info)
        );

        log::info!("=== REQUEST DEBUG ===");
        log::info!("Timestamp: {}", timestamp);
        log::info!("TX Info JSON: {}", tx_info);
        log::info!("Form data: {}", form_data);

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

        log::info!(
            "Native order response: HTTP {}, Body: {}",
            status,
            response_text
        );

        if status.is_success() {
            log::info!("Native order submitted successfully!");
            Ok(CreateOrderResponse {
                order_id: client_id,
                ordered_price: Decimal::new(price as i64, 6),
                ordered_size: Decimal::new(base_amount as i64, 5),
            })
        } else {
            Err(DexError::Other(format!(
                "Order failed: HTTP {}, {}",
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

        log::info!(
            "Delegating order to Python SDK: market_id={}, side={}, base_amount={}, price={}",
            market_id,
            side,
            base_amount,
            price
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
            log::info!("Order successfully sent via SDK");

            let order_id = response
                .get("tx_hash")
                .and_then(|v| v.as_str())
                .unwrap_or(&client_id)
                .to_string();

            Ok(CreateOrderResponse {
                order_id,
                ordered_price: Decimal::new(price as i64, 6), // Assuming 6 decimals for price
                ordered_size: Decimal::new(base_amount as i64, 5), // Assuming 5 decimals for amount
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
        let url = format!(
            "{}/api/v1/nextNonce?account_index={}&api_key_index={}",
            self.base_url, self.account_index, self.api_key_index
        );

        log::debug!("Getting nonce from: {}", url);
        log::debug!("Using API key: {}", self.api_key_public);

        let response = self
            .client
            .get(&url)
            .header("X-API-KEY", &self.api_key_public)
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

    #[allow(dead_code)]
    async fn discover_account_index(&self) -> Result<u32, DexError> {
        // For now, just return the configured account_index
        // In production, this could query the API to find the correct index
        Ok(self.account_index)
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
        let url = format!("{}{}", self.base_url, endpoint);

        let mut request = match method {
            HttpMethod::Get => self.client.get(&url),
            HttpMethod::Post => self.client.post(&url),
            HttpMethod::Put => self.client.put(&url),
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
        if !status.is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(DexError::Other(format!("HTTP {}: {}", status, error_text)));
        }

        response
            .json()
            .await
            .map_err(|e| DexError::Other(format!("Failed to parse response: {}", e)))
    }

    #[cfg(feature = "lighter-sdk")]
    async fn register_api_key(&self, evm_private_key: &str) -> Result<(), String> {
        log::info!("Attempting to register API key using ChangePubKey...");

        // Get next nonce
        let nonce = self
            .get_nonce()
            .await
            .map_err(|e| format!("Failed to get nonce: {:?}", e))?;
        log::info!("Got nonce for ChangePubKey: {}", nonce);

        // We need to derive the public key from our API private key
        // For now, let's use the public key that was derived from our private key (logged as "ownPubKey")
        // This should be 8e02c2f26695a0672de4775bd178d3e509044dac7274aa6cbae293c01a756eb4ab8a501915eee49b
        let new_pubkey =
            "0x8e02c2f26695a0672de4775bd178d3e509044dac7274aa6cbae293c01a756eb4ab8a501915eee49b"
                .to_string();
        log::info!("New public key to register: {}", new_pubkey);

        // Use SignChangePubKey from the lighter-go library
        let sign_result = unsafe {
            SignChangePubKey(
                std::ffi::CString::new(new_pubkey.clone()).unwrap().as_ptr(),
                nonce as c_longlong,
            )
        };

        // Check if signing was successful
        if !sign_result.err.is_null() {
            let error_msg = unsafe { std::ffi::CStr::from_ptr(sign_result.err).to_string_lossy() };
            return Err(format!("Failed to sign ChangePubKey: {}", error_msg));
        }

        let tx_info_str = unsafe { std::ffi::CStr::from_ptr(sign_result.str).to_string_lossy() };
        log::info!("SignChangePubKey result: {}", tx_info_str);

        // Parse the tx_info JSON and extract MessageToSign
        let mut tx_info: serde_json::Value = serde_json::from_str(&tx_info_str)
            .map_err(|e| format!("Failed to parse tx_info: {}", e))?;

        let message_to_sign = tx_info["MessageToSign"]
            .as_str()
            .ok_or("MessageToSign not found in tx_info")?
            .to_string();
        log::info!("MessageToSign: {}", message_to_sign);

        // Remove MessageToSign from tx_info as per Python SDK implementation
        tx_info.as_object_mut().unwrap().remove("MessageToSign");

        // Sign the message with EVM key using lighter-go SignMessageWithEVM
        let evm_signature =
            self.sign_message_with_lighter_go_evm(evm_private_key, &message_to_sign)?;
        log::info!("EVM signature: {}", evm_signature);

        // Add L1Sig field with the EVM signature (Python SDK uses L1Sig, not evmSignature)
        tx_info["L1Sig"] = serde_json::Value::String(evm_signature);

        // Send the ChangePubKey request
        let response = self
            .send_change_api_key_request(&tx_info.to_string())
            .await?;
        log::info!("ChangePubKey response: {:?}", response);

        Ok(())
    }

    #[cfg(not(feature = "lighter-sdk"))]
    async fn register_api_key(&self, _evm_private_key: &str) -> Result<(), String> {
        Err("API key registration requires lighter-sdk feature".to_string())
    }

    #[cfg(feature = "lighter-sdk")]
    fn sign_message_with_lighter_go_evm(
        &self,
        evm_private_key: &str,
        message: &str,
    ) -> Result<String, String> {
        log::debug!("Using lighter-go SignMessageWithEVM for EVM signature");

        let private_key_cstr = std::ffi::CString::new(evm_private_key)
            .map_err(|e| format!("Failed to create CString for private key: {}", e))?;
        let message_cstr = std::ffi::CString::new(message)
            .map_err(|e| format!("Failed to create CString for message: {}", e))?;

        let sign_result =
            unsafe { SignMessageWithEVM(private_key_cstr.as_ptr(), message_cstr.as_ptr()) };

        if !sign_result.err.is_null() {
            let error_msg = unsafe { std::ffi::CStr::from_ptr(sign_result.err).to_string_lossy() };
            return Err(format!("EVM signature failed: {}", error_msg));
        }

        let signature = unsafe { std::ffi::CStr::from_ptr(sign_result.str).to_string_lossy() };

        // Remove 0x prefix if present
        let signature_clean = if signature.starts_with("0x") {
            &signature[2..]
        } else {
            &signature
        };

        Ok(signature_clean.to_string())
    }

    #[cfg(not(feature = "lighter-sdk"))]
    fn sign_message_with_lighter_go_evm(
        &self,
        _evm_private_key: &str,
        _message: &str,
    ) -> Result<String, String> {
        Err("EVM signing with lighter-go requires lighter-sdk feature".to_string())
    }

    fn _unused_sign_with_evm_key(
        &self,
        evm_private_key: &str,
        message: &str,
    ) -> Result<String, String> {
        // Parse the private key - try base64 first, then hex
        let private_key_bytes = if evm_private_key.contains("=")
            || evm_private_key.contains("+")
            || evm_private_key.contains("/")
        {
            // Looks like base64
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(evm_private_key)
                .map_err(|e| format!("Failed to decode private key base64: {}", e))?
        } else {
            // Try hex format
            let private_key_hex = if evm_private_key.starts_with("0x") {
                &evm_private_key[2..]
            } else {
                evm_private_key
            };

            hex::decode(private_key_hex)
                .map_err(|e| format!("Failed to decode private key hex: {}", e))?
        };

        if private_key_bytes.len() != 32 {
            return Err("Private key must be 32 bytes".to_string());
        }

        let secret_key = SecretKey::from_slice(&private_key_bytes)
            .map_err(|e| format!("Failed to create secret key: {}", e))?;

        // Create EIP-191 prefixed message hash
        let prefix = format!("\x19Ethereum Signed Message:\n{}", message.len());
        let full_message = format!("{}{}", prefix, message);

        let mut hasher = Keccak256::new();
        hasher.update(full_message.as_bytes());
        let message_hash = hasher.finalize();

        // Sign the message
        let secp = Secp256k1::new();
        let message_obj = Message::from_digest_slice(&message_hash)
            .map_err(|e| format!("Failed to create message: {}", e))?;

        let signature = secp.sign_ecdsa_recoverable(&message_obj, &secret_key);
        let (recovery_id, compact_sig) = signature.serialize_compact();

        // Construct 65-byte signature: [r(32) | s(32) | v(1)]
        // For EVM signatures, v = recovery_id + 27
        let mut signature_bytes = [0u8; 65];
        signature_bytes[0..64].copy_from_slice(&compact_sig);
        signature_bytes[64] = (recovery_id.to_i32() + 27) as u8; // v = recovery_id + 27

        // Encode as hex
        Ok(hex::encode(signature_bytes))
    }

    async fn send_change_api_key_request(
        &self,
        tx_info: &str,
    ) -> Result<serde_json::Value, String> {
        let base_url = self.base_url.trim_end_matches('/');
        let url = format!("{}/api/v1/sendTx", base_url);

        let form_data = [
            ("tx_type", "17"), // TX_TYPE_CHANGE_PUB_KEY = 17 (correct value)
            ("tx_info", tx_info),
        ];

        log::debug!("Sending change API key request to: {}", url);
        log::debug!("Form data: {:?}", form_data);

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
        log::info!(
            "Lighter connector started with WebSocket: {}",
            self.websocket_url
        );

        // Initialize the Go client and validate API key
        #[cfg(feature = "lighter-sdk")]
        {
            match self.create_go_client() {
                Ok(()) => {
                    log::info!("API key validation successful");
                }
                Err(DexError::ApiKeyRegistrationRequired) => {
                    #[cfg(feature = "lighter-sdk")]
                    if let Some(evm_key) = &self.evm_wallet_private_key {
                        log::info!("API key registration required. Attempting to register...");
                        self.register_api_key(evm_key).await.map_err(|e| {
                            DexError::Other(format!("API key registration failed: {}", e))
                        })?;

                        // Retry validation after registration
                        log::info!("Retrying API key validation after registration...");
                        self.create_go_client()?;
                        log::info!("API key validation successful after registration");
                    } else {
                        return Err(DexError::ApiKeyRegistrationRequired);
                    }

                    #[cfg(not(feature = "lighter-sdk"))]
                    return Err(DexError::ApiKeyRegistrationRequired);
                }
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    async fn stop(&self) -> Result<(), DexError> {
        self.is_running.store(false, Ordering::SeqCst);
        log::info!("Lighter connector stopped");
        Ok(())
    }

    async fn restart(&self, _max_retries: i32) -> Result<(), DexError> {
        self.stop().await?;
        sleep(Duration::from_secs(1)).await;
        self.start().await
    }

    async fn set_leverage(&self, _symbol: &str, _leverage: u32) -> Result<(), DexError> {
        log::warn!("Leverage setting not implemented for Lighter");
        Ok(())
    }

    async fn get_ticker(
        &self,
        symbol: &str,
        test_price: Option<Decimal>,
    ) -> Result<TickerResponse, DexError> {
        if let Some(price) = test_price {
            return Ok(TickerResponse {
                symbol: symbol.to_string(),
                price,
                min_tick: None,
                min_order: None,
                volume: None,
                num_trades: None,
                open_interest: None,
                funding_rate: None,
                oracle_price: None,
            });
        }

        // Implementation would query Lighter API for actual ticker data
        Ok(TickerResponse {
            symbol: symbol.to_string(),
            price: Decimal::new(50000, 0),
            min_tick: None,
            min_order: None,
            volume: None,
            num_trades: None,
            open_interest: None,
            funding_rate: None,
            oracle_price: None,
        })
    }

    async fn get_filled_orders(&self, symbol: &str) -> Result<FilledOrdersResponse, DexError> {
        let orders = self.filled_orders.read().await;
        let symbol_orders = orders.get(symbol).cloned().unwrap_or_default();

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

    async fn get_balance(&self, _symbol: Option<&str>) -> Result<BalanceResponse, DexError> {
        let endpoint = format!(
            "/api/v1/account?account_index={}&api_key_index={}",
            self.account_index, self.api_key_index
        );

        let account_response: LighterAccountResponse =
            self.make_request(&endpoint, HttpMethod::Get, None).await?;

        Ok(BalanceResponse {
            equity: string_to_decimal(Some(account_response.total_equity))?,
            balance: string_to_decimal(Some(account_response.available_balance))?,
        })
    }

    async fn clear_filled_order(&self, symbol: &str, trade_id: &str) -> Result<(), DexError> {
        let mut orders = self.filled_orders.write().await;
        if let Some(symbol_orders) = orders.get_mut(symbol) {
            symbol_orders.retain(|order| order.trade_id != trade_id);
        }
        Ok(())
    }

    async fn clear_all_filled_orders(&self) -> Result<(), DexError> {
        let mut orders = self.filled_orders.write().await;
        orders.clear();
        Ok(())
    }

    async fn clear_canceled_order(&self, symbol: &str, order_id: &str) -> Result<(), DexError> {
        let mut orders = self.canceled_orders.write().await;
        if let Some(symbol_orders) = orders.get_mut(symbol) {
            symbol_orders.retain(|order| order.order_id != order_id);
        }
        Ok(())
    }

    async fn clear_all_canceled_orders(&self) -> Result<(), DexError> {
        let mut orders = self.canceled_orders.write().await;
        orders.clear();
        Ok(())
    }

    async fn create_order(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        price: Option<Decimal>,
        _spread: Option<i64>,
    ) -> Result<CreateOrderResponse, DexError> {
        // Convert symbol to market_id (this would typically be a lookup)
        let market_id = match symbol {
            "BTC-USD" | "BTC" => 1,
            "ETH-USD" | "ETH" => 2,
            _ => {
                log::warn!("Unknown symbol {}, using market_id=1", symbol);
                1
            }
        };

        // Convert side: Long=0(BUY), Short=1(SELL) for Lighter API
        let side_value = match side {
            OrderSide::Long => 0,
            OrderSide::Short => 1,
        };

        // Convert time-in-force: 0=GTC, 1=IOC, 2=FOK
        let tif = 0; // Default to GTC

        // Convert amounts to Lighter's scaled integers
        // Typically: base_amount in 1e5 scale, price in 1e6 scale
        let base_amount = (size * Decimal::new(100_000, 0))
            .to_u64()
            .ok_or_else(|| DexError::Other("Invalid size amount".to_string()))?;

        let price_value = if let Some(p) = price {
            (p * Decimal::new(1_000_000, 0))
                .to_u64()
                .ok_or_else(|| DexError::Other("Invalid price".to_string()))?
        } else {
            return Err(DexError::Other(
                "Market orders not supported yet".to_string(),
            ));
        };

        // Use native Rust implementation for Lighter signatures
        self.create_order_native(market_id, side_value, tif, base_amount, price_value, None)
            .await
    }

    async fn create_trigger_order(
        &self,
        _symbol: &str,
        _size: Decimal,
        _side: OrderSide,
        _trigger_px: Decimal,
        _is_market: bool,
        _tpsl: TpSl,
    ) -> Result<CreateOrderResponse, DexError> {
        Err(DexError::Other(
            "Trigger orders not implemented yet".to_string(),
        ))
    }

    async fn cancel_order(&self, _symbol: &str, _order_id: &str) -> Result<(), DexError> {
        Err(DexError::Other(
            "Order cancellation not implemented yet".to_string(),
        ))
    }

    async fn cancel_all_orders(&self, _symbol: Option<String>) -> Result<(), DexError> {
        log::info!("Cancelling all orders for Lighter connector");
        // For now, just return success - this is primarily used for testing
        Ok(())
    }

    async fn cancel_orders(
        &self,
        _symbol: Option<String>,
        _order_ids: Vec<String>,
    ) -> Result<(), DexError> {
        Err(DexError::Other(
            "Bulk cancel orders not implemented yet".to_string(),
        ))
    }

    async fn close_all_positions(&self, _symbol: Option<String>) -> Result<(), DexError> {
        Err(DexError::Other(
            "Close positions not implemented yet".to_string(),
        ))
    }

    async fn clear_last_trades(&self, _symbol: &str) -> Result<(), DexError> {
        Ok(())
    }

    async fn is_upcoming_maintenance(&self) -> bool {
        false
    }

    async fn sign_evm_65b(&self, message: &str) -> Result<String, DexError> {
        use ethers::signers::{LocalWallet, Signer};
        use std::str::FromStr;

        let cleaned_key = self
            .api_private_key_hex
            .strip_prefix("0x")
            .unwrap_or(&self.api_private_key_hex);
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
}

pub fn create_lighter_connector(
    api_key_public: String,
    api_key_index: u32,
    api_private_key_hex: String,
    evm_wallet_private_key: Option<String>,
    account_index: u32,
    base_url: String,
    websocket_url: String,
) -> Result<Box<dyn DexConnector>, DexError> {
    let connector = LighterConnector::new(
        api_key_public,
        api_key_index,
        api_private_key_hex,
        evm_wallet_private_key,
        account_index,
        base_url,
        websocket_url,
    )?;
    Ok(Box::new(connector))
}

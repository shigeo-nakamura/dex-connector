#![cfg(feature = "lighter-sdk")]

use crate::{
    dex_connector::{string_to_decimal, DexConnector},
    dex_request::{DexError, HttpMethod},
    dex_websocket::DexWebSocket,
    BalanceResponse, CanceledOrder, CanceledOrdersResponse, CreateOrderResponse, FilledOrder,
    FilledOrdersResponse, LastTrade, LastTradesResponse, OrderSide, TickerResponse, TpSl,
};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    str::FromStr,
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
use tokio_tungstenite;

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

    fn GetClientPubKey(api_key_index: c_int, account_index: c_longlong) -> *mut c_char;

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

    fn SignCancelAllOrders(time_in_force: c_int, time: c_longlong, nonce: c_longlong) -> StrOrErr;

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
    // WebSocket data storage
    current_price: Arc<RwLock<Option<(Decimal, u64)>>>, // (price, timestamp)
    current_volume: Arc<RwLock<Option<Decimal>>>,
    order_book: Arc<RwLock<Option<LighterOrderBook>>>,
}

#[derive(Deserialize, Debug, Clone)]
struct LighterOrderBook {
    bids: Vec<LighterOrderBookEntry>,
    asks: Vec<LighterOrderBookEntry>,
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
    available_balance: String,
    collateral: String,
    total_asset_value: String,
    positions: Vec<LighterPosition>,
}

#[derive(Deserialize, Debug)]
struct LighterPosition {
    market_id: u8,
    symbol: String,
    position: String,
    sign: i8,
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
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct LighterExchangeStats {
    code: i32,
    order_book_stats: Vec<LighterOrderBookStats>,
    daily_usd_volume: f64,
    daily_trades_count: u32,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct LighterOrderBookStats {
    symbol: String,
    last_trade_price: f64,
    daily_trades_count: u32,
    daily_base_token_volume: f64,
    daily_quote_token_volume: f64,
    daily_price_change: f64,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct LighterFundingRates {
    code: i32,
    funding_rates: Vec<LighterFundingRate>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct LighterFundingRate {
    market_id: u32,
    exchange: String,
    symbol: String,
    rate: f64,
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
    account_index: u32,
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

            // Get the correct public key from the server
            let server_pubkey = match self.get_server_public_key().await {
                Ok(pubkey) => pubkey,
                Err(e) => {
                    log::error!("Failed to get server public key: {}", e);
                    return Err(e);
                }
            };

            // Get the public key derived by the Go shared library
            let go_pubkey_result = GetClientPubKey(
                self.api_key_index as c_int,
                self.account_index as c_longlong,
            );

            let go_derived_pubkey = if !go_pubkey_result.is_null() {
                let pubkey_cstr = CStr::from_ptr(go_pubkey_result);
                let pubkey_str = pubkey_cstr.to_string_lossy().to_string();
                libc::free(go_pubkey_result as *mut libc::c_void);
                Some(pubkey_str)
            } else {
                log::error!("Failed to get public key from Go client");
                None
            };

            // Compare Go-derived key with server key
            if let Some(go_key) = &go_derived_pubkey {
                let srv = server_pubkey
                    .to_lowercase()
                    .trim_start_matches("0x")
                    .to_string();
                let loc = go_key.to_lowercase().trim_start_matches("0x").to_string();

                if loc != srv {
                    log::debug!(
                        "API key mismatch detected (account={}, index={}). server={}…{} vs local={}…{} — will attempt ChangePubKey",
                        self.account_index,
                        self.api_key_index,
                        &srv[..8], &srv[srv.len()-8..],
                        &loc[..8], &loc[loc.len()-8..]
                    );
                } else {
                }
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

                // If we have the Go-derived public key and EVM wallet key, try to update the API key
                #[cfg(feature = "lighter-sdk")]
                if let (Some(_), Some(_)) = (&go_derived_pubkey, &self.evm_wallet_private_key) {
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

            Ok(())
        }
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

        log::debug!(
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
            current_price: Arc::new(RwLock::new(None)),
            current_volume: Arc::new(RwLock::new(None)),
            order_book: Arc::new(RwLock::new(None)),
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

        log::debug!(
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
            current_price: Arc::new(RwLock::new(None)),
            current_volume: Arc::new(RwLock::new(None)),
            order_book: Arc::new(RwLock::new(None)),
        })
    }

    /// Create native order without Python SDK
    #[allow(dead_code)]
    async fn create_order_native(
        &self,
        market_id: u32,
        side: u32,
        tif: u32,
        base_amount: u64,
        price: u64,
        client_order_id: Option<String>,
    ) -> Result<CreateOrderResponse, DexError> {
        self.create_order_native_with_type(
            market_id,
            side,
            tif,
            base_amount,
            price,
            client_order_id,
            0,
            false,
        )
        .await
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
    ) -> Result<CreateOrderResponse, DexError> {
        let timestamp = chrono::Utc::now().timestamp_millis() as u64;
        let _client_id = client_order_id.unwrap_or_else(|| format!("rust-native-{}", timestamp));
        let nonce = self.get_nonce().await?;

        log::debug!(
            "Creating native order: market_id={}, side={}, base_amount={}, price={}, calculated_price_usd={:.1}",
            market_id,
            side,
            base_amount,
            price,
            price as f64 / 10.0
        );

        // Use timestamp as unique client_order_index instead of hardcoded value
        let client_order_index = timestamp; // Use unique timestamp for each order
        let order_type_param = order_type as u64; // Use passed order type
        let time_in_force = tif as u64; // Use passed time in force
        let reduce_only_param = if reduce_only { 1u64 } else { 0u64 };
        let trigger_price = 0u64; // no trigger
                                  // For market orders and IOC orders, use 0 as expiry. For GTC orders, use future timestamp
        let order_expiry = if order_type == 1 || tif == 0 {
            // ORDER_TYPE_MARKET or IOC orders
            0i64 // NilOrderExpiry for immediate-or-cancel orders
        } else {
            // For GTC limit orders, use current timestamp + 24 hours
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            now_ms + (24 * 60 * 60 * 1000) // 24 hours in milliseconds
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
                reduce_only as i32,
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
                Decimal::new(base_amount as i64, 5)
            );

            Ok(CreateOrderResponse {
                order_id,
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

        log::debug!(
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
            log::debug!("Order successfully sent via SDK");

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
        self.get_nonce_with_key(&self.api_key_public).await
    }

    async fn get_nonce_with_key(&self, api_key: &str) -> Result<u64, DexError> {
        let url = format!(
            "{}/api/v1/nextNonce?account_index={}&api_key_index={}",
            self.base_url, self.account_index, self.api_key_index
        );

        log::debug!("Getting nonce from: {}", url);
        log::debug!("Using API key: {}", api_key);

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

    #[allow(dead_code)]
    async fn discover_account_index(&self) -> Result<u32, DexError> {
        // For now, just return the configured account_index
        // In production, this could query the API to find the correct index
        Ok(self.account_index)
    }

    async fn get_server_public_key(&self) -> Result<String, DexError> {
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

        let server_pubkey = &response.api_keys[0].public_key;

        Ok(server_pubkey.clone())
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
            )
        };

        // Check if signing was successful
        if !sign_result.err.is_null() {
            let error_msg = unsafe { std::ffi::CStr::from_ptr(sign_result.err).to_string_lossy() };
            return Err(format!("Failed to sign ChangePubKey: {}", error_msg));
        }

        let tx_info_str = unsafe { std::ffi::CStr::from_ptr(sign_result.str).to_string_lossy() };
        log::debug!("SignChangePubKey result: {}", tx_info_str);

        // Parse the tx_info JSON and extract MessageToSign
        let mut tx_info: serde_json::Value = serde_json::from_str(&tx_info_str)
            .map_err(|e| format!("Failed to parse tx_info: {}", e))?;

        let message_to_sign = tx_info["MessageToSign"]
            .as_str()
            .ok_or("MessageToSign not found in tx_info")?
            .to_string();
        log::debug!("MessageToSign: {}", message_to_sign);

        // Remove MessageToSign from tx_info as per Python SDK implementation
        tx_info.as_object_mut().unwrap().remove("MessageToSign");

        // Sign the message with EVM key using lighter-go SignMessageWithEVM
        let evm_signature =
            self.sign_message_with_lighter_go_evm(evm_private_key, &message_to_sign)?;
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

        // Ensure 0x prefix is present (Lighter expects 0x-prefixed signatures)
        let signature_with_prefix = if signature.starts_with("0x") {
            signature.to_string()
        } else {
            format!("0x{}", signature)
        };

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
    fn sign_message_with_lighter_go_evm(
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
            ("tx_type", "8"), // TX_TYPE_CHANGE_PUB_KEY = 8 (correct value from Go SDK)
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
        log::debug!(
            "Lighter connector started with WebSocket: {}",
            self.websocket_url
        );

        // Initialize the Go client and validate API key
        #[cfg(feature = "lighter-sdk")]
        {
            match self.create_go_client().await {
                Ok(()) => {}
                Err(DexError::ApiKeyRegistrationRequired) => {
                    #[cfg(feature = "lighter-sdk")]
                    if let Some(evm_key) = &self.evm_wallet_private_key {
                        // Get Go-derived public key for registration
                        let go_pubkey_result = unsafe {
                            GetClientPubKey(
                                self.api_key_index as c_int,
                                self.account_index as c_longlong,
                            )
                        };

                        if !go_pubkey_result.is_null() {
                            let pubkey_cstr = unsafe { CStr::from_ptr(go_pubkey_result) };
                            let go_key = pubkey_cstr.to_string_lossy().to_string();
                            unsafe { libc::free(go_pubkey_result as *mut libc::c_void) };

                            log::debug!("API key registration required. Attempting to register...");

                            // Get server public key for ChangePubKey
                            let server_pubkey =
                                self.get_server_public_key().await.map_err(|e| {
                                    DexError::Other(format!(
                                        "Failed to get server public key: {}",
                                        e
                                    ))
                                })?;

                            self.register_api_key(evm_key, &go_key, &server_pubkey)
                                .await
                                .map_err(|e| {
                                    DexError::Other(format!("API key registration failed: {}", e))
                                })?;

                            // Retry validation after registration
                            self.create_go_client().await?;
                        } else {
                            log::error!("Failed to get Go-derived public key for registration");
                            return Err(DexError::Other(
                                "Cannot get Go-derived public key".to_string(),
                            ));
                        }
                    } else {
                        return Err(DexError::ApiKeyRegistrationRequired);
                    }

                    #[cfg(not(feature = "lighter-sdk"))]
                    return Err(DexError::ApiKeyRegistrationRequired);
                }
                Err(e) => return Err(e),
            }
        }

        // Start WebSocket connection
        self.start_websocket().await?;

        Ok(())
    }

    async fn stop(&self) -> Result<(), DexError> {
        self.is_running.store(false, Ordering::SeqCst);
        log::debug!("Lighter connector stopped");
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
            let min_tick = Self::calculate_min_tick(price, 3, false); // BTC perpetual with 3 decimals
            return Ok(TickerResponse {
                symbol: symbol.to_string(),
                price,
                min_tick: Some(min_tick),
                min_order: None,
                volume: Some(Decimal::ZERO),
                num_trades: None,
                open_interest: None,
                funding_rate: None,
                oracle_price: None,
            });
        }

        // Get statistics data from API
        let stats_data = self.get_exchange_stats().await.ok();
        let funding_data = self.get_funding_rates().await.ok();

        // Try to get price from WebSocket first, but check if it's recent
        if let Some((ws_price, price_timestamp)) = *self.current_price.read().await {
            let current_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            // Check if WebSocket price is stale (older than 30 seconds)
            let price_age = current_time.saturating_sub(price_timestamp);
            if price_age > 30 {
                log::warn!(
                    "WebSocket price is stale ({}s old), falling back to REST API",
                    price_age
                );
                // Fall through to REST API fallback below
            } else {
                let min_tick = Self::calculate_min_tick(ws_price, 3, false);

                let (volume, num_trades) = if let Some(stats) = &stats_data {
                    if let Some(btc_stats) = stats
                        .order_book_stats
                        .iter()
                        .find(|s| s.symbol == "BTC" || s.symbol == "BTCUSD")
                    {
                        (
                            Some(
                                Decimal::from_f64_retain(btc_stats.daily_base_token_volume)
                                    .unwrap_or(Decimal::ZERO),
                            ),
                            Some(btc_stats.daily_trades_count as u64),
                        )
                    } else {
                        (Some(Decimal::ZERO), None)
                    }
                } else {
                    (Some(Decimal::ZERO), None)
                };

                let funding_rate = if let Some(funding) = &funding_data {
                    funding
                        .funding_rates
                        .iter()
                        .find(|f| f.symbol == "BTC" || f.symbol == "BTCUSD")
                        .and_then(|f| Decimal::from_f64_retain(f.rate))
                } else {
                    None
                };

                log::debug!(
                    "Using WebSocket price with API stats: price={}, volume={:?}, trades={:?}",
                    ws_price,
                    volume,
                    num_trades
                );

                return Ok(TickerResponse {
                    symbol: symbol.to_string(),
                    price: ws_price,
                    min_tick: Some(min_tick),
                    min_order: None,
                    volume,
                    num_trades,
                    open_interest: None,
                    funding_rate,
                    oracle_price: None,
                });
            }
        }

        // Fallback to REST API if WebSocket data is not available
        log::warn!("WebSocket data not available, falling back to REST API");

        // Get market_id for the symbol
        let market_id = match symbol {
            "BTC" => 1,
            _ => return Err(DexError::Other(format!("Unknown symbol: {}", symbol))),
        };

        // Query recent trades to get current price and volume
        let endpoint = format!("/api/v1/recentTrades?market_id={}&limit=100", market_id);

        // Debug the raw response first
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

        log::debug!(
            "Trades API response (status: {}): {}",
            status,
            response_text
        );

        if !status.is_success() {
            return Err(DexError::Other(format!(
                "HTTP {}: {}",
                status, response_text
            )));
        }

        let trades_response: LighterTradesResponse = serde_json::from_str(&response_text)
            .map_err(|e| DexError::Other(format!("Failed to parse response: {}", e)))?;

        let price = if let Some(trade) = trades_response.trades.first() {
            string_to_decimal(Some(trade.price.clone()))?
        } else {
            // Fallback to default if no trades found
            Decimal::new(50000, 0)
        };

        let min_tick = Self::calculate_min_tick(price, 3, false); // BTC perpetual with 3 decimals

        // Get funding rate
        let funding_rate = if let Some(funding) = &funding_data {
            funding
                .funding_rates
                .iter()
                .find(|f| f.symbol == "BTC" || f.symbol == "BTCUSD")
                .and_then(|f| Decimal::from_f64_retain(f.rate))
        } else {
            None
        };

        // Use stats data if available, otherwise fallback to trades data
        let (volume, num_trades) = if let Some(stats) = &stats_data {
            if let Some(btc_stats) = stats
                .order_book_stats
                .iter()
                .find(|s| s.symbol == "BTC" || s.symbol == "BTCUSD")
            {
                (
                    Some(
                        Decimal::from_f64_retain(btc_stats.daily_base_token_volume)
                            .unwrap_or(Decimal::ZERO),
                    ),
                    Some(btc_stats.daily_trades_count as u64),
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
            min_order: None,
            volume,
            num_trades,
            open_interest: None,
            funding_rate,
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
        let endpoint = format!("/api/v1/account?by=index&value={}", self.account_index);

        // First, get the raw response text for debugging
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

        log::debug!(
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
        Ok(BalanceResponse {
            equity: string_to_decimal(Some(account.total_asset_value.clone()))?,
            balance: string_to_decimal(Some(account.available_balance.clone()))?,
        })
    }

    async fn get_last_trades(&self, symbol: &str) -> Result<LastTradesResponse, DexError> {
        // Get market_id for the symbol
        let market_id = match symbol {
            "BTC" => 1,
            _ => return Err(DexError::Other(format!("Unknown symbol: {}", symbol))),
        };

        // Query recent trades
        let endpoint = format!("/api/v1/recentTrades?market_id={}&limit=10", market_id);

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

        log::debug!(
            "Last trades API response (status: {}): {}",
            status,
            response_text
        );

        if !status.is_success() {
            return Err(DexError::Other(format!(
                "HTTP {}: {}",
                status, response_text
            )));
        }

        let trades_response: LighterTradesResponse = serde_json::from_str(&response_text)
            .map_err(|e| DexError::Other(format!("Failed to parse response: {}", e)))?;

        let trades = trades_response
            .trades
            .into_iter()
            .map(|t| LastTrade {
                price: string_to_decimal(Some(t.price)).unwrap_or_default(),
            })
            .collect();

        Ok(LastTradesResponse { trades })
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
        let _tif = 0; // Default to GTC

        // Convert amounts to Lighter's scaled integers
        // Base amount in 1e5 scale, price scaled to match Go SDK (much smaller scale)
        let base_amount = (size * Decimal::new(100_000, 0))
            .to_u64()
            .ok_or_else(|| DexError::Other("Invalid size amount".to_string()))?;

        let (price_value, order_type, tif) = if let Some(p) = price {
            // Apply spread if provided (for MarketMake strategy)
            let final_price = if let Some(spread_ticks) = _spread {
                // Get tick size from market data (assuming 0.1 for BTC)
                let tick_size = Decimal::new(1, 1); // 0.1
                let spread_amount = Decimal::from(spread_ticks) * tick_size;
                p + spread_amount
            } else {
                p
            };

            // Limit order
            let price_val = (final_price * Decimal::new(10, 0))
                .to_u32()
                .ok_or_else(|| DexError::Other("Invalid price".to_string()))?
                as u64;
            log::debug!("Creating limit order: side={}, original_price={}, spread_ticks={:?}, final_price={}, scaled_price={}, size={}, scaled_base_amount={}",
                side_value, p, _spread, final_price, price_val, size, base_amount);
            (price_val, 0u32, 1u32) // order_type=0 (limit), tif=1 (GTC like Hyperliquid)
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

            let price_val = (protection_price * Decimal::new(10, 0))
                .to_u32()
                .ok_or_else(|| DexError::Other("Invalid protection price".to_string()))?
                as u64;

            log::debug!(
                "Market order: current_price={}, protection_price={}, side={}",
                current_price,
                protection_price,
                side_value
            );

            (price_val, 1u32, 0u32) // order_type=1 (market), tif=0 (IOC)
        };

        // Use native Rust implementation for Lighter signatures
        self.create_order_native_with_type(
            market_id,
            side_value,
            tif,
            base_amount,
            price_value,
            None,
            order_type,
            false,
        )
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
        log::debug!("Cancelling all orders for Lighter connector");

        #[cfg(feature = "lighter-sdk")]
        {
            // Get nonce from API
            let nonce = self.get_nonce().await?;

            // Use ImmediateCancelAll (time_in_force=0, time=0)
            let time_in_force = 0; // ImmediateCancelAll
            let time = 0; // Not used for immediate cancel

            unsafe {
                let result = SignCancelAllOrders(time_in_force, time, nonce as i64);

                if !result.err.is_null() {
                    let error_cstr = CStr::from_ptr(result.err);
                    let error_msg = error_cstr.to_string_lossy().to_string();
                    libc::free(result.err as *mut libc::c_void);
                    return Err(DexError::Other(format!(
                        "Cancel all orders failed: {}",
                        error_msg
                    )));
                }

                // Get the transaction JSON
                let result_cstr = CStr::from_ptr(result.str);
                let tx_json = result_cstr.to_string_lossy().to_string();
                libc::free(result.str as *mut libc::c_void);

                // Submit the cancel transaction to the API
                let _timestamp = chrono::Utc::now().timestamp_millis() as u64;
                let form_data = format!(
                    "tx_type=16&tx_info={}&price_protection=false",
                    urlencoding::encode(&tx_json)
                );

                let response = self
                    .client
                    .post(&format!("{}/api/v1/sendTx", self.base_url))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .body(form_data)
                    .send()
                    .await
                    .map_err(|e| DexError::Other(format!("Network error: {}", e)))?;

                if !response.status().is_success() {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    return Err(DexError::Other(format!(
                        "Cancel all orders failed: HTTP {}, {}",
                        status, body
                    )));
                }

                log::debug!("Cancel all orders submitted successfully");
            }
        }

        #[cfg(not(feature = "lighter-sdk"))]
        {
            log::warn!("Cancel all orders not implemented without lighter-sdk feature");
        }

        Ok(())
    }

    async fn cancel_orders(
        &self,
        _symbol: Option<String>,
        _order_ids: Vec<String>,
    ) -> Result<(), DexError> {
        Err(DexError::Other(
            "Individual order cancellation not implemented for Lighter. Use cancel_all_orders instead.".to_string(),
        ))
    }

    async fn close_all_positions(&self, _symbol: Option<String>) -> Result<(), DexError> {
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
            if let Ok(pos_size) = position.position.parse::<f64>() {
                if pos_size.abs() > 0.00001 {
                    // Small threshold for floating point comparison
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
            log::info!("No open positions found, nothing to close");
            return Ok(());
        }

        // Close each open position by placing market orders in opposite direction
        for position in &account.positions {
            if let Ok(pos_size) = position.position.parse::<f64>() {
                if pos_size.abs() > 0.00001 {
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
                    let base_amount = (pos_size.abs() * 100000.0) as u64; // Convert to base units

                    // Create market order to close position
                    match self
                        .create_market_close_order_internal(market_id, base_amount, order_side)
                        .await
                    {
                        Ok(_) => {
                            log::info!(
                                "Successfully submitted close order for {} position in market {}",
                                position.symbol,
                                market_id
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

    async fn is_upcoming_maintenance(&self) -> bool {
        false
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
}

impl LighterConnector {
    async fn get_exchange_stats(&self) -> Result<LighterExchangeStats, DexError> {
        let url = format!("{}/api/v1/exchangeStats", self.base_url);

        let response = self
            .client
            .get(&url)
            .header("X-API-KEY", &self.api_key_public)
            .send()
            .await
            .map_err(|e| DexError::Other(format!("Failed to get exchange stats: {}", e)))?;

        let status = response.status();
        let response_text = response.text().await.map_err(|e| {
            DexError::Other(format!("Failed to read exchange stats response: {}", e))
        })?;

        if !status.is_success() {
            return Err(DexError::Other(format!(
                "Exchange stats HTTP {}: {}",
                status, response_text
            )));
        }

        log::trace!("Exchange stats response: {}", response_text);

        serde_json::from_str(&response_text)
            .map_err(|e| DexError::Other(format!("Failed to parse exchange stats: {}", e)))
    }

    async fn get_funding_rates(&self) -> Result<LighterFundingRates, DexError> {
        let url = format!("{}/api/v1/funding-rates", self.base_url);

        let response = self
            .client
            .get(&url)
            .header("X-API-KEY", &self.api_key_public)
            .send()
            .await
            .map_err(|e| DexError::Other(format!("Failed to get funding rates: {}", e)))?;

        let status = response.status();
        let response_text = response.text().await.map_err(|e| {
            DexError::Other(format!("Failed to read funding rates response: {}", e))
        })?;

        if !status.is_success() {
            return Err(DexError::Other(format!(
                "Funding rates HTTP {}: {}",
                status, response_text
            )));
        }

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

        let ws = match self._ws.as_ref() {
            Some(ws) => ws,
            None => return Err(DexError::Other("WebSocket not initialized".to_string())),
        };

        let (_sink, _stream) = ws
            .connect()
            .await
            .map_err(|_| DexError::Other("Failed to connect to WebSocket".to_string()))?;

        // Clone necessary data for the WebSocket task
        let current_price = self.current_price.clone();
        let current_volume = self.current_volume.clone();
        let order_book = self.order_book.clone();
        let filled_orders = self.filled_orders.clone();
        let canceled_orders = self.canceled_orders.clone();
        let is_running = self.is_running.clone();
        let account_index = self.account_index;

        // Spawn WebSocket handler task with reconnection logic
        let ws_url_clone = ws_url.clone();
        tokio::spawn(async move {
            loop {
                if !is_running.load(Ordering::SeqCst) {
                    log::info!("WebSocket task stopping due to is_running flag");
                    break;
                }

                log::info!("Attempting WebSocket connection to: {}", ws_url_clone);

                // Try to establish connection
                let connection_result = tokio_tungstenite::connect_async(&ws_url_clone).await;

                match connection_result {
                    Ok((mut ws_stream, _)) => {
                        log::info!("WebSocket connected successfully");

                        // Send subscription messages
                        let subscribe_orderbook = serde_json::json!({
                            "type": "subscribe",
                            "channel": "order_book/1"
                        });

                        let subscribe_account = serde_json::json!({
                            "type": "subscribe",
                            "channel": format!("account_all/{}", account_index)
                        });

                        if let Err(e) = ws_stream
                            .send(tokio_tungstenite::tungstenite::Message::Text(
                                subscribe_orderbook.to_string(),
                            ))
                            .await
                        {
                            log::error!("Failed to send orderbook subscription: {}", e);
                            continue;
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

                        // Handle messages in this connection
                        while let Some(message) = ws_stream.next().await {
                            if !is_running.load(Ordering::SeqCst) {
                                log::info!("WebSocket stopping due to is_running flag");
                                break;
                            }

                            match message {
                                Ok(message) => match message {
                                    tokio_tungstenite::tungstenite::Message::Text(text) => {
                                        log::trace!("WebSocket text message: {}", text);

                                        if let Ok(parsed) = serde_json::from_str::<Value>(&text) {
                                            Self::handle_websocket_message(
                                                parsed,
                                                &current_price,
                                                &current_volume,
                                                &order_book,
                                                &filled_orders,
                                                &canceled_orders,
                                                account_index,
                                            )
                                            .await;
                                        } else {
                                            log::warn!(
                                                "Failed to parse WebSocket message as JSON: {}",
                                                text
                                            );
                                        }
                                    }
                                    tokio_tungstenite::tungstenite::Message::Ping(_) => {
                                        log::trace!("Received WebSocket ping");
                                    }
                                    tokio_tungstenite::tungstenite::Message::Pong(_) => {
                                        log::trace!("Received WebSocket pong - connection healthy");
                                    }
                                    tokio_tungstenite::tungstenite::Message::Close(frame) => {
                                        log::info!("WebSocket close frame received: {:?}", frame);
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
                                    log::error!(
                                        "WebSocket error: {}. Will attempt reconnection.",
                                        e
                                    );
                                    break; // Break inner loop to attempt reconnection
                                }
                            }
                        }

                        log::warn!(
                            "WebSocket connection lost. Will attempt reconnection in 5 seconds."
                        );
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to connect to WebSocket: {}. Will retry in 5 seconds.",
                            e
                        );
                    }
                }

                // Wait before reconnection attempt
                tokio::time::sleep(Duration::from_secs(5)).await;
            }

            log::info!("WebSocket task ended");
        });

        Ok(())
    }

    async fn handle_websocket_message(
        message: Value,
        current_price: &Arc<RwLock<Option<(Decimal, u64)>>>,
        current_volume: &Arc<RwLock<Option<Decimal>>>,
        order_book: &Arc<RwLock<Option<LighterOrderBook>>>,
        filled_orders: &Arc<RwLock<HashMap<String, Vec<FilledOrder>>>>,
        canceled_orders: &Arc<RwLock<HashMap<String, Vec<CanceledOrder>>>>,
        account_index: u32,
    ) {
        let msg_type = message.get("type").and_then(|t| t.as_str()).unwrap_or("");
        log::trace!(
            "WebSocket message received: type='{}', message={:?}",
            msg_type,
            message
        );

        match msg_type {
            "subscribed/order_book" | "update/order_book" => {
                if let Some(order_book_data) = message.get("order_book") {
                    if let Ok(ob) =
                        serde_json::from_value::<LighterOrderBook>(order_book_data.clone())
                    {
                        // Update current price from best bid/ask
                        if let (Some(best_bid), Some(best_ask)) = (ob.bids.first(), ob.asks.first())
                        {
                            if let (Ok(bid_price), Ok(ask_price)) = (
                                string_to_decimal(Some(best_bid.price.clone())),
                                string_to_decimal(Some(best_ask.price.clone())),
                            ) {
                                let mid_price = (bid_price + ask_price) / Decimal::from(2);
                                let timestamp = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs();
                                *current_price.write().await = Some((mid_price, timestamp));
                                log::trace!(
                                    "Updated price from WebSocket: {} at {}",
                                    mid_price,
                                    timestamp
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

                        *order_book.write().await = Some(ob);
                    }
                }
            }
            "subscribed/account_all" | "update/account_all" => {
                log::debug!(
                    "Received account message: type={}, message={:?}",
                    msg_type,
                    message
                );
                // Handle account updates (filled/canceled orders)
                // For Lighter DEX, the data is directly in the message, not in a 'data' field
                Self::handle_account_update(
                    &message,
                    filled_orders,
                    canceled_orders,
                    account_index as u64,
                )
                .await;
            }
            _ => {
                log::trace!("Unhandled WebSocket message type: {}", msg_type);
            }
        }
    }

    async fn handle_account_update(
        data: &Value,
        filled_orders: &Arc<RwLock<HashMap<String, Vec<FilledOrder>>>>,
        canceled_orders: &Arc<RwLock<HashMap<String, Vec<CanceledOrder>>>>,
        account_id: u64,
    ) {
        log::debug!("handle_account_update called with data: {:?}", data);

        // Handle filled orders - try both 'fills' and 'trades' fields
        if let Some(fills) = data.get("fills").and_then(|f| f.as_array()) {
            log::debug!("Found {} fills in account update", fills.len());
            let mut filled_map = filled_orders.write().await;
            for fill in fills {
                log::debug!("Processing fill: {:?}", fill);
                if let Ok(filled_order) = Self::parse_filled_order(fill, account_id) {
                    log::info!("Added filled order: {:?}", filled_order);
                    filled_map
                        .entry("BTC".to_string())
                        .or_insert_with(Vec::new)
                        .push(filled_order);
                } else {
                    log::warn!("Failed to parse filled order: {:?}", fill);
                }
            }
        } else if let Some(trades) = data.get("trades") {
            log::debug!("Found trades object: {:?}", trades);
            // Handle trades object - Lighter DEX format: {"market_id": [trade_array]}
            if let Some(trades_obj) = trades.as_object() {
                let _filled_map = filled_orders.write().await;
                for (market_id, trade_array) in trades_obj {
                    log::debug!(
                        "Processing trades for market {}: {:?}",
                        market_id,
                        trade_array
                    );
                    if let Some(trades_array) = trade_array.as_array() {
                        for trade_data in trades_array {
                            log::debug!("Processing individual trade: {:?}", trade_data);
                            // Skip filled order processing for Lighter DEX (using timeout-based strategy)
                            log::trace!("Skipping trade data processing: {:?}", trade_data);
                        }
                    }
                }
            }
        } else {
            log::debug!("No 'fills' array or 'trades' object found in account data");
        }

        // Handle canceled orders
        if let Some(cancels) = data.get("cancels").and_then(|c| c.as_array()) {
            let mut canceled_map = canceled_orders.write().await;
            for cancel in cancels {
                if let Ok(canceled_order) = Self::parse_canceled_order(cancel) {
                    canceled_map
                        .entry("BTC".to_string())
                        .or_insert_with(Vec::new)
                        .push(canceled_order);
                }
            }
        }
    }

    fn parse_filled_order(_data: &Value, _account_id: u64) -> Result<FilledOrder, DexError> {
        // Filled order tracking is not supported for Lighter DEX
        // MarketMake strategy will use timeout-based order management instead
        Err(DexError::Other(
            "Filled order tracking not supported for Lighter DEX".to_string(),
        ))
    }

    fn parse_canceled_order(data: &Value) -> Result<CanceledOrder, DexError> {
        // Parse canceled order from WebSocket data
        // This is a simplified implementation - adjust based on actual Lighter WebSocket format
        Ok(CanceledOrder {
            order_id: data
                .get("order_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            canceled_timestamp: data.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0),
        })
    }

    fn calculate_min_tick(price: Decimal, sz_decimals: u32, is_spot: bool) -> Decimal {
        let price_str = price.to_string();
        let integer_part = price_str.split('.').next().unwrap_or("");
        let integer_digits = if integer_part == "0" {
            0
        } else {
            integer_part.len()
        };

        let scale_by_sig: u32 = if integer_digits >= 5 {
            0
        } else {
            (5 - integer_digits) as u32
        };

        let max_decimals: u32 = if is_spot { 8u32 } else { 6u32 };
        let scale_by_dec: u32 = max_decimals.saturating_sub(sz_decimals);
        let scale: u32 = scale_by_sig.min(scale_by_dec);

        Decimal::new(1, scale)
    }

    async fn create_market_close_order_internal(
        &self,
        market_id: u8,
        base_amount: u64,
        is_ask: u8,
    ) -> Result<String, DexError> {
        // Get current market price - prefer WebSocket data if available
        let current_price = if market_id == 1 {
            // First try to get price from WebSocket (more recent)
            let ws_price_data = *self.current_price.read().await;

            let price_decimal = if let Some((ws_price, price_timestamp)) = ws_price_data {
                let current_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let price_age = current_time.saturating_sub(price_timestamp);

                if price_age <= 30 {
                    log::debug!("Using WebSocket price for close order: {}", ws_price);
                    ws_price
                } else {
                    log::warn!(
                        "WebSocket price is stale ({}s old) for close order, using REST API",
                        price_age
                    );
                    // Fallback to REST API
                    let endpoint = format!("/api/v1/recentTrades?market_id={}&limit=1", market_id);
                    let url = format!("{}{}", self.base_url, endpoint);
                    let response = self
                        .client
                        .get(&url)
                        .send()
                        .await
                        .map_err(|e| DexError::Reqwest(e))?;

                    if response.status().is_success() {
                        let trades: Vec<serde_json::Value> = response
                            .json()
                            .await
                            .map_err(|e| DexError::Other(e.to_string()))?;

                        if let Some(trade) = trades.first() {
                            if let Some(price_str) = trade.get("price").and_then(|p| p.as_str()) {
                                Decimal::from_str(price_str)
                                    .map_err(|e| DexError::Other(e.to_string()))?
                            } else {
                                return Err(DexError::Other(
                                    "No price in recent trade".to_string(),
                                ));
                            }
                        } else {
                            return Err(DexError::Other("No recent trades available".to_string()));
                        }
                    } else {
                        return Err(DexError::Other("Failed to get recent trades".to_string()));
                    }
                }
            } else {
                // Fallback to REST API
                log::debug!("WebSocket price not available, using REST API");
                let endpoint = format!("/api/v1/recentTrades?market_id={}&limit=1", market_id);
                let url = format!("{}{}", self.base_url, endpoint);
                let response = self
                    .client
                    .get(&url)
                    .header("X-API-KEY", &self.api_key_public)
                    .send()
                    .await
                    .map_err(|e| DexError::Other(format!("Failed to get current price: {}", e)))?;

                let response_text = response.text().await.map_err(|e| {
                    DexError::Other(format!("Failed to read price response: {}", e))
                })?;

                let trades_response: LighterTradesResponse = serde_json::from_str(&response_text)
                    .map_err(|e| {
                    DexError::Other(format!("Failed to parse trades response: {}", e))
                })?;

                if let Some(trade) = trades_response.trades.first() {
                    string_to_decimal(Some(trade.price.clone()))?
                } else {
                    return Err(DexError::Other(
                        "No recent trades found for pricing".to_string(),
                    ));
                }
            };

            // Use very large buffer for market orders - this is protection price, not execution price
            let buffer_multiplier = if is_ask == 1 {
                Decimal::new(800, 3) // 0.800 (20% buffer for market sells - worst acceptable price)
            } else {
                Decimal::new(1200, 3) // 1.200 (20% buffer for market buys - worst acceptable price)
            };
            let market_price = price_decimal * buffer_multiplier;

            log::debug!(
                "Close order pricing: original={}, buffer={}, final={}",
                price_decimal,
                buffer_multiplier,
                market_price
            );

            // Convert to Lighter's price format (multiply by 10, same as create_order)
            (market_price * Decimal::new(10, 0)).to_u64().unwrap_or(0)
        } else {
            return Err(DexError::Other(format!(
                "Market ID {} not supported yet",
                market_id
            )));
        };

        log::debug!(
            "Creating market order: market_id={}, side={}, base_amount={}, price={}",
            market_id,
            is_ask,
            base_amount,
            current_price
        );

        // Create market order using existing native order infrastructure
        let response = self
            .create_order_native_with_type(
                market_id as u32,
                is_ask as u32,
                0, // IOC time in force (immediate or cancel)
                base_amount,
                current_price,
                None, // No specific client order index
                1,    // ORDER_TYPE_MARKET
                true, // reduce_only=true for position closing
            )
            .await?;

        Ok(response.order_id)
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

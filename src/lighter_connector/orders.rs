//! Private order-placement helpers and WS-fed order-tracking updates
//! for `LighterConnector`.
//!
//! All methods are `impl LighterConnector` blocks rather than free
//! functions — they read connector state (`client`, `base_url`,
//! `account_index`, `api_key_index`, `nonce_cache`, `market_cache`,
//! `cached_open_orders`, …) and feed back into REST + Go-SDK signing
//! paths. The `impl DexConnector for LighterConnector` trait surface
//! stays in mod.rs (Rust's trait-impl coherence rule); these are the
//! private helpers that the trait methods (`create_order`,
//! `cancel_order`, etc.) delegate into.
//!
//! Scope mirrors `src/extended_connector/orders.rs`: order placement,
//! nonce signing helpers, and the WS-fed `cached_open_orders`
//! update sites. Authentication / account-discovery / API-key
//! registration helpers live in mod.rs because they're a separate
//! concern from per-order operations.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use rust_decimal::Decimal;
use tokio::sync::RwLock;

use super::models::LighterNonceResponse;
use super::order_payload;
use super::parsing::ten_pow;
use super::rest::track_api_call;
use super::{
    normalize_symbol, LighterConnector, NonceCache, DEFAULT_PRICE_DECIMALS, DEFAULT_SIZE_DECIMALS,
    MAX_DECIMAL_PRECISION,
};
use crate::dex_request::DexError;
use crate::{CreateOrderResponse, OpenOrder, OrderSide};

impl LighterConnector {
    #[allow(clippy::too_many_arguments)] // params mirror the FFI signature; OrderPayload struct consolidates downstream.
    pub(super) async fn create_order_native_with_type(
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
        let now_ms = chrono::Utc::now().timestamp_millis();
        let client_order_index = client_order_id
            .as_deref()
            .and_then(|id| id.parse::<u64>().ok())
            .unwrap_or(now_ms as u64);
        let nonce = self.get_nonce().await?;
        let decimals = {
            let cache = self.market_cache.read().await;
            order_payload::OrderDecimals::resolve(&cache.by_id, market_id)
        };

        let payload = order_payload::build_order_payload_type_only(
            market_id,
            side,
            tif,
            base_amount,
            price,
            client_order_index,
            order_type,
            reduce_only,
            expiry_secs,
            nonce,
            now_ms,
        );

        log::debug!(
            "Creating native order: market_id={}, side={}, base_amount={}, price={}, price_decimals={}, size_decimals={}",
            market_id,
            side,
            base_amount,
            price,
            decimals.price_decimals,
            decimals.size_decimals,
        );

        let tx_info = self.call_go_sign_for_payload(&payload).await?;
        let form_data = order_payload::build_send_tx_form(&tx_info);

        track_api_call("POST /api/v1/sendTx", "POST");

        // Order submission path — wait up to 5s for budget rather than drop
        // the order. A small queued delay is vastly preferable to a missed
        // trade. See bot-strategy#79.
        self.acquire_rest_budget(
            "/api/v1/sendTx",
            crate::lighter_ratelimit::AcquirePolicy::Wait { max_ms: 5_000 },
        )
        .await?;

        let response = self
            .client
            .post(format!("{}/api/v1/sendTx", self.base_url))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(form_data)
            .send()
            .await
            .map_err(|e| DexError::Transient(format!("HTTP request failed: {}", e)))?;

        let status = response.status();
        let response_text = response
            .text()
            .await
            .map_err(|e| DexError::Transient(format!("Failed to read response: {}", e)))?;

        log::debug!(
            "Native order response: HTTP {}, Body: {}",
            status,
            response_text
        );

        if status.is_success() {
            Ok(order_payload::build_create_order_response(
                &payload, decimals,
            ))
        } else {
            self.invalidate_nonce_cache().await;
            Err(DexError::Transient(format!(
                "Order failed: HTTP {}, {}",
                status, response_text
            )))
        }
    }

    #[allow(clippy::too_many_arguments)] // params mirror the FFI signature; OrderPayload struct consolidates downstream.
    pub(super) async fn create_order_native_with_trigger(
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
        let now_ms = chrono::Utc::now().timestamp_millis();
        let client_order_index = client_order_id
            .as_deref()
            .and_then(|id| id.parse::<u64>().ok())
            .unwrap_or(now_ms as u64);
        let nonce = self.get_nonce().await?;
        let decimals = {
            let cache = self.market_cache.read().await;
            order_payload::OrderDecimals::resolve(&cache.by_id, market_id)
        };

        let payload = order_payload::build_order_payload_trigger(
            market_id,
            side,
            tif,
            base_amount,
            price,
            trigger_price,
            client_order_index,
            order_type,
            reduce_only,
            expiry_secs,
            nonce,
            now_ms,
        );

        log::debug!(
            "Creating trigger order: market_id={}, side={}, base_amount={}, price={}, trigger_price={}, order_type={}",
            market_id,
            side,
            base_amount,
            price,
            trigger_price,
            order_type,
        );

        let tx_info = self.call_go_sign_for_payload(&payload).await.map_err(|e| {
            log::error!("Failed to sign trigger order via Go SDK: {}", e);
            DexError::Transient(format!("Signature generation failed: {}", e))
        })?;
        let form_data = order_payload::build_send_tx_form(&tx_info);

        // Protective orders (SL/TP) must not be dropped — wait for budget.
        self.acquire_rest_budget(
            "/api/v1/sendTx",
            crate::lighter_ratelimit::AcquirePolicy::Wait { max_ms: 5_000 },
        )
        .await?;

        let response = self
            .client
            .post(format!("{}/api/v1/sendTx", self.base_url))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(form_data)
            .send()
            .await
            .map_err(|e| DexError::Transient(e.to_string()))?;

        let status = response.status();
        let response_text = response
            .text()
            .await
            .map_err(|e| DexError::Transient(e.to_string()))?;

        log::debug!("Trigger order response: HTTP {}, {}", status, response_text);

        if status.is_success() {
            log::info!(
                "✅ [TRIGGER_ORDER] Successfully created trigger order: {} (type={}, trigger_price={})",
                client_order_index,
                order_type,
                trigger_price,
            );
            Ok(order_payload::build_create_order_response(
                &payload, decimals,
            ))
        } else {
            self.invalidate_nonce_cache().await;
            Err(DexError::Transient(format!(
                "Trigger order failed: HTTP {}, {}",
                status, response_text
            )))
        }
    }

    #[allow(dead_code)]
    pub(super) async fn send_order_via_sdk(
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
            .arg(format!("--market-id={}", market_id))
            .arg(format!("--side={}", side))
            .arg(format!("--tif={}", tif))
            .arg(format!("--base-amt={}", base_amount))
            .arg(format!("--price={}", price))
            .arg(format!("--client-id={}", client_id))
            .env("LIGHTER_ACCOUNT_INDEX", self.account_index.to_string())
            .env("LIGHTER_API_KEY_INDEX", self.api_key_index.to_string())
            .env("LIGHTER_PRIVATE_API_KEY", &self.api_private_key_hex)
            .current_dir(".")
            .output()
            .map_err(|e| DexError::Transient(format!("Failed to execute SDK script: {}", e)))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        if !output.status.success() {
            log::error!("SDK delegation failed. stderr: {}", stderr);
            return Err(DexError::Transient(format!(
                "SDK execution failed: {}",
                stderr
            )));
        }

        if !stderr.is_empty() {
            log::warn!("SDK delegation warnings: {}", stderr);
        }

        // Parse JSON response
        let response: serde_json::Value = serde_json::from_str(&stdout)
            .map_err(|e| DexError::Transient(format!("Failed to parse SDK response: {}", e)))?;

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
            Err(DexError::Transient(format!("SDK order error: {}", error)))
        } else {
            Err(DexError::Transient(
                "Unexpected SDK response format".to_string(),
            ))
        }
    }

    #[allow(dead_code)]
    pub(super) async fn get_nonce(&self) -> Result<u64, DexError> {
        self.get_nonce_with_key(&self.api_key_public).await
    }

    pub(super) async fn get_nonce_with_key(&self, api_key: &str) -> Result<u64, DexError> {
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

    pub(super) async fn fetch_nonce(&self, api_key: &str) -> Result<u64, DexError> {
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
            .map_err(|e| DexError::Transient(format!("Failed to get nonce: {}", e)))?;

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
            return Err(DexError::Transient(format!(
                "Failed to get nonce: HTTP {}, Body: {}",
                status, error_body
            )));
        }

        let nonce_response: LighterNonceResponse = response
            .json()
            .await
            .map_err(|e| DexError::Transient(format!("Failed to parse nonce response: {}", e)))?;

        Ok(nonce_response.nonce)
    }

    pub(super) async fn invalidate_nonce_cache(&self) {
        let mut cache = self.nonce_cache.lock().await;
        *cache = None;
    }

    pub(super) async fn update_order_tracking_after_create(
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

    /// Update the cached price of an order after a native in-place modify
    /// (bot-strategy#471). The order keeps its identity (`order_id`), so we
    /// reprice the existing tracked entry rather than add a duplicate. A
    /// missing entry is a no-op — the WS order feed reconciles tracking
    /// authoritatively on the next update.
    pub(super) async fn update_order_tracking_after_modify(
        &self,
        symbol: &str,
        order_id: &str,
        new_price: Decimal,
    ) {
        let mut orders_guard = self.cached_open_orders.write().await;
        if let Some(orders) = orders_guard.get_mut(symbol) {
            for order in orders.iter_mut() {
                if order.order_id == order_id {
                    order.price = new_price;
                }
            }
        }
    }

    /// Update order tracking after order cancellation
    #[allow(dead_code)]
    pub(super) async fn update_order_tracking_after_cancel(&self, symbol: &str, order_id: &str) {
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

    pub(super) async fn remove_tracked_order(
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

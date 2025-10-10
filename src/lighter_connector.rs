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
use tokio::{sync::RwLock, time::sleep};

#[derive(Clone)]
pub struct LighterConnector {
    api_key_public: String,     // X-API-KEY header (from Lighter UI)
    api_key_index: u32,         // api_key_index query param
    l1_private_key_hex: String, // L1 EVM private key for signing (0x-prefixed hex)
    account_index: u32,         // account_index query param
    base_url: String,
    websocket_url: String,
    _l1_address: String, // derived from l1_private_key_hex (used for address derivation)
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

impl LighterConnector {
    pub fn new(
        api_key_public: String,
        api_key_index: u32,
        l1_private_key_hex: String,
        account_index: u32,
        base_url: String,
        websocket_url: String,
    ) -> Result<Self, DexError> {
        let l1_address = Self::derive_l1_address(&l1_private_key_hex)?;

        log::info!("Creating LighterConnector with address: {}", l1_address);

        Ok(Self {
            api_key_public,
            api_key_index,
            l1_private_key_hex,
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

    fn derive_l1_address(private_key_hex: &str) -> Result<String, DexError> {
        use ethers::signers::{LocalWallet, Signer};
        use std::str::FromStr;

        let cleaned_key = private_key_hex.strip_prefix("0x").unwrap_or(private_key_hex);
        let wallet = LocalWallet::from_str(cleaned_key)
            .map_err(|e| DexError::Other(format!("Invalid private key: {}", e)))?;

        Ok(format!("0x{:x}", wallet.address()))
    }

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

        log::info!("Delegating order to Python SDK: market_id={}, side={}, base_amount={}, price={}",
                   market_id, side, base_amount, price);

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
            .env("LIGHTER_PRIVATE_KEY", &self.l1_private_key_hex)
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

            let order_id = response.get("tx_hash")
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
            Err(DexError::Other("Unexpected SDK response format".to_string()))
        }
    }

    #[allow(dead_code)]
    async fn get_nonce(&self) -> Result<u64, DexError> {
        let url = format!(
            "{}/api/v1/nextNonce?account_index={}&api_key_index={}",
            self.base_url, self.account_index, self.api_key_index
        );

        let response = self
            .client
            .get(&url)
            .header("X-API-KEY", &self.api_key_public)
            .send()
            .await
            .map_err(|e| DexError::Other(format!("Failed to get nonce: {}", e)))?;

        if !response.status().is_success() {
            return Err(DexError::Other(format!(
                "Failed to get nonce: HTTP {}",
                response.status()
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
            return Err(DexError::Other(format!(
                "HTTP {}: {}",
                status,
                error_text
            )));
        }

        response
            .json()
            .await
            .map_err(|e| DexError::Other(format!("Failed to parse response: {}", e)))
    }
}

#[async_trait]
impl DexConnector for LighterConnector {
    async fn start(&self) -> Result<(), DexError> {
        self.is_running.store(true, Ordering::SeqCst);
        log::info!("Lighter connector started with WebSocket: {}", self.websocket_url);
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

        let account_response: LighterAccountResponse = self
            .make_request(&endpoint, HttpMethod::Get, None)
            .await?;

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
        let base_amount = (size * Decimal::new(100_000, 0)).to_u64()
            .ok_or_else(|| DexError::Other("Invalid size amount".to_string()))?;

        let price_value = if let Some(p) = price {
            (p * Decimal::new(1_000_000, 0)).to_u64()
                .ok_or_else(|| DexError::Other("Invalid price".to_string()))?
        } else {
            return Err(DexError::Other("Market orders not supported yet".to_string()));
        };

        // Delegate to Python SDK for proper Schnorr+Poseidon2 signatures
        self.send_order_via_sdk(
            market_id,
            side_value,
            tif,
            base_amount,
            price_value,
            None,
        ).await
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
        Err(DexError::Other("Trigger orders not implemented yet".to_string()))
    }

    async fn cancel_order(&self, _symbol: &str, _order_id: &str) -> Result<(), DexError> {
        Err(DexError::Other("Order cancellation not implemented yet".to_string()))
    }

    async fn cancel_all_orders(&self, _symbol: Option<String>) -> Result<(), DexError> {
        Err(DexError::Other("Cancel all orders not implemented yet".to_string()))
    }

    async fn cancel_orders(
        &self,
        _symbol: Option<String>,
        _order_ids: Vec<String>,
    ) -> Result<(), DexError> {
        Err(DexError::Other("Bulk cancel orders not implemented yet".to_string()))
    }

    async fn close_all_positions(&self, _symbol: Option<String>) -> Result<(), DexError> {
        Err(DexError::Other("Close positions not implemented yet".to_string()))
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

        let cleaned_key = self.l1_private_key_hex.strip_prefix("0x").unwrap_or(&self.l1_private_key_hex);
        let wallet = LocalWallet::from_str(cleaned_key)
            .map_err(|e| DexError::Other(format!("Invalid private key: {}", e)))?;

        let signature = wallet.sign_message(message.as_bytes()).await
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
    l1_private_key_hex: String,
    account_index: u32,
    base_url: String,
    websocket_url: String,
) -> Result<Box<dyn DexConnector>, DexError> {
    let connector = LighterConnector::new(
        api_key_public,
        api_key_index,
        l1_private_key_hex,
        account_index,
        base_url,
        websocket_url,
    )?;
    Ok(Box::new(connector))
}
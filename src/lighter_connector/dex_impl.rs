//! The public `impl DexConnector for LighterConnector` trait surface,
//! split out of `mod.rs` (bot-strategy#501). Rust's trait-impl coherence
//! requires this to stay a single block, so ticker / balance / positions /
//! order / maintenance trait methods all live here; the heavy private
//! plumbing they delegate into lives in `orders.rs`, `account.rs`,
//! `ws.rs`, and the other concern modules.

use super::*;

#[async_trait]
impl DexConnector for LighterConnector {
    async fn start(&self) -> Result<(), DexError> {
        self.is_running.store(true, Ordering::SeqCst);
        log::debug!(
            "Lighter connector started with WebSocket: {}",
            self.websocket_url
        );

        // If a prior process (or earlier call on this host) engaged a WAF /
        // API rate-limit cooldown, wait it out before the first REST call of
        // start() rather than burning a budget slot on a doomed request and
        // re-learning the cooldown from a fresh 429. See bot-strategy#148.
        if let Some(remaining) =
            crate::lighter_waf_cooldown::pending_cooldown_wait("LighterConnector::start")
        {
            tokio::time::sleep(remaining).await;
        }

        // Initialize the Go client and validate API key
        #[cfg(feature = "lighter-sdk")]
        {
            match self.create_go_client().await {
                Ok(()) => {}
                Err(DexError::ApiKeyRegistrationRequired) => {
                    #[cfg(feature = "lighter-sdk")]
                    if let Some(evm_key) = &self.evm_wallet_private_key {
                        let go_key = self.get_go_pubkey_from_check().map_err(|e| {
                            DexError::Transient(format!(
                                "Failed to derive Go public key from CheckClient: {}",
                                e
                            ))
                        })?;

                        log::debug!("API key registration required. Attempting to register...");

                        // Get server public key for ChangePubKey
                        let server_pubkey = self.get_server_public_key().await.map_err(|e| {
                            DexError::Transient(format!("Failed to get server public key: {}", e))
                        })?;

                        self.register_api_key(evm_key, &go_key, &server_pubkey)
                            .await
                            .map_err(|e| {
                                DexError::Transient(format!("API key registration failed: {}", e))
                            })?;

                        // Retry validation after registration
                        self.create_go_client().await?;
                    } else {
                        return Err(DexError::ApiKeyRegistrationRequired);
                    }

                    #[cfg(not(feature = "lighter-sdk"))]
                    return Err(DexError::ApiKeyRegistrationRequired);
                }
                Err(e) => return Err(e),
            }
        }

        // Stagger startup across bot instances to reduce WAF pressure.
        {
            use rand::Rng;
            let startup_jitter_secs: u64 = std::env::var("LIGHTER_STARTUP_JITTER_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30);
            if startup_jitter_secs > 0 {
                let jitter = rand::thread_rng().gen_range(0..=startup_jitter_secs);
                log::info!(
                    "Startup jitter: sleeping {}s (max {}s)",
                    jitter,
                    startup_jitter_secs
                );
                tokio::time::sleep(std::time::Duration::from_secs(jitter)).await;
            }
        }

        // Preload market metadata once at startup to prevent repeated REST calls later.
        self.ensure_market_metadata_loaded().await?;

        // Start WebSocket connection
        self.start_websocket().await?;

        // Start auto-cleanup background task (every 6 hours)
        self.start_auto_cleanup(6);
        log::info!("🗑️ [AUTO_CLEANUP] Started background cleanup task (every 6 hours)");

        // Start the maintenance feed refresher off the strategy hot path
        // so a slow status.lighter.xyz response can never blow STEP_OVERRUN
        // (bot-strategy#160).
        self.start_maintenance_refresher();

        Ok(())
    }

    async fn stop(&self) -> Result<(), DexError> {
        self.is_running.store(false, Ordering::SeqCst);

        // Optionally abort cleanup task for immediate shutdown
        if let Some(handle) = self.cleanup_handle.lock().await.take() {
            handle.abort();
            self.cleanup_started.store(false, Ordering::SeqCst);
            log::debug!("🛑 [AUTO_CLEANUP] task forcibly aborted on stop");
        }

        log::debug!("Lighter connector stopped");
        Ok(())
    }

    async fn restart(&self, _max_retries: i32) -> Result<(), DexError> {
        self.stop().await?;
        sleep(Duration::from_secs(1)).await;
        self.start().await
    }

    async fn set_leverage(&self, _symbol: &str, _leverage: u32) -> Result<(), DexError> {
        log::debug!("Leverage setting not implemented for Lighter");
        Ok(())
    }

    async fn get_ticker(
        &self,
        symbol: &str,
        test_price: Option<Decimal>,
    ) -> Result<TickerResponse, DexError> {
        self.fetch_ticker(symbol, test_price).await
    }

    async fn get_filled_orders(&self, symbol: &str) -> Result<FilledOrdersResponse, DexError> {
        let orders = self.filled_orders.read().await;
        let normalized = normalize_symbol(symbol);
        let symbol_orders = orders
            .get(symbol)
            .or_else(|| orders.get(&normalized))
            .cloned()
            .unwrap_or_default();

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

    async fn get_balance(&self, symbol: Option<&str>) -> Result<BalanceResponse, DexError> {
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

    async fn get_combined_balance(&self) -> Result<CombinedBalanceResponse, DexError> {
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

    async fn get_positions(&self) -> Result<Vec<PositionSnapshot>, DexError> {
        if !self.positions_ready.load(Ordering::SeqCst) {
            return Err(DexError::Transient(
                "positions not ready from websocket".to_string(),
            ));
        }
        let state = self.account_state.read().await;
        Ok(state.positions.clone())
    }

    async fn get_open_orders(&self, symbol: &str) -> Result<OpenOrdersResponse, DexError> {
        log::debug!(
            "[WS_ORDER_TRACKING] get_open_orders called for symbol: {} (WebSocket-only)",
            symbol
        );

        // Return WebSocket-tracked orders only (no API fallback)
        let orders_guard = self.cached_open_orders.read().await;
        let orders = orders_guard.get(symbol).cloned().unwrap_or_default();

        log::debug!(
            "[WS_ORDER_TRACKING] Returning {} orders for {} from WebSocket tracking",
            orders.len(),
            symbol
        );

        Ok(OpenOrdersResponse { orders })
    }

    async fn get_last_trades(&self, symbol: &str) -> Result<LastTradesResponse, DexError> {
        self.fetch_last_trades(symbol).await
    }

    async fn get_order_book(
        &self,
        symbol: &str,
        depth: usize,
    ) -> Result<OrderBookSnapshot, DexError> {
        self.fetch_order_book(symbol, depth).await
    }

    async fn clear_filled_order(&self, symbol: &str, trade_id: &str) -> Result<(), DexError> {
        let mut filled_orders = self.filled_orders.write().await;
        if let Some(orders) = filled_orders.get_mut(symbol) {
            let initial_len = orders.len();
            orders.retain(|order| order.trade_id != trade_id);
            if orders.len() < initial_len {
                log::debug!(
                    "🗑️ [CLEAR_FILL] Removed trade_id {} for {}",
                    trade_id,
                    symbol
                );
                Ok(())
            } else {
                Err(DexError::Transient(format!(
                    "Trade ID {} not found for symbol {}",
                    trade_id, symbol
                )))
            }
        } else {
            Err(DexError::Transient(format!(
                "No filled orders found for symbol {}",
                symbol
            )))
        }
    }

    async fn clear_all_filled_orders(&self) -> Result<(), DexError> {
        let mut filled_orders = self.filled_orders.write().await;
        let total_cleared = filled_orders.values().map(|v| v.len()).sum::<usize>();
        filled_orders.clear();
        log::info!(
            "🗑️ [CLEAR_ALL_FILLS] Cleared {} filled orders across all symbols",
            total_cleared
        );
        Ok(())
    }

    async fn clear_canceled_order(&self, _symbol: &str, _order_id: &str) -> Result<(), DexError> {
        Err(DexError::Transient(
            "clear_canceled_order not supported for Lighter - canceled orders are streamed via WebSocket only".to_string()
        ))
    }

    async fn clear_all_canceled_orders(&self) -> Result<(), DexError> {
        Err(DexError::Transient(
            "clear_all_canceled_orders not supported for Lighter - canceled orders are streamed via WebSocket only".to_string()
        ))
    }

    async fn create_order(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        price: Option<Decimal>,
        _spread: Option<i64>,
        reduce_only: bool,
        expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        // Resolve market metadata for symbol
        let market_info = self.resolve_market_info(symbol).await?;
        let market_id = market_info.market_id;

        // Convert side: Long=0(BUY), Short=1(SELL) for Lighter API
        let side_value = match side {
            OrderSide::Long => 0,
            OrderSide::Short => 1,
        };

        // Convert time-in-force: 0=IOC, 1=GTT, 2=PostOnly
        // Use spread parameter to specify TIF when negative values:
        // spread >= 0: normal spread adjustment
        // spread = -1: Request IOC (degraded to GTT on Lighter)
        // spread = -2: Post-only order
        let default_tif = TIF_GTT;

        let price_decimals = market_info.price_decimals;
        let size_decimals = market_info.size_decimals;

        // Convert amounts to Lighter's scaled integers using market metadata
        let size_abs = size.abs();
        let mut base_amount = scale_decimal_to_u64(
            size_abs,
            size_decimals,
            RoundingStrategy::ToZero,
            "base amount",
        )?;

        if base_amount == 0 && size_abs > Decimal::ZERO {
            log::debug!(
                "Rounded base amount to zero for size {} (decimals {}), forcing minimum base_amount=1",
                size_abs,
                size_decimals
            );
            base_amount = 1;
        }

        let (price_value, order_type, tif) = if let Some(p) = price {
            // Handle spread parameter: negative values for TIF, positive
            // for price adjustment. See `resolve_spread_to_tif_and_price`
            // for the full mapping (extracted to allow unit testing —
            // bot-strategy#317).
            let tick_decimals = price_decimals.min(MAX_DECIMAL_PRECISION);
            if tick_decimals != price_decimals {
                if let Some(s) = _spread {
                    if s > 0 {
                        log::warn!(
                            "Price decimals {} exceed supported max {}, clamping for spread adjustment",
                            price_decimals,
                            MAX_DECIMAL_PRECISION
                        );
                    }
                }
            }
            let tick_size = Decimal::new(1, tick_decimals);
            let (final_price, order_tif) =
                resolve_spread_to_tif_and_price(p, _spread, tick_size, default_tif);

            // Limit order
            let price_u32 = scale_decimal_to_u32(
                final_price,
                price_decimals,
                RoundingStrategy::MidpointAwayFromZero,
                "price",
            )?;
            let price_val = u64::from(price_u32);

            let tif_name = match order_tif {
                v if v == TIF_IOC => "IOC",
                v if v == TIF_GTT => "GTT",
                v if v == TIF_POST_ONLY => "POST_ONLY",
                _ => "UNKNOWN",
            };

            log::debug!("Creating limit order: side={}, original_price={}, spread_param={:?}, final_price={}, TIF={} ({}), scaled_price={}, size={}, scaled_base_amount={}",
                side_value, p, _spread, final_price, order_tif, tif_name, price_val, size_abs, base_amount);
            (price_val, ORDER_TYPE_LIMIT, order_tif)
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

            let price_u32 = scale_decimal_to_u32(
                protection_price,
                price_decimals,
                RoundingStrategy::MidpointAwayFromZero,
                "protection price",
            )?;
            let price_val = u64::from(price_u32);

            log::debug!(
                "Market order: current_price={}, protection_price={}, side={}, price_decimals={}, size_decimals={}",
                current_price,
                protection_price,
                side_value,
                price_decimals,
                size_decimals
            );

            (price_val, ORDER_TYPE_IOC, TIF_IOC) // Market orders use IOC semantics
        };

        // Use native Rust implementation for Lighter signatures
        let cid = chrono::Utc::now().timestamp_millis().to_string();
        let result = self
            .create_order_native_with_type(
                market_id,
                side_value,
                tif,
                base_amount,
                price_value,
                Some(cid),
                order_type,
                reduce_only,
                expiry_secs,
            )
            .await;

        // Update order tracking if order creation was successful
        if let Ok(ref response) = result {
            let actual_price = price.unwrap_or(Decimal::ZERO);
            self.update_order_tracking_after_create(
                symbol,
                &response.order_id,
                side,
                size,
                actual_price,
            )
            .await;
        }

        result
    }

    async fn create_advanced_trigger_order(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        trigger_px: Decimal,
        limit_px: Option<Decimal>,
        order_style: TriggerOrderStyle,
        slippage_bps: Option<u32>,
        tpsl: TpSl,
        reduce_only: bool,
        expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        log::info!(
            "🎯 [ADVANCED_TRIGGER_ORDER] Creating {} order for {}: style={:?}, trigger={}, limit={:?}, slippage_bps={:?}",
            match tpsl { TpSl::Sl => "stop loss", TpSl::Tp => "take profit" },
            symbol,
            order_style,
            trigger_px,
            limit_px,
            slippage_bps
        );

        let market_info = self.resolve_market_info(symbol).await?;
        let market_id = market_info.market_id;

        let side_value = if is_buy_for_tpsl(side) {
            SIDE_BUY
        } else {
            SIDE_SELL
        };

        let (is_market, final_limit_price, order_type) = match order_style {
            TriggerOrderStyle::Market => {
                let order_type = match tpsl {
                    TpSl::Sl => 2, // StopLossOrder
                    TpSl::Tp => 4, // TakeProfitOrder
                };
                (true, trigger_px, order_type)
            }
            TriggerOrderStyle::MarketWithSlippageControl => {
                if let Some(slippage) = slippage_bps {
                    let slippage_factor = Decimal::new(slippage as i64, 4);
                    // "Market equivalent with slippage control" means we prioritize execution over price
                    // Always adjust towards the WORSE direction for guaranteed execution
                    let adjusted_price = match (side, tpsl) {
                        (OrderSide::Long, TpSl::Sl) => {
                            // Long Stop Loss (sell): worse price for selling = lower price
                            trigger_px * (Decimal::ONE - slippage_factor)
                        }
                        (OrderSide::Short, TpSl::Sl) => {
                            // Short Stop Loss (buy): worse price for buying = higher price
                            trigger_px * (Decimal::ONE + slippage_factor)
                        }
                        (OrderSide::Long, TpSl::Tp) => {
                            // Long Take Profit executed as market (sell): worse price = lower price
                            trigger_px * (Decimal::ONE - slippage_factor)
                        }
                        (OrderSide::Short, TpSl::Tp) => {
                            // Short Take Profit executed as market (buy): worse price = higher price
                            trigger_px * (Decimal::ONE + slippage_factor)
                        }
                    };
                    let order_type = match tpsl {
                        TpSl::Sl => 3, // StopLossLimitOrder with slippage control
                        TpSl::Tp => 5, // TakeProfitLimitOrder with slippage control
                    };
                    (false, adjusted_price, order_type)
                } else {
                    // Fallback to pure market
                    let order_type = match tpsl {
                        TpSl::Sl => 2,
                        TpSl::Tp => 4,
                    };
                    (true, trigger_px, order_type)
                }
            }
            TriggerOrderStyle::Limit => {
                let limit_price = limit_px.ok_or_else(|| DexError::InvalidInput {
                    field: "limit_px".to_string(),
                    value: "required for Limit order style".to_string(),
                })?;

                // Validate limit price vs trigger price for the order type
                // The validation should be based on order execution direction, not position side
                match (side, tpsl) {
                    (OrderSide::Long, TpSl::Sl) => {
                        // Buy stop loss: limit should be >= trigger (worse price for buying)
                        if limit_price < trigger_px {
                            return Err(DexError::InvalidInput {
                                field: "limit_px".to_string(),
                                value: "For Buy Stop Loss, limit_px must be >= trigger_px"
                                    .to_string(),
                            });
                        }
                    }
                    (OrderSide::Short, TpSl::Sl) => {
                        // Sell stop loss: limit should be <= trigger (worse price for selling)
                        if limit_price > trigger_px {
                            return Err(DexError::InvalidInput {
                                field: "limit_px".to_string(),
                                value: "For Sell Stop Loss, limit_px must be <= trigger_px"
                                    .to_string(),
                            });
                        }
                    }
                    (OrderSide::Long, TpSl::Tp) => {
                        // Buy take profit: limit should be <= trigger (better price for buying)
                        if limit_price > trigger_px {
                            return Err(DexError::InvalidInput {
                                field: "limit_px".to_string(),
                                value: "For Buy Take Profit, limit_px must be <= trigger_px"
                                    .to_string(),
                            });
                        }
                    }
                    (OrderSide::Short, TpSl::Tp) => {
                        // Sell take profit: limit should be >= trigger (better price for selling)
                        if limit_price < trigger_px {
                            return Err(DexError::InvalidInput {
                                field: "limit_px".to_string(),
                                value: "For Sell Take Profit, limit_px must be >= trigger_px"
                                    .to_string(),
                            });
                        }
                    }
                }

                let order_type = match tpsl {
                    TpSl::Sl => 3, // StopLossLimitOrder
                    TpSl::Tp => 5, // TakeProfitLimitOrder
                };
                (false, limit_price, order_type)
            }
        };

        // Convert to native units with proper error handling
        let size_abs = size.abs();
        let mut base_amount = scale_decimal_to_u64(
            size_abs,
            market_info.size_decimals,
            RoundingStrategy::ToZero,
            "trigger order base amount",
        )?;
        if base_amount == 0 && size_abs > Decimal::ZERO {
            log::debug!(
                "Rounded trigger order base amount to zero for size {}, forcing minimum base_amount=1",
                size_abs
            );
            base_amount = 1;
        }

        let trigger_price_native = u64::from(scale_decimal_to_u32(
            trigger_px,
            market_info.price_decimals,
            RoundingStrategy::MidpointAwayFromZero,
            "trigger price",
        )?);

        let execution_price_native = if is_market {
            0 // Market orders: server ignores execution_price, use 0 for clarity
        } else {
            u64::from(scale_decimal_to_u32(
                final_limit_price,
                market_info.price_decimals,
                RoundingStrategy::MidpointAwayFromZero,
                "execution price",
            )?)
        };

        // Set TimeInForce based on order type (using global protocol constants)
        let time_in_force = if is_market { TIF_IOC } else { TIF_GTT };

        log::debug!(
            "Creating trigger order: market_id={}, side={}, base_amount={}, price={}, trigger_price={}, order_type={}",
            market_id, side_value, base_amount, execution_price_native, trigger_price_native, order_type
        );

        let cid = chrono::Utc::now().timestamp_millis().to_string();
        let result = self
            .create_order_native_with_trigger(
                market_id,
                side_value,
                time_in_force,
                base_amount,
                execution_price_native,
                trigger_price_native,
                Some(cid),
                order_type,
                reduce_only,
                expiry_secs,
            )
            .await;

        if let Ok(ref response) = result {
            let tracked_price = response.ordered_price;
            self.update_order_tracking_after_create(
                symbol,
                &response.order_id,
                side,
                size,
                tracked_price,
            )
            .await;
        }

        result
    }

    async fn modify_order(
        &self,
        symbol: &str,
        order_id: &str,
        _side: OrderSide,
        target_total_size: Decimal,
        _open_remaining_size: Decimal,
        price: Option<Decimal>,
        _spread: Option<i64>,
        _reduce_only: bool,
    ) -> Result<CreateOrderResponse, DexError> {
        // L2ModifyOrder cannot retarget time-in-force (the modify tx carries
        // no TIF field), so it can only reprice a resting order — it cannot
        // turn a passive maker into a taker. Callers that want market/IOC
        // takeover must fall back to cancel+reissue. `side` / `reduce_only`
        // are immutable properties of the existing order and are ignored
        // here; `open_remaining_size` is unused for Lighter because the
        // engine re-derives the open remainder from `base_amount - filled`.
        let new_price = price.ok_or_else(|| {
            DexError::Permanent(
                "Lighter modify_order requires a limit price (cannot retarget TIF to market)"
                    .to_string(),
            )
        })?;

        let market_info = self.resolve_market_info(symbol).await?;
        let market_id = market_info.market_id;
        let price_decimals = market_info.price_decimals;
        let size_decimals = market_info.size_decimals;

        let order_index =
            parse_cancel_order_index(order_id).ok_or_else(|| DexError::InvalidInput {
                field: "order_id".to_string(),
                value: order_id.to_string(),
            })?;

        // Re-assert the order's *total* base amount. Because this equals the
        // value originally sent to `create_order`, the matching engine keeps
        // the already-filled portion immutable and re-opens only the unfilled
        // remainder — no double-fill is possible (bot-strategy#471 / #470).
        let total_abs = target_total_size.abs();
        let mut base_amount = scale_decimal_to_u64(
            total_abs,
            size_decimals,
            RoundingStrategy::ToZero,
            "base amount",
        )?;
        if base_amount == 0 && total_abs > Decimal::ZERO {
            base_amount = 1;
        }

        // Honour any positive tick adjustment carried on `spread`; the TIF
        // output is irrelevant to a modify (only the price changes).
        let tick_decimals = price_decimals.min(MAX_DECIMAL_PRECISION);
        let tick_size = Decimal::new(1, tick_decimals);
        let (final_price, _tif) =
            resolve_spread_to_tif_and_price(new_price, _spread, tick_size, TIF_GTT);
        let price_u32 = scale_decimal_to_u32(
            final_price,
            price_decimals,
            RoundingStrategy::MidpointAwayFromZero,
            "price",
        )?;

        let nonce = self.get_nonce().await? as i64;
        let tx_json = self
            .call_go_sign_modify_order(
                market_id as i32,
                order_index,
                base_amount as i64,
                u64::from(price_u32) as i64,
                0, // trigger_price unchanged (NilOrderTriggerPrice)
                nonce,
            )
            .await?;

        let form_data = format!(
            "tx_type=17&tx_info={}&price_protection=false",
            urlencoding::encode(&tx_json)
        );

        track_api_call("POST /api/v1/sendTx (modify_order)", "POST");

        // Amend is on the order-management path — wait for budget rather than
        // drop it, matching create/cancel.
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

        if !status.is_success() {
            self.invalidate_nonce_cache().await;
            return Err(DexError::Transient(format!(
                "Modify order failed: HTTP {}, {}",
                status, response_text
            )));
        }

        let ordered_price = Decimal::new(i64::from(price_u32), price_decimals);
        // The order keeps its identity (same order_id) — update the tracked
        // price in place rather than adding a duplicate entry.
        self.update_order_tracking_after_modify(symbol, order_id, ordered_price)
            .await;

        log::info!(
            "[MODIFY_ORDER] Repriced order {} for {} to {} (base_amount={})",
            order_id,
            symbol,
            ordered_price,
            base_amount,
        );

        Ok(CreateOrderResponse {
            order_id: order_id.to_string(),
            exchange_order_id: None,
            ordered_price,
            ordered_size: total_abs,
            client_order_id: None,
        })
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<(), DexError> {
        let market_info = self.resolve_market_info(symbol).await?;

        let order_index = match parse_cancel_order_index(order_id) {
            Some(idx) => idx,
            None => {
                log::warn!(
                    "[CANCEL_ORDER] Unable to derive numeric index from order_id '{}'. Skipping cancel request.",
                    order_id
                );
                return Ok(());
            }
        };

        let nonce = self.get_nonce().await? as i64;
        let tx_json = self
            .call_go_sign_cancel_order(market_info.market_id as i32, order_index, nonce)
            .await?;

        let form_data = format!(
            "tx_type=15&tx_info={}&price_protection=false",
            urlencoding::encode(&tx_json)
        );

        track_api_call("POST /api/v1/sendTx (cancel_order)", "POST");

        // Cancel is on the risk-reduction path — wait for budget.
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

        if !status.is_success() {
            self.invalidate_nonce_cache().await;
            return Err(DexError::Transient(format!(
                "Cancel order failed: HTTP {}, {}",
                status, response_text
            )));
        }

        self.update_order_tracking_after_cancel(symbol, order_id)
            .await;

        log::info!(
            "[CANCEL_ORDER] Successfully cancelled order {} for {}",
            order_id,
            symbol
        );

        Ok(())
    }

    async fn cancel_all_orders(&self, symbol: Option<String>) -> Result<(), DexError> {
        let targets: Vec<(String, Vec<String>)> = {
            let orders_guard = self.cached_open_orders.read().await;
            match symbol {
                Some(sym) => {
                    let ids = orders_guard
                        .get(&sym)
                        .map(|orders| orders.iter().map(|o| o.order_id.clone()).collect())
                        .unwrap_or_default();
                    vec![(sym, ids)]
                }
                None => orders_guard
                    .iter()
                    .map(|(sym, orders)| {
                        (
                            sym.clone(),
                            orders.iter().map(|o| o.order_id.clone()).collect(),
                        )
                    })
                    .collect(),
            }
        };

        let mut last_err: Option<DexError> = None;
        for (sym, ids) in targets {
            for order_id in ids {
                if let Err(e) = self.cancel_order(&sym, &order_id).await {
                    log::error!(
                        "[CANCEL_ORDER] Failed to cancel order {} for {}: {}",
                        order_id,
                        sym,
                        e
                    );
                    last_err = Some(e);
                }
            }
        }

        if let Some(err) = last_err {
            Err(err)
        } else {
            Ok(())
        }
    }

    async fn cancel_orders(
        &self,
        symbol: Option<String>,
        order_ids: Vec<String>,
    ) -> Result<(), DexError> {
        let symbol = match symbol {
            Some(sym) => sym,
            None => {
                return Err(DexError::Transient(
                    "cancel_orders requires a symbol on Lighter".to_string(),
                ))
            }
        };

        if order_ids.is_empty() {
            return Ok(());
        }

        let mut last_err: Option<DexError> = None;
        for order_id in order_ids {
            if let Err(e) = self.cancel_order(&symbol, &order_id).await {
                log::error!(
                    "[CANCEL_ORDER] Failed to cancel order {} for {}: {}",
                    order_id,
                    symbol,
                    e
                );
                last_err = Some(e);
            }
        }

        if let Some(err) = last_err {
            Err(err)
        } else {
            Ok(())
        }
    }

    async fn close_all_positions(&self, symbol: Option<String>) -> Result<(), DexError> {
        // Get current account info to check positions
        let endpoint = format!("/api/v1/account?by=index&value={}", self.account_index);

        let url = format!("{}{}", self.base_url, endpoint);
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

        if !status.is_success() {
            return Err(DexError::Transient(format!(
                "HTTP {}: {}",
                status, response_text
            )));
        }

        let account_response: LighterAccountResponse = serde_json::from_str(&response_text)
            .map_err(|e| DexError::Transient(format!("Failed to parse response: {}", e)))?;

        if account_response.accounts.is_empty() {
            return Err(DexError::Transient("No account found".to_string()));
        }

        let account = &account_response.accounts[0];

        // Check if there are any open positions (position != "0.00000")
        let mut has_positions = false;
        for position in &account.positions {
            if let Some(ref sym) = symbol {
                if position.symbol != *sym {
                    continue;
                }
            }
            if let Ok(pos_size) = position.position.parse::<f64>() {
                if pos_size.abs() > 0.0 {
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
            log::info!("No open positions found (threshold: > 0.0), nothing to close");
            // Log all positions for debugging
            for position in &account.positions {
                if let Ok(pos_size) = position.position.parse::<f64>() {
                    if pos_size.abs() > 0.0 {
                        log::debug!("Small position below threshold: market_id={}, symbol={}, size={} (abs: {})",
                                   position.market_id, position.symbol, position.position, pos_size.abs());
                    }
                }
            }
            return Ok(());
        }

        // Close each open position by placing market orders in opposite direction
        for position in &account.positions {
            if let Some(ref sym) = symbol {
                if position.symbol != *sym {
                    continue;
                }
            }
            if let Ok(pos_size) = position.position.parse::<f64>() {
                if pos_size.abs() > 0.0 {
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
                    let market_info = match self.resolve_market_info(&position.symbol).await {
                        Ok(info) => info,
                        Err(err) => {
                            log::warn!(
                                "Failed to resolve market info for {} (market_id={}): {}. Skipping position close",
                                position.symbol,
                                market_id,
                                err
                            );
                            continue;
                        }
                    };

                    // Use rust_decimal for precise conversion to avoid floating point errors
                    let pos_decimal =
                        rust_decimal::Decimal::from_str(&position.position.replace('-', ""))
                            .unwrap_or_else(|_| {
                                // Fallback: convert to string first then parse
                                let pos_str = format!("{:.8}", pos_size.abs());
                                rust_decimal::Decimal::from_str(&pos_str)
                                    .unwrap_or(rust_decimal::Decimal::ZERO)
                            });
                    let mut base_amount = match scale_decimal_to_u64(
                        pos_decimal,
                        market_info.size_decimals,
                        RoundingStrategy::ToZero,
                        "position base amount",
                    ) {
                        Ok(value) => value,
                        Err(err) => {
                            log::warn!(
                                "Failed to scale position size {} with {} decimals: {}. Falling back to default decimals {}",
                                pos_decimal,
                                market_info.size_decimals,
                                err,
                                DEFAULT_SIZE_DECIMALS
                            );
                            scale_decimal_to_u64(
                                pos_decimal,
                                DEFAULT_SIZE_DECIMALS,
                                RoundingStrategy::ToZero,
                                "fallback position base amount",
                            )
                            .unwrap_or(0)
                        }
                    };

                    // Ensure minimum of 1 unit for very small positions
                    if base_amount == 0 && pos_size.abs() > 0.0 {
                        base_amount = 1;
                        log::debug!(
                            "Position too small for conversion, using minimum base_amount=1"
                        );
                    }

                    log::debug!(
                        "Converting position {} to base_amount: {} (original: {}, decimal: {})",
                        position.position,
                        base_amount,
                        pos_size,
                        pos_decimal
                    );

                    // Create reduce-only market order to close position (requires less margin)
                    log::info!(
                        "Placing reduce-only market order to close position: market_id={}, side={}, size={}",
                        market_id, order_side, pos_decimal
                    );

                    // Get current price for market order using ticker data
                    let ticker_price = match self.get_ticker(&position.symbol, None).await {
                        Ok(ticker) => ticker.price,
                        Err(e) => {
                            log::warn!(
                                "Failed to fetch ticker for {} while closing position: {}. Using fallback price",
                                position.symbol,
                                e
                            );
                            rust_decimal::Decimal::new(50000, 0)
                        }
                    };

                    let protection_price = if order_side == 1 {
                        // Sell: set low protection price
                        ticker_price * rust_decimal::Decimal::new(700, 3) // 30% below market
                    } else {
                        // Buy: set high protection price
                        ticker_price * rust_decimal::Decimal::new(1300, 3) // 30% above market
                    };

                    let current_price = match scale_decimal_to_u32(
                        protection_price,
                        market_info.price_decimals,
                        RoundingStrategy::MidpointAwayFromZero,
                        "close-position protection price",
                    ) {
                        Ok(value) => u64::from(value),
                        Err(err) => {
                            log::warn!(
                                "Failed to scale protection price {} with {} decimals: {}. Using fallback 0",
                                protection_price,
                                market_info.price_decimals,
                                err
                            );
                            0
                        }
                    };

                    // Create reduce-only market order directly
                    match self
                        .create_order_native_with_type(
                            market_id as u32,
                            order_side as u32,
                            0, // IOC time in force
                            base_amount,
                            current_price,
                            None,
                            1,    // Market order type
                            true, // reduce_only=true for position closing (prevents overshooting)
                            None, // No expiry for position closing
                        )
                        .await
                    {
                        Ok(response) => {
                            log::info!(
                                "Successfully submitted reduce-only close order for {} position in market {}: Order ID {}",
                                position.symbol,
                                market_id,
                                response.order_id
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

    async fn is_upcoming_maintenance(&self, hours_ahead: i64) -> bool {
        // Operator kill-switch. When `LIGHTER_MAINTENANCE_DISABLED=1` is set,
        // never report an upcoming maintenance window. The background
        // refresher (see `start_maintenance_refresher`) honors the same env
        // var and skips spawning, so the cache stays empty and this branch
        // is the only thing producing a result. See bot-strategy#32.
        if matches!(
            std::env::var("LIGHTER_MAINTENANCE_DISABLED").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE")
        ) {
            return false;
        }

        // Pure cache read. The actual REST fetch happens off the hot path
        // in the background refresher task spawned at start(). If the cache
        // is empty (refresher hasn't completed its first iteration yet, or
        // every fetch since startup has failed) we treat that as "no
        // upcoming maintenance" — the same default behavior the previous
        // inline-fetch design returned on first call. See bot-strategy#160.
        let now = Utc::now();
        let cached_start = {
            let info = self.maintenance.read().await;
            info.next_start
        };
        let res = maintenance_within_window(cached_start, &now, hours_ahead);
        log::debug!(
            "Lighter maintenance check (cached): start={:?} now={} result={}",
            cached_start,
            now,
            res
        );
        res
    }

    async fn maintenance_status(&self, hours_ahead: i64) -> Option<String> {
        if matches!(
            std::env::var("LIGHTER_MAINTENANCE_DISABLED").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE")
        ) {
            return None;
        }

        let degraded_reason = {
            let detector = self
                .outage_detector
                .lock()
                .expect("outage detector poisoned");
            detector.reason()
        };
        if degraded_reason.is_some() {
            return Some("degraded_observed".to_string());
        }

        if self.is_upcoming_maintenance(hours_ahead).await {
            Some("upcoming_or_active".to_string())
        } else {
            None
        }
    }

    async fn sign_evm_65b(&self, message: &str) -> Result<String, DexError> {
        use ethers::signers::{LocalWallet, Signer};
        use std::str::FromStr;

        let private_key = self
            .evm_wallet_private_key
            .as_ref()
            .ok_or_else(|| DexError::Permanent("EVM wallet private key not set".to_string()))?;
        let cleaned_key = private_key.strip_prefix("0x").unwrap_or(private_key);
        let wallet = LocalWallet::from_str(cleaned_key)
            .map_err(|e| DexError::Permanent(format!("Invalid private key: {}", e)))?;

        let signature = wallet
            .sign_message(message.as_bytes())
            .await
            .map_err(|e| DexError::Permanent(format!("Signing failed: {}", e)))?;

        Ok(format!("0x{}", signature))
    }

    async fn sign_evm_65b_with_eip191(&self, message: &str) -> Result<String, DexError> {
        // EIP-191 adds the prefix "\x19Ethereum Signed Message:\n" + message.len() + message
        let prefixed = format!("\x19Ethereum Signed Message:\n{}{}", message.len(), message);
        self.sign_evm_65b(&prefixed).await
    }

    fn subscribe_price_updates(
        &self,
    ) -> Result<tokio::sync::broadcast::Receiver<crate::PriceUpdate>, DexError> {
        Ok(self.price_update_tx.subscribe())
    }
}

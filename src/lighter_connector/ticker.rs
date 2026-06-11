//! Read-only market-data helpers for `LighterConnector` — ticker, recent
//! trades, and order-book snapshots.
//!
//! Extracted from `dex_impl.rs` (bot-strategy#501 item 1): the
//! `DexConnector` trait methods `get_ticker` / `get_last_trades` /
//! `get_order_book` now delegate into the `fetch_*` inherent helpers below.
//! Behaviour is unchanged from the prior in-`dex_impl.rs` implementation.

use super::*;

impl LighterConnector {
    pub(super) async fn fetch_ticker(
        &self,
        symbol: &str,
        test_price: Option<Decimal>,
    ) -> Result<TickerResponse, DexError> {
        if let Some(price) = test_price {
            let min_tick = calculate_min_tick(price, DEFAULT_PRICE_DECIMALS, false);
            return Ok(TickerResponse {
                symbol: symbol.to_string(),
                price,
                min_tick: Some(min_tick),
                min_order: None,
                size_decimals: None,
                volume: Some(Decimal::ZERO),
                num_trades: None,
                open_interest: None,
                funding_rate: None,
                oracle_price: None,
                exchange_ts: None,
            });
        }

        let market_info = self.resolve_market_info(symbol).await?;
        let canonical_symbol = market_info.canonical_symbol.clone();
        let min_order = market_info.min_order;

        // Funding rate is fed by the `market_stats/{market_id}` WS channel
        // (bot-strategy#162). Cold-start before the first WS push returns None,
        // matching the prior REST error-path fallback. The switch also moves
        // us from the accidental binance rate (first `.find()` hit in the REST
        // list) to Lighter's own rate — behavior change is immaterial under
        // production config (`funding_entry_z_scale=0`, `net_funding_min_per_hour=-0.01`).
        let funding_rate_from_ws = self
            .funding_rate_cache
            .read()
            .await
            .get(&market_info.market_id)
            .copied();

        // Try to get price from WebSocket first, but check if it's recent
        if let Some((ws_price, price_timestamp)) = self
            .current_price
            .read()
            .await
            .get(&canonical_symbol)
            .copied()
        {
            let current_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            // Check if WebSocket price is stale (older than 30 seconds).
            // `price_timestamp` is now ms (bot-strategy#274 / #276).
            let price_age_ms = current_time.saturating_sub(price_timestamp);
            if price_age_ms > 30_000 {
                log::warn!(
                    "WebSocket price is stale ({}ms old), falling back to REST API",
                    price_age_ms
                );
                // Fall through to REST API fallback below
            } else {
                let min_tick = calculate_min_tick(ws_price, market_info.price_decimals, false);

                // exchangeStats was dead data — volume / num_trades have no
                // downstream consumer (pairtrade, slow-mm). The WS-price path
                // reports volume=0 / num_trades=None. See bot-strategy#128.
                let (volume, num_trades) = (Some(Decimal::ZERO), None);

                let funding_rate = funding_rate_from_ws;

                log::trace!(
                    "Using WebSocket price: price={}, volume={:?}, trades={:?}",
                    ws_price,
                    volume,
                    num_trades
                );

                return Ok(TickerResponse {
                    symbol: symbol.to_string(),
                    price: ws_price,
                    min_tick: Some(min_tick),
                    min_order,
                    size_decimals: Some(market_info.size_decimals),
                    volume,
                    num_trades,
                    open_interest: None,
                    funding_rate,
                    oracle_price: None,
                    exchange_ts: Some(price_timestamp),
                });
            }
        }

        // Fallback to REST API if WebSocket data is not available.
        // Routed through `fetch_text_with_waf_guard` so the first 429 engages
        // `lighter_waf_cooldown` and subsequent calls within the cooldown
        // window shed locally without sending HTTP. Without this gating the
        // bot's own retry storm extends Lighter's lockout (bot-strategy#281).
        log::warn!("WebSocket data not available, falling back to REST API");

        let market_id = market_info.market_id;
        let endpoint = format!("/api/v1/recentTrades?market_id={}&limit=100", market_id);
        let url = format!("{}{}", self.base_url, endpoint);

        let response_text = self.fetch_text_with_waf_guard(&url, "recentTrades").await?;

        log::trace!("Trades API response: {}", response_text);

        let trades_response: LighterTradesResponse = serde_json::from_str(&response_text)
            .map_err(|e| DexError::Transient(format!("Failed to parse response: {}", e)))?;

        let price = if let Some(trade) = trades_response.trades.first() {
            string_to_decimal(Some(trade.price.clone()))?
        } else {
            // Fallback to default if no trades found
            Decimal::new(50000, 0)
        };

        let min_tick = calculate_min_tick(price, market_info.price_decimals, false);

        // Funding rate comes from the WS cache populated by market_stats.
        let funding_rate = funding_rate_from_ws;

        // exchangeStats was dead data (bot-strategy#128); volume / num_trades
        // are derived from the recentTrades response only.
        let volume = trades_response
            .trades
            .iter()
            .map(|trade| string_to_decimal(Some(trade.size.clone())))
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .sum();
        let (volume, num_trades) = (Some(volume), Some(trades_response.trades.len() as u64));

        Ok(TickerResponse {
            symbol: symbol.to_string(),
            price,
            min_tick: Some(min_tick),
            min_order,
            size_decimals: Some(market_info.size_decimals),
            volume,
            num_trades,
            open_interest: None,
            funding_rate,
            oracle_price: None,
            // REST fallback path: no exchange ts available, leave as None so
            // callers know to fall back to local clock for bucketing.
            exchange_ts: None,
        })
    }

    pub(super) async fn fetch_last_trades(
        &self,
        symbol: &str,
    ) -> Result<LastTradesResponse, DexError> {
        // Get market_id for the symbol
        let market_id = self.resolve_market_info(symbol).await?.market_id;

        // Query recent trades
        let endpoint = format!("/api/v1/recentTrades?market_id={}&limit=10", market_id);

        let url = format!("{}{}", self.base_url, endpoint);
        let response_text = self.fetch_text_with_waf_guard(&url, "recentTrades").await?;

        log::debug!("Last trades API response: {}", response_text);

        let trades_response: LighterTradesResponse = serde_json::from_str(&response_text)
            .map_err(|e| DexError::Transient(format!("Failed to parse response: {}", e)))?;

        let trades = trades_response
            .trades
            .into_iter()
            .map(|t| LastTrade {
                price: string_to_decimal(Some(t.price)).unwrap_or_default(),
                size: string_to_decimal(Some(t.size)).ok(),
                side: map_side(t.side.as_deref()),
            })
            .collect();

        Ok(LastTradesResponse { trades })
    }

    pub(super) async fn fetch_order_book(
        &self,
        symbol: &str,
        depth: usize,
    ) -> Result<OrderBookSnapshot, DexError> {
        let market_id = self.resolve_market_info(symbol).await?.market_id;
        let (ob, updated_at) = {
            let ob_guard = self.order_book.read().await;
            if let Some(entry) = ob_guard.get(&market_id) {
                (entry.order_book.clone(), entry.updated_at)
            } else {
                log::debug!(
                    "order book snapshot unavailable for {} (market_id={}, cached_entries={})",
                    symbol,
                    market_id,
                    ob_guard.len()
                );
                self.record_outage_failure(OutageSignal::ReadMid);
                return Err(DexError::Transient(
                    "order book snapshot unavailable (no recent update)".to_string(),
                ));
            }
        };
        if updated_at.elapsed() > self.ob_stale_after {
            // Don't remove stale entry from cache — it may be needed as a
            // baseline for delta merges after WS reconnection. The entry will
            // be replaced once a fresh update arrives. See: dex-connector#2
            self.record_outage_failure(OutageSignal::ReadMid);
            return Err(DexError::Transient(
                "order book snapshot unavailable (no recent update)".to_string(),
            ));
        }
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        for entry in ob.bids.iter().take(depth) {
            if let (Ok(price), Ok(size)) = (
                string_to_decimal(Some(entry.price.clone())),
                string_to_decimal(Some(entry.size.clone())),
            ) {
                bids.push(OrderBookLevel { price, size });
            }
        }
        for entry in ob.asks.iter().take(depth) {
            if let (Ok(price), Ok(size)) = (
                string_to_decimal(Some(entry.price.clone())),
                string_to_decimal(Some(entry.size.clone())),
            ) {
                asks.push(OrderBookLevel { price, size });
            }
        }
        self.record_outage_success(OutageSignal::ReadMid);
        Ok(OrderBookSnapshot { bids, asks })
    }
}

//! Private order-placement helpers for ExtendedConnector. Owns the
//! create / IOC / refresh-on-invalid-price retry path, the StarkNet
//! settlement wiring that hands off to `signing`, and the REST
//! `/user/trades` fill-fetch path used by the public DexConnector
//! methods that remain in `mod.rs`.
//!
//! `bootstrap_order_id_map` also lives here so the recovery seed and
//! the live trade path can share the same `OpenOrderModel` shape.

use chrono::{Duration, Utc};
use rust_decimal::Decimal;

use crate::dex_connector::{slippage_price, DexConnector};
use crate::dex_request::DexError;
use crate::{CreateOrderResponse, FilledOrder, OrderBookLevel, OrderBookSnapshot, OrderSide};

use super::models::{
    AccountTradeModel, MarketModel, OpenOrderModel, OrderbookUpdateModel, PlacedOrderModel,
};
use super::pricing;
use super::rest::build_query;
use super::signing::NewOrderModel;
use super::ExtendedConnector;

impl ExtendedConnector {
    pub(super) async fn get_order_book_rest(
        &self,
        symbol: &str,
        depth: usize,
    ) -> Result<OrderBookSnapshot, DexError> {
        // Same bare-symbol → market-name resolution as get_order_book below.
        let market = self.get_market(symbol).await?;
        let path = format!("/info/markets/{}/orderbook", market.name);
        let snapshot: OrderbookUpdateModel = self.api.get(path, true).await?;
        let bids = snapshot
            .bid
            .into_iter()
            .take(depth)
            .map(|level| OrderBookLevel {
                price: level.price,
                size: level.qty,
            })
            .collect::<Vec<_>>();
        let asks = snapshot
            .ask
            .into_iter()
            .take(depth)
            .map(|level| OrderBookLevel {
                price: level.price,
                size: level.qty,
            })
            .collect::<Vec<_>>();
        Ok(OrderBookSnapshot { bids, asks })
    }

    pub(super) async fn choose_base_price(
        &self,
        symbol: &str,
        side: OrderSide,
        explicit_price: Option<Decimal>,
    ) -> Result<Decimal, DexError> {
        if let Some(px) = explicit_price {
            return Ok(px);
        }

        // Prefer top of book to stay within current price band
        if let Ok(ob) = self.get_order_book(symbol, 1).await {
            match side {
                OrderSide::Long => {
                    if let Some(level) = ob.asks.first() {
                        return Ok(level.price);
                    }
                }
                OrderSide::Short => {
                    if let Some(level) = ob.bids.first() {
                        return Ok(level.price);
                    }
                }
            }
        }

        if let Ok(ob) = self.get_order_book_rest(symbol, 1).await {
            match side {
                OrderSide::Long => {
                    if let Some(level) = ob.asks.first() {
                        return Ok(level.price);
                    }
                }
                OrderSide::Short => {
                    if let Some(level) = ob.bids.first() {
                        return Ok(level.price);
                    }
                }
            }
        }

        // Fall back to market stats when WS data isn't ready
        let market = self.get_market(symbol).await?;
        let base_price = if market.market_stats.index_price > Decimal::ZERO {
            market.market_stats.index_price
        } else {
            market.market_stats.last_price
        };
        Ok(slippage_price(base_price, side == OrderSide::Long))
    }

    pub(super) async fn fetch_filled_orders_via_http(
        &self,
        symbol: &str,
    ) -> Result<Vec<FilledOrder>, DexError> {
        // Extended REST rejects bare tokens ("BTC") with {"code":1001,
        // "message":"Market not found"} — `?market=` expects the
        // collateral-qualified market name ("BTC-USD"). Callers across
        // pairtrade use bare tokens.
        let market_name = self.get_market(symbol).await?.name;
        let path = build_query(
            "/user/trades",
            vec![("market".to_string(), market_name.clone())],
        );
        let trades: Vec<AccountTradeModel> = self.api.get(path, true).await?;
        let mut needs_history = false;
        {
            let map = self.order_id_map.read().await;
            for trade in &trades {
                if !map.contains_key(&trade.order_id) {
                    needs_history = true;
                    break;
                }
            }
        }
        if needs_history {
            let history_path = build_query(
                "/user/orders/history",
                vec![("market".to_string(), market_name.clone())],
            );
            let orders_history: Vec<OpenOrderModel> = self.api.get(history_path, true).await?;
            let mut map = self.order_id_map.write().await;
            for order in orders_history {
                map.insert(order.id, order.external_id.clone());
            }
        }
        let map = self.order_id_map.read().await;
        let orders = trades
            .into_iter()
            .map(|trade| {
                let order_id = map
                    .get(&trade.order_id)
                    .cloned()
                    .unwrap_or_else(|| trade.order_id.to_string());
                FilledOrder {
                    order_id,
                    is_rejected: false,
                    trade_id: trade.id.to_string(),
                    filled_side: match trade.side.as_str() {
                        "BUY" => Some(OrderSide::Long),
                        "SELL" => Some(OrderSide::Short),
                        _ => None,
                    },
                    filled_size: Some(trade.qty),
                    filled_value: Some(trade.value),
                    filled_fee: Some(trade.fee),
                    filled_ts_ms: Some(trade.created_time),
                }
            })
            .collect::<Vec<_>>();
        Ok(orders)
    }

    // Internal recursive helper: `refreshed` is a one-shot reentry guard for
    // the market-refresh retry, so the parameter list grows by one beyond the
    // public order params. A struct refactor would obscure the recursion site
    // for no real readability win.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn create_order_internal(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        price: Option<Decimal>,
        reduce_only: bool,
        expiry_secs: Option<u64>,
        post_only: bool,
        refreshed: bool,
    ) -> Result<CreateOrderResponse, DexError> {
        let mut refreshed_once = refreshed;
        let mut fallback_price: Option<Decimal> = None;
        loop {
            let market = if refreshed_once {
                self.refresh_market(symbol).await?
            } else {
                self.get_market(symbol).await?
            };

            let order_price = match fallback_price {
                Some(px) => px,
                None => self.choose_base_price(symbol, side, price).await?,
            };

            match self
                .submit_order_with_market(
                    &market,
                    size,
                    side,
                    order_price,
                    reduce_only,
                    expiry_secs,
                    post_only,
                )
                .await
            {
                Ok(res) => return Ok(res),
                Err(err) if !refreshed_once && pricing::is_invalid_price_error(&err) => {
                    log::warn!(
                        "[create_order][extended] Invalid price for {}; refreshing market data and retrying once (raw_price={} min_tick={} floor={} cap={})",
                        symbol,
                        order_price,
                        market.trading_config.min_price_change,
                        market.trading_config.limit_price_floor,
                        market.trading_config.limit_price_cap
                    );
                    refreshed_once = true;
                    continue;
                }
                Err(err) if fallback_price.is_none() && pricing::is_invalid_price_error(&err) => {
                    let level_price =
                        self.get_cached_order_book(symbol, 1)
                            .await
                            .and_then(|snap| match side {
                                OrderSide::Long => snap.asks.first().map(|l| l.price),
                                OrderSide::Short => snap.bids.first().map(|l| l.price),
                            });
                    if let Some(px) = level_price {
                        log::warn!(
                            "[create_order][extended] Invalid price persisted; retrying with best level price {} for {}",
                            px,
                            symbol
                        );
                        fallback_price = Some(px);
                        refreshed_once = true;
                        continue;
                    }

                    log::warn!(
                        "[create_order][extended] Invalid price persisted and no orderbook price available for {}; giving up",
                        symbol
                    );
                    return Err(err);
                }
                Err(err) => return Err(err),
            }
        }
    }

    #[allow(clippy::too_many_arguments)] // mirrors the public order-param shape; struct wrapping adds no clarity here.
    pub(super) async fn submit_order_with_market(
        &self,
        market: &MarketModel,
        size: Decimal,
        side: OrderSide,
        order_price: Decimal,
        reduce_only: bool,
        expiry_secs: Option<u64>,
        post_only: bool,
    ) -> Result<CreateOrderResponse, DexError> {
        let expire_time = match expiry_secs {
            Some(secs) => Utc::now() + Duration::seconds(secs as i64),
            None => Utc::now() + Duration::hours(1),
        };

        let nonce = rand::random::<u32>() as u64;
        let rounded_size = pricing::round_size_for_market(size, market)?;
        let rounded_price = pricing::round_price_for_market(order_price, market, side);
        let side_str = match side {
            OrderSide::Long => "BUY",
            OrderSide::Short => "SELL",
        };
        if rounded_price <= Decimal::ZERO {
            return Err(DexError::InvalidInput {
                field: "price".to_string(),
                value: format!(
                    "rounded {} is non-positive for {}",
                    rounded_price, market.name
                ),
            });
        }
        let tc = &market.trading_config;
        log::debug!(
            "[create_order][extended] sym={} side={} raw_price={} rounded_price={} tick={} floor={} cap={} raw_size={} rounded_size={} post_only={}",
            market.name,
            side_str,
            order_price,
            rounded_price,
            tc.min_price_change,
            tc.limit_price_floor,
            tc.limit_price_cap,
            size,
            rounded_size,
            post_only
        );

        let settlement = self.compute_settlement(
            market,
            side_str,
            rounded_size,
            rounded_price,
            expire_time,
            nonce,
        )?;

        let order_id = settlement.order_hash.to_string();

        let order = NewOrderModel {
            id: order_id.clone(),
            market: market.name.clone(),
            order_type: "LIMIT".to_string(),
            side: side_str.to_string(),
            qty: rounded_size,
            price: rounded_price,
            reduce_only,
            post_only,
            time_in_force: "GTT".to_string(),
            expiry_epoch_millis: Self::to_epoch_millis(expire_time),
            fee: settlement.fee_rate,
            self_trade_protection_level: "ACCOUNT".to_string(),
            nonce: Decimal::from(nonce),
            cancel_id: None,
            settlement: Some(settlement.settlement),
            tp_sl_type: None,
            take_profit: None,
            stop_loss: None,
            debugging_amounts: Some(settlement.debugging_amounts),
            builder_fee: None,
            builder_id: None,
        };

        let response: PlacedOrderModel = self
            .api
            .post("/user/order".to_string(), order, true)
            .await?;

        Ok(CreateOrderResponse {
            order_id: response.external_id,
            exchange_order_id: Some(response.id.to_string()),
            ordered_price: rounded_price,
            ordered_size: rounded_size,
            client_order_id: None,
        })
    }

    /// bot-strategy#302: low-level IOC submit. Mirrors the
    /// `close_all_positions` IOC path but lets the caller drive size /
    /// side / reduce_only directly so it can be reused for entry-side
    /// taker semantics (where `close_all_positions` would refuse —
    /// no resting position to close).
    ///
    /// Pricing matches `close_all_positions`: top-of-book ± 1 tick
    /// (aggressive) ± `slippage_bps`, falling back to market stats +
    /// default 5% slippage if the order book is unreachable. The
    /// venue's IOC TIF terminates the order on first match (filled or
    /// zero-fill cancel) within ~ms, so callers do not need to chase or
    /// cancel — `poll_fill_status` will see a terminal response on the
    /// next read.
    pub(super) async fn submit_taker_ioc(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        slippage_bps: u32,
        reduce_only: bool,
    ) -> Result<CreateOrderResponse, DexError> {
        let market = self.get_market(symbol).await?;
        let side_str = match side {
            OrderSide::Long => "BUY",
            OrderSide::Short => "SELL",
        };
        let tick = market.trading_config.min_price_change;

        // Top of opposing side; REST fallback if WS cache empty.
        let mut base_price: Option<Decimal> = None;
        if let Ok(ob) = self.get_order_book(symbol, 1).await {
            base_price = match side {
                OrderSide::Long => ob.asks.first().map(|level| level.price),
                OrderSide::Short => ob.bids.first().map(|level| level.price),
            };
        }
        if base_price.is_none() {
            if let Ok(ob) = self.get_order_book_rest(symbol, 1).await {
                base_price = match side {
                    OrderSide::Long => ob.asks.first().map(|level| level.price),
                    OrderSide::Short => ob.bids.first().map(|level| level.price),
                };
            }
        }

        let (mut order_price, used_market_stats) = if let Some(px) = base_price {
            (px, false)
        } else {
            let stats_price = if market.market_stats.index_price > Decimal::ZERO {
                market.market_stats.index_price
            } else {
                market.market_stats.last_price
            };
            (stats_price, true)
        };
        if used_market_stats {
            order_price = slippage_price(order_price, side == OrderSide::Long);
        } else {
            if tick > Decimal::ZERO {
                match side {
                    OrderSide::Long => order_price += tick,
                    OrderSide::Short => order_price -= tick,
                }
            }
            order_price = pricing::apply_close_slippage_bps(order_price, slippage_bps, side);
        }
        let rounded_price = pricing::round_price_for_market_aggressive(order_price, &market, side);
        let rounded_size = pricing::round_size_for_market(size, &market)?;

        let expire_time = Utc::now() + Duration::hours(1);
        let nonce = rand::random::<u32>() as u64;
        let settlement = self.compute_settlement(
            &market,
            side_str,
            rounded_size,
            rounded_price,
            expire_time,
            nonce,
        )?;

        let order = NewOrderModel {
            id: settlement.order_hash.to_string(),
            market: market.name.clone(),
            order_type: "LIMIT".to_string(),
            side: side_str.to_string(),
            qty: rounded_size,
            price: rounded_price,
            reduce_only,
            post_only: false,
            time_in_force: "IOC".to_string(),
            expiry_epoch_millis: Self::to_epoch_millis(expire_time),
            fee: settlement.fee_rate,
            self_trade_protection_level: "ACCOUNT".to_string(),
            nonce: Decimal::from(nonce),
            cancel_id: None,
            settlement: Some(settlement.settlement),
            tp_sl_type: None,
            take_profit: None,
            stop_loss: None,
            debugging_amounts: Some(settlement.debugging_amounts),
            builder_fee: None,
            builder_id: None,
        };

        log::info!(
            "[create_order_taker_ioc] {} side={} size={} price={} tif=IOC reduce_only={} source={} slippage_bps={}",
            symbol,
            side_str,
            rounded_size,
            rounded_price,
            reduce_only,
            if used_market_stats { "stats" } else { "order_book" },
            if used_market_stats { 0 } else { slippage_bps },
        );

        let response: PlacedOrderModel = self
            .api
            .post("/user/order".to_string(), order, true)
            .await?;

        Ok(CreateOrderResponse {
            order_id: response.external_id,
            exchange_order_id: Some(response.id.to_string()),
            ordered_price: rounded_price,
            ordered_size: rounded_size,
            client_order_id: None,
        })
    }

    /// Fetch recent order history via REST and seed `order_id_map`.
    /// Best-effort: failures are logged and ignored so startup isn't
    /// blocked by a transient Extended 5xx. bot-strategy#206.
    pub(super) async fn bootstrap_order_id_map(&self) -> Result<(), DexError> {
        for symbol in self.tracked_symbols.iter() {
            let market_name = match self.get_market(symbol).await {
                Ok(m) => m.name,
                Err(e) => {
                    log::warn!(
                        "[order_id_map][extended] get_market({}) failed during bootstrap: {:?}",
                        symbol,
                        e
                    );
                    continue;
                }
            };
            let path = build_query(
                "/user/orders/history",
                vec![("market".to_string(), market_name.clone())],
            );
            let history: Vec<OpenOrderModel> = match self.api.get(path, true).await {
                Ok(v) => v,
                Err(e) => {
                    log::warn!(
                        "[order_id_map][extended] /user/orders/history({}) failed: {:?}",
                        market_name,
                        e
                    );
                    continue;
                }
            };
            let seeded = history.len();
            let mut map = self.order_id_map.write().await;
            for order in history {
                map.insert(order.id, order.external_id.clone());
            }
            log::info!(
                "[order_id_map][extended] bootstrap seeded {} entries for {}",
                seeded,
                market_name
            );
        }
        Ok(())
    }
}

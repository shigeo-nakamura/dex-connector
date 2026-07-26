mod book;
mod models;
mod rest;
mod ws;

use self::book::{BookState, BookTop};
use self::models::MarketInfo;
use crate::{
    dex_connector::DexConnector, dex_request::DexError, BalanceResponse, CanceledOrdersResponse,
    CombinedBalanceResponse, CreateOrderResponse, FilledOrdersResponse, LastTradesResponse,
    OpenOrdersResponse, OrderBookSnapshot, OrderSide, PositionSnapshot, PriceUpdate,
    TickerResponse, TpSl, TriggerOrderStyle,
};
use async_trait::async_trait;
use reqwest::Client;
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio::task::JoinHandle;

const DEFAULT_BASE_URL: &str = "https://api.arcus.xyz";
const DEFAULT_WEBSOCKET_URL: &str = "wss://api.arcus.xyz/v1/ws";
const DEFAULT_OB_STALE_SECS: u64 = 10;
// Cached market metadata (funding rate, oracle price, open interest, volume,
// trade count) refresh interval. Without this, a healthy order-book stream
// would let get_ticker return the metadata snapshot cached at start()
// indefinitely (bot-strategy#749 review).
pub(super) const MARKET_METADATA_TTL_MS: u64 = 30_000;

type BookCache = Arc<RwLock<HashMap<String, BookState>>>;

#[derive(Clone, Debug)]
pub struct ArcusConnectorConfig {
    pub base_url: String,
    pub websocket_url: String,
    pub tracked_symbols: Vec<String>,
    pub ob_stale_secs: Option<u64>,
}

impl Default for ArcusConnectorConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            websocket_url: DEFAULT_WEBSOCKET_URL.to_string(),
            tracked_symbols: Vec::new(),
            ob_stale_secs: None,
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct TrackedMarket {
    pub(super) market: String,
    pub(super) output_symbol: String,
}

pub struct ArcusConnector {
    base_url: String,
    websocket_url: String,
    client: Client,
    tracked_markets: Vec<TrackedMarket>,
    ob_stale_ms: u64,
    market_info: Arc<RwLock<HashMap<String, MarketInfo>>>,
    market_info_fetched_ms: Arc<RwLock<HashMap<String, u64>>>,
    books: BookCache,
    price_update_tx: broadcast::Sender<PriceUpdate>,
    ws_task: Mutex<Option<JoinHandle<()>>>,
}

pub fn create_arcus_connector(
    config: ArcusConnectorConfig,
) -> Result<Box<dyn DexConnector>, DexError> {
    Ok(Box::new(ArcusConnector::new(config)?))
}

impl ArcusConnector {
    pub fn new(config: ArcusConnectorConfig) -> Result<Self, DexError> {
        let client = Client::builder()
            .user_agent("debot/1.0")
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(15))
            .build()?;
        let base_url = nonempty_url(config.base_url, DEFAULT_BASE_URL);
        let websocket_url = nonempty_url(config.websocket_url, DEFAULT_WEBSOCKET_URL);
        let tracked_markets = build_tracked_markets(config.tracked_symbols);
        let ob_stale_ms = config
            .ob_stale_secs
            .unwrap_or(DEFAULT_OB_STALE_SECS)
            .max(1)
            .saturating_mul(1_000);
        let (price_update_tx, _) = broadcast::channel(1_024);

        Ok(Self {
            base_url,
            websocket_url,
            client,
            tracked_markets,
            ob_stale_ms,
            market_info: Arc::new(RwLock::new(HashMap::new())),
            market_info_fetched_ms: Arc::new(RwLock::new(HashMap::new())),
            books: Arc::new(RwLock::new(HashMap::new())),
            price_update_tx,
            ws_task: Mutex::new(None),
        })
    }

    async fn cached_top(&self, market: &str) -> Option<BookTop> {
        let now = now_ms();
        self.books
            .read()
            .await
            .get(market)
            .filter(|book| book.is_fresh(now, self.ob_stale_ms))
            .and_then(BookState::top)
    }

    fn unsupported(operation: &str) -> DexError {
        DexError::Permanent(format!(
            "Arcus {operation} is not enabled in the read-only connector slice (bot-strategy#749)"
        ))
    }
}

#[async_trait]
impl DexConnector for ArcusConnector {
    async fn start(&self) -> Result<(), DexError> {
        self.refresh_market_metadata().await?;
        {
            let markets = self.market_info.read().await;
            for tracked in &self.tracked_markets {
                let info = markets.get(&tracked.market).ok_or_else(|| {
                    DexError::Permanent(format!(
                        "Arcus tracked market not found: {}",
                        tracked.market
                    ))
                })?;
                if info.status != "ONLINE" {
                    return Err(DexError::Permanent(format!(
                        "Arcus tracked market is not ONLINE: {} status={}",
                        info.market, info.status
                    )));
                }
            }
        }

        let mut task = self.ws_task.lock().await;
        if task.as_ref().is_some_and(|handle| !handle.is_finished()) {
            return Ok(());
        }
        if let Some(finished) = task.take() {
            finished.abort();
        }

        if self.tracked_markets.is_empty() {
            log::info!("[arcus] read-only connector started without WS subscriptions");
            return Ok(());
        }

        let websocket_url = self.websocket_url.clone();
        let tracked_markets = self.tracked_markets.clone();
        let books = Arc::clone(&self.books);
        let price_update_tx = self.price_update_tx.clone();
        *task = Some(tokio::spawn(async move {
            ws::websocket_loop(websocket_url, tracked_markets, books, price_update_tx).await;
        }));
        log::info!(
            "[arcus] read-only connector started (markets={})",
            self.tracked_markets
                .iter()
                .map(|tracked| tracked.market.as_str())
                .collect::<Vec<_>>()
                .join(",")
        );
        Ok(())
    }

    async fn stop(&self) -> Result<(), DexError> {
        if let Some(task) = self.ws_task.lock().await.take() {
            task.abort();
        }
        for state in self.books.write().await.values_mut() {
            state.reset();
        }
        Ok(())
    }

    async fn restart(&self, max_retries: i32) -> Result<(), DexError> {
        self.stop().await?;
        let attempts = max_retries.max(1) as u32;
        let mut last_error = None;
        for attempt in 1..=attempts {
            match self.start().await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    last_error = Some(err);
                    if attempt < attempts {
                        tokio::time::sleep(Duration::from_secs(attempt.min(5) as u64)).await;
                    }
                }
            }
        }
        Err(last_error.unwrap_or_else(|| {
            DexError::Transient("Arcus restart failed without an error".to_string())
        }))
    }

    async fn set_leverage(&self, _symbol: &str, _leverage: u32) -> Result<(), DexError> {
        Err(Self::unsupported("set_leverage"))
    }

    async fn get_ticker(
        &self,
        symbol: &str,
        test_price: Option<Decimal>,
    ) -> Result<TickerResponse, DexError> {
        let market = normalize_market(symbol);
        let had_top = self.cached_top(&market).await.is_some();
        let info = if had_top {
            self.market_info_for(&market).await?
        } else {
            // With no healthy streaming book, refresh the single-market REST
            // row so mark/oracle/funding values are not the startup snapshot.
            self.fetch_market_info(&market).await?
        };
        // Re-read the cached top after the metadata await (which can take up
        // to the REST client's 15s timeout) instead of reusing the value
        // captured above, so a book that went stale mid-refresh is correctly
        // dropped by the `ob_stale_ms` guard (bot-strategy#749 review).
        let top = self.cached_top(&market).await;
        if info.status != "ONLINE" {
            return Err(DexError::Permanent(format!(
                "Arcus market is not ONLINE: {} status={}",
                info.market, info.status
            )));
        }

        let price = test_price
            .or_else(|| top.map(|book| book.mid))
            .or(info.mark_price)
            .or(info.last_trade_price)
            .ok_or_else(|| {
                DexError::Transient(format!("Arcus {} has no usable price", info.market))
            })?;

        Ok(TickerResponse {
            symbol: symbol.to_string(),
            price,
            min_tick: Some(info.tick_size_for(price)),
            min_order: None,
            size_decimals: Some(info.step_size.normalize().scale()),
            volume: info.volume24h,
            num_trades: info.trades24h,
            open_interest: info.open_interest,
            funding_rate: info.funding_rate,
            oracle_price: info.oracle_price,
            // A caller-supplied test price is synthetic, not an
            // exchange-reported observation; don't attribute it to the
            // unrelated book update's timestamp (bot-strategy#749 review).
            // When Arcus itself omitted the timestamp, `exchange_timestamp_ms`
            // is `None` rather than the local receive time substituted into
            // `timestamp_ms` (bot-strategy#749 review).
            exchange_ts: if test_price.is_some() {
                None
            } else {
                top.and_then(|book| book.exchange_timestamp_ms)
            },
        })
    }

    async fn get_filled_orders(&self, _symbol: &str) -> Result<FilledOrdersResponse, DexError> {
        Err(Self::unsupported("get_filled_orders"))
    }

    async fn get_canceled_orders(&self, _symbol: &str) -> Result<CanceledOrdersResponse, DexError> {
        Err(Self::unsupported("get_canceled_orders"))
    }

    async fn get_open_orders(&self, _symbol: &str) -> Result<OpenOrdersResponse, DexError> {
        Err(Self::unsupported("get_open_orders"))
    }

    async fn get_balance(&self, _symbol: Option<&str>) -> Result<BalanceResponse, DexError> {
        Err(Self::unsupported("get_balance"))
    }

    async fn get_combined_balance(&self) -> Result<CombinedBalanceResponse, DexError> {
        Err(Self::unsupported("get_combined_balance"))
    }

    async fn get_positions(&self) -> Result<Vec<PositionSnapshot>, DexError> {
        Err(Self::unsupported("get_positions"))
    }

    async fn get_last_trades(&self, symbol: &str) -> Result<LastTradesResponse, DexError> {
        self.fetch_last_trades(&normalize_market(symbol)).await
    }

    async fn get_order_book(
        &self,
        symbol: &str,
        depth: usize,
    ) -> Result<OrderBookSnapshot, DexError> {
        let market = normalize_market(symbol);
        let now = now_ms();
        if let Some(snapshot) = self
            .books
            .read()
            .await
            .get(&market)
            .filter(|book| book.is_fresh(now, self.ob_stale_ms))
            .and_then(|book| book.snapshot(depth))
        {
            return Ok(snapshot);
        }
        self.fetch_order_book_rest(&market, depth).await
    }

    async fn clear_filled_order(&self, _symbol: &str, _trade_id: &str) -> Result<(), DexError> {
        Err(Self::unsupported("clear_filled_order"))
    }

    async fn clear_all_filled_orders(&self) -> Result<(), DexError> {
        Err(Self::unsupported("clear_all_filled_orders"))
    }

    async fn clear_canceled_order(&self, _symbol: &str, _order_id: &str) -> Result<(), DexError> {
        Err(Self::unsupported("clear_canceled_order"))
    }

    async fn clear_all_canceled_orders(&self) -> Result<(), DexError> {
        Err(Self::unsupported("clear_all_canceled_orders"))
    }

    async fn create_order(
        &self,
        _symbol: &str,
        _size: Decimal,
        _side: OrderSide,
        _price: Option<Decimal>,
        _spread: Option<i64>,
        _reduce_only: bool,
        _expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        Err(Self::unsupported("create_order"))
    }

    async fn create_advanced_trigger_order(
        &self,
        _symbol: &str,
        _size: Decimal,
        _side: OrderSide,
        _trigger_px: Decimal,
        _limit_px: Option<Decimal>,
        _order_style: TriggerOrderStyle,
        _slippage_bps: Option<u32>,
        _tpsl: TpSl,
        _reduce_only: bool,
        _expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        Err(Self::unsupported("create_advanced_trigger_order"))
    }

    async fn create_order_taker_ioc(
        &self,
        _symbol: &str,
        _size: Decimal,
        _side: OrderSide,
        _slippage_bps: u32,
        _reduce_only: bool,
    ) -> Result<CreateOrderResponse, DexError> {
        Err(Self::unsupported("create_order_taker_ioc"))
    }

    async fn modify_order(
        &self,
        _symbol: &str,
        _order_id: &str,
        _side: OrderSide,
        _target_total_size: Decimal,
        _open_remaining_size: Decimal,
        _price: Option<Decimal>,
        _spread: Option<i64>,
        _reduce_only: bool,
    ) -> Result<CreateOrderResponse, DexError> {
        Err(Self::unsupported("modify_order"))
    }

    async fn cancel_order(&self, _symbol: &str, _order_id: &str) -> Result<(), DexError> {
        Err(Self::unsupported("cancel_order"))
    }

    async fn cancel_all_orders(&self, _symbol: Option<String>) -> Result<(), DexError> {
        Err(Self::unsupported("cancel_all_orders"))
    }

    async fn cancel_orders(
        &self,
        _symbol: Option<String>,
        _order_ids: Vec<String>,
    ) -> Result<(), DexError> {
        Err(Self::unsupported("cancel_orders"))
    }

    async fn close_all_positions(&self, _symbol: Option<String>) -> Result<(), DexError> {
        Err(Self::unsupported("close_all_positions"))
    }

    async fn clear_last_trades(&self, _symbol: &str) -> Result<(), DexError> {
        Err(Self::unsupported("clear_last_trades"))
    }

    async fn is_upcoming_maintenance(&self, _hours_ahead: i64) -> bool {
        if self.tracked_markets.is_empty() {
            return false;
        }
        // Maintenance checks are commonly called right before other venue
        // operations, so they can't rely on a `get_ticker` call having
        // refreshed the cache first. Route through the same TTL-refreshing
        // path as get_ticker, falling back to the last-known status only if
        // the refresh itself fails (bot-strategy#749 review).
        for tracked in &self.tracked_markets {
            match self.market_info_for(&tracked.market).await {
                Ok(info) => {
                    if info.status != "ONLINE" {
                        return true;
                    }
                }
                Err(err) => {
                    log::warn!(
                        "[arcus] maintenance check failed to refresh {}: {err}",
                        tracked.market
                    );
                }
            }
        }
        false
    }

    async fn sign_evm_65b(&self, _message: &str) -> Result<String, DexError> {
        Err(Self::unsupported("sign_evm_65b"))
    }

    async fn sign_evm_65b_with_eip191(&self, _message: &str) -> Result<String, DexError> {
        Err(Self::unsupported("sign_evm_65b_with_eip191"))
    }

    fn subscribe_price_updates(&self) -> Result<broadcast::Receiver<PriceUpdate>, DexError> {
        Ok(self.price_update_tx.subscribe())
    }
}

fn nonempty_url(configured: String, default: &str) -> String {
    let trimmed = configured.trim();
    if trimmed.is_empty() {
        default.to_string()
    } else {
        trimmed.trim_end_matches('/').to_string()
    }
}

pub(super) fn normalize_market(symbol: &str) -> String {
    let normalized = symbol.trim().to_ascii_uppercase().replace('_', "-");
    if normalized.contains('-') {
        normalized
    } else {
        format!("{normalized}-USD")
    }
}

fn build_tracked_markets(symbols: Vec<String>) -> Vec<TrackedMarket> {
    let mut seen = HashSet::new();
    symbols
        .into_iter()
        .filter_map(|symbol| {
            let output_symbol = symbol.trim().to_ascii_uppercase();
            if output_symbol.is_empty() {
                return None;
            }
            let market = normalize_market(&output_symbol);
            seen.insert(market.clone()).then_some(TrackedMarket {
                market,
                output_symbol,
            })
        })
        .collect()
}

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arcus_connector::models::{MarketInfo, MarketsResponse, WsBookContents};
    use std::str::FromStr;

    fn market_info_fixture(market: &str, status: &str) -> MarketInfo {
        MarketInfo {
            market: market.to_string(),
            market_id: 1,
            status: status.to_string(),
            base_asset: "BTC".to_string(),
            quote_asset: "USD".to_string(),
            tick_size: Decimal::from_str("0.1").unwrap(),
            step_size: Decimal::from_str("0.01").unwrap(),
            tick_tiers: Vec::new(),
            min_order_notional: None,
            oracle_price: None,
            mark_price: None,
            last_trade_price: None,
            funding_rate: None,
            volume24h: None,
            trades24h: None,
            open_interest: None,
        }
    }

    #[test]
    fn normalizes_arcus_market_symbols_and_deduplicates() {
        assert_eq!(normalize_market("btc"), "BTC-USD");
        assert_eq!(normalize_market("ETH_USD"), "ETH-USD");
        assert_eq!(normalize_market("SOL-USDC"), "SOL-USDC");

        let tracked = build_tracked_markets(vec![
            "btc".to_string(),
            "BTC-USD".to_string(),
            "eth".to_string(),
        ]);
        assert_eq!(tracked.len(), 2);
        assert_eq!(tracked[0].market, "BTC-USD");
        assert_eq!(tracked[0].output_symbol, "BTC");
        assert_eq!(tracked[1].market, "ETH-USD");
    }

    #[test]
    fn parses_current_market_payload() {
        let response: MarketsResponse = serde_json::from_str(
            r#"{
                "markets": [{
                    "marketDisplayName": "BTC-USD",
                    "marketId": 1,
                    "status": "ONLINE",
                    "baseAsset": "BTC",
                    "quoteAsset": "USD",
                    "tickSize": "0.1",
                    "stepSize": "0.00000001",
                    "tickTiers": [
                        {"upToPrice": "500000", "tick": "0.1"},
                        {"upToPrice": "1000000", "tick": "0.2"},
                        {"tick": "5"}
                    ],
                    "minOrderNotional": "5",
                    "oraclePrice": "65420.6",
                    "markPrice": "65456",
                    "lastTradePrice": "65420.3",
                    "fundingRate": "0.0000125",
                    "volume24h": "36.89",
                    "trades24h": 385,
                    "openInterest": "13.9281536"
                }]
            }"#,
        )
        .expect("Arcus market fixture");
        let info = MarketInfo::try_from(
            response
                .markets
                .into_iter()
                .next()
                .expect("BTC market fixture"),
        )
        .expect("parsed BTC market");
        assert_eq!(info.market_id, 1);
        assert_eq!(info.base_asset, "BTC");
        assert_eq!(info.quote_asset, "USD");
        assert_eq!(info.tick_size, Decimal::from_str("0.1").unwrap());
        assert_eq!(
            info.tick_size_for(Decimal::from(499_999)),
            Decimal::from_str("0.1").unwrap()
        );
        assert_eq!(
            info.tick_size_for(Decimal::from(750_000)),
            Decimal::from_str("0.2").unwrap()
        );
        assert_eq!(
            info.tick_size_for(Decimal::from(2_000_000)),
            Decimal::from(5)
        );
        assert_eq!(info.step_size.normalize().scale(), 8);
        assert_eq!(info.min_order_notional, Some(Decimal::from(5)));
    }

    #[test]
    fn sequence_gap_invalidates_book_until_new_snapshot() {
        let mut book = BookState::default();
        let snapshot = WsBookContents {
            bids: vec![["99".into(), "1".into()]],
            asks: vec![["101".into(), "2".into()]],
            last_sequence_id: 10,
            global_sequence_id: 100,
            timestamp: Some(1_784_880_000_000_000),
        };
        book.replace_from_ws("BTC-USD", &snapshot, 1)
            .expect("snapshot");
        assert!(book.is_ready());

        let gap = WsBookContents {
            bids: vec![["100".into(), "1".into()]],
            asks: vec![],
            last_sequence_id: 12,
            global_sequence_id: 102,
            timestamp: None,
        };
        assert!(book
            .apply_delta("BTC-USD", &gap, 2)
            .expect_err("gap")
            .to_string()
            .contains("expected=11 got=12"));
        assert!(!book.is_ready());

        let recovered = WsBookContents {
            bids: vec![["100".into(), "3".into()]],
            asks: vec![["102".into(), "4".into()]],
            last_sequence_id: 20,
            global_sequence_id: 200,
            timestamp: Some(1_784_880_001_000_000),
        };
        let top = book
            .replace_from_ws("BTC-USD", &recovered, 3)
            .expect("recovery snapshot")
            .expect("recovery top");
        assert_eq!(top.mid, Decimal::from(101));
        assert_eq!(book.last_sequence_id(), 20);
    }

    #[test]
    fn freshness_uses_local_receive_time_not_exchange_clock() {
        let mut book = BookState::default();
        // Exchange clock is far ahead of local wall-clock time.
        let skewed_exchange_ts_us = 9_999_999_000_000u64;
        let snapshot = WsBookContents {
            bids: vec![["100".into(), "1".into()]],
            asks: vec![["101".into(), "1".into()]],
            last_sequence_id: 1,
            global_sequence_id: 1,
            timestamp: Some(skewed_exchange_ts_us),
        };
        let received_at = 1_000u64;
        book.replace_from_ws("BTC-USD", &snapshot, received_at)
            .expect("snapshot");

        assert!(book.is_fresh(received_at, 5_000));
        // The exposed timestamp still reflects the exchange clock.
        assert_eq!(
            book.top().unwrap().timestamp_ms,
            skewed_exchange_ts_us / 1_000
        );

        // 10s of local wall-clock time pass with no further update. Judged
        // against the skewed-ahead exchange clock this would look fresh
        // forever; judged against the local receive time it must go stale.
        let ten_seconds_later = received_at + 10_000;
        assert!(!book.is_fresh(ten_seconds_later, 5_000));
    }

    #[test]
    fn missing_ws_timestamp_is_not_exposed_as_exchange_timestamp() {
        let mut book = BookState::default();
        // Arcus omits `timestamp` (permitted by `WsBookContents`); the book
        // still needs a receive-time fallback for `timestamp_ms`/freshness,
        // but must not report that fallback as an exchange-reported time
        // (bot-strategy#749 review).
        let snapshot = WsBookContents {
            bids: vec![["100".into(), "1".into()]],
            asks: vec![["101".into(), "1".into()]],
            last_sequence_id: 1,
            global_sequence_id: 1,
            timestamp: None,
        };
        let received_at = 1_000u64;
        book.replace_from_ws("BTC-USD", &snapshot, received_at)
            .expect("snapshot");

        let top = book.top().unwrap();
        assert_eq!(top.timestamp_ms, received_at);
        assert_eq!(top.exchange_timestamp_ms, None);

        // A subsequent delta that also omits the timestamp must keep the
        // same behavior.
        let delta = WsBookContents {
            bids: vec![["100".into(), "2".into()]],
            asks: vec![],
            last_sequence_id: 2,
            global_sequence_id: 2,
            timestamp: None,
        };
        let top = book
            .apply_delta("BTC-USD", &delta, received_at + 500)
            .expect("delta")
            .unwrap();
        assert_eq!(top.exchange_timestamp_ms, None);

        // Once Arcus supplies a real timestamp again, it must be exposed.
        let delta_with_ts = WsBookContents {
            bids: vec![["100".into(), "3".into()]],
            asks: vec![],
            last_sequence_id: 3,
            global_sequence_id: 3,
            timestamp: Some(2_000_000),
        };
        let top = book
            .apply_delta("BTC-USD", &delta_with_ts, received_at + 1_000)
            .expect("delta")
            .unwrap();
        assert_eq!(top.exchange_timestamp_ms, Some(2_000));
    }

    #[tokio::test]
    async fn maintenance_status_ignores_untracked_markets() {
        let connector = ArcusConnector::new(ArcusConnectorConfig {
            tracked_symbols: vec!["BTC".to_string()],
            ..ArcusConnectorConfig::default()
        })
        .expect("Arcus connector");

        {
            let mut markets = connector.market_info.write().await;
            markets.insert(
                "BTC-USD".to_string(),
                market_info_fixture("BTC-USD", "ONLINE"),
            );
            // An untracked market goes into maintenance; a BTC-only
            // connector must not report it.
            markets.insert(
                "SOL-USD".to_string(),
                market_info_fixture("SOL-USD", "MAINTENANCE"),
            );
            // Mark both entries fresh so `is_upcoming_maintenance` (which now
            // routes through the TTL-refreshing `market_info_for`) reads the
            // cache directly instead of attempting a real REST refresh.
            let now = now_ms();
            let mut fetched = connector.market_info_fetched_ms.write().await;
            fetched.insert("BTC-USD".to_string(), now);
            fetched.insert("SOL-USD".to_string(), now);
        }
        assert!(!connector.is_upcoming_maintenance(24).await);

        {
            let mut markets = connector.market_info.write().await;
            markets.get_mut("BTC-USD").expect("BTC-USD entry").status = "MAINTENANCE".to_string();
            let now = now_ms();
            connector
                .market_info_fetched_ms
                .write()
                .await
                .insert("BTC-USD".to_string(), now);
        }
        assert!(connector.is_upcoming_maintenance(24).await);
    }

    #[tokio::test]
    async fn trading_methods_are_explicitly_disabled() {
        let connector = ArcusConnector::new(ArcusConnectorConfig::default())
            .expect("read-only Arcus connector");
        let err = connector
            .create_order(
                "BTC",
                Decimal::ONE,
                OrderSide::Long,
                Some(Decimal::from(60_000)),
                None,
                false,
                None,
            )
            .await
            .expect_err("P0 must not place orders");
        assert!(err.to_string().contains("read-only connector slice"));
    }

    #[tokio::test]
    #[ignore = "requires Arcus public API network access"]
    async fn public_mainnet_rest_smoke() {
        let connector =
            ArcusConnector::new(ArcusConnectorConfig::default()).expect("Arcus mainnet connector");
        let ticker = connector.get_ticker("BTC", None).await.expect("BTC ticker");
        assert!(ticker.price > Decimal::ZERO);
        assert_eq!(ticker.min_tick, Some(Decimal::from_str("0.1").unwrap()));

        let book = connector
            .get_order_book("BTC", 5)
            .await
            .expect("BTC orderbook");
        assert_eq!(book.bids.len(), 5);
        assert_eq!(book.asks.len(), 5);
        assert!(book.book_ts_ms.is_none());

        let trades = connector
            .get_last_trades("BTC")
            .await
            .expect("BTC recent trades");
        assert!(!trades.trades.is_empty());

        let eth_ticker = connector.get_ticker("ETH", None).await.expect("ETH ticker");
        assert!(eth_ticker.price > Decimal::ZERO);
        let eth_book = connector
            .get_order_book("ETH", 5)
            .await
            .expect("ETH orderbook");
        assert_eq!(eth_book.bids.len(), 5);
        assert_eq!(eth_book.asks.len(), 5);
    }

    #[tokio::test]
    #[ignore = "requires Arcus public API network access"]
    async fn public_testnet_rest_smoke() {
        let connector = ArcusConnector::new(ArcusConnectorConfig {
            base_url: "https://api.testnet.arcus.xyz".to_string(),
            websocket_url: "wss://api.testnet.arcus.xyz/v1/ws".to_string(),
            ..ArcusConnectorConfig::default()
        })
        .expect("Arcus testnet connector");
        for symbol in ["BTC", "ETH"] {
            let ticker = connector.get_ticker(symbol, None).await.expect("ticker");
            assert!(ticker.price > Decimal::ZERO);
            let book = connector
                .get_order_book(symbol, 5)
                .await
                .expect("orderbook");
            assert_eq!(book.bids.len(), 5);
            assert_eq!(book.asks.len(), 5);
        }
    }

    #[tokio::test]
    #[ignore = "requires Arcus public API network access"]
    async fn public_mainnet_ws_smoke() {
        let connector = ArcusConnector::new(ArcusConnectorConfig {
            tracked_symbols: vec!["BTC".to_string()],
            ..ArcusConnectorConfig::default()
        })
        .expect("Arcus mainnet connector");
        let mut updates = connector
            .subscribe_price_updates()
            .expect("Arcus price subscription");
        connector.start().await.expect("start Arcus WS");

        let update = tokio::time::timeout(Duration::from_secs(15), updates.recv())
            .await
            .expect("Arcus WS price timeout")
            .expect("Arcus WS price update");
        assert_eq!(update.symbol, "BTC");
        assert!(update.best_bid > Decimal::ZERO);
        assert!(update.best_ask >= update.best_bid);
        assert_eq!(
            update.mid_price,
            (update.best_bid + update.best_ask) / Decimal::TWO
        );
        assert!(update.timestamp > 0);
        connector.stop().await.expect("stop Arcus WS");
    }

    #[tokio::test]
    #[ignore = "requires Arcus public API network access"]
    async fn public_testnet_ws_smoke() {
        let connector = ArcusConnector::new(ArcusConnectorConfig {
            base_url: "https://api.testnet.arcus.xyz".to_string(),
            websocket_url: "wss://api.testnet.arcus.xyz/v1/ws".to_string(),
            tracked_symbols: vec!["BTC".to_string()],
            ob_stale_secs: None,
        })
        .expect("Arcus testnet connector");
        let mut updates = connector
            .subscribe_price_updates()
            .expect("Arcus testnet price subscription");
        connector.start().await.expect("start Arcus testnet WS");

        let update = tokio::time::timeout(Duration::from_secs(15), updates.recv())
            .await
            .expect("Arcus testnet WS price timeout")
            .expect("Arcus testnet WS price update");
        assert_eq!(update.symbol, "BTC");
        assert!(update.best_bid > Decimal::ZERO);
        assert!(update.best_ask >= update.best_bid);
        assert!(update.timestamp > 0);
        connector.stop().await.expect("stop Arcus testnet WS");
    }
}

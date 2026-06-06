use crate::{
    dex_connector::{slippage_price, DexConnector},
    dex_request::DexError,
    BalanceResponse, CanceledOrder, CanceledOrdersResponse, CombinedBalanceResponse,
    CreateOrderResponse, FilledOrder, FilledOrdersResponse, LastTrade, LastTradesResponse,
    OpenOrder, OpenOrdersResponse, OrderBookLevel, OrderBookSnapshot, OrderSide, PositionSnapshot,
    TickerResponse, TpSl, TriggerOrderStyle,
};
use async_trait::async_trait;
#[cfg(test)]
use chrono::DateTime;
use chrono::{Duration, Utc};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::Deserialize;
use serde_json::json;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{
    atomic::{AtomicBool, AtomicI64, Ordering},
    Arc,
};
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;

const ORDERBOOK_STALE_AFTER: StdDuration = StdDuration::from_secs(5);
const DEFAULT_CLOSE_ALL_POSITIONS_SLIPPAGE_BPS: u32 = 50;

mod maintenance;
mod models;
mod orders;
mod parsing;
mod pricing;
mod rest;
mod signing;
mod ws;

#[cfg(test)]
use maintenance::parse_maintenance_window;
use maintenance::{
    extended_maintenance_disabled, parse_maintenance_windows_env, MaintenanceWindow,
};
use models::{
    AccountTradeModel, BalanceModel, MarketModel, OpenOrderModel, OrderbookUpdateModel,
    PlacedOrderModel, PositionModel,
};
use parsing::{copy_balance, default_taker_fee, normalize_symbol, position_snapshot_from_model};
use rest::{build_query, ExtendedApi, ExtendedEnvironment};
use signing::{NewOrderModel, TpslLegModel};
#[cfg(test)]
use ws::{apply_orderbook_frame, StreamOrderbookUpdate, WrappedStreamResponse};
use ws::{fallback_price, stream_account, stream_orderbooks, stream_trades};

pub struct ExtendedConnector {
    api: ExtendedApi,
    public_key: String,
    private_key: String,
    vault: u64,
    env: ExtendedEnvironment,
    websocket_url: Option<String>,
    tracked_symbols: Vec<String>,
    market_cache: Arc<RwLock<HashMap<String, MarketModel>>>,
    order_book_cache: Arc<RwLock<HashMap<String, OrderBookCacheEntry>>>,
    balance_cache: Arc<RwLock<Option<BalanceResponse>>>,
    open_orders_cache: Arc<RwLock<HashMap<String, Vec<OpenOrder>>>>,
    order_id_map: Arc<RwLock<HashMap<i64, String>>>,
    filled_orders: Arc<RwLock<HashMap<String, Vec<FilledOrder>>>>,
    canceled_orders: Arc<RwLock<HashMap<String, Vec<CanceledOrder>>>>,
    last_trades: Arc<RwLock<HashMap<String, Vec<LastTrade>>>>,
    positions_cache: Arc<RwLock<Option<Vec<PositionSnapshot>>>>,
    close_all_positions_slippage_bps: u32,
    ws_started: AtomicBool,
    ws_tasks: Mutex<Vec<JoinHandle<()>>>,
    market_logged: Mutex<HashSet<String>>,
    maintenance_windows: Arc<Vec<MaintenanceWindow>>,
    // Set to true when any tracked symbol's `/info/markets` status is not
    // `ACTIVE` (i.e. `DISABLED` / `REDUCE_ONLY`). Updated by the background
    // refresher spawned in `start()`.
    maintenance_symbol_inactive: Arc<AtomicBool>,
    maintenance_refresher_started: AtomicBool,
    // UNIX-seconds timestamp at which the REST-1010 fallback expires. Set
    // to `now + REST_1010_GRACE_SECS` whenever a REST response carries
    // code 1010 ("Maintenance mode"); `is_upcoming_maintenance` returns
    // true while this is in the future. Bridges the gap when neither the
    // operator-declared windows nor the `/info/markets` poller catches a
    // live maintenance event. See bot-strategy#321.
    rest_1010_clear_at: Arc<AtomicI64>,
    // Same shape as `rest_1010_clear_at` but for code 1011 ("Post only
    // mode"). Folded into `is_upcoming_maintenance` so the existing
    // pairtrade/xvenue-arb suppression wiring covers transient
    // post-only-only windows (auction / reopen). See bot-strategy#327.
    rest_1011_clear_at: Arc<AtomicI64>,
}

#[derive(Debug, Clone)]
struct OrderBookCacheEntry {
    // Book state as (price -> qty). Extended's ws stream delivers an initial
    // SNAPSHOT followed by incremental DELTA frames; maintaining sorted maps
    // makes the merge O(log n) per level and keeps best bid/ask retrievable
    // without a per-query sort.
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    updated_at: Instant,
    // Exchange-side publish timestamp (ms since epoch) from the wrapping
    // `ts` field on the WS frame. Surfaced via `get_ticker` as
    // `exchange_ts` so multi-process deploys converge on identical bar
    // closes (bot-strategy#274 / #276 / #280).
    last_publish_ts_ms: Option<i64>,
}

impl Default for OrderBookCacheEntry {
    fn default() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            updated_at: Instant::now(),
            last_publish_ts_ms: None,
        }
    }
}

impl ExtendedConnector {
    pub async fn new(
        api_key: String,
        public_key: String,
        private_key: String,
        vault: u64,
        base_url: Option<String>,
        websocket_url: Option<String>,
        tracked_symbols: Vec<String>,
    ) -> Result<Self, DexError> {
        let env = base_url
            .as_deref()
            .map(Self::infer_environment)
            .unwrap_or(ExtendedEnvironment::Mainnet);
        let api_base = base_url.unwrap_or_else(|| env.api_base().to_string());
        let rest_1010_clear_at = Arc::new(AtomicI64::new(0));
        let rest_1011_clear_at = Arc::new(AtomicI64::new(0));
        let api = ExtendedApi::new(
            api_base,
            api_key,
            Arc::clone(&rest_1010_clear_at),
            Arc::clone(&rest_1011_clear_at),
        )
        .await?;
        let close_all_positions_slippage_bps = std::env::var("CLOSE_ALL_POSITIONS_SLIPPAGE_BPS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(DEFAULT_CLOSE_ALL_POSITIONS_SLIPPAGE_BPS);

        let market_cache = Arc::new(RwLock::new(HashMap::new()));
        // Fetch all markets during initialization
        let markets: Vec<MarketModel> = api.get("/info/markets".to_string(), true).await?;
        {
            let mut cache = market_cache.write().await;
            for market in markets {
                cache.insert(market.name.clone(), market);
            }
        }
        {
            let cache = market_cache.read().await;
            let mut logged = HashSet::new();
            for (name, m) in cache.iter() {
                let tc = &m.trading_config;
                log::info!(
                    "[MARKET][init] {} tick={} floor={} cap={}",
                    name,
                    tc.min_price_change,
                    tc.limit_price_floor,
                    tc.limit_price_cap
                );
                logged.insert(name.clone());
            }
        }

        Ok(Self {
            api,
            public_key,
            private_key,
            vault,
            env,
            websocket_url,
            tracked_symbols,
            market_cache,
            order_book_cache: Arc::new(RwLock::new(HashMap::new())),
            balance_cache: Arc::new(RwLock::new(None)),
            open_orders_cache: Arc::new(RwLock::new(HashMap::new())),
            order_id_map: Arc::new(RwLock::new(HashMap::new())),
            filled_orders: Arc::new(RwLock::new(HashMap::new())),
            canceled_orders: Arc::new(RwLock::new(HashMap::new())),
            last_trades: Arc::new(RwLock::new(HashMap::new())),
            positions_cache: Arc::new(RwLock::new(None)),
            close_all_positions_slippage_bps,
            ws_started: AtomicBool::new(false),
            ws_tasks: Mutex::new(Vec::new()),
            market_logged: Mutex::new(HashSet::new()),
            maintenance_windows: Arc::new(parse_maintenance_windows_env()),
            maintenance_symbol_inactive: Arc::new(AtomicBool::new(false)),
            maintenance_refresher_started: AtomicBool::new(false),
            rest_1010_clear_at,
            rest_1011_clear_at,
        })
    }

    fn infer_environment(base_url: &str) -> ExtendedEnvironment {
        let lowered = base_url.to_lowercase();
        if lowered.contains("sepolia") || lowered.contains("testnet") {
            ExtendedEnvironment::Testnet
        } else {
            ExtendedEnvironment::Mainnet
        }
    }

    fn build_ws_url(&self, path: &str) -> Option<String> {
        let base = self.websocket_url.as_ref()?;
        let base = base.trim_end_matches('/');
        Some(format!("{base}{path}"))
    }

    async fn cached_publish_ts_ms(&self, symbol: &str) -> Option<u64> {
        let cache = self.order_book_cache.read().await;
        let entry = cache.get(symbol)?;
        if entry.updated_at.elapsed() > ORDERBOOK_STALE_AFTER {
            return None;
        }
        entry
            .last_publish_ts_ms
            .and_then(|ts| u64::try_from(ts).ok())
    }

    async fn get_cached_order_book(&self, symbol: &str, depth: usize) -> Option<OrderBookSnapshot> {
        let (bids_map, asks_map, updated_at) = {
            let cache = self.order_book_cache.read().await;
            let entry = cache.get(symbol)?;
            (entry.bids.clone(), entry.asks.clone(), entry.updated_at)
        };
        let age = updated_at.elapsed();
        if age > ORDERBOOK_STALE_AFTER {
            let mut cache = self.order_book_cache.write().await;
            if let Some(entry) = cache.get(symbol) {
                if entry.updated_at.elapsed() > ORDERBOOK_STALE_AFTER {
                    cache.remove(symbol);
                }
            }
            log::debug!("orderbook {} stale (age={}ms)", symbol, age.as_millis());
            return None;
        }
        // Best bids = highest prices first; asks = lowest prices first.
        let bids = bids_map
            .iter()
            .rev()
            .take(depth)
            .map(|(price, size)| OrderBookLevel {
                price: *price,
                size: *size,
            })
            .collect();
        let asks = asks_map
            .iter()
            .take(depth)
            .map(|(price, size)| OrderBookLevel {
                price: *price,
                size: *size,
            })
            .collect();
        Some(OrderBookSnapshot { bids, asks })
    }

    // `ws_tasks` mutex is held across `.get_market().await` etc. This runs
    // exactly once per connector instance (guarded by `ws_started`), so the
    // lock is effectively single-threaded here (bot-strategy#391).
    #[allow(clippy::await_holding_invalid_type)]
    async fn spawn_ws_tasks(&self) {
        if self.websocket_url.is_none() {
            return;
        }
        if self.ws_started.swap(true, Ordering::SeqCst) {
            return;
        }

        let mut handles = self.ws_tasks.lock().await;
        let symbols = self.tracked_symbols.clone();
        let reconnect_policy = crate::ws_reconnect::WsReconnectPolicy::extended();

        for symbol in symbols.iter() {
            // Callers track bare tokens ("BTC") but Extended's stream paths
            // require the collateral-qualified market name ("BTC-USD"). If
            // we connect to /orderbooks/BTC the server accepts the upgrade
            // and then sends nothing, so the cache never populates. Resolve
            // once here and keep the bare symbol as the cache key.
            let market_name = match self.get_market(symbol).await {
                Ok(market) => market.name,
                Err(err) => {
                    log::warn!(
                        "extended ws: cannot resolve market for {symbol}: {err}; skipping streams"
                    );
                    continue;
                }
            };

            let orderbook_path = format!("/orderbooks/{market_name}");
            if let Some(url) = self.build_ws_url(&orderbook_path) {
                let order_book_cache = Arc::clone(&self.order_book_cache);
                let symbol = symbol.clone();
                let policy = reconnect_policy;
                handles.push(tokio::spawn(async move {
                    loop {
                        if let Err(err) = stream_orderbooks(&url, &symbol, &order_book_cache).await
                        {
                            log::warn!("orderbook stream error: {err}");
                        }
                        policy.wait(0).await;
                    }
                }));
            }

            let trades_path = format!("/publicTrades/{market_name}");
            if let Some(url) = self.build_ws_url(&trades_path) {
                let last_trades = Arc::clone(&self.last_trades);
                let symbol = symbol.clone();
                let policy = reconnect_policy;
                handles.push(tokio::spawn(async move {
                    loop {
                        if let Err(err) = stream_trades(&url, &symbol, &last_trades).await {
                            log::warn!("public trades stream error: {err}");
                        }
                        policy.wait(0).await;
                    }
                }));
            }
        }

        if let Some(url) = self.build_ws_url("/account") {
            let api_key = self.api.api_key.clone();
            let balance_cache = Arc::clone(&self.balance_cache);
            let open_orders_cache = Arc::clone(&self.open_orders_cache);
            let order_id_map = Arc::clone(&self.order_id_map);
            let filled_orders = Arc::clone(&self.filled_orders);
            let positions_cache = Arc::clone(&self.positions_cache);
            let policy = reconnect_policy;
            handles.push(tokio::spawn(async move {
                loop {
                    if let Err(err) = stream_account(
                        &url,
                        &api_key,
                        &balance_cache,
                        &open_orders_cache,
                        &order_id_map,
                        &filled_orders,
                        &positions_cache,
                    )
                    .await
                    {
                        if matches!(err, DexError::ApiKeyRegistrationRequired) {
                            log::warn!("account stream unavailable: {err}");
                            break;
                        }
                        log::warn!("account stream error: {err}");
                    }
                    policy.wait(0).await;
                }
            }));
        }
    }

    async fn get_market(&self, symbol: &str) -> Result<MarketModel, DexError> {
        // Callers across the project use bare token symbols (e.g. "BTC",
        // "ETH") while Extended's market names carry the collateral suffix
        // ("BTC-USD", "ETH-USD"). If the bare form misses, retry with
        // "-USD" appended so pairtrade's universe_pairs keep working
        // against Extended without a per-DEX translation layer.
        let candidates: Vec<String> = if symbol.contains('-') {
            vec![symbol.to_string()]
        } else {
            vec![symbol.to_string(), format!("{}-USD", symbol)]
        };

        {
            let cache = self.market_cache.read().await;
            for cand in &candidates {
                if let Some(market) = cache.get(cand) {
                    return Ok(market.clone());
                }
            }
        }

        let mut last_err: Option<DexError> = None;
        for cand in &candidates {
            let path = build_query("/info/markets", vec![("market".to_string(), cand.clone())]);
            let markets: Result<Vec<MarketModel>, DexError> = self.api.get(path, true).await;
            let markets = match markets {
                Ok(m) => m,
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            };
            let Some(market) = markets.into_iter().find(|m| m.name == *cand) else {
                continue;
            };
            {
                let mut logged = self.market_logged.lock().await;
                if logged.insert(symbol.to_string()) {
                    let tc = &market.trading_config;
                    log::info!(
                        "[MARKET] {} (resolved as {}) tick={} floor={} cap={}",
                        symbol,
                        cand,
                        tc.min_price_change,
                        tc.limit_price_floor,
                        tc.limit_price_cap
                    );
                }
            }
            let mut cache = self.market_cache.write().await;
            // Cache under both the caller's symbol and the resolved market
            // name so subsequent lookups with either form hit the cache.
            cache.insert(symbol.to_string(), market.clone());
            if cand != symbol {
                cache.insert(cand.clone(), market.clone());
            }
            return Ok(market);
        }

        Err(last_err
            .unwrap_or_else(|| DexError::Permanent(format!("Market not found: {}", symbol))))
    }

    #[allow(dead_code)]
    async fn refresh_market(&self, symbol: &str) -> Result<MarketModel, DexError> {
        let path = build_query(
            "/info/markets",
            vec![("market".to_string(), symbol.to_string())],
        );
        let markets: Vec<MarketModel> = self.api.get(path, true).await?;
        let market = markets
            .into_iter()
            .find(|m| m.name == symbol)
            .ok_or_else(|| DexError::Permanent(format!("Market not found: {}", symbol)))?;

        let tc = &market.trading_config;
        log::info!(
            "[MARKET][refresh] {} tick={} floor={} cap={}",
            symbol,
            tc.min_price_change,
            tc.limit_price_floor,
            tc.limit_price_cap
        );

        let mut cache = self.market_cache.write().await;
        cache.insert(symbol.to_string(), market.clone());
        Ok(market)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn normalize_symbol_strips_usd_suffix() {
        assert_eq!(normalize_symbol("BTC-USD"), "BTC");
        assert_eq!(normalize_symbol("ETH-USDT"), "ETH");
    }

    #[test]
    fn normalize_symbol_passes_bare_token_through() {
        assert_eq!(normalize_symbol("BTC"), "BTC");
        assert_eq!(normalize_symbol("SOL"), "SOL");
    }

    #[test]
    fn normalize_symbol_handles_multi_dash() {
        // Prefix before first '-' wins; first segment is the base.
        assert_eq!(normalize_symbol("CRCL_24_5-USD"), "CRCL_24_5");
    }

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    #[test]
    fn parse_maintenance_window_rfc3339_and_duration() {
        let w = parse_maintenance_window("2026-05-01T10:00:00Z/2h").unwrap();
        assert_eq!(w.start.to_rfc3339(), "2026-05-01T10:00:00+00:00");
        assert_eq!(w.end.to_rfc3339(), "2026-05-01T12:00:00+00:00");

        let w = parse_maintenance_window("2026-05-01T10:00:00Z/45m").unwrap();
        assert_eq!(w.end.to_rfc3339(), "2026-05-01T10:45:00+00:00");

        let w = parse_maintenance_window("2026-05-01T10:00:00Z/2026-05-01T11:30:00Z").unwrap();
        assert_eq!(w.end.to_rfc3339(), "2026-05-01T11:30:00+00:00");
    }

    #[test]
    fn parse_maintenance_window_rejects_bad_inputs() {
        assert!(parse_maintenance_window("").is_none());
        assert!(parse_maintenance_window("not-a-date/2h").is_none());
        assert!(parse_maintenance_window("2026-05-01T10:00:00Z/2x").is_none());
        // End must be after start.
        assert!(parse_maintenance_window("2026-05-01T10:00:00Z/-30m").is_none());
        // Missing separator.
        assert!(parse_maintenance_window("2026-05-01T10:00:00Z").is_none());
    }

    #[test]
    fn maintenance_within_window_upcoming_and_active() {
        let start = DateTime::parse_from_rfc3339("2026-05-01T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let end = start + Duration::hours(2);
        let windows = vec![MaintenanceWindow { start, end }];

        // 1h before start, hours_ahead=2 → upcoming hit.
        let now = start - Duration::hours(1);
        assert!(ExtendedConnector::maintenance_within_window(
            &windows, &now, 2
        ));

        // 1h before start, hours_ahead=0 → miss (not upcoming within horizon).
        assert!(!ExtendedConnector::maintenance_within_window(
            &windows, &now, 0
        ));

        // 30min past start → active (within 90-min grace).
        let now = start + Duration::minutes(30);
        assert!(ExtendedConnector::maintenance_within_window(
            &windows, &now, 2
        ));

        // 2h past start → beyond grace → miss (grace is 90min, not window end).
        let now = start + Duration::hours(2);
        assert!(!ExtendedConnector::maintenance_within_window(
            &windows, &now, 2
        ));
    }

    #[test]
    fn maintenance_within_window_empty_list_is_false() {
        let now = Utc::now();
        assert!(!ExtendedConnector::maintenance_within_window(&[], &now, 24));
    }

    #[test]
    fn apply_orderbook_frame_records_publish_ts_from_payload() {
        // Synthetic SNAPSHOT frame (Extended camelCase aliases). The wrapping
        // `ts` is the exchange publish time the connector now surfaces as
        // `exchange_ts` on the ticker. See bot-strategy#280.
        let raw = r#"{
            "type": "SNAPSHOT",
            "ts": 1717023600123,
            "seq": 42,
            "data": {
                "m": "BTC-USD",
                "b": [{"p": "100.0", "q": "1.5"}],
                "a": [{"p": "101.0", "q": "2.0"}]
            }
        }"#;
        let payload: WrappedStreamResponse<StreamOrderbookUpdate> =
            serde_json::from_str(raw).expect("synthetic frame parses");
        let msg_type = payload.msg_type.clone().unwrap_or_default();
        let update = payload.data.expect("snapshot has data");

        let mut entry = OrderBookCacheEntry::default();
        apply_orderbook_frame(&msg_type, update, payload.ts, &mut entry);

        assert_eq!(entry.last_publish_ts_ms, Some(1717023600123));
        assert_eq!(entry.bids.get(&dec("100.0")), Some(&dec("1.5")));
        assert_eq!(entry.asks.get(&dec("101.0")), Some(&dec("2.0")));
    }

    #[test]
    fn apply_orderbook_frame_overwrites_publish_ts_on_delta() {
        // First a SNAPSHOT seeds the book, then a later DELTA must update the
        // publish timestamp so downstream `exchange_ts` reflects the most
        // recent exchange-side event rather than the snapshot time.
        let snapshot: WrappedStreamResponse<StreamOrderbookUpdate> = serde_json::from_str(
            r#"{
                "type": "SNAPSHOT",
                "ts": 1000,
                "seq": 1,
                "data": {"m": "BTC-USD", "b": [{"p": "1.0", "q": "1.0"}], "a": []}
            }"#,
        )
        .unwrap();
        let mut entry = OrderBookCacheEntry::default();
        apply_orderbook_frame(
            &snapshot.msg_type.clone().unwrap_or_default(),
            snapshot.data.unwrap(),
            snapshot.ts,
            &mut entry,
        );
        assert_eq!(entry.last_publish_ts_ms, Some(1000));

        let delta: WrappedStreamResponse<StreamOrderbookUpdate> = serde_json::from_str(
            r#"{
                "type": "DELTA",
                "ts": 2500,
                "seq": 2,
                "data": {"m": "BTC-USD", "b": [{"p": "1.0", "q": "0", "c": "0"}], "a": []}
            }"#,
        )
        .unwrap();
        apply_orderbook_frame(
            &delta.msg_type.clone().unwrap_or_default(),
            delta.data.unwrap(),
            delta.ts,
            &mut entry,
        );

        assert_eq!(entry.last_publish_ts_ms, Some(2500));
        // qty=0 on DELTA removes the level rather than wiping the book.
        assert!(entry.bids.is_empty());
    }
}

#[async_trait]
impl DexConnector for ExtendedConnector {
    async fn start(&self) -> Result<(), DexError> {
        self.spawn_ws_tasks().await;
        self.spawn_maintenance_refresher();
        // Bootstrap order_id_map from recent history so cross-session
        // trades don't fall back to /user/orders/history mid-session
        // (which occasionally 5xxs during Extended instability).
        // bot-strategy#206.
        if let Err(e) = self.bootstrap_order_id_map().await {
            log::warn!(
                "[order_id_map][extended] bootstrap failed: {:?} — continuing without pre-populated map",
                e
            );
        }
        Ok(())
    }

    async fn stop(&self) -> Result<(), DexError> {
        let mut handles = self.ws_tasks.lock().await;
        for handle in handles.drain(..) {
            handle.abort();
        }
        self.ws_started.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn restart(&self, _max_retries: i32) -> Result<(), DexError> {
        Ok(())
    }

    async fn set_leverage(&self, symbol: &str, leverage: u32) -> Result<(), DexError> {
        let market_name = self.get_market(symbol).await?.name;
        let payload = json!({
            "market": market_name,
            "leverage": leverage,
        });
        let _response: EmptyResponse = self
            .api
            .patch("/user/leverage".to_string(), payload, true)
            .await?;
        Ok(())
    }

    async fn get_ticker(
        &self,
        symbol: &str,
        _test_price: Option<Decimal>,
    ) -> Result<TickerResponse, DexError> {
        let market = self.get_market(symbol).await?;
        // Prefer a fresh orderbook midpoint — the trade cache is sparse on
        // thin Extended markets and `market.market_stats.last_price` is only
        // populated once at startup (market_cache is never refreshed). See
        // bot-strategy#134. We reach for `get_order_book` so both the
        // WS-cache path and the REST fallback get exercised automatically.
        let price = match self.get_order_book(symbol, 1).await {
            Ok(snapshot) => {
                let bid = snapshot.bids.first().map(|v| v.price);
                let ask = snapshot.asks.first().map(|v| v.price);
                match (bid, ask) {
                    (Some(b), Some(a)) => (b + a) / Decimal::new(2, 0),
                    (Some(b), None) => b,
                    (None, Some(a)) => a,
                    _ => fallback_price(&self.last_trades, symbol, &market).await?,
                }
            }
            Err(_) => fallback_price(&self.last_trades, symbol, &market).await?,
        };
        // Only the WS-cache path carries an exchange-side timestamp; the REST
        // fallback book has none and stays at None so callers fall back to
        // wall-clock bucketing.
        let exchange_ts = self.cached_publish_ts_ms(symbol).await;
        Ok(TickerResponse {
            symbol: market.name.clone(),
            price,
            min_tick: Some(market.trading_config.min_price_change),
            min_order: Some(market.trading_config.min_order_size),
            size_decimals: Some(market.trading_config.min_order_size_change.scale()),
            volume: Some(market.market_stats.daily_volume),
            num_trades: None,
            open_interest: Some(market.market_stats.open_interest),
            funding_rate: Some(market.market_stats.funding_rate),
            oracle_price: Some(market.market_stats.index_price),
            exchange_ts,
        })
    }

    async fn get_filled_orders(&self, symbol: &str) -> Result<FilledOrdersResponse, DexError> {
        let key = normalize_symbol(symbol);
        if self.websocket_url.is_some() {
            let cache = self.filled_orders.read().await;
            if let Some(orders) = cache.get(&key) {
                return Ok(FilledOrdersResponse {
                    orders: orders.clone(),
                });
            }
            log::debug!(
                "[filled_orders][extended] cache miss for {}; falling back to REST",
                key
            );
        }

        let orders = self.fetch_filled_orders_via_http(symbol).await?;
        // bot-strategy#459: do NOT write REST results back to the cache
        // when WS is configured. `stream_account` owns the cache; a
        // post-REST insert here can clobber a fill that WS pushed
        // between the #432 cache-invalidate and this REST round-trip,
        // re-introducing the false `filled=0` poll that #432 set out to
        // fix. Non-WS environments (tests / dev) still cache REST
        // results to avoid pathological re-fetching.
        if self.websocket_url.is_none() {
            let mut cache = self.filled_orders.write().await;
            cache.insert(key, orders.clone());
        }
        Ok(FilledOrdersResponse { orders })
    }

    async fn get_canceled_orders(&self, symbol: &str) -> Result<CanceledOrdersResponse, DexError> {
        let market_name = self.get_market(symbol).await?.name;
        let path = build_query(
            "/user/orders/history",
            vec![("market".to_string(), market_name)],
        );
        let orders_history: Vec<OpenOrderModel> = self.api.get(path, true).await?;
        let orders = orders_history
            .into_iter()
            .filter(|order| order.status == "CANCELLED")
            .map(|order| CanceledOrder {
                order_id: order.external_id,
                canceled_timestamp: order.updated_time as u64,
            })
            .collect::<Vec<_>>();

        let mut cache = self.canceled_orders.write().await;
        cache.insert(normalize_symbol(symbol), orders.clone());
        Ok(CanceledOrdersResponse { orders })
    }

    async fn get_open_orders(&self, symbol: &str) -> Result<OpenOrdersResponse, DexError> {
        let key = normalize_symbol(symbol);
        let orders = if self.websocket_url.is_some() {
            let cache = self.open_orders_cache.read().await;
            cache.get(&key).cloned().unwrap_or_default()
        } else {
            let market_name = self.get_market(symbol).await?.name;
            let path = build_query("/user/orders", vec![("market".to_string(), market_name)]);
            let open_orders: Vec<OpenOrderModel> = self.api.get(path, true).await?;
            {
                let mut map = self.order_id_map.write().await;
                for order in open_orders.iter() {
                    map.insert(order.id, order.external_id.clone());
                }
            }
            open_orders
                .into_iter()
                .map(|order| OpenOrder {
                    order_id: order.external_id.clone(),
                    symbol: order.market.clone(),
                    side: if order.side == "BUY" {
                        OrderSide::Long
                    } else {
                        OrderSide::Short
                    },
                    size: order.qty,
                    price: order.price.unwrap_or(Decimal::ZERO),
                    status: order.status,
                })
                .collect::<Vec<_>>()
        };
        Ok(OpenOrdersResponse { orders })
    }

    async fn get_balance(&self, symbol: Option<&str>) -> Result<BalanceResponse, DexError> {
        let balance = if self.websocket_url.is_some() {
            let cache = self.balance_cache.read().await;
            cache.as_ref().map(copy_balance).ok_or_else(|| {
                DexError::Transient("balance unavailable: waiting for websocket data".to_string())
            })?
        } else {
            let balance: BalanceModel = self.api.get("/user/balance".to_string(), true).await?;
            if let Some(symbol) = symbol {
                if symbol != balance.collateral_name {
                    return Err(DexError::InvalidInput {
                        field: "symbol".to_string(),
                        value: format!(
                            "{} unsupported (collateral: {})",
                            symbol, balance.collateral_name
                        ),
                    });
                }
            }
            BalanceResponse {
                equity: balance.equity,
                balance: balance.balance,
                position_entry_price: None,
                position_sign: None,
            }
        };
        if symbol.is_some() && self.websocket_url.is_some() {
            // Collateral symbol isn't supplied in account stream payload.
        }
        Ok(balance)
    }

    async fn get_combined_balance(&self) -> Result<CombinedBalanceResponse, DexError> {
        let balance = if self.websocket_url.is_some() {
            let cache = self.balance_cache.read().await;
            cache.as_ref().map(copy_balance).ok_or_else(|| {
                DexError::Transient("balance unavailable: waiting for websocket data".to_string())
            })?
        } else {
            let balance: BalanceModel = self.api.get("/user/balance".to_string(), true).await?;
            BalanceResponse {
                equity: balance.equity,
                balance: balance.balance,
                position_entry_price: None,
                position_sign: None,
            }
        };
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
        Ok(CombinedBalanceResponse {
            usd_balance: balance.equity,
            total_asset_value: balance.equity,
            token_balances,
            spot_assets: Vec::new(),
        })
    }

    async fn get_positions(&self) -> Result<Vec<PositionSnapshot>, DexError> {
        if self.websocket_url.is_some() {
            match self
                .api
                .get::<Vec<PositionModel>>("/user/positions".to_string(), true)
                .await
            {
                Ok(positions) => {
                    let mut out = Vec::new();
                    for position in positions {
                        if let Some(snapshot) = position_snapshot_from_model(position) {
                            out.push(snapshot);
                        }
                    }
                    let mut cache = self.positions_cache.write().await;
                    *cache = Some(out.clone());
                    return Ok(out);
                }
                Err(err) => {
                    let cache = self.positions_cache.read().await;
                    if let Some(cached) = cache.clone() {
                        if !cached.is_empty() {
                            log::warn!(
                                "[positions][extended] REST fetch failed; using WS cache: {}",
                                err
                            );
                            return Ok(cached);
                        }
                    }
                    return Err(err);
                }
            }
        }

        let positions: Vec<PositionModel> =
            self.api.get("/user/positions".to_string(), true).await?;
        let mut out = Vec::new();
        for position in positions {
            if let Some(snapshot) = position_snapshot_from_model(position) {
                out.push(snapshot);
            }
        }
        Ok(out)
    }

    async fn get_last_trades(&self, symbol: &str) -> Result<LastTradesResponse, DexError> {
        let key = normalize_symbol(symbol);
        if self.websocket_url.is_some() {
            let cache = self.last_trades.read().await;
            let trades = cache.get(&key).cloned().ok_or_else(|| {
                DexError::Transient(
                    "last trades unavailable: waiting for websocket data".to_string(),
                )
            })?;
            Ok(LastTradesResponse { trades })
        } else {
            let market_name = self.get_market(symbol).await?.name;
            let path = build_query("/user/trades", vec![("market".to_string(), market_name)]);
            let trades: Vec<AccountTradeModel> = self.api.get(path, true).await?;
            let last_trades = trades
                .into_iter()
                .map(|trade| LastTrade {
                    price: trade.price,
                    size: Some(trade.qty),
                    side: match trade.side.as_str() {
                        "BUY" => Some(OrderSide::Long),
                        "SELL" => Some(OrderSide::Short),
                        _ => None,
                    },
                })
                .collect::<Vec<_>>();

            let mut cache = self.last_trades.write().await;
            cache.insert(normalize_symbol(symbol), last_trades.clone());
            Ok(LastTradesResponse {
                trades: last_trades,
            })
        }
    }

    async fn get_order_book(
        &self,
        symbol: &str,
        depth: usize,
    ) -> Result<OrderBookSnapshot, DexError> {
        if self.websocket_url.is_some() {
            self.get_cached_order_book(symbol, depth)
                .await
                .ok_or_else(|| {
                    DexError::Transient(
                        "order book unavailable: waiting for websocket data".to_string(),
                    )
                })
        } else {
            // Resolve through get_market so a bare symbol like "BTC" hits
            // the real market name "BTC-USD" on the REST URL. Without this
            // the /info/markets/BTC/orderbook endpoint returns empty and
            // pairtrade's step loop spams `Extended API returned empty data`.
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
    }

    async fn clear_filled_order(&self, symbol: &str, trade_id: &str) -> Result<(), DexError> {
        let key = normalize_symbol(symbol);
        let mut filled_orders = self.filled_orders.write().await;
        if let Some(orders) = filled_orders.get_mut(&key) {
            let initial_len = orders.len();
            orders.retain(|order| order.trade_id != trade_id);
            if orders.len() < initial_len {
                Ok(())
            } else {
                Err(DexError::Permanent(format!(
                    "Trade ID {} not found for symbol {}",
                    trade_id, symbol
                )))
            }
        } else {
            Err(DexError::Permanent(format!(
                "No filled orders found for symbol {}",
                symbol
            )))
        }
    }

    async fn clear_all_filled_orders(&self) -> Result<(), DexError> {
        let mut filled_orders = self.filled_orders.write().await;
        filled_orders.clear();
        Ok(())
    }

    async fn clear_canceled_order(&self, symbol: &str, order_id: &str) -> Result<(), DexError> {
        let mut canceled_orders = self.canceled_orders.write().await;
        if let Some(orders) = canceled_orders.get_mut(symbol) {
            let initial_len = orders.len();
            orders.retain(|order| order.order_id != order_id);
            if orders.len() < initial_len {
                Ok(())
            } else {
                Err(DexError::Permanent(format!(
                    "Order ID {} not found for symbol {}",
                    order_id, symbol
                )))
            }
        } else {
            Err(DexError::Permanent(format!(
                "No canceled orders found for symbol {}",
                symbol
            )))
        }
    }

    async fn clear_all_canceled_orders(&self) -> Result<(), DexError> {
        let mut canceled_orders = self.canceled_orders.write().await;
        canceled_orders.clear();
        Ok(())
    }

    async fn create_order(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        price: Option<Decimal>,
        spread: Option<i64>,
        reduce_only: bool,
        expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        // bot-strategy#480: `price=None` is the contract for "market" — every
        // pairtrade caller (entry / exit / hedge recovery / partial-fill
        // reissue, 9 sites) passes None to mean "cross now". Matches Lighter's
        // `create_order(price=None)` which already routes to IOC with a 20 %
        // protection price. Pre-#480, Extended ran this through
        // `choose_base_price` → touch → `submit_order_with_market` which
        // emits LIMIT GTT (1 h passive maker). In fast markets the touch
        // moves by ≥ 1 tick before the order lands, the limit becomes
        // non-crossing maker, sits for the next 5 s reconcile tick, gets
        // cancelled, and the cycle repeats — observed on `debot-pair-btceth-
        // extended` 2026-06-03..06-06 as a 54 k-retry order/cancel loop that
        // burned no capital but blocked all entries / exits. Route the
        // `price=None` case to `submit_taker_ioc` (touch ± 1 tick + IOC) so
        // the order always either fills or is venue-cancelled on first match,
        // never sitting as a maker.
        if price.is_none() {
            return self
                .submit_taker_ioc(symbol, size, side, 0, reduce_only)
                .await;
        }
        let post_only = matches!(spread, Some(-2));
        self.create_order_internal(
            symbol,
            size,
            side,
            price,
            reduce_only,
            expiry_secs,
            post_only,
            false,
        )
        .await
    }

    /// bot-strategy#302: real taker IOC for callers that want
    /// "cross now or cancel" semantics. Reuses the same IOC + aggressive
    /// pricing pattern as `close_all_positions` (touch ± 1 tick + slippage,
    /// `time_in_force=IOC`, `post_only=false`). Unlike `create_order`,
    /// this does not fall back to GTT — a non-crossing order is cancelled
    /// by the venue immediately rather than left as a 1 h passive maker.
    async fn create_order_taker_ioc(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        slippage_bps: u32,
        reduce_only: bool,
    ) -> Result<CreateOrderResponse, DexError> {
        self.submit_taker_ioc(symbol, size, side, slippage_bps, reduce_only)
            .await
    }

    /// Standalone TPSL (currently only Stop-Loss × MarketWithSlippageControl).
    ///
    /// Extended models a standalone SL as `type=TPSL` / `tpSlType=ORDER` with
    /// the main leg carrying `price=0` and **no settlement**; the SL leg
    /// carries its own Stark signature computed over (same side, size,
    /// slippage-adjusted price).
    ///
    /// `side` is the **execution side** of the SL trigger (i.e. the close
    /// trade direction): `Short` = SELL to close a long position, `Long` =
    /// BUY to close a short position. The slippage adjustment always moves
    /// the execution price against the trader for guaranteed fill.
    ///
    /// TP, Limit and Market-without-slippage variants return an error for
    /// now. They can be added incrementally when a caller needs them.
    async fn create_advanced_trigger_order(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        trigger_px: Decimal,
        _limit_px: Option<Decimal>,
        order_style: TriggerOrderStyle,
        slippage_bps: Option<u32>,
        tpsl: TpSl,
        reduce_only: bool,
        expiry_secs: Option<u64>,
    ) -> Result<CreateOrderResponse, DexError> {
        if !matches!(tpsl, TpSl::Sl) {
            return Err(DexError::Permanent(
                "Extended trigger: only TpSl::Sl is implemented (TP not supported yet)".into(),
            ));
        }
        if !matches!(order_style, TriggerOrderStyle::MarketWithSlippageControl) {
            return Err(DexError::Permanent(format!(
                "Extended trigger: only MarketWithSlippageControl is implemented; got {:?}",
                order_style
            )));
        }
        let slippage_bps = slippage_bps.ok_or_else(|| DexError::InvalidInput {
            field: "slippage_bps".to_string(),
            value: "required for MarketWithSlippageControl".to_string(),
        })?;
        if !reduce_only {
            return Err(DexError::InvalidInput {
                field: "reduce_only".to_string(),
                value: "Extended TPSL orders must be reduce_only=true (Python SDK parity)"
                    .to_string(),
            });
        }

        let market = self.get_market(symbol).await?;
        let rounded_size = pricing::round_size_for_market(size, &market)?;

        // Execution price = trigger ± slippage, always worse for the close side.
        let slippage_factor = Decimal::new(slippage_bps as i64, 4);
        let raw_leg_price = match side {
            OrderSide::Long => trigger_px * (Decimal::ONE + slippage_factor),
            OrderSide::Short => trigger_px * (Decimal::ONE - slippage_factor),
        };
        // Tick-only rounding (no floor/cap clamp): trigger orders must be able
        // to sit well outside the current index band. `round_price_for_market`
        // would clamp an SL execution price back *into* the current band and
        // the resulting StarkEx signature then covers a different price than
        // the API receives, producing "Invalid StarkEx signature" (1101) on
        // the server.
        let tick = market.trading_config.min_price_change;
        let leg_rounding = match side {
            OrderSide::Long => RoundingStrategy::ToPositiveInfinity, // worse for buyer = higher
            OrderSide::Short => RoundingStrategy::ToNegativeInfinity, // worse for seller = lower
        };
        let leg_price = pricing::round_to_step(raw_leg_price, tick, leg_rounding);
        let trigger_px_rounded =
            pricing::round_to_step(trigger_px, tick, RoundingStrategy::MidpointAwayFromZero);
        if leg_price <= Decimal::ZERO || trigger_px_rounded <= Decimal::ZERO {
            return Err(DexError::InvalidInput {
                field: "price".to_string(),
                value: format!(
                    "non-positive rounded (trigger={} leg={}) for {}",
                    trigger_px_rounded, leg_price, market.name
                ),
            });
        }

        // SL safety net usually outlives entry trades by minutes/hours at
        // least. Default 28d matches slow-mm's convention.
        let expire_time = match expiry_secs {
            Some(secs) => Utc::now() + Duration::seconds(secs as i64),
            None => Utc::now() + Duration::days(28),
        };
        let nonce = rand::random::<u32>() as u64;
        let side_str = match side {
            OrderSide::Long => "BUY",
            OrderSide::Short => "SELL",
        };

        // Main order id is the Poseidon hash of (same side, size, price=0),
        // following the Python SDK. Price is 0 because the main leg of a
        // TPSL order is not an economic transaction — the leg is.
        let main_settlement = self.compute_settlement(
            &market,
            side_str,
            rounded_size,
            Decimal::ZERO,
            expire_time,
            nonce,
        )?;
        let order_id = main_settlement.order_hash.to_string();

        // SL leg settlement: signs the *execution* price. Python SDK reuses
        // the same nonce and expire_time across main and leg via a shared
        // SettlementDataCtx; do the same here to match server expectations.
        let leg_settlement = self.compute_settlement(
            &market,
            side_str,
            rounded_size,
            leg_price,
            expire_time,
            nonce,
        )?;

        let stop_loss_leg = TpslLegModel {
            trigger_price: trigger_px_rounded,
            trigger_price_type: "LAST".to_string(),
            price: leg_price,
            price_type: "LIMIT".to_string(), // slippage-adjusted limit
            settlement: leg_settlement.settlement,
            debugging_amounts: Some(leg_settlement.debugging_amounts),
        };

        let order = NewOrderModel {
            id: order_id.clone(),
            market: market.name.clone(),
            order_type: "TPSL".to_string(),
            side: side_str.to_string(),
            qty: rounded_size,
            price: Decimal::ZERO,
            reduce_only: true,
            post_only: false,
            time_in_force: "GTT".to_string(),
            expiry_epoch_millis: Self::to_epoch_millis(expire_time),
            fee: main_settlement.fee_rate,
            self_trade_protection_level: "ACCOUNT".to_string(),
            nonce: Decimal::from(nonce),
            cancel_id: None,
            settlement: None,
            tp_sl_type: Some("ORDER".to_string()),
            take_profit: None,
            stop_loss: Some(stop_loss_leg),
            debugging_amounts: None,
            builder_fee: None,
            builder_id: None,
        };

        log::info!(
            "[trigger_order][extended] sym={} side={} trigger={} leg_price={} size={} slippage_bps={}",
            market.name, side_str, trigger_px_rounded, leg_price, rounded_size, slippage_bps
        );

        let response: PlacedOrderModel = self
            .api
            .post("/user/order".to_string(), order, true)
            .await?;

        Ok(CreateOrderResponse {
            order_id: response.external_id,
            exchange_order_id: Some(response.id.to_string()),
            ordered_price: leg_price,
            ordered_size: rounded_size,
            client_order_id: None,
        })
    }

    async fn cancel_order(&self, _symbol: &str, order_id: &str) -> Result<(), DexError> {
        let path = build_query(
            "/user/order",
            vec![("externalId".to_string(), order_id.to_string())],
        );
        let _response: EmptyResponse = self.api.delete(path, true).await?;
        Ok(())
    }

    async fn cancel_all_orders(&self, symbol: Option<String>) -> Result<(), DexError> {
        let payload = match symbol {
            Some(symbol) => json!({
                "markets": vec![symbol],
                "cancelAll": false,
            }),
            None => json!({
                "cancelAll": true,
            }),
        };
        let _response: EmptyResponse = self
            .api
            .post("/user/order/massCancel".to_string(), payload, true)
            .await?;
        Ok(())
    }

    async fn cancel_orders(
        &self,
        _symbol: Option<String>,
        order_ids: Vec<String>,
    ) -> Result<(), DexError> {
        let payload = json!({
            "externalOrderIds": order_ids,
        });
        let _response: EmptyResponse = self
            .api
            .post("/user/order/massCancel".to_string(), payload, true)
            .await?;
        Ok(())
    }

    async fn close_all_positions(&self, symbol: Option<String>) -> Result<(), DexError> {
        let positions = if self.websocket_url.is_some() {
            match self
                .api
                .get::<Vec<PositionModel>>("/user/positions".to_string(), true)
                .await
            {
                Ok(positions) => {
                    let snapshots = positions
                        .into_iter()
                        .filter_map(position_snapshot_from_model)
                        .collect::<Vec<_>>();
                    let mut cache = self.positions_cache.write().await;
                    *cache = Some(snapshots.clone());
                    snapshots
                }
                Err(err) => {
                    let cache = self.positions_cache.read().await;
                    if let Some(cached) = cache.clone() {
                        if !cached.is_empty() {
                            log::warn!(
                                "[close_all_positions][extended] REST fetch failed; using WS cache: {}",
                                err
                            );
                            cached
                        } else {
                            return Err(err);
                        }
                    } else {
                        return Err(err);
                    }
                }
            }
        } else {
            let positions: Vec<PositionModel> =
                self.api.get("/user/positions".to_string(), true).await?;
            positions
                .into_iter()
                .filter_map(position_snapshot_from_model)
                .collect::<Vec<_>>()
        };

        let mut last_err: Option<DexError> = None;
        for position in positions {
            if symbol.as_deref().is_some_and(|s| s != position.symbol) {
                continue;
            }

            let side = if position.sign > 0 {
                OrderSide::Short
            } else {
                OrderSide::Long
            };
            let expire_time = Utc::now() + Duration::hours(1);

            let nonce = rand::random::<u32>() as u64;
            let market = self.get_market(&position.symbol).await?;
            let side_str = match side {
                OrderSide::Long => "BUY",
                OrderSide::Short => "SELL",
            };
            let tick = market.trading_config.min_price_change;
            let mut base_price: Option<Decimal> = None;
            if let Ok(ob) = self.get_order_book_rest(&position.symbol, 1).await {
                base_price = match side {
                    OrderSide::Long => ob.asks.first().map(|level| level.price),
                    OrderSide::Short => ob.bids.first().map(|level| level.price),
                };
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
                        OrderSide::Long => {
                            order_price += tick;
                        }
                        OrderSide::Short => {
                            order_price -= tick;
                        }
                    }
                }
                order_price = pricing::apply_close_slippage_bps(
                    order_price,
                    self.close_all_positions_slippage_bps,
                    side,
                );
            }
            let rounded_price =
                pricing::round_price_for_market_aggressive(order_price, &market, side);
            let rounded_size = pricing::round_size_for_market(position.size, &market)?;

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
                reduce_only: true,
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
                "[close_all_positions] {} side={} size={} price={} tif=IOC reduce_only=true source={} slippage_bps={}",
                position.symbol,
                side_str,
                rounded_size,
                rounded_price,
                if used_market_stats { "stats" } else { "order_book" },
                if used_market_stats {
                    0
                } else {
                    self.close_all_positions_slippage_bps
                }
            );

            match self
                .api
                .post::<PlacedOrderModel, _>("/user/order".to_string(), order, true)
                .await
            {
                Ok(response) => {
                    log::info!(
                        "[close_all_positions] {} order placed id={} external_id={}",
                        position.symbol,
                        response.id,
                        response.external_id
                    );
                }
                Err(err) => {
                    log::error!(
                        "[close_all_positions] Failed to close {}: {}",
                        position.symbol,
                        err
                    );
                    last_err = Some(err);
                }
            }
        }

        if let Some(err) = last_err {
            Err(err)
        } else {
            Ok(())
        }
    }

    async fn clear_last_trades(&self, symbol: &str) -> Result<(), DexError> {
        let mut last_trades = self.last_trades.write().await;
        if last_trades.remove(symbol).is_some() {
            Ok(())
        } else {
            Err(DexError::Permanent(format!(
                "No last trades found for symbol {}",
                symbol
            )))
        }
    }

    async fn is_upcoming_maintenance(&self, hours_ahead: i64) -> bool {
        // Operator kill-switch: short-circuit before touching any cache.
        if extended_maintenance_disabled() {
            return false;
        }
        // REST-1010 fallback: a recent REST response carried `code:1010
        // "Maintenance mode"`, so the venue is rejecting requests right
        // now even if the `/info/markets` poller hasn't flipped its flag.
        // Auto-clears `REST_1010_GRACE_SECS` after the last 1010 hit.
        // bot-strategy#321.
        let now_ts = Utc::now().timestamp();
        let clear_at_1010 = self.rest_1010_clear_at.load(Ordering::SeqCst);
        if clear_at_1010 > now_ts {
            log::debug!(
                "[EXTENDED_MAINTENANCE] is_upcoming_maintenance=true via REST 1010 fallback (clears in {}s)",
                clear_at_1010 - now_ts
            );
            return true;
        }
        // REST-1011 fallback: a recent REST response carried `code:1011
        // "Post only mode"`, i.e. the venue is currently rejecting
        // aggressive (non-post-only) orders. Treated as the same business
        // class as maintenance — entries / aggressive closes will keep
        // failing until the window ends. Self-clears after a much shorter
        // grace than 1010 (~25s observed window). bot-strategy#327.
        let clear_at_1011 = self.rest_1011_clear_at.load(Ordering::SeqCst);
        if clear_at_1011 > now_ts {
            log::debug!(
                "[EXTENDED_MAINTENANCE] is_upcoming_maintenance=true via REST 1011 fallback (clears in {}s)",
                clear_at_1011 - now_ts
            );
            return true;
        }
        // Reactive path: refresher observed a tracked symbol in a
        // non-ACTIVE state. Covers already-started maintenance regardless
        // of `hours_ahead`.
        if self.maintenance_symbol_inactive.load(Ordering::SeqCst) {
            log::debug!(
                "[EXTENDED_MAINTENANCE] is_upcoming_maintenance=true via reactive symbol status"
            );
            return true;
        }
        // Scheduled path: operator-declared windows from env var.
        let now = Utc::now();
        let hit =
            Self::maintenance_within_window(self.maintenance_windows.as_ref(), &now, hours_ahead);
        if hit {
            log::debug!(
                "[EXTENDED_MAINTENANCE] is_upcoming_maintenance=true via declared window (hours_ahead={hours_ahead})"
            );
        }
        hit
    }

    async fn sign_evm_65b(&self, _message: &str) -> Result<String, DexError> {
        Err(DexError::Permanent(
            "sign_evm_65b not supported for Extended".to_string(),
        ))
    }

    async fn sign_evm_65b_with_eip191(&self, _message: &str) -> Result<String, DexError> {
        Err(DexError::Permanent(
            "sign_evm_65b_with_eip191 not supported for Extended".to_string(),
        ))
    }
}

#[derive(Debug, Deserialize)]
struct EmptyResponse {}

pub async fn create_extended_connector(
    api_key: String,
    public_key: String,
    private_key: String,
    vault: u64,
    base_url: Option<String>,
    websocket_url: Option<String>,
    tracked_symbols: Vec<String>,
) -> Result<Box<dyn DexConnector>, DexError> {
    let connector = ExtendedConnector::new(
        api_key,
        public_key,
        private_key,
        vault,
        base_url,
        websocket_url,
        tracked_symbols,
    )
    .await?;
    Ok(Box::new(connector))
}

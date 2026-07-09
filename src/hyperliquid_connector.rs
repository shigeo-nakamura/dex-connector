use crate::{
    dex_connector::DexConnector, dex_request::DexError, BalanceResponse, CanceledOrdersResponse,
    CombinedBalanceResponse, CreateOrderResponse, FilledOrdersResponse, LastTradesResponse,
    OpenOrdersResponse, OrderBookLevel, OrderBookSnapshot, OrderSide, PositionSnapshot,
    PriceUpdate, TickerResponse, TpSl, TriggerOrderStyle,
};
use async_trait::async_trait;
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::RwLock;

const DEFAULT_BASE_URL: &str = "https://api.hyperliquid.xyz";

#[derive(Clone, Debug)]
pub struct HyperliquidConnectorConfig {
    pub base_url: String,
    pub tracked_symbols: Vec<String>,
}

pub struct HyperliquidConnector {
    base_url: String,
    client: Client,
    tracked_symbols: Vec<String>,
    market_info: RwLock<HashMap<String, StaticMarketInfo>>,
}

#[derive(Clone, Debug)]
struct StaticMarketInfo {
    size_decimals: u32,
    #[allow(dead_code)]
    max_leverage: Option<u32>,
    is_delisted: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MetaResponse {
    universe: Vec<MetaUniverse>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MetaUniverse {
    name: String,
    sz_decimals: u32,
    max_leverage: Option<u32>,
    #[serde(default)]
    is_delisted: bool,
}

#[derive(Debug, Deserialize)]
struct L2BookResponse {
    #[allow(dead_code)]
    coin: String,
    time: u64,
    levels: [Vec<L2Level>; 2],
}

#[derive(Clone, Debug, Deserialize)]
struct L2Level {
    px: String,
    sz: String,
    #[allow(dead_code)]
    n: u64,
}

pub fn create_hyperliquid_connector(
    config: HyperliquidConnectorConfig,
) -> Result<Box<dyn DexConnector>, DexError> {
    Ok(Box::new(HyperliquidConnector::new(config)?))
}

impl HyperliquidConnector {
    pub fn new(config: HyperliquidConnectorConfig) -> Result<Self, DexError> {
        let client = Client::builder()
            .user_agent("debot/1.0")
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(15))
            .build()?;
        let base_url = if config.base_url.trim().is_empty() {
            DEFAULT_BASE_URL.to_string()
        } else {
            config.base_url.trim_end_matches('/').to_string()
        };
        Ok(Self {
            base_url,
            client,
            tracked_symbols: config.tracked_symbols,
            market_info: RwLock::new(HashMap::new()),
        })
    }

    async fn post_info<T>(&self, payload: serde_json::Value) -> Result<T, DexError>
    where
        T: serde::de::DeserializeOwned,
    {
        let url = format!("{}/info", self.base_url);
        let response = self.client.post(&url).json(&payload).send().await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(DexError::ServerResponse(format!(
                "Hyperliquid /info status={} body={}",
                status, body
            )));
        }
        serde_json::from_str(&body).map_err(DexError::Serde)
    }

    async fn refresh_market_metadata(&self) -> Result<(), DexError> {
        let meta: MetaResponse = self.post_info(json!({ "type": "meta" })).await?;
        let mut cache = self.market_info.write().await;
        cache.clear();
        for market in meta.universe {
            cache.insert(
                market.name,
                StaticMarketInfo {
                    size_decimals: market.sz_decimals,
                    max_leverage: market.max_leverage,
                    is_delisted: market.is_delisted,
                },
            );
        }
        Ok(())
    }

    async fn market_info_for(&self, symbol: &str) -> Result<StaticMarketInfo, DexError> {
        let coin = resolve_coin(symbol);
        if let Some(info) = self.market_info.read().await.get(&coin).cloned() {
            return Ok(info);
        }
        self.refresh_market_metadata().await?;
        self.market_info
            .read()
            .await
            .get(&coin)
            .cloned()
            .ok_or_else(|| DexError::Permanent(format!("Hyperliquid market not found: {symbol}")))
    }

    async fn get_all_mids(&self) -> Result<HashMap<String, String>, DexError> {
        self.post_info(json!({ "type": "allMids" })).await
    }

    async fn get_l2_book(&self, symbol: &str) -> Result<L2BookResponse, DexError> {
        let coin = resolve_coin(symbol);
        self.post_info(json!({ "type": "l2Book", "coin": coin }))
            .await
    }

    fn unsupported(operation: &str) -> DexError {
        DexError::Permanent(format!(
            "Hyperliquid {operation} is not enabled in the read-only connector slice (bot-strategy#709)"
        ))
    }
}

#[async_trait]
impl DexConnector for HyperliquidConnector {
    async fn start(&self) -> Result<(), DexError> {
        self.refresh_market_metadata().await?;
        log::info!(
            "[hyperliquid] read-only connector started (tracked_symbols={})",
            self.tracked_symbols.join(",")
        );
        Ok(())
    }

    async fn stop(&self) -> Result<(), DexError> {
        Ok(())
    }

    async fn restart(&self, _max_retries: i32) -> Result<(), DexError> {
        self.start().await
    }

    async fn set_leverage(&self, _symbol: &str, _leverage: u32) -> Result<(), DexError> {
        Err(Self::unsupported("set_leverage"))
    }

    async fn get_ticker(
        &self,
        symbol: &str,
        test_price: Option<Decimal>,
    ) -> Result<TickerResponse, DexError> {
        let coin = resolve_coin(symbol);
        let market_info = match self.market_info_for(symbol).await {
            Ok(info) => Some(info),
            Err(err) if test_price.is_some() => {
                log::debug!("[hyperliquid] metadata unavailable for test ticker {symbol}: {err}");
                None
            }
            Err(err) => return Err(err),
        };
        if matches!(market_info.as_ref(), Some(info) if info.is_delisted) {
            return Err(DexError::Permanent(format!(
                "Hyperliquid market is delisted: {symbol}"
            )));
        }
        let size_decimals = market_info
            .as_ref()
            .map(|info| info.size_decimals)
            .unwrap_or(0);

        let price = if let Some(price) = test_price {
            price
        } else {
            let mids = self.get_all_mids().await?;
            let raw = mids.get(&coin).ok_or_else(|| {
                DexError::Transient(format!("Hyperliquid allMids missing symbol: {symbol}"))
            })?;
            parse_decimal(raw, "allMids price")?
        };

        Ok(TickerResponse {
            symbol: symbol.to_string(),
            price,
            min_tick: Some(calculate_min_tick(price, size_decimals, false)),
            min_order: None,
            size_decimals: Some(size_decimals),
            volume: Some(Decimal::ZERO),
            num_trades: None,
            open_interest: None,
            funding_rate: None,
            oracle_price: None,
            exchange_ts: None,
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

    async fn get_last_trades(&self, _symbol: &str) -> Result<LastTradesResponse, DexError> {
        Err(Self::unsupported("get_last_trades"))
    }

    async fn get_order_book(
        &self,
        symbol: &str,
        depth: usize,
    ) -> Result<OrderBookSnapshot, DexError> {
        let book = self.get_l2_book(symbol).await?;
        let convert = |levels: &[L2Level]| -> Result<Vec<OrderBookLevel>, DexError> {
            levels
                .iter()
                .take(depth)
                .map(|level| {
                    Ok(OrderBookLevel {
                        price: parse_decimal(&level.px, "l2Book px")?,
                        size: parse_decimal(&level.sz, "l2Book sz")?,
                    })
                })
                .collect()
        };
        Ok(OrderBookSnapshot {
            bids: convert(&book.levels[0])?,
            asks: convert(&book.levels[1])?,
            // REST-polled book: no genuine feed age, so this must stay None
            // per the OrderBookSnapshot contract (bot-strategy#552) until the
            // connector has a streaming book source.
            book_ts_ms: None,
        })
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
        false
    }

    async fn sign_evm_65b(&self, _message: &str) -> Result<String, DexError> {
        Err(Self::unsupported("sign_evm_65b"))
    }

    async fn sign_evm_65b_with_eip191(&self, _message: &str) -> Result<String, DexError> {
        Err(Self::unsupported("sign_evm_65b_with_eip191"))
    }

    fn subscribe_price_updates(
        &self,
    ) -> Result<tokio::sync::broadcast::Receiver<PriceUpdate>, DexError> {
        Err(Self::unsupported("subscribe_price_updates"))
    }
}

fn parse_decimal(raw: &str, field: &str) -> Result<Decimal, DexError> {
    Decimal::from_str(raw).map_err(|err| {
        DexError::Transient(format!(
            "Hyperliquid decimal parse failed for {field}: value={raw} err={err}"
        ))
    })
}

fn resolve_coin(symbol: &str) -> String {
    symbol
        .split_once('-')
        .map(|(base, _)| base)
        .unwrap_or(symbol)
        .to_string()
}

fn calculate_min_tick(price: Decimal, sz_decimals: u32, is_spot: bool) -> Decimal {
    let price_str = price.normalize().to_string();
    let integer_part = price_str
        .split('.')
        .next()
        .unwrap_or("0")
        .trim_start_matches('-');
    let integer_digits = if integer_part == "0" {
        0
    } else {
        integer_part.len()
    };
    let scale_by_sig = if integer_digits >= 5 {
        0
    } else if integer_digits > 0 {
        (5 - integer_digits) as u32
    } else {
        // Sub-dollar prices: significant figures start at the first non-zero
        // fractional digit, so each leading zero extends the allowed scale.
        let fraction = price_str.split('.').nth(1).unwrap_or("");
        let leading_zeros = fraction.chars().take_while(|c| *c == '0').count() as u32;
        leading_zeros + 5
    };
    let max_decimals: u32 = if is_spot { 8 } else { 6 };
    let scale_by_dec = max_decimals.saturating_sub(sz_decimals);
    Decimal::new(1, scale_by_sig.min(scale_by_dec))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_perp_coin_from_common_symbol_forms() {
        assert_eq!(resolve_coin("BTC"), "BTC");
        assert_eq!(resolve_coin("BTC-USD"), "BTC");
        assert_eq!(resolve_coin("ETH-USDC"), "ETH");
    }

    #[test]
    fn calculates_hyperliquid_perp_ticks() {
        assert_eq!(
            calculate_min_tick(Decimal::from_str("62757.0").unwrap(), 5, false),
            Decimal::new(1, 0)
        );
        assert_eq!(
            calculate_min_tick(Decimal::from_str("1775.05").unwrap(), 4, false),
            Decimal::new(1, 1)
        );
        assert_eq!(
            calculate_min_tick(Decimal::from_str("81.3405").unwrap(), 2, false),
            Decimal::new(1, 3)
        );
        // Sub-dollar: sig figs start at the first non-zero fractional digit
        // (0.001234 → 5 sig figs would allow 7 decimals, capped at 6).
        assert_eq!(
            calculate_min_tick(Decimal::from_str("0.001234").unwrap(), 0, false),
            Decimal::new(1, 6)
        );
        assert_eq!(
            calculate_min_tick(Decimal::from_str("0.12345").unwrap(), 0, false),
            Decimal::new(1, 5)
        );
        // szDecimals still caps the scale for sub-dollar prices.
        assert_eq!(
            calculate_min_tick(Decimal::from_str("0.001234").unwrap(), 2, false),
            Decimal::new(1, 4)
        );
    }

    #[test]
    fn parses_l2_book_shape() {
        let raw = r#"{
            "coin":"BTC",
            "time":1783266991685,
            "levels":[
                [{"px":"62757.0","sz":"15.91948","n":63}],
                [{"px":"62758.0","sz":"5.3272","n":24}]
            ]
        }"#;
        let parsed: L2BookResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(parsed.coin, "BTC");
        assert_eq!(parsed.time, 1783266991685);
        assert_eq!(parsed.levels[0][0].px, "62757.0");
        assert_eq!(parsed.levels[1][0].sz, "5.3272");
    }
}

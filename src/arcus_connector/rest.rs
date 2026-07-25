use super::book::BookState;
use super::models::{MarketInfo, MarketsResponse, OrderbookSnapshotWire, TradesResponse};
use super::{ArcusConnector, MARKET_METADATA_TTL_MS};
use crate::{DexError, LastTrade, LastTradesResponse, OrderBookSnapshot};
use reqwest::{RequestBuilder, StatusCode};
use serde::de::DeserializeOwned;
use std::collections::HashMap;

impl ArcusConnector {
    pub(super) async fn refresh_market_metadata(&self) -> Result<(), DexError> {
        let response: MarketsResponse = self
            .request_json(
                self.client.get(format!("{}/v1/markets", self.base_url)),
                "GET /v1/markets",
            )
            .await?;

        let mut parsed = HashMap::with_capacity(response.markets.len());
        for wire in response.markets {
            let info = MarketInfo::try_from(wire)?;
            parsed.insert(info.market.clone(), info);
        }
        if parsed.is_empty() {
            return Err(DexError::Transient(
                "Arcus GET /v1/markets returned no markets".to_string(),
            ));
        }
        let now = super::now_ms();
        let fetched_ms = parsed.keys().map(|market| (market.clone(), now)).collect();
        *self.market_info.write().await = parsed;
        *self.market_info_fetched_ms.write().await = fetched_ms;
        Ok(())
    }

    /// Returns cached market metadata, transparently refreshing it over REST
    /// once it is older than `MARKET_METADATA_TTL_MS`. This keeps funding
    /// rate, oracle price, open interest, volume, and trade count from going
    /// permanently stale on a connector with a healthy WebSocket book, since
    /// `start()` only populates this cache once (bot-strategy#749 review).
    pub(super) async fn market_info_for(&self, symbol: &str) -> Result<MarketInfo, DexError> {
        let market = super::normalize_market(symbol);
        if self.market_info_is_fresh(&market).await {
            if let Some(info) = self.market_info.read().await.get(&market).cloned() {
                return Ok(info);
            }
        }
        match self.fetch_market_info(&market).await {
            Ok(info) => Ok(info),
            Err(err) => match self.market_info.read().await.get(&market).cloned() {
                Some(stale) => {
                    log::warn!(
                        "[arcus] market metadata refresh failed for {market}, using stale cache: {err}"
                    );
                    Ok(stale)
                }
                None => Err(err),
            },
        }
    }

    async fn market_info_is_fresh(&self, market: &str) -> bool {
        let now = super::now_ms();
        self.market_info_fetched_ms
            .read()
            .await
            .get(market)
            .is_some_and(|fetched_ms| now.saturating_sub(*fetched_ms) <= MARKET_METADATA_TTL_MS)
    }

    pub(super) async fn fetch_market_info(&self, market: &str) -> Result<MarketInfo, DexError> {
        let response: MarketsResponse = self
            .request_json(
                self.client
                    .get(format!("{}/v1/markets", self.base_url))
                    .query(&[("market", market)]),
                "GET /v1/markets?market",
            )
            .await?;
        let wire = response
            .markets
            .into_iter()
            .next()
            .ok_or_else(|| DexError::Permanent(format!("Arcus market not found: {market}")))?;
        let info = MarketInfo::try_from(wire)?;
        self.market_info
            .write()
            .await
            .insert(info.market.clone(), info.clone());
        self.market_info_fetched_ms
            .write()
            .await
            .insert(info.market.clone(), super::now_ms());
        Ok(info)
    }

    pub(super) async fn fetch_order_book_rest(
        &self,
        market: &str,
        depth: usize,
    ) -> Result<OrderBookSnapshot, DexError> {
        if depth == 0 {
            return Ok(OrderBookSnapshot::default());
        }
        let n_levels = depth.clamp(1, 100).to_string();
        let encoded = urlencoding::encode(market);
        let wire: OrderbookSnapshotWire = self
            .request_json(
                self.client
                    .get(format!("{}/v1/l2OrderBook/{}", self.base_url, encoded))
                    .query(&[("nLevels", n_levels.as_str())]),
                "GET /v1/l2OrderBook/{market}",
            )
            .await?;

        let mut state = BookState::default();
        state.replace_from_rest(market, &wire, super::now_ms())?;
        let mut snapshot = state.snapshot(depth).unwrap_or_default();
        // A REST response is a point-in-time fetch, not evidence that the
        // streaming feed is currently fresh. Keep this None per the shared
        // OrderBookSnapshot contract (bot-strategy#552).
        snapshot.book_ts_ms = None;
        Ok(snapshot)
    }

    pub(super) async fn fetch_last_trades(
        &self,
        market: &str,
    ) -> Result<LastTradesResponse, DexError> {
        let response: TradesResponse = self
            .request_json(
                self.client
                    .get(format!("{}/v1/trades", self.base_url))
                    .query(&[("market", market), ("limit", "100")]),
                "GET /v1/trades",
            )
            .await?;

        let trades = response
            .trades
            .into_iter()
            .map(|trade| {
                Ok(LastTrade {
                    price: super::models::parse_decimal(&trade.price, market, "trade.price")?,
                    size: Some(super::models::parse_decimal(
                        &trade.size,
                        market,
                        "trade.size",
                    )?),
                    side: trade.order_side(),
                })
            })
            .collect::<Result<Vec<_>, DexError>>()?;
        Ok(LastTradesResponse { trades })
    }

    async fn request_json<T>(&self, request: RequestBuilder, operation: &str) -> Result<T, DexError>
    where
        T: DeserializeOwned,
    {
        let response = request.send().await?;
        let status = response.status();
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned);
        let body = response.text().await?;

        if !status.is_success() {
            let detail = format!(
                "Arcus {operation} status={status}{} body={body}",
                retry_after
                    .as_deref()
                    .map(|value| format!(" retry_after={value}"))
                    .unwrap_or_default()
            );
            return if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                Err(DexError::Transient(detail))
            } else {
                Err(DexError::Permanent(detail))
            };
        }

        serde_json::from_str(&body).map_err(|err| {
            DexError::Transient(format!(
                "Arcus {operation} response decode failed: {err}; body={body}"
            ))
        })
    }
}

use super::{
    parse_raw_amount, validate_token, ArcusSpotClient, ArcusSpotError, ArcusSpotObservation,
    ArcusSpotToken,
};
use chrono::{DateTime, Utc};
use ethers::types::{Address, H256, U256};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashSet},
    str::FromStr,
};

const DEFAULT_TRADE_PAGE_LIMIT: u16 = 50;
const MAX_TRADE_PAGE_LIMIT: u16 = 500;

/// Per-chain finality and lag reported by the public Arcus Spot indexer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ArcusSpotIndexerChainStats {
    pub chain_id: u64,
    pub name: String,
    pub last_indexed_block: String,
    pub finalized_head: String,
    pub lag: String,
    pub trades: u64,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Public indexer status envelope.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArcusSpotIndexerStats {
    pub chains: Vec<ArcusSpotIndexerChainStats>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Token metadata attached to indexer trade pages.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ArcusSpotIndexerToken {
    pub chain_id: u64,
    pub address: String,
    pub symbol: String,
    pub name: String,
    pub decimals: u32,
    #[serde(default)]
    pub underlying: Option<String>,
    pub first_seen_block: String,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Exact, string-preserving execution-quality fields from a finalized trade.
///
/// Arcus currently emits values with more fractional digits than Decimal can
/// represent. Keep the wire values byte-exact and validate their syntax.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ArcusSpotTradePnl {
    pub priced: bool,
    pub usd_notional: Option<String>,
    pub fee_usd: String,
    pub token_exec_price_usd: Option<String>,
    pub price_improvement_usd: Option<String>,
    pub slippage_headroom_usd: Option<String>,
    pub side: Option<String>,
    pub base_token: Option<String>,
    pub base_qty: Option<String>,
    pub realized_pnl_usd: Option<String>,
    pub avg_cost_usd: Option<String>,
    pub matched_qty: Option<String>,
    pub position_qty_after: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// One finalized SwapShell.SwapExecuted row.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ArcusSpotTrade {
    pub id: String,
    pub chain_id: u64,
    pub block_number: String,
    pub block_timestamp: DateTime<Utc>,
    pub tx_hash: String,
    pub log_index: u64,
    pub swap_shell: String,
    pub taker: String,
    pub token_in: String,
    pub token_out: String,
    pub min_amount_out: String,
    pub amount_in: String,
    pub quoted_amount_in: String,
    pub quoted_amount_out: String,
    pub amount_out: String,
    pub token_in_benchmark_price: String,
    pub token_out_benchmark_price: String,
    pub router: String,
    pub route_tag: Option<String>,
    pub route_tag_raw: String,
    pub success: bool,
    pub reason: String,
    pub token_in_wrapped: bool,
    pub token_out_wrapped: bool,
    pub raw_token_in: String,
    pub raw_token_out: String,
    pub pnl: ArcusSpotTradePnl,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Newest-first cursor page. Transaction lookups omit nextCursor.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ArcusSpotTradePage {
    pub trades: Vec<ArcusSpotTrade>,
    pub tokens: BTreeMap<String, ArcusSpotIndexerToken>,
    #[serde(default)]
    pub next_cursor: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Public trade filters. The chain always comes from ArcusSpotConfig.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct ArcusSpotTradeQuery {
    pub taker: Option<String>,
    pub token_in: Option<String>,
    pub token_out: Option<String>,
    pub router: Option<String>,
    pub route_tag: Option<String>,
    pub success: Option<bool>,
    pub priced: Option<bool>,
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub cursor: Option<String>,
    pub limit: u16,
}

impl Default for ArcusSpotTradeQuery {
    fn default() -> Self {
        Self {
            taker: None,
            token_in: None,
            token_out: None,
            router: None,
            route_tag: None,
            success: None,
            priced: None,
            from_block: None,
            to_block: None,
            cursor: None,
            limit: DEFAULT_TRADE_PAGE_LIMIT,
        }
    }
}

/// Filters an indexer trade page is expected to satisfy. Bundled into a
/// struct (rather than passed positionally) so `ArcusSpotTradePage::validate`
/// stays under clippy's argument-count lint as filters are added.
#[derive(Debug, Clone, Copy, Default)]
struct TradeExpectations<'a> {
    token_in: Option<&'a str>,
    token_out: Option<&'a str>,
    taker: Option<&'a str>,
    tx_hash: Option<&'a str>,
    router: Option<&'a str>,
    route_tag: Option<&'a str>,
    success: Option<bool>,
    priced: Option<bool>,
    from_block: Option<u64>,
    to_block: Option<u64>,
}

impl ArcusSpotClient {
    /// Fetch public finalized-indexer lag and chain status.
    pub async fn indexer_stats(
        &self,
    ) -> Result<ArcusSpotObservation<ArcusSpotIndexerStats>, ArcusSpotError> {
        let observation: ArcusSpotObservation<ArcusSpotIndexerStats> = self
            .get_json(&self.inner.indexer_base_url, "stats", &[])
            .await?;
        observation
            .payload
            .validate_for_chain(self.inner.config.chain_id)?;
        Ok(observation)
    }

    /// Fetch a validated page from the public finalized trade feed.
    pub async fn indexer_trades(
        &self,
        query: &ArcusSpotTradeQuery,
    ) -> Result<ArcusSpotObservation<ArcusSpotTradePage>, ArcusSpotError> {
        let query_fields = query.to_query(self.inner.config.chain_id)?;
        let observation: ArcusSpotObservation<ArcusSpotTradePage> = self
            .get_json(&self.inner.indexer_base_url, "trades", &query_fields)
            .await?;
        observation.payload.validate(
            self.inner.config.chain_id,
            &TradeExpectations {
                token_in: query.token_in.as_deref(),
                token_out: query.token_out.as_deref(),
                taker: query.taker.as_deref(),
                router: query.router.as_deref(),
                route_tag: query.route_tag.as_deref(),
                success: query.success,
                priced: query.priced,
                from_block: query.from_block,
                to_block: query.to_block,
                ..TradeExpectations::default()
            },
        )?;
        Ok(observation)
    }

    /// Resolve two verified router tokens and reconcile indexer metadata.
    pub async fn indexer_trades_by_symbols(
        &self,
        token_in_symbol: &str,
        token_out_symbol: &str,
        limit: u16,
        cursor: Option<String>,
    ) -> Result<ArcusSpotObservation<ArcusSpotTradePage>, ArcusSpotError> {
        let token_in = self.verified_token(token_in_symbol).await?;
        let token_out = self.verified_token(token_out_symbol).await?;
        if token_in.address.eq_ignore_ascii_case(&token_out.address) {
            return Err(ArcusSpotError::InvalidResponse(
                "indexer trade tokens must be distinct".to_string(),
            ));
        }
        let query = ArcusSpotTradeQuery {
            token_in: Some(token_in.address.clone()),
            token_out: Some(token_out.address.clone()),
            limit,
            cursor,
            ..ArcusSpotTradeQuery::default()
        };
        let observation = self.indexer_trades(&query).await?;
        observation
            .payload
            .reconcile_router_tokens(&[&token_in, &token_out])?;
        Ok(observation)
    }

    /// Fetch finalized trades for one public taker address.
    pub async fn indexer_taker_trades(
        &self,
        taker: &str,
        limit: u16,
        cursor: Option<String>,
    ) -> Result<ArcusSpotObservation<ArcusSpotTradePage>, ArcusSpotError> {
        validate_address("taker", taker)?;
        validate_limit(limit)?;
        validate_cursor(cursor.as_deref())?;
        let mut query = vec![("limit", limit.to_string())];
        if let Some(cursor) = cursor {
            query.push(("cursor", cursor));
        }
        let path = format!("takers/{}/{}/trades", self.inner.config.chain_id, taker);
        let observation: ArcusSpotObservation<ArcusSpotTradePage> = self
            .get_json(&self.inner.indexer_base_url, &path, &query)
            .await?;
        observation.payload.validate(
            self.inner.config.chain_id,
            &TradeExpectations {
                taker: Some(taker),
                ..TradeExpectations::default()
            },
        )?;
        Ok(observation)
    }

    /// Fetch every finalized swap event emitted in one transaction.
    pub async fn indexer_transaction_trades(
        &self,
        tx_hash: &str,
    ) -> Result<ArcusSpotObservation<ArcusSpotTradePage>, ArcusSpotError> {
        validate_hash("txHash", tx_hash)?;
        let path = format!("trades/{}/{}", self.inner.config.chain_id, tx_hash);
        let observation: ArcusSpotObservation<ArcusSpotTradePage> = self
            .get_json(&self.inner.indexer_base_url, &path, &[])
            .await?;
        observation.payload.validate(
            self.inner.config.chain_id,
            &TradeExpectations {
                tx_hash: Some(tx_hash),
                ..TradeExpectations::default()
            },
        )?;
        if observation.payload.next_cursor.is_some() {
            return Err(ArcusSpotError::InvalidResponse(
                "transaction trade response unexpectedly included nextCursor".to_string(),
            ));
        }
        Ok(observation)
    }
}

impl ArcusSpotTradeQuery {
    fn to_query(&self, chain_id: u64) -> Result<Vec<(&'static str, String)>, ArcusSpotError> {
        validate_limit(self.limit)?;
        validate_cursor(self.cursor.as_deref())?;
        for (field, address) in [
            ("taker", self.taker.as_deref()),
            ("tokenIn", self.token_in.as_deref()),
            ("tokenOut", self.token_out.as_deref()),
            ("router", self.router.as_deref()),
        ] {
            if let Some(address) = address {
                validate_address(field, address)?;
            }
        }
        if self
            .route_tag
            .as_deref()
            .is_some_and(|route_tag| route_tag.trim().is_empty())
        {
            return Err(ArcusSpotError::InvalidResponse(
                "routeTag must not be empty".to_string(),
            ));
        }
        if matches!((self.from_block, self.to_block), (Some(from), Some(to)) if from > to) {
            return Err(ArcusSpotError::InvalidResponse(
                "fromBlock must be less than or equal to toBlock".to_string(),
            ));
        }

        let mut query = vec![
            ("chainId", chain_id.to_string()),
            ("limit", self.limit.to_string()),
        ];
        push_optional(&mut query, "taker", self.taker.as_ref());
        push_optional(&mut query, "tokenIn", self.token_in.as_ref());
        push_optional(&mut query, "tokenOut", self.token_out.as_ref());
        push_optional(&mut query, "router", self.router.as_ref());
        push_optional(&mut query, "routeTag", self.route_tag.as_ref());
        if let Some(value) = self.success {
            query.push(("success", value.to_string()));
        }
        if let Some(value) = self.priced {
            query.push(("priced", value.to_string()));
        }
        if let Some(value) = self.from_block {
            query.push(("fromBlock", value.to_string()));
        }
        if let Some(value) = self.to_block {
            query.push(("toBlock", value.to_string()));
        }
        push_optional(&mut query, "cursor", self.cursor.as_ref());
        Ok(query)
    }
}

impl ArcusSpotIndexerStats {
    fn validate_for_chain(&self, expected_chain_id: u64) -> Result<(), ArcusSpotError> {
        if self.chains.is_empty() {
            return Err(ArcusSpotError::InvalidResponse(
                "indexer stats contains no chains".to_string(),
            ));
        }
        let mut seen = HashSet::new();
        let mut found_expected = false;
        for chain in &self.chains {
            if !seen.insert(chain.chain_id) {
                return Err(ArcusSpotError::InvalidResponse(format!(
                    "indexer stats contains duplicate chain {}",
                    chain.chain_id
                )));
            }
            if chain.name.trim().is_empty() {
                return Err(ArcusSpotError::InvalidResponse(format!(
                    "indexer chain {} has an empty name",
                    chain.chain_id
                )));
            }
            let last = parse_raw_amount("lastIndexedBlock", &chain.last_indexed_block)?;
            let head = parse_raw_amount("finalizedHead", &chain.finalized_head)?;
            let lag = parse_raw_amount("lag", &chain.lag)?;
            if head < last || head - last != lag {
                return Err(ArcusSpotError::InvalidResponse(format!(
                    "indexer chain {} has inconsistent last/head/lag: {}/{}/{}",
                    chain.chain_id, chain.last_indexed_block, chain.finalized_head, chain.lag
                )));
            }
            found_expected |= chain.chain_id == expected_chain_id;
        }
        if !found_expected {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "indexer stats does not contain configured chain {expected_chain_id}"
            )));
        }
        Ok(())
    }
}

impl ArcusSpotTradePage {
    fn validate(
        &self,
        expected_chain_id: u64,
        expected: &TradeExpectations<'_>,
    ) -> Result<(), ArcusSpotError> {
        validate_cursor(self.next_cursor.as_deref())?;

        for (key, token) in &self.tokens {
            token.validate(expected_chain_id)?;
            let expected_key = format!("{}:{}", token.chain_id, token.address.to_ascii_lowercase());
            if !key.eq_ignore_ascii_case(&expected_key) {
                return Err(ArcusSpotError::InvalidResponse(format!(
                    "indexer token key mismatch: key={key} metadata={expected_key}"
                )));
            }
        }

        for trade in &self.trades {
            trade.validate(expected_chain_id)?;
            validate_filter("tokenIn", &trade.token_in, expected.token_in)?;
            validate_filter("tokenOut", &trade.token_out, expected.token_out)?;
            validate_filter("taker", &trade.taker, expected.taker)?;
            validate_filter("txHash", &trade.tx_hash, expected.tx_hash)?;
            validate_filter("router", &trade.router, expected.router)?;
            validate_filter(
                "routeTag",
                trade.route_tag.as_deref().unwrap_or_default(),
                expected.route_tag,
            )?;
            if expected
                .success
                .is_some_and(|success| trade.success != success)
            {
                return Err(ArcusSpotError::InvalidResponse(format!(
                    "indexer trade {} has success={} for filter success={}",
                    trade.id,
                    trade.success,
                    expected.success.unwrap()
                )));
            }
            if expected
                .priced
                .is_some_and(|priced| trade.pnl.priced != priced)
            {
                return Err(ArcusSpotError::InvalidResponse(format!(
                    "indexer trade {} has priced={} for filter priced={}",
                    trade.id,
                    trade.pnl.priced,
                    expected.priced.unwrap()
                )));
            }
            if expected.from_block.is_some() || expected.to_block.is_some() {
                let block_number = parse_raw_amount("blockNumber", &trade.block_number)?;
                if expected
                    .from_block
                    .is_some_and(|from_block| block_number < U256::from(from_block))
                {
                    return Err(ArcusSpotError::InvalidResponse(format!(
                        "indexer trade {} has blockNumber {} below fromBlock {}",
                        trade.id,
                        trade.block_number,
                        expected.from_block.unwrap()
                    )));
                }
                if expected
                    .to_block
                    .is_some_and(|to_block| block_number > U256::from(to_block))
                {
                    return Err(ArcusSpotError::InvalidResponse(format!(
                        "indexer trade {} has blockNumber {} above toBlock {}",
                        trade.id,
                        trade.block_number,
                        expected.to_block.unwrap()
                    )));
                }
            }
            self.require_trade_token(&trade.token_in)?;
            self.require_trade_token(&trade.token_out)?;
        }
        Ok(())
    }

    fn require_trade_token(&self, address: &str) -> Result<(), ArcusSpotError> {
        if self
            .tokens
            .values()
            .any(|token| token.address.eq_ignore_ascii_case(address))
        {
            return Ok(());
        }
        Err(ArcusSpotError::InvalidResponse(format!(
            "indexer token map does not resolve trade token {address}"
        )))
    }

    fn reconcile_router_tokens(
        &self,
        router_tokens: &[&ArcusSpotToken],
    ) -> Result<(), ArcusSpotError> {
        if self.trades.is_empty() {
            return Ok(());
        }
        for router_token in router_tokens {
            validate_token(router_token)?;
            let indexer_token = self
                .tokens
                .values()
                .find(|token| token.address.eq_ignore_ascii_case(&router_token.address))
                .ok_or_else(|| {
                    ArcusSpotError::InvalidResponse(format!(
                        "indexer token map does not contain router token {} ({})",
                        router_token.symbol, router_token.address
                    ))
                })?;
            if indexer_token.chain_id != router_token.chain_id
                || !indexer_token
                    .symbol
                    .eq_ignore_ascii_case(&router_token.symbol)
                || indexer_token.decimals != router_token.decimals
            {
                return Err(ArcusSpotError::InvalidResponse(format!(
                    "router/indexer metadata mismatch for {}: router chain/address/decimals={}/{}/{} indexer={}/{}/{}",
                    router_token.symbol,
                    router_token.chain_id,
                    router_token.address,
                    router_token.decimals,
                    indexer_token.chain_id,
                    indexer_token.address,
                    indexer_token.decimals
                )));
            }
        }
        Ok(())
    }
}

impl ArcusSpotIndexerToken {
    fn validate(&self, expected_chain_id: u64) -> Result<(), ArcusSpotError> {
        if self.chain_id != expected_chain_id {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "indexer token {} has chain {}, expected {}",
                self.symbol, self.chain_id, expected_chain_id
            )));
        }
        validate_address("indexer token address", &self.address)?;
        if self.symbol.trim().is_empty() {
            return Err(ArcusSpotError::InvalidResponse(
                "indexer token has an empty symbol".to_string(),
            ));
        }
        if self.decimals > u8::MAX as u32 {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "indexer token {} has invalid decimals {}",
                self.symbol, self.decimals
            )));
        }
        if let Some(underlying) = &self.underlying {
            validate_address("indexer token underlying", underlying)?;
        }
        parse_raw_amount("firstSeenBlock", &self.first_seen_block)?;
        Ok(())
    }
}

impl ArcusSpotTrade {
    fn validate(&self, expected_chain_id: u64) -> Result<(), ArcusSpotError> {
        if self.chain_id != expected_chain_id {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "indexer trade {} has chain {}, expected {}",
                self.id, self.chain_id, expected_chain_id
            )));
        }
        parse_raw_amount("trade id", &self.id)?;
        parse_raw_amount("blockNumber", &self.block_number)?;
        validate_hash("txHash", &self.tx_hash)?;
        validate_hash("routeTagRaw", &self.route_tag_raw)?;
        for (field, address) in [
            ("swapShell", self.swap_shell.as_str()),
            ("taker", self.taker.as_str()),
            ("tokenIn", self.token_in.as_str()),
            ("tokenOut", self.token_out.as_str()),
            ("router", self.router.as_str()),
            ("rawTokenIn", self.raw_token_in.as_str()),
            ("rawTokenOut", self.raw_token_out.as_str()),
        ] {
            validate_address(field, address)?;
        }
        for (field, amount) in [
            ("minAmountOut", self.min_amount_out.as_str()),
            ("amountIn", self.amount_in.as_str()),
            ("quotedAmountIn", self.quoted_amount_in.as_str()),
            ("quotedAmountOut", self.quoted_amount_out.as_str()),
            ("amountOut", self.amount_out.as_str()),
            (
                "tokenInBenchmarkPrice",
                self.token_in_benchmark_price.as_str(),
            ),
            (
                "tokenOutBenchmarkPrice",
                self.token_out_benchmark_price.as_str(),
            ),
        ] {
            parse_raw_amount(field, amount)?;
        }
        if self
            .route_tag
            .as_deref()
            .is_some_and(|tag| tag.trim().is_empty())
        {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "indexer trade {} has an empty routeTag",
                self.id
            )));
        }
        if !self.token_in_wrapped && !self.raw_token_in.eq_ignore_ascii_case(&self.token_in) {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "trade {} marks tokenIn unwrapped but rawTokenIn differs",
                self.id
            )));
        }
        if !self.token_out_wrapped && !self.raw_token_out.eq_ignore_ascii_case(&self.token_out) {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "trade {} marks tokenOut unwrapped but rawTokenOut differs",
                self.id
            )));
        }
        self.pnl.validate(&self.id)?;
        Ok(())
    }
}

impl ArcusSpotTradePnl {
    fn validate(&self, trade_id: &str) -> Result<(), ArcusSpotError> {
        validate_decimal("feeUsd", &self.fee_usd)?;
        for (field, value) in [
            ("usdNotional", self.usd_notional.as_deref()),
            ("tokenExecPriceUsd", self.token_exec_price_usd.as_deref()),
            ("priceImprovementUsd", self.price_improvement_usd.as_deref()),
            ("slippageHeadroomUsd", self.slippage_headroom_usd.as_deref()),
            ("baseQty", self.base_qty.as_deref()),
            ("realizedPnlUsd", self.realized_pnl_usd.as_deref()),
            ("avgCostUsd", self.avg_cost_usd.as_deref()),
            ("matchedQty", self.matched_qty.as_deref()),
            ("positionQtyAfter", self.position_qty_after.as_deref()),
        ] {
            if let Some(value) = value {
                validate_decimal(field, value)?;
            }
        }
        if self.priced && self.usd_notional.is_none() {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "priced indexer trade {trade_id} has no usdNotional"
            )));
        }
        if let Some(side) = &self.side {
            if !matches!(side.as_str(), "buy" | "sell") {
                return Err(ArcusSpotError::InvalidResponse(format!(
                    "indexer trade {trade_id} has invalid PnL side {side:?}"
                )));
            }
            if self.base_token.is_none() || self.base_qty.is_none() {
                return Err(ArcusSpotError::InvalidResponse(format!(
                    "indexer trade {trade_id} has a PnL side without base token/quantity"
                )));
            }
        }
        if let Some(base_token) = &self.base_token {
            validate_address("pnl.baseToken", base_token)?;
        }
        Ok(())
    }
}

fn push_optional(
    query: &mut Vec<(&'static str, String)>,
    key: &'static str,
    value: Option<&String>,
) {
    if let Some(value) = value {
        query.push((key, value.clone()));
    }
}

fn validate_filter(
    field: &str,
    actual: &str,
    expected: Option<&str>,
) -> Result<(), ArcusSpotError> {
    if expected.is_some_and(|expected| !actual.eq_ignore_ascii_case(expected)) {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "indexer returned unexpected {field} {actual} for filter {}",
            expected.unwrap_or_default()
        )));
    }
    Ok(())
}

fn validate_limit(limit: u16) -> Result<(), ArcusSpotError> {
    if !(1..=MAX_TRADE_PAGE_LIMIT).contains(&limit) {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "indexer trade limit must be between 1 and {MAX_TRADE_PAGE_LIMIT}"
        )));
    }
    Ok(())
}

fn validate_cursor(cursor: Option<&str>) -> Result<(), ArcusSpotError> {
    if cursor.is_some_and(|cursor| cursor.trim().is_empty()) {
        return Err(ArcusSpotError::InvalidResponse(
            "indexer cursor must not be empty".to_string(),
        ));
    }
    Ok(())
}

fn validate_address(field: &str, raw: &str) -> Result<(), ArcusSpotError> {
    Address::from_str(raw).map_err(|error| {
        ArcusSpotError::InvalidResponse(format!(
            "{field} is not a valid EVM address: {raw}: {error}"
        ))
    })?;
    Ok(())
}

fn validate_hash(field: &str, raw: &str) -> Result<(), ArcusSpotError> {
    H256::from_str(raw).map_err(|error| {
        ArcusSpotError::InvalidResponse(format!(
            "{field} is not a 32-byte hex value: {raw}: {error}"
        ))
    })?;
    Ok(())
}

fn validate_decimal(field: &str, raw: &str) -> Result<(), ArcusSpotError> {
    if raw.is_empty() || raw.trim() != raw {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "{field} is not a canonical decimal string: {raw:?}"
        )));
    }
    let unsigned = raw.strip_prefix('-').unwrap_or(raw);
    if unsigned.is_empty() {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "{field} is not a decimal string: {raw:?}"
        )));
    }
    let mut parts = unsigned.split('.');
    let integer = parts.next().unwrap_or_default();
    let fraction = parts.next();
    if parts.next().is_some()
        || integer.is_empty()
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.is_some_and(|digits| {
            digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit())
        })
    {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "{field} is not a decimal string: {raw:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    const TOKEN_FIXTURE: &str = include_str!("fixtures/tokens_nvda_amd.json");
    const STATS_FIXTURE: &str = include_str!("fixtures/indexer_stats.json");
    const TRADES_FIXTURE: &str = include_str!("fixtures/indexer_trades_nvda_amd.json");

    async fn spawn_http_sequence(
        bodies: Vec<&'static str>,
    ) -> (String, Arc<Mutex<Vec<String>>>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server_requests = requests.clone();
        let server = tokio::spawn(async move {
            for body in bodies {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = vec![0_u8; 8192];
                let size = socket.read(&mut request).await.unwrap();
                server_requests
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&request[..size]).into_owned());
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (format!("http://{address}"), requests, server)
    }

    #[test]
    fn live_fixtures_preserve_large_amounts_and_nullable_pnl() {
        let stats: ArcusSpotIndexerStats = serde_json::from_str(STATS_FIXTURE).unwrap();
        stats.validate_for_chain(4663).unwrap();

        let page: ArcusSpotTradePage = serde_json::from_str(TRADES_FIXTURE).unwrap();
        page.validate(4663, &TradeExpectations::default()).unwrap();
        assert_eq!(page.trades[0].route_tag.as_deref(), Some("RIALTO"));
        assert!(!page.trades[0].pnl.priced);
        assert_eq!(page.trades[0].amount_in, "62757776408482226");
    }

    #[test]
    fn rejects_trade_page_that_ignores_router_filter() {
        let page: ArcusSpotTradePage = serde_json::from_str(TRADES_FIXTURE).unwrap();
        let error = page
            .validate(
                4663,
                &TradeExpectations {
                    router: Some("0x0000000000000000000000000000000000000001"),
                    ..TradeExpectations::default()
                },
            )
            .unwrap_err();
        assert!(matches!(error, ArcusSpotError::InvalidResponse(_)));
    }

    #[test]
    fn rejects_trade_page_that_ignores_block_range_filter() {
        let page: ArcusSpotTradePage = serde_json::from_str(TRADES_FIXTURE).unwrap();
        let trade_block: u64 = page.trades[0].block_number.parse().unwrap();
        let error = page
            .validate(
                4663,
                &TradeExpectations {
                    to_block: Some(trade_block - 1),
                    ..TradeExpectations::default()
                },
            )
            .unwrap_err();
        assert!(matches!(error, ArcusSpotError::InvalidResponse(_)));
    }

    #[test]
    fn accepts_lossless_high_precision_decimal_strings() {
        let value = "-0.0025264959399778331649673525372643037600";
        validate_decimal("priceImprovementUsd", value).unwrap();
    }

    #[test]
    fn rejects_inconsistent_stats_lag() {
        let mut stats: ArcusSpotIndexerStats = serde_json::from_str(STATS_FIXTURE).unwrap();
        stats.chains[0].lag = "1".to_string();
        let error = stats.validate_for_chain(4663).unwrap_err();
        assert!(error.to_string().contains("inconsistent last/head/lag"));
    }

    #[test]
    fn rejects_router_indexer_decimal_mismatch() {
        let mut page: ArcusSpotTradePage = serde_json::from_str(TRADES_FIXTURE).unwrap();
        page.tokens
            .values_mut()
            .find(|token| token.symbol == "NVDA")
            .unwrap()
            .decimals = 6;
        let router_tokens: Vec<ArcusSpotToken> = serde_json::from_str(TOKEN_FIXTURE).unwrap();
        let error = page
            .reconcile_router_tokens(&[&router_tokens[0], &router_tokens[1]])
            .unwrap_err();
        assert!(error.to_string().contains("metadata mismatch"));
    }

    #[test]
    fn validates_query_bounds_before_network() {
        let query = ArcusSpotTradeQuery {
            from_block: Some(20),
            to_block: Some(10),
            ..ArcusSpotTradeQuery::default()
        };
        assert!(query.to_query(4663).is_err());

        let query = ArcusSpotTradeQuery {
            limit: 501,
            ..ArcusSpotTradeQuery::default()
        };
        assert!(query.to_query(4663).is_err());
    }

    #[tokio::test]
    async fn pair_feed_resolves_router_tokens_and_reconciles_indexer_metadata() {
        let (base_url, requests, server) =
            spawn_http_sequence(vec![TOKEN_FIXTURE, TRADES_FIXTURE]).await;
        let client = ArcusSpotClient::new(super::super::ArcusSpotConfig {
            router_base_url: base_url.clone(),
            meta_base_url: base_url.clone(),
            indexer_base_url: base_url,
            min_request_interval_ms: 0,
            max_attempts: 1,
            ..super::super::ArcusSpotConfig::default()
        })
        .unwrap();

        let observation = client
            .indexer_trades_by_symbols("NVDA", "AMD", 2, None)
            .await
            .unwrap();
        assert_eq!(observation.payload.trades.len(), 1);
        server.await.unwrap();

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].starts_with("GET /v1/tokens HTTP/1.1"));
        assert!(requests[1].starts_with("GET /trades?"));
        assert!(requests[1].contains("chainId=4663"));
        assert!(requests[1].contains("tokenIn=0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC"));
        assert!(requests[1].contains("tokenOut=0x86923f96303D656E4aa86D9d42D1e57ad2023fdC"));
        assert!(requests[1].contains("limit=2"));
    }

    #[tokio::test]
    async fn transaction_lookup_rejects_rows_from_another_transaction() {
        let (base_url, _, server) = spawn_http_sequence(vec![TRADES_FIXTURE]).await;
        let client = ArcusSpotClient::new(super::super::ArcusSpotConfig {
            router_base_url: base_url.clone(),
            meta_base_url: base_url.clone(),
            indexer_base_url: base_url,
            min_request_interval_ms: 0,
            max_attempts: 1,
            ..super::super::ArcusSpotConfig::default()
        })
        .unwrap();

        let error = client
            .indexer_transaction_trades(
                "0x7d4c199d86f219cdcb33bbd79cd004e442805b4d41b1515f7003557a1cc98e1e",
            )
            .await
            .unwrap_err();
        server.await.unwrap();
        assert!(error.to_string().contains("unexpected txHash"));
    }

    #[tokio::test]
    #[ignore = "read-only public Arcus indexer smoke; requires network"]
    async fn public_arcus_spot_indexer_smoke() {
        let client = ArcusSpotClient::new(super::super::ArcusSpotConfig::default()).unwrap();
        let stats = client.indexer_stats().await.unwrap();
        assert!(stats
            .payload
            .chains
            .iter()
            .any(|chain| chain.chain_id == 4663));

        let page = client
            .indexer_trades_by_symbols("NVDA", "AMD", 2, None)
            .await
            .unwrap();
        assert!(page.payload.trades.len() <= 2);
    }
}

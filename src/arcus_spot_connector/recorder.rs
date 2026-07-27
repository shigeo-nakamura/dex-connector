use super::{
    ArcusSpotClient, ArcusSpotError, ArcusSpotFailureClass, ArcusSpotIndexerStats,
    ArcusSpotObservation, ArcusSpotOverviewEntry, ArcusSpotRouteObservation, ArcusSpotToken,
};
use chrono::{DateTime, Utc};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

const RECORDER_SCHEMA_VERSION: u32 = 3;

/// A directed Arcus Spot route recorded in both directions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ArcusSpotPair {
    pub sell_symbol: String,
    pub buy_symbol: String,
}

impl FromStr for ArcusSpotPair {
    type Err = ArcusSpotError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut parts = value.split('/');
        let sell_symbol = parts.next().unwrap_or_default().trim().to_ascii_uppercase();
        let buy_symbol = parts.next().unwrap_or_default().trim().to_ascii_uppercase();
        if sell_symbol.is_empty() || buy_symbol.is_empty() || parts.next().is_some() {
            return Err(ArcusSpotError::InvalidConfig(format!(
                "invalid recorder pair {value:?}; expected SELL/BUY"
            )));
        }
        if sell_symbol == buy_symbol {
            return Err(ArcusSpotError::InvalidConfig(format!(
                "recorder pair must contain distinct symbols: {value:?}"
            )));
        }
        Ok(Self {
            sell_symbol,
            buy_symbol,
        })
    }
}

/// Routes and USD notionals collected by one recorder invocation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArcusSpotRecorderConfig {
    pub pairs: Vec<ArcusSpotPair>,
    pub notionals_usd: Vec<Decimal>,
}

impl Default for ArcusSpotRecorderConfig {
    fn default() -> Self {
        Self {
            pairs: ["NVDA/AMD", "SPY/QQQ", "META/GOOGL"]
                .into_iter()
                .map(|pair| pair.parse().expect("built-in recorder pair is valid"))
                .collect(),
            notionals_usd: [5_u64, 10, 25, 50].into_iter().map(Decimal::from).collect(),
        }
    }
}

impl ArcusSpotRecorderConfig {
    pub fn from_csv(pairs: &str, notionals_usd: &str) -> Result<Self, ArcusSpotError> {
        let pairs = pairs
            .split(',')
            .map(str::parse)
            .collect::<Result<Vec<_>, _>>()?;
        let notionals_usd = notionals_usd
            .split(',')
            .map(|value| {
                Decimal::from_str(value.trim()).map_err(|error| {
                    ArcusSpotError::InvalidConfig(format!(
                        "invalid recorder USD notional {value:?}: {error}"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let config = Self {
            pairs,
            notionals_usd,
        };
        config.validate()?;
        Ok(config)
    }

    fn normalize(&mut self) {
        for pair in &mut self.pairs {
            pair.sell_symbol = pair.sell_symbol.trim().to_ascii_uppercase();
            pair.buy_symbol = pair.buy_symbol.trim().to_ascii_uppercase();
        }
    }

    fn validate(&self) -> Result<(), ArcusSpotError> {
        if self.pairs.is_empty() {
            return Err(ArcusSpotError::InvalidConfig(
                "recorder requires at least one pair".to_string(),
            ));
        }
        if self.notionals_usd.is_empty() {
            return Err(ArcusSpotError::InvalidConfig(
                "recorder requires at least one USD notional".to_string(),
            ));
        }
        if let Some(notional) = self
            .notionals_usd
            .iter()
            .find(|notional| **notional <= Decimal::ZERO)
        {
            return Err(ArcusSpotError::InvalidConfig(format!(
                "recorder USD notionals must be positive: {notional}"
            )));
        }
        let mut pairs = HashSet::new();
        for pair in &self.pairs {
            if pair.sell_symbol.is_empty()
                || pair.buy_symbol.is_empty()
                || pair.sell_symbol == pair.buy_symbol
            {
                return Err(ArcusSpotError::InvalidConfig(format!(
                    "invalid recorder pair: {}/{}",
                    pair.sell_symbol, pair.buy_symbol
                )));
            }
            if !pairs.insert(pair) {
                return Err(ArcusSpotError::InvalidConfig(format!(
                    "duplicate recorder pair: {}/{}",
                    pair.sell_symbol, pair.buy_symbol
                )));
            }
        }
        let mut notionals = HashSet::new();
        for notional in &self.notionals_usd {
            if !notionals.insert(notional.normalize().to_string()) {
                return Err(ArcusSpotError::InvalidConfig(format!(
                    "duplicate recorder USD notional: {notional}"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArcusSpotRecorderStage {
    IndexerStats,
    TokenMetadata,
    ReferenceOverview,
    ReferenceValidation,
    AmountConversion,
    ForwardPrice,
    ReversePrice,
    RoundTripCalculation,
}

/// A structured recorder failure that remains useful after JSON archival.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArcusSpotRecordedError {
    pub stage: ArcusSpotRecorderStage,
    pub classification: ArcusSpotFailureClass,
    pub retryable: bool,
    pub message: String,
}

impl ArcusSpotRecordedError {
    fn from_client(stage: ArcusSpotRecorderStage, error: &ArcusSpotError) -> Self {
        Self {
            stage,
            classification: error.classification(),
            retryable: error.retryable(),
            message: error.to_string(),
        }
    }

    fn validation(stage: ArcusSpotRecorderStage, message: impl Into<String>) -> Self {
        Self {
            stage,
            classification: ArcusSpotFailureClass::ResponseValidation,
            retryable: false,
            message: message.into(),
        }
    }
}

/// A captured endpoint result. Errors are serialized rather than discarded.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ArcusSpotCapture<T> {
    Success { observation: T },
    Error { error: ArcusSpotRecordedError },
}

impl<T> ArcusSpotCapture<T> {
    fn is_success(&self) -> bool {
        matches!(self, Self::Success { .. })
    }
}

/// One optimistic indicative round trip. No wallet or state mutation occurs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArcusSpotRoundTripRecord {
    pub pair: ArcusSpotPair,
    pub notional_usd: String,
    pub sell_reference_price_usd: Option<String>,
    pub buy_reference_price_usd: Option<String>,
    pub requested_sell_amount: Option<String>,
    pub forward: Option<ArcusSpotRouteObservation>,
    pub reverse: Option<ArcusSpotRouteObservation>,
    pub optimistic_return_amount: Option<String>,
    pub optimistic_round_trip_loss_bps: Option<String>,
    pub errors: Vec<ArcusSpotRecordedError>,
}

/// Self-contained evidence envelope emitted by one recorder invocation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArcusSpotRecorderSnapshot {
    pub schema_version: u32,
    pub mode: String,
    pub chain_id: u64,
    pub collection_started_at: DateTime<Utc>,
    pub collection_finished_at: DateTime<Utc>,
    pub indexer_stats: ArcusSpotCapture<ArcusSpotObservation<ArcusSpotIndexerStats>>,
    pub token_metadata: ArcusSpotCapture<ArcusSpotObservation<Vec<ArcusSpotToken>>>,
    pub reference_overview: ArcusSpotCapture<ArcusSpotObservation<Vec<ArcusSpotOverviewEntry>>>,
    pub round_trips: Vec<ArcusSpotRoundTripRecord>,
}

impl ArcusSpotRecorderSnapshot {
    pub fn is_complete(&self) -> bool {
        self.indexer_stats.is_success()
            && self.token_metadata.is_success()
            && self.reference_overview.is_success()
            && self.round_trips.iter().all(|row| row.errors.is_empty())
    }
}

/// Read-only Arcus Spot recorder using only the public client GET surface.
pub struct ArcusSpotRecorder {
    client: ArcusSpotClient,
    config: ArcusSpotRecorderConfig,
}

impl ArcusSpotRecorder {
    pub fn new(
        client: ArcusSpotClient,
        mut config: ArcusSpotRecorderConfig,
    ) -> Result<Self, ArcusSpotError> {
        config.normalize();
        config.validate()?;
        Ok(Self { client, config })
    }

    pub async fn collect_once(&self) -> ArcusSpotRecorderSnapshot {
        let collection_started_at = Utc::now();

        let indexer_result = self.client.indexer_stats().await;
        let token_result = self.client.refresh_tokens().await;
        let overview_result = self.client.reference_overview().await;

        let token_map = token_result
            .as_ref()
            .ok()
            .map(|observation| {
                observation
                    .payload
                    .iter()
                    .map(|token| (token.symbol.to_ascii_uppercase(), token))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();
        let overview_map = overview_result
            .as_ref()
            .ok()
            .map(|observation| {
                observation
                    .payload
                    .iter()
                    .map(|entry| (entry.ticker.to_ascii_uppercase(), entry))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();

        let mut round_trips = Vec::with_capacity(
            self.config
                .pairs
                .len()
                .saturating_mul(self.config.notionals_usd.len()),
        );
        for pair in &self.config.pairs {
            for notional in &self.config.notionals_usd {
                round_trips.push(
                    self.collect_round_trip(pair.clone(), *notional, &token_map, &overview_map)
                        .await,
                );
            }
        }
        let selected_symbols = self
            .config
            .pairs
            .iter()
            .flat_map(|pair| [&pair.sell_symbol, &pair.buy_symbol])
            .cloned()
            .collect::<HashSet<_>>();
        let token_result = token_result.map(|observation| {
            observation.map(|tokens| {
                tokens
                    .into_iter()
                    .filter(|token| selected_symbols.contains(&token.symbol.to_ascii_uppercase()))
                    .collect()
            })
        });
        let overview_result = overview_result.map(|observation| {
            observation.map(|entries| {
                entries
                    .into_iter()
                    .filter(|entry| selected_symbols.contains(&entry.ticker.to_ascii_uppercase()))
                    .collect()
            })
        });

        ArcusSpotRecorderSnapshot {
            schema_version: RECORDER_SCHEMA_VERSION,
            mode: "public_indicative_read_only".to_string(),
            chain_id: self.client.config().chain_id,
            collection_started_at,
            collection_finished_at: Utc::now(),
            indexer_stats: capture(ArcusSpotRecorderStage::IndexerStats, indexer_result),
            token_metadata: capture(ArcusSpotRecorderStage::TokenMetadata, token_result),
            reference_overview: capture(ArcusSpotRecorderStage::ReferenceOverview, overview_result),
            round_trips,
        }
    }

    async fn collect_round_trip(
        &self,
        pair: ArcusSpotPair,
        notional_usd: Decimal,
        tokens: &HashMap<String, &ArcusSpotToken>,
        overview: &HashMap<String, &ArcusSpotOverviewEntry>,
    ) -> ArcusSpotRoundTripRecord {
        let mut row = ArcusSpotRoundTripRecord {
            pair,
            notional_usd: notional_usd.normalize().to_string(),
            sell_reference_price_usd: None,
            buy_reference_price_usd: None,
            requested_sell_amount: None,
            forward: None,
            reverse: None,
            optimistic_return_amount: None,
            optimistic_round_trip_loss_bps: None,
            errors: Vec::new(),
        };

        let Some(sell_token) = tokens.get(&row.pair.sell_symbol).copied() else {
            row.errors.push(ArcusSpotRecordedError::validation(
                ArcusSpotRecorderStage::TokenMetadata,
                format!(
                    "verified token metadata is missing for {}",
                    row.pair.sell_symbol
                ),
            ));
            return row;
        };
        let Some(buy_token) = tokens.get(&row.pair.buy_symbol).copied() else {
            row.errors.push(ArcusSpotRecordedError::validation(
                ArcusSpotRecorderStage::TokenMetadata,
                format!(
                    "verified token metadata is missing for {}",
                    row.pair.buy_symbol
                ),
            ));
            return row;
        };
        let sell_reference = match validated_reference(sell_token, overview) {
            Ok(entry) => entry,
            Err(error) => {
                row.errors.push(error);
                return row;
            }
        };
        let buy_reference = match validated_reference(buy_token, overview) {
            Ok(entry) => entry,
            Err(error) => {
                row.errors.push(error);
                return row;
            }
        };
        let sell_price = sell_reference
            .quote
            .price
            .expect("validated reference has a price");
        let buy_price = buy_reference
            .quote
            .price
            .expect("validated reference has a price");
        row.sell_reference_price_usd = Some(sell_price.normalize().to_string());
        row.buy_reference_price_usd = Some(buy_price.normalize().to_string());

        let requested_sell_amount =
            match notional_to_raw_amount(notional_usd, sell_price, sell_token.decimals) {
                Ok(amount) => amount,
                Err(message) => {
                    row.errors.push(ArcusSpotRecordedError::validation(
                        ArcusSpotRecorderStage::AmountConversion,
                        message,
                    ));
                    return row;
                }
            };
        row.requested_sell_amount = Some(requested_sell_amount.clone());

        let forward = match self
            .client
            .indicative_price_by_symbol(
                &row.pair.sell_symbol,
                &row.pair.buy_symbol,
                &requested_sell_amount,
            )
            .await
        {
            Ok(observation) => observation,
            Err(error) => {
                row.errors.push(ArcusSpotRecordedError::from_client(
                    ArcusSpotRecorderStage::ForwardPrice,
                    &error,
                ));
                return row;
            }
        };
        let reverse_sell_amount = match forward.response.payload.recommended_quote() {
            Ok(quote) => quote.buy_amount.clone(),
            Err(error) => {
                row.errors.push(ArcusSpotRecordedError::from_client(
                    ArcusSpotRecorderStage::ForwardPrice,
                    &error,
                ));
                row.forward = Some(forward);
                return row;
            }
        };
        row.forward = Some(forward);

        let reverse = match self
            .client
            .indicative_price_by_symbol(
                &row.pair.buy_symbol,
                &row.pair.sell_symbol,
                &reverse_sell_amount,
            )
            .await
        {
            Ok(observation) => observation,
            Err(error) => {
                row.errors.push(ArcusSpotRecordedError::from_client(
                    ArcusSpotRecorderStage::ReversePrice,
                    &error,
                ));
                return row;
            }
        };
        let optimistic_return_amount = match reverse.response.payload.recommended_quote() {
            Ok(quote) => quote.buy_amount.clone(),
            Err(error) => {
                row.errors.push(ArcusSpotRecordedError::from_client(
                    ArcusSpotRecorderStage::ReversePrice,
                    &error,
                ));
                row.reverse = Some(reverse);
                return row;
            }
        };
        row.optimistic_return_amount = Some(optimistic_return_amount.clone());
        match round_trip_loss_bps(&requested_sell_amount, &optimistic_return_amount) {
            Ok(loss_bps) => {
                row.optimistic_round_trip_loss_bps = Some(loss_bps.normalize().to_string())
            }
            Err(message) => row.errors.push(ArcusSpotRecordedError::validation(
                ArcusSpotRecorderStage::RoundTripCalculation,
                message,
            )),
        }
        row.reverse = Some(reverse);
        row
    }
}

fn capture<T>(
    stage: ArcusSpotRecorderStage,
    result: Result<T, ArcusSpotError>,
) -> ArcusSpotCapture<T> {
    match result {
        Ok(observation) => ArcusSpotCapture::Success { observation },
        Err(error) => ArcusSpotCapture::Error {
            error: ArcusSpotRecordedError::from_client(stage, &error),
        },
    }
}

fn validated_reference<'a>(
    token: &ArcusSpotToken,
    overview: &'a HashMap<String, &ArcusSpotOverviewEntry>,
) -> Result<&'a ArcusSpotOverviewEntry, ArcusSpotRecordedError> {
    let entry = overview
        .get(&token.symbol.to_ascii_uppercase())
        .copied()
        .ok_or_else(|| {
            ArcusSpotRecordedError::validation(
                ArcusSpotRecorderStage::ReferenceValidation,
                format!("reference overview is missing {}", token.symbol),
            )
        })?;
    if !entry.contract_address.eq_ignore_ascii_case(&token.address) {
        return Err(ArcusSpotRecordedError::validation(
            ArcusSpotRecorderStage::ReferenceValidation,
            format!(
                "reference address mismatch for {}: token-list={} overview={}",
                token.symbol, token.address, entry.contract_address
            ),
        ));
    }
    if entry.quote.price.is_none_or(|price| price <= Decimal::ZERO) {
        return Err(ArcusSpotRecordedError::validation(
            ArcusSpotRecorderStage::ReferenceValidation,
            format!(
                "reference price for {} is null or non-positive",
                token.symbol
            ),
        ));
    }
    Ok(entry)
}

fn notional_to_raw_amount(
    notional_usd: Decimal,
    reference_price_usd: Decimal,
    decimals: u32,
) -> Result<String, String> {
    if notional_usd <= Decimal::ZERO || reference_price_usd <= Decimal::ZERO {
        return Err("notional and reference price must be positive".to_string());
    }
    let raw_scale = 10_i128
        .checked_pow(decimals)
        .ok_or_else(|| format!("token decimals {decimals} exceed the recorder Decimal range"))?;
    let scale = Decimal::try_from_i128_with_scale(raw_scale, 0).map_err(|error| {
        format!("token decimals {decimals} exceed the recorder Decimal range: {error}")
    })?;
    let raw = notional_usd
        .checked_div(reference_price_usd)
        .and_then(|quantity| quantity.checked_mul(scale))
        .ok_or_else(|| {
            format!(
                "USD notional {notional_usd} at price {reference_price_usd} exceeds the recorder Decimal range"
            )
        })?
        .round_dp_with_strategy(0, RoundingStrategy::ToZero);
    if raw <= Decimal::ZERO {
        return Err(format!(
            "USD notional {notional_usd} rounds to zero raw units at price {reference_price_usd} and {decimals} decimals"
        ));
    }
    Ok(raw.normalize().to_string())
}

fn round_trip_loss_bps(start_amount: &str, return_amount: &str) -> Result<Decimal, String> {
    let start = Decimal::from_str(start_amount)
        .map_err(|error| format!("invalid starting raw amount {start_amount:?}: {error}"))?;
    let returned = Decimal::from_str(return_amount)
        .map_err(|error| format!("invalid returned raw amount {return_amount:?}: {error}"))?;
    if start <= Decimal::ZERO || returned <= Decimal::ZERO {
        return Err("round-trip raw amounts must be positive".to_string());
    }
    let ratio = returned
        .checked_div(start)
        .ok_or_else(|| "round-trip ratio exceeds the recorder Decimal range".to_string())?;
    (Decimal::ONE - ratio)
        .checked_mul(Decimal::from(10_000_u64))
        .ok_or_else(|| "round-trip bps exceeds the recorder Decimal range".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ArcusSpotConfig;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    const TOKEN_FIXTURE: &str = include_str!("fixtures/tokens_nvda_amd.json");
    const PRICE_FIXTURE: &str = include_str!("fixtures/price_nvda_amd.json");
    const OVERVIEW_FIXTURE: &str = include_str!("fixtures/overview_nvda_amd.json");
    const INDEXER_FIXTURE: &str = include_str!("fixtures/indexer_stats.json");

    #[test]
    fn parses_and_validates_csv_config() {
        let config = ArcusSpotRecorderConfig::from_csv(" nvda/amd ", "5,10.5").unwrap();
        assert_eq!(config.pairs[0].sell_symbol, "NVDA");
        assert_eq!(config.pairs[0].buy_symbol, "AMD");
        assert_eq!(
            config.notionals_usd,
            vec![Decimal::from(5), Decimal::new(105, 1)]
        );
        assert!(ArcusSpotRecorderConfig::from_csv("NVDA/NVDA", "5").is_err());
        assert!(ArcusSpotRecorderConfig::from_csv("NVDA/AMD", "0").is_err());
        assert!(ArcusSpotRecorderConfig::from_csv("NVDA/AMD,NVDA/AMD", "5").is_err());
    }

    #[test]
    fn converts_reference_notional_to_fixture_raw_amount_exactly() {
        assert_eq!(
            notional_to_raw_amount(Decimal::from(5), Decimal::new(20675, 2), 18).unwrap(),
            "24183796856106408"
        );
    }

    #[test]
    fn unsupported_token_decimals_return_errors_instead_of_panicking() {
        for decimals in [29, 38, 39, 255] {
            let error = notional_to_raw_amount(Decimal::ONE, Decimal::ONE, decimals).unwrap_err();
            assert!(
                error.contains("exceed the recorder Decimal range"),
                "unexpected error for {decimals} decimals: {error}"
            );
        }
    }

    #[test]
    fn computes_signed_optimistic_round_trip_loss() {
        let loss = round_trip_loss_bps("1000", "990").unwrap();
        assert_eq!(loss, Decimal::from(100));
        let gain = round_trip_loss_bps("1000", "1010").unwrap();
        assert_eq!(gain, Decimal::from(-100));
    }

    #[tokio::test]
    async fn snapshot_preserves_health_metadata_and_both_route_directions() {
        let reverse_fixture = serde_json::json!({
            "recommended": "arcus",
            "all": [{
                "venue": "arcus",
                "buyAmount": "23900000000000000",
                "sellAmount": "9393852601996744",
                "fees": [{"type": "protocol", "amount": "7"}],
                "routeId": "reverse-kept"
            }],
            "errors": [{"venue": "lifi", "message": "no route"}]
        })
        .to_string();
        let (base_url, server) = spawn_http_sequence(vec![
            INDEXER_FIXTURE.to_string(),
            TOKEN_FIXTURE.to_string(),
            OVERVIEW_FIXTURE.to_string(),
            PRICE_FIXTURE.to_string(),
            reverse_fixture,
        ])
        .await;
        let client = ArcusSpotClient::new(ArcusSpotConfig {
            router_base_url: base_url.clone(),
            meta_base_url: base_url.clone(),
            indexer_base_url: base_url,
            min_request_interval_ms: 0,
            max_attempts: 1,
            ..ArcusSpotConfig::default()
        })
        .unwrap();
        let recorder = ArcusSpotRecorder::new(
            client,
            ArcusSpotRecorderConfig::from_csv("NVDA/AMD", "5").unwrap(),
        )
        .unwrap();

        let snapshot = recorder.collect_once().await;
        server.await.unwrap();

        assert!(snapshot.is_complete());
        assert_eq!(snapshot.schema_version, 3);
        assert_eq!(snapshot.round_trips.len(), 1);
        let ArcusSpotCapture::Success { observation } = &snapshot.token_metadata else {
            panic!("token metadata should be captured");
        };
        assert_eq!(observation.payload.len(), 2);
        let ArcusSpotCapture::Success { observation } = &snapshot.reference_overview else {
            panic!("reference overview should be captured");
        };
        assert_eq!(observation.payload.len(), 2);
        let row = &snapshot.round_trips[0];
        assert_eq!(
            row.requested_sell_amount.as_deref(),
            Some("24183796856106408")
        );
        assert_eq!(
            row.forward.as_ref().unwrap().response.payload.quotes.len(),
            3
        );
        let reverse = row.reverse.as_ref().unwrap();
        assert_eq!(reverse.response.payload.quotes[0].fees[0]["amount"], "7");
        assert_eq!(
            reverse.response.payload.quotes[0].extra["routeId"],
            "reverse-kept"
        );
        assert_eq!(reverse.response.payload.errors[0]["venue"], "lifi");
        assert!(row.optimistic_round_trip_loss_bps.is_some());
    }

    #[tokio::test]
    async fn reverse_failure_keeps_the_successful_forward_observation() {
        let invalid_reverse = serde_json::json!({
            "recommended": "arcus",
            "all": [],
            "errors": [{"venue": "arcus", "message": "temporarily unavailable"}]
        })
        .to_string();
        let (base_url, server) = spawn_http_sequence(vec![
            INDEXER_FIXTURE.to_string(),
            TOKEN_FIXTURE.to_string(),
            OVERVIEW_FIXTURE.to_string(),
            PRICE_FIXTURE.to_string(),
            invalid_reverse,
        ])
        .await;
        let client = ArcusSpotClient::new(ArcusSpotConfig {
            router_base_url: base_url.clone(),
            meta_base_url: base_url.clone(),
            indexer_base_url: base_url,
            min_request_interval_ms: 0,
            max_attempts: 1,
            ..ArcusSpotConfig::default()
        })
        .unwrap();
        let recorder = ArcusSpotRecorder::new(
            client,
            ArcusSpotRecorderConfig {
                pairs: vec![ArcusSpotPair {
                    sell_symbol: " nvda ".to_string(),
                    buy_symbol: "amd".to_string(),
                }],
                notionals_usd: vec![Decimal::from(5)],
            },
        )
        .unwrap();

        let snapshot = recorder.collect_once().await;
        server.await.unwrap();

        assert!(!snapshot.is_complete());
        let row = &snapshot.round_trips[0];
        assert_eq!(row.pair.sell_symbol, "NVDA");
        assert_eq!(row.pair.buy_symbol, "AMD");
        assert!(row.forward.is_some());
        assert!(row.reverse.is_none());
        assert_eq!(row.errors.len(), 1);
        assert_eq!(row.errors[0].stage, ArcusSpotRecorderStage::ReversePrice);
        assert_eq!(
            row.errors[0].classification,
            ArcusSpotFailureClass::ResponseValidation
        );
    }

    async fn spawn_http_sequence(responses: Vec<String>) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            for body in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 4096];
                let _ = socket.read(&mut request).await.unwrap();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (format!("http://{address}"), task)
    }

    #[tokio::test]
    #[ignore = "read-only public Arcus API smoke; requires network"]
    async fn public_recorder_smoke() {
        let client = ArcusSpotClient::new(ArcusSpotConfig::default()).unwrap();
        let recorder = ArcusSpotRecorder::new(
            client,
            ArcusSpotRecorderConfig::from_csv("NVDA/AMD", "5").unwrap(),
        )
        .unwrap();
        let snapshot = recorder.collect_once().await;
        assert!(snapshot.is_complete(), "{snapshot:#?}");
    }
}

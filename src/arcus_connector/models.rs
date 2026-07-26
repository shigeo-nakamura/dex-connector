use crate::{DexError, OrderSide};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Debug, Deserialize)]
pub(super) struct MarketsResponse {
    pub(super) markets: Vec<MarketWire>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct MarketWire {
    pub(super) market_display_name: String,
    pub(super) market_id: u32,
    pub(super) status: String,
    pub(super) base_asset: String,
    pub(super) quote_asset: String,
    pub(super) tick_size: String,
    pub(super) step_size: String,
    #[serde(default)]
    pub(super) tick_tiers: Vec<TickTierWire>,
    #[serde(default)]
    pub(super) min_order_notional: Option<String>,
    #[serde(default)]
    pub(super) oracle_price: Option<String>,
    #[serde(default)]
    pub(super) mark_price: Option<String>,
    #[serde(default)]
    pub(super) last_trade_price: Option<String>,
    #[serde(default)]
    pub(super) funding_rate: Option<String>,
    #[serde(default)]
    pub(super) volume24h: Option<String>,
    #[serde(default)]
    pub(super) trades24h: Option<u64>,
    #[serde(default)]
    pub(super) open_interest: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TickTierWire {
    #[serde(default)]
    pub(super) up_to_price: Option<String>,
    pub(super) tick: String,
}

#[derive(Clone, Debug)]
pub(super) struct TickTier {
    pub(super) up_to_price: Option<Decimal>,
    pub(super) tick: Decimal,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(super) struct MarketInfo {
    pub(super) market: String,
    pub(super) market_id: u32,
    pub(super) status: String,
    pub(super) base_asset: String,
    pub(super) quote_asset: String,
    pub(super) tick_size: Decimal,
    pub(super) step_size: Decimal,
    pub(super) tick_tiers: Vec<TickTier>,
    pub(super) min_order_notional: Option<Decimal>,
    pub(super) oracle_price: Option<Decimal>,
    pub(super) mark_price: Option<Decimal>,
    pub(super) last_trade_price: Option<Decimal>,
    pub(super) funding_rate: Option<Decimal>,
    pub(super) volume24h: Option<Decimal>,
    pub(super) trades24h: Option<u64>,
    pub(super) open_interest: Option<Decimal>,
}

impl TryFrom<MarketWire> for MarketInfo {
    type Error = DexError;

    fn try_from(value: MarketWire) -> Result<Self, Self::Error> {
        let venue = value.market_display_name.clone();
        let tick_size = parse_positive_decimal(&value.tick_size, &venue, "tickSize")?;
        let step_size = parse_positive_decimal(&value.step_size, &venue, "stepSize")?;
        let tick_tiers = value
            .tick_tiers
            .into_iter()
            .enumerate()
            .map(|(index, tier)| {
                Ok(TickTier {
                    up_to_price: parse_optional_decimal(
                        tier.up_to_price.as_deref(),
                        &venue,
                        &format!("tickTiers[{index}].upToPrice"),
                    )?,
                    tick: parse_positive_decimal(
                        &tier.tick,
                        &venue,
                        &format!("tickTiers[{index}].tick"),
                    )?,
                })
            })
            .collect::<Result<Vec<_>, DexError>>()?;
        Ok(Self {
            market: value.market_display_name,
            market_id: value.market_id,
            status: value.status,
            base_asset: value.base_asset,
            quote_asset: value.quote_asset,
            tick_size,
            step_size,
            tick_tiers,
            min_order_notional: parse_optional_decimal(
                value.min_order_notional.as_deref(),
                &venue,
                "minOrderNotional",
            )?,
            oracle_price: parse_optional_decimal(
                value.oracle_price.as_deref(),
                &venue,
                "oraclePrice",
            )?,
            mark_price: parse_optional_decimal(value.mark_price.as_deref(), &venue, "markPrice")?,
            last_trade_price: parse_optional_decimal(
                value.last_trade_price.as_deref(),
                &venue,
                "lastTradePrice",
            )?,
            funding_rate: parse_optional_decimal(
                value.funding_rate.as_deref(),
                &venue,
                "fundingRate",
            )?,
            volume24h: parse_optional_decimal(value.volume24h.as_deref(), &venue, "volume24h")?,
            trades24h: value.trades24h,
            open_interest: parse_optional_decimal(
                value.open_interest.as_deref(),
                &venue,
                "openInterest",
            )?,
        })
    }
}

impl MarketInfo {
    pub(super) fn tick_size_for(&self, price: Decimal) -> Decimal {
        self.tick_tiers
            .iter()
            .find(|tier| tier.up_to_price.is_none_or(|upper| price <= upper))
            .map(|tier| tier.tick)
            .unwrap_or(self.tick_size)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct OrderbookSnapshotWire {
    #[serde(default)]
    pub(super) bids: Vec<[String; 2]>,
    #[serde(default)]
    pub(super) asks: Vec<[String; 2]>,
    pub(super) last_sequence_id: u64,
    #[serde(default)]
    pub(super) global_sequence_id: u64,
    #[serde(default)]
    pub(super) timestamp: u64,
}

#[derive(Debug, Deserialize)]
pub(super) struct TradesResponse {
    #[serde(default)]
    pub(super) trades: Vec<TradeWire>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TradeWire {
    pub(super) price: String,
    pub(super) size: String,
    pub(super) side: String,
    #[allow(dead_code)]
    pub(super) timestamp: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AccountWire {
    pub(super) equity: String,
    pub(super) free_collateral: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct PositionsResponseWire {
    #[serde(default)]
    pub(super) positions: HashMap<String, PositionWire>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct PositionWire {
    pub(super) market_display_name: String,
    pub(super) side: String,
    pub(super) size: String,
    pub(super) average_entry_price: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct OpenOrdersResponseWire {
    #[serde(default)]
    pub(super) orders: Vec<OpenOrderWire>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct OpenOrderWire {
    pub(super) order_id: String,
    pub(super) market_display_name: String,
    pub(super) side: String,
    pub(super) status: String,
    pub(super) price: String,
    pub(super) remaining_size: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SetLeverageResponseWire {
    pub(super) status: String,
    #[serde(default)]
    pub(super) reject_reason: Option<String>,
}

impl TradeWire {
    pub(super) fn order_side(&self) -> Option<OrderSide> {
        match self.side.as_str() {
            "BUY" => Some(OrderSide::Long),
            "SELL" => Some(OrderSide::Short),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct WsBookEnvelope {
    #[serde(rename = "type")]
    pub(super) kind: String,
    pub(super) id: String,
    #[serde(default)]
    pub(super) contents: Option<WsBookContents>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WsBookContents {
    #[serde(default)]
    pub(super) bids: Vec<[String; 2]>,
    #[serde(default)]
    pub(super) asks: Vec<[String; 2]>,
    pub(super) last_sequence_id: u64,
    #[serde(default)]
    pub(super) global_sequence_id: u64,
    #[serde(default)]
    pub(super) timestamp: Option<u64>,
}

pub(super) fn parse_decimal(raw: &str, market: &str, field: &str) -> Result<Decimal, DexError> {
    Decimal::from_str(raw).map_err(|err| {
        DexError::Transient(format!(
            "Arcus decimal parse failed market={market} field={field} value={raw}: {err}"
        ))
    })
}

fn parse_positive_decimal(raw: &str, market: &str, field: &str) -> Result<Decimal, DexError> {
    let value = parse_decimal(raw, market, field)?;
    if value <= Decimal::ZERO {
        return Err(DexError::Permanent(format!(
            "Arcus invalid non-positive market metadata market={market} field={field} value={value}"
        )));
    }
    Ok(value)
}

fn parse_optional_decimal(
    raw: Option<&str>,
    market: &str,
    field: &str,
) -> Result<Option<Decimal>, DexError> {
    raw.filter(|value| !value.is_empty())
        .map(|value| parse_decimal(value, market, field))
        .transpose()
}

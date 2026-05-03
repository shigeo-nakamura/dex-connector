//! Extended StarkNet SNIP-12 settlement path: domain-separator
//! constants, settlement / debug-amount / TPSL leg / NewOrderModel
//! request structs, and the helpers on ExtendedConnector that compute
//! the StarkNet order hash + signature for every order placement.
//!
//! Already feature-gated transitively because the parent module is
//! only compiled under `#[cfg(feature = "extended-sdk")]`.

use super::{ExtendedConnector, MarketModel};
use crate::dex_request::DexError;
use chrono::{DateTime, Duration, Utc};
use rust_crypto_lib_base::{get_order_hash, sign_message};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::Serialize;
use starknet::core::types::Felt;

pub(super) const DOMAIN_NAME: &str = "Perpetuals";
pub(super) const DOMAIN_VERSION: &str = "v0";
pub(super) const DOMAIN_REVISION: &str = "1";

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SettlementSignatureModel {
    pub(super) r: String,
    pub(super) s: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StarkSettlementModel {
    pub(super) signature: SettlementSignatureModel,
    pub(super) stark_key: String,
    pub(super) collateral_position: Decimal,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StarkDebuggingOrderAmountsModel {
    pub(super) collateral_amount: Decimal,
    pub(super) fee_amount: Decimal,
    pub(super) synthetic_amount: Decimal,
}

/// Take-profit / stop-loss leg attached to a parent order (or standalone
/// TPSL order when the parent has `type=TPSL` and `tpSlType=ORDER`).
/// See x10xchange/python_sdk → CreateOrderTpslTriggerModel.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TpslLegModel {
    pub(super) trigger_price: Decimal,
    pub(super) trigger_price_type: String, // "LAST" | "MARK" | "INDEX"
    pub(super) price: Decimal,             // execution price; for market-with-slippage this is slippage-adjusted
    pub(super) price_type: String,         // "LIMIT" | "MARKET"
    pub(super) settlement: StarkSettlementModel,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) debugging_amounts: Option<StarkDebuggingOrderAmountsModel>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct NewOrderModel {
    pub(super) id: String,
    pub(super) market: String,
    #[serde(rename = "type")]
    pub(super) order_type: String,
    pub(super) side: String,
    pub(super) qty: Decimal,
    pub(super) price: Decimal,
    pub(super) reduce_only: bool,
    pub(super) post_only: bool,
    pub(super) time_in_force: String,
    pub(super) expiry_epoch_millis: i64,
    pub(super) fee: Decimal,
    pub(super) self_trade_protection_level: String,
    pub(super) nonce: Decimal,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) cancel_id: Option<String>,
    // TPSL standalone orders omit the main settlement (the SL/TP leg carries
    // its own signature). Limit/Market orders always include it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) settlement: Option<StarkSettlementModel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) tp_sl_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) take_profit: Option<TpslLegModel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) stop_loss: Option<TpslLegModel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) debugging_amounts: Option<StarkDebuggingOrderAmountsModel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "builderFee")]
    pub(super) builder_fee: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "builderId")]
    pub(super) builder_id: Option<i64>,
}

#[allow(dead_code)]
pub(super) struct SettlementData {
    pub(super) stark_synthetic_amount: i64,
    pub(super) stark_collateral_amount: i64,
    pub(super) stark_fee_amount: u64,
    pub(super) fee_rate: Decimal,
    pub(super) order_hash: Felt,
    pub(super) settlement: StarkSettlementModel,
    pub(super) debugging_amounts: StarkDebuggingOrderAmountsModel,
}

impl ExtendedConnector {
    pub(super) fn domain_chain_id(&self) -> &'static str {
        self.env.chain_id()
    }

    pub(super) fn parse_private_key(&self) -> Result<Felt, DexError> {
        Felt::from_hex(&self.private_key)
            .map_err(|e| DexError::Other(format!("Invalid private key hex: {}", e)))
    }

    pub(super) fn parse_public_key(&self) -> Result<Felt, DexError> {
        Felt::from_hex(&self.public_key)
            .map_err(|e| DexError::Other(format!("Invalid public key hex: {}", e)))
    }

    pub(super) fn to_epoch_millis(value: DateTime<Utc>) -> i64 {
        let secs = value.timestamp();
        let nanos = value.timestamp_subsec_nanos() as i64;
        let extra_ms = if nanos % 1_000_000 == 0 { 0 } else { 1 };
        secs * 1000 + (nanos / 1_000_000) + extra_ms
    }

    pub(super) fn settlement_expiration_secs(value: DateTime<Utc>) -> i64 {
        let expiry = value + Duration::days(14);
        let secs = expiry.timestamp();
        let nanos = expiry.timestamp_subsec_nanos() as i64;
        secs + if nanos == 0 { 0 } else { 1 }
    }

    pub(super) fn to_stark_amount(
        value: Decimal,
        resolution: i64,
        rounding: RoundingStrategy,
    ) -> Result<i64, DexError> {
        let scaled = value
            * Decimal::from_i64(resolution).ok_or_else(|| {
                DexError::Other("Invalid resolution for amount conversion".to_string())
            })?;
        let rounded = scaled.round_dp_with_strategy(0, rounding);
        rounded
            .to_i64()
            .ok_or_else(|| DexError::Other("Failed to convert amount to i64".to_string()))
    }

    pub(super) fn compute_settlement(
        &self,
        market: &MarketModel,
        side: &str,
        synthetic_amount: Decimal,
        price: Decimal,
        expire_time: DateTime<Utc>,
        nonce: u64,
    ) -> Result<SettlementData, DexError> {
        let is_buying = side == "BUY";
        let synthetic_resolution = market.l2_config.synthetic_resolution;
        let collateral_resolution = market.l2_config.collateral_resolution;

        let collateral_amount = synthetic_amount * price;
        let total_fee_rate = super::default_taker_fee();
        let fee_amount = total_fee_rate * collateral_amount;

        let rounding_main = if is_buying {
            RoundingStrategy::ToPositiveInfinity
        } else {
            RoundingStrategy::ToNegativeInfinity
        };
        let rounding_fee = RoundingStrategy::ToPositiveInfinity;

        let mut stark_synthetic_amount =
            Self::to_stark_amount(synthetic_amount, synthetic_resolution, rounding_main)?;
        let mut stark_collateral_amount =
            Self::to_stark_amount(collateral_amount, collateral_resolution, rounding_main)?;
        let stark_fee_amount =
            Self::to_stark_amount(fee_amount, collateral_resolution, rounding_fee)? as u64;

        if is_buying {
            stark_collateral_amount = -stark_collateral_amount;
        } else {
            stark_synthetic_amount = -stark_synthetic_amount;
        }

        let expiration_secs = Self::settlement_expiration_secs(expire_time);
        let order_hash = get_order_hash(
            self.vault.to_string(),
            market.l2_config.synthetic_id.clone(),
            stark_synthetic_amount.to_string(),
            market.l2_config.collateral_id.clone(),
            stark_collateral_amount.to_string(),
            market.l2_config.collateral_id.clone(),
            stark_fee_amount.to_string(),
            expiration_secs.to_string(),
            nonce.to_string(),
            self.public_key.clone(),
            DOMAIN_NAME.to_string(),
            DOMAIN_VERSION.to_string(),
            self.domain_chain_id().to_string(),
            DOMAIN_REVISION.to_string(),
        )
        .map_err(|e| DexError::Other(format!("Failed to compute order hash: {}", e)))?;

        let private_key = self.parse_private_key()?;
        let signature = sign_message(&order_hash, &private_key)
            .map_err(|e| DexError::Other(format!("Failed to sign order: {}", e)))?;

        let settlement = StarkSettlementModel {
            signature: SettlementSignatureModel {
                r: signature.r.to_hex_string(),
                s: signature.s.to_hex_string(),
            },
            stark_key: self.parse_public_key()?.to_hex_string(),
            collateral_position: Decimal::from(self.vault),
        };

        let debugging_amounts = StarkDebuggingOrderAmountsModel {
            collateral_amount: Decimal::from(stark_collateral_amount),
            fee_amount: Decimal::from(stark_fee_amount),
            synthetic_amount: Decimal::from(stark_synthetic_amount),
        };

        Ok(SettlementData {
            stark_synthetic_amount,
            stark_collateral_amount,
            stark_fee_amount,
            fee_rate: total_fee_rate,
            order_hash,
            settlement,
            debugging_amounts,
        })
    }
}

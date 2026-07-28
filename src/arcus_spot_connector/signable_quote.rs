//! GET-only validation for Arcus Spot pre-sign quote payloads.
//!
//! Arcus' RFQ model produces the maker-signed firm quote only after the
//! taker intent is signed. `GET /v1/quote` instead returns signable taker
//! intents and venue transactions. This module validates those payloads but
//! deliberately cannot sign or submit them.

use super::{
    parse_raw_amount, validate_token, ArcusSpotClient, ArcusSpotError, ArcusSpotObservation,
    ArcusSpotToken,
};
use ethers::types::{
    transaction::eip712::{Eip712, TypedData, Types as Eip712Types},
    Address, U256,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, str::FromStr};

const BPS_SCALE: u32 = 10_000;
const MAX_DRY_RUN_SLIPPAGE_BPS: u32 = 1_000;
const PERMIT2_PRIMARY_TYPE: &str = "PermitWitnessTransferFrom";
const PERMIT2_ADDRESS: &str = "0x000000000022D473030F116dDEE9F6B43aC78BA3";

/// Whether the pre-sign quote may fall back to Arcus wrapped-token delivery.
///
/// DirectTokenOnly is the safe default for dry-run evidence because wrapped
/// stock tokens carry delayed-settlement and maker-credit risk.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArcusSpotQuoteRoutePolicy {
    #[default]
    DirectTokenOnly,
    WrappedFallbackAllowed,
}

impl ArcusSpotQuoteRoutePolicy {
    fn allow_wrapped(self) -> bool {
        matches!(self, Self::WrappedFallbackAllowed)
    }
}

/// Parameters for the public, GET-only Arcus pre-sign quote endpoint.
///
/// This request does not contain a key or signature. P1 callers should use a
/// dedicated address for which no signing key is present in the process.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArcusSpotSignableQuoteRequest {
    pub sell_symbol: String,
    pub buy_symbol: String,
    pub sell_amount: String,
    pub taker: String,
    pub slippage_bps: u32,
    pub route_policy: ArcusSpotQuoteRoutePolicy,
}

impl ArcusSpotSignableQuoteRequest {
    pub fn new(
        sell_symbol: impl Into<String>,
        buy_symbol: impl Into<String>,
        sell_amount: impl Into<String>,
        taker: impl Into<String>,
        slippage_bps: u32,
        route_policy: ArcusSpotQuoteRoutePolicy,
    ) -> Self {
        Self {
            sell_symbol: sell_symbol.into(),
            buy_symbol: buy_symbol.into(),
            sell_amount: sell_amount.into(),
            taker: taker.into(),
            slippage_bps,
            route_policy,
        }
    }

    fn validate(&self) -> Result<Address, ArcusSpotError> {
        if self.sell_symbol.trim().is_empty() || self.buy_symbol.trim().is_empty() {
            return Err(ArcusSpotError::InvalidResponse(
                "signable quote symbols must not be empty".to_string(),
            ));
        }
        if self.sell_symbol.eq_ignore_ascii_case(&self.buy_symbol) {
            return Err(ArcusSpotError::InvalidResponse(
                "signable quote symbols must be distinct".to_string(),
            ));
        }
        let amount = parse_raw_amount("sellAmount", &self.sell_amount)?;
        if amount.is_zero() {
            return Err(ArcusSpotError::InvalidResponse(
                "sellAmount must be greater than zero".to_string(),
            ));
        }
        if self.slippage_bps > MAX_DRY_RUN_SLIPPAGE_BPS {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "dry-run slippageBps must be at most {MAX_DRY_RUN_SLIPPAGE_BPS}, got {}",
                self.slippage_bps
            )));
        }
        let taker = parse_address("taker", &self.taker)?;
        if taker == Address::zero() {
            return Err(ArcusSpotError::InvalidResponse(
                "taker must not be the zero address".to_string(),
            ));
        }
        Ok(taker)
    }
}

/// One venue returned by GET /v1/quote.
///
/// Arcus uses different signing payloads for its RFQ, Rialto, and LiFi
/// venues. The full values remain preserved while helpers validate the common
/// Permit2 envelope and venue-specific bindings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ArcusSpotSignableVenueQuote {
    pub venue: String,
    pub buy_amount: String,
    pub sell_amount: String,
    #[serde(default)]
    pub min_buy_amount: Option<String>,
    #[serde(default)]
    pub expiry: Option<u64>,
    #[serde(default)]
    pub fees: Vec<Value>,
    pub to_sign: Value,
    #[serde(default)]
    pub tx: Option<Value>,
    #[serde(default)]
    pub needs_allowance: Option<bool>,
    #[serde(default)]
    pub raw: Option<Value>,
    #[serde(default)]
    pub arcus: Option<Value>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl ArcusSpotSignableVenueQuote {
    /// Decode the venue payload as EIP-712 without exposing a signer.
    pub fn typed_data(&self) -> Result<TypedData, ArcusSpotError> {
        serde_json::from_value(self.to_sign.clone()).map_err(|error| {
            ArcusSpotError::InvalidResponse(format!(
                "venue {} returned invalid EIP-712 typed data: {error}",
                self.venue
            ))
        })
    }

    /// Compute the EIP-712 digest as validation evidence only.
    pub fn eip712_digest(&self) -> Result<String, ArcusSpotError> {
        let typed_data = self.typed_data()?;
        let digest = typed_data.encode_eip712().map_err(|error| {
            ArcusSpotError::InvalidResponse(format!(
                "venue {} EIP-712 payload could not be encoded: {error}",
                self.venue
            ))
        })?;
        Ok(format!("0x{}", hex::encode(digest)))
    }

    pub fn minimum_received(&self) -> Result<U256, ArcusSpotError> {
        let mut values = Vec::new();
        if let Some(value) = &self.min_buy_amount {
            values.push(("minBuyAmount", parse_raw_amount("minBuyAmount", value)?));
        }
        push_pointer_u256(
            &mut values,
            &self.to_sign,
            "/message/witness/minBuyAmount",
            "toSign.message.witness.minBuyAmount",
        )?;
        if let Some(arcus) = &self.arcus {
            push_pointer_u256(&mut values, arcus, "/minAmountOut", "arcus.minAmountOut")?;
        }
        one_consistent_u256(&self.venue, "minimum received", values)
    }

    pub fn expires_at(&self) -> Result<u64, ArcusSpotError> {
        let mut values = Vec::new();
        if let Some(expiry) = self.expiry {
            values.push(("expiry", expiry));
        }
        push_pointer_u64(
            &mut values,
            &self.to_sign,
            "/message/deadline",
            "toSign.message.deadline",
        )?;
        push_pointer_u64(
            &mut values,
            &self.to_sign,
            "/message/witness/deadline",
            "toSign.message.witness.deadline",
        )?;
        one_consistent_u64(&self.venue, "expiry/deadline", values)
    }

    pub fn estimated_gas(&self) -> Result<Option<U256>, ArcusSpotError> {
        let mut values = Vec::new();
        if let Some(tx) = &self.tx {
            push_pointer_u256(&mut values, tx, "/estimatedGas", "tx.estimatedGas")?;
        }
        if let Some(raw) = &self.raw {
            push_pointer_u256(
                &mut values,
                raw,
                "/tx/estimated_gas",
                "raw.tx.estimated_gas",
            )?;
            push_pointer_u256(&mut values, raw, "/network_fee/gas", "raw.network_fee.gas")?;
        }
        if values.is_empty() {
            return Ok(None);
        }
        one_consistent_u256(&self.venue, "estimated gas", values).map(Some)
    }

    pub fn network_fee_native(&self) -> Result<Option<String>, ArcusSpotError> {
        let Some(raw) = &self.raw else {
            return Ok(None);
        };
        let Some(value) = raw.pointer("/network_fee/amount") else {
            return Ok(None);
        };
        Ok(Some(
            value_u256(value, "raw.network_fee.amount")?.to_string(),
        ))
    }

    pub fn platform_fee_bps(&self) -> Result<Option<u64>, ArcusSpotError> {
        let Some(raw) = &self.raw else {
            return Ok(None);
        };
        let Some(value) = raw.pointer("/platform_fee/total_bps") else {
            return Ok(None);
        };
        Ok(Some(value_u64(value, "raw.platform_fee.total_bps")?))
    }

    pub fn observed_sell_balance(&self) -> Result<Option<U256>, ArcusSpotError> {
        let Some(raw) = &self.raw else {
            return Ok(None);
        };
        let Some(value) = raw.pointer("/issues/balance/actual") else {
            return Ok(None);
        };
        Ok(Some(value_u256(value, "raw.issues.balance.actual")?))
    }

    fn validate(&self, expected: &QuoteExpectations) -> Result<(), ArcusSpotError> {
        if self.venue.trim().is_empty() {
            return Err(ArcusSpotError::InvalidResponse(
                "signable quote contains an empty venue".to_string(),
            ));
        }
        let buy_amount = parse_raw_amount("buyAmount", &self.buy_amount)?;
        if buy_amount.is_zero() {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "venue {} returned a zero buyAmount",
                self.venue
            )));
        }
        let sell_amount = parse_raw_amount("sellAmount", &self.sell_amount)?;
        if sell_amount != expected.sell_amount {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "venue {} echoed sellAmount {} but {} was requested",
                self.venue, self.sell_amount, expected.sell_amount
            )));
        }

        let minimum_received = self.minimum_received()?;
        if minimum_received.is_zero() || minimum_received > buy_amount {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "venue {} returned invalid minimum received {} for buyAmount {}",
                self.venue, minimum_received, buy_amount
            )));
        }
        validate_slippage_floor(
            &self.venue,
            buy_amount,
            minimum_received,
            expected.slippage_bps,
        )?;

        let deadline = self.expires_at()?;
        if deadline <= expected.now_unix {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "venue {} quote expired at {} before receipt time {}",
                self.venue, deadline, expected.now_unix
            )));
        }

        let typed_data = self.typed_data()?;
        if typed_data.primary_type != PERMIT2_PRIMARY_TYPE {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "venue {} EIP-712 primaryType is {:?}, expected {:?}",
                self.venue, typed_data.primary_type, PERMIT2_PRIMARY_TYPE
            )));
        }
        if typed_data.domain.name.as_deref() != Some("Permit2") {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "venue {} EIP-712 domain name is not Permit2",
                self.venue
            )));
        }
        let chain_id = typed_data.domain.chain_id.ok_or_else(|| {
            ArcusSpotError::InvalidResponse(format!(
                "venue {} EIP-712 domain omitted chainId",
                self.venue
            ))
        })?;
        if chain_id != U256::from(expected.chain_id) {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "venue {} EIP-712 chainId {} does not match {}",
                self.venue, chain_id, expected.chain_id
            )));
        }
        let verifying_contract = typed_data.domain.verifying_contract.ok_or_else(|| {
            ArcusSpotError::InvalidResponse(format!(
                "venue {} EIP-712 domain omitted verifyingContract",
                self.venue
            ))
        })?;
        if verifying_contract == Address::zero() {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "venue {} EIP-712 verifyingContract is zero",
                self.venue
            )));
        }
        let expected_permit2 = parse_address("Permit2 address", PERMIT2_ADDRESS)?;
        if verifying_contract != expected_permit2 {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "venue {} EIP-712 verifyingContract {verifying_contract:#x} is not Permit2 {expected_permit2:#x}",
                self.venue
            )));
        }
        if typed_data.domain.version.is_some() || typed_data.domain.salt.is_some() {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "venue {} Permit2 domain unexpectedly includes version or salt",
                self.venue
            )));
        }
        // A type name being present in `types` only means the schema declares
        // *a* struct with that name; it says nothing about which fields, in
        // which order, that struct actually commits to the signature. EIP-712
        // hashes the full `encodeType` string (name + every field name/type in
        // declared order), so a schema that renames, reorders, adds, or omits
        // a field still lets the pointer-based checks below find every value
        // they look for while producing a type hash the venue contract never
        // agreed to. The declared schema must therefore match the venue's
        // canonical struct exactly, not just contain the fields we happen to
        // read.
        let witness_schema = canonical_witness_schema(&self.venue)?;
        require_exact_eip712_fields(
            &typed_data.types,
            PERMIT2_PRIMARY_TYPE,
            &permit2_type_fields(witness_schema.type_name),
            &self.venue,
        )?;
        require_exact_eip712_fields(
            &typed_data.types,
            "TokenPermissions",
            TOKEN_PERMISSIONS_FIELDS,
            &self.venue,
        )?;
        require_exact_eip712_fields(
            &typed_data.types,
            witness_schema.type_name,
            witness_schema.fields,
            &self.venue,
        )?;
        self.eip712_digest()?;

        expect_pointer_address(
            &self.to_sign,
            "/message/permitted/token",
            "toSign.message.permitted.token",
            expected.sell_token,
        )?;
        expect_pointer_u256(
            &self.to_sign,
            "/message/permitted/amount",
            "toSign.message.permitted.amount",
            expected.sell_amount,
        )?;
        let spender = pointer_address(&self.to_sign, "/message/spender", "toSign.message.spender")?;
        if spender == Address::zero() {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "venue {} EIP-712 spender is zero",
                self.venue
            )));
        }

        if let Some(raw) = &self.raw {
            if raw.pointer("/issues/balance").is_some() {
                expect_pointer_address(
                    raw,
                    "/issues/balance/token",
                    "raw.issues.balance.token",
                    expected.sell_token,
                )?;
                expect_pointer_u256(
                    raw,
                    "/issues/balance/expected",
                    "raw.issues.balance.expected",
                    expected.sell_amount,
                )?;
                self.observed_sell_balance()?;
            }
        }

        match self.venue.to_ascii_lowercase().as_str() {
            "arcus" => self.validate_arcus(expected, deadline, minimum_received),
            "rialto" => self.validate_rialto(expected, minimum_received),
            "lifi" => self.validate_lifi(expected),
            other => Err(ArcusSpotError::InvalidResponse(format!(
                "venue {other} has no signable-quote validator and cannot be trusted"
            ))),
        }
    }

    fn validate_arcus(
        &self,
        expected: &QuoteExpectations,
        deadline: u64,
        minimum_received: U256,
    ) -> Result<(), ArcusSpotError> {
        if self.expiry != Some(deadline) {
            return Err(ArcusSpotError::InvalidResponse(
                "Arcus venue must expose expiry matching its signed deadline".to_string(),
            ));
        }
        expect_pointer_address(
            &self.to_sign,
            "/message/witness/taker",
            "toSign.message.witness.taker",
            expected.taker,
        )?;
        expect_pointer_address(
            &self.to_sign,
            "/message/witness/takerSellToken",
            "toSign.message.witness.takerSellToken",
            expected.sell_token,
        )?;
        expect_pointer_address(
            &self.to_sign,
            "/message/witness/takerBuyToken",
            "toSign.message.witness.takerBuyToken",
            expected.buy_token,
        )?;
        expect_pointer_u256(
            &self.to_sign,
            "/message/witness/sellAmount",
            "toSign.message.witness.sellAmount",
            expected.sell_amount,
        )?;
        expect_pointer_u256(
            &self.to_sign,
            "/message/witness/minBuyAmount",
            "toSign.message.witness.minBuyAmount",
            minimum_received,
        )?;
        let allow_wrapped = pointer_bool(
            &self.to_sign,
            "/message/witness/allowWrapped",
            "toSign.message.witness.allowWrapped",
        )?;
        if allow_wrapped != expected.route_policy.allow_wrapped() {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "Arcus venue echoed allowWrapped={allow_wrapped}, requested {}",
                expected.route_policy.allow_wrapped()
            )));
        }
        expect_equal_pointers(
            &self.to_sign,
            "/message/nonce",
            "/message/witness/nonce",
            "Arcus Permit2/witness nonce",
        )?;
        Ok(())
    }

    fn validate_rialto(
        &self,
        expected: &QuoteExpectations,
        minimum_received: U256,
    ) -> Result<(), ArcusSpotError> {
        expect_pointer_address(&self.to_sign, "/owner", "toSign.owner", expected.taker)?;
        expect_pointer_address(
            &self.to_sign,
            "/message/witness/recipient",
            "toSign.message.witness.recipient",
            expected.taker,
        )?;
        expect_pointer_address(
            &self.to_sign,
            "/message/witness/buyToken",
            "toSign.message.witness.buyToken",
            expected.buy_token,
        )?;
        expect_pointer_u256(
            &self.to_sign,
            "/message/witness/minBuyAmount",
            "toSign.message.witness.minBuyAmount",
            minimum_received,
        )?;
        let spender = pointer_address(&self.to_sign, "/message/spender", "toSign.message.spender")?;
        let tx = self.tx.as_ref().ok_or_else(|| {
            ArcusSpotError::InvalidResponse("Rialto venue omitted transaction metadata".to_string())
        })?;
        expect_pointer_address(tx, "/to", "tx.to", spender)?;
        if let Some(raw) = &self.raw {
            expect_pointer_u64(raw, "/chain_id", "raw.chain_id", expected.chain_id)?;
            expect_pointer_address(raw, "/sell_token", "raw.sell_token", expected.sell_token)?;
            expect_pointer_address(raw, "/buy_token", "raw.buy_token", expected.buy_token)?;
            expect_pointer_u256(raw, "/sell_amount", "raw.sell_amount", expected.sell_amount)?;
            expect_pointer_address(raw, "/taker", "raw.taker", expected.taker)?;
            expect_pointer_u64(
                raw,
                "/slippage_bps",
                "raw.slippage_bps",
                u64::from(expected.slippage_bps),
            )?;
        }
        Ok(())
    }

    fn validate_lifi(&self, expected: &QuoteExpectations) -> Result<(), ArcusSpotError> {
        let tx = self.tx.as_ref().ok_or_else(|| {
            ArcusSpotError::InvalidResponse("LiFi venue omitted transaction metadata".to_string())
        })?;
        expect_pointer_address(tx, "/buyToken", "tx.buyToken", expected.buy_token)?;
        let permit2_proxy = pointer_address(tx, "/permit2Proxy", "tx.permit2Proxy")?;
        let spender = pointer_address(&self.to_sign, "/message/spender", "toSign.message.spender")?;
        if permit2_proxy != spender {
            return Err(ArcusSpotError::InvalidResponse(
                "LiFi permit2Proxy does not match the EIP-712 spender".to_string(),
            ));
        }
        let calldata = pointer_value(tx, "/diamondCalldata", "tx.diamondCalldata")?
            .as_str()
            .ok_or_else(|| {
                ArcusSpotError::InvalidResponse(
                    "tx.diamondCalldata is not a hex string".to_string(),
                )
            })?;
        let calldata_bytes =
            hex::decode(calldata.strip_prefix("0x").unwrap_or(calldata)).map_err(|error| {
                ArcusSpotError::InvalidResponse(format!(
                    "tx.diamondCalldata is not valid hex: {error}"
                ))
            })?;
        let expected_hash = pointer_value(
            &self.to_sign,
            "/message/witness/diamondCalldataHash",
            "toSign.message.witness.diamondCalldataHash",
        )?
        .as_str()
        .ok_or_else(|| {
            ArcusSpotError::InvalidResponse(
                "toSign.message.witness.diamondCalldataHash is not a hex string".to_string(),
            )
        })?;
        let actual_hash = format!(
            "0x{}",
            hex::encode(ethers::utils::keccak256(&calldata_bytes))
        );
        if !actual_hash.eq_ignore_ascii_case(expected_hash) {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "LiFi diamondCalldata hash {actual_hash} does not match witness {expected_hash}"
            )));
        }
        // The signature only commits to a hash of the opaque diamond calldata,
        // not to its decoded meaning; LiFi's facets vary by route so this
        // cannot be fully ABI-decoded generically. Instead require that the
        // expected recipient and buy-token addresses actually appear as
        // 32-byte-aligned parameters inside the signed bytes: every real
        // transfer/swap/bridge call ABI-encodes its addresses this way, so a
        // calldata blob that redirects funds elsewhere or swaps into another
        // asset cannot satisfy both this check and the hash check above.
        if !calldata_references_address(&calldata_bytes, expected.taker) {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "LiFi diamondCalldata does not reference the expected recipient {:#x}",
                expected.taker
            )));
        }
        if !calldata_references_address(&calldata_bytes, expected.buy_token) {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "LiFi diamondCalldata does not reference the expected buy token {:#x}",
                expected.buy_token
            )));
        }
        // raw.action/raw.transactionRequest are the router's own unsigned
        // account of who receives funds and which contract the transaction
        // targets. Treating them as optional let a response skip these
        // cross-checks entirely; require them so every LiFi quote is bound by
        // both the signed-calldata scan above and this independent metadata.
        let raw = self.raw.as_ref().ok_or_else(|| {
            ArcusSpotError::InvalidResponse("LiFi venue omitted raw route metadata".to_string())
        })?;
        let diamond_address = pointer_address(
            &self.to_sign,
            "/message/witness/diamondAddress",
            "toSign.message.witness.diamondAddress",
        )?;
        expect_pointer_address(
            raw,
            "/transactionRequest/to",
            "raw.transactionRequest.to",
            diamond_address,
        )?;
        expect_pointer_address(
            raw,
            "/action/fromAddress",
            "raw.action.fromAddress",
            expected.taker,
        )?;
        expect_pointer_address(
            raw,
            "/action/toAddress",
            "raw.action.toAddress",
            expected.taker,
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArcusSpotSignableQuoteResponse {
    pub recommended: String,
    #[serde(rename = "all")]
    pub quotes: Vec<ArcusSpotSignableVenueQuote>,
    #[serde(default)]
    pub errors: Vec<Value>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl ArcusSpotSignableQuoteResponse {
    pub fn recommended_quote(&self) -> Result<&ArcusSpotSignableVenueQuote, ArcusSpotError> {
        self.quotes
            .iter()
            .find(|quote| quote.venue == self.recommended)
            .ok_or_else(|| {
                ArcusSpotError::InvalidResponse(format!(
                    "recommended venue {:?} is absent from all signable quotes",
                    self.recommended
                ))
            })
    }

    fn validate(&self, expected: &QuoteExpectations) -> Result<(), ArcusSpotError> {
        if self.recommended.trim().is_empty() {
            return Err(ArcusSpotError::InvalidResponse(
                "signable quote response has an empty recommended venue".to_string(),
            ));
        }
        if self.quotes.is_empty() {
            return Err(ArcusSpotError::InvalidResponse(
                "signable quote response has no venue quotes".to_string(),
            ));
        }
        for quote in &self.quotes {
            quote.validate(expected)?;
        }
        self.recommended_quote()?;
        Ok(())
    }
}

/// Request and validated response evidence for one directed pre-sign quote.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArcusSpotSignableQuoteObservation {
    pub chain_id: u64,
    pub request: ArcusSpotSignableQuoteRequest,
    pub sell_token: ArcusSpotToken,
    pub buy_token: ArcusSpotToken,
    pub response: ArcusSpotObservation<ArcusSpotSignableQuoteResponse>,
}

/// Cost and safety fields extracted from one venue without a signature.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArcusSpotSignableVenueAnalysis {
    pub venue: String,
    pub recommended: bool,
    pub buy_amount: String,
    pub minimum_received: String,
    pub expires_at: u64,
    pub ttl_secs_at_receipt: u64,
    pub eip712_digest: String,
    pub estimated_gas: Option<String>,
    pub network_fee_native: Option<String>,
    pub platform_fee_bps: Option<u64>,
    pub observed_sell_balance: Option<String>,
    pub quote_to_reference_deviation_bps: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArcusSpotSignableQuoteAnalysis {
    pub mode: String,
    pub recommended_venue: String,
    pub slippage_bps: u32,
    pub route_policy: ArcusSpotQuoteRoutePolicy,
    pub arcus_route_policy_validated: bool,
    pub non_funded_sell_balance_confirmed: bool,
    pub venues: Vec<ArcusSpotSignableVenueAnalysis>,
    pub venue_errors: Vec<Value>,
}

impl ArcusSpotSignableQuoteObservation {
    /// Extract venue availability, expiry, minimum received, EIP-712 digest,
    /// fee/gas fields, and optional quote-to-reference deviation.
    pub fn analyze(
        &self,
        sell_reference_price_usd: Option<Decimal>,
        buy_reference_price_usd: Option<Decimal>,
    ) -> Result<ArcusSpotSignableQuoteAnalysis, ArcusSpotError> {
        if sell_reference_price_usd.is_some() != buy_reference_price_usd.is_some() {
            return Err(ArcusSpotError::InvalidResponse(
                "both reference prices must be supplied together".to_string(),
            ));
        }
        let received_unix = timestamp_u64(self.response.received_at.timestamp())?;
        let mut venues = Vec::with_capacity(self.response.payload.quotes.len());
        for quote in &self.response.payload.quotes {
            let expires_at = quote.expires_at()?;
            let deviation = match (sell_reference_price_usd, buy_reference_price_usd) {
                (Some(sell_price), Some(buy_price)) => Some(
                    quote_reference_deviation_bps(
                        &self.sell_token,
                        &self.buy_token,
                        &self.request.sell_amount,
                        &quote.buy_amount,
                        sell_price,
                        buy_price,
                    )?
                    .normalize()
                    .to_string(),
                ),
                _ => None,
            };
            venues.push(ArcusSpotSignableVenueAnalysis {
                venue: quote.venue.clone(),
                recommended: quote.venue == self.response.payload.recommended,
                buy_amount: quote.buy_amount.clone(),
                minimum_received: quote.minimum_received()?.to_string(),
                expires_at,
                ttl_secs_at_receipt: expires_at.saturating_sub(received_unix),
                eip712_digest: quote.eip712_digest()?,
                estimated_gas: quote.estimated_gas()?.map(|value| value.to_string()),
                network_fee_native: quote.network_fee_native()?,
                platform_fee_bps: quote.platform_fee_bps()?,
                observed_sell_balance: quote
                    .observed_sell_balance()?
                    .map(|value| value.to_string()),
                quote_to_reference_deviation_bps: deviation,
            });
        }
        let observed_balances = venues
            .iter()
            .filter_map(|venue| venue.observed_sell_balance.as_deref())
            .collect::<Vec<_>>();
        let non_funded_sell_balance_confirmed = !observed_balances.is_empty()
            && observed_balances.iter().all(|balance| *balance == "0");
        Ok(ArcusSpotSignableQuoteAnalysis {
            mode: "public_pre_sign_quote_read_only".to_string(),
            recommended_venue: self.response.payload.recommended.clone(),
            slippage_bps: self.request.slippage_bps,
            route_policy: self.request.route_policy,
            arcus_route_policy_validated: self
                .response
                .payload
                .quotes
                .iter()
                .any(|quote| quote.venue.eq_ignore_ascii_case("arcus")),
            non_funded_sell_balance_confirmed,
            venues,
            venue_errors: self.response.payload.errors.clone(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArcusSpotSignableRoundTripAnalysis {
    pub forward_venue: String,
    pub reverse_venue: String,
    pub start_amount: String,
    pub return_amount: String,
    pub optimistic_round_trip_loss_bps: String,
}

/// Calculate an optimistic pre-sign round trip from the router-selected
/// forward and reverse venues. This is quote evidence, not executable PnL.
pub fn analyze_signable_round_trip(
    forward: &ArcusSpotSignableQuoteObservation,
    reverse: &ArcusSpotSignableQuoteObservation,
) -> Result<ArcusSpotSignableRoundTripAnalysis, ArcusSpotError> {
    if !forward
        .buy_token
        .address
        .eq_ignore_ascii_case(&reverse.sell_token.address)
        || !forward
            .sell_token
            .address
            .eq_ignore_ascii_case(&reverse.buy_token.address)
    {
        return Err(ArcusSpotError::InvalidResponse(
            "signable round-trip token directions do not reverse".to_string(),
        ));
    }
    let forward_quote = forward.response.payload.recommended_quote()?;
    if forward_quote.buy_amount != reverse.request.sell_amount {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "reverse sellAmount {} does not match forward recommended buyAmount {}",
            reverse.request.sell_amount, forward_quote.buy_amount
        )));
    }
    let reverse_quote = reverse.response.payload.recommended_quote()?;
    let loss_bps =
        raw_round_trip_loss_bps(&forward.request.sell_amount, &reverse_quote.buy_amount)?;
    Ok(ArcusSpotSignableRoundTripAnalysis {
        forward_venue: forward_quote.venue.clone(),
        reverse_venue: reverse_quote.venue.clone(),
        start_amount: forward.request.sell_amount.clone(),
        return_amount: reverse_quote.buy_amount.clone(),
        optimistic_round_trip_loss_bps: loss_bps.normalize().to_string(),
    })
}

impl ArcusSpotClient {
    /// Request and validate the public pre-sign quote for two verified tokens.
    ///
    /// This is deliberately GET-only. The returned EIP-712 payload is decoded
    /// and hashed for evidence, but this crate exposes no Arcus Spot signer or
    /// submit method.
    pub async fn signable_quote_by_symbol(
        &self,
        request: &ArcusSpotSignableQuoteRequest,
    ) -> Result<ArcusSpotSignableQuoteObservation, ArcusSpotError> {
        let taker = request.validate()?;
        let sell = self.verified_token(&request.sell_symbol).await?;
        let buy = self.verified_token(&request.buy_symbol).await?;
        validate_token(&sell)?;
        validate_token(&buy)?;
        if sell.chain_id != self.inner.config.chain_id || buy.chain_id != self.inner.config.chain_id
        {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "signable quote token chain mismatch: expected {}, got sell={} buy={}",
                self.inner.config.chain_id, sell.chain_id, buy.chain_id
            )));
        }
        let sell_address = parse_address("sellToken", &sell.address)?;
        let buy_address = parse_address("buyToken", &buy.address)?;
        if sell_address == buy_address {
            return Err(ArcusSpotError::InvalidResponse(
                "sell and buy token addresses are identical".to_string(),
            ));
        }
        let sell_amount = parse_raw_amount("sellAmount", &request.sell_amount)?;
        let query = vec![
            ("chainId", self.inner.config.chain_id.to_string()),
            ("sellToken", sell.address.clone()),
            ("buyToken", buy.address.clone()),
            ("sellAmount", request.sell_amount.clone()),
            ("taker", format!("{taker:#x}")),
            ("slippageBps", request.slippage_bps.to_string()),
            (
                "allowWrapped",
                request.route_policy.allow_wrapped().to_string(),
            ),
        ];
        let response: ArcusSpotObservation<ArcusSpotSignableQuoteResponse> = self
            .get_json(&self.inner.router_base_url, "v1/quote", &query)
            .await?;
        let now_unix = timestamp_u64(response.received_at.timestamp())?;
        response.payload.validate(&QuoteExpectations {
            chain_id: self.inner.config.chain_id,
            sell_token: sell_address,
            buy_token: buy_address,
            sell_amount,
            taker,
            slippage_bps: request.slippage_bps,
            route_policy: request.route_policy,
            now_unix,
        })?;
        Ok(ArcusSpotSignableQuoteObservation {
            chain_id: self.inner.config.chain_id,
            request: request.clone(),
            sell_token: sell,
            buy_token: buy,
            response,
        })
    }
}

struct QuoteExpectations {
    chain_id: u64,
    sell_token: Address,
    buy_token: Address,
    sell_amount: U256,
    taker: Address,
    slippage_bps: u32,
    route_policy: ArcusSpotQuoteRoutePolicy,
    now_unix: u64,
}

fn validate_slippage_floor(
    venue: &str,
    buy_amount: U256,
    minimum_received: U256,
    slippage_bps: u32,
) -> Result<(), ArcusSpotError> {
    let protected_bps = BPS_SCALE.checked_sub(slippage_bps).ok_or_else(|| {
        ArcusSpotError::InvalidResponse(format!("slippageBps {slippage_bps} exceeds {BPS_SCALE}"))
    })?;
    let numerator = buy_amount
        .checked_mul(U256::from(protected_bps))
        .ok_or_else(|| {
            ArcusSpotError::InvalidResponse(format!(
                "venue {venue} buyAmount overflows slippage validation"
            ))
        })?;
    let scale = U256::from(BPS_SCALE);
    let floor = numerator / scale;
    let remainder = numerator % scale;
    let ceil = if remainder.is_zero() {
        floor
    } else {
        floor.checked_add(U256::one()).ok_or_else(|| {
            ArcusSpotError::InvalidResponse(format!(
                "venue {venue} minimum-received ceiling overflow"
            ))
        })?
    };
    if minimum_received != floor && minimum_received != ceil {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "venue {venue} minimum received {minimum_received} does not match +             buyAmount {buy_amount} with slippageBps {slippage_bps} (expected {floor} or {ceil})"
        )));
    }
    Ok(())
}

fn quote_reference_deviation_bps(
    sell_token: &ArcusSpotToken,
    buy_token: &ArcusSpotToken,
    sell_amount: &str,
    buy_amount: &str,
    sell_reference_price_usd: Decimal,
    buy_reference_price_usd: Decimal,
) -> Result<Decimal, ArcusSpotError> {
    if sell_reference_price_usd <= Decimal::ZERO || buy_reference_price_usd <= Decimal::ZERO {
        return Err(ArcusSpotError::InvalidResponse(
            "reference prices must be positive".to_string(),
        ));
    }
    let sell_raw = decimal_raw_amount("sellAmount", sell_amount)?;
    let buy_raw = decimal_raw_amount("buyAmount", buy_amount)?;
    let sell_scale = token_decimal_scale(sell_token)?;
    let buy_scale = token_decimal_scale(buy_token)?;
    let sell_units = sell_raw.checked_div(sell_scale).ok_or_else(|| {
        ArcusSpotError::InvalidResponse("sell amount scale division failed".to_string())
    })?;
    let expected_buy_units = sell_units
        .checked_mul(sell_reference_price_usd)
        .and_then(|value| value.checked_div(buy_reference_price_usd))
        .ok_or_else(|| {
            ArcusSpotError::InvalidResponse(
                "reference-price conversion exceeds Decimal range".to_string(),
            )
        })?;
    let actual_buy_units = buy_raw.checked_div(buy_scale).ok_or_else(|| {
        ArcusSpotError::InvalidResponse("buy amount scale division failed".to_string())
    })?;
    if expected_buy_units <= Decimal::ZERO {
        return Err(ArcusSpotError::InvalidResponse(
            "reference conversion produced a non-positive buy amount".to_string(),
        ));
    }
    actual_buy_units
        .checked_div(expected_buy_units)
        .and_then(|ratio| ratio.checked_sub(Decimal::ONE))
        .and_then(|deviation| deviation.checked_mul(Decimal::from(BPS_SCALE)))
        .ok_or_else(|| {
            ArcusSpotError::InvalidResponse(
                "quote-to-reference deviation exceeds Decimal range".to_string(),
            )
        })
}

fn raw_round_trip_loss_bps(
    start_amount: &str,
    return_amount: &str,
) -> Result<Decimal, ArcusSpotError> {
    let start = decimal_raw_amount("startAmount", start_amount)?;
    let returned = decimal_raw_amount("returnAmount", return_amount)?;
    if start <= Decimal::ZERO {
        return Err(ArcusSpotError::InvalidResponse(
            "round-trip start amount must be positive".to_string(),
        ));
    }
    start
        .checked_sub(returned)
        .and_then(|loss| loss.checked_div(start))
        .and_then(|ratio| ratio.checked_mul(Decimal::from(BPS_SCALE)))
        .ok_or_else(|| {
            ArcusSpotError::InvalidResponse("round-trip loss exceeds Decimal range".to_string())
        })
}

fn decimal_raw_amount(field: &str, raw: &str) -> Result<Decimal, ArcusSpotError> {
    Decimal::from_str(raw).map_err(|error| {
        ArcusSpotError::InvalidResponse(format!(
            "{field} exceeds analysis Decimal range: {raw}: {error}"
        ))
    })
}

fn token_decimal_scale(token: &ArcusSpotToken) -> Result<Decimal, ArcusSpotError> {
    if token.decimals > 28 {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "token {} decimals {} exceed analysis precision",
            token.symbol, token.decimals
        )));
    }
    Decimal::from_str(&format!("1{}", "0".repeat(token.decimals as usize))).map_err(|error| {
        ArcusSpotError::InvalidResponse(format!(
            "could not build decimal scale for {}: {error}",
            token.symbol
        ))
    })
}

fn timestamp_u64(timestamp: i64) -> Result<u64, ArcusSpotError> {
    u64::try_from(timestamp).map_err(|_| {
        ArcusSpotError::InvalidResponse(format!("negative receipt timestamp: {timestamp}"))
    })
}

fn parse_address(field: &str, raw: &str) -> Result<Address, ArcusSpotError> {
    Address::from_str(raw).map_err(|error| {
        ArcusSpotError::InvalidResponse(format!("{field} is not an address: {raw}: {error}"))
    })
}

fn pointer_value<'a>(
    value: &'a Value,
    pointer: &str,
    field: &str,
) -> Result<&'a Value, ArcusSpotError> {
    value
        .pointer(pointer)
        .ok_or_else(|| ArcusSpotError::InvalidResponse(format!("signable quote omitted {field}")))
}

const TOKEN_PERMISSIONS_FIELDS: &[(&str, &str)] = &[("token", "address"), ("amount", "uint256")];

const ARCUS_WITNESS_FIELDS: &[(&str, &str)] = &[
    ("taker", "address"),
    ("takerSellToken", "address"),
    ("takerBuyToken", "address"),
    ("sellAmount", "uint256"),
    ("minBuyAmount", "uint256"),
    ("allowWrapped", "bool"),
    ("nonce", "uint256"),
    ("deadline", "uint256"),
];

const RIALTO_WITNESS_FIELDS: &[(&str, &str)] = &[
    ("recipient", "address"),
    ("buyToken", "address"),
    ("minBuyAmount", "uint256"),
    ("deadline", "uint64"),
    ("feeRecipient", "address"),
    ("srcBps", "uint16"),
    ("dstBps", "uint16"),
    ("referralCode", "bytes32"),
    ("quoteId", "bytes32"),
    ("actionsHash", "bytes32"),
];

const LIFI_WITNESS_FIELDS: &[(&str, &str)] = &[
    ("diamondAddress", "address"),
    ("diamondCalldataHash", "bytes32"),
];

/// The canonical witness struct name and complete, ordered field list a venue
/// must declare for its signature to bind the values this module reads.
struct WitnessSchema {
    type_name: &'static str,
    fields: &'static [(&'static str, &'static str)],
}

/// Fails closed for any venue without a known canonical schema, and returns
/// the schema this module trusts instead of whatever struct name the router
/// response happens to declare.
fn canonical_witness_schema(venue: &str) -> Result<WitnessSchema, ArcusSpotError> {
    match venue.to_ascii_lowercase().as_str() {
        "arcus" => Ok(WitnessSchema {
            type_name: "TakerIntent",
            fields: ARCUS_WITNESS_FIELDS,
        }),
        "rialto" => Ok(WitnessSchema {
            type_name: "RialtoSwap",
            fields: RIALTO_WITNESS_FIELDS,
        }),
        "lifi" => Ok(WitnessSchema {
            type_name: "LiFiCall",
            fields: LIFI_WITNESS_FIELDS,
        }),
        other => Err(ArcusSpotError::InvalidResponse(format!(
            "venue {other} has no signable-quote validator and cannot be trusted"
        ))),
    }
}

fn permit2_type_fields(witness_type: &'static str) -> [(&'static str, &'static str); 5] {
    [
        ("permitted", "TokenPermissions"),
        ("spender", "address"),
        ("nonce", "uint256"),
        ("deadline", "uint256"),
        ("witness", witness_type),
    ]
}

/// Fails unless `type_name` is declared in the venue's EIP-712 schema with
/// exactly `expected`'s field names, types, and order — nothing renamed,
/// reordered, added, or omitted. EIP-712's type hash is computed from that
/// full `encodeType` string, so any deviation produces a signature that does
/// not commit to what this module's pointer-based checks read, even though
/// each individual field it looks for is still present.
fn require_exact_eip712_fields(
    types: &Eip712Types,
    type_name: &str,
    expected: &[(&str, &str)],
    venue: &str,
) -> Result<(), ArcusSpotError> {
    let declared = types.get(type_name).ok_or_else(|| {
        ArcusSpotError::InvalidResponse(format!("venue {venue} EIP-712 types omit {type_name}"))
    })?;
    let actual: Vec<(&str, &str)> = declared
        .iter()
        .map(|field| (field.name.as_str(), field.r#type.as_str()))
        .collect();
    if actual.as_slice() != expected {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "venue {venue} EIP-712 {type_name} fields {actual:?} do not exactly match the canonical schema {expected:?}"
        )));
    }
    Ok(())
}

/// Whether `address` appears as a 32-byte-aligned ABI parameter anywhere in
/// `calldata` after its 4-byte selector. Real transfer/swap/bridge calldata
/// always encodes address arguments this way, so this is a lightweight bound
/// on what a signed-but-undecoded calldata blob can be doing without needing
/// venue-specific ABI knowledge.
fn calldata_references_address(calldata: &[u8], address: Address) -> bool {
    if calldata.len() <= 4 {
        return false;
    }
    let mut padded = [0_u8; 32];
    padded[12..].copy_from_slice(address.as_bytes());
    calldata[4..].chunks_exact(32).any(|word| word == padded)
}

fn pointer_address(value: &Value, pointer: &str, field: &str) -> Result<Address, ArcusSpotError> {
    let raw = pointer_value(value, pointer, field)?
        .as_str()
        .ok_or_else(|| ArcusSpotError::InvalidResponse(format!("{field} is not a string")))?;
    parse_address(field, raw)
}

fn pointer_bool(value: &Value, pointer: &str, field: &str) -> Result<bool, ArcusSpotError> {
    pointer_value(value, pointer, field)?
        .as_bool()
        .ok_or_else(|| ArcusSpotError::InvalidResponse(format!("{field} is not a boolean")))
}

fn expect_pointer_address(
    value: &Value,
    pointer: &str,
    field: &str,
    expected: Address,
) -> Result<(), ArcusSpotError> {
    let actual = pointer_address(value, pointer, field)?;
    if actual != expected {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "{field} {actual:#x} does not match expected {expected:#x}"
        )));
    }
    Ok(())
}

fn expect_pointer_u256(
    value: &Value,
    pointer: &str,
    field: &str,
    expected: U256,
) -> Result<(), ArcusSpotError> {
    let actual = value_u256(pointer_value(value, pointer, field)?, field)?;
    if actual != expected {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "{field} {actual} does not match expected {expected}"
        )));
    }
    Ok(())
}

fn expect_pointer_u64(
    value: &Value,
    pointer: &str,
    field: &str,
    expected: u64,
) -> Result<(), ArcusSpotError> {
    let actual = value_u64(pointer_value(value, pointer, field)?, field)?;
    if actual != expected {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "{field} {actual} does not match expected {expected}"
        )));
    }
    Ok(())
}

fn expect_equal_pointers(
    value: &Value,
    left: &str,
    right: &str,
    field: &str,
) -> Result<(), ArcusSpotError> {
    let left_value = value_u256(pointer_value(value, left, field)?, field)?;
    let right_value = value_u256(pointer_value(value, right, field)?, field)?;
    if left_value != right_value {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "{field} differs: {left_value} != {right_value}"
        )));
    }
    Ok(())
}

fn value_u256(value: &Value, field: &str) -> Result<U256, ArcusSpotError> {
    let raw = match value {
        Value::String(raw) => raw.clone(),
        Value::Number(number) => number.to_string(),
        _ => {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "{field} is not a uint256 string or number"
            )))
        }
    };
    if let Some(hex) = raw.strip_prefix("0x") {
        U256::from_str_radix(hex, 16).map_err(|error| {
            ArcusSpotError::InvalidResponse(format!("{field} is not uint256 hex: {raw}: {error}"))
        })
    } else {
        parse_raw_amount(field, &raw)
    }
}

fn value_u64(value: &Value, field: &str) -> Result<u64, ArcusSpotError> {
    let parsed = value_u256(value, field)?;
    if parsed > U256::from(u64::MAX) {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "{field} exceeds u64: {parsed}"
        )));
    }
    Ok(parsed.as_u64())
}

fn push_pointer_u256<'a>(
    values: &mut Vec<(&'a str, U256)>,
    object: &'a Value,
    pointer: &str,
    field: &'a str,
) -> Result<(), ArcusSpotError> {
    if let Some(value) = object.pointer(pointer) {
        values.push((field, value_u256(value, field)?));
    }
    Ok(())
}

fn push_pointer_u64<'a>(
    values: &mut Vec<(&'a str, u64)>,
    object: &'a Value,
    pointer: &str,
    field: &'a str,
) -> Result<(), ArcusSpotError> {
    if let Some(value) = object.pointer(pointer) {
        values.push((field, value_u64(value, field)?));
    }
    Ok(())
}

fn one_consistent_u256(
    venue: &str,
    field: &str,
    values: Vec<(&str, U256)>,
) -> Result<U256, ArcusSpotError> {
    let Some((first_name, first)) = values.first().copied() else {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "venue {venue} omitted {field}"
        )));
    };
    if let Some((name, value)) = values.iter().copied().find(|(_, value)| *value != first) {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "venue {venue} has inconsistent {field}: {first_name}={first}, {name}={value}"
        )));
    }
    Ok(first)
}

fn one_consistent_u64(
    venue: &str,
    field: &str,
    values: Vec<(&str, u64)>,
) -> Result<u64, ArcusSpotError> {
    let Some((first_name, first)) = values.first().copied() else {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "venue {venue} omitted {field}"
        )));
    };
    if let Some((name, value)) = values.iter().copied().find(|(_, value)| *value != first) {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "venue {venue} has inconsistent {field}: {first_name}={first}, {name}={value}"
        )));
    }
    Ok(first)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use std::sync::{Arc, Mutex as StdMutex};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    const TOKEN_FIXTURE: &str = include_str!("fixtures/tokens_nvda_amd.json");
    const QUOTE_FIXTURE: &str = include_str!("fixtures/quote_nvda_amd.json");
    const TEST_TAKER: &str = "0x7600000000000000000000000000000000000001";
    const SELL_TOKEN: &str = "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC";
    const BUY_TOKEN: &str = "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC";

    fn fixture_expectations() -> QuoteExpectations {
        QuoteExpectations {
            chain_id: 4663,
            sell_token: parse_address("sell", SELL_TOKEN).unwrap(),
            buy_token: parse_address("buy", BUY_TOKEN).unwrap(),
            sell_amount: U256::from(1000_u64),
            taker: parse_address("taker", TEST_TAKER).unwrap(),
            slippage_bps: 37,
            route_policy: ArcusSpotQuoteRoutePolicy::DirectTokenOnly,
            now_unix: 1_700_000_000,
        }
    }

    fn fixture_observation() -> ArcusSpotSignableQuoteObservation {
        let tokens: Vec<ArcusSpotToken> = serde_json::from_str(TOKEN_FIXTURE).unwrap();
        ArcusSpotSignableQuoteObservation {
            chain_id: 4663,
            request: ArcusSpotSignableQuoteRequest::new(
                "NVDA",
                "AMD",
                "1000",
                TEST_TAKER,
                37,
                ArcusSpotQuoteRoutePolicy::DirectTokenOnly,
            ),
            sell_token: tokens
                .iter()
                .find(|token| token.symbol == "NVDA")
                .unwrap()
                .clone(),
            buy_token: tokens
                .iter()
                .find(|token| token.symbol == "AMD")
                .unwrap()
                .clone(),
            response: ArcusSpotObservation {
                payload: serde_json::from_str(QUOTE_FIXTURE).unwrap(),
                requested_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
                received_at: Utc.timestamp_opt(1_700_000_001, 0).unwrap(),
                latency_ms: 10,
                attempts: 1,
            },
        }
    }

    #[test]
    fn fixture_validates_every_venue_typed_data_and_cost_field() {
        let response: ArcusSpotSignableQuoteResponse = serde_json::from_str(QUOTE_FIXTURE).unwrap();
        response.validate(&fixture_expectations()).unwrap();
        assert_eq!(response.quotes.len(), 3);
        assert_eq!(response.recommended_quote().unwrap().venue, "arcus");
        for quote in &response.quotes {
            assert_eq!(quote.eip712_digest().unwrap().len(), 66);
            assert!(quote.expires_at().unwrap() > 1_700_000_000);
            assert!(quote.minimum_received().unwrap() > U256::zero());
        }
        let rialto = response
            .quotes
            .iter()
            .find(|quote| quote.venue == "rialto")
            .unwrap();
        assert_eq!(rialto.estimated_gas().unwrap(), Some(U256::from(240_000)));
        assert_eq!(rialto.observed_sell_balance().unwrap(), Some(U256::zero()));
        assert_eq!(rialto.platform_fee_bps().unwrap(), Some(6));
    }

    #[test]
    fn analysis_separates_reference_deviation_fee_gas_and_balance() {
        let observation = fixture_observation();
        observation
            .response
            .payload
            .validate(&fixture_expectations())
            .unwrap();
        let analysis = observation
            .analyze(Some(Decimal::from(100)), Some(Decimal::from(100)))
            .unwrap();
        assert_eq!(analysis.mode, "public_pre_sign_quote_read_only");
        assert!(analysis.arcus_route_policy_validated);
        assert!(analysis.non_funded_sell_balance_confirmed);
        assert_eq!(analysis.venues.len(), 3);
        let arcus = analysis
            .venues
            .iter()
            .find(|venue| venue.venue == "arcus")
            .unwrap();
        assert_eq!(
            arcus.quote_to_reference_deviation_bps.as_deref(),
            Some("-100")
        );
        let rialto = analysis
            .venues
            .iter()
            .find(|venue| venue.venue == "rialto")
            .unwrap();
        assert_eq!(rialto.estimated_gas.as_deref(), Some("240000"));
        assert_eq!(rialto.network_fee_native.as_deref(), Some("8400000000000"));
        assert_eq!(rialto.platform_fee_bps, Some(6));
        assert_eq!(rialto.observed_sell_balance.as_deref(), Some("0"));
    }

    #[test]
    fn wrapped_flag_and_slippage_mismatches_are_rejected() {
        let mut response: ArcusSpotSignableQuoteResponse =
            serde_json::from_str(QUOTE_FIXTURE).unwrap();
        response.quotes[0].to_sign["message"]["witness"]["allowWrapped"] = Value::Bool(true);
        let error = response.validate(&fixture_expectations()).unwrap_err();
        assert!(error.to_string().contains("allowWrapped"));

        let mut response: ArcusSpotSignableQuoteResponse =
            serde_json::from_str(QUOTE_FIXTURE).unwrap();
        response.quotes[0].arcus.as_mut().unwrap()["minAmountOut"] =
            Value::String("900".to_string());
        response.quotes[0].to_sign["message"]["witness"]["minBuyAmount"] =
            Value::String("900".to_string());
        let error = response.validate(&fixture_expectations()).unwrap_err();
        assert!(error.to_string().contains("slippageBps"));
    }

    #[test]
    fn wrong_permit2_or_lifi_calldata_binding_is_rejected() {
        let mut response: ArcusSpotSignableQuoteResponse =
            serde_json::from_str(QUOTE_FIXTURE).unwrap();
        response.quotes[0].to_sign["domain"]["verifyingContract"] =
            Value::String("0x0000000000000000000000000000000000000001".to_string());
        assert!(response
            .validate(&fixture_expectations())
            .unwrap_err()
            .to_string()
            .contains("is not Permit2"));

        let mut response: ArcusSpotSignableQuoteResponse =
            serde_json::from_str(QUOTE_FIXTURE).unwrap();
        response.quotes[2].tx.as_mut().unwrap()["diamondCalldata"] =
            Value::String("0x01".to_string());
        assert!(response
            .validate(&fixture_expectations())
            .unwrap_err()
            .to_string()
            .contains("diamondCalldata hash"));
    }

    #[test]
    fn truncated_eip712_type_declaration_is_rejected() {
        let mut response: ArcusSpotSignableQuoteResponse =
            serde_json::from_str(QUOTE_FIXTURE).unwrap();
        // The message body still carries a spender value, but the type no
        // longer declares the field, so EIP-712 encoding would not actually
        // commit to it; the value must not be treated as signed.
        response.quotes[0].to_sign["types"]["PermitWitnessTransferFrom"]
            .as_array_mut()
            .unwrap()
            .retain(|field| field["name"] != "spender");
        let error = response.validate(&fixture_expectations()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("do not exactly match the canonical schema"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn renamed_or_reordered_witness_type_is_rejected() {
        let mut response: ArcusSpotSignableQuoteResponse =
            serde_json::from_str(QUOTE_FIXTURE).unwrap();
        // Swap the order of two witness fields; every individual field is
        // still declared, but the EIP-712 type hash depends on their order.
        let witness_fields = response.quotes[0].to_sign["types"]["TakerIntent"]
            .as_array_mut()
            .unwrap();
        witness_fields.swap(0, 1);
        let error = response.validate(&fixture_expectations()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("do not exactly match the canonical schema"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn lifi_calldata_not_referencing_expected_recipient_is_rejected() {
        let mut response: ArcusSpotSignableQuoteResponse =
            serde_json::from_str(QUOTE_FIXTURE).unwrap();
        let lifi = response
            .quotes
            .iter_mut()
            .find(|quote| quote.venue == "lifi")
            .unwrap();
        // Hash-consistent calldata that does not encode the expected taker or
        // buy token anywhere; only an unrelated decoy address is present.
        lifi.tx.as_mut().unwrap()["diamondCalldata"] = Value::String(
            "0xaabbccdd0000000000000000000000000000000000000000000000000000000000000099"
                .to_string(),
        );
        lifi.to_sign["message"]["witness"]["diamondCalldataHash"] = Value::String(
            "0x5dafc5958a8a7eeafac1368cb1db09246b4e81d32959d8a279d0ee0148ec51a3".to_string(),
        );
        let error = response.validate(&fixture_expectations()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not reference the expected recipient"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn lifi_missing_raw_route_metadata_is_rejected() {
        let mut response: ArcusSpotSignableQuoteResponse =
            serde_json::from_str(QUOTE_FIXTURE).unwrap();
        let lifi = response
            .quotes
            .iter_mut()
            .find(|quote| quote.venue == "lifi")
            .unwrap();
        lifi.raw = None;
        let error = response.validate(&fixture_expectations()).unwrap_err();
        assert!(
            error.to_string().contains("omitted raw route metadata"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn venue_without_a_validator_is_rejected() {
        let mut response: ArcusSpotSignableQuoteResponse =
            serde_json::from_str(QUOTE_FIXTURE).unwrap();
        response.quotes[0].venue = "liif".to_string();
        let error = response.validate(&fixture_expectations()).unwrap_err();
        assert!(
            error.to_string().contains("no signable-quote validator"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rialto_tx_target_mismatched_with_spender_is_rejected() {
        let mut response: ArcusSpotSignableQuoteResponse =
            serde_json::from_str(QUOTE_FIXTURE).unwrap();
        let rialto = response
            .quotes
            .iter_mut()
            .find(|quote| quote.venue == "rialto")
            .unwrap();
        rialto.tx.as_mut().unwrap()["to"] =
            Value::String("0x0000000000000000000000000000000000000099".to_string());
        let error = response.validate(&fixture_expectations()).unwrap_err();
        assert!(
            error.to_string().contains("tx.to"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn expired_or_wrong_chain_typed_data_is_rejected() {
        let response: ArcusSpotSignableQuoteResponse = serde_json::from_str(QUOTE_FIXTURE).unwrap();
        let mut expected = fixture_expectations();
        expected.now_unix = 4_102_444_801;
        assert!(response
            .validate(&expected)
            .unwrap_err()
            .to_string()
            .contains("expired"));

        let mut response: ArcusSpotSignableQuoteResponse =
            serde_json::from_str(QUOTE_FIXTURE).unwrap();
        response.quotes[0].to_sign["domain"]["chainId"] = Value::from(1);
        assert!(response
            .validate(&fixture_expectations())
            .unwrap_err()
            .to_string()
            .contains("chainId"));
    }

    async fn spawn_quote_server(
        responses: Vec<String>,
    ) -> (
        String,
        Arc<StdMutex<Vec<String>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let request_log = requests.clone();
        let server = tokio::spawn(async move {
            for response_body in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = vec![0_u8; 16_384];
                let size = socket.read(&mut request).await.unwrap();
                request.truncate(size);
                request_log
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&request).to_string());
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    response_body.len(),
                    response_body
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (format!("http://{address}"), requests, server)
    }

    #[tokio::test]
    async fn client_uses_get_with_every_pre_sign_safety_parameter() {
        let (base_url, requests, server) =
            spawn_quote_server(vec![TOKEN_FIXTURE.to_string(), QUOTE_FIXTURE.to_string()]).await;
        let client = ArcusSpotClient::new(super::super::ArcusSpotConfig {
            router_base_url: base_url.clone(),
            meta_base_url: base_url,
            min_request_interval_ms: 0,
            max_attempts: 1,
            ..super::super::ArcusSpotConfig::default()
        })
        .unwrap();
        let request = ArcusSpotSignableQuoteRequest::new(
            "NVDA",
            "AMD",
            "1000",
            TEST_TAKER,
            37,
            ArcusSpotQuoteRoutePolicy::DirectTokenOnly,
        );
        let result = client.signable_quote_by_symbol(&request).await.unwrap();
        assert_eq!(result.response.payload.recommended, "arcus");
        server.await.unwrap();

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        let quote_request = &requests[1];
        assert!(quote_request.starts_with("GET /v1/quote?"));
        for expected in [
            "chainId=4663",
            "sellAmount=1000",
            "taker=0x7600000000000000000000000000000000000001",
            "slippageBps=37",
            "allowWrapped=false",
        ] {
            assert!(
                quote_request.contains(expected),
                "request omitted {expected}: {quote_request}"
            );
        }
    }

    #[test]
    fn optimistic_round_trip_requires_exact_reverse_size() {
        let forward = fixture_observation();
        let mut reverse = fixture_observation();
        std::mem::swap(&mut reverse.sell_token, &mut reverse.buy_token);
        reverse.request.sell_symbol = "AMD".to_string();
        reverse.request.buy_symbol = "NVDA".to_string();
        reverse.request.sell_amount = "990".to_string();
        reverse.response.payload.recommended = "arcus".to_string();
        reverse.response.payload.quotes[0].buy_amount = "980".to_string();
        let analysis = analyze_signable_round_trip(&forward, &reverse).unwrap();
        assert_eq!(analysis.optimistic_round_trip_loss_bps, "200");

        reverse.request.sell_amount = "989".to_string();
        assert!(analyze_signable_round_trip(&forward, &reverse).is_err());
    }

    #[tokio::test]
    #[ignore = "GET-only public Arcus pre-sign quote smoke; requires network and a no-key address"]
    async fn public_arcus_pre_sign_quote_smoke() {
        let taker = std::env::var("ARCUS_SPOT_TEST_TAKER")
            .expect("set ARCUS_SPOT_TEST_TAKER to a dedicated address with no key in this process");
        let client = ArcusSpotClient::new(super::super::ArcusSpotConfig::default()).unwrap();
        let request = ArcusSpotSignableQuoteRequest::new(
            "NVDA",
            "AMD",
            "24183796856106408",
            &taker,
            37,
            ArcusSpotQuoteRoutePolicy::DirectTokenOnly,
        );
        let forward = client.signable_quote_by_symbol(&request).await.unwrap();
        let sell_reference = client
            .reference_price_by_symbol("NVDA")
            .await
            .unwrap()
            .payload
            .quote
            .price
            .unwrap();
        let buy_reference = client
            .reference_price_by_symbol("AMD")
            .await
            .unwrap()
            .payload
            .quote
            .price
            .unwrap();
        let forward_analysis = forward
            .analyze(Some(sell_reference), Some(buy_reference))
            .unwrap();
        assert!(forward_analysis.arcus_route_policy_validated);
        assert!(forward_analysis.non_funded_sell_balance_confirmed);
        assert!(forward_analysis
            .venues
            .iter()
            .all(|venue| venue.ttl_secs_at_receipt > 0));

        let reverse_sell_amount = forward
            .response
            .payload
            .recommended_quote()
            .unwrap()
            .buy_amount
            .clone();
        let reverse_request = ArcusSpotSignableQuoteRequest::new(
            "AMD",
            "NVDA",
            reverse_sell_amount,
            &taker,
            37,
            ArcusSpotQuoteRoutePolicy::DirectTokenOnly,
        );
        let reverse = client
            .signable_quote_by_symbol(&reverse_request)
            .await
            .unwrap();
        let round_trip = analyze_signable_round_trip(&forward, &reverse).unwrap();
        println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "forward": forward_analysis,
                "roundTrip": round_trip,
            }))
            .unwrap()
        );
    }
}

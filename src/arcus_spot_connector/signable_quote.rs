//! GET-only validation for Arcus Spot pre-sign quote payloads.
//!
//! Arcus' RFQ model produces the maker-signed firm quote only after the
//! taker intent is signed. `GET /v1/quote` instead returns signable taker
//! intents and venue transactions. This module validates those payloads but
//! deliberately cannot sign or submit them.

use super::{
    normalize_symbol, parse_raw_amount, validate_token, ArcusSpotClient, ArcusSpotError,
    ArcusSpotObservation, ArcusSpotToken,
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
/// An RFQ intent is meant to be exercised near-immediately; Permit2 nonces
/// only prevent *reuse* after execution, they do nothing to stop an unused
/// signature from sitting idle and being exercised much later at a stale
/// minimum price. Cap how far past receipt a signed deadline may sit.
const MAX_SIGNING_DEADLINE_TTL_SECS: u64 = 300;

/// `expected.now_unix` is truncated to whole seconds from the response's
/// receipt time, so a `deadline` just one second past it can still leave a
/// genuine remaining lifetime anywhere from a few milliseconds up to just
/// under two seconds — not enough to survive EIP-712 validation and the
/// caller signing it. Require this many whole seconds of slack past the
/// truncated receipt time before treating a quote as signable.
const MIN_SIGNING_DEADLINE_TTL_SECS: u64 = 5;

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
        let ttl_secs = deadline.saturating_sub(expected.now_unix);
        if ttl_secs < MIN_SIGNING_DEADLINE_TTL_SECS {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "venue {} quote deadline {} is only {ttl_secs}s after receipt time {}, below the {MIN_SIGNING_DEADLINE_TTL_SECS}s minimum usable signing TTL",
                self.venue, deadline, expected.now_unix
            )));
        }
        if ttl_secs > MAX_SIGNING_DEADLINE_TTL_SECS {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "venue {} quote deadline {} is {ttl_secs}s after receipt time {}, exceeding the {MAX_SIGNING_DEADLINE_TTL_SECS}s maximum signing TTL",
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
        // `ethers`'s `encode_eip712()` derives the domain separator from the
        // *parsed* `domain` struct above, independent of whatever `types`
        // declares for `EIP712Domain` (if that entry is even present — it is
        // optional per EIP-712). A signer consuming the raw typed-data JSON
        // this module validates may instead honor a declared `EIP712Domain`
        // schema, so a response that reorders its fields or changes a field
        // type there (while the parsed values above still pass) can hash a
        // payload the digest computed here does not represent. Validate the
        // declaration against the canonical domain fields whenever supplied,
        // matching exactly the fields this module already requires present
        // (name, chainId, verifyingContract; version/salt are rejected
        // above).
        if typed_data.types.contains_key(EIP712_DOMAIN_TYPE_NAME) {
            require_exact_eip712_fields(
                &typed_data.types,
                EIP712_DOMAIN_TYPE_NAME,
                EIP712_DOMAIN_FIELDS,
                &self.venue,
            )?;
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
        // The venue-specific checks below only compare `spender` against
        // other fields in this same (attacker-influenceable) response; none
        // of them independently confirm it names a real venue settlement
        // contract. Since the Permit2 signature authorizes this address to
        // pull the sell token, a compromised or misconfigured router could
        // otherwise substitute an unrelated contract and still pass. Refuse
        // to treat any quote as validated unless the deployer has configured
        // at least one trusted spender for this chain. Checked per venue,
        // not against one shared set: a response labeled e.g. "arcus" must
        // use the Arcus deployment's own spender, not an address only
        // trusted for a different venue such as Rialto, otherwise a
        // compromised or misconfigured router could mislabel a quote to
        // reuse another venue's trusted spender while still going through
        // this venue's (mismatched) settlement semantics.
        let venue_key = self.venue.to_ascii_lowercase();
        let trusted_for_venue = expected
            .trusted_spenders
            .get(&venue_key)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if !trusted_for_venue.contains(&spender) {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "venue {} EIP-712 spender {spender:#x} is not in the configured trusted_permit2_spenders allowlist for that venue",
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
            // LiFi's signed witness commits only to a hash of opaque diamond
            // calldata (see `canonical_witness_schema`'s doc comment); without
            // a maintained LiFi facet ABI to decode that calldata, this
            // module cannot verify its actual recipient, output token, or
            // minimum output, so it is refused like any other untrusted
            // venue rather than accepted on unsigned metadata alone.
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
        // recommended_quote() below returns the *first* match by venue name,
        // so two validated quotes sharing a venue would silently make that
        // selection (and this response's "recommended" flag) ambiguous —
        // round-trip and cost analysis could then use whichever one happens
        // to be first in the array, which is not a property the response
        // schema guarantees.
        let mut seen_venues: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for quote in &self.quotes {
            if !seen_venues.insert(quote.venue.to_ascii_lowercase()) {
                return Err(ArcusSpotError::InvalidResponse(format!(
                    "signable quote response has duplicate venue {:?}",
                    quote.venue
                )));
            }
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
    if forward.chain_id != reverse.chain_id {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "signable round-trip chain mismatch: forward {} != reverse {}",
            forward.chain_id, reverse.chain_id
        )));
    }
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
    // The forward intent delivers its purchased tokens to forward's taker; a
    // reverse intent requesting a different taker assumes ownership of funds
    // it never actually received, so the two legs cannot chain into one real
    // round trip even though their token directions and amounts line up.
    let forward_taker = parse_address("forward taker", &forward.request.taker)?;
    let reverse_taker = parse_address("reverse taker", &reverse.request.taker)?;
    if forward_taker != reverse_taker {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "signable round-trip taker mismatch: forward {forward_taker:#x} != reverse {reverse_taker:#x}"
        )));
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
        self.require_trusted_token_address(&sell)?;
        self.require_trusted_token_address(&buy)?;
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
        let trusted_spenders = self
            .inner
            .config
            .trusted_permit2_spenders
            .iter()
            .map(|(venue, addresses)| {
                let parsed = addresses
                    .iter()
                    .map(|address| parse_address("trusted_permit2_spenders entry", address))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| {
                        ArcusSpotError::InvalidConfig(format!(
                            "trusted_permit2_spenders is misconfigured for venue {venue}: {error}"
                        ))
                    })?;
                Ok((venue.to_ascii_lowercase(), parsed))
            })
            .collect::<Result<BTreeMap<_, _>, ArcusSpotError>>()?;
        response.payload.validate(&QuoteExpectations {
            chain_id: self.inner.config.chain_id,
            sell_token: sell_address,
            buy_token: buy_address,
            sell_amount,
            taker,
            slippage_bps: request.slippage_bps,
            route_policy: request.route_policy,
            now_unix,
            trusted_spenders,
        })?;
        Ok(ArcusSpotSignableQuoteObservation {
            chain_id: self.inner.config.chain_id,
            request: request.clone(),
            sell_token: sell,
            buy_token: buy,
            response,
        })
    }

    /// Refuse a router-supplied token address or decimals that are not
    /// independently pinned in `ArcusSpotConfig::trusted_token_addresses` /
    /// `trusted_token_decimals`.
    ///
    /// `verified_token` trusts the router's own `v1/tokens` list; if that
    /// router is compromised or misconfigured it could map a requested
    /// symbol to a different, valuable token address while still marking it
    /// `verified`. Since a signable quote's Permit2 signature authorizes
    /// pulling exactly the returned sell-token address, this independent
    /// source of truth must confirm both legs before that address is treated
    /// as signing evidence. The address pin alone does not protect
    /// `analyze()`'s quote_to_reference_deviation_bps: incorrect `decimals`
    /// on an otherwise correctly-pinned address still scales that
    /// comparison by a corresponding order of magnitude, so decimals are
    /// pinned and checked here too.
    fn require_trusted_token_address(&self, token: &ArcusSpotToken) -> Result<(), ArcusSpotError> {
        let key = normalize_symbol(&token.symbol);
        let trusted_raw = self
            .inner
            .config
            .trusted_token_addresses
            .get(&key)
            .ok_or_else(|| {
                ArcusSpotError::InvalidConfig(format!(
                    "trusted_token_addresses is not configured for symbol {key}; signable quotes cannot be trusted without a deployer-pinned address"
                ))
            })?;
        let trusted_address = parse_address("trusted_token_addresses entry", trusted_raw)?;
        let router_address = parse_address("token address", &token.address)?;
        if trusted_address != router_address {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "token {key} address {router_address:#x} from the router does not match the trusted_token_addresses pin {trusted_address:#x}"
            )));
        }
        let trusted_decimals = self
            .inner
            .config
            .trusted_token_decimals
            .get(&key)
            .ok_or_else(|| {
                ArcusSpotError::InvalidConfig(format!(
                    "trusted_token_decimals is not configured for symbol {key}; signable quotes cannot be trusted without a deployer-pinned decimals value"
                ))
            })?;
        if *trusted_decimals != token.decimals {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "token {key} decimals {} from the router does not match the trusted_token_decimals pin {trusted_decimals}",
                token.decimals
            )));
        }
        Ok(())
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
    /// Trusted Permit2 spenders keyed by lowercase venue name; see
    /// `ArcusSpotConfig::trusted_permit2_spenders`.
    trusted_spenders: BTreeMap<String, Vec<Address>>,
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

const EIP712_DOMAIN_TYPE_NAME: &str = "EIP712Domain";
const EIP712_DOMAIN_FIELDS: &[(&str, &str)] = &[
    ("name", "string"),
    ("chainId", "uint256"),
    ("verifyingContract", "address"),
];

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
        // LiFi is deliberately excluded: its witness commits only to a hash
        // of opaque diamond calldata, and without a maintained LiFi facet ABI
        // to decode that calldata, this module cannot verify the recipient,
        // output token, or minimum output it actually encodes. It is refused
        // like any other unrecognized venue rather than accepted on unsigned
        // metadata alone.
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

    const ARCUS_SPENDER: &str = "0x006102b16A04c20306A28b652745D3973D7D24fa";
    const RIALTO_SPENDER: &str = "0xc94135b63772b91d79d0a2daab2a8801f32359bd";

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
            trusted_spenders: BTreeMap::from([
                (
                    "arcus".to_string(),
                    vec![parse_address("arcus spender", ARCUS_SPENDER).unwrap()],
                ),
                (
                    "rialto".to_string(),
                    vec![parse_address("rialto spender", RIALTO_SPENDER).unwrap()],
                ),
            ]),
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
        assert_eq!(response.quotes.len(), 2);
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
        assert_eq!(analysis.venues.len(), 2);
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
    fn untrusted_spender_is_rejected_even_when_internally_consistent() {
        // The router substitutes an unrelated (but otherwise consistently
        // echoed) spender address for the arcus venue. Nothing else in the
        // response contradicts it, so only an independent allowlist can
        // catch this.
        let mut response: ArcusSpotSignableQuoteResponse =
            serde_json::from_str(QUOTE_FIXTURE).unwrap();
        let rogue = "0x0000000000000000000000000000000000000bad";
        response.quotes[0].to_sign["message"]["spender"] = Value::String(rogue.to_string());
        let error = response.validate(&fixture_expectations()).unwrap_err();
        assert!(
            error.to_string().contains("trusted_permit2_spenders"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn spender_trusted_for_a_different_venue_is_rejected() {
        // The router labels the quote "arcus" but supplies Rialto's own
        // trusted spender address. A shared, venue-blind allowlist would
        // accept this since the address is trusted for *some* venue; it
        // must only be accepted for the venue it actually belongs to.
        let mut response: ArcusSpotSignableQuoteResponse =
            serde_json::from_str(QUOTE_FIXTURE).unwrap();
        assert_eq!(response.quotes[0].venue, "arcus");
        response.quotes[0].to_sign["message"]["spender"] =
            Value::String(RIALTO_SPENDER.to_string());
        let error = response.validate(&fixture_expectations()).unwrap_err();
        assert!(
            error.to_string().contains("trusted_permit2_spenders"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn duplicate_venue_quotes_are_rejected() {
        // recommended_quote() returns the *first* array match by venue
        // name, so two validated quotes sharing a venue would make that
        // selection ambiguous depending on array order.
        let mut response: ArcusSpotSignableQuoteResponse =
            serde_json::from_str(QUOTE_FIXTURE).unwrap();
        let duplicate = response.quotes[0].clone();
        response.quotes.push(duplicate);
        let error = response.validate(&fixture_expectations()).unwrap_err();
        assert!(
            error.to_string().contains("duplicate venue"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn empty_trusted_spender_allowlist_rejects_every_quote() {
        let response: ArcusSpotSignableQuoteResponse = serde_json::from_str(QUOTE_FIXTURE).unwrap();
        let mut expected = fixture_expectations();
        expected.trusted_spenders.clear();
        let error = response.validate(&expected).unwrap_err();
        assert!(
            error.to_string().contains("trusted_permit2_spenders"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn wrong_permit2_binding_is_rejected() {
        let mut response: ArcusSpotSignableQuoteResponse =
            serde_json::from_str(QUOTE_FIXTURE).unwrap();
        response.quotes[0].to_sign["domain"]["verifyingContract"] =
            Value::String("0x0000000000000000000000000000000000000001".to_string());
        assert!(response
            .validate(&fixture_expectations())
            .unwrap_err()
            .to_string()
            .contains("is not Permit2"));
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
    fn declared_eip712_domain_schema_mismatch_is_rejected() {
        let mut response: ArcusSpotSignableQuoteResponse =
            serde_json::from_str(QUOTE_FIXTURE).unwrap();
        let rialto = response
            .quotes
            .iter_mut()
            .find(|quote| quote.venue == "rialto")
            .unwrap();
        // The parsed `domain` struct (name/chainId/verifyingContract) is
        // unchanged, but the declared EIP712Domain type reorders two fields;
        // `encode_eip712()` would still hash the parsed struct, hiding a
        // digest mismatch a schema-honoring signer would produce.
        let domain_fields = rialto.to_sign["types"]["EIP712Domain"]
            .as_array_mut()
            .unwrap();
        domain_fields.swap(0, 1);
        let error = response.validate(&fixture_expectations()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("do not exactly match the canonical schema"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn lifi_venue_is_rejected_outright() {
        // LiFi's signed witness only commits to a hash of opaque diamond
        // calldata; without a maintained LiFi facet ABI to decode it, this
        // module cannot verify the actual recipient, output token, or
        // minimum output it encodes, so the venue is refused outright even
        // when every other check (Permit2 domain, EIP-712 schema, calldata
        // hash) would otherwise pass.
        let mut response: ArcusSpotSignableQuoteResponse =
            serde_json::from_str(QUOTE_FIXTURE).unwrap();
        let mut lifi = response.quotes[0].clone();
        lifi.venue = "lifi".to_string();
        response.quotes.push(lifi);
        let error = response.validate(&fixture_expectations()).unwrap_err();
        assert!(
            error.to_string().contains("no signable-quote validator"),
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
    fn excessively_long_lived_deadline_is_rejected() {
        // The deadline is not yet expired (receipt time is unchanged), but a
        // year-2100 deadline lets a signed-but-unexercised intent sit idle
        // and be exercised much later at a stale minimum price.
        let mut response: ArcusSpotSignableQuoteResponse =
            serde_json::from_str(QUOTE_FIXTURE).unwrap();
        response.quotes[0].expiry = Some(4_102_444_800);
        response.quotes[0].to_sign["message"]["deadline"] = Value::String("4102444800".to_string());
        response.quotes[0].to_sign["message"]["witness"]["deadline"] =
            Value::String("4102444800".to_string());
        let error = response.validate(&fixture_expectations()).unwrap_err();
        assert!(
            error.to_string().contains("maximum signing TTL"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn a_deadline_only_seconds_after_receipt_is_rejected() {
        // now_unix is truncated to whole seconds from the response's
        // receipt time, so a deadline only 2s past it can leave anywhere
        // from a few milliseconds to just under 2s of genuine remaining
        // lifetime — not enough to survive EIP-712 validation and signing.
        let mut response: ArcusSpotSignableQuoteResponse =
            serde_json::from_str(QUOTE_FIXTURE).unwrap();
        let near_deadline = fixture_expectations().now_unix + 2;
        response.quotes[0].expiry = Some(near_deadline);
        response.quotes[0].to_sign["message"]["deadline"] =
            Value::String(near_deadline.to_string());
        response.quotes[0].to_sign["message"]["witness"]["deadline"] =
            Value::String(near_deadline.to_string());
        let error = response.validate(&fixture_expectations()).unwrap_err();
        assert!(
            error.to_string().contains("minimum usable signing TTL"),
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

    /// QUOTE_FIXTURE's deadlines are fixed so tests can validate against the
    /// fixed `fixture_expectations().now_unix`. This client-path test instead
    /// runs against the real clock (its `now_unix` comes from the mock
    /// server response's actual receipt time), so its deadlines must be live
    /// (`now + a TTL within MAX_SIGNING_DEADLINE_TTL_SECS`) rather than fixed.
    fn quote_fixture_with_live_deadlines() -> String {
        let mut fixture: Value = serde_json::from_str(QUOTE_FIXTURE).unwrap();
        let deadline = Utc::now().timestamp() + 60;
        for quote in fixture["all"].as_array_mut().unwrap() {
            if let Some(expiry) = quote.get_mut("expiry") {
                *expiry = Value::from(deadline);
            }
            quote["toSign"]["message"]["deadline"] = Value::String(deadline.to_string());
            quote["toSign"]["message"]["witness"]["deadline"] = Value::String(deadline.to_string());
        }
        fixture.to_string()
    }

    #[tokio::test]
    async fn unpinned_token_symbol_is_rejected() {
        let (base_url, _requests, server) =
            spawn_quote_server(vec![TOKEN_FIXTURE.to_string()]).await;
        let client = ArcusSpotClient::new(super::super::ArcusSpotConfig {
            router_base_url: base_url.clone(),
            meta_base_url: base_url,
            min_request_interval_ms: 0,
            max_attempts: 1,
            trusted_permit2_spenders: BTreeMap::from([
                ("arcus".to_string(), vec![ARCUS_SPENDER.to_string()]),
                ("rialto".to_string(), vec![RIALTO_SPENDER.to_string()]),
            ]),
            // trusted_token_addresses left empty: the router's own token
            // list must not be trusted as signing evidence on its own.
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
        let error = client.signable_quote_by_symbol(&request).await.unwrap_err();
        assert!(
            error.to_string().contains("trusted_token_addresses"),
            "unexpected error: {error}"
        );
        // The server only ever received the /v1/tokens request; the pin
        // check must fail before a /v1/quote request is made.
        server.await.unwrap();
    }

    #[tokio::test]
    async fn router_token_address_mismatched_with_pin_is_rejected() {
        let (base_url, _requests, server) =
            spawn_quote_server(vec![TOKEN_FIXTURE.to_string()]).await;
        let wrong_address = "0x0000000000000000000000000000000000000bad";
        let client = ArcusSpotClient::new(super::super::ArcusSpotConfig {
            router_base_url: base_url.clone(),
            meta_base_url: base_url,
            min_request_interval_ms: 0,
            max_attempts: 1,
            trusted_permit2_spenders: BTreeMap::from([
                ("arcus".to_string(), vec![ARCUS_SPENDER.to_string()]),
                ("rialto".to_string(), vec![RIALTO_SPENDER.to_string()]),
            ]),
            trusted_token_addresses: BTreeMap::from([
                ("NVDA".to_string(), wrong_address.to_string()),
                ("AMD".to_string(), BUY_TOKEN.to_string()),
            ]),
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
        let error = client.signable_quote_by_symbol(&request).await.unwrap_err();
        assert!(
            error.to_string().contains("trusted_token_addresses pin"),
            "unexpected error: {error}"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn router_token_decimals_mismatched_with_pin_is_rejected() {
        // The address pin matches, but the router reports 18 decimals for
        // NVDA while only 6 is pinned as trusted: an address pin alone does
        // not protect quote_to_reference_deviation_bps from an incorrect
        // decimals value on that same, correctly-pinned address.
        let (base_url, _requests, server) =
            spawn_quote_server(vec![TOKEN_FIXTURE.to_string()]).await;
        let client = ArcusSpotClient::new(super::super::ArcusSpotConfig {
            router_base_url: base_url.clone(),
            meta_base_url: base_url,
            min_request_interval_ms: 0,
            max_attempts: 1,
            trusted_permit2_spenders: BTreeMap::from([
                ("arcus".to_string(), vec![ARCUS_SPENDER.to_string()]),
                ("rialto".to_string(), vec![RIALTO_SPENDER.to_string()]),
            ]),
            trusted_token_addresses: BTreeMap::from([
                ("NVDA".to_string(), SELL_TOKEN.to_string()),
                ("AMD".to_string(), BUY_TOKEN.to_string()),
            ]),
            trusted_token_decimals: BTreeMap::from([
                ("NVDA".to_string(), 6),
                ("AMD".to_string(), 18),
            ]),
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
        let error = client.signable_quote_by_symbol(&request).await.unwrap_err();
        assert!(
            error.to_string().contains("trusted_token_decimals pin"),
            "unexpected error: {error}"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_uses_get_with_every_pre_sign_safety_parameter() {
        let (base_url, requests, server) = spawn_quote_server(vec![
            TOKEN_FIXTURE.to_string(),
            quote_fixture_with_live_deadlines(),
        ])
        .await;
        let client = ArcusSpotClient::new(super::super::ArcusSpotConfig {
            router_base_url: base_url.clone(),
            meta_base_url: base_url,
            min_request_interval_ms: 0,
            max_attempts: 1,
            trusted_permit2_spenders: BTreeMap::from([
                ("arcus".to_string(), vec![ARCUS_SPENDER.to_string()]),
                ("rialto".to_string(), vec![RIALTO_SPENDER.to_string()]),
            ]),
            trusted_token_addresses: BTreeMap::from([
                ("NVDA".to_string(), SELL_TOKEN.to_string()),
                ("AMD".to_string(), BUY_TOKEN.to_string()),
            ]),
            trusted_token_decimals: BTreeMap::from([
                ("NVDA".to_string(), 18),
                ("AMD".to_string(), 18),
            ]),
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

    #[test]
    fn optimistic_round_trip_requires_the_same_taker_for_both_legs() {
        let forward = fixture_observation();
        let mut reverse = fixture_observation();
        std::mem::swap(&mut reverse.sell_token, &mut reverse.buy_token);
        reverse.request.sell_symbol = "AMD".to_string();
        reverse.request.buy_symbol = "NVDA".to_string();
        reverse.request.sell_amount = "990".to_string();
        reverse.request.taker = "0x7600000000000000000000000000000000000002".to_string();
        reverse.response.payload.recommended = "arcus".to_string();
        reverse.response.payload.quotes[0].buy_amount = "980".to_string();
        let error = analyze_signable_round_trip(&forward, &reverse).unwrap_err();
        assert!(
            error.to_string().contains("taker mismatch"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn optimistic_round_trip_requires_the_same_chain_for_both_legs() {
        let forward = fixture_observation();
        let mut reverse = fixture_observation();
        std::mem::swap(&mut reverse.sell_token, &mut reverse.buy_token);
        reverse.request.sell_symbol = "AMD".to_string();
        reverse.request.buy_symbol = "NVDA".to_string();
        reverse.request.sell_amount = "990".to_string();
        reverse.chain_id = forward.chain_id + 1;
        reverse.response.payload.recommended = "arcus".to_string();
        reverse.response.payload.quotes[0].buy_amount = "980".to_string();
        let error = analyze_signable_round_trip(&forward, &reverse).unwrap_err();
        assert!(
            error.to_string().contains("chain mismatch"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    #[ignore = "GET-only public Arcus pre-sign quote smoke; requires network and a no-key address"]
    async fn public_arcus_pre_sign_quote_smoke() {
        let taker = std::env::var("ARCUS_SPOT_TEST_TAKER")
            .expect("set ARCUS_SPOT_TEST_TAKER to a dedicated address with no key in this process");
        // ArcusSpotConfig::default() leaves trusted_permit2_spenders empty,
        // which now rejects every quote fail-closed; this smoke test needs
        // the real deployment spender addresses to reach its assertions.
        let trusted_permit2_spenders = std::env::var("ARCUS_SPOT_TEST_TRUSTED_PERMIT2_SPENDERS")
            .expect(
                "set ARCUS_SPOT_TEST_TRUSTED_PERMIT2_SPENDERS to a comma-separated list of \
                 venue:address pairs (e.g. arcus:0x...,rialto:0x...) naming the real per-chain \
                 Permit2 spender addresses for the venues under test",
            )
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .fold(std::collections::BTreeMap::new(), |mut map, entry| {
                let (venue, address) = entry.split_once(':').unwrap_or_else(|| {
                    panic!(
                        "ARCUS_SPOT_TEST_TRUSTED_PERMIT2_SPENDERS entry {entry:?} must be \
                         venue:address"
                    )
                });
                map.entry(venue.trim().to_ascii_lowercase())
                    .or_insert_with(Vec::new)
                    .push(address.trim().to_string());
                map
            });
        // trusted_token_addresses is left empty by ArcusSpotConfig::default(),
        // which now rejects every symbol fail-closed; this smoke test needs
        // the real deployment token addresses to reach its assertions.
        let trusted_token_addresses = std::env::var("ARCUS_SPOT_TEST_TRUSTED_TOKEN_ADDRESSES")
            .expect(
                "set ARCUS_SPOT_TEST_TRUSTED_TOKEN_ADDRESSES to a comma-separated list of \
                 symbol:address pairs (e.g. NVDA:0x...,AMD:0x...) naming the real per-chain \
                 token addresses for the symbols under test",
            )
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(|entry| {
                let (symbol, address) = entry.split_once(':').unwrap_or_else(|| {
                    panic!(
                        "ARCUS_SPOT_TEST_TRUSTED_TOKEN_ADDRESSES entry {entry:?} must be \
                         symbol:address"
                    )
                });
                (
                    symbol.trim().to_ascii_uppercase(),
                    address.trim().to_string(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        // trusted_token_decimals is left empty by ArcusSpotConfig::default(),
        // which now rejects every symbol fail-closed; this smoke test needs
        // the real deployment token decimals to reach its assertions.
        let trusted_token_decimals = std::env::var("ARCUS_SPOT_TEST_TRUSTED_TOKEN_DECIMALS")
            .expect(
                "set ARCUS_SPOT_TEST_TRUSTED_TOKEN_DECIMALS to a comma-separated list of \
                 symbol:decimals pairs (e.g. NVDA:18,AMD:18) naming the real per-chain \
                 token decimals for the symbols under test",
            )
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(|entry| {
                let (symbol, decimals) = entry.split_once(':').unwrap_or_else(|| {
                    panic!(
                        "ARCUS_SPOT_TEST_TRUSTED_TOKEN_DECIMALS entry {entry:?} must be \
                         symbol:decimals"
                    )
                });
                let decimals: u32 = decimals.trim().parse().unwrap_or_else(|error| {
                    panic!("ARCUS_SPOT_TEST_TRUSTED_TOKEN_DECIMALS entry {entry:?}: {error}")
                });
                (symbol.trim().to_ascii_uppercase(), decimals)
            })
            .collect::<BTreeMap<_, _>>();
        let client = ArcusSpotClient::new(super::super::ArcusSpotConfig {
            trusted_permit2_spenders,
            trusted_token_addresses,
            trusted_token_decimals,
            ..super::super::ArcusSpotConfig::default()
        })
        .unwrap();
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

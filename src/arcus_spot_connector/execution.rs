//! Explicitly gated Arcus Spot signing and execution primitives.
//!
//! POST /v1/submit is deliberately one-shot: this module never retries a
//! submission. A transport timeout, response-body failure, or malformed
//! success response is classified as UNKNOWN, because the router may have
//! accepted the signed intent before the client lost the response.

use super::{
    parse_raw_amount, ArcusSpotClient, ArcusSpotConfig, ArcusSpotError, ArcusSpotFailureClass,
    ArcusSpotObservation, ArcusSpotQuoteRoutePolicy, ArcusSpotSignableQuoteObservation,
    ArcusSpotSignableVenueQuote,
};
use chrono::Utc;
use ethers::{
    signers::Signer,
    types::{
        transaction::eip712::{Eip712, TypedData},
        Address, Signature, H256,
    },
};
use reqwest::header::RETRY_AFTER;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fmt::Display, str::FromStr, time::Instant};
use thiserror::Error;

const ARCUS_VENUE: &str = "arcus";
const MIN_EXECUTION_TTL_SECS: u64 = 5;

/// Exact-value EIP-2612 permit inputs obtained from trusted chain reads.
///
/// The value is intentionally absent: the builder always signs exactly the
/// quote sell amount, avoiding an unlimited Permit2 allowance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArcusSpotEip2612PermitContext {
    pub token_name: String,
    pub token_version: String,
    pub nonce: String,
    pub deadline: u64,
}

/// Router wire shape for an EIP-2612 authorization accompanying a quote.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArcusSpotPermitAuthorization {
    pub token: String,
    pub value: String,
    pub deadline: String,
    pub v: u64,
    pub r: String,
    pub s: String,
}

/// Official Arcus web-client wire shape for POST /v1/submit (Arcus venue).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ArcusSpotSignedQuoteSubmission {
    pub venue: String,
    pub chain_id: u64,
    pub taker: String,
    pub typed_data: Value,
    pub signature: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub permits: Vec<ArcusSpotPermitAuthorization>,
}

impl ArcusSpotSignedQuoteSubmission {
    /// Stable local identifier for the caller's durable execution ledger.
    pub fn payload_hash(&self) -> Result<String, ArcusSpotError> {
        let bytes = serde_json::to_vec(self).map_err(|error| {
            ArcusSpotError::InvalidResponse(format!(
                "could not serialize signed submission for hashing: {error}"
            ))
        })?;
        Ok(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
    }
}

/// Submit/status response shape used by the official Arcus web client.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ArcusSpotSwapStatus {
    pub venue: String,
    pub status: String,
    pub tx_hash: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub error_code: Option<String>,
    #[serde(default)]
    pub swap: Option<Value>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl ArcusSpotSwapStatus {
    pub fn is_confirmed(&self) -> bool {
        self.status.eq_ignore_ascii_case("confirmed")
    }

    pub fn is_failed(&self) -> bool {
        self.status.eq_ignore_ascii_case("failed")
    }

    pub fn is_unknown(&self) -> bool {
        self.status.eq_ignore_ascii_case("unknown")
    }

    fn validate(
        &self,
        expected_venue: &str,
        expected_tx_hash: Option<H256>,
    ) -> Result<(), ArcusSpotError> {
        if !self.venue.eq_ignore_ascii_case(expected_venue) {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "swap status venue {:?} does not match {:?}",
                self.venue, expected_venue
            )));
        }
        if self.status.trim().is_empty() {
            return Err(ArcusSpotError::InvalidResponse(
                "swap status is empty".to_string(),
            ));
        }
        let tx_hash = H256::from_str(&self.tx_hash).map_err(|error| {
            ArcusSpotError::InvalidResponse(format!(
                "swap status txHash {:?} is invalid: {error}",
                self.tx_hash
            ))
        })?;
        if let Some(expected) = expected_tx_hash {
            if tx_hash != expected {
                return Err(ArcusSpotError::InvalidResponse(format!(
                    "swap status txHash {tx_hash:#x} does not match requested id {expected:#x}"
                )));
            }
        }
        Ok(())
    }
}

/// A POST failure after dispatch is ambiguous by construction. No variant in
/// this error type is retryable.
#[derive(Debug, Error)]
pub enum ArcusSpotSubmitError {
    #[error("Arcus Spot submission preflight failed: {0}")]
    Preflight(#[from] ArcusSpotError),
    #[error("Arcus Spot submission was rejected by HTTP {status} from {endpoint}: {body}")]
    Rejected {
        endpoint: String,
        status: u16,
        body: String,
    },
    #[error(
        "Arcus Spot submission outcome is UNKNOWN for {endpoint} ({classification}): {detail}"
    )]
    Unknown {
        endpoint: String,
        classification: ArcusSpotFailureClass,
        detail: String,
    },
}

impl ArcusSpotSubmitError {
    pub fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown { .. })
    }
}

/// Sign the direct Arcus venue quote and, optionally, an exact-value EIP-2612
/// permit. Custody remains outside this connector, so S may be an AWS KMS
/// signer without exposing key material here.
pub async fn sign_arcus_spot_quote<S>(
    client: &ArcusSpotClient,
    observation: &ArcusSpotSignableQuoteObservation,
    signer: &S,
    permit: Option<&ArcusSpotEip2612PermitContext>,
) -> Result<ArcusSpotSignedQuoteSubmission, ArcusSpotError>
where
    S: Signer + Sync,
    S::Error: Display,
{
    client.revalidate_arcus_signable_observation(observation)?;
    let (quote, taker, typed_data) = execution_quote(observation)?;
    if signer.address() != taker {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "signer address {:#x} does not match quote taker {taker:#x}",
            signer.address()
        )));
    }

    require_execution_ttl(quote)?;
    let signature = signer.sign_typed_data(&typed_data).await.map_err(|error| {
        ArcusSpotError::InvalidResponse(format!("Arcus quote signing failed: {error}"))
    })?;
    verify_signature(&typed_data, &signature, taker, "quote")?;

    let permits = match permit {
        Some(context) => {
            vec![sign_exact_permit(observation, quote, signer, taker, context).await?]
        }
        None => Vec::new(),
    };
    Ok(ArcusSpotSignedQuoteSubmission {
        venue: ARCUS_VENUE.to_string(),
        chain_id: observation.chain_id,
        taker: format!("{taker:#x}"),
        typed_data: serde_json::to_value(typed_data).map_err(|error| {
            ArcusSpotError::InvalidResponse(format!(
                "could not serialize validated quote typed data: {error}"
            ))
        })?,
        signature: signature.to_string(),
        permits,
    })
}

async fn sign_exact_permit<S>(
    observation: &ArcusSpotSignableQuoteObservation,
    quote: &ArcusSpotSignableVenueQuote,
    signer: &S,
    taker: Address,
    context: &ArcusSpotEip2612PermitContext,
) -> Result<ArcusSpotPermitAuthorization, ArcusSpotError>
where
    S: Signer + Sync,
    S::Error: Display,
{
    if context.token_name.trim().is_empty() || context.token_version.trim().is_empty() {
        return Err(ArcusSpotError::InvalidResponse(
            "EIP-2612 token name/version must not be empty".to_string(),
        ));
    }
    let nonce = parse_raw_amount("permit nonce", &context.nonce)?;
    let quote_deadline = quote.expires_at()?;
    if context.deadline > quote_deadline {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "permit deadline {} exceeds quote deadline {quote_deadline}",
            context.deadline
        )));
    }
    require_deadline_ttl("EIP-2612 permit", context.deadline)?;

    let token = Address::from_str(&observation.sell_token.address).map_err(|error| {
        ArcusSpotError::InvalidResponse(format!("invalid sell-token address: {error}"))
    })?;
    let quote_typed_data = quote.typed_data()?;
    let permit2 = quote_typed_data.domain.verifying_contract.ok_or_else(|| {
        ArcusSpotError::InvalidResponse("quote omitted Permit2 verifyingContract".to_string())
    })?;
    let amount = parse_raw_amount("sellAmount", &observation.request.sell_amount)?;
    let permit_typed_data: TypedData = serde_json::from_value(json!({
        "domain": {
            "name": context.token_name,
            "version": context.token_version,
            "chainId": observation.chain_id,
            "verifyingContract": format!("{token:#x}")
        },
        "types": {
            "Permit": [
                {"name": "owner", "type": "address"},
                {"name": "spender", "type": "address"},
                {"name": "value", "type": "uint256"},
                {"name": "nonce", "type": "uint256"},
                {"name": "deadline", "type": "uint256"}
            ]
        },
        "primaryType": "Permit",
        "message": {
            "owner": format!("{taker:#x}"),
            "spender": format!("{permit2:#x}"),
            "value": amount.to_string(),
            "nonce": nonce.to_string(),
            "deadline": context.deadline.to_string()
        }
    }))
    .map_err(|error| {
        ArcusSpotError::InvalidResponse(format!(
            "could not build EIP-2612 permit typed data: {error}"
        ))
    })?;
    let signature = signer
        .sign_typed_data(&permit_typed_data)
        .await
        .map_err(|error| {
            ArcusSpotError::InvalidResponse(format!("EIP-2612 permit signing failed: {error}"))
        })?;
    verify_signature(&permit_typed_data, &signature, taker, "permit")?;
    let normalized_v = match signature.v {
        0 | 1 => signature.v + 27,
        27 | 28 => signature.v,
        other => {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "unsupported permit recovery byte {other}"
            )))
        }
    };
    Ok(ArcusSpotPermitAuthorization {
        token: format!("{token:#x}"),
        value: amount.to_string(),
        deadline: context.deadline.to_string(),
        v: normalized_v,
        r: format!("{:#066x}", signature.r),
        s: format!("{:#066x}", signature.s),
    })
}

fn execution_quote(
    observation: &ArcusSpotSignableQuoteObservation,
) -> Result<(&ArcusSpotSignableVenueQuote, Address, TypedData), ArcusSpotError> {
    if observation.request.route_policy != ArcusSpotQuoteRoutePolicy::DirectTokenOnly {
        return Err(ArcusSpotError::InvalidResponse(
            "live execution requires direct_token_only (allowWrapped=false)".to_string(),
        ));
    }
    let mut quotes = observation
        .response
        .payload
        .quotes
        .iter()
        .filter(|quote| quote.venue.eq_ignore_ascii_case(ARCUS_VENUE));
    let quote = quotes.next().ok_or_else(|| {
        ArcusSpotError::InvalidResponse("validated response has no Arcus venue quote".to_string())
    })?;
    if quotes.next().is_some() {
        return Err(ArcusSpotError::InvalidResponse(
            "validated response has duplicate Arcus venue quotes".to_string(),
        ));
    }
    let taker = Address::from_str(&observation.request.taker).map_err(|error| {
        ArcusSpotError::InvalidResponse(format!("invalid quote taker: {error}"))
    })?;
    if quote
        .to_sign
        .pointer("/message/witness/allowWrapped")
        .and_then(Value::as_bool)
        != Some(false)
    {
        return Err(ArcusSpotError::InvalidResponse(
            "Arcus quote must sign allowWrapped=false".to_string(),
        ));
    }
    Ok((quote, taker, quote.typed_data()?))
}

fn require_execution_ttl(quote: &ArcusSpotSignableVenueQuote) -> Result<(), ArcusSpotError> {
    require_deadline_ttl("Arcus quote", quote.expires_at()?)
}

fn require_deadline_ttl(label: &str, deadline: u64) -> Result<(), ArcusSpotError> {
    let now = u64::try_from(Utc::now().timestamp()).map_err(|_| {
        ArcusSpotError::InvalidResponse("current time precedes Unix epoch".to_string())
    })?;
    if deadline.saturating_sub(now) < MIN_EXECUTION_TTL_SECS {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "{label} has insufficient execution TTL: deadline={deadline}, now={now}"
        )));
    }
    Ok(())
}

fn verify_signature(
    typed_data: &TypedData,
    signature: &Signature,
    expected: Address,
    label: &str,
) -> Result<(), ArcusSpotError> {
    let digest = typed_data.encode_eip712().map_err(|error| {
        ArcusSpotError::InvalidResponse(format!("could not hash {label} typed data: {error}"))
    })?;
    let recovered = signature.recover(H256::from(digest)).map_err(|error| {
        ArcusSpotError::InvalidResponse(format!("could not recover {label} signer: {error}"))
    })?;
    if recovered != expected {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "{label} signature recovered {recovered:#x}, expected {expected:#x}"
        )));
    }
    Ok(())
}

fn is_definitive_submit_rejection(status: u16) -> bool {
    matches!(status, 400 | 401 | 403 | 404 | 405 | 413 | 415 | 422)
}

impl ArcusSpotClient {
    /// Dispatch one signed submission exactly once. Never retries.
    pub async fn submit_signed_quote_once(
        &self,
        submission: &ArcusSpotSignedQuoteSubmission,
    ) -> Result<ArcusSpotObservation<ArcusSpotSwapStatus>, ArcusSpotSubmitError> {
        validate_submission(submission, &self.inner.config)?;
        let endpoint = self
            .inner
            .router_base_url
            .join("v1/submit")
            .map_err(|error| {
                ArcusSpotError::InvalidConfig(format!("invalid submit endpoint: {error}"))
            })?;
        self.pace().await;
        // The shared pacing gate may outlive a short quote. Re-run the full
        // pre-dispatch validation after waiting and immediately before POST.
        validate_submission(submission, &self.inner.config)?;
        let requested_at = Utc::now();
        let started = Instant::now();
        let response = self
            .inner
            .submit_http
            .post(endpoint.clone())
            .json(submission)
            .send()
            .await
            .map_err(|source| ArcusSpotSubmitError::Unknown {
                endpoint: endpoint.to_string(),
                classification: if source.is_timeout() {
                    ArcusSpotFailureClass::Timeout
                } else {
                    ArcusSpotFailureClass::Transport
                },
                detail: source.to_string(),
            })?;
        if let Some(delay) = super::retry_after_duration(
            response.headers().get(RETRY_AFTER),
            std::time::SystemTime::now(),
        ) {
            self.record_retry_after(delay).await;
        }
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|source| ArcusSpotSubmitError::Unknown {
                endpoint: endpoint.to_string(),
                classification: if source.is_timeout() {
                    ArcusSpotFailureClass::Timeout
                } else {
                    ArcusSpotFailureClass::Transport
                },
                detail: format!(
                    "response body unavailable after HTTP {}: {source}",
                    status.as_u16()
                ),
            })?;
        if !status.is_success() {
            let status_code = status.as_u16();
            let body = super::truncated_body(&body);
            if is_definitive_submit_rejection(status_code) {
                return Err(ArcusSpotSubmitError::Rejected {
                    endpoint: endpoint.to_string(),
                    status: status_code,
                    body,
                });
            }
            return Err(ArcusSpotSubmitError::Unknown {
                endpoint: endpoint.to_string(),
                classification: ArcusSpotFailureClass::Http,
                detail: format!(
                    "HTTP {status_code} did not prove pre-acceptance rejection: {body}"
                ),
            });
        }
        let payload: ArcusSpotSwapStatus =
            serde_json::from_slice(&body).map_err(|error| ArcusSpotSubmitError::Unknown {
                endpoint: endpoint.to_string(),
                classification: ArcusSpotFailureClass::InvalidJson,
                detail: error.to_string(),
            })?;
        payload
            .validate(ARCUS_VENUE, None)
            .map_err(|error| ArcusSpotSubmitError::Unknown {
                endpoint: endpoint.to_string(),
                classification: ArcusSpotFailureClass::ResponseValidation,
                detail: error.to_string(),
            })?;
        Ok(ArcusSpotObservation {
            payload,
            requested_at,
            received_at: Utc::now(),
            latency_ms: super::duration_millis_u64(started.elapsed()),
            attempts: 1,
        })
    }

    /// Poll canonical router status. GET retries remain safe.
    pub async fn swap_status(
        &self,
        venue: &str,
        tx_hash: H256,
    ) -> Result<ArcusSpotObservation<ArcusSpotSwapStatus>, ArcusSpotError> {
        if !venue.eq_ignore_ascii_case(ARCUS_VENUE) {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "initial live probe only supports Arcus venue status, got {venue:?}"
            )));
        }
        let query = vec![
            ("venue", ARCUS_VENUE.to_string()),
            ("id", format!("{tx_hash:#x}")),
            ("chainId", self.inner.config.chain_id.to_string()),
        ];
        let response: ArcusSpotObservation<ArcusSpotSwapStatus> = self
            .get_json(&self.inner.router_base_url, "v1/status", &query)
            .await?;
        response.payload.validate(ARCUS_VENUE, Some(tx_hash))?;
        Ok(response)
    }
}

fn validate_submission(
    submission: &ArcusSpotSignedQuoteSubmission,
    config: &ArcusSpotConfig,
) -> Result<(), ArcusSpotError> {
    let expected_chain_id = config.chain_id;
    if !submission.venue.eq_ignore_ascii_case(ARCUS_VENUE) {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "initial live probe only supports venue=arcus, got {:?}",
            submission.venue
        )));
    }
    if submission.chain_id != expected_chain_id {
        return Err(ArcusSpotError::InvalidResponse(format!(
            "submission chainId {} does not match configured {expected_chain_id}",
            submission.chain_id
        )));
    }
    let taker = Address::from_str(&submission.taker).map_err(|error| {
        ArcusSpotError::InvalidResponse(format!("submission taker is invalid: {error}"))
    })?;
    let typed_data: TypedData =
        serde_json::from_value(submission.typed_data.clone()).map_err(|error| {
            ArcusSpotError::InvalidResponse(format!("submission typedData is invalid: {error}"))
        })?;
    super::signable_quote::validate_arcus_typed_data_schema(&typed_data)?;
    if typed_data.domain.chain_id != Some(expected_chain_id.into()) {
        return Err(ArcusSpotError::InvalidResponse(
            "submission typedData chainId mismatch".to_string(),
        ));
    }
    if submission
        .typed_data
        .pointer("/message/witness/taker")
        .and_then(Value::as_str)
        .and_then(|raw| Address::from_str(raw).ok())
        != Some(taker)
    {
        return Err(ArcusSpotError::InvalidResponse(
            "submission typedData witness.taker mismatch".to_string(),
        ));
    }
    if submission
        .typed_data
        .pointer("/message/witness/allowWrapped")
        .and_then(Value::as_bool)
        != Some(false)
    {
        return Err(ArcusSpotError::InvalidResponse(
            "submission must sign allowWrapped=false".to_string(),
        ));
    }
    let permitted_token = submission_address(
        &submission.typed_data,
        "/message/permitted/token",
        "permitted token",
    )?;
    let witness_sell_token = submission_address(
        &submission.typed_data,
        "/message/witness/takerSellToken",
        "witness sell token",
    )?;
    let witness_buy_token = submission_address(
        &submission.typed_data,
        "/message/witness/takerBuyToken",
        "witness buy token",
    )?;
    if permitted_token == Address::zero()
        || witness_buy_token == Address::zero()
        || permitted_token != witness_sell_token
        || permitted_token == witness_buy_token
    {
        return Err(ArcusSpotError::InvalidResponse(
            "submission token bindings are invalid".to_string(),
        ));
    }
    require_trusted_submission_token(config, permitted_token)?;
    require_trusted_submission_token(config, witness_buy_token)?;

    let permitted_amount = submission_u256(
        &submission.typed_data,
        "/message/permitted/amount",
        "permitted amount",
    )?;
    let witness_sell_amount = submission_u256(
        &submission.typed_data,
        "/message/witness/sellAmount",
        "witness sell amount",
    )?;
    let minimum_buy_amount = submission_u256(
        &submission.typed_data,
        "/message/witness/minBuyAmount",
        "minimum buy amount",
    )?;
    if permitted_amount.is_zero()
        || minimum_buy_amount.is_zero()
        || permitted_amount != witness_sell_amount
    {
        return Err(ArcusSpotError::InvalidResponse(
            "submission amount bindings are invalid".to_string(),
        ));
    }

    let spender = submission_address(
        &submission.typed_data,
        "/message/spender",
        "Permit2 spender",
    )?;
    require_trusted_submission_spender(config, spender)?;
    let nonce = submission_u256(&submission.typed_data, "/message/nonce", "Permit2 nonce")?;
    let witness_nonce = submission_u256(
        &submission.typed_data,
        "/message/witness/nonce",
        "witness nonce",
    )?;
    if nonce != witness_nonce {
        return Err(ArcusSpotError::InvalidResponse(
            "submission nonce bindings are invalid".to_string(),
        ));
    }
    let deadline = submission_u64(
        &submission.typed_data,
        "/message/deadline",
        "submission deadline",
    )?;
    let witness_deadline = submission_u64(
        &submission.typed_data,
        "/message/witness/deadline",
        "witness deadline",
    )?;
    if deadline != witness_deadline {
        return Err(ArcusSpotError::InvalidResponse(
            "submission deadline bindings are invalid".to_string(),
        ));
    }
    require_deadline_ttl("submission", deadline)?;
    validate_submission_permit(submission, permitted_token, permitted_amount, deadline)?;
    let signature = Signature::from_str(&submission.signature).map_err(|error| {
        ArcusSpotError::InvalidResponse(format!("submission signature is invalid: {error}"))
    })?;
    verify_signature(&typed_data, &signature, taker, "submission")
}

fn submission_address(
    payload: &Value,
    pointer: &str,
    label: &str,
) -> Result<Address, ArcusSpotError> {
    let raw = payload
        .pointer(pointer)
        .and_then(Value::as_str)
        .ok_or_else(|| ArcusSpotError::InvalidResponse(format!("submission omitted {label}")))?;
    Address::from_str(raw).map_err(|error| {
        ArcusSpotError::InvalidResponse(format!("submission {label} is invalid: {error}"))
    })
}

fn submission_u256(
    payload: &Value,
    pointer: &str,
    label: &str,
) -> Result<ethers::types::U256, ArcusSpotError> {
    let value = payload
        .pointer(pointer)
        .ok_or_else(|| ArcusSpotError::InvalidResponse(format!("submission omitted {label}")))?;
    let raw = match value {
        Value::String(raw) => raw.clone(),
        Value::Number(raw) => raw.to_string(),
        _ => {
            return Err(ArcusSpotError::InvalidResponse(format!(
                "submission {label} must be an integer"
            )))
        }
    };
    ethers::types::U256::from_dec_str(&raw).map_err(|error| {
        ArcusSpotError::InvalidResponse(format!("submission {label} is invalid: {error}"))
    })
}

fn submission_u64(payload: &Value, pointer: &str, label: &str) -> Result<u64, ArcusSpotError> {
    let value = payload
        .pointer(pointer)
        .ok_or_else(|| ArcusSpotError::InvalidResponse(format!("submission omitted {label}")))?;
    match value {
        Value::String(raw) => raw.parse::<u64>().map_err(|error| {
            ArcusSpotError::InvalidResponse(format!("submission {label} is invalid: {error}"))
        }),
        Value::Number(raw) => raw.as_u64().ok_or_else(|| {
            ArcusSpotError::InvalidResponse(format!("submission {label} is not a non-negative u64"))
        }),
        _ => Err(ArcusSpotError::InvalidResponse(format!(
            "submission {label} must be an integer"
        ))),
    }
}

fn require_trusted_submission_token(
    config: &ArcusSpotConfig,
    token: Address,
) -> Result<(), ArcusSpotError> {
    if config.trusted_token_addresses.is_empty() {
        return Err(ArcusSpotError::InvalidConfig(
            "trusted_token_addresses is empty for submission".to_string(),
        ));
    }
    for raw in config.trusted_token_addresses.values() {
        let trusted = Address::from_str(raw).map_err(|error| {
            ArcusSpotError::InvalidConfig(format!(
                "trusted_token_addresses contains invalid address {raw:?}: {error}"
            ))
        })?;
        if trusted == token {
            return Ok(());
        }
    }
    Err(ArcusSpotError::InvalidResponse(format!(
        "submission token {token:#x} is not independently pinned"
    )))
}

fn require_trusted_submission_spender(
    config: &ArcusSpotConfig,
    spender: Address,
) -> Result<(), ArcusSpotError> {
    let trusted = config
        .trusted_permit2_spenders
        .iter()
        .find(|(venue, _)| venue.eq_ignore_ascii_case(ARCUS_VENUE))
        .map(|(_, addresses)| addresses)
        .ok_or_else(|| {
            ArcusSpotError::InvalidConfig(
                "trusted_permit2_spenders has no Arcus entry for submission".to_string(),
            )
        })?;
    for raw in trusted {
        let address = Address::from_str(raw).map_err(|error| {
            ArcusSpotError::InvalidConfig(format!(
                "trusted Arcus spender {raw:?} is invalid: {error}"
            ))
        })?;
        if address == spender {
            return Ok(());
        }
    }
    Err(ArcusSpotError::InvalidResponse(format!(
        "submission spender {spender:#x} is not pinned for Arcus"
    )))
}

fn validate_submission_permit(
    submission: &ArcusSpotSignedQuoteSubmission,
    sell_token: Address,
    sell_amount: ethers::types::U256,
    quote_deadline: u64,
) -> Result<(), ArcusSpotError> {
    if submission.permits.len() > 1 {
        return Err(ArcusSpotError::InvalidResponse(
            "submission may contain at most one exact-value permit".to_string(),
        ));
    }
    let Some(permit) = submission.permits.first() else {
        return Ok(());
    };
    let permit_token = Address::from_str(&permit.token).map_err(|error| {
        ArcusSpotError::InvalidResponse(format!("permit token is invalid: {error}"))
    })?;
    let permit_value = ethers::types::U256::from_dec_str(&permit.value).map_err(|error| {
        ArcusSpotError::InvalidResponse(format!("permit value is invalid: {error}"))
    })?;
    let permit_deadline = permit.deadline.parse::<u64>().map_err(|error| {
        ArcusSpotError::InvalidResponse(format!("permit deadline is invalid: {error}"))
    })?;
    if permit_token != sell_token || permit_value != sell_amount || permit_deadline > quote_deadline
    {
        return Err(ArcusSpotError::InvalidResponse(
            "permit is not bound to the exact submission token, amount, and deadline".to_string(),
        ));
    }
    require_deadline_ttl("submission permit", permit_deadline)?;
    if !matches!(permit.v, 27 | 28) {
        return Err(ArcusSpotError::InvalidResponse(
            "permit recovery byte must be 27 or 28".to_string(),
        ));
    }
    let r = H256::from_str(&permit.r).map_err(|error| {
        ArcusSpotError::InvalidResponse(format!("permit r is invalid: {error}"))
    })?;
    let s = H256::from_str(&permit.s).map_err(|error| {
        ArcusSpotError::InvalidResponse(format!("permit s is invalid: {error}"))
    })?;
    if r == H256::zero() || s == H256::zero() {
        return Err(ArcusSpotError::InvalidResponse(
            "permit signature components must be non-zero".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arcus_spot_connector::ArcusSpotSignableQuoteResponse;
    use ethers::signers::LocalWallet;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        time::{timeout, Duration},
    };

    fn trusted_client(router_base_url: String) -> ArcusSpotClient {
        ArcusSpotClient::new(ArcusSpotConfig {
            router_base_url,
            request_timeout_ms: 1_000,
            min_request_interval_ms: 1,
            max_attempts: 5,
            trusted_permit2_spenders: BTreeMap::from([(
                ARCUS_VENUE.to_string(),
                vec!["0x006102b16A04c20306A28b652745D3973D7D24fa".to_string()],
            )]),
            trusted_token_addresses: BTreeMap::from([
                (
                    "NVDA".to_string(),
                    "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC".to_string(),
                ),
                (
                    "AMD".to_string(),
                    "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC".to_string(),
                ),
            ]),
            trusted_token_decimals: BTreeMap::from([
                ("NVDA".to_string(), 18),
                ("AMD".to_string(), 18),
            ]),
            ..ArcusSpotConfig::default()
        })
        .unwrap()
    }

    fn fresh_arcus_observation(taker: Address) -> ArcusSpotSignableQuoteObservation {
        fresh_arcus_observation_with_ttl(taker, 60)
    }

    fn fresh_arcus_observation_with_ttl(
        taker: Address,
        ttl_secs: u64,
    ) -> ArcusSpotSignableQuoteObservation {
        let now = Utc::now();
        let deadline = u64::try_from(now.timestamp()).unwrap() + ttl_secs;
        let mut response: ArcusSpotSignableQuoteResponse =
            serde_json::from_str(include_str!("fixtures/quote_nvda_amd.json")).unwrap();
        response
            .quotes
            .retain(|quote| quote.venue.eq_ignore_ascii_case(ARCUS_VENUE));
        response.recommended = ARCUS_VENUE.to_string();
        let quote = response.quotes.first_mut().unwrap();
        quote.expiry = Some(deadline);
        quote.to_sign["message"]["witness"]["taker"] = Value::String(format!("{taker:#x}"));
        quote.to_sign["message"]["deadline"] = Value::String(deadline.to_string());
        quote.to_sign["message"]["witness"]["deadline"] = Value::String(deadline.to_string());
        let tokens: Vec<crate::arcus_spot_connector::ArcusSpotToken> =
            serde_json::from_str(include_str!("fixtures/tokens_nvda_amd.json")).unwrap();
        ArcusSpotSignableQuoteObservation {
            chain_id: 4663,
            request: crate::arcus_spot_connector::ArcusSpotSignableQuoteRequest::new(
                "NVDA",
                "AMD",
                "1000",
                format!("{taker:#x}"),
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
                payload: response,
                requested_at: now,
                received_at: now,
                latency_ms: 1,
                attempts: 1,
            },
        }
    }

    #[tokio::test]
    async fn signing_revalidates_deserialized_observation_against_client_trust() {
        let wallet = LocalWallet::from_str(
            "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d",
        )
        .unwrap();
        let mut observation = fresh_arcus_observation(wallet.address());
        observation.response.payload.quotes[0].to_sign["domain"]["verifyingContract"] =
            Value::String("0x0000000000000000000000000000000000000001".to_string());
        let client = trusted_client("http://127.0.0.1:1/".to_string());

        let error = sign_arcus_spot_quote(&client, &observation, &wallet, None)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("is not Permit2"));
    }

    #[tokio::test]
    async fn submission_preflight_rejects_noncanonical_typed_data_schema() {
        let wallet = LocalWallet::from_str(
            "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d",
        )
        .unwrap();
        let client = trusted_client("http://127.0.0.1:1/".to_string());
        let observation = fresh_arcus_observation(wallet.address());
        let mut submission = sign_arcus_spot_quote(&client, &observation, &wallet, None)
            .await
            .unwrap();
        submission.typed_data["types"]["PermitWitnessTransferFrom"]
            .as_array_mut()
            .unwrap()
            .swap(0, 1);

        let error = validate_submission(&submission, client.config()).unwrap_err();

        assert!(error
            .to_string()
            .contains("do not exactly match the canonical schema"));
    }

    #[test]
    fn status_parses_official_unknown_shape() {
        let status: ArcusSpotSwapStatus = serde_json::from_str(
            r#"{"venue":"arcus","status":"unknown","txHash":"0x0000000000000000000000000000000000000000000000000000000000000000","reason":null,"errorCode":null,"swap":null}"#,
        )
        .unwrap();
        assert!(status.is_unknown());
        status.validate("arcus", Some(H256::zero())).unwrap();
    }

    #[test]
    fn payload_hash_is_stable() {
        let submission = ArcusSpotSignedQuoteSubmission {
            venue: "arcus".to_string(),
            chain_id: 4663,
            taker: format!("{:#x}", Address::zero()),
            typed_data: json!({"x": 1}),
            signature: "0x01".to_string(),
            permits: Vec::new(),
        };
        assert_eq!(
            submission.payload_hash().unwrap(),
            submission.payload_hash().unwrap()
        );
    }

    #[test]
    fn submit_error_marks_timeout_unknown() {
        let error = ArcusSpotSubmitError::Unknown {
            endpoint: "https://example.invalid/v1/submit".to_string(),
            classification: ArcusSpotFailureClass::Timeout,
            detail: "timeout".to_string(),
        };
        assert!(error.is_unknown());
    }

    #[test]
    fn status_requires_matching_hash() {
        let status = ArcusSpotSwapStatus {
            venue: "arcus".to_string(),
            status: "confirmed".to_string(),
            tx_hash: format!("{:#x}", H256::from_low_u64_be(1)),
            reason: None,
            error_code: None,
            swap: None,
            extra: BTreeMap::new(),
        };
        assert!(status
            .validate("arcus", Some(H256::from_low_u64_be(2)))
            .is_err());
    }
    #[tokio::test]
    async fn redirect_is_unknown_and_post_is_not_retried() {
        let wallet = LocalWallet::from_str(
            "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d",
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = trusted_client(format!("http://{address}/"));
        let observation = fresh_arcus_observation(wallet.address());
        let submission = sign_arcus_spot_quote(&client, &observation, &wallet, None)
            .await
            .unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 8_192];
            let bytes_read = socket.read(&mut request).await.unwrap();
            assert!(bytes_read > 0);
            let response = format!(
                "HTTP/1.1 307 Temporary Redirect\r\nlocation: http://{address}/redirected\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            assert!(
                timeout(Duration::from_millis(150), listener.accept())
                    .await
                    .is_err(),
                "submit path followed the redirect with a second POST"
            );
            1_u32
        });

        let error = client
            .submit_signed_quote_once(&submission)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ArcusSpotSubmitError::Unknown {
                classification: ArcusSpotFailureClass::Http,
                ..
            }
        ));
        assert_eq!(server.await.unwrap(), 1);
    }

    #[tokio::test]
    async fn pacing_wait_rechecks_deadline_before_post() {
        let wallet = LocalWallet::from_str(
            "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d",
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = trusted_client(format!("http://{address}/"));
        let observation = fresh_arcus_observation_with_ttl(wallet.address(), 6);
        let submission = sign_arcus_spot_quote(&client, &observation, &wallet, None)
            .await
            .unwrap();
        client
            .record_retry_after(Duration::from_millis(2_100))
            .await;

        let error = client
            .submit_signed_quote_once(&submission)
            .await
            .unwrap_err();

        assert!(matches!(error, ArcusSpotSubmitError::Preflight(_)));
        assert!(
            timeout(Duration::from_millis(150), listener.accept())
                .await
                .is_err(),
            "expired submission reached the POST endpoint"
        );
    }

    #[tokio::test]
    async fn server_error_is_unknown_and_post_is_not_retried() {
        let wallet = LocalWallet::from_str(
            "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d",
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = trusted_client(format!("http://{address}/"));
        let observation = fresh_arcus_observation(wallet.address());
        let submission = sign_arcus_spot_quote(&client, &observation, &wallet, None)
            .await
            .unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 8_192];
            let bytes_read = socket.read(&mut request).await.unwrap();
            assert!(bytes_read > 0);
            socket
                .write_all(
                    b"HTTP/1.1 500 Internal Server Error\r\ncontent-length: 4\r\nconnection: close\r\n\r\noops",
                )
                .await
                .unwrap();
            assert!(
                timeout(Duration::from_millis(150), listener.accept())
                    .await
                    .is_err(),
                "submit path opened a second connection"
            );
            1_u32
        });

        let error = client
            .submit_signed_quote_once(&submission)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ArcusSpotSubmitError::Unknown {
                classification: ArcusSpotFailureClass::Http,
                ..
            }
        ));
        assert_eq!(server.await.unwrap(), 1);
    }

    #[tokio::test]
    async fn malformed_success_is_unknown_and_post_is_not_retried() {
        let wallet = LocalWallet::from_str(
            "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d",
        )
        .unwrap();
        let deadline = u64::try_from(Utc::now().timestamp()).unwrap() + 60;
        let mut response: ArcusSpotSignableQuoteResponse =
            serde_json::from_str(include_str!("fixtures/quote_nvda_amd.json")).unwrap();
        let quote = response
            .quotes
            .iter_mut()
            .find(|quote| quote.venue.eq_ignore_ascii_case(ARCUS_VENUE))
            .unwrap();
        quote.to_sign["message"]["witness"]["taker"] =
            Value::String(format!("{:#x}", wallet.address()));
        quote.to_sign["message"]["deadline"] = Value::String(deadline.to_string());
        quote.to_sign["message"]["witness"]["deadline"] = Value::String(deadline.to_string());
        let typed_data: TypedData = serde_json::from_value(quote.to_sign.clone()).unwrap();
        let signature = wallet.sign_typed_data(&typed_data).await.unwrap();
        let submission = ArcusSpotSignedQuoteSubmission {
            venue: ARCUS_VENUE.to_string(),
            chain_id: 4663,
            taker: format!("{:#x}", wallet.address()),
            typed_data: serde_json::to_value(typed_data).unwrap(),
            signature: signature.to_string(),
            permits: Vec::new(),
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 8_192];
            let bytes_read = socket.read(&mut request).await.unwrap();
            assert!(bytes_read > 0);
            socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 1\r\nconnection: close\r\n\r\n{")
                .await
                .unwrap();
            assert!(
                timeout(Duration::from_millis(150), listener.accept())
                    .await
                    .is_err(),
                "submit path opened a second connection"
            );
            1_u32
        });

        let trusted_permit2_spenders = BTreeMap::from([(
            ARCUS_VENUE.to_string(),
            vec!["0x006102b16A04c20306A28b652745D3973D7D24fa".to_string()],
        )]);
        let trusted_token_addresses = BTreeMap::from([
            (
                "NVDA".to_string(),
                "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC".to_string(),
            ),
            (
                "AMD".to_string(),
                "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC".to_string(),
            ),
        ]);
        let config = ArcusSpotConfig {
            router_base_url: format!("http://{address}/"),
            request_timeout_ms: 1_000,
            min_request_interval_ms: 1,
            max_attempts: 5,
            trusted_permit2_spenders,
            trusted_token_addresses,
            ..ArcusSpotConfig::default()
        };
        let client = ArcusSpotClient::new(config).unwrap();
        let error = client
            .submit_signed_quote_once(&submission)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ArcusSpotSubmitError::Unknown {
                classification: ArcusSpotFailureClass::InvalidJson,
                ..
            }
        ));
        assert_eq!(server.await.unwrap(), 1);
    }
}

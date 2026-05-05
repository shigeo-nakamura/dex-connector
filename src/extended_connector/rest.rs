//! Extended REST plumbing: env (mainnet/testnet) URL + chain-ID
//! constants, the `WrappedApiResponse<T>` envelope, the retry loop
//! around transient 5xx / transport errors, and `build_query` for
//! constructing GET URLs with encoded params.

use crate::dex_request::{DexError, DexRequest, HttpMethod};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

pub(super) const MAINNET_API_BASE: &str = "https://api.starknet.extended.exchange/api/v1";
pub(super) const TESTNET_API_BASE: &str = "https://api.starknet.sepolia.extended.exchange/api/v1";

pub(super) const MAINNET_CHAIN_ID: &str = "SN_MAIN";
pub(super) const TESTNET_CHAIN_ID: &str = "SN_SEPOLIA";

/// Extended REST error code for "Maintenance mode" envelope (`{"status":
/// "ERROR","error":{"code":1010,"message":"Maintenance mode"}}`). Observed
/// during the 2026-05-05 window. See bot-strategy#321.
pub(super) const EXTENDED_MAINTENANCE_API_CODE: i64 = 1010;

/// Extended REST error code for "Post only mode" envelope (`{"status":
/// "ERROR","error":{"code":1011,"message":"Post only mode"}}`). Returned
/// during transient post-only-only windows (auction / reopen phase) where
/// the venue rejects aggressive IOC / non-post-only orders. Observed
/// 2026-05-05 17:15-17:16 UTC, ~25s window. See bot-strategy#327.
pub(super) const EXTENDED_POST_ONLY_API_CODE: i64 = 1011;

/// How long after the last observed code-1010 response we keep treating
/// the venue as in maintenance. Long enough for the next `/info/markets`
/// poll (60s cadence) to reconfirm via the symbol-status path; short
/// enough that we re-arm quickly once the window genuinely ends.
pub(super) const REST_1010_GRACE_SECS: i64 = 600;

/// How long after the last observed code-1011 response we keep treating
/// the venue as rejecting aggressive orders. Observed window in the wild
/// is ~25s; 60s gives one extra retry cycle of headroom while still
/// re-arming quickly. Folded into `is_upcoming_maintenance` so existing
/// pairtrade/xvenue-arb suppression wiring covers it. See bot-strategy#327.
pub(super) const REST_1011_GRACE_SECS: i64 = 60;

#[derive(Debug, Clone, Copy)]
pub(super) enum ExtendedEnvironment {
    Mainnet,
    Testnet,
}

impl ExtendedEnvironment {
    pub(super) fn chain_id(&self) -> &'static str {
        match self {
            ExtendedEnvironment::Mainnet => MAINNET_CHAIN_ID,
            ExtendedEnvironment::Testnet => TESTNET_CHAIN_ID,
        }
    }

    pub(super) fn api_base(&self) -> &'static str {
        match self {
            ExtendedEnvironment::Mainnet => MAINNET_API_BASE,
            ExtendedEnvironment::Testnet => TESTNET_API_BASE,
        }
    }
}

#[derive(Clone)]
pub(super) struct ExtendedApi {
    request: DexRequest,
    pub(super) api_key: String,
    /// Shared with `ExtendedConnector`. Bumped to `now + REST_1010_GRACE_SECS`
    /// every time a response carries code 1010, so the connector's
    /// `is_upcoming_maintenance` can short-circuit to true while the venue
    /// is rejecting requests. bot-strategy#321.
    rest_1010_clear_at: Arc<AtomicI64>,
    /// Shared with `ExtendedConnector`. Bumped to `now + REST_1011_GRACE_SECS`
    /// every time a response carries code 1011 ("Post only mode"). Folded
    /// into the same `is_upcoming_maintenance` predicate as 1010 so the
    /// existing suppression wiring covers transient post-only-only windows.
    /// bot-strategy#327.
    rest_1011_clear_at: Arc<AtomicI64>,
}

impl ExtendedApi {
    pub(super) async fn new(
        api_base: String,
        api_key: String,
        rest_1010_clear_at: Arc<AtomicI64>,
        rest_1011_clear_at: Arc<AtomicI64>,
    ) -> Result<Self, DexError> {
        Ok(Self {
            request: DexRequest::new(api_base).await?,
            api_key,
            rest_1010_clear_at,
            rest_1011_clear_at,
        })
    }

    pub(super) async fn get<T>(&self, path: String, authed: bool) -> Result<T, DexError>
    where
        T: serde::de::DeserializeOwned,
    {
        self.send(HttpMethod::Get, path, authed, None::<serde_json::Value>)
            .await
    }

    pub(super) async fn post<T, U>(
        &self,
        path: String,
        payload: U,
        authed: bool,
    ) -> Result<T, DexError>
    where
        T: serde::de::DeserializeOwned,
        U: Serialize,
    {
        self.send(HttpMethod::Post, path, authed, Some(payload))
            .await
    }

    pub(super) async fn delete<T>(&self, path: String, authed: bool) -> Result<T, DexError>
    where
        T: serde::de::DeserializeOwned,
    {
        self.send(HttpMethod::Delete, path, authed, None::<serde_json::Value>)
            .await
    }

    pub(super) async fn patch<T, U>(
        &self,
        path: String,
        payload: U,
        authed: bool,
    ) -> Result<T, DexError>
    where
        T: serde::de::DeserializeOwned,
        U: Serialize,
    {
        self.send(HttpMethod::Patch, path, authed, Some(payload))
            .await
    }

    async fn send<T, U>(
        &self,
        method: HttpMethod,
        path: String,
        authed: bool,
        payload: Option<U>,
    ) -> Result<T, DexError>
    where
        T: serde::de::DeserializeOwned,
        U: Serialize,
    {
        let mut headers = HashMap::new();
        if authed {
            headers.insert("X-Api-Key".to_string(), self.api_key.clone());
        }

        let json_payload = match payload {
            Some(body) => serde_json::to_string(&body)
                .map_err(|e| DexError::Other(format!("Failed to serialize payload: {}", e)))?,
            None => String::new(),
        };

        // Retry transient Extended 5xx (code 1006 "Internal Server Error") and
        // transport-level errors only for idempotent GETs. Extended's REST
        // sporadically returns 500 and typically recovers within a few seconds
        // (see bot-strategy#206). Non-GET verbs are never retried to avoid
        // double-execution of state-changing operations.
        let max_attempts: u32 = if matches!(method, HttpMethod::Get) {
            3
        } else {
            1
        };

        for attempt in 1..=max_attempts {
            let response_result: Result<WrappedApiResponse<T>, DexError> = self
                .request
                .handle_request::<WrappedApiResponse<T>, serde_json::Value>(
                    method,
                    path.clone(),
                    &headers,
                    json_payload.clone(),
                )
                .await;

            let response = match response_result {
                Ok(r) => r,
                Err(ref e) if attempt < max_attempts && is_transient_transport_err(e) => {
                    let backoff_ms = 500u64 * (1u64 << (attempt - 1));
                    log::warn!(
                        "[extended] transport retry {}/{} ({}ms) on {}: {}",
                        attempt,
                        max_attempts,
                        backoff_ms,
                        path,
                        e
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            };

            if response.status != ResponseStatus::Ok || response.error.is_some() {
                let code = response.error.as_ref().map(|err| err.code).unwrap_or(0);
                let message = response
                    .error
                    .as_ref()
                    .map(|err| err.message.clone())
                    .unwrap_or_else(|| "Extended API error".to_string());
                if code == EXTENDED_MAINTENANCE_API_CODE {
                    bump_rest_1010_clear_at(&self.rest_1010_clear_at, Utc::now().timestamp());
                } else if code == EXTENDED_POST_ONLY_API_CODE {
                    bump_rest_1011_clear_at(&self.rest_1011_clear_at, Utc::now().timestamp());
                }
                if attempt < max_attempts && is_transient_api_code(code) {
                    let backoff_ms = 500u64 * (1u64 << (attempt - 1));
                    log::warn!(
                        "[extended] api retry {}/{} ({}ms) on {} (code={}): {}",
                        attempt,
                        max_attempts,
                        backoff_ms,
                        path,
                        code,
                        message
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                return Err(DexError::ServerResponse(message));
            }

            return response
                .data
                .ok_or_else(|| DexError::Other("Extended API returned empty data".to_string()));
        }
        unreachable!("extended send retry loop exited without Ok/Err")
    }
}

/// Push the REST-1010 grace deadline to `now + REST_1010_GRACE_SECS`.
/// Uses `fetch_max` so a stale bump (clock drift between concurrent
/// requests) never shortens an already-extended deadline. bot-strategy#321.
fn bump_rest_1010_clear_at(clear_at: &AtomicI64, now_ts: i64) {
    let new_clear = now_ts.saturating_add(REST_1010_GRACE_SECS);
    clear_at.fetch_max(new_clear, Ordering::SeqCst);
}

/// Push the REST-1011 grace deadline to `now + REST_1011_GRACE_SECS`.
/// Same `fetch_max` semantics as the 1010 helper. bot-strategy#327.
fn bump_rest_1011_clear_at(clear_at: &AtomicI64, now_ts: i64) {
    let new_clear = now_ts.saturating_add(REST_1011_GRACE_SECS);
    clear_at.fetch_max(new_clear, Ordering::SeqCst);
}

/// True while the REST-1010 grace deadline has not yet passed.
#[cfg(test)]
fn rest_1010_active(clear_at: &AtomicI64, now_ts: i64) -> bool {
    clear_at.load(Ordering::SeqCst) > now_ts
}

/// True while the REST-1011 grace deadline has not yet passed.
#[cfg(test)]
fn rest_1011_active(clear_at: &AtomicI64, now_ts: i64) -> bool {
    clear_at.load(Ordering::SeqCst) > now_ts
}

/// Transport-level failures that may succeed on retry (connection reset,
/// DNS hiccup, TLS handshake blip). Reqwest groups all of these under
/// `DexError::Reqwest`.
fn is_transient_transport_err(err: &DexError) -> bool {
    matches!(err, DexError::Reqwest(_))
}

/// Extended API error codes that map to HTTP 5xx and are safe to retry.
/// Keep this list minimal — only codes we've observed resolve on retry.
fn is_transient_api_code(code: i64) -> bool {
    // 1006 = "Internal Server Error" (Extended's HTTP 500 envelope). Matches
    // what production saw on 2026-04-24 during the bot-strategy#207 window.
    code == 1006
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
enum ResponseStatus {
    Ok,
    Error,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
struct ResponseError {
    code: i64,
    message: String,
    debug_info: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
struct WrappedApiResponse<T> {
    status: ResponseStatus,
    data: Option<T>,
    error: Option<ResponseError>,
    pagination: Option<Pagination>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
struct Pagination {
    cursor: Option<i64>,
    count: i64,
}

pub(super) fn build_query(base: &str, params: Vec<(String, String)>) -> String {
    if params.is_empty() {
        return base.to_string();
    }
    let mut parts = Vec::new();
    for (key, value) in params {
        let encoded = format!(
            "{}={}",
            urlencoding::encode(&key),
            urlencoding::encode(&value)
        );
        parts.push(encoded);
    }
    format!("{}?{}", base, parts.join("&"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rest_1010_bump_arms_grace_window() {
        let clear_at = AtomicI64::new(0);
        bump_rest_1010_clear_at(&clear_at, 1_000);
        assert_eq!(clear_at.load(Ordering::SeqCst), 1_000 + REST_1010_GRACE_SECS);
        assert!(rest_1010_active(&clear_at, 1_500));
        assert!(rest_1010_active(&clear_at, 1_000 + REST_1010_GRACE_SECS - 1));
        // Boundary: at the deadline itself we're already clear.
        assert!(!rest_1010_active(&clear_at, 1_000 + REST_1010_GRACE_SECS));
        assert!(!rest_1010_active(&clear_at, 1_000 + REST_1010_GRACE_SECS + 60));
    }

    #[test]
    fn rest_1010_bump_extends_deadline_forward() {
        let clear_at = AtomicI64::new(0);
        bump_rest_1010_clear_at(&clear_at, 1_000);
        bump_rest_1010_clear_at(&clear_at, 1_300);
        // Second bump from a later wall clock pushes the deadline forward.
        assert_eq!(clear_at.load(Ordering::SeqCst), 1_300 + REST_1010_GRACE_SECS);
        // Active at any `now` strictly less than the new deadline.
        assert!(rest_1010_active(&clear_at, 1_300 + REST_1010_GRACE_SECS - 1));
        assert!(!rest_1010_active(&clear_at, 1_300 + REST_1010_GRACE_SECS));
    }

    #[test]
    fn rest_1010_bump_does_not_shrink_deadline() {
        let clear_at = AtomicI64::new(0);
        bump_rest_1010_clear_at(&clear_at, 1_000);
        let after_first = clear_at.load(Ordering::SeqCst);
        // A stale concurrent bump (e.g. with an earlier observed `now`)
        // must never pull the deadline backward.
        bump_rest_1010_clear_at(&clear_at, 500);
        assert_eq!(clear_at.load(Ordering::SeqCst), after_first);
    }

    #[test]
    fn rest_1010_inactive_when_never_bumped() {
        let clear_at = AtomicI64::new(0);
        assert!(!rest_1010_active(&clear_at, 0));
        assert!(!rest_1010_active(&clear_at, 1_000));
    }

    #[test]
    fn rest_1011_bump_arms_grace_window() {
        let clear_at = AtomicI64::new(0);
        bump_rest_1011_clear_at(&clear_at, 1_000);
        assert_eq!(clear_at.load(Ordering::SeqCst), 1_000 + REST_1011_GRACE_SECS);
        assert!(rest_1011_active(&clear_at, 1_030));
        assert!(rest_1011_active(&clear_at, 1_000 + REST_1011_GRACE_SECS - 1));
        assert!(!rest_1011_active(&clear_at, 1_000 + REST_1011_GRACE_SECS));
        assert!(!rest_1011_active(&clear_at, 1_000 + REST_1011_GRACE_SECS + 60));
    }

    #[test]
    fn rest_1011_bump_extends_deadline_forward() {
        let clear_at = AtomicI64::new(0);
        bump_rest_1011_clear_at(&clear_at, 1_000);
        bump_rest_1011_clear_at(&clear_at, 1_030);
        assert_eq!(clear_at.load(Ordering::SeqCst), 1_030 + REST_1011_GRACE_SECS);
        assert!(rest_1011_active(&clear_at, 1_030 + REST_1011_GRACE_SECS - 1));
        assert!(!rest_1011_active(&clear_at, 1_030 + REST_1011_GRACE_SECS));
    }

    #[test]
    fn rest_1011_bump_does_not_shrink_deadline() {
        let clear_at = AtomicI64::new(0);
        bump_rest_1011_clear_at(&clear_at, 1_000);
        let after_first = clear_at.load(Ordering::SeqCst);
        bump_rest_1011_clear_at(&clear_at, 500);
        assert_eq!(clear_at.load(Ordering::SeqCst), after_first);
    }

    #[test]
    fn rest_1011_inactive_when_never_bumped() {
        let clear_at = AtomicI64::new(0);
        assert!(!rest_1011_active(&clear_at, 0));
        assert!(!rest_1011_active(&clear_at, 1_000));
    }
}

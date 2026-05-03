//! Extended REST plumbing: env (mainnet/testnet) URL + chain-ID
//! constants, the `WrappedApiResponse<T>` envelope, the retry loop
//! around transient 5xx / transport errors, and `build_query` for
//! constructing GET URLs with encoded params.

use crate::dex_request::{DexError, DexRequest, HttpMethod};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub(super) const MAINNET_API_BASE: &str = "https://api.starknet.extended.exchange/api/v1";
pub(super) const TESTNET_API_BASE: &str = "https://api.starknet.sepolia.extended.exchange/api/v1";

pub(super) const MAINNET_CHAIN_ID: &str = "SN_MAIN";
pub(super) const TESTNET_CHAIN_ID: &str = "SN_SEPOLIA";

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
}

impl ExtendedApi {
    pub(super) async fn new(api_base: String, api_key: String) -> Result<Self, DexError> {
        Ok(Self {
            request: DexRequest::new(api_base).await?,
            api_key,
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

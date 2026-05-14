use debot_utils::ParseDecimalError;
use reqwest::{
    header::{HeaderMap, HeaderValue},
    Client, Method,
};
use serde::Serialize;
use std::{collections::HashMap, time::Duration};
use thiserror::Error;

#[derive(Clone, Copy)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl From<HttpMethod> for Method {
    fn from(method: HttpMethod) -> Self {
        match method {
            HttpMethod::Get => Method::GET,
            HttpMethod::Post => Method::POST,
            HttpMethod::Put => Method::PUT,
            HttpMethod::Patch => Method::PATCH,
            HttpMethod::Delete => Method::DELETE,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DexRequest {
    client: Client,
    endpoint: String,
}

/// Errors emitted by `DexConnector` implementations.
///
/// Variants are split so callers can choose retry policy without
/// regex-matching error strings (bot-strategy#389). The categories are:
///
/// - [`Transient`](Self::Transient) — retry with backoff.
/// - [`Permanent`](Self::Permanent) — surface; retry will not help.
/// - [`InvalidInput`](Self::InvalidInput) — caller-side validation failure.
/// - [`RateLimited`](Self::RateLimited) — do NOT retry until `until_unix`.
/// - Wrapped underlying errors ([`Serde`](Self::Serde),
///   [`Reqwest`](Self::Reqwest)) carry the original error for diagnostics.
#[derive(Debug, Error)]
pub enum DexError {
    #[error("Serde JSON error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),

    /// Venue returned an application-level rejection (typically a 4xx with a
    /// parsed business message such as Extended's 1136/1137 reduce-only codes).
    #[error("Server response error: {0}")]
    ServerResponse(String),

    #[error("WebSocket error: {0}")]
    WebSocketError(String),

    #[error("No running WebSocketConnection")]
    NoConnection,

    #[error("Network upgrade scheduled in < 2h")]
    UpcomingMaintenance,

    #[error("API key registration is required")]
    ApiKeyRegistrationRequired,

    /// Lighter WAF / per-IP rate-limit cooldown is currently active.
    /// `until_unix` is the unix-epoch second at which the cooldown expires.
    /// Callers must NOT retry while this error is being returned — every
    /// additional Lighter REST call refreshes the WAF rolling window and
    /// extends the block. See bot-strategy#35.
    #[error("Lighter WAF cooldown active until unix={until_unix} (rate-limited)")]
    RateLimited { until_unix: i64 },

    /// Transient I/O / parse / dependency failure. Caller may retry with
    /// backoff.
    #[error("Transient error: {0}")]
    Transient(String),

    /// Permanent failure — caller should surface or give up. Examples:
    /// signing-key parse failures, "market not found", unimplemented
    /// connector methods.
    #[error("Permanent error: {0}")]
    Permanent(String),

    /// Caller-side input validation failure. `field` is the parameter
    /// name; `value` is the rejected stringified value.
    #[error("Invalid input: {field}={value}")]
    InvalidInput { field: String, value: String },
}

impl From<ParseDecimalError> for DexError {
    fn from(e: ParseDecimalError) -> Self {
        DexError::Transient(format!("{:?}", e))
    }
}

impl DexRequest {
    pub async fn new(endpoint: String) -> Result<Self, DexError> {
        // Bound REST calls so a hung server can't stall callers indefinitely.
        // Mirrors the Lighter pattern (see lighter_connector.rs builder, 5s
        // connect + 15s overall). Combined with extended_connector.rs's send
        // retry (3 attempts, 500/1000/2000ms backoff) the worst-case is
        // ~50s before bubbling up; well below the 167-min stuck event from
        // the bot-strategy#102 P2 incident.
        let client = Client::builder()
            .cookie_store(true)
            .user_agent("debot/1.0")
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(15))
            .build()?;

        Ok(DexRequest { client, endpoint })
    }

    pub async fn handle_request<T: serde::de::DeserializeOwned, U: Serialize + ?Sized>(
        &self,
        method: HttpMethod,
        request_url: String,
        headers: &HashMap<String, String>,
        json_payload: String,
    ) -> Result<T, DexError> {
        let url = format!("{}{}", self.endpoint, request_url);

        let mut header_map = HeaderMap::new();
        header_map.insert(
            reqwest::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );

        for (key, value) in headers.iter() {
            let key = reqwest::header::HeaderName::from_bytes(key.as_bytes())
                .expect("Failed to create HeaderName");
            let value = HeaderValue::from_str(value).expect("Failed to create HeaderValue");
            header_map.insert(key, value);
        }

        let client = self.client.clone();
        let request_builder = client
            .request(Method::from(method), &url)
            .headers(header_map);

        log::trace!("payload = {}", json_payload);

        let request_builder = if !json_payload.is_empty() {
            request_builder.body(json_payload.clone())
        } else {
            request_builder
        };

        let response = request_builder.send().await.map_err(DexError::from)?;
        let status = response.status();

        // 4xx is an application-level rejection (the caller knows the context
        // and re-logs at the right level — e.g. Extended's 1137 race). 5xx is
        // a server-side issue worth surfacing to ops, so keep WARN for that.
        let non_success_level = if status.is_client_error() {
            log::Level::Info
        } else {
            log::Level::Warn
        };

        if !status.is_success() {
            log::log!(
                non_success_level,
                "Server returned error: {}. requested url: {}",
                status,
                url
            );
        }

        let response_headers = response.headers().clone();
        log::trace!("Response header: {:?}", response_headers);

        let response_body = response.text().await.map_err(DexError::from)?;
        if !status.is_success() {
            log::log!(
                non_success_level,
                "Response body (non-success): {}",
                response_body
            );
        } else {
            log::trace!("Response body: {}", response_body);
        }

        serde_json::from_str(&response_body).map_err(|e| {
            log::error!(
                "Failed to deserialize response: {}, payload = {}",
                e,
                json_payload
            );
            DexError::Serde(e)
        })
    }
}

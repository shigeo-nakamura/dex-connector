use debot_utils::ParseDecimalError;
use reqwest::{
    header::{HeaderMap, HeaderValue},
    Client, Method,
};
use serde::Serialize;
use std::{
    collections::HashMap,
    error::Error as StdError,
    fmt::{self, Display},
    time::Duration,
};

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

#[derive(Debug)]
pub enum DexError {
    Serde(serde_json::Error),
    Reqwest(reqwest::Error),
    ServerResponse(String),
    WebSocketError(String),
    Other(String),
    NoConnection,
    UpcomingMaintenance,
    ApiKeyRegistrationRequired,
    /// Lighter WAF / per-IP rate-limit cooldown is currently active.
    /// `until_unix` is the unix-epoch second at which the cooldown expires.
    /// Callers must NOT retry while this error is being returned — every
    /// additional Lighter REST call refreshes the WAF rolling window and
    /// extends the block. See bot-strategy#35.
    RateLimited { until_unix: i64 },
}

impl From<ParseDecimalError> for DexError {
    fn from(e: ParseDecimalError) -> Self {
        DexError::Other(format!("{:?}", e))
    }
}

impl Display for DexError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DexError::Serde(ref e) => write!(f, "Serde JSON error: {}", e),
            DexError::Reqwest(ref e) => write!(f, "Reqwest error: {}", e),
            DexError::ServerResponse(ref e) => write!(f, "Server response error: {}", e),
            DexError::Other(ref e) => write!(f, "Other error: {}", e),
            DexError::NoConnection => write!(f, "No running WebSocketConnection"),
            DexError::WebSocketError(ref e) => write!(f, "WebSocket error: {}", e),
            DexError::UpcomingMaintenance => write!(f, "Network upgrade scheduled in < 2h"),
            DexError::ApiKeyRegistrationRequired => write!(f, "API key registration is required"),
            DexError::RateLimited { until_unix } => write!(
                f,
                "Lighter WAF cooldown active until unix={} (rate-limited)",
                until_unix
            ),
        }
    }
}

impl StdError for DexError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match *self {
            DexError::Serde(ref e) => Some(e),
            DexError::Reqwest(ref e) => Some(e),
            DexError::ServerResponse(_) => None,
            DexError::Other(_) => None,
            DexError::NoConnection => None,
            DexError::WebSocketError(_) => None,
            DexError::UpcomingMaintenance => None,
            DexError::ApiKeyRegistrationRequired => None,
            DexError::RateLimited { .. } => None,
        }
    }
}

impl From<reqwest::Error> for DexError {
    fn from(error: reqwest::Error) -> Self {
        DexError::Reqwest(error)
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

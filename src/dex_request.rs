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
};

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
        let client = Client::builder().cookie_store(true).build()?;

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

        if !status.is_success() {
            let error_message =
                format!("Server returned error: {}. requested url: {}", status, url);
            log::error!("{}", &error_message);
        }

        let response_headers = response.headers().clone();
        log::trace!("Response header: {:?}", response_headers);

        let response_body = response.text().await.map_err(DexError::from)?;
        log::trace!("Response body: {}", response_body);

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

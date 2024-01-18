use reqwest::Client;
use std::{
    error::Error as StdError,
    fmt::{self, Display},
};

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
    Other(String),
}

impl Display for DexError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DexError::Serde(ref e) => write!(f, "Serde JSON error: {}", e),
            DexError::Reqwest(ref e) => write!(f, "Reqwest error: {}", e),
            DexError::ServerResponse(ref e) => write!(f, "Server response error: {}", e),
            DexError::Other(ref e) => write!(f, "Other error: {}", e),
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
        let client = Client::builder()
            // .default_headers(Self::headers_with_hashed_api_key(api_key))
            .build()?;

        Ok(DexRequest { client, endpoint })
    }

    // fn generate_auth_headers(api_key: String) -> HeaderMap {
    //     let mut hasher = Sha256::new();
    //     hasher.update(api_key);
    //     let hashed_api_key = format!("{:x}", hasher.finalize());

    //     let mut headers = HeaderMap::new();
    //     headers.insert("Authorization", hashed_api_key.parse().unwrap());
    //     headers
    // }

    // pub async fn handle_request<T: serde::de::DeserializeOwned>(
    //     &self,
    //     result: Result<reqwest::Response, reqwest::Error>,
    //     url: &str,
    // ) -> Result<T, DexError> {
    //     let response = result.map_err(DexError::from)?;
    //     let status = response.status();

    //     if status.is_success() {
    //         let headers = response.headers().clone();
    //         let body = response.text().await.map_err(DexError::from)?;
    //         log::trace!("Response body: {}", body);

    //         serde_json::from_str(&body).map_err(|e| {
    //             log::warn!("Response header: {:?}", headers);
    //             log::error!("Failed to deserialize response: {}", e);
    //             DexError::Serde(e)
    //         })
    //     } else {
    //         let error_response: CommonErrorResponse = response
    //             .json()
    //             .await
    //             .unwrap_or(CommonErrorResponse { message: None });
    //         let error_message = format!(
    //             "Server returned error: {}. Requested url: {}, message: {:?}",
    //             status, url, error_response.message,
    //         );
    //         log::error!("{}", &error_message);
    //         Err(DexError::ServerResponse(error_message))
    //     }
    // }
}

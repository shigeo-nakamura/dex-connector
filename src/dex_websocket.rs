use futures::stream::{SplitSink, SplitStream};
use futures::StreamExt;
use http;
use reqwest::Url;
use tokio::net::TcpStream;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[derive(Clone, Debug)]
pub struct DexWebSocket {
    endpoint: String,
}

impl DexWebSocket {
    pub fn new(endpoint: String) -> Self {
        DexWebSocket { endpoint }
    }

    #[allow(dead_code)]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub async fn connect(
        &self,
    ) -> Result<
        (
            SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
            SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        ),
        (),
    > {
        let url = match Url::parse(&self.endpoint) {
            Ok(url) => url,
            Err(e) => {
                log::error!("Failed to parse: {:?}", e);
                return Err(());
            }
        };

        // Build a custom WebSocket request with a User-Agent header.
        // This is crucial as many servers reject connections without a proper User-Agent.
        let request = match http::request::Request::builder()
            .method("GET")
            .uri(url.as_str())
            .header("Host", url.host_str().unwrap_or_default())
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header(
                "Sec-WebSocket-Key",
                tokio_tungstenite::tungstenite::handshake::client::generate_key(),
            )
            .header("User-Agent", "debot/1.0") // Add the missing User-Agent header
            .body(())
        {
            Ok(req) => req,
            Err(e) => {
                log::error!("Failed to build WebSocket request: {:?}", e);
                return Err(());
            }
        };

        let (ws_stream, _) = match connect_async(request).await {
            Ok(conn) => conn,
            Err(e) => {
                log::error!("Failed to connect: {:?}", e);
                return Err(());
            }
        };

        log::info!("websocket is connected to {}", self.endpoint);

        let (write, read) = ws_stream.split();

        Ok((write, read))
    }
}

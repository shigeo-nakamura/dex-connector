use futures::stream::{SplitSink, SplitStream};
use futures::StreamExt;
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

        let (ws_stream, _) = match connect_async(url).await {
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

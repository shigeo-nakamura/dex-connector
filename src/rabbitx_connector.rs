use std::sync::Arc;

use crate::{
    dex_connector::DexConnector,
    dex_request::{DexError, DexRequest},
    dex_websocket::DexWebSocket,
    BalanceResponse, CreateOrderResponse, DefaultResponse, FilledOrdersResponse, TickerResponse,
};
use async_trait::async_trait;

pub struct RabbitxConnector {
    request: DexRequest,
    web_socket: DexWebSocket,
}

impl RabbitxConnector {
    pub async fn new(rest_endpoint: &str, web_socket_endpoint: &str) -> Result<Self, DexError> {
        let request = DexRequest::new(rest_endpoint.to_owned()).await?;
        let web_socket = DexWebSocket::new(web_socket_endpoint.to_owned());
        Ok(RabbitxConnector {
            request,
            web_socket,
        })
    }
}

#[async_trait]
impl DexConnector for RabbitxConnector {
    async fn get_ticker(&self, symbol: &str) -> Result<TickerResponse, DexError> {
        Ok(TickerResponse::default())
    }

    async fn get_filled_orders(
        &self,
        dex: &str,
        symbol: &str,
    ) -> Result<FilledOrdersResponse, DexError> {
        Ok(FilledOrdersResponse::default())
    }

    async fn get_balance(&self) -> Result<BalanceResponse, DexError> {
        Ok(BalanceResponse::default())
    }

    async fn clear_filled_order(
        &self,
        symbol: &str,
        order_id: &str,
    ) -> Result<DefaultResponse, DexError> {
        Ok(DefaultResponse::default())
    }

    async fn create_order(
        &self,
        symbol: &str,
        size: &str,
        side: &str,
        price: Option<String>,
    ) -> Result<CreateOrderResponse, DexError> {
        Ok(CreateOrderResponse::default())
    }

    async fn cancel_order(&self, order_id: &str) -> Result<DefaultResponse, DexError> {
        Ok(DefaultResponse::default())
    }

    async fn close_all_positions(
        &self,
        symbol: Option<String>,
    ) -> Result<DefaultResponse, DexError> {
        Ok(DefaultResponse::default())
    }
}

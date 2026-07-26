use super::models::{
    AccountWire, OpenOrderWire, OpenOrdersResponseWire, PositionWire, PositionsResponseWire,
    SetLeverageResponseWire,
};
use super::{normalize_market, now_ns, ArcusConnector};
use crate::{
    BalanceResponse, CombinedBalanceResponse, DexError, OpenOrder, OpenOrdersResponse, OrderSide,
    PositionSnapshot,
};
use reqwest::RequestBuilder;
use rust_decimal::Decimal;
use serde::Serialize;
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SetLeverageRequest<'a> {
    address: &'a str,
    market_id: u32,
    leverage: u32,
    account_index: u8,
}

impl ArcusConnector {
    fn account_context(&self) -> Result<&super::auth::ArcusAuth, DexError> {
        self.auth.as_ref().ok_or_else(|| {
            DexError::Permanent(
                "Arcus account read requires address configuration (bot-strategy#749)".to_string(),
            )
        })
    }

    fn account_read(&self, path: &str) -> Result<RequestBuilder, DexError> {
        let auth = self.account_context()?;
        Ok(self
            .client
            .get(format!("{}{}", self.base_url, path))
            .query(&[("address", auth.address())]))
    }

    pub(super) async fn fetch_account(&self) -> Result<AccountWire, DexError> {
        let request = self.account_read("/v1/account")?;
        self.request_json(request, "GET /v1/account").await
    }

    pub(super) async fn fetch_positions(&self) -> Result<Vec<PositionSnapshot>, DexError> {
        let request = self.account_read("/v1/positions")?;
        let response: PositionsResponseWire =
            self.request_json(request, "GET /v1/positions").await?;
        response
            .positions
            .into_values()
            .map(position_snapshot)
            .collect()
    }

    pub(super) async fn fetch_open_orders(
        &self,
        symbol: &str,
    ) -> Result<OpenOrdersResponse, DexError> {
        let market = normalize_market(symbol);
        let request = self.account_read("/v1/openOrders")?;
        let response: OpenOrdersResponseWire =
            self.request_json(request, "GET /v1/openOrders").await?;
        let orders = response
            .orders
            .into_iter()
            .filter(|order| normalize_market(&order.market_display_name) == market)
            .map(open_order)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(OpenOrdersResponse { orders })
    }

    pub(super) async fn apply_leverage(&self, symbol: &str, leverage: u32) -> Result<(), DexError> {
        if leverage == 0 {
            return Err(DexError::InvalidInput {
                field: "leverage".to_string(),
                value: leverage.to_string(),
            });
        }
        let auth = self.account_context()?;
        if !auth.can_sign() {
            return Err(DexError::Permanent(
                "Arcus authenticated mutation requires api_key and api_private_key_hex (bot-strategy#749)"
                    .to_string(),
            ));
        }
        let api_key = auth
            .api_key()
            .expect("can_sign guarantees an Arcus public API key");
        let info = self.market_info_for(symbol).await?;
        let body = SetLeverageRequest {
            address: auth.address(),
            market_id: info.market_id,
            leverage,
            account_index: auth.account_index(),
        };
        let timestamp_ns = now_ns();
        let signature = auth.sign_legacy(timestamp_ns, "setLeverage", &body)?;
        let request = self
            .client
            .post(format!("{}/v1/setLeverage", self.base_url))
            .query(&[("address", auth.address())])
            .header("X-API-Key", api_key)
            .header("X-Timestamp", timestamp_ns.to_string())
            .header("X-Signature", signature)
            .json(&body);
        let response: SetLeverageResponseWire =
            self.request_json(request, "POST /v1/setLeverage").await?;
        match response.status.as_str() {
            "APPLIED" | "ACK" => Ok(()),
            status => Err(DexError::ServerResponse(format!(
                "Arcus setLeverage status={status} reject_reason={}",
                response.reject_reason.as_deref().unwrap_or("unknown")
            ))),
        }
    }
}

pub(super) fn balance_response(account: &AccountWire) -> Result<BalanceResponse, DexError> {
    Ok(BalanceResponse {
        equity: parse_account_decimal(&account.equity, "equity")?,
        // `netQuoteBalance` is the cash-ledger value, not funds available
        // for new orders; with open positions or reserved margin it can
        // materially overstate usable balance. `freeCollateral` matches the
        // available-balance convention `lighter_connector::fetch_balance`
        // uses (bot-strategy#749 review).
        balance: parse_account_decimal(&account.free_collateral, "freeCollateral")?,
        position_entry_price: None,
        position_sign: None,
    })
}

pub(super) fn combined_balance_response(
    account: &AccountWire,
) -> Result<CombinedBalanceResponse, DexError> {
    let balance = balance_response(account)?;
    let mut token_balances = HashMap::new();
    token_balances.insert("USD".to_string(), balance.clone());
    Ok(CombinedBalanceResponse {
        usd_balance: balance.equity,
        total_asset_value: balance.equity,
        token_balances,
        spot_assets: Vec::new(),
    })
}

fn position_snapshot(position: PositionWire) -> Result<PositionSnapshot, DexError> {
    let raw_size = parse_account_decimal(&position.size, "position.size")?;
    let sign = match position.side.as_str() {
        "LONG" => 1,
        "SHORT" => -1,
        other => {
            return Err(DexError::Transient(format!(
                "Arcus unknown position side: {other}"
            )))
        }
    };
    Ok(PositionSnapshot {
        symbol: position.market_display_name,
        size: raw_size.abs(),
        sign,
        entry_price: Some(parse_account_decimal(
            &position.average_entry_price,
            "position.averageEntryPrice",
        )?),
    })
}

fn open_order(order: OpenOrderWire) -> Result<OpenOrder, DexError> {
    let side = match order.side.as_str() {
        "BUY" => OrderSide::Long,
        "SELL" => OrderSide::Short,
        other => {
            return Err(DexError::Transient(format!(
                "Arcus unknown order side: {other}"
            )))
        }
    };
    Ok(OpenOrder {
        order_id: order.order_id,
        symbol: order.market_display_name,
        side,
        size: parse_account_decimal(&order.remaining_size, "order.remainingSize")?,
        price: parse_account_decimal(&order.price, "order.price")?,
        status: order.status,
    })
}

fn parse_account_decimal(raw: &str, field: &str) -> Result<Decimal, DexError> {
    Decimal::from_str(raw).map_err(|err| {
        DexError::Transient(format!(
            "Arcus account decimal parse failed field={field} value={raw}: {err}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_account_position_and_open_order_fixtures() {
        let account: AccountWire = serde_json::from_str(
            r#"{
                "netQuoteBalance":"975.5","equity":"1001.25","freeCollateral":"800",
                "positions":{"1":{"marketDisplayName":"BTC-USD","side":"LONG","size":"0.02","averageEntryPrice":"50000"}}
            }"#,
        )
        .expect("account fixture");
        let balance = balance_response(&account).expect("balance");
        assert_eq!(balance.equity, Decimal::from_str("1001.25").unwrap());
        assert_eq!(balance.balance, Decimal::from_str("800").unwrap());
        let position = position_snapshot(PositionWire {
            market_display_name: "BTC-USD".to_string(),
            side: "LONG".to_string(),
            size: "0.02".to_string(),
            average_entry_price: "50000".to_string(),
        })
        .expect("position");
        assert_eq!(position.symbol, "BTC-USD");
        assert_eq!(position.sign, 1);
        assert_eq!(position.size, Decimal::from_str("0.02").unwrap());

        let order = open_order(OpenOrderWire {
            order_id: "order-1".to_string(),
            market_display_name: "BTC-USD".to_string(),
            side: "SELL".to_string(),
            status: "OPEN".to_string(),
            price: "51000.5".to_string(),
            remaining_size: "0.01".to_string(),
        })
        .expect("open order");
        assert_eq!(order.side, OrderSide::Short);
        assert_eq!(order.size, Decimal::from_str("0.01").unwrap());
    }
}

//! Wire-protocol model structs deserialized from Lighter REST / WS
//! responses, plus the Tx-envelope structs we serialize back to the
//! sendTx signer.
//!
//! Mostly internal to the connector — fields are `pub(super)` so
//! mod.rs and sibling sub-modules (ws.rs, market_cache.rs, orders.rs)
//! can read/write them as before without exposing anything on the
//! crate API.
//!
//! `#[allow(dead_code)]` on individual structs preserves fields that
//! arrive on the wire but aren't read today — useful when the wire
//! shape changes and we want to bring up a new field without first
//! noisily adding it to the struct.

use std::time::Instant;

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug, Clone)]
pub(super) struct LighterOrderBook {
    pub(super) bids: Vec<LighterOrderBookEntry>,
    pub(super) asks: Vec<LighterOrderBookEntry>,
}

#[derive(Debug, Clone)]
pub(super) struct LighterOrderBookCacheEntry {
    pub(super) order_book: LighterOrderBook,
    pub(super) updated_at: Instant,
    /// Book-update time in Unix ms: the exchange's `last_updated_at` when the
    /// WS frame carries it, else local wall-clock at receive. Surfaced on
    /// `OrderBookSnapshot::book_ts_ms` for feed-age watchdogs (bot-strategy#552).
    pub(super) book_ts_ms: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub(super) struct LighterOrderBookEntry {
    pub(super) price: String,
    pub(super) size: String,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub(super) struct LighterAccountResponse {
    pub(super) code: i32,
    pub(super) total: i32,
    pub(super) accounts: Vec<LighterAccountInfo>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub(super) struct LighterAccountInfo {
    pub(super) account_index: i64,
    #[serde(default)]
    pub(super) l1_address: String,
    pub(super) available_balance: String,
    pub(super) collateral: String,
    pub(super) total_asset_value: String,
    pub(super) positions: Vec<LighterPosition>,
    #[serde(default)]
    pub(super) assets: Vec<LighterAsset>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub(super) struct LighterAsset {
    pub(super) symbol: String,
    pub(super) asset_id: u32,
    pub(super) balance: String,
    pub(super) locked_balance: String,
    #[serde(default)]
    pub(super) margin_balance: String,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub(super) struct LighterPosition {
    pub(super) market_id: u8,
    pub(super) symbol: String,
    pub(super) position: String,
    pub(super) sign: i8,
    pub(super) open_order_count: u32,
    pub(super) avg_entry_price: String,
    #[serde(default)]
    pub(super) unrealized_pnl: String,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub(super) struct LighterTradesResponse {
    pub(super) code: i32,
    pub(super) trades: Vec<LighterTrade>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub(super) struct LighterTrade {
    pub(super) trade_id: u64,
    pub(super) price: String,
    pub(super) size: String,
    pub(super) usd_amount: String,
    pub(super) market_id: u8,
    #[serde(default)]
    pub(super) side: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
#[allow(dead_code)]
pub(super) struct LighterFundingRates {
    pub(super) code: i32,
    pub(super) funding_rates: Vec<LighterFundingRate>,
}

#[derive(Deserialize, Debug, Clone)]
#[allow(dead_code)]
pub(super) struct LighterFundingRate {
    pub(super) market_id: u32,
    pub(super) exchange: String,
    pub(super) symbol: String,
    pub(super) rate: f64,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub(super) struct LighterOrderBookDetailsResponse {
    pub(super) code: i32,
    #[serde(rename = "order_book_details")]
    pub(super) order_book_details: Vec<LighterOrderBookDetail>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub(super) struct LighterOrderBookDetail {
    pub(super) market_id: u32,
    pub(super) symbol: String,
    #[serde(rename = "min_base_amount")]
    pub(super) min_base_amount: Option<String>,
    #[serde(rename = "supported_price_decimals")]
    pub(super) supported_price_decimals: Option<u32>,
    #[serde(rename = "supported_size_decimals")]
    pub(super) supported_size_decimals: Option<u32>,
}

#[derive(Deserialize, Debug)]
pub(super) struct LighterOrderBooksResponse {
    #[serde(rename = "order_books")]
    pub(super) order_books: Vec<LighterOrderBookMeta>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub(super) struct LighterOrderBookMeta {
    pub(super) market_id: u32,
    pub(super) symbol: String,
    #[serde(rename = "min_base_amount")]
    pub(super) min_base_amount: Option<String>,
    #[serde(rename = "supported_price_decimals")]
    pub(super) supported_price_decimals: Option<u32>,
    #[serde(rename = "supported_size_decimals")]
    pub(super) supported_size_decimals: Option<u32>,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
pub(super) struct LighterNonceResponse {
    pub(super) nonce: u64,
}

#[derive(Deserialize, Debug)]
pub(super) struct ApiKeyInfo {
    #[serde(rename = "account_index")]
    #[allow(dead_code)]
    pub(super) account_index: u64,
    #[serde(rename = "api_key_index")]
    #[allow(dead_code)]
    pub(super) api_key_index: u32,
    #[allow(dead_code)]
    pub(super) nonce: u32,
    #[serde(rename = "public_key")]
    pub(super) public_key: String,
}

#[derive(Deserialize, Debug)]
pub(super) struct ApiKeyResponse {
    #[allow(dead_code)]
    pub(super) code: u32,
    #[serde(rename = "api_keys")]
    pub(super) api_keys: Vec<ApiKeyInfo>,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
pub(super) struct LighterOrderResponse {
    pub(super) order_id: String,
    pub(super) price: String,
    pub(super) amount: String,
}

#[allow(dead_code)]
#[derive(Serialize, Debug)]
pub(super) struct LighterTx {
    pub(super) tx_type: String,
    pub(super) ticker: String,
    pub(super) amount: String,
    pub(super) price: Option<String>,
    pub(super) order_type: String,
    pub(super) time_in_force: String,
}

#[allow(dead_code)]
#[derive(Serialize, Debug)]
pub(super) struct LighterSignedEnvelope {
    pub(super) sig: String,
    pub(super) nonce: u64,
    pub(super) tx: LighterTx,
}

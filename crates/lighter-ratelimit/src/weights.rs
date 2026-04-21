//! Lighter endpoint weight table. Source: bot-strategy#7.
//!
//! Weights are per-request costs against the 60,000 weight/min Standard-tier
//! budget. A single `GET /account` costs 300 weight, not 1 req.

/// Default weight for generic reads (account, orderBook, etc.).
pub const WEIGHT_DEFAULT: u32 = 300;
/// Trade-related transactions.
pub const WEIGHT_SEND_TX: u32 = 6;
pub const WEIGHT_SEND_TX_BATCH: u32 = 6;
pub const WEIGHT_NEXT_NONCE: u32 = 6;
/// `trades` endpoint (heavier read).
pub const WEIGHT_TRADES: u32 = 600;
/// `changeAccountTier` (rare, expensive).
pub const WEIGHT_CHANGE_ACCOUNT_TIER: u32 = 3_000;

/// Lookup the weight for a given endpoint path. Default is 300 for unknowns —
/// safer to overcount than to undercount on unfamiliar endpoints.
pub fn weight_for(path: &str) -> u32 {
    // Normalize to the path segment before any query string.
    let base = path.split('?').next().unwrap_or(path);
    if base.ends_with("/sendTx") || base.ends_with("/sendTxBatch") {
        WEIGHT_SEND_TX
    } else if base.ends_with("/nextNonce") {
        WEIGHT_NEXT_NONCE
    } else if base.ends_with("/trades") {
        WEIGHT_TRADES
    } else if base.ends_with("/changeAccountTier") {
        WEIGHT_CHANGE_ACCOUNT_TIER
    } else {
        WEIGHT_DEFAULT
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_endpoints() {
        assert_eq!(weight_for("/api/v1/sendTx"), 6);
        assert_eq!(weight_for("/api/v1/nextNonce?account_index=1"), 6);
        assert_eq!(weight_for("/api/v1/trades"), 600);
        assert_eq!(weight_for("/api/v1/changeAccountTier"), 3_000);
    }

    #[test]
    fn unknown_defaults_to_300() {
        assert_eq!(weight_for("/api/v1/account?by=index&value=1"), 300);
        assert_eq!(weight_for("/api/v1/wombat"), 300);
    }
}

//! UDS line-delimited JSON protocol between clients and the sidecar.
//!
//! Every request and response is a single JSON object terminated by `\n`.
//! This keeps parsing trivial and avoids framing headaches across languages.

use serde::{Deserialize, Serialize};

/// Acquire policy when the bucket does not have enough tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "policy")]
pub enum AcquirePolicy {
    /// Fail immediately if tokens are insufficient. Appropriate for periodic
    /// monitoring calls (equity refresh, polling) where skipping a cycle is safe.
    Shed,
    /// Wait up to `max_ms` for tokens to refill, then grant. If still insufficient
    /// after `max_ms`, fall back to Shed. Appropriate for critical calls (order
    /// submission) where latency is preferable to dropping the request.
    Wait { max_ms: u64 },
}

impl AcquirePolicy {
    pub fn max_wait_ms(self) -> u64 {
        match self {
            AcquirePolicy::Shed => 0,
            AcquirePolicy::Wait { max_ms } => max_ms,
        }
    }
}

/// Request sent from client to the sidecar.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcquireRequest {
    /// Weight to consume, matching Lighter's per-endpoint weight table.
    pub weight: u32,
    /// Optional free-form tag for telemetry (e.g. endpoint name).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    #[serde(flatten)]
    pub policy: AcquirePolicy,
}

/// Response from sidecar to client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcquireResponse {
    /// `true` if the tokens were granted (possibly after waiting).
    pub granted: bool,
    /// Milliseconds spent waiting before grant. `0` on immediate grant or shed.
    #[serde(default)]
    pub waited_ms: u64,
    /// Tokens remaining after this acquire (informational, sampled post-grant).
    #[serde(default)]
    pub tokens_remaining: i64,
}

impl AcquireResponse {
    pub fn granted(waited_ms: u64, tokens_remaining: i64) -> Self {
        Self {
            granted: true,
            waited_ms,
            tokens_remaining,
        }
    }

    pub fn shed(tokens_remaining: i64) -> Self {
        Self {
            granted: false,
            waited_ms: 0,
            tokens_remaining,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_shed_request_roundtrip() {
        let req = AcquireRequest {
            weight: 300,
            tag: Some("account".to_string()),
            policy: AcquirePolicy::Shed,
        };
        let s = serde_json::to_string(&req).unwrap();
        assert!(s.contains("\"policy\":\"shed\""));
        assert!(s.contains("\"weight\":300"));
        let back: AcquireRequest = serde_json::from_str(&s).unwrap();
        assert_eq!(back.weight, 300);
        assert_eq!(back.policy, AcquirePolicy::Shed);
    }

    #[test]
    fn serde_wait_request_roundtrip() {
        let req = AcquireRequest {
            weight: 6,
            tag: None,
            policy: AcquirePolicy::Wait { max_ms: 5000 },
        };
        let s = serde_json::to_string(&req).unwrap();
        let back: AcquireRequest = serde_json::from_str(&s).unwrap();
        assert_eq!(back.policy, AcquirePolicy::Wait { max_ms: 5000 });
    }

    #[test]
    fn response_shed_has_not_granted() {
        let r = AcquireResponse::shed(42);
        assert!(!r.granted);
        assert_eq!(r.waited_ms, 0);
        assert_eq!(r.tokens_remaining, 42);
    }
}

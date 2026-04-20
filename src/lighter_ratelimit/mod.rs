//! Host-shared weight-based rate limiter for Lighter REST.
//!
//! Context: Lighter enforces a 60,000 weight/min ceiling per IP (Standard
//! tier). Every bot on the prod host shares the same IP, so a WS-reconnect
//! cascade can coordinate a burst past the ceiling and trip the WAF CAPTCHA
//! (see bot-strategy#7, #20, #78). Reactive mitigations (#35 CAPTCHA
//! detection, #52 429 cooldown) handle the aftermath but cannot prevent the
//! initial burst.
//!
//! This module provides the two halves of the proactive fix:
//!   - `bucket` — the token bucket algorithm (capacity, refill, try_acquire).
//!   - `client` — UDS client with an in-process fallback.
//!   - `protocol` — wire types between client and sidecar daemon.
//!   - `weights` — Lighter endpoint weight table (#7).
//!
//! The sidecar daemon itself lives at `src/bin/lighter-ratelimit.rs` and
//! consumes the same types from this module. Single source of truth for
//! policy, capacity, and protocol versioning.

pub mod bucket;
pub mod client;
pub mod protocol;
pub mod weights;

pub use bucket::{BucketConfig, TokenBucket};
pub use client::{AcquireOutcome, RateLimitClient, DEFAULT_SOCKET_PATH};
pub use protocol::{AcquirePolicy, AcquireRequest, AcquireResponse};

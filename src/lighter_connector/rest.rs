//! Lighter REST plumbing — generic JSON request helper, raw-text helper
//! for endpoints that need WAF gating without serde deserialization, and
//! rate-limit budget acquisition.
//!
//! All helpers here funnel through:
//!
//! 1. `track_api_call` — keeps a 60s rolling window of recent calls so
//!    journalctl shows the burst pattern that triggered any 429.
//! 2. `crate::lighter_waf_cooldown::cooldown_remaining()` — host-shared
//!    cooldown gate; if engaged, fail fast with `DexError::RateLimited`
//!    rather than refresh the WAF rolling window. (bot-strategy#35.)
//! 3. `crate::lighter_ratelimit` — proactive per-IP budget. Reads default
//!    to `Shed` policy; sendTx / nonce paths use `Wait`. (bot-strategy#79.)

use super::LighterConnector;
use crate::dex_request::{DexError, HttpMethod};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};
use std::time::{Duration, Instant};

/// Global API call counter for monitoring Lighter Protocol rate limits.
pub(super) static API_CALL_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(super) static API_CALL_TRACKER: std::sync::LazyLock<Mutex<Vec<(Instant, String)>>> =
    std::sync::LazyLock::new(|| Mutex::new(Vec::new()));

/// Track and log API calls for rate limit monitoring.
pub(super) fn track_api_call(endpoint: &str, method: &str) {
    let call_count = API_CALL_COUNTER.fetch_add(1, Ordering::SeqCst) + 1;
    let now = Instant::now();

    // Clean old entries (older than 60 seconds)
    {
        let mut tracker = API_CALL_TRACKER.lock().unwrap();
        tracker.retain(|(time, _)| now.duration_since(*time) < Duration::from_secs(60));
        tracker.push((now, format!("{} {}", method, endpoint)));

        let recent_calls = tracker.len();
        log::info!(
            "[API_TRACKER] #{} {} {} | Recent calls (60s): {} | Rate: {:.1}/min",
            call_count,
            method,
            endpoint,
            recent_calls,
            recent_calls as f64
        );

        // Warn if approaching rate limit
        if recent_calls > 45 {
            log::info!(
                "[API_TRACKER] ⚠️  Approaching rate limit: {}/60 calls in last 60s",
                recent_calls
            );
        }
    }
}

impl LighterConnector {
    /// Direct REST GET helper that performs the same WAF cooldown gating as
    /// `make_request()`, for the few call sites that bypass the generic helper
    /// and want the raw response body. Returns `Err(DexError::RateLimited)` if
    /// the host-shared cooldown is active or the response itself is a WAF
    /// challenge. See bot-strategy#35.
    pub(super) async fn fetch_text_with_waf_guard(
        &self,
        url: &str,
        err_label: &str,
    ) -> Result<String, DexError> {
        // Log the call through API_TRACKER so `/recentTrades`, `/funding-rates`,
        // `/orderBookDetails`, etc. show up in the 60s rolling window alongside
        // `/account`. Without this they fire silently and any 429 they trigger
        // appears in journalctl as a cooldown engagement with no visible
        // preceding request.
        track_api_call(err_label, "GET");

        if let Some(remaining) = crate::lighter_waf_cooldown::cooldown_remaining() {
            return Err(DexError::RateLimited {
                until_unix: chrono::Utc::now().timestamp() + remaining.as_secs() as i64,
            });
        }

        let response = self
            .client
            .get(url)
            .header("X-API-KEY", &self.api_key_public)
            .send()
            .await
            .map_err(|e| DexError::Other(format!("{}: {}", err_label, e)))?;

        let status = response.status();
        let headers = response.headers().clone();

        if let Some(src) = crate::lighter_waf_cooldown::classify_rate_limit(status, &headers) {
            let dur = crate::lighter_waf_cooldown::engage_cooldown(src);
            return Err(DexError::RateLimited {
                until_unix: chrono::Utc::now().timestamp() + dur.as_secs() as i64,
            });
        }

        let response_text = response
            .text()
            .await
            .map_err(|e| DexError::Other(format!("{} read body: {}", err_label, e)))?;

        if !status.is_success() {
            if let Some(src) =
                crate::lighter_waf_cooldown::classify_rate_limit_body(status, &response_text)
            {
                let dur = crate::lighter_waf_cooldown::engage_cooldown(src);
                return Err(DexError::RateLimited {
                    until_unix: chrono::Utc::now().timestamp() + dur.as_secs() as i64,
                });
            }
            return Err(DexError::Other(format!(
                "{} HTTP {}: {}",
                err_label, status, response_text
            )));
        }

        Ok(response_text)
    }

    /// Acquire host-shared rate-limit budget for an upcoming REST call.
    /// Returns `Err(DexError::RateLimited)` on shed so the caller can fall
    /// back to cached data or skip the cycle. See bot-strategy#79.
    pub(super) async fn acquire_rest_budget(
        &self,
        endpoint: &str,
        policy: crate::lighter_ratelimit::AcquirePolicy,
    ) -> Result<(), DexError> {
        let weight = crate::lighter_ratelimit::weights::weight_for(endpoint);
        match self
            .rate_limiter
            .acquire(weight, policy, Some(endpoint))
            .await
        {
            crate::lighter_ratelimit::AcquireOutcome::Granted { .. } => Ok(()),
            crate::lighter_ratelimit::AcquireOutcome::Shed => Err(DexError::RateLimited {
                until_unix: chrono::Utc::now().timestamp() + 2,
            }),
        }
    }

    pub(super) async fn make_request<T>(
        &self,
        endpoint: &str,
        method: HttpMethod,
        body: Option<&str>,
    ) -> Result<T, DexError>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        // Track API call
        let method_str = match method {
            HttpMethod::Get => "GET",
            HttpMethod::Post => "POST",
            HttpMethod::Put => "PUT",
            HttpMethod::Patch => "PATCH",
            HttpMethod::Delete => "DELETE",
        };
        track_api_call(endpoint, method_str);

        // Fail fast if a host-shared WAF cooldown is active. Sending the
        // request would only refresh the WAF rolling window. See bot-strategy#35.
        if let Some(remaining) = crate::lighter_waf_cooldown::cooldown_remaining() {
            return Err(DexError::RateLimited {
                until_unix: chrono::Utc::now().timestamp() + remaining.as_secs() as i64,
            });
        }

        // Proactive per-IP rate limit. Shed policy by default for generic
        // reads: callers that need delivery guarantees (sendTx, nonce) go
        // through dedicated code paths with Wait policy. See bot-strategy#79.
        self.acquire_rest_budget(endpoint, crate::lighter_ratelimit::AcquirePolicy::Shed)
            .await?;

        let url = format!("{}{}", self.base_url, endpoint);

        let mut request = match method {
            HttpMethod::Get => self.client.get(&url),
            HttpMethod::Post => self.client.post(&url),
            HttpMethod::Put => self.client.put(&url),
            HttpMethod::Patch => self.client.patch(&url),
            HttpMethod::Delete => self.client.delete(&url),
        };

        request = request.header("X-API-KEY", &self.api_key_public);

        if let Some(body_content) = body {
            request = request
                .header("Content-Type", "application/json")
                .body(body_content.to_string());
        }

        let response = request
            .send()
            .await
            .map_err(|e| DexError::Other(format!("Request failed: {}", e)))?;

        let status = response.status();
        let headers = response.headers().clone();

        // Detect WAF CAPTCHA / 429 / bare-405 and engage the host-shared
        // cooldown so all bots on this IP back off in lockstep.
        if let Some(src) = crate::lighter_waf_cooldown::classify_rate_limit(status, &headers) {
            let dur = crate::lighter_waf_cooldown::engage_cooldown(src);
            return Err(DexError::RateLimited {
                until_unix: chrono::Utc::now().timestamp() + dur.as_secs() as i64,
            });
        }

        if !status.is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            // Defensive body-based detection (in case headers were stripped).
            if let Some(src) =
                crate::lighter_waf_cooldown::classify_rate_limit_body(status, &error_text)
            {
                let dur = crate::lighter_waf_cooldown::engage_cooldown(src);
                return Err(DexError::RateLimited {
                    until_unix: chrono::Utc::now().timestamp() + dur.as_secs() as i64,
                });
            }
            return Err(DexError::Other(format!("HTTP {}: {}", status, error_text)));
        }

        response
            .json()
            .await
            .map_err(|e| DexError::Other(format!("Failed to parse response: {}", e)))
    }
}

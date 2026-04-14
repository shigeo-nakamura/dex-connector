//! Host-shared cooldown for Lighter rate-limit responses (HTTP 429 / 405).
//!
//! Background: there are two distinct enforcement layers that surface as
//! rate-limit responses on `mainnet.zklighter.elliot.ai`:
//!
//! 1. **AWS WAF CAPTCHA (per-IP rolling window)** — as of Lighter v4.0.53
//!    (2026-04-07), per-IP violations return `HTTP 405` with header
//!    `x-amzn-waf-action: captcha` and an AWS WAF challenge HTML body. The WAF
//!    window is per-IP and is reset on every additional violation, so a bot
//!    that retries during the cooldown refreshes the window and effectively
//!    never recovers. Full incident: bot-strategy#35.
//!
//! 2. **API-server rate limit** — effective 2026-04-17 (see bot-strategy#52
//!    and https://apidocs.lighter.xyz/docs/rate-limits#cooldown), the API
//!    itself may return **either `HTTP 429` or a bare `HTTP 405`** (no WAF
//!    header) when the weighted request budget is exhausted. Cooldown is 60s
//!    static for the firewall layer or `weightOfEndpoint/(totalWeight/60)` for
//!    the API-server layer.
//!
//! Because both layers can trip the same host simultaneously and the WAF window
//! is the more fragile one, we apply a single jittered 90-120s cooldown for any
//! of the above signals. That is over-cautious for an API-server 60s window
//! but safely clears the WAF window if both were tripped.
//!
//! False-positive risk: a genuine `405 Method Not Allowed` from a code bug
//! would also engage the cooldown. The resulting INFO log identifies the
//! source as `ApiRateLimit` so the operator can diagnose. Acceptable trade-off
//! vs. the previous behavior of retry-storming into a WAF lock-out.
//!
//! This module exposes:
//!   * [`classify_rate_limit`] — pure detection helper for an HTTP response;
//!     returns the source if rate-limited.
//!   * [`cooldown_remaining`] — non-blocking check used before sending any REST
//!     call. Returns `Some(_)` if a cooldown is currently active.
//!   * [`engage_cooldown`] — called when a rate-limit response is observed.
//!     Writes a unix-epoch expiry to a shared file under `/tmp` so all bot
//!     processes on the same host see the cooldown. Returns the duration set.
//!
//! The shared state is a single file containing a unix epoch (seconds) of the
//! cooldown deadline. Atomic writes use tmp-file-then-rename. Multiple writers
//! racing is harmless: the worst case is the deadline being slightly later than
//! either writer expected, which is exactly the safer behavior.

use reqwest::header::HeaderMap;
use reqwest::StatusCode;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Shared cooldown file. `/tmp` is world-readable/writable so any bot process on
/// the host can read/write it without elevated permissions.
const COOLDOWN_FILE: &str = "/tmp/lighter_waf_cooldown";

/// Minimum cooldown in seconds. The Lighter WAF rolling window is 60s, so 90s
/// gives a safety margin against clock skew and in-flight requests racing the
/// engagement.
const COOLDOWN_MIN_SECS: u64 = 90;

/// Maximum cooldown in seconds (jitter ceiling). Spreading the unblock moment
/// across bots prevents a synchronized re-burst that would immediately re-trip
/// the WAF.
const COOLDOWN_MAX_SECS: u64 = 120;

/// In-process memo of the last engaged deadline. Used purely to suppress
/// duplicate WARN log lines while a cooldown is active in this process.
static LAST_LOGGED_DEADLINE: AtomicI64 = AtomicI64::new(0);

/// Identifies which Lighter rate-limit layer produced the response, so that
/// the engagement log line points to the relevant runbook.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitSource {
    /// HTTP 405 with `x-amzn-waf-action: captcha` or the matching WAF body.
    /// Per-IP rolling window that resets on each violation — handled per #35.
    WafCaptcha,
    /// HTTP 429, or HTTP 405 without a WAF signal. Weighted API-server budget
    /// or firewall (60s static). Effective 2026-04-17 — handled per #52.
    ApiRateLimit,
}

impl RateLimitSource {
    fn label(&self) -> &'static str {
        match self {
            RateLimitSource::WafCaptcha => "WAF CAPTCHA (bot-strategy#35)",
            RateLimitSource::ApiRateLimit => "API rate-limit (bot-strategy#52)",
        }
    }
}

fn cooldown_path() -> PathBuf {
    PathBuf::from(COOLDOWN_FILE)
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn read_deadline() -> Option<i64> {
    let contents = fs::read_to_string(cooldown_path()).ok()?;
    contents.trim().parse::<i64>().ok()
}

fn write_deadline(deadline: i64) {
    let path = cooldown_path();
    let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
    if fs::write(&tmp, deadline.to_string()).is_ok() {
        let _ = fs::rename(&tmp, &path);
    }
}

/// Returns the remaining cooldown if one is currently active.
///
/// Reads the shared cooldown file every call. The cost is one stat + small
/// read; this is acceptable for the REST call rates we run at and avoids any
/// risk of stale per-process state.
pub fn cooldown_remaining() -> Option<Duration> {
    let deadline = read_deadline()?;
    let now = now_unix();
    if deadline > now {
        Some(Duration::from_secs((deadline - now) as u64))
    } else {
        None
    }
}

/// Pick a jittered cooldown length in `[COOLDOWN_MIN_SECS, COOLDOWN_MAX_SECS]`.
///
/// Uses nanosecond entropy from the system clock so we don't pull in a `rand`
/// dependency for one call site.
fn jittered_cooldown_secs() -> u64 {
    let extra = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0)
        % (COOLDOWN_MAX_SECS - COOLDOWN_MIN_SECS + 1);
    COOLDOWN_MIN_SECS + extra
}

/// Engage the cooldown. Idempotent: if a longer cooldown is already in effect,
/// the existing deadline wins. Logs a single WARN per engagement event in this
/// process, tagged with the observed [`RateLimitSource`]. Returns the duration
/// that is now in effect (which may be the pre-existing one).
pub fn engage_cooldown(source: RateLimitSource) -> Duration {
    let now = now_unix();
    let new_deadline = now + jittered_cooldown_secs() as i64;

    // Don't shorten an existing longer cooldown.
    let final_deadline = match read_deadline() {
        Some(existing) if existing > new_deadline => existing,
        _ => {
            write_deadline(new_deadline);
            new_deadline
        }
    };

    // Log once per engagement event in this process. We treat it as a "new
    // event" if the deadline we just observed is strictly later than the last
    // one this process logged about.
    let last_logged = LAST_LOGGED_DEADLINE.load(Ordering::Acquire);
    if final_deadline > last_logged {
        LAST_LOGGED_DEADLINE.store(final_deadline, Ordering::Release);
        let secs = (final_deadline - now).max(0);
        log::warn!(
            "[Lighter rate-limit] cooldown engaged for {}s (source={}, deadline unix={}, pid={}). All Lighter REST calls will fail-fast until then.",
            secs,
            source.label(),
            final_deadline,
            std::process::id()
        );
    }

    Duration::from_secs((final_deadline - now).max(0) as u64)
}

/// Detect whether an HTTP response from Lighter indicates a rate-limit
/// condition that should engage the cooldown, and classify which enforcement
/// layer produced it.
///
/// Returns:
///   * `Some(WafCaptcha)` — `405` with `x-amzn-waf-action: captcha`.
///   * `Some(ApiRateLimit)` — `429`, or any other `405` from Lighter. Per
///     https://apidocs.lighter.xyz/docs/rate-limits#cooldown and
///     bot-strategy#52 (effective 2026-04-17), the API layer may return either
///     status code for a rate-limit hit, with no distinguishing header.
///   * `None` — not rate-limited.
///
/// Use [`classify_rate_limit_body`] as a defensive fallback when only the body
/// is available (no header access).
pub fn classify_rate_limit(status: StatusCode, headers: &HeaderMap) -> Option<RateLimitSource> {
    if status == StatusCode::TOO_MANY_REQUESTS {
        return Some(RateLimitSource::ApiRateLimit);
    }
    if status == StatusCode::METHOD_NOT_ALLOWED {
        if let Some(v) = headers.get("x-amzn-waf-action") {
            if let Ok(s) = v.to_str() {
                if s.eq_ignore_ascii_case("captcha") {
                    return Some(RateLimitSource::WafCaptcha);
                }
            }
        }
        // Bare 405 without the WAF header: classify as API-layer rate limit
        // per #52. This may produce a false positive on a genuine "Method Not
        // Allowed" from a code bug — preferred over under-backoff, and the
        // source label in the engagement log makes it diagnosable.
        return Some(RateLimitSource::ApiRateLimit);
    }
    None
}

/// Defensive body-based detector. Some intermediaries strip headers but the
/// AWS WAF challenge HTML is recognizable. Use only when header inspection is
/// not possible (already consumed via `.text()`).
///
/// Returns the same variants as [`classify_rate_limit`]. A bare 405 (no WAF
/// markers in the body) is classified as `ApiRateLimit` per #52.
pub fn classify_rate_limit_body(status: StatusCode, body: &str) -> Option<RateLimitSource> {
    if status == StatusCode::TOO_MANY_REQUESTS {
        return Some(RateLimitSource::ApiRateLimit);
    }
    if status == StatusCode::METHOD_NOT_ALLOWED {
        if body.contains("awsWafIntegration") || body.contains("Human Verification") {
            return Some(RateLimitSource::WafCaptcha);
        }
        return Some(RateLimitSource::ApiRateLimit);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue};
    use std::sync::Mutex;

    // The cooldown file is global state shared by every test in the module, so
    // file-touching tests must run serially even when `cargo test` parallelizes.
    static FILE_LOCK: Mutex<()> = Mutex::new(());

    fn clear() {
        let _ = fs::remove_file(cooldown_path());
        LAST_LOGGED_DEADLINE.store(0, Ordering::Release);
    }

    #[test]
    fn detects_429_as_api_rate_limit() {
        let h = HeaderMap::new();
        assert_eq!(
            classify_rate_limit(StatusCode::TOO_MANY_REQUESTS, &h),
            Some(RateLimitSource::ApiRateLimit)
        );
    }

    #[test]
    fn detects_405_with_waf_header_as_waf_captcha() {
        let mut h = HeaderMap::new();
        h.insert("x-amzn-waf-action", HeaderValue::from_static("captcha"));
        assert_eq!(
            classify_rate_limit(StatusCode::METHOD_NOT_ALLOWED, &h),
            Some(RateLimitSource::WafCaptcha)
        );
    }

    #[test]
    fn detects_bare_405_as_api_rate_limit() {
        // Effective 2026-04-17 the API server itself may return a bare 405 for
        // rate limits (bot-strategy#52). Treat as rate-limit so we back off
        // rather than retry-storm into a WAF lock-out.
        let h = HeaderMap::new();
        assert_eq!(
            classify_rate_limit(StatusCode::METHOD_NOT_ALLOWED, &h),
            Some(RateLimitSource::ApiRateLimit)
        );
    }

    #[test]
    fn ignores_200() {
        let mut h = HeaderMap::new();
        h.insert("x-amzn-waf-action", HeaderValue::from_static("captcha"));
        assert_eq!(classify_rate_limit(StatusCode::OK, &h), None);
    }

    #[test]
    fn body_fallback_recognizes_aws_waf_html() {
        assert_eq!(
            classify_rate_limit_body(StatusCode::METHOD_NOT_ALLOWED, "<html>...awsWafIntegration..."),
            Some(RateLimitSource::WafCaptcha)
        );
        assert_eq!(
            classify_rate_limit_body(StatusCode::METHOD_NOT_ALLOWED, "Human Verification page"),
            Some(RateLimitSource::WafCaptcha)
        );
        // Bare 405 body → still classified as API rate-limit per #52.
        assert_eq!(
            classify_rate_limit_body(StatusCode::METHOD_NOT_ALLOWED, "ordinary 405 body"),
            Some(RateLimitSource::ApiRateLimit)
        );
    }

    #[test]
    fn engage_then_remaining_then_clear() {
        let _g = FILE_LOCK.lock().unwrap();
        clear();
        assert!(cooldown_remaining().is_none());
        let dur = engage_cooldown(RateLimitSource::ApiRateLimit);
        assert!(dur.as_secs() >= COOLDOWN_MIN_SECS);
        assert!(dur.as_secs() <= COOLDOWN_MAX_SECS);

        let remaining = cooldown_remaining().expect("cooldown should be active");
        assert!(remaining.as_secs() >= COOLDOWN_MIN_SECS - 1);
        assert!(remaining.as_secs() <= COOLDOWN_MAX_SECS);

        // Simulate expiry by writing a past deadline.
        write_deadline(now_unix() - 1);
        assert!(cooldown_remaining().is_none());
        clear();
    }

    #[test]
    fn engage_does_not_shorten_existing_cooldown() {
        let _g = FILE_LOCK.lock().unwrap();
        clear();
        // Plant a far-future deadline directly.
        let far = now_unix() + 600;
        write_deadline(far);
        let _ = engage_cooldown(RateLimitSource::WafCaptcha);
        let read_back = read_deadline().unwrap();
        assert_eq!(read_back, far, "engage must not shorten an existing cooldown");
        clear();
    }
}

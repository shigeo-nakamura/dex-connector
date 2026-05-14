//! Cross-venue WebSocket reconnect policy.
//!
//! Captures the shape of the wait-before-retry curve, jitter, and the
//! "long enough success period to reset the attempt counter" threshold
//! so each connector's reconnect loop reduces to a `policy.wait(attempt)`
//! call plus venue-specific state. Heartbeat / ping logic is *not* shared
//! — Lighter sends client-side pings on a dedicated task with miss
//! counting, while Extended (and most simple stream readers) only echo
//! server pings inline. The two patterns are too different to merge
//! without losing fidelity, so each connector keeps its own.

use rand::Rng;
use std::time::Duration;

/// How the delay grows with the attempt count.
#[derive(Debug, Clone, Copy)]
pub enum BackoffCurve {
    /// Same delay every attempt. Suitable when the upstream tolerates
    /// rapid retries and there is no per-IP rate limit pressure
    /// (e.g. Extended's 2 s flat sleep between stream restarts).
    Linear { delay_secs: f64 },
    /// `base.powi(attempt)` clamped to `max_secs`. Suitable for venues
    /// behind a WAF where back-to-back retries can trigger 429s or
    /// short IP bans (Lighter).
    Exponential {
        base: f64,
        max_secs: f64,
        /// Cap on the exponent so `base^n` doesn't blow up on a
        /// runaway attempt counter. Lighter has historically capped
        /// this at 12.
        attempt_cap: u32,
    },
}

/// Reconnect timing policy for a single connector.
///
/// Future DEX integrations can construct a custom policy directly; the
/// `lighter()` / `extended()` constructors capture the values that were
/// inlined in each connector before this module existed.
#[derive(Debug, Clone, Copy)]
pub struct WsReconnectPolicy {
    pub curve: BackoffCurve,
    /// Inclusive jitter range in ms added to every delay. `None` = no
    /// jitter. Lighter uses (0, 250) to spread retries across hosts.
    pub jitter_ms_range: Option<(u64, u64)>,
    /// If a connection stays up at least this long, the next call to
    /// `should_reset_attempt` returns true so the caller's attempt
    /// counter can be zeroed. `None` = never reset (e.g. Extended,
    /// which has no attempt counter to begin with).
    pub attempt_reset_after_secs: Option<u64>,
}

impl WsReconnectPolicy {
    /// Matches `lighter_connector::ws`'s previous inline
    /// `reconnect_backoff` (`BACKOFF_BASE=1.5`, `BACKOFF_MAX_SECS=60`,
    /// jitter 0..=250 ms, attempt counter reset after 300 s of stable
    /// connection).
    pub fn lighter() -> Self {
        Self {
            curve: BackoffCurve::Exponential {
                base: 1.5,
                max_secs: 60.0,
                attempt_cap: 12,
            },
            jitter_ms_range: Some((0, 250)),
            attempt_reset_after_secs: Some(300),
        }
    }

    /// Matches `extended_connector::mod`'s previous 2 s flat sleep
    /// between stream restarts. No jitter, no attempt counter.
    pub fn extended() -> Self {
        Self {
            curve: BackoffCurve::Linear { delay_secs: 2.0 },
            jitter_ms_range: None,
            attempt_reset_after_secs: None,
        }
    }

    /// Compute the wait before reconnect attempt `attempt` (1-indexed
    /// in callers, but `attempt = 0` is valid and returns the curve's
    /// base value).
    pub fn delay(&self, attempt: u32) -> Duration {
        let base_secs = match self.curve {
            BackoffCurve::Linear { delay_secs } => delay_secs,
            BackoffCurve::Exponential {
                base,
                max_secs,
                attempt_cap,
            } => {
                let exp = base.powi(attempt.min(attempt_cap) as i32);
                exp.min(max_secs)
            }
        };
        let mut dur = Duration::from_secs_f64(base_secs);
        if let Some((lo, hi)) = self.jitter_ms_range {
            let j: u64 = rand::thread_rng().gen_range(lo..=hi);
            dur += Duration::from_millis(j);
        }
        dur
    }

    /// Convenience: sleep for `delay(attempt)`.
    pub async fn wait(&self, attempt: u32) {
        tokio::time::sleep(self.delay(attempt)).await;
    }

    /// True iff `elapsed_secs` since the last reconnect crosses the
    /// reset threshold. Callers maintaining an attempt counter call
    /// this each tick and zero the counter on `true`.
    pub fn should_reset_attempt(&self, elapsed_secs: u64) -> bool {
        match self.attempt_reset_after_secs {
            Some(threshold) => elapsed_secs > threshold,
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linear_returns_fixed_delay_regardless_of_attempt() {
        let p = WsReconnectPolicy {
            curve: BackoffCurve::Linear { delay_secs: 2.0 },
            jitter_ms_range: None,
            attempt_reset_after_secs: None,
        };
        assert_eq!(p.delay(0), Duration::from_secs(2));
        assert_eq!(p.delay(1), Duration::from_secs(2));
        assert_eq!(p.delay(50), Duration::from_secs(2));
    }

    #[test]
    fn exponential_matches_old_lighter_math() {
        // The old Lighter inline `reconnect_backoff` computed
        // `1.5.powi(attempt.min(12)).min(60.0)` seconds. Spot-check a
        // few attempts against that formula.
        let p = WsReconnectPolicy {
            curve: BackoffCurve::Exponential {
                base: 1.5,
                max_secs: 60.0,
                attempt_cap: 12,
            },
            jitter_ms_range: None,
            attempt_reset_after_secs: None,
        };
        assert_eq!(p.delay(0), Duration::from_secs_f64(1.0));
        assert_eq!(p.delay(1), Duration::from_secs_f64(1.5));
        assert_eq!(p.delay(2), Duration::from_secs_f64(2.25));
        // 1.5^12 ≈ 129.7, clamped to 60.
        assert_eq!(p.delay(12), Duration::from_secs_f64(60.0));
        // Beyond cap, still clamped (and attempt_cap prevents f64 blow-up).
        assert_eq!(p.delay(1000), Duration::from_secs_f64(60.0));
    }

    #[test]
    fn jitter_stays_within_range() {
        let p = WsReconnectPolicy {
            curve: BackoffCurve::Linear { delay_secs: 1.0 },
            jitter_ms_range: Some((0, 250)),
            attempt_reset_after_secs: None,
        };
        for _ in 0..1000 {
            let d = p.delay(0);
            assert!(d >= Duration::from_secs(1));
            assert!(d <= Duration::from_secs(1) + Duration::from_millis(250));
        }
    }

    #[test]
    fn should_reset_attempt_honours_threshold() {
        let p = WsReconnectPolicy::lighter();
        assert!(!p.should_reset_attempt(0));
        assert!(!p.should_reset_attempt(300));
        assert!(p.should_reset_attempt(301));
        assert!(p.should_reset_attempt(86_400));
    }

    #[test]
    fn should_reset_attempt_false_when_disabled() {
        let p = WsReconnectPolicy::extended();
        assert!(!p.should_reset_attempt(0));
        assert!(!p.should_reset_attempt(86_400));
    }

    #[test]
    fn lighter_preset_matches_legacy_constants() {
        // Sanity: the preset reproduces every value the Lighter
        // connector inlined before this module existed.
        let p = WsReconnectPolicy::lighter();
        match p.curve {
            BackoffCurve::Exponential {
                base,
                max_secs,
                attempt_cap,
            } => {
                assert_eq!(base, 1.5);
                assert_eq!(max_secs, 60.0);
                assert_eq!(attempt_cap, 12);
            }
            _ => panic!("lighter preset must be Exponential"),
        }
        assert_eq!(p.jitter_ms_range, Some((0, 250)));
        assert_eq!(p.attempt_reset_after_secs, Some(300));
    }

    #[test]
    fn extended_preset_matches_legacy_constants() {
        let p = WsReconnectPolicy::extended();
        match p.curve {
            BackoffCurve::Linear { delay_secs } => assert_eq!(delay_secs, 2.0),
            _ => panic!("extended preset must be Linear"),
        }
        assert!(p.jitter_ms_range.is_none());
        assert!(p.attempt_reset_after_secs.is_none());
    }
}

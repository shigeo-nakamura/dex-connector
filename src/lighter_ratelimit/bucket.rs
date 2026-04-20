//! Weight-based token bucket shared across all clients on the host.
//!
//! - Capacity and refill rate match Lighter's Standard tier (60,000 weight/min).
//! - Refill is continuous and computed lazily on each acquire using a monotonic
//!   clock, so idle periods correctly replenish tokens without a background timer.
//! - `try_acquire` is the single source of truth: it atomically deducts weight
//!   or declines. `acquire_with_wait` layers a spin-sleep on top for Wait policy.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Capacity and refill parameters. Derived from Lighter's Standard tier spec:
///   60,000 weighted requests per 60s → 1,000 weight/s sustained, with 60,000
///   token headroom for bursts.
#[derive(Debug, Clone, Copy)]
pub struct BucketConfig {
    pub capacity: i64,
    pub refill_per_sec: i64,
}

impl Default for BucketConfig {
    fn default() -> Self {
        Self {
            capacity: 60_000,
            refill_per_sec: 1_000,
        }
    }
}

impl BucketConfig {
    pub fn from_env() -> Self {
        let capacity = std::env::var("LIGHTER_RATELIMIT_CAPACITY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(60_000);
        let refill_per_sec = std::env::var("LIGHTER_RATELIMIT_REFILL_PER_SEC")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1_000);
        Self {
            capacity,
            refill_per_sec,
        }
    }
}

/// Shared bucket state. Safe to `Arc<TokenBucket>` across tokio tasks.
///
/// Uses atomics rather than a `Mutex` so `try_acquire` is lock-free on the hot
/// path. The refill timestamp advances monotonically under CAS; the token
/// counter is adjusted with `fetch_*` ops.
pub struct TokenBucket {
    config: BucketConfig,
    /// Current token count. May briefly go negative under contention before
    /// being clamped back on the next refill; callers treat any value < weight
    /// as "insufficient".
    tokens: AtomicI64,
    /// Reference instant anchoring the monotonic clock. Nanoseconds since this
    /// anchor are stored in `last_refill_nanos`.
    anchor: Instant,
    /// Last time refill math was applied, in ns since `anchor`.
    last_refill_nanos: AtomicU64,
}

impl TokenBucket {
    pub fn new(config: BucketConfig) -> Self {
        Self {
            config,
            tokens: AtomicI64::new(config.capacity),
            anchor: Instant::now(),
            last_refill_nanos: AtomicU64::new(0),
        }
    }

    pub fn config(&self) -> BucketConfig {
        self.config
    }

    /// Returns the current token count (informational; may be stale by one CAS).
    pub fn tokens_remaining(&self) -> i64 {
        self.tokens.load(Ordering::Relaxed)
    }

    /// Advance the refill clock and credit accumulated tokens, capped at capacity.
    fn refill(&self) {
        let now_nanos = self.anchor.elapsed().as_nanos() as u64;
        let last = self.last_refill_nanos.load(Ordering::Relaxed);
        if now_nanos <= last {
            return;
        }
        // Only one task should do the refill per tick; the rest CAS-fail and skip.
        if self
            .last_refill_nanos
            .compare_exchange(last, now_nanos, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let delta_nanos = now_nanos - last;
        // Integer math: nanos * refill_per_sec / 1e9, keeping precision.
        let refill = (delta_nanos as i128 * self.config.refill_per_sec as i128)
            / 1_000_000_000i128;
        if refill <= 0 {
            return;
        }
        let refill = refill as i64;
        let cap = self.config.capacity;
        // Add, then clamp to capacity.
        let prev = self.tokens.fetch_add(refill, Ordering::AcqRel);
        let after = prev.saturating_add(refill);
        if after > cap {
            // Clamp by subtracting the overshoot. Another task may have run
            // in between, but the net overshoot is bounded by a single refill.
            let overshoot = after - cap;
            self.tokens.fetch_sub(overshoot, Ordering::AcqRel);
        }
    }

    /// Attempt to deduct `weight` tokens. Returns `true` on success.
    ///
    /// Rolls back on failure so a failed attempt does not briefly dip the counter
    /// below what actually happened — important when several concurrent callers
    /// race for the last few tokens.
    pub fn try_acquire(&self, weight: i64) -> bool {
        self.refill();
        let prev = self.tokens.fetch_sub(weight, Ordering::AcqRel);
        if prev >= weight {
            true
        } else {
            self.tokens.fetch_add(weight, Ordering::AcqRel);
            false
        }
    }

    /// Time in milliseconds until enough tokens could be available by refill
    /// alone (ignoring concurrent drains). Used to compute a minimum wait.
    pub fn estimated_wait_ms(&self, weight: i64) -> u64 {
        let remaining = self.tokens.load(Ordering::Relaxed);
        if remaining >= weight {
            return 0;
        }
        let deficit = (weight - remaining).max(0) as u64;
        // deficit tokens / refill_per_sec tokens_per_sec = seconds; convert to ms.
        (deficit * 1000).div_ceil(self.config.refill_per_sec.max(1) as u64)
    }
}

/// Sleep helper used by the async acquire path. Kept here so it can be swapped
/// in tests (constant-time virtual clock) without touching policy logic.
pub async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket(capacity: i64, refill: i64) -> TokenBucket {
        TokenBucket::new(BucketConfig {
            capacity,
            refill_per_sec: refill,
        })
    }

    #[test]
    fn fresh_bucket_is_full() {
        let b = bucket(1000, 100);
        assert_eq!(b.tokens_remaining(), 1000);
    }

    #[test]
    fn acquire_deducts_on_success() {
        let b = bucket(1000, 100);
        assert!(b.try_acquire(300));
        assert!(b.try_acquire(300));
        assert_eq!(b.tokens_remaining(), 400);
    }

    #[test]
    fn acquire_declines_and_rolls_back_when_insufficient() {
        let b = bucket(1000, 100);
        assert!(b.try_acquire(800));
        assert!(!b.try_acquire(300));
        // Rolled back to 200, not -100.
        assert_eq!(b.tokens_remaining(), 200);
    }

    // Time-based tests use real sleeps because the bucket relies on
    // `std::time::Instant`, which is not affected by tokio's test clock.
    // Tolerances are loose enough to survive CI scheduling jitter.

    #[tokio::test]
    async fn refill_replenishes_over_time() {
        let b = bucket(1000, 1000); // 1,000 per second
        assert!(b.try_acquire(1000));
        assert_eq!(b.tokens_remaining(), 0);
        tokio::time::sleep(Duration::from_millis(600)).await;
        // After ~600ms at 1000/s, we should have ~600 tokens (allowing drift).
        assert!(b.try_acquire(400), "expected at least 400 tokens after 600ms");
    }

    #[tokio::test]
    async fn refill_does_not_overflow_capacity() {
        let b = bucket(1000, 1000);
        assert!(b.try_acquire(500));
        tokio::time::sleep(Duration::from_millis(2000)).await;
        b.refill();
        assert_eq!(
            b.tokens_remaining(),
            1000,
            "refill should saturate at capacity"
        );
    }

    #[test]
    fn estimated_wait_zero_when_already_enough() {
        let b = bucket(1000, 100);
        assert_eq!(b.estimated_wait_ms(300), 0);
    }

    #[test]
    fn estimated_wait_scales_with_deficit_and_refill_rate() {
        let b = bucket(1000, 1000);
        assert!(b.try_acquire(1000));
        // 300 token deficit at 1000/s → 300ms
        assert_eq!(b.estimated_wait_ms(300), 300);
    }
}

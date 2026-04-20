//! UDS client for the Lighter rate-limit sidecar, with an in-process fallback
//! when the sidecar is unreachable.
//!
//! Design intent: every Lighter REST call in dex-connector acquires weight
//! before firing. Doing this out-of-process lets all bots on the same host
//! share one bucket (the 60k weight/min cap is per-IP). If the sidecar is
//! missing (dev machine, BT runs, or crashed), we still get coarse per-process
//! protection — safer than falling open.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::Mutex;

use super::bucket::{sleep_ms, BucketConfig, TokenBucket};
use super::protocol::{AcquirePolicy, AcquireRequest, AcquireResponse};

/// Default socket path matching the systemd RuntimeDirectory.
pub const DEFAULT_SOCKET_PATH: &str = "/run/lighter-ratelimit/lighter-ratelimit.sock";

/// Outcome of an acquire, exposed to callers. Mirrors the wire response.
#[derive(Debug, Clone, Copy)]
pub enum AcquireOutcome {
    Granted { waited_ms: u64 },
    Shed,
}

impl AcquireOutcome {
    pub fn is_granted(self) -> bool {
        matches!(self, AcquireOutcome::Granted { .. })
    }
}

/// Client interface. Cheap to `clone()` — internals are `Arc`-shared.
#[derive(Clone)]
pub struct RateLimitClient {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    socket_path: Option<PathBuf>,
    /// In-process fallback bucket, used when the sidecar is unreachable or
    /// disabled via env. Always present so the client can never deadlock the
    /// process on a missing sidecar.
    fallback: TokenBucket,
    /// Serialize sidecar writes. Only one request flies at a time per socket
    /// so response framing stays trivial (request-response over a shared
    /// connection is harder than per-request reconnect; reconnect is fine
    /// because UDS connect is <100us).
    sidecar_write_lock: Mutex<()>,
    /// Toggled off once we've seen the sidecar fail; avoids per-call EEXIST
    /// spam. A recovery probe path could be added later, but for now clients
    /// can restart to re-enable sidecar use.
    disabled: std::sync::atomic::AtomicBool,
}

impl RateLimitClient {
    /// Build from env. Set `LIGHTER_RATELIMIT_DISABLE=1` to skip both sidecar
    /// and fallback (useful for BT and offline development where rate-limits
    /// are not a concern).
    pub fn from_env() -> Self {
        let disabled_env = std::env::var("LIGHTER_RATELIMIT_DISABLE")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let socket_path = if disabled_env {
            None
        } else {
            Some(
                std::env::var("LIGHTER_RATELIMIT_SOCKET")
                    .unwrap_or_else(|_| DEFAULT_SOCKET_PATH.to_string())
                    .into(),
            )
        };
        let config = BucketConfig::from_env();
        Self {
            inner: Arc::new(ClientInner {
                socket_path,
                fallback: TokenBucket::new(config),
                sidecar_write_lock: Mutex::new(()),
                disabled: std::sync::atomic::AtomicBool::new(disabled_env),
            }),
        }
    }

    /// Convenience constructor for tests and the daemon itself.
    pub fn fallback_only(config: BucketConfig) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                socket_path: None,
                fallback: TokenBucket::new(config),
                sidecar_write_lock: Mutex::new(()),
                disabled: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }

    /// Constructor that bypasses env vars. Primarily for tests where
    /// parallel execution would otherwise race on the shared
    /// `LIGHTER_RATELIMIT_SOCKET` variable.
    pub fn with_socket<P: Into<PathBuf>>(path: P, fallback_config: BucketConfig) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                socket_path: Some(path.into()),
                fallback: TokenBucket::new(fallback_config),
                sidecar_write_lock: Mutex::new(()),
                disabled: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }

    /// True if acquire always grants without contacting sidecar or bucket.
    /// Only set via `LIGHTER_RATELIMIT_DISABLE=1`. In that mode REST calls
    /// proceed without any rate limiting — useful for BT, harmful in prod.
    pub fn is_fully_disabled(&self) -> bool {
        self.inner.socket_path.is_none()
            && self
                .inner
                .disabled
                .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Acquire `weight` tokens per the given policy.
    ///
    /// Fast path: sidecar accepts the request and responds. Fallback path:
    /// sidecar unreachable → use in-process bucket. Disabled path:
    /// `LIGHTER_RATELIMIT_DISABLE=1` → grant immediately with 0 wait.
    pub async fn acquire(
        &self,
        weight: u32,
        policy: AcquirePolicy,
        tag: Option<&str>,
    ) -> AcquireOutcome {
        if self.is_fully_disabled() {
            return AcquireOutcome::Granted { waited_ms: 0 };
        }

        // Try sidecar unless known-bad. Any error transparently downgrades to
        // the in-process bucket for this call (and flips `disabled` so future
        // calls skip the attempt until the process restarts).
        if let Some(path) = &self.inner.socket_path {
            if !self
                .inner
                .disabled
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                match self.acquire_via_sidecar(path, weight, policy, tag).await {
                    Ok(resp) => {
                        return if resp.granted {
                            AcquireOutcome::Granted {
                                waited_ms: resp.waited_ms,
                            }
                        } else {
                            AcquireOutcome::Shed
                        };
                    }
                    Err(e) => {
                        log::warn!(
                            "[lighter-ratelimit] sidecar unreachable ({e}); falling back \
                             to in-process bucket for this client instance"
                        );
                        self.inner
                            .disabled
                            .store(true, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        }

        // Fallback in-process bucket.
        self.acquire_via_fallback(weight as i64, policy).await
    }

    async fn acquire_via_sidecar(
        &self,
        path: &std::path::Path,
        weight: u32,
        policy: AcquirePolicy,
        tag: Option<&str>,
    ) -> std::io::Result<AcquireResponse> {
        // Short connect timeout — if the sidecar is gone we want to know fast
        // so the fallback kicks in before the REST call is due.
        let stream = tokio::time::timeout(
            Duration::from_millis(200),
            UnixStream::connect(path),
        )
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timeout"))??;

        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        let req = AcquireRequest {
            weight,
            tag: tag.map(|s| s.to_string()),
            policy,
        };
        let mut line = serde_json::to_vec(&req)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        line.push(b'\n');

        // Serialize writes per-client so we don't interleave requests on a
        // shared future; per-call this is a fresh connection so contention is
        // minimal, but the lock is free insurance.
        let _guard = self.inner.sidecar_write_lock.lock().await;
        write_half.write_all(&line).await?;
        write_half.flush().await?;

        let read_deadline =
            Duration::from_millis(policy.max_wait_ms().saturating_add(2_000).max(500));
        let mut response_line = String::new();
        tokio::time::timeout(read_deadline, reader.read_line(&mut response_line))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "response timeout"))??;

        let resp: AcquireResponse = serde_json::from_str(response_line.trim())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(resp)
    }

    async fn acquire_via_fallback(&self, weight: i64, policy: AcquirePolicy) -> AcquireOutcome {
        // Fast path: enough tokens right now.
        if self.inner.fallback.try_acquire(weight) {
            return AcquireOutcome::Granted { waited_ms: 0 };
        }
        match policy {
            AcquirePolicy::Shed => AcquireOutcome::Shed,
            AcquirePolicy::Wait { max_ms } => {
                let started = Instant::now();
                // Poll-with-backoff: estimated wait, then retry. Avoids sleeping
                // longer than necessary if other callers return tokens.
                loop {
                    let est = self.inner.fallback.estimated_wait_ms(weight);
                    let elapsed = started.elapsed().as_millis() as u64;
                    if elapsed >= max_ms {
                        return AcquireOutcome::Shed;
                    }
                    let remaining_budget = max_ms - elapsed;
                    // Sleep at most `remaining_budget`, and at most 100ms to stay
                    // responsive to other drains/refills.
                    let sleep_for = est.min(remaining_budget).min(100).max(1);
                    sleep_ms(sleep_for).await;
                    if self.inner.fallback.try_acquire(weight) {
                        return AcquireOutcome::Granted {
                            waited_ms: started.elapsed().as_millis() as u64,
                        };
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn disabled_client_always_grants() {
        std::env::set_var("LIGHTER_RATELIMIT_DISABLE", "1");
        let client = RateLimitClient::from_env();
        assert!(client.is_fully_disabled());
        let outcome = client.acquire(60_000, AcquirePolicy::Shed, None).await;
        assert!(outcome.is_granted());
        std::env::remove_var("LIGHTER_RATELIMIT_DISABLE");
    }

    #[tokio::test]
    async fn fallback_shed_when_empty() {
        let client = RateLimitClient::fallback_only(BucketConfig {
            capacity: 100,
            refill_per_sec: 10,
        });
        let g = client.acquire(100, AcquirePolicy::Shed, None).await;
        assert!(g.is_granted());
        let s = client.acquire(50, AcquirePolicy::Shed, None).await;
        assert!(!s.is_granted());
    }

    #[tokio::test]
    async fn fallback_wait_eventually_grants() {
        let client = RateLimitClient::fallback_only(BucketConfig {
            capacity: 100,
            refill_per_sec: 500, // 500 tokens/sec — fast refill for a quick test
        });
        assert!(client.acquire(100, AcquirePolicy::Shed, None).await.is_granted());
        // Need 50 tokens @ 500/s ≈ 100ms wait; give 1s budget.
        let outcome = client
            .acquire(50, AcquirePolicy::Wait { max_ms: 1_000 }, None)
            .await;
        assert!(
            outcome.is_granted(),
            "expected grant after refill, got {:?}",
            outcome
        );
    }

    #[tokio::test]
    async fn fallback_wait_sheds_on_timeout() {
        let client = RateLimitClient::fallback_only(BucketConfig {
            capacity: 100,
            refill_per_sec: 1, // 1 token/sec — very slow refill
        });
        assert!(client.acquire(100, AcquirePolicy::Shed, None).await.is_granted());
        // Need 50 tokens @ 1/s = 50s, but budget is 50ms → must shed.
        let outcome = client
            .acquire(50, AcquirePolicy::Wait { max_ms: 50 }, None)
            .await;
        assert!(!outcome.is_granted());
    }
}

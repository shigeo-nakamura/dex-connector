//! Lighter rate-limit sidecar daemon.
//!
//! One instance per EC2 host. All bots on the same IP share a single token
//! bucket (60k weight/min by default). Listens on a UDS and responds to
//! line-delimited JSON `AcquireRequest`s with `AcquireResponse`s.
//!
//! Deploy as a systemd unit (see `deploy/lighter-ratelimit.service`). Graceful
//! shutdown on SIGINT/SIGTERM lets systemd cycle the process without stranding
//! clients — any in-flight acquire finishes or times out naturally.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

use dex_connector::lighter_ratelimit::{
    bucket::{sleep_ms, BucketConfig, TokenBucket},
    client::DEFAULT_SOCKET_PATH,
    protocol::{AcquirePolicy, AcquireRequest, AcquireResponse},
};

/// Accumulated counters for periodic stats logging.
struct Stats {
    weight_granted: AtomicU64,
    shed_count: AtomicU64,
    wait_count: AtomicU64,
    wait_total_ms: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Self {
            weight_granted: AtomicU64::new(0),
            shed_count: AtomicU64::new(0),
            wait_count: AtomicU64::new(0),
            wait_total_ms: AtomicU64::new(0),
        }
    }

    fn record_grant(&self, weight: u32, waited_ms: u64) {
        self.weight_granted
            .fetch_add(weight as u64, Ordering::Relaxed);
        if waited_ms > 0 {
            self.wait_count.fetch_add(1, Ordering::Relaxed);
            self.wait_total_ms
                .fetch_add(waited_ms, Ordering::Relaxed);
        }
    }

    fn record_shed(&self) {
        self.shed_count.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    // Simple stderr logger driven by RUST_LOG; systemd captures stderr.
    let _ = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info"),
    )
    .format_timestamp_secs()
    .try_init();

    let socket_path: PathBuf = std::env::var("LIGHTER_RATELIMIT_SOCKET")
        .unwrap_or_else(|_| DEFAULT_SOCKET_PATH.to_string())
        .into();
    let config = BucketConfig::from_env();
    log::info!(
        "[lighter-ratelimit] starting on {} (capacity={}, refill={}/s)",
        socket_path.display(),
        config.capacity,
        config.refill_per_sec
    );

    if let Some(parent) = socket_path.parent() {
        if !parent.exists() {
            // systemd's `RuntimeDirectory=` should have created this, but if
            // running outside systemd (local dev) make a best-effort mkdir.
            std::fs::create_dir_all(parent).ok();
        }
    }
    // Remove a stale socket file from a prior run; systemd handles this too.
    if socket_path.exists() {
        std::fs::remove_file(&socket_path).ok();
    }

    let listener = UnixListener::bind(&socket_path)?;
    // Permissions: loose enough for the bot user on the host. Adjust in the
    // systemd unit via `SocketMode=` if a shared group is required.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(
            &socket_path,
            std::fs::Permissions::from_mode(0o666),
        );
    }

    let bucket = Arc::new(TokenBucket::new(config));
    let stats = Arc::new(Stats::new());

    // Stats reporter — one line per 60s, counters reset each window.
    {
        let stats = Arc::clone(&stats);
        let bucket = Arc::clone(&bucket);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            interval.tick().await; // skip the immediate fire
            loop {
                interval.tick().await;
                let weight = stats.weight_granted.swap(0, Ordering::Relaxed);
                let shed = stats.shed_count.swap(0, Ordering::Relaxed);
                let waits = stats.wait_count.swap(0, Ordering::Relaxed);
                let wait_ms_total = stats.wait_total_ms.swap(0, Ordering::Relaxed);
                let avg_wait = if waits > 0 { wait_ms_total / waits } else { 0 };
                log::info!(
                    "[lighter-ratelimit] stats 60s: weight_granted={} ({:.1}/s) \
                     shed={} waits={} avg_wait_ms={} tokens_remaining={}",
                    weight,
                    weight as f64 / 60.0,
                    shed,
                    waits,
                    avg_wait,
                    bucket.tokens_remaining(),
                );
            }
        });
    }

    // Shutdown signal handler — SIGINT/SIGTERM cancel the accept loop so the
    // bind drops and the socket file is removed on exit.
    let shutdown_signal = async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
            let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");
            tokio::select! {
                _ = sigterm.recv() => log::info!("[lighter-ratelimit] SIGTERM received"),
                _ = sigint.recv() => log::info!("[lighter-ratelimit] SIGINT received"),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    };

    let serve = async {
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let bucket = Arc::clone(&bucket);
                    let stats = Arc::clone(&stats);
                    tokio::spawn(handle_connection(stream, bucket, stats));
                }
                Err(e) => {
                    log::warn!("[lighter-ratelimit] accept error: {e}");
                    // Back off briefly on accept storms (fd exhaustion, etc.).
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    };

    tokio::select! {
        _ = shutdown_signal => {}
        _ = serve => {}
    }

    // Best-effort cleanup; systemd will also remove the RuntimeDirectory.
    std::fs::remove_file(&socket_path).ok();
    log::info!("[lighter-ratelimit] shutdown complete");
    Ok(())
}

/// Handle one client connection. A client may send multiple requests on the
/// same connection; responses are written in order.
async fn handle_connection(stream: UnixStream, bucket: Arc<TokenBucket>, stats: Arc<Stats>) {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => break, // client closed
            Ok(_) => {}
            Err(e) => {
                log::debug!("[lighter-ratelimit] read error: {e}");
                break;
            }
        }
        let req: AcquireRequest = match serde_json::from_str(line.trim()) {
            Ok(r) => r,
            Err(e) => {
                log::warn!("[lighter-ratelimit] invalid request: {e}");
                // Send a well-formed shed so the client's fallback kicks in
                // rather than hanging on read.
                let resp = AcquireResponse::shed(bucket.tokens_remaining());
                write_response(&mut write_half, &resp).await.ok();
                continue;
            }
        };
        let resp = handle_acquire(&bucket, &stats, req).await;
        if write_response(&mut write_half, &resp).await.is_err() {
            break;
        }
    }
}

async fn write_response(
    write_half: &mut tokio::net::unix::OwnedWriteHalf,
    resp: &AcquireResponse,
) -> std::io::Result<()> {
    let mut buf = serde_json::to_vec(resp)?;
    buf.push(b'\n');
    write_half.write_all(&buf).await?;
    write_half.flush().await
}

async fn handle_acquire(
    bucket: &TokenBucket,
    stats: &Stats,
    req: AcquireRequest,
) -> AcquireResponse {
    let weight = req.weight as i64;
    if bucket.try_acquire(weight) {
        stats.record_grant(req.weight, 0);
        return AcquireResponse::granted(0, bucket.tokens_remaining());
    }
    match req.policy {
        AcquirePolicy::Shed => {
            stats.record_shed();
            AcquireResponse::shed(bucket.tokens_remaining())
        }
        AcquirePolicy::Wait { max_ms } => {
            let started = Instant::now();
            loop {
                let elapsed = started.elapsed().as_millis() as u64;
                if elapsed >= max_ms {
                    stats.record_shed();
                    return AcquireResponse::shed(bucket.tokens_remaining());
                }
                let est = bucket.estimated_wait_ms(weight);
                // Cap per-iteration sleep at 100ms so responsiveness stays high
                // even if many callers share the deficit.
                let budget = max_ms - elapsed;
                let sleep_for = est.min(budget).min(100).max(1);
                sleep_ms(sleep_for).await;
                if bucket.try_acquire(weight) {
                    let waited = started.elapsed().as_millis() as u64;
                    stats.record_grant(req.weight, waited);
                    return AcquireResponse::granted(waited, bucket.tokens_remaining());
                }
            }
        }
    }
}

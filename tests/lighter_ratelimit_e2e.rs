//! End-to-end test of the sidecar protocol: spin up a minimal server using
//! the same logic the daemon uses, connect real `RateLimitClient` instances
//! over UDS, and verify the shared-bucket invariants that motivate #79.

#![cfg(feature = "lighter-sdk")]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;

use dex_connector::lighter_ratelimit::{
    bucket::{BucketConfig, TokenBucket},
    client::RateLimitClient,
    protocol::{AcquirePolicy, AcquireRequest, AcquireResponse},
};

/// Minimal test-only server that mirrors the production handler loop. Kept
/// here rather than exported from the daemon binary because tests shouldn't
/// depend on `src/bin` layout.
async fn serve(listener: UnixListener, bucket: Arc<TokenBucket>) {
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(s) => s,
            Err(_) => return,
        };
        let bucket = Arc::clone(&bucket);
        tokio::spawn(async move {
            let (r, mut w) = stream.into_split();
            let mut reader = BufReader::new(r);
            let mut line = String::new();
            loop {
                line.clear();
                if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                    break;
                }
                let req: AcquireRequest = match serde_json::from_str(line.trim()) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let weight = req.weight as i64;
                let resp = if bucket.try_acquire(weight) {
                    AcquireResponse::granted(0, bucket.tokens_remaining())
                } else {
                    match req.policy {
                        AcquirePolicy::Shed => AcquireResponse::shed(bucket.tokens_remaining()),
                        AcquirePolicy::Wait { max_ms } => {
                            let started = std::time::Instant::now();
                            loop {
                                let elapsed = started.elapsed().as_millis() as u64;
                                if elapsed >= max_ms {
                                    break AcquireResponse::shed(bucket.tokens_remaining());
                                }
                                tokio::time::sleep(Duration::from_millis(10)).await;
                                if bucket.try_acquire(weight) {
                                    break AcquireResponse::granted(
                                        started.elapsed().as_millis() as u64,
                                        bucket.tokens_remaining(),
                                    );
                                }
                            }
                        }
                    }
                };
                let mut buf = serde_json::to_vec(&resp).unwrap();
                buf.push(b'\n');
                if w.write_all(&buf).await.is_err() {
                    break;
                }
                let _ = w.flush().await;
            }
        });
    }
}

fn make_socket_path(name: &str) -> std::path::PathBuf {
    let pid = std::process::id();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("lighter-ratelimit-{name}-{pid}-{ts}.sock"))
}

fn spawn_server(capacity: i64, refill: i64, socket_path: &std::path::Path) {
    if socket_path.exists() {
        let _ = std::fs::remove_file(socket_path);
    }
    let listener = UnixListener::bind(socket_path).expect("bind UDS");
    let bucket = Arc::new(TokenBucket::new(BucketConfig {
        capacity,
        refill_per_sec: refill,
    }));
    tokio::spawn(serve(listener, bucket));
}

fn make_client(socket_path: &std::path::Path) -> RateLimitClient {
    // Bypass env vars so parallel tests don't race on LIGHTER_RATELIMIT_SOCKET.
    RateLimitClient::with_socket(
        socket_path,
        BucketConfig {
            capacity: 1_000_000, // large fallback so a sidecar glitch doesn't mask real shed behavior
            refill_per_sec: 1_000_000,
        },
    )
}

#[tokio::test]
async fn sidecar_grants_and_sheds_on_capacity() {
    let socket = make_socket_path("basic");
    spawn_server(1_000, 100, &socket);
    tokio::time::sleep(Duration::from_millis(30)).await;
    let client = make_client(&socket);

    // Drain the bucket with Shed policy.
    let r1 = client.acquire(800, AcquirePolicy::Shed, Some("test")).await;
    assert!(r1.is_granted(), "first 800 should be granted");
    let r2 = client.acquire(300, AcquirePolicy::Shed, Some("test")).await;
    assert!(!r2.is_granted(), "300 more should be shed (only 200 left)");
    let r3 = client.acquire(200, AcquirePolicy::Shed, Some("test")).await;
    assert!(r3.is_granted(), "the remaining 200 fits");
}

#[tokio::test]
async fn multiple_clients_share_one_bucket() {
    // The point of the sidecar: two RateLimitClient instances on the same
    // host must not each think they have their own 60k budget. They share
    // one bucket via the UDS.
    let socket = make_socket_path("shared");
    spawn_server(1_000, 100, &socket);
    tokio::time::sleep(Duration::from_millis(30)).await;

    let c1 = make_client(&socket);
    let c2 = make_client(&socket);

    // Each client tries to grab 600. First one wins 600, second is shed.
    let (r1, r2) = tokio::join!(
        c1.acquire(600, AcquirePolicy::Shed, Some("c1")),
        c2.acquire(600, AcquirePolicy::Shed, Some("c2")),
    );
    let granted = [r1, r2].iter().filter(|o| o.is_granted()).count();
    assert_eq!(
        granted, 1,
        "exactly one of two concurrent 600-weight acquires must be granted \
         against a 1000-capacity shared bucket"
    );
}

#[tokio::test]
async fn wait_policy_queues_until_refill() {
    // Fast refill so the test runs in <1s.
    let socket = make_socket_path("wait");
    spawn_server(100, 500, &socket); // 100 capacity, 500/s refill
    tokio::time::sleep(Duration::from_millis(30)).await;
    let client = make_client(&socket);

    assert!(client
        .acquire(100, AcquirePolicy::Shed, None)
        .await
        .is_granted());
    let r = client
        .acquire(60, AcquirePolicy::Wait { max_ms: 1_000 }, None)
        .await;
    assert!(r.is_granted(), "wait should eventually grant after refill");
}

#[tokio::test]
async fn unreachable_sidecar_falls_back_in_process() {
    // Point the client at a socket path that does not exist; the first
    // acquire should log a warning, flip to fallback, and then grant.
    let bogus = std::env::temp_dir().join(format!(
        "nonexistent-ratelimit-{}.sock",
        std::process::id()
    ));
    if bogus.exists() {
        let _ = std::fs::remove_file(&bogus);
    }
    std::env::set_var("LIGHTER_RATELIMIT_SOCKET", bogus.to_string_lossy().to_string());
    std::env::remove_var("LIGHTER_RATELIMIT_DISABLE");
    let client = RateLimitClient::from_env();
    let r = client.acquire(300, AcquirePolicy::Shed, None).await;
    assert!(
        r.is_granted(),
        "fallback bucket should grant when sidecar is unreachable"
    );
}

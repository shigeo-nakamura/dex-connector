//! Connector lifecycle helpers: auto-cleanup, maintenance refresh, and
//! observed-outage transitions.

use super::*;

impl LighterConnector {
    /// Start auto-cleanup background task for filled orders
    /// Removes orders older than specified duration to prevent memory bloat
    pub fn start_auto_cleanup(&self, cleanup_interval_hours: u64) {
        if self.cleanup_started.swap(true, Ordering::SeqCst) {
            log::debug!("[AUTO_CLEANUP] already started; ignoring.");
            return;
        }

        log::info!(
            "[AUTO_CLEANUP] Starting background task (interval: {}h)",
            cleanup_interval_hours
        );

        let filled_orders = Arc::clone(&self.filled_orders);
        let canceled_orders = Arc::clone(&self.canceled_orders);
        let is_running = Arc::clone(&self.is_running);
        let cleanup_started = Arc::clone(&self.cleanup_started);
        let cleanup_handle = Arc::clone(&self.cleanup_handle);

        let handle = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(cleanup_interval_hours * 3600));
            // Skip the first immediate tick to delay initial cleanup
            interval.tick().await;

            while is_running.load(Ordering::Relaxed) {
                interval.tick().await;

                let mut filled_removed = 0usize;
                let mut canceled_removed = 0usize;

                // Clean up filled orders - simple approach since FilledOrder doesn't have timestamp
                {
                    let mut filled = filled_orders.write().await;
                    for (symbol, orders) in filled.iter_mut() {
                        const KEEP_FILLED_PER_SYMBOL: usize = 50;
                        if orders.len() > KEEP_FILLED_PER_SYMBOL {
                            let remove_count = orders.len() - KEEP_FILLED_PER_SYMBOL;
                            // Assumes first elements are oldest (order insertion maintains chronological order)
                            orders.drain(0..remove_count);
                            filled_removed += remove_count;
                            log::debug!(
                                "🗑️ [AUTO_CLEANUP] Removed {} old filled orders for {} (kept {})",
                                remove_count,
                                symbol,
                                KEEP_FILLED_PER_SYMBOL
                            );
                        }
                    }
                    // Remove empty symbol entries
                    filled.retain(|_, orders| !orders.is_empty());
                }

                // Clean up canceled orders older than 24 hours
                {
                    let mut canceled = canceled_orders.write().await;
                    // NOTE: canceled_timestamp is seconds since epoch (not milliseconds)
                    let cutoff_secs = (Utc::now() - ChronoDuration::hours(24)).timestamp() as u64;

                    for (symbol, orders) in canceled.iter_mut() {
                        let initial_len = orders.len();
                        // Keep orders newer than 24 hours (timestamp > cutoff means newer)
                        orders.retain(|order| order.canceled_timestamp > cutoff_secs);
                        let removed = initial_len.saturating_sub(orders.len());
                        canceled_removed += removed;

                        if removed > 0 {
                            log::debug!(
                                "🗑️ [AUTO_CLEANUP] Removed {} old canceled orders for {}",
                                removed,
                                symbol
                            );
                        }
                    }
                    // Remove empty symbol entries
                    canceled.retain(|_, orders| !orders.is_empty());
                }

                let total_removed = filled_removed + canceled_removed;
                if total_removed > 0 {
                    log::info!(
                        "🗑️ [AUTO_CLEANUP] removed total={} (filled={}, canceled={})",
                        total_removed,
                        filled_removed,
                        canceled_removed
                    );
                }
            }

            // Cleanup on exit: reset state for potential restart
            cleanup_started.store(false, Ordering::SeqCst);
            let mut guard = cleanup_handle.lock().await;
            *guard = None;
            log::info!("🛑 [AUTO_CLEANUP] task exited, ready for restart");
        });

        // Store the handle using async context
        let cleanup_handle_for_storage = Arc::clone(&self.cleanup_handle);
        tokio::spawn(async move {
            let mut guard = cleanup_handle_for_storage.lock().await;
            *guard = Some(handle);
        });
    }

    pub(super) fn record_outage_failure(&self, signal: OutageSignal) {
        let transition = {
            let mut detector = self
                .outage_detector
                .lock()
                .expect("outage detector poisoned");
            detector.record_failure(signal, Instant::now())
        };
        if let Some(OutageTransition::Latched { reason }) = transition {
            log::warn!(
                "[MAINTENANCE] Lighter degraded-mode latched via observed {} failures",
                reason
            );
        }
    }

    pub(super) fn record_outage_success(&self, signal: OutageSignal) {
        let transition = {
            let mut detector = self
                .outage_detector
                .lock()
                .expect("outage detector poisoned");
            detector.record_success(signal, Instant::now())
        };
        if let Some(OutageTransition::Cleared { reason }) = transition {
            log::info!(
                "[MAINTENANCE] Lighter degraded-mode cleared after {} recovery",
                reason
            );
        }
    }

    /// Start the background refresher for the Lighter status-page maintenance
    /// feed. The previous design awaited `fetch_next_maintenance_window`
    /// inline from `is_upcoming_maintenance` (which is on the strategy's hot
    /// `step()` path), so a slow status.lighter.xyz response — that endpoint
    /// is a single-IP backend with no Anycast/CDN failover — could blow the
    /// 7.5s STEP_OVERRUN budget or, with default `connect_timeout(5s)`, leak
    /// a WARN line. Move the fetch off the hot path entirely. See
    /// bot-strategy#160.
    pub fn start_maintenance_refresher(&self) {
        // Operator kill-switch: if maintenance handling is disabled, never
        // spawn the task. `is_upcoming_maintenance` returns false when the
        // cache is empty, which matches the disabled semantics.
        if matches!(
            std::env::var("LIGHTER_MAINTENANCE_DISABLED").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE")
        ) {
            log::info!("[MAINTENANCE] LIGHTER_MAINTENANCE_DISABLED set, skipping refresher spawn");
            return;
        }

        if self
            .maintenance_refresher_started
            .swap(true, Ordering::SeqCst)
        {
            log::debug!("[MAINTENANCE] refresher already started; ignoring.");
            return;
        }

        // Dedicated client with tight timeouts. status.lighter.xyz is a
        // third-party CDN we don't control, so cap the worst-case stall at
        // a few seconds rather than the trading-client's 15s budget. Connect
        // timeout is the long pole — `error trying to connect: operation
        // timed out` was the failure mode observed on 2026-04-22 17:23.
        let client = match Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(5))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                log::warn!(
                    "[MAINTENANCE] failed to build refresher HTTP client: {} — refresher disabled",
                    e
                );
                self.maintenance_refresher_started
                    .store(false, Ordering::SeqCst);
                return;
            }
        };

        let maintenance = Arc::clone(&self.maintenance);
        let is_running = Arc::clone(&self.is_running);

        tokio::spawn(async move {
            log::info!("[MAINTENANCE] background refresher started");
            // Small jitter (0-30s) on first iteration so co-located bots
            // don't all hit the status page CDN at the exact same instant
            // post-restart. Per-iteration sleep handles the steady-state
            // staggering implicitly via wall-clock drift.
            let initial_jitter = Duration::from_secs(rand::random::<u64>() % 31);
            tokio::time::sleep(initial_jitter).await;

            while is_running.load(Ordering::Relaxed) {
                let ttl_mins = maintenance_ttl_mins();
                let backoff_mins = match fetch_next_maintenance_window_with(&client).await {
                    Ok(next_start) => {
                        let now = Utc::now();
                        let mut info = maintenance.write().await;
                        info.next_start = next_start;
                        info.last_checked = Some(now);
                        ttl_mins
                    }
                    Err(err) => {
                        let err_str = format!("{:?}", err);
                        let backoff = if err_str.contains("429") {
                            ttl_mins.max(MAINTENANCE_BACKOFF_429_MINS)
                        } else {
                            MAINTENANCE_BACKOFF_OTHER_MINS
                        };
                        log::warn!(
                            "[MAINTENANCE] refresh failed (backing off {}min): {:?}",
                            backoff,
                            err
                        );
                        backoff
                    }
                };

                let sleep_secs = (backoff_mins.max(1) as u64).saturating_mul(60);
                let mut remaining = sleep_secs;
                // Wake every 5s so a stop() flipping `is_running` doesn't
                // wait the full TTL before the task exits.
                while remaining > 0 && is_running.load(Ordering::Relaxed) {
                    let chunk = remaining.min(5);
                    tokio::time::sleep(Duration::from_secs(chunk)).await;
                    remaining = remaining.saturating_sub(chunk);
                }
            }

            log::info!("[MAINTENANCE] background refresher exited");
        });
    }
}

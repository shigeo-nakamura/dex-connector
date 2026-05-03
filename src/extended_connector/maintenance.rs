//! Extended maintenance detection. Two signals are combined:
//!
//! 1. Operator-declared windows via `EXTENDED_MAINTENANCE_WINDOWS`
//!    (sourced from Discord announcements). Gives the `hours_ahead`
//!    lead time the `is_upcoming_maintenance` contract expects.
//! 2. Reactive per-symbol status polled from `/info/markets`. A
//!    tracked symbol flipping to `DISABLED` / `REDUCE_ONLY` means
//!    maintenance already started on that market.
//!
//! See bot-strategy#196.

use super::ExtendedConnector;
use crate::dex_request::DexError;
use chrono::{DateTime, Duration, Utc};
use reqwest::Client as HttpClient;
use std::collections::HashMap;
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration as StdDuration;

pub(super) const EXTENDED_MAINTENANCE_POLL_SECS: u64 = 60;
// Once a window has started, keep reporting maintenance until 90 min
// past the start, even if the window's nominal end is shorter.
pub(super) const EXTENDED_MAINTENANCE_ACTIVE_GRACE_MINS: i64 = 90;

#[derive(Debug, Clone, Copy)]
pub(super) struct MaintenanceWindow {
    pub(super) start: DateTime<Utc>,
    pub(super) end: DateTime<Utc>,
}

/// Parse one entry of `EXTENDED_MAINTENANCE_WINDOWS`.
///
/// Accepts two forms separated by `/`:
/// - `<RFC3339>/<RFC3339>` — absolute start/end (both UTC)
/// - `<RFC3339>/<N><unit>`  — start plus duration; unit ∈ {s, m, h}
///
/// Returns `None` on any parse failure (caller logs-and-skips).
pub(super) fn parse_maintenance_window(entry: &str) -> Option<MaintenanceWindow> {
    let entry = entry.trim();
    if entry.is_empty() {
        return None;
    }
    let (start_str, end_str) = entry.split_once('/')?;
    let start = DateTime::parse_from_rfc3339(start_str.trim())
        .ok()?
        .with_timezone(&Utc);
    let end_str = end_str.trim();
    let end = if let Ok(dt) = DateTime::parse_from_rfc3339(end_str) {
        dt.with_timezone(&Utc)
    } else {
        // Duration form: strip the last char as unit, parse the rest as i64.
        let (num, unit) = end_str.split_at(end_str.len().saturating_sub(1));
        let n: i64 = num.parse().ok()?;
        let delta = match unit {
            "s" | "S" => Duration::seconds(n),
            "m" | "M" => Duration::minutes(n),
            "h" | "H" => Duration::hours(n),
            _ => return None,
        };
        start + delta
    };
    if end <= start {
        return None;
    }
    Some(MaintenanceWindow { start, end })
}

/// Parse the `EXTENDED_MAINTENANCE_WINDOWS` env var into a list of windows.
/// Invalid entries are logged and dropped rather than failing the whole set.
pub(super) fn parse_maintenance_windows_env() -> Vec<MaintenanceWindow> {
    let raw = match std::env::var("EXTENDED_MAINTENANCE_WINDOWS") {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    raw.split(',')
        .filter_map(|entry| {
            let parsed = parse_maintenance_window(entry);
            if parsed.is_none() && !entry.trim().is_empty() {
                log::warn!(
                    "[EXTENDED_MAINTENANCE] ignoring invalid window entry: {:?}",
                    entry
                );
            }
            parsed
        })
        .collect()
}

pub(super) fn extended_maintenance_disabled() -> bool {
    matches!(
        std::env::var("EXTENDED_MAINTENANCE_DISABLED").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes")
    )
}

impl ExtendedConnector {
    /// Evaluate the env-var-declared maintenance windows against `now` using
    /// the same upcoming+active semantics as Lighter. The active grace is
    /// fixed at `EXTENDED_MAINTENANCE_ACTIVE_GRACE_MINS`; the window's own
    /// `end` is only used to decide "upcoming".
    pub(super) fn maintenance_within_window(
        windows: &[MaintenanceWindow],
        now: &DateTime<Utc>,
        hours_ahead: i64,
    ) -> bool {
        let horizon = Duration::hours(hours_ahead.max(0));
        let active_window = Duration::minutes(EXTENDED_MAINTENANCE_ACTIVE_GRACE_MINS);
        for w in windows {
            let upcoming = *now < w.start && (w.start - *now) <= horizon && *now <= w.end;
            let active = *now >= w.start && (*now - w.start) <= active_window;
            if upcoming || active {
                return true;
            }
        }
        false
    }

    /// Background refresher for Extended's per-market status. Polls
    /// `/info/markets` every `EXTENDED_MAINTENANCE_POLL_SECS` and flips
    /// `maintenance_symbol_inactive` when any tracked symbol reports a
    /// non-`ACTIVE` status. Runs off the hot path (same rationale as
    /// Lighter's `start_maintenance_refresher`).
    pub(super) fn spawn_maintenance_refresher(&self) {
        if extended_maintenance_disabled() {
            log::info!(
                "[EXTENDED_MAINTENANCE] EXTENDED_MAINTENANCE_DISABLED set; refresher not spawned"
            );
            return;
        }
        if self
            .maintenance_refresher_started
            .swap(true, Ordering::SeqCst)
        {
            return;
        }
        if self.tracked_symbols.is_empty() {
            // Nothing to watch reactively; env-var windows still work.
            return;
        }

        let api_base = self.env.api_base().to_string();
        let tracked: Vec<String> = self.tracked_symbols.iter().cloned().collect();
        let flag = Arc::clone(&self.maintenance_symbol_inactive);

        let client = match HttpClient::builder()
            .timeout(StdDuration::from_secs(10))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                log::warn!(
                    "[EXTENDED_MAINTENANCE] failed to build refresher HTTP client: {e}"
                );
                return;
            }
        };

        tokio::spawn(async move {
            log::info!(
                "[EXTENDED_MAINTENANCE] background refresher started: tracked={:?}",
                tracked
            );
            loop {
                match fetch_extended_market_statuses(&client, &api_base).await {
                    Ok(statuses) => {
                        let mut inactive_hits: Vec<(String, String)> = Vec::new();
                        for sym in &tracked {
                            // Extended keys are "BTC-USD" style; tracked
                            // symbols may be either bare ("BTC") or qualified.
                            // Try both.
                            let qualified = format!("{sym}-USD");
                            let status = statuses
                                .get(sym)
                                .or_else(|| statuses.get(&qualified));
                            if let Some(s) = status {
                                if s != "ACTIVE" {
                                    inactive_hits.push((sym.clone(), s.clone()));
                                }
                            }
                        }
                        let any_inactive = !inactive_hits.is_empty();
                        let prev = flag.swap(any_inactive, Ordering::SeqCst);
                        if any_inactive && !prev {
                            log::warn!(
                                "[EXTENDED_MAINTENANCE] tracked symbol(s) non-ACTIVE: {:?}",
                                inactive_hits
                            );
                        } else if !any_inactive && prev {
                            log::info!(
                                "[EXTENDED_MAINTENANCE] all tracked symbols back to ACTIVE"
                            );
                        }
                    }
                    Err(e) => {
                        log::debug!("[EXTENDED_MAINTENANCE] refresh failed: {e:?}");
                    }
                }
                tokio::time::sleep(StdDuration::from_secs(EXTENDED_MAINTENANCE_POLL_SECS)).await;
            }
        });
    }
}

async fn fetch_extended_market_statuses(
    client: &HttpClient,
    api_base: &str,
) -> Result<HashMap<String, String>, DexError> {
    let url = format!("{api_base}/info/markets");
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| DexError::Other(format!("extended market status fetch: {e}")))?;
    if !resp.status().is_success() {
        return Err(DexError::Other(format!(
            "extended market status HTTP {}",
            resp.status()
        )));
    }
    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| DexError::Other(format!("extended market status parse: {e}")))?;
    let arr = body
        .get("data")
        .and_then(|v| v.as_array())
        .ok_or_else(|| DexError::Other("extended market status: no data array".into()))?;
    let mut out = HashMap::with_capacity(arr.len());
    for item in arr {
        if let (Some(name), Some(status)) = (
            item.get("name").and_then(|v| v.as_str()),
            item.get("status").and_then(|v| v.as_str()),
        ) {
            out.insert(name.to_string(), status.to_string());
        }
    }
    Ok(out)
}

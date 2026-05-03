//! Lighter Better Stack status-feed scraping.
//!
//! Lighter operates an official status page that exposes a public RSS feed
//! of incidents and scheduled maintenance. We fetch this instead of the
//! trading API's `/api/v1/announcement` endpoint, which is rate-limited and
//! WAF-protected (see bot-strategy#15, #32). The status page CDN is
//! independent of the trading endpoints, so it does not share rate limits.

use crate::dex_request::DexError;
use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveDate, Utc};
use lazy_static::lazy_static;
use regex::Regex;
use reqwest::Client;

pub(super) const LIGHTER_STATUS_FEED_URL: &str = "https://status.lighter.xyz/feed.rss";

/// Default cache TTL for the maintenance check. Override at runtime via the
/// `LIGHTER_MAINTENANCE_TTL_MINS` env var. The status page CDN tolerates
/// frequent polling, so we refresh aggressively to minimize detection
/// latency for newly announced maintenance windows.
pub(super) const DEFAULT_MAINTENANCE_CACHE_TTL_MINS: i64 = 5;

/// Backoff after a 429 response. The status page CDN should not rate-limit
/// us in practice, but if it does, back off conservatively.
pub(super) const MAINTENANCE_BACKOFF_429_MINS: i64 = 15;

/// Backoff after a non-429 fetch error (network, parse, etc.). Short
/// because transient network errors are expected to clear quickly.
pub(super) const MAINTENANCE_BACKOFF_OTHER_MINS: i64 = 5;

/// Read the maintenance check TTL from `LIGHTER_MAINTENANCE_TTL_MINS`.
/// Falls back to `DEFAULT_MAINTENANCE_CACHE_TTL_MINS` if the env var is
/// missing, empty, or unparseable. Negative or zero values are treated as
/// invalid and the default is used (a zero TTL would cause every call to
/// hit the network).
pub(super) fn maintenance_ttl_mins() -> i64 {
    match std::env::var("LIGHTER_MAINTENANCE_TTL_MINS") {
        Ok(s) => match s.trim().parse::<i64>() {
            Ok(v) if v > 0 => v,
            _ => DEFAULT_MAINTENANCE_CACHE_TTL_MINS,
        },
        Err(_) => DEFAULT_MAINTENANCE_CACHE_TTL_MINS,
    }
}

pub(super) struct MaintenanceInfo {
    pub(super) next_start: Option<DateTime<Utc>>,
    pub(super) last_checked: Option<DateTime<Utc>>,
}

/// One `<item>` extracted from the Lighter status RSS feed.
#[derive(Debug, Clone)]
struct LighterRssItem {
    title: String,
    description: String,
    pub_date: Option<DateTime<Utc>>,
}

pub(super) fn announcement_mentions_downtime(title: &str, content: &str) -> bool {
    let keywords = ["maintenance", "upgrade", "downtime"];
    let lower_title = title.to_ascii_lowercase();
    let lower_content = content.to_ascii_lowercase();

    keywords
        .iter()
        .any(|keyword| lower_title.contains(keyword) || lower_content.contains(keyword))
}

pub(super) fn parse_datetime_from_text(
    title: &str,
    content: &str,
    pub_date_hint: Option<DateTime<Utc>>,
) -> Option<DateTime<Utc>> {
    // Examples from announcements:
    // - "We're doing a network upgrade on Sunday November 30th at 12PM UTC."
    // - "November 23, 2025 at 12:00 PM UTC"
    lazy_static! {
        static ref DT_RE: Regex = Regex::new(
            r"(?ix)
            (January|February|March|April|May|June|July|August|September|October|November|December)
            \s+
            (\d{1,2})       # day
            (?:st|nd|rd|th)? # optional ordinal suffix
            (?:,?\s*(\d{4}))? # optional year
            \s+at\s+
            (\d{1,2})       # hour
            (?::(\d{2}))?   # optional minutes
            \s*(AM|PM)?     # optional meridiem (default to 24h if missing)
            \s*UTC
            "
        )
        .unwrap();
    }

    let haystack = format!("{} {}", title, content);
    let caps = DT_RE.captures(&haystack)?;
    let month_str = caps.get(1)?.as_str();
    let day: u32 = caps.get(2)?.as_str().parse().ok()?;
    // When the announcement text omits the year, derive it from the
    // RSS pub_date if available (the year of publication is almost
    // always the year the event occurs in for Lighter status posts).
    // Falling back to "current year" is dangerous because old RSS
    // items get reinterpreted as future events. See bot-strategy#32.
    let (year, year_was_inferred) = if let Some(m) = caps.get(3) {
        (m.as_str().parse().ok()?, false)
    } else if let Some(hint) = pub_date_hint {
        (hint.year(), true)
    } else {
        (Utc::now().year(), true)
    };
    let hour_raw: u32 = caps.get(4)?.as_str().parse().ok()?;
    let minute: u32 = caps
        .get(5)
        .map(|m| m.as_str().parse().unwrap_or(0))
        .unwrap_or(0);
    let meridiem = caps.get(6).map(|m| m.as_str().to_ascii_uppercase());

    let month = match month_str.to_ascii_lowercase().as_str() {
        "january" => 1,
        "february" => 2,
        "march" => 3,
        "april" => 4,
        "may" => 5,
        "june" => 6,
        "july" => 7,
        "august" => 8,
        "september" => 9,
        "october" => 10,
        "november" => 11,
        "december" => 12,
        _ => return None,
    };

    let hour_24 = match meridiem.as_deref() {
        Some("AM") => {
            if hour_raw == 12 {
                0
            } else {
                hour_raw
            }
        }
        Some("PM") => {
            if hour_raw == 12 {
                12
            } else {
                hour_raw + 12
            }
        }
        None => hour_raw, // assume already 24h format
        _ => return None,
    };

    let naive = NaiveDate::from_ymd_opt(year, month, day)?.and_hms_opt(hour_24, minute, 0)?;
    let dt = DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);

    // Year-rollover heuristic: only apply when the year was inferred
    // *without* a pub_date hint (i.e. fall-back to current-year). With
    // a pub_date hint we trust the publication year — bumping a past
    // date to "next year" would falsely turn a stale RSS item into a
    // future event, which is exactly the bug from bot-strategy#32.
    if year_was_inferred && pub_date_hint.is_none() && dt < Utc::now() {
        let naive_next =
            NaiveDate::from_ymd_opt(year + 1, month, day)?.and_hms_opt(hour_24, minute, 0)?;
        return Some(DateTime::<Utc>::from_naive_utc_and_offset(naive_next, Utc));
    }

    Some(dt)
}

pub(super) fn maintenance_within_window(
    next_start: Option<DateTime<Utc>>,
    now: &DateTime<Utc>,
    hours_ahead: i64,
) -> bool {
    if let Some(start) = next_start {
        let horizon = ChronoDuration::hours(hours_ahead.max(0));
        // Lighter advertises typical downtimes of "under 15 minutes".
        // 30min gives a 2x safety margin without locking the bot out
        // of trading for an excessive period after a short upgrade.
        let active_window = ChronoDuration::minutes(30);
        let upcoming = *now <= start && (start - *now) <= horizon;
        let active = *now >= start && (*now - start) <= active_window;
        return upcoming || active;
    }
    false
}

/// Decode minimal HTML/XML entities that appear in Better Stack RSS
/// `<description>` text. We don't need a full HTML decoder — only the
/// five XML entities plus a couple of common numeric escapes.
fn decode_xml_entities(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// Parse a Better Stack RSS feed body into a list of items. We use
/// regex rather than a full XML parser because the feed schema is
/// trivial (flat `<item>` blocks with no nesting we care about) and
/// we already depend on `regex`. This avoids pulling in `quick-xml`.
fn parse_lighter_rss(body: &str) -> Vec<LighterRssItem> {
    lazy_static! {
        static ref ITEM_RE: Regex = Regex::new(r"(?s)<item>(.*?)</item>").unwrap();
        static ref TITLE_RE: Regex = Regex::new(r"(?s)<title>(.*?)</title>").unwrap();
        static ref DESC_RE: Regex = Regex::new(r"(?s)<description>(.*?)</description>").unwrap();
        static ref DATE_RE: Regex = Regex::new(r"(?s)<pubDate>(.*?)</pubDate>").unwrap();
    }

    ITEM_RE
        .captures_iter(body)
        .filter_map(|item_caps| {
            let inner = item_caps.get(1)?.as_str();
            let title = TITLE_RE
                .captures(inner)
                .and_then(|c| c.get(1))
                .map(|m| decode_xml_entities(m.as_str().trim()))
                .unwrap_or_default();
            let description = DESC_RE
                .captures(inner)
                .and_then(|c| c.get(1))
                .map(|m| decode_xml_entities(m.as_str().trim()))
                .unwrap_or_default();
            let pub_date = DATE_RE
                .captures(inner)
                .and_then(|c| c.get(1))
                .and_then(|m| {
                    DateTime::parse_from_rfc2822(m.as_str().trim())
                        .ok()
                        .map(|dt| dt.with_timezone(&Utc))
                });

            if title.is_empty() && description.is_empty() {
                None
            } else {
                Some(LighterRssItem {
                    title,
                    description,
                    pub_date,
                })
            }
        })
        .collect()
}

pub(super) async fn fetch_next_maintenance_window_with(
    client: &Client,
) -> Result<Option<DateTime<Utc>>, DexError> {
    let url = LIGHTER_STATUS_FEED_URL;

    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| DexError::Other(format!("Failed to fetch Lighter status feed: {}", e)))?;

    if !response.status().is_success() {
        return Err(DexError::Other(format!(
            "Lighter status feed returned HTTP {}",
            response.status()
        )));
    }

    let body = response
        .text()
        .await
        .map_err(|e| DexError::Other(format!("Failed to read Lighter status feed body: {}", e)))?;

    let items = parse_lighter_rss(&body);
    let now = Utc::now();

    log::info!(
        "Lighter maintenance fetch ok: url={} items={}",
        url,
        items.len()
    );

    let mut upcoming: Vec<DateTime<Utc>> = items
        .into_iter()
        .filter(|item| announcement_mentions_downtime(&item.title, &item.description))
        .filter_map(|item| {
            if let Some(dt) =
                parse_datetime_from_text(&item.title, &item.description, item.pub_date)
            {
                if dt >= now {
                    return Some(dt);
                }
                log::debug!(
                    "Lighter maintenance parse: parsed datetime {} already past, skipping (title={})",
                    dt,
                    item.title
                );
            }

            // Fall back to pubDate. RSS pubDate is *publication* time, not
            // event time, so this only catches cases where the body text
            // didn't parse — better to err on the side of warning the bot
            // about a maintenance window than to silently drop it.
            match item.pub_date {
                Some(dt) if dt >= now => Some(dt),
                Some(dt) => {
                    log::debug!(
                        "Lighter maintenance skip past pub_date: pub_date={} now={} title={}",
                        dt,
                        now,
                        item.title
                    );
                    None
                }
                None => {
                    log::debug!(
                        "Lighter maintenance parse: no pub_date and no parseable text, title={}",
                        item.title
                    );
                    None
                }
            }
        })
        .collect();

    upcoming.sort();
    let next = upcoming.into_iter().next();

    log::info!(
        "Lighter maintenance result: next_start={:?} now={}",
        next,
        now
    );

    Ok(next)
}

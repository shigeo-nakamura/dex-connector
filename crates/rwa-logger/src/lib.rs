//! Shared helpers for the read-only RWA loggers (bot-strategy#574):
//! `rwa-spot-logger` (Jupiter spot) and `apex-perp-logger` (ApeX RWA perps).

/// Default poll cadence when `*_POLL_SECS` is unset/unparseable.
pub const DEFAULT_POLL_SECS: u64 = 5;
/// Floor for the poll cadence. A `0` would make the sleep loop a no-op and turn
/// a logger into an unbounded tight poll/write loop, so any value below this
/// (incl. a 0 typo) is clamped up.
pub const MIN_POLL_SECS: u64 = 1;

/// Resolve the poll cadence from the raw env value, clamping a 0/too-small value
/// up to `MIN_POLL_SECS` and falling back to the default on absent or
/// unparseable input (so a typo can never produce a tight loop).
pub fn resolve_poll_secs(raw: Option<&str>) -> u64 {
    match raw {
        None => DEFAULT_POLL_SECS,
        Some(s) => match s.trim().parse::<u64>() {
            Ok(n) if n >= MIN_POLL_SECS => n,
            Ok(n) => {
                log::warn!("poll secs {n} below minimum, clamping to {MIN_POLL_SECS}s");
                MIN_POLL_SECS
            }
            Err(_) => {
                log::warn!("poll secs '{s}' not a valid u64, using default {DEFAULT_POLL_SECS}s");
                DEFAULT_POLL_SECS
            }
        },
    }
}

/// Parse `label:value,label:value` into `[(label, value)]`. Splits on the first
/// `:` only (a value may itself contain none). Errors on malformed/empty input.
pub fn parse_pairs(spec: &str) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    for entry in spec.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (label, value) = entry
            .split_once(':')
            .ok_or_else(|| format!("entry '{entry}' is not in label:value form"))?;
        let (label, value) = (label.trim(), value.trim());
        if label.is_empty() || value.is_empty() {
            return Err(format!("entry '{entry}' has empty label or value"));
        }
        out.push((label.to_string(), value.to_string()));
    }
    if out.is_empty() {
        return Err("parsed to zero entries".to_string());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pairs_basic() {
        let t = parse_pairs("TSLAx:Mint1, SPCX:Mint2").unwrap();
        assert_eq!(
            t,
            vec![
                ("TSLAx".to_string(), "Mint1".to_string()),
                ("SPCX".to_string(), "Mint2".to_string())
            ]
        );
    }

    #[test]
    fn parse_pairs_rejects_bad() {
        assert!(parse_pairs("noColon").is_err());
        assert!(parse_pairs("").is_err());
        assert!(parse_pairs(":value").is_err());
    }

    #[test]
    fn resolve_poll_secs_default_when_absent() {
        assert_eq!(resolve_poll_secs(None), DEFAULT_POLL_SECS);
    }

    #[test]
    fn resolve_poll_secs_clamps_zero() {
        assert_eq!(resolve_poll_secs(Some("0")), MIN_POLL_SECS);
    }

    #[test]
    fn resolve_poll_secs_passthrough_valid() {
        assert_eq!(resolve_poll_secs(Some("3")), 3);
        assert_eq!(resolve_poll_secs(Some(" 10 ")), 10);
    }

    #[test]
    fn resolve_poll_secs_default_when_unparseable() {
        assert_eq!(resolve_poll_secs(Some("abc")), DEFAULT_POLL_SECS);
    }
}

use std::collections::VecDeque;
use std::time::{Duration, Instant};

const FAILURE_WINDOW: Duration = Duration::from_secs(60);
const FAILURE_THRESHOLD: usize = 6;
const CLEAR_SUCCESS_THRESHOLD: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OutageSignal {
    Rest,
    Ws,
    ReadMid,
}

impl OutageSignal {
    fn label(self) -> &'static str {
        match self {
            Self::Rest => "rest",
            Self::Ws => "ws",
            Self::ReadMid => "read_mid",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum OutageTransition {
    Latched { reason: &'static str },
    Cleared { reason: &'static str },
}

#[derive(Debug, Default)]
pub(super) struct OutageDetector {
    rest_failures: VecDeque<Instant>,
    ws_failures: VecDeque<Instant>,
    read_mid_failures: VecDeque<Instant>,
    degraded_reason: Option<&'static str>,
    consecutive_successes: usize,
}

impl OutageDetector {
    #[cfg(test)]
    pub(super) fn is_degraded(&self) -> bool {
        self.degraded_reason.is_some()
    }

    pub(super) fn reason(&self) -> Option<&'static str> {
        self.degraded_reason
    }

    pub(super) fn record_failure(
        &mut self,
        signal: OutageSignal,
        now: Instant,
    ) -> Option<OutageTransition> {
        self.consecutive_successes = 0;

        let failures = match signal {
            OutageSignal::Rest => &mut self.rest_failures,
            OutageSignal::Ws => &mut self.ws_failures,
            OutageSignal::ReadMid => &mut self.read_mid_failures,
        };
        failures.push_back(now);
        prune(failures, now);

        if self.degraded_reason.is_none() && failures.len() >= FAILURE_THRESHOLD {
            let reason = signal.label();
            self.degraded_reason = Some(reason);
            return Some(OutageTransition::Latched { reason });
        }

        None
    }

    pub(super) fn record_success(
        &mut self,
        signal: OutageSignal,
        now: Instant,
    ) -> Option<OutageTransition> {
        match signal {
            OutageSignal::Rest => prune(&mut self.rest_failures, now),
            OutageSignal::Ws => prune(&mut self.ws_failures, now),
            OutageSignal::ReadMid => prune(&mut self.read_mid_failures, now),
        }

        let reason = self.degraded_reason?;
        if reason != signal.label() {
            return None;
        }
        self.consecutive_successes += 1;
        if self.consecutive_successes >= CLEAR_SUCCESS_THRESHOLD {
            self.degraded_reason = None;
            self.consecutive_successes = 0;
            return Some(OutageTransition::Cleared { reason });
        }

        None
    }
}

fn prune(failures: &mut VecDeque<Instant>, now: Instant) {
    while failures
        .front()
        .is_some_and(|oldest| now.duration_since(*oldest) > FAILURE_WINDOW)
    {
        failures.pop_front();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latches_after_six_read_mid_failures_inside_window() {
        let mut detector = OutageDetector::default();
        let t0 = Instant::now();

        for i in 0..5 {
            assert_eq!(
                detector.record_failure(OutageSignal::ReadMid, t0 + Duration::from_secs(i)),
                None
            );
            assert!(!detector.is_degraded());
        }

        assert_eq!(
            detector.record_failure(OutageSignal::ReadMid, t0 + Duration::from_secs(5)),
            Some(OutageTransition::Latched { reason: "read_mid" })
        );
        assert!(detector.is_degraded());
        assert_eq!(detector.reason(), Some("read_mid"));
    }

    #[test]
    fn failures_outside_window_do_not_latch() {
        let mut detector = OutageDetector::default();
        let t0 = Instant::now();

        for i in 0..5 {
            detector.record_failure(OutageSignal::Rest, t0 + Duration::from_secs(i));
        }

        assert_eq!(
            detector.record_failure(OutageSignal::Rest, t0 + Duration::from_secs(61)),
            None
        );
        assert!(!detector.is_degraded());
    }

    #[test]
    fn clears_after_three_successes() {
        let mut detector = OutageDetector::default();
        let t0 = Instant::now();

        for i in 0..6 {
            detector.record_failure(OutageSignal::Ws, t0 + Duration::from_secs(i));
        }
        assert!(detector.is_degraded());

        assert_eq!(
            detector.record_success(OutageSignal::Rest, t0 + Duration::from_secs(10)),
            None,
            "a different read path must not clear the latched signal"
        );
        assert_eq!(
            detector.record_success(OutageSignal::Ws, t0 + Duration::from_secs(11)),
            None
        );
        assert_eq!(
            detector.record_success(OutageSignal::Ws, t0 + Duration::from_secs(12)),
            None
        );
        assert_eq!(
            detector.record_success(OutageSignal::Ws, t0 + Duration::from_secs(13)),
            Some(OutageTransition::Cleared { reason: "ws" })
        );
        assert!(!detector.is_degraded());
    }
}

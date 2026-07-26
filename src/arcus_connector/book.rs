use super::models::{parse_decimal, OrderbookSnapshotWire, WsBookContents};
use crate::{DexError, OrderBookLevel, OrderBookSnapshot};
use rust_decimal::Decimal;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct BookTop {
    pub(super) best_bid: Decimal,
    pub(super) best_ask: Decimal,
    pub(super) mid: Decimal,
    pub(super) timestamp_ms: u64,
    // `Some` only when `timestamp_ms` came from Arcus; `None` when it was
    // substituted with the local receive time because Arcus omitted the
    // timestamp. Callers that report an exchange-event time (e.g.
    // `TickerResponse.exchange_ts`) must use this, not `timestamp_ms`
    // (bot-strategy#749 review).
    pub(super) exchange_timestamp_ms: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct BookState {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    last_sequence_id: u64,
    global_sequence_id: u64,
    // Exchange-supplied timestamp, exposed via `top().timestamp_ms` and
    // `snapshot().book_ts_ms`. Exchange clock skew must not affect this.
    last_update_ms: u64,
    // Whether `last_update_ms` came from Arcus, vs. being substituted with
    // the local receive time because Arcus omitted the timestamp.
    last_update_is_exchange: bool,
    // Local receive time, used only for `is_fresh`. Keeping this separate
    // from `last_update_ms` means exchange clock skew can't make a fresh
    // book look stale (or a frozen book look fresh).
    last_received_ms: u64,
    ready: bool,
}

struct ReplaceSnapshot<'a> {
    bids: &'a [[String; 2]],
    asks: &'a [[String; 2]],
    last_sequence_id: u64,
    global_sequence_id: u64,
    // Exchange-supplied timestamp (or the local receive time if the
    // exchange did not provide one), exposed to callers.
    timestamp_ms: u64,
    // Whether `timestamp_ms` above is actually exchange-supplied.
    timestamp_is_exchange: bool,
    // Local receive time, used only for freshness bookkeeping.
    received_ms: u64,
}

impl BookState {
    pub(super) fn replace_from_rest(
        &mut self,
        market: &str,
        snapshot: &OrderbookSnapshotWire,
        now_ms: u64,
    ) -> Result<Option<BookTop>, DexError> {
        self.replace(
            market,
            ReplaceSnapshot {
                bids: &snapshot.bids,
                asks: &snapshot.asks,
                last_sequence_id: snapshot.last_sequence_id,
                global_sequence_id: snapshot.global_sequence_id,
                timestamp_ms: microseconds_to_millis(snapshot.timestamp).unwrap_or(now_ms),
                timestamp_is_exchange: microseconds_to_millis(snapshot.timestamp).is_some(),
                received_ms: now_ms,
            },
        )
    }

    pub(super) fn replace_from_ws(
        &mut self,
        market: &str,
        contents: &WsBookContents,
        now_ms: u64,
    ) -> Result<Option<BookTop>, DexError> {
        self.replace(
            market,
            ReplaceSnapshot {
                bids: &contents.bids,
                asks: &contents.asks,
                last_sequence_id: contents.last_sequence_id,
                global_sequence_id: contents.global_sequence_id,
                timestamp_ms: contents
                    .timestamp
                    .and_then(microseconds_to_millis)
                    .unwrap_or(now_ms),
                timestamp_is_exchange: contents
                    .timestamp
                    .and_then(microseconds_to_millis)
                    .is_some(),
                received_ms: now_ms,
            },
        )
    }

    fn replace(
        &mut self,
        market: &str,
        fields: ReplaceSnapshot<'_>,
    ) -> Result<Option<BookTop>, DexError> {
        let parsed_bids = parse_levels(market, "bids", fields.bids)?;
        let parsed_asks = parse_levels(market, "asks", fields.asks)?;

        self.bids.clear();
        self.asks.clear();
        insert_snapshot_levels(&mut self.bids, parsed_bids);
        insert_snapshot_levels(&mut self.asks, parsed_asks);
        self.last_sequence_id = fields.last_sequence_id;
        self.global_sequence_id = fields.global_sequence_id;
        self.last_update_ms = fields.timestamp_ms;
        self.last_update_is_exchange = fields.timestamp_is_exchange;
        self.last_received_ms = fields.received_ms;
        self.ready = true;
        Ok(self.top())
    }

    pub(super) fn apply_delta(
        &mut self,
        market: &str,
        contents: &WsBookContents,
        now_ms: u64,
    ) -> Result<Option<BookTop>, DexError> {
        if !self.ready {
            return Err(DexError::Transient(format!(
                "Arcus {market} orderbook delta arrived before snapshot"
            )));
        }

        if contents.last_sequence_id <= self.last_sequence_id {
            return Ok(None);
        }

        let expected = self.last_sequence_id.saturating_add(1);
        if contents.last_sequence_id != expected {
            let got = contents.last_sequence_id;
            self.reset();
            return Err(DexError::Transient(format!(
                "Arcus {market} orderbook sequence gap expected={expected} got={got}"
            )));
        }

        let bids = parse_levels(market, "bids", &contents.bids)?;
        let asks = parse_levels(market, "asks", &contents.asks)?;
        apply_delta_levels(&mut self.bids, bids);
        apply_delta_levels(&mut self.asks, asks);
        self.last_sequence_id = contents.last_sequence_id;
        self.global_sequence_id = contents.global_sequence_id;
        self.last_update_is_exchange = contents
            .timestamp
            .and_then(microseconds_to_millis)
            .is_some();
        self.last_update_ms = contents
            .timestamp
            .and_then(microseconds_to_millis)
            .unwrap_or(now_ms);
        self.last_received_ms = now_ms;
        Ok(self.top())
    }

    pub(super) fn reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.last_sequence_id = 0;
        self.global_sequence_id = 0;
        self.last_update_ms = 0;
        self.last_update_is_exchange = false;
        self.last_received_ms = 0;
        self.ready = false;
    }

    pub(super) fn is_fresh(&self, now_ms: u64, stale_after_ms: u64) -> bool {
        self.ready
            && self.last_received_ms > 0
            && now_ms.saturating_sub(self.last_received_ms) <= stale_after_ms
    }

    pub(super) fn top(&self) -> Option<BookTop> {
        let best_bid = *self.bids.last_key_value()?.0;
        let best_ask = *self.asks.first_key_value()?.0;
        if best_bid <= Decimal::ZERO || best_ask <= Decimal::ZERO || best_bid > best_ask {
            return None;
        }
        Some(BookTop {
            best_bid,
            best_ask,
            mid: (best_bid + best_ask) / Decimal::TWO,
            timestamp_ms: self.last_update_ms,
            exchange_timestamp_ms: self.last_update_is_exchange.then_some(self.last_update_ms),
        })
    }

    pub(super) fn snapshot(&self, depth: usize) -> Option<OrderBookSnapshot> {
        if !self.ready {
            return None;
        }
        let bids = self
            .bids
            .iter()
            .rev()
            .take(depth)
            .map(|(price, size)| OrderBookLevel {
                price: *price,
                size: *size,
            })
            .collect();
        let asks = self
            .asks
            .iter()
            .take(depth)
            .map(|(price, size)| OrderBookLevel {
                price: *price,
                size: *size,
            })
            .collect();
        Some(OrderBookSnapshot {
            bids,
            asks,
            book_ts_ms: Some(self.last_update_ms),
        })
    }

    #[cfg(test)]
    pub(super) fn last_sequence_id(&self) -> u64 {
        self.last_sequence_id
    }

    #[cfg(test)]
    pub(super) fn is_ready(&self) -> bool {
        self.ready
    }
}

fn parse_levels(
    market: &str,
    side: &str,
    levels: &[[String; 2]],
) -> Result<Vec<(Decimal, Decimal)>, DexError> {
    levels
        .iter()
        .map(|level| {
            let price = parse_decimal(&level[0], market, &format!("{side}.price"))?;
            let size = parse_decimal(&level[1], market, &format!("{side}.size"))?;
            if price <= Decimal::ZERO || size < Decimal::ZERO {
                return Err(DexError::Transient(format!(
                    "Arcus invalid {side} level market={market} price={price} size={size}"
                )));
            }
            Ok((price, size))
        })
        .collect()
}

fn insert_snapshot_levels(side: &mut BTreeMap<Decimal, Decimal>, levels: Vec<(Decimal, Decimal)>) {
    for (price, size) in levels {
        if !size.is_zero() {
            side.insert(price, size);
        }
    }
}

fn apply_delta_levels(side: &mut BTreeMap<Decimal, Decimal>, levels: Vec<(Decimal, Decimal)>) {
    for (price, size) in levels {
        if size.is_zero() {
            side.remove(&price);
        } else {
            side.insert(price, size);
        }
    }
}

fn microseconds_to_millis(timestamp_us: u64) -> Option<u64> {
    (timestamp_us > 0).then_some(timestamp_us / 1_000)
}

/// Per-symbol order book state machine for Binance Futures depth stream.
use std::collections::{BTreeMap, VecDeque};

use log::{debug, info, warn};
use rust_decimal::Decimal;

const BUFFER_MAX: usize = 5000;
const BBO_MISMATCH_THRESHOLD: u32 = 10;

/// Parse raw string price/qty pairs into Decimal tuples.
pub fn parse_levels(raw: &[(String, String)]) -> Vec<(Decimal, Decimal)> {
    raw.iter()
        .map(|(p, q)| {
            (
                p.parse::<Decimal>().expect("invalid price"),
                q.parse::<Decimal>().expect("invalid qty"),
            )
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookState {
    Uninitialized,
    Buffering,
    Live,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Publish,
    NeedSnapshot,
    None_,
}

/// (price, qty) levels for one side of the book.
pub type Levels = Vec<(Decimal, Decimal)>;

/// A buffered depth diff: (U, u, pu, bids, asks).
type BufferedEvent = (u64, u64, u64, Levels, Levels);

/// One symbol's L2 order book, synced via depth diffs + REST snapshots.
///
/// State machine: Uninitialized -> Buffering -> Live.
/// On sequence gap or integrity failure: falls back to Buffering, re-snapshots.
#[derive(Debug)]
pub struct Book {
    pub symbol: String,
    pub bids: BTreeMap<Decimal, Decimal>,
    pub asks: BTreeMap<Decimal, Decimal>,
    pub last_update_id: u64,
    pub state: BookState,
    pub last_update_time: f64,
    pub snapshot_count: u64,
    pub gap_count: u64,
    pub crossed_count: u64,
    /// Diffs shed because the resync buffer hit BUFFER_MAX. Non-zero means a
    /// snapshot took so long we lost events we might have needed to replay.
    pub buffer_dropped: u64,
    /// Snapshots where the documented bridge (`U <= lastUpdateId <= u`) could
    /// not be proven because no buffered event straddled the snapshot.
    pub unverified_bridge_count: u64,
    buffer: VecDeque<BufferedEvent>,
    need_first_event: bool,
    ticker_bid: Decimal,
    ticker_ask: Decimal,
    bbo_mismatch_count: u32,
}

impl Book {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: 0,
            state: BookState::Uninitialized,
            buffer: VecDeque::new(),
            last_update_time: 0.0,
            snapshot_count: 0,
            gap_count: 0,
            crossed_count: 0,
            buffer_dropped: 0,
            unverified_bridge_count: 0,
            need_first_event: false,
            ticker_bid: Decimal::ZERO,
            ticker_ask: Decimal::ZERO,
            bbo_mismatch_count: 0,
        }
    }

    /// Process a depth diff event.
    ///
    /// Returns `Action::Publish` if the book was updated and should be published,
    /// `Action::NeedSnapshot` if a REST snapshot should be fetched, or `Action::None_`.
    #[allow(non_snake_case)]
    pub fn on_depth(
        &mut self,
        U: u64,
        u: u64,
        pu: u64,
        bids: Vec<(Decimal, Decimal)>,
        asks: Vec<(Decimal, Decimal)>,
    ) -> Action {
        match self.state {
            BookState::Uninitialized => {
                self.state = BookState::Buffering;
                self.buffer_push((U, u, pu, bids, asks));
                Action::NeedSnapshot
            }
            BookState::Buffering => {
                self.buffer_push((U, u, pu, bids, asks));
                Action::None_
            }
            BookState::Live => {
                if u <= self.last_update_id {
                    return Action::None_;
                }

                if self.need_first_event {
                    // Only set when NO buffered event straddled the snapshot, so there
                    // is nothing to chain pu against: last_update_id is a REST
                    // lastUpdateId, not a stream 'u'. Binance Futures draws U/u/pu from
                    // a venue-wide counter, so pu here belongs to a different point in
                    // the global sequence and comparing it would false-positive. Accept
                    // once, then the normal pu chain takes over. Counted as
                    // unverified_bridge_count.
                    self.need_first_event = false;
                } else if pu != self.last_update_id {
                    self.gap_count += 1;
                    warn!(
                        "{}: sequence gap (expected pu={}, got {}) [#{}]",
                        self.symbol, self.last_update_id, pu, self.gap_count
                    );
                    self.fall_back_to_buffering(U, u, pu, bids, asks);
                    return Action::NeedSnapshot;
                }

                self.apply_diff(&bids, &asks, u);

                if !self.check_integrity() {
                    self.fall_back_to_buffering(U, u, pu, bids, asks);
                    return Action::NeedSnapshot;
                }

                Action::Publish
            }
        }
    }

    /// Apply a REST snapshot and replay buffered events. Returns true if live.
    pub fn on_snapshot(
        &mut self,
        last_update_id: u64,
        bids: &[(Decimal, Decimal)],
        asks: &[(Decimal, Decimal)],
    ) -> bool {
        if self.state != BookState::Buffering {
            return true;
        }

        self.bids.clear();
        for &(p, q) in bids {
            if q > Decimal::ZERO {
                self.bids.insert(p, q);
            }
        }

        self.asks.clear();
        for &(p, q) in asks {
            if q > Decimal::ZERO {
                self.asks.insert(p, q);
            }
        }

        self.last_update_id = last_update_id;
        self.snapshot_count += 1;

        let mut applied_any = false;
        // Drain the buffer into a local vec to avoid borrow issues.
        let buffered: Vec<_> = self.buffer.drain(..).collect();
        for (buf_big_u, buf_u, buf_pu, buf_bids, buf_asks) in &buffered {
            if *buf_u < last_update_id {
                continue;
            }
            if !applied_any {
                // Binance's documented bridge: the first replayed event must
                // straddle the snapshot (U <= lastUpdateId <= u). u >= lui is
                // already true here, so only U needs checking. If U > lui the
                // diffs covering (lui, U) are gone -- going live now would
                // silently serve a book with a hole, so reject and re-snapshot.
                if *buf_big_u > last_update_id {
                    warn!(
                        "{}: snapshot bridge broken (U={} > lastUpdateId={}), re-snapshotting",
                        self.symbol, buf_big_u, last_update_id
                    );
                    return false;
                }
            } else if *buf_pu != self.last_update_id {
                debug!("{}: pu gap in buffer during sync", self.symbol);
                return false;
            }
            applied_any = true;
            self.apply_diff(buf_bids, buf_asks, *buf_u);
        }

        self.state = BookState::Live;
        // No buffered event straddled the snapshot, so the bridge is unproven.
        // We accept the next live event without a pu check (Binance Futures
        // draws U/u from a venue-wide counter, so pu will not line up with a
        // REST lastUpdateId). Counted so this stays visible rather than silent.
        self.need_first_event = !applied_any;
        if !applied_any {
            self.unverified_bridge_count += 1;
        }
        self.bbo_mismatch_count = 0;

        if !self.check_integrity() {
            self.state = BookState::Buffering;
            return false;
        }

        info!(
            "{}: LIVE ({} bids, {} asks) [snapshot #{}]",
            self.symbol,
            self.bids.len(),
            self.asks.len(),
            self.snapshot_count
        );
        true
    }

    /// Store the latest bookTicker BBO for cross-validation.
    pub fn set_ticker_bbo(&mut self, bid: Decimal, ask: Decimal) {
        self.ticker_bid = bid;
        self.ticker_ask = ask;
    }

    /// Reset and re-enter `Buffering` so a snapshot can be requested without
    /// waiting for the next depth event. Used by the staleness sweeper: a symbol
    /// that stopped ticking is exactly the one whose next depth event may be
    /// minutes away, so `reset()` alone leaves it dark until then.
    pub fn mark_for_resync(&mut self) {
        self.reset();
        self.state = BookState::Buffering;
    }

    pub fn reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.last_update_id = 0;
        self.state = BookState::Uninitialized;
        self.buffer.clear();
        self.need_first_event = false;
        self.ticker_bid = Decimal::ZERO;
        self.ticker_ask = Decimal::ZERO;
        self.bbo_mismatch_count = 0;
    }

    /// Return the top `n` bid and ask levels, sorted best-first.
    pub fn top_levels(&self, n: usize) -> (Levels, Levels) {
        // BTreeMap is ascending. Best bids are the highest prices (take from the end).
        let top_bids: Levels = self
            .bids
            .iter()
            .rev()
            .take(n)
            .map(|(&p, &q)| (p, q))
            .collect();

        // Best asks are the lowest prices (take from the start).
        let top_asks: Levels =
            self.asks.iter().take(n).map(|(&p, &q)| (p, q)).collect();

        (top_bids, top_asks)
    }

    fn apply_diff(&mut self, bids: &[(Decimal, Decimal)], asks: &[(Decimal, Decimal)], u: u64) {
        for &(price, qty) in bids {
            if qty.is_zero() {
                self.bids.remove(&price);
            } else {
                self.bids.insert(price, qty);
            }
        }
        for &(price, qty) in asks {
            if qty.is_zero() {
                self.asks.remove(&price);
            } else {
                self.asks.insert(price, qty);
            }
        }
        self.last_update_id = u;
    }

    /// Returns false if the book is detectably corrupt.
    fn check_integrity(&mut self) -> bool {
        if self.bids.is_empty() || self.asks.is_empty() {
            return true;
        }

        // BTreeMap: last key is max (best bid), first key is min (best ask).
        let book_bid = *self.bids.keys().next_back().unwrap();
        let book_ask = *self.asks.keys().next().unwrap();

        if book_bid >= book_ask {
            self.crossed_count += 1;
            warn!(
                "{}: crossed book (bid={} >= ask={}) [#{}]",
                self.symbol, book_bid, book_ask, self.crossed_count
            );
            return false;
        }

        if !self.ticker_bid.is_zero() && !self.ticker_ask.is_zero() {
            if book_bid != self.ticker_bid || book_ask != self.ticker_ask {
                self.bbo_mismatch_count += 1;
                if self.bbo_mismatch_count >= BBO_MISMATCH_THRESHOLD {
                    warn!(
                        "{}: BBO diverged from ticker for {} updates (book={}/{}, ticker={}/{})",
                        self.symbol,
                        self.bbo_mismatch_count,
                        book_bid,
                        book_ask,
                        self.ticker_bid,
                        self.ticker_ask
                    );
                    return false;
                }
            } else {
                self.bbo_mismatch_count = 0;
            }
        }

        true
    }

    fn fall_back_to_buffering(
        &mut self,
        #[allow(non_snake_case)] U: u64,
        u: u64,
        pu: u64,
        bids: Vec<(Decimal, Decimal)>,
        asks: Vec<(Decimal, Decimal)>,
    ) {
        self.state = BookState::Buffering;
        self.buffer.clear();
        self.buffer_push((U, u, pu, bids, asks));
        self.bbo_mismatch_count = 0;
    }

    /// Push an event into the buffer, dropping the oldest if at capacity.
    /// Drops are counted -- losing the oldest buffered diff can break the
    /// snapshot bridge, so this must never be silent.
    fn buffer_push(&mut self, event: BufferedEvent) {
        if self.buffer.len() >= BUFFER_MAX {
            self.buffer.pop_front();
            self.buffer_dropped += 1;
        }
        self.buffer.push_back(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn make_levels(pairs: &[(&str, &str)]) -> Vec<(Decimal, Decimal)> {
        pairs
            .iter()
            .map(|(p, q)| (p.parse().unwrap(), q.parse().unwrap()))
            .collect()
    }

    fn make_live_book(
        bids: Option<Vec<(Decimal, Decimal)>>,
        asks: Option<Vec<(Decimal, Decimal)>>,
    ) -> Book {
        let bids = bids.unwrap_or_else(|| make_levels(&[("100", "1")]));
        let asks = asks.unwrap_or_else(|| make_levels(&[("101", "1")]));
        let mut b = Book::new("TEST".to_string());
        b.on_depth(1, 10, 0, vec![], vec![]);
        b.on_snapshot(100, &bids, &asks);
        b.on_depth(101, 110, 99, vec![], vec![]);
        b
    }

    #[test]
    fn test_first_event_transitions_to_buffering() {
        let mut b = Book::new("TEST".to_string());
        assert_eq!(b.state, BookState::Uninitialized);
        let result = b.on_depth(1, 10, 0, vec![], vec![]);
        assert_eq!(b.state, BookState::Buffering);
        assert_eq!(result, Action::NeedSnapshot);
    }

    #[test]
    fn test_snapshot_goes_live() {
        let mut b = Book::new("TEST".to_string());
        b.on_depth(1, 10, 0, make_levels(&[("100", "1")]), make_levels(&[("101", "1")]));
        assert!(b.on_snapshot(5, &make_levels(&[("100", "1")]), &make_levels(&[("101", "1")])));
        assert_eq!(b.state, BookState::Live);
    }

    #[test]
    fn test_snapshot_replays_buffered_events() {
        let mut b = Book::new("TEST".to_string());
        b.on_depth(1, 10, 0, make_levels(&[("100", "1")]), vec![]);
        b.on_depth(11, 20, 10, make_levels(&[("99", "2")]), vec![]);
        b.on_snapshot(5, &make_levels(&[("100", "1")]), &make_levels(&[("101", "1")]));
        assert_eq!(b.state, BookState::Live);
        assert_eq!(b.bids.get(&dec!(99)), Some(&dec!(2)));
    }

    #[test]
    fn test_first_event_after_snapshot_skips_pu_check() {
        let mut b = Book::new("TEST".to_string());
        b.on_depth(1, 10, 0, vec![], vec![]);
        b.on_snapshot(100, &make_levels(&[("100", "1")]), &make_levels(&[("101", "1")]));
        assert_eq!(b.state, BookState::Live);
        let result = b.on_depth(501, 510, 500, make_levels(&[("99", "1")]), vec![]);
        assert_eq!(result, Action::Publish);
    }

    #[test]
    fn test_sequence_gap_falls_back_to_buffering() {
        let mut b = make_live_book(None, None);
        let result = b.on_depth(201, 210, 200, vec![], vec![]);
        assert_eq!(result, Action::NeedSnapshot);
        assert_eq!(b.state, BookState::Buffering);
        assert_eq!(b.gap_count, 1);
    }

    #[test]
    fn test_stale_event_is_ignored() {
        let mut b = make_live_book(None, None);
        let result = b.on_depth(50, 55, 45, make_levels(&[("98", "1")]), vec![]);
        assert_eq!(result, Action::None_);
        assert!(!b.bids.contains_key(&dec!(98)));
    }

    #[test]
    fn test_apply_diff_adds_and_removes() {
        let mut b = make_live_book(
            Some(make_levels(&[("100", "1"), ("99", "2")])),
            Some(make_levels(&[("101", "1"), ("102", "3")])),
        );
        b.on_depth(111, 120, 110, make_levels(&[("100", "0"), ("98", "5")]), make_levels(&[("102", "0"), ("103", "1")]));
        assert!(!b.bids.contains_key(&dec!(100)));
        assert_eq!(b.bids.get(&dec!(98)), Some(&dec!(5)));
        assert!(!b.asks.contains_key(&dec!(102)));
        assert_eq!(b.asks.get(&dec!(103)), Some(&dec!(1)));
    }

    #[test]
    fn test_crossed_book_triggers_resnapshot() {
        let mut b = make_live_book(None, None);
        let result = b.on_depth(111, 120, 110, make_levels(&[("102", "1")]), vec![]);
        assert_eq!(result, Action::NeedSnapshot);
        assert_eq!(b.state, BookState::Buffering);
        assert_eq!(b.crossed_count, 1);
    }

    #[test]
    fn test_bbo_mismatch_at_threshold_triggers_resnapshot() {
        let mut b = make_live_book(None, None);
        b.set_ticker_bbo(dec!(99), dec!(102));
        let mut uid = 110u64;
        let mut result = Action::None_;
        for _ in 0..10 {
            result = b.on_depth(uid + 1, uid + 10, uid, vec![], vec![]);
            uid += 10;
        }
        assert_eq!(result, Action::NeedSnapshot);
        assert_eq!(b.state, BookState::Buffering);
    }

    #[test]
    fn test_reset_clears_all_state() {
        let mut b = make_live_book(None, None);
        b.gap_count = 3;
        b.reset();
        assert_eq!(b.state, BookState::Uninitialized);
        assert!(b.bids.is_empty());
        assert!(b.asks.is_empty());
        assert_eq!(b.last_update_id, 0);
    }

    #[test]
    fn test_top_levels_sorted_correctly() {
        let mut b = Book::new("TEST".to_string());
        b.on_depth(1, 10, 0, vec![], vec![]);
        b.on_snapshot(5, &make_levels(&[("100", "1"), ("98", "2"), ("99", "3")]),
                         &make_levels(&[("101", "1"), ("103", "2"), ("102", "3")]));
        let (top_bids, top_asks) = b.top_levels(2);
        assert_eq!(top_bids[0].0, dec!(100));
        assert_eq!(top_bids[1].0, dec!(99));
        assert_eq!(top_asks[0].0, dec!(101));
        assert_eq!(top_asks[1].0, dec!(102));
    }

    #[test]
    fn test_parse_levels() {
        let raw = vec![
            ("64000.50".to_string(), "1.234".to_string()),
            ("63999.00".to_string(), "0.5".to_string()),
        ];
        let result = parse_levels(&raw);
        assert_eq!(result[0].0, dec!(64000.50));
        assert_eq!(result[0].1, dec!(1.234));
    }

    #[test]
    fn test_snapshot_sync_failure_on_pu_gap_in_buffer() {
        let mut b = Book::new("TEST".to_string());
        b.on_depth(1, 10, 0, vec![], vec![]);
        b.on_depth(11, 20, 10, vec![], vec![]);
        b.on_depth(50, 60, 40, vec![], vec![]);
        assert!(!b.on_snapshot(15, &make_levels(&[("100", "1")]), &make_levels(&[("101", "1")])));
    }

    #[test]
    fn test_events_during_buffering_are_queued() {
        let mut b = Book::new("TEST".to_string());
        b.on_depth(1, 10, 0, vec![], vec![]);
        let result = b.on_depth(11, 20, 10, vec![], vec![]);
        assert_eq!(result, Action::None_);
    }

    #[test]
    fn test_snapshot_skips_old_buffered_events() {
        let mut b = Book::new("TEST".to_string());
        b.on_depth(1, 3, 0, make_levels(&[("50", "1")]), vec![]);
        b.on_depth(4, 10, 3, make_levels(&[("99", "2")]), vec![]);
        b.on_snapshot(8, &make_levels(&[("100", "1")]), &make_levels(&[("101", "1")]));
        assert_eq!(b.state, BookState::Live);
        assert!(!b.bids.contains_key(&dec!(50)));
        assert_eq!(b.bids.get(&dec!(99)), Some(&dec!(2)));
    }

    #[test]
    fn test_first_event_after_snapshot_when_buffer_applied() {
        let mut b = Book::new("TEST".to_string());
        b.on_depth(1, 10, 0, vec![], vec![]);
        b.on_depth(11, 20, 10, vec![], vec![]);
        b.on_snapshot(5, &make_levels(&[("100", "1")]), &make_levels(&[("101", "1")]));
        assert_eq!(b.state, BookState::Live);
        let result = b.on_depth(21, 30, 20, make_levels(&[("99", "1")]), vec![]);
        assert_eq!(result, Action::Publish);
    }

    #[test]
    fn test_zero_qty_in_snapshot_excluded() {
        let mut b = Book::new("TEST".to_string());
        b.on_depth(1, 10, 0, vec![], vec![]);
        b.on_snapshot(
            5,
            &make_levels(&[("100", "0"), ("99", "1")]),
            &make_levels(&[("101", "0"), ("102", "1")]),
        );
        assert!(!b.bids.contains_key(&dec!(100)));
        assert!(!b.asks.contains_key(&dec!(101)));
    }

    #[test]
    fn test_bbo_mismatch_below_threshold_passes() {
        let mut b = make_live_book(None, None);
        b.set_ticker_bbo(dec!(99), dec!(102));
        let result = b.on_depth(111, 120, 110, vec![], vec![]);
        assert_eq!(b.state, BookState::Live);
        assert_eq!(result, Action::Publish);
    }

    #[test]
    fn test_bbo_match_resets_mismatch_counter() {
        let mut b = make_live_book(None, None);
        b.set_ticker_bbo(dec!(99), dec!(102));
        b.on_depth(111, 120, 110, vec![], vec![]);
        b.set_ticker_bbo(dec!(100), dec!(101));
        b.on_depth(121, 130, 120, vec![], vec![]);
        assert_eq!(b.state, BookState::Live);
    }

    #[test]
    fn test_empty_book_passes_integrity() {
        let mut b = Book::new("TEST".to_string());
        b.on_depth(1, 10, 0, vec![], vec![]);
        b.on_snapshot(5, &[], &[]);
        assert_eq!(b.state, BookState::Live);
    }

    #[test]
    fn test_reset_allows_full_resync() {
        let mut b = make_live_book(None, None);
        b.reset();
        let result = b.on_depth(200, 210, 190, vec![], vec![]);
        assert_eq!(result, Action::NeedSnapshot);
        assert_eq!(b.state, BookState::Buffering);
    }

    #[test]
    fn test_snapshot_when_live_is_noop() {
        let mut b = make_live_book(None, None);
        let original_bid_count = b.bids.len();
        assert!(b.on_snapshot(
            999,
            &make_levels(&[("50", "1")]),
            &make_levels(&[("51", "1")]),
        ));
        assert_eq!(b.bids.len(), original_bid_count);
    }

    #[test]
    fn test_multiple_gaps_increment_counter() {
        let mut b = make_live_book(None, None);
        b.on_depth(201, 210, 200, vec![], vec![]);
        assert_eq!(b.gap_count, 1);
        b.on_snapshot(
            300,
            &make_levels(&[("100", "1")]),
            &make_levels(&[("101", "1")]),
        );
        b.on_depth(301, 310, 300, vec![], vec![]);
        b.on_depth(401, 410, 400, vec![], vec![]);
        assert_eq!(b.gap_count, 2);
    }

    // --- snapshot bridge / drop accounting ---

    #[test]
    fn test_snapshot_bridge_broken_is_rejected() {
        // Buffer's first replayable event starts AFTER the snapshot, so the
        // diffs covering (lastUpdateId, U) are gone. Must not go live.
        let mut b = Book::new("TEST".to_string());
        b.on_depth(200, 210, 190, make_levels(&[("99", "1")]), vec![]);
        assert!(!b.on_snapshot(100, &make_levels(&[("100", "1")]), &make_levels(&[("101", "1")])));
        assert_eq!(b.state, BookState::Buffering);
        assert!(!b.bids.contains_key(&dec!(99)));
    }

    #[test]
    fn test_snapshot_bridge_straddling_event_is_accepted() {
        // U <= lastUpdateId <= u: the documented bridge holds, so go live.
        let mut b = Book::new("TEST".to_string());
        b.on_depth(90, 210, 89, make_levels(&[("99", "1")]), vec![]);
        assert!(b.on_snapshot(100, &make_levels(&[("100", "1")]), &make_levels(&[("101", "1")])));
        assert_eq!(b.state, BookState::Live);
        assert_eq!(b.bids.get(&dec!(99)), Some(&dec!(1)));
        assert_eq!(b.unverified_bridge_count, 0);
    }

    #[test]
    fn test_unverified_bridge_is_counted() {
        // Snapshot is newer than everything buffered: nothing straddles it, so
        // the bridge is unproven and we fall back to accepting the next event.
        let mut b = Book::new("TEST".to_string());
        b.on_depth(1, 10, 0, vec![], vec![]);
        assert!(b.on_snapshot(100, &make_levels(&[("100", "1")]), &make_levels(&[("101", "1")])));
        assert_eq!(b.state, BookState::Live);
        assert_eq!(b.unverified_bridge_count, 1);
    }

    #[test]
    fn test_buffer_overflow_drops_are_counted() {
        let mut b = Book::new("TEST".to_string());
        for i in 0..(BUFFER_MAX as u64 + 7) {
            b.on_depth(i * 10 + 1, i * 10 + 10, i * 10, vec![], vec![]);
        }
        assert_eq!(b.buffer_dropped, 7);
    }
}

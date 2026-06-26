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

type BufferedEvent = (
    u64,
    u64,
    u64,
    Vec<(Decimal, Decimal)>,
    Vec<(Decimal, Decimal)>,
);

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
                    // Binance Futures uses global update IDs shared across all symbols.
                    // After a snapshot, the first depth event for THIS symbol often has
                    // U > snapshot.lastUpdateId because other symbols' events filled the
                    // gap. Safe to apply -- no depth changes for this symbol were missed.
                    self.need_first_event = false;
                } else if pu != self.last_update_id {
                    self.gap_count += 1;
                    warn!(
                        "{}: sequence gap (expected pu={}, got {}) [#{}]",
                        self.symbol, self.last_update_id, pu, self.gap_count
                    );
                    self.to_buffering(U, u, pu, bids, asks);
                    return Action::NeedSnapshot;
                }

                self.apply_diff(&bids, &asks, u);

                if !self.check_integrity() {
                    self.to_buffering(U, u, pu, bids, asks);
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
        for (_, buf_u, buf_pu, buf_bids, buf_asks) in &buffered {
            if *buf_u < last_update_id {
                continue;
            }
            if applied_any && *buf_pu != self.last_update_id {
                debug!("{}: pu gap in buffer during sync", self.symbol);
                return false;
            }
            applied_any = true;
            self.apply_diff(buf_bids, buf_asks, *buf_u);
        }

        self.state = BookState::Live;
        self.need_first_event = !applied_any;
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
    pub fn top_levels(&self, n: usize) -> (Vec<(Decimal, Decimal)>, Vec<(Decimal, Decimal)>) {
        // BTreeMap is ascending. Best bids are the highest prices (take from the end).
        let top_bids: Vec<(Decimal, Decimal)> = self
            .bids
            .iter()
            .rev()
            .take(n)
            .map(|(&p, &q)| (p, q))
            .collect();

        // Best asks are the lowest prices (take from the start).
        let top_asks: Vec<(Decimal, Decimal)> =
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

    fn to_buffering(
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
    fn buffer_push(&mut self, event: BufferedEvent) {
        if self.buffer.len() >= BUFFER_MAX {
            self.buffer.pop_front();
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
}

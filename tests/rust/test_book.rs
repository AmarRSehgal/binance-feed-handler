/// Tests for the Rust order book state machine.
///
/// Mirrors the Python test suite in tests/python/test_book.py.
/// Each test name matches its Python counterpart for easy cross-reference.
///
/// To run: place this file in rust/tests/ or include via #[cfg(test)] mod tests in book.rs,
/// then `cargo test`.
#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use crate::book::{Action, Book, BookState, parse_levels};

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

    // === STATE TRANSITIONS ===

    #[test]
    fn test_first_event_transitions_to_buffering() {
        let mut b = Book::new("TEST".to_string());
        assert_eq!(b.state, BookState::Uninitialized);
        let result = b.on_depth(1, 10, 0, vec![], vec![]);
        assert_eq!(b.state, BookState::Buffering);
        assert_eq!(result, Action::NeedSnapshot);
    }

    #[test]
    fn test_events_during_buffering_are_queued() {
        let mut b = Book::new("TEST".to_string());
        b.on_depth(1, 10, 0, vec![], vec![]);
        let result = b.on_depth(11, 20, 10, vec![], vec![]);
        assert_eq!(result, Action::None_);
    }

    #[test]
    fn test_snapshot_goes_live() {
        let mut b = Book::new("TEST".to_string());
        b.on_depth(
            1, 10, 0,
            make_levels(&[("100", "1")]),
            make_levels(&[("101", "1")]),
        );
        assert!(b.on_snapshot(
            5,
            &make_levels(&[("100", "1")]),
            &make_levels(&[("101", "1")]),
        ));
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
    fn test_snapshot_skips_old_buffered_events() {
        let mut b = Book::new("TEST".to_string());
        b.on_depth(1, 3, 0, make_levels(&[("50", "1")]), vec![]);
        b.on_depth(4, 10, 3, make_levels(&[("99", "2")]), vec![]);
        b.on_snapshot(8, &make_levels(&[("100", "1")]), &make_levels(&[("101", "1")]));
        assert_eq!(b.state, BookState::Live);
        assert!(!b.bids.contains_key(&dec!(50)));
        assert_eq!(b.bids.get(&dec!(99)), Some(&dec!(2)));
    }

    // === GLOBAL UPDATE ID HANDLING ===

    #[test]
    fn test_first_event_after_snapshot_skips_pu_check() {
        let mut b = Book::new("TEST".to_string());
        b.on_depth(1, 10, 0, vec![], vec![]);
        b.on_snapshot(100, &make_levels(&[("100", "1")]), &make_levels(&[("101", "1")]));
        assert_eq!(b.state, BookState::Live);
        let result = b.on_depth(501, 510, 500, make_levels(&[("99", "1")]), vec![]);
        assert_eq!(result, Action::Publish);
        assert_eq!(b.state, BookState::Live);
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

    // === SEQUENCE GAP DETECTION ===

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

    // === DIFF APPLICATION ===

    #[test]
    fn test_apply_diff_adds_and_removes() {
        let mut b = make_live_book(
            Some(make_levels(&[("100", "1"), ("99", "2")])),
            Some(make_levels(&[("101", "1"), ("102", "3")])),
        );
        b.on_depth(
            111, 120, 110,
            make_levels(&[("100", "0"), ("98", "5")]),
            make_levels(&[("102", "0"), ("103", "1")]),
        );
        assert!(!b.bids.contains_key(&dec!(100)));
        assert_eq!(b.bids.get(&dec!(98)), Some(&dec!(5)));
        assert!(!b.asks.contains_key(&dec!(102)));
        assert_eq!(b.asks.get(&dec!(103)), Some(&dec!(1)));
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

    // === INTEGRITY CHECKS ===

    #[test]
    fn test_crossed_book_triggers_resnapshot() {
        let mut b = make_live_book(None, None);
        let result = b.on_depth(111, 120, 110, make_levels(&[("102", "1")]), vec![]);
        assert_eq!(result, Action::NeedSnapshot);
        assert_eq!(b.state, BookState::Buffering);
        assert_eq!(b.crossed_count, 1);
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
    fn test_bbo_match_resets_mismatch_counter() {
        let mut b = make_live_book(None, None);
        b.set_ticker_bbo(dec!(99), dec!(102));
        b.on_depth(111, 120, 110, vec![], vec![]);
        b.set_ticker_bbo(dec!(100), dec!(101));
        b.on_depth(121, 130, 120, vec![], vec![]);
        // If mismatch counter reset, book stays live
        assert_eq!(b.state, BookState::Live);
    }

    #[test]
    fn test_empty_book_passes_integrity() {
        let mut b = Book::new("TEST".to_string());
        b.on_depth(1, 10, 0, vec![], vec![]);
        b.on_snapshot(5, &[], &[]);
        assert_eq!(b.state, BookState::Live);
    }

    // === RESET ===

    #[test]
    fn test_reset_clears_all_state() {
        let mut b = make_live_book(None, None);
        b.set_ticker_bbo(dec!(100), dec!(101));
        b.gap_count = 3;
        b.reset();
        assert_eq!(b.state, BookState::Uninitialized);
        assert!(b.bids.is_empty());
        assert!(b.asks.is_empty());
        assert_eq!(b.last_update_id, 0);
    }

    #[test]
    fn test_reset_allows_full_resync() {
        let mut b = make_live_book(None, None);
        b.reset();
        let result = b.on_depth(200, 210, 190, vec![], vec![]);
        assert_eq!(result, Action::NeedSnapshot);
        assert_eq!(b.state, BookState::Buffering);
    }

    // === TOP LEVELS ===

    #[test]
    fn test_top_levels_sorted_correctly() {
        let mut b = Book::new("TEST".to_string());
        b.on_depth(1, 10, 0, vec![], vec![]);
        b.on_snapshot(
            5,
            &make_levels(&[("100", "1"), ("98", "2"), ("99", "3")]),
            &make_levels(&[("101", "1"), ("103", "2"), ("102", "3")]),
        );
        let (top_bids, top_asks) = b.top_levels(2);
        assert_eq!(top_bids[0].0, dec!(100));
        assert_eq!(top_bids[1].0, dec!(99));
        assert_eq!(top_asks[0].0, dec!(101));
        assert_eq!(top_asks[1].0, dec!(102));
    }

    // === PARSE LEVELS ===

    #[test]
    fn test_parse_levels() {
        let raw = vec![
            ("64000.50".to_string(), "1.234".to_string()),
            ("63999.00".to_string(), "0.5".to_string()),
        ];
        let result = parse_levels(&raw);
        assert_eq!(result[0].0, dec!(64000.50));
        assert_eq!(result[0].1, dec!(1.234));
        assert_eq!(result[1].0, dec!(63999.00));
        assert_eq!(result[1].1, dec!(0.5));
    }

    // === SNAPSHOT EDGE CASES ===

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
    fn test_snapshot_sync_failure_on_pu_gap_in_buffer() {
        let mut b = Book::new("TEST".to_string());
        b.on_depth(1, 10, 0, vec![], vec![]);
        b.on_depth(11, 20, 10, vec![], vec![]);
        b.on_depth(50, 60, 40, vec![], vec![]);
        assert!(!b.on_snapshot(
            15,
            &make_levels(&[("100", "1")]),
            &make_levels(&[("101", "1")]),
        ));
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
}

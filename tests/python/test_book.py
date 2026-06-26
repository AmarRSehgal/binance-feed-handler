"""Tests for the order book state machine.

Focuses on the state transitions and edge cases that matter for correctness:
sequence gaps, snapshot sync with global IDs, integrity checks, and reset behavior.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))

from decimal import Decimal
from book import Book, parse_levels

D = Decimal


def make_levels(pairs: list[tuple[str, str]]) -> list[tuple[Decimal, Decimal]]:
    return [(D(p), D(q)) for p, q in pairs]


def make_live_book(bids=None, asks=None):
    """Helper: create a Book that's already live with given levels and last_update_id=100."""
    b = Book("TEST")
    if bids is None:
        bids = make_levels([("100", "1")])
    if asks is None:
        asks = make_levels([("101", "1")])
    b.on_depth(1, 10, 0, [], [])
    b.on_snapshot(100, bids, asks)
    # Snapshot lastUpdateId=100, buffered event u=10 < 100 so skipped.
    # _need_first_event = True (no buffered events applied).
    # Send first event to consume the flag and get to steady state.
    b.on_depth(101, 110, 99, [], [])
    # Now live, last_update_id=110, _need_first_event=False.
    return b


# === STATE TRANSITIONS ===

def test_first_event_transitions_to_buffering():
    b = Book("TEST")
    assert b.state == "uninitialized"
    result = b.on_depth(1, 10, 0, [], [])
    assert b.state == "buffering"
    assert result == "need_snapshot"


def test_events_during_buffering_are_queued():
    b = Book("TEST")
    b.on_depth(1, 10, 0, [], [])
    result = b.on_depth(11, 20, 10, [], [])
    assert result is None
    assert len(b.buffer) == 2


def test_snapshot_goes_live():
    b = Book("TEST")
    b.on_depth(1, 10, 0, make_levels([("100", "1")]), make_levels([("101", "1")]))
    bids = make_levels([("100", "1")])
    asks = make_levels([("101", "1")])
    assert b.on_snapshot(5, bids, asks)
    assert b.state == "live"


def test_snapshot_replays_buffered_events():
    b = Book("TEST")
    b.on_depth(1, 10, 0, make_levels([("100", "1")]), [])
    b.on_depth(11, 20, 10, make_levels([("99", "2")]), [])
    b.on_snapshot(5, make_levels([("100", "1")]), make_levels([("101", "1")]))
    assert b.state == "live"
    assert b.bids[D("99")] == D("2")


def test_snapshot_skips_old_buffered_events():
    """Events with u < snapshot.lastUpdateId should be ignored during replay."""
    b = Book("TEST")
    b.on_depth(1, 3, 0, make_levels([("50", "1")]), [])
    b.on_depth(4, 10, 3, make_levels([("99", "2")]), [])
    b.on_snapshot(8, make_levels([("100", "1")]), make_levels([("101", "1")]))
    assert b.state == "live"
    assert D("50") not in b.bids
    assert b.bids[D("99")] == D("2")


# === GLOBAL UPDATE ID HANDLING ===

def test_first_event_after_snapshot_skips_pu_check():
    """Binance Futures: first depth event after snapshot may have U > snapshot.lastUpdateId
    because global IDs from other symbols filled the gap."""
    b = Book("TEST")
    b.on_depth(1, 10, 0, [], [])
    # Snapshot lastUpdateId=100, buffered event u=10 < 100 so skipped entirely.
    b.on_snapshot(100, make_levels([("100", "1")]), make_levels([("101", "1")]))
    assert b.state == "live"
    assert b._need_first_event is True
    # First real event has pu=500 (way past 100) -- should still be accepted.
    result = b.on_depth(501, 510, 500, make_levels([("99", "1")]), [])
    assert result == "publish"
    assert b.state == "live"
    assert b._need_first_event is False


def test_first_event_after_snapshot_when_buffer_applied():
    """If buffered events were applied during snapshot, _need_first_event is False
    and normal pu checking resumes immediately."""
    b = Book("TEST")
    b.on_depth(1, 10, 0, [], [])
    b.on_depth(11, 20, 10, [], [])
    # Snapshot lastUpdateId=5, both buffered events have u >= 5 so they're applied.
    b.on_snapshot(5, make_levels([("100", "1")]), make_levels([("101", "1")]))
    assert b.state == "live"
    assert not b._need_first_event
    result = b.on_depth(21, 30, 20, make_levels([("99", "1")]), [])
    assert result == "publish"


# === SEQUENCE GAP DETECTION ===

def test_sequence_gap_falls_back_to_buffering():
    b = make_live_book()
    # last_update_id is 110. Send event with pu=200 (gap).
    result = b.on_depth(201, 210, 200, [], [])
    assert result == "need_snapshot"
    assert b.state == "buffering"
    assert b.gap_count == 1


def test_stale_event_is_ignored():
    """Events with u <= last_update_id are silently dropped."""
    b = make_live_book()
    result = b.on_depth(50, 55, 45, make_levels([("98", "1")]), [])
    assert result is None
    assert D("98") not in b.bids


# === DIFF APPLICATION ===

def test_apply_diff_adds_and_removes():
    b = make_live_book(
        bids=make_levels([("100", "1"), ("99", "2")]),
        asks=make_levels([("101", "1"), ("102", "3")]),
    )
    b.on_depth(111, 120, 110,
               make_levels([("100", "0"), ("98", "5")]),
               make_levels([("102", "0"), ("103", "1")]))
    assert D("100") not in b.bids
    assert b.bids[D("98")] == D("5")
    assert D("102") not in b.asks
    assert b.asks[D("103")] == D("1")


def test_zero_qty_in_snapshot_excluded():
    b = Book("TEST")
    b.on_depth(1, 10, 0, [], [])
    b.on_snapshot(5, make_levels([("100", "0"), ("99", "1")]),
                  make_levels([("101", "0"), ("102", "1")]))
    assert D("100") not in b.bids
    assert D("101") not in b.asks


# === INTEGRITY CHECKS ===

def test_crossed_book_triggers_resnapshot():
    b = make_live_book()
    # Add a bid at 102, which crosses the ask at 101.
    result = b.on_depth(111, 120, 110, make_levels([("102", "1")]), [])
    assert result == "need_snapshot"
    assert b.state == "buffering"
    assert b.crossed_count == 1


def test_bbo_mismatch_below_threshold_passes():
    b = make_live_book()
    b.set_ticker_bbo(D("99"), D("102"))
    result = b.on_depth(111, 120, 110, [], [])
    assert b.state == "live"
    assert result == "publish"
    assert b._bbo_mismatch_count == 1


def test_bbo_mismatch_at_threshold_triggers_resnapshot():
    b = make_live_book()
    b.set_ticker_bbo(D("99"), D("102"))
    uid = 110
    for _ in range(10):
        result = b.on_depth(uid + 1, uid + 10, uid, [], [])
        uid += 10
    assert result == "need_snapshot"
    assert b.state == "buffering"


def test_bbo_match_resets_mismatch_counter():
    b = make_live_book()
    b.set_ticker_bbo(D("99"), D("102"))
    b.on_depth(111, 120, 110, [], [])
    assert b._bbo_mismatch_count == 1
    b.set_ticker_bbo(D("100"), D("101"))
    b.on_depth(121, 130, 120, [], [])
    assert b._bbo_mismatch_count == 0


def test_empty_book_passes_integrity():
    b = Book("TEST")
    b.on_depth(1, 10, 0, [], [])
    b.on_snapshot(5, [], [])
    assert b.state == "live"


# === RESET ===

def test_reset_clears_all_state():
    b = make_live_book()
    b.set_ticker_bbo(D("100"), D("101"))
    b.gap_count = 3
    b.reset()
    assert b.state == "uninitialized"
    assert len(b.bids) == 0
    assert len(b.asks) == 0
    assert b.last_update_id == 0
    assert b._ticker_bid == D(0)
    assert b._ticker_ask == D(0)
    assert b._bbo_mismatch_count == 0


def test_reset_allows_full_resync():
    b = make_live_book()
    b.reset()
    result = b.on_depth(200, 210, 190, [], [])
    assert result == "need_snapshot"
    assert b.state == "buffering"


# === TOP LEVELS ===

def test_top_levels_sorted_correctly():
    b = Book("TEST")
    b.on_depth(1, 10, 0, [], [])
    b.on_snapshot(5,
                  make_levels([("100", "1"), ("98", "2"), ("99", "3")]),
                  make_levels([("101", "1"), ("103", "2"), ("102", "3")]))
    top_bids, top_asks = b.top_levels(2)
    assert [p for p, _ in top_bids] == [D("100"), D("99")]
    assert [p for p, _ in top_asks] == [D("101"), D("102")]


# === PARSE LEVELS ===

def test_parse_levels():
    raw = [["64000.50", "1.234"], ["63999.00", "0.5"]]
    result = parse_levels(raw)
    assert result == [(D("64000.50"), D("1.234")), (D("63999.00"), D("0.5"))]


# === SNAPSHOT EDGE CASES ===

def test_snapshot_when_live_is_noop():
    b = make_live_book()
    original_bids = dict(b.bids)
    assert b.on_snapshot(999, make_levels([("50", "1")]), make_levels([("51", "1")]))
    assert b.bids == original_bids


def test_snapshot_sync_failure_on_pu_gap_in_buffer():
    """If buffered events have a pu gap during replay, snapshot fails."""
    b = Book("TEST")
    b.on_depth(1, 10, 0, [], [])
    b.on_depth(11, 20, 10, [], [])
    b.on_depth(50, 60, 40, [], [])
    result = b.on_snapshot(15, make_levels([("100", "1")]), make_levels([("101", "1")]))
    assert result is False


def test_multiple_gaps_increment_counter():
    b = make_live_book()
    b.on_depth(201, 210, 200, [], [])
    assert b.gap_count == 1
    b.on_snapshot(300, make_levels([("100", "1")]), make_levels([("101", "1")]))
    b.on_depth(301, 310, 300, [], [])
    b.on_depth(401, 410, 400, [], [])
    assert b.gap_count == 2

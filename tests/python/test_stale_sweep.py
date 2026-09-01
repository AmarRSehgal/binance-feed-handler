"""Tests for the staleness sweeper.

A live book that stops receiving depth events must be marked for resync AND
have a snapshot requested -- reset() alone leaves the symbol dark until the next
depth event, which on an illiquid perp can be minutes.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))

from decimal import Decimal

from book import Book
from feed_handler import take_stale_books

D = Decimal


def live_book(symbol: str, last_update_time: float) -> Book:
    b = Book(symbol)
    b.on_depth(1, 10, 0, [], [])
    b.on_snapshot(100, [(D(100), D(1))], [(D(101), D(1))])
    b.last_update_time = last_update_time
    return b


def books_of(*books: Book) -> dict[str, Book]:
    return {b.symbol: b for b in books}


def test_stale_book_is_marked_for_resync():
    books = books_of(live_book("STALE", 0.0))
    assert take_stale_books(books, 100.0, 30.0) == ["STALE"]
    # "buffering" (not "uninitialized") is what makes sync_book actually fetch.
    assert books["STALE"].state == "buffering"


def test_fresh_book_is_left_alone():
    books = books_of(live_book("FRESH", 90.0))
    assert take_stale_books(books, 100.0, 30.0) == []
    assert books["FRESH"].state == "live"


def test_non_live_book_is_not_swept():
    b = Book("BUFFERING")
    b.on_depth(1, 10, 0, [], [])
    assert take_stale_books(books_of(b), 1e9, 30.0) == []


def test_sweep_does_not_refire_on_the_next_tick():
    # A symbol whose stream is dead must not re-snapshot every sweep: the
    # trigger pushes last_update_time forward, and the book is no longer live.
    books = books_of(live_book("DEAD", 0.0))
    assert take_stale_books(books, 100.0, 30.0) == ["DEAD"]
    assert take_stale_books(books, 115.0, 30.0) == []


def test_mark_for_resync_clears_book_but_stays_buffering():
    b = live_book("X", 0.0)
    assert b.bids
    b.mark_for_resync()
    assert b.state == "buffering"
    assert not b.bids and not b.asks
    assert b.last_update_id == 0
    # A depth event arriving now is buffered, not treated as a first event.
    assert b.on_depth(200, 210, 190, [], []) is None

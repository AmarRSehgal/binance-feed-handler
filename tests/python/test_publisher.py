"""Tests for the pub/sub dispatcher and filtering logic.

Focuses on subscriber routing, symbol filtering, stream filtering,
and drop-oldest overflow behavior.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))

import asyncio
import pytest
import publisher


@pytest.fixture(autouse=True)
def clear_subscribers():
    publisher._subscribers.clear()
    yield
    publisher._subscribers.clear()


# === SUBSCRIBE / UNSUBSCRIBE ===

def test_subscribe_returns_queue():
    q = publisher.subscribe()
    assert isinstance(q, asyncio.Queue)
    assert len(publisher._subscribers) == 1


def test_subscribe_with_symbol_filter():
    q = publisher.subscribe(stream="bbo", symbols={"BTCUSDT"})
    sub = publisher._subscribers[0]
    assert sub["symbols"] == {"BTCUSDT"}
    assert sub["stream"] == "bbo"


def test_unsubscribe_removes_by_queue_identity():
    q1 = publisher.subscribe()
    q2 = publisher.subscribe()
    assert len(publisher._subscribers) == 2
    publisher.unsubscribe(q1)
    assert len(publisher._subscribers) == 1
    assert publisher._subscribers[0]["queue"] is q2


def test_unsubscribe_nonexistent_is_harmless():
    q1 = publisher.subscribe()
    fake_q = asyncio.Queue()
    publisher.unsubscribe(fake_q)
    assert len(publisher._subscribers) == 1


# === PUBLISH FILTERING ===

def test_publish_routes_to_matching_stream():
    q_bbo = publisher.subscribe(stream="bbo")
    q_book = publisher.subscribe(stream="book")
    publisher._publish({"symbol": "BTCUSDT", "bid": 100}, "bbo")
    assert not q_bbo.empty()
    assert q_book.empty()


def test_publish_all_stream_receives_everything():
    q_all = publisher.subscribe(stream="all")
    publisher._publish({"symbol": "BTCUSDT"}, "bbo")
    publisher._publish({"symbol": "BTCUSDT"}, "book")
    assert q_all.qsize() == 2


def test_publish_symbol_filter():
    q = publisher.subscribe(stream="bbo", symbols={"BTCUSDT"})
    publisher._publish({"symbol": "BTCUSDT"}, "bbo")
    publisher._publish({"symbol": "ETHUSDT"}, "bbo")
    assert q.qsize() == 1
    msg = q.get_nowait()
    assert msg["symbol"] == "BTCUSDT"


def test_publish_no_symbol_filter_receives_all():
    q = publisher.subscribe(stream="bbo")
    publisher._publish({"symbol": "BTCUSDT"}, "bbo")
    publisher._publish({"symbol": "ETHUSDT"}, "bbo")
    assert q.qsize() == 2


def test_publish_fan_out_to_multiple_subscribers():
    q1 = publisher.subscribe(stream="bbo")
    q2 = publisher.subscribe(stream="bbo")
    publisher._publish({"symbol": "BTCUSDT"}, "bbo")
    assert q1.qsize() == 1
    assert q2.qsize() == 1


# === OVERFLOW BEHAVIOR ===

def test_put_drops_oldest_on_full():
    q = asyncio.Queue(maxsize=2)
    publisher._put(q, {"seq": 1})
    publisher._put(q, {"seq": 2})
    publisher._put(q, {"seq": 3})
    assert q.qsize() == 2
    first = q.get_nowait()
    assert first["seq"] == 2


# === DISPATCHER ===

@pytest.mark.asyncio
async def test_dispatcher_routes_messages():
    bbo_q = asyncio.Queue()
    book_q = asyncio.Queue()
    sub_q = publisher.subscribe(stream="bbo", symbols={"BTCUSDT"})

    task = asyncio.create_task(publisher.run_dispatcher(bbo_q, book_q))
    await bbo_q.put({"symbol": "BTCUSDT", "bid": 100})
    await bbo_q.put({"symbol": "ETHUSDT", "bid": 200})
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert sub_q.qsize() == 1
    msg = sub_q.get_nowait()
    assert msg["symbol"] == "BTCUSDT"

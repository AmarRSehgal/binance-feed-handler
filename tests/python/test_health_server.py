"""End-to-end tests for the /health, /stats and /metrics handlers.

render_prometheus() being unit-tested is not the same as /metrics working: the
first version of the handler passed aiohttp both content_type= and an explicit
content-type header, which is a ValueError, and every renderer test still passed.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))

import asyncio
import socket
from decimal import Decimal as D

import aiohttp

from book import Book
from feed_handler import METRIC_FAMILIES, start_health_server


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def live_book(symbol: str) -> Book:
    b = Book(symbol)
    b.on_depth(1, 10, 0, [], [])
    b.on_snapshot(100, [(D(100), D(1))], [(D(101), D(1))])
    return b


def shard(connected: bool, latency):
    return {"id": 0, "connected": connected, "msg_count": 3,
            "last_msg_time": 1.0, "latency_ms": latency}


async def serve(books, shard_infos) -> str:
    port = free_port()
    await start_health_server(books, shard_infos, port, start_time=0.0)
    return f"http://127.0.0.1:{port}"


def test_health_200_when_every_shard_is_connected_and_books_are_live():
    async def scenario():
        base = await serve({"BTCUSDT": live_book("BTCUSDT")}, [shard(True, 5.0)])
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{base}/health") as r:
                assert r.status == 200
                assert (await r.json())["healthy"] is True

    asyncio.run(scenario())


def test_health_503_when_a_shard_is_down():
    async def scenario():
        base = await serve({"BTCUSDT": live_book("BTCUSDT")}, [shard(False, None)])
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{base}/health") as r:
                assert r.status == 503
                assert (await r.json())["healthy"] is False

    asyncio.run(scenario())


def test_metrics_serves_prometheus_text_with_every_family():
    async def scenario():
        base = await serve({"BTCUSDT": live_book("BTCUSDT")}, [shard(True, -85.9)])
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{base}/metrics") as r:
                assert r.status == 200
                assert r.headers["content-type"].startswith("text/plain")
                body = await r.text()
        for fam in METRIC_FAMILIES:
            assert f"# TYPE {fam} " in body, f"{fam} missing from /metrics"
        assert "bfh_up 1\n" in body
        # A negative sample must survive to the wire, not be filtered to zero.
        assert 'bfh_shard_latency_ms{shard="0"} -85.9' in body

    asyncio.run(scenario())


def test_stats_exposes_every_shed_point_and_the_bridge_counter():
    async def scenario():
        base = await serve({"BTCUSDT": live_book("BTCUSDT")}, [shard(True, None)])
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{base}/stats") as r:
                assert r.status == 200
                stats = await r.json()
        assert stats["book_states"] == {"live": 1}
        assert set(stats["dropped"]) == {"bbo_queue", "book_queue",
                                         "subscriber_queue", "book_buffer"}
        assert stats["total_unverified_bridges"] == 1
        assert stats["max_latency_ms"] is None

    asyncio.run(scenario())

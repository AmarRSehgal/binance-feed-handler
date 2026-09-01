"""Tests for the ranked snapshot pacer.

A ~570-symbol cold start is floor-bounded near 190s by Binance's REST weight
budget, so ORDER is what matters: the previous pacer was a lock plus a timestamp
and served whoever contended first, which put BTCUSDT (rank 0 of 568) as likely
at second 180 as at second 1.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))

import asyncio

import pytest

from feed_handler import SnapshotPacer, rank_by_volume


def run(coro):
    return asyncio.run(coro)


def test_pacer_serves_lowest_rank_first():
    async def scenario():
        # Every symbol asks at once, exactly as a cold start does.
        pacer = SnapshotPacer(rate=100.0)
        served = []

        async def ask(rank):
            await pacer.acquire(rank)
            served.append(rank)

        tasks = [asyncio.create_task(ask(r)) for r in (7, 3, 0, 5, 1)]
        # Let all five register before the first slot is handed out.
        await asyncio.sleep(0)
        await asyncio.gather(*tasks)
        pacer.stop()
        return served

    assert run(scenario()) == [0, 1, 3, 5, 7]


def test_pacer_breaks_rank_ties_fifo():
    async def scenario():
        pacer = SnapshotPacer(rate=100.0)
        served = []

        async def ask(tag):
            await pacer.acquire(5)
            served.append(tag)

        tasks = []
        for tag in ("a", "b", "c"):
            tasks.append(asyncio.create_task(ask(tag)))
            await asyncio.sleep(0)
        await asyncio.gather(*tasks)
        pacer.stop()
        return served

    assert run(scenario()) == ["a", "b", "c"]


def test_pacer_enforces_the_rate():
    async def scenario():
        pacer = SnapshotPacer(rate=50.0)
        loop = asyncio.get_running_loop()
        start = loop.time()
        for _ in range(5):
            await pacer.acquire(0)
        elapsed = loop.time() - start
        pacer.stop()
        return elapsed

    # 5 slots at 50/s, the first one interval out: 5 x 20ms.
    assert run(scenario()) >= 0.100


def test_nonpositive_rate_is_fatal_at_startup():
    async def scenario():
        with pytest.raises(SystemExit):
            SnapshotPacer(rate=0.0)

    run(scenario())


def test_rank_is_zero_based_and_liquidity_ordered():
    ranks = rank_by_volume(["AAAUSDT", "BTCUSDT", "ZZZUSDT"],
                           {"AAAUSDT": 1.0, "BTCUSDT": 9e9, "ZZZUSDT": 5.0})
    assert ranks == {"BTCUSDT": 0, "ZZZUSDT": 1, "AAAUSDT": 2}


def test_rank_puts_symbols_missing_from_the_ticker_last():
    ranks = rank_by_volume(["AAA", "MISSING"], {"AAA": 1.0})
    assert ranks["AAA"] == 0
    assert ranks["MISSING"] == 1

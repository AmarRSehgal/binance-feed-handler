"""Tests for the telemetry snapshot and Prometheus rendering.

The metric-family list is a cross-language contract: METRIC_FAMILIES here must
match feed_handler::METRIC_FAMILIES in the Rust port, or a scraper's dashboard
breaks depending on which implementation is deployed.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))

import feed_handler
from feed_handler import METRIC_FAMILIES, Telemetry, render_prometheus


def telemetry_fixture(**overrides) -> Telemetry:
    base = dict(
        uptime_s=42,
        shards=[{"id": 0, "connected": True, "msg_count": 7,
                 "last_msg_time": 1.0, "latency_ms": 12.5}],
        connected=1,
        book_states={"live": 2},
        live=2,
        total=2,
        gaps=1,
        crossed=0,
        snapshots_per_book=3,
        unverified_bridges=1,
        buffer_dropped=0,
        max_latency_ms=12.5,
        bbo_published=100,
        book_published=50,
        snapshots=3,
        bbo_dropped=0,
        book_dropped=0,
        subscriber_dropped=0,
        subscribers=1,
    )
    base.update(overrides)
    return Telemetry(**base)


def declared_families(body: str) -> list[str]:
    return [line.split()[0] for line in
            (l[len("# TYPE "):] for l in body.splitlines() if l.startswith("# TYPE "))]


def test_prometheus_declares_every_family_exactly_once():
    body = render_prometheus(telemetry_fixture())
    for fam in METRIC_FAMILIES:
        assert body.count(f"# TYPE {fam} ") == 1, f"{fam} must be declared exactly once"
    # Nothing may be emitted that is not in the declared contract.
    assert declared_families(body) == METRIC_FAMILIES


def test_prometheus_reports_unhealthy_as_zero():
    t = telemetry_fixture(connected=0)
    assert not t.healthy
    assert "bfh_up 0\n" in render_prometheus(t)


def test_healthy_needs_all_shards_and_a_live_majority():
    assert telemetry_fixture().healthy
    assert not telemetry_fixture(live=1).healthy, "1 of 2 live is not a majority"
    assert not telemetry_fixture(connected=0).healthy, "a disconnected shard is never healthy"


def test_total_dropped_sums_every_shed_point():
    t = telemetry_fixture(bbo_dropped=1, book_dropped=2,
                          subscriber_dropped=4, buffer_dropped=8)
    assert t.total_dropped == 15


def test_queue_overflow_is_counted_not_silent():
    import asyncio

    q = asyncio.Queue(maxsize=2)
    assert feed_handler._put(q, "a") is True
    assert feed_handler._put(q, "b") is True
    # Third put overflows: oldest is shed and the caller is told about it.
    assert feed_handler._put(q, "c") is False
    assert q.qsize() == 2
    assert q.get_nowait() == "b"

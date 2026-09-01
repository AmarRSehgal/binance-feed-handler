"""Binance USD-M Futures feed handler.

Subscribes to all perpetual futures, maintains per-symbol order books,
and publishes BBO + book updates to downstream consumers via asyncio queues.

Usage:
    python feed_handler.py                    # all ~570 perps
    python feed_handler.py --max-symbols 10   # just 10 for testing
"""
import argparse
import asyncio
import json
import logging
import random
import signal
import time
from dataclasses import dataclass
from decimal import Decimal

import aiohttp
import websockets

from book import Book, parse_levels
import publisher

logger = logging.getLogger(__name__)

# === CONFIGURATION ===

BINANCE_FAPI = "https://fapi.binance.com"
BINANCE_WS = "wss://fstream.binance.com/ws"

MAX_SYMBOLS_PER_SHARD = 100
SUBSCRIBE_BATCH_SIZE = 50
SNAPSHOT_RATE_DEFAULT = 3.0
RECONNECT_BASE = 1.0
RECONNECT_MAX = 30.0
STATS_INTERVAL = 10
STALE_THRESHOLD = 30.0
# Negative event-time lag below this means the local clock disagrees with the
# venue by enough that reported latencies are meaningless. Warned, not hidden.
CLOCK_SKEW_WARN_MS = 50.0
QUEUE_MAX = 100_000

# Global snapshot pacer + liquidity ranks (initialized in main)
_pacer: "SnapshotPacer | None" = None
_snapshot_rank: dict[str, int] = {}
_snap_count = 0

# Publish counters
_bbo_count = 0
_book_count = 0

# Events shed because a dispatcher queue was full. Never silent: surfaced in
# /stats, /metrics and the periodic stats line.
_bbo_dropped = 0
_book_dropped = 0


# === SNAPSHOT PACING ===

class SnapshotPacer:
    """The only rate limit on /fapi/v1/depth, handing out slots in liquidity-rank
    order.

    Weight math: /fapi/v1/depth?limit=500 costs 10 request weight and the USD-M IP
    budget is 2400 weight/minute, so 4.0 req/s is 100% of the budget. The 3.0
    default leaves headroom for exchangeInfo/ticker and for the resnapshots that
    gaps and staleness trigger during steady state. A ~570-symbol cold start is
    therefore floor-bounded near 190s -- that is the venue's limit, not ours.

    Which is why ORDER matters more than throughput. The previous pacer was a lock
    plus a last-request timestamp, so slots went out in whatever order tasks
    happened to contend, and BTCUSDT (rank 0 of 568) was as likely to be served at
    second 180 as at second 1.
    """

    def __init__(self, rate: float):
        if rate <= 0:
            raise SystemExit(f"--snapshot-rate must be positive, got {rate}")
        self._interval = 1.0 / rate
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._seq = 0
        self._task = asyncio.create_task(self._pace())

    async def acquire(self, rank: int):
        """Wait for a slot. Lower `rank` is served first."""
        fut = asyncio.get_running_loop().create_future()
        self._seq += 1
        await self._queue.put((rank, self._seq, fut))
        await fut

    async def _pace(self):
        loop = asyncio.get_running_loop()
        # Start one interval out rather than at now(): the first slot is the most
        # valuable one of a cold start, and granting it instantly would hand it to
        # whichever of ~570 sync tasks happened to register first instead of to
        # rank 0.
        next_at = loop.time() + self._interval
        while True:
            wait = next_at - loop.time()
            if wait > 0:
                await asyncio.sleep(wait)
            # get() after the sleep, so a rank-0 request that arrived during it
            # does not queue behind the tail symbol that happened to ask first.
            _rank, _seq, fut = await self._queue.get()
            if fut.done():
                # Requester was cancelled; do not burn a slot on it.
                continue
            fut.set_result(None)
            next_at = loop.time() + self._interval

    def stop(self):
        self._task.cancel()


# === HELPERS ===

def _put(q: asyncio.Queue, item) -> bool:
    """Enqueue, shedding the OLDEST item on overflow. Returns False if anything
    was shed so the caller can count it.

    Cross-language divergence, deliberate and documented: Rust's tokio mpsc has
    no sender-side pop, so the Rust handler sheds the NEWEST event instead.
    Both count into the same metric name.
    """
    try:
        q.put_nowait(item)
        return True
    except asyncio.QueueFull:
        pass
    try:
        q.get_nowait()
    except asyncio.QueueEmpty:
        pass
    try:
        q.put_nowait(item)
    except asyncio.QueueFull:
        pass
    return False


# === REST ===

def rank_by_volume(symbols: list[str], volumes: dict[str, float]) -> dict[str, int]:
    """Rank symbols by 24h quote volume, 0 = most liquid. Drives both the
    --max-symbols cap and snapshot ordering. Symbols missing from the ticker
    response rank last; ties break on name, so the ordering is reproducible."""
    ranked = sorted(symbols, key=lambda s: (-volumes.get(s, 0.0), s))
    return {sym: i for i, sym in enumerate(ranked)}


def pick_top_by_volume(symbols: list[str], volumes: dict[str, float], n: int) -> list[str]:
    """Keep the `n` highest-24h-quote-volume symbols, returned alphabetically so
    shard assignment stays deterministic across restarts.

    A plain symbols[:n] after an alphabetical sort is what this replaces: it
    selected 1000BONK/1000FLOKI/AAVE/ACE... and never once included BTCUSDT, so
    every capped test run exercised the thinnest books on the venue.
    Symbols missing from the ticker response rank last; ties break on name.
    """
    ranks = rank_by_volume(symbols, volumes)
    return sorted(s for s in symbols if ranks[s] < n)


async def fetch_quote_volumes(session: aiohttp.ClientSession) -> dict[str, float]:
    url = f"{BINANCE_FAPI}/fapi/v1/ticker/24hr"
    async with session.get(url) as resp:
        resp.raise_for_status()
        data = await resp.json()
    return {t["symbol"]: float(t["quoteVolume"]) for t in data}


async def resolve_universe(session: aiohttp.ClientSession, max_symbols: int | None = None,
                          explicit: list[str] | None = None
                          ) -> tuple[list[str], dict[str, int]]:
    """The symbol set to track (alphabetical, for deterministic sharding) plus the
    order to snapshot it in (0 = snapshot first, ranked by 24h quote volume)."""
    url = f"{BINANCE_FAPI}/fapi/v1/exchangeInfo"
    async with session.get(url) as resp:
        resp.raise_for_status()
        data = await resp.json()
    symbols = sorted(
        s["symbol"] for s in data["symbols"]
        if s["contractType"] == "PERPETUAL" and s["status"] == "TRADING"
    )

    if explicit:
        # A typo'd symbol would otherwise subscribe to a stream that never ticks
        # and look like a dead feed, so reject it at startup.
        unknown = [s for s in explicit if s not in set(symbols)]
        if unknown:
            raise SystemExit(f"--symbols: not a trading USD-M perpetual: {', '.join(unknown)}")
        symbols = sorted(set(explicit))

    # One ticker call serves both the cap and the snapshot ordering. Fatal on
    # failure: this is startup, and a silent fall back to arrival order would
    # reintroduce "BTCUSDT goes live at minute three" without saying so.
    volumes = await fetch_quote_volumes(session)
    if not explicit and max_symbols and max_symbols < len(symbols):
        symbols = pick_top_by_volume(symbols, volumes, max_symbols)
    return symbols, rank_by_volume(symbols, volumes)


async def fetch_snapshot(session: aiohttp.ClientSession, symbol: str) -> dict:
    """Fetch a REST depth snapshot, paced in liquidity-rank order."""
    global _snap_count

    await _pacer.acquire(_snapshot_rank.get(symbol, 1 << 30))

    url = f"{BINANCE_FAPI}/fapi/v1/depth"
    async with session.get(url, params={"symbol": symbol, "limit": 500}) as resp:
        _snap_count += 1
        if resp.status == 429:
            retry_after = int(resp.headers.get("Retry-After", "10"))
            logger.warning("%s: rate limited, backing off %ds", symbol, retry_after)
            await asyncio.sleep(retry_after)
            raise Exception(f"rate limited ({symbol})")
        resp.raise_for_status()
        return await resp.json()


# === SNAPSHOT SYNC ===

async def sync_book(session: aiohttp.ClientSession, book: Book):
    """Fetch snapshots until the book goes live. Retries with backoff on failure."""
    delay = 1.0
    while book.state == "buffering":
        try:
            data = await fetch_snapshot(session, book.symbol)
            if book.on_snapshot(data["lastUpdateId"],
                                parse_levels(data["bids"]),
                                parse_levels(data["asks"])):
                return
            await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error("%s: snapshot failed: %s. Retry in %.1fs", book.symbol, e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10.0)


# === WEBSOCKET ===

async def run_shard(shard_id: int, symbols: list[str], books: dict[str, Book],
                    session: aiohttp.ClientSession, bbo_q: asyncio.Queue,
                    book_q: asyncio.Queue, info: dict):
    """Run one WS connection for a subset of symbols. Reconnects forever."""
    global _bbo_count, _book_count, _bbo_dropped, _book_dropped
    reconnect_delay = RECONNECT_BASE
    sync_tasks: dict[str, asyncio.Task] = {}

    def _request_sync(sym):
        if sym in sync_tasks and not sync_tasks[sym].done():
            sync_tasks[sym].cancel()
        sync_tasks[sym] = asyncio.create_task(sync_book(session, books[sym]))

    while True:
        try:
            stream_names = []
            for sym in symbols:
                s = sym.lower()
                stream_names.append(f"{s}@depth@100ms")
                stream_names.append(f"{s}@bookTicker")

            logger.info("Shard %d: connecting (%d symbols)", shard_id, len(symbols))

            async with websockets.connect(BINANCE_WS, ping_interval=20,
                                          ping_timeout=10, max_size=10_000_000) as ws:
                info["connected"] = True
                reconnect_delay = RECONNECT_BASE

                for i in range(0, len(stream_names), SUBSCRIBE_BATCH_SIZE):
                    batch = stream_names[i : i + SUBSCRIBE_BATCH_SIZE]
                    await ws.send(json.dumps({
                        "method": "SUBSCRIBE", "params": batch,
                        "id": i // SUBSCRIBE_BATCH_SIZE + 1,
                    }))
                    await asyncio.sleep(0.25)

                logger.info("Shard %d: subscribed (%d streams)", shard_id, len(stream_names))

                async for raw in ws:
                    info["msg_count"] += 1
                    info["last_msg_time"] = time.time()

                    msg = json.loads(raw)
                    if "result" in msg or ("id" in msg and "e" not in msg):
                        continue

                    symbol = msg.get("s")
                    if not symbol or symbol not in books:
                        continue

                    if "E" in msg:
                        info["latency_ms"] = time.time() * 1000 - msg["E"]

                    book = books[symbol]
                    evt = msg["e"]

                    if evt == "depthUpdate":
                        book.last_update_time = time.time()
                        action = book.on_depth(
                            msg["U"], msg["u"], msg["pu"],
                            parse_levels(msg["b"]), parse_levels(msg["a"]),
                        )
                        if action == "need_snapshot":
                            _request_sync(symbol)
                        elif action == "publish":
                            _book_count += 1
                            top_bids, top_asks = book.top_levels()
                            if not _put(book_q, {
                                "symbol": symbol, "bids": top_bids, "asks": top_asks,
                                "last_update_id": book.last_update_id, "ts": time.time(),
                            }):
                                _book_dropped += 1

                    elif evt == "bookTicker":
                        book.set_ticker_bbo(Decimal(msg["b"]), Decimal(msg["a"]))
                        _bbo_count += 1
                        if not _put(bbo_q, {
                            "symbol": symbol,
                            "bid_price": Decimal(msg["b"]), "bid_qty": Decimal(msg["B"]),
                            "ask_price": Decimal(msg["a"]), "ask_qty": Decimal(msg["A"]),
                            "ts": time.time(),
                        }):
                            _bbo_dropped += 1

        except asyncio.CancelledError:
            for t in sync_tasks.values():
                t.cancel()
            break
        except Exception as e:
            info["connected"] = False
            # Unsync every book the moment the socket dies, not at the top of the
            # next connect attempt: reconnect backoff runs up to 30s, and a book
            # reported "live" during that window is a book whose depth is frozen.
            # This also re-arms the state machine before we re-subscribe.
            for sym in symbols:
                books[sym].reset()
            for t in sync_tasks.values():
                t.cancel()
            sync_tasks.clear()
            jittered = reconnect_delay + random.uniform(0, 1.0)
            logger.error("Shard %d: disconnected (%s). Reconnecting in %.1fs",
                         shard_id, e, jittered)
            await asyncio.sleep(jittered)
            reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX)


# === TELEMETRY ===

# The metric families /metrics exposes. The Rust port renders the same set
# (feed_handler::METRIC_FAMILIES is the mirror); a serialized metrics contract is
# cross-language, so both ends move together or not at all.
METRIC_FAMILIES = [
    "bfh_up",
    "bfh_uptime_seconds",
    "bfh_shards",
    "bfh_books",
    "bfh_published_total",
    "bfh_snapshot_requests_total",
    "bfh_book_snapshots_total",
    "bfh_sequence_gaps_total",
    "bfh_crossed_books_total",
    "bfh_unverified_bridges_total",
    "bfh_dropped_total",
    "bfh_subscribers",
    "bfh_shard_messages_total",
    "bfh_shard_latency_ms",
]


@dataclass
class Telemetry:
    """One consistent read of everything the observability surfaces report, taken
    in a single pass over the books. /health, /stats, /metrics and the periodic
    stats line all derive from this, so they can never disagree -- and a new
    counter is added in one place instead of four."""
    uptime_s: int
    shards: list[dict]
    connected: int
    book_states: dict[str, int]
    live: int
    total: int
    gaps: int
    crossed: int
    snapshots_per_book: int
    unverified_bridges: int
    buffer_dropped: int
    max_latency_ms: float | None
    bbo_published: int
    book_published: int
    snapshots: int
    bbo_dropped: int
    book_dropped: int
    subscriber_dropped: int
    subscribers: int

    @property
    def healthy(self) -> bool:
        """Every shard connected and a majority of books live. Same predicate the
        /health status code is derived from, so a liveness probe and a human
        reading /health can never draw opposite conclusions."""
        return self.connected == len(self.shards) and self.live > self.total // 2

    @property
    def total_dropped(self) -> int:
        return (self.bbo_dropped + self.book_dropped
                + self.subscriber_dropped + self.buffer_dropped)

    def book_state(self, state: str) -> int:
        return self.book_states.get(state, 0)


def gather_telemetry(books: dict[str, Book], shard_infos: list[dict],
                     start_time: float) -> Telemetry:
    states: dict[str, int] = {}
    gaps = crossed = snaps_per_book = unverified = buf_dropped = 0
    for b in books.values():
        states[b.state] = states.get(b.state, 0) + 1
        gaps += b.gap_count
        crossed += b.crossed_count
        snaps_per_book += b.snapshot_count
        unverified += b.unverified_bridge_count
        buf_dropped += b.buffer_dropped
    latencies = [s["latency_ms"] for s in shard_infos if s["latency_ms"] is not None]
    return Telemetry(
        uptime_s=int(time.time() - start_time),
        shards=shard_infos,
        connected=sum(1 for s in shard_infos if s["connected"]),
        book_states=states,
        live=states.get("live", 0),
        total=len(books),
        gaps=gaps,
        crossed=crossed,
        snapshots_per_book=snaps_per_book,
        unverified_bridges=unverified,
        buffer_dropped=buf_dropped,
        max_latency_ms=max(latencies) if latencies else None,
        bbo_published=_bbo_count,
        book_published=_book_count,
        snapshots=_snap_count,
        bbo_dropped=_bbo_dropped,
        book_dropped=_book_dropped,
        subscriber_dropped=publisher.dropped_count(),
        subscribers=publisher.subscriber_count(),
    )


def render_prometheus(t: Telemetry) -> str:
    """Prometheus text exposition. bfh_sequence_gaps_total and bfh_dropped_total
    are the two worth alerting on."""
    out: list[str] = []

    def family(name: str, help_text: str, kind: str, body: str):
        out.append(f"# HELP {name} {help_text}\n# TYPE {name} {kind}\n{body}")

    family("bfh_up", "1 when every shard is connected and a majority of books are live",
           "gauge", f"bfh_up {int(t.healthy)}\n")
    family("bfh_uptime_seconds", "Process uptime", "gauge",
           f"bfh_uptime_seconds {t.uptime_s}\n")
    family("bfh_shards", "WebSocket shards by connection state", "gauge",
           f'bfh_shards{{state="connected"}} {t.connected}\n'
           f'bfh_shards{{state="disconnected"}} {len(t.shards) - t.connected}\n')
    family("bfh_books", "Order books by sync state", "gauge",
           f'bfh_books{{state="live"}} {t.book_state("live")}\n'
           f'bfh_books{{state="buffering"}} {t.book_state("buffering")}\n'
           f'bfh_books{{state="uninitialized"}} {t.book_state("uninitialized")}\n')
    family("bfh_published_total", "Events published downstream", "counter",
           f'bfh_published_total{{stream="bbo"}} {t.bbo_published}\n'
           f'bfh_published_total{{stream="book"}} {t.book_published}\n')
    family("bfh_snapshot_requests_total", "REST depth snapshots requested", "counter",
           f"bfh_snapshot_requests_total {t.snapshots}\n")
    family("bfh_book_snapshots_total", "Snapshots successfully applied to a book",
           "counter", f"bfh_book_snapshots_total {t.snapshots_per_book}\n")
    family("bfh_sequence_gaps_total", "Depth sequence gaps detected (pu chain broken)",
           "counter", f"bfh_sequence_gaps_total {t.gaps}\n")
    family("bfh_crossed_books_total", "Integrity failures where bid >= ask", "counter",
           f"bfh_crossed_books_total {t.crossed}\n")
    family("bfh_unverified_bridges_total",
           "Snapshots that went live without a buffered event proving "
           "U <= lastUpdateId <= u", "counter",
           f"bfh_unverified_bridges_total {t.unverified_bridges}\n")
    family("bfh_dropped_total", "Messages shed, by shedding point", "counter",
           f'bfh_dropped_total{{queue="bbo"}} {t.bbo_dropped}\n'
           f'bfh_dropped_total{{queue="book"}} {t.book_dropped}\n'
           f'bfh_dropped_total{{queue="subscriber"}} {t.subscriber_dropped}\n'
           f'bfh_dropped_total{{queue="resync_buffer"}} {t.buffer_dropped}\n')
    family("bfh_subscribers", "Connected downstream subscribers", "gauge",
           f"bfh_subscribers {t.subscribers}\n")
    family("bfh_shard_messages_total", "WebSocket messages received per shard",
           "counter",
           "".join(f'bfh_shard_messages_total{{shard="{s["id"]}"}} {s["msg_count"]}\n'
                   for s in t.shards))
    # A shard with no sample yet emits no series: reporting 0 would be a lie a
    # dashboard cannot distinguish from genuinely zero lag.
    family("bfh_shard_latency_ms", "Event-time to receive-time lag per shard", "gauge",
           "".join(f'bfh_shard_latency_ms{{shard="{s["id"]}"}} {s["latency_ms"]:.1f}\n'
                   for s in t.shards if s["latency_ms"] is not None))
    return "".join(out)


# === HEALTH ===

async def start_health_server(books: dict[str, Book], shard_infos: list[dict],
                              port: int, start_time: float):
    """HTTP health + stats + metrics endpoints using aiohttp."""
    from aiohttp import web

    async def health_handler(_req):
        t = gather_telemetry(books, shard_infos, start_time)
        return web.json_response({
            "healthy": t.healthy,
            "uptime_s": t.uptime_s,
            "shards": f"{t.connected}/{len(t.shards)} connected",
            "books": f"{t.live}/{t.total} live",
        }, status=200 if t.healthy else 503)

    async def stats_handler(_req):
        t = gather_telemetry(books, shard_infos, start_time)
        return web.json_response({
            "uptime_s": t.uptime_s,
            "book_states": t.book_states,
            "shards": t.shards,
            "bbo_published": t.bbo_published,
            "book_published": t.book_published,
            "snapshots": t.snapshots,
            "total_gaps": t.gaps,
            "total_crossed": t.crossed,
            "total_snapshots_per_book": t.snapshots_per_book,
            "total_unverified_bridges": t.unverified_bridges,
            "max_latency_ms": t.max_latency_ms,
            "dropped": {
                "bbo_queue": t.bbo_dropped,
                "book_queue": t.book_dropped,
                "subscriber_queue": t.subscriber_dropped,
                "book_buffer": t.buffer_dropped,
            },
            "subscribers": t.subscribers,
        })

    async def metrics_handler(_req):
        body = render_prometheus(gather_telemetry(books, shard_infos, start_time))
        # content_type= and an explicit content-type header are mutually
        # exclusive in aiohttp; the version parameter has to ride the header.
        return web.Response(text=body,
                            headers={"content-type": "text/plain; version=0.0.4"})

    app = web.Application()
    app.router.add_get("/health", health_handler)
    app.router.add_get("/stats", stats_handler)
    app.router.add_get("/metrics", metrics_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


# === MONITORING ===

async def log_stats(books: dict[str, Book], shard_infos: list[dict],
                    start_time: float):
    while True:
        await asyncio.sleep(STATS_INTERVAL)
        t = gather_telemetry(books, shard_infos, start_time)
        logger.info(
            "shards=%d/%d | books: %d live, %d buffering | msgs=%d | "
            "pub: %d bbo, %d book | snaps=%d | latency=%s | "
            "gaps=%d crossed=%d dropped=%d",
            t.connected, len(t.shards), t.book_state("live"), t.book_state("buffering"),
            sum(s["msg_count"] for s in t.shards),
            t.bbo_published, t.book_published, t.snapshots,
            "n/a" if t.max_latency_ms is None else f"{t.max_latency_ms:.0f}ms",
            t.gaps, t.crossed, t.total_dropped,
        )
        if t.max_latency_ms is not None and t.max_latency_ms < -CLOCK_SKEW_WARN_MS:
            logger.warning("event-time lag is %.0fms: the local clock is behind the "
                           "venue, so every latency figure this process reports is "
                           "offset by that much", t.max_latency_ms)


def take_stale_books(books: dict[str, Book], now: float,
                     threshold: float = STALE_THRESHOLD) -> list[str]:
    """Mark every live-but-silent book for resync and return the symbols to
    re-snapshot. Pure over `books` so the sweep decision is unit-testable.

    last_update_time is pushed forward to `now` on trigger: a symbol whose
    stream is genuinely dead would otherwise re-qualify on every tick and
    snapshot-storm the REST budget.
    """
    stale = []
    for book in books.values():
        if book.state == "live" and (now - book.last_update_time) > threshold:
            logger.warning("%s: stale for %.0fs, re-snapshotting", book.symbol,
                           now - book.last_update_time)
            book.mark_for_resync()
            book.last_update_time = now
            stale.append(book.symbol)
    return stale


async def check_stale(books: dict[str, Book], session: aiohttp.ClientSession):
    resyncs: set[asyncio.Task] = set()
    while True:
        await asyncio.sleep(STALE_THRESHOLD / 2)
        # Reset alone only re-arms the state machine; the snapshot still has to
        # be asked for, or recovery waits on a depth event that may be minutes
        # out on an illiquid symbol.
        for sym in take_stale_books(books, time.time()):
            task = asyncio.create_task(sync_book(session, books[sym]))
            resyncs.add(task)
            task.add_done_callback(resyncs.discard)


async def demo_consumer():
    """Sample consumer — subscribes via publisher, logs every 5000th BBO update."""
    q = publisher.subscribe(stream="bbo")
    count = 0
    while True:
        event = await q.get()
        count += 1
        if count % 5000 == 0:
            spread = event["ask_price"] - event["bid_price"]
            logger.info("CONSUMER | %s  bid=%s  ask=%s  spread=%s  (msg #%d)",
                        event["symbol"], event["bid_price"], event["ask_price"],
                        spread, count)


# === MAIN ===

async def main(max_symbols: int | None = None, port: int = 8080, ws_port: int = 8081,
               symbols_arg: list[str] | None = None,
               snapshot_rate: float = SNAPSHOT_RATE_DEFAULT):
    global _pacer, _snapshot_rank
    _pacer = SnapshotPacer(snapshot_rate)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logger.info("Starting Binance USD-M Futures Feed Handler")

    async with aiohttp.ClientSession() as session:
        # Discover symbols
        symbols, _snapshot_rank = await resolve_universe(session, max_symbols, symbols_arg)
        logger.info("Tracking %d perpetual symbols; snapshotting in liquidity "
                    "order at %s/s", len(symbols), snapshot_rate)

        # Create per-symbol books
        books = {sym: Book(sym) for sym in symbols}

        # Create output queues (downstream consumers read from these)
        bbo_q: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX)
        book_q: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX)

        # Shard symbols across WS connections
        shard_infos = []
        shard_tasks = []
        for i in range(0, len(symbols), MAX_SYMBOLS_PER_SHARD):
            chunk = symbols[i : i + MAX_SYMBOLS_PER_SHARD]
            # latency_ms is None until the first sample, NOT 0.0: a real sample
            # can legitimately be negative (clock skew against the venue) and a
            # `> 0` filter would silently report that as zero lag.
            info = {"id": len(shard_infos), "connected": False, "msg_count": 0,
                    "last_msg_time": 0.0, "latency_ms": None}
            shard_infos.append(info)
            shard_tasks.append(asyncio.create_task(
                run_shard(info["id"], chunk, books, session, bbo_q, book_q, info)
            ))
        logger.info("Created %d WS shards", len(shard_infos))

        # Start health server + publisher
        start_time = time.time()
        await start_health_server(books, shard_infos, port, start_time)
        logger.info("Health server on :%d", port)
        await publisher.start_ws_server(ws_port)

        bg_tasks = [
            asyncio.create_task(publisher.run_dispatcher(bbo_q, book_q)),
            asyncio.create_task(log_stats(books, shard_infos, start_time)),
            asyncio.create_task(check_stale(books, session)),
            asyncio.create_task(demo_consumer()),
        ]

        # Wait for shutdown signal
        stop = asyncio.Event()

        def on_signal():
            logger.info("Shutdown signal received")
            stop.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_event_loop().add_signal_handler(sig, on_signal)

        await stop.wait()

        _pacer.stop()
        for t in shard_tasks + bg_tasks:
            t.cancel()
        await asyncio.gather(*shard_tasks, *bg_tasks, return_exceptions=True)
        logger.info("Stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Binance USD-M Futures Feed Handler")
    parser.add_argument("--max-symbols", type=int, default=None,
                        help="Cap symbol count, taking the most liquid by 24h "
                             "quote volume (default: all perps)")
    parser.add_argument("--symbols", type=lambda v: v.split(","), default=None,
                        help="Track exactly these symbols, e.g. BTCUSDT,ETHUSDT. "
                             "Overrides --max-symbols")
    parser.add_argument("--port", type=int, default=8080, help="Health server port")
    parser.add_argument("--ws-port", type=int, default=8081, help="WebSocket server port")
    parser.add_argument("--snapshot-rate", type=float, default=SNAPSHOT_RATE_DEFAULT,
                        help="REST depth snapshots per second (4.0 is 100%% of the "
                             "USD-M IP weight budget at limit=500)")
    args = parser.parse_args()
    asyncio.run(main(args.max_symbols, args.port, args.ws_port, args.symbols,
                     args.snapshot_rate))

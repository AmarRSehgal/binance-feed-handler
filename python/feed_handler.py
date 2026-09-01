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
SNAPSHOT_RATE = 3.0
RECONNECT_BASE = 1.0
RECONNECT_MAX = 30.0
STATS_INTERVAL = 10
STALE_THRESHOLD = 30.0
QUEUE_MAX = 100_000

# Module-level snapshot rate limiter (initialized in main)
_snap_lock: asyncio.Lock | None = None
_snap_last_t = 0.0
_snap_count = 0

# Publish counters
_bbo_count = 0
_book_count = 0


# === HELPERS ===

def _put(q: asyncio.Queue, item):
    """Put item on queue, dropping oldest on overflow."""
    try:
        q.put_nowait(item)
    except asyncio.QueueFull:
        try:
            q.get_nowait()
        except asyncio.QueueEmpty:
            pass
        try:
            q.put_nowait(item)
        except asyncio.QueueFull:
            pass


# === REST ===

def pick_top_by_volume(symbols: list[str], volumes: dict[str, float], n: int) -> list[str]:
    """Keep the `n` highest-24h-quote-volume symbols, returned alphabetically so
    shard assignment stays deterministic across restarts.

    A plain symbols[:n] after an alphabetical sort is what this replaces: it
    selected 1000BONK/1000FLOKI/AAVE/ACE... and never once included BTCUSDT, so
    every capped test run exercised the thinnest books on the venue.
    Symbols missing from the ticker response rank last; ties break on name.
    """
    ranked = sorted(symbols, key=lambda s: (-volumes.get(s, 0.0), s))
    return sorted(ranked[:n])


async def fetch_quote_volumes(session: aiohttp.ClientSession) -> dict[str, float]:
    url = f"{BINANCE_FAPI}/fapi/v1/ticker/24hr"
    async with session.get(url) as resp:
        resp.raise_for_status()
        data = await resp.json()
    return {t["symbol"]: float(t["quoteVolume"]) for t in data}


async def fetch_symbols(session: aiohttp.ClientSession, max_symbols: int | None = None,
                        explicit: list[str] | None = None) -> list[str]:
    """Get the USD-M perpetual symbols to track."""
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
        return sorted(set(explicit))

    if max_symbols and max_symbols < len(symbols):
        return pick_top_by_volume(symbols, await fetch_quote_volumes(session), max_symbols)
    return symbols


async def fetch_snapshot(session: aiohttp.ClientSession, symbol: str) -> dict:
    """Fetch REST depth snapshot, rate-limited to SNAPSHOT_RATE req/sec."""
    global _snap_last_t, _snap_count

    # Pace requests to stay under Binance weight limits
    async with _snap_lock:
        now = asyncio.get_event_loop().time()
        wait = _snap_last_t + (1.0 / SNAPSHOT_RATE) - now
        if wait > 0:
            await asyncio.sleep(wait)
        _snap_last_t = asyncio.get_event_loop().time()

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
    global _bbo_count, _book_count
    reconnect_delay = RECONNECT_BASE
    sync_tasks: dict[str, asyncio.Task] = {}

    def _request_sync(sym):
        if sym in sync_tasks and not sync_tasks[sym].done():
            sync_tasks[sym].cancel()
        sync_tasks[sym] = asyncio.create_task(sync_book(session, books[sym]))

    while True:
        try:
            # Reset books so they re-sync from snapshot on reconnect
            for sym in symbols:
                books[sym].reset()
            for t in sync_tasks.values():
                t.cancel()
            sync_tasks.clear()

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
                            _put(book_q, {
                                "symbol": symbol, "bids": top_bids, "asks": top_asks,
                                "last_update_id": book.last_update_id, "ts": time.time(),
                            })

                    elif evt == "bookTicker":
                        book.set_ticker_bbo(Decimal(msg["b"]), Decimal(msg["a"]))
                        _bbo_count += 1
                        _put(bbo_q, {
                            "symbol": symbol,
                            "bid_price": Decimal(msg["b"]), "bid_qty": Decimal(msg["B"]),
                            "ask_price": Decimal(msg["a"]), "ask_qty": Decimal(msg["A"]),
                            "ts": time.time(),
                        })

        except asyncio.CancelledError:
            for t in sync_tasks.values():
                t.cancel()
            break
        except Exception as e:
            info["connected"] = False
            jittered = reconnect_delay + random.uniform(0, 1.0)
            logger.error("Shard %d: disconnected (%s). Reconnecting in %.1fs",
                         shard_id, e, jittered)
            await asyncio.sleep(jittered)
            reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX)


# === HEALTH ===

async def start_health_server(books: dict[str, Book], shard_infos: list[dict],
                              port: int, start_time: float):
    """HTTP health + stats endpoints using aiohttp."""
    from aiohttp import web

    async def health_handler(_req):
        connected = sum(1 for s in shard_infos if s["connected"])
        live = sum(1 for b in books.values() if b.state == "live")
        total = len(books)
        healthy = connected == len(shard_infos) and live > total * 0.5
        return web.json_response({
            "healthy": healthy,
            "uptime_s": int(time.time() - start_time),
            "shards": f"{connected}/{len(shard_infos)} connected",
            "books": f"{live}/{total} live",
        }, status=200 if healthy else 503)

    async def stats_handler(_req):
        states = {}
        for b in books.values():
            states[b.state] = states.get(b.state, 0) + 1
        latencies = [s["latency_ms"] for s in shard_infos if s["latency_ms"] > 0]
        return web.json_response({
            "uptime_s": int(time.time() - start_time),
            "book_states": states,
            "shards": shard_infos,
            "bbo_published": _bbo_count,
            "book_published": _book_count,
            "snapshots": _snap_count,
            "total_gaps": sum(b.gap_count for b in books.values()),
            "total_crossed": sum(b.crossed_count for b in books.values()),
            "total_snapshots_per_book": sum(b.snapshot_count for b in books.values()),
            "max_latency_ms": max(latencies) if latencies else 0.0,
        })

    app = web.Application()
    app.router.add_get("/health", health_handler)
    app.router.add_get("/stats", stats_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


# === MONITORING ===

async def log_stats(books: dict[str, Book], shard_infos: list[dict]):
    while True:
        await asyncio.sleep(STATS_INTERVAL)
        live = sum(1 for b in books.values() if b.state == "live")
        buffering = sum(1 for b in books.values() if b.state == "buffering")
        connected = sum(1 for s in shard_infos if s["connected"])
        total_msgs = sum(s["msg_count"] for s in shard_infos)
        latencies = [s["latency_ms"] for s in shard_infos if s["latency_ms"] > 0]
        max_lat = max(latencies) if latencies else 0.0
        total_gaps = sum(b.gap_count for b in books.values())
        total_crossed = sum(b.crossed_count for b in books.values())
        logger.info(
            "shards=%d/%d | books: %d live, %d buffering | msgs=%d | "
            "pub: %d bbo, %d book | snaps=%d | latency=%.0fms | gaps=%d crossed=%d",
            connected, len(shard_infos), live, buffering, total_msgs,
            _bbo_count, _book_count, _snap_count, max_lat, total_gaps, total_crossed,
        )


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
               symbols_arg: list[str] | None = None):
    global _snap_lock
    _snap_lock = asyncio.Lock()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logger.info("Starting Binance USD-M Futures Feed Handler")

    async with aiohttp.ClientSession() as session:
        # Discover symbols
        symbols = await fetch_symbols(session, max_symbols, symbols_arg)
        logger.info("Tracking %d perpetual symbols", len(symbols))

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
            info = {"id": len(shard_infos), "connected": False, "msg_count": 0,
                    "last_msg_time": 0.0, "latency_ms": 0.0}
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
            asyncio.create_task(log_stats(books, shard_infos)),
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
    args = parser.parse_args()
    asyncio.run(main(args.max_symbols, args.port, args.ws_port, args.symbols))

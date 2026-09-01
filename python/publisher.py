"""Pub/sub dispatcher + WebSocket server for feed handler consumers.

Fans out BBO and book updates from feed_handler's output queues to
multiple subscribers, with optional per-symbol and per-stream filtering.

Internal consumers: call subscribe() for a filtered asyncio.Queue.
External consumers: connect via WebSocket, send a subscribe message.

WebSocket protocol:
    -> {"action": "subscribe", "stream": "bbo", "symbols": ["BTCUSDT", "ETHUSDT"]}
    <- {"status": "subscribed", "stream": "bbo", "symbols": ["BTCUSDT", "ETHUSDT"]}
    <- {"symbol": "BTCUSDT", "bid_price": "64000.50", "ask_price": "64001.00", ...}
    -> {"action": "unsubscribe"}
    <- {"status": "unsubscribed"}
"""
import asyncio
import json
import logging
from decimal import Decimal

from aiohttp import web

logger = logging.getLogger(__name__)

# === CONFIGURATION ===

SUBSCRIBER_QUEUE_MAX = 10_000

# === STATE ===

_subscribers: list[dict] = []

# Messages shed because a subscriber's queue was full (slow consumer).
# Never silent: surfaced in /stats and /metrics.
_dropped = 0


def dropped_count() -> int:
    return _dropped


def subscriber_count() -> int:
    return len(_subscribers)


# === SERIALIZATION ===

def _default(o):
    if isinstance(o, Decimal):
        return str(o)
    raise TypeError(f"not serializable: {type(o)}")


# === PUB/SUB ===

def subscribe(stream: str = "all", symbols: set[str] | None = None) -> asyncio.Queue:
    """Register a consumer. Returns a queue that receives matching updates.

    stream: "bbo", "book", or "all"
    symbols: set of symbol names to filter, or None for all symbols
    """
    q: asyncio.Queue = asyncio.Queue(maxsize=SUBSCRIBER_QUEUE_MAX)
    _subscribers.append({
        "queue": q,
        "symbols": set(symbols) if symbols else None,
        "stream": stream,
    })
    logger.info("Subscriber added: stream=%s, symbols=%s (%d total)",
                stream, "all" if symbols is None else len(symbols), len(_subscribers))
    return q


def unsubscribe(q: asyncio.Queue):
    """Remove a subscriber by its queue reference."""
    _subscribers[:] = [s for s in _subscribers if s["queue"] is not q]
    logger.info("Subscriber removed (%d remaining)", len(_subscribers))


# === DISPATCHER ===

def _put(q: asyncio.Queue, msg: dict):
    """Enqueue, shedding the OLDEST message on overflow. Counted, never silent.

    Cross-language divergence, deliberate and documented: Rust's tokio mpsc has
    no sender-side pop, so the Rust publisher sheds the NEWEST message instead.
    Both count into the same metric name.
    """
    global _dropped
    try:
        q.put_nowait(msg)
        return
    except asyncio.QueueFull:
        pass
    _dropped += 1
    try:
        q.get_nowait()
    except asyncio.QueueEmpty:
        pass
    try:
        q.put_nowait(msg)
    except asyncio.QueueFull:
        pass


def _publish(msg: dict, stream: str):
    symbol = msg.get("symbol")
    for sub in _subscribers:
        if sub["stream"] not in ("all", stream):
            continue
        if sub["symbols"] is not None and symbol not in sub["symbols"]:
            continue
        _put(sub["queue"], msg)


async def run_dispatcher(bbo_q: asyncio.Queue, book_q: asyncio.Queue):
    """Drain feed handler queues and fan out to all subscribers."""
    async def drain(source_q: asyncio.Queue, stream_name: str):
        while True:
            msg = await source_q.get()
            _publish(msg, stream_name)

    await asyncio.gather(
        drain(bbo_q, "bbo"),
        drain(book_q, "book"),
    )


# === WEBSOCKET SERVER ===

async def _stream_to_ws(ws: web.WebSocketResponse, q: asyncio.Queue):
    try:
        while not ws.closed:
            msg = await q.get()
            await ws.send_str(json.dumps(msg, default=_default))
    except (ConnectionResetError, asyncio.CancelledError):
        pass


async def _ws_handler(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    logger.info("WebSocket client connected from %s", request.remote)

    q = None
    stream_task = None

    try:
        async for raw in ws:
            if raw.type != web.WSMsgType.TEXT:
                continue
            try:
                msg = json.loads(raw.data)
            except json.JSONDecodeError:
                await ws.send_str('{"error": "invalid JSON"}')
                continue

            action = msg.get("action")

            if action == "subscribe":
                if stream_task is not None:
                    stream_task.cancel()
                if q is not None:
                    unsubscribe(q)

                stream = msg.get("stream", "all")
                symbols = msg.get("symbols")
                q = subscribe(stream=stream, symbols=symbols)
                stream_task = asyncio.create_task(_stream_to_ws(ws, q))
                await ws.send_str(json.dumps({
                    "status": "subscribed",
                    "stream": stream,
                    "symbols": symbols,
                }))

            elif action == "unsubscribe":
                if stream_task is not None:
                    stream_task.cancel()
                    stream_task = None
                if q is not None:
                    unsubscribe(q)
                    q = None
                await ws.send_str('{"status": "unsubscribed"}')

    finally:
        if stream_task is not None:
            stream_task.cancel()
        if q is not None:
            unsubscribe(q)
        logger.info("WebSocket client disconnected from %s", request.remote)

    return ws


async def start_ws_server(port: int = 8081):
    app = web.Application()
    app.router.add_get("/ws", _ws_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()
    logger.info("WebSocket server on :%d/ws", port)

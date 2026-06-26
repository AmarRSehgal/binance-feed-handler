# Binance USD-M Futures Feed Handler

A production-minded feed handler that subscribes to all Binance USD-M perpetual futures, maintains per-symbol L2 order books, and publishes BBO + book updates to downstream consumers.

Implemented in both Python (prototype) and Rust (production target). The two implementations share identical state machine logic and pass the same test suite.

---

## Getting Started

### Prerequisites

| Requirement | Version | Check |
|---|---|---|
| Python | 3.13+ | `python3 --version` |
| pip | any | `pip --version` |
| Rust | 1.75+ (2021 edition) | `rustc --version` |
| cargo | (ships with Rust) | `cargo --version` |

No API keys required -- Binance public WebSocket streams are unauthenticated.

### Install Dependencies

```bash
# Python
pip install -r python/requirements.txt

# Rust (first build downloads and compiles dependencies, takes ~1-2 minutes)
cd rust && cargo build --release && cd ..
```

---

## Running the Feed Handler

### Python

```bash
# All ~570 perpetual futures
python python/feed_handler.py

# 10 symbols only (good for a quick test)
python python/feed_handler.py --max-symbols 10

# Custom ports
python python/feed_handler.py --max-symbols 10 --port 9090 --ws-port 9091
```

### Rust

```bash
# All ~570 perpetual futures
./rust/target/release/binance-feed-handler

# 10 symbols only
./rust/target/release/binance-feed-handler --max-symbols 10

# Custom ports
./rust/target/release/binance-feed-handler --max-symbols 10 --port 9090 --ws-port 9091
```

### CLI Options (identical for both)

| Flag | Default | Description |
|---|---|---|
| `--max-symbols N` | all (~570) | Cap the number of symbols to subscribe to |
| `--port N` | 8080 | HTTP health/stats server port |
| `--ws-port N` | 8081 | WebSocket consumer server port |

### What You Should See

On startup (with `--max-symbols 10`):

```
14:32:01.204 [INFO] Fetched 10 symbols, 1 shard(s)
14:32:01.518 [INFO] [shard-0] connected, subscribing to 20 streams
14:32:02.100 [INFO] [shard-0] subscribed to 20 streams
14:32:02.300 [INFO] BTCUSDT: snapshot applied (lastUpdateId=...)
...
14:32:12.000 [INFO] shards=1/1 | books: 10 live | msgs=4830 | pub: 2100 bbo, 950 book | snaps=10 | latency=45ms | gaps=0 crossed=0
```

The periodic stats line (every 10s) shows live book counts, message throughput, snapshot counts, processing latency, and integrity counters.

---

## Subscribing to Data

There are three ways to consume data from the feed handler.

### Option 1: WebSocket Client (any language, recommended for reviewers)

Connect to the WebSocket server and send a JSON subscribe message. Works with any WebSocket client -- `websocat`, `wscat`, Python, JavaScript, etc.

**Using websocat (install: `brew install websocat` or `cargo install websocat`):**

```bash
# Start the feed handler in one terminal
python python/feed_handler.py --max-symbols 10

# In another terminal, subscribe to all BBO updates
echo '{"action":"subscribe","stream":"bbo"}' | websocat ws://localhost:8081/ws

# Subscribe to book updates for specific symbols only
echo '{"action":"subscribe","stream":"book","symbols":["BTCUSDT","ETHUSDT"]}' | websocat ws://localhost:8081/ws

# Subscribe to everything (BBO + book, all symbols)
echo '{"action":"subscribe","stream":"all"}' | websocat ws://localhost:8081/ws
```

**Using Python (no extra deps beyond websockets, already in requirements.txt):**

```python
import asyncio
import json
import websockets

async def main():
    async with websockets.connect("ws://localhost:8081/ws") as ws:
        # Subscribe to BBO for two symbols
        await ws.send(json.dumps({
            "action": "subscribe",
            "stream": "bbo",
            "symbols": ["BTCUSDT", "ETHUSDT"]
        }))

        # First message back is the subscription confirmation
        print(await ws.recv())

        # All subsequent messages are live market data
        async for msg in ws:
            data = json.loads(msg)
            print(f"{data['symbol']}  bid={data['bid_price']}  ask={data['ask_price']}")

asyncio.run(main())
```

**Using JavaScript / Node.js:**

```javascript
const ws = new WebSocket("ws://localhost:8081/ws");

ws.onopen = () => {
    ws.send(JSON.stringify({
        action: "subscribe",
        stream: "bbo",
        symbols: ["BTCUSDT"]
    }));
};

ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    console.log(`${data.symbol}  bid=${data.bid_price}  ask=${data.ask_price}`);
};
```

### Option 2: HTTP Health & Stats Endpoints

No subscription needed -- just curl while the feed handler is running.

```bash
# Health check (200 = healthy, 503 = degraded)
curl http://localhost:8080/health

# Full stats as JSON
curl -s http://localhost:8080/stats | python3 -m json.tool
```

Stats response includes:
- Per-symbol book states (live/buffering/uninitialized)
- Per-shard connection status and message counts
- Publish counters (BBO and book events)
- Processing latency (ms)
- Integrity counters (gaps, crossed books, BBO mismatches)

### Option 3: In-Process Python Consumer

For consumers in the same process, subscribe directly to the publisher module. This is what the built-in `demo_consumer` uses.

```python
import asyncio
import publisher

async def my_consumer():
    # Get a filtered queue -- only BBO for BTCUSDT and ETHUSDT
    q = publisher.subscribe(stream="bbo", symbols={"BTCUSDT", "ETHUSDT"})

    while True:
        event = await q.get()
        spread = event["ask_price"] - event["bid_price"]
        print(f"{event['symbol']}  bid={event['bid_price']}  ask={event['ask_price']}  spread={spread}")

    # When done, clean up
    publisher.unsubscribe(q)
```

### WebSocket Protocol Reference

**Subscribe:**
```json
--> {"action": "subscribe", "stream": "bbo", "symbols": ["BTCUSDT", "ETHUSDT"]}
<-- {"status": "subscribed", "stream": "bbo", "symbols": ["BTCUSDT", "ETHUSDT"]}
<-- {"symbol": "BTCUSDT", "bid_price": "64000.50", "bid_qty": "1.234", "ask_price": "64001.00", "ask_qty": "0.567", "ts": 1719396000.123}
<-- ...
```

**Unsubscribe:**
```json
--> {"action": "unsubscribe"}
<-- {"status": "unsubscribed"}
```

**Stream options:**
| Value | What you get |
|---|---|
| `"bbo"` | Best bid/ask updates (from bookTicker stream) |
| `"book"` | L2 top-of-book snapshots (top 5 bids + asks after each depth diff) |
| `"all"` | Both BBO and book updates |

**Symbol filtering:**
- Omit `"symbols"` to get all symbols.
- Include `"symbols": ["BTCUSDT", "ETHUSDT"]` to filter to specific symbols only.

**Message schemas:**

BBO event:
```json
{"symbol": "BTCUSDT", "bid_price": "64000.50", "bid_qty": "1.234", "ask_price": "64001.00", "ask_qty": "0.567", "ts": 1719396000.123}
```

Book event:
```json
{"symbol": "BTCUSDT", "bids": [["64000.50", "1.234"], ["63999.00", "2.0"], ...], "asks": [["64001.00", "0.567"], ["64002.00", "3.0"], ...], "last_update_id": 12345678, "ts": 1719396000.123}
```

**Queue overflow:** If a consumer falls behind, oldest messages are dropped (not newest). Stale market data has negative value.

---

## Running Tests

```bash
# Python unit tests (34 tests: 23 book + 11 publisher)
python -m pytest tests/python/ -v

# Rust unit tests (13 tests)
cd rust && cargo test && cd ..

# Cross-language comparison -- verifies Python and Rust produce identical results
# Python-only (prints expected outputs):
python tests/cross_language_comparison.py

# Automated comparison against Rust binary:
python tests/cross_language_comparison.py --rust-binary rust/target/debug/binance-feed-handler
```

For the cross-language comparison, build the debug binary first: `cd rust && cargo build && cd ..`

The comparison runs 5 deterministic scenarios through both implementations and diffs every field:

| Scenario | What it tests |
|---|---|
| `basic_lifecycle` | Full state machine: uninitialized -> buffering -> snapshot sync -> live -> diff application -> sequence gap |
| `crossed_book` | Integrity check fires when best bid >= best ask, triggers re-snapshot |
| `bbo_divergence` | Mismatch counter increments on divergence, resets on match, triggers re-snapshot at threshold |
| `reset_and_resync` | Reset clears all state, allows fresh sync cycle |
| `top_levels_ordering` | BTreeMap (Rust) vs sorted dict (Python) produce identical bid/ask ordering |

---

## Architecture

### Data Flow

```
Binance WS --> run_shard() --> Book.on_depth()        --> book_q (L2 top-of-book)
                           --> Book.set_ticker_bbo()   |
                           --> bbo_q (best bid/ask)     |
                                                        v
book_q / bbo_q --> publisher.run_dispatcher() --> subscriber queues (filtered)
                                              --> WebSocket server (:8081/ws)

Book enters "buffering" --> sync_book() --> fetch_snapshot() --> Book.on_snapshot()
```

### Dual-Stream Design

Each symbol subscribes to two independent WebSocket streams:

- **`@depth@100ms`** -- incremental order book diffs with sequence IDs (`U`, `u`, `pu`). Requires snapshot synchronization to build the full L2 book.
- **`@bookTicker`** -- self-contained best bid/ask snapshots. No sequencing, no state. Used as a low-latency BBO feed and as a cross-validation signal.

### Order Book State Machine

```
uninitialized --(first depth event)--> buffering --(snapshot sync)--> live
                                           ^                           |
                                           +-- (gap / integrity fail) -+
```

The state machine is pure logic with no I/O. `on_depth()` returns an action (`"publish"`, `"need_snapshot"`, `None`) and the caller handles I/O. This makes it testable without mocking and identical across Python and Rust.

### Connection Sharding

Binance limits ~200 streams per WebSocket. With two streams per symbol, we shard at 100 symbols per connection (~6 shards for ~570 perps).

---

## Design Decisions

### Global Update IDs (Binance Futures)

Unlike Spot (per-symbol IDs), Futures uses a **global sequence** shared across all symbols. The standard bridge check (`U <= lastUpdateId <= u`) fails systematically because other symbols' events consume the IDs between consecutive events for any given symbol.

**Solution**: a `_need_first_event` flag that unconditionally accepts the first depth event after a snapshot. Safe because no depth changes for this symbol were missed -- the intervening IDs belonged to other symbols.

### Token-Bucket Rate Limiting

Startup causes ~570 simultaneous snapshot requests. A lock-based rate limiter paces at 3 req/sec, staying well within Binance's ~2400 weight/minute limit.

### Reconnect with Exponential Backoff + Jitter

1s base, 30s cap, plus 0-1s random jitter to prevent thundering herd.

### Non-Blocking Queue Publishing

Drop-oldest policy on overflow. For market data, the latest state is always more valuable than queued stale data.

### Decimal Arithmetic

Python uses `Decimal`; Rust uses `rust_decimal`. No floating-point representation errors.

---

## Observability

**Periodic log line** (every 10s):
```
shards=6/6 | books: 568 live, 2 buffering | msgs=184920 | pub: 92100 bbo, 45300 book | snaps=570 | latency=45ms | gaps=0 crossed=0
```

**`/health`**: 200 if all shards connected and >50% of books live, 503 otherwise.

**`/stats`**: JSON with book states, per-shard info, publish counts, latency, integrity counters.

**Processing latency**: Binance's `E` (event time) vs local clock. Measures data staleness including network transit and clock skew.

**Stale detection**: books with no update in 30s are automatically reset.

## Integrity Checks

| Check | Trigger | Response |
|---|---|---|
| Crossed book | `best_bid >= best_ask` | Fall back to buffering, re-snapshot |
| BBO divergence | 10 consecutive mismatches vs bookTicker | Fall back to buffering, re-snapshot |
| Sequence gap | `pu != last_update_id` | Fall back to buffering, re-snapshot |

---

## Configuration

All constants are defined at the top of each source file.

| Constant | Default | Purpose |
|---|---|---|
| `MAX_SYMBOLS_PER_SHARD` | 100 | Symbols per WebSocket connection |
| `SUBSCRIBE_BATCH_SIZE` | 50 | Streams per SUBSCRIBE message |
| `SNAPSHOT_RATE` | 3.0 | REST snapshot requests per second |
| `RECONNECT_BASE` / `_MAX` | 1.0 / 30.0 | Reconnect delay bounds (seconds) |
| `STATS_INTERVAL` | 10 | Seconds between log stats |
| `STALE_THRESHOLD` | 30.0 | Seconds before a book is considered stale |
| `QUEUE_MAX` | 100,000 | Output queue/channel capacity |
| `BUFFER_MAX` | 5,000 | Per-book event buffer during sync |
| `BBO_MISMATCH_THRESHOLD` | 10 | Consecutive mismatches before re-snapshot |

---

## Python vs Rust

| Aspect | Python | Rust |
|---|---|---|
| `Book` state machine | `class Book` | `struct Book` |
| `Decimal` type | `decimal.Decimal` | `rust_decimal::Decimal` |
| Action returns | `str \| None` | `enum Action` |
| Book state | string (`"live"`) | `enum BookState` |
| Sorted levels | `dict` + `sorted()` O(n log n) | `BTreeMap` O(log n) |
| Async runtime | `asyncio` | `tokio` |
| WebSocket | `websockets` | `tokio-tungstenite` |
| HTTP client | `aiohttp` | `reqwest` |
| HTTP server | `aiohttp.web` | `axum` |
| Channels | `asyncio.Queue` | `tokio::sync::mpsc` |
| Counters | `int` (GIL-safe) | `AtomicU64` |

The Rust version gains: zero-cost abstractions on the hot path, no GC pauses, true concurrent async (no GIL), and compile-time safety for shared state.

---

## Known Limitations

### Not Implemented (Would Do With More Time)

- **Warm standby connections**: shadow WS connections pre-subscribed and buffering, instant failover on primary drop.
- **Prometheus / OTLP metrics**: current stats endpoint is JSON-over-HTTP. Production would use histogram latencies and alert-ready counters.
- **Snapshot priority**: high-volume symbols could be prioritized in the snapshot queue.
- **Message compression**: `permessage-deflate` would reduce bandwidth at CPU cost.
- **Persistent state**: warm restarts via serialized book state (snapshot re-sync is ~3 minutes for 570 symbols, so marginal benefit).
- **Multi-process**: horizontal scaling would shard symbols across processes/hosts.

### Intentional Tradeoffs

- **100 symbols per shard** vs 200 limit: headroom for subscribe/unsubscribe churn.
- **500-level snapshots** (10 weight) vs 1000-level (50 weight): covers the actionable range.
- **Drop-oldest queues**: stale market data has negative value.
- **No authentication**: public data streams only.

---

## Repository Structure

```
python/                  Python implementation (prototype)
  book.py                  Per-symbol order book state machine (pure logic, no I/O)
  feed_handler.py          WebSocket connections, REST snapshots, health server, monitoring
  publisher.py             Pub/sub dispatcher + WebSocket server for consumers
  requirements.txt         Python dependencies

rust/                    Rust implementation (production target)
  src/
    main.rs                CLI entry point + cross-language test scenarios
    book.rs                Order book state machine (port of book.py)
    feed_handler.rs        Async runtime (port of feed_handler.py)
    publisher.rs           Pub/sub + WebSocket server (port of publisher.py)
  Cargo.toml               Rust dependencies

tests/
  python/
    test_book.py           23 tests for the Python book state machine
    test_publisher.py      11 tests for pub/sub filtering and dispatch
  rust/
    test_book.rs           Standalone Rust test file (also embedded in book.rs)
  cross_language_comparison.py   5 deterministic scenarios, compares Python vs Rust output
```

## AI Disclosure

This project was developed with assistance from Claude (Anthropic). Claude was used for:

- Architecture discussion and Binance Futures WebSocket protocol research
- Code generation for both Python and Rust implementations
- Iterative debugging of the snapshot synchronization bridge check (the global update ID problem required understanding *why* the naive approach fails before coding a fix)
- Integrity checks (crossed-book detection, BBO cross-validation)
- Test suite and cross-language comparison harness
- Writing this README

All code was reviewed, understood, and validated by the author.

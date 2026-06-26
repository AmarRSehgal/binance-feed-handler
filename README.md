# Binance USD-M Futures Feed Handler

A production-minded feed handler that subscribes to all Binance USD-M perpetual futures, maintains per-symbol L2 order books, and publishes BBO + book updates to downstream consumers.

Implemented in both Python (prototype) and Rust (production target). The two implementations share identical state machine logic and pass the same test suite.

## Repository Structure

```
python/                  Python implementation (prototype)
  book.py                  Per-symbol order book state machine (pure logic, no I/O)
  feed_handler.py          WebSocket connections, REST snapshots, health server, monitoring
  publisher.py             Pub/sub dispatcher + WebSocket server for consumers
  requirements.txt         Python dependencies

rust/                    Rust implementation (production target)
  src/
    main.rs                CLI entry point
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
  cross_language_comparison.py   Generates deterministic scenarios, compares outputs
```

## Quick Start

### Python

```bash
cd python
pip install -r requirements.txt

python feed_handler.py                    # all ~570 perps
python feed_handler.py --max-symbols 10   # 10 symbols for testing
python feed_handler.py --port 9090        # custom health server port
```

### Rust

```bash
cd rust
cargo build --release
./target/release/binance-feed-handler
./target/release/binance-feed-handler --max-symbols 10
```

### Endpoints

- Health: `curl http://localhost:8080/health`
- Stats: `curl http://localhost:8080/stats`
- WebSocket: `ws://localhost:8081/ws` (subscribe with JSON messages)

## Running Tests

```bash
# Python unit tests
python -m pytest tests/python/ -v

# Rust unit tests
cd rust && cargo test

# Cross-language comparison (generates expected_outputs.json)
python tests/cross_language_comparison.py

# With Rust binary for automated comparison
python tests/cross_language_comparison.py --rust-binary rust/target/debug/binance-feed-handler
```

## Architecture

### Data Flow

```
Binance WS --> run_shard() --> Book.on_depth()        --> book_q (L2 top-of-book)
                           --> Book.set_ticker_bbo()   |
                           --> bbo_q (best bid/ask)    |
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

### Consumer API

**Internal** (same process):
```python
q = publisher.subscribe(stream="bbo", symbols={"BTCUSDT", "ETHUSDT"})
while True:
    msg = await q.get()
```

**External** (WebSocket):
```json
--> {"action": "subscribe", "stream": "bbo", "symbols": ["BTCUSDT"]}
<-- {"status": "subscribed", "stream": "bbo", "symbols": ["BTCUSDT"]}
<-- {"symbol": "BTCUSDT", "bid_price": "64000.50", ...}
```

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

## Configuration

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

## AI Disclosure

This project was developed with assistance from Claude (Anthropic). Claude was used for:

- Architecture discussion and Binance Futures WebSocket protocol research
- Code generation for both Python and Rust implementations
- Iterative debugging of the snapshot synchronization bridge check (the global update ID problem required understanding *why* the naive approach fails before coding a fix)
- Integrity checks (crossed-book detection, BBO cross-validation)
- Test suite and cross-language comparison harness
- Writing this README

All code was reviewed, understood, and validated by the author.

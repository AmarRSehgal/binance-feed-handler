# Binance USD-M Futures Feed Handler

Subscribes to all Binance USD-M perpetual futures (~570 symbols), maintains per-symbol L2 order books, and publishes BBO + book updates to consumers over WebSocket.

Built in Rust. Python prototype included for reference and cross-language testing.

## How to run

```bash
# build
cd rust && cargo build --release && cd ..

# run (all ~570 perps)
./rust/target/release/binance-feed-handler

# run (10 symbols, good for testing)
./rust/target/release/binance-feed-handler --max-symbols 10

# health / stats while running
curl http://localhost:8080/health
curl -s http://localhost:8080/stats | python3 -m json.tool
```

Flags: `--max-symbols N`, `--port N` (default 8080), `--ws-port N` (default 8081).

### Subscribing to data

Connect a WebSocket client to `ws://localhost:8081/ws` and send a subscribe message:

```json
{"action": "subscribe", "stream": "bbo", "symbols": ["BTCUSDT", "ETHUSDT"]}
```

Stream options: `"bbo"` (best bid/ask), `"book"` (L2 top-of-book), `"all"`. Omit `"symbols"` to get everything. Send `{"action": "unsubscribe"}` to stop.

Example clients in `tests/`:

```bash
# Python
pip install websockets
python tests/example_ws_client.py
python tests/example_ws_client.py --stream book --symbols BTCUSDT ETHUSDT

# Node.js (v22+, native WebSocket)
node tests/example_ws_client.js
node tests/example_ws_client.js book BTCUSDT ETHUSDT
```

### Running the Python version

```bash
pip install -r python/requirements.txt
python python/feed_handler.py --max-symbols 10
```

Same CLI flags, same WebSocket protocol, same ports.

### Tests

```bash
# Python (34 tests)
python -m pytest tests/python/ -v

# Rust (13 tests)
cd rust && cargo test && cd ..

# Cross-language comparison (5 deterministic scenarios, diffs every field)
cd rust && cargo build && cd ..
python tests/cross_language_comparison.py --rust-binary rust/target/debug/binance-feed-handler
```

## Design decisions

**Per-symbol state machine (uninitialized -> buffering -> live).** The book is pure logic with no I/O -- `on_depth()` returns an action and the caller handles networking. Makes it testable without mocking and portable across languages.

**Global update IDs.** Binance Futures shares a single sequence counter across all symbols (unlike Spot). The standard bridge check (`U <= lastUpdateId <= u`) fails because other symbols consume IDs between consecutive events for a given symbol. Fixed with a `need_first_event` flag that unconditionally accepts the first depth event after a snapshot -- safe because the intervening IDs belonged to other symbols, not ours.

**Dual-stream cross-validation.** Each symbol subscribes to `@depth@100ms` (sequenced diffs for the full book) and `@bookTicker` (independent BBO snapshots). The bookTicker feed cross-validates the book's best levels -- 10 consecutive mismatches trigger a re-snapshot. Catches drift that sequence checks alone would miss.

**Connection sharding at 100 symbols per WebSocket.** Binance caps ~200 streams per connection. Two streams per symbol means 200 is the hard max; 100 gives headroom for subscribe churn without hitting the limit.

**Token-bucket rate limiting (3 req/sec).** Startup fires ~570 snapshot requests simultaneously. The rate limiter paces them to stay within Binance's weight limits. Lock-based, not token-bucket in the traditional sense -- simpler, same effect for a single-consumer queue.

**Drop-oldest queue overflow.** When a consumer falls behind, oldest messages are dropped. Stale market data has negative value.

**500-level snapshots (10 weight) vs 1000-level (50 weight).** 500 levels covers the actionable range. 5x cheaper per request matters at startup when you're snapshotting everything.

**Reconnect with exponential backoff + jitter.** 1s base, 30s cap, random 0-1s jitter. Prevents thundering herd when Binance drops connections.

**Decimal arithmetic everywhere.** Python uses `decimal.Decimal`, Rust uses `rust_decimal`. No floating-point representation errors on price levels.

## Integrity checks

| Check | Trigger | Response |
|---|---|---|
| Sequence gap | `pu != last_update_id` | Fall back to buffering, re-snapshot |
| Crossed book | `best_bid >= best_ask` | Fall back to buffering, re-snapshot |
| BBO divergence | 10 consecutive mismatches vs bookTicker | Fall back to buffering, re-snapshot |
| Stale book | No update in 30s | Reset, re-sync from scratch |

## Observability

Stats line every 10s:
```
shards=6/6 | books: 568 live, 2 buffering | msgs=184920 | pub: 92100 bbo, 45300 book | snaps=570 | latency=45ms | gaps=0 crossed=0
```

`/health` returns 200 when all shards are connected and >50% of books are live, 503 otherwise. `/stats` returns the full breakdown as JSON.

## Known limitations / what I'd do with more time

- **No Prometheus/OTLP metrics.** Stats are JSON-over-HTTP. Production would use histogram latencies and alertable counters.
- **No warm standby connections.** On disconnect, there's a cold reconnect + re-subscribe + re-snapshot cycle. Shadow connections pre-buffering would give instant failover.
- **No snapshot prioritization.** All symbols snapshot in arrival order. High-volume symbols (BTC, ETH) should go first.
- **No message compression.** `permessage-deflate` would cut bandwidth at the cost of CPU.
- **No persistent state.** Warm restarts would need serialized book state. Re-sync takes ~3 minutes for 570 symbols so the benefit is marginal.
- **Single-process.** Horizontal scaling would shard symbols across processes or hosts.

## How I used AI

Claude was used throughout -- architecture discussion, protocol research, code generation for both implementations, and this README.

Where it helped most: working through the global update ID problem. The naive bridge check from the Binance docs fails silently on Futures because the docs describe the Spot behavior. Claude helped reason through *why* it fails and validate the `need_first_event` fix.

Where I caught it: the Rust port initially compiled with `String` arguments where `tokio-tungstenite` and `axum` expect `Utf8Bytes` -- five type mismatches that required `.into()` conversions. The cross-language comparison also caught a subtle difference in how the Python and Rust versions handle the `_need_first_event` flag during test scenarios, which required aligning the test helpers.

Overall approach: prototype in Python first (faster iteration, easier to reason about the state machine), then port to Rust once the logic was validated. The cross-language comparison harness ensures the two implementations stay in sync.

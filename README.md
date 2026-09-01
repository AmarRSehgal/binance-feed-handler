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

Flags: `--max-symbols N`, `--port N` (default 8080), `--ws-port N` (default 8081), `--ws-base URL` (default `wss://fstream.binance.com/ws`).

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
# Python (38 tests)
pip install -r python/requirements-dev.txt
python -m pytest tests/python/ -v

# Rust (27 tests)
cd rust && cargo test && cd ..

# Cross-language comparison (5 deterministic scenarios, diffs every field)
cd rust && cargo build && cd ..
python tests/cross_language_comparison.py --rust-binary rust/target/debug/binance-feed-handler
```

## Design decisions

**Per-symbol state machine (uninitialized -> buffering -> live).** The book is pure logic with no I/O -- `on_depth()` returns an action and the caller handles networking. Makes it testable without mocking and portable across languages.

**Global update IDs.** Binance Futures draws `U`/`u`/`pu` from a single venue-wide counter rather than a per-symbol one (unlike Spot). Measured against the live feed: three symbols sampled concurrently occupy the *same* numeric ID range and their `[U, u]` intervals interleave, so the merged stream is not monotonic.

That does **not** break the documented bridge check, though. Each symbol's `[U, u]` interval covers the whole global range since that symbol's own previous event, so per symbol the intervals tile contiguously and a REST `lastUpdateId` lands inside exactly one buffered event. Measured across BTC/ETH/SOL/DOGE/XRP: `U <= lastUpdateId <= u` held on 5/5 syncs, and `pu[i] == u[i-1]` held on 688/688 consecutive events. So the bridge check is enforced -- a snapshot whose first replayable diff starts after `lastUpdateId` is rejected and re-fetched rather than applied over a hole.

The `need_first_event` escape hatch survives only for the case where the bridge genuinely cannot be proven: the snapshot is newer than everything buffered, nothing straddles it, and `last_update_id` is a REST id with no stream `u` to chain `pu` against. That path is counted (`unverified_bridge_count`) rather than silently trusted.

**Dual-stream cross-validation.** Each symbol subscribes to `@depth@100ms` (sequenced diffs for the full book) and `@bookTicker` (independent BBO snapshots). The bookTicker feed cross-validates the book's best levels -- 10 consecutive mismatches trigger a re-snapshot. Catches drift that sequence checks alone would miss.

**Connection sharding at 100 symbols per WebSocket.** Two streams per symbol, so 100 symbols is 200 streams per connection. Binance raised the per-connection stream cap from 200 to 1024 on 2025-07-02, so this is now ~5x more conservative than it needs to be -- deliberately, since fewer symbols per socket means a disconnect re-snapshots fewer books. Worth revisiting if connection count becomes the constraint.

**Stream endpoint is a flag, not a constant (`--ws-base`).** Binance announced (changelog 2026-03-05) a split of the futures stream host into `/public`, `/market` and `/private`, with the legacy `/ws` path to be decommissioned 2026-04-23. As of 2026-08-31 that has not happened: `/public` and `/market` both return HTTP 404 while `/ws` serves everything normally. Both streams used here (`@depth`, `@bookTicker`) are in the `/public` tier, so when the cutover does land the migration is `--ws-base wss://fstream.binance.com/public` with no code change.

**Token-bucket rate limiting (3 req/sec).** Startup fires ~570 snapshot requests simultaneously. The rate limiter paces them to stay within Binance's weight limits. Lock-based, not token-bucket in the traditional sense -- simpler, same effect for a single-consumer queue.

**Queue overflow sheds load, and counts it.** When a consumer falls behind, messages are dropped rather than blocking the read loop -- stale market data has negative value. The two implementations shed different ends: Python pops the oldest (`asyncio.Queue`), Rust sheds the newest, because tokio's `mpsc` has no sender-side pop. Both count every drop and surface it in `/stats` under `dropped` (`bbo_queue`, `book_queue`, `subscriber_queue`, `book_buffer`) and in the 10s stats line. A silent drop is a correctness bug, not a performance trade-off.

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
shards=6/6 | books: 568 live, 2 buffering | msgs=184920 | pub: 92100 bbo, 45300 book | snaps=570 | latency=45ms | gaps=0 crossed=0 dropped=0
```

`/health` reports `healthy: true` when all shards are connected and >50% of books are live. Note it always answers HTTP 200 -- the verdict is the `healthy` field in the body, so a probe must read the JSON, not just the status code. `/stats` returns the full breakdown as JSON, including per-queue `dropped` counters and the live `subscribers` count.

## Known limitations / what I'd do with more time

- **No Prometheus/OTLP metrics.** Stats are JSON-over-HTTP. Production would use histogram latencies and alertable counters.
- **No warm standby connections.** On disconnect, there's a cold reconnect + re-subscribe + re-snapshot cycle. Shadow connections pre-buffering would give instant failover.
- **No snapshot prioritization.** All symbols snapshot in arrival order. High-volume symbols (BTC, ETH) should go first.
- **No message compression.** `permessage-deflate` would cut bandwidth at the cost of CPU.
- **No persistent state.** Warm restarts would need serialized book state. Re-sync takes ~3 minutes for 570 symbols so the benefit is marginal.
- **Single-process.** Horizontal scaling would shard symbols across processes or hosts.

## How I used AI

Claude was used throughout -- architecture discussion, protocol research, code generation for both implementations, and this README.

Where it helped most: working through the global update ID problem -- and, on a later audit pass, correcting it. The original conclusion here was that the documented bridge check (`U <= lastUpdateId <= u`) "fails silently on Futures because the docs describe the Spot behavior," and the `need_first_event` flag was written to skip the check entirely. Measuring against the live feed showed that was half right: the IDs really are drawn from one venue-wide counter, but the per-symbol `[U, u]` intervals still tile that counter contiguously, so the documented check holds (5/5 syncs, 688/688 `pu` links). Skipping it was disabling the one guard that catches a torn snapshot. The check is now enforced and the escape hatch is narrowed to the genuinely unprovable case, and counted.

The lesson worth keeping: a plausible mechanism ("other symbols consume the IDs") was reasoned into a design change without ever being measured. The measurement took about a minute.

Where I caught it: the Rust port initially compiled with `String` arguments where `tokio-tungstenite` and `axum` expect `Utf8Bytes` -- five type mismatches that required `.into()` conversions. The cross-language comparison also caught a subtle difference in how the Python and Rust versions handle the `_need_first_event` flag during test scenarios, which required aligning the test helpers.

Overall approach: prototype in Python first (faster iteration, easier to reason about the state machine), then port to Rust once the logic was validated. The cross-language comparison harness ensures the two implementations stay in sync.

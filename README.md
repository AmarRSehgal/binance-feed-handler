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

Flags: `--max-symbols N` (takes the N most liquid by 24h quote volume, so a capped run includes BTCUSDT), `--symbols BTCUSDT,ETHUSDT` (explicit universe; unknown symbols are fatal at startup), `--snapshot-rate` (REST snapshots/sec, default 3.0), `--port N` (default 8080), `--ws-port N` (default 8081), `--ws-base URL` (default `wss://fstream.binance.com/ws`), `--fapi-base URL` (default `https://fapi.binance.com`).

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
# Python (64 tests)
pip install -r python/requirements-dev.txt
python -m pytest tests/python/ -v

# Rust (44 unit + 1 end-to-end reconnect/resync test against a mock venue)
cd rust && cargo test && cd ..

# Cross-language: 5 deterministic scenarios + a differential fuzzer that drives
# both implementations through an identical LCG-generated event script
cd rust && cargo build && cd ..
python tests/cross_language_comparison.py --rust-binary rust/target/debug/binance-feed-handler
python tests/cross_language_comparison.py --rust-binary rust/target/debug/binance-feed-handler \
    --fuzz-seeds 40 --fuzz-steps 8000
```

## Design decisions

**Per-symbol state machine (uninitialized -> buffering -> live).** The book is pure logic with no I/O -- `on_depth()` returns an action and the caller handles networking. Makes it testable without mocking and portable across languages.

**Global update IDs.** Binance Futures draws `U`/`u`/`pu` from a single venue-wide counter rather than a per-symbol one (unlike Spot). Measured against the live feed: three symbols sampled concurrently occupy the *same* numeric ID range and their `[U, u]` intervals interleave, so the merged stream is not monotonic.

Each symbol's `[U, u]` interval covers the whole global range since that symbol's own previous event, so per symbol the intervals tile contiguously (`pu[i] == u[i-1]`: 688/688 consecutive events) and a REST `lastUpdateId` lands inside exactly one of them. The bridge check is enforced: a snapshot whose first replayable diff starts after `lastUpdateId` is rejected and re-fetched rather than applied over a hole.

The catch, measured on 2026-09-01: **the straddling event usually has not arrived yet when the snapshot does.** The REST snapshot reflects exchange state ahead of where the `@depth@100ms` stream has delivered, so `lastUpdateId` runs ahead of the symbol's last received `u` -- 36,769 global ids on BTCUSDT, 16,296 on ETHUSDT, 12,472 on SOLUSDT, 0 on the illiquid `1000BONKUSDC`. Every buffered diff is therefore older than the snapshot, nothing straddles it, and the sync takes the unproven path. In a live 15-symbol run 13 of 15 books went live that way, so `total_unverified_bridges` is the **normal** state on this venue, not an anomaly -- do not alert on it.

Waiting instead of going live immediately would fix that: the straddling event does arrive, 26-70ms and 2-7 events later (BTCUSDT 26ms/7 events, ETHUSDT 69ms/3, SOLUSDT 27ms/2). See "what I'd do with more time".

The `need_first_event` escape hatch covers that unproven path: nothing straddles the snapshot, so `last_update_id` is a `lastUpdateId` with no stream `u` to chain `pu` against, and the first live event is accepted without a sequence check. It is not a hole -- every diff with `u <= lastUpdateId` is already contained in the snapshot and is discarded, and the first accepted diff starts at or before `lastUpdateId + 1`, so the chain is contiguous. But it *is* one unverified event per sync, and it is counted (`unverified_bridge_count`) rather than silently trusted.

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
| Stale book | No update in 30s | Re-enter buffering and request a snapshot immediately |
| Disconnect | Socket closed or errored | Un-sync every book on that shard at once, then reconnect and re-snapshot |

## Observability

Stats line every 10s:
```
shards=6/6 | books: 568 live, 2 buffering | msgs=184920 | pub: 92100 bbo, 45300 book | snaps=570 | latency=45ms | gaps=0 crossed=0 dropped=0
```

`/health` answers **200 when healthy and 503 when not** (all shards connected and >50% of books live), so an orchestrator probe can act on the status code alone. `/stats` returns the full breakdown as JSON, including per-queue `dropped` counters, `total_unverified_bridges`, and the live `subscribers` count. `/metrics` exposes the same numbers in Prometheus text format; `bfh_sequence_gaps_total` and `bfh_dropped_total` are the two worth alerting on.

All four surfaces (`/health`, `/stats`, `/metrics`, the stats line) derive from a single telemetry snapshot taken in one pass over the books, so they cannot disagree with each other.

## Known limitations / what I'd do with more time

- **Latency is a last-value gauge, not a histogram.** `/metrics` exposes alertable counters, but per-shard latency is the most recent sample rather than a percentile distribution. Production wants a histogram.
- **No warm standby connections.** On disconnect, there's a cold reconnect + re-subscribe + re-snapshot cycle. Shadow connections pre-buffering would give instant failover.
- **The snapshot bridge is provable but is not being proved.** On an unstraddled snapshot the book goes live immediately and accepts one unverified event. Measured, the straddling event arrives 26-70ms later, so staying in `buffering` until it does would prove `U <= lastUpdateId <= u` on every active symbol and let `need_first_event` be deleted. Needs a third `on_snapshot` outcome (hold, don't refetch) plus a bounded fallback -- a symbol with no flow at all (e.g. `ALPACAUSDT`, whose `lastUpdateId` is 71bn ids behind the counter) will never produce a straddling event and must still go live.
- **No message compression.** `permessage-deflate` would cut bandwidth at the cost of CPU.
- **Cold start is ~190s for 570 symbols, and that is the venue's floor, not ours.** `/fapi/v1/depth?limit=500` costs 10 request weight against a 2400/min IP budget, so 4 req/s is 100% of it. Snapshots are paced in liquidity order (`--snapshot-rate`, default 3/s), which is why BTCUSDT is live at ~0.7s rather than at minute two, but the tail still takes minutes. Going faster means `limit=100` (weight 5) at the cost of book depth.
- **No persistent state.** Warm restarts would need serialized book state.
- **Load shedding drops different ends in the two implementations.** Python sheds the oldest queued event, Rust the newest, because tokio's `mpsc` has no sender-side pop. Drop-oldest is the right behaviour for market data, so Rust is the side that is wrong; fixing it means `tokio::sync::broadcast`, whose `Lagged(n)` is exactly the drop-oldest-and-count semantics wanted.
- **Single-process.** Horizontal scaling would shard symbols across processes or hosts.

## How I used AI

Claude was used throughout -- architecture discussion, protocol research, code generation for both implementations, and this README.

Where it helped most: the global update ID problem, which took three passes to get right and is a decent case study in how a plausible mechanism survives longer than it should.

Pass one concluded that the documented bridge check (`U <= lastUpdateId <= u`) "fails silently on Futures because the docs describe the Spot behavior," and wrote `need_first_event` to skip the check entirely. Pass two measured it: the ids really are drawn from one venue-wide counter, but per symbol the `[U, u]` intervals tile that counter contiguously, so the check is sound -- skipping it was disabling the one guard against a torn snapshot. The check went back in.

Pass three measured the thing neither earlier pass had: *how often the check actually fires*. 13 of 15 books in a live run went live on the unproven path, because the REST snapshot is ahead of the stream by tens of thousands of global ids and the straddling diff simply has not arrived yet. Pass two's "5/5 syncs" had measured the `pu` chain, not the bridge, and read the result as confirming both.

The lesson worth keeping is the same one twice: each pass reasoned from a mechanism instead of a measurement, and each measurement took about a minute. Pass three also produced the fix -- wait 26-70ms for the straddling event -- which neither reasoning pass had reached.

Where I caught it: the Rust port initially compiled with `String` arguments where `tokio-tungstenite` and `axum` expect `Utf8Bytes` -- five type mismatches that required `.into()` conversions. The cross-language comparison also caught a subtle difference in how the Python and Rust versions handle the `_need_first_event` flag during test scenarios, which required aligning the test helpers.

Overall approach: prototype in Python first (faster iteration, easier to reason about the state machine), then port to Rust once the logic was validated. The cross-language comparison harness ensures the two implementations stay in sync.

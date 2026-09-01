/// Binance USD-M Futures feed handler.
///
/// Subscribes to all perpetual futures, maintains per-symbol order books,
/// and publishes BBO + book updates to downstream consumers via tokio channels.
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use axum::routing::get;
use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn};
use rand::Rng;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{mpsc, Mutex};
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::book::{parse_levels, Action, Book, BookState};
use crate::publisher;

// === CONFIGURATION ===

/// REST base for exchangeInfo / depth snapshots. Overridable via `--fapi-base`
/// so the reconnect/resync integration test can point the handler at a mock
/// venue instead of the live exchange.
pub const BINANCE_FAPI_DEFAULT: &str = "https://fapi.binance.com";

/// Binance announced (changelog 2026-03-05) a split of the futures stream host
/// into /public, /market and /private, with the legacy /ws and /stream paths to
/// be decommissioned 2026-04-23. As of 2026-08-31 that has NOT happened: /public
/// and /market both return HTTP 404 and /ws still serves everything. Both streams
/// we use (@depth and @bookTicker) are in the /public tier, so when the cutover
/// does land, `--ws-base wss://fstream.binance.com/public` is the whole migration.
pub const BINANCE_WS_DEFAULT: &str = "wss://fstream.binance.com/ws";

const MAX_SYMBOLS_PER_SHARD: usize = 100;
const SUBSCRIBE_BATCH_SIZE: usize = 50;
const SNAPSHOT_RATE: f64 = 3.0;
const RECONNECT_BASE: f64 = 1.0;
const RECONNECT_MAX: f64 = 30.0;
const STATS_INTERVAL: u64 = 10;
const STALE_THRESHOLD: f64 = 30.0;
const QUEUE_MAX: usize = 100_000;

// === TYPES ===

#[derive(Debug, Clone, Serialize)]
pub struct BboEvent {
    pub symbol: String,
    pub bid_price: String,
    pub bid_qty: String,
    pub ask_price: String,
    pub ask_qty: String,
    pub ts: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BookEvent {
    pub symbol: String,
    pub bids: Vec<(String, String)>,
    pub asks: Vec<(String, String)>,
    pub last_update_id: u64,
    pub ts: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShardInfo {
    pub id: usize,
    pub connected: bool,
    pub msg_count: u64,
    pub last_msg_time: f64,
    pub latency_ms: f64,
}

/// Shared handles every shard needs. Grouped into a struct so shards cannot be
/// wired up with the wrong `Arc` by positional mistake.
#[derive(Clone)]
struct ShardCtx {
    books: Arc<Mutex<HashMap<String, Book>>>,
    client: reqwest::Client,
    bbo_tx: mpsc::Sender<BboEvent>,
    book_tx: mpsc::Sender<BookEvent>,
    shard_infos: Arc<Mutex<Vec<ShardInfo>>>,
    snap_lock: Arc<Mutex<f64>>,
    snap_count: Arc<AtomicU64>,
    bbo_count: Arc<AtomicU64>,
    book_count: Arc<AtomicU64>,
    ws_base: String,
    fapi_base: String,
}

#[derive(Clone)]
struct AppState {
    books: Arc<Mutex<HashMap<String, Book>>>,
    shard_infos: Arc<Mutex<Vec<ShardInfo>>>,
    bbo_count: Arc<AtomicU64>,
    book_count: Arc<AtomicU64>,
    snap_count: Arc<AtomicU64>,
    start_time: Instant,
}

// === HELPERS ===

/// Events shed because the dispatcher channel was full. Never silent: surfaced
/// in /stats and the periodic stats line.
pub static BBO_DROPPED: AtomicU64 = AtomicU64::new(0);
pub static BOOK_DROPPED: AtomicU64 = AtomicU64::new(0);

/// Non-blocking enqueue. tokio's mpsc has no sender-side pop, so a full channel
/// sheds the NEWEST event (not the oldest). Counted via `dropped`.
fn put<T>(tx: &mpsc::Sender<T>, item: T, dropped: &AtomicU64) {
    if tx.try_send(item).is_err() {
        dropped.fetch_add(1, Ordering::Relaxed);
    }
}

fn now_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

// === REST ===

#[derive(Deserialize)]
struct ExchangeInfo {
    symbols: Vec<SymbolInfo>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SymbolInfo {
    symbol: String,
    contract_type: String,
    status: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Ticker24h {
    symbol: String,
    quote_volume: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DepthSnapshot {
    last_update_id: u64,
    bids: Vec<(String, String)>,
    asks: Vec<(String, String)>,
}

/// Keep the `n` highest-24h-quote-volume symbols, returned alphabetically so
/// shard assignment stays deterministic across restarts.
///
/// A plain `truncate(n)` after an alphabetical sort is what this replaces: it
/// selected 1000BONK/1000FLOKI/AAVE/ACE... and never once included BTCUSDT, so
/// every capped test run exercised the thinnest books on the venue.
/// Symbols missing from the ticker response rank last; ties break on name.
fn pick_top_by_volume(
    symbols: &[String],
    volumes: &HashMap<String, f64>,
    n: usize,
) -> Vec<String> {
    let mut ranked: Vec<&String> = symbols.iter().collect();
    ranked.sort_by(|a, b| {
        let va = volumes.get(*a).copied().unwrap_or(0.0);
        let vb = volumes.get(*b).copied().unwrap_or(0.0);
        vb.partial_cmp(&va).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.cmp(b))
    });
    let mut top: Vec<String> = ranked.into_iter().take(n).cloned().collect();
    top.sort();
    top
}

async fn fetch_quote_volumes(
    client: &reqwest::Client,
    fapi_base: &str,
) -> anyhow::Result<HashMap<String, f64>> {
    let url = format!("{}/fapi/v1/ticker/24hr", fapi_base);
    let tickers: Vec<Ticker24h> = client.get(&url).send().await?.error_for_status()?.json().await?;
    Ok(tickers
        .into_iter()
        .map(|t| (t.symbol, t.quote_volume.parse::<f64>().unwrap_or(0.0)))
        .collect())
}

async fn fetch_symbols(
    client: &reqwest::Client,
    fapi_base: &str,
    max_symbols: Option<usize>,
    explicit: &[String],
) -> anyhow::Result<Vec<String>> {
    let url = format!("{}/fapi/v1/exchangeInfo", fapi_base);
    let info: ExchangeInfo = client.get(&url).send().await?.error_for_status()?.json().await?;
    let mut symbols: Vec<String> = info
        .symbols
        .into_iter()
        .filter(|s| s.contract_type == "PERPETUAL" && s.status == "TRADING")
        .map(|s| s.symbol)
        .collect();
    symbols.sort();

    if !explicit.is_empty() {
        // A typo'd symbol would otherwise subscribe to a stream that never ticks
        // and look like a dead feed, so reject it at startup.
        let known: std::collections::HashSet<&str> =
            symbols.iter().map(String::as_str).collect();
        let unknown: Vec<&str> = explicit
            .iter()
            .map(String::as_str)
            .filter(|s| !known.contains(s))
            .collect();
        if !unknown.is_empty() {
            anyhow::bail!(
                "--symbols: not a trading USD-M perpetual: {}",
                unknown.join(", ")
            );
        }
        let mut picked: Vec<String> = explicit.to_vec();
        picked.sort();
        picked.dedup();
        return Ok(picked);
    }

    if let Some(max) = max_symbols {
        if max < symbols.len() {
            let volumes = fetch_quote_volumes(client, fapi_base).await?;
            symbols = pick_top_by_volume(&symbols, &volumes, max);
        }
    }
    Ok(symbols)
}

async fn fetch_snapshot(
    client: &reqwest::Client,
    fapi_base: &str,
    symbol: &str,
    snap_lock: &Mutex<f64>,
    snap_count: &AtomicU64,
) -> anyhow::Result<DepthSnapshot> {
    {
        let mut last_t = snap_lock.lock().await;
        let now = now_secs();
        let wait = *last_t + (1.0 / SNAPSHOT_RATE) - now;
        if wait > 0.0 {
            sleep(Duration::from_secs_f64(wait)).await;
        }
        *last_t = now_secs();
    }

    let url = format!("{}/fapi/v1/depth", fapi_base);
    let resp = client
        .get(&url)
        .query(&[("symbol", symbol), ("limit", "500")])
        .send()
        .await?;

    snap_count.fetch_add(1, Ordering::Relaxed);

    if resp.status() == 429 {
        let retry_after = resp
            .headers()
            .get("Retry-After")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(10);
        warn!("{}: rate limited, backing off {}s", symbol, retry_after);
        sleep(Duration::from_secs(retry_after)).await;
        anyhow::bail!("rate limited ({})", symbol);
    }

    Ok(resp.json().await?)
}

// === SNAPSHOT SYNC ===

async fn sync_book(
    client: reqwest::Client,
    fapi_base: String,
    symbol: String,
    books: Arc<Mutex<HashMap<String, Book>>>,
    snap_lock: Arc<Mutex<f64>>,
    snap_count: Arc<AtomicU64>,
) {
    let mut delay = 1.0;
    loop {
        {
            let books = books.lock().await;
            if books.get(&symbol).is_none_or(|b| b.state != BookState::Buffering) {
                return;
            }
        }

        match fetch_snapshot(&client, &fapi_base, &symbol, &snap_lock, &snap_count).await {
            Ok(data) => {
                let bids = parse_levels(&data.bids);
                let asks = parse_levels(&data.asks);
                let mut books = books.lock().await;
                if let Some(book) = books.get_mut(&symbol) {
                    if book.on_snapshot(data.last_update_id, &bids, &asks) {
                        return;
                    }
                }
                sleep(Duration::from_millis(500)).await;
            }
            Err(e) => {
                error!("{}: snapshot failed: {}. Retry in {:.1}s", symbol, e, delay);
                sleep(Duration::from_secs_f64(delay)).await;
                delay = (delay * 2.0).min(10.0);
            }
        }
    }
}

// === WEBSOCKET ===

async fn run_shard(shard_id: usize, symbols: Vec<String>, ctx: ShardCtx) {
    let ShardCtx {
        books,
        client,
        bbo_tx,
        book_tx,
        shard_infos,
        snap_lock,
        snap_count,
        bbo_count,
        book_count,
        ws_base,
        fapi_base,
    } = ctx;

    let mut reconnect_delay = RECONNECT_BASE;
    let mut sync_handles: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();

    let request_sync = |sym: &str, sync_handles: &mut HashMap<String, tokio::task::JoinHandle<()>>| {
        if let Some(h) = sync_handles.remove(sym) {
            h.abort();
        }
        let handle = tokio::spawn(sync_book(
            client.clone(),
            fapi_base.clone(),
            sym.to_string(),
            books.clone(),
            snap_lock.clone(),
            snap_count.clone(),
        ));
        sync_handles.insert(sym.to_string(), handle);
    };

    loop {
        let mut stream_names = Vec::new();
        for sym in &symbols {
            let s = sym.to_lowercase();
            stream_names.push(format!("{}@depth@100ms", s));
            stream_names.push(format!("{}@bookTicker", s));
        }

        info!("Shard {}: connecting ({} symbols)", shard_id, symbols.len());

        match connect_async(&ws_base).await {
            Ok((ws_stream, _)) => {
                {
                    let mut infos = shard_infos.lock().await;
                    infos[shard_id].connected = true;
                }
                reconnect_delay = RECONNECT_BASE;

                let (mut write, mut read) = ws_stream.split();

                for batch in stream_names.chunks(SUBSCRIBE_BATCH_SIZE) {
                    let subscribe_msg = serde_json::json!({
                        "method": "SUBSCRIBE",
                        "params": batch,
                        "id": 1,
                    });
                    if write
                        .send(Message::Text(subscribe_msg.to_string().into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                    sleep(Duration::from_millis(250)).await;
                }

                info!(
                    "Shard {}: subscribed ({} streams)",
                    shard_id,
                    stream_names.len()
                );

                while let Some(msg_result) = read.next().await {
                    let raw = match msg_result {
                        Ok(Message::Text(t)) => t,
                        Ok(Message::Ping(d)) => {
                            let _ = write.send(Message::Pong(d)).await;
                            continue;
                        }
                        Ok(Message::Close(_)) => break,
                        Err(e) => {
                            error!("Shard {}: ws error: {}", shard_id, e);
                            break;
                        }
                        _ => continue,
                    };

                    {
                        let mut infos = shard_infos.lock().await;
                        infos[shard_id].msg_count += 1;
                        infos[shard_id].last_msg_time = now_secs();
                    }

                    let msg: Value = match serde_json::from_str(&raw) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    if msg.get("result").is_some() {
                        continue;
                    }
                    if msg.get("id").is_some() && msg.get("e").is_none() {
                        continue;
                    }

                    let symbol = match msg.get("s").and_then(|v| v.as_str()) {
                        Some(s) => s.to_string(),
                        None => continue,
                    };

                    if let Some(event_time) = msg.get("E").and_then(|v| v.as_f64()) {
                        let latency = now_secs() * 1000.0 - event_time;
                        let mut infos = shard_infos.lock().await;
                        infos[shard_id].latency_ms = latency;
                    }

                    let evt = match msg.get("e").and_then(|v| v.as_str()) {
                        Some(e) => e.to_string(),
                        None => continue,
                    };

                    if evt == "depthUpdate" {
                        let big_u = msg["U"].as_u64().unwrap_or(0);
                        let small_u = msg["u"].as_u64().unwrap_or(0);
                        let pu = msg["pu"].as_u64().unwrap_or(0);
                        let raw_bids: Vec<(String, String)> = msg["b"]
                            .as_array()
                            .map(|a| {
                                a.iter()
                                    .filter_map(|v| {
                                        let arr = v.as_array()?;
                                        Some((arr[0].as_str()?.to_string(), arr[1].as_str()?.to_string()))
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        let raw_asks: Vec<(String, String)> = msg["a"]
                            .as_array()
                            .map(|a| {
                                a.iter()
                                    .filter_map(|v| {
                                        let arr = v.as_array()?;
                                        Some((arr[0].as_str()?.to_string(), arr[1].as_str()?.to_string()))
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();

                        let bids = parse_levels(&raw_bids);
                        let asks = parse_levels(&raw_asks);

                        let action = {
                            let mut books = books.lock().await;
                            if let Some(book) = books.get_mut(&symbol) {
                                book.last_update_time = now_secs();
                                book.on_depth(big_u, small_u, pu, bids, asks)
                            } else {
                                Action::None_
                            }
                        };

                        match action {
                            Action::NeedSnapshot => {
                                request_sync(&symbol, &mut sync_handles);
                            }
                            Action::Publish => {
                                book_count.fetch_add(1, Ordering::Relaxed);
                                let books = books.lock().await;
                                if let Some(book) = books.get(&symbol) {
                                    let (top_bids, top_asks) = book.top_levels(20);
                                    put(
                                        &book_tx,
                                        BookEvent {
                                            symbol: symbol.clone(),
                                            bids: top_bids
                                                .into_iter()
                                                .map(|(p, q)| (p.to_string(), q.to_string()))
                                                .collect(),
                                            asks: top_asks
                                                .into_iter()
                                                .map(|(p, q)| (p.to_string(), q.to_string()))
                                                .collect(),
                                            last_update_id: book.last_update_id,
                                            ts: now_secs(),
                                        },
                                        &BOOK_DROPPED,
                                    );
                                }
                            }
                            Action::None_ => {}
                        }
                    } else if evt == "bookTicker" {
                        let bid_price = msg["b"].as_str().unwrap_or("0").to_string();
                        let ask_price = msg["a"].as_str().unwrap_or("0").to_string();
                        let bid_qty = msg["B"].as_str().unwrap_or("0").to_string();
                        let ask_qty = msg["A"].as_str().unwrap_or("0").to_string();

                        {
                            let mut books = books.lock().await;
                            if let Some(book) = books.get_mut(&symbol) {
                                let bid: Decimal = bid_price.parse().unwrap_or_default();
                                let ask: Decimal = ask_price.parse().unwrap_or_default();
                                book.set_ticker_bbo(bid, ask);
                            }
                        }

                        bbo_count.fetch_add(1, Ordering::Relaxed);
                        put(
                            &bbo_tx,
                            BboEvent {
                                symbol: symbol.clone(),
                                bid_price,
                                bid_qty,
                                ask_price,
                                ask_qty,
                                ts: now_secs(),
                            },
                            &BBO_DROPPED,
                        );
                    }
                }
            }
            Err(e) => {
                error!("Shard {}: connection failed: {}", shard_id, e);
            }
        }

        {
            let mut infos = shard_infos.lock().await;
            infos[shard_id].connected = false;
        }
        // Unsync every book the moment the socket dies, not at the top of the
        // next connect attempt: reconnect backoff runs up to 30s, and a book
        // reported "live" during that window is a book whose depth is frozen.
        // This also re-arms the state machine before we re-subscribe.
        {
            let mut books = books.lock().await;
            for sym in &symbols {
                if let Some(book) = books.get_mut(sym) {
                    book.reset();
                }
            }
        }
        for (_, h) in sync_handles.drain() {
            h.abort();
        }

        let jitter: f64 = rand::rng().random_range(0.0..1.0);
        let wait = reconnect_delay + jitter;
        error!(
            "Shard {}: disconnected. Reconnecting in {:.1}s",
            shard_id, wait
        );
        sleep(Duration::from_secs_f64(wait)).await;
        reconnect_delay = (reconnect_delay * 2.0).min(RECONNECT_MAX);
    }
}

// === TELEMETRY ===

/// One consistent read of everything the observability surfaces report, taken
/// under a single pass over the books. /health, /stats, /metrics and the periodic
/// stats line all derive from this, so they can never disagree -- and a new
/// counter is added in one place instead of four.
struct Telemetry {
    uptime_s: u64,
    shards: Vec<ShardInfo>,
    connected: usize,
    book_states: HashMap<String, usize>,
    live: usize,
    total: usize,
    gaps: u64,
    crossed: u64,
    snapshots_per_book: u64,
    unverified_bridges: u64,
    buffer_dropped: u64,
    max_latency_ms: f64,
    bbo_published: u64,
    book_published: u64,
    snapshots: u64,
    bbo_dropped: u64,
    book_dropped: u64,
    subscriber_dropped: u64,
    subscribers: usize,
}

impl Telemetry {
    async fn gather(state: &AppState) -> Self {
        let books = state.books.lock().await;
        let shards = state.shard_infos.lock().await.clone();

        let mut book_states: HashMap<String, usize> = HashMap::new();
        let mut gaps = 0;
        let mut crossed = 0;
        let mut snapshots_per_book = 0;
        let mut unverified_bridges = 0;
        let mut buffer_dropped = 0;
        let mut live = 0;
        for b in books.values() {
            *book_states
                .entry(format!("{:?}", b.state).to_lowercase())
                .or_default() += 1;
            if b.state == BookState::Live {
                live += 1;
            }
            gaps += b.gap_count;
            crossed += b.crossed_count;
            snapshots_per_book += b.snapshot_count;
            unverified_bridges += b.unverified_bridge_count;
            buffer_dropped += b.buffer_dropped;
        }

        Self {
            uptime_s: state.start_time.elapsed().as_secs(),
            connected: shards.iter().filter(|s| s.connected).count(),
            max_latency_ms: shards
                .iter()
                .filter(|s| s.latency_ms > 0.0)
                .map(|s| s.latency_ms)
                .fold(0.0_f64, f64::max),
            shards,
            book_states,
            live,
            total: books.len(),
            gaps,
            crossed,
            snapshots_per_book,
            unverified_bridges,
            buffer_dropped,
            bbo_published: state.bbo_count.load(Ordering::Relaxed),
            book_published: state.book_count.load(Ordering::Relaxed),
            snapshots: state.snap_count.load(Ordering::Relaxed),
            bbo_dropped: BBO_DROPPED.load(Ordering::Relaxed),
            book_dropped: BOOK_DROPPED.load(Ordering::Relaxed),
            subscriber_dropped: publisher::SUBSCRIBER_DROPPED.load(Ordering::Relaxed),
            subscribers: publisher::subscriber_count().await,
        }
    }

    /// Every shard connected and a majority of books live. Same predicate the
    /// /health status code is derived from, so an orchestrator's liveness probe
    /// and a human reading /health can never draw opposite conclusions.
    fn healthy(&self) -> bool {
        self.connected == self.shards.len() && self.live > self.total / 2
    }

    fn total_dropped(&self) -> u64 {
        self.bbo_dropped + self.book_dropped + self.subscriber_dropped + self.buffer_dropped
    }

    fn book_state(&self, state: &str) -> usize {
        self.book_states.get(state).copied().unwrap_or(0)
    }
}

// === HEALTH ===

async fn health_handler(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    let t = Telemetry::gather(&state).await;
    // 503 when unhealthy: a probe that only ever sees 200 cannot restart or
    // depool anything, which is the whole point of the endpoint.
    let code = if t.healthy() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        code,
        Json(serde_json::json!({
            "healthy": t.healthy(),
            "uptime_s": t.uptime_s,
            "shards": format!("{}/{} connected", t.connected, t.shards.len()),
            "books": format!("{}/{} live", t.live, t.total),
        })),
    )
}

async fn stats_handler(State(state): State<AppState>) -> Json<Value> {
    let t = Telemetry::gather(&state).await;
    Json(serde_json::json!({
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
    }))
}

/// Prometheus text exposition. Every counter that /stats reports, in the form a
/// scraper can alert on -- `bfh_sequence_gaps_total` and `bfh_dropped_total` are
/// the two worth paging on.
async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let t = Telemetry::gather(&state).await;
    let mut out = String::with_capacity(2048);

    let mut gauge = |name: &str, help: &str, kind: &str, body: String| {
        out.push_str(&format!("# HELP {} {}\n# TYPE {} {}\n{}", name, help, name, kind, body));
    };

    gauge(
        "bfh_up",
        "1 when every shard is connected and a majority of books are live",
        "gauge",
        format!("bfh_up {}\n", u8::from(t.healthy())),
    );
    gauge("bfh_uptime_seconds", "Process uptime", "gauge",
          format!("bfh_uptime_seconds {}\n", t.uptime_s));
    gauge("bfh_shards", "WebSocket shards by connection state", "gauge",
          format!("bfh_shards{{state=\"connected\"}} {}\nbfh_shards{{state=\"disconnected\"}} {}\n",
                  t.connected, t.shards.len() - t.connected));
    gauge("bfh_books", "Order books by sync state", "gauge",
          format!("bfh_books{{state=\"live\"}} {}\nbfh_books{{state=\"buffering\"}} {}\nbfh_books{{state=\"uninitialized\"}} {}\n",
                  t.book_state("live"), t.book_state("buffering"), t.book_state("uninitialized")));
    gauge("bfh_published_total", "Events published downstream", "counter",
          format!("bfh_published_total{{stream=\"bbo\"}} {}\nbfh_published_total{{stream=\"book\"}} {}\n",
                  t.bbo_published, t.book_published));
    gauge("bfh_snapshot_requests_total", "REST depth snapshots requested", "counter",
          format!("bfh_snapshot_requests_total {}\n", t.snapshots));
    gauge("bfh_book_snapshots_total", "Snapshots successfully applied to a book", "counter",
          format!("bfh_book_snapshots_total {}\n", t.snapshots_per_book));
    gauge("bfh_sequence_gaps_total", "Depth sequence gaps detected (pu chain broken)", "counter",
          format!("bfh_sequence_gaps_total {}\n", t.gaps));
    gauge("bfh_crossed_books_total", "Integrity failures where bid >= ask", "counter",
          format!("bfh_crossed_books_total {}\n", t.crossed));
    gauge("bfh_unverified_bridges_total",
          "Snapshots that went live without a buffered event proving U <= lastUpdateId <= u",
          "counter",
          format!("bfh_unverified_bridges_total {}\n", t.unverified_bridges));
    gauge("bfh_dropped_total", "Messages shed, by shedding point", "counter",
          format!("bfh_dropped_total{{queue=\"bbo\"}} {}\nbfh_dropped_total{{queue=\"book\"}} {}\nbfh_dropped_total{{queue=\"subscriber\"}} {}\nbfh_dropped_total{{queue=\"resync_buffer\"}} {}\n",
                  t.bbo_dropped, t.book_dropped, t.subscriber_dropped, t.buffer_dropped));
    gauge("bfh_subscribers", "Connected downstream subscribers", "gauge",
          format!("bfh_subscribers {}\n", t.subscribers));
    gauge("bfh_shard_messages_total", "WebSocket messages received per shard", "counter",
          t.shards.iter()
              .map(|s| format!("bfh_shard_messages_total{{shard=\"{}\"}} {}\n", s.id, s.msg_count))
              .collect::<String>());
    gauge("bfh_shard_latency_ms", "Event-time to receive-time lag per shard", "gauge",
          t.shards.iter()
              .map(|s| format!("bfh_shard_latency_ms{{shard=\"{}\"}} {:.1}\n", s.id, s.latency_ms))
              .collect::<String>());

    ([("content-type", "text/plain; version=0.0.4")], out)
}

// === MONITORING ===

async fn log_stats(state: AppState) {
    loop {
        sleep(Duration::from_secs(STATS_INTERVAL)).await;
        let t = Telemetry::gather(&state).await;
        info!(
            "shards={}/{} | books: {} live, {} buffering | msgs={} | \
             pub: {} bbo, {} book | snaps={} | latency={:.0}ms | gaps={} crossed={} dropped={}",
            t.connected,
            t.shards.len(),
            t.book_state("live"),
            t.book_state("buffering"),
            t.shards.iter().map(|s| s.msg_count).sum::<u64>(),
            t.bbo_published,
            t.book_published,
            t.snapshots,
            t.max_latency_ms,
            t.gaps,
            t.crossed,
            t.total_dropped(),
        );
    }
}

/// Mark every live-but-silent book for resync and return the symbols to
/// re-snapshot. Pure over `books` so the sweep decision is unit-testable.
///
/// `last_update_time` is pushed forward to `now` on trigger: a symbol whose
/// stream is genuinely dead would otherwise re-qualify on every tick and
/// snapshot-storm the REST budget.
fn take_stale_books(
    books: &mut HashMap<String, Book>,
    now: f64,
    threshold: f64,
) -> Vec<String> {
    let mut stale = Vec::new();
    for book in books.values_mut() {
        if book.state == BookState::Live && (now - book.last_update_time) > threshold {
            warn!(
                "{}: stale for {:.0}s, re-snapshotting",
                book.symbol,
                now - book.last_update_time
            );
            book.mark_for_resync();
            book.last_update_time = now;
            stale.push(book.symbol.clone());
        }
    }
    stale
}

async fn check_stale(
    books: Arc<Mutex<HashMap<String, Book>>>,
    client: reqwest::Client,
    fapi_base: String,
    snap_lock: Arc<Mutex<f64>>,
    snap_count: Arc<AtomicU64>,
) {
    loop {
        sleep(Duration::from_secs_f64(STALE_THRESHOLD / 2.0)).await;
        let stale = {
            let mut books = books.lock().await;
            take_stale_books(&mut books, now_secs(), STALE_THRESHOLD)
        };
        // Reset alone only re-arms the state machine; the snapshot still has to
        // be asked for, or recovery waits on a depth event that may be minutes
        // out on an illiquid symbol.
        for symbol in stale {
            tokio::spawn(sync_book(
                client.clone(),
                fapi_base.clone(),
                symbol,
                books.clone(),
                snap_lock.clone(),
                snap_count.clone(),
            ));
        }
    }
}

// === MAIN ===

/// Everything `run` needs. A struct rather than positional args so a test
/// pointing at a mock venue cannot transpose the two base URLs.
#[derive(Clone, Debug)]
pub struct Config {
    pub max_symbols: Option<usize>,
    pub symbols: Vec<String>,
    pub port: u16,
    pub ws_port: u16,
    pub ws_base: String,
    pub fapi_base: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_symbols: None,
            symbols: Vec::new(),
            port: 8080,
            ws_port: 8081,
            ws_base: BINANCE_WS_DEFAULT.to_string(),
            fapi_base: BINANCE_FAPI_DEFAULT.to_string(),
        }
    }
}

pub async fn run(cfg: Config) -> anyhow::Result<()> {
    let Config {
        max_symbols,
        symbols: symbols_arg,
        port,
        ws_port,
        ws_base,
        fapi_base,
    } = cfg;
    info!("Starting Binance USD-M Futures Feed Handler");

    let client = reqwest::Client::new();
    let symbols = fetch_symbols(&client, &fapi_base, max_symbols, &symbols_arg).await?;
    info!("Tracking {} perpetual symbols", symbols.len());

    let books: HashMap<String, Book> = symbols
        .iter()
        .map(|s| (s.clone(), Book::new(s.clone())))
        .collect();
    let books = Arc::new(Mutex::new(books));

    let (bbo_tx, bbo_rx) = mpsc::channel::<BboEvent>(QUEUE_MAX);
    let (book_tx, book_rx) = mpsc::channel::<BookEvent>(QUEUE_MAX);

    let snap_lock = Arc::new(Mutex::new(0.0_f64));
    let snap_count = Arc::new(AtomicU64::new(0));
    let bbo_count = Arc::new(AtomicU64::new(0));
    let book_count = Arc::new(AtomicU64::new(0));

    // Build the full vec and share ONE Arc with every shard. Each shard writes its
    // own slot; /health, /stats and log_stats read the same allocation.
    let shard_infos: Vec<ShardInfo> = (0..symbols.chunks(MAX_SYMBOLS_PER_SHARD).count())
        .map(|i| ShardInfo {
            id: i,
            connected: false,
            msg_count: 0,
            last_msg_time: 0.0,
            latency_ms: 0.0,
        })
        .collect();
    let shard_infos = Arc::new(Mutex::new(shard_infos));

    let shard_ctx = ShardCtx {
        books: books.clone(),
        client: client.clone(),
        bbo_tx,
        book_tx,
        shard_infos: shard_infos.clone(),
        snap_lock: snap_lock.clone(),
        snap_count: snap_count.clone(),
        bbo_count: bbo_count.clone(),
        book_count: book_count.clone(),
        ws_base,
        fapi_base: fapi_base.clone(),
    };
    for (i, chunk) in symbols.chunks(MAX_SYMBOLS_PER_SHARD).enumerate() {
        tokio::spawn(run_shard(i, chunk.to_vec(), shard_ctx.clone()));
    }
    info!("Created {} WS shards", symbols.chunks(MAX_SYMBOLS_PER_SHARD).len());

    let start_time = Instant::now();
    let app_state = AppState {
        books: books.clone(),
        shard_infos: shard_infos.clone(),
        bbo_count: bbo_count.clone(),
        book_count: book_count.clone(),
        snap_count: snap_count.clone(),
        start_time,
    };

    let app = axum::Router::new()
        .route("/health", get(health_handler))
        .route("/stats", get(stats_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(app_state.clone());

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    info!("Health server on :{}", port);
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });

    publisher::start_ws_server(ws_port).await?;

    tokio::spawn(log_stats(app_state));
    tokio::spawn(check_stale(
        books.clone(),
        client.clone(),
        fapi_base.clone(),
        snap_lock.clone(),
        snap_count.clone(),
    ));

    // Start dispatcher
    publisher::run_dispatcher(bbo_rx, book_rx).await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn live_book(symbol: &str, last_update_time: f64) -> Book {
        let mut b = Book::new(symbol.to_string());
        b.on_depth(1, 10, 0, vec![], vec![]);
        b.on_snapshot(100, &[(dec!(100), dec!(1))], &[(dec!(101), dec!(1))]);
        b.last_update_time = last_update_time;
        b
    }

    fn books_of(entries: Vec<Book>) -> HashMap<String, Book> {
        entries.into_iter().map(|b| (b.symbol.clone(), b)).collect()
    }

    #[test]
    fn test_stale_book_is_marked_for_resync() {
        let mut books = books_of(vec![live_book("STALE", 0.0)]);
        let stale = take_stale_books(&mut books, 100.0, 30.0);
        assert_eq!(stale, vec!["STALE".to_string()]);
        // Buffering (not Uninitialized) is what makes sync_book actually fetch.
        assert_eq!(books["STALE"].state, BookState::Buffering);
    }

    #[test]
    fn test_fresh_book_is_left_alone() {
        let mut books = books_of(vec![live_book("FRESH", 90.0)]);
        assert!(take_stale_books(&mut books, 100.0, 30.0).is_empty());
        assert_eq!(books["FRESH"].state, BookState::Live);
    }

    #[test]
    fn test_non_live_book_is_not_swept() {
        let mut b = Book::new("BUFFERING".to_string());
        b.on_depth(1, 10, 0, vec![], vec![]);
        let mut books = books_of(vec![b]);
        assert!(take_stale_books(&mut books, 1e9, 30.0).is_empty());
    }

    fn vols(pairs: &[(&str, f64)]) -> HashMap<String, f64> {
        pairs.iter().map(|(s, v)| (s.to_string(), *v)).collect()
    }

    fn syms(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_cap_picks_the_most_liquid_not_the_alphabetically_first() {
        // The regression: an alphabetical truncate(2) gives 1000BONK + AAVE and
        // never BTCUSDT, so a capped run tests only the thinnest books.
        let all = syms(&["1000BONKUSDT", "AAVEUSDT", "BTCUSDT", "ETHUSDT"]);
        let v = vols(&[
            ("1000BONKUSDT", 1.0e7),
            ("AAVEUSDT", 5.0e7),
            ("BTCUSDT", 9.0e9),
            ("ETHUSDT", 4.0e9),
        ]);
        assert_eq!(pick_top_by_volume(&all, &v, 2), syms(&["BTCUSDT", "ETHUSDT"]));
    }

    #[test]
    fn test_pick_returns_alphabetical_for_deterministic_sharding() {
        let all = syms(&["AAA", "BBB", "CCC"]);
        let v = vols(&[("AAA", 1.0), ("BBB", 3.0), ("CCC", 2.0)]);
        assert_eq!(pick_top_by_volume(&all, &v, 3), syms(&["AAA", "BBB", "CCC"]));
    }

    #[test]
    fn test_pick_ranks_missing_volume_last_and_breaks_ties_by_name() {
        let all = syms(&["AAA", "BBB", "ZZZ"]);
        // AAA and BBB tie; ZZZ has no ticker row at all.
        let v = vols(&[("AAA", 5.0), ("BBB", 5.0)]);
        assert_eq!(pick_top_by_volume(&all, &v, 2), syms(&["AAA", "BBB"]));
        assert_eq!(pick_top_by_volume(&all, &v, 1), syms(&["AAA"]));
    }

    #[test]
    fn test_pick_beyond_universe_size_returns_everything() {
        let all = syms(&["AAA", "BBB"]);
        assert_eq!(pick_top_by_volume(&all, &vols(&[("AAA", 1.0)]), 99), all);
    }

    #[test]
    fn test_sweep_does_not_refire_on_the_next_tick() {
        // A symbol whose stream is dead must not re-snapshot every sweep: the
        // trigger pushes last_update_time forward, and the book is no longer Live.
        let mut books = books_of(vec![live_book("DEAD", 0.0)]);
        assert_eq!(take_stale_books(&mut books, 100.0, 30.0).len(), 1);
        assert!(take_stale_books(&mut books, 115.0, 30.0).is_empty());
    }
}

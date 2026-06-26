/// Binance USD-M Futures feed handler.
///
/// Subscribes to all perpetual futures, maintains per-symbol order books,
/// and publishes BBO + book updates to downstream consumers via tokio channels.
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::response::Json;
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

const BINANCE_FAPI: &str = "https://fapi.binance.com";
const BINANCE_WS: &str = "wss://fstream.binance.com/ws";

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

fn put<T>(tx: &mpsc::Sender<T>, item: T) {
    let _ = tx.try_send(item);
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
struct DepthSnapshot {
    last_update_id: u64,
    bids: Vec<(String, String)>,
    asks: Vec<(String, String)>,
}

async fn fetch_symbols(
    client: &reqwest::Client,
    max_symbols: Option<usize>,
) -> anyhow::Result<Vec<String>> {
    let url = format!("{}/fapi/v1/exchangeInfo", BINANCE_FAPI);
    let info: ExchangeInfo = client.get(&url).send().await?.json().await?;
    let mut symbols: Vec<String> = info
        .symbols
        .into_iter()
        .filter(|s| s.contract_type == "PERPETUAL" && s.status == "TRADING")
        .map(|s| s.symbol)
        .collect();
    symbols.sort();
    if let Some(max) = max_symbols {
        symbols.truncate(max);
    }
    Ok(symbols)
}

async fn fetch_snapshot(
    client: &reqwest::Client,
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

    let url = format!("{}/fapi/v1/depth", BINANCE_FAPI);
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
    symbol: String,
    books: Arc<Mutex<HashMap<String, Book>>>,
    snap_lock: Arc<Mutex<f64>>,
    snap_count: Arc<AtomicU64>,
) {
    let mut delay = 1.0;
    loop {
        {
            let books = books.lock().await;
            if books.get(&symbol).map_or(true, |b| b.state != BookState::Buffering) {
                return;
            }
        }

        match fetch_snapshot(&client, &symbol, &snap_lock, &snap_count).await {
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

async fn run_shard(
    shard_id: usize,
    symbols: Vec<String>,
    books: Arc<Mutex<HashMap<String, Book>>>,
    client: reqwest::Client,
    bbo_tx: mpsc::Sender<BboEvent>,
    book_tx: mpsc::Sender<BookEvent>,
    shard_infos: Arc<Mutex<Vec<ShardInfo>>>,
    snap_lock: Arc<Mutex<f64>>,
    snap_count: Arc<AtomicU64>,
    bbo_count: Arc<AtomicU64>,
    book_count: Arc<AtomicU64>,
) {
    let mut reconnect_delay = RECONNECT_BASE;
    let mut sync_handles: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();

    let request_sync = |sym: &str, sync_handles: &mut HashMap<String, tokio::task::JoinHandle<()>>| {
        if let Some(h) = sync_handles.remove(sym) {
            h.abort();
        }
        let handle = tokio::spawn(sync_book(
            client.clone(),
            sym.to_string(),
            books.clone(),
            snap_lock.clone(),
            snap_count.clone(),
        ));
        sync_handles.insert(sym.to_string(), handle);
    };

    loop {
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

        let mut stream_names = Vec::new();
        for sym in &symbols {
            let s = sym.to_lowercase();
            stream_names.push(format!("{}@depth@100ms", s));
            stream_names.push(format!("{}@bookTicker", s));
        }

        info!("Shard {}: connecting ({} symbols)", shard_id, symbols.len());

        match connect_async(BINANCE_WS).await {
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

// === HEALTH ===

async fn health_handler(State(state): State<AppState>) -> Json<Value> {
    let books = state.books.lock().await;
    let infos = state.shard_infos.lock().await;
    let connected = infos.iter().filter(|s| s.connected).count();
    let live = books.values().filter(|b| b.state == BookState::Live).count();
    let total = books.len();
    let healthy = connected == infos.len() && live > total / 2;
    Json(serde_json::json!({
        "healthy": healthy,
        "uptime_s": state.start_time.elapsed().as_secs(),
        "shards": format!("{}/{} connected", connected, infos.len()),
        "books": format!("{}/{} live", live, total),
    }))
}

async fn stats_handler(State(state): State<AppState>) -> Json<Value> {
    let books = state.books.lock().await;
    let infos = state.shard_infos.lock().await;
    let mut states: HashMap<String, usize> = HashMap::new();
    for b in books.values() {
        let key = format!("{:?}", b.state).to_lowercase();
        *states.entry(key).or_default() += 1;
    }
    let latencies: Vec<f64> = infos
        .iter()
        .filter(|s| s.latency_ms > 0.0)
        .map(|s| s.latency_ms)
        .collect();
    let max_lat = latencies.iter().cloned().fold(0.0_f64, f64::max);

    Json(serde_json::json!({
        "uptime_s": state.start_time.elapsed().as_secs(),
        "book_states": states,
        "shards": infos.iter().map(|s| serde_json::json!({
            "id": s.id,
            "connected": s.connected,
            "msg_count": s.msg_count,
            "last_msg_time": s.last_msg_time,
            "latency_ms": s.latency_ms,
        })).collect::<Vec<_>>(),
        "bbo_published": state.bbo_count.load(Ordering::Relaxed),
        "book_published": state.book_count.load(Ordering::Relaxed),
        "snapshots": state.snap_count.load(Ordering::Relaxed),
        "total_gaps": books.values().map(|b| b.gap_count).sum::<u64>(),
        "total_crossed": books.values().map(|b| b.crossed_count).sum::<u64>(),
        "total_snapshots_per_book": books.values().map(|b| b.snapshot_count).sum::<u64>(),
        "max_latency_ms": max_lat,
    }))
}

// === MONITORING ===

async fn log_stats(
    books: Arc<Mutex<HashMap<String, Book>>>,
    shard_infos: Arc<Mutex<Vec<ShardInfo>>>,
    bbo_count: Arc<AtomicU64>,
    book_count: Arc<AtomicU64>,
    snap_count: Arc<AtomicU64>,
) {
    loop {
        sleep(Duration::from_secs(STATS_INTERVAL)).await;
        let books = books.lock().await;
        let infos = shard_infos.lock().await;
        let live = books.values().filter(|b| b.state == BookState::Live).count();
        let buffering = books
            .values()
            .filter(|b| b.state == BookState::Buffering)
            .count();
        let connected = infos.iter().filter(|s| s.connected).count();
        let total_msgs: u64 = infos.iter().map(|s| s.msg_count).sum();
        let max_lat = infos
            .iter()
            .filter(|s| s.latency_ms > 0.0)
            .map(|s| s.latency_ms)
            .fold(0.0_f64, f64::max);
        let total_gaps: u64 = books.values().map(|b| b.gap_count).sum();
        let total_crossed: u64 = books.values().map(|b| b.crossed_count).sum();

        info!(
            "shards={}/{} | books: {} live, {} buffering | msgs={} | \
             pub: {} bbo, {} book | snaps={} | latency={:.0}ms | gaps={} crossed={}",
            connected,
            infos.len(),
            live,
            buffering,
            total_msgs,
            bbo_count.load(Ordering::Relaxed),
            book_count.load(Ordering::Relaxed),
            snap_count.load(Ordering::Relaxed),
            max_lat,
            total_gaps,
            total_crossed,
        );
    }
}

async fn check_stale(books: Arc<Mutex<HashMap<String, Book>>>) {
    loop {
        sleep(Duration::from_secs_f64(STALE_THRESHOLD / 2.0)).await;
        let now = now_secs();
        let mut books = books.lock().await;
        for book in books.values_mut() {
            if book.state == BookState::Live && (now - book.last_update_time) > STALE_THRESHOLD {
                warn!(
                    "{}: stale for {:.0}s, resetting",
                    book.symbol,
                    now - book.last_update_time
                );
                book.reset();
            }
        }
    }
}

// === MAIN ===

pub async fn run(max_symbols: Option<usize>, port: u16, ws_port: u16) -> anyhow::Result<()> {
    info!("Starting Binance USD-M Futures Feed Handler");

    let client = reqwest::Client::new();
    let symbols = fetch_symbols(&client, max_symbols).await?;
    info!("Found {} perpetual symbols", symbols.len());

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

    let mut shard_infos = Vec::new();
    for (i, chunk) in symbols.chunks(MAX_SYMBOLS_PER_SHARD).enumerate() {
        shard_infos.push(ShardInfo {
            id: i,
            connected: false,
            msg_count: 0,
            last_msg_time: 0.0,
            latency_ms: 0.0,
        });
        let syms = chunk.to_vec();
        tokio::spawn(run_shard(
            i,
            syms,
            books.clone(),
            client.clone(),
            bbo_tx.clone(),
            book_tx.clone(),
            Arc::new(Mutex::new(shard_infos.clone())),
            snap_lock.clone(),
            snap_count.clone(),
            bbo_count.clone(),
            book_count.clone(),
        ));
    }
    let shard_infos = Arc::new(Mutex::new(shard_infos));
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
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    info!("Health server on :{}", port);
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });

    publisher::start_ws_server(ws_port).await?;

    tokio::spawn(log_stats(
        books.clone(),
        shard_infos.clone(),
        bbo_count.clone(),
        book_count.clone(),
        snap_count.clone(),
    ));
    tokio::spawn(check_stale(books.clone()));

    // Demo consumer
    let demo_bbo_rx = publisher::subscribe("bbo".to_string(), None).await;
    tokio::spawn(async move {
        let mut rx = demo_bbo_rx;
        let mut count = 0u64;
        while let Some(msg) = rx.recv().await {
            count += 1;
            if count % 5000 == 0 {
                info!("CONSUMER | msg #{} | {:?}", count, msg);
            }
        }
    });

    // Start dispatcher
    publisher::run_dispatcher(bbo_rx, book_rx).await;

    Ok(())
}

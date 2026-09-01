//! End-to-end reconnect / resync test against a mock Binance venue.
//!
//! Until now the reconnect path was unit-tested at the `Book` level only: nothing
//! ever exercised run_shard's "drop the socket, reset every book, re-subscribe,
//! re-snapshot" loop, which is the code that runs every time Binance cycles a
//! stream host. This drives the real handler (real WS client, real REST client,
//! real snapshot pacing) against a scripted venue and asserts the two properties
//! that matter:
//!
//!   * a mid-stream sequence gap costs exactly one gap and self-heals, and
//!   * a dropped connection resyncs via snapshot WITHOUT being reported as a
//!     sequence gap -- a reconnect that surfaced as a gap would mean the book
//!     went live on a stale `last_update_id`.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::{IntoResponse, Json};
use axum::routing::get;
use serde_json::{json, Value};

/// Depth-diff sequence width. Binance's [U, u] windows are contiguous per symbol
/// (u of one event == pu of the next), which is what the handler chains on.
const SEQ_STEP: u64 = 10;
const EVENT_INTERVAL_MS: u64 = 20;
const SYMBOL: &str = "TESTUSDT";

#[derive(Default)]
struct Venue {
    /// `u` of the most recently emitted depth event; also the snapshot's lastUpdateId.
    last_u: AtomicU64,
    /// Bumped to hang up every live socket, simulating a Binance stream-host cycle.
    generation: AtomicU64,
    /// Makes the next emitted event skip the sequence, forcing a pu mismatch.
    inject_gap: AtomicBool,
    snapshots_served: AtomicU64,
}

impl Venue {
    /// Emit the next contiguous depth event, or a deliberately discontinuous one.
    fn next_depth(&self) -> Value {
        let prev = self.last_u.load(Ordering::Relaxed);
        let skip = if self.inject_gap.swap(false, Ordering::Relaxed) {
            SEQ_STEP * 10
        } else {
            0
        };
        let u = prev + skip + SEQ_STEP;
        self.last_u.store(u, Ordering::Relaxed);
        json!({
            "e": "depthUpdate", "E": now_ms(), "s": SYMBOL,
            "U": prev + skip + 1, "u": u, "pu": prev + skip,
            "b": [["100", "1"], ["99", "2"]],
            "a": [["101", "1"], ["102", "3"]],
        })
    }

    fn book_ticker(&self) -> Value {
        json!({
            "e": "bookTicker", "E": now_ms(), "s": SYMBOL,
            "b": "100", "B": "1", "a": "101", "A": "1",
        })
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// === MOCK VENUE HTTP + WS ===

async fn ticker_24hr() -> Json<Value> {
    Json(json!([{"symbol": SYMBOL, "quoteVolume": "1000000"}]))
}

async fn exchange_info() -> Json<Value> {
    Json(json!({"symbols": [
        {"symbol": SYMBOL, "contractType": "PERPETUAL", "status": "TRADING"},
        // A non-perp and a halted perp, so the filter is exercised too.
        {"symbol": "TESTUSDT_260327", "contractType": "CURRENT_QUARTER", "status": "TRADING"},
        {"symbol": "HALTEDUSDT", "contractType": "PERPETUAL", "status": "SETTLING"},
    ]}))
}

async fn depth(State(venue): State<Arc<Venue>>) -> Json<Value> {
    venue.snapshots_served.fetch_add(1, Ordering::Relaxed);
    Json(json!({
        "lastUpdateId": venue.last_u.load(Ordering::Relaxed),
        "bids": [["100", "1"], ["99", "2"]],
        "asks": [["101", "1"], ["102", "3"]],
    }))
}

async fn ws_upgrade(ws: WebSocketUpgrade, State(venue): State<Arc<Venue>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| serve_stream(socket, venue))
}

async fn serve_stream(mut socket: WebSocket, venue: Arc<Venue>) {
    let my_generation = venue.generation.load(Ordering::Relaxed);
    let mut subscribed = false;
    let mut tick = tokio::time::interval(Duration::from_millis(EVENT_INTERVAL_MS));

    loop {
        if venue.generation.load(Ordering::Relaxed) != my_generation {
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
        tokio::select! {
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Text(_))) => {
                    // Any control frame here is a SUBSCRIBE batch; ack it the way
                    // Binance does so the handler's result-filter is exercised.
                    subscribed = true;
                    if socket.send(Message::Text(r#"{"result":null,"id":1}"#.into())).await.is_err() {
                        return;
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => return,
            },
            _ = tick.tick() => {
                if !subscribed {
                    continue;
                }
                let depth = venue.next_depth().to_string();
                let ticker = venue.book_ticker().to_string();
                if socket.send(Message::Text(depth.into())).await.is_err() {
                    return;
                }
                if socket.send(Message::Text(ticker.into())).await.is_err() {
                    return;
                }
            }
        }
    }
}

async fn start_venue(venue: Arc<Venue>) -> SocketAddr {
    let app = axum::Router::new()
        .route("/fapi/v1/exchangeInfo", get(exchange_info))
        .route("/fapi/v1/ticker/24hr", get(ticker_24hr))
        .route("/fapi/v1/depth", get(depth))
        .route("/ws", get(ws_upgrade))
        .with_state(venue);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    addr
}

// === HARNESS ===

/// Grab a free port and release it. The handler binds it microseconds later; a
/// fixed port would collide with whatever else is on the machine.
async fn free_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    port
}

struct Handler {
    client: reqwest::Client,
    stats_url: String,
    health_url: String,
}

impl Handler {
    async fn stats(&self) -> Value {
        self.client
            .get(&self.stats_url)
            .send()
            .await
            .expect("stats unreachable")
            .json()
            .await
            .expect("stats not JSON")
    }

    async fn health(&self) -> (u16, Value) {
        let resp = self.client.get(&self.health_url).send().await.expect("health unreachable");
        let status = resp.status().as_u16();
        (status, resp.json().await.expect("health not JSON"))
    }

    async fn live_books(&self) -> u64 {
        self.stats().await["book_states"]["live"].as_u64().unwrap_or(0)
    }

    async fn total_gaps(&self) -> u64 {
        self.stats().await["total_gaps"].as_u64().unwrap()
    }

    async fn snapshots(&self) -> u64 {
        self.stats().await["snapshots"].as_u64().unwrap()
    }
}

/// Poll `check` until it returns true. Panics with `what` on timeout so a
/// failure names the property that never held rather than "assertion failed".
async fn eventually<F, Fut>(what: &str, timeout: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out after {:?} waiting for: {}", timeout, what);
}

#[tokio::test(flavor = "multi_thread")]
async fn reconnect_and_gap_recovery_resync_the_book() {
    let venue = Arc::new(Venue::default());
    let venue_addr = start_venue(venue.clone()).await;
    let http_port = free_port().await;
    let pub_port = free_port().await;

    tokio::spawn(binance_feed_handler::feed_handler::run(
        binance_feed_handler::feed_handler::Config {
            max_symbols: None,
            symbols: Vec::new(),
            port: http_port,
            ws_port: pub_port,
            ws_base: format!("ws://{}/ws", venue_addr),
            fapi_base: format!("http://{}", venue_addr),
            snapshot_rate: binance_feed_handler::feed_handler::SNAPSHOT_RATE_DEFAULT,
        },
    ));

    let h = Handler {
        client: reqwest::Client::new(),
        stats_url: format!("http://127.0.0.1:{}/stats", http_port),
        health_url: format!("http://127.0.0.1:{}/health", http_port),
    };
    eventually("the health server to bind", Duration::from_secs(10), || async {
        h.client.get(&h.stats_url).send().await.is_ok()
    })
    .await;

    // --- Phase 1: cold start ---
    eventually("the book to reach live from a cold start", Duration::from_secs(15), || async {
        h.live_books().await == 1
    })
    .await;

    let stats = h.stats().await;
    assert_eq!(
        stats["book_states"],
        json!({"live": 1}),
        "only the TRADING perpetual should be tracked (got {})",
        stats["book_states"]
    );
    assert_eq!(h.total_gaps().await, 0, "cold start must not report a sequence gap");
    let (health_status, health) = h.health().await;
    assert_eq!(health_status, 200, "a fully live handler must answer /health 200");
    assert_eq!(health["healthy"], json!(true));

    // --- Phase 2: a mid-stream sequence gap heals via snapshot ---
    let snapshots_before_gap = h.snapshots().await;
    venue.inject_gap.store(true, Ordering::Relaxed);

    eventually("the injected gap to be detected", Duration::from_secs(10), || async {
        h.total_gaps().await == 1
    })
    .await;
    eventually("the book to go live again after the gap", Duration::from_secs(15), || async {
        h.live_books().await == 1
    })
    .await;
    assert!(
        h.snapshots().await > snapshots_before_gap,
        "gap recovery must fetch a fresh snapshot"
    );
    assert_eq!(h.total_gaps().await, 1, "one injected gap must cost exactly one gap");

    // --- Phase 3: a dropped connection resyncs without surfacing as a gap ---
    let snapshots_before_drop = h.snapshots().await;
    venue.generation.fetch_add(1, Ordering::Relaxed);

    eventually("the shard to notice the disconnect", Duration::from_secs(10), || async {
        h.stats().await["shards"][0]["connected"] == json!(false)
    })
    .await;
    // The book must stop claiming to be live immediately, not after the backoff:
    // reconnect can take up to 30s and its depth is frozen the whole time.
    eventually("the book to be un-synced on disconnect", Duration::from_secs(2), || async {
        h.live_books().await == 0
    })
    .await;
    let (down_status, down_health) = h.health().await;
    assert_eq!(down_status, 503, "a disconnected handler must answer /health 503");
    assert_eq!(down_health["healthy"], json!(false));

    // Reconnect is 1s base + up to 1s jitter, then re-subscribe (250ms) and
    // re-snapshot at 3/s.
    eventually("the book to reach live again after reconnect", Duration::from_secs(30), || async {
        h.live_books().await == 1
    })
    .await;

    assert!(
        h.snapshots().await > snapshots_before_drop,
        "reconnect must re-snapshot, not resume on a stale last_update_id"
    );
    assert_eq!(
        h.total_gaps().await,
        1,
        "reconnect resyncs from a snapshot, so it must not be counted as a sequence gap"
    );
    assert_eq!(h.stats().await["total_crossed"], json!(0), "no crossed book expected");
    assert_eq!(
        h.stats().await["dropped"]["book_buffer"],
        json!(0),
        "the resync buffer must not overflow in a 1-symbol run"
    );
    let (up_status, _) = h.health().await;
    assert_eq!(up_status, 200, "/health must recover to 200 after the resync");

    // --- /metrics reflects the same run ---
    let metrics = h
        .client
        .get(format!("http://127.0.0.1:{}/metrics", http_port))
        .send()
        .await
        .expect("metrics unreachable");
    assert!(metrics
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("text/plain"));
    let body = metrics.text().await.unwrap();
    for line in [
        "bfh_up 1",
        "bfh_books{state=\"live\"} 1",
        "bfh_sequence_gaps_total 1",
        "bfh_crossed_books_total 0",
        "bfh_dropped_total{queue=\"resync_buffer\"} 0",
        "bfh_shard_messages_total{shard=\"0\"}",
    ] {
        assert!(body.contains(line), "/metrics missing {:?}:\n{}", line, body);
    }
    // Prometheus requires HELP/TYPE exactly once per metric family.
    assert_eq!(body.matches("# TYPE bfh_up ").count(), 1);
}

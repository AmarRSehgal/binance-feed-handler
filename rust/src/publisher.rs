/// Pub/sub dispatcher + WebSocket server for feed handler consumers.
///
/// Fans out BBO and book updates from feed handler output channels to
/// multiple subscribers, with optional per-symbol and per-stream filtering.
use std::collections::HashSet;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use axum::extract::WebSocketUpgrade;
use axum::response::IntoResponse;
use axum::routing::get;
use log::info;
use serde_json::Value;
use tokio::sync::{mpsc, Mutex};

use crate::feed_handler::{BboEvent, BookEvent};

// === CONFIGURATION ===

const SUBSCRIBER_QUEUE_MAX: usize = 10_000;

// === TYPES ===

#[derive(Debug, Clone, PartialEq)]
enum StreamFilter {
    All,
    Bbo,
    Book,
}

struct Subscriber {
    tx: mpsc::Sender<Value>,
    stream: StreamFilter,
    symbols: Option<HashSet<String>>,
}

// === STATE ===

static SUBSCRIBERS: std::sync::LazyLock<Arc<Mutex<Vec<Subscriber>>>> =
    std::sync::LazyLock::new(|| Arc::new(Mutex::new(Vec::new())));

// === PUB/SUB ===

pub async fn subscribe(
    stream: String,
    symbols: Option<Vec<String>>,
) -> mpsc::Receiver<Value> {
    let (tx, rx) = mpsc::channel(SUBSCRIBER_QUEUE_MAX);
    let filter = match stream.as_str() {
        "bbo" => StreamFilter::Bbo,
        "book" => StreamFilter::Book,
        _ => StreamFilter::All,
    };
    let sym_set = symbols.map(|s| s.into_iter().collect::<HashSet<String>>());
    let mut subs = SUBSCRIBERS.lock().await;
    subs.push(Subscriber {
        tx,
        stream: filter,
        symbols: sym_set,
    });
    info!("Subscriber added ({} total)", subs.len());
    rx
}

pub async fn unsubscribe(target_tx: &mpsc::Sender<Value>) {
    let mut subs = SUBSCRIBERS.lock().await;
    subs.retain(|s| !s.tx.same_channel(target_tx));
    info!("Subscriber removed ({} remaining)", subs.len());
}

fn put_value(tx: &mpsc::Sender<Value>, msg: Value) {
    let _ = tx.try_send(msg);
}

async fn publish(msg: Value, stream: &StreamFilter) {
    let symbol = msg.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
    let subs = SUBSCRIBERS.lock().await;
    for sub in subs.iter() {
        if sub.stream != *stream && sub.stream != StreamFilter::All {
            continue;
        }
        if let Some(ref symbols) = sub.symbols {
            if !symbols.contains(symbol) {
                continue;
            }
        }
        put_value(&sub.tx, msg.clone());
    }
}

// === DISPATCHER ===

pub async fn run_dispatcher(
    mut bbo_rx: mpsc::Receiver<BboEvent>,
    mut book_rx: mpsc::Receiver<BookEvent>,
) {
    let bbo_handle = tokio::spawn(async move {
        while let Some(event) = bbo_rx.recv().await {
            let msg = serde_json::to_value(&event).unwrap_or_default();
            publish(msg, &StreamFilter::Bbo).await;
        }
    });

    let book_handle = tokio::spawn(async move {
        while let Some(event) = book_rx.recv().await {
            let msg = serde_json::to_value(&event).unwrap_or_default();
            publish(msg, &StreamFilter::Book).await;
        }
    });

    let _ = tokio::join!(bbo_handle, book_handle);
}

// === WEBSOCKET SERVER ===

async fn ws_handler(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(handle_ws_client)
}

async fn handle_ws_client(mut socket: WebSocket) {
    info!("WebSocket client connected");
    let mut sub_tx: Option<mpsc::Sender<Value>> = None;
    let mut stream_handle: Option<tokio::task::JoinHandle<()>> = None;

    loop {
        tokio::select! {
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let parsed: Value = match serde_json::from_str(&text) {
                            Ok(v) => v,
                            Err(_) => {
                                let _ = socket.send(Message::Text(r#"{"error":"invalid JSON"}"#.into())).await;
                                continue;
                            }
                        };

                        let action = parsed.get("action").and_then(|v| v.as_str()).unwrap_or("");

                        if action == "subscribe" {
                            if let Some(h) = stream_handle.take() { h.abort(); }
                            if let Some(ref tx) = sub_tx {
                                unsubscribe(tx).await;
                            }

                            let stream = parsed.get("stream").and_then(|v| v.as_str()).unwrap_or("all").to_string();
                            let symbols: Option<Vec<String>> = parsed.get("symbols").and_then(|v| {
                                v.as_array().map(|a| a.iter().filter_map(|s| s.as_str().map(String::from)).collect())
                            });

                            let mut rx = subscribe(stream.clone(), symbols.clone()).await;
                            sub_tx = None;

                            let confirmation = serde_json::json!({
                                "status": "subscribed",
                                "stream": stream,
                                "symbols": symbols,
                            });
                            let _ = socket.send(Message::Text(confirmation.to_string().into())).await;

                            let (out_tx, mut out_rx) = mpsc::channel::<String>(1000);
                            stream_handle = Some(tokio::spawn(async move {
                                while let Some(val) = rx.recv().await {
                                    let json_str = serde_json::to_string(&val).unwrap_or_default();
                                    if out_tx.send(json_str).await.is_err() {
                                        break;
                                    }
                                }
                            }));

                            let mut streaming = true;
                            while streaming {
                                tokio::select! {
                                    Some(out_msg) = out_rx.recv() => {
                                        if socket.send(Message::Text(out_msg.into())).await.is_err() {
                                            streaming = false;
                                        }
                                    }
                                    msg = socket.recv() => {
                                        match msg {
                                            Some(Ok(Message::Text(t))) => {
                                                if let Ok(v) = serde_json::from_str::<Value>(&t) {
                                                    if v.get("action").and_then(|a| a.as_str()) == Some("unsubscribe") {
                                                        if let Some(h) = stream_handle.take() { h.abort(); }
                                                        let _ = socket.send(Message::Text(r#"{"status":"unsubscribed"}"#.into())).await;
                                                        streaming = false;
                                                    }
                                                }
                                            }
                                            Some(Ok(Message::Close(_))) | None => {
                                                streaming = false;
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
        }
    }

    if let Some(h) = stream_handle.take() {
        h.abort();
    }
    info!("WebSocket client disconnected");
}

pub async fn start_ws_server(port: u16) -> anyhow::Result<()> {
    let app = axum::Router::new().route("/ws", get(ws_handler));
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    info!("WebSocket server on :{}/ws", port);
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    Ok(())
}

//! Binance USD-M Futures feed handler.
//!
//! Exposed as a library so `tests/` can drive the whole handler against a mock
//! venue; `src/main.rs` is a thin CLI over `feed_handler::run`.
pub mod book;
pub mod feed_handler;
pub mod publisher;

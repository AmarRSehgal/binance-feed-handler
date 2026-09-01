use clap::Parser;

mod book;
mod feed_handler;
mod publisher;

#[derive(Parser)]
#[command(name = "binance-feed-handler")]
#[command(about = "Binance USD-M Futures Feed Handler")]
struct Args {
    #[arg(
        long,
        help = "Cap symbol count, taking the most liquid by 24h quote volume (default: all perps)"
    )]
    max_symbols: Option<usize>,

    #[arg(
        long,
        value_delimiter = ',',
        help = "Track exactly these symbols, e.g. BTCUSDT,ETHUSDT. Overrides --max-symbols"
    )]
    symbols: Vec<String>,

    #[arg(long, default_value = "8080", help = "Health server port")]
    port: u16,

    #[arg(long, default_value = "8081", help = "WebSocket server port")]
    ws_port: u16,

    #[arg(
        long,
        default_value = feed_handler::BINANCE_WS_DEFAULT,
        help = "Binance stream base URL. Switch to wss://fstream.binance.com/public once Binance actually retires the legacy /ws path"
    )]
    ws_base: String,

    #[arg(long, help = "Run cross-language comparison scenarios and print JSON")]
    test_scenarios: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if args.test_scenarios {
        let output = scenarios::run_all();
        println!("{}", serde_json::to_string_pretty(&output)?);
        return Ok(());
    }

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    feed_handler::run(
        args.max_symbols,
        args.symbols,
        args.port,
        args.ws_port,
        args.ws_base,
    )
    .await
}

mod scenarios {
    use crate::book::{Book, parse_levels};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use serde_json::{json, Value};

    fn ml(pairs: &[(&str, &str)]) -> Vec<(Decimal, Decimal)> {
        pairs.iter().map(|(p, q)| (p.parse().unwrap(), q.parse().unwrap())).collect()
    }

    fn action_str(a: crate::book::Action) -> Value {
        match a {
            crate::book::Action::Publish => json!("publish"),
            crate::book::Action::NeedSnapshot => json!("need_snapshot"),
            crate::book::Action::None_ => Value::Null,
        }
    }

    fn state_str(b: &Book) -> &'static str {
        match b.state {
            crate::book::BookState::Uninitialized => "uninitialized",
            crate::book::BookState::Buffering => "buffering",
            crate::book::BookState::Live => "live",
        }
    }

    fn scenario_basic_lifecycle() -> Value {
        let mut b = Book::new("BTCUSDT".to_string());
        let mut results = Vec::new();

        let r = b.on_depth(1, 10, 0, vec![], vec![]);
        results.push(json!({"step": "first_depth", "action": action_str(r), "state": state_str(&b)}));

        let r = b.on_depth(11, 20, 10, vec![], vec![]);
        results.push(json!({"step": "buffer_depth", "action": action_str(r), "state": state_str(&b)}));

        let bids = parse_levels(&[("64000.00".into(), "1.5".into()), ("63999.00".into(), "2.0".into())]);
        let asks = parse_levels(&[("64001.00".into(), "1.0".into()), ("64002.00".into(), "3.0".into())]);
        let ok = b.on_snapshot(100, &bids, &asks);
        results.push(json!({
            "step": "snapshot",
            "ok": ok,
            "state": state_str(&b),
            "bid_count": b.bids.len(),
            "ask_count": b.asks.len(),
            "last_update_id": b.last_update_id,
        }));

        let r = b.on_depth(101, 110, 99, ml(&[("63998.00", "1.0")]), vec![]);
        let (top_bids, top_asks) = b.top_levels(3);
        results.push(json!({
            "step": "first_live_depth",
            "action": action_str(r),
            "state": state_str(&b),
            "top_bids": top_bids.iter().map(|(p,q)| json!([p.to_string(), q.to_string()])).collect::<Vec<_>>(),
            "top_asks": top_asks.iter().map(|(p,q)| json!([p.to_string(), q.to_string()])).collect::<Vec<_>>(),
        }));

        let r = b.on_depth(111, 120, 110,
            ml(&[("64000.00", "0")]),
            ml(&[("64001.50", "0.5")]));
        results.push(json!({
            "step": "apply_diff",
            "action": action_str(r),
            "has_64000": b.bids.contains_key(&dec!(64000.00)),
            "has_64001_50": b.asks.contains_key(&dec!(64001.50)),
        }));

        let r = b.on_depth(500, 510, 499, vec![], vec![]);
        results.push(json!({
            "step": "sequence_gap",
            "action": action_str(r),
            "state": state_str(&b),
            "gap_count": b.gap_count,
        }));

        json!(results)
    }

    fn scenario_crossed_book() -> Value {
        let mut b = Book::new("ETHUSDT".to_string());
        b.on_depth(1, 10, 0, vec![], vec![]);
        b.on_snapshot(100, &ml(&[("3000.00", "1.0")]), &ml(&[("3001.00", "1.0")]));
        b.on_depth(101, 110, 99, vec![], vec![]);

        let r = b.on_depth(111, 120, 110, ml(&[("3002.00", "1.0")]), vec![]);
        json!({
            "action": action_str(r),
            "state": state_str(&b),
            "crossed_count": b.crossed_count,
        })
    }

    fn scenario_bbo_divergence() -> Value {
        let mut b = Book::new("SOLUSDT".to_string());
        b.on_depth(1, 10, 0, vec![], vec![]);
        b.on_snapshot(100, &ml(&[("150.00", "10.0")]), &ml(&[("151.00", "10.0")]));
        b.on_depth(101, 110, 99, vec![], vec![]);

        b.set_ticker_bbo(dec!(149.00), dec!(152.00));

        let mut uid = 110u64;
        let mut mismatch_counts = Vec::new();
        for _ in 0..5 {
            b.on_depth(uid + 1, uid + 10, uid, vec![], vec![]);
            uid += 10;
            // We can't directly access bbo_mismatch_count (private), so we infer
            // from the state. But let's track based on state remaining live.
            mismatch_counts.push(if b.state == crate::book::BookState::Live { "live" } else { "buffering" });
        }

        b.set_ticker_bbo(dec!(150.00), dec!(151.00));
        b.on_depth(uid + 1, uid + 10, uid, vec![], vec![]);
        uid += 10;
        mismatch_counts.push(if b.state == crate::book::BookState::Live { "live" } else { "buffering" });

        b.set_ticker_bbo(dec!(149.00), dec!(152.00));
        let mut final_action = crate::book::Action::None_;
        for _ in 0..10 {
            final_action = b.on_depth(uid + 1, uid + 10, uid, vec![], vec![]);
            uid += 10;
            if b.state != crate::book::BookState::Live { break; }
        }

        json!({
            "all_live_during_warmup": mismatch_counts.iter().all(|s| *s == "live"),
            "final_action": action_str(final_action),
            "final_state": state_str(&b),
        })
    }

    fn scenario_reset_and_resync() -> Value {
        let mut b = Book::new("DOGEUSDT".to_string());
        b.on_depth(1, 10, 0, vec![], vec![]);
        b.on_snapshot(100, &ml(&[("0.10", "1000")]), &ml(&[("0.11", "1000")]));
        b.on_depth(101, 110, 99, vec![], vec![]);

        let pre_reset = json!({
            "state": state_str(&b),
            "bid_count": b.bids.len(),
            "last_update_id": b.last_update_id,
        });

        b.reset();

        let post_reset = json!({
            "state": state_str(&b),
            "bid_count": b.bids.len(),
            "last_update_id": b.last_update_id,
        });

        let r = b.on_depth(200, 210, 190, vec![], vec![]);

        json!({
            "pre_reset": pre_reset,
            "post_reset": post_reset,
            "after_depth_action": action_str(r),
            "after_depth_state": state_str(&b),
        })
    }

    fn scenario_top_levels_ordering() -> Value {
        let mut b = Book::new("AVAXUSDT".to_string());
        b.on_depth(1, 10, 0, vec![], vec![]);
        b.on_snapshot(100,
            &ml(&[("30.00", "1"), ("28.00", "2"), ("29.00", "3"), ("27.00", "4"), ("31.00", "5")]),
            &ml(&[("32.00", "1"), ("35.00", "2"), ("33.00", "3"), ("34.00", "4"), ("36.00", "5")]));

        let (top_bids, top_asks) = b.top_levels(3);
        json!({
            "top_3_bid_prices": top_bids.iter().map(|(p,_)| p.to_string()).collect::<Vec<_>>(),
            "top_3_ask_prices": top_asks.iter().map(|(p,_)| p.to_string()).collect::<Vec<_>>(),
        })
    }

    pub fn run_all() -> Value {
        json!({
            "basic_lifecycle": scenario_basic_lifecycle(),
            "crossed_book": scenario_crossed_book(),
            "bbo_divergence": scenario_bbo_divergence(),
            "reset_and_resync": scenario_reset_and_resync(),
            "top_levels_ordering": scenario_top_levels_ordering(),
        })
    }
}

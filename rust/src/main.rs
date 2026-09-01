use clap::Parser;

use binance_feed_handler::feed_handler;

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

    #[arg(
        long,
        default_value = feed_handler::BINANCE_FAPI_DEFAULT,
        help = "Binance REST base URL (exchangeInfo, depth snapshots)"
    )]
    fapi_base: String,

    #[arg(long, help = "Run cross-language comparison scenarios and print JSON")]
    test_scenarios: bool,

    #[arg(
        long,
        help = "Run the differential fuzz script for this seed and print a per-step JSON digest"
    )]
    test_fuzz: Option<u64>,

    #[arg(long, default_value = "2000", help = "Steps for --test-fuzz")]
    test_fuzz_steps: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if args.test_scenarios {
        let output = scenarios::run_all();
        println!("{}", serde_json::to_string_pretty(&output)?);
        return Ok(());
    }

    if let Some(seed) = args.test_fuzz {
        let output = fuzz::run(seed, args.test_fuzz_steps);
        println!("{}", serde_json::to_string(&output)?);
        return Ok(());
    }

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    feed_handler::run(feed_handler::Config {
        max_symbols: args.max_symbols,
        symbols: args.symbols,
        port: args.port,
        ws_port: args.ws_port,
        ws_base: args.ws_base,
        fapi_base: args.fapi_base,
    })
    .await
}

mod scenarios {
    use binance_feed_handler::book::{parse_levels, Action, Book, BookState};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use serde_json::{json, Value};

    fn ml(pairs: &[(&str, &str)]) -> Vec<(Decimal, Decimal)> {
        pairs.iter().map(|(p, q)| (p.parse().unwrap(), q.parse().unwrap())).collect()
    }

    fn action_str(a: Action) -> Value {
        match a {
            Action::Publish => json!("publish"),
            Action::NeedSnapshot => json!("need_snapshot"),
            Action::None_ => Value::Null,
        }
    }

    fn state_str(b: &Book) -> &'static str {
        match b.state {
            BookState::Uninitialized => "uninitialized",
            BookState::Buffering => "buffering",
            BookState::Live => "live",
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
            mismatch_counts.push(if b.state == BookState::Live { "live" } else { "buffering" });
        }

        b.set_ticker_bbo(dec!(150.00), dec!(151.00));
        b.on_depth(uid + 1, uid + 10, uid, vec![], vec![]);
        uid += 10;
        mismatch_counts.push(if b.state == BookState::Live { "live" } else { "buffering" });

        b.set_ticker_bbo(dec!(149.00), dec!(152.00));
        let mut final_action = Action::None_;
        for _ in 0..10 {
            final_action = b.on_depth(uid + 1, uid + 10, uid, vec![], vec![]);
            uid += 10;
            if b.state != BookState::Live { break; }
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

/// Differential fuzz driver. The generator below is a byte-for-byte mirror of
/// `tests/cross_language_comparison.py`'s: same LCG, same constants, same branch
/// thresholds, so both implementations see an identical event script for a given
/// seed and any behavioural divergence shows up as a digest mismatch.
///
/// Hand-written scenarios only cover the transitions someone thought to write
/// down. This covers the interleavings nobody would: a snapshot landing behind
/// the buffer, a reset mid-resync, a stale event straddling a gap.
mod fuzz {
    use binance_feed_handler::book::{Book, BookState};
    use rust_decimal::Decimal;
    use serde_json::{json, Value};

    const BID_PRICES: [&str; 3] = ["100", "99", "98"];
    const ASK_PRICES: [&str; 3] = ["101", "102", "103"];

    struct Lcg(u64);

    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0 >> 33
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn levels(rng: &mut Lcg, prices: &[&str; 3]) -> Vec<(Decimal, Decimal)> {
        let n = rng.below(3);
        (0..n)
            .map(|_| {
                let price = prices[rng.below(3) as usize];
                // A zero qty is a level delete -- the most common real diff.
                let qty = if rng.below(4) == 0 {
                    "0".to_string()
                } else {
                    (rng.below(9) + 1).to_string()
                };
                (price.parse().unwrap(), qty.parse().unwrap())
            })
            .collect()
    }

    fn state_str(b: &Book) -> &'static str {
        match b.state {
            BookState::Uninitialized => "uninitialized",
            BookState::Buffering => "buffering",
            BookState::Live => "live",
        }
    }

    fn digest(step: usize, op: &str, action: Option<&str>, b: &Book) -> Value {
        let (top_bids, top_asks) = b.top_levels(3);
        json!({
            "step": step,
            "op": op,
            "action": action,
            "state": state_str(b),
            "last_update_id": b.last_update_id,
            "bids": b.bids.len(),
            "asks": b.asks.len(),
            "top_bids": top_bids.iter().map(|(p, q)| json!([p.to_string(), q.to_string()])).collect::<Vec<_>>(),
            "top_asks": top_asks.iter().map(|(p, q)| json!([p.to_string(), q.to_string()])).collect::<Vec<_>>(),
            "gaps": b.gap_count,
            "crossed": b.crossed_count,
            "snapshots": b.snapshot_count,
            "unverified_bridges": b.unverified_bridge_count,
            "buffer_dropped": b.buffer_dropped,
        })
    }

    pub fn run(seed: u64, steps: usize) -> Value {
        let mut rng = Lcg(seed);
        let mut b = Book::new("FUZZUSDT".to_string());
        let mut seq: u64 = 1000;
        let mut out = Vec::with_capacity(steps);

        for step in 0..steps {
            let roll = rng.below(100);
            if roll < 55 || (65..78).contains(&roll) {
                // Contiguous diff, or one that deliberately skips the sequence.
                let skip = if roll < 55 { 0 } else { 10 * (1 + rng.below(5)) };
                let bids = levels(&mut rng, &BID_PRICES);
                let asks = levels(&mut rng, &ASK_PRICES);
                let (big_u, u, pu) = (seq + skip + 1, seq + skip + 10, seq + skip);
                seq = u;
                let action = b.on_depth(big_u, u, pu, bids, asks);
                let name = match action {
                    binance_feed_handler::book::Action::Publish => Some("publish"),
                    binance_feed_handler::book::Action::NeedSnapshot => Some("need_snapshot"),
                    binance_feed_handler::book::Action::None_ => None,
                };
                out.push(digest(step, if skip == 0 { "depth" } else { "depth_gap" }, name, &b));
            } else if roll < 88 {
                let lui = match rng.below(3) {
                    0 => seq,
                    1 => seq + 10 * (1 + rng.below(3)),
                    _ => seq.saturating_sub(10 * (1 + rng.below(3))),
                };
                let bids = levels(&mut rng, &BID_PRICES);
                let asks = levels(&mut rng, &ASK_PRICES);
                let ok = b.on_snapshot(lui, &bids, &asks);
                out.push(digest(step, "snapshot", Some(if ok { "live" } else { "retry" }), &b));
            } else if roll < 93 {
                // A stale replay: u is far behind last_update_id.
                let action = b.on_depth(1, 2, 0, vec![], vec![]);
                let name = match action {
                    binance_feed_handler::book::Action::Publish => Some("publish"),
                    binance_feed_handler::book::Action::NeedSnapshot => Some("need_snapshot"),
                    binance_feed_handler::book::Action::None_ => None,
                };
                out.push(digest(step, "stale", name, &b));
            } else if roll < 96 {
                b.reset();
                out.push(digest(step, "reset", None, &b));
            } else if roll < 98 {
                b.mark_for_resync();
                out.push(digest(step, "mark_for_resync", None, &b));
            } else {
                let bid: Decimal = BID_PRICES[rng.below(3) as usize].parse().unwrap();
                let ask: Decimal = ASK_PRICES[rng.below(3) as usize].parse().unwrap();
                b.set_ticker_bbo(bid, ask);
                out.push(digest(step, "ticker", None, &b));
            }
        }

        json!({"seed": seed, "steps": steps, "digest": out})
    }
}

"""Cross-language comparison: verifies Python and Rust book implementations
produce identical results for the same input sequences.

Usage:
    # Python-only (prints expected outputs):
    python tests/cross_language_comparison.py

    # Full comparison (requires `cd rust && cargo build` first):
    python tests/cross_language_comparison.py --rust-binary rust/target/debug/binance-feed-handler
"""
import sys
import os
import json
import subprocess
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))

from decimal import Decimal
from book import Book, parse_levels


def scenario_basic_lifecycle():
    """Full lifecycle: uninitialized -> buffering -> live -> gap -> buffering."""
    b = Book("BTCUSDT")
    results = []

    r = b.on_depth(1, 10, 0, [], [])
    results.append({"step": "first_depth", "action": r, "state": b.state})

    r = b.on_depth(11, 20, 10, [], [])
    results.append({"step": "buffer_depth", "action": r, "state": b.state})

    bids = parse_levels([["64000.00", "1.5"], ["63999.00", "2.0"]])
    asks = parse_levels([["64001.00", "1.0"], ["64002.00", "3.0"]])
    ok = b.on_snapshot(100, bids, asks)
    results.append({
        "step": "snapshot",
        "ok": ok,
        "state": b.state,
        "bid_count": len(b.bids),
        "ask_count": len(b.asks),
        "last_update_id": b.last_update_id,
    })

    r = b.on_depth(101, 110, 99, parse_levels([["63998.00", "1.0"]]), [])
    top_bids, top_asks = b.top_levels(3)
    results.append({
        "step": "first_live_depth",
        "action": r,
        "state": b.state,
        "top_bids": [[str(p), str(q)] for p, q in top_bids],
        "top_asks": [[str(p), str(q)] for p, q in top_asks],
    })

    r = b.on_depth(111, 120, 110,
                   parse_levels([["64000.00", "0"]]),
                   parse_levels([["64001.50", "0.5"]]))
    results.append({
        "step": "apply_diff",
        "action": r,
        "has_64000": Decimal("64000.00") in b.bids,
        "has_64001_50": Decimal("64001.50") in b.asks,
    })

    r = b.on_depth(500, 510, 499, [], [])
    results.append({
        "step": "sequence_gap",
        "action": r,
        "state": b.state,
        "gap_count": b.gap_count,
    })

    return results


def scenario_crossed_book():
    """Integrity check: crossed book triggers re-snapshot."""
    b = Book("ETHUSDT")
    b.on_depth(1, 10, 0, [], [])
    b.on_snapshot(100,
                  parse_levels([["3000.00", "1.0"]]),
                  parse_levels([["3001.00", "1.0"]]))
    b.on_depth(101, 110, 99, [], [])

    r = b.on_depth(111, 120, 110, parse_levels([["3002.00", "1.0"]]), [])
    return {
        "action": r,
        "state": b.state,
        "crossed_count": b.crossed_count,
    }


def scenario_bbo_divergence():
    """BBO cross-validation: mismatches accumulate, match resets, threshold triggers."""
    b = Book("SOLUSDT")
    b.on_depth(1, 10, 0, [], [])
    b.on_snapshot(100,
                  parse_levels([["150.00", "10.0"]]),
                  parse_levels([["151.00", "10.0"]]))
    b.on_depth(101, 110, 99, [], [])

    b.set_ticker_bbo(Decimal("149.00"), Decimal("152.00"))

    uid = 110
    all_live = True
    for _ in range(5):
        b.on_depth(uid + 1, uid + 10, uid, [], [])
        uid += 10
        if b.state != "live":
            all_live = False

    b.set_ticker_bbo(Decimal("150.00"), Decimal("151.00"))
    b.on_depth(uid + 1, uid + 10, uid, [], [])
    uid += 10
    if b.state != "live":
        all_live = False

    b.set_ticker_bbo(Decimal("149.00"), Decimal("152.00"))
    r = None
    for _ in range(10):
        r = b.on_depth(uid + 1, uid + 10, uid, [], [])
        uid += 10
        if b.state != "live":
            break

    return {
        "all_live_during_warmup": all_live,
        "final_action": r,
        "final_state": b.state,
    }


def scenario_reset_and_resync():
    """Reset clears everything and allows fresh sync."""
    b = Book("DOGEUSDT")
    b.on_depth(1, 10, 0, [], [])
    b.on_snapshot(100,
                  parse_levels([["0.10", "1000"]]),
                  parse_levels([["0.11", "1000"]]))
    b.on_depth(101, 110, 99, [], [])

    pre_reset = {
        "state": b.state,
        "bid_count": len(b.bids),
        "last_update_id": b.last_update_id,
    }

    b.reset()

    post_reset = {
        "state": b.state,
        "bid_count": len(b.bids),
        "last_update_id": b.last_update_id,
    }

    r = b.on_depth(200, 210, 190, [], [])

    return {
        "pre_reset": pre_reset,
        "post_reset": post_reset,
        "after_depth_action": r,
        "after_depth_state": b.state,
    }


def scenario_top_levels_ordering():
    """Verify top_levels returns correct sort order."""
    b = Book("AVAXUSDT")
    b.on_depth(1, 10, 0, [], [])
    b.on_snapshot(100,
                  parse_levels([["30.00", "1"], ["28.00", "2"], ["29.00", "3"],
                                ["27.00", "4"], ["31.00", "5"]]),
                  parse_levels([["32.00", "1"], ["35.00", "2"], ["33.00", "3"],
                                ["34.00", "4"], ["36.00", "5"]]))

    top_bids, top_asks = b.top_levels(3)
    return {
        "top_3_bid_prices": [str(p) for p, _ in top_bids],
        "top_3_ask_prices": [str(p) for p, _ in top_asks],
    }


def run_all():
    return {
        "basic_lifecycle": scenario_basic_lifecycle(),
        "crossed_book": scenario_crossed_book(),
        "bbo_divergence": scenario_bbo_divergence(),
        "reset_and_resync": scenario_reset_and_resync(),
        "top_levels_ordering": scenario_top_levels_ordering(),
    }


def normalize(obj):
    """Normalize for comparison: sort keys, convert types."""
    return json.loads(json.dumps(obj, sort_keys=True, default=str))


def main():
    parser = argparse.ArgumentParser(description="Cross-language comparison test")
    parser.add_argument("--rust-binary", type=str, default=None,
                        help="Path to Rust binary (build with: cd rust && cargo build)")
    args = parser.parse_args()

    py_results = run_all()

    if not args.rust_binary:
        print("Python scenarios:")
        print(json.dumps(py_results, indent=2, default=str))
        print("\nTo compare against Rust, run:")
        print("  cd rust && cargo build")
        print("  python tests/cross_language_comparison.py --rust-binary rust/target/debug/binance-feed-handler")
        return 0

    try:
        rust_proc = subprocess.run(
            [args.rust_binary, "--test-scenarios"],
            capture_output=True, text=True, timeout=10,
        )
    except FileNotFoundError:
        print(f"ERROR: Rust binary not found at {args.rust_binary}")
        print("Build with: cd rust && cargo build")
        return 1

    if rust_proc.returncode != 0:
        print(f"ERROR: Rust binary failed:\n{rust_proc.stderr}")
        return 1

    rs_results = json.loads(rust_proc.stdout)

    all_match = True
    for name in py_results:
        py_norm = normalize(py_results[name])
        rs_norm = normalize(rs_results.get(name, {}))
        if py_norm == rs_norm:
            print(f"  {name}: MATCH")
        else:
            print(f"  {name}: MISMATCH")
            print(f"    Python: {json.dumps(py_norm, sort_keys=True)}")
            print(f"    Rust:   {json.dumps(rs_norm, sort_keys=True)}")
            all_match = False

    if all_match:
        print(f"\nAll {len(py_results)} scenarios match between Python and Rust.")
        return 0
    else:
        print("\nSome scenarios differ.")
        return 1


if __name__ == "__main__":
    sys.exit(main())

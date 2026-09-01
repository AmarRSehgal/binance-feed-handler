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


# === DIFFERENTIAL FUZZ ===
#
# The generator below is a byte-for-byte mirror of the `fuzz` module in
# rust/src/main.rs: same LCG, same constants, same branch thresholds, so both
# implementations see an identical event script for a given seed and any
# behavioural divergence shows up as a digest mismatch.
#
# Hand-written scenarios only cover the transitions someone thought to write
# down. This covers the interleavings nobody would: a snapshot landing behind the
# buffer, a reset mid-resync, a stale event straddling a gap.

BID_PRICES = ["100", "99", "98"]
ASK_PRICES = ["101", "102", "103"]
_U64 = (1 << 64) - 1


class Lcg:
    __slots__ = ("state",)

    def __init__(self, seed: int):
        self.state = seed

    def next(self) -> int:
        self.state = (self.state * 6364136223846793005 + 1442695040888963407) & _U64
        return self.state >> 33

    def below(self, n: int) -> int:
        return self.next() % n


def _levels(rng: Lcg, prices: list[str]) -> list[tuple[Decimal, Decimal]]:
    out = []
    for _ in range(rng.below(3)):
        price = prices[rng.below(3)]
        # A zero qty is a level delete -- the most common real diff.
        qty = "0" if rng.below(4) == 0 else str(rng.below(9) + 1)
        out.append((Decimal(price), Decimal(qty)))
    return out


def _digest(step: int, op: str, action, b: Book) -> dict:
    top_bids, top_asks = b.top_levels(3)
    return {
        "step": step,
        "op": op,
        "action": action,
        "state": b.state,
        "last_update_id": b.last_update_id,
        "bids": len(b.bids),
        "asks": len(b.asks),
        "top_bids": [[str(p), str(q)] for p, q in top_bids],
        "top_asks": [[str(p), str(q)] for p, q in top_asks],
        "gaps": b.gap_count,
        "crossed": b.crossed_count,
        "snapshots": b.snapshot_count,
        "unverified_bridges": b.unverified_bridge_count,
        "buffer_dropped": b.buffer_dropped,
    }


def run_fuzz(seed: int, steps: int = 2000) -> dict:
    rng = Lcg(seed)
    b = Book("FUZZUSDT")
    seq = 1000
    out = []

    for step in range(steps):
        roll = rng.below(100)
        if roll < 55 or 65 <= roll < 78:
            # Contiguous diff, or one that deliberately skips the sequence.
            skip = 0 if roll < 55 else 10 * (1 + rng.below(5))
            bids = _levels(rng, BID_PRICES)
            asks = _levels(rng, ASK_PRICES)
            big_u, u, pu = seq + skip + 1, seq + skip + 10, seq + skip
            seq = u
            action = b.on_depth(big_u, u, pu, bids, asks)
            out.append(_digest(step, "depth" if skip == 0 else "depth_gap", action, b))
        elif roll < 88:
            r = rng.below(3)
            if r == 0:
                lui = seq
            elif r == 1:
                lui = seq + 10 * (1 + rng.below(3))
            else:
                lui = max(0, seq - 10 * (1 + rng.below(3)))
            bids = _levels(rng, BID_PRICES)
            asks = _levels(rng, ASK_PRICES)
            ok = b.on_snapshot(lui, bids, asks)
            out.append(_digest(step, "snapshot", "live" if ok else "retry", b))
        elif roll < 93:
            # A stale replay: u is far behind last_update_id.
            out.append(_digest(step, "stale", b.on_depth(1, 2, 0, [], []), b))
        elif roll < 96:
            b.reset()
            out.append(_digest(step, "reset", None, b))
        elif roll < 98:
            b.mark_for_resync()
            out.append(_digest(step, "mark_for_resync", None, b))
        else:
            b.set_ticker_bbo(Decimal(BID_PRICES[rng.below(3)]),
                             Decimal(ASK_PRICES[rng.below(3)]))
            out.append(_digest(step, "ticker", None, b))

    return {"seed": seed, "steps": steps, "digest": out}


def compare_fuzz(rust_binary: str, seeds: list[int], steps: int) -> bool:
    """Run the same fuzz script through both implementations and diff the digests."""
    all_ok = True
    for seed in seeds:
        expected = run_fuzz(seed, steps)
        proc = subprocess.run(
            [rust_binary, "--test-fuzz", str(seed), "--test-fuzz-steps", str(steps)],
            capture_output=True, text=True,
        )
        if proc.returncode != 0:
            print(f"  seed {seed}: RUST FAILED\n{proc.stderr}")
            all_ok = False
            continue
        actual = json.loads(proc.stdout)
        if actual == expected:
            print(f"  seed {seed} ({steps} steps): MATCH")
            continue
        all_ok = False
        mismatches = [
            (e, a) for e, a in zip(expected["digest"], actual["digest"]) if e != a
        ]
        print(f"  seed {seed}: MISMATCH at {len(mismatches)} of {steps} steps")
        for e, a in mismatches[:3]:
            print(f"    step {e['step']} op={e['op']}")
            for k in e:
                if e[k] != a.get(k):
                    print(f"      {k}: python={e[k]!r} rust={a.get(k)!r}")
    return all_ok


def main():
    parser = argparse.ArgumentParser(description="Cross-language comparison test")
    parser.add_argument("--rust-binary", type=str, default=None,
                        help="Path to Rust binary (build with: cd rust && cargo build)")
    parser.add_argument("--fuzz-seeds", type=int, default=8,
                        help="Differential fuzz seeds to compare (0 to skip)")
    parser.add_argument("--fuzz-steps", type=int, default=2000,
                        help="Event-script length per fuzz seed")
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

    if args.fuzz_seeds:
        print(f"\nDifferential fuzz ({args.fuzz_seeds} seeds x {args.fuzz_steps} steps):")
        if not compare_fuzz(args.rust_binary, list(range(1, args.fuzz_seeds + 1)),
                            args.fuzz_steps):
            all_match = False

    if all_match:
        print(f"\nAll {len(py_results)} scenarios and every fuzz seed match "
              "between Python and Rust.")
        return 0
    print("\nPython and Rust diverge -- see above.")
    return 1


if __name__ == "__main__":
    sys.exit(main())

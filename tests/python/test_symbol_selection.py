"""Tests for symbol universe selection.

--max-symbols used to truncate an alphabetical sort, so a capped run tracked
1000BONK/1000FLOKI/AAVE/... and never BTCUSDT -- testing only the thinnest
books on the venue.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))

from feed_handler import pick_top_by_volume


def test_cap_picks_the_most_liquid_not_the_alphabetically_first():
    all_syms = ["1000BONKUSDT", "AAVEUSDT", "BTCUSDT", "ETHUSDT"]
    volumes = {"1000BONKUSDT": 1e7, "AAVEUSDT": 5e7, "BTCUSDT": 9e9, "ETHUSDT": 4e9}
    assert pick_top_by_volume(all_syms, volumes, 2) == ["BTCUSDT", "ETHUSDT"]


def test_pick_returns_alphabetical_for_deterministic_sharding():
    assert pick_top_by_volume(["AAA", "BBB", "CCC"],
                              {"AAA": 1.0, "BBB": 3.0, "CCC": 2.0}, 3) == ["AAA", "BBB", "CCC"]


def test_pick_ranks_missing_volume_last_and_breaks_ties_by_name():
    # AAA and BBB tie; ZZZ has no ticker row at all.
    volumes = {"AAA": 5.0, "BBB": 5.0}
    assert pick_top_by_volume(["AAA", "BBB", "ZZZ"], volumes, 2) == ["AAA", "BBB"]
    assert pick_top_by_volume(["AAA", "BBB", "ZZZ"], volumes, 1) == ["AAA"]


def test_pick_beyond_universe_size_returns_everything():
    assert pick_top_by_volume(["AAA", "BBB"], {"AAA": 1.0}, 99) == ["AAA", "BBB"]

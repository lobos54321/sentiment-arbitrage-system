import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from triple_barrier_label import TripleBarrierConfig, label_triple_barrier_path


def test_triple_barrier_hits_first_upper_before_lower():
    label = label_triple_barrier_path(
        [
            {"ts": 1_010, "quote_pnl_pct": 0.10, "quote_clean": True, "quote_executable": True, "route_available": True},
            {"ts": 1_020, "quote_pnl_pct": 0.55, "quote_clean": True, "quote_executable": True, "route_available": True},
            {"ts": 1_030, "quote_pnl_pct": -0.25, "quote_clean": True, "quote_executable": True, "route_available": True},
        ],
        entry_ts=1_000,
    )

    assert label.label == "UPPER"
    assert label.hit_upper == 0.50
    assert label.time_to_terminal_sec == 20
    assert label.max_pnl_pct == 0.55


def test_triple_barrier_hits_lower_before_later_dog():
    label = label_triple_barrier_path(
        [
            {"ts": 1_010, "quote_pnl_pct": -0.21, "quote_clean": True, "quote_executable": True, "route_available": True},
            {"ts": 1_020, "quote_pnl_pct": 1.10, "quote_clean": True, "quote_executable": True, "route_available": True},
        ],
        entry_ts=1_000,
    )

    assert label.label == "LOWER"
    assert label.hit_lower == -0.20
    assert label.min_pnl_pct == -0.21


def test_triple_barrier_no_route_is_absorbing_before_price_barriers():
    label = label_triple_barrier_path(
        [
            {"ts": 1_010, "quote_pnl_pct": 0.80, "no_route_flag": True},
            {"ts": 1_020, "quote_pnl_pct": 2.00, "quote_clean": True},
        ],
        entry_ts=1_000,
    )

    assert label.label == "NO_ROUTE"
    assert label.no_route_seen is True
    assert label.hit_upper is None


def test_triple_barrier_ignores_dirty_samples_when_required():
    label = label_triple_barrier_path(
        [
            {"ts": 1_010, "quote_pnl_pct": 0.80, "quote_clean": False, "quote_executable": False},
            {"ts": 1_030, "quote_pnl_pct": 0.10, "quote_clean": True, "quote_executable": True, "route_available": True},
        ],
        entry_ts=1_000,
        config=TripleBarrierConfig(horizon_sec=60, require_quote_clean=True),
    )

    assert label.label == "TIMEOUT"
    assert label.max_pnl_pct == 0.10
    assert label.quote_clean_sample_count == 1


def test_triple_barrier_reports_missing_data():
    label = label_triple_barrier_path([], entry_ts=1_000)

    assert label.label == "DATA_MISSING"
    assert label.data_quality == "missing"

import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from canonical_ledger import init_canonical_ledger, record_canonical_trade_entry, record_canonical_trade_exit
from entry_mode_scorecard import build_entry_mode_scorecard


def memory_db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_canonical_ledger(db)
    return db


def add_trade(db, trade_id, mode, *, entry_ts=1000, exit_sol=0.0012, entry_sol=0.001, peak=0.25, outlier=False, no_route=False):
    record_canonical_trade_entry(
        db,
        {
            "trade_id": trade_id,
            "token_ca": f"Token{trade_id}",
            "symbol": trade_id,
            "entry_ts": entry_ts,
            "entry_size_sol": entry_sol,
            "normalized_mode": mode,
            "entry_quote_executable": True,
            "entry_route_available": True,
        },
    )
    record_canonical_trade_exit(
        db,
        trade_id,
        {
            "exit_ts": entry_ts + 60,
            "realized_exit_sol": exit_sol,
            "peak_quote_pnl_pct": peak,
            "no_route_flag": no_route,
            "outlier_flag": outlier,
        },
    )


def test_scorecard_calculates_ev_rates_and_outlier_adjusted_pnl():
    db = memory_db()
    add_trade(db, "t1", "ATH_CONTINUATION", exit_sol=0.0012, peak=0.25)
    add_trade(db, "t2", "ATH_CONTINUATION", exit_sol=0.0008, peak=0.0)
    add_trade(db, "t3", "ATH_CONTINUATION", exit_sol=0.0100, peak=5.0, outlier=True)

    card = build_entry_mode_scorecard(db, min_sample_to_live=2)
    row = card["rows"][0]

    assert card["available"] is True
    assert row["mode"] == "ATH_CONTINUATION"
    assert row["trades"] == 3
    assert row["closed_trades"] == 3
    assert round(row["win_rate"], 4) == 0.6667
    assert round(row["peak20_rate"], 4) == 0.6667
    assert round(row["doa_rate"], 4) == 0.3333
    assert round(row["total_pnl_sol"], 6) == 0.009
    assert round(row["outlier_adjusted_total_pnl_sol"], 6) == 0.0


def test_scorecard_status_disables_high_no_route_mode():
    db = memory_db()
    add_trade(db, "n1", "LOTTO_TINY_SCOUT", exit_sol=0.0, peak=0.0, no_route=True)
    add_trade(db, "n2", "LOTTO_TINY_SCOUT", exit_sol=0.0, peak=0.0, no_route=True)

    row = build_entry_mode_scorecard(db, min_sample_to_live=2)["rows"][0]

    assert row["status"] == "DISABLED"
    assert row["allowed_max_size_sol"] == 0.0


def test_scorecard_reports_missing_ledger_table():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row

    card = build_entry_mode_scorecard(db)

    assert card["available"] is False
    assert card["reason"] == "canonical_trade_ledger_missing"

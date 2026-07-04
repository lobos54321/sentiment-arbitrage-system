import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from a_class_runtime_safety import (
    A_CLASS_RUNTIME_MODE_KEY,
    fetch_mode_runtime_state,
    record_loss_cap_breach_reaction,
    summarize_runtime_safety,
)
from canonical_ledger import record_canonical_trade_entry, record_canonical_trade_exit


def memory_db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    return db


def _record_a_class_trade(db, *, trade_id="trade-a", exit_sol=0.00075, exit_ts=1_030):
    record_canonical_trade_entry(
        db,
        {
            "trade_id": trade_id,
            "token_ca": "TokenA",
            "symbol": "AFAST",
            "entry_ts": 1_000,
            "entry_size_sol": 0.001,
            "entry_mode": "a_class_fastlane_tiny_canary",
            "normalized_mode": "A_GRADE_RESONANCE_FASTLANE",
            "is_a_class_fastlane": True,
            "entry_quote_source": "gmgn",
            "entry_route_available": True,
            "entry_quote_executable": True,
        },
    )
    record_canonical_trade_exit(
        db,
        trade_id,
        {
            "exit_ts": exit_ts,
            "realized_exit_sol": exit_sol,
            "exit_reason": "hard_stop",
            "loss_cap_pct": 0.20,
        },
    )


def test_loss_cap_breach_downgrades_a_class_mode_and_is_idempotent():
    db = memory_db()
    _record_a_class_trade(db)

    detail = record_loss_cap_breach_reaction(db, "trade-a", now_ts=1_030, cooldown_sec=600)

    assert detail["breach"] is True
    assert detail["should_record_event"] is True
    assert detail["mode_key"] == A_CLASS_RUNTIME_MODE_KEY
    assert detail["status"] == "CIRCUIT_BROKEN"
    assert detail["action"] == "SHADOW"

    state = fetch_mode_runtime_state(db, "a_class_fastlane_tiny_canary", now_ts=1_100)
    assert state["status"] == "CIRCUIT_BROKEN"
    assert state["circuit_broken"] is True
    assert state["cooldown_remaining_sec"] == 530

    duplicate = record_loss_cap_breach_reaction(db, "trade-a", now_ts=1_040, cooldown_sec=600)
    assert duplicate["breach"] is True
    assert duplicate["should_record_event"] is False

    row = db.execute(
        "SELECT breach_count, source_trade_id FROM a_class_mode_runtime_state WHERE mode_key = ?",
        (A_CLASS_RUNTIME_MODE_KEY,),
    ).fetchone()
    assert row["breach_count"] == 1
    assert row["source_trade_id"] == "trade-a"


def test_paper_only_loss_cap_breach_records_paper_market_recovery_contract():
    db = memory_db()
    _record_a_class_trade(db, trade_id="71", exit_sol=0.0007025)
    db.execute(
        """
        CREATE TABLE paper_trades(
          id INTEGER PRIMARY KEY,
          paper_only INTEGER,
          token_ca TEXT,
          symbol TEXT,
          premium_signal_id INTEGER,
          entry_ts INTEGER,
          exit_ts INTEGER,
          entry_price REAL,
          exit_price REAL,
          pnl_pct REAL,
          exit_reason TEXT
        )
        """
    )
    db.execute(
        "INSERT INTO paper_trades VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (71, 1, "TokenA", "USDC", 49535, 1_000, 1_030, 1.0, 0.7025, -0.2975, "probe_quote_guard_stop"),
    )
    db.commit()

    detail = record_loss_cap_breach_reaction(db, "71", now_ts=1_030, cooldown_sec=600)

    assert detail["breach"] is True
    assert detail["breach_class"] == "PAPER_MARKET"
    assert detail["paper_only"] is True
    assert detail["paper_recovery_contract"]["paper_auto_recovery_counter_started"] is True
    assert detail["paper_recovery_contract"]["live_reenable_requires_human_operator"] is True
    state = fetch_mode_runtime_state(db, "A_CLASS_FASTLANE", now_ts=1_031)
    assert state["detail"]["breach_class"] == "PAPER_MARKET"


def test_paper_only_loss_cap_breach_defaults_to_short_paper_market_sla():
    db = memory_db()
    _record_a_class_trade(db, trade_id="72", exit_sol=0.0007025)
    db.execute(
        """
        CREATE TABLE paper_trades(
          id INTEGER PRIMARY KEY,
          paper_only INTEGER,
          token_ca TEXT,
          symbol TEXT,
          premium_signal_id INTEGER,
          entry_ts INTEGER,
          exit_ts INTEGER,
          entry_price REAL,
          exit_price REAL,
          pnl_pct REAL,
          exit_reason TEXT
        )
        """
    )
    db.execute(
        "INSERT INTO paper_trades VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (72, 1, "TokenA", "USDC", 49536, 1_000, 1_030, 1.0, 0.7025, -0.2975, "probe_quote_guard_stop"),
    )
    db.commit()

    detail = record_loss_cap_breach_reaction(db, "72", now_ts=1_030)

    assert detail["breach_class"] == "PAPER_MARKET"
    assert detail["cooldown_sec"] == 4 * 60 * 60
    assert detail["clean_windows_required"] == 6
    assert detail["paper_auto_recovery_counter_started"] is True


def test_cooldown_expiry_returns_shadow_not_live():
    db = memory_db()
    _record_a_class_trade(db)

    record_loss_cap_breach_reaction(db, "trade-a", now_ts=1_030, cooldown_sec=60)

    state = fetch_mode_runtime_state(db, "A_GRADE_RESONANCE_FASTLANE", now_ts=1_200)
    assert state["status"] == "SHADOW"
    assert state["action"] == "SHADOW"
    assert state["circuit_broken"] is False
    assert state["recovery_required"] is True
    assert state["reason"] == "cooldown_elapsed_requires_clean_windows"


def test_non_breach_and_non_a_class_do_not_downgrade():
    db = memory_db()
    record_canonical_trade_entry(
        db,
        {
            "trade_id": "trade-main",
            "token_ca": "TokenB",
            "symbol": "MAIN",
            "entry_ts": 1_000,
            "entry_size_sol": 0.01,
            "entry_mode": "main_mode",
            "normalized_mode": "ATH_CONTINUATION",
        },
    )
    record_canonical_trade_exit(
        db,
        "trade-main",
        {
            "exit_ts": 1_020,
            "realized_exit_sol": 0.007,
            "exit_reason": "hard_stop",
            "loss_cap_pct": 0.20,
        },
    )

    detail = record_loss_cap_breach_reaction(db, "trade-main", now_ts=1_020, cooldown_sec=600)
    assert detail["breach"] is True
    assert detail["reason"] == "non_a_class_mode_ignored"

    state = fetch_mode_runtime_state(db, "A_CLASS_FASTLANE", now_ts=1_030)
    assert state["status"] == "LIVE"


def test_runtime_safety_summary_exposes_breach_and_downgraded_mode():
    db = memory_db()
    _record_a_class_trade(db)
    record_loss_cap_breach_reaction(db, "trade-a", now_ts=1_030, cooldown_sec=600)

    summary = summarize_runtime_safety(db, since_ts=900, now_ts=1_100)

    assert summary["loss_cap_breach_n"] == 1
    assert summary["mode_circuit_broken"] is True
    assert summary["downgraded_modes"][0]["mode_key"] == A_CLASS_RUNTIME_MODE_KEY
    assert summary["next_safe_action"] == "keep_breached_modes_shadow_until_cooldown"

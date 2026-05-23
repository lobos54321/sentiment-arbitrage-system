import sqlite3
import sys

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog  # noqa: E402
from v27_mirror_recovery_controls import mirror_recovery_controls  # noqa: E402


def create_signal_db(path):
    db = sqlite3.connect(str(path))
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE premium_signals (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            created_at TEXT
        )
        """
    )
    db.execute(
        "INSERT INTO premium_signals (id, token_ca, symbol, created_at) VALUES (?, ?, ?, ?)",
        (1, "TokenRecovery", "RECO", "2026-01-15 00:00:00"),
    )
    db.commit()
    return db


def create_paper_db(path):
    db = sqlite3.connect(str(path))
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE paper_trades (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            premium_signal_id INTEGER,
            entry_price REAL,
            entry_ts INTEGER,
            signal_ts INTEGER,
            position_size_sol REAL,
            entry_mode TEXT,
            signal_route TEXT
        )
        """
    )
    db.execute(
        """
        CREATE TABLE paper_missed_signal_attribution (
            id INTEGER PRIMARY KEY,
            decision_event_id INTEGER,
            created_event_ts REAL,
            token_ca TEXT,
            symbol TEXT,
            lifecycle_id TEXT,
            signal_id INTEGER,
            signal_ts INTEGER,
            route TEXT,
            component TEXT,
            decision TEXT,
            reject_reason TEXT,
            tradable_peak_pnl REAL,
            min_pnl_recorded REAL,
            tradable_missed INTEGER,
            status TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO paper_trades
            (id, token_ca, symbol, premium_signal_id, entry_price, entry_ts, signal_ts, position_size_sol, entry_mode, signal_route)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (1, "TokenRecovery", "RECO", 1, 0.001, 1_700_000_004_000, 1_700_000_000_000, 0.01, "unit_entry", "unit_route"),
    )
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution
            (id, decision_event_id, created_event_ts, token_ca, symbol, lifecycle_id, signal_id, signal_ts,
             route, component, decision, reject_reason, tradable_peak_pnl, min_pnl_recorded, tradable_missed, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (1, 10, 1_700_000_010, "TokenMissed", "MISS", "PoolMiss", 2, 1_700_000_000_000, "unit_route", "unit", "skip", "quote_unavailable", 0.75, -0.20, 1, "resolved"),
    )
    db.commit()
    return db


def test_recovery_control_mirror_appends_no_fill_and_recovery_events(tmp_path):
    signal_db = tmp_path / "signals.db"
    paper_db = tmp_path / "paper.db"
    event_log_dir = tmp_path / "events"
    with create_signal_db(signal_db), create_paper_db(paper_db):
        result = mirror_recovery_controls(
            paper_db,
            signal_db,
            event_log_dir,
            limit=10,
            missed_limit=10,
            environment_id="unit",
        )

    events = list(V27EventLog(event_log_dir).iter_events())
    no_fill_events = [event for event in events if event["event_type"] == "no_fill_outcome_recorded"]
    recovery_events = [event for event in events if event["event_type"] == "runtime_recovery_control_recorded"]

    assert result["mirror"]["paper_trade_rows"] == 1
    assert result["mirror"]["missed_rows"] == 1
    assert result["mirror"]["appended"] == 3
    assert len(no_fill_events) == 2
    assert len(recovery_events) == 1
    assert {event["payload"]["outcome_state"] for event in no_fill_events} == {"filled_paper", "no_fill"}
    assert all("legacy_paper_trade" not in event["payload"] for event in no_fill_events)
    assert all("legacy_missed_attribution" not in event["payload"] for event in no_fill_events)
    assert all(
        event["payload"].get("legacy_paper_trade_ref") or event["payload"].get("legacy_missed_attribution_ref")
        for event in no_fill_events
    )
    missed_payload = [event["payload"] for event in no_fill_events if event["payload"]["outcome_state"] == "no_fill"][0]
    assert missed_payload["no_fill_reason"] == "quote_unavailable"
    assert missed_payload["no_fill_cost"] == 0.75
    assert missed_payload["no_fill_saved_loss"] == 0.2
    recovery_payload = recovery_events[0]["payload"]
    assert recovery_payload["state"] == "clean_start"
    assert recovery_payload["orphan_scan_result"]["status"] == "ok"
    assert recovery_payload["drain_status"] == "completed"
    assert V27EventLog(event_log_dir).verify()["event_count"] == 3


def test_recovery_control_mirror_is_idempotent_for_same_source_cursor(tmp_path):
    signal_db = tmp_path / "signals.db"
    paper_db = tmp_path / "paper.db"
    event_log_dir = tmp_path / "events"
    with create_signal_db(signal_db), create_paper_db(paper_db):
        first = mirror_recovery_controls(paper_db, signal_db, event_log_dir, limit=10, missed_limit=10, environment_id="unit")
        second = mirror_recovery_controls(paper_db, signal_db, event_log_dir, limit=10, missed_limit=10, environment_id="unit")

    assert first["mirror"]["appended"] == 3
    assert second["mirror"]["appended"] == 0
    assert second["mirror"]["duplicate"] == 3
    assert V27EventLog(event_log_dir).verify()["event_count"] == 3

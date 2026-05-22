import sqlite3
import sys
from types import SimpleNamespace

sys.path.insert(0, "scripts")

from v27_denominator_projection import build_denominator_projection  # noqa: E402
from v27_event_log import V27EventLog  # noqa: E402
from v27_mirror_paper_trade_source_labels import (  # noqa: E402
    acquire_loop_lock,
    mirror_paper_trade_source_labels,
    run_mirror_once,
    verify_paper_trade_source_label_mirror_parity,
)


def create_signal_db(db_path):
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE premium_signals (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            timestamp INTEGER,
            source_message_ts INTEGER,
            receive_ts INTEGER,
            signal_type TEXT,
            parse_status TEXT,
            gate_result TEXT,
            raw_message TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO premium_signals
            (id, token_ca, symbol, timestamp, source_message_ts, receive_ts,
             signal_type, parse_status, gate_result, raw_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            101,
            "TokenTradeDog",
            "TDOG",
            1_700_000_000_000,
            1_700_000_000_000,
            1_700_000_003_000,
            "NOT_ATH",
            "parsed",
            '{"status":"PASS"}',
            "trade dog signal",
        ),
    )
    db.commit()
    return db


def create_paper_db(db_path):
    db = sqlite3.connect(str(db_path))
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
            exit_ts INTEGER,
            peak_pnl REAL
        )
        """
    )
    db.commit()
    return db


def insert_trade(db, *, trade_id, peak_pnl, premium_signal_id=101, token_ca="TokenTradeDog"):
    db.execute(
        """
        INSERT INTO paper_trades
            (id, token_ca, symbol, premium_signal_id, entry_price, entry_ts, exit_ts, peak_pnl)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (trade_id, token_ca, "TDOG", premium_signal_id, 0.001, 1_700_000_004_000, 1_700_000_300_000, peak_pnl),
    )
    db.commit()


def test_paper_trade_source_label_mirror_binds_trade_peak_to_telegram_anchor(tmp_path):
    signal_db = tmp_path / "signals.db"
    paper_db = tmp_path / "paper.db"
    event_log_dir = tmp_path / "v27"
    with create_signal_db(signal_db), create_paper_db(paper_db) as paper:
        insert_trade(paper, trade_id=1, peak_pnl=0.75)

    first = mirror_paper_trade_source_labels(paper_db, signal_db, event_log_dir)
    duplicate = mirror_paper_trade_source_labels(paper_db, signal_db, event_log_dir)
    parity = verify_paper_trade_source_label_mirror_parity(paper_db, signal_db, event_log_dir)

    assert first["read_rows"] == 1
    assert first["eligible_rows"] == 1
    assert first["selected_rows"] == 1
    assert first["signal_appended"] == 1
    assert first["source_appended"] == 1
    assert duplicate["signal_duplicate"] == 1
    assert duplicate["source_duplicate"] == 1
    assert parity["parity_ok"] is True

    events = list(V27EventLog(event_log_dir).iter_events())
    assert [event["event_type"] for event in events] == ["telegram_signal_seen", "source_dog_label_recorded"]
    assert events[0]["idempotency_key"] == "premium_signals:101"
    assert events[1]["idempotency_key"] == "paper_trade_source_label:1"
    assert events[1]["source"] == "paper_trades"
    assert events[1]["payload"]["source_dog_label"] == "silver"
    assert events[1]["payload"]["source_reference_price"] == 0.001
    assert events[1]["payload"]["source_reference_price_type"] == "legacy_entry_price"
    assert events[1]["payload"]["source_label_research_only"] is True


def test_paper_trade_source_label_mirror_closes_d0_signal_credit_and_reference_price(tmp_path):
    signal_db = tmp_path / "signals.db"
    paper_db = tmp_path / "paper.db"
    event_log_dir = tmp_path / "v27"
    with create_signal_db(signal_db), create_paper_db(paper_db) as paper:
        insert_trade(paper, trade_id=1, peak_pnl=0.75)

    mirror_paper_trade_source_labels(paper_db, signal_db, event_log_dir)
    projection = build_denominator_projection(event_log_dir, include_records=True)

    assert projection["metrics"]["telegram_gold_silver_total_D0"] == 1
    assert projection["metrics"]["telegram_realtime_observable_gold_silver_D1"] == 1
    assert projection["health"]["signal_credit_assignment_ok"] is True
    assert projection["health"]["reference_price_ok"] is True
    assert projection["contract_evidence"]["SignalCreditAssignmentContract"]["missing_count"] == 0
    assert projection["contract_evidence"]["ReferencePriceContract"]["missing_count"] == 0
    assert projection["records"][0]["signal_credit_assignment"]["credited_signal_id"] == 101
    assert projection["records"][0]["reference_price_contract"]["reference_price"] == 0.001


def test_paper_trade_source_label_mirror_selects_highest_peak_per_denominator_key(tmp_path):
    signal_db = tmp_path / "signals.db"
    paper_db = tmp_path / "paper.db"
    event_log_dir = tmp_path / "v27"
    with create_signal_db(signal_db), create_paper_db(paper_db) as paper:
        insert_trade(paper, trade_id=1, peak_pnl=0.55)
        insert_trade(paper, trade_id=2, peak_pnl=1.20)

    summary = mirror_paper_trade_source_labels(paper_db, signal_db, event_log_dir)
    parity = verify_paper_trade_source_label_mirror_parity(paper_db, signal_db, event_log_dir)

    assert summary["read_rows"] == 2
    assert summary["eligible_rows"] == 2
    assert summary["selected_rows"] == 1
    assert summary["skipped"] == 1
    assert parity["parity_ok"] is True
    assert parity["eligible_db_rows"] == 1

    source_event = [event for event in V27EventLog(event_log_dir).iter_events() if event["event_type"] == "source_dog_label_recorded"][0]
    assert source_event["payload"]["paper_trade_id"] == 2
    assert source_event["payload"]["source_dog_label"] == "gold"


def test_paper_trade_source_label_mirror_new_only_advances_from_event_log_cursor(tmp_path):
    signal_db = tmp_path / "signals.db"
    paper_db = tmp_path / "paper.db"
    event_log_dir = tmp_path / "v27"
    with create_signal_db(signal_db), create_paper_db(paper_db) as paper:
        insert_trade(paper, trade_id=1, peak_pnl=0.75)
        insert_trade(paper, trade_id=2, peak_pnl=0.80)

    args = SimpleNamespace(
        paper_db=str(paper_db),
        signal_db=str(signal_db),
        event_log_dir=str(event_log_dir),
        since_id=None,
        until_id=None,
        limit=1,
        min_peak_pnl=0.5,
        dry_run=False,
        table="paper_trades",
        signal_table="premium_signals",
        default_chain="solana",
        new_only=True,
    )
    first = run_mirror_once(args)
    second = run_mirror_once(args)

    assert first["cursor"]["since_id"] is None
    assert first["mirror"]["source_appended"] == 1
    assert first["cursor"]["max_mirrored_paper_trade_id"] == 1
    assert second["cursor"]["since_id"] == 2
    assert second["mirror"]["source_appended"] == 1
    assert second["cursor"]["max_mirrored_paper_trade_id"] == 2


def test_paper_trade_source_label_mirror_loop_lock_rejects_duplicate_worker(tmp_path):
    lock_path = tmp_path / "v27_paper_trade_source_label.lock"
    first = acquire_loop_lock(lock_path)
    assert first is not None
    try:
        assert acquire_loop_lock(lock_path) is None
    finally:
        first.close()

    second = acquire_loop_lock(lock_path)
    assert second is not None
    second.close()

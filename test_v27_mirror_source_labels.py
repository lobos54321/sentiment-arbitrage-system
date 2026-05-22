import sqlite3
import sys
from types import SimpleNamespace

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog  # noqa: E402
from v27_mirror_source_labels import acquire_loop_lock, mirror_source_labels, run_mirror_once, verify_source_label_mirror_parity  # noqa: E402


def create_source_label_db(db_path):
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE signal_features (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            chain TEXT,
            symbol TEXT,
            entry_price REAL,
            max_gain_24h REAL,
            is_gold_dog INTEGER DEFAULT 0,
            is_silver_dog INTEGER DEFAULT 0,
            captured_at TEXT,
            tracked_at TEXT
        )
        """
    )
    db.commit()
    return db


def test_source_label_mirror_derives_legacy_gold_and_is_idempotent(tmp_path):
    db_path = tmp_path / "signals.db"
    event_log_dir = tmp_path / "v27"
    with create_source_label_db(db_path) as db:
        db.execute(
            """
            INSERT INTO signal_features
                (id, token_ca, chain, symbol, entry_price, max_gain_24h,
                 is_gold_dog, is_silver_dog, captured_at, tracked_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "TokenGold", "SOL", "GOLD", 0.001, 125.0, 0, 0, "2026-01-15 00:00:00", "2026-01-14 00:00:00"),
        )
        db.commit()

    first = mirror_source_labels(db_path, event_log_dir)
    duplicate = mirror_source_labels(db_path, event_log_dir)
    parity = verify_source_label_mirror_parity(db_path, event_log_dir)

    assert first["read_rows"] == 1
    assert first["appended"] == 1
    assert duplicate["duplicate"] == 1
    assert parity["parity_ok"] is True

    event = next(V27EventLog(event_log_dir).iter_events())
    assert event["event_type"] == "source_dog_label_recorded"
    assert event["source"] == "signal_features"
    assert event["aggregate_id"] == "source_label:solana:TokenGold:unknown_pool:0"
    assert event["idempotency_key"] == "signal_features_source_label:1"
    assert event["observed_at"] == "2026-01-15T00:00:00Z"
    assert event["payload"]["source_dog_label"] == "gold"
    assert event["payload"]["source_label_quality"] == "legacy_max_gain_24h_pct"
    assert event["payload"]["source_label_research_only"] is True
    assert event["payload"]["source_reference_price_type"] == "legacy_entry_price"


def test_source_label_mirror_keeps_unresolved_label_missing(tmp_path):
    db_path = tmp_path / "signals.db"
    event_log_dir = tmp_path / "v27"
    with create_source_label_db(db_path) as db:
        db.execute(
            """
            INSERT INTO signal_features
                (id, token_ca, chain, symbol, entry_price, max_gain_24h,
                 is_gold_dog, is_silver_dog, captured_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "TokenUnknown", "SOL", "UNK", None, None, 0, 0, "2026-01-15 00:00:00"),
        )
        db.commit()

    summary = mirror_source_labels(db_path, event_log_dir)

    assert summary["appended"] == 1
    event = next(V27EventLog(event_log_dir).iter_events())
    assert event["payload"]["source_dog_label"] is None
    assert event["payload"]["source_label_quality"] == "legacy_label_unresolved"
    assert event["payload"]["source_reference_price_type"] == "missing"


def test_source_label_scoped_parity_does_not_treat_previous_ids_as_orphans(tmp_path):
    db_path = tmp_path / "signals.db"
    event_log_dir = tmp_path / "v27"
    with create_source_label_db(db_path) as db:
        for label_id in (1, 2):
            db.execute(
                """
                INSERT INTO signal_features
                    (id, token_ca, chain, symbol, entry_price, max_gain_24h,
                     is_gold_dog, is_silver_dog, captured_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (label_id, f"Token{label_id}", "SOL", f"T{label_id}", 0.001, 125.0, 0, 0, "2026-01-15 00:00:00"),
            )
        db.commit()

    mirror_source_labels(db_path, event_log_dir)
    scoped = verify_source_label_mirror_parity(db_path, event_log_dir, since_id=2, limit=1)

    assert scoped["db_rows"] == 1
    assert scoped["mirrored_events"] == 1
    assert scoped["orphan_mirrored_source_label_ids"] == []
    assert scoped["parity_ok"] is True


def test_source_label_mirror_new_only_cursor_advances_from_event_log(tmp_path):
    db_path = tmp_path / "signals.db"
    event_log_dir = tmp_path / "v27"
    with create_source_label_db(db_path) as db:
        for label_id in (1, 2):
            db.execute(
                """
                INSERT INTO signal_features
                    (id, token_ca, chain, symbol, entry_price, max_gain_24h,
                     is_gold_dog, is_silver_dog, captured_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (label_id, f"Token{label_id}", "SOL", f"T{label_id}", 0.001, 125.0, 0, 0, "2026-01-15 00:00:00"),
            )
        db.commit()

    args = SimpleNamespace(
        db=str(db_path),
        event_log_dir=str(event_log_dir),
        since_id=None,
        until_id=None,
        limit=1,
        dry_run=False,
        table="signal_features",
        new_only=True,
    )
    first = run_mirror_once(args)
    second = run_mirror_once(args)

    assert first["cursor"]["since_id"] is None
    assert first["mirror"]["appended"] == 1
    assert first["cursor"]["max_mirrored_source_label_id"] == 1
    assert second["cursor"]["since_id"] == 2
    assert second["mirror"]["appended"] == 1
    assert second["cursor"]["max_mirrored_source_label_id"] == 2


def test_source_label_mirror_loop_lock_rejects_duplicate_worker(tmp_path):
    lock_path = tmp_path / "v27_source_label.lock"
    first = acquire_loop_lock(lock_path)
    assert first is not None
    try:
        assert acquire_loop_lock(lock_path) is None
    finally:
        first.close()

    second = acquire_loop_lock(lock_path)
    assert second is not None
    second.close()

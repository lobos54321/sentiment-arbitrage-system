import sqlite3
import sys

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog  # noqa: E402
from v27_mirror_lifecycle_tracks import mirror_lifecycle_tracks, verify_lifecycle_mirror_parity  # noqa: E402


def create_lifecycle_db(db_path):
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            signal_ts INTEGER,
            entry_price REAL,
            entry_ts INTEGER,
            pool_address TEXT,
            status TEXT DEFAULT 'active',
            complete_ts INTEGER,
            complete_reason TEXT
        )
        """
    )
    db.commit()
    return db


def test_lifecycle_track_mirror_is_idempotent_and_preserves_pool_identity(tmp_path):
    lifecycle_db = tmp_path / "lifecycle.db"
    event_log_dir = tmp_path / "v27"
    with create_lifecycle_db(lifecycle_db) as db:
        db.execute(
            """
            INSERT INTO tracks
                (id, token_ca, symbol, signal_ts, entry_price, entry_ts,
                 pool_address, status, complete_ts, complete_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "TokenLife", "LIFE", 1_700_000_000, 0.001, 1_700_000_003, "PoolA", "active", None, None),
        )
        db.commit()

    first = mirror_lifecycle_tracks(lifecycle_db, event_log_dir)
    duplicate = mirror_lifecycle_tracks(lifecycle_db, event_log_dir)
    parity = verify_lifecycle_mirror_parity(lifecycle_db, event_log_dir)

    assert first["read_rows"] == 1
    assert first["appended"] == 1
    assert duplicate["duplicate"] == 1
    assert parity["parity_ok"] is True

    event = next(V27EventLog(event_log_dir).iter_events())
    assert event["event_type"] == "token_lifecycle_identity_resolved"
    assert event["source"] == "lifecycle_tracks"
    assert event["aggregate_id"] == "token_lifecycle:solana:TokenLife:PoolA:0"
    assert event["idempotency_key"] == "lifecycle_tracks:1"
    assert event["observed_at"] == "2023-11-14T22:13:20Z"
    assert event["available_at"] == "2023-11-14T22:13:23Z"
    assert event["payload"]["token_ca"] == "TokenLife"
    assert event["payload"]["canonical_pool_group"] == "PoolA"
    assert event["payload"]["lifecycle_id"] == "TokenLife:1700000000"
    assert event["payload"]["pool_resolution_quality"] == "legacy_lifecycle_track"


def test_lifecycle_track_mirror_keeps_missing_pool_unresolved(tmp_path):
    lifecycle_db = tmp_path / "lifecycle.db"
    event_log_dir = tmp_path / "v27"
    with create_lifecycle_db(lifecycle_db) as db:
        db.execute(
            """
            INSERT INTO tracks
                (id, token_ca, symbol, signal_ts, entry_price, entry_ts, pool_address, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "TokenNoPool", "NOP", 1_700_000_000, 0.001, 1_700_000_003, None, "active"),
        )
        db.commit()

    summary = mirror_lifecycle_tracks(lifecycle_db, event_log_dir)

    assert summary["appended"] == 1
    event = next(V27EventLog(event_log_dir).iter_events())
    assert event["aggregate_id"] == "token_lifecycle:solana:TokenNoPool:unknown_pool:0"
    assert event["payload"]["canonical_pool_group"] == "unknown_pool"
    assert event["payload"]["pool_resolution_quality"] == "missing_pool"

import sqlite3
import sys
from types import SimpleNamespace

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog  # noqa: E402
from v27_mirror_lifecycle_tracks import acquire_loop_lock, mirror_lifecycle_tracks, run_mirror_once, verify_lifecycle_mirror_parity  # noqa: E402


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


def test_lifecycle_scoped_parity_does_not_treat_previous_ids_as_orphans(tmp_path):
    lifecycle_db = tmp_path / "lifecycle.db"
    event_log_dir = tmp_path / "v27"
    with create_lifecycle_db(lifecycle_db) as db:
        for track_id in (1, 2):
            db.execute(
                """
                INSERT INTO tracks
                    (id, token_ca, symbol, signal_ts, entry_price, entry_ts,
                     pool_address, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (track_id, f"Token{track_id}", f"T{track_id}", 1_700_000_000 + track_id, 0.001, 1_700_000_003 + track_id, f"Pool{track_id}", "active"),
            )
        db.commit()

    mirror_lifecycle_tracks(lifecycle_db, event_log_dir)
    scoped = verify_lifecycle_mirror_parity(lifecycle_db, event_log_dir, since_id=2, limit=1)

    assert scoped["db_rows"] == 1
    assert scoped["mirrored_events"] == 1
    assert scoped["orphan_mirrored_track_ids"] == []
    assert scoped["parity_ok"] is True


def test_lifecycle_mirror_new_only_cursor_advances_from_event_log(tmp_path):
    lifecycle_db = tmp_path / "lifecycle.db"
    event_log_dir = tmp_path / "v27"
    with create_lifecycle_db(lifecycle_db) as db:
        for track_id in (1, 2):
            db.execute(
                """
                INSERT INTO tracks
                    (id, token_ca, symbol, signal_ts, entry_price, entry_ts,
                     pool_address, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (track_id, f"Token{track_id}", f"T{track_id}", 1_700_000_000 + track_id, 0.001, 1_700_000_003 + track_id, f"Pool{track_id}", "active"),
            )
        db.commit()

    args = SimpleNamespace(
        lifecycle_db=str(lifecycle_db),
        event_log_dir=str(event_log_dir),
        since_id=None,
        until_id=None,
        limit=1,
        dry_run=False,
        table="tracks",
        default_chain="solana",
        new_only=True,
    )
    first = run_mirror_once(args)
    second = run_mirror_once(args)

    assert first["cursor"]["since_id"] is None
    assert first["mirror"]["appended"] == 1
    assert first["cursor"]["max_mirrored_track_id"] == 1
    assert second["cursor"]["since_id"] == 2
    assert second["mirror"]["appended"] == 1
    assert second["cursor"]["max_mirrored_track_id"] == 2


def test_lifecycle_mirror_loop_lock_rejects_duplicate_worker(tmp_path):
    lock_path = tmp_path / "v27_lifecycle.lock"
    first = acquire_loop_lock(lock_path)
    assert first is not None
    try:
        assert acquire_loop_lock(lock_path) is None
    finally:
        first.close()

    second = acquire_loop_lock(lock_path)
    assert second is not None
    second.close()

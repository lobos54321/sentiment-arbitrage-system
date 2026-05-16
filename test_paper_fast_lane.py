import sqlite3
import time

from scripts import paper_fast_lane as fast


def test_fast_queue_deduplicates_by_queue_key(tmp_path):
    db_path = tmp_path / "paper.db"
    db = fast.connect_db(db_path)
    fast.init_fast_lane_schema(db)

    first = fast.enqueue_fast_entry(
        db,
        source_type="hard_gate_fast",
        token_ca="TokenA",
        symbol="A",
        signal_ts=1_000,
        entry_branch="hard_gate_fast_clean",
    )
    second = fast.enqueue_fast_entry(
        db,
        source_type="hard_gate_fast",
        token_ca="TokenA",
        symbol="A",
        signal_ts=1_000,
        entry_branch="hard_gate_fast_clean",
    )

    assert first is True
    assert second is False
    assert db.execute("SELECT COUNT(*) FROM paper_fast_entry_queue").fetchone()[0] == 1


def test_claim_queue_item_is_single_owner(tmp_path):
    db_path = tmp_path / "paper.db"
    db = fast.connect_db(db_path)
    fast.init_fast_lane_schema(db)
    fast.enqueue_fast_entry(
        db,
        source_type="source_resonance_fast",
        token_ca="TokenB",
        symbol="B",
        signal_ts=2_000,
        entry_branch="source_resonance_quote_clean_fast",
    )

    row = fast.claim_queue_item(db, "worker-1")
    assert row is not None
    assert row["claimed_by"] == "worker-1"
    assert fast.claim_queue_item(db, "worker-2") is None


def test_entry_guard_rejects_hard_drift():
    now = int(time.time())
    row = {
        "source_signal_ts": now,
        "signal_receive_ts": now,
        "created_at": now,
        "trigger_price": 1.0,
        "hard_gate_status": "PASS",
    }

    detail = fast.entry_guard_detail(
        row,
        1.5,
        quote_request_ts_ms=now * 1000,
        quote_response_ts_ms=now * 1000,
    )

    assert detail["pass"] is False
    assert detail["reason"] == "fast_lane_quote_drift_hard_reject"


def test_retry_watch_requeues_until_queue_age_expires(tmp_path):
    db_path = tmp_path / "paper.db"
    db = fast.connect_db(db_path)
    fast.init_fast_lane_schema(db)
    fast.enqueue_fast_entry(
        db,
        source_type="missing_quote_recovery_fast",
        token_ca="TokenC",
        symbol="C",
        signal_ts=3_000,
        entry_branch="missing_trigger_or_quote",
    )
    row = fast.claim_queue_item(db, "worker-1")
    fast.mark_queue(db, row["id"], "retry_watch", "entry_quote_failed")
    db.execute(
        "UPDATE paper_fast_entry_queue SET updated_at = ? WHERE id = ?",
        (time.time() - 11, row["id"]),
    )
    db.commit()

    fast.refresh_retry_watch(db)
    status = db.execute("SELECT status FROM paper_fast_entry_queue WHERE id = ?", (row["id"],)).fetchone()[0]
    assert status == "queued"

import sqlite3
import time
import json

from scripts import paper_fast_lane as fast


def test_fast_queue_deduplicates_by_queue_key(tmp_path):
    db_path = tmp_path / "paper.db"
    db = fast.connect_db(db_path)
    fast.init_fast_lane_schema(db)
    now = int(time.time())

    first = fast.enqueue_fast_entry(
        db,
        source_type="hard_gate_fast",
        token_ca="TokenA",
        symbol="A",
        signal_ts=now,
        receive_ts=now,
        entry_branch="hard_gate_fast_clean",
    )
    second = fast.enqueue_fast_entry(
        db,
        source_type="hard_gate_fast",
        token_ca="TokenA",
        symbol="A",
        signal_ts=now,
        receive_ts=now,
        entry_branch="hard_gate_fast_clean",
    )

    assert first is True
    assert second is False
    assert db.execute("SELECT COUNT(*) FROM paper_fast_entry_queue").fetchone()[0] == 1


def test_fast_queue_deduplicates_recent_token_across_signal_ts(tmp_path):
    db_path = tmp_path / "paper.db"
    db = fast.connect_db(db_path)
    fast.init_fast_lane_schema(db)

    first = fast.enqueue_fast_entry(
        db,
        source_type="hard_gate_fast",
        token_ca="TokenA",
        symbol="A",
        signal_ts=1_000,
        receive_ts=int(time.time()),
        entry_branch="hard_gate_fast_clean",
        priority=10,
    )
    second = fast.enqueue_fast_entry(
        db,
        source_type="hard_gate_fast",
        token_ca="TokenA",
        symbol="A",
        signal_ts=1_030,
        receive_ts=int(time.time()),
        entry_branch="hard_gate_fast_clean",
        priority=10,
    )

    assert first is True
    assert second is False
    assert db.execute("SELECT COUNT(*) FROM paper_fast_entry_queue").fetchone()[0] == 1


def test_fast_queue_upgrades_existing_token_priority(tmp_path):
    db_path = tmp_path / "paper.db"
    db = fast.connect_db(db_path)
    fast.init_fast_lane_schema(db)

    assert fast.enqueue_fast_entry(
        db,
        source_type="source_resonance_fast",
        token_ca="TokenA",
        signal_ts=1_000,
        receive_ts=int(time.time()),
        entry_branch="source_resonance_gmgn_fast",
        priority=18,
    )
    assert not fast.enqueue_fast_entry(
        db,
        source_type="hard_gate_fast",
        token_ca="TokenA",
        signal_ts=1_001,
        receive_ts=int(time.time()),
        entry_branch="hard_gate_fast_clean",
        priority=10,
    )

    row = db.execute("SELECT priority, source_type, entry_branch FROM paper_fast_entry_queue").fetchone()
    assert row["priority"] == 10
    assert row["source_type"] == "hard_gate_fast"
    assert row["entry_branch"] == "hard_gate_fast_clean"


def test_queue_pressure_skips_low_priority_but_keeps_high_priority(tmp_path, monkeypatch):
    db_path = tmp_path / "paper.db"
    db = fast.connect_db(db_path)
    fast.init_fast_lane_schema(db)
    monkeypatch.setattr(fast, "FAST_ENTRY_MAX_QUEUE_DEPTH", 1)

    assert fast.enqueue_fast_entry(
        db,
        source_type="hard_gate_fast",
        token_ca="TokenA",
        signal_ts=1_000,
        receive_ts=int(time.time()),
        entry_branch="hard_gate_fast_clean",
        priority=10,
    )
    assert not fast.enqueue_fast_entry(
        db,
        source_type="ttl_rescue_fast",
        token_ca="TokenB",
        signal_ts=1_001,
        receive_ts=int(time.time()),
        entry_branch="tracking_ttl_expired",
        priority=35,
    )
    assert fast.enqueue_fast_entry(
        db,
        source_type="hard_gate_fast",
        token_ca="TokenC",
        signal_ts=1_002,
        receive_ts=int(time.time()),
        entry_branch="hard_gate_fast_clean",
        priority=10,
    )

    assert db.execute("SELECT COUNT(*) FROM paper_fast_entry_queue").fetchone()[0] == 2


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


def test_entry_guard_keeps_original_signal_age_separate_from_fast_lane_sla():
    row = {
        "source_type": "source_resonance_fast",
        "entry_branch": "source_resonance_quote_clean_fast",
        "source_signal_ts": 1_000,
        "signal_receive_ts": 1_000,
        "created_at": 2_000,
        "trigger_price": 1.0,
        "hard_gate_status": "PASS",
    }

    detail = fast.entry_guard_detail(
        row,
        1.0,
        quote_request_ts_ms=2_001_000,
        quote_response_ts_ms=2_001_200,
    )

    assert detail["pass"] is True
    assert detail["signal_to_quote_latency_ms"] == 1_000
    assert detail["fast_lane_sla_latency_ms"] == 1_000
    assert detail["original_signal_to_quote_latency_ms"] == 1_001_000


def test_open_position_cap_blocks_when_fast_lane_positions_are_full(tmp_path, monkeypatch):
    db_path = tmp_path / "paper.db"
    db = fast.connect_db(db_path)
    db.execute("CREATE TABLE paper_trades (replay_source TEXT, exit_reason TEXT)")
    db.execute("INSERT INTO paper_trades(replay_source, exit_reason) VALUES ('paper_fast_lane', NULL)")
    db.commit()
    monkeypatch.setattr(fast, "FAST_ENTRY_MAX_OPEN_POSITIONS", 1)

    assert fast.open_position_cap_allows(db) is False


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


def test_gmgn_only_source_resonance_is_watch_only_by_default():
    detail = fast.direct_fill_policy({
        "source_type": "source_resonance_fast",
        "entry_branch": "source_resonance_gmgn_fast",
        "payload_json": json.dumps({"gmgn_pre_seen": 1}),
    })

    assert detail["pass"] is False
    assert detail["status"] == "watch_only"
    assert detail["reason"] == "source_resonance_gmgn_only_watch_only"


def test_quote_clean_source_requires_activity_confirmation(monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_SOURCE_QUOTE_CLEAN_ACTIVITY_REQUIRED", True)
    now = int(time.time())
    stale_free_payload = {
        "quote_clean_seen": 1,
        "source_updated_at": "2099-01-01 00:00:00",
        "original_signal_ts": now,
    }

    blocked = fast.direct_fill_policy({
        "source_type": "source_resonance_fast",
        "entry_branch": "source_resonance_quote_clean_fast",
        "payload_json": json.dumps(stale_free_payload),
    }, now_ts=now)
    allowed = fast.direct_fill_policy({
        "source_type": "source_resonance_fast",
        "entry_branch": "source_resonance_quote_clean_fast",
        "payload_json": json.dumps({**stale_free_payload, "gmgn_momentum_confirmed": 1}),
    }, now_ts=now)

    assert blocked["pass"] is False
    assert blocked["reason"] == "source_quote_clean_activity_not_confirmed"
    assert allowed["pass"] is True


def test_quote_clean_source_rejects_stale_original_signal(monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_SOURCE_QUOTE_CLEAN_MAX_ORIGINAL_AGE_SEC", 180)
    now = int(time.time())
    detail = fast.direct_fill_policy({
        "source_type": "source_resonance_fast",
        "entry_branch": "source_resonance_quote_clean_fast",
        "payload_json": json.dumps({
            "quote_clean_seen": 1,
            "source_updated_at": "2099-01-01 00:00:00",
            "original_signal_ts": now - 181,
            "gmgn_momentum_confirmed": 1,
        }),
    }, now_ts=now)

    assert detail["pass"] is False
    assert detail["status"] == "watch_only"
    assert detail["reason"] == "source_quote_clean_original_signal_stale_watch_only"


def test_ttl_rescue_requires_fresh_tradable_timestamp(monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_TTL_RESCUE_MAX_TRADABLE_AGE_SEC", 300)
    now = int(time.time())
    stale = fast.direct_fill_policy({
        "source_type": "ttl_rescue_fast",
        "entry_branch": "tracking_ttl_expired",
        "payload_json": json.dumps({"first_tradable_ts": now - 301}),
    }, now_ts=now)
    fresh = fast.direct_fill_policy({
        "source_type": "ttl_rescue_fast",
        "entry_branch": "tracking_ttl_expired",
        "payload_json": json.dumps({"first_tradable_ts": now - 60}),
    }, now_ts=now)

    assert stale["pass"] is False
    assert stale["reason"] == "ttl_rescue_tradable_signal_stale_watch_only"
    assert fresh["pass"] is True


def test_kline_rescue_is_counterfactual_only_by_default():
    detail = fast.direct_fill_policy({
        "source_type": "kline_retry_reclaim_fast",
        "entry_branch": "not_ath_prebuy_kline_retry_expired",
        "payload_json": "{}",
    })

    assert detail["pass"] is False
    assert detail["status"] == "counterfactual_only"
    assert detail["reason"] == "kline_rescue_direct_fill_disabled"


def test_watch_observation_is_not_claimed(tmp_path):
    db_path = tmp_path / "paper.db"
    db = fast.connect_db(db_path)
    fast.init_fast_lane_schema(db)
    assert fast.record_fast_lane_observation(
        db,
        source_type="source_resonance_fast",
        token_ca="TokenWatch",
        signal_ts=int(time.time()),
        entry_branch="source_resonance_gmgn_fast",
        status="watch_only",
        reason="source_resonance_gmgn_only_watch_only",
    )

    assert fast.claim_queue_item(db, "worker-1") is None
    row = db.execute("SELECT status, last_error FROM paper_fast_entry_queue").fetchone()
    assert row["status"] == "watch_only"
    assert row["last_error"] == "source_resonance_gmgn_only_watch_only"

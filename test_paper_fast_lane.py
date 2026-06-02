import sqlite3
import time
import json
import datetime as dt
import threading

from scripts import paper_fast_lane as fast
from scripts.sqlite_write_coordinator import SQLiteSingleWriterLock


def write_mode_readiness(path, *, highest_allowed_mode="normal_tiny", blocked_modes=()):
    blocked = set(blocked_modes)
    modes = {}
    for mode in ("observe_only", "shadow", "ultra_tiny", "normal_tiny"):
        modes[mode] = {
            "status": "blocked" if mode in blocked else "allowed",
            "blocking_contracts": ["UnitBlockingContract"] if mode in blocked else [],
        }
    payload = {
        "matrix_schema_version": "v2.7.0.mode_readiness.v1",
        "highest_allowed_mode": highest_allowed_mode,
        "health": {
            "status": "mode_readiness_evaluated",
            "observe_only_ready": True,
            "shadow_ready": True,
            "ultra_tiny_ready": highest_allowed_mode in ("ultra_tiny", "normal_tiny"),
            "normal_tiny_ready": highest_allowed_mode == "normal_tiny",
        },
        "modes": modes,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


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


def test_fast_queue_promotes_recent_watch_observation_to_queued(tmp_path):
    db_path = tmp_path / "paper.db"
    db = fast.connect_db(db_path)
    fast.init_fast_lane_schema(db)
    now = int(time.time())

    assert fast.record_fast_lane_observation(
        db,
        source_type="source_resonance_fast",
        token_ca="TokenWatchUpgrade",
        symbol="UP",
        signal_ts=now,
        receive_ts=now,
        entry_branch="source_resonance_gmgn_fast",
        priority=18,
        status="watch_only",
        reason="source_resonance_gmgn_only_watch_only",
        now_ts=now,
    )
    promoted = fast.enqueue_fast_entry(
        db,
        source_type="ttl_final_reclaim_fast",
        token_ca="TokenWatchUpgrade",
        symbol="UP",
        signal_ts=now + 1,
        receive_ts=now + 1,
        entry_branch="tracking_ttl_reclaim_quote_clean_tiny_probe",
        entry_mode_hint="lotto_not_ath_reclaim_tiny_probe",
        priority=18,
        payload={"direct_fill_reason": "tracking_ttl_reclaim_quote_clean_tiny_probe"},
        now_ts=now + 1,
    )

    assert promoted is True
    row = db.execute(
        """
        SELECT status, priority, source_type, entry_branch, entry_mode_hint, last_error
        FROM paper_fast_entry_queue
        WHERE token_ca = 'TokenWatchUpgrade'
        """
    ).fetchone()
    assert row["status"] == "queued"
    assert row["priority"] == 18
    assert row["source_type"] == "ttl_final_reclaim_fast"
    assert row["entry_branch"] == "tracking_ttl_reclaim_quote_clean_tiny_probe"
    assert row["entry_mode_hint"] == "lotto_not_ath_reclaim_tiny_probe"
    assert row["last_error"] is None
    claimed = fast.claim_queue_item(db, "worker-1")
    assert claimed is not None
    assert claimed["token_ca"] == "TokenWatchUpgrade"


def test_fast_queue_reactivates_old_reclaim_watch_observation_queue_key(tmp_path):
    db_path = tmp_path / "paper.db"
    db = fast.connect_db(db_path)
    fast.init_fast_lane_schema(db)
    now = int(time.time())
    signal_ts = now - 600

    assert fast.record_fast_lane_observation(
        db,
        source_type="ttl_final_reclaim_fast",
        token_ca="TokenOldWatch",
        symbol="OLD",
        signal_ts=signal_ts,
        receive_ts=signal_ts,
        entry_branch="tracking_ttl_reclaim_quote_clean_tiny_probe",
        entry_mode_hint="lotto_not_ath_reclaim_tiny_probe",
        priority=18,
        status="watch_only",
        reason="clean_dog_reclaim_recovery_tradable_signal_stale_watch_only",
        now_ts=now - fast.FAST_ENTRY_QUEUE_DEDUPE_SEC - 10,
    )

    promoted = fast.enqueue_fast_entry(
        db,
        source_type="ttl_final_reclaim_fast",
        token_ca="TokenOldWatch",
        symbol="OLD",
        signal_ts=signal_ts,
        receive_ts=signal_ts,
        entry_branch="tracking_ttl_reclaim_quote_clean_tiny_probe",
        entry_mode_hint="lotto_not_ath_reclaim_tiny_probe",
        priority=fast.FAST_ENTRY_CLEAN_DOG_RECLAIM_PRIORITY,
        payload={"direct_fill_reason": "tracking_ttl_reclaim_quote_clean_tiny_probe"},
        now_ts=now,
    )

    assert promoted is True
    row = db.execute(
        """
        SELECT status, priority, source_type, entry_branch, entry_mode_hint,
               last_error, first_error, status_history_json
        FROM paper_fast_entry_queue
        WHERE token_ca = 'TokenOldWatch'
        """
    ).fetchone()
    assert row["status"] == "queued"
    assert row["priority"] == fast.FAST_ENTRY_CLEAN_DOG_RECLAIM_PRIORITY
    assert row["source_type"] == "ttl_final_reclaim_fast"
    assert row["entry_branch"] == "tracking_ttl_reclaim_quote_clean_tiny_probe"
    assert row["entry_mode_hint"] == "lotto_not_ath_reclaim_tiny_probe"
    assert row["last_error"] is None
    assert row["first_error"] == "clean_dog_reclaim_recovery_tradable_signal_stale_watch_only"
    history = json.loads(row["status_history_json"])
    assert history[-1]["status"] == "queued"


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


def test_process_queue_item_respects_entry_mode_quality_shadow(tmp_path, monkeypatch):
    monkeypatch.setenv("V27_MODE_READINESS_PATH", str(write_mode_readiness(tmp_path / "mode_readiness.json")))
    db_path = tmp_path / "paper.db"
    db = fast.connect_db(db_path)
    fast.init_fast_lane_schema(db)
    now = int(time.time())
    entry_mode = "pre_pass_resonance_tiny_probe"
    db.execute(
        """
        CREATE TABLE paper_trades (
            entry_mode TEXT,
            entry_branch TEXT,
            peak_pnl REAL,
            pnl_pct REAL,
            replay_source TEXT,
            entry_ts INTEGER,
            exit_ts INTEGER
        )
        """
    )
    pnls = [-0.40, -0.35, -0.05, -0.04, -0.03, 0.01, 0.02, 0.04]
    for idx, pnl in enumerate(pnls):
        db.execute(
            """
            INSERT INTO paper_trades(entry_mode, entry_branch, peak_pnl, pnl_pct, replay_source, entry_ts, exit_ts)
            VALUES (?, 'not_ath_reclaim_quote_clean_tiny_probe', 0.20, ?, 'paper_fast_lane', ?, ?)
            """,
            (entry_mode, pnl, now - idx, now - idx + 1),
        )
    db.commit()
    assert fast.enqueue_fast_entry(
        db,
        source_type="missing_quote_recovery_fast",
        token_ca="TokenQuality",
        symbol="QUAL",
        signal_ts=now,
        receive_ts=now,
        entry_branch="missing_trigger_or_quote",
        entry_mode_hint=entry_mode,
        now_ts=now,
    )
    row = fast.claim_queue_item(db, "worker-1")

    fast.process_queue_item(db, row, "worker-1")

    queue = db.execute(
        "SELECT status, last_error FROM paper_fast_entry_queue WHERE token_ca = 'TokenQuality'"
    ).fetchone()
    assert queue["status"] == "watch_only"
    assert queue["last_error"] == "entry_mode_quality_shadow_only_mode"


def test_process_queue_item_blocks_when_v27_mode_readiness_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("V27_MODE_READINESS_PATH", str(tmp_path / "missing_mode_readiness.json"))
    db_path = tmp_path / "paper.db"
    db = fast.connect_db(db_path)
    fast.init_fast_lane_schema(db)
    now = int(time.time())
    assert fast.enqueue_fast_entry(
        db,
        source_type="missing_quote_recovery_fast",
        token_ca="TokenModeGate",
        symbol="MODE",
        signal_ts=now,
        receive_ts=now,
        entry_branch="missing_trigger_or_quote",
        entry_mode_hint="pre_pass_resonance_tiny_probe",
        now_ts=now,
    )
    row = fast.claim_queue_item(db, "worker-1")

    fast.process_queue_item(db, row, "worker-1")

    queue = db.execute(
        "SELECT status, last_error FROM paper_fast_entry_queue WHERE token_ca = 'TokenModeGate'"
    ).fetchone()
    assert queue["status"] == "watch_only"
    assert queue["last_error"] == "v27_mode_readiness_missing"


def test_process_queue_item_enters_lotto_not_ath_reclaim_when_gates_pass(tmp_path, monkeypatch):
    monkeypatch.setenv("V27_MODE_READINESS_PATH", str(write_mode_readiness(tmp_path / "mode_readiness.json")))
    fast.evaluate_entry_mode_quality.__globals__["_SHADOW_UNTIL"].clear()
    db_path = tmp_path / "paper.db"
    db = fast.ptm.init_paper_db(str(db_path))
    fast.init_fast_lane_schema(db)
    now = int(time.time())

    def fake_execution(*_args, **_kwargs):
        return {
            "success": True,
            "effectivePrice": 0.000001,
            "quotedOutAmountRaw": "2000000",
            "outputDecimals": 6,
            "quoteTs": now * 1000,
            "routePlan": [],
        }

    def fake_markov_forecast(_db, pending, **_kwargs):
        forecast = {
            "entry_mode": pending.get("entry_mode"),
            "policy_version": fast.ptm.LOTTO_RECLAIM_MARKOV_POLICY_VERSION,
            "sample_n": fast.ptm.LOTTO_MICRO_RECLAIM_MARKOV_MIN_SAMPLE_N,
            "p_absorb_peak30": 0.30,
            "p_absorb_stop_before_peak": 0.10,
        }
        forecast["gate"] = fast.ptm._lotto_reclaim_markov_gate(pending.get("entry_mode"), forecast)
        return forecast

    monkeypatch.setattr(fast.ptm, "simulate_entry_execution", fake_execution)
    monkeypatch.setattr(fast.ptm, "attach_lotto_reclaim_markov_forecast", fake_markov_forecast)
    assert fast.enqueue_fast_entry(
        db,
        source_type="ttl_final_reclaim_fast",
        token_ca="TokenEnter",
        symbol="ENT",
        signal_ts=now - 1200,
        receive_ts=now - 1200,
        entry_branch="tracking_ttl_reclaim_quote_clean_tiny_probe",
        entry_mode_hint="lotto_not_ath_reclaim_tiny_probe",
        trigger_price=0.000001,
        priority=fast.FAST_ENTRY_CLEAN_DOG_RECLAIM_PRIORITY,
        payload={
            "tradable_missed": 1,
            "would_stop_before_peak": 0,
            "recovery_quote_clean": True,
            "last_tradable_ts": now,
            "last_clean_quote_ts": now,
            "executable_peak_pnl": 0.8,
            "route": "LOTTO",
        },
        now_ts=now,
    )
    row = fast.claim_queue_item(db, "worker-1")

    fast.process_queue_item(db, row, "worker-1")

    queue = db.execute(
        "SELECT status, last_error FROM paper_fast_entry_queue WHERE token_ca = 'TokenEnter'"
    ).fetchone()
    trade = db.execute(
        """
        SELECT replay_source, entry_mode, entry_branch, signal_route, position_size_sol
        FROM paper_trades
        WHERE token_ca = 'TokenEnter'
        """
    ).fetchone()
    assert queue["status"] == "entered"
    assert queue["last_error"] is None
    assert trade["replay_source"] == "paper_fast_lane"
    assert trade["entry_mode"] == "lotto_not_ath_reclaim_tiny_probe"
    assert trade["entry_branch"] == "tracking_ttl_reclaim_quote_clean_tiny_probe"
    assert trade["signal_route"] == "ttl_final_reclaim_fast"
    assert trade["position_size_sol"] == fast.ptm.PAPER_TINY_SCOUT_SIZE_SOL


def test_lotto_not_ath_watch_canary_scan_queues_strict_would_enter(tmp_path, monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_NOT_ATH_WATCH_MARKOV_CANARY_ENABLED", True)
    db_path = tmp_path / "paper.db"
    db = fast.ptm.init_paper_db(str(db_path))
    fast.init_fast_lane_schema(db)
    now = int(time.time())
    signal_ts = now - 2400
    latest_snapshot = {
        "snapshot_ts": now - 5,
        "quote_price": 0.0000011,
        "mark_price": 0.00000108,
        "quote_gap_pct": 1.8,
        "spread_pct": 1.8,
        "liquidity_usd": 18000,
        "volume_m5": 12000,
        "tx_m5": 80,
        "buy_sell_ratio": 1.4,
        "price_change_m5": 6.0,
        "quote_clean": True,
        "activity_reclaim": True,
        "volume_reclaim": True,
        "momentum_reclaim": True,
        "snapshot_pass": True,
    }
    fast.ptm.record_decision_event(
        db,
        component="lotto_not_ath_watch_shadow",
        event_type="would_enter",
        decision="WOULD_ENTER",
        reason="not_ath_two_snapshot_quote_clean_reclaim_confirmed",
        token_ca="TokenWatchCanary",
        symbol="WAT",
        lifecycle_id=fast.ptm.build_lifecycle_id("TokenWatchCanary", signal_ts),
        signal_ts=signal_ts,
        signal_id=123,
        route="LOTTO",
        data_source="test",
        payload={
            "source_reject_reason": "lotto_stale_2140s",
            "parent_blocker": "lotto_stale",
            "baseline_price": 0.000001,
            "tradable_peak_pnl": 1.2,
            "would_stop_before_peak": 0,
            "confirmation": {
                "latest_snapshot": latest_snapshot,
                "confirming_snapshots": [
                    {**latest_snapshot, "snapshot_ts": now - 305, "horizon_sec": 1800},
                    {**latest_snapshot, "snapshot_ts": now - 5, "horizon_sec": 2100},
                ],
            },
            "latest_snapshot": latest_snapshot,
        },
        event_ts=now - 4,
    )

    result = fast.scan_lotto_not_ath_watch_canary_once(db, now_ts=now, limit=5)

    assert result["queued"] == 1
    row = db.execute(
        """
        SELECT status, source_type, entry_branch, entry_mode_hint, trigger_price, payload_json
        FROM paper_fast_entry_queue
        WHERE token_ca = 'TokenWatchCanary'
        """
    ).fetchone()
    queued_payload = json.loads(row["payload_json"])
    assert row["status"] == "queued"
    assert row["source_type"] == fast.LOTTO_NOT_ATH_WATCH_CANARY_SOURCE_TYPE
    assert row["entry_branch"] == "not_ath_reclaim_quote_clean_tiny_probe"
    assert row["entry_mode_hint"] == "lotto_not_ath_reclaim_tiny_probe"
    assert row["trigger_price"] == latest_snapshot["quote_price"]
    assert queued_payload["watch_mode"] == "markov_green_canary"
    assert queued_payload["live_entry_enabled"] is True
    assert queued_payload["clean_dog_reclaim_eligibility"]["direct_reclaim_ok"] is True
    audit = db.execute(
        """
        SELECT event_type, decision
        FROM paper_decision_events
        WHERE component = ?
          AND token_ca = 'TokenWatchCanary'
        ORDER BY id DESC
        LIMIT 1
        """,
        (fast.LOTTO_NOT_ATH_WATCH_CANARY_COMPONENT,),
    ).fetchone()
    assert audit["event_type"] == "entry_queued"
    assert audit["decision"] == "queue"


def test_lotto_not_ath_watch_canary_scan_blocks_without_clean_snapshot(tmp_path, monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_NOT_ATH_WATCH_MARKOV_CANARY_ENABLED", True)
    db_path = tmp_path / "paper.db"
    db = fast.ptm.init_paper_db(str(db_path))
    fast.init_fast_lane_schema(db)
    now = int(time.time())
    signal_ts = now - 2400
    fast.ptm.record_decision_event(
        db,
        component="lotto_not_ath_watch_shadow",
        event_type="would_enter",
        decision="WOULD_ENTER",
        reason="not_ath_two_snapshot_quote_clean_reclaim_confirmed",
        token_ca="TokenWatchBlocked",
        symbol="BLK",
        lifecycle_id=fast.ptm.build_lifecycle_id("TokenWatchBlocked", signal_ts),
        signal_ts=signal_ts,
        route="LOTTO",
        data_source="test",
        payload={
            "source_reject_reason": "not_ath_prebuy_kline_block",
            "confirmation": {"latest_snapshot": {"snapshot_ts": now - 5, "quote_clean": False}},
        },
        event_ts=now - 4,
    )

    result = fast.scan_lotto_not_ath_watch_canary_once(db, now_ts=now, limit=5)

    assert result["blocked"] == 1
    assert db.execute("SELECT COUNT(*) FROM paper_fast_entry_queue").fetchone()[0] == 0
    audit = db.execute(
        """
        SELECT event_type, decision, reason
        FROM paper_decision_events
        WHERE component = ?
          AND token_ca = 'TokenWatchBlocked'
        ORDER BY id DESC
        LIMIT 1
        """,
        (fast.LOTTO_NOT_ATH_WATCH_CANARY_COMPONENT,),
    ).fetchone()
    assert audit["event_type"] == "entry_block"
    assert audit["decision"] == "watch_only"
    assert audit["reason"] == "clean_dog_reclaim_recovery_quote_clean_missing"


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


def test_retry_watch_expiry_preserves_first_failure_reason(tmp_path, monkeypatch):
    db_path = tmp_path / "paper.db"
    db = fast.connect_db(db_path)
    fast.init_fast_lane_schema(db)
    now = time.time()
    monkeypatch.setattr(fast, "FAST_ENTRY_MAX_QUEUE_AGE_SEC", 20)
    assert fast.enqueue_fast_entry(
        db,
        source_type="hard_gate_fast",
        token_ca="TokenReason",
        symbol="RSN",
        signal_ts=int(now),
        receive_ts=int(now),
        entry_branch="hard_gate_fast_clean",
        now_ts=now,
    )
    row = fast.claim_queue_item(db, "worker-1")
    fast.mark_queue(db, row["id"], "retry_watch", "entry_quote_failed_429")
    db.execute(
        "UPDATE paper_fast_entry_queue SET created_at = ?, updated_at = ? WHERE id = ?",
        (now - 25, now - 25, row["id"]),
    )
    db.commit()

    fast.refresh_retry_watch(db, now_ts=now)
    final = db.execute(
        "SELECT status, last_error, first_error, status_history_json FROM paper_fast_entry_queue WHERE id = ?",
        (row["id"],),
    ).fetchone()

    assert final["status"] == "expired"
    assert final["last_error"] == "fast_lane_retry_watch_expired"
    assert final["first_error"] == "entry_quote_failed_429"
    history = json.loads(final["status_history_json"])
    assert any(event["status"] == "retry_watch" and event["error"] == "entry_quote_failed_429" for event in history)


def test_fast_queue_records_market_session(tmp_path):
    db_path = tmp_path / "paper.db"
    db = fast.connect_db(db_path)
    fast.init_fast_lane_schema(db)
    now = int(time.time())

    assert fast.enqueue_fast_entry(
        db,
        source_type="hard_gate_fast",
        token_ca="TokenSession",
        symbol="SES",
        signal_ts=now,
        receive_ts=now,
        entry_branch="hard_gate_fast_clean",
    )

    row = db.execute("SELECT market_session FROM paper_fast_entry_queue").fetchone()
    assert row["market_session"] in {"asia", "europe", "us", "quiet"}


def test_gmgn_only_source_resonance_is_watch_only_by_default():
    detail = fast.direct_fill_policy({
        "source_type": "source_resonance_fast",
        "entry_branch": "source_resonance_gmgn_fast",
        "payload_json": json.dumps({"gmgn_pre_seen": 1}),
    })

    assert detail["pass"] is False
    assert detail["status"] == "watch_only"
    assert detail["reason"] == "source_resonance_gmgn_only_watch_only"
    assert detail["detail"]["shadow_entry"] is True
    assert detail["detail"]["required_next_state"] == "telegram_gmgn_quote_clean"


def test_quote_clean_source_requires_activity_confirmation(monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_SOURCE_QUOTE_CLEAN_ACTIVITY_REQUIRED", True)
    now = int(time.time())
    stale_free_payload = {
        "quote_clean_seen": 1,
        "source_updated_at": "2099-01-01 00:00:00",
        "original_signal_ts": now,
        "liquidity_usd": 12000,
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


def test_quote_clean_source_requires_liquidity(monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_SOURCE_QUOTE_CLEAN_ACTIVITY_REQUIRED", True)
    now = int(time.time())
    detail = fast.direct_fill_policy({
        "source_type": "source_resonance_fast",
        "entry_branch": "source_resonance_quote_clean_fast",
        "payload_json": json.dumps({
            "quote_clean_seen": 1,
            "source_updated_at": "2099-01-01 00:00:00",
            "original_signal_ts": now,
            "gmgn_momentum_confirmed": 1,
            "liquidity_usd": 1000,
        }),
    }, now_ts=now)

    assert detail["pass"] is False
    assert detail["status"] == "watch_only"
    assert detail["reason"] == "entry_execution_liquidity_required"
    assert detail["detail"]["liquidity_ok"] is False


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


def test_clean_reclaim_uses_recent_missed_update_as_fresh_anchor(monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_NOT_ATH_RECLAIM_MAX_TRADABLE_AGE_SEC", 300)
    now = int(time.time())
    detail = fast.direct_fill_policy({
        "source_type": "not_ath_reclaim_fast",
        "entry_branch": "not_ath_reclaim_quote_clean_tiny_probe",
        "payload_json": json.dumps({
            "tradable_missed": 1,
            "would_stop_before_peak": 0,
            "first_tradable_ts": now - 1800,
            "missed_updated_at": dt.datetime.utcfromtimestamp(now - 20).strftime("%Y-%m-%d %H:%M:%S"),
        }),
    }, now_ts=now)

    assert detail["pass"] is True
    assert detail["detail"]["tradable_age_sec"] <= 25


def test_not_ath_reclaim_default_window_allows_late_clean_candidate():
    now = int(time.time())
    detail = fast.direct_fill_policy({
        "source_type": "pre_pass_stale_reclaim_fast",
        "entry_branch": "pre_pass_stale_reclaim_quote_clean_tiny_probe",
        "payload_json": json.dumps({
            "tradable_missed": 1,
            "recovery_quote_clean": True,
            "first_tradable_ts": now - 1200,
            "last_clean_quote_ts": now - 1200,
            "would_stop_before_peak": 0,
            "activity_reclaim": True,
            "route": "LOTTO",
        }),
    }, now_ts=now)

    assert detail["pass"] is True
    assert detail["reason"] == "pre_pass_stale_reclaim_quote_clean_tiny_probe"
    assert detail["detail"]["max_tradable_age_sec"] == 1800.0
    assert detail["detail"]["last_tradable_age_sec"] == 1200


def test_kline_rescue_is_counterfactual_only_by_default():
    detail = fast.direct_fill_policy({
        "source_type": "kline_retry_reclaim_fast",
        "entry_branch": "not_ath_prebuy_kline_retry_expired",
        "payload_json": "{}",
    })

    assert detail["pass"] is False
    assert detail["status"] == "counterfactual_only"
    assert detail["reason"] == "kline_rescue_direct_fill_disabled"


def test_gmgn_momentum_canary_allows_confirmed_non_quiet(monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_SOURCE_GMGN_MOMENTUM_CANARY_ENABLED", True)
    now = int(time.time())
    detail = fast.direct_fill_policy({
        "source_type": "source_resonance_fast",
        "entry_branch": "source_gmgn_momentum_canary",
        "payload_json": json.dumps({
            "gmgn_pre_seen": 1,
            "quote_clean_seen": 1,
            "gmgn_momentum_confirmed": 1,
            "resonance_level": 3,
            "market_session": "asia",
            "liquidity_usd": 12000,
        }),
    }, now_ts=now)

    assert detail["pass"] is True
    assert detail["reason"] == "source_gmgn_momentum_canary"


def test_gmgn_momentum_canary_blocks_unconfirmed_or_quiet(monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_SOURCE_GMGN_MOMENTUM_CANARY_ENABLED", True)
    monkeypatch.setattr(fast, "FAST_ENTRY_SOURCE_GMGN_CANARY_QUIET_ENABLED", False)
    now = int(time.time())
    unconfirmed = fast.direct_fill_policy({
        "source_type": "source_resonance_fast",
        "entry_branch": "source_gmgn_momentum_canary",
        "payload_json": json.dumps({
            "gmgn_pre_seen": 1,
            "resonance_level": 3,
            "market_session": "asia",
        }),
    }, now_ts=now)
    quiet = fast.direct_fill_policy({
        "source_type": "source_resonance_fast",
        "entry_branch": "source_gmgn_momentum_canary",
        "payload_json": json.dumps({
            "gmgn_pre_seen": 1,
            "gmgn_momentum_confirmed": 1,
            "resonance_level": 3,
            "market_session": "quiet",
        }),
    }, now_ts=now)

    assert unconfirmed["pass"] is False
    assert unconfirmed["reason"] == "source_gmgn_momentum_canary_unconfirmed"
    assert quiet["pass"] is False
    assert quiet["reason"] == "source_gmgn_momentum_canary_quiet_session"


def test_source_quote_clean_refresh_can_be_disabled_by_flag(monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_SOURCE_QUOTE_CLEAN_REFRESH_ENABLED", False)
    now = int(time.time())
    detail = fast.direct_fill_policy({
        "source_type": "source_resonance_fast",
        "entry_branch": "source_quote_clean_refresh_tiny_probe",
        "payload_json": json.dumps({
            "quote_clean_seen": 1,
            "two_quote_clean_snapshots": 1,
            "gmgn_volume_confirmed": 1,
            "original_signal_ts": now - 60,
        }),
    }, now_ts=now)

    assert detail["pass"] is False
    assert detail["reason"] == "source_quote_clean_refresh_disabled"


def test_source_quote_clean_refresh_default_allows_short_stale_window():
    now = int(time.time())
    detail = fast.direct_fill_policy({
        "source_type": "source_resonance_fast",
        "entry_branch": "source_quote_clean_refresh_tiny_probe",
        "payload_json": json.dumps({
            "quote_clean_seen": 1,
            "two_quote_clean_snapshots": 1,
            "source_updated_at": dt.datetime.utcfromtimestamp(now).strftime("%Y-%m-%d %H:%M:%S"),
            "gmgn_volume_confirmed": 1,
            "original_signal_ts": now - 210,
            "market_session": "asia",
            "liquidity_usd": 12000,
        }),
    }, now_ts=now)

    assert detail["pass"] is True
    assert detail["reason"] == "source_quote_clean_refresh_tiny_probe"
    assert detail["detail"]["original_age_sec"] == 210


def test_source_quote_clean_refresh_canary_requires_non_stale_two_snapshot_activity(monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_SOURCE_QUOTE_CLEAN_REFRESH_ENABLED", True)
    monkeypatch.setattr(fast, "FAST_ENTRY_SOURCE_QUOTE_CLEAN_REFRESH_REQUIRE_TWO_SNAPSHOTS", True)
    monkeypatch.setattr(fast, "FAST_ENTRY_SOURCE_QUOTE_CLEAN_REFRESH_MAX_ORIGINAL_AGE_SEC", 120)
    now = int(time.time())
    allowed = fast.direct_fill_policy({
        "source_type": "source_resonance_fast",
        "entry_branch": "source_quote_clean_refresh_tiny_probe",
        "payload_json": json.dumps({
            "quote_clean_seen": 1,
            "two_quote_clean_snapshots": 1,
            "source_updated_at": "2099-01-01 00:00:00",
            "gmgn_volume_confirmed": 1,
            "original_signal_ts": now - 60,
            "market_session": "asia",
            "liquidity_usd": 12000,
        }),
    }, now_ts=now)
    stale = fast.direct_fill_policy({
        "source_type": "source_resonance_fast",
        "entry_branch": "source_quote_clean_refresh_tiny_probe",
        "payload_json": json.dumps({
            "quote_clean_seen": 1,
            "two_quote_clean_snapshots": 1,
            "source_updated_at": "2099-01-01 00:00:00",
            "gmgn_volume_confirmed": 1,
            "original_signal_ts": now - 900,
            "market_session": "asia",
        }),
    }, now_ts=now)
    missing_activity = fast.direct_fill_policy({
        "source_type": "source_resonance_fast",
        "entry_branch": "source_quote_clean_refresh_tiny_probe",
        "payload_json": json.dumps({
            "quote_clean_seen": 1,
            "two_quote_clean_snapshots": 1,
            "source_updated_at": "2099-01-01 00:00:00",
            "original_signal_ts": now - 60,
            "market_session": "asia",
        }),
    }, now_ts=now)

    assert allowed["pass"] is True
    assert allowed["reason"] == "source_quote_clean_refresh_tiny_probe"
    assert stale["pass"] is False
    assert stale["reason"] == "source_quote_clean_refresh_original_signal_stale"
    assert missing_activity["pass"] is False
    assert missing_activity["reason"] == "source_quote_clean_refresh_activity_not_confirmed"


def test_kline_recovery_canary_requires_fresh_tradable_timestamp(monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_KLINE_RECOVERY_CANARY_ENABLED", True)
    monkeypatch.setattr(fast, "FAST_ENTRY_RECOVERY_MAX_TRADABLE_AGE_SEC", 120)
    now = int(time.time())
    stale = fast.direct_fill_policy({
        "source_type": "kline_recovery_fast",
        "entry_branch": "kline_recovery_quote_clean_tiny_probe",
        "payload_json": json.dumps({"tradable_missed": 1, "first_tradable_ts": now - 121}),
    }, now_ts=now)
    missing_strong = fast.direct_fill_policy({
        "source_type": "kline_recovery_fast",
        "entry_branch": "kline_recovery_quote_clean_tiny_probe",
        "payload_json": json.dumps({"tradable_missed": 1, "first_tradable_ts": now - 30}),
    }, now_ts=now)
    fresh = fast.direct_fill_policy({
        "source_type": "kline_recovery_fast",
        "entry_branch": "kline_recovery_quote_clean_tiny_probe",
        "payload_json": json.dumps({
            "tradable_missed": 1,
            "first_tradable_ts": now - 30,
            "strong_signal_seen": 1,
        }),
    }, now_ts=now)

    assert stale["pass"] is False
    assert stale["status"] == "counterfactual_only"
    assert stale["reason"] == "kline_recovery_tradable_signal_stale_watch_only"
    assert missing_strong["pass"] is False
    assert missing_strong["reason"] == "kline_recovery_strong_signal_missing"
    assert fresh["pass"] is True
    assert fresh["reason"] == "kline_recovery_quote_clean_tiny_probe"


def test_not_ath_reclaim_canary_allows_clean_fresh_reclaim(monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_NOT_ATH_RECLAIM_CANARY_ENABLED", True)
    monkeypatch.setattr(fast, "FAST_ENTRY_NOT_ATH_RECLAIM_MAX_TRADABLE_AGE_SEC", 300)
    now = int(time.time())
    stopped = fast.direct_fill_policy({
        "source_type": "not_ath_reclaim_fast",
        "entry_branch": "not_ath_reclaim_quote_clean_tiny_probe",
        "payload_json": json.dumps({
            "tradable_missed": 1,
            "first_tradable_ts": now - 30,
            "would_stop_before_peak": 1,
        }),
    }, now_ts=now)
    fresh = fast.direct_fill_policy({
        "source_type": "not_ath_reclaim_fast",
        "entry_branch": "not_ath_reclaim_quote_clean_tiny_probe",
        "payload_json": json.dumps({
            "tradable_missed": 1,
            "first_tradable_ts": now - 30,
            "would_stop_before_peak": 0,
        }),
    }, now_ts=now)

    assert stopped["pass"] is False
    assert stopped["reason"] == "clean_dog_reclaim_stop_before_peak_watch_only"
    assert fresh["pass"] is True
    assert fresh["reason"] == "not_ath_reclaim_quote_clean_tiny_probe"


def test_clean_dog_reclaim_requires_fresh_clean_quote_not_rescue_created(monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_NOT_ATH_RECLAIM_CANARY_ENABLED", True)
    monkeypatch.setattr(fast, "FAST_ENTRY_NOT_ATH_RECLAIM_MAX_TRADABLE_AGE_SEC", 300)
    now = int(time.time())

    stale = fast.direct_fill_policy({
        "source_type": "not_ath_reclaim_fast",
        "entry_branch": "not_ath_reclaim_quote_clean_tiny_probe",
        "payload_json": json.dumps({
            "tradable_missed": 1,
            "recovery_quote_clean": True,
            "first_tradable_ts": now - 900,
            "rescue_created_ts": now,
            "would_stop_before_peak": 0,
            "executable_peak_pnl": 0.8,
        }),
    }, now_ts=now)

    assert stale["pass"] is False
    assert stale["status"] == "watch_only"
    assert stale["reason"] == "clean_dog_reclaim_recovery_tradable_signal_stale_watch_only"
    assert stale["detail"]["last_tradable_fresh_ok"] is False


def test_branch_circuit_downgrades_negative_ev_session(tmp_path, monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_BRANCH_CIRCUIT_ENABLED", True)
    monkeypatch.setattr(fast, "FAST_ENTRY_BRANCH_CIRCUIT_MIN_CLOSED", 20)
    monkeypatch.setattr(fast, "FAST_ENTRY_BRANCH_CIRCUIT_AVG_PNL_FLOOR", -0.03)
    db = fast.connect_db(tmp_path / "paper.db")
    db.execute(
        """
        CREATE TABLE paper_trades (
            entry_branch TEXT,
            pnl_pct REAL,
            trusted_peak_pnl REAL,
            entry_ts INTEGER,
            exit_ts INTEGER,
            signal_ts INTEGER
        )
        """
    )
    # 15:00 UTC is the US bucket. This should not contaminate Asia/Europe.
    us_ts = 1_779_116_400
    db.executemany(
        """
        INSERT INTO paper_trades(entry_branch, pnl_pct, trusted_peak_pnl, entry_ts, exit_ts, signal_ts)
        VALUES ('source_quote_clean_refresh_tiny_probe', ?, ?, ?, ?, ?)
        """,
        [(-0.06, 0.0, us_ts, us_ts + 60, us_ts) for _ in range(20)],
    )
    db.commit()

    us = fast.branch_circuit_detail(
        db,
        "source_quote_clean_refresh_tiny_probe",
        market_session="us",
        now_ts=us_ts + 120,
    )
    asia = fast.branch_circuit_detail(
        db,
        "source_quote_clean_refresh_tiny_probe",
        market_session="asia",
        now_ts=us_ts + 120,
    )

    assert us["pass"] is False
    assert us["reason"] == "branch_circuit_negative_ev"
    assert us["closed_n"] == 20
    assert us["avg_pnl"] < -0.03
    assert asia["pass"] is True
    assert asia["closed_n"] == 0


def test_branch_circuit_learning_bypass_allows_markov_green_not_ath_canary(monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_BRANCH_CIRCUIT_LEARNING_BYPASS_ENABLED", True)
    monkeypatch.setattr(
        fast,
        "FAST_ENTRY_BRANCH_CIRCUIT_LEARNING_BYPASS_BRANCHES",
        {"not_ath_reclaim_quote_clean_tiny_probe"},
    )
    monkeypatch.setattr(
        fast,
        "FAST_ENTRY_BRANCH_CIRCUIT_LEARNING_BYPASS_REASONS",
        {"branch_circuit_catastrophic_loss"},
    )
    monkeypatch.setattr(fast, "FAST_ENTRY_BRANCH_CIRCUIT_LEARNING_BYPASS_BUCKETS", {"green"})
    monkeypatch.setattr(fast, "FAST_ENTRY_BRANCH_CIRCUIT_LEARNING_BYPASS_MAX_SIZE_SOL", 0.003)

    detail = fast.branch_circuit_learning_bypass_detail(
        "not_ath_reclaim_quote_clean_tiny_probe",
        fast.ptm.LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE,
        {"pass": False, "reason": "branch_circuit_catastrophic_loss", "max_loss": -0.92},
        markov_reclaim_forecast={
            "gate": {
                "pass": True,
                "markov_bucket": "green",
                "reason": "lotto_reclaim_cohort_markov_green",
            }
        },
        entry_size_sol=0.003,
    )

    assert detail["pass"] is True
    assert detail["reason"] == "branch_circuit_learning_bypass_markov_green_tiny_canary"
    assert detail["paper_only"] is True
    assert detail["markov_bucket"] == "green"


def test_branch_circuit_learning_bypass_allows_markov_green_lotto_micro_canary(monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_BRANCH_CIRCUIT_LEARNING_BYPASS_ENABLED", True)
    monkeypatch.setattr(
        fast,
        "FAST_ENTRY_BRANCH_CIRCUIT_LEARNING_BYPASS_BRANCHES",
        {"smart_entry_reclaim_quote_clean_tiny_probe"},
    )
    monkeypatch.setattr(
        fast,
        "FAST_ENTRY_BRANCH_CIRCUIT_LEARNING_BYPASS_REASONS",
        {"branch_circuit_negative_ev"},
    )
    monkeypatch.setattr(fast, "FAST_ENTRY_BRANCH_CIRCUIT_LEARNING_BYPASS_BUCKETS", {"green"})
    monkeypatch.setattr(fast, "FAST_ENTRY_BRANCH_CIRCUIT_LEARNING_BYPASS_MAX_SIZE_SOL", 0.003)

    detail = fast.branch_circuit_learning_bypass_detail(
        "smart_entry_reclaim_quote_clean_tiny_probe",
        fast.ptm.LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE,
        {"pass": False, "reason": "branch_circuit_negative_ev", "avg_pnl": -0.08},
        markov_reclaim_forecast={
            "gate": {
                "pass": True,
                "markov_bucket": "green",
                "reason": "lotto_reclaim_cohort_markov_green",
            }
        },
        entry_size_sol=0.001,
    )

    assert detail["pass"] is True
    assert detail["reason"] == "branch_circuit_learning_bypass_markov_green_tiny_canary"
    assert detail["paper_only"] is True
    assert detail["markov_bucket"] == "green"


def test_branch_circuit_learning_bypass_default_scope_matches_reclaim_sampling_plan():
    assert {
        "not_ath_reclaim_quote_clean_tiny_probe",
        "tracking_ttl_reclaim_quote_clean_tiny_probe",
        "pre_pass_stale_reclaim_quote_clean_tiny_probe",
        "smart_entry_reclaim_quote_clean_tiny_probe",
    }.issubset(fast.FAST_ENTRY_BRANCH_CIRCUIT_LEARNING_BYPASS_BRANCHES)
    assert {
        "branch_circuit_negative_ev",
        "branch_circuit_tail_loss",
        "branch_circuit_catastrophic_loss",
    }.issubset(fast.FAST_ENTRY_BRANCH_CIRCUIT_LEARNING_BYPASS_REASONS)


def test_branch_circuit_learning_bypass_blocks_red_or_oversized_canary(monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_BRANCH_CIRCUIT_LEARNING_BYPASS_ENABLED", True)
    monkeypatch.setattr(
        fast,
        "FAST_ENTRY_BRANCH_CIRCUIT_LEARNING_BYPASS_BRANCHES",
        {"not_ath_reclaim_quote_clean_tiny_probe"},
    )
    monkeypatch.setattr(
        fast,
        "FAST_ENTRY_BRANCH_CIRCUIT_LEARNING_BYPASS_REASONS",
        {"branch_circuit_catastrophic_loss"},
    )
    monkeypatch.setattr(fast, "FAST_ENTRY_BRANCH_CIRCUIT_LEARNING_BYPASS_BUCKETS", {"green"})
    monkeypatch.setattr(fast, "FAST_ENTRY_BRANCH_CIRCUIT_LEARNING_BYPASS_MAX_SIZE_SOL", 0.003)

    red = fast.branch_circuit_learning_bypass_detail(
        "not_ath_reclaim_quote_clean_tiny_probe",
        fast.ptm.LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE,
        {"pass": False, "reason": "branch_circuit_catastrophic_loss"},
        markov_reclaim_forecast={"gate": {"pass": False, "markov_bucket": "red"}},
        entry_size_sol=0.003,
    )
    oversized = fast.branch_circuit_learning_bypass_detail(
        "not_ath_reclaim_quote_clean_tiny_probe",
        fast.ptm.LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE,
        {"pass": False, "reason": "branch_circuit_catastrophic_loss"},
        markov_reclaim_forecast={"gate": {"pass": True, "markov_bucket": "green"}},
        entry_size_sol=0.01,
    )

    assert red["pass"] is False
    assert red["reason"] == "branch_circuit_learning_bypass_markov_not_green"
    assert oversized["pass"] is False
    assert oversized["reason"] == "branch_circuit_learning_bypass_size_not_tiny"


def test_smart_quality_and_matrix_reclaim_require_fresh_quote_evidence(monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_SMART_QUALITY_RECHECK_CANARY_ENABLED", True)
    monkeypatch.setattr(fast, "FAST_ENTRY_MATRIX_TIMEOUT_CANARY_ENABLED", True)
    now = int(time.time())
    smart = fast.direct_fill_policy({
        "source_type": "smart_quality_reclaim_fast",
        "entry_branch": "smart_quality_reclaim_tiny_probe",
        "payload_json": json.dumps({"tradable_missed": 1, "first_tradable_ts": now - 30}),
    }, now_ts=now)
    matrix = fast.direct_fill_policy({
        "source_type": "matrix_timeout_reclaim_fast",
        "entry_branch": "matrix_timeout_final_quote_tiny_probe",
        "payload_json": json.dumps({"tradable_missed": 1, "first_tradable_ts": now - 30}),
    }, now_ts=now)

    assert smart["pass"] is True
    assert smart["reason"] == "smart_quality_reclaim_tiny_probe"
    assert matrix["pass"] is True
    assert matrix["reason"] == "matrix_timeout_final_quote_tiny_probe"


def test_new_canary_branches_use_degraded_size(monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_SIZE_SOL", 0.002)
    monkeypatch.setattr(fast, "FAST_ENTRY_DEGRADED_SIZE_SOL", 0.001)
    now = int(time.time())
    row = {
        "source_type": "smart_quality_reclaim_fast",
        "entry_branch": "smart_quality_reclaim_tiny_probe",
        "source_signal_ts": now,
        "signal_receive_ts": now,
        "created_at": now,
        "trigger_price": 1.0,
        "hard_gate_status": "PASS",
    }

    detail = fast.entry_guard_detail(
        row,
        1.0,
        quote_request_ts_ms=now * 1000,
        quote_response_ts_ms=now * 1000,
    )

    assert detail["pass"] is True
    assert detail["position_size_sol"] == 0.001
    assert detail["canary_branch"] == "smart_quality_reclaim_tiny_probe"


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


def test_watch_observation_records_shadow_decision_event(tmp_path):
    db_path = tmp_path / "paper.db"
    db = fast.connect_db(db_path)
    fast.init_fast_lane_schema(db)
    fast.ptm.init_decision_audit(db)
    now = int(time.time())

    assert fast.record_fast_lane_observation(
        db,
        source_type="source_resonance_fast",
        token_ca="TokenShadow",
        symbol="SHDW",
        signal_ts=now,
        entry_branch="source_resonance_gmgn_fast",
        source_resonance_cohort="telegram_gmgn",
        status="watch_only",
        reason="source_resonance_gmgn_only_watch_only",
        payload={"gmgn_pre_seen": 1},
        now_ts=now,
    )

    row = db.execute(
        """
        SELECT component, event_type, decision, reason, payload_json
        FROM paper_decision_events
        WHERE token_ca = 'TokenShadow'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    payload = json.loads(row["payload_json"])

    assert row["component"] == "paper_fast_lane"
    assert row["event_type"] == "shadow_observation"
    assert row["decision"] == "shadow"
    assert row["reason"] == "source_resonance_gmgn_only_watch_only"
    assert payload["shadow_entry"] is True
    assert payload["source_resonance_cohort"] == "telegram_gmgn"


def test_premium_scan_reconciles_recent_status_changes(tmp_path, monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_HARD_GATE_DIRECT_ENABLED", True)
    signal_db = fast.connect_db(tmp_path / "signals.db")
    paper_db = fast.connect_db(tmp_path / "paper.db")
    now = int(time.time())
    signal_db.execute(
        """
        CREATE TABLE premium_signals (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            timestamp INTEGER,
            hard_gate_status TEXT,
            signal_type TEXT,
            receive_ts INTEGER,
            created_at INTEGER,
            market_cap REAL,
            description TEXT
        )
        """
    )
    signal_db.execute(
        """
        INSERT INTO premium_signals (
            id, token_ca, symbol, timestamp, hard_gate_status, signal_type,
            receive_ts, created_at, market_cap, description
        ) VALUES (1, 'TokenPass', 'PASSDOG', ?, 'PASS', 'ATH', ?, ?, 12345, '')
        """,
        (now - 5, now - 5, now - 5),
    )
    signal_db.commit()

    result = fast.scan_premium_once(
        signal_db,
        paper_db,
        last_id=1,
        lookback_sec=120,
        now_ts=now,
    )

    assert result["rows"] == 1
    assert result["queued"] == 1
    row = paper_db.execute(
        "SELECT token_ca, source_type, entry_branch, status FROM paper_fast_entry_queue"
    ).fetchone()
    assert row["token_ca"] == "TokenPass"
    assert row["source_type"] == "hard_gate_fast"
    assert row["entry_branch"] == "hard_gate_fast_clean"
    assert row["status"] == "queued"


def test_premium_scan_records_hard_gate_pass_as_counterfactual_when_direct_disabled(tmp_path, monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_HARD_GATE_DIRECT_ENABLED", False)
    signal_db = fast.connect_db(tmp_path / "signals.db")
    paper_db = fast.connect_db(tmp_path / "paper.db")
    now = int(time.time())
    signal_db.execute(
        """
        CREATE TABLE premium_signals (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            timestamp INTEGER,
            hard_gate_status TEXT,
            signal_type TEXT,
            receive_ts INTEGER,
            created_at INTEGER,
            market_cap REAL,
            description TEXT
        )
        """
    )
    signal_db.execute(
        """
        INSERT INTO premium_signals (
            id, token_ca, symbol, timestamp, hard_gate_status, signal_type,
            receive_ts, created_at, market_cap, description
        ) VALUES (1, 'TokenPass', 'PASSDOG', ?, 'PASS', 'ATH', ?, ?, 12345, '')
        """,
        (now - 5, now - 5, now - 5),
    )
    signal_db.commit()

    result = fast.scan_premium_once(
        signal_db,
        paper_db,
        last_id=0,
        lookback_sec=120,
        now_ts=now,
    )

    assert result["rows"] == 1
    assert result["watch_only"] == 1
    row = paper_db.execute(
        "SELECT source_type, entry_branch, status, last_error FROM paper_fast_entry_queue"
    ).fetchone()
    assert row["source_type"] == "hard_gate_fast"
    assert row["entry_branch"] == "hard_gate_fast_clean"
    assert row["status"] == "counterfactual_only"
    assert row["last_error"] == "hard_gate_fast_direct_entry_disabled_counterfactual_only"


def test_premium_scan_records_stale_pass_as_watch_only(tmp_path, monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_RETRY_LATENCY_SEC", 10)
    signal_db = fast.connect_db(tmp_path / "signals.db")
    paper_db = fast.connect_db(tmp_path / "paper.db")
    now = int(time.time())
    signal_db.execute(
        """
        CREATE TABLE premium_signals (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            timestamp INTEGER,
            hard_gate_status TEXT,
            signal_type TEXT,
            receive_ts INTEGER,
            created_at INTEGER,
            market_cap REAL,
            description TEXT
        )
        """
    )
    signal_db.execute(
        """
        INSERT INTO premium_signals (
            id, token_ca, symbol, timestamp, hard_gate_status, signal_type,
            receive_ts, created_at, market_cap, description
        ) VALUES (1, 'TokenStalePass', 'LATE', ?, 'PASS', 'ATH', ?, ?, 12345, '')
        """,
        (now - 60, now - 60, now - 60),
    )
    signal_db.commit()

    result = fast.scan_premium_once(
        signal_db,
        paper_db,
        last_id=1,
        lookback_sec=120,
        now_ts=now,
    )

    assert result["rows"] == 1
    assert result["watch_only"] == 1
    row = paper_db.execute(
        "SELECT status, last_error FROM paper_fast_entry_queue WHERE token_ca = 'TokenStalePass'"
    ).fetchone()
    assert row["status"] == "watch_only"
    assert row["last_error"] == "premium_signal_stale_watch_only"


def test_missed_rescue_scans_tradability_signature_not_only_new_ids(tmp_path, monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_MISSED_RESCUE_LOOKBACK_SEC", 3600)
    monkeypatch.setattr(fast, "FAST_ENTRY_TTL_RESCUE_MAX_TRADABLE_AGE_SEC", 300)
    monkeypatch.setattr(fast, "FAST_ENTRY_MISSED_RESCUE_SCAN_PROCESSED", True)
    db = fast.connect_db(tmp_path / "paper.db")
    fast.init_fast_lane_schema(db)
    now = int(time.time())
    db.execute(
        """
        CREATE TABLE paper_missed_signal_attribution (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            signal_ts INTEGER,
            signal_id INTEGER,
            route TEXT,
            component TEXT,
            reject_reason TEXT,
            baseline_price REAL,
            baseline_ts INTEGER,
            created_event_ts INTEGER,
            first_tradable_ts INTEGER,
            tradable_missed INTEGER,
            executable_peak_pnl REAL,
            updated_at TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution (
            id, token_ca, symbol, signal_ts, signal_id, route, component,
            reject_reason, baseline_price, baseline_ts, created_event_ts,
            first_tradable_ts, tradable_missed, executable_peak_pnl, updated_at
        ) VALUES (
            1, 'TokenMiss', 'MISS', ?, 11, 'ATH', 'discovery_tracking',
            'tracking_ttl_expired', 1.0, ?, ?, ?, 1, 0.7,
            datetime(?, 'unixepoch')
        )
        """,
        (now - 240, now - 240, now - 240, now - 30, now),
    )
    db.commit()

    first = fast.scan_missed_rescue_once(db, now_ts=now)
    second = fast.scan_missed_rescue_once(db, now_ts=now + 1)
    db.execute(
        """
        UPDATE paper_missed_signal_attribution
        SET executable_peak_pnl = 1.2,
            updated_at = datetime(?, 'unixepoch')
        WHERE id = 1
        """,
        (now + 2,),
    )
    db.commit()
    third = fast.scan_missed_rescue_once(db, now_ts=now + 2)

    assert first["processed"] == 1
    assert first["queued"] == 1
    assert second["processed"] == 0
    assert third["processed"] == 1
    assert third["deduped"] == 1
    row = db.execute(
        """
        SELECT rescue_signature, last_status
        FROM paper_fast_missed_rescue_state
        WHERE missed_attribution_id = 1
        """
    ).fetchone()
    assert "1.2" in row["rescue_signature"]
    assert row["last_status"] == "deduped"


def test_missed_rescue_prioritizes_unprocessed_rows_before_deduped_window(tmp_path, monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_MISSED_RESCUE_LOOKBACK_SEC", 3600)
    monkeypatch.setattr(fast, "FAST_ENTRY_MISSED_RESCUE_BACKLOG_LOOKBACK_SEC", 24 * 3600)
    monkeypatch.setattr(fast, "FAST_ENTRY_TTL_RESCUE_MAX_TRADABLE_AGE_SEC", 300)
    db = fast.connect_db(tmp_path / "paper.db")
    fast.init_fast_lane_schema(db)
    now = int(time.time())
    db.execute(
        """
        CREATE TABLE paper_missed_signal_attribution (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            signal_ts INTEGER,
            signal_id INTEGER,
            route TEXT,
            component TEXT,
            reject_reason TEXT,
            baseline_price REAL,
            baseline_ts INTEGER,
            created_event_ts INTEGER,
            first_tradable_ts INTEGER,
            tradable_missed INTEGER,
            would_stop_before_peak INTEGER,
            executable_peak_pnl REAL,
            updated_at TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution (
            id, token_ca, symbol, signal_ts, signal_id, route, component,
            reject_reason, baseline_price, baseline_ts, created_event_ts,
            first_tradable_ts, tradable_missed, would_stop_before_peak,
            executable_peak_pnl, updated_at
        ) VALUES (
            1, 'TokenOldDeduped', 'OLD', ?, 11, 'LOTTO', 'discovery_tracking',
            'tracking_ttl_expired', 1.0, ?, ?, ?, 1, 0, 0.8,
            datetime(?, 'unixepoch')
        )
        """,
        (now - 300, now - 300, now - 300, now - 30, now - 30),
    )
    db.commit()

    first = fast.scan_missed_rescue_once(db, now_ts=now, limit=1)
    assert first["processed"] == 1

    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution (
            id, token_ca, symbol, signal_ts, signal_id, route, component,
            reject_reason, baseline_price, baseline_ts, created_event_ts,
            first_tradable_ts, tradable_missed, would_stop_before_peak,
            executable_peak_pnl, updated_at
        ) VALUES (
            2, 'TokenNewUnprocessed', 'NEW', ?, 12, 'LOTTO', 'discovery_tracking',
            'tracking_ttl_expired', 1.0, ?, ?, ?, 1, 0, 0.9,
            datetime(?, 'unixepoch')
        )
        """,
        (now - 200, now - 200, now - 200, now - 20, now - 20),
    )
    db.commit()

    second = fast.scan_missed_rescue_once(db, now_ts=now + 1, limit=1)
    new_state = db.execute(
        """
        SELECT last_status
        FROM paper_fast_missed_rescue_state
        WHERE missed_attribution_id = 2
        """
    ).fetchone()

    assert second["processed"] == 1
    assert second["deduped"] == 0
    assert new_state is not None


def test_missed_rescue_cursor_uses_rowid_when_id_is_not_primary_key(tmp_path, monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_MISSED_RESCUE_LOOKBACK_SEC", 3600)
    monkeypatch.setattr(fast, "FAST_ENTRY_MISSED_RESCUE_BACKLOG_LOOKBACK_SEC", 24 * 3600)
    monkeypatch.setattr(fast, "FAST_ENTRY_TTL_RESCUE_MAX_TRADABLE_AGE_SEC", 300)
    monkeypatch.setattr(fast, "FAST_ENTRY_MISSED_RESCUE_CURSOR_BATCH", 1)
    monkeypatch.setattr(fast, "FAST_ENTRY_MISSED_RESCUE_MAX_CURSOR_BATCHES", 1)
    db = fast.connect_db(tmp_path / "paper.db")
    fast.init_fast_lane_schema(db)
    now = int(time.time())
    db.execute(
        """
        CREATE TABLE paper_missed_signal_attribution (
            id INTEGER,
            token_ca TEXT,
            symbol TEXT,
            signal_ts INTEGER,
            signal_id INTEGER,
            route TEXT,
            component TEXT,
            reject_reason TEXT,
            baseline_price REAL,
            baseline_ts INTEGER,
            created_event_ts INTEGER,
            first_tradable_ts INTEGER,
            tradable_missed INTEGER,
            would_stop_before_peak INTEGER,
            executable_peak_pnl REAL,
            updated_at TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution (
            id, token_ca, symbol, signal_ts, signal_id, route, component,
            reject_reason, baseline_price, baseline_ts, created_event_ts,
            first_tradable_ts, tradable_missed, would_stop_before_peak,
            executable_peak_pnl, updated_at
        ) VALUES (
            999, 'TokenOldProcessed', 'OLD', ?, 11, 'LOTTO', 'discovery_tracking',
            'tracking_ttl_expired', 1.0, ?, ?, ?, 1, 0, 0.8,
            datetime(?, 'unixepoch')
        )
        """,
        (now - 300, now - 300, now - 300, now - 30, now - 30),
    )
    db.commit()

    first = fast.scan_missed_rescue_once(db, now_ts=now, limit=1)
    assert first["processed"] == 1

    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution (
            id, token_ca, symbol, signal_ts, signal_id, route, component,
            reject_reason, baseline_price, baseline_ts, created_event_ts,
            first_tradable_ts, tradable_missed, would_stop_before_peak,
            executable_peak_pnl, updated_at
        ) VALUES (
            10, 'TokenNewestRowid', 'NEW', ?, 12, 'LOTTO', 'discovery_tracking',
            'tracking_ttl_expired', 1.0, ?, ?, ?, 1, 0, 0.9,
            datetime(?, 'unixepoch')
        )
        """,
        (now - 200, now - 200, now - 200, now - 20, now - 20),
    )
    db.commit()

    second = fast.scan_missed_rescue_once(db, now_ts=now + 1, limit=1)
    new_state = db.execute(
        """
        SELECT token_ca, last_status
        FROM paper_fast_missed_rescue_state
        WHERE missed_attribution_id = 10
        """
    ).fetchone()

    assert second["processed"] == 1
    assert second["deduped"] == 0
    assert second["cursor_batches"] == 1
    assert second["cursor_candidates"] == 1
    assert new_state["token_ca"] == "TokenNewestRowid"


def test_missed_not_ath_kline_rescue_queues_lotto_reclaim_mode(tmp_path, monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_MISSED_RESCUE_LOOKBACK_SEC", 3600)
    monkeypatch.setattr(fast, "FAST_ENTRY_NOT_ATH_RECLAIM_MAX_TRADABLE_AGE_SEC", 300)
    db = fast.connect_db(tmp_path / "paper.db")
    fast.init_fast_lane_schema(db)
    now = int(time.time())
    db.execute(
        """
        CREATE TABLE paper_missed_signal_attribution (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            signal_ts INTEGER,
            signal_id INTEGER,
            route TEXT,
            component TEXT,
            reject_reason TEXT,
            baseline_price REAL,
            baseline_ts INTEGER,
            created_event_ts INTEGER,
            first_tradable_ts INTEGER,
            tradable_missed INTEGER,
            would_stop_before_peak INTEGER,
            executable_peak_pnl REAL,
            updated_at TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution (
            id, token_ca, symbol, signal_ts, signal_id, route, component,
            reject_reason, baseline_price, baseline_ts, created_event_ts,
            first_tradable_ts, tradable_missed, would_stop_before_peak,
            executable_peak_pnl, updated_at
        ) VALUES (
            1, 'TokenNotAth', 'NATH', ?, 11, 'LOTTO', 'discovery_tracking',
            'not_ath_prebuy_kline_block', 1.0, ?, ?, ?, 1, 0, 0.7,
            datetime(?, 'unixepoch')
        )
        """,
        (now - 180, now - 180, now - 180, now - 20, now),
    )
    db.commit()

    result = fast.scan_missed_rescue_once(db, now_ts=now)
    queue = db.execute(
        """
        SELECT source_type, entry_branch, entry_mode_hint, status, priority
        FROM paper_fast_entry_queue
        WHERE token_ca = 'TokenNotAth'
        """
    ).fetchone()

    assert result["queued"] == 1
    assert queue["source_type"] == "not_ath_reclaim_fast"
    assert queue["entry_branch"] == "not_ath_reclaim_quote_clean_tiny_probe"
    assert queue["entry_mode_hint"] == "lotto_not_ath_reclaim_tiny_probe"
    assert queue["status"] == "queued"
    assert queue["priority"] == fast.FAST_ENTRY_CLEAN_DOG_RECLAIM_PRIORITY
    state = db.execute(
        """
        SELECT token_ca, entry_branch, entry_mode_hint, state, blocker,
               policy_version, eligibility_json
        FROM paper_fast_missed_rescue_state
        WHERE missed_attribution_id = 1
        """
    ).fetchone()
    eligibility = json.loads(state["eligibility_json"])
    assert state["token_ca"] == "TokenNotAth"
    assert state["entry_branch"] == "not_ath_reclaim_quote_clean_tiny_probe"
    assert state["entry_mode_hint"] == "lotto_not_ath_reclaim_tiny_probe"
    assert state["state"] == "queued"
    assert state["blocker"] == "not_ath_prebuy_kline_block"
    assert state["policy_version"] == fast.CLEAN_DOG_RECLAIM_POLICY_VERSION
    assert eligibility["direct_reclaim_ok"] is True


def test_missed_rescue_backfills_unprocessed_clean_dog_backlog(tmp_path, monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_MISSED_RESCUE_LOOKBACK_SEC", 3600)
    monkeypatch.setattr(fast, "FAST_ENTRY_MISSED_RESCUE_BACKLOG_LOOKBACK_SEC", 24 * 3600)
    monkeypatch.setattr(fast, "FAST_ENTRY_NOT_ATH_RECLAIM_MAX_TRADABLE_AGE_SEC", 300)
    db = fast.connect_db(tmp_path / "paper.db")
    fast.init_fast_lane_schema(db)
    now = int(time.time())
    db.execute(
        """
        CREATE TABLE paper_missed_signal_attribution (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            signal_ts INTEGER,
            signal_id INTEGER,
            route TEXT,
            component TEXT,
            reject_reason TEXT,
            baseline_price REAL,
            baseline_ts INTEGER,
            created_event_ts INTEGER,
            first_tradable_ts INTEGER,
            tradable_missed INTEGER,
            would_stop_before_peak INTEGER,
            executable_peak_pnl REAL,
            updated_at TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution (
            id, token_ca, symbol, signal_ts, signal_id, route, component,
            reject_reason, baseline_price, baseline_ts, created_event_ts,
            first_tradable_ts, tradable_missed, would_stop_before_peak,
            executable_peak_pnl, updated_at
        ) VALUES (
            1, 'TokenBacklog', 'BACK', ?, 11, 'LOTTO', 'discovery_tracking',
            'tracking_ttl_expired', 1.0, ?, ?, ?, 1, 0, 0.9,
            datetime(?, 'unixepoch')
        )
        """,
        (
            now - 12 * 3600,
            now - 12 * 3600,
            now - 12 * 3600,
            now - 12 * 3600,
            now - 12 * 3600,
        ),
    )
    db.commit()

    first = fast.scan_missed_rescue_once(db, now_ts=now)
    second = fast.scan_missed_rescue_once(db, now_ts=now + 1)
    state = db.execute(
        """
        SELECT state, last_status, last_reason, entry_branch
        FROM paper_fast_missed_rescue_state
        WHERE missed_attribution_id = 1
        """
    ).fetchone()
    queue = db.execute(
        """
        SELECT status, last_error, payload_json
        FROM paper_fast_entry_queue
        WHERE token_ca = 'TokenBacklog'
        """
    ).fetchone()
    queue_payload = json.loads(queue["payload_json"])

    assert first["processed"] == 1
    assert first["watch_only"] == 1
    assert first["backlog_lookback_sec"] == 24 * 3600
    assert second["processed"] == 0
    assert queue["status"] == "watch_only"
    assert queue["last_error"] == "clean_dog_reclaim_recovery_tradable_signal_stale_watch_only"
    assert queue_payload["dog_capture_canary"] is True
    assert state["state"] == "stale"
    assert state["last_status"] == "watch_only"
    assert state["last_reason"] == "clean_dog_reclaim_recovery_tradable_signal_stale_watch_only"
    assert state["entry_branch"] == "tracking_ttl_reclaim_quote_clean_tiny_probe"


def test_missed_rescue_backlog_uses_any_recent_signal_time_not_created_only(tmp_path, monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_MISSED_RESCUE_LOOKBACK_SEC", 3600)
    monkeypatch.setattr(fast, "FAST_ENTRY_MISSED_RESCUE_BACKLOG_LOOKBACK_SEC", 24 * 3600)
    monkeypatch.setattr(fast, "FAST_ENTRY_NOT_ATH_RECLAIM_MAX_TRADABLE_AGE_SEC", 300)
    db = fast.connect_db(tmp_path / "paper.db")
    fast.init_fast_lane_schema(db)
    now = int(time.time())
    db.execute(
        """
        CREATE TABLE paper_missed_signal_attribution (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            signal_ts INTEGER,
            signal_id INTEGER,
            route TEXT,
            component TEXT,
            reject_reason TEXT,
            baseline_price REAL,
            baseline_ts INTEGER,
            created_event_ts INTEGER,
            first_tradable_ts INTEGER,
            tradable_missed INTEGER,
            would_stop_before_peak INTEGER,
            executable_peak_pnl REAL,
            updated_at TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution (
            id, token_ca, symbol, signal_ts, signal_id, route, component,
            reject_reason, baseline_price, baseline_ts, created_event_ts,
            first_tradable_ts, tradable_missed, would_stop_before_peak,
            executable_peak_pnl, updated_at
        ) VALUES (
            1, 'TokenCreatedOldSignalRecent', 'COSR', ?, 11, 'LOTTO', 'discovery_tracking',
            'tracking_ttl_expired', 1.0, ?, ?, NULL, 1, 0, 0.9,
            datetime(?, 'unixepoch')
        )
        """,
        (
            now - 12 * 3600,
            now - 12 * 3600,
            now - 30 * 3600,
            now - 30 * 3600,
        ),
    )
    db.commit()

    result = fast.scan_missed_rescue_once(db, now_ts=now)
    state = db.execute(
        """
        SELECT state, last_status, entry_branch
        FROM paper_fast_missed_rescue_state
        WHERE missed_attribution_id = 1
        """
    ).fetchone()

    assert result["processed"] == 1
    assert result["watch_only"] == 1
    assert state["state"] == "stale"
    assert state["last_status"] == "watch_only"
    assert state["entry_branch"] == "tracking_ttl_reclaim_quote_clean_tiny_probe"


def test_missed_not_ath_v17_rescue_queues_lotto_reclaim_mode(tmp_path, monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_MISSED_RESCUE_LOOKBACK_SEC", 3600)
    monkeypatch.setattr(fast, "FAST_ENTRY_NOT_ATH_RECLAIM_MAX_TRADABLE_AGE_SEC", 300)
    db = fast.connect_db(tmp_path / "paper.db")
    fast.init_fast_lane_schema(db)
    now = int(time.time())
    db.execute(
        """
        CREATE TABLE paper_missed_signal_attribution (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            signal_ts INTEGER,
            signal_id INTEGER,
            route TEXT,
            component TEXT,
            reject_reason TEXT,
            baseline_price REAL,
            baseline_ts INTEGER,
            created_event_ts INTEGER,
            first_tradable_ts INTEGER,
            tradable_missed INTEGER,
            would_stop_before_peak INTEGER,
            executable_peak_pnl REAL,
            updated_at TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution (
            id, token_ca, symbol, signal_ts, signal_id, route, component,
            reject_reason, baseline_price, baseline_ts, created_event_ts,
            first_tradable_ts, tradable_missed, would_stop_before_peak,
            executable_peak_pnl, updated_at
        ) VALUES (
            1, 'TokenNotAthV17', 'V17', ?, 11, 'LOTTO', 'upstream_gate',
            'not_ath_v17', 1.0, ?, ?, ?, 1, 0, 0.85,
            datetime(?, 'unixepoch')
        )
        """,
        (now - 180, now - 180, now - 180, now - 20, now),
    )
    db.commit()

    result = fast.scan_missed_rescue_once(db, now_ts=now)
    queue = db.execute(
        """
        SELECT source_type, entry_branch, entry_mode_hint, status, priority
        FROM paper_fast_entry_queue
        WHERE token_ca = 'TokenNotAthV17'
        """
    ).fetchone()

    assert result["queued"] == 1
    assert queue["source_type"] == "not_ath_reclaim_fast"
    assert queue["entry_branch"] == "not_ath_reclaim_quote_clean_tiny_probe"
    assert queue["entry_mode_hint"] == "lotto_not_ath_reclaim_tiny_probe"
    assert queue["status"] == "queued"
    assert queue["priority"] == fast.FAST_ENTRY_CLEAN_DOG_RECLAIM_PRIORITY


def test_missed_lotto_missing_mc_rescue_queues_clean_reclaim(tmp_path, monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_MISSED_RESCUE_LOOKBACK_SEC", 3600)
    monkeypatch.setattr(fast, "FAST_ENTRY_NOT_ATH_RECLAIM_MAX_TRADABLE_AGE_SEC", 300)
    db = fast.connect_db(tmp_path / "paper.db")
    fast.init_fast_lane_schema(db)
    now = int(time.time())
    db.execute(
        """
        CREATE TABLE paper_missed_signal_attribution (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            signal_ts INTEGER,
            signal_id INTEGER,
            route TEXT,
            component TEXT,
            reject_reason TEXT,
            baseline_price REAL,
            baseline_ts INTEGER,
            created_event_ts INTEGER,
            first_tradable_ts INTEGER,
            tradable_missed INTEGER,
            would_stop_before_peak INTEGER,
            executable_peak_pnl REAL,
            updated_at TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution (
            id, token_ca, symbol, signal_ts, signal_id, route, component,
            reject_reason, baseline_price, baseline_ts, created_event_ts,
            first_tradable_ts, tradable_missed, would_stop_before_peak,
            executable_peak_pnl, updated_at
        ) VALUES (
            1, 'TokenMissingMc', 'MMC', ?, 11, 'LOTTO', 'lotto_entry_gate',
            'lotto_mc_0', 1.0, ?, ?, ?, 1, 0, 0.728,
            datetime(?, 'unixepoch')
        )
        """,
        (now - 120, now - 120, now - 120, now - 20, now),
    )
    db.commit()

    assert fast.missed_rescue_reason_allowed("lotto_mc_0")
    assert not fast.missed_rescue_reason_allowed("lotto_mc_500000")

    result = fast.scan_missed_rescue_once(db, now_ts=now)
    queue = db.execute(
        """
        SELECT source_type, entry_branch, entry_mode_hint, status, priority
        FROM paper_fast_entry_queue
        WHERE token_ca = 'TokenMissingMc'
        """
    ).fetchone()

    assert result["queued"] == 1
    assert queue["source_type"] == "lotto_missing_mc_reclaim_fast"
    assert queue["entry_branch"] == "not_ath_reclaim_quote_clean_tiny_probe"
    assert queue["entry_mode_hint"] == "lotto_not_ath_reclaim_tiny_probe"
    assert queue["status"] == "queued"
    assert queue["priority"] == fast.FAST_ENTRY_CLEAN_DOG_RECLAIM_PRIORITY


def test_missed_pre_pass_stale_rescue_queues_clean_reclaim(tmp_path, monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_MISSED_RESCUE_LOOKBACK_SEC", 3600)
    monkeypatch.setattr(fast, "FAST_ENTRY_NOT_ATH_RECLAIM_MAX_TRADABLE_AGE_SEC", 300)
    db = fast.connect_db(tmp_path / "paper.db")
    fast.init_fast_lane_schema(db)
    now = int(time.time())
    db.execute(
        """
        CREATE TABLE paper_missed_signal_attribution (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            signal_ts INTEGER,
            signal_id INTEGER,
            route TEXT,
            component TEXT,
            reject_reason TEXT,
            baseline_price REAL,
            baseline_ts INTEGER,
            created_event_ts INTEGER,
            first_tradable_ts INTEGER,
            tradable_missed INTEGER,
            would_stop_before_peak INTEGER,
            executable_peak_pnl REAL,
            updated_at TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution (
            id, token_ca, symbol, signal_ts, signal_id, route, component,
            reject_reason, baseline_price, baseline_ts, created_event_ts,
            first_tradable_ts, tradable_missed, would_stop_before_peak,
            executable_peak_pnl, updated_at
        ) VALUES (
            1, 'TokenPrePass', 'PRE', ?, 11, 'LOTTO', 'upstream_gate',
            'pre_pass_signal_too_stale', 1.0, ?, ?, ?, 1, 0, 1.1,
            datetime(?, 'unixepoch')
        )
        """,
        (now - 1800, now - 1800, now - 1800, now - 1600, now - 15),
    )
    db.commit()

    result = fast.scan_missed_rescue_once(db, now_ts=now)
    queue = db.execute(
        """
        SELECT source_type, entry_branch, entry_mode_hint, status, priority
        FROM paper_fast_entry_queue
        WHERE token_ca = 'TokenPrePass'
        """
    ).fetchone()

    assert result["queued"] == 1
    assert queue["source_type"] == "pre_pass_stale_reclaim_fast"
    assert queue["entry_branch"] == "pre_pass_stale_reclaim_quote_clean_tiny_probe"
    assert queue["entry_mode_hint"] == "lotto_not_ath_reclaim_tiny_probe"
    assert queue["status"] == "queued"
    assert queue["priority"] == fast.FAST_ENTRY_CLEAN_DOG_RECLAIM_PRIORITY


def test_missed_momentum_fading_rescue_queues_smart_reclaim(tmp_path, monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_MISSED_RESCUE_LOOKBACK_SEC", 3600)
    monkeypatch.setattr(fast, "FAST_ENTRY_NOT_ATH_RECLAIM_MAX_TRADABLE_AGE_SEC", 300)
    db = fast.connect_db(tmp_path / "paper.db")
    fast.init_fast_lane_schema(db)
    now = int(time.time())
    db.execute(
        """
        CREATE TABLE paper_missed_signal_attribution (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            signal_ts INTEGER,
            signal_id INTEGER,
            route TEXT,
            component TEXT,
            reject_reason TEXT,
            baseline_price REAL,
            baseline_ts INTEGER,
            created_event_ts INTEGER,
            first_tradable_ts INTEGER,
            tradable_missed INTEGER,
            would_stop_before_peak INTEGER,
            executable_peak_pnl REAL,
            updated_at TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution (
            id, token_ca, symbol, signal_ts, signal_id, route, component,
            reject_reason, baseline_price, baseline_ts, created_event_ts,
            first_tradable_ts, tradable_missed, would_stop_before_peak,
            executable_peak_pnl, updated_at
        ) VALUES (
            1, 'TokenMomentumFade', 'MOM', ?, 11, 'LOTTO', 'smart_entry',
            'momentum_fading', 1.0, ?, ?, ?, 1, 0, 0.425,
            datetime(?, 'unixepoch')
        )
        """,
        (now - 120, now - 120, now - 120, now - 20, now),
    )
    db.commit()

    result = fast.scan_missed_rescue_once(db, now_ts=now)
    queue = db.execute(
        """
        SELECT source_type, entry_branch, entry_mode_hint, status, priority
        FROM paper_fast_entry_queue
        WHERE token_ca = 'TokenMomentumFade'
        """
    ).fetchone()

    assert result["queued"] == 1
    assert queue["source_type"] == "smart_quality_reclaim_fast"
    assert queue["entry_branch"] == "smart_entry_reclaim_quote_clean_tiny_probe"
    assert queue["entry_mode_hint"] == "lotto_micro_reclaim_tiny_probe"
    assert queue["status"] == "queued"
    assert queue["priority"] == fast.FAST_ENTRY_CLEAN_DOG_BRONZE_PRIORITY


def test_missed_ath_soft_quality_dog_capture_queues_ath_tiny_scout(tmp_path, monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_MISSED_RESCUE_LOOKBACK_SEC", 3600)
    monkeypatch.setattr(fast, "FAST_ENTRY_NOT_ATH_RECLAIM_MAX_TRADABLE_AGE_SEC", 300)
    monkeypatch.setattr(fast, "FAST_ENTRY_DOG_CAPTURE_MIN_PEAK_PNL", 0.50)
    db = fast.connect_db(tmp_path / "paper.db")
    fast.init_fast_lane_schema(db)
    now = int(time.time())
    db.execute(
        """
        CREATE TABLE paper_missed_signal_attribution (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            signal_ts INTEGER,
            signal_id INTEGER,
            route TEXT,
            component TEXT,
            reject_reason TEXT,
            baseline_price REAL,
            baseline_ts INTEGER,
            created_event_ts INTEGER,
            first_tradable_ts INTEGER,
            tradable_missed INTEGER,
            would_stop_before_peak INTEGER,
            executable_peak_pnl REAL,
            updated_at TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution (
            id, token_ca, symbol, signal_ts, signal_id, route, component,
            reject_reason, baseline_price, baseline_ts, created_event_ts,
            first_tradable_ts, tradable_missed, would_stop_before_peak,
            executable_peak_pnl, updated_at
        ) VALUES (
            1, 'TokenAthSoft', 'ATHS', ?, 11, 'ATH', 'ath_uncertainty_scout',
            'scout_quality_buy_pressure_weak', 1.0, ?, ?, ?, 1, 0, 0.72,
            datetime(?, 'unixepoch')
        )
        """,
        (now - 120, now - 120, now - 120, now - 20, now),
    )
    db.commit()

    result = fast.scan_missed_rescue_once(db, now_ts=now)
    queue = db.execute(
        """
        SELECT source_type, entry_branch, entry_mode_hint, status, priority, payload_json
        FROM paper_fast_entry_queue
        WHERE token_ca = 'TokenAthSoft'
        """
    ).fetchone()
    payload = json.loads(queue["payload_json"])

    assert result["queued"] == 1
    assert queue["source_type"] == "smart_quality_reclaim_fast"
    assert queue["entry_branch"] == "smart_entry_reclaim_quote_clean_tiny_probe"
    assert queue["entry_mode_hint"] == "ath_uncertainty_tiny_scout"
    assert queue["status"] == "queued"
    assert queue["priority"] == fast.FAST_ENTRY_CLEAN_DOG_RECLAIM_PRIORITY
    assert payload["dog_capture_canary"] is True
    assert payload["dog_capture_parent_reason"] == "scout_quality_buy_pressure_weak"
    assert payload["dog_capture_detail"]["fresh_canary_ok"] is True


def test_missed_rescue_records_stale_clean_dog_state(tmp_path, monkeypatch):
    monkeypatch.setattr(fast, "FAST_ENTRY_MISSED_RESCUE_LOOKBACK_SEC", 3600)
    monkeypatch.setattr(fast, "FAST_ENTRY_NOT_ATH_RECLAIM_MAX_TRADABLE_AGE_SEC", 300)
    monkeypatch.setattr(fast, "FAST_ENTRY_DOG_CAPTURE_MAX_TRADABLE_AGE_SEC", 300)
    db = fast.connect_db(tmp_path / "paper.db")
    fast.init_fast_lane_schema(db)
    now = int(time.time())
    db.execute(
        """
        CREATE TABLE paper_missed_signal_attribution (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            signal_ts INTEGER,
            signal_id INTEGER,
            route TEXT,
            component TEXT,
            reject_reason TEXT,
            baseline_price REAL,
            baseline_ts INTEGER,
            created_event_ts INTEGER,
            first_tradable_ts INTEGER,
            tradable_missed INTEGER,
            would_stop_before_peak INTEGER,
            executable_peak_pnl REAL,
            updated_at TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution (
            id, token_ca, symbol, signal_ts, signal_id, route, component,
            reject_reason, baseline_price, baseline_ts, created_event_ts,
            first_tradable_ts, tradable_missed, would_stop_before_peak,
            executable_peak_pnl, updated_at
        ) VALUES (
            1, 'TokenStaleClean', 'STALE', ?, 11, 'LOTTO', 'upstream_gate',
            'not_ath_prebuy_kline_block', 1.0, ?, ?, ?, 1, 0, 0.9,
            datetime(?, 'unixepoch')
        )
        """,
        (now - 1200, now - 1200, now - 1200, now - 900, now - 900),
    )
    db.commit()

    result = fast.scan_missed_rescue_once(db, now_ts=now)
    state = db.execute(
        """
        SELECT state, last_reason, entry_branch, eligibility_json
        FROM paper_fast_missed_rescue_state
        WHERE missed_attribution_id = 1
        """
    ).fetchone()
    queue = db.execute(
        """
        SELECT status, last_error, payload_json
        FROM paper_fast_entry_queue
        WHERE token_ca = 'TokenStaleClean'
        """
    ).fetchone()
    payload = json.loads(queue["payload_json"])

    assert result["watch_only"] == 1
    assert state["state"] == "stale"
    assert state["last_reason"] == "clean_dog_reclaim_recovery_tradable_signal_stale_watch_only"
    assert state["entry_branch"] == "not_ath_reclaim_quote_clean_tiny_probe"
    assert json.loads(state["eligibility_json"])["last_tradable_fresh_ok"] is False
    assert queue["status"] == "watch_only"
    assert queue["last_error"] == "clean_dog_reclaim_recovery_tradable_signal_stale_watch_only"
    assert payload["dog_capture_canary"] is True
    assert payload["dog_capture_detail"]["reason"] == "dog_capture_recovery_tradable_signal_stale_watch_only"


def test_fast_lane_health_preserves_last_error_during_scanning_heartbeat(tmp_path, monkeypatch):
    health_path = tmp_path / "paper-fast-lane-health.json"
    paper_db_path = tmp_path / "paper.db"
    monkeypatch.setenv("PAPER_FAST_LANE_HEALTH_PATH", str(health_path))
    fast.FAST_LANE_HEALTH_STATE.update({
        "missed_rescue_scan_count": 0,
        "missed_rescue_error_count": 0,
        "missed_rescue_last_result": None,
        "missed_rescue_last_error": None,
    })

    fast.write_fast_lane_health(
        paper_db_path=paper_db_path,
        error=TimeoutError("sqlite single-writer lock timeout holder=123 paper"),
        now_ts=1_780_000_000,
    )
    fast.write_fast_lane_health(
        paper_db_path=paper_db_path,
        worker_state="missed_rescue_scanning",
        now_ts=1_780_000_010,
    )

    payload = json.loads(health_path.read_text(encoding="utf-8"))
    assert payload["worker_state"] == "missed_rescue_scanning"
    assert payload["missed_rescue"]["scan_count"] == 1
    assert payload["missed_rescue"]["error_count"] == 1
    assert payload["missed_rescue"]["last_error"]["type"] == "TimeoutError"
    assert "holder=123" in payload["missed_rescue"]["last_error"]["message"]


def test_sqlite_writer_lock_times_out_on_process_lock_contention(tmp_path):
    lock_path = tmp_path / "paper-writer.lock"
    entered = threading.Event()
    release = threading.Event()
    holder_errors = []

    def hold_lock():
        try:
            with SQLiteSingleWriterLock("holder", lock_file=lock_path, timeout_sec=1.0):
                entered.set()
                release.wait(1.0)
        except BaseException as exc:
            holder_errors.append(exc)
            entered.set()

    holder = threading.Thread(target=hold_lock)
    holder.start()
    assert entered.wait(1.0)
    assert holder_errors == []

    try:
        try:
            with SQLiteSingleWriterLock("contender", lock_file=lock_path, timeout_sec=0.05):
                pass
        except TimeoutError as exc:
            message = str(exc)
        else:
            raise AssertionError("expected process lock timeout")

        assert "sqlite single-writer process lock timeout" in message
        assert "contender" in message
    finally:
        release.set()
        holder.join(1.0)

    assert not holder.is_alive()
    assert holder_errors == []

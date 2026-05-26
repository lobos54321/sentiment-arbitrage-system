import sqlite3
import time
import json
import datetime as dt

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


def test_process_queue_item_respects_entry_mode_quality_shadow(tmp_path):
    db_path = tmp_path / "paper.db"
    db = fast.connect_db(db_path)
    fast.init_fast_lane_schema(db)
    now = int(time.time())
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
            VALUES ('pre_pass_resonance_tiny_probe', 'pre_pass_resonance', 0.20, ?, 'paper_fast_lane', ?, ?)
            """,
            (pnl, now - idx, now - idx + 1),
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
        entry_mode_hint="pre_pass_resonance_tiny_probe",
        now_ts=now,
    )
    row = fast.claim_queue_item(db, "worker-1")

    fast.process_queue_item(db, row, "worker-1")

    queue = db.execute(
        "SELECT status, last_error FROM paper_fast_entry_queue WHERE token_ca = 'TokenQuality'"
    ).fetchone()
    assert queue["status"] == "watch_only"
    assert queue["last_error"] == "entry_mode_quality_tail_loss"


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


def test_missed_rescue_records_stale_clean_dog_state(tmp_path, monkeypatch):
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

    assert result["watch_only"] == 1
    assert state["state"] == "stale"
    assert state["last_reason"] == "clean_dog_reclaim_recovery_tradable_signal_stale_watch_only"
    assert state["entry_branch"] == "not_ath_reclaim_quote_clean_tiny_probe"
    assert json.loads(state["eligibility_json"])["last_tradable_fresh_ok"] is False

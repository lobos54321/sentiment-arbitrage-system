import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from a_class_fastlane import (
    AClassDecision,
    AClassCandidate,
    candidate_from_decision_event_row,
    candidate_from_external_alpha_row,
    candidate_from_fast_queue_row,
    decide_size,
    enrich_candidate_with_db_evidence,
    evaluate_a_class_fastlane,
    hard_prefilter,
    record_a_class_fastlane_shadow_candidates,
)
from fastlane_config import load_a_class_config


def base_candidate(**overrides):
    data = {
        "token_ca": "TokenCA123",
        "symbol": "AFAST",
        "lifecycle_id": "life-1",
        "route_bucket": "ATH",
        "signal_ts": 100,
        "quote_available": True,
        "quote_executable": True,
        "quote_clean": True,
        "quote_source": "gmgn",
        "quote_age_sec": 5,
        "route_available": True,
        "route_stable_recent": True,
        "liquidity_usd": 50_000,
        "spread_pct": 1.0,
        "gmgn_pre_seen": True,
        "gmgn_activity_fresh": True,
        "gmgn_last_seen_age_sec": 5,
        "source_resonance": True,
        "fresh_momentum": True,
        "fresh_ath_refresh": True,
        "ath_continuation": True,
        "top10_pct": 40,
        "bundler_rate": 0.05,
        "rat_trader_rate": 0.01,
        "entrapment_ratio": 0.01,
    }
    data.update(overrides)
    return AClassCandidate.from_mapping(data)


def test_no_route_must_block():
    decision = evaluate_a_class_fastlane(
        base_candidate(route_available=False),
        now_ts=1_000,
        config=load_a_class_config({}),
    )

    assert decision.action == "BLOCK"
    assert "route_unavailable" in decision.hard_blockers


def test_a_class_decision_constructs_without_new_expected_rr_kwargs():
    decision = AClassDecision(
        action="SHADOW",
        grade="REJECT",
        size_sol=0.0,
        reason="unit",
        hard_blockers=[],
        soft_notes=[],
        score=0.0,
        freshness_detail={},
        budget_detail={},
        risk_detail={},
    )

    assert decision.expected_rr_detail == {}
    assert decision.would_action is None
    assert decision.denominator_key is None


def test_hard_prefilter_still_blocks_green_candidate_with_one_hard_blocker():
    decision = evaluate_a_class_fastlane(
        base_candidate(quote_executable=False, source_resonance=True, gmgn_pre_seen=True, fresh_ath_refresh=True),
        now_ts=1_000,
        config=load_a_class_config({}),
    )

    assert decision.action == "BLOCK"
    assert decision.size_sol == 0
    assert "quote_not_executable" in decision.hard_blockers


def test_quote_not_executable_must_block():
    decision = evaluate_a_class_fastlane(
        base_candidate(quote_executable=False),
        now_ts=1_000,
        config=load_a_class_config({}),
    )

    assert decision.action == "BLOCK"
    assert "quote_not_executable" in decision.hard_blockers


def test_liquidity_unknown_must_block():
    passed, blockers, _detail = hard_prefilter(
        base_candidate(liquidity_usd=None),
        config=load_a_class_config({}),
    )

    assert passed is False
    assert "liquidity_unknown" in blockers


def test_extreme_spread_must_block():
    decision = evaluate_a_class_fastlane(
        base_candidate(spread_pct=8.0),
        now_ts=1_000,
        config=load_a_class_config({}),
    )

    assert decision.action == "BLOCK"
    assert "spread_extreme" in decision.hard_blockers


def test_security_and_cooldown_blocks_cannot_be_overridden_by_high_score():
    decision = evaluate_a_class_fastlane(
        base_candidate(risk_flags=["obvious_rug"], recent_hard_loss=True),
        now_ts=1_000,
        config=load_a_class_config({}),
    )

    assert decision.action == "BLOCK"
    assert "security_red_flag" in decision.hard_blockers
    assert "recent_hard_loss" in decision.hard_blockers
    assert decision.score == 0


def test_same_lifecycle_duplicate_must_block():
    decision = evaluate_a_class_fastlane(
        base_candidate(prior_fastlane_in_lifecycle=True),
        now_ts=1_000,
        config=load_a_class_config({}),
    )

    assert decision.action == "BLOCK"
    assert "prior_fastlane_in_lifecycle" in decision.hard_blockers


def test_old_signal_with_fresh_quote_and_gmgn_can_enter():
    decision = evaluate_a_class_fastlane(
        base_candidate(signal_ts=1_000 - 3_600),
        now_ts=1_000,
        config=load_a_class_config({}),
    )

    assert decision.action == "ENTER"
    assert decision.grade == "A_PLUS"
    assert decision.size_sol == 0.003
    assert decision.freshness_detail["raw_signal_age_sec"] == 3_600
    assert "fresh_quote" in decision.freshness_detail["freshness_sources"]


def test_raw_signal_fresh_but_quote_stale_cannot_enter():
    decision = evaluate_a_class_fastlane(
        base_candidate(signal_ts=970, quote_age_sec=120, gmgn_activity_fresh=False, fresh_momentum=False, fresh_ath_refresh=False),
        now_ts=1_000,
        config=load_a_class_config({}),
    )

    assert decision.action == "BLOCK"
    assert "quote_stale" in decision.hard_blockers


def test_size_thresholds_are_capped():
    config = load_a_class_config({"A_CLASS_MAX_SIZE_SOL": "0.003"})

    assert decide_size(70, config=config) == ("A", 0.001)
    assert decide_size(82, config=config) == ("STRONG_A", 0.002)
    assert decide_size(92, config=config) == ("A_PLUS", 0.003)


def test_safe_canary_force_defaults_only_for_process_environment(monkeypatch):
    monkeypatch.setenv("A_CLASS_ENABLED", "false")
    monkeypatch.delenv("A_CLASS_SAFE_CANARY_FORCE", raising=False)

    assert load_a_class_config().enabled is True
    assert load_a_class_config({"A_CLASS_ENABLED": "false"}).enabled is False
    assert load_a_class_config(
        {"A_CLASS_ENABLED": "false", "A_CLASS_SAFE_CANARY_FORCE": "true"}
    ).enabled is True


def test_decision_event_hydrates_nested_quote_execution_evidence():
    candidate = candidate_from_decision_event_row(
        {
            "id": 1,
            "event_ts": 995,
            "token_ca": "DecisionToken",
            "symbol": "DEC",
            "lifecycle_id": "life-dec",
            "signal_ts": 100,
            "route": "ATH",
            "component": "ath_uncertainty_scout",
            "reason": "scout_quality_buy_pressure_weak",
            "payload_json": """
            {
              "gmgn_pre_seen": true,
              "gmgn_momentum_confirmed": true,
              "source_resonance": true,
              "guard": {
                "success": true,
                "routeAvailable": true,
                "quote_spread_pct": 0.7
              },
              "latest_snapshot": {
                "quote_clean": true,
                "quote_source": "jupiter",
                "snapshot_ts": 996,
                "liquidity_usd": 75000,
                "spread_pct": 0.7
              },
              "top10_pct": 35,
              "bundler_rate": 0.01,
              "rat_trader_rate": 0.01,
              "entrapment_ratio": 0.01
            }
            """,
        },
        now_ts=1000,
    )

    assert candidate.quote_available is True
    assert candidate.quote_executable is True
    assert candidate.route_available is True
    assert candidate.quote_source == "jupiter"
    assert candidate.quote_age_sec == 4
    assert candidate.liquidity_usd == 75000
    assert candidate.spread_pct == 0.7

    decision = evaluate_a_class_fastlane(candidate, now_ts=1000, config=load_a_class_config({}))
    assert decision.action == "ENTER"
    assert "fresh_quote" in decision.freshness_detail["freshness_sources"]


def test_fast_queue_quote_clean_verified_can_satisfy_spread_without_inventing_number():
    candidate = candidate_from_fast_queue_row(
        {
            "id": 2,
            "created_at": 990,
            "updated_at": 998,
            "source_signal_ts": 100,
            "token_ca": "QueueToken",
            "symbol": "QUEUE",
            "source_type": "source_resonance_fast",
            "entry_branch": "source_resonance_quote_clean_fast",
            "entry_mode_hint": "source_resonance_tiny_probe",
            "status": "watch_only",
            "first_error": "source_quote_clean_original_signal_stale_watch_only",
            "payload_json": """
            {
              "quote_clean_seen": true,
              "two_quote_clean_snapshots": true,
              "last_clean_quote_ts": 997,
              "liquidity_usd": 90000,
              "gmgn_pre_seen": true,
              "gmgn_momentum_confirmed": true,
              "source_resonance": true,
              "top10_pct": 30,
              "bundler_rate": 0.01,
              "rat_trader_rate": 0.01,
              "entrapment_ratio": 0.01
            }
            """,
        },
        now_ts=1000,
    )

    assert candidate.spread_pct is None
    assert candidate.spread_verified is True
    assert candidate.quote_clean_verified is True
    passed, blockers, detail = hard_prefilter(candidate, config=load_a_class_config({}))
    assert passed is True
    assert "spread_unknown" not in blockers
    assert detail["spread_verified"] is True


def test_decision_event_can_hydrate_quote_evidence_from_shadow_snapshot_table():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.executescript("""
    CREATE TABLE lotto_not_ath_watch_shadow_snapshots (
        id INTEGER PRIMARY KEY,
        token_ca TEXT,
        snapshot_ts REAL,
        quote_clean INTEGER,
        snapshot_pass INTEGER,
        liquidity_usd REAL,
        spread_pct REAL,
        quote_price REAL
    );
    INSERT INTO lotto_not_ath_watch_shadow_snapshots
        (token_ca, snapshot_ts, quote_clean, snapshot_pass, liquidity_usd, spread_pct, quote_price)
    VALUES
        ('HydrateToken', 997, 1, 1, 88000, 0.8, 0.00042);
    """)
    candidate = candidate_from_decision_event_row(
        {
            "id": 7,
            "event_ts": 996,
            "token_ca": "HydrateToken",
            "symbol": "HYD",
            "route": "ATH",
            "component": "ath_uncertainty_scout",
            "reason": "scout_quality_buy_pressure_weak",
            "payload_json": """
            {
              "gmgn_pre_seen": true,
              "source_resonance": true,
              "gmgn_momentum_confirmed": true
            }
            """,
        },
        now_ts=1000,
    )
    assert candidate.quote_available is False

    candidate = enrich_candidate_with_db_evidence(db, candidate, now_ts=1000, config=load_a_class_config({}))

    assert candidate.quote_available is True
    assert candidate.quote_executable is True
    assert candidate.route_available is True
    assert candidate.quote_source == "lotto_not_ath_watch_shadow"
    assert candidate.quote_age_sec == 3
    assert candidate.liquidity_usd == 88000
    assert candidate.spread_pct == 0.8
    passed, blockers, _ = hard_prefilter(candidate, config=load_a_class_config({}))
    assert passed is True
    assert blockers == []


def test_shadow_scan_records_counterfactual_sources():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.executescript(
        """
        CREATE TABLE paper_fast_entry_queue (
            id INTEGER PRIMARY KEY,
            created_at REAL,
            updated_at REAL,
            source_signal_ts INTEGER,
            signal_receive_ts INTEGER,
            signal_recorded_ts INTEGER,
            token_ca TEXT,
            symbol TEXT,
            source_type TEXT,
            entry_mode_hint TEXT,
            entry_branch TEXT,
            status TEXT,
            last_error TEXT,
            first_error TEXT,
            payload_json TEXT
        );
        CREATE TABLE source_resonance_candidates (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            signal_ts INTEGER,
            signal_type TEXT,
            gmgn_pre_seen INTEGER,
            gmgn_last_seen_ts INTEGER,
            gmgn_last_market_cap REAL,
            gmgn_last_liquidity REAL,
            quote_clean_seen INTEGER,
            two_quote_clean_snapshots INTEGER,
            source_count INTEGER,
            resonance_level INTEGER,
            resonance_score REAL,
            cohort TEXT,
            payload_json TEXT,
            updated_at REAL
        );
        CREATE TABLE paper_decision_events (
            id INTEGER PRIMARY KEY,
            event_ts REAL,
            token_ca TEXT,
            symbol TEXT,
            lifecycle_id TEXT,
            signal_ts INTEGER,
            route TEXT,
            component TEXT,
            event_type TEXT,
            decision TEXT,
            reason TEXT,
            data_source TEXT,
            payload_json TEXT
        );
        CREATE TABLE external_alpha_state (
            chain TEXT NOT NULL,
            token_ca TEXT NOT NULL,
            first_seen_ts INTEGER NOT NULL,
            last_seen_ts INTEGER NOT NULL,
            seen_count INTEGER DEFAULT 0,
            changed_count INTEGER DEFAULT 0,
            source_last TEXT,
            category_last TEXT,
            symbol TEXT,
            name TEXT,
            last_market_cap REAL,
            last_liquidity REAL,
            last_volume REAL,
            last_swaps REAL,
            last_buys REAL,
            last_sells REAL,
            momentum_rounds INTEGER DEFAULT 1,
            momentum_start_mc REAL,
            momentum_gain_pct REAL DEFAULT 0,
            momentum_confirmed INTEGER DEFAULT 0,
            volume_confirmed INTEGER DEFAULT 0,
            buy_pressure REAL DEFAULT 0,
            last_snapshot_json TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (chain, token_ca)
        );
        """
    )
    db.execute(
        """
        INSERT INTO paper_fast_entry_queue (
            id, created_at, updated_at, source_signal_ts, token_ca, symbol,
            source_type, entry_mode_hint, entry_branch, status, first_error, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1,
            990,
            995,
            100,
            "FastToken",
            "FAST",
            "source_resonance_gmgn_fast",
            "a_class_fastlane_shadow",
            "source_resonance_gmgn_fast",
            "watch_only",
            "source_resonance_gmgn_only_watch_only",
            '{"quote_available":true,"quote_executable":true,"quote_clean":true,"quote_source":"jupiter","quote_age_sec":5,"route_available":true,"route_stable_recent":true,"liquidity_usd":50000,"spread_pct":1.0,"gmgn_pre_seen":true,"gmgn_activity_fresh":true,"top10_pct":40,"bundler_rate":0.01,"rat_trader_rate":0.01,"entrapment_ratio":0.01}',
        ),
    )
    db.execute(
        """
        INSERT INTO source_resonance_candidates (
            id, token_ca, symbol, signal_ts, signal_type, gmgn_pre_seen,
            gmgn_last_seen_ts, gmgn_last_market_cap, gmgn_last_liquidity,
            quote_clean_seen, two_quote_clean_snapshots, source_count,
            resonance_level, resonance_score, cohort, payload_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (2, "ResToken", "RES", 100000, "ATH", 1, 997, 100000, 60000, 1, 2, 2, 2, 85, "telegram_gmgn", "{}", 998),
    )
    db.execute(
        """
        INSERT INTO paper_decision_events (
            id, event_ts, token_ca, symbol, lifecycle_id, signal_ts, route,
            component, event_type, decision, reason, data_source, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            3,
            996,
            "DecisionToken",
            "DEC",
            "life-dec",
            100,
            "ATH",
            "ath_uncertainty_scout",
            "reject",
            "block",
            "scout_quality_buy_pressure_weak",
            "paper_decision_events",
            '{"quote_available":true,"quote_executable":true,"quote_clean":true,"quote_source":"jupiter","quote_age_sec":4,"route_available":true,"route_stable_recent":true,"liquidity_usd":80000,"spread_pct":0.8,"gmgn_pre_seen":true,"gmgn_activity_fresh":true,"source_resonance":true,"fresh_ath_refresh":true,"top10_pct":35,"bundler_rate":0.01,"rat_trader_rate":0.01,"entrapment_ratio":0.01}',
        ),
    )
    db.execute(
        """
        INSERT INTO external_alpha_state (
            chain, token_ca, first_seen_ts, last_seen_ts, seen_count, changed_count,
            source_last, category_last, symbol, name, last_market_cap, last_liquidity,
            last_volume, last_swaps, last_buys, last_sells, momentum_rounds,
            momentum_start_mc, momentum_gain_pct, momentum_confirmed,
            volume_confirmed, buy_pressure, last_snapshot_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "sol",
            "AlphaToken",
            900,
            999,
            3,
            2,
            "gmgn",
            "lotto_momentum",
            "ALPHA",
            "Alpha",
            120000,
            70000,
            42000,
            35,
            24,
            8,
            3,
            90000,
            33.3,
            1,
            1,
            3.0,
            '{"quote_available":true,"quote_executable":true,"quote_clean":true,"quote_source":"jupiter","quote_ts":999,"route_available":true,"route_stable_recent":true,"liquidity_usd":70000,"spread_pct":0.9,"top10_pct":38,"bundler_rate":0.01,"rat_trader_rate":0.01,"entrapment_ratio":0.01}',
        ),
    )

    summary = record_a_class_fastlane_shadow_candidates(
        db,
        now_ts=1000,
        limit=10,
        config=load_a_class_config({"A_CLASS_ENABLED": "false"}),
    )

    assert summary["candidates"] == 4
    assert summary["sources"]["paper_fast_entry_queue"]["candidates"] == 1
    assert summary["sources"]["source_resonance_candidates"]["candidates"] == 1
    assert summary["sources"]["external_alpha_state"]["candidates"] == 1
    assert summary["sources"]["paper_decision_events"]["candidates"] == 1
    source_rows = db.execute(
        "SELECT source_table, action FROM a_class_decision_events ORDER BY source_table"
    ).fetchall()
    assert {row["source_table"] for row in source_rows} == {
        "external_alpha_state",
        "paper_fast_entry_queue",
        "source_resonance_candidates",
        "paper_decision_events",
    }
    assert any(row["action"] == "WOULD_ENTER" for row in source_rows)
    assert db.execute("SELECT COUNT(*) FROM canonical_trade_ledger").fetchone()[0] == 0


def test_live_enabled_shadow_scan_keeps_would_enter_and_returns_enter_candidates():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.executescript(
        """
        CREATE TABLE paper_decision_events (
            id INTEGER PRIMARY KEY,
            event_ts REAL,
            token_ca TEXT,
            symbol TEXT,
            lifecycle_id TEXT,
            signal_ts INTEGER,
            route TEXT,
            component TEXT,
            event_type TEXT,
            decision TEXT,
            reason TEXT,
            data_source TEXT,
            payload_json TEXT
        );
        """
    )
    payload_json = (
        '{"quote_available":true,"quote_executable":true,"quote_clean":true,'
        '"quote_source":"jupiter","quote_age_sec":4,"route_available":true,'
        '"route_stable_recent":true,"liquidity_usd":90000,"spread_pct":0.8,'
        '"gmgn_pre_seen":true,"gmgn_activity_fresh":true,'
        '"source_resonance":true,"fresh_ath_refresh":true,"top10_pct":35,'
        '"bundler_rate":0.01,"rat_trader_rate":0.01,"entrapment_ratio":0.01}'
    )
    db.execute(
        """
        INSERT INTO paper_decision_events (
            id, event_ts, token_ca, symbol, lifecycle_id, signal_ts, route,
            component, event_type, decision, reason, data_source, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            31,
            996,
            "LiveReadyToken",
            "LIVE",
            "life-live",
            100,
            "ATH",
            "ath_uncertainty_scout",
            "reject",
            "block",
            "scout_quality_buy_pressure_weak",
            "paper_decision_events",
            payload_json,
        ),
    )

    summary = record_a_class_fastlane_shadow_candidates(
        db,
        now_ts=1000,
        limit=10,
        config=load_a_class_config({"A_CLASS_ENABLED": "true"}),
    )

    assert summary["candidates"] == 1
    assert summary["would_enter"] == 1
    assert len(summary["enter_candidates"]) == 1
    row = db.execute("SELECT action FROM a_class_decision_events").fetchone()
    assert row["action"] == "WOULD_ENTER"
    opportunity = db.execute(
        "SELECT would_enter_a_class, did_enter FROM opportunity_events"
    ).fetchone()
    assert opportunity["would_enter_a_class"] == 1
    assert opportunity["did_enter"] == 0
    assert db.execute("SELECT COUNT(*) FROM canonical_trade_ledger").fetchone()[0] == 0


def test_external_alpha_candidate_uses_gmgn_state_and_snapshot_quote():
    row = {
        "chain": "sol",
        "token_ca": "AlphaToken",
        "first_seen_ts": 900,
        "last_seen_ts": 999,
        "seen_count": 3,
        "changed_count": 2,
        "source_last": "gmgn",
        "category_last": "lotto_momentum",
        "symbol": "ALPHA",
        "last_market_cap": 120000,
        "last_liquidity": 70000,
        "last_volume": 42000,
        "last_swaps": 35,
        "last_buys": 24,
        "last_sells": 8,
        "momentum_rounds": 3,
        "momentum_start_mc": 90000,
        "momentum_gain_pct": 33.3,
        "momentum_confirmed": 1,
        "volume_confirmed": 1,
        "buy_pressure": 3.0,
        "last_snapshot_json": '{"quote_available":true,"quote_executable":true,"quote_clean":true,"quote_source":"jupiter","quote_ts":999,"route_available":true,"route_stable_recent":true,"liquidity_usd":70000,"spread_pct":0.9}',
    }

    candidate = candidate_from_external_alpha_row(row, now_ts=1000)

    assert candidate.source_component == "external_alpha_shadow"
    assert candidate.route_bucket == "LOTTO"
    assert candidate.gmgn_pre_seen is True
    assert candidate.gmgn_activity_fresh is True
    assert candidate.fresh_momentum is True
    assert candidate.quote_available is True
    assert candidate.quote_executable is True
    assert candidate.route_available is True
    assert candidate.quote_age_sec == 1
    assert candidate.liquidity_usd == 70000


def test_shadow_scan_deduplicates_would_enter_by_token_lifecycle_window():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.executescript(
        """
        CREATE TABLE paper_decision_events (
            id INTEGER PRIMARY KEY,
            event_ts REAL,
            token_ca TEXT,
            symbol TEXT,
            lifecycle_id TEXT,
            signal_ts INTEGER,
            route TEXT,
            component TEXT,
            event_type TEXT,
            decision TEXT,
            reason TEXT,
            data_source TEXT,
            payload_json TEXT
        );
        """
    )
    payload = '{"quote_available":true,"quote_executable":true,"quote_clean":true,"quote_source":"jupiter","quote_age_sec":4,"route_available":true,"route_stable_recent":true,"liquidity_usd":80000,"spread_pct":0.8,"gmgn_pre_seen":true,"gmgn_activity_fresh":true,"source_resonance":true,"fresh_ath_refresh":true,"top10_pct":35,"bundler_rate":0.01,"rat_trader_rate":0.01,"entrapment_ratio":0.01}'
    for row_id, component in ((10, "ath_uncertainty_scout"), (11, "entry_mode_quality")):
        db.execute(
            """
            INSERT INTO paper_decision_events (
                id, event_ts, token_ca, symbol, lifecycle_id, signal_ts, route,
                component, event_type, decision, reason, data_source, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row_id,
                996 + (row_id - 10),
                "DupToken",
                "DUP",
                "life-dup",
                100,
                "ATH",
                component,
                "reject",
                "block",
                "scout_quality_buy_pressure_weak",
                "paper_decision_events",
                payload,
            ),
        )

    summary = record_a_class_fastlane_shadow_candidates(
        db,
        now_ts=1000,
        limit=10,
        config=load_a_class_config({"A_CLASS_ENABLED": "false", "A_CLASS_OPPORTUNITY_DEDUP_SEC": "300"}),
    )

    assert summary["candidates"] == 2
    assert summary["would_enter"] == 1
    assert summary["shadow"] == 1
    rows = db.execute(
        """
        SELECT action, reason, opportunity_key, is_duplicate, duplicate_of_id, score
        FROM a_class_decision_events
        ORDER BY id
        """
    ).fetchall()
    assert [row["action"] for row in rows] == ["WOULD_ENTER", "SHADOW"]
    assert rows[1]["reason"] == "a_class_duplicate_opportunity_window"
    assert rows[1]["is_duplicate"] == 1
    assert rows[1]["duplicate_of_id"] == 1
    assert rows[0]["opportunity_key"] == rows[1]["opportunity_key"]


def test_provider_hydrator_refreshes_missing_execution_evidence():
    calls = []

    def fake_fetcher(**kwargs):
        calls.append(kwargs)
        return {
            "status": 200,
            "latency_ms": 42,
            "data": {
                "outAmount": "12345",
                "priceImpactPct": "0.004",
                "routePlan": [{"percent": 100}],
                "requestId": "req-1",
            },
        }

    candidate = base_candidate(
        quote_available=False,
        quote_executable=False,
        quote_source=None,
        quote_age_sec=None,
        route_available=False,
        route_failure_reason="route_unavailable_unknown",
        spread_pct=None,
        spread_verified=False,
    )
    budget = {"remaining": 1}
    hydrated = enrich_candidate_with_db_evidence(
        sqlite3.connect(":memory:"),
        candidate,
        now_ts=2_000,
        config=load_a_class_config({"A_CLASS_PROVIDER_HYDRATE_ENABLED": "true"}),
        provider_budget=budget,
        provider_fetcher=fake_fetcher,
    )

    assert len(calls) == 1
    assert budget["remaining"] == 0
    assert hydrated.quote_available is True
    assert hydrated.quote_executable is True
    assert hydrated.route_available is True
    assert hydrated.quote_source == "jupiter_ultra_provider_hydrate"
    assert hydrated.quote_age_sec == 0
    assert hydrated.spread_pct == 0.4
    assert hydrated.route_failure_reason == "provider_hydrated_route_ok"

    decision = evaluate_a_class_fastlane(
        hydrated,
        now_ts=2_000,
        config=load_a_class_config({"A_CLASS_PROVIDER_HYDRATE_ENABLED": "false"}),
    )
    assert decision.action == "ENTER"


def test_provider_hydrator_failure_stays_fail_closed():
    def fake_fetcher(**_kwargs):
        return {
            "status": 400,
            "latency_ms": 30,
            "data": {"errorCode": "COULD_NOT_FIND_ANY_ROUTE", "message": "Could not find any route"},
        }

    candidate = base_candidate(
        quote_available=False,
        quote_executable=False,
        quote_source=None,
        quote_age_sec=None,
        route_available=False,
        route_failure_reason="route_unavailable_unknown",
        spread_pct=None,
        spread_verified=False,
    )
    hydrated = enrich_candidate_with_db_evidence(
        sqlite3.connect(":memory:"),
        candidate,
        now_ts=2_000,
        config=load_a_class_config({"A_CLASS_PROVIDER_HYDRATE_ENABLED": "true"}),
        provider_budget={"remaining": 1},
        provider_fetcher=fake_fetcher,
    )

    assert hydrated.route_available is False
    assert hydrated.route_failure_reason == "no_route"
    decision = evaluate_a_class_fastlane(
        hydrated,
        now_ts=2_000,
        config=load_a_class_config({"A_CLASS_PROVIDER_HYDRATE_ENABLED": "false"}),
    )
    assert decision.action == "BLOCK"
    assert "route_failure_red_flag" in decision.hard_blockers


def test_provider_hydrator_budget_prevents_extra_probe():
    calls = []

    def fake_fetcher(**kwargs):
        calls.append(kwargs)
        return {"status": 200, "latency_ms": 1, "data": {"outAmount": "1", "routePlan": [{}]}}

    candidate = base_candidate(
        quote_available=False,
        quote_executable=False,
        quote_source=None,
        quote_age_sec=None,
        route_available=False,
        spread_pct=None,
        spread_verified=False,
    )
    hydrated = enrich_candidate_with_db_evidence(
        sqlite3.connect(":memory:"),
        candidate,
        now_ts=2_000,
        config=load_a_class_config({"A_CLASS_PROVIDER_HYDRATE_ENABLED": "true"}),
        provider_budget={"remaining": 0},
        provider_fetcher=fake_fetcher,
    )

    assert calls == []
    assert hydrated.quote_executable is False

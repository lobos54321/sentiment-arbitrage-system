import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from a_class_block_cause_breakdown import build_breakdown, classify_blocker, classify_event


def memory_db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    return db


def test_route_unavailable_disambiguates_infra_from_market():
    infra = classify_blocker(
        "route_unavailable",
        {"data_confidence": "unknown", "quote_source": None, "route_failure_reason": None},
    )
    market = classify_blocker(
        "route_unavailable",
        {"data_confidence": "quote_clean", "quote_source": "jupiter", "route_failure_reason": "no_route"},
    )

    assert infra["category"] == "INFRA"
    assert infra["recoverability"] == "provider_or_evidence_recoverable"
    assert market["category"] == "MARKET"


def test_event_market_red_flag_wins_over_infra_quote_missing():
    event = classify_event(
        {
            "action": "BLOCK",
            "hard_blockers_json": '["quote_not_available", "creator_close"]',
            "risk_json": '{"data_confidence":"unknown"}',
        }
    )

    assert event["category"] == "MARKET"
    assert event["blocked"] is True


def test_persisted_block_cause_wins_over_legacy_fallback():
    event = classify_event(
        {
            "action": "WOULD_ENTER",
            "block_cause": "INFRA",
            "recoverability": "provider_or_evidence_recoverable",
            "classification_reason": "persisted_at_write_time",
            "hard_blockers_json": '["creator_close"]',
            "blocker_classifications_json": '[{"blocker":"quote_not_available","category":"INFRA","recoverability":"provider_or_evidence_recoverable","reason":"quote_provider_or_freshness_missing"}]',
        }
    )

    assert event["category"] == "INFRA"
    assert event["recoverability"] == "provider_or_evidence_recoverable"
    assert event["classification_reason"] == "persisted_at_write_time"
    assert event["would_enter_a_class"] is True


def test_live_market_red_flag_names_are_market():
    assert classify_blocker("liquidity_below_min", {})["category"] == "MARKET"
    assert classify_blocker("entrapment_red_flag", {})["category"] == "MARKET"
    assert classify_blocker("bundler_red_flag", {})["category"] == "MARKET"


def test_build_breakdown_from_a_class_and_opportunity_events():
    db = memory_db()
    db.executescript(
        """
        CREATE TABLE a_class_decision_events (
            id INTEGER PRIMARY KEY,
            event_ts REAL,
            token_ca TEXT,
            symbol TEXT,
            lifecycle_id TEXT,
            route_bucket TEXT,
            source_table TEXT,
            source_component TEXT,
            source_reason TEXT,
            action TEXT,
            would_action TEXT,
            reason TEXT,
            hard_blockers_json TEXT,
            risk_json TEXT,
            candidate_json TEXT,
            denominator_key TEXT,
            expected_rr REAL,
            score REAL,
            grade TEXT,
            size_sol REAL
        );
        CREATE TABLE opportunity_events (
            id INTEGER PRIMARY KEY,
            event_ts REAL,
            token_ca TEXT,
            symbol TEXT,
            lifecycle_id TEXT,
            route_bucket TEXT,
            source_type TEXT,
            source_component TEXT,
            source_reason TEXT,
            hard_blockers_json TEXT,
            raw_payload_json TEXT,
            expected_rr REAL,
            matrix_score REAL,
            quote_available INTEGER,
            quote_executable INTEGER,
            quote_clean INTEGER,
            route_available INTEGER,
            quote_source TEXT,
            quote_age_sec REAL,
            data_confidence TEXT,
            provider_data_state TEXT,
            provider_reason TEXT,
            evidence_status TEXT,
            quote_failure_reason TEXT,
            liquidity_usd REAL,
            spread_pct REAL,
            would_enter_a_class INTEGER,
            did_enter INTEGER
        );
        """
    )
    db.executemany(
        """
        INSERT INTO a_class_decision_events (
            id, event_ts, token_ca, symbol, route_bucket, source_table,
            source_component, source_reason, action, would_action, reason,
            hard_blockers_json, risk_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                1,
                1000,
                "infra-token",
                "INFRA",
                "A_GRADE",
                "external_alpha_state",
                "external_alpha_shadow",
                "gmgn_trending_1m",
                "BLOCK",
                "WOULD_ENTER",
                "hard_prefilter_failed",
                '["quote_not_available","quote_source_missing","route_unavailable"]',
                '{"data_confidence":"unknown"}',
            ),
            (
                2,
                1001,
                "market-token",
                "MARKET",
                "A_GRADE",
                "external_alpha_state",
                "external_alpha_shadow",
                "gmgn_trending_1m",
                "BLOCK",
                None,
                "hard_prefilter_failed",
                '["creator_close","quote_not_available"]',
                '{"data_confidence":"unknown"}',
            ),
        ],
    )
    db.execute(
        """
        INSERT INTO opportunity_events (
            id, event_ts, token_ca, symbol, route_bucket, source_type,
            source_component, source_reason, hard_blockers_json,
            quote_available, quote_executable, route_available, data_confidence,
            would_enter_a_class, did_enter
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            3,
            1002,
            "policy-token",
            "POLICY",
            "ATH",
            "paper_decision_events",
            "matrix_evaluator",
            "matrices not yet aligned",
            '["matrices not yet aligned"]',
            1,
            1,
            1,
            "quote_clean",
            0,
            0,
        ),
    )
    db.commit()

    result = build_breakdown(db, since_ts=999, source="all", recent_limit=10)

    assert result["total_events"] == 3
    assert result["infra_recoverable"]["events"] == 1
    assert result["infra_recoverable"]["would_enter_n"] == 1
    assert result["market_unexecutable"]["events"] == 1
    assert result["policy_guardrail"]["events"] == 1
    assert result["source_issues"] == []
    assert result["blocker_summary"][0]["n"] >= 1

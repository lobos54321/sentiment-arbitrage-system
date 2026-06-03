import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from fastlane_config import load_a_class_config
from opportunity_freshness import evaluate_opportunity_freshness


def test_old_raw_signal_with_fresh_quote_is_fresh():
    now = 1_000.0
    decision = evaluate_opportunity_freshness(
        {
            "signal_ts": now - 3_600,
            "quote_age_sec": 5,
            "quote_available": True,
            "quote_executable": True,
            "quote_clean": True,
        },
        now_ts=now,
        config=load_a_class_config({}),
    )

    assert decision.fresh is True
    assert decision.raw_signal_age_sec == 3_600
    assert decision.opportunity_age_sec == 5
    assert "fresh_quote" in decision.freshness_sources


def test_fresh_raw_signal_with_stale_quote_is_not_fresh():
    now = 1_000.0
    decision = evaluate_opportunity_freshness(
        {
            "signal_ts": now - 30,
            "quote_age_sec": 120,
            "quote_available": True,
            "quote_executable": True,
        },
        now_ts=now,
        config=load_a_class_config({}),
    )

    assert decision.fresh is False
    assert decision.reason == "no_fresh_opportunity"


def test_fresh_gmgn_activity_resets_opportunity_age():
    now = 1_000.0
    decision = evaluate_opportunity_freshness(
        {
            "signal_ts": now - 900,
            "quote_age_sec": 120,
            "quote_available": True,
            "quote_executable": True,
            "gmgn_activity_fresh": True,
            "gmgn_last_seen_age_sec": 12,
        },
        now_ts=now,
        config=load_a_class_config({}),
    )

    assert decision.fresh is True
    assert decision.opportunity_age_sec == 12
    assert "fresh_gmgn_activity" in decision.freshness_sources


def test_fresh_reclaim_and_ath_refresh_are_sources():
    now = 1_000.0
    decision = evaluate_opportunity_freshness(
        {
            "signal_ts": now - 600,
            "fresh_reclaim": True,
            "reclaim_age_sec": 20,
            "fresh_ath_refresh": True,
            "ath_refresh_age_sec": 40,
        },
        now_ts=now,
        config=load_a_class_config({}),
    )

    assert decision.fresh is True
    assert decision.opportunity_age_sec == 20
    assert "fresh_reclaim" in decision.freshness_sources
    assert "fresh_ath_refresh" in decision.freshness_sources


def test_no_fresh_source_is_stale():
    decision = evaluate_opportunity_freshness(
        {"signal_ts": 900, "source_resonance": True},
        now_ts=1_000,
        config=load_a_class_config({}),
    )

    assert decision.fresh is False
    assert decision.reason == "no_fresh_opportunity"

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from a_class_fastlane import AClassCandidate
from a_class_opportunity_matrix import evaluate_a_class_opportunity_matrix
from opportunity_freshness import FreshnessDecision


def fresh():
    return FreshnessDecision(
        fresh=True,
        opportunity_ts=100,
        freshness_sources=["fresh_quote", "fresh_gmgn_activity", "fresh_momentum"],
        raw_signal_age_sec=3600,
        opportunity_age_sec=8,
        reason="fresh_opportunity",
        data_confidence="quote_clean",
    )


def test_green_matrix_for_clean_resonance_candidate():
    candidate = AClassCandidate(
        token_ca="TokenA",
        route_bucket="ATH",
        quote_available=True,
        quote_executable=True,
        quote_clean=True,
        quote_source="gmgn",
        quote_age_sec=3,
        route_available=True,
        route_stable_recent=True,
        liquidity_usd=50_000,
        spread_pct=1.0,
        gmgn_pre_seen=True,
        source_resonance=True,
        fresh_momentum=True,
        momentum_pct=0.02,
        fresh_ath_refresh=True,
        top10_pct=35,
        bundler_rate=0.05,
        rat_trader_rate=0.01,
        entrapment_ratio=0.0,
        missed_dog_cohort_strong=True,
    )

    matrix = evaluate_a_class_opportunity_matrix(candidate, fresh())

    assert matrix["matrix_version"] == "v1.a_class_18_cell"
    assert matrix["red_count"] == 0
    assert matrix["execution_quality"] == "GREEN"
    assert matrix["security_cleanliness"] == "GREEN"
    assert matrix["matrix_grade"] in {"A", "STRONG_A", "A_PLUS"}


def test_hard_execution_red_cannot_be_a_tiny_candidate():
    candidate = AClassCandidate(
        token_ca="TokenB",
        route_bucket="LOTTO",
        quote_available=True,
        quote_executable=False,
        quote_source="gmgn",
        quote_age_sec=2,
        route_available=False,
        liquidity_usd=100_000,
        spread_pct=1.0,
        gmgn_pre_seen=True,
        source_resonance=True,
    )

    matrix = evaluate_a_class_opportunity_matrix(candidate, fresh())

    assert matrix["execution_quality"] == "RED"
    assert "execution_quality" in matrix["hard_red_dimensions"]
    assert matrix["action_floor"] == "BLOCK"

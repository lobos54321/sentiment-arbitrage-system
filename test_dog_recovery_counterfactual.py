import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

from dog_recovery_counterfactual import assess_candidate, build_report, tier_from_pct, tier_from_ratio


def test_dog_tiers():
    assert tier_from_ratio(1.0) == "gold"
    assert tier_from_ratio(0.5) == "silver"
    assert tier_from_ratio(0.25) == "bronze"
    assert tier_from_ratio(0.249) == "sub25"
    assert tier_from_pct(100) == "gold"
    assert tier_from_pct(50) == "silver"
    assert tier_from_pct(25) == "bronze"


def test_assessment_maps_ttl_to_extension_watch():
    row = {
        "symbol": "DOG",
        "token_ca": "Token1",
        "route": "LOTTO",
        "component": "upstream_gate",
        "reject_reason": "tracking_ttl_expired",
        "max_pnl": 1.2,
        "tradability_status": "tradable_reclaim",
    }

    assessment = assess_candidate(row, [])

    assert assessment.tier == "gold"
    assert assessment.lane == "ttl_extension_watch"
    assert assessment.defensible_first_entry is True


def test_assessment_marks_post_exit_and_stop_before_peak_as_not_first_entry_safe():
    row = {
        "symbol": "DOG",
        "token_ca": "Token1",
        "route": "LOTTO",
        "component": "upstream_gate",
        "reject_reason": "tracking_ttl_expired",
        "max_pnl": 2.0,
        "tradability_status": "would_stop_before_peak",
    }
    lifecycles = [
        {
            "token_ca": "Token1",
            "has_trade": True,
            "trade_id": 7,
            "exit_reason": "phase_probe_no_follow_fast_fail_20s",
        }
    ]

    assessment = assess_candidate(row, lifecycles)

    assert assessment.lane == "post_exit_runner_watch"
    assert "stop_before_peak_requires_reentry" in assessment.vulnerabilities
    assert "already_traded_needs_post_exit_logic" in assessment.vulnerabilities
    assert assessment.defensible_first_entry is False


def test_build_report_counts_caught_and_missed_dogs():
    missed = {
        "top_clean_quote_dogs": [
            {
                "symbol": "MISS",
                "token_ca": "Miss1",
                "route": "ATH",
                "component": "matrix_evaluator",
                "reject_reason": "timeout (121min >= 120min)",
                "max_pnl": 0.75,
                "tradability_status": "tradable_reclaim",
            }
        ]
    }
    replay = {
        "trades": [
            {
                "token_ca": "Hit1",
                "symbol": "HIT",
                "position_size_sol": 0.003,
                "peak_pnl_pct": 120,
                "pnl_pct": 80,
                "entry_mode": "lotto_not_ath_reclaim_tiny_probe",
            }
        ]
    }
    lifecycle = {"lifecycles": []}

    report = build_report(missed, replay, lifecycle)

    assert report["missed_summary"]["tier_counts"] == {"silver": 1}
    assert report["missed_summary"]["lane_counts"] == {"matrix_recovery_watch": 1}
    assert report["caught_summary"]["by_peak"]["gold"]["trades"] == 1
    assert report["caught_summary"]["by_realized"]["silver"]["trades"] == 1

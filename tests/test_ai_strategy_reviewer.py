import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from ai_counterfactual_auditor import audit_counterfactual_denominator
from ai_missed_dog_reviewer import review_missed_dog_blockers
from strategy_goal_controller import build_strategy_goal_controller_actions


def p0_fixture():
    return {
        "available": True,
        "quote_clean_gold_silver_seen_count": 12,
        "quote_clean_gold_silver_would_enter_count": 7,
        "outlier_trimmed_would_rr": 3.1,
        "would_enter_no_route_rate": 0.02,
        "would_enter_trapped_rate": 0.0,
        "unknown_data_rate": 0.0,
        "discovery_exit": {
            "advisory": "PROMOTE_TINY_CANARY",
            "canary_size_sol": 0.001,
        },
        "missed_blockers": [
            {
                "route": "ATH",
                "component": "scout_quality",
                "reject_reason": "scout_quality_buy_pressure_weak",
                "unique_tokens": 4,
                "gold_n": 1,
                "silver_n": 1,
                "max_adjusted_peak": 1.5,
            },
            {
                "route": "LOTTO",
                "component": "security",
                "reject_reason": "security_red_flag_creator_dump",
                "unique_tokens": 1,
                "gold_n": 1,
                "silver_n": 0,
                "max_adjusted_peak": 2.0,
            },
        ],
    }


def test_missed_dog_reviewer_never_downgrades_security_blocker():
    review = review_missed_dog_blockers(p0_fixture())
    by_reason = {row["reject_reason"]: row for row in review["recommendations"]}

    assert by_reason["scout_quality_buy_pressure_weak"]["recommendation"] == "allow_a_class_only"
    assert by_reason["security_red_flag_creator_dump"]["recommendation"] == "keep_hard_block"
    assert review["can_trigger_trade"] is False


def test_counterfactual_audit_and_controller_promote_only_advisory_tiny():
    audit = audit_counterfactual_denominator(p0_fixture())
    controller = build_strategy_goal_controller_actions(
        rolling_goal_status={"status": "under_target"},
        a_class_p0_discovery=p0_fixture(),
        counterfactual_audit=audit,
        missed_dog_review={"allow_a_class_only_count": 1},
    )

    assert audit["pass"] is True
    assert controller["advisory_only"] is True
    assert any(action["action"] == "TINY_CANARY" for action in controller["actions"])
    assert any(action["action"] == "ALLOW_A_CLASS_ONLY" for action in controller["actions"])

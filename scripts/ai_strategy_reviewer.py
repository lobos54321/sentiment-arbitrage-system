"""Composite advisory reviewer for A_CLASS P0 evidence."""

from __future__ import annotations

from ai_counterfactual_auditor import audit_counterfactual_denominator
from ai_missed_dog_reviewer import review_missed_dog_blockers
from strategy_goal_controller import build_strategy_goal_controller_actions


def build_ai_strategy_review(a_class_p0_discovery: dict | None, rolling_goal_status: dict | None = None) -> dict:
    missed_review = review_missed_dog_blockers(a_class_p0_discovery)
    counterfactual_audit = audit_counterfactual_denominator(a_class_p0_discovery)
    controller = build_strategy_goal_controller_actions(
        rolling_goal_status=rolling_goal_status,
        a_class_p0_discovery=a_class_p0_discovery,
        counterfactual_audit=counterfactual_audit,
        missed_dog_review=missed_review,
    )
    return {
        "schema_version": "v1.ai_strategy_review.bundle",
        "advisory_only": True,
        "missed_dog_review": missed_review,
        "counterfactual_audit": counterfactual_audit,
        "controller_actions": controller,
    }

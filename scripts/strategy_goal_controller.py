"""Rolling strategy goal controller actions.

The controller is advisory-only in this phase. It reads objective evidence and
returns mode actions; execution code must still apply hard gates and budgets.
"""

from __future__ import annotations

from typing import Any


def _num(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def build_strategy_goal_controller_actions(
    *,
    rolling_goal_status: dict | None = None,
    a_class_p0_discovery: dict | None = None,
    counterfactual_audit: dict | None = None,
    missed_dog_review: dict | None = None,
) -> dict:
    goal = rolling_goal_status or {}
    p0 = a_class_p0_discovery or {}
    audit = counterfactual_audit or {}
    missed = missed_dog_review or {}
    actions = []
    blockers = []

    if goal.get("status") in {"insufficient_sample", "evidence_unavailable"}:
        blockers.append("rolling_goal_sample_or_evidence_insufficient")
    if goal.get("max_single_trade_loss_ok") is False:
        actions.append({
            "mode": "ALL_LIVE_RISK",
            "action": "DISABLE",
            "reason": "single_trade_loss_limit_breached",
        })
    if p0.get("discovery_exit", {}).get("advisory") == "PROMOTE_TINY_CANARY" and audit.get("pass"):
        actions.append({
            "mode": "A_CLASS_FASTLANE",
            "action": "TINY_CANARY",
            "size_sol": p0.get("discovery_exit", {}).get("canary_size_sol", 0.001),
            "reason": "counterfactual_denominator_and_rr_passed",
            "requires_human_approval": True,
        })
    elif p0.get("available") is False or not audit.get("pass", False):
        actions.append({
            "mode": "A_CLASS_FASTLANE",
            "action": "SHADOW",
            "reason": "p0_discovery_or_counterfactual_audit_not_green",
        })

    no_route_rate = _num(p0.get("would_enter_no_route_rate"), 0.0) or 0.0
    trapped_rate = _num(p0.get("would_enter_trapped_rate"), 0.0) or 0.0
    if no_route_rate > 0.10 or trapped_rate > 0.10:
        actions.append({
            "mode": "A_CLASS_FASTLANE",
            "action": "DISABLE",
            "reason": "route_health_risk_above_threshold",
        })
    allow_count = int((missed.get("allow_a_class_only_count") or missed.get("extra", {}).get("allow_a_class_only_count") or 0))
    if allow_count:
        actions.append({
            "mode": "MISSED_DOG_BLOCKERS",
            "action": "ALLOW_A_CLASS_ONLY",
            "reason": "missed_dog_reviewer_found_soft_blocker_candidates",
            "candidate_blocker_count": allow_count,
        })

    next_safe_action = "keep_a_class_shadow"
    if any(action["action"] == "TINY_CANARY" for action in actions):
        next_safe_action = "prepare_0_001_tiny_paper_after_observability_green"
    if any(action["action"] == "DISABLE" for action in actions):
        next_safe_action = "disable_or_shadow_risky_modes"

    return {
        "schema_version": "v1.strategy_goal_controller.advisory",
        "advisory_only": True,
        "can_trigger_trade": False,
        "actions": actions,
        "blockers": blockers,
        "next_safe_action": next_safe_action,
    }

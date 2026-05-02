#!/usr/bin/env python3
"""Unified entry decision contract for LOTTO/MATRIX final buy checks."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class EntryDecisionContract:
    lifecycle_profile: str
    entry_mode: str
    data_confidence: float
    p_follow: float
    expected_upside_pct: float
    expected_loss_pct: float
    odds_r: float
    spread_cost_pct: float
    exit_cost_buffer_pct: float
    decision: str
    reason: str

    def to_dict(self) -> dict:
        return asdict(self)


def build_entry_decision_contract(
    *,
    entry_readiness_policy=None,
    entry_mode="",
    data_confidence=1.0,
    p_follow=None,
    spread_cost_pct=0.0,
    exit_cost_buffer_pct=1.5,
    timing_confirmed=True,
):
    """Build the canonical final-entry contract.

    The contract is deliberately small: it converts lifecycle policy, timing,
    and execution costs into one auditable pass/reject decision.
    """
    policy = entry_readiness_policy or {}
    if hasattr(policy, "to_dict"):
        policy = policy.to_dict()
    lifecycle_profile = str(policy.get("lifecycle_profile") or "UNKNOWN")
    min_p_follow = float(policy.get("min_p_follow") or 0.0)
    expected_upside_pct = float(policy.get("expected_upside_pct") or 0.0)
    expected_loss_pct = float(policy.get("expected_loss_pct") or 0.0)
    min_odds_r = float(policy.get("min_odds_r") or 0.0)
    actual_p_follow = float(p_follow if p_follow is not None else min_p_follow)
    total_cost_pct = max(0.0, float(spread_cost_pct or 0.0)) + max(0.0, float(exit_cost_buffer_pct or 0.0))
    odds_r = ((expected_upside_pct - total_cost_pct) / expected_loss_pct) if expected_loss_pct > 0 else 0.0

    decision = "pass"
    reason = "entry_contract_pass"
    if not timing_confirmed:
        decision = "reject"
        reason = "timing_not_confirmed"
    elif actual_p_follow < min_p_follow:
        decision = "reject"
        reason = "p_follow_below_policy"
    elif odds_r < min_odds_r:
        decision = "reject"
        reason = "odds_after_cost_below_policy"
    elif data_confidence < 0.5:
        decision = "observe"
        reason = "data_confidence_low"

    return EntryDecisionContract(
        lifecycle_profile=lifecycle_profile,
        entry_mode=str(entry_mode or ""),
        data_confidence=round(float(data_confidence or 0.0), 3),
        p_follow=round(actual_p_follow, 3),
        expected_upside_pct=round(expected_upside_pct, 3),
        expected_loss_pct=round(expected_loss_pct, 3),
        odds_r=round(odds_r, 3),
        spread_cost_pct=round(float(spread_cost_pct or 0.0), 3),
        exit_cost_buffer_pct=round(float(exit_cost_buffer_pct or 0.0), 3),
        decision=decision,
        reason=reason,
    )

#!/usr/bin/env python3
"""Read-only reject-reason counterfactual reviewer.

Combines quality/timing reject instrumentation with stale-before-final watch
evidence. It produces protective / harmful / mixed / indeterminate verdicts
with dud-inclusive denominators. It never changes strategy, gates,
final_entry_contract, A_CLASS mode, executor, wallet, canary, or risk.
"""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from collections import defaultdict
from pathlib import Path


SCHEMA_VERSION = "reject_reason_counterfactual_review.v1"
EVIDENCE_LEVEL = "discovery_same_window_shadow_only"


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_json(path: str | Path, default=None):
    target = Path(path)
    if not target.exists():
        return default if default is not None else {}
    try:
        return json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return default if default is not None else {}


def write_json(path: str | Path, payload) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def rate(num, den):
    if not den:
        return None
    return round(float(num) / float(den), 6)


def reason_key(row: dict) -> str:
    return "|".join(
        str(row.get(key) or "UNKNOWN")
        for key in ["stage", "component", "event_type", "decision", "reason"]
    )


def reason_name(row: dict) -> str:
    return str(row.get("reason") or "UNKNOWN")


def classify_mechanism(row: dict) -> str:
    component = row.get("component") or "UNKNOWN"
    reason = row.get("reason") or "UNKNOWN"
    return f"{component}:{reason}"


def infer_lean(row: dict, examples: list[dict], base_rate: float | None) -> tuple[str, list[str]]:
    reasons = []
    p_gs = row.get("p_gold_silver_given_reject_by_reason")
    raw_gs = int(row.get("raw_gs_reject_signal_count") or row.get("raw_gs_reject_events") or 0)
    dud_kills = row.get("dud_kills")
    if row.get("all_reject_signal_count") is None:
        reasons.append("missing_dud_inclusive_denominator")
        return "indeterminate", reasons
    matching_examples = [
        ex for ex in examples
        if (ex.get("attribution") or {}).get("reason") == row.get("reason")
        and (ex.get("attribution") or {}).get("component") == row.get("component")
    ]
    ordering_counts = defaultdict(int)
    for ex in matching_examples:
        ordering = ((ex.get("peak_vs_reject_ordering") or {}).get("ordering")) or "missing"
        ordering_counts[ordering] += 1
    reject_before = ordering_counts.get("reject_before_or_at_peak", 0)
    reject_after = ordering_counts.get("reject_after_peak", 0)
    reason_text = " ".join(str(row.get(k) or "").lower() for k in ["component", "event_type", "reason"])
    monotone_stale = "stale" in reason_text
    if raw_gs == 0 and dud_kills:
        reasons.append("no_raw_gold_silver_kills_with_dud_denominator")
        return "protective", reasons
    if base_rate is not None and p_gs is not None:
        if p_gs <= max(0.0, base_rate - 0.03) and dud_kills and dud_kills > raw_gs:
            reasons.append("gold_silver_rate_below_base_and_mostly_duds")
            return "protective", reasons
        if p_gs >= base_rate + 0.05 and raw_gs > 0:
            if reject_before and (monotone_stale or reject_after == 0):
                reasons.append("reject_before_peak_with_above_base_gold_silver_rate")
                return "harmful", reasons
            reasons.append("above_base_gold_silver_rate_but_ordering_or_monotonicity_mixed")
            return "mixed", reasons
    if reject_before and reject_after:
        reasons.append("both_reject_before_peak_and_reject_after_peak_examples")
        return "mixed", reasons
    if reject_before and monotone_stale:
        reasons.append("monotone_stale_reject_before_peak")
        return "harmful", reasons
    if reject_after and not reject_before:
        reasons.append("observed_reject_after_peak_examples")
        return "protective", reasons
    if raw_gs > 0:
        reasons.append("raw_gold_silver_kills_present_but_formal_ordering_insufficient")
        return "mixed", reasons
    reasons.append("insufficient_named_ordering_evidence")
    return "indeterminate", reasons


def build_ordering_evidence(row: dict, examples: list[dict]) -> dict:
    matched = [
        ex for ex in examples
        if (ex.get("attribution") or {}).get("reason") == row.get("reason")
        and (ex.get("attribution") or {}).get("component") == row.get("component")
    ]
    counts = defaultdict(int)
    named = []
    for ex in matched[:20]:
        ordering = ((ex.get("peak_vs_reject_ordering") or {}).get("ordering")) or "missing"
        counts[ordering] += 1
        named.append(
            {
                "signal_id": ex.get("signal_id"),
                "token_ca": ex.get("token_ca"),
                "decision_event_id": ex.get("decision_event_id"),
                "reject_ts": ex.get("reject_ts"),
                "price_at_reject": ex.get("price_at_reject"),
                "quote_age_at_reject": ex.get("quote_age_at_reject"),
                "ordering": ex.get("peak_vs_reject_ordering"),
            }
        )
    return {
        "named_example_count": len(matched),
        "ordering_counts": dict(counts),
        "named_examples": named,
        "coverage_note": "Named examples are limited to examples retained by the quality_timing_reject_research_audit artifact.",
    }


def build_report(args) -> dict:
    base = Path(args.artifact_dir)
    qt = load_json(base / "quality_timing_reject_research_audit_24h.json", {})
    stale = load_json(base / "pending_stale_before_final_watch_validation_24h.json", {})
    gap = load_json(base / "capture_60_gap_report.json", {})
    metrics = load_json(base / "capture_stage_metrics.json", {})
    cf = qt.get("reject_counterfactuals") or {}
    denom = cf.get("dud_inclusive_denominator") or {}
    examples = qt.get("top_examples") or []
    base_rate = denom.get("base_gold_silver_rate")
    if base_rate is not None:
        base_rate = float(base_rate)

    target_60_count = int(metrics.get("target_60_count") or gap.get("target_60_count") or 0)
    current_final = int(metrics.get("final_eligibility_capture_count") or 0)
    per_reason = []
    for row in cf.get("per_reason_denominators") or []:
        lean, lean_reasons = infer_lean(row, examples, base_rate)
        gs_kills = int(row.get("raw_gs_reject_signal_count") or row.get("raw_gs_reject_events") or 0)
        ceiling_count = current_final + gs_kills
        per_reason.append(
            {
                "reason_key": reason_key(row),
                "mechanism": classify_mechanism(row),
                "stage": row.get("stage"),
                "component": row.get("component"),
                "event_type": row.get("event_type"),
                "decision": row.get("decision"),
                "reason": row.get("reason"),
                "gs_kills": {
                    "raw_gs_reject_events": row.get("raw_gs_reject_events"),
                    "raw_gs_reject_signal_count": row.get("raw_gs_reject_signal_count"),
                    "stage": row.get("stage"),
                },
                "dud_kills": {
                    "all_reject_signal_count": row.get("all_reject_signal_count"),
                    "dud_kills": row.get("dud_kills"),
                    "p_gold_silver_given_reject_by_reason": row.get("p_gold_silver_given_reject_by_reason"),
                    "base_gold_silver_rate": base_rate,
                    "lift_vs_base_rate": row.get("lift_vs_base_rate"),
                },
                "ordering_evidence": build_ordering_evidence(row, examples),
                "lean": lean,
                "lean_reasons": lean_reasons,
                "ceiling_if_bridged": {
                    "current_final_eligibility_count": current_final,
                    "ceiling_count_if_reason_bridged": ceiling_count,
                    "target_60_count": target_60_count,
                    "would_reach_60_alone": bool(target_60_count and ceiling_count >= target_60_count),
                },
                "proposed_shadow_probe": (
                    "quote_fresh_reanchor_shadow_probe"
                    if "stale" in reason_name(row).lower()
                    else row.get("shadow_quote_fresh_reanchor_watch_only")
                ),
                "promotion_allowed": False,
            }
        )
    per_reason.sort(
        key=lambda row: (
            -(row["gs_kills"].get("raw_gs_reject_signal_count") or 0),
            -(row["gs_kills"].get("raw_gs_reject_events") or 0),
            row["reason_key"],
        )
    )

    lean_counts = defaultdict(int)
    for row in per_reason:
        lean_counts[row["lean"]] += 1

    stale_probe = cf.get("shadow_quote_fresh_reanchor_variant") or {}
    report = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "reject_reason_counterfactual_review",
        "generated_at": utc_now(),
        "classification": "REJECT_REASON_COUNTERFACTUAL_REVIEW_READY",
        "evidence_level": EVIDENCE_LEVEL,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "paper_enablement_allowed": False,
        "automatic_runtime_change_allowed": False,
        "window_pins": {
            "quality_timing_reject_research_audit_24h.json": {
                "exists": bool(qt),
                "generated_at": qt.get("generated_at"),
                "schema_version": qt.get("schema_version"),
                "classification": qt.get("classification"),
                "since_ts": (qt.get("window") or {}).get("since_ts"),
                "until_ts": (qt.get("window") or {}).get("until_ts"),
            },
            "pending_stale_before_final_watch_validation_24h.json": {
                "exists": bool(stale),
                "generated_at": stale.get("generated_at"),
                "schema_version": stale.get("schema_version"),
                "classification": stale.get("classification"),
            },
        },
        "denominator": {
            **denom,
            "quality_timing_reject_event_rows": qt.get("quality_timing_reject_event_rows"),
            "target_60_count": target_60_count,
            "current_final_eligibility_count": current_final,
            "dud_inclusive_denominator_available": bool(denom.get("available")),
        },
        "formal_reason_verdicts": per_reason,
        "lean_counts": dict(lean_counts),
        "entry_execution_signal_stale_quote_reanchor_shadow_probe": {
            **stale_probe,
            "forward_data_status": "observed_shadow_only_same_window",
            "verdict": "SHADOW_PROBE_CONTINUE",
            "promotion_allowed": False,
            "notes": [
                "This is not a threshold change and not promotion evidence.",
                "The probe asks whether a fresh quote at reject time would have made stale-by-original-signal-time less harmful.",
            ],
        },
        "pending_stale_before_final": {
            "classification": stale.get("classification"),
            "current_stale_before_final_event_count": stale.get("current_stale_before_final_event_count"),
            "repeated_selected_cluster_count": stale.get("repeated_selected_cluster_count"),
            "repeated_selected_cluster_rate": stale.get("repeated_selected_cluster_rate"),
            "next_action": stale.get("next_action"),
            "promotion_allowed": stale.get("promotion_allowed", False),
        },
        "notes": [
            "Lean values are shadow-only counterfactual judgments, not strategy changes.",
            "Reasons without complete named ordering evidence are deliberately mixed or indeterminate even when denominators exist.",
        ],
    }
    return report


def run_self_test() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        write_json(base / "capture_stage_metrics.json", {"target_60_count": 6, "final_eligibility_capture_count": 1})
        write_json(
            base / "quality_timing_reject_research_audit_24h.json",
            {
                "schema_version": "test",
                "generated_at": "t",
                "classification": "READY",
                "quality_timing_reject_event_rows": 2,
                "reject_counterfactuals": {
                    "dud_inclusive_denominator": {"available": True, "base_gold_silver_rate": 0.2},
                    "per_reason_denominators": [
                        {
                            "stage": "decision_no_pass_or_allow",
                            "component": "entry_execution_eligibility",
                            "event_type": "entry_block",
                            "decision": "watch_only",
                            "reason": "entry_execution_signal_stale",
                            "raw_gs_reject_events": 1,
                            "raw_gs_reject_signal_count": 1,
                            "all_reject_signal_count": 2,
                            "dud_kills": 1,
                            "p_gold_silver_given_reject_by_reason": 0.5,
                        }
                    ],
                    "shadow_quote_fresh_reanchor_variant": {"stale_reject_events": 1, "would_be_quote_fresh_events": 1},
                },
                "top_examples": [
                    {
                        "signal_id": "1",
                        "token_ca": "t",
                        "attribution": {"component": "entry_execution_eligibility", "reason": "entry_execution_signal_stale"},
                        "reject_ts": 100,
                        "price_at_reject": 1.0,
                        "peak_vs_reject_ordering": {"ordering": "reject_before_or_at_peak"},
                    }
                ],
            },
        )
        args = argparse.Namespace(artifact_dir=str(base), out=None)
        report = build_report(args)
        assert report["formal_reason_verdicts"][0]["lean"] == "harmful"
        assert report["promotion_allowed"] is False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", default="/app/data/agent_runs/latest")
    parser.add_argument("--out", default="/app/data/agent_runs/latest/reject_reason_counterfactual_review_24h.json")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        run_self_test()
        print("reject_reason_counterfactual_review self-test passed")
        return 0
    report = build_report(args)
    write_json(args.out, report)
    print(json.dumps({"out": args.out, "classification": report["classification"], "promotion_allowed": False}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

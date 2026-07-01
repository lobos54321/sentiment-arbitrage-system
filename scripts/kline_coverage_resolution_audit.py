#!/usr/bin/env python3
"""Read-only kline coverage blocker resolution audit.

This report reconciles formal kline coverage with the low-confidence research
split emitted by volume_kline_coverage_audit. It never changes strategy, gates,
entry policy, final_entry_contract, A_CLASS mode, executor, wallet, canary, or
risk settings.
"""

from __future__ import annotations

import argparse
import json
import math
import tempfile
import time
from pathlib import Path


SCHEMA_VERSION = "kline_coverage_resolution_audit.v1"
TARGET_RATE = 0.8


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_json(path):
    if not path:
        return {}
    target = Path(path)
    if not target.exists():
        return {}
    with target.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, dict) else {}


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def as_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def as_float(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def rate(num, den):
    return None if not den else round(float(num) / float(den), 6)


def target_count(total, target_rate=TARGET_RATE):
    return int(math.ceil(float(total) * float(target_rate))) if total else 0


def build_report(args):
    volume_kline = load_json(args.volume_kline_audit)
    low_confidence_capture = load_json(args.low_confidence_audit)
    matured_recheck = load_json(args.matured_kline_recheck)
    matured_volume_cross = load_json(args.matured_volume_cross)

    raw_kline = volume_kline.get("raw_gold_silver_kline") or {}
    low_split = raw_kline.get("low_confidence_research_audit") or {}
    uncovered_root_causes = raw_kline.get("kline_uncovered_root_cause_counts") or {}
    baseline_uncovered = raw_kline.get("baseline_confidence_counts_uncovered") or {}
    coverage_uncovered = raw_kline.get("coverage_reason_counts_uncovered") or {}

    total = as_int(raw_kline.get("raw_all_gold_silver_event_rows"))
    formal_covered = as_int(raw_kline.get("kline_covered_rows"))
    formal_uncovered = as_int(raw_kline.get("kline_uncovered_rows"))
    target_80 = target_count(total)
    additional_needed = max(0, target_80 - formal_covered)

    low_uncovered = as_int(low_split.get("low_confidence_uncovered_rows"))
    low_before_peak = as_int(low_split.get("low_confidence_baseline_before_sustained_peak_rows"))
    low_after_or_unknown = as_int(low_split.get("low_confidence_baseline_after_or_unknown_peak_rows"))
    confidence_adjusted_rows = as_int(low_split.get("confidence_adjusted_research_kline_covered_rows"))
    confidence_adjusted_rate = as_float(low_split.get("confidence_adjusted_research_kline_coverage_rate"))
    outlier_rows = as_int(uncovered_root_causes.get("outlier_price"))

    time_legal_recoverable_rows = min(low_before_peak, low_uncovered)
    formal_plus_time_legal = formal_covered + time_legal_recoverable_rows
    formal_plus_time_legal_rate = rate(formal_plus_time_legal, total)
    time_legal_recoverable_reaches_80 = bool(total and formal_plus_time_legal >= target_80)

    confidence_adjusted_reaches_80 = bool(
        confidence_adjusted_rate is not None and confidence_adjusted_rate >= TARGET_RATE
    )
    matured_recoverable_rate = as_float((matured_recheck.get("recheck") or {}).get("recoverable_known_rate"))
    matured_volume_known_rate = as_float((matured_volume_cross.get("matured_volume_context") or {}).get("known_rate"))

    blockers = []
    if formal_covered < target_80:
        blockers.append("formal_kline_coverage_below_80pct")
    if low_uncovered:
        blockers.append("low_confidence_rows_research_only")
    if outlier_rows:
        blockers.append("outlier_price_rows_block_formal_kline")

    if not total:
        classification = "KLINE_RESOLUTION_BLOCKED_NO_RAW_GS_DENOMINATOR"
        next_action = "rerun_raw_gold_silver_denominator_before_kline_resolution"
    elif formal_covered >= target_80:
        classification = "KLINE_FORMAL_COVERAGE_CLEAN"
        next_action = "allow_formal_kline_discovery_only"
    elif confidence_adjusted_reaches_80:
        classification = "KLINE_FORMAL_BLOCKED_RESEARCH_RECOVERABLE"
        next_action = "audit_low_confidence_baseline_lag_time_legality_without_changing_formal_denominator"
    else:
        classification = "KLINE_FORMAL_BLOCKED_DATA_SOURCE"
        next_action = "investigate_raw_kline_source_coverage_and_outlier_price_rows"

    allowed_resolution_tracks = []
    if time_legal_recoverable_rows:
        allowed_resolution_tracks.append({
            "track": "low_confidence_time_legal_research",
            "description": "Rows are low-confidence in the formal denominator but baseline is before sustained peak; audit source lag/time legality before any denominator change.",
            "recoverable_rows": time_legal_recoverable_rows,
            "formal_plus_recoverable_rate": formal_plus_time_legal_rate,
            "reaches_80pct_if_accepted": time_legal_recoverable_reaches_80,
            "allowed_use": "research_only",
            "promotion_allowed": False,
        })
    if outlier_rows:
        allowed_resolution_tracks.append({
            "track": "outlier_price_row_audit",
            "description": "Outlier-price rows remain outside the formal denominator until price/outlier attribution is explained.",
            "recoverable_rows": outlier_rows,
            "allowed_use": "data_quality_audit_only",
            "promotion_allowed": False,
        })
    if matured_recoverable_rate is not None:
        allowed_resolution_tracks.append({
            "track": "matured_volume_shadow_recheck",
            "description": "Delayed kline availability can recover volume profiles for shadow research without changing original formal observations.",
            "recoverable_known_rate": matured_recoverable_rate,
            "matured_volume_known_rate": matured_volume_known_rate,
            "allowed_use": "shadow_only",
            "promotion_allowed": False,
        })

    low_den = (low_confidence_capture.get("denominator") or {}).get("low_confidence_research_gold_silver") or {}
    low_candidate_layer = low_confidence_capture.get("candidate_layer") or {}
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "kline_coverage_resolution_audit",
        "generated_at": utc_now(),
        "evidence_level": "discovery_readiness",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "formal_denominator_changed": False,
        "inputs": {
            "volume_kline_audit": args.volume_kline_audit,
            "low_confidence_audit": args.low_confidence_audit,
            "matured_kline_recheck": args.matured_kline_recheck,
            "matured_volume_cross": args.matured_volume_cross,
        },
        "overall": {
            "classification": classification,
            "next_action": next_action,
            "promotion_allowed": False,
            "formal_kline_slices_blocked": formal_covered < target_80,
            "research_kline_recoverable": confidence_adjusted_reaches_80,
        },
        "formal_kline_coverage": {
            "raw_all_gold_silver_event_rows": total,
            "target_rate": TARGET_RATE,
            "target_80_count": target_80,
            "formal_covered_rows": formal_covered,
            "formal_uncovered_rows": formal_uncovered,
            "formal_coverage_rate": rate(formal_covered, total),
            "additional_formal_rows_needed_to_80pct": additional_needed,
            "blockers": blockers,
            "uncovered_root_cause_counts": uncovered_root_causes,
            "baseline_confidence_counts_uncovered": baseline_uncovered,
            "coverage_reason_counts_uncovered": coverage_uncovered,
        },
        "research_recoverability": {
            "low_confidence_uncovered_rows": low_uncovered,
            "low_confidence_baseline_before_sustained_peak_rows": low_before_peak,
            "low_confidence_baseline_after_or_unknown_peak_rows": low_after_or_unknown,
            "confidence_adjusted_research_kline_covered_rows": confidence_adjusted_rows,
            "confidence_adjusted_research_kline_coverage_rate": confidence_adjusted_rate,
            "confidence_adjusted_reaches_80pct": confidence_adjusted_reaches_80,
            "time_legal_recoverable_rows": time_legal_recoverable_rows,
            "formal_plus_time_legal_recoverable_rows": formal_plus_time_legal,
            "formal_plus_time_legal_recoverable_rate": formal_plus_time_legal_rate,
            "time_legal_recoverable_reaches_80pct": time_legal_recoverable_reaches_80,
        },
        "low_confidence_capture_summary": {
            "available": bool(low_confidence_capture),
            "event_rows": low_den.get("event_rows"),
            "unique_tokens": low_den.get("unique_tokens"),
            "candidate_match_any_rate": low_candidate_layer.get("candidate_match_any_rate"),
            "top_candidates_by_low_confidence_raw_gs_match": (
                low_candidate_layer.get("top_candidates_by_low_confidence_raw_gs_match") or []
            )[:10],
        },
        "allowed_resolution_tracks": allowed_resolution_tracks,
    }


def compact_summary(report):
    return {
        "overall": report.get("overall") or {},
        "formal_kline_coverage": report.get("formal_kline_coverage") or {},
        "research_recoverability": report.get("research_recoverability") or {},
        "low_confidence_capture_summary": report.get("low_confidence_capture_summary") or {},
        "allowed_resolution_tracks": report.get("allowed_resolution_tracks") or [],
        "promotion_allowed": False,
    }


def self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        volume_path = root / "volume.json"
        low_path = root / "low.json"
        matured_path = root / "matured.json"
        cross_path = root / "cross.json"
        write_json(volume_path, {
            "raw_gold_silver_kline": {
                "raw_all_gold_silver_event_rows": 10,
                "kline_covered_rows": 4,
                "kline_uncovered_rows": 6,
                "kline_uncovered_root_cause_counts": {
                    "baseline_confidence_low_low_30_60s": 4,
                    "outlier_price": 2,
                },
                "baseline_confidence_counts_uncovered": {"low": 4, "medium": 2},
                "coverage_reason_counts_uncovered": {"covered": 4, "outlier_price": 2},
                "low_confidence_research_audit": {
                    "low_confidence_uncovered_rows": 4,
                    "low_confidence_baseline_before_sustained_peak_rows": 4,
                    "low_confidence_baseline_after_or_unknown_peak_rows": 0,
                    "confidence_adjusted_research_kline_covered_rows": 8,
                    "confidence_adjusted_research_kline_coverage_rate": 0.8,
                },
            }
        })
        write_json(low_path, {
            "denominator": {"low_confidence_research_gold_silver": {"event_rows": 4, "unique_tokens": 4}},
            "candidate_layer": {
                "candidate_match_any_rate": 1.0,
                "top_candidates_by_low_confidence_raw_gs_match": [{"candidate_id": "current_all"}],
            },
        })
        write_json(matured_path, {"recheck": {"recoverable_known_rate": 0.95}})
        write_json(cross_path, {"matured_volume_context": {"known_rate": 0.94}})
        report = build_report(argparse.Namespace(
            volume_kline_audit=str(volume_path),
            low_confidence_audit=str(low_path),
            matured_kline_recheck=str(matured_path),
            matured_volume_cross=str(cross_path),
            out=None,
        ))
        assert report["promotion_allowed"] is False
        assert report["overall"]["classification"] == "KLINE_FORMAL_BLOCKED_RESEARCH_RECOVERABLE"
        assert report["formal_kline_coverage"]["additional_formal_rows_needed_to_80pct"] == 4
        assert report["research_recoverability"]["time_legal_recoverable_reaches_80pct"] is True
        assert report["low_confidence_capture_summary"]["candidate_match_any_rate"] == 1.0
        assert compact_summary(report)["promotion_allowed"] is False
    print("SELF_TEST_PASS kline_coverage_resolution_audit")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--volume-kline-audit", default="/app/data/agent_runs/latest/volume_kline_coverage_audit_24h.json")
    parser.add_argument("--low-confidence-audit", default="/app/data/agent_runs/latest/low_confidence_research_capture_audit_24h.json")
    parser.add_argument("--matured-kline-recheck", default="/app/data/agent_runs/latest/matured_kline_volume_recheck_audit_24h.json")
    parser.add_argument("--matured-volume-cross", default="/app/data/agent_runs/latest/matured_volume_capture_cross_audit_24h.json")
    parser.add_argument("--out")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return 0
    report = build_report(args)
    if args.out:
        write_json(args.out, report)
    else:
        print(json.dumps(compact_summary(report), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

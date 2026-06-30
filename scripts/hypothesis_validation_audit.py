#!/usr/bin/env python3
"""Read-only hypothesis validation audit for capture discovery.

This audit validates frozen shadow-only hypotheses from hypothesis_registry
against a supplied discovery report. It never changes strategy, gates,
A_CLASS mode, final_entry_contract, executor settings, wallet, or risk.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import tempfile
import time
from pathlib import Path


SCHEMA_VERSION = "hypothesis_validation_audit.v1"


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_json(path):
    if not path:
        return {}
    target = Path(path)
    if not target.exists():
        return {}
    with target.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def parse_time(value):
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        pass
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return int(dt.datetime.fromisoformat(text).timestamp())
    except Exception:
        return None


def as_float(value):
    try:
        return float(value)
    except Exception:
        return None


def compact_slice(row):
    if not row:
        return None
    keys = (
        "candidate_id",
        "family",
        "dimension",
        "slice_value",
        "verdict",
        "slice_signal_count",
        "slice_raw_gs_count",
        "candidate_match_count",
        "matched_gs_count",
        "match_recall_event",
        "match_precision_event",
        "candidate_baseline_recall_event",
        "candidate_baseline_precision_event",
        "recall_lift_vs_candidate_baseline",
        "precision_lift_vs_candidate_baseline",
        "promotion_allowed",
    )
    return {key: row.get(key) for key in keys if key in row}


def slice_key(candidate_id, dimension, slice_value):
    return (str(candidate_id or ""), str(dimension or ""), str(slice_value or ""))


def index_matured_volume_slices(cross):
    rows = []
    rows.extend(cross.get("top_slices") or [])
    rows.extend((cross.get("h1_matured_building_volume") or {}).get("rows") or [])
    indexed = {}
    for row in rows:
        key = slice_key(row.get("candidate_id"), row.get("dimension"), row.get("slice_value"))
        current = indexed.get(key)
        if current is None:
            indexed[key] = row
            continue
        current_score = (
            current.get("verdict") == "MATURED_VOLUME_DISCOVERY_WATCH",
            current.get("matched_gs_count") or 0,
            current.get("candidate_match_count") or 0,
        )
        row_score = (
            row.get("verdict") == "MATURED_VOLUME_DISCOVERY_WATCH",
            row.get("matched_gs_count") or 0,
            row.get("candidate_match_count") or 0,
        )
        if row_score > current_score:
            indexed[key] = row
    return indexed


def is_repeated_watch(row):
    if not row:
        return False
    if row.get("verdict") != "MATURED_VOLUME_DISCOVERY_WATCH":
        return False
    if (row.get("matched_gs_count") or 0) <= 0:
        return False
    recall_lift = as_float(row.get("recall_lift_vs_candidate_baseline"))
    precision_lift = as_float(row.get("precision_lift_vs_candidate_baseline"))
    return (recall_lift is not None and recall_lift > 0) and (precision_lift is not None and precision_lift >= 0)


def validate_matured_volume_hypotheses(registry, cross):
    hypotheses = registry.get("shadow_only_matured_volume_watch") or []
    indexed = index_matured_volume_slices(cross)
    window = cross.get("window") or {}
    registry_updated_ts = parse_time(registry.get("updated_at"))
    eval_since_ts = parse_time(window.get("since_ts"))
    eval_until_ts = parse_time(window.get("until_ts"))
    frozen_before_eval = (
        registry_updated_ts is not None
        and eval_since_ts is not None
        and registry_updated_ts <= eval_since_ts
    )
    rows = []
    for item in hypotheses:
        definition = item.get("definition") or {}
        key = slice_key(
            definition.get("candidate_id"),
            definition.get("dimension") or "matured_volume_profile",
            definition.get("slice_value"),
        )
        current = indexed.get(key)
        repeated = is_repeated_watch(current)
        rows.append(
            {
                "hypothesis_id": item.get("hypothesis_id"),
                "scope": item.get("scope"),
                "evidence_level": item.get("evidence_level"),
                "definition": definition,
                "registry_latest_metrics": item.get("latest_metrics") or {},
                "current_slice": compact_slice(current),
                "current_found": current is not None,
                "repeated_watch": repeated,
                "registry_frozen_before_eval_window": frozen_before_eval,
                "oos_evaluable": bool(frozen_before_eval and current is not None),
                "promotion_allowed": False,
                "status": (
                    "OOS_REPEATED_WATCH_PENDING_REVIEW"
                    if frozen_before_eval and repeated
                    else "OOS_NO_REPEAT"
                    if frozen_before_eval and current is not None
                    else "NOT_FOUND_IN_CURRENT_TOP_SLICES"
                    if current is None
                    else "SAME_WINDOW_REPEAT_NOT_OOS"
                    if repeated
                    else "SAME_WINDOW_NO_REPEAT_NOT_OOS"
                ),
            }
        )
    repeated_count = sum(1 for row in rows if row["repeated_watch"])
    found_count = sum(1 for row in rows if row["current_found"])
    oos_repeated_count = sum(1 for row in rows if row["status"] == "OOS_REPEATED_WATCH_PENDING_REVIEW")
    return {
        "registry_updated_at": registry.get("updated_at"),
        "registry_updated_ts": registry_updated_ts,
        "eval_window": {
            "since_ts": eval_since_ts,
            "until_ts": eval_until_ts,
            "hours": window.get("hours"),
        },
        "registry_frozen_before_eval_window": frozen_before_eval,
        "registered_hypothesis_count": len(hypotheses),
        "found_in_current_report_count": found_count,
        "repeated_watch_count": repeated_count,
        "oos_repeated_watch_count": oos_repeated_count,
        "hypotheses": rows,
    }


def build_report(args):
    registry = load_json(args.registry)
    matured_volume_cross = load_json(args.matured_volume_cross)
    matured_volume_validation = validate_matured_volume_hypotheses(registry, matured_volume_cross)
    if not registry:
        classification = "BLOCKED_REGISTRY_MISSING"
        next_action = "materialize_hypothesis_registry"
    elif not matured_volume_cross:
        classification = "BLOCKED_MATURED_VOLUME_CROSS_MISSING"
        next_action = "run_matured_volume_capture_cross_audit"
    elif matured_volume_validation["registered_hypothesis_count"] <= 0:
        classification = "NO_REGISTERED_SHADOW_HYPOTHESES"
        next_action = "continue_discovery_until_watch_hypotheses_exist"
    elif not matured_volume_validation["registry_frozen_before_eval_window"]:
        classification = "SAME_WINDOW_ONLY_PENDING_NEXT_WINDOW"
        next_action = "wait_for_next_window_or_run_non_overlapping_eval"
    elif matured_volume_validation["oos_repeated_watch_count"] > 0:
        classification = "OOS_WATCH_REPEATED_PENDING_REVIEW"
        next_action = "keep_shadow_only_and_prepare_human_review_after_additional_window"
    else:
        classification = "OOS_NO_REPEAT_CONTINUE_WATCH"
        next_action = "continue_shadow_tracking_without_promotion"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "hypothesis_validation_audit",
        "generated_at": utc_now(),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "paper_enablement_allowed": False,
        "usage": "shadow_only_hypothesis_validation",
        "inputs": {
            "registry": args.registry,
            "matured_volume_cross": args.matured_volume_cross,
        },
        "matured_volume_hypothesis_validation": matured_volume_validation,
        "overall": {
            "classification": classification,
            "next_action": next_action,
            "promotion_allowed": False,
            "human_action_required": False,
        },
    }


def compact_summary(report):
    validation = report.get("matured_volume_hypothesis_validation") or {}
    return {
        "overall": report.get("overall") or {},
        "promotion_allowed": False,
        "matured_volume_hypothesis_validation": {
            key: validation.get(key)
            for key in (
                "registry_frozen_before_eval_window",
                "registered_hypothesis_count",
                "found_in_current_report_count",
                "repeated_watch_count",
                "oos_repeated_watch_count",
            )
        },
    }


def self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        registry_path = root / "registry.json"
        cross_path = root / "cross.json"
        out_path = root / "out.json"
        registry = {
            "schema_version": "hypothesis_registry.v2",
            "updated_at": "2026-06-30T05:00:00Z",
            "promotion_allowed": False,
            "shadow_only_matured_volume_watch": [
                {
                    "hypothesis_id": "matured_volume:entry_mode_registry:ath_flat_structure_tiny_scout:building",
                    "scope": "shadow_only_matured_volume_context",
                    "evidence_level": "discovery_same_window",
                    "definition": {
                        "candidate_id": "entry_mode_registry:ath_flat_structure_tiny_scout",
                        "dimension": "matured_volume_profile",
                        "slice_value": "building",
                    },
                    "latest_metrics": {"verdict": "MATURED_VOLUME_DISCOVERY_WATCH"},
                    "promotion_allowed": False,
                }
            ],
        }
        cross = {
            "window": {"since_ts": 1782799200, "until_ts": 1782885600, "hours": 24},
            "top_slices": [
                {
                    "candidate_id": "entry_mode_registry:ath_flat_structure_tiny_scout",
                    "family": "entry_mode_registry",
                    "dimension": "matured_volume_profile",
                    "slice_value": "building",
                    "verdict": "MATURED_VOLUME_DISCOVERY_WATCH",
                    "slice_signal_count": 100,
                    "slice_raw_gs_count": 10,
                    "candidate_match_count": 50,
                    "matched_gs_count": 5,
                    "match_recall_event": 0.5,
                    "match_precision_event": 0.1,
                    "recall_lift_vs_candidate_baseline": 0.1,
                    "precision_lift_vs_candidate_baseline": 0.01,
                    "promotion_allowed": False,
                }
            ],
        }
        write_json(registry_path, registry)
        write_json(cross_path, cross)
        args = argparse.Namespace(
            registry=str(registry_path),
            matured_volume_cross=str(cross_path),
            out=str(out_path),
            json_summary=False,
        )
        report = build_report(args)
        assert report["overall"]["classification"] == "OOS_WATCH_REPEATED_PENDING_REVIEW"
        assert report["promotion_allowed"] is False
        assert report["matured_volume_hypothesis_validation"]["oos_repeated_watch_count"] == 1
        write_json(out_path, report)
        assert load_json(out_path)["schema_version"] == SCHEMA_VERSION

        registry["updated_at"] = "2026-06-30T06:30:00Z"
        write_json(registry_path, registry)
        report = build_report(args)
        assert report["overall"]["classification"] == "SAME_WINDOW_ONLY_PENDING_NEXT_WINDOW"
    print("SELF_TEST_PASS hypothesis_validation_audit")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default="/app/data/hypothesis_registry.json")
    parser.add_argument("--matured-volume-cross", default="/app/data/agent_runs/latest/matured_volume_capture_cross_audit_24h.json")
    parser.add_argument("--out", default="/app/data/agent_runs/latest/hypothesis_validation_audit_24h.json")
    parser.add_argument("--json-summary", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return 0
    report = build_report(args)
    write_json(args.out, report)
    summary = compact_summary(report)
    print(json.dumps(summary if args.json_summary else {"out": args.out, **summary}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

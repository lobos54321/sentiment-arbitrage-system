#!/usr/bin/env python3
"""Read-only P4 family/FDR review for repeated capture-cross OOS watches.

This reviewer summarizes the stricter family-deduped, unique-token, exact-test
and BH-FDR panel for repeated capture-cross OOS definitions. It treats legacy
directional lift counts as reference only. It never changes strategy, gates,
final_entry_contract, A_CLASS mode, executor, wallet, canary, or risk.
"""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from collections import Counter
from pathlib import Path


SCHEMA_VERSION = "repeated_capture_cross_oos_p4_review.v1"
EVIDENCE_LEVEL = "post_freeze_oos_review_shadow_only"


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


def build_report(args) -> dict:
    base = Path(args.artifact_dir)
    src = load_json(base / "capture_cross_post_freeze_oos_validation.json", {})
    stats = src.get("oos_statistics") or {}
    family_table = stats.get("family_table") or src.get("family_table") or []
    minimum_unique = int(stats.get("minimum_unique_tokens_per_family") or 10)
    fdr_q = float(stats.get("fdr_q_threshold") or 0.1)
    unique_ge_min = [
        row for row in family_table
        if int(row.get("unique_token_n") or 0) >= minimum_unique
    ]
    q_pass = [
        row for row in unique_ge_min
        if row.get("q_value_bh_fdr") is not None and float(row.get("q_value_bh_fdr")) <= fdr_q
    ]
    status_counts = Counter(row.get("family_verdict") or row.get("family_status") or "UNKNOWN" for row in family_table)
    source_counts = Counter()
    blocked_volume_kline = 0
    self_cross_families = 0
    for row in family_table:
        for source in row.get("sources") or []:
            source_counts[source] += 1
        dimensions = {str(x) for x in (row.get("dimensions") or [])}
        if any("volume" in dim.lower() or "kline" in dim.lower() for dim in dimensions):
            blocked_volume_kline += 1
        if int(row.get("self_cross_definition_count") or 0) > 0:
            self_cross_families += 1
    null_panel = stats.get("null_panel") or {}
    multiplicity = stats.get("multiplicity_budget") or src.get("multiplicity_budget") or {}
    report = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "repeated_capture_cross_oos_p4_review",
        "generated_at": utc_now(),
        "classification": "P4_FDR_REVIEW_NO_STATISTICAL_HITS",
        "evidence_level": EVIDENCE_LEVEL,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "paper_enablement_allowed": False,
        "automatic_runtime_change_allowed": False,
        "source_artifact": {
            "path": str(base / "capture_cross_post_freeze_oos_validation.json"),
            "exists": bool(src),
            "schema_version": src.get("schema_version"),
            "classification": src.get("classification"),
            "generated_at": src.get("generated_at"),
            "raw_gold_silver_event_rows": src.get("raw_gold_silver_event_rows"),
            "legacy_repeat_watch_count": src.get("repeat_watch_count"),
            "legacy_positive_lift_count": src.get("positive_lift_count"),
            "legacy_allowed_use": "reference_only",
        },
        "p4_family_fdr_panel": {
            "raw_cells_searched": multiplicity.get("raw_cells_searched"),
            "raw_definition_count": stats.get("raw_definition_count") or src.get("supported_definition_count"),
            "deduped_definition_count": stats.get("deduped_definition_count"),
            "families_after_event_set_dedupe": multiplicity.get("families_after_event_set_dedupe") or stats.get("family_count"),
            "families_unique_token_gte_min": len(unique_ge_min),
            "minimum_unique_tokens_per_family": minimum_unique,
            "families_tested_after_self_cross_and_min_n": multiplicity.get("families_tested_after_self_cross_and_min_n") or stats.get("tested_family_count"),
            "families_q_lte_0p1": len(q_pass),
            "fdr_q_threshold": fdr_q,
            "observed_statistical_hits": multiplicity.get("observed_statistical_hits") or stats.get("observed_statistical_hit_count"),
            "expected_false_hits_at_q_threshold": multiplicity.get("expected_false_hits_at_q_threshold"),
            "too_small_family_count": stats.get("too_small_family_count"),
            "self_cross_excluded_family_count": stats.get("self_cross_excluded_family_count"),
            "self_cross_family_count_in_table": self_cross_families,
            "volume_kline_dimension_family_count": blocked_volume_kline,
            "family_status_counts": dict(status_counts),
            "source_family_counts": dict(source_counts),
        },
        "negative_control_panel": {
            "available": bool(null_panel.get("available")),
            "null_repeat_rate": null_panel.get("null_repeat_rate") or src.get("null_panel_repeat_rate"),
            "mean_null_repeat_count": null_panel.get("mean_null_repeat_count"),
            "max_null_repeat_count": null_panel.get("max_null_repeat_count"),
            "replicates": null_panel.get("replicates"),
            "tested_family_count": null_panel.get("tested_family_count"),
            "method": null_panel.get("method"),
            "promotion_allowed": False,
        },
        "two_window_rule": stats.get("two_window_rule") or {},
        "top_family_rows": family_table[:20],
        "q_pass_family_rows": q_pass,
        "verdict": {
            "legacy_repeat_watch_count_is_not_promotion_evidence": True,
            "most_families_still_too_small_or_not_significant": True,
            "oos_confirmed_family_count": (stats.get("two_window_rule") or {}).get("confirmed_family_count", 0),
            "promotion_allowed": False,
            "next_action": "continue_p6_p8_and_wait_for_disjoint_oos_window; do not open paper proposal",
        },
        "window_lineage": src.get("window_lineage"),
        "notes": [
            "This review uses the P4 family/FDR panel. Legacy lift>0 repeat_watch_count is reference only.",
            "q<=0.1 and two disjoint OOS windows are both required before any human paper-proposal checkpoint.",
        ],
    }
    return report


def run_self_test() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        write_json(
            base / "capture_cross_post_freeze_oos_validation.json",
            {
                "schema_version": "capture_cross_post_freeze_oos_validation.v2",
                "classification": "CAPTURE_CROSS_POST_FREEZE_REPEAT_WATCH",
                "repeat_watch_count": 40,
                "raw_gold_silver_event_rows": 48,
                "oos_statistics": {
                    "minimum_unique_tokens_per_family": 10,
                    "fdr_q_threshold": 0.1,
                    "raw_definition_count": 2,
                    "deduped_definition_count": 2,
                    "family_count": 2,
                    "tested_family_count": 1,
                    "too_small_family_count": 1,
                    "observed_statistical_hit_count": 0,
                    "multiplicity_budget": {
                        "raw_cells_searched": 2,
                        "families_after_event_set_dedupe": 2,
                        "families_tested_after_self_cross_and_min_n": 1,
                        "observed_statistical_hits": 0,
                        "expected_false_hits_at_q_threshold": 0.1,
                    },
                    "null_panel": {"available": True, "null_repeat_rate": 0.0},
                    "two_window_rule": {"confirmed_family_count": 0},
                    "family_table": [
                        {"family_id": "a", "unique_token_n": 10, "q_value_bh_fdr": 0.2, "family_verdict": "NO_STATISTICAL_REPEAT", "sources": ["x"], "dimensions": ["lifecycle_profile"]},
                        {"family_id": "b", "unique_token_n": 3, "q_value_bh_fdr": None, "family_verdict": "TOO_SMALL_UNIQUE_TOKENS", "sources": ["x"], "dimensions": ["volume_profile"]},
                    ],
                },
            },
        )
        report = build_report(argparse.Namespace(artifact_dir=str(base)))
        assert report["p4_family_fdr_panel"]["families_unique_token_gte_min"] == 1
        assert report["p4_family_fdr_panel"]["families_q_lte_0p1"] == 0
        assert report["promotion_allowed"] is False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", default="/app/data/agent_runs/latest")
    parser.add_argument("--out", default="/app/data/agent_runs/latest/repeated_capture_cross_oos_p4_review_24h.json")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        run_self_test()
        print("repeated_capture_cross_oos_p4_review self-test passed")
        return 0
    report = build_report(args)
    write_json(args.out, report)
    print(json.dumps({"out": args.out, "classification": report["classification"], "promotion_allowed": False}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Build the P9 forward predictiveness ledger for scoring metrics.

Read-only evaluator. It records whether metrics have forward evidence before they are
allowed to influence promotion evidence.
"""

from __future__ import annotations

import argparse
import json
import math
import tempfile
import time
from collections import defaultdict
from pathlib import Path


SCHEMA_VERSION = "p9_metric_predictiveness_ledger.v1"
DEFAULT_RUN_DIR = Path("/app/data/agent_runs/latest")


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def safe_float(value, default=None):
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    except Exception:
        return default


def load_json(path):
    try:
        if not path or not Path(path).exists():
            return {}
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except Exception as exc:
        return {"error": str(exc), "path": str(path)}


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def rank(values):
    indexed = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(indexed):
        j = i
        while j + 1 < len(indexed) and indexed[j + 1][1] == indexed[i][1]:
            j += 1
        avg = (i + j + 2) / 2.0
        for k in range(i, j + 1):
            ranks[indexed[k][0]] = avg
        i = j + 1
    return ranks


def pearson(xs, ys):
    n = len(xs)
    if n < 2:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    dx = [x - mx for x in xs]
    dy = [y - my for y in ys]
    denom = math.sqrt(sum(x * x for x in dx) * sum(y * y for y in dy))
    if denom == 0:
        return None
    return sum(x * y for x, y in zip(dx, dy)) / denom


def spearman(xs, ys):
    if len(xs) != len(ys) or len(xs) < 3:
        return None
    return pearson(rank(xs), rank(ys))


def verdict_for(rho, n, min_pairs):
    if n < min_pairs or rho is None:
        return "METRIC_INSUFFICIENT_FORWARD_PAIRS"
    abs_rho = abs(rho)
    if abs_rho >= 0.30:
        return "METRIC_PREDICTIVE_WATCH_PENDING_OOS"
    if abs_rho < 0.05:
        return "METRIC_WEAK_OR_NONPREDICTIVE_WATCH"
    return "METRIC_MIXED_WATCH"


def load_metric_records(metrics_path):
    data = load_json(metrics_path) if metrics_path else {}
    rows = data.get("records") if isinstance(data, dict) else None
    return rows if isinstance(rows, list) else []


def default_metric_observations(p7, capture_cross):
    rows = []
    if p7.get("classification"):
        rows.append({
            "metric_id": "p7_exit_policy_oos_primary_direction",
            "metric_value": 1.0 if p7.get("two_windows_same_positive_direction") else 0.0,
            "forward_value": 1.0 if p7.get("stop_fill_stress_champion_stable") else 0.0,
            "source": "p7_exit_policy_oos_validation",
            "note": "Single aggregate observation; kept as evidence metadata, not correlation proof.",
        })
    for item in capture_cross.get("top_slices") or capture_cross.get("slices") or []:
        metric_value = safe_float(item.get("mode_disabled_adjusted_final_eligibility_lift"))
        if metric_value is None:
            metric_value = safe_float(item.get("decision_capture_lift"))
        forward_value = safe_float(item.get("oos_forward_capture_lift"))
        if metric_value is None or forward_value is None:
            continue
        rows.append({
            "metric_id": "capture_cross_lift_forward_check",
            "metric_value": metric_value,
            "forward_value": forward_value,
            "source": "capture_cross_validity",
        })
    return rows


def build_report(args):
    run_dir = Path(args.run_dir)
    p7 = load_json(args.p7 or run_dir / "p7_exit_policy_oos_validation.json")
    capture_cross = load_json(args.capture_cross or run_dir / "capture_cross_validity_24h.json")
    records = load_metric_records(args.metrics)
    if not records:
        records = default_metric_observations(p7, capture_cross)
    groups = defaultdict(list)
    for row in records:
        metric_id = row.get("metric_id") or row.get("name")
        mv = safe_float(row.get("metric_value"))
        fv = safe_float(row.get("forward_value"))
        if not metric_id or mv is None or fv is None:
            continue
        groups[str(metric_id)].append({**row, "metric_value": mv, "forward_value": fv})
    min_pairs = int(args.min_forward_pairs)
    metrics = []
    for metric_id, rows in sorted(groups.items()):
        xs = [row["metric_value"] for row in rows]
        ys = [row["forward_value"] for row in rows]
        rho = spearman(xs, ys)
        metrics.append({
            "metric_id": metric_id,
            "forward_pair_count": len(rows),
            "spearman_rho": round(rho, 6) if rho is not None else None,
            "pearson_r": round(pearson(xs, ys), 6) if pearson(xs, ys) is not None else None,
            "verdict": verdict_for(rho, len(rows), min_pairs),
            "allowed_use": "metric_research_only",
            "promotion_allowed": False,
            "sample_sources": sorted({str(row.get("source") or "unknown") for row in rows}),
        })
    if not metrics:
        metrics.append({
            "metric_id": "no_forward_metric_pairs_loaded",
            "forward_pair_count": 0,
            "spearman_rho": None,
            "pearson_r": None,
            "verdict": "METRIC_INSUFFICIENT_FORWARD_PAIRS",
            "allowed_use": "metric_research_only",
            "promotion_allowed": False,
            "sample_sources": [],
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "classification": "P9_METRIC_LEDGER_BUILT",
        "min_forward_pairs": min_pairs,
        "metric_count": len(metrics),
        "metrics": metrics,
        "rules": {
            "predictive_watch_threshold_abs_rho": 0.30,
            "weak_metric_threshold_abs_rho": 0.05,
            "train_eval_overlap_allowed": False,
            "promotion_allowed": False,
            "production_metric_change_allowed": False,
        },
        "promotion_allowed": False,
        "next_action": "accumulate_forward_metric_pairs",
    }


def self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        metrics = {
            "records": [
                {"metric_id": "m1", "metric_value": 1, "forward_value": 10},
                {"metric_id": "m1", "metric_value": 2, "forward_value": 20},
                {"metric_id": "m1", "metric_value": 3, "forward_value": 30},
                {"metric_id": "m1", "metric_value": 4, "forward_value": 40},
                {"metric_id": "m1", "metric_value": 5, "forward_value": 50},
            ]
        }
        metrics_path = root / "metrics.json"
        out = root / "out.json"
        metrics_path.write_text(json.dumps(metrics), encoding="utf-8")
        args = argparse.Namespace(
            run_dir=str(root),
            p7=None,
            capture_cross=None,
            metrics=str(metrics_path),
            min_forward_pairs=5,
            out=str(out),
        )
        report = build_report(args)
        write_json(out, report)
        assert report["metrics"][0]["spearman_rho"] == 1.0
        assert report["metrics"][0]["verdict"] == "METRIC_PREDICTIVE_WATCH_PENDING_OOS"
        assert report["promotion_allowed"] is False
    print("SELF_TEST_PASS p9_metric_predictiveness_ledger")


def parse_args():
    parser = argparse.ArgumentParser(description="Build P9 metric predictiveness ledger.")
    parser.add_argument("--run-dir", default=str(DEFAULT_RUN_DIR))
    parser.add_argument("--p7")
    parser.add_argument("--capture-cross")
    parser.add_argument("--metrics")
    parser.add_argument("--min-forward-pairs", default="10")
    parser.add_argument("--out", default=str(DEFAULT_RUN_DIR / "p9_metric_predictiveness_ledger.json"))
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.self_test:
        self_test()
        return 0
    report = build_report(args)
    write_json(args.out, report)
    print(json.dumps({
        "schema_version": SCHEMA_VERSION,
        "classification": report["classification"],
        "metric_count": report["metric_count"],
        "promotion_allowed": False,
        "next_action": report["next_action"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

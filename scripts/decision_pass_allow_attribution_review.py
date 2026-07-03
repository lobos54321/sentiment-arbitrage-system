#!/usr/bin/env python3
"""Read-only decision -> pass_allow attribution review.

This is a reviewer artifact for the current capture funnel bottleneck. It does
not change strategy, gates, final_entry_contract, A_CLASS mode, executor,
wallet, canary, or risk settings.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path

from offline_raw_gold_silver_funnel_audit import (
    SHADOW_DECISION_BRIDGE_TABLE,
    _decision_reason_key,
    _extract_hard_blockers,
    _choose_decision_no_pass_row,
    columns,
    expr,
    jloads,
    load_raw_dogs,
    safe_float,
    signal_id_key,
    table_exists,
)


SCHEMA_VERSION = "decision_pass_allow_attribution_review.v1"
EVIDENCE_LEVEL = "discovery_same_window"
BLOCKER_CLASSES = {"INSTRUMENTATION", "POLICY", "EPISTEMIC", "CAUSAL"}


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


def classify_reason(component: str, event_type: str, decision: str, reason: str) -> str:
    text = " ".join(str(x or "").lower() for x in [component, event_type, decision, reason])
    if "shadow_decision_bridge" in text or "signal_id_mismatch" in text or "matched_no_decision_bridge" in text:
        return "INSTRUMENTATION"
    if any(
        key in text
        for key in [
            "mode_disabled",
            "direct_entry_disabled",
            "shadow_only",
            "gmgn_pre_seen_required",
            "not_ath",
            "notath",
            "top10_pct_above_max",
        ]
    ):
        return "POLICY"
    if any(key in text for key in ["kline_unknown", "unknown_data", "liq_unknown", "quote_unknown", "missing"]):
        return "EPISTEMIC"
    if any(
        key in text
        for key in [
            "kline_block",
            "low_mc_vol",
            "liquidity_too_low",
            "activity_unconfirmed",
            "matrices not yet aligned",
            "matrix",
            "volume_low",
            "buy_pressure",
        ]
    ):
        return "CAUSAL"
    return "EPISTEMIC"


def stage_table_from_metrics(metrics: dict) -> list[dict]:
    raw = int(metrics.get("raw_gold_silver_denominator") or metrics.get("raw_gold_silver_event_denominator") or 0)
    stages = [
        ("detector_capture", metrics.get("detector_capture_count")),
        ("decision_capture", metrics.get("decision_capture_count")),
        ("pass_allow_capture", metrics.get("pass_allow_capture_count")),
        ("pending_capture", metrics.get("pending_capture_count")),
        ("final_eligibility_capture", metrics.get("final_eligibility_capture_count")),
        ("mode_disabled_adjusted_final_eligibility", metrics.get("mode_disabled_adjusted_final_eligibility_count")),
        ("paper_capture", metrics.get("paper_capture_count")),
        ("realized_capture", metrics.get("realized_capture_count")),
    ]
    out = []
    previous = raw
    for name, value in stages:
        count = int(value or 0)
        out.append(
            {
                "stage": name,
                "event_count": count,
                "event_cumulative_rate": rate(count, raw),
                "conditional_rate_vs_previous_stage": rate(count, previous),
                "absolute_loss_from_previous_stage": max(0, int(previous or 0) - count),
                "unique_token_count": None,
                "unique_token_count_available": False,
            }
        )
        previous = count
    return out


def artifact_window_pins(base: Path, names: list[str]) -> dict:
    pins = {}
    for name in names:
        p = base / name
        data = load_json(p, {})
        pins[name] = {
            "exists": p.exists(),
            "generated_at": data.get("generated_at"),
            "since_ts": (data.get("window") or {}).get("since_ts"),
            "until_ts": (data.get("window") or {}).get("until_ts"),
            "schema_version": data.get("schema_version"),
            "classification": data.get("classification") or data.get("verdict"),
        }
    return pins


def rows_by_exact_signal_id(paper_db: sqlite3.Connection, raw_ids: set[str], since_ts: int, until_ts: int) -> dict[str, list]:
    rows = []
    if table_exists(paper_db, "paper_decision_events"):
        window_rows = paper_db.execute(
            """
            SELECT event_ts, signal_id, component, event_type, decision, reason, payload_json
            FROM paper_decision_events
            WHERE event_ts >= ? AND event_ts <= ?
              AND signal_id IS NOT NULL
            """,
            [since_ts - 60, until_ts + 900],
        ).fetchall()
        rows.extend([row for row in window_rows if signal_id_key(row["signal_id"]) in raw_ids])
    if table_exists(paper_db, SHADOW_DECISION_BRIDGE_TABLE):
        cols = columns(paper_db, SHADOW_DECISION_BRIDGE_TABLE)
        window_rows = paper_db.execute(
            f"""
            SELECT event_ts, signal_id,
                   {expr(cols, 'source_component', "'shadow_decision_bridge_mirror'")} AS component,
                   {expr(cols, 'event_type', "'shadow_decision_bridge_evidence'")} AS event_type,
                   {expr(cols, 'decision', "'EVIDENCE'")} AS decision,
                   {expr(cols, 'root_cause', "'shadow_decision_bridge_mirror'")} AS reason,
                   {expr(cols, 'payload_json', "'{}'")} AS payload_json
            FROM {SHADOW_DECISION_BRIDGE_TABLE}
            WHERE event_ts >= ? AND event_ts <= ?
              AND signal_id IS NOT NULL
            """,
            [since_ts - 60, until_ts + 900],
        ).fetchall()
        rows.extend([row for row in window_rows if signal_id_key(row["signal_id"]) in raw_ids])
    by_signal = defaultdict(list)
    for row in rows:
        key = signal_id_key(row["signal_id"])
        if key:
            by_signal[key].append(row)
    for key in list(by_signal):
        by_signal[key].sort(key=lambda row: safe_float(row["event_ts"]) or 0)
    return by_signal


def compute_full_decision_no_pass(args, metrics: dict, raw_funnel: dict) -> tuple[list[dict], dict]:
    paper_path = Path(args.db)
    raw_path = Path(args.raw_db)
    if not paper_path.exists() or not raw_path.exists():
        return [], {
            "available": False,
            "reason": "db_missing",
            "paper_db_exists": paper_path.exists(),
            "raw_db_exists": raw_path.exists(),
        }
    window = raw_funnel.get("window") or {}
    since_ts = int(window.get("since_ts") or (int(time.time()) - int(args.hours * 3600)))
    until_ts = int(window.get("until_ts") or int(time.time()))
    raw_db = sqlite3.connect(str(raw_path))
    raw_db.row_factory = sqlite3.Row
    paper_db = sqlite3.connect(str(paper_path))
    paper_db.row_factory = sqlite3.Row
    try:
        raw_rows = load_raw_dogs(raw_db, since_ts)
        raw_by_signal = {
            signal_id_key(row.get("signal_id")): dict(row)
            for row in raw_rows
            if signal_id_key(row.get("signal_id"))
        }
        raw_ids = set(raw_by_signal)
        by_signal = rows_by_exact_signal_id(paper_db, raw_ids, since_ts, until_ts)
        pass_allow = set()
        for signal_id, rows in by_signal.items():
            for row in rows:
                event_type = str(row["event_type"] or "").lower()
                decision = str(row["decision"] or "").upper()
                if decision in {"PASS", "ALLOW", "WOULD_ENTER", "ENTER"} or event_type in {"would_enter", "enter"}:
                    pass_allow.add(signal_id)
        decision_no_pass = sorted((set(by_signal) - pass_allow) & raw_ids)
        out = []
        for signal_id in decision_no_pass:
            chosen = _choose_decision_no_pass_row(by_signal.get(signal_id, []))
            reason_key = _decision_reason_key(chosen)
            payload = jloads(chosen["payload_json"]) if chosen is not None else {}
            raw = raw_by_signal.get(signal_id) or {}
            klass = classify_reason(*reason_key)
            out.append(
                {
                    "signal_id": signal_id,
                    "token_ca": raw.get("token_ca"),
                    "tier": raw.get("tier") or raw.get("raw_sustained_tier") or raw.get("raw_primary_tier"),
                    "signal_ts": raw.get("signal_ts") or raw.get("signal_ts_norm"),
                    "max_sustained_peak_pct": raw.get("max_sustained_peak_pct"),
                    "attribution": {
                        "component": reason_key[0],
                        "event_type": reason_key[1],
                        "decision": reason_key[2],
                        "reason": reason_key[3],
                        "hard_blockers": _extract_hard_blockers(payload),
                    },
                    "blocker_class": klass,
                    "promotion_allowed": False,
                }
            )
        return out, {
            "available": True,
            "raw_ids": len(raw_ids),
            "with_exact_decision_rows": len(set(by_signal) & raw_ids),
            "decision_no_pass_rows": len(out),
        }
    finally:
        raw_db.close()
        paper_db.close()


def reason_rows_from_artifact(entry_bridge: dict) -> list[dict]:
    rows = (
        ((entry_bridge.get("raw_signal_decision_bridge") or {}).get("decision_no_pass_or_allow_reason_counts"))
        or []
    )
    out = []
    for row in rows:
        klass = classify_reason(row.get("component"), row.get("event_type"), row.get("decision"), row.get("reason"))
        out.append({**row, "blocker_class": klass, "promotion_allowed": False})
    return out


def build_report(args) -> dict:
    base = Path(args.artifact_dir)
    metrics = load_json(base / "capture_stage_metrics.json", {})
    gap = load_json(base / "capture_60_gap_report.json", {})
    raw_funnel = load_json(base / "raw_gold_silver_funnel_audit_24h.json", {})
    pass_gap = load_json(base / "pass_allow_capture_gap_audit.json", {})
    qt = load_json(base / "quality_timing_reject_research_audit_24h.json", {})
    decision_review = load_json(base / "decision_no_pass_quality_timing_review.json", {})

    raw = int(metrics.get("raw_gold_silver_denominator") or 0)
    decision = int(metrics.get("decision_capture_count") or 0)
    pass_allow = int(metrics.get("pass_allow_capture_count") or 0)
    loss = max(0, decision - pass_allow)
    target = int(metrics.get("target_60_count") or gap.get("target_60_count") or 0)
    needed = max(0, target - pass_allow)

    entry_bridge = (raw_funnel.get("summary") or {}).get("entry_bridge_layer") or {}
    reason_rows = reason_rows_from_artifact(entry_bridge)
    class_counts = Counter()
    for row in reason_rows:
        class_counts[row["blocker_class"]] += int(row.get("count") or 0)

    full_rows, full_meta = compute_full_decision_no_pass(args, metrics, raw_funnel) if args.db and args.raw_db else ([], {"available": False, "reason": "db_not_configured"})
    if full_rows:
        class_counts = Counter(row["blocker_class"] for row in full_rows)
    examples = (
        ((entry_bridge.get("raw_signal_decision_bridge") or {}).get("decision_no_pass_or_allow_examples"))
        or []
    )
    per_event_detail_complete = bool(full_rows) and len(full_rows) == loss
    if not per_event_detail_complete and examples:
        full_rows = [
            {
                "signal_id": row.get("signal_id"),
                "attribution": row.get("attribution"),
                "blocker_class": classify_reason(
                    (row.get("attribution") or {}).get("component"),
                    (row.get("attribution") or {}).get("event_type"),
                    (row.get("attribution") or {}).get("decision"),
                    (row.get("attribution") or {}).get("reason"),
                ),
                "detail_status": "artifact_example_only",
                "promotion_allowed": False,
            }
            for row in examples
        ]

    if not set(class_counts).issubset(BLOCKER_CLASSES):
        raise ValueError("unexpected blocker class")

    report = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "decision_pass_allow_attribution_review",
        "generated_at": utc_now(),
        "evidence_level": EVIDENCE_LEVEL,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "paper_enablement_allowed": False,
        "automatic_runtime_change_allowed": False,
        "classification": "DECISION_PASS_ALLOW_ATTRIBUTION_REVIEW_READY",
        "window_pins": artifact_window_pins(
            base,
            [
                "capture_stage_metrics.json",
                "capture_60_gap_report.json",
                "raw_gold_silver_funnel_audit_24h.json",
                "pass_allow_capture_gap_audit.json",
                "decision_no_pass_quality_timing_review.json",
                "quality_timing_reject_research_audit_24h.json",
            ],
        ),
        "stage_table": stage_table_from_metrics(metrics),
        "transition_under_review": {
            "from_stage": "decision_capture",
            "to_stage": "pass_allow_capture",
            "from_count": decision,
            "to_count": pass_allow,
            "lost_event_count": loss,
            "lost_rate_of_previous": rate(loss, decision),
            "target_60_count": target,
            "additional_pass_allow_events_needed_to_60": needed,
        },
        "loss_reasons_by_transition": {
            "decision_capture_to_pass_allow_capture": {
                "reason_counts": reason_rows,
                "blocker_class_counts": dict(class_counts),
                "source": "raw_gold_silver_funnel_audit.entry_bridge_layer.raw_signal_decision_bridge",
                "quality_timing_named_upper_bound": (
                    (pass_gap.get("decision_no_pass_quality_timing") or {}).get("quality_timing_decision_no_pass_or_allow_events")
                    or decision_review.get("decision_no_pass_quality_timing_event_count")
                ),
            }
        },
        "per_event_loss_rows": full_rows,
        "per_event_detail": {
            "complete": per_event_detail_complete,
            "row_count": len(full_rows),
            "expected_lost_event_count": loss,
            "source": "db_recomputed" if per_event_detail_complete else "artifact_examples_truncated",
            "db_recompute": full_meta,
            "instrumentation_gap": None
            if per_event_detail_complete
            else "raw_funnel artifact retained only the first 20 decision_no_pass_or_allow examples; run with paper/raw DB access to emit all 67 rows",
        },
        "sixty_pct_math": {
            "raw_gold_silver_event_denominator": raw,
            "target_capture_rate": 0.6,
            "target_60_count": target,
            "current_pass_allow_count": pass_allow,
            "additional_pass_allow_events_needed_to_60": needed,
            "bridging_all_decision_to_pass_allow_losses_would_reach_60": pass_allow + loss >= target,
            "bridging_quality_timing_named_upper_bound_would_reach_60": pass_allow
            + int(((pass_gap.get("decision_no_pass_quality_timing") or {}).get("quality_timing_decision_no_pass_or_allow_events") or 0))
            >= target,
            "joint_necessity_statement": "The 60% pass_allow target needs 25 additional events. Named quality/timing decision-no-pass rows alone are insufficient; instrumentation/policy/epistemic/causal buckets must be reviewed before any strategy proposal.",
        },
        "notes": [
            "Bridge-fixed windows are not comparable to old detector->decision windows; decision_capture is now 100%.",
            "This is a read-only reviewer artifact. It does not authorize runtime or strategy changes.",
        ],
        "inputs": {
            "artifact_dir": str(base),
            "paper_db": args.db,
            "raw_db": args.raw_db,
        },
    }
    return report


def run_self_test() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        write_json(
            base / "capture_stage_metrics.json",
            {
                "schema_version": "test",
                "generated_at": "t",
                "raw_gold_silver_denominator": 10,
                "target_60_count": 6,
                "detector_capture_count": 10,
                "decision_capture_count": 10,
                "pass_allow_capture_count": 4,
                "pending_capture_count": 2,
                "final_eligibility_capture_count": 1,
                "mode_disabled_adjusted_final_eligibility_count": 1,
                "paper_capture_count": 0,
                "realized_capture_count": 0,
            },
        )
        write_json(base / "capture_60_gap_report.json", {"target_60_count": 6})
        write_json(
            base / "raw_gold_silver_funnel_audit_24h.json",
            {
                "schema_version": "test",
                "generated_at": "t",
                "window": {"since_ts": 1, "until_ts": 2},
                "summary": {
                    "entry_bridge_layer": {
                        "raw_signal_decision_bridge": {
                            "decision_no_pass_or_allow_reason_counts": [
                                {
                                    "component": "shadow_decision_bridge_mirror",
                                    "event_type": "shadow_decision_bridge_evidence",
                                    "decision": "EVIDENCE",
                                    "reason": "shadow_entry_hypotheses_matched_no_decision_bridge",
                                    "count": 3,
                                },
                                {
                                    "component": "matrix_evaluator",
                                    "event_type": "matrix_decision",
                                    "decision": "wait",
                                    "reason": "matrices not yet aligned",
                                    "count": 3,
                                },
                            ],
                            "decision_no_pass_or_allow_examples": [
                                {"signal_id": "1", "attribution": {"component": "matrix_evaluator", "event_type": "matrix_decision", "decision": "wait", "reason": "matrices not yet aligned"}}
                            ],
                        }
                    }
                },
            },
        )
        write_json(base / "pass_allow_capture_gap_audit.json", {"decision_no_pass_quality_timing": {"quality_timing_decision_no_pass_or_allow_events": 3}})
        args = argparse.Namespace(artifact_dir=str(base), db=None, raw_db=None, hours=24)
        report = build_report(args)
        assert report["transition_under_review"]["lost_event_count"] == 6
        assert report["loss_reasons_by_transition"]["decision_capture_to_pass_allow_capture"]["blocker_class_counts"]["INSTRUMENTATION"] == 3
        assert report["loss_reasons_by_transition"]["decision_capture_to_pass_allow_capture"]["blocker_class_counts"]["CAUSAL"] == 3
        assert report["promotion_allowed"] is False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", default="/app/data/agent_runs/latest")
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--hours", type=float, default=24.0)
    parser.add_argument("--out", default="/app/data/agent_runs/latest/decision_pass_allow_attribution_review_24h.json")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        run_self_test()
        print("decision_pass_allow_attribution_review self-test passed")
        return 0
    report = build_report(args)
    write_json(args.out, report)
    print(json.dumps({"out": args.out, "classification": report["classification"], "promotion_allowed": False}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

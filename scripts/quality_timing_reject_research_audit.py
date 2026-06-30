#!/usr/bin/env python3
"""Read-only quality/timing reject research audit.

This report inspects raw gold/silver events that were blocked by quality or
timing logic before final_entry_contract. It is discovery/readiness evidence
only: it never changes strategy, gates, final_entry_contract, A_CLASS mode,
executor, wallet, canary, or risk settings.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path

from a_class_fastlane_mode_readiness_audit import (
    classify_pending_gap_reason,
    classify_upstream_gap_reason,
)
from offline_raw_gold_silver_funnel_audit import (
    attach_records,
    decision_would_enter,
    load_candidate_observations,
    load_paper_decisions,
    load_paper_trades,
    load_raw_dogs,
    make_raw_indexes,
    normalize_ts,
    pct,
    rate,
    safe_float,
    signal_id_key,
    table_exists,
)


SCHEMA_VERSION = "quality_timing_reject_research_audit.v1"
EVIDENCE_LEVEL = "discovery_same_window"
DEFAULT_EXPECTED_CANDIDATES = 84


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def jdump(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def decision_value(row, key, default=None):
    try:
        if isinstance(row, dict):
            return row.get(key, default)
        return row[key] if key in row.keys() else default
    except Exception:
        return default


def reason_row(row):
    if row is None:
        return {
            "component": "UNKNOWN",
            "event_type": "missing_decision_event",
            "decision": "UNKNOWN",
            "reason": "missing_decision_event",
        }
    return {
        "component": decision_value(row, "source_component")
        or decision_value(row, "component")
        or "UNKNOWN",
        "event_type": decision_value(row, "event_type") or "UNKNOWN",
        "decision": decision_value(row, "decision") or decision_value(row, "action") or "UNKNOWN",
        "reason": decision_value(row, "reason")
        or decision_value(row, "block_cause")
        or decision_value(row, "quote_failure_reason")
        or "UNKNOWN",
    }


def row_ts(row):
    return safe_float(decision_value(row, "event_ts_norm") or decision_value(row, "event_ts")) or 0


def choose_decision_no_pass_row(signal_rows):
    sorted_rows = sorted(signal_rows or [], key=row_ts)
    terminal_rows = [
        row
        for row in sorted_rows
        if str(decision_value(row, "event_type") or "").lower()
        in {"entry_block", "probe_reject", "quality_gate", "timing_decision", "entry_abort"}
        or str(decision_value(row, "decision") or "").upper()
        in {"BLOCK", "REJECT", "WATCH_ONLY", "WAIT", "SKIP"}
        or str(decision_value(row, "action") or "").upper()
        in {"BLOCK", "REJECT", "WATCH_ONLY", "WAIT", "SKIP"}
    ]
    return terminal_rows[-1] if terminal_rows else (sorted_rows[-1] if sorted_rows else None)


def choose_pass_without_pending_row(signal_rows):
    sorted_rows = sorted(signal_rows or [], key=row_ts)
    pass_ts_values = [
        row_ts(row)
        for row in sorted_rows
        if decision_would_enter(row)
    ]
    first_pass_ts = min(pass_ts_values, default=None)
    after_pass = [
        row
        for row in sorted_rows
        if first_pass_ts is None or row_ts(row) >= first_pass_ts
    ]
    terminal_rows = [
        row
        for row in after_pass
        if str(decision_value(row, "event_type") or "").lower()
        in {"entry_block", "entry_abort", "pending_reject"}
        or str(decision_value(row, "decision") or "").upper()
        in {"BLOCK", "REJECT", "WATCH_ONLY", "WAIT", "SKIP"}
        or str(decision_value(row, "action") or "").upper()
        in {"BLOCK", "REJECT", "WATCH_ONLY", "WAIT", "SKIP"}
    ]
    return terminal_rows[0] if terminal_rows else (after_pass[-1] if after_pass else None), first_pass_ts


def choose_pending_without_final_row(signal_rows):
    sorted_rows = sorted(signal_rows or [], key=row_ts)
    pending_ts_values = [
        row_ts(row)
        for row in sorted_rows
        if str(decision_value(row, "event_type") or "").lower() == "pending_entry"
    ]
    first_pending_ts = min(pending_ts_values, default=None)
    after_pending = [
        row
        for row in sorted_rows
        if first_pending_ts is None or row_ts(row) >= first_pending_ts
    ]
    terminal_rows = [
        row
        for row in after_pending
        if str(decision_value(row, "event_type") or "").lower() == "entry_block"
        or str(decision_value(row, "decision") or "").upper() in {"BLOCK", "REJECT", "WATCH_ONLY"}
        or str(decision_value(row, "action") or "").upper() in {"BLOCK", "REJECT", "WATCH_ONLY"}
    ]
    return terminal_rows[0] if terminal_rows else (after_pending[-1] if after_pending else None), first_pending_ts


def is_final_entry_contract(row):
    return str(decision_value(row, "source_component") or decision_value(row, "component") or "") == "final_entry_contract"


def stage_quality_timing_events(raw_rows, decisions):
    raw_ids = {row.get("signal_id_key") for row in raw_rows if row.get("signal_id_key")}
    decisions_by_signal = defaultdict(list)
    for row in decisions:
        key = signal_id_key(decision_value(row, "signal_id"))
        if key in raw_ids:
            decisions_by_signal[key].append(row)

    events = {}
    for raw in raw_rows:
        signal_id = raw.get("signal_id_key")
        if not signal_id:
            continue
        rows = sorted(decisions_by_signal.get(signal_id, []), key=row_ts)
        if not rows:
            continue
        pass_allow = [row for row in rows if decision_would_enter(row)]
        pending = [
            row
            for row in rows
            if str(decision_value(row, "event_type") or "").lower() == "pending_entry"
        ]
        final_contract = [row for row in rows if is_final_entry_contract(row)]

        stage = None
        chosen = None
        first_stage_ts = None
        if not pass_allow:
            stage = "decision_no_pass_or_allow"
            chosen = choose_decision_no_pass_row(rows)
            category = classify_upstream_gap_reason(reason_row(chosen), stage)
        elif not pending:
            stage = "pass_or_allow_without_pending_entry"
            chosen, first_stage_ts = choose_pass_without_pending_row(rows)
            category = classify_upstream_gap_reason(reason_row(chosen), stage)
        elif not final_contract:
            stage = "pending_without_final_entry_contract"
            chosen, first_stage_ts = choose_pending_without_final_row(rows)
            category = classify_pending_gap_reason(reason_row(chosen))
        else:
            continue

        if category != "QUALITY_OR_TIMING_REJECT":
            continue
        item = {
            "signal_id": signal_id,
            "stage": stage,
            "category": category,
            "first_stage_ts": first_stage_ts,
            "attribution": reason_row(chosen),
        }
        item["reason_key"] = (
            item["stage"],
            item["attribution"]["component"],
            item["attribution"]["event_type"],
            item["attribution"]["decision"],
            item["attribution"]["reason"],
        )
        events[signal_id] = item
    return events


def compact_counter(counter, names, limit=30):
    rows = []
    for key, count in counter.most_common(limit):
        if not isinstance(key, tuple):
            key = (key,)
        item = {names[idx]: key[idx] if idx < len(key) else None for idx in range(len(names))}
        item["count"] = count
        rows.append(item)
    return rows


def top_examples(rows, limit):
    rows = sorted(
        rows,
        key=lambda row: safe_float(row.get("max_sustained_peak_pct")) or 0,
        reverse=True,
    )
    return rows[:limit]


def build_report(args):
    now_ts = int(args.now_ts or time.time())
    since_ts = now_ts - int(float(args.hours) * 3600)
    raw_db = sqlite3.connect(args.raw_db)
    raw_db.row_factory = sqlite3.Row
    paper_db = sqlite3.connect(args.db)
    paper_db.row_factory = sqlite3.Row
    try:
        if not table_exists(raw_db, "raw_signal_outcomes"):
            raise SystemExit("raw_signal_outcomes table missing")
        raw_rows = load_raw_dogs(raw_db, since_ts)
        raw_signal_ids, raw_tokens, _, _ = make_raw_indexes(raw_rows)
        until_ts = max([row.get("signal_ts_norm") or since_ts for row in raw_rows] + [now_ts])
        observations, obs_meta = load_candidate_observations(paper_db, raw_signal_ids, since_ts)
        decisions = load_paper_decisions(paper_db, raw_tokens, since_ts, until_ts)
        trades = load_paper_trades(paper_db, raw_tokens, since_ts, until_ts)
        audits = attach_records(raw_rows, observations, decisions, trades, args.expected_candidates)
        audits_by_signal = {row.get("signal_id"): row for row in audits if row.get("signal_id")}
        observations_by_signal = defaultdict(list)
        for obs in observations:
            if obs.get("signal_id_key"):
                observations_by_signal[obs["signal_id_key"]].append(obs)

        qt_events = stage_quality_timing_events(raw_rows, decisions)
        qt_rows = []
        stage_counts = Counter()
        reason_counts = Counter()
        candidate_counts = Counter()
        family_counts = Counter()
        context_counts = Counter()
        lifecycle_counts = Counter()
        source_counts = Counter()
        markov_counts = Counter()
        schema_counts = Counter()
        quote_clean_counts = Counter()
        quote_exec_counts = Counter()
        coverage_ok = 0
        matched_any = 0
        total_obs_rows = 0

        for signal_id, event in qt_events.items():
            audit = audits_by_signal.get(signal_id) or {}
            signal_obs = observations_by_signal.get(signal_id, [])
            matched_obs = [obs for obs in signal_obs if obs.get("matched")]
            candidate_coverage_ok = len({obs.get("candidate_id") for obs in signal_obs}) == args.expected_candidates
            stage_counts[event["stage"]] += 1
            reason_counts[event["reason_key"]] += 1
            if candidate_coverage_ok:
                coverage_ok += 1
            if matched_obs:
                matched_any += 1
            total_obs_rows += len(signal_obs)
            lifecycle = audit.get("lifecycle_profile") or "UNKNOWN"
            source = audit.get("source_component") or "UNKNOWN"
            lifecycle_counts[lifecycle] += 1
            source_counts[source] += 1
            context_counts[(lifecycle, source)] += 1
            markov_counts[str(audit.get("markov_bucket") or "UNKNOWN")] += 1
            schema_counts[str(audit.get("context_schema_version") or "legacy_or_missing")] += 1
            quote_clean_counts[str(audit.get("source_quote_clean"))] += 1
            quote_exec_counts[str(audit.get("source_quote_executable"))] += 1
            for obs in matched_obs:
                candidate_counts[(obs.get("candidate_id") or "UNKNOWN", obs.get("family") or "UNKNOWN")] += 1
                family_counts[obs.get("family") or "UNKNOWN"] += 1
            qt_rows.append(
                {
                    "signal_id": signal_id,
                    "token_ca": audit.get("token_ca"),
                    "symbol": audit.get("symbol"),
                    "tier": audit.get("tier"),
                    "stage": event["stage"],
                    "attribution": event["attribution"],
                    "candidate_observation_count": len(signal_obs),
                    "candidate_coverage_ok": candidate_coverage_ok,
                    "matched_candidate_count": len(matched_obs),
                    "top_matched_candidates": [
                        {
                            "candidate_id": obs.get("candidate_id"),
                            "family": obs.get("family"),
                            "reason": obs.get("reason"),
                        }
                        for obs in matched_obs[:20]
                    ],
                    "lifecycle_profile": lifecycle,
                    "source_component": source,
                    "markov_bucket": audit.get("markov_bucket"),
                    "source_quote_clean": audit.get("source_quote_clean"),
                    "source_quote_executable": audit.get("source_quote_executable"),
                    "context_schema_version": audit.get("context_schema_version"),
                    "max_sustained_peak_pct": audit.get("max_sustained_peak_pct"),
                    "time_to_sustained_peak_sec": audit.get("time_to_sustained_peak_sec"),
                }
            )

        qt_count = len(qt_events)
        blockers = []
        if not raw_rows:
            blockers.append("raw_gold_silver_denominator_empty")
        if qt_count and rate(coverage_ok, qt_count) is not None and rate(coverage_ok, qt_count) < 0.99:
            blockers.append("quality_timing_candidate_coverage_incomplete")
        verdict = "QUALITY_TIMING_REJECT_RESEARCH_READY"
        if not qt_count:
            verdict = "QUALITY_TIMING_REJECT_RESEARCH_EMPTY"
        elif blockers:
            verdict = "QUALITY_TIMING_REJECT_RESEARCH_BLOCKED_DATA"

        return {
            "schema_version": SCHEMA_VERSION,
            "report_type": "quality_timing_reject_research_audit",
            "generated_at": utc_now(),
            "window": {"hours": args.hours, "since_ts": since_ts, "until_ts": now_ts},
            "inputs": {"paper_db": args.db, "raw_db": args.raw_db, "raw_funnel": args.raw_funnel},
            "evidence_level": EVIDENCE_LEVEL,
            "usage": "read_only_shadow_research_quality_timing_rejects",
            "promotion_allowed": False,
            "can_promote_live": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "final_entry_contract_change_allowed": False,
            "verdict": verdict,
            "blockers": blockers,
            "denominator": {
                "raw_all_gold_silver_event_rows": len(raw_rows),
                "raw_all_gold_silver_unique_tokens": len({row.get("token_ca") for row in raw_rows if row.get("token_ca")}),
                "quality_timing_reject_event_rows": qt_count,
                "quality_timing_reject_unique_tokens": len({
                    (audits_by_signal.get(signal_id) or {}).get("token_ca")
                    for signal_id in qt_events
                    if (audits_by_signal.get(signal_id) or {}).get("token_ca")
                }),
                "quality_timing_reject_share_of_raw_all": rate(qt_count, len(raw_rows)),
                "quality_timing_reject_share_of_raw_all_pct": pct(qt_count, len(raw_rows)),
            },
            "observation_load": obs_meta,
            "candidate_match_attribution": {
                "expected_candidates": args.expected_candidates,
                "candidate_observation_rows": total_obs_rows,
                "events_with_full_candidate_coverage": coverage_ok,
                "full_candidate_coverage_rate": rate(coverage_ok, qt_count),
                "full_candidate_coverage_pct": pct(coverage_ok, qt_count),
                "candidate_matched_any_events": matched_any,
                "candidate_matched_any_rate": rate(matched_any, qt_count),
                "candidate_matched_any_pct": pct(matched_any, qt_count),
                "top_candidates": compact_counter(candidate_counts, ["candidate_id", "family"], args.limit),
                "top_families": compact_counter(family_counts, ["family"], args.limit),
            },
            "stage_attribution": {
                "stage_counts": compact_counter(stage_counts, ["stage"], args.limit),
                "reason_counts": compact_counter(
                    reason_counts,
                    ["stage", "component", "event_type", "decision", "reason"],
                    args.limit,
                ),
            },
            "context_attribution": {
                "lifecycle_profile_counts": compact_counter(lifecycle_counts, ["lifecycle_profile"], args.limit),
                "source_component_counts": compact_counter(source_counts, ["source_component"], args.limit),
                "lifecycle_source_counts": compact_counter(
                    context_counts,
                    ["lifecycle_profile", "source_component"],
                    args.limit,
                ),
                "markov_bucket_counts": compact_counter(markov_counts, ["markov_bucket"], args.limit),
                "context_schema_version_counts": compact_counter(schema_counts, ["context_schema_version"], args.limit),
                "source_quote_clean_counts": compact_counter(quote_clean_counts, ["source_quote_clean"], args.limit),
                "source_quote_executable_counts": compact_counter(
                    quote_exec_counts,
                    ["source_quote_executable"],
                    args.limit,
                ),
            },
            "shadow_only_next_actions": [
                "review_shadow_candidates_for_quality_timing_rejects",
                "compare quality/timing reject candidate families against pending/final eligibility lifts",
                "do not relax timing or quality thresholds without human approval",
            ],
            "top_examples": top_examples(qt_rows, args.limit),
            "notes": [
                "Research-only upper-bound audit. This report explains quality/timing rejects; it does not authorize strategy, gate, final_entry_contract, A_CLASS, executor, or risk changes.",
                "promotion_allowed remains false.",
            ],
        }
    finally:
        raw_db.close()
        paper_db.close()


def compact_summary(report):
    return {
        "verdict": report.get("verdict"),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "denominator": report.get("denominator") or {},
        "candidate_match_attribution": report.get("candidate_match_attribution") or {},
        "top_stage_counts": ((report.get("stage_attribution") or {}).get("stage_counts") or [])[:8],
        "top_reason_counts": ((report.get("stage_attribution") or {}).get("reason_counts") or [])[:8],
        "top_candidates": ((report.get("candidate_match_attribution") or {}).get("top_candidates") or [])[:10],
        "top_contexts": ((report.get("context_attribution") or {}).get("lifecycle_source_counts") or [])[:10],
        "blockers": report.get("blockers") or [],
    }


def self_test():
    now = 2_000_000
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        paper_path = root / "paper.db"
        raw_path = root / "raw.db"
        paper = sqlite3.connect(paper_path)
        paper.execute(
            """
            CREATE TABLE candidate_shadow_observations(
              signal_id TEXT, token_ca TEXT, signal_ts INTEGER, candidate_id TEXT, family TEXT,
              matched INTEGER, reason TEXT, observed_at INTEGER, payload_json TEXT
            )
            """
        )
        payload = json.dumps({
            "lifecycle_profile": "ATH_SHALLOW_PULLBACK:OBSERVE",
            "source_component": "matrix_evaluator",
            "markov_bucket": "insufficient",
            "source_quote_clean": True,
            "source_quote_executable": False,
            "context_schema_version": "candidate-shadow-context-v2.no_signal_price_quote_inference",
        })
        for sig, token, cand, family, matched in [
            ("101", "QT1", "current_all", "baseline", 1),
            ("101", "QT1", "kline:active_mom20_first3", "kline", 1),
            ("102", "QT2", "current_all", "baseline", 1),
            ("102", "QT2", "entry_mode_registry:pullback_tiny_scout", "entry_mode_registry", 1),
            ("103", "OK", "current_all", "baseline", 1),
            ("103", "OK", "kline:active_mom20_first3", "kline", 1),
        ]:
            paper.execute(
                "INSERT INTO candidate_shadow_observations VALUES (?,?,?,?,?,?,?,?,?)",
                (sig, token, now - 100, cand, family, matched, "self_test", now - 50, payload if cand == "current_all" else "{}"),
            )
        paper.execute(
            """
            CREATE TABLE paper_decision_events(
              id INTEGER, event_ts INTEGER, signal_id TEXT, token_ca TEXT, symbol TEXT,
              lifecycle_id TEXT, component TEXT, reason TEXT, event_type TEXT, decision TEXT,
              route TEXT, data_source TEXT, lifecycle_state TEXT, payload_json TEXT
            )
            """
        )
        paper.executemany(
            "INSERT INTO paper_decision_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (1, now - 80, "101", "QT1", "QT1", "lc1", "smart_entry", "quality_score_low", "quality_gate", "REJECT", None, None, None, "{}"),
                (2, now - 70, "102", "QT2", "QT2", "lc2", "smart_entry", "pass", "would_enter", "PASS", None, None, None, "{}"),
                (3, now - 65, "102", "QT2", "QT2", "lc2", "scout_quality", "timing_too_late", "timing_decision", "REJECT", None, None, None, "{}"),
                (4, now - 60, "103", "OK", "OK", "lc3", "smart_entry", "pass", "would_enter", "PASS", None, None, None, "{}"),
                (5, now - 55, "103", "OK", "OK", "lc3", "entry_engine", "pending", "pending_entry", "PENDING", None, None, None, "{}"),
                (6, now - 50, "103", "OK", "OK", "lc3", "final_entry_contract", "mode_disabled", "entry_block", "BLOCK", None, None, None, "{}"),
            ],
        )
        paper.commit()
        paper.close()

        raw = sqlite3.connect(raw_path)
        raw.execute(
            """
            CREATE TABLE raw_signal_outcomes(
              signal_id TEXT, token_ca TEXT, symbol TEXT, signal_ts INTEGER,
              signal_type TEXT, source TEXT, observation_status TEXT,
              kline_covered INTEGER, coverage_reason TEXT, baseline_confidence TEXT,
              same_source_path INTEGER, outlier_flag INTEGER, outlier_reason TEXT,
              sustained_evaluable INTEGER, sustained_reason TEXT, raw_sustained_tier TEXT,
              raw_primary_tier TEXT, max_sustained_peak_pct REAL, max_wick_peak_pct REAL,
              time_to_sustained_peak_sec REAL, did_enter INTEGER, entered_before_peak INTEGER,
              held_to_silver INTEGER, held_to_gold INTEGER, raw_dog_entered INTEGER,
              raw_dog_realized INTEGER, sold_before_silver INTEGER, sold_before_gold INTEGER,
              exit_reason TEXT, payload_json TEXT, source_kind TEXT, source_family TEXT
            )
            """
        )
        raw.executemany(
            "INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                ("101", "QT1", "QT1", now - 100, "ATH", "src", "matured", 1, "covered", "high", 1, 0, None, 1, None, "silver", "silver", 90, 95, 100, 0, 0, 0, 0, 0, 0, 0, 0, None, "{}", "dex", "native"),
                ("102", "QT2", "QT2", now - 90, "ATH", "src", "matured", 1, "covered", "high", 1, 0, None, 1, None, "gold", "gold", 300, 320, 120, 0, 0, 0, 0, 0, 0, 0, 0, None, "{}", "dex", "native"),
                ("103", "OK", "OK", now - 80, "ATH", "src", "matured", 1, "covered", "high", 1, 0, None, 1, None, "silver", "silver", 110, 120, 100, 0, 0, 0, 0, 0, 0, 0, 0, None, "{}", "dex", "native"),
            ],
        )
        raw.commit()
        raw.close()

        args = argparse.Namespace(
            db=str(paper_path),
            raw_db=str(raw_path),
            raw_funnel=None,
            hours=1,
            expected_candidates=2,
            now_ts=now,
            limit=10,
            out=None,
            compact=False,
        )
        report = build_report(args)
        assert report["promotion_allowed"] is False
        assert report["strategy_change_allowed"] is False
        assert report["verdict"] == "QUALITY_TIMING_REJECT_RESEARCH_READY"
        assert report["denominator"]["quality_timing_reject_event_rows"] == 2
        assert report["candidate_match_attribution"]["candidate_matched_any_events"] == 2
        assert report["candidate_match_attribution"]["full_candidate_coverage_rate"] == 1.0
        stages = {row["stage"]: row["count"] for row in report["stage_attribution"]["stage_counts"]}
        assert stages["decision_no_pass_or_allow"] == 1
        assert stages["pass_or_allow_without_pending_entry"] == 1
        assert "pending_without_final_entry_contract" not in stages
        compact = compact_summary(report)
        assert compact["promotion_allowed"] is False
    print("SELF_TEST_PASS quality_timing_reject_research_audit")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--raw-funnel", default=None)
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--expected-candidates", type=int, default=DEFAULT_EXPECTED_CANDIDATES)
    parser.add_argument("--now-ts", type=int, default=None)
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--out")
    parser.add_argument("--compact", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    report = build_report(args)
    payload = compact_summary(report) if args.compact else report
    if args.out:
        jdump(args.out, payload)
    else:
        print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

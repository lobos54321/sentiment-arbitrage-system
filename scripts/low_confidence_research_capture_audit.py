#!/usr/bin/env python3
"""Read-only low-confidence raw dog capture research audit.

This report is intentionally discovery-only. It inspects raw gold/silver rows
that failed the formal denominator only because the baseline confidence was
low, then asks whether those rows still had candidate/decision/funnel signal.

It never changes the formal denominator, backfills kline, promotes candidates,
or modifies strategy, gates, entry policy, executor, mode, wallet, canary, or
risk settings.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import tempfile
import time
from collections import Counter
from pathlib import Path

from offline_raw_gold_silver_funnel_audit import (
    attach_records,
    columns,
    load_candidate_observations,
    load_paper_decisions,
    load_paper_trades,
    normalize_ts,
    rate,
    signal_id_key,
    table_exists,
    truthy,
)


SCHEMA_VERSION = "low_confidence_research_capture_audit.v1"
EVIDENCE_LEVEL = "discovery_same_window"
DEFAULT_EXPECTED_CANDIDATES = 84


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def safe_float(value):
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except Exception:
        return None


def safe_int(value):
    parsed = safe_float(value)
    return int(parsed) if parsed is not None else None


def pct(num, den):
    value = rate(num, den)
    return None if value is None else round(value * 100.0, 4)


def select_expr(cols, names):
    return [name if name in cols else f"NULL AS {name}" for name in names]


def gold_silver_sql(cols):
    exprs = []
    if "raw_primary_tier" in cols:
        exprs.append("raw_primary_tier IN ('gold', 'silver')")
    if "raw_sustained_tier" in cols:
        exprs.append("raw_sustained_tier IN ('gold', 'silver')")
    return " OR ".join(exprs) if exprs else None


def norm_text(value):
    if value is None:
        return "MISSING"
    text = str(value).strip()
    return text if text else "UNKNOWN"


def baseline_lag_bucket(value):
    parsed = safe_float(value)
    if parsed is None:
        return "missing"
    if parsed < 0:
        return "invalid_negative"
    if parsed <= 10:
        return "high_le_10s"
    if parsed <= 30:
        return "medium_10_30s"
    if parsed <= 60:
        return "low_30_60s"
    if parsed <= 120:
        return "low_60_120s"
    if parsed <= 300:
        return "low_120_300s"
    return "not_evaluable_gt_300s"


def first_bar_lag_bucket(value):
    parsed = safe_float(value)
    if parsed is None:
        return "missing"
    if parsed < 0:
        return "invalid_negative"
    if parsed <= 60:
        return "le_60s"
    if parsed <= 120:
        return "le_120s"
    if parsed <= 300:
        return "le_300s"
    return "gt_300s"


def raw_tier(row):
    return str(row.get("raw_sustained_tier") or row.get("raw_primary_tier") or "").lower()


def formal_evaluable_failures(row):
    failures = []
    if row.get("observation_status") != "matured":
        failures.append("not_matured")
    if not truthy(row.get("kline_covered")):
        failures.append("kline_uncovered")
    if row.get("baseline_confidence") not in {"high", "medium"}:
        failures.append("low_confidence")
    if not truthy(row.get("same_source_path")):
        failures.append("not_same_source_path")
    if truthy(row.get("outlier_flag")):
        failures.append("outlier")
    if not truthy(row.get("sustained_evaluable")):
        failures.append("not_sustained_evaluable")
    return failures


def is_low_confidence_research_row(row):
    lag = safe_float(row.get("baseline_lag_sec"))
    return (
        row.get("observation_status") == "matured"
        and not truthy(row.get("kline_covered"))
        and str(row.get("baseline_confidence") or "").lower() == "low"
        and truthy(row.get("same_source_path"))
        and not truthy(row.get("outlier_flag"))
        and truthy(row.get("sustained_evaluable"))
        and lag is not None
        and 30.0 < lag <= 300.0
    )


def normalize_raw_row(row, idx):
    item = dict(row)
    item["_event_idx"] = idx
    item["signal_id_key"] = signal_id_key(item.get("signal_id"))
    item["signal_ts_norm"] = normalize_ts(item.get("signal_ts"))
    item["tier"] = raw_tier(item)
    item["evaluable_failures"] = formal_evaluable_failures(item)
    item["evaluable"] = len(item["evaluable_failures"]) == 0
    item["low_confidence_research_eligible"] = is_low_confidence_research_row(item)
    item["baseline_lag_bucket"] = baseline_lag_bucket(item.get("baseline_lag_sec"))
    item["raw_dog_entered_bool"] = truthy(item.get("raw_dog_entered") or item.get("did_enter"))
    item["raw_dog_realized_bool"] = truthy(
        item.get("raw_dog_realized") or item.get("held_to_silver") or item.get("held_to_gold")
    )
    return item


def load_raw_gold_silver_rows(raw_db, since_ts):
    if not table_exists(raw_db, "raw_signal_outcomes"):
        raise SystemExit("raw_signal_outcomes table missing")
    cols = columns(raw_db, "raw_signal_outcomes")
    tier_expr = gold_silver_sql(cols)
    if not tier_expr:
        raise SystemExit("raw_signal_outcomes has no raw gold/silver tier columns")
    names = (
        "id",
        "signal_id",
        "token_ca",
        "symbol",
        "signal_ts",
        "signal_type",
        "source",
        "source_kind",
        "source_family",
        "observation_status",
        "kline_covered",
        "coverage_reason",
        "baseline_lag_sec",
        "baseline_confidence",
        "same_source_path",
        "outlier_flag",
        "outlier_reason",
        "sustained_evaluable",
        "sustained_reason",
        "raw_primary_tier",
        "raw_sustained_tier",
        "max_sustained_peak_pct",
        "max_wick_peak_pct",
        "time_to_sustained_peak_sec",
        "did_enter",
        "entered_before_peak",
        "held_to_silver",
        "held_to_gold",
        "raw_dog_entered",
        "raw_dog_realized",
        "sold_before_silver",
        "sold_before_gold",
        "exit_reason",
        "first_bar_lag_sec",
        "early_15m_bar_coverage_pct",
        "early_15m_complete",
    )
    rows = raw_db.execute(
        f"""
        SELECT {", ".join(select_expr(cols, names))}
        FROM raw_signal_outcomes
        WHERE COALESCE(signal_ts, 0) >= ?
          AND ({tier_expr})
        ORDER BY signal_ts ASC, signal_id ASC
        """,
        (int(since_ts),),
    ).fetchall()
    return [normalize_raw_row(dict(row), idx) for idx, row in enumerate(rows)]


def signal_ids(raw_rows):
    return sorted({row.get("signal_id_key") for row in raw_rows if row.get("signal_id_key")})


def tokens(raw_rows):
    return sorted({str(row.get("token_ca")) for row in raw_rows if row.get("token_ca")})


def summarize_candidate_layer(audits, observations, expected_candidates):
    matched_any = sum(1 for row in audits if row.get("matched_candidate_count", 0) > 0)
    coverage_ok = sum(1 for row in audits if row.get("candidate_coverage_ok"))
    candidate_counts = Counter()
    family_counts = Counter()
    for audit in audits:
        for cand in audit.get("top_matched_candidates") or []:
            key = (cand.get("candidate_id") or "UNKNOWN", cand.get("family") or "UNKNOWN")
            candidate_counts[key] += 1
            family_counts[cand.get("family") or "UNKNOWN"] += 1
    den = len(audits)
    return {
        "expected_candidates": expected_candidates,
        "observation_rows_loaded": len(observations),
        "events_with_full_candidate_coverage": coverage_ok,
        "full_candidate_coverage_rate": rate(coverage_ok, den),
        "full_candidate_coverage_pct": pct(coverage_ok, den),
        "candidate_matched_any_events": matched_any,
        "candidate_matched_none_events": max(0, den - matched_any),
        "candidate_match_any_rate": rate(matched_any, den),
        "top_candidates_by_low_confidence_raw_gs_match": [
            {
                "candidate_id": key[0],
                "family": key[1],
                "matched_low_confidence_raw_gs_events": count,
                "low_confidence_raw_gs_recall": rate(count, den),
            }
            for key, count in candidate_counts.most_common(30)
        ],
        "matched_candidate_family_counts": dict(family_counts.most_common()),
    }


def summarize_decision_layer(audits, decisions, trades):
    den = len(audits)
    terminal = Counter(row.get("terminal_bucket") or "UNKNOWN" for row in audits)
    has_decision = sum(1 for row in audits if row.get("decision_record_count", 0) > 0)
    would_enter = sum(1 for row in audits if row.get("would_enter_count", 0) > 0)
    entered = sum(1 for row in audits if row.get("entered"))
    realized = sum(1 for row in audits if row.get("raw_dog_realized"))
    reason_counts = Counter()
    for audit in audits:
        for dec in audit.get("decision_reason_sample") or []:
            key = (
                dec.get("source_kind") or "UNKNOWN",
                dec.get("source_component") or "UNKNOWN",
                dec.get("action") or dec.get("decision") or dec.get("event_type") or "UNKNOWN",
                dec.get("reason") or dec.get("block_cause") or dec.get("quote_failure_reason") or "UNKNOWN",
            )
            reason_counts[key] += 1
    return {
        "decision_records_loaded": len(decisions),
        "paper_trades_loaded": len(trades),
        "events_with_decision_record": has_decision,
        "decision_record_rate": rate(has_decision, den),
        "would_enter_events": would_enter,
        "would_enter_rate": rate(would_enter, den),
        "entered_events": entered,
        "entered_rate": rate(entered, den),
        "realized_events": realized,
        "realized_rate": rate(realized, den),
        "terminal_bucket_counts": dict(terminal.most_common()),
        "decision_reason_top": [
            {
                "source_kind": key[0],
                "source_component": key[1],
                "action_or_decision": key[2],
                "reason": key[3],
                "events": count,
            }
            for key, count in reason_counts.most_common(20)
        ],
    }


def build_denominator_summary(raw_rows, low_rows):
    formal = [row for row in raw_rows if row.get("evaluable")]
    low_31_60 = [
        row
        for row in low_rows
        if baseline_lag_bucket(row.get("baseline_lag_sec")) == "low_30_60s"
    ]
    low_before_peak = 0
    low_after_or_unknown_peak = 0
    for row in low_rows:
        baseline_lag = safe_float(row.get("baseline_lag_sec"))
        peak_lag = safe_float(row.get("time_to_sustained_peak_sec"))
        if baseline_lag is not None and peak_lag is not None and baseline_lag <= peak_lag:
            low_before_peak += 1
        else:
            low_after_or_unknown_peak += 1
    return {
        "formal_denominator_changed": False,
        "formal_denominator_policy": "unchanged_high_or_medium_baseline_confidence_only",
        "research_denominator_policy": "matured_same_source_not_outlier_sustained_evaluable_low_confidence_uncovered_baseline_lag_30_300s",
        "raw_all_gold_silver": {
            "event_rows": len(raw_rows),
            "unique_tokens": len({row.get("token_ca") for row in raw_rows if row.get("token_ca")}),
        },
        "formal_evaluable_gold_silver": {
            "event_rows": len(formal),
            "unique_tokens": len({row.get("token_ca") for row in formal if row.get("token_ca")}),
        },
        "low_confidence_research_gold_silver": {
            "event_rows": len(low_rows),
            "unique_tokens": len({row.get("token_ca") for row in low_rows if row.get("token_ca")}),
            "baseline_lag_bucket_counts": dict(
                Counter(row.get("baseline_lag_bucket") or "UNKNOWN" for row in low_rows).most_common()
            ),
            "baseline_before_sustained_peak_rows": low_before_peak,
            "baseline_after_or_unknown_peak_rows": low_after_or_unknown_peak,
        },
        "low_confidence_31_60_gold_silver": {
            "event_rows": len(low_31_60),
            "unique_tokens": len({row.get("token_ca") for row in low_31_60 if row.get("token_ca")}),
        },
        "formal_drop_breakdown_non_exclusive": dict(
            Counter(failure for row in raw_rows for failure in row.get("evaluable_failures", [])).most_common()
        ),
    }


def build_time_legality_summary(raw_rows, low_rows, denominator):
    formal = (denominator.get("formal_evaluable_gold_silver") or {}).get("event_rows") or 0
    raw_all = (denominator.get("raw_all_gold_silver") or {}).get("event_rows") or len(raw_rows)
    before_peak = 0
    after_or_unknown = 0
    lag_deltas = []
    for row in low_rows:
        baseline_lag = safe_float(row.get("baseline_lag_sec"))
        peak_lag = safe_float(row.get("time_to_sustained_peak_sec"))
        if baseline_lag is not None and peak_lag is not None and baseline_lag <= peak_lag:
            before_peak += 1
            lag_deltas.append(peak_lag - baseline_lag)
        else:
            after_or_unknown += 1
    formal_plus_time_legal = formal + before_peak
    return {
        "classification": (
            "LOW_CONFIDENCE_TIME_LEGAL_RESEARCH_RECOVERABLE"
            if raw_all and formal_plus_time_legal / raw_all >= 0.8
            else "LOW_CONFIDENCE_TIME_LEGAL_RESEARCH_INSUFFICIENT"
        ),
        "promotion_allowed": False,
        "formal_denominator_changed": False,
        "allowed_use": "research_only",
        "raw_all_gold_silver_event_rows": raw_all,
        "formal_evaluable_event_rows": formal,
        "low_confidence_research_event_rows": len(low_rows),
        "baseline_before_sustained_peak_rows": before_peak,
        "baseline_after_or_unknown_peak_rows": after_or_unknown,
        "baseline_before_sustained_peak_rate": rate(before_peak, len(low_rows)),
        "baseline_lag_bucket_counts": dict(
            Counter(row.get("baseline_lag_bucket") or "UNKNOWN" for row in low_rows).most_common()
        ),
        "first_bar_lag_bucket_counts": dict(
            Counter(first_bar_lag_bucket(row.get("first_bar_lag_sec")) for row in low_rows).most_common()
        ),
        "median_peak_after_baseline_sec": (
            sorted(lag_deltas)[len(lag_deltas) // 2] if lag_deltas else None
        ),
        "formal_plus_time_legal_recoverable_rows": formal_plus_time_legal,
        "formal_plus_time_legal_recoverable_rate": rate(formal_plus_time_legal, raw_all),
        "reaches_80pct_if_research_rows_accepted": bool(
            raw_all and formal_plus_time_legal / raw_all >= 0.8
        ),
        "note": (
            "This is a research-only time-legality audit. It does not add low-confidence rows "
            "to the formal denominator and cannot be used for promotion without clean OOS validation."
        ),
    }


def build_report(args):
    now_ts = int(args.now_ts or time.time())
    since_ts = now_ts - int(float(args.hours) * 3600)
    raw_db = sqlite3.connect(args.raw_db)
    raw_db.row_factory = sqlite3.Row
    paper_db = sqlite3.connect(args.db)
    paper_db.row_factory = sqlite3.Row
    try:
        raw_rows = load_raw_gold_silver_rows(raw_db, since_ts)
        low_rows = [row for row in raw_rows if row.get("low_confidence_research_eligible")]
        obs, obs_meta = load_candidate_observations(paper_db, signal_ids(low_rows), since_ts)
        until_ts = max([row.get("signal_ts_norm") or since_ts for row in low_rows] + [now_ts])
        decisions = [] if args.skip_decisions else load_paper_decisions(paper_db, tokens(low_rows), since_ts, until_ts)
        trades = [] if args.skip_trades else load_paper_trades(paper_db, tokens(low_rows), since_ts, until_ts)
        audits = attach_records(low_rows, obs, decisions, trades, args.expected_candidates)
        candidate_layer = summarize_candidate_layer(audits, obs, args.expected_candidates)
        decision_layer = summarize_decision_layer(audits, decisions, trades)
        denominator = build_denominator_summary(raw_rows, low_rows)
        time_legality = build_time_legality_summary(raw_rows, low_rows, denominator)
        blockers = []
        if not raw_rows:
            blockers.append("raw_gold_silver_denominator_empty")
        if not low_rows:
            blockers.append("low_confidence_research_denominator_empty")
        if candidate_layer.get("full_candidate_coverage_rate") is not None and candidate_layer["full_candidate_coverage_rate"] < 0.99:
            blockers.append("low_confidence_candidate_coverage_incomplete")
        verdict = "LOW_CONFIDENCE_RESEARCH_READY"
        if not low_rows:
            verdict = "LOW_CONFIDENCE_RESEARCH_EMPTY"
        elif blockers:
            verdict = "LOW_CONFIDENCE_RESEARCH_BLOCKED_DATA"
        return {
            "schema_version": SCHEMA_VERSION,
            "report_type": "low_confidence_research_capture_audit",
            "generated_at": utc_now(),
            "window": {"hours": args.hours, "since_ts": since_ts, "until_ts": now_ts},
            "inputs": {"paper_db": args.db, "raw_db": args.raw_db},
            "evidence_level": EVIDENCE_LEVEL,
            "usage": "research_only_low_confidence_denominator",
            "promotion_allowed": False,
            "can_promote_live": False,
            "strategy_change_allowed": False,
            "canonical_backfill_performed": False,
            "formal_denominator_changed": False,
            "verdict": verdict,
            "blockers": blockers,
            "observation_load": obs_meta,
            "denominator": denominator,
            "time_legality": time_legality,
            "candidate_layer": candidate_layer,
            "decision_layer": decision_layer,
            "low_confidence_examples": sorted(
                [
                    {
                        "signal_id": row.get("signal_id_key"),
                        "token_ca": row.get("token_ca"),
                        "symbol": row.get("symbol"),
                        "baseline_lag_sec": row.get("baseline_lag_sec"),
                        "baseline_lag_bucket": row.get("baseline_lag_bucket"),
                        "time_to_sustained_peak_sec": row.get("time_to_sustained_peak_sec"),
                        "max_sustained_peak_pct": row.get("max_sustained_peak_pct"),
                    }
                    for row in low_rows
                ],
                key=lambda row: safe_float(row.get("max_sustained_peak_pct")) or 0,
                reverse=True,
            )[: args.limit],
            "missed_examples": sorted(
                audits,
                key=lambda row: safe_float(row.get("max_sustained_peak_pct")) or 0,
                reverse=True,
            )[: args.limit],
            "notes": [
                "Research-only diagnostic. Formal gold/silver denominator remains unchanged.",
                "This report must not be used for promotion without clean-window and OOS validation.",
            ],
        }
    finally:
        raw_db.close()
        paper_db.close()


def compact_summary(report):
    denominator = report.get("denominator") or {}
    low_den = denominator.get("low_confidence_research_gold_silver") or {}
    candidate = report.get("candidate_layer") or {}
    decision = report.get("decision_layer") or {}
    time_legality = report.get("time_legality") or {}
    return {
        "verdict": report.get("verdict"),
        "promotion_allowed": False,
        "formal_denominator_changed": False,
        "low_confidence_research_event_rows": low_den.get("event_rows"),
        "low_confidence_research_unique_tokens": low_den.get("unique_tokens"),
        "low_confidence_31_60_event_rows": (
            denominator.get("low_confidence_31_60_gold_silver") or {}
        ).get("event_rows"),
        "candidate_match_any_rate": candidate.get("candidate_match_any_rate"),
        "full_candidate_coverage_rate": candidate.get("full_candidate_coverage_rate"),
        "decision_record_rate": decision.get("decision_record_rate"),
        "would_enter_rate": decision.get("would_enter_rate"),
        "entered_rate": decision.get("entered_rate"),
        "terminal_bucket_counts": decision.get("terminal_bucket_counts"),
        "time_legality": {
            "classification": time_legality.get("classification"),
            "formal_plus_time_legal_recoverable_rate": time_legality.get("formal_plus_time_legal_recoverable_rate"),
            "reaches_80pct_if_research_rows_accepted": time_legality.get("reaches_80pct_if_research_rows_accepted"),
            "baseline_before_sustained_peak_rows": time_legality.get("baseline_before_sustained_peak_rows"),
            "baseline_after_or_unknown_peak_rows": time_legality.get("baseline_after_or_unknown_peak_rows"),
            "promotion_allowed": False,
            "formal_denominator_changed": False,
        },
        "top_candidates": candidate.get("top_candidates_by_low_confidence_raw_gs_match"),
        "blockers": report.get("blockers") or [],
    }


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


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
        for sig, token, cand, matched in [
            ("101", "LOW1", "current_all", 1),
            ("101", "LOW1", "cand_a", 1),
            ("102", "LOW2", "current_all", 1),
            ("102", "LOW2", "cand_a", 0),
        ]:
            paper.execute(
                "INSERT INTO candidate_shadow_observations VALUES (?,?,?,?,?,?,?,?,?)",
                (sig, token, now - 100, cand, "entry" if cand != "current_all" else "baseline", matched, "self_test", now - 50, "{}"),
            )
        paper.execute(
            """
            CREATE TABLE paper_decision_events(
              id INTEGER, event_ts INTEGER, signal_id TEXT, token_ca TEXT, symbol TEXT,
              lifecycle_id TEXT, component TEXT, reason TEXT, event_type TEXT, decision TEXT,
              route TEXT, data_source TEXT, lifecycle_state TEXT
            )
            """
        )
        paper.execute(
            "INSERT INTO paper_decision_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (1, now - 80, "101", "LOW1", "LOW1", "lc1", "final_entry_contract", "mode_disabled", "entry_block", "BLOCK", None, None, None),
        )
        paper.commit()
        paper.close()

        raw = sqlite3.connect(raw_path)
        raw.execute(
            """
            CREATE TABLE raw_signal_outcomes(
              id INTEGER, signal_id TEXT, token_ca TEXT, symbol TEXT, signal_ts INTEGER,
              signal_type TEXT, source TEXT, source_kind TEXT, source_family TEXT,
              observation_status TEXT, kline_covered INTEGER, coverage_reason TEXT,
              baseline_lag_sec REAL, baseline_confidence TEXT, same_source_path INTEGER,
              outlier_flag INTEGER, outlier_reason TEXT, sustained_evaluable INTEGER,
              sustained_reason TEXT, raw_primary_tier TEXT, raw_sustained_tier TEXT,
              max_sustained_peak_pct REAL, max_wick_peak_pct REAL,
              time_to_sustained_peak_sec REAL, did_enter INTEGER, entered_before_peak INTEGER,
              held_to_silver INTEGER, held_to_gold INTEGER, raw_dog_entered INTEGER,
              raw_dog_realized INTEGER, sold_before_silver INTEGER, sold_before_gold INTEGER,
              exit_reason TEXT, first_bar_lag_sec REAL, early_15m_bar_coverage_pct REAL,
              early_15m_complete INTEGER
            )
            """
        )
        raw.executemany(
            "INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (1, "101", "LOW1", "LOW1", now - 100, "ATH", "src", "dex", "native", "matured", 0, "low_confidence", 45, "low", 1, 0, None, 1, None, "silver", "silver", 120, 130, 100, 0, 0, 0, 0, 0, 0, 0, 0, None, 50, 100, 1),
                (2, "102", "LOW2", "LOW2", now - 90, "ATH", "src", "dex", "native", "matured", 0, "low_confidence", 100, "low", 1, 0, None, 1, None, "gold", "gold", 400, 420, 120, 0, 0, 0, 0, 0, 0, 0, 0, None, 60, 90, 1),
                (3, "103", "HIGH", "HIGH", now - 80, "ATH", "src", "dex", "native", "matured", 1, "covered", 8, "high", 1, 0, None, 1, None, "silver", "silver", 90, 95, 80, 0, 0, 0, 0, 0, 0, 0, 0, None, 10, 100, 1),
                (4, "104", "OUT", "OUT", now - 70, "ATH", "src", "dex", "native", "matured", 0, "outlier", 40, "low", 1, 1, "outlier", 1, None, "silver", "silver", 90, 95, 80, 0, 0, 0, 0, 0, 0, 0, 0, None, 10, 100, 1),
            ],
        )
        raw.commit()
        raw.close()

        args = argparse.Namespace(
            db=str(paper_path),
            raw_db=str(raw_path),
            hours=1,
            expected_candidates=2,
            now_ts=now,
            limit=10,
            out=None,
            skip_decisions=False,
            skip_trades=False,
        )
        report = build_report(args)
        assert report["promotion_allowed"] is False
        assert report["formal_denominator_changed"] is False
        assert report["denominator"]["raw_all_gold_silver"]["event_rows"] == 4
        assert report["denominator"]["formal_evaluable_gold_silver"]["event_rows"] == 1
        assert report["denominator"]["low_confidence_research_gold_silver"]["event_rows"] == 2
        assert report["denominator"]["low_confidence_31_60_gold_silver"]["event_rows"] == 1
        assert report["time_legality"]["baseline_before_sustained_peak_rows"] == 2
        assert report["time_legality"]["baseline_after_or_unknown_peak_rows"] == 0
        assert report["time_legality"]["formal_plus_time_legal_recoverable_rows"] == 3
        assert report["time_legality"]["formal_denominator_changed"] is False
        assert report["time_legality"]["promotion_allowed"] is False
        assert report["candidate_layer"]["candidate_matched_any_events"] == 2
        assert report["candidate_layer"]["full_candidate_coverage_rate"] == 1.0
        assert report["decision_layer"]["events_with_decision_record"] == 1
        compact = compact_summary(report)
        assert compact["low_confidence_research_event_rows"] == 2
        assert compact["time_legality"]["baseline_before_sustained_peak_rows"] == 2
        assert compact["promotion_allowed"] is False
    print("SELF_TEST_PASS low_confidence_research_capture_audit")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--expected-candidates", type=int, default=DEFAULT_EXPECTED_CANDIDATES)
    parser.add_argument("--now-ts", type=int, default=None)
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--out")
    parser.add_argument("--skip-decisions", action="store_true")
    parser.add_argument("--skip-trades", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return
    report = build_report(args)
    if args.out:
        write_json(args.out, report)
    print(json.dumps(compact_summary(report), sort_keys=True))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Replay strategy-memory hypotheses with entry-delay sensitivity buckets."""

from __future__ import annotations

import argparse
import json
import sqlite3
import tempfile
import time
from pathlib import Path

from strategy_hypothesis_registry import (
    DEFAULT_DATA_DIR,
    hypothesis_match_proxy,
    is_gold_silver,
    load_candidate_observations,
    load_hypotheses,
    load_raw_signal_outcomes,
    simulation_peak_pct,
    stage_sets_from_paper,
    write_json,
)


DELAYS = (0, 5, 10, 20, 30, 60)


def delay_penalty(row, delay_sec):
    """Conservative proxy when no quote/kline path is available."""
    payload = row.get("payload") or {}
    fbr = payload.get("first_bar_return_pct")
    try:
        fbr = float(fbr)
    except Exception:
        fbr = None
    if delay_sec == 0:
        return 1.0
    if fbr is None:
        return max(0.0, 1.0 - delay_sec / 120.0)
    if fbr > 10:
        return max(0.0, 1.0 - delay_sec / 45.0)
    if fbr > 3:
        return max(0.0, 1.0 - delay_sec / 75.0)
    return max(0.0, 1.0 - delay_sec / 150.0)


def build_report(args):
    now_ts = int(args.now_ts or time.time())
    hypotheses_report = load_hypotheses(args.hypotheses, source_docx=args.source_docx, source_text=args.source_text)
    raw_rows, raw_meta = load_raw_signal_outcomes(args.raw_db, hours=args.hours, now_ts=now_ts, limit=args.limit, gold_silver_only=False)
    signal_ids = [row["signal_id_key"] for row in raw_rows if row.get("signal_id_key")]
    observations, obs_meta = load_candidate_observations(args.db, signal_ids=signal_ids, hours=args.hours, now_ts=now_ts)
    obs_by_signal = {}
    for obs in observations:
        obs_by_signal.setdefault(obs.get("signal_id_key"), []).append(obs)
    stage_sets, stage_meta = stage_sets_from_paper(args.db, signal_ids, hours=args.hours, now_ts=now_ts)
    rows = []
    denominator_gs = [row for row in raw_rows if is_gold_silver(row)]
    for hyp in hypotheses_report.get("hypotheses", []):
        matched_rows = []
        for raw in raw_rows:
            payload = (obs_by_signal.get(raw.get("signal_id_key")) or [{}])[0].get("payload") or raw.get("payload") or {}
            if hypothesis_match_proxy(hyp, raw, payload):
                matched_rows.append(raw)
        delay_metrics = []
        for delay in DELAYS:
            adjusted = [row for row in matched_rows if delay_penalty(row, delay) >= 0.5]
            adjusted_ids = {row.get("signal_id_key") for row in adjusted}
            gs_adjusted = [row for row in adjusted if is_gold_silver(row)]
            decisions = len(adjusted_ids & stage_sets.get("decision", set()))
            pending = len(adjusted_ids & stage_sets.get("pending", set()))
            final = len(adjusted_ids & stage_sets.get("final_entry", set()))
            paper = len(adjusted_ids & stage_sets.get("paper", set()))
            realized = len(adjusted_ids & stage_sets.get("realized", set()))
            delay_metrics.append({
                "entry_delay_sec": delay,
                "matched_signals": len(adjusted),
                "gold_silver_matched": len(gs_adjusted),
                "recall": None if not denominator_gs else round(len(gs_adjusted) / len(denominator_gs), 6),
                "precision": None if not adjusted else round(len(gs_adjusted) / len(adjusted), 6),
                "decision_capture": None if not adjusted else round(decisions / len(adjusted), 6),
                "pending_capture": None if not adjusted else round(pending / len(adjusted), 6),
                "final_eligibility_capture": None if not adjusted else round(final / len(adjusted), 6),
                "mode_disabled_adjusted_final_eligibility_capture": None if not adjusted else round(final / len(adjusted), 6),
                "paper_capture": None if not adjusted else round(paper / len(adjusted), 6),
                "realized_capture": None if not adjusted else round(realized / len(adjusted), 6),
                "pnl_secondary_peak_proxy_avg": None if not adjusted else round(sum((simulation_peak_pct(row) or 0) for row in adjusted) / len(adjusted), 6),
            })
        rows.append({
            "hypothesis_id": hyp["id"],
            "strategy_family": hyp["strategy_family"],
            "allowed_use": "shadow_only",
            "promotion_allowed": False,
            "delay_adjustment_basis": "proxy_only_without_quote_or_real_kline_path",
            "ath1_delay_replay_required": hyp["strategy_family"] == "ATH1 early scout",
            "metrics_by_delay": delay_metrics,
        })
    return {
        "schema_version": "execution_delay_adjusted_replay.v1",
        "report_type": f"execution_delay_adjusted_replay_{int(args.hours)}h",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_ts)),
        "hours": args.hours,
        "raw_meta": raw_meta,
        "candidate_observation_meta": obs_meta,
        "stage_meta": stage_meta,
        "raw_signal_denominator": len(raw_rows),
        "raw_gold_silver_denominator": len(denominator_gs),
        "entry_delays_sec": list(DELAYS),
        "delay_replay_done": True,
        "historical_pnl_is_promotion_evidence": False,
        "same_window_discovery_is_promotion_evidence": False,
        "promotion_allowed": False,
        "allowed_use": "shadow_only",
        "hypotheses": rows,
    }


def self_test():
    with tempfile.TemporaryDirectory() as td:
        raw = Path(td) / "raw.db"
        paper = Path(td) / "paper.db"
        hyp = Path(td) / "h.json"
        now = int(time.time())
        db = sqlite3.connect(raw)
        db.execute("CREATE TABLE raw_signal_outcomes(signal_id INTEGER, token_ca TEXT, symbol TEXT, signal_ts INTEGER, signal_type TEXT, raw_primary_tier TEXT, max_sustained_peak_pct REAL, payload_json TEXT)")
        db.execute("INSERT INTO raw_signal_outcomes VALUES (1,'CA','AAA',?,'ATH','gold',120,?)", (now, json.dumps({"market_cap": 70000, "first_bar_return_pct": 5})))
        db.commit(); db.close()
        db = sqlite3.connect(paper)
        db.execute("CREATE TABLE candidate_shadow_observations(signal_id INTEGER, token_ca TEXT, signal_ts INTEGER, candidate_id TEXT, family TEXT, matched INTEGER, reason TEXT, observed_at INTEGER, payload_json TEXT)")
        db.commit(); db.close()
        hyp.write_text(json.dumps(load_hypotheses(source_text=None)), encoding="utf-8")
        args = argparse.Namespace(raw_db=str(raw), db=str(paper), hypotheses=str(hyp), source_docx=None, source_text=None, hours=24, now_ts=now, limit=10)
        report = build_report(args)
        assert report["promotion_allowed"] is False
        assert report["delay_replay_done"] is True
        assert report["entry_delays_sec"] == list(DELAYS)
    print("SELF_TEST_PASS execution_delay_adjusted_replay")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=str(DEFAULT_DATA_DIR / "paper_trades.db"))
    parser.add_argument("--raw-db", default=str(DEFAULT_DATA_DIR / "raw_signal_outcomes.db"))
    parser.add_argument("--hypotheses", default=str(DEFAULT_DATA_DIR / "strategy_memory_hypotheses.json"))
    parser.add_argument("--source-docx", default=None)
    parser.add_argument("--source-text", default=None)
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--now-ts", type=int, default=None)
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--out", default=str(DEFAULT_DATA_DIR / "execution_delay_adjusted_replay_24h.json"))
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return
    report = build_report(args)
    write_json(args.out, report)
    print(json.dumps({"out": args.out, "delay_replay_done": True, "promotion_allowed": False}, sort_keys=True))


if __name__ == "__main__":
    main()

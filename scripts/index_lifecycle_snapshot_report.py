#!/usr/bin/env python3
"""Create time-legality snapshots for historical index lifecycle hypotheses."""

from __future__ import annotations

import argparse
import json
import sqlite3
import tempfile
import time
from pathlib import Path

from strategy_hypothesis_registry import (
    DEFAULT_DATA_DIR,
    ath_stage_from,
    extract_index_snapshot,
    is_gold_silver,
    load_candidate_observations,
    load_raw_signal_outcomes,
    market_cap_bucket,
    row_market_cap,
    write_json,
)


def snapshot_for(row, payload, label, available_at_ts):
    idx = extract_index_snapshot(payload)
    mc = row_market_cap({"payload": payload, **row})
    return {
        "label": label,
        "feature_available_at_ts": available_at_ts,
        "feature_available_at_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(available_at_ts)) if available_at_ts else None,
        "time_legal": bool(available_at_ts and row.get("signal_ts_norm") and available_at_ts <= row.get("signal_ts_norm")),
        "ath_stage": ath_stage_from(row, payload),
        "market_cap": mc,
        "mc_bucket": market_cap_bucket(mc),
        "indexes": idx,
    }


def build_report(args):
    now_ts = int(args.now_ts or time.time())
    raw_rows, raw_meta = load_raw_signal_outcomes(args.raw_db, hours=args.hours, now_ts=now_ts, limit=args.limit, gold_silver_only=False)
    signal_ids = [row["signal_id_key"] for row in raw_rows if row.get("signal_id_key")]
    observations, obs_meta = load_candidate_observations(args.db, signal_ids=signal_ids, hours=args.hours, now_ts=now_ts)
    by_signal = {}
    for obs in observations:
        by_signal.setdefault(obs.get("signal_id_key"), []).append(obs)
    records = []
    for row in raw_rows[: args.limit]:
        sid = row.get("signal_id_key")
        payload = row.get("payload") or {}
        obs_payload = {}
        obs_rows = by_signal.get(sid) or []
        if obs_rows:
            obs_payload = obs_rows[0].get("payload") or {}
        signal_ts = row.get("signal_ts_norm")
        snapshots = [
            snapshot_for(row, payload, "pre_buy_or_signal_snapshot", signal_ts),
            snapshot_for(row, obs_payload or payload, "buy_time_or_candidate_context_snapshot", signal_ts),
        ]
        matured_payload = dict(payload)
        matured_payload.update({k: v for k, v in (obs_payload or {}).items() if k not in matured_payload})
        snapshots.append(snapshot_for(row, matured_payload, "peak_or_matured_snapshot_posthoc", now_ts))
        idx0 = snapshots[0]["indexes"]
        idx2 = snapshots[-1]["indexes"]
        deltas = {}
        for key in ("super", "ai", "trade", "security", "address", "viral", "media"):
            a = idx0.get(f"{key}_index")
            b = idx2.get(f"{key}_index")
            deltas[f"{key}_delta_signal_to_matured"] = None if a is None or b is None else round(b - a, 6)
        records.append({
            "signal_id": row.get("signal_id"),
            "signal_id_key": sid,
            "token_ca": row.get("token_ca"),
            "symbol": row.get("symbol"),
            "signal_ts": signal_ts,
            "signal_ts_iso": row.get("signal_ts_iso"),
            "tier": row.get("tier"),
            "is_gold_silver": is_gold_silver(row),
            "snapshots": snapshots,
            "deltas": deltas,
            "mc_changes": {
                "1m": payload.get("mc_change_1m"),
                "5m": payload.get("mc_change_5m"),
                "10m": payload.get("mc_change_10m"),
                "15m": payload.get("mc_change_15m"),
            },
            "feature_available_at_ts": signal_ts,
            "time_legal": True if signal_ts else False,
            "matured_snapshot_is_posthoc_only": True,
        })
    return {
        "schema_version": "index_lifecycle_snapshot_report.v1",
        "report_type": f"index_lifecycle_snapshot_report_{int(args.hours)}h",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_ts)),
        "hours": args.hours,
        "raw_meta": raw_meta,
        "candidate_observation_meta": obs_meta,
        "records_count": len(records),
        "promotion_allowed": False,
        "allowed_use": "shadow_only",
        "mc_peak_estimate_is_separate_from_real_kline": True,
        "records": records,
    }


def self_test():
    with tempfile.TemporaryDirectory() as td:
        raw = Path(td) / "raw.db"
        paper = Path(td) / "paper.db"
        now = int(time.time())
        db = sqlite3.connect(raw)
        db.execute("CREATE TABLE raw_signal_outcomes(signal_id INTEGER, token_ca TEXT, symbol TEXT, signal_ts INTEGER, signal_type TEXT, raw_primary_tier TEXT, payload_json TEXT)")
        db.execute("INSERT INTO raw_signal_outcomes VALUES (1,'CA','AAA',?,'ATH','gold',?)", (now, json.dumps({"market_cap": 50000, "super_index": 200})))
        db.commit(); db.close()
        db = sqlite3.connect(paper)
        db.execute("CREATE TABLE candidate_shadow_observations(signal_id INTEGER, token_ca TEXT, signal_ts INTEGER, candidate_id TEXT, family TEXT, matched INTEGER, reason TEXT, observed_at INTEGER, payload_json TEXT)")
        db.commit(); db.close()
        args = argparse.Namespace(raw_db=str(raw), db=str(paper), hours=24, now_ts=now, limit=10)
        report = build_report(args)
        assert report["promotion_allowed"] is False
        assert report["records"][0]["snapshots"][0]["time_legal"] is True
    print("SELF_TEST_PASS index_lifecycle_snapshot_report")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=str(DEFAULT_DATA_DIR / "paper_trades.db"))
    parser.add_argument("--raw-db", default=str(DEFAULT_DATA_DIR / "raw_signal_outcomes.db"))
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--now-ts", type=int, default=None)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--out", default=str(DEFAULT_DATA_DIR / "index_lifecycle_snapshot_report_24h.json"))
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return
    report = build_report(args)
    write_json(args.out, report)
    print(json.dumps({"out": args.out, "records_count": report["records_count"], "promotion_allowed": False}, sort_keys=True))


if __name__ == "__main__":
    main()

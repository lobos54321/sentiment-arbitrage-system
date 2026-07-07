#!/usr/bin/env python3
"""Audit Phase 3 long-horizon path observation readiness.

This is a read-only report. It does not change observers, runtime mode, strategy,
gates, executors, wallets, or risk settings.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import tempfile
import time
from pathlib import Path


SCHEMA_VERSION = "phase3_path_horizon_audit.v1"
DEFAULT_DATA_DIR = Path("/app/data")
DEFAULT_RAW_DB = DEFAULT_DATA_DIR / "raw_signal_outcomes.db"
DEFAULT_PAPER_DB = DEFAULT_DATA_DIR / "paper_trades.db"


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def safe_float(value, default=None):
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    except Exception:
        return default


def safe_int(value, default=None):
    parsed = safe_float(value)
    return default if parsed is None else int(parsed)


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def connect_readonly(path):
    path = Path(path)
    if not path.exists():
        return None
    try:
        uri = f"file:{path}?mode=ro"
        db = sqlite3.connect(uri, uri=True)
        db.row_factory = sqlite3.Row
        return db
    except sqlite3.Error:
        return None


def tables(db):
    if db is None:
        return set()
    return {
        str(row[0])
        for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def columns(db, table):
    if db is None:
        return set()
    try:
        return {str(row[1]) for row in db.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def first_col(cols, names):
    for name in names:
        if name in cols:
            return name
    return None


def sample_recent_rows(db, table, cols, max_rows):
    selected = []
    for col in cols:
        if col and col not in selected:
            selected.append(col)
    if not selected:
        return []
    column_sql = ", ".join(selected)
    try:
        rows = db.execute(
            f"SELECT {column_sql} FROM {table} ORDER BY rowid DESC LIMIT ?",
            (int(max_rows),),
        ).fetchall()
        return [dict(row) for row in rows]
    except Exception:
        try:
            rows = db.execute(
                f"SELECT {column_sql} FROM {table} LIMIT ?",
                (int(max_rows),),
            ).fetchall()
            return [dict(row) for row in rows]
        except Exception:
            return []


def horizon_summary(db, table, since_ts, max_sample_rows):
    cols = columns(db, table)
    ts_col = first_col(cols, ("ts", "timestamp", "event_ts", "observed_at", "created_at", "signal_ts", "bar_ts"))
    token_col = first_col(cols, ("token_ca", "token", "mint", "address", "token_address"))
    signal_col = first_col(cols, ("signal_id", "source_signal_id", "raw_signal_id"))
    if not ts_col:
        return {
            "table": table,
            "supported": False,
            "reason": "no_timestamp_column",
            "columns_seen": sorted(cols)[:30],
        }
    sample_rows = sample_recent_rows(db, table, (ts_col, token_col, signal_col), max_sample_rows)
    recent_sample = [
        row for row in sample_rows
        if safe_int(row.get(ts_col), 0) >= int(since_ts)
    ]
    max_span_sec = None
    token_count = None
    if token_col and recent_sample:
        spans = {}
        for row in recent_sample:
            token = str(row.get(token_col) or "")
            ts = safe_int(row.get(ts_col), None)
            if not token or ts is None:
                continue
            prev = spans.get(token)
            if prev is None:
                spans[token] = [ts, ts]
            else:
                prev[0] = min(prev[0], ts)
                prev[1] = max(prev[1], ts)
        token_count = len(spans)
        max_span_sec = max((hi - lo for lo, hi in spans.values()), default=0)
    return {
        "table": table,
        "supported": True,
        "timestamp_column": ts_col,
        "token_column": token_col,
        "signal_column": signal_col,
        "recent_rows": len(recent_sample),
        "sampled_rows": len(sample_rows),
        "sample_strategy": "rowid_desc_limited",
        "max_sample_rows": int(max_sample_rows),
        "recent_unique_tokens": token_count,
        "max_observed_path_span_sec": max_span_sec,
        "max_observed_path_span_hours": round(max_span_sec / 3600.0, 3) if max_span_sec is not None else None,
    }


def build_report(args):
    now_ts = int(time.time())
    since_ts = now_ts - int(float(args.hours) * 3600)
    target_horizon_sec = int(float(args.target_horizon_hours) * 3600)
    raw_db = connect_readonly(args.raw_db)
    paper_db = connect_readonly(args.paper_db)
    raw_tables = tables(raw_db)
    paper_tables = tables(paper_db)
    candidate_tables = [
        "raw_path_observations",
        "raw_signal_path_observations",
        "token_path_observations",
        "raw_price_bars_1m",
        "candidate_shadow_virtual_trades",
        "paper_decision_events",
    ]
    table_reports = []
    for table in candidate_tables:
        db = raw_db if table in raw_tables else paper_db if table in paper_tables else None
        if db is None:
            table_reports.append({"table": table, "supported": False, "reason": "table_missing"})
        else:
            table_reports.append(horizon_summary(db, table, since_ts, int(args.max_sample_rows)))
    observed_24h_tables = [
        row for row in table_reports
        if row.get("supported") and safe_int(row.get("max_observed_path_span_sec"), 0) >= target_horizon_sec
    ]
    partial_tables = [
        row for row in table_reports
        if row.get("supported") and safe_int(row.get("recent_rows"), 0) > 0
    ]
    classification = "PATH_24H_OBSERVED_SHADOW_ONLY" if observed_24h_tables else "PATH_24H_READY_FOR_SHADOW_IMPLEMENTATION"
    blockers = []
    if not raw_db:
        blockers.append("raw_signal_outcomes_db_unavailable")
    if not observed_24h_tables:
        blockers.append("no_observer_table_with_24h_path_span")
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "classification": classification,
        "window_hours": float(args.hours),
        "target_horizon_hours": float(args.target_horizon_hours),
        "raw_db_available": raw_db is not None,
        "paper_db_available": paper_db is not None,
        "table_reports": table_reports,
        "supported_path_table_count": len(partial_tables),
        "target_horizon_table_count": len(observed_24h_tables),
        "blockers": blockers,
        "readiness": {
            "allowed_use": "shadow_only",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "runtime_change_allowed": False,
            "required_contract": {
                "observation_horizon_hours": float(args.target_horizon_hours),
                "storage_cap_required": True,
                "provider_rate_limit_required": True,
                "missing_bars_must_be_recorded": True,
                "production_entry_exit_unchanged": True,
            },
        },
        "next_action": (
            "continue_read_only_long_horizon_monitoring"
            if observed_24h_tables
            else "implement_or_enable_shadow_only_24h_path_observer"
        ),
    }


def self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        raw = root / "raw.db"
        db = sqlite3.connect(raw)
        db.execute("CREATE TABLE raw_path_observations(token_ca TEXT, ts INTEGER, price REAL)")
        now = int(time.time())
        for offset in (0, 3600, 12 * 3600, 25 * 3600):
            db.execute(
                "INSERT INTO raw_path_observations(token_ca, ts, price) VALUES (?, ?, ?)",
                ("tokenA", now - 25 * 3600 + offset, 1.0 + offset),
            )
        db.commit()
        db.close()
        out = root / "out.json"
        args = argparse.Namespace(
            raw_db=str(raw),
            paper_db=str(root / "paper.db"),
            hours=48,
            target_horizon_hours=24,
            max_sample_rows=1000,
            out=str(out),
        )
        report = build_report(args)
        write_json(out, report)
        assert report["classification"] == "PATH_24H_OBSERVED_SHADOW_ONLY"
        assert report["readiness"]["promotion_allowed"] is False
        assert out.exists()
    print("SELF_TEST_PASS phase3_path_horizon_audit")


def parse_args():
    parser = argparse.ArgumentParser(description="Audit Phase 3 24h path observer readiness.")
    parser.add_argument("--raw-db", default=str(DEFAULT_RAW_DB))
    parser.add_argument("--paper-db", default=str(DEFAULT_PAPER_DB))
    parser.add_argument("--hours", default="24")
    parser.add_argument("--target-horizon-hours", default="24")
    parser.add_argument("--max-sample-rows", default="2000")
    parser.add_argument("--out", default=str(DEFAULT_DATA_DIR / "agent_runs/latest/phase3_path_horizon_audit_24h.json"))
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
        "target_horizon_table_count": report["target_horizon_table_count"],
        "promotion_allowed": False,
        "next_action": report["next_action"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

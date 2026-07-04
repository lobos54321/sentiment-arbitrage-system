#!/usr/bin/env python3
"""P8 pump.fun shadow source side-by-side comparison.

Read-only evaluator. It compares the isolated pump_fun_shadow signal store with
the existing Telegram/premium signal source and raw gold/silver outcomes. It
does not write production signal, decision, paper, executor, gate, canary, or
risk tables.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import tempfile
import time
from collections import defaultdict
from pathlib import Path


SCHEMA_VERSION = "pump_fun_shadow_source_comparison.v1"
PUMP_SOURCE = "pump_fun_shadow"
TG_SOURCE = "telegram_premium"


def now_sec() -> int:
    return int(time.time())


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def connect(path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not table_exists(conn, table):
        return set()
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}


def norm_token(value) -> str:
    return str(value or "").strip()


def safe_int(value, default=0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def normalize_ts(value) -> int | None:
    if value is None or value == "":
        return None
    try:
        num = float(value)
    except Exception:
        return None
    if num > 1_000_000_000_000:
        return int(num // 1000)
    return int(num)


def write_json(path: str | Path, payload) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def load_pump_signals(path: str | Path, since_ts: int) -> tuple[list[dict], dict]:
    if not path or not Path(path).exists():
        return [], {"available": False, "path": str(path), "reason": "db_missing"}
    conn = connect(path)
    try:
        if not table_exists(conn, "pump_fun_shadow_signals"):
            return [], {"available": False, "path": str(path), "reason": "table_missing"}
        cols = columns(conn, "pump_fun_shadow_signals")
        required = ["source_event_id", "mint", "symbol", "name", "event_ts", "observed_at", "provider", "source_component"]
        select = [name if name in cols else f"NULL AS {name}" for name in required]
        rows = conn.execute(
            f"""
            SELECT {", ".join(select)}
            FROM pump_fun_shadow_signals
            WHERE COALESCE(event_ts, observed_at, 0) >= ?
            ORDER BY COALESCE(event_ts, observed_at, 0) ASC, id ASC
            """,
            (since_ts,),
        ).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            token = norm_token(item.get("mint"))
            if not token:
                continue
            ts = normalize_ts(item.get("event_ts") or item.get("observed_at"))
            out.append(
                {
                    "source": PUMP_SOURCE,
                    "token_ca": token,
                    "symbol": item.get("symbol"),
                    "name": item.get("name"),
                    "event_ts": ts,
                    "source_event_id": item.get("source_event_id"),
                    "source_component": item.get("source_component") or "pump_fun_launch_stream",
                    "provider": item.get("provider") or "pump_fun",
                }
            )
        return out, {"available": True, "path": str(path), "rows": len(out)}
    finally:
        conn.close()


def load_tg_signals(path: str | Path, since_ts: int) -> tuple[list[dict], dict]:
    if not path or not Path(path).exists():
        return [], {"available": False, "path": str(path), "reason": "db_missing"}
    conn = connect(path)
    try:
        if not table_exists(conn, "premium_signals"):
            return [], {"available": False, "path": str(path), "reason": "table_missing"}
        cols = columns(conn, "premium_signals")
        if "token_ca" not in cols:
            return [], {"available": False, "path": str(path), "reason": "token_ca_missing"}
        timestamp_expr = (
            "CASE WHEN timestamp > 1000000000000 THEN CAST(timestamp / 1000 AS INTEGER) ELSE CAST(timestamp AS INTEGER) END"
            if "timestamp" in cols
            else "0"
        )
        select = [
            "id" if "id" in cols else "NULL AS id",
            "token_ca",
            "symbol" if "symbol" in cols else "NULL AS symbol",
            f"{timestamp_expr} AS event_ts",
            "signal_source" if "signal_source" in cols else "NULL AS signal_source",
            "source_event_id" if "source_event_id" in cols else "NULL AS source_event_id",
            "signal_type" if "signal_type" in cols else "NULL AS signal_type",
        ]
        rows = conn.execute(
            f"""
            SELECT {", ".join(select)}
            FROM premium_signals
            WHERE token_ca IS NOT NULL
              AND {timestamp_expr} >= ?
            ORDER BY {timestamp_expr} ASC, {"id" if "id" in cols else timestamp_expr} ASC
            """,
            (since_ts,),
        ).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            token = norm_token(item.get("token_ca"))
            if not token:
                continue
            out.append(
                {
                    "source": TG_SOURCE,
                    "token_ca": token,
                    "symbol": item.get("symbol"),
                    "event_ts": normalize_ts(item.get("event_ts")),
                    "source_event_id": item.get("source_event_id") or item.get("id"),
                    "signal_type": item.get("signal_type"),
                    "signal_source_raw": item.get("signal_source"),
                }
            )
        return out, {"available": True, "path": str(path), "rows": len(out)}
    finally:
        conn.close()


def raw_tier(row: dict) -> str | None:
    for name in ("raw_sustained_tier", "raw_primary_tier"):
        value = str(row.get(name) or "").lower()
        if value in {"gold", "silver"}:
            return value
    return None


def load_raw_gold_silver(path: str | Path, since_ts: int) -> tuple[list[dict], dict]:
    if not path or not Path(path).exists():
        return [], {"available": False, "path": str(path), "reason": "db_missing"}
    conn = connect(path)
    try:
        if not table_exists(conn, "raw_signal_outcomes"):
            return [], {"available": False, "path": str(path), "reason": "table_missing"}
        cols = columns(conn, "raw_signal_outcomes")
        tier_exprs = []
        if "raw_sustained_tier" in cols:
            tier_exprs.append("raw_sustained_tier IN ('gold', 'silver')")
        if "raw_primary_tier" in cols:
            tier_exprs.append("raw_primary_tier IN ('gold', 'silver')")
        if not tier_exprs:
            return [], {"available": False, "path": str(path), "reason": "tier_columns_missing"}
        fields = [
            "id",
            "signal_id",
            "token_ca",
            "symbol",
            "signal_ts",
            "source",
            "source_kind",
            "source_family",
            "raw_sustained_tier",
            "raw_primary_tier",
            "max_sustained_peak_pct",
            "time_to_sustained_peak_sec",
            "observation_status",
        ]
        select = [name if name in cols else f"NULL AS {name}" for name in fields]
        rows = conn.execute(
            f"""
            SELECT {", ".join(select)}
            FROM raw_signal_outcomes
            WHERE COALESCE(signal_ts, 0) >= ?
              AND ({' OR '.join(tier_exprs)})
            ORDER BY COALESCE(signal_ts, 0) ASC, COALESCE(id, 0) ASC
            """,
            (since_ts,),
        ).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            token = norm_token(item.get("token_ca"))
            if not token:
                continue
            out.append(
                {
                    "raw_event_id": item.get("id"),
                    "signal_id": item.get("signal_id"),
                    "token_ca": token,
                    "symbol": item.get("symbol"),
                    "signal_ts": normalize_ts(item.get("signal_ts")),
                    "source": item.get("source"),
                    "source_kind": item.get("source_kind"),
                    "source_family": item.get("source_family"),
                    "tier": raw_tier(item),
                    "max_sustained_peak_pct": item.get("max_sustained_peak_pct"),
                    "time_to_sustained_peak_sec": item.get("time_to_sustained_peak_sec"),
                    "observation_status": item.get("observation_status"),
                }
            )
        return out, {"available": True, "path": str(path), "rows": len(out)}
    finally:
        conn.close()


def unique_tokens(rows: list[dict]) -> set[str]:
    return {norm_token(row.get("token_ca")) for row in rows if norm_token(row.get("token_ca"))}


def rows_by_token(rows: list[dict]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        token = norm_token(row.get("token_ca"))
        if token:
            out[token].append(row)
    return out


def has_time_overlap(source_rows: list[dict], raw_row: dict, tolerance_sec: int) -> bool:
    raw_ts = normalize_ts(raw_row.get("signal_ts"))
    if raw_ts is None:
        return bool(source_rows)
    for row in source_rows:
        event_ts = normalize_ts(row.get("event_ts"))
        if event_ts is None:
            continue
        if abs(event_ts - raw_ts) <= tolerance_sec:
            return True
        # Pump launch often precedes Telegram discovery; count forward launches
        # within the tolerance as seen-before-source evidence.
        if event_ts <= raw_ts and raw_ts - event_ts <= tolerance_sec:
            return True
    return False


def source_raw_stats(source_rows: list[dict], raw_rows: list[dict], tolerance_sec: int) -> dict:
    source_index = rows_by_token(source_rows)
    matched_events = []
    matched_tokens = set()
    for raw in raw_rows:
        token = norm_token(raw.get("token_ca"))
        candidates = source_index.get(token) or []
        if candidates and has_time_overlap(candidates, raw, tolerance_sec):
            matched_events.append(raw)
            matched_tokens.add(token)
    return {
        "raw_gold_silver_event_rows_seen_by_source": len(matched_events),
        "raw_gold_silver_unique_tokens_seen_by_source": len(matched_tokens),
        "tiers": {
            "gold": sum(1 for row in matched_events if row.get("tier") == "gold"),
            "silver": sum(1 for row in matched_events if row.get("tier") == "silver"),
        },
    }


def span_days(rows: list[dict]) -> float:
    timestamps = [normalize_ts(row.get("event_ts")) for row in rows if normalize_ts(row.get("event_ts")) is not None]
    if not timestamps:
        return 0.0
    return max(0.0, (max(timestamps) - min(timestamps)) / 86400.0)


def sample_tokens(tokens: set[str], n: int = 20) -> list[str]:
    return sorted(tokens)[:n]


def build_report(args) -> dict:
    generated_at = utc_now()
    since_ts = now_sec() - int(args.hours * 3600)
    pump_rows, pump_meta = load_pump_signals(args.pump_db, since_ts)
    tg_rows, tg_meta = load_tg_signals(args.signal_db, since_ts)
    raw_rows, raw_meta = load_raw_gold_silver(args.raw_db, since_ts)

    pump_tokens = unique_tokens(pump_rows)
    tg_tokens = unique_tokens(tg_rows)
    raw_tokens = unique_tokens(raw_rows)
    both_tokens = pump_tokens & tg_tokens
    pump_only_tokens = pump_tokens - tg_tokens
    tg_only_tokens = tg_tokens - pump_tokens
    pump_raw_tokens = pump_tokens & raw_tokens
    tg_raw_tokens = tg_tokens & raw_tokens
    pump_incremental_raw_tokens = pump_raw_tokens - tg_tokens

    trial_span = span_days(pump_rows)
    trial_status = "P8_TRIAL_ACCUMULATING"
    if not pump_meta.get("available"):
        trial_status = "P8_SHADOW_SOURCE_DB_UNAVAILABLE"
    elif len(pump_rows) <= 0:
        trial_status = "P8_TRIAL_NO_PUMP_ROWS_YET"
    elif trial_span >= float(args.trial_days):
        trial_status = "P8_TRIAL_WINDOW_COMPLETE_READY_FOR_REVIEW"

    source_stats = {
        PUMP_SOURCE: {
            "signal_rows": len(pump_rows),
            "unique_tokens": len(pump_tokens),
            "observed_span_days": trial_span,
            **source_raw_stats(pump_rows, raw_rows, args.overlap_tolerance_sec),
        },
        TG_SOURCE: {
            "signal_rows": len(tg_rows),
            "unique_tokens": len(tg_tokens),
            "observed_span_days": span_days(tg_rows),
            **source_raw_stats(tg_rows, raw_rows, args.overlap_tolerance_sec),
        },
    }

    report = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "status": trial_status,
        "hours": args.hours,
        "since_ts": since_ts,
        "trial_days_required": args.trial_days,
        "overlap_tolerance_sec": args.overlap_tolerance_sec,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "production_impact": "zero_shadow_only",
        "guardrails": {
            "writes_premium_signals": False,
            "writes_candidate_observations": False,
            "writes_paper_trades": False,
            "changes_entry_policy": False,
            "changes_gates": False,
            "changes_executor": False,
            "changes_risk": False,
        },
        "inputs": {
            "pump_db": pump_meta,
            "signal_db": tg_meta,
            "raw_db": raw_meta,
        },
        "source_denominators": source_stats,
        "raw_gold_silver_denominator": {
            "event_rows": len(raw_rows),
            "unique_tokens": len(raw_tokens),
            "raw_source": raw_meta.get("source", "raw_signal_outcomes_db"),
        },
        "overlap_matrix": {
            "pump_fun_unique_tokens": len(pump_tokens),
            "telegram_unique_tokens": len(tg_tokens),
            "both_unique_tokens": len(both_tokens),
            "pump_only_unique_tokens": len(pump_only_tokens),
            "telegram_only_unique_tokens": len(tg_only_tokens),
            "pump_fun_raw_gold_silver_unique_tokens": len(pump_raw_tokens),
            "telegram_raw_gold_silver_unique_tokens": len(tg_raw_tokens),
            "pump_incremental_raw_gold_silver_unique_tokens_vs_telegram": len(pump_incremental_raw_tokens),
            "pump_only_sample_tokens": sample_tokens(pump_only_tokens),
            "telegram_only_sample_tokens": sample_tokens(tg_only_tokens),
            "pump_incremental_raw_dog_sample_tokens": sample_tokens(pump_incremental_raw_tokens),
        },
        "evidence_notes": [
            "P8 comparison is discovery-only; it cannot promote candidates or change production routes.",
            "Current raw_signal_outcomes may mostly reflect Telegram/premium signals; pump-only dog maturity requires the 30-day side-by-side trial.",
            "GMGN confirmation, X narrative, and smart-money precision layers are explicitly out of scope for this P8 artifact.",
        ],
        "verdict": {
            "classification": trial_status,
            "promotion_allowed": False,
            "next_action": (
                "continue_pump_fun_shadow_trial"
                if trial_status in {"P8_TRIAL_ACCUMULATING", "P8_TRIAL_NO_PUMP_ROWS_YET"}
                else "review_completed_30d_side_by_side"
            ),
        },
    }
    return report


def run_self_test() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        pump_db = tmp_path / "pump.db"
        signal_db = tmp_path / "signal.db"
        raw_db = tmp_path / "raw.db"
        now = 1_800_000_000

        pc = connect(pump_db)
        pc.executescript(
            """
            CREATE TABLE pump_fun_shadow_signals(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              source_event_id TEXT,
              mint TEXT,
              symbol TEXT,
              name TEXT,
              event_ts INTEGER,
              observed_at INTEGER,
              provider TEXT,
              source_component TEXT
            );
            """
        )
        pc.executemany(
            "INSERT INTO pump_fun_shadow_signals(source_event_id,mint,symbol,name,event_ts,observed_at,provider,source_component) VALUES (?,?,?,?,?,?,?,?)",
            [
                ("p1", "PUMP1", "P1", "Pump One", now - 100, now - 100, "self", "pump_fun_launch_stream"),
                ("p2", "SHARED", "SH", "Shared", now - 90, now - 90, "self", "pump_fun_launch_stream"),
            ],
        )
        pc.commit()
        pc.close()

        sc = connect(signal_db)
        sc.executescript(
            """
            CREATE TABLE premium_signals(
              id INTEGER PRIMARY KEY,
              token_ca TEXT,
              symbol TEXT,
              timestamp INTEGER,
              signal_source TEXT,
              source_event_id TEXT,
              signal_type TEXT
            );
            """
        )
        sc.executemany(
            "INSERT INTO premium_signals(id,token_ca,symbol,timestamp,signal_source,source_event_id,signal_type) VALUES (?,?,?,?,?,?,?)",
            [
                (1, "TG1", "TG", now - 80, "premium_channel", "t1", "NEW_TRENDING"),
                (2, "SHARED", "SH", now - 70, "premium_channel", "t2", "NEW_TRENDING"),
            ],
        )
        sc.commit()
        sc.close()

        rc = connect(raw_db)
        rc.executescript(
            """
            CREATE TABLE raw_signal_outcomes(
              id INTEGER PRIMARY KEY,
              signal_id TEXT,
              token_ca TEXT,
              symbol TEXT,
              signal_ts INTEGER,
              source TEXT,
              source_kind TEXT,
              source_family TEXT,
              raw_sustained_tier TEXT,
              raw_primary_tier TEXT,
              max_sustained_peak_pct REAL,
              time_to_sustained_peak_sec INTEGER,
              observation_status TEXT
            );
            """
        )
        rc.executemany(
            "INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (1, "r1", "PUMP1", "P1", now - 60, "raw", None, None, "gold", None, 120, 300, "matured"),
                (2, "r2", "TG1", "TG", now - 50, "raw", None, None, "silver", None, 70, 250, "matured"),
                (3, "r3", "SHARED", "SH", now - 40, "raw", None, None, "gold", None, 140, 400, "matured"),
            ],
        )
        rc.commit()
        rc.close()

        out = tmp_path / "comparison.json"
        args = argparse.Namespace(
            pump_db=str(pump_db),
            signal_db=str(signal_db),
            raw_db=str(raw_db),
            hours=24,
            trial_days=30,
            overlap_tolerance_sec=3600,
            out=str(out),
        )
        report = build_report(args)
        write_json(out, report)
        assert report["overlap_matrix"]["both_unique_tokens"] == 1
        assert report["overlap_matrix"]["pump_only_unique_tokens"] == 1
        assert report["overlap_matrix"]["telegram_only_unique_tokens"] == 1
        assert report["overlap_matrix"]["pump_incremental_raw_gold_silver_unique_tokens_vs_telegram"] == 1
        assert report["promotion_allowed"] is False
        print(json.dumps({"ok": True, "summary_path": str(out)}, indent=2))


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pump-db", default=os.environ.get("PUMP_FUN_SHADOW_DB", "/app/data/pump_fun_shadow_signals.db"))
    parser.add_argument("--signal-db", default=os.environ.get("SENTIMENT_DB", "/app/data/sentiment_arb.db"))
    parser.add_argument("--raw-db", default=os.environ.get("RAW_SIGNAL_OUTCOMES_DB", "/app/data/raw_signal_outcomes.db"))
    parser.add_argument("--hours", type=int, default=int(os.environ.get("PUMP_FUN_SHADOW_COMPARE_HOURS", "24")))
    parser.add_argument("--trial-days", type=int, default=int(os.environ.get("PUMP_FUN_SHADOW_TRIAL_DAYS", "30")))
    parser.add_argument("--overlap-tolerance-sec", type=int, default=int(os.environ.get("PUMP_FUN_SHADOW_OVERLAP_TOLERANCE_SEC", "21600")))
    parser.add_argument("--out", default=os.environ.get("PUMP_FUN_SHADOW_COMPARISON_OUT", "/app/data/agent_runs/latest/pump_fun_shadow_source_comparison_24h.json"))
    parser.add_argument("--quiet", action="store_true", help="Write the artifact without printing the full JSON payload.")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.self_test:
        run_self_test()
        return 0
    report = build_report(args)
    write_json(args.out, report)
    if args.quiet:
        print(json.dumps({
            "ok": True,
            "out": args.out,
            "status": report.get("status"),
            "pump_fun_unique_tokens": ((report.get("source_denominators") or {}).get(PUMP_SOURCE) or {}).get("unique_tokens"),
            "telegram_unique_tokens": ((report.get("source_denominators") or {}).get(TG_SOURCE) or {}).get("unique_tokens"),
            "promotion_allowed": report.get("promotion_allowed"),
        }, indent=2, sort_keys=True))
    else:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

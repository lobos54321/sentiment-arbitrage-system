#!/usr/bin/env python3
"""Materialize candidate-shadow cross summaries with runtime/source evidence.

This is intentionally a batch job, not a dashboard request path. It reads the
hot paper DB once, joins old candidate shadow rows to runtime/source evidence,
and writes a small JSON snapshot for dashboard/report consumption.
"""

from __future__ import annotations

import argparse
import bisect
import json
import math
import os
import sqlite3
import time
from collections import defaultdict
from pathlib import Path
from typing import Any


RUNTIME_MATCH_WINDOW_SEC = 600
DEFAULT_OUTPUT = "/app/data/candidate_shadow_runtime_cross_summary.json"
RUNTIME_TABLES = ("paper_decision_events", "paper_missed_signal_attribution")
DIMENSIONS = (
    "source_resonance_state",
    "source_quote_clean",
    "source_quote_executable_proxy",
    "markov_bucket",
    "lifecycle_profile",
    "lifecycle_state",
)


def now_sec() -> int:
    return int(time.time())


def parse_json(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        data = json.loads(str(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def first_value(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return None


def safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def table_exists(db: sqlite3.Connection, table: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return bool(row)


def columns(db: sqlite3.Connection, table: str) -> set[str]:
    if not table_exists(db, table):
        return set()
    return {row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}


def optional_col(cols: set[str], name: str, fallback: str = "NULL") -> str:
    return name if name in cols else f"{fallback} AS {name}"


def row_ts(row: sqlite3.Row, *names: str) -> int | None:
    for name in names:
        if name in row.keys():
            value = to_float(row[name])
            if value is not None:
                return int(value / 1000) if value > 10_000_000_000 else int(value)
    return None


def nested_get(payload: dict[str, Any], path: tuple[str, ...]) -> Any:
    cur: Any = payload
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur


def extract_markov_bucket(payload: dict[str, Any]) -> str | None:
    paths = (
        ("gate", "markov_bucket"),
        ("gate", "markovBucket"),
        ("markov_reclaim_gate", "markov_bucket"),
        ("markovReclaimGate", "markov_bucket"),
        ("markov_reclaim_forecast", "gate", "markov_bucket"),
        ("markovReclaimForecast", "gate", "markov_bucket"),
        ("lotto_markov_reclaim_forecast", "gate", "markov_bucket"),
        ("revival_canary", "markov_bucket"),
        ("revival_canary", "markovBucket"),
        ("revival_canary", "markov_reclaim_gate", "markov_bucket"),
        ("revival_canary", "markov_reclaim_forecast", "gate", "markov_bucket"),
        ("learning_bypass", "markov_bucket"),
        ("learning_bypass", "markovBucket"),
        ("markov_bucket",),
        ("markovBucket",),
    )
    value = first_value(*(nested_get(payload, path) for path in paths))
    return str(value).strip().lower() if value not in (None, "") else None


def extract_runtime_payload(row: sqlite3.Row) -> dict[str, Any]:
    payload = {}
    for key in ("payload_json", "raw_payload_json", "candidate_json", "details_json"):
        if key in row.keys():
            payload.update(parse_json(row[key]))
    return payload


def extract_runtime_features(row: sqlite3.Row) -> dict[str, Any]:
    payload = extract_runtime_payload(row)
    lifecycle_state = first_value(
        row["lifecycle_state"] if "lifecycle_state" in row.keys() else None,
        payload.get("lifecycle_state"),
        payload.get("lifecycleState"),
        nested_get(payload, ("lifecycle", "state")),
        nested_get(payload, ("revival_canary", "lifecycle_state")),
    )
    entry_bias = first_value(
        row["entry_bias"] if "entry_bias" in row.keys() else None,
        payload.get("entry_bias"),
        payload.get("entryBias"),
        nested_get(payload, ("lifecycle", "entry_bias")),
        nested_get(payload, ("revival_canary", "entry_bias")),
    )
    vitality = first_value(
        row["vitality_score"] if "vitality_score" in row.keys() else None,
        payload.get("vitality_score"),
        payload.get("vitalityScore"),
        nested_get(payload, ("lifecycle", "vitality_score")),
    )
    state_s = str(lifecycle_state).strip() if lifecycle_state not in (None, "") else None
    bias_s = str(entry_bias).strip() if entry_bias not in (None, "") else None
    profile = ":".join(part for part in (state_s, bias_s) if part) or None
    return {
        "runtime_state_seen": True,
        "runtime_state_source": row["runtime_source_table"],
        "markov_bucket": extract_markov_bucket(payload),
        "markov_bucket_seen": bool(extract_markov_bucket(payload)),
        "lifecycle_state": state_s,
        "lifecycle_entry_bias": bias_s,
        "lifecycle_vitality_score": to_float(vitality),
        "lifecycle_profile": profile,
    }


def source_features_from_row(row: sqlite3.Row) -> dict[str, Any]:
    quote_clean = bool(
        safe_bool(row["quote_clean_seen"] if "quote_clean_seen" in row.keys() else None)
        or safe_bool(row["two_quote_clean_snapshots"] if "two_quote_clean_snapshots" in row.keys() else None)
        or safe_bool(row["entry_quote_success_seen"] if "entry_quote_success_seen" in row.keys() else None)
    )
    level = to_float(row["resonance_level"] if "resonance_level" in row.keys() else None)
    cohort = row["cohort"] if "cohort" in row.keys() else None
    return {
        "source_resonance_seen": True,
        "source_resonance_state": cohort or (f"level_{int(level)}" if level is not None else "seen"),
        "source_resonance_cohort": cohort,
        "source_resonance_level": level,
        "source_resonance_score": to_float(row["resonance_score"] if "resonance_score" in row.keys() else None),
        "gmgn_pre_seen": safe_bool(row["gmgn_pre_seen"] if "gmgn_pre_seen" in row.keys() else None),
        "gmgn_lead_time_sec": to_float(row["gmgn_lead_time_sec"] if "gmgn_lead_time_sec" in row.keys() else None),
        "source_quote_clean_seen": quote_clean,
        "source_quote_executable_proxy": quote_clean,
        "source_entry_quote_fail_seen": safe_bool(row["entry_quote_fail_seen"] if "entry_quote_fail_seen" in row.keys() else None),
    }


def nearest_by_ts(rows_by_token: dict[str, list[tuple[int, dict[str, Any]]]], token_ca: str, ts: int | None) -> dict[str, Any]:
    if not token_ca or ts is None:
        return {}
    rows = rows_by_token.get(token_ca) or []
    if not rows:
        return {}
    times = [item[0] for item in rows]
    idx = bisect.bisect_left(times, int(ts))
    candidates = []
    if idx < len(rows):
        candidates.append(rows[idx])
    if idx > 0:
        candidates.append(rows[idx - 1])
    if not candidates:
        return {}
    best_ts, best = min(candidates, key=lambda item: abs(item[0] - int(ts)))
    return best if abs(best_ts - int(ts)) <= RUNTIME_MATCH_WINDOW_SEC else {}


def load_observations(db: sqlite3.Connection, since_ts: int) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in db.execute(
            """
            SELECT signal_id, token_ca, signal_ts, candidate_id, family, matched,
                   observed_at, payload_json
            FROM candidate_shadow_observations
            WHERE observed_at >= ?
            """,
            (since_ts,),
        ).fetchall()
    ]


def load_virtual_trades(db: sqlite3.Connection, since_ts: int) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in db.execute(
            """
            SELECT signal_id, token_ca, signal_ts, candidate_id, family, status,
                   entry_ts, exit_ts, net_pnl_pct, observed_at
            FROM candidate_shadow_virtual_trades
            WHERE observed_at >= ?
            """,
            (since_ts,),
        ).fetchall()
    ]


def load_source_rows(db: sqlite3.Connection, since_ts: int) -> dict[str, list[tuple[int, dict[str, Any]]]]:
    if not table_exists(db, "source_resonance_candidates"):
        return {}
    cols = columns(db, "source_resonance_candidates")
    wanted = (
        "token_ca",
        "signal_ts",
        "cohort",
        "quote_clean_seen",
        "two_quote_clean_snapshots",
        "entry_quote_success_seen",
        "entry_quote_fail_seen",
        "gmgn_pre_seen",
        "gmgn_lead_time_sec",
        "resonance_level",
        "resonance_score",
    )
    select_cols = ", ".join(optional_col(cols, col) for col in wanted)
    rows = db.execute(
        f"""
        SELECT {select_cols}
        FROM source_resonance_candidates
        WHERE signal_ts >= ?
        """,
        (since_ts - RUNTIME_MATCH_WINDOW_SEC,),
    ).fetchall()
    by_token: dict[str, list[tuple[int, dict[str, Any]]]] = defaultdict(list)
    for row in rows:
        token = row["token_ca"]
        ts = row_ts(row, "signal_ts")
        if token and ts is not None:
            by_token[token].append((ts, source_features_from_row(row)))
    for values in by_token.values():
        values.sort(key=lambda item: item[0])
    return by_token


def runtime_select_sql(table: str, cols: set[str]) -> str:
    ts_candidates = [name for name in ("event_ts", "signal_ts", "created_event_ts", "baseline_ts") if name in cols]
    ts_expr = ts_candidates[0] if len(ts_candidates) == 1 else f"COALESCE({', '.join(ts_candidates)}, 0)"
    if not ts_candidates:
        ts_expr = "0"
    return f"""
      SELECT
        '{table}' AS runtime_source_table,
        {optional_col(cols, 'id')},
        {optional_col(cols, 'signal_id')},
        {optional_col(cols, 'token_ca')},
        {optional_col(cols, 'signal_ts')},
        {optional_col(cols, 'event_ts')},
        {optional_col(cols, 'created_event_ts')},
        {optional_col(cols, 'baseline_ts')},
        {optional_col(cols, 'lifecycle_state')},
        {optional_col(cols, 'entry_bias')},
        {optional_col(cols, 'vitality_score')},
        {optional_col(cols, 'payload_json')},
        {optional_col(cols, 'raw_payload_json')},
        {optional_col(cols, 'candidate_json')},
        {optional_col(cols, 'details_json')},
        {ts_expr} AS runtime_ts
      FROM {table}
      WHERE {ts_expr} >= ?
    """


def load_runtime_rows(db: sqlite3.Connection, since_ts: int) -> tuple[dict[int, dict[str, Any]], dict[str, list[tuple[int, dict[str, Any]]]]]:
    by_signal: dict[int, dict[str, Any]] = {}
    by_token: dict[str, list[tuple[int, dict[str, Any]]]] = defaultdict(list)
    for table in RUNTIME_TABLES:
        if not table_exists(db, table):
            continue
        cols = columns(db, table)
        rows = db.execute(runtime_select_sql(table, cols), (since_ts - RUNTIME_MATCH_WINDOW_SEC,)).fetchall()
        for row in rows:
            features = extract_runtime_features(row)
            if not (features.get("markov_bucket") or features.get("lifecycle_profile") or features.get("lifecycle_state")):
                continue
            sid = row["signal_id"] if "signal_id" in row.keys() else None
            try:
                if sid is not None:
                    by_signal.setdefault(int(sid), features)
            except (TypeError, ValueError):
                pass
            token = row["token_ca"] if "token_ca" in row.keys() else None
            ts = row_ts(row, "runtime_ts", "event_ts", "signal_ts", "created_event_ts", "baseline_ts")
            if token and ts is not None:
                by_token[token].append((ts, features))
    for values in by_token.values():
        values.sort(key=lambda item: item[0])
    return by_signal, by_token


def merged_signal_features(
    obs: dict[str, Any],
    source_by_token: dict[str, list[tuple[int, dict[str, Any]]]],
    runtime_by_signal: dict[int, dict[str, Any]],
    runtime_by_token: dict[str, list[tuple[int, dict[str, Any]]]],
) -> dict[str, Any]:
    payload = parse_json(obs.get("payload_json"))
    token = obs.get("token_ca")
    signal_ts = int(obs["signal_ts"]) if obs.get("signal_ts") is not None else None
    features = dict(payload)
    source = nearest_by_ts(source_by_token, str(token), signal_ts)
    runtime = runtime_by_signal.get(int(obs["signal_id"])) or nearest_by_ts(runtime_by_token, str(token), signal_ts)
    features.update({k: v for k, v in source.items() if v is not None})
    features.update({k: v for k, v in runtime.items() if v is not None})
    return features


def dim_value(features: dict[str, Any], dim: str) -> str:
    if dim == "source_quote_clean":
        return "true" if safe_bool(features.get("source_quote_clean_seen")) else ("false" if "source_quote_clean_seen" in features else "unknown")
    if dim == "source_quote_executable_proxy":
        return "true" if safe_bool(features.get("source_quote_executable_proxy")) else ("false" if "source_quote_executable_proxy" in features else "unknown")
    value = features.get(dim)
    return str(value) if value not in (None, "") else "UNKNOWN"


def round_number(value: Any, digits: int = 4) -> float | None:
    f = to_float(value)
    return round(f, digits) if f is not None else None


def aggregate_rows(
    virtual_rows: list[dict[str, Any]],
    features_by_key: dict[tuple[int, str], dict[str, Any]],
    dimension: str,
    min_closed: int,
    limit: int,
) -> list[dict[str, Any]]:
    stats: dict[tuple[str, str, str], dict[str, Any]] = {}
    for trade in virtual_rows:
        key = (int(trade["signal_id"]), str(trade["candidate_id"]))
        features = features_by_key.get(key) or {}
        slice_value = dim_value(features, dimension)
        out_key = (str(trade["candidate_id"]), str(trade.get("family") or ""), slice_value)
        item = stats.setdefault(out_key, {
            "candidate_id": out_key[0],
            "family": out_key[1],
            "dimension": dimension,
            "slice_value": slice_value,
            "virtual_rows": 0,
            "closed_n": 0,
            "unique_tokens": set(),
            "wins": 0,
            "sum_pnl": 0.0,
            "gross_win_pct": 0.0,
            "gross_loss_pct": 0.0,
            "worst_net_pnl_pct": None,
            "best_net_pnl_pct": None,
            "hold_minutes_sum": 0.0,
            "hold_minutes_n": 0,
        })
        item["virtual_rows"] += 1
        if trade.get("token_ca"):
            item["unique_tokens"].add(trade["token_ca"])
        if trade.get("status") != "VIRTUAL_CLOSED":
            continue
        pnl = to_float(trade.get("net_pnl_pct"))
        if pnl is None:
            continue
        item["closed_n"] += 1
        item["sum_pnl"] += pnl
        if pnl > 0:
            item["wins"] += 1
            item["gross_win_pct"] += pnl
        elif pnl < 0:
            item["gross_loss_pct"] += -pnl
        item["worst_net_pnl_pct"] = pnl if item["worst_net_pnl_pct"] is None else min(item["worst_net_pnl_pct"], pnl)
        item["best_net_pnl_pct"] = pnl if item["best_net_pnl_pct"] is None else max(item["best_net_pnl_pct"], pnl)
        entry_ts = to_float(trade.get("entry_ts"))
        exit_ts = to_float(trade.get("exit_ts"))
        if entry_ts is not None and exit_ts is not None:
            item["hold_minutes_sum"] += (exit_ts - entry_ts) / 60.0
            item["hold_minutes_n"] += 1
    rows = []
    for item in stats.values():
        closed = int(item["closed_n"])
        if closed < min_closed:
            continue
        gross_loss = float(item["gross_loss_pct"])
        rows.append({
            "candidate_id": item["candidate_id"],
            "family": item["family"],
            "dimension": item["dimension"],
            "slice_value": item["slice_value"],
            "virtual_rows": int(item["virtual_rows"]),
            "closed_n": closed,
            "unique_tokens": len(item["unique_tokens"]),
            "wins": int(item["wins"]),
            "win_rate_pct": round(item["wins"] / closed * 100.0, 2) if closed else None,
            "avg_net_pnl_pct": round(item["sum_pnl"] / closed, 4) if closed else None,
            "total_net_pnl_pct": round(item["sum_pnl"], 4),
            "worst_net_pnl_pct": round_number(item["worst_net_pnl_pct"]),
            "best_net_pnl_pct": round_number(item["best_net_pnl_pct"]),
            "profit_factor": round(item["gross_win_pct"] / gross_loss, 4) if gross_loss > 0 else None,
            "avg_hold_minutes": round(item["hold_minutes_sum"] / item["hold_minutes_n"], 2) if item["hold_minutes_n"] else None,
        })
    rows.sort(key=lambda row: (row["avg_net_pnl_pct"] if row["avg_net_pnl_pct"] is not None else -10**9), reverse=True)
    return rows[:limit]


def build_snapshot(db_path: str, hours: int, min_closed: int, limit: int) -> dict[str, Any]:
    since_ts = now_sec() - int(hours) * 3600
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=30000")
    try:
        required = {"candidate_shadow_observations", "candidate_shadow_virtual_trades"}
        missing = [name for name in required if not table_exists(db, name)]
        if missing:
            return {
                "schema_version": "candidate_shadow_runtime_cross_summary.v1",
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "db_path": db_path,
                "window_hours": hours,
                "since_ts": since_ts,
                "available": False,
                "error": "missing_required_tables",
                "missing_tables": missing,
            }
        observations = load_observations(db, since_ts)
        virtual_rows = load_virtual_trades(db, since_ts)
        source_by_token = load_source_rows(db, since_ts)
        runtime_by_signal, runtime_by_token = load_runtime_rows(db, since_ts)
        features_by_key: dict[tuple[int, str], dict[str, Any]] = {}
        signal_seen: dict[int, dict[str, Any]] = {}
        for obs in observations:
            sid = int(obs["signal_id"])
            features = merged_signal_features(obs, source_by_token, runtime_by_signal, runtime_by_token)
            features_by_key[(sid, str(obs["candidate_id"]))] = features
            signal_seen.setdefault(sid, features)
        coverage_counts = {
            "source_resonance_seen_signals": sum(1 for f in signal_seen.values() if f.get("source_resonance_seen")),
            "source_quote_clean_seen_signals": sum(1 for f in signal_seen.values() if "source_quote_clean_seen" in f),
            "markov_bucket_seen_signals": sum(1 for f in signal_seen.values() if f.get("markov_bucket")),
            "lifecycle_profile_seen_signals": sum(1 for f in signal_seen.values() if f.get("lifecycle_profile") or f.get("lifecycle_state")),
        }
        dimensions = {
            dim: aggregate_rows(virtual_rows, features_by_key, dim, min_closed, limit)
            for dim in DIMENSIONS
        }
        return {
            "schema_version": "candidate_shadow_runtime_cross_summary.v1",
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "db_path": db_path,
            "window_hours": hours,
            "since_ts": since_ts,
            "since_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(since_ts)),
            "available": True,
            "min_closed": min_closed,
            "limit": limit,
            "coverage": {
                "signals": len(signal_seen),
                "observation_rows": len(observations),
                "virtual_rows": len(virtual_rows),
                "candidate_ids": len({row["candidate_id"] for row in observations}),
                "runtime_signal_features": coverage_counts,
            },
            "dimensions": dimensions,
        }
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--paper-db", default=os.environ.get("PAPER_DB") or os.environ.get("PAPER_TRADES_DB") or "/app/data/paper_trades.db")
    parser.add_argument("--out", default=os.environ.get("CANDIDATE_SHADOW_RUNTIME_CROSS_SUMMARY_PATH") or DEFAULT_OUTPUT)
    parser.add_argument("--hours", type=int, default=int(os.environ.get("CANDIDATE_SHADOW_RUNTIME_CROSS_HOURS", "24")))
    parser.add_argument("--min-closed", type=int, default=int(os.environ.get("CANDIDATE_SHADOW_RUNTIME_CROSS_MIN_CLOSED", "20")))
    parser.add_argument("--limit", type=int, default=int(os.environ.get("CANDIDATE_SHADOW_RUNTIME_CROSS_LIMIT", "500")))
    args = parser.parse_args()
    snapshot = build_snapshot(args.paper_db, max(1, args.hours), max(0, args.min_closed), max(1, args.limit))
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(out.suffix + ".tmp")
    tmp.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(out)
    print(json.dumps({
        "out": str(out),
        "available": snapshot.get("available"),
        "coverage": snapshot.get("coverage"),
    }, sort_keys=True))
    return 0 if snapshot.get("available") else 2


if __name__ == "__main__":
    raise SystemExit(main())

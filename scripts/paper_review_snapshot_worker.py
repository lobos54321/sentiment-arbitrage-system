#!/usr/bin/env python3
"""Materialize live-safe paper review snapshots.

This worker keeps small JSON summaries for 2h/8h/24h review windows so the
dashboard does not need to scan the live SQLite database for heavy reviews.
It is paper-only observability; it never changes trades or decisions.
"""

import argparse
import datetime as dt
import fcntl
import json
import os
import sqlite3
import tempfile
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PAPER_DB = PROJECT_ROOT / "data" / "paper_trades.db"
DEFAULT_OUT_DIR = PROJECT_ROOT / "data" / "review-artifacts" / "live"


def connect(path):
    db = sqlite3.connect(path, timeout=float(os.environ.get("PAPER_REVIEW_SQLITE_TIMEOUT_SEC", "30")))
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout = 30000")
    return db


def table_exists(db, table):
    return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def columns(db, table):
    if not table_exists(db, table):
        return set()
    return {row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}


def col_expr(cols, name, fallback="NULL", alias=None):
    target = alias or name
    return name if name in cols else f"{fallback} AS {target}"


def coalesce_expr(cols, names, fallback="0"):
    available = [name for name in names if name in cols]
    available.append(fallback)
    return f"COALESCE({', '.join(available)})"


def rows_as_dicts(rows):
    return [dict(row) for row in rows]


def one_as_dict(row):
    return dict(row) if row else {}


def missed_summary(db, since_ts, limit):
    if not table_exists(db, "paper_missed_signal_attribution"):
        return {"available": False, "reason": "paper_missed_signal_attribution_missing"}
    cols = columns(db, "paper_missed_signal_attribution")
    event_ts_expr = coalesce_expr(cols, ["created_event_ts", "signal_ts", "baseline_ts"], "0")
    max_pnl_expr = coalesce_expr(
        cols,
        [
            "executable_peak_pnl",
            "quote_clean_peak_pnl",
            "tradable_peak_pnl",
            "theoretical_peak_pnl",
            "max_pnl_recorded",
            "pnl_24h",
            "pnl_60m",
            "pnl_15m",
            "pnl_5m",
        ],
        "0",
    )
    quote_exec_expr = (
        """
          CASE
            WHEN COALESCE(tradable_missed, 0) = 1
             AND COALESCE(would_stop_before_peak, 0) != 1
            THEN 1 ELSE 0
          END
        """
        if "tradable_missed" in cols else "0"
    )
    tradable_expr = "COALESCE(tradable_missed, 0)" if "tradable_missed" in cols else "0"
    stop_before_expr = "COALESCE(would_stop_before_peak, 0)" if "would_stop_before_peak" in cols else "0"
    params = {"since": since_ts, "limit": limit}
    base = """
      WITH base AS (
        SELECT
          token_ca,
          COALESCE({symbol_expr}, substr(token_ca, 1, 8), '?') AS symbol,
          COALESCE({route_expr}, '-') AS route,
          COALESCE({component_expr}, '-') AS component,
          COALESCE({reject_reason_expr}, '-') AS reject_reason,
          {event_ts_expr} AS event_ts,
          {max_pnl_expr} AS max_pnl,
          {quote_exec_expr} AS quote_exec,
          {tradable_expr} AS tradable_missed,
          {stop_before_expr} AS would_stop_before_peak
        FROM paper_missed_signal_attribution
        WHERE {event_ts_expr} >= :since
          AND token_ca IS NOT NULL
          AND token_ca != ''
      ),
      per_token AS (
        SELECT
          token_ca,
          MAX(symbol) AS symbol,
          MAX(route) AS route,
          MAX(component) AS component,
          MAX(reject_reason) AS reject_reason,
          MAX(max_pnl) AS max_pnl,
          MAX(quote_exec) AS quote_exec,
          MAX(tradable_missed) AS tradable_missed,
          MAX(would_stop_before_peak) AS would_stop_before_peak
        FROM base
        GROUP BY token_ca
      ),
      per_blocker_token AS (
        SELECT
          route,
          component,
          reject_reason,
          token_ca,
          MAX(max_pnl) AS max_pnl,
          MAX(quote_exec) AS quote_exec,
          MAX(tradable_missed) AS tradable_missed,
          MAX(would_stop_before_peak) AS would_stop_before_peak
        FROM base
        GROUP BY route, component, reject_reason, token_ca
      )
    """.format(
        symbol_expr="symbol" if "symbol" in cols else "NULL",
        route_expr="route" if "route" in cols else "NULL",
        component_expr="component" if "component" in cols else "NULL",
        reject_reason_expr="reject_reason" if "reject_reason" in cols else "NULL",
        event_ts_expr=event_ts_expr,
        max_pnl_expr=max_pnl_expr,
        quote_exec_expr=quote_exec_expr,
        tradable_expr=tradable_expr,
        stop_before_expr=stop_before_expr,
    )
    overall = one_as_dict(db.execute(
        base
        + """
        SELECT
          COUNT(*) AS unique_tokens,
          SUM(CASE WHEN max_pnl >= 1.0 THEN 1 ELSE 0 END) AS gold_unique,
          SUM(CASE WHEN max_pnl >= 0.5 AND max_pnl < 1.0 THEN 1 ELSE 0 END) AS silver_unique,
          SUM(CASE WHEN max_pnl >= 0.25 AND max_pnl < 0.5 THEN 1 ELSE 0 END) AS bronze_unique,
          SUM(CASE WHEN quote_exec = 1 THEN 1 ELSE 0 END) AS quote_executable_unique,
          SUM(CASE WHEN tradable_missed = 1 THEN 1 ELSE 0 END) AS tradable_unique,
          SUM(CASE WHEN would_stop_before_peak = 1 THEN 1 ELSE 0 END) AS stop_before_peak_unique,
          MAX(max_pnl) AS max_pnl
        FROM per_token
        """,
        params,
    ).fetchone())
    by_gate = rows_as_dicts(db.execute(
        base
        + """
        SELECT
          route,
          component,
          reject_reason,
          COUNT(*) AS unique_tokens,
          SUM(CASE WHEN max_pnl >= 1.0 THEN 1 ELSE 0 END) AS gold_unique,
          SUM(CASE WHEN max_pnl >= 0.5 AND max_pnl < 1.0 THEN 1 ELSE 0 END) AS silver_unique,
          SUM(CASE WHEN max_pnl >= 0.25 AND max_pnl < 0.5 THEN 1 ELSE 0 END) AS bronze_unique,
          SUM(CASE WHEN quote_exec = 1 THEN 1 ELSE 0 END) AS quote_executable_unique,
          SUM(CASE WHEN tradable_missed = 1 THEN 1 ELSE 0 END) AS tradable_unique,
          SUM(CASE WHEN would_stop_before_peak = 1 THEN 1 ELSE 0 END) AS stop_before_peak_unique,
          MAX(max_pnl) AS max_pnl
        FROM per_blocker_token
        GROUP BY route, component, reject_reason
        ORDER BY gold_unique DESC, silver_unique DESC, bronze_unique DESC,
                 quote_executable_unique DESC, unique_tokens DESC, max_pnl DESC
        LIMIT :limit
        """,
        params,
    ).fetchall())
    top = rows_as_dicts(db.execute(
        base
        + """
        SELECT
          symbol,
          token_ca,
          route,
          component,
          reject_reason,
          max_pnl,
          quote_exec,
          tradable_missed,
          would_stop_before_peak
        FROM per_token
        WHERE max_pnl >= 0.25
        ORDER BY quote_exec DESC, max_pnl DESC
        LIMIT :limit
        """,
        params,
    ).fetchall())
    return {
        "available": True,
        "overall": overall,
        "by_gate": by_gate,
        "top_dogs": top,
    }


def trade_summary(db, since_ts, limit):
    if not table_exists(db, "paper_trades"):
        return {"available": False, "reason": "paper_trades_missing"}
    cols = columns(db, "paper_trades")
    entry_ts_expr = coalesce_expr(cols, ["entry_ts", "signal_ts"], "0")
    exit_ts_expr = "exit_ts" if "exit_ts" in cols else "NULL"
    pnl_expr = "pnl_pct" if "pnl_pct" in cols else "0"
    peak_expr = "peak_pnl" if "peak_pnl" in cols else "0"
    size_expr = "position_size_sol" if "position_size_sol" in cols else "0"
    mode_expr = "entry_mode" if "entry_mode" in cols else "strategy_stage" if "strategy_stage" in cols else "NULL"
    branch_expr = "entry_branch" if "entry_branch" in cols else "NULL"
    params = {"since": since_ts, "limit": limit}
    where = f"""
      WHERE (
        {entry_ts_expr} >= :since
        OR COALESCE({exit_ts_expr}, 0) >= :since
        OR {exit_ts_expr} IS NULL
      )
    """
    totals = one_as_dict(db.execute(
        f"""
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN {exit_ts_expr} IS NOT NULL THEN 1 ELSE 0 END) AS closed,
          SUM(CASE WHEN {exit_ts_expr} IS NULL THEN 1 ELSE 0 END) AS open,
          SUM(CASE WHEN {pnl_expr} > 0 THEN 1 ELSE 0 END) AS wins,
          AVG({pnl_expr}) AS avg_pnl,
          AVG({peak_expr}) AS avg_peak,
          SUM(COALESCE({size_expr}, 0) * COALESCE({pnl_expr}, 0)) AS est_pnl_sol
        FROM paper_trades
        {where}
        """,
        params,
    ).fetchone())
    by_mode = rows_as_dicts(db.execute(
        f"""
        SELECT
          COALESCE({mode_expr}, 'unknown') AS entry_mode,
          COALESCE({branch_expr}, '') AS entry_branch,
          COUNT(*) AS total,
          SUM(CASE WHEN {exit_ts_expr} IS NOT NULL THEN 1 ELSE 0 END) AS closed,
          SUM(CASE WHEN {pnl_expr} > 0 THEN 1 ELSE 0 END) AS wins,
          AVG({pnl_expr}) AS avg_pnl,
          AVG({peak_expr}) AS avg_peak,
          SUM(COALESCE({size_expr}, 0) * COALESCE({pnl_expr}, 0)) AS est_pnl_sol
        FROM paper_trades
        {where}
        GROUP BY COALESCE({mode_expr}, 'unknown'), COALESCE({branch_expr}, '')
        ORDER BY total DESC
        LIMIT :limit
        """,
        params,
    ).fetchall())
    return {"available": True, "totals": totals, "by_mode": by_mode}


def fast_lane_summary(db, since_ts, limit):
    if not table_exists(db, "paper_fast_entry_queue"):
        return {"available": False, "reason": "paper_fast_entry_queue_missing"}
    cols = columns(db, "paper_fast_entry_queue")
    created_expr = "created_at" if "created_at" in cols else "0"
    updated_expr = "updated_at" if "updated_at" in cols else created_expr
    status_expr = "status" if "status" in cols else "'unknown'"
    branch_expr = "entry_branch" if "entry_branch" in cols else "source_type" if "source_type" in cols else "NULL"
    first_error_expr = coalesce_expr(cols, ["first_error", "last_error"], "'none'")
    session_expr = "market_session" if "market_session" in cols else "'unknown'"
    params = {"since": since_ts, "limit": limit}
    status = rows_as_dicts(db.execute(
        f"""
        SELECT {status_expr} AS status, COUNT(*) AS n, MAX({updated_expr}) AS latest_updated_at
        FROM paper_fast_entry_queue
        WHERE COALESCE({created_expr}, {updated_expr}, 0) >= :since
           OR COALESCE({updated_expr}, {created_expr}, 0) >= :since
        GROUP BY {status_expr}
        ORDER BY n DESC
        """,
        params,
    ).fetchall())
    branches = rows_as_dicts(db.execute(
        f"""
        SELECT COALESCE({branch_expr}, 'unknown') AS entry_branch,
               {status_expr} AS status,
               COUNT(*) AS n
        FROM paper_fast_entry_queue
        WHERE COALESCE({created_expr}, {updated_expr}, 0) >= :since
           OR COALESCE({updated_expr}, {created_expr}, 0) >= :since
        GROUP BY COALESCE({branch_expr}, 'unknown'), {status_expr}
        ORDER BY n DESC
        LIMIT :limit
        """,
        params,
    ).fetchall())
    reasons = rows_as_dicts(db.execute(
        f"""
        SELECT {status_expr} AS status,
               {first_error_expr} AS reason,
               COUNT(*) AS n
        FROM paper_fast_entry_queue
        WHERE COALESCE({created_expr}, {updated_expr}, 0) >= :since
           OR COALESCE({updated_expr}, {created_expr}, 0) >= :since
        GROUP BY {status_expr}, {first_error_expr}
        ORDER BY n DESC
        LIMIT :limit
        """,
        params,
    ).fetchall())
    sessions = rows_as_dicts(db.execute(
        f"""
        SELECT COALESCE({session_expr}, 'unknown') AS market_session,
               {status_expr} AS status,
               COUNT(*) AS n
        FROM paper_fast_entry_queue
        WHERE COALESCE({created_expr}, {updated_expr}, 0) >= :since
           OR COALESCE({updated_expr}, {created_expr}, 0) >= :since
        GROUP BY COALESCE({session_expr}, 'unknown'), {status_expr}
        ORDER BY n DESC
        LIMIT :limit
        """,
        params,
    ).fetchall())
    return {
        "available": True,
        "queue_status": status,
        "branch_summary": branches,
        "reason_summary": reasons,
        "session_summary": sessions,
    }


def build_snapshot(db, hours, limit):
    now_ts = int(time.time())
    since_ts = now_ts - int(hours * 3600)
    generated_at = dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "schema_version": 1,
        "snapshot_id": f"paper_live_{hours}h_{now_ts}",
        "generated_at": generated_at,
        "window": {
            "hours": hours,
            "since_ts": since_ts,
            "until_ts": now_ts,
            "since_iso": dt.datetime.fromtimestamp(since_ts, dt.timezone.utc).isoformat().replace("+00:00", "Z"),
            "until_iso": dt.datetime.fromtimestamp(now_ts, dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        },
        "missed": missed_summary(db, since_ts, limit),
        "trades": trade_summary(db, since_ts, limit),
        "fast_lane": fast_lane_summary(db, since_ts, limit),
    }


def write_atomic(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, sort_keys=True)
            fh.write("\n")
        os.replace(tmp_name, path)
    finally:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except OSError:
            pass


def acquire_lock(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    fh = path.open("w", encoding="utf-8")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print(f"paper review snapshot lock held at {path}; duplicate worker idling", flush=True)
        return None
    fh.write(str(os.getpid()))
    fh.flush()
    return fh


def run_once(args):
    db = connect(args.paper_db)
    try:
        for hours in args.windows:
            started = time.time()
            snapshot = build_snapshot(db, hours, args.limit)
            snapshot["query_ms"] = int((time.time() - started) * 1000)
            out = Path(args.out_dir) / f"paper_review_{hours}h.json"
            write_atomic(out, snapshot)
            print(f"wrote {out} query_ms={snapshot['query_ms']}", flush=True)
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--paper-db", default=os.environ.get("PAPER_DB", str(DEFAULT_PAPER_DB)))
    parser.add_argument("--out-dir", default=os.environ.get("PAPER_REVIEW_LIVE_DIR", str(DEFAULT_OUT_DIR)))
    parser.add_argument("--windows", default=os.environ.get("PAPER_REVIEW_WINDOWS", "2,8,24"))
    parser.add_argument("--limit", type=int, default=int(os.environ.get("PAPER_REVIEW_SNAPSHOT_LIMIT", "40")))
    parser.add_argument("--interval", type=float, default=float(os.environ.get("PAPER_REVIEW_SNAPSHOT_INTERVAL_SEC", "300")))
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--lock-file", default=os.environ.get("PAPER_REVIEW_SNAPSHOT_LOCK_FILE", "/tmp/paper_review_snapshot.lock"))
    args = parser.parse_args()
    args.windows = [int(item.strip()) for item in str(args.windows).split(",") if item.strip()]

    lock_fh = acquire_lock(Path(args.lock_file))
    if lock_fh is None:
        while True:
            time.sleep(300)

    while True:
        try:
            run_once(args)
        except Exception as exc:
            print(f"paper review snapshot worker failed: {exc}", flush=True)
        if not args.loop:
            return
        time.sleep(args.interval)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Seed v2.7 source dog labels from paper trades tied to Telegram signals."""

import argparse
import calendar
import fcntl
import json
import os
import signal
import sqlite3
import sys
import time
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_event_log import V27EventLog, V27EventLogError  # noqa: E402
from v27_mirror_telegram_signals import _signal_payload, _timestamp_to_iso as _signal_timestamp_to_iso  # noqa: E402


DEFAULT_SIGNAL_DB = PROJECT_ROOT / "data" / "sentiment_arb.db"
DEFAULT_PAPER_DB = PROJECT_ROOT / "data" / "paper_trades.db"
DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
DEFAULT_LOCK_FILE = Path("/tmp/v27_paper_trade_source_label_mirror.lock")
SIGNAL_EVENT_TYPE = "telegram_signal_seen"
SOURCE_LABEL_EVENT_TYPE = "source_dog_label_recorded"
REQUIRED_PAPER_COLUMNS = {"id", "premium_signal_id", "entry_price", "peak_pnl"}


def _connect(db_path):
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    return db


def _table_exists(db, table):
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _table_columns(db, table):
    if not _table_exists(db, table):
        return set()
    return {row["name"] for row in db.execute(f"PRAGMA table_info({table})")}


def _row_dict(row):
    return {key: row[key] for key in row.keys()}


def _value(row, *names):
    for name in names:
        if name in row and row.get(name) is not None:
            return row.get(name)
    return None


def _as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_epoch_seconds(value):
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            ts = float(stripped)
            return ts / 1000.0 if ts > 1_000_000_000_000 else ts
        except ValueError:
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    parsed = time.strptime(stripped.replace("Z", "").split(".")[0], fmt)
                    return float(calendar.timegm(parsed))
                except Exception:
                    continue
            return None
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None
    return ts / 1000.0 if ts > 1_000_000_000_000 else ts


def _timestamp_to_iso(value):
    ts = _to_epoch_seconds(value)
    if ts is None:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def _row_filters(since_id=None, until_id=None):
    clauses = [
        "premium_signal_id IS NOT NULL",
        "entry_price IS NOT NULL",
        "entry_price > 0",
        "peak_pnl IS NOT NULL",
    ]
    params = []
    if since_id is not None:
        clauses.append("id >= ?")
        params.append(since_id)
    if until_id is not None:
        clauses.append("id <= ?")
        params.append(until_id)
    return f" WHERE {' AND '.join(clauses)}", params


def _paper_trade_schema_error(db, table):
    if not _table_exists(db, table):
        return f"{table} table missing"
    missing = sorted(REQUIRED_PAPER_COLUMNS - _table_columns(db, table))
    if missing:
        return f"{table} missing required columns: {', '.join(missing)}"
    return None


def iter_paper_trade_rows(db, *, since_id=None, until_id=None, limit=None, min_peak_pnl=0.5, table="paper_trades"):
    if _paper_trade_schema_error(db, table):
        return
    where, params = _row_filters(since_id=since_id, until_id=until_id)
    sql = f"SELECT * FROM {table}{where} AND peak_pnl >= ? ORDER BY id"
    params.append(min_peak_pnl)
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    yield from db.execute(sql, params)


def _signal_row_by_id(signal_db, signal_id, *, table="premium_signals"):
    if signal_id is None or not _table_exists(signal_db, table):
        return None
    row = signal_db.execute(f"SELECT * FROM {table} WHERE id = ? LIMIT 1", (signal_id,)).fetchone()
    return _row_dict(row) if row else None


def _label_from_peak(peak):
    if peak is None:
        return None
    if peak >= 1.0:
        return "gold"
    if peak >= 0.5:
        return "silver"
    if peak >= 0.3:
        return "copper"
    return "none"


def _denominator_key_from_signal(signal_payload):
    return ":".join(
        [
            str(signal_payload.get("chain") or "unknown_chain"),
            str(signal_payload.get("token_ca")),
            str(signal_payload.get("canonical_pool_group") or "unknown_pool"),
            str(signal_payload.get("lifecycle_epoch", 0)),
        ]
    )


def _trade_source_label_payload(row, signal_payload, *, cursor_high_watermark):
    peak = _as_float(row.get("peak_pnl"))
    label_available_at = _value(row, "exit_ts", "updated_at", "entry_ts", "signal_ts", "created_at")
    reference_price = _as_float(row.get("entry_price"))
    return {
        "paper_trade_id": row.get("id"),
        "paper_trade_cursor_high_watermark": cursor_high_watermark,
        "premium_signal_id": row.get("premium_signal_id"),
        "telegram_signal_id": signal_payload.get("telegram_signal_id"),
        "token_ca": signal_payload.get("token_ca"),
        "symbol": signal_payload.get("symbol") or _value(row, "symbol"),
        "chain": signal_payload.get("chain") or "solana",
        "canonical_pool_group": signal_payload.get("canonical_pool_group") or "unknown_pool",
        "lifecycle_epoch": signal_payload.get("lifecycle_epoch") or 0,
        "source_dog_label": _label_from_peak(peak),
        "source_dog_label_version": "legacy_paper_trade_peak_seed_v0.1",
        "source_label_quality": "legacy_paper_trade_peak_pnl",
        "source_label_research_only": True,
        "source_reference_price_type": "legacy_entry_price" if reference_price is not None else "missing",
        "source_reference_price": reference_price,
        "source_reference_price_ts": _value(row, "entry_ts", "signal_ts"),
        "source_label_window": "legacy_paper_trade_recorded_peak",
        "source_peak_type": "legacy_peak_pnl",
        "source_peak_value": peak,
        "source_gold_threshold": 1.0,
        "source_silver_threshold": 0.5,
        "source_copper_threshold": 0.3,
        "source_label_available_at": label_available_at,
        "legacy_paper_trade": row,
    }


def _candidate_rows(paper_db, signal_db, *, since_id=None, until_id=None, limit=None, min_peak_pnl=0.5, table="paper_trades", signal_table="premium_signals", default_chain="solana"):
    rows = [_row_dict(row) for row in iter_paper_trade_rows(paper_db, since_id=since_id, until_id=until_id, limit=limit, min_peak_pnl=min_peak_pnl, table=table)]
    candidates = []
    skipped = []
    for row in rows:
        signal_id = _as_int(row.get("premium_signal_id"))
        signal_row = _signal_row_by_id(signal_db, signal_id, table=signal_table)
        if not signal_row:
            skipped.append({"paper_trade_id": row.get("id"), "reason": "premium_signal_missing", "premium_signal_id": signal_id})
            continue
        signal_payload = _signal_payload(signal_row, default_chain=default_chain)
        signal_token = signal_payload.get("token_ca")
        paper_token = _value(row, "token_ca")
        if not signal_token:
            skipped.append({"paper_trade_id": row.get("id"), "reason": "premium_signal_missing_token_ca", "premium_signal_id": signal_id})
            continue
        if paper_token and str(paper_token).strip() != str(signal_token).strip():
            skipped.append(
                {
                    "paper_trade_id": row.get("id"),
                    "reason": "paper_trade_signal_token_mismatch",
                    "paper_trade_token_ca": paper_token,
                    "signal_token_ca": signal_token,
                }
            )
            continue
        candidates.append(
            {
                "row": row,
                "signal_payload": signal_payload,
                "signal_row": signal_row,
                "denominator_key": _denominator_key_from_signal(signal_payload),
                "peak_pnl": _as_float(row.get("peak_pnl")) or 0.0,
            }
        )
    selected = {}
    for candidate in candidates:
        key = candidate["denominator_key"]
        previous = selected.get(key)
        if previous is None:
            selected[key] = candidate
            continue
        if (candidate["peak_pnl"], _as_int(candidate["row"].get("id")) or 0) > (previous["peak_pnl"], _as_int(previous["row"].get("id")) or 0):
            selected[key] = candidate
    selected_ids = {candidate["row"].get("id") for candidate in selected.values()}
    for candidate in candidates:
        if candidate["row"].get("id") not in selected_ids:
            skipped.append(
                {
                    "paper_trade_id": candidate["row"].get("id"),
                    "reason": "lower_peak_duplicate_denominator_key",
                    "denominator_key": candidate["denominator_key"],
                }
            )
    return rows, candidates, list(selected.values()), skipped


def mirror_paper_trade_source_labels(
    paper_db_path,
    signal_db_path,
    event_log_dir,
    *,
    since_id=None,
    until_id=None,
    limit=None,
    min_peak_pnl=0.5,
    dry_run=False,
    table="paper_trades",
    signal_table="premium_signals",
    default_chain="solana",
):
    summary = {
        "paper_db": str(paper_db_path),
        "signal_db": str(signal_db_path),
        "event_log_dir": str(event_log_dir),
        "table": table,
        "signal_table": signal_table,
        "min_peak_pnl": min_peak_pnl,
        "read_rows": 0,
        "eligible_rows": 0,
        "selected_rows": 0,
        "signal_appended": 0,
        "signal_duplicate": 0,
        "source_appended": 0,
        "source_duplicate": 0,
        "skipped": 0,
        "failed": 0,
        "dry_run": bool(dry_run),
        "skip_reasons": [],
        "failures": [],
    }
    with _connect(paper_db_path) as paper_db, _connect(signal_db_path) as signal_db:
        schema_error = _paper_trade_schema_error(paper_db, table)
        if schema_error:
            summary["failed"] += 1
            summary["failures"].append({"reason": schema_error})
            return summary
        if not _table_exists(signal_db, signal_table):
            summary["failed"] += 1
            summary["failures"].append({"reason": f"{signal_table} table missing"})
            return summary

        rows, candidates, selected, skipped = _candidate_rows(
            paper_db,
            signal_db,
            since_id=since_id,
            until_id=until_id,
            limit=limit,
            min_peak_pnl=min_peak_pnl,
            table=table,
            signal_table=signal_table,
            default_chain=default_chain,
        )
        summary["read_rows"] = len(rows)
        summary["eligible_rows"] = len(candidates)
        summary["selected_rows"] = len(selected)
        summary["skipped"] = len(skipped)
        summary["skip_reasons"] = skipped[:20]
        cursor_high_watermark = max([_as_int(row.get("id")) or 0 for row in rows], default=None)

        if dry_run:
            return summary

        event_log = V27EventLog(event_log_dir)
        for candidate in selected:
            row = candidate["row"]
            signal_payload = candidate["signal_payload"]
            signal_row = candidate["signal_row"]
            try:
                token_ca = signal_payload.get("token_ca")
                signal_aggregate_id = f"telegram_signal:{signal_payload.get('chain')}:{token_ca}:unknown_pool:0"
                signal_result = event_log.append_event(
                    event_type=SIGNAL_EVENT_TYPE,
                    aggregate_id=signal_aggregate_id,
                    payload=signal_payload,
                    source="premium_signals",
                    idempotency_key=f"premium_signals:{signal_payload.get('telegram_signal_id')}",
                    observed_at=_signal_timestamp_to_iso(signal_payload.get("source_message_ts")),
                    available_at=_signal_timestamp_to_iso(signal_payload.get("receive_ts")),
                )
                signal_status = signal_result.get("status")
                if signal_status == "appended":
                    summary["signal_appended"] += 1
                elif signal_status == "duplicate":
                    summary["signal_duplicate"] += 1
                else:
                    raise RuntimeError(f"unexpected signal status {signal_status}")

                source_payload = _trade_source_label_payload(row, signal_payload, cursor_high_watermark=cursor_high_watermark)
                source_result = event_log.append_event(
                    event_type=SOURCE_LABEL_EVENT_TYPE,
                    aggregate_id=f"source_label:{source_payload.get('chain')}:{source_payload.get('token_ca')}:unknown_pool:0",
                    payload=source_payload,
                    source="paper_trades",
                    idempotency_key=f"paper_trade_source_label:{row.get('id')}",
                    observed_at=_timestamp_to_iso(source_payload.get("source_label_available_at")),
                    available_at=_timestamp_to_iso(source_payload.get("source_label_available_at")),
                    causal_parent_event_id=signal_result.get("event", {}).get("event_id"),
                )
                source_status = source_result.get("status")
                if source_status == "appended":
                    summary["source_appended"] += 1
                elif source_status == "duplicate":
                    summary["source_duplicate"] += 1
                else:
                    raise RuntimeError(f"unexpected source status {source_status}")
            except Exception as exc:
                summary["failed"] += 1
                summary["failures"].append({"paper_trade_id": row.get("id"), "signal_row_id": signal_row.get("id"), "reason": str(exc)})
    return summary


def _mirrored_paper_trade_ids(event_log_dir):
    counts = {}
    if not (Path(event_log_dir) / "events.jsonl").exists():
        return counts
    for event in V27EventLog(event_log_dir).iter_events():
        if event.get("event_type") != SOURCE_LABEL_EVENT_TYPE:
            continue
        payload = event.get("payload") or {}
        paper_trade_id = payload.get("paper_trade_id")
        if paper_trade_id is None:
            continue
        counts[int(paper_trade_id)] = counts.get(int(paper_trade_id), 0) + 1
    return counts


def max_mirrored_paper_trade_id(event_log_dir):
    max_seen = None
    if not (Path(event_log_dir) / "events.jsonl").exists():
        return None
    for event in V27EventLog(event_log_dir).iter_events():
        payload = event.get("payload") or {}
        if payload.get("paper_trade_id") is None:
            continue
        for value in (payload.get("paper_trade_id"), payload.get("paper_trade_cursor_high_watermark")):
            parsed = _as_int(value)
            if parsed is not None:
                max_seen = parsed if max_seen is None else max(max_seen, parsed)
    return max_seen


def next_unmirrored_since_id(event_log_dir, configured_since_id=None):
    max_id = max_mirrored_paper_trade_id(event_log_dir)
    if max_id is None:
        return configured_since_id
    next_id = int(max_id) + 1
    if configured_since_id is not None:
        return max(next_id, int(configured_since_id))
    return next_id


def _eligible_paper_trade_ids(paper_db_path, signal_db_path, *, since_id=None, until_id=None, limit=None, min_peak_pnl=0.5, table="paper_trades", signal_table="premium_signals", default_chain="solana"):
    with _connect(paper_db_path) as paper_db, _connect(signal_db_path) as signal_db:
        if _paper_trade_schema_error(paper_db, table) or not _table_exists(signal_db, signal_table):
            return []
        _rows, _candidates, selected, _skipped = _candidate_rows(
            paper_db,
            signal_db,
            since_id=since_id,
            until_id=until_id,
            limit=limit,
            min_peak_pnl=min_peak_pnl,
            table=table,
            signal_table=signal_table,
            default_chain=default_chain,
        )
        return [candidate["row"]["id"] for candidate in selected]


def verify_paper_trade_source_label_mirror_parity(
    paper_db_path,
    signal_db_path,
    event_log_dir,
    *,
    since_id=None,
    until_id=None,
    limit=None,
    min_peak_pnl=0.5,
    table="paper_trades",
    signal_table="premium_signals",
    default_chain="solana",
):
    summary = {
        "paper_db": str(paper_db_path),
        "signal_db": str(signal_db_path),
        "event_log_dir": str(event_log_dir),
        "table": table,
        "signal_table": signal_table,
        "eligible_db_rows": 0,
        "mirrored_events": 0,
        "missing_paper_trade_ids": [],
        "duplicate_paper_trade_ids": [],
        "orphan_mirrored_paper_trade_ids": [],
        "event_log_verify": None,
        "event_log_error": None,
        "parity_ok": False,
    }
    db_ids = _eligible_paper_trade_ids(
        paper_db_path,
        signal_db_path,
        since_id=since_id,
        until_id=until_id,
        limit=limit,
        min_peak_pnl=min_peak_pnl,
        table=table,
        signal_table=signal_table,
        default_chain=default_chain,
    )
    mirrored_counts = _mirrored_paper_trade_ids(event_log_dir)
    db_id_set = set(db_ids)
    mirrored_id_set = set(mirrored_counts)
    scoped = since_id is not None or until_id is not None or limit is not None

    summary["eligible_db_rows"] = len(db_ids)
    summary["mirrored_events"] = sum(mirrored_counts.get(paper_trade_id, 0) for paper_trade_id in db_id_set) if scoped else sum(mirrored_counts.values())
    summary["missing_paper_trade_ids"] = sorted(db_id_set - mirrored_id_set)
    summary["duplicate_paper_trade_ids"] = sorted([paper_trade_id for paper_trade_id in db_id_set if mirrored_counts.get(paper_trade_id, 0) > 1])
    summary["orphan_mirrored_paper_trade_ids"] = [] if scoped else sorted(mirrored_id_set - db_id_set)
    try:
        summary["event_log_verify"] = V27EventLog(event_log_dir).verify()
    except V27EventLogError as exc:
        summary["event_log_error"] = str(exc)
    summary["parity_ok"] = (
        not summary["missing_paper_trade_ids"]
        and not summary["duplicate_paper_trade_ids"]
        and summary["event_log_error"] is None
    )
    return summary


def acquire_loop_lock(lock_path):
    lock_path = Path(lock_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = lock_path.open("w", encoding="utf-8")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fh.close()
        return None
    fh.write(f"{os.getpid()}\n")
    fh.flush()
    return fh


def run_mirror_once(args):
    event_log_dir = Path(args.event_log_dir)
    since_id = next_unmirrored_since_id(event_log_dir, args.since_id) if args.new_only else args.since_id
    mirror_summary = mirror_paper_trade_source_labels(
        Path(args.paper_db),
        Path(args.signal_db),
        event_log_dir,
        since_id=since_id,
        until_id=args.until_id,
        limit=args.limit,
        min_peak_pnl=args.min_peak_pnl,
        dry_run=args.dry_run,
        table=args.table,
        signal_table=args.signal_table,
        default_chain=args.default_chain,
    )
    verify_summary = None
    if not args.dry_run:
        verify_summary = verify_paper_trade_source_label_mirror_parity(
            Path(args.paper_db),
            Path(args.signal_db),
            event_log_dir,
            since_id=since_id,
            until_id=args.until_id,
            limit=args.limit,
            min_peak_pnl=args.min_peak_pnl,
            table=args.table,
            signal_table=args.signal_table,
            default_chain=args.default_chain,
        )
    return {
        "mirror": mirror_summary,
        "verify": verify_summary,
        "cursor": {
            "new_only": bool(args.new_only),
            "since_id": since_id,
            "until_id": args.until_id,
            "limit": args.limit,
            "max_mirrored_paper_trade_id": max_mirrored_paper_trade_id(event_log_dir),
        },
    }


def run_mirror_loop(args):
    interval = max(5, int(args.interval))
    stop_requested = False

    def request_stop(_signum, _frame):
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    lock_fh = acquire_loop_lock(args.lock_file)
    if lock_fh is None:
        print(f"v2.7 paper trade source label mirror lock held at {args.lock_file}; duplicate worker idling", flush=True)
        while not stop_requested:
            time.sleep(interval)
        return {"status": "duplicate_worker_stopped", "lock_file": str(args.lock_file)}

    if args.initial_delay:
        time.sleep(max(0, int(args.initial_delay)))

    last_result = None
    try:
        while not stop_requested:
            try:
                last_result = run_mirror_once(args)
                print(json.dumps(last_result, ensure_ascii=False, sort_keys=True), flush=True)
            except Exception as exc:
                print(json.dumps({"mirror": {"failed": 1, "failures": [{"reason": str(exc)}]}, "verify": None, "error": str(exc)}, ensure_ascii=False, sort_keys=True), flush=True)
            slept = 0
            while slept < interval and not stop_requested:
                time.sleep(min(1, interval - slept))
                slept += 1
    finally:
        try:
            lock_fh.close()
        except Exception:
            pass
    return last_result or {"status": "stopped_before_first_mirror"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--paper-db", default=str(DEFAULT_PAPER_DB))
    parser.add_argument("--signal-db", default=str(DEFAULT_SIGNAL_DB))
    parser.add_argument("--event-log-dir", default=str(DEFAULT_EVENT_LOG_DIR))
    parser.add_argument("--table", default="paper_trades")
    parser.add_argument("--signal-table", default="premium_signals")
    parser.add_argument("--default-chain", default="solana")
    parser.add_argument("--since-id", type=int)
    parser.add_argument("--until-id", type=int)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--min-peak-pnl", type=float, default=0.5)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--new-only", action="store_true")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=int, default=30)
    parser.add_argument("--initial-delay", type=int, default=0)
    parser.add_argument("--lock-file", default=str(DEFAULT_LOCK_FILE))
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()

    if args.verify_only:
        summary = verify_paper_trade_source_label_mirror_parity(
            Path(args.paper_db),
            Path(args.signal_db),
            Path(args.event_log_dir),
            since_id=args.since_id,
            until_id=args.until_id,
            limit=args.limit,
            min_peak_pnl=args.min_peak_pnl,
            table=args.table,
            signal_table=args.signal_table,
            default_chain=args.default_chain,
        )
        print(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2))
        raise SystemExit(0 if summary["parity_ok"] else 1)

    result = run_mirror_loop(args) if args.loop else run_mirror_once(args)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    mirror_summary = result.get("mirror") or {}
    verify_summary = result.get("verify")
    if mirror_summary.get("failed") or (verify_summary and not verify_summary["parity_ok"]):
        raise SystemExit(1)


if __name__ == "__main__":
    main()

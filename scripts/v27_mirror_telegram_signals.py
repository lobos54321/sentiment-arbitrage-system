#!/usr/bin/env python3
"""Backfill premium Telegram signal anchors into the v2.7 event log."""

import argparse
import calendar
import hashlib
import json
import sqlite3
import sys
import time
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_event_log import V27EventLog, V27EventLogError  # noqa: E402


DEFAULT_SIGNAL_DB = PROJECT_ROOT / "data" / "sentiment_arb.db"
DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
SIGNAL_EVENT_TYPE = "telegram_signal_seen"


def _table_exists(db, table):
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _connect(db_path):
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    return db


def _row_dict(row):
    return {key: row[key] for key in row.keys()}


def _value(row, *names):
    for name in names:
        if name in row and row.get(name) is not None:
            return row.get(name)
    return None


def _parse_json_object(value):
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except Exception as exc:
        return {"_raw_json": value, "_json_parse_error": str(exc)}
    return parsed if isinstance(parsed, dict) else {"_raw_json": value, "_json_parse_error": "json value is not an object"}


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
            try:
                parsed = time.strptime(stripped.replace("Z", ""), "%Y-%m-%dT%H:%M:%S")
                return float(calendar.timegm(parsed))
            except Exception:
                try:
                    parsed = time.strptime(stripped.split(".")[0], "%Y-%m-%d %H:%M:%S")
                    return float(calendar.timegm(parsed))
                except Exception:
                    return None
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None
    if ts > 1_000_000_000_000:
        ts = ts / 1000.0
    return ts


def _timestamp_to_iso(value):
    ts = _to_epoch_seconds(value)
    if ts is None:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def _sha256_text(value):
    if value is None:
        return None
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def _signal_filters(since_id=None, until_id=None):
    clauses = []
    params = []
    if since_id is not None:
        clauses.append("id >= ?")
        params.append(since_id)
    if until_id is not None:
        clauses.append("id <= ?")
        params.append(until_id)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    return where, params


def iter_premium_signal_rows(db, *, since_id=None, until_id=None, limit=None, table="premium_signals"):
    if not _table_exists(db, table):
        return
    where, params = _signal_filters(since_id=since_id, until_id=until_id)
    sql = f"SELECT * FROM {table}{where} ORDER BY id"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    yield from db.execute(sql, params)


def _signal_payload(row, *, default_chain="solana"):
    gate_result = _parse_json_object(row.get("gate_result"))
    signal_links = _parse_json_object(row.get("signal_links_json"))
    narrative_features = _parse_json_object(row.get("narrative_features_json"))
    raw_message = _value(row, "raw_message", "message_text", "description")
    token_ca = _value(row, "token_ca", "ca", "contract_address")
    source_message_ts = _value(row, "source_message_ts", "message_ts", "timestamp")
    receive_ts = _value(row, "receive_ts", "created_at", "timestamp")
    backfilled = bool(gate_result.get("backfilled") is True)
    parse_status = str(_value(row, "parse_status") or "").strip().lower()
    realtime_observable = bool(token_ca and receive_ts is not None and parse_status in {"", "parsed", "ok"} and not backfilled)
    return {
        "telegram_signal_id": row.get("id"),
        "remote_signal_id": _value(row, "remote_signal_id"),
        "source_event_id": _value(row, "source_event_id"),
        "token_ca": token_ca,
        "symbol": _value(row, "symbol"),
        "chain": _value(row, "chain") or default_chain,
        "canonical_pool_group": _value(row, "canonical_pool_group", "pool_group") or "unknown_pool",
        "lifecycle_epoch": _value(row, "lifecycle_epoch") or 0,
        "telegram_seen": bool(token_ca),
        "realtime_observable": realtime_observable,
        "realtime_observable_quality": "backfilled" if backfilled else "realtime_seed" if realtime_observable else "unproven",
        "signal_type": _value(row, "signal_type"),
        "is_ath": _value(row, "is_ath"),
        "source_message_ts": source_message_ts,
        "receive_ts": receive_ts,
        "signal_source": _value(row, "signal_source"),
        "source_registry_status": "legacy_unregistered",
        "parse_status": _value(row, "parse_status"),
        "parse_missing_fields": _value(row, "parse_missing_fields"),
        "hard_gate_status": _value(row, "hard_gate_status"),
        "ai_action": _value(row, "ai_action"),
        "ai_confidence": _value(row, "ai_confidence"),
        "market_cap": _value(row, "market_cap"),
        "holders": _value(row, "holders"),
        "volume_24h": _value(row, "volume_24h"),
        "top10_pct": _value(row, "top10_pct"),
        "raw_message_hash": _sha256_text(raw_message),
        "raw_message_length": len(str(raw_message)) if raw_message is not None else 0,
        "gate_result": gate_result,
        "signal_links": signal_links,
        "narrative_features": narrative_features,
        "legacy_premium_signal": row,
    }


def mirror_premium_signals(
    signal_db,
    event_log_dir,
    *,
    since_id=None,
    until_id=None,
    limit=None,
    dry_run=False,
    table="premium_signals",
    default_chain="solana",
):
    summary = {
        "signal_db": str(signal_db),
        "event_log_dir": str(event_log_dir),
        "table": table,
        "read_rows": 0,
        "appended": 0,
        "duplicate": 0,
        "failed": 0,
        "dry_run": bool(dry_run),
        "failures": [],
    }
    with _connect(signal_db) as db:
        if not _table_exists(db, table):
            summary["failed"] += 1
            summary["failures"].append({"reason": f"{table} table missing"})
            return summary
        for row in iter_premium_signal_rows(db, since_id=since_id, until_id=until_id, limit=limit, table=table):
            row = _row_dict(row)
            summary["read_rows"] += 1
            if dry_run:
                continue
            try:
                payload = _signal_payload(row, default_chain=default_chain)
                token_ca = payload.get("token_ca")
                aggregate_id = f"telegram_signal:{payload.get('chain')}:{token_ca}:unknown_pool:0" if token_ca else f"telegram_signal:row:{row.get('id')}"
                result = V27EventLog(event_log_dir).append_event(
                    event_type=SIGNAL_EVENT_TYPE,
                    aggregate_id=aggregate_id,
                    payload=payload,
                    source="premium_signals",
                    idempotency_key=f"premium_signals:{row.get('id')}",
                    observed_at=_timestamp_to_iso(payload.get("source_message_ts")),
                    available_at=_timestamp_to_iso(payload.get("receive_ts")),
                )
            except Exception as exc:
                summary["failed"] += 1
                summary["failures"].append({"premium_signal_id": row.get("id"), "reason": str(exc)})
                continue
            status = result.get("status")
            if status == "appended":
                summary["appended"] += 1
            elif status == "duplicate":
                summary["duplicate"] += 1
            else:
                summary["failed"] += 1
                summary["failures"].append({"premium_signal_id": row.get("id"), "reason": f"unexpected status {status}"})
    return summary


def _signal_ids_from_db(db, *, since_id=None, until_id=None, limit=None, table="premium_signals"):
    return [row["id"] for row in iter_premium_signal_rows(db, since_id=since_id, until_id=until_id, limit=limit, table=table)]


def _mirrored_signal_ids(event_log_dir):
    counts = {}
    if not (Path(event_log_dir) / "events.jsonl").exists():
        return counts
    for event in V27EventLog(event_log_dir).iter_events():
        if event.get("event_type") != SIGNAL_EVENT_TYPE:
            continue
        payload = event.get("payload") or {}
        signal_id = payload.get("telegram_signal_id")
        if signal_id is None:
            continue
        counts[int(signal_id)] = counts.get(int(signal_id), 0) + 1
    return counts


def verify_signal_mirror_parity(signal_db, event_log_dir, *, since_id=None, until_id=None, limit=None, table="premium_signals"):
    summary = {
        "signal_db": str(signal_db),
        "event_log_dir": str(event_log_dir),
        "table": table,
        "db_rows": 0,
        "mirrored_events": 0,
        "missing_signal_ids": [],
        "duplicate_signal_ids": [],
        "orphan_mirrored_signal_ids": [],
        "event_log_verify": None,
        "event_log_error": None,
        "parity_ok": False,
    }
    with _connect(signal_db) as db:
        if not _table_exists(db, table):
            summary["event_log_error"] = f"{table} table missing"
            return summary
        db_ids = _signal_ids_from_db(db, since_id=since_id, until_id=until_id, limit=limit, table=table)

    mirrored_counts = _mirrored_signal_ids(event_log_dir)
    db_id_set = set(db_ids)
    mirrored_id_set = set(mirrored_counts)

    summary["db_rows"] = len(db_ids)
    summary["mirrored_events"] = sum(mirrored_counts.values())
    summary["missing_signal_ids"] = sorted(db_id_set - mirrored_id_set)
    summary["duplicate_signal_ids"] = sorted([signal_id for signal_id, count in mirrored_counts.items() if count > 1])
    summary["orphan_mirrored_signal_ids"] = sorted(mirrored_id_set - db_id_set)
    try:
        summary["event_log_verify"] = V27EventLog(event_log_dir).verify()
    except V27EventLogError as exc:
        summary["event_log_error"] = str(exc)

    summary["parity_ok"] = not summary["missing_signal_ids"] and not summary["duplicate_signal_ids"] and summary["event_log_error"] is None
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--signal-db", default=str(DEFAULT_SIGNAL_DB))
    parser.add_argument("--event-log-dir", default=str(DEFAULT_EVENT_LOG_DIR))
    parser.add_argument("--table", default="premium_signals")
    parser.add_argument("--default-chain", default="solana")
    parser.add_argument("--since-id", type=int)
    parser.add_argument("--until-id", type=int)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()

    signal_db = Path(args.signal_db)
    event_log_dir = Path(args.event_log_dir)

    if args.verify_only:
        summary = verify_signal_mirror_parity(
            signal_db,
            event_log_dir,
            since_id=args.since_id,
            until_id=args.until_id,
            limit=args.limit,
            table=args.table,
        )
        print(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2))
        raise SystemExit(0 if summary["parity_ok"] else 1)

    mirror_summary = mirror_premium_signals(
        signal_db,
        event_log_dir,
        since_id=args.since_id,
        until_id=args.until_id,
        limit=args.limit,
        dry_run=args.dry_run,
        table=args.table,
        default_chain=args.default_chain,
    )
    verify_summary = None
    if not args.dry_run:
        verify_summary = verify_signal_mirror_parity(
            signal_db,
            event_log_dir,
            since_id=args.since_id,
            until_id=args.until_id,
            limit=args.limit,
            table=args.table,
        )
    result = {"mirror": mirror_summary, "verify": verify_summary}
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    if mirror_summary["failed"] or (verify_summary and not verify_summary["parity_ok"]):
        raise SystemExit(1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Backfill and verify legacy paper_decision_events in the v2.7 event log."""

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from paper_decision_audit import _mirror_v27_decision_event  # noqa: E402
from v27_event_log import V27EventLog, V27EventLogError  # noqa: E402


DEFAULT_DB = PROJECT_ROOT / "data" / "paper_trades.db"
DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
MIRRORED_EVENT_TYPE = "paper_decision_event_recorded"
MIRRORED_MISSED_EVENT_TYPE = "paper_missed_signal_attribution_recorded"


def _table_exists(db, table):
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _parse_json_object(value, *, field_name):
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except Exception as exc:
        return {
            "_raw_json": value,
            "_json_parse_error": str(exc),
            "_json_field": field_name,
        }
    if isinstance(parsed, dict):
        return parsed
    return {
        "_raw_json": value,
        "_json_parse_error": "json value is not an object",
        "_json_field": field_name,
    }


def _connect(db_path):
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    return db


def _row_dict(row):
    return {key: row[key] for key in row.keys()}


def _event_ts_to_iso(value):
    if value is None:
        return None
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None
    if ts > 1_000_000_000_000:
        ts = ts / 1000.0
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def _as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _row_filters(since_id=None, until_id=None):
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


def iter_decision_rows(db, *, since_id=None, until_id=None, limit=None):
    if not _table_exists(db, "paper_decision_events"):
        raise RuntimeError("paper_decision_events table missing")
    where, params = _row_filters(since_id=since_id, until_id=until_id)
    sql = f"SELECT * FROM paper_decision_events{where} ORDER BY id"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    yield from db.execute(sql, params)


def iter_missed_rows(db, *, since_id=None, until_id=None, limit=None):
    if not _table_exists(db, "paper_missed_signal_attribution"):
        return
    where, params = _row_filters(since_id=since_id, until_id=until_id)
    sql = f"SELECT * FROM paper_missed_signal_attribution{where} ORDER BY id"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    yield from db.execute(sql, params)


def _row_to_mirror_kwargs(row):
    lifecycle_features = _parse_json_object(row["lifecycle_features_json"], field_name="lifecycle_features_json")
    payload = _parse_json_object(row["payload_json"], field_name="payload_json")
    lifecycle = {
        "lifecycle_state": row["lifecycle_state"],
        "vitality_score": row["vitality_score"],
        "entry_bias": row["entry_bias"],
        "lifecycle_features": lifecycle_features,
    }
    return {
        "decision_event_id": row["id"],
        "event_ts": row["event_ts"],
        "signal_id": row["signal_id"],
        "token_ca": row["token_ca"],
        "symbol": row["symbol"],
        "lifecycle_id": row["lifecycle_id"],
        "trade_id": row["trade_id"],
        "signal_ts": row["signal_ts"],
        "strategy_stage": row["strategy_stage"],
        "route": row["route"],
        "component": row["component"],
        "event_type": row["event_type"],
        "decision": row["decision"],
        "reason": row["reason"],
        "data_source": row["data_source"],
        "payload": payload,
        "lifecycle": lifecycle,
    }


def mirror_paper_decisions(db_path, event_log_dir, *, since_id=None, until_id=None, limit=None, dry_run=False):
    summary = {
        "db_path": str(db_path),
        "event_log_dir": str(event_log_dir),
        "read_rows": 0,
        "appended": 0,
        "duplicate": 0,
        "failed": 0,
        "dry_run": bool(dry_run),
        "failures": [],
    }
    with _connect(db_path) as db:
        for row in iter_decision_rows(db, since_id=since_id, until_id=until_id, limit=limit):
            summary["read_rows"] += 1
            if dry_run:
                continue
            result = _mirror_v27_decision_event(
                **_row_to_mirror_kwargs(row),
                event_log_dir=str(event_log_dir),
                enabled=True,
            )
            if not result:
                summary["failed"] += 1
                summary["failures"].append({"decision_event_id": row["id"], "reason": "mirror returned no result"})
                continue
            status = result.get("status")
            if status == "appended":
                summary["appended"] += 1
            elif status == "duplicate":
                summary["duplicate"] += 1
            else:
                summary["failed"] += 1
                summary["failures"].append({"decision_event_id": row["id"], "reason": f"unexpected status {status}"})
    return summary


def _first_numeric(row, field_names):
    for field in field_names:
        if field not in row:
            continue
        value = _as_float(row.get(field))
        if value is not None:
            return value, field
    return None, None


def _source_label_from_peak(peak):
    if peak is None:
        return None
    if peak >= 1.0:
        return "gold"
    if peak >= 0.5:
        return "silver"
    if peak >= 0.3:
        return "copper"
    return "none"


def _missed_source_label(row):
    peak, peak_field = _first_numeric(
        row,
        [
            "executable_peak_pnl",
            "tradable_peak_pnl",
            "max_pnl_recorded",
            "pnl_24h",
            "pnl_60m",
            "pnl_15m",
            "pnl_5m",
        ],
    )
    if _as_bool(row.get("would_stop_before_peak")) is True:
        return None, peak, peak_field, "legacy_stop_before_peak"
    if peak is None:
        return None, peak, peak_field, "legacy_peak_missing"
    return _source_label_from_peak(peak), peak, peak_field, "legacy_missed_attribution_seed"


def _missed_payload(row):
    label, peak, peak_field, quality = _missed_source_label(row)
    baseline_price = _as_float(row.get("baseline_price"))
    telegram_seen = bool(row.get("signal_id") is not None or row.get("signal_ts") is not None)
    realtime_observable = baseline_price is not None and baseline_price > 0
    return {
        "missed_attribution_id": row.get("id"),
        "decision_event_id": row.get("decision_event_id"),
        "event_ts": row.get("created_event_ts"),
        "token_ca": row.get("token_ca"),
        "symbol": row.get("symbol"),
        "lifecycle_id": row.get("lifecycle_id"),
        "signal_id": row.get("signal_id"),
        "signal_ts": row.get("signal_ts"),
        "route": row.get("route"),
        "component": row.get("component"),
        "legacy_event_type": "missed_signal_attribution",
        "decision": row.get("decision"),
        "reason": row.get("reject_reason"),
        "source_dog_label": label,
        "source_dog_label_version": "legacy_missed_attribution_seed_v0.1",
        "source_label_quality": quality,
        "source_label_research_only": True,
        "source_label_peak": peak,
        "source_label_peak_field": peak_field,
        "telegram_seen": telegram_seen,
        "realtime_observable": realtime_observable,
        "baseline_price": baseline_price,
        "baseline_source": row.get("baseline_source"),
        "baseline_ts": row.get("baseline_ts"),
        "tradable_missed": row.get("tradable_missed"),
        "tradability_status": row.get("tradability_status"),
        "would_stop_before_peak": row.get("would_stop_before_peak"),
        "legacy_missed_attribution": row,
    }


def mirror_missed_attributions(db_path, event_log_dir, *, since_id=None, until_id=None, limit=None, dry_run=False):
    summary = {
        "db_path": str(db_path),
        "event_log_dir": str(event_log_dir),
        "read_rows": 0,
        "appended": 0,
        "duplicate": 0,
        "failed": 0,
        "dry_run": bool(dry_run),
        "failures": [],
    }
    with _connect(db_path) as db:
        for row in iter_missed_rows(db, since_id=since_id, until_id=until_id, limit=limit):
            row = _row_dict(row)
            summary["read_rows"] += 1
            if dry_run:
                continue
            try:
                event_log = V27EventLog(event_log_dir)
                token_ca = row.get("token_ca")
                aggregate_id = f"paper_missed:token:{token_ca}" if token_ca else f"paper_missed:row:{row.get('id')}"
                result = event_log.append_event(
                    event_type=MIRRORED_MISSED_EVENT_TYPE,
                    aggregate_id=aggregate_id,
                    payload=_missed_payload(row),
                    source="paper_missed_signal_attribution",
                    idempotency_key=f"paper_missed_signal_attribution:{row.get('id')}",
                    observed_at=_event_ts_to_iso(row.get("created_event_ts")),
                    available_at=_event_ts_to_iso(row.get("created_event_ts")),
                )
            except Exception as exc:
                summary["failed"] += 1
                summary["failures"].append({"missed_attribution_id": row.get("id"), "reason": str(exc)})
                continue
            status = result.get("status")
            if status == "appended":
                summary["appended"] += 1
            elif status == "duplicate":
                summary["duplicate"] += 1
            else:
                summary["failed"] += 1
                summary["failures"].append({"missed_attribution_id": row.get("id"), "reason": f"unexpected status {status}"})
    return summary


def _decision_ids_from_db(db, *, since_id=None, until_id=None, limit=None):
    return [row["id"] for row in iter_decision_rows(db, since_id=since_id, until_id=until_id, limit=limit)]


def _mirrored_decision_ids(event_log_dir):
    counts = {}
    if not (Path(event_log_dir) / "events.jsonl").exists():
        return counts
    for event in V27EventLog(event_log_dir).iter_events():
        if event.get("event_type") != MIRRORED_EVENT_TYPE:
            continue
        payload = event.get("payload") or {}
        decision_event_id = payload.get("decision_event_id")
        if decision_event_id is None:
            continue
        counts[int(decision_event_id)] = counts.get(int(decision_event_id), 0) + 1
    return counts


def _mirrored_missed_ids(event_log_dir):
    counts = {}
    if not (Path(event_log_dir) / "events.jsonl").exists():
        return counts
    for event in V27EventLog(event_log_dir).iter_events():
        if event.get("event_type") != MIRRORED_MISSED_EVENT_TYPE:
            continue
        payload = event.get("payload") or {}
        missed_attribution_id = payload.get("missed_attribution_id")
        if missed_attribution_id is None:
            continue
        counts[int(missed_attribution_id)] = counts.get(int(missed_attribution_id), 0) + 1
    return counts


def verify_mirror_parity(db_path, event_log_dir, *, since_id=None, until_id=None, limit=None):
    summary = {
        "db_path": str(db_path),
        "event_log_dir": str(event_log_dir),
        "db_rows": 0,
        "mirrored_events": 0,
        "missing_decision_event_ids": [],
        "duplicate_decision_event_ids": [],
        "orphan_mirrored_decision_event_ids": [],
        "event_log_verify": None,
        "event_log_error": None,
        "parity_ok": False,
    }
    with _connect(db_path) as db:
        db_ids = _decision_ids_from_db(db, since_id=since_id, until_id=until_id, limit=limit)

    mirrored_counts = _mirrored_decision_ids(event_log_dir)
    db_id_set = set(db_ids)
    mirrored_id_set = set(mirrored_counts)

    summary["db_rows"] = len(db_ids)
    summary["mirrored_events"] = sum(mirrored_counts.values())
    summary["missing_decision_event_ids"] = sorted(db_id_set - mirrored_id_set)
    summary["duplicate_decision_event_ids"] = sorted([event_id for event_id, count in mirrored_counts.items() if count > 1])
    summary["orphan_mirrored_decision_event_ids"] = sorted(mirrored_id_set - db_id_set)
    try:
        summary["event_log_verify"] = V27EventLog(event_log_dir).verify()
    except V27EventLogError as exc:
        summary["event_log_error"] = str(exc)

    summary["parity_ok"] = (
        not summary["missing_decision_event_ids"]
        and not summary["duplicate_decision_event_ids"]
        and summary["event_log_error"] is None
    )
    return summary


def verify_missed_mirror_parity(db_path, event_log_dir, *, since_id=None, until_id=None, limit=None):
    summary = {
        "db_path": str(db_path),
        "event_log_dir": str(event_log_dir),
        "db_rows": 0,
        "mirrored_events": 0,
        "missing_missed_attribution_ids": [],
        "duplicate_missed_attribution_ids": [],
        "orphan_mirrored_missed_attribution_ids": [],
        "event_log_verify": None,
        "event_log_error": None,
        "parity_ok": False,
    }
    with _connect(db_path) as db:
        db_ids = [row["id"] for row in iter_missed_rows(db, since_id=since_id, until_id=until_id, limit=limit)]

    mirrored_counts = _mirrored_missed_ids(event_log_dir)
    db_id_set = set(db_ids)
    mirrored_id_set = set(mirrored_counts)

    summary["db_rows"] = len(db_ids)
    summary["mirrored_events"] = sum(mirrored_counts.values())
    summary["missing_missed_attribution_ids"] = sorted(db_id_set - mirrored_id_set)
    summary["duplicate_missed_attribution_ids"] = sorted([event_id for event_id, count in mirrored_counts.items() if count > 1])
    summary["orphan_mirrored_missed_attribution_ids"] = sorted(mirrored_id_set - db_id_set)
    try:
        summary["event_log_verify"] = V27EventLog(event_log_dir).verify()
    except V27EventLogError as exc:
        summary["event_log_error"] = str(exc)

    summary["parity_ok"] = (
        not summary["missing_missed_attribution_ids"]
        and not summary["duplicate_missed_attribution_ids"]
        and summary["event_log_error"] is None
    )
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--event-log-dir", default=str(DEFAULT_EVENT_LOG_DIR))
    parser.add_argument("--since-id", type=int)
    parser.add_argument("--until-id", type=int)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--include-missed", action="store_true")
    args = parser.parse_args()

    db_path = Path(args.db)
    event_log_dir = Path(args.event_log_dir)

    if args.verify_only:
        summary = verify_mirror_parity(
            db_path,
            event_log_dir,
            since_id=args.since_id,
            until_id=args.until_id,
            limit=args.limit,
        )
        result = {"decisions": summary}
        if args.include_missed:
            result["missed"] = verify_missed_mirror_parity(
                db_path,
                event_log_dir,
                since_id=args.since_id,
                until_id=args.until_id,
                limit=args.limit,
            )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
        ok = summary["parity_ok"] and (not args.include_missed or result["missed"]["parity_ok"])
        raise SystemExit(0 if ok else 1)

    mirror_summary = mirror_paper_decisions(
        db_path,
        event_log_dir,
        since_id=args.since_id,
        until_id=args.until_id,
        limit=args.limit,
        dry_run=args.dry_run,
    )
    verify_summary = None
    missed_summary = None
    missed_verify_summary = None
    if not args.dry_run:
        verify_summary = verify_mirror_parity(
            db_path,
            event_log_dir,
            since_id=args.since_id,
            until_id=args.until_id,
            limit=args.limit,
        )
    if args.include_missed:
        missed_summary = mirror_missed_attributions(
            db_path,
            event_log_dir,
            since_id=args.since_id,
            until_id=args.until_id,
            limit=args.limit,
            dry_run=args.dry_run,
        )
        if not args.dry_run:
            missed_verify_summary = verify_missed_mirror_parity(
                db_path,
                event_log_dir,
                since_id=args.since_id,
                until_id=args.until_id,
                limit=args.limit,
            )
    result = {
        "mirror": mirror_summary,
        "verify": verify_summary,
        "missed_mirror": missed_summary,
        "missed_verify": missed_verify_summary,
    }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    if (
        mirror_summary["failed"]
        or (verify_summary and not verify_summary["parity_ok"])
        or (missed_summary and missed_summary["failed"])
        or (missed_verify_summary and not missed_verify_summary["parity_ok"])
    ):
        raise SystemExit(1)


if __name__ == "__main__":
    main()

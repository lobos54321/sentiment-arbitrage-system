#!/usr/bin/env python3
"""Mirror strategy experiment randomness-control evidence into v2.7 events."""

import argparse
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

from v27_event_log import V27EventLog, V27EventLogError, sha256_hex  # noqa: E402


DEFAULT_DB = PROJECT_ROOT / "data" / "sentiment_arb.db"
DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
DEFAULT_LOCK_FILE = Path("/tmp/v27_randomness_control_mirror.lock")
RANDOMNESS_CONTROL_EVENT_TYPE = "randomness_control_recorded"
DEFAULT_AUDIT_VERSION = "legacy_strategy_experiment_randomness_control_v0.1"
DEFAULT_RANDOMIZATION_UNIT = "strategy_experiment_candidate"
DEFAULT_ENVIRONMENT_ID = os.environ.get("V27_ENVIRONMENT_ID") or os.environ.get("NODE_ENV") or "local"
REQUIRED_COLUMNS = {
    "candidate_id",
    "status",
    "created_at",
    "created_by",
    "config_version",
}
JSON_COLUMNS = [
    "mutation_set_json",
    "dataset_refs_json",
    "metrics_json",
    "guardrail_results_json",
    "strategy_config_json",
    "notes",
]
RNG_SEED_KEYS = {
    "rng_seed",
    "rngseed",
    "random_seed",
    "randomseed",
    "randomness_seed",
    "randomnessseed",
    "rng_seed_hash",
    "rngseedhash",
}
RNG_VERSION_KEYS = {
    "rng_version",
    "rngversion",
    "randomness_version",
    "randomnessversion",
    "randomization_version",
    "randomizationversion",
}
RANDOMIZATION_UNIT_KEYS = {
    "randomization_unit",
    "randomizationunit",
}
ASSIGNMENT_ID_KEYS = {
    "assignment_id",
    "assignmentid",
    "experiment_assignment_id",
    "experimentassignmentid",
}
ASSIGNMENT_ALGORITHM_KEYS = {
    "assignment_algorithm",
    "assignmentalgorithm",
    "randomization_algorithm",
    "randomizationalgorithm",
    "rng_algorithm",
    "rngalgorithm",
}
ASSIGNED_BUCKET_KEYS = {
    "assigned_bucket",
    "assignedbucket",
    "experiment_bucket",
    "experimentbucket",
    "bucket",
}
RANDOMIZED_CREATORS = {
    "autoresearch-loop",
    "challenger-generator",
}


def _connect(db_path):
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    return db


def _table_exists(db, table):
    row = db.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)).fetchone()
    return row is not None


def _table_columns(db, table):
    if not _table_exists(db, table):
        return set()
    return {row["name"] for row in db.execute(f"PRAGMA table_info({table})")}


def _schema_error(db, table):
    if not _table_exists(db, table):
        return f"{table} table missing"
    missing = sorted(REQUIRED_COLUMNS - _table_columns(db, table))
    if missing:
        return f"{table} missing required columns: {', '.join(missing)}"
    return None


def _row_dict(row):
    return {key: row[key] for key in row.keys()}


def _text(value, default=None):
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _normalize_key(key):
    return "".join(ch for ch in str(key).lower() if ch.isalnum())


def _parse_json(value):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    text = str(value).strip()
    if not text:
        return None
    if not (text.startswith("{") or text.startswith("[")):
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _is_scalar(value):
    return value is None or isinstance(value, (str, int, float, bool))


def _find_nested_value(value, wanted_keys):
    if isinstance(value, dict):
        for key, child in value.items():
            if _normalize_key(key) in wanted_keys and _is_scalar(child):
                return child
        for child in value.values():
            found = _find_nested_value(child, wanted_keys)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _find_nested_value(child, wanted_keys)
            if found is not None:
                return found
    return None


def _json_material(row):
    material = {}
    for column in JSON_COLUMNS:
        parsed = _parse_json(row.get(column))
        if parsed is not None:
            material[column] = parsed
    return material


def _row_or_json_value(row, row_keys, json_keys, json_material):
    for key in row_keys:
        value = _text(row.get(key)) if key in row else None
        if value:
            return value
    return _text(_find_nested_value(json_material, json_keys))


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


def _timestamp_to_iso(value):
    text = _text(value)
    if not text:
        return None
    if "T" in text:
        return text
    try:
        numeric = float(text)
    except ValueError:
        return text
    if numeric > 10_000_000_000:
        numeric = numeric / 1000.0
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(numeric))


def _row_filters(statuses=None, since_created_at=None, until_created_at=None):
    clauses = []
    params = []
    if statuses:
        placeholders = ",".join("?" for _ in statuses)
        clauses.append(f"status IN ({placeholders})")
        params.extend(statuses)
    if since_created_at is not None:
        clauses.append("created_at >= ?")
        params.append(since_created_at)
    if until_created_at is not None:
        clauses.append("created_at <= ?")
        params.append(until_created_at)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    return where, params


def iter_strategy_experiment_rows(db, *, statuses=None, since_created_at=None, until_created_at=None, limit=None, table="strategy_experiments"):
    if _schema_error(db, table):
        return
    where, params = _row_filters(statuses=statuses, since_created_at=since_created_at, until_created_at=until_created_at)
    sql = f"SELECT * FROM {table}{where} ORDER BY created_at, candidate_id"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    yield from db.execute(sql, params)


def _source_row_reference(row, *, table):
    material = dict(row or {})
    return {
        "source_table": table,
        "source_row_id": row.get("candidate_id"),
        "source_row_hash": sha256_hex(material),
        "source_row_field_count": len(material),
    }


def _infer_randomization_enabled(row):
    explicit = _as_bool(row.get("randomization_enabled")) if "randomization_enabled" in row else None
    if explicit is not None:
        return explicit
    creator = _text(row.get("created_by"))
    if creator in RANDOMIZED_CREATORS:
        return True
    candidate_id = _text(row.get("candidate_id"), "")
    if candidate_id.startswith("candidate-"):
        return True
    return None


def _randomness_payload(
    row,
    *,
    audit_version=DEFAULT_AUDIT_VERSION,
    default_randomization_unit=DEFAULT_RANDOMIZATION_UNIT,
    environment_id=DEFAULT_ENVIRONMENT_ID,
    table="strategy_experiments",
):
    json_material = _json_material(row)
    assignment_id = (
        _row_or_json_value(row, ["assignment_id"], ASSIGNMENT_ID_KEYS, json_material)
        or _text(row.get("candidate_id"))
    )
    randomization_unit = (
        _row_or_json_value(row, ["randomization_unit"], RANDOMIZATION_UNIT_KEYS, json_material)
        or default_randomization_unit
    )
    rng_seed = _row_or_json_value(row, ["rng_seed", "random_seed"], RNG_SEED_KEYS, json_material)
    rng_version = _row_or_json_value(row, ["rng_version", "randomness_version"], RNG_VERSION_KEYS, json_material)
    assignment_algorithm = _row_or_json_value(
        row,
        ["assignment_algorithm", "randomization_algorithm", "rng_algorithm"],
        ASSIGNMENT_ALGORITHM_KEYS,
        json_material,
    )
    assigned_bucket = _row_or_json_value(row, ["assigned_bucket", "experiment_bucket"], ASSIGNED_BUCKET_KEYS, json_material)
    source_ref = _source_row_reference(row, table=table)
    assignment_hash = sha256_hex(
        {
            "assignment_id": assignment_id,
            "randomization_unit": randomization_unit,
            "rng_seed": rng_seed,
            "rng_version": rng_version,
            "source_row_hash": source_ref["source_row_hash"],
        }
    )
    randomization_enabled = _infer_randomization_enabled(row)
    payload = {
        "randomness_control_audit_version": audit_version,
        "rng_seed": rng_seed,
        "rng_version": rng_version,
        "randomization_unit": randomization_unit,
        "assignment_id": assignment_id,
        "assignment_status": _text(row.get("status")),
        "randomization_enabled": randomization_enabled,
        "deterministic_assignment": False if randomization_enabled is True else None,
        "assignment_algorithm": assignment_algorithm,
        "assigned_bucket": assigned_bucket,
        "assignment_hash": assignment_hash,
        "evidence_source": table,
        "environment_id": environment_id,
        "candidate_id": _text(row.get("candidate_id")),
        "created_by": _text(row.get("created_by")),
        "config_version": row.get("config_version"),
        "created_at": _text(row.get("created_at")),
        "qualified_at": _text(row.get("qualified_at")),
        "activated_at": _text(row.get("activated_at")),
        "promoted_at": _text(row.get("promoted_at")),
        "retired_at": _text(row.get("retired_at")),
        "paused_at": _text(row.get("paused_at")),
        "decision_available_at": _timestamp_to_iso(row.get("created_at")),
        "source_row_reference": source_ref,
        "source_row_hash": source_ref["source_row_hash"],
        "randomness_control_proof_level": (
            "explicit_rng_control_material"
            if rng_seed and rng_version
            else "strategy_experiment_without_explicit_rng_control"
        ),
    }
    return payload


def _missing_required_fields(payload):
    missing = []
    for field in ("rng_seed", "rng_version", "randomization_unit", "assignment_id"):
        value = payload.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing.append(field)
    return missing


def _fingerprint(payload):
    return f"{payload.get('candidate_id')}:{payload.get('source_row_hash')}"


def _event_idempotency_key(payload):
    return f"randomness_control:{_fingerprint(payload)}:{payload.get('randomness_control_audit_version')}"


def _aggregate_id(payload):
    return f"randomness_control:{payload.get('assignment_id') or payload.get('candidate_id') or 'unknown_assignment'}"


def _split_csv(values):
    result = []
    for value in values or []:
        for item in str(value).split(","):
            text = item.strip()
            if text:
                result.append(text)
    return result


def _mirrored_randomness_control_fingerprints(event_log_dir, *, audit_version=None):
    counts = {}
    if not (Path(event_log_dir) / "events.jsonl").exists():
        return counts
    for event in V27EventLog(event_log_dir).iter_events():
        if event.get("event_type") != RANDOMNESS_CONTROL_EVENT_TYPE:
            continue
        payload = event.get("payload") or {}
        if audit_version and payload.get("randomness_control_audit_version") != audit_version:
            continue
        if not payload.get("candidate_id") or not payload.get("source_row_hash"):
            continue
        key = _fingerprint(payload)
        counts[key] = counts.get(key, 0) + 1
    return counts


def mirror_randomness_controls(
    db_path,
    event_log_dir,
    *,
    statuses=None,
    since_created_at=None,
    until_created_at=None,
    limit=None,
    dry_run=False,
    new_only=False,
    table="strategy_experiments",
    audit_version=DEFAULT_AUDIT_VERSION,
    default_randomization_unit=DEFAULT_RANDOMIZATION_UNIT,
    environment_id=DEFAULT_ENVIRONMENT_ID,
):
    statuses = _split_csv(statuses)
    summary = {
        "db": str(db_path),
        "event_log_dir": str(event_log_dir),
        "table": table,
        "read_rows": 0,
        "eligible_randomness_controls": 0,
        "valid_randomness_controls": 0,
        "malformed_randomness_controls": 0,
        "unchanged": 0,
        "appended": 0,
        "duplicate": 0,
        "failed": 0,
        "failures": [],
        "dry_run": bool(dry_run),
        "new_only": bool(new_only),
        "statuses": statuses,
        "randomness_control_audit_version": audit_version,
    }
    mirrored = _mirrored_randomness_control_fingerprints(event_log_dir, audit_version=audit_version) if new_only else {}
    append_budget = limit if new_only else None
    with _connect(db_path) as db:
        schema_error = _schema_error(db, table)
        if schema_error:
            summary["failed"] += 1
            summary["failures"].append({"reason": schema_error})
            return summary
        for row in iter_strategy_experiment_rows(
            db,
            statuses=statuses,
            since_created_at=since_created_at,
            until_created_at=until_created_at,
            limit=None if new_only else limit,
            table=table,
        ):
            row = _row_dict(row)
            summary["read_rows"] += 1
            try:
                payload = _randomness_payload(
                    row,
                    audit_version=audit_version,
                    default_randomization_unit=default_randomization_unit,
                    environment_id=environment_id,
                    table=table,
                )
                if new_only and _fingerprint(payload) in mirrored:
                    summary["unchanged"] += 1
                    continue
                if append_budget is not None and append_budget <= 0:
                    break
                summary["eligible_randomness_controls"] += 1
                missing = _missing_required_fields(payload)
                if missing:
                    summary["malformed_randomness_controls"] += 1
                else:
                    summary["valid_randomness_controls"] += 1
                if dry_run:
                    if append_budget is not None:
                        append_budget -= 1
                    continue
                result = V27EventLog(event_log_dir).append_event(
                    event_type=RANDOMNESS_CONTROL_EVENT_TYPE,
                    aggregate_id=_aggregate_id(payload),
                    payload=payload,
                    source=table,
                    idempotency_key=_event_idempotency_key(payload),
                    observed_at=payload.get("decision_available_at"),
                    available_at=payload.get("decision_available_at"),
                )
            except Exception as exc:
                summary["failed"] += 1
                summary["failures"].append({"candidate_id": row.get("candidate_id"), "reason": str(exc)})
                continue
            status = result.get("status")
            if status == "appended":
                summary["appended"] += 1
                if append_budget is not None:
                    append_budget -= 1
                mirrored[_fingerprint(payload)] = mirrored.get(_fingerprint(payload), 0) + 1
            elif status == "duplicate":
                summary["duplicate"] += 1
                if append_budget is not None:
                    append_budget -= 1
            else:
                summary["failed"] += 1
                summary["failures"].append({"candidate_id": row.get("candidate_id"), "reason": f"unexpected status {status}"})
    return summary


def _current_db_fingerprints(db_path, *, statuses=None, since_created_at=None, until_created_at=None, limit=None, table="strategy_experiments", audit_version=DEFAULT_AUDIT_VERSION, default_randomization_unit=DEFAULT_RANDOMIZATION_UNIT, environment_id=DEFAULT_ENVIRONMENT_ID):
    fingerprints = []
    with _connect(db_path) as db:
        if _schema_error(db, table):
            return fingerprints
        for row in iter_strategy_experiment_rows(
            db,
            statuses=_split_csv(statuses),
            since_created_at=since_created_at,
            until_created_at=until_created_at,
            limit=limit,
            table=table,
        ):
            payload = _randomness_payload(
                _row_dict(row),
                audit_version=audit_version,
                default_randomization_unit=default_randomization_unit,
                environment_id=environment_id,
                table=table,
            )
            fingerprints.append(_fingerprint(payload))
    return fingerprints


def verify_randomness_control_mirror_parity(
    db_path,
    event_log_dir,
    *,
    statuses=None,
    since_created_at=None,
    until_created_at=None,
    limit=None,
    table="strategy_experiments",
    audit_version=DEFAULT_AUDIT_VERSION,
    default_randomization_unit=DEFAULT_RANDOMIZATION_UNIT,
    environment_id=DEFAULT_ENVIRONMENT_ID,
):
    db_fingerprints = _current_db_fingerprints(
        db_path,
        statuses=statuses,
        since_created_at=since_created_at,
        until_created_at=until_created_at,
        limit=limit,
        table=table,
        audit_version=audit_version,
        default_randomization_unit=default_randomization_unit,
        environment_id=environment_id,
    )
    mirrored_counts = _mirrored_randomness_control_fingerprints(event_log_dir, audit_version=audit_version)
    db_set = set(db_fingerprints)
    summary = {
        "db": str(db_path),
        "event_log_dir": str(event_log_dir),
        "table": table,
        "db_rows": len(db_fingerprints),
        "mirrored_current_events": sum(mirrored_counts.get(key, 0) for key in db_set),
        "missing_fingerprints": sorted(db_set - set(mirrored_counts)),
        "duplicate_fingerprints": sorted([key for key in db_set if mirrored_counts.get(key, 0) > 1]),
        "superseded_mirrored_fingerprint_count": sum(
            count for key, count in mirrored_counts.items() if key not in db_set
        ),
        "event_log_verify": None,
        "event_log_error": None,
        "parity_ok": False,
        "randomness_control_audit_version": audit_version,
    }
    try:
        summary["event_log_verify"] = V27EventLog(event_log_dir).verify()
    except V27EventLogError as exc:
        summary["event_log_error"] = str(exc)
    summary["parity_ok"] = (
        not summary["missing_fingerprints"]
        and not summary["duplicate_fingerprints"]
        and summary["event_log_error"] is None
    )
    return summary


def acquire_loop_lock(lock_file):
    lock_file = Path(lock_file)
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    fh = lock_file.open("a+", encoding="utf-8")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fh.close()
        return None
    return fh


def run_mirror_once(args):
    statuses = _split_csv(args.status)
    mirror_summary = mirror_randomness_controls(
        args.db,
        args.event_log_dir,
        statuses=statuses,
        since_created_at=args.since_created_at,
        until_created_at=args.until_created_at,
        limit=args.limit,
        dry_run=args.dry_run,
        new_only=args.new_only,
        table=args.table,
        audit_version=args.audit_version,
        default_randomization_unit=args.default_randomization_unit,
        environment_id=args.environment_id,
    )
    verify_summary = None
    if not args.dry_run:
        verify_summary = verify_randomness_control_mirror_parity(
            args.db,
            args.event_log_dir,
            statuses=statuses,
            since_created_at=args.since_created_at,
            until_created_at=args.until_created_at,
            limit=args.limit,
            table=args.table,
            audit_version=args.audit_version,
            default_randomization_unit=args.default_randomization_unit,
            environment_id=args.environment_id,
        )
    return {
        "cursor": {
            "new_only": bool(args.new_only),
            "since_created_at": args.since_created_at,
            "until_created_at": args.until_created_at,
            "limit": args.limit,
            "statuses": statuses,
        },
        "mirror": mirror_summary,
        "verify": verify_summary,
    }


def _install_signal_handlers(stop):
    def _handler(_signum, _frame):
        stop["stop"] = True

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--event-log-dir", default=str(DEFAULT_EVENT_LOG_DIR))
    parser.add_argument("--status", action="append", default=[])
    parser.add_argument("--since-created-at")
    parser.add_argument("--until-created-at")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--new-only", action="store_true")
    parser.add_argument("--table", default="strategy_experiments")
    parser.add_argument("--audit-version", default=os.environ.get("V27_RANDOMNESS_CONTROL_AUDIT_VERSION", DEFAULT_AUDIT_VERSION))
    parser.add_argument("--default-randomization-unit", default=os.environ.get("V27_RANDOMNESS_CONTROL_DEFAULT_UNIT", DEFAULT_RANDOMIZATION_UNIT))
    parser.add_argument("--environment-id", default=os.environ.get("V27_ENVIRONMENT_ID", DEFAULT_ENVIRONMENT_ID))
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument("--initial-delay", type=float, default=0.0)
    parser.add_argument("--lock-file", default=str(DEFAULT_LOCK_FILE))
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    lock_fh = acquire_loop_lock(args.lock_file)
    if lock_fh is None:
        print(json.dumps({"status": "lock_busy", "lock_file": args.lock_file}, sort_keys=True))
        return
    stop = {"stop": False}
    _install_signal_handlers(stop)
    try:
        if args.initial_delay > 0:
            time.sleep(args.initial_delay)
        while True:
            result = run_mirror_once(args)
            print(json.dumps(result, ensure_ascii=False, sort_keys=True))
            sys.stdout.flush()
            failed = result.get("mirror", {}).get("failed", 0)
            verify = result.get("verify")
            failed += 0 if verify is None or verify.get("parity_ok", True) else 1
            if args.strict and failed:
                raise SystemExit(1)
            if not args.loop or stop["stop"]:
                break
            time.sleep(max(5.0, args.interval))
    finally:
        try:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
        finally:
            lock_fh.close()


if __name__ == "__main__":
    main()

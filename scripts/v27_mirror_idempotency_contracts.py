#!/usr/bin/env python3
"""Mirror execution idempotency evidence into v2.7 events."""

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
from v27_mirror_trade_outcomes import _as_int, _signal_context, _timestamp_to_iso  # noqa: E402


DEFAULT_PAPER_DB = PROJECT_ROOT / "data" / "paper_trades.db"
DEFAULT_SIGNAL_DB = PROJECT_ROOT / "data" / "sentiment_arb.db"
DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
DEFAULT_LOCK_FILE = Path("/tmp/v27_idempotency_contract_mirror.lock")
IDEMPOTENCY_EVENT_TYPE = "idempotency_contract_recorded"
DEFAULT_CONTRACT_VERSION = "legacy_paper_entry_idempotency_v0.1"
DEFAULT_NAMESPACE = "paper_entry_execution"
DEFAULT_ENVIRONMENT_ID = os.environ.get("V27_ENVIRONMENT_ID") or os.environ.get("NODE_ENV") or "local"
DEFAULT_COLLISION_POLICY = "reject_same_namespace_key_with_different_intent_hash"
DEFAULT_HASH_ALGORITHM = "sha256(canonical_json)"
REQUIRED_PAPER_COLUMNS = {"id", "token_ca", "entry_price", "entry_ts"}


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


def _row_dict(row):
    return {key: row[key] for key in row.keys()}


def _paper_trade_schema_error(db, table):
    if not _table_exists(db, table):
        return f"{table} table missing"
    missing = sorted(REQUIRED_PAPER_COLUMNS - _table_columns(db, table))
    if missing:
        return f"{table} missing required columns: {', '.join(missing)}"
    return None


def _row_filters(since_id=None, until_id=None):
    clauses = [
        "entry_price IS NOT NULL",
        "entry_price > 0",
        "entry_ts IS NOT NULL",
    ]
    params = []
    if since_id is not None:
        clauses.append("id >= ?")
        params.append(since_id)
    if until_id is not None:
        clauses.append("id <= ?")
        params.append(until_id)
    return f" WHERE {' AND '.join(clauses)}", params


def iter_paper_trade_rows(db, *, since_id=None, until_id=None, limit=None, table="paper_trades"):
    if _paper_trade_schema_error(db, table):
        return
    where, params = _row_filters(since_id=since_id, until_id=until_id)
    sql = f"SELECT * FROM {table}{where} ORDER BY id"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    yield from db.execute(sql, params)


def _value(row, key, default=None):
    return row.get(key, default) if isinstance(row, dict) else default


def _text(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _token_lifecycle_key(row, signal_context):
    chain = signal_context.get("chain") or _value(row, "chain") or "solana"
    token_ca = _text(_value(row, "token_ca")) or "unknown_token"
    pool = signal_context.get("canonical_pool_group") or _value(row, "lifecycle_id") or "unknown_pool"
    epoch = signal_context.get("lifecycle_epoch") or _value(row, "lifecycle_epoch") or 0
    lifecycle_anchor = _value(row, "lifecycle_id") or _value(row, "premium_signal_id") or _value(row, "id")
    return f"{chain}:{token_ca}:{pool}:{epoch}:{lifecycle_anchor}"


def _route(row):
    return (
        _text(_value(row, "signal_route"))
        or _text(_value(row, "entry_mode"))
        or "legacy_unknown_route"
    )


def _idempotency_payload(
    row,
    signal_context,
    *,
    contract_version=DEFAULT_CONTRACT_VERSION,
    namespace=DEFAULT_NAMESPACE,
    environment_id=DEFAULT_ENVIRONMENT_ID,
    collision_policy=DEFAULT_COLLISION_POLICY,
    hash_algorithm=DEFAULT_HASH_ALGORITHM,
):
    paper_trade_id = _value(row, "id")
    route = _route(row)
    action = "paper_entry"
    token_lifecycle_key = _token_lifecycle_key(row, signal_context)
    decision_id = f"paper_trade:{paper_trade_id}:entry_decision"
    execution_id = f"paper_trade:{paper_trade_id}:entry_execution"
    key_material = {
        "environment_id": environment_id,
        "namespace": namespace,
        "token_lifecycle_key": token_lifecycle_key,
        "action": action,
        "route": route,
        "decision_id": decision_id,
        "execution_id": execution_id,
    }
    intent_hash = sha256_hex(key_material)
    idempotency_key = f"{environment_id}:{namespace}:{intent_hash}"
    chain = signal_context.get("chain") or _value(row, "chain") or "solana"
    pool = signal_context.get("canonical_pool_group") or _value(row, "lifecycle_id") or "unknown_pool"
    lifecycle_epoch = signal_context.get("lifecycle_epoch") or _value(row, "lifecycle_epoch") or 0
    observed_at = _timestamp_to_iso(_value(row, "entry_ts"))
    return {
        "paper_trade_id": paper_trade_id,
        "premium_signal_id": signal_context.get("premium_signal_id"),
        "telegram_signal_id": signal_context.get("telegram_signal_id"),
        "token_ca": _value(row, "token_ca"),
        "symbol": _value(row, "symbol"),
        "chain": chain,
        "canonical_pool_group": pool,
        "lifecycle_epoch": lifecycle_epoch,
        "idempotency_contract_version": contract_version,
        "decision_id": decision_id,
        "execution_id": execution_id,
        "idempotency_key": idempotency_key,
        "token_lifecycle_key": token_lifecycle_key,
        "action": action,
        "namespace": namespace,
        "environment_id": environment_id,
        "route": route,
        "hash_algorithm": hash_algorithm,
        "collision_policy": collision_policy,
        "idempotency_intent_hash": intent_hash,
        "key_material_hash": intent_hash,
        "key_material": key_material,
        "namespace_isolation_prefix": f"{environment_id}:{namespace}:",
        "cross_environment_isolated": idempotency_key.startswith(f"{environment_id}:{namespace}:"),
        "duplicate_policy": "same_key_same_intent_returns_existing_execution",
        "idempotency_proof_level": "legacy_paper_trade_entry_execution",
        "observed_at": observed_at,
        "legacy_paper_trade": row,
    }


def mirror_idempotency_contracts(
    paper_db_path,
    signal_db_path,
    event_log_dir,
    *,
    since_id=None,
    until_id=None,
    limit=None,
    dry_run=False,
    table="paper_trades",
    signal_table="premium_signals",
    default_chain="solana",
    contract_version=DEFAULT_CONTRACT_VERSION,
    namespace=DEFAULT_NAMESPACE,
    environment_id=DEFAULT_ENVIRONMENT_ID,
    collision_policy=DEFAULT_COLLISION_POLICY,
    hash_algorithm=DEFAULT_HASH_ALGORITHM,
):
    summary = {
        "paper_db": str(paper_db_path),
        "signal_db": str(signal_db_path),
        "event_log_dir": str(event_log_dir),
        "table": table,
        "read_rows": 0,
        "appended": 0,
        "duplicate": 0,
        "failed": 0,
        "dry_run": bool(dry_run),
        "failures": [],
        "idempotency_contract_version": contract_version,
        "namespace": namespace,
        "environment_id": environment_id,
        "collision_policy": collision_policy,
    }
    with _connect(paper_db_path) as paper_db, _connect(signal_db_path) as signal_db:
        schema_error = _paper_trade_schema_error(paper_db, table)
        if schema_error:
            summary["failed"] += 1
            summary["failures"].append({"reason": schema_error})
            return summary
        for row in iter_paper_trade_rows(paper_db, since_id=since_id, until_id=until_id, limit=limit, table=table):
            row = _row_dict(row)
            summary["read_rows"] += 1
            if dry_run:
                continue
            try:
                signal_context = _signal_context(signal_db, row, signal_table=signal_table, default_chain=default_chain)
                payload = _idempotency_payload(
                    row,
                    signal_context,
                    contract_version=contract_version,
                    namespace=namespace,
                    environment_id=environment_id,
                    collision_policy=collision_policy,
                    hash_algorithm=hash_algorithm,
                )
                aggregate_id = ":".join(
                    [
                        "idempotency_contract",
                        str(payload.get("chain") or "unknown_chain"),
                        str(payload.get("token_ca")),
                        str(payload.get("canonical_pool_group") or "unknown_pool"),
                        str(payload.get("lifecycle_epoch", 0)),
                        str(payload.get("paper_trade_id")),
                    ]
                )
                result = V27EventLog(event_log_dir).append_event(
                    event_type=IDEMPOTENCY_EVENT_TYPE,
                    aggregate_id=aggregate_id,
                    payload=payload,
                    source="paper_trades",
                    idempotency_key=f"idempotency_contract:{environment_id}:{paper_trade_id_key(row)}:{contract_version}",
                    observed_at=payload.get("observed_at"),
                    available_at=payload.get("observed_at"),
                )
            except Exception as exc:
                summary["failed"] += 1
                summary["failures"].append({"paper_trade_id": row.get("id"), "reason": str(exc)})
                continue
            status = result.get("status")
            if status == "appended":
                summary["appended"] += 1
            elif status == "duplicate":
                summary["duplicate"] += 1
            else:
                summary["failed"] += 1
                summary["failures"].append({"paper_trade_id": row.get("id"), "reason": f"unexpected status {status}"})
    return summary


def paper_trade_id_key(row):
    return row.get("id") if isinstance(row, dict) else "unknown"


def _paper_trade_ids_from_db(db, *, since_id=None, until_id=None, limit=None, table="paper_trades"):
    return [row["id"] for row in iter_paper_trade_rows(db, since_id=since_id, until_id=until_id, limit=limit, table=table)]


def _mirrored_idempotency_contract_ids(event_log_dir, *, contract_version=None):
    counts = {}
    if not (Path(event_log_dir) / "events.jsonl").exists():
        return counts
    for event in V27EventLog(event_log_dir).iter_events():
        if event.get("event_type") != IDEMPOTENCY_EVENT_TYPE:
            continue
        payload = event.get("payload") or {}
        if contract_version and payload.get("idempotency_contract_version") != contract_version:
            continue
        paper_trade_id = payload.get("paper_trade_id")
        if paper_trade_id is None:
            continue
        counts[int(paper_trade_id)] = counts.get(int(paper_trade_id), 0) + 1
    return counts


def max_mirrored_idempotency_contract_id(event_log_dir, *, contract_version=DEFAULT_CONTRACT_VERSION):
    mirrored = _mirrored_idempotency_contract_ids(event_log_dir, contract_version=contract_version)
    return max(mirrored) if mirrored else None


def next_unmirrored_idempotency_contract_since_id(event_log_dir, configured_since_id=None, *, contract_version=DEFAULT_CONTRACT_VERSION):
    max_id = max_mirrored_idempotency_contract_id(event_log_dir, contract_version=contract_version)
    if max_id is None:
        return configured_since_id
    next_id = int(max_id) + 1
    if configured_since_id is not None:
        return max(next_id, int(configured_since_id))
    return next_id


def verify_idempotency_contract_mirror_parity(
    paper_db_path,
    event_log_dir,
    *,
    since_id=None,
    until_id=None,
    limit=None,
    table="paper_trades",
    contract_version=DEFAULT_CONTRACT_VERSION,
):
    summary = {
        "paper_db": str(paper_db_path),
        "event_log_dir": str(event_log_dir),
        "table": table,
        "db_rows": 0,
        "mirrored_events": 0,
        "missing_paper_trade_ids": [],
        "duplicate_paper_trade_ids": [],
        "orphan_mirrored_paper_trade_ids": [],
        "event_log_verify": None,
        "event_log_error": None,
        "parity_ok": False,
        "idempotency_contract_version": contract_version,
    }
    with _connect(paper_db_path) as db:
        db_ids = _paper_trade_ids_from_db(db, since_id=since_id, until_id=until_id, limit=limit, table=table)
    mirrored_counts = _mirrored_idempotency_contract_ids(event_log_dir, contract_version=contract_version)
    db_id_set = set(db_ids)
    mirrored_id_set = set(mirrored_counts)
    scoped = since_id is not None or until_id is not None or limit is not None
    summary["db_rows"] = len(db_ids)
    summary["mirrored_events"] = sum(mirrored_counts.get(trade_id, 0) for trade_id in db_id_set) if scoped else sum(mirrored_counts.values())
    summary["missing_paper_trade_ids"] = sorted(db_id_set - mirrored_id_set)
    summary["duplicate_paper_trade_ids"] = sorted([trade_id for trade_id in db_id_set if mirrored_counts.get(trade_id, 0) > 1])
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
    since_id = args.since_id
    if getattr(args, "new_only", False):
        since_id = next_unmirrored_idempotency_contract_since_id(
            args.event_log_dir,
            configured_since_id=since_id,
            contract_version=args.contract_version,
        )
    mirror_summary = mirror_idempotency_contracts(
        args.paper_db,
        args.signal_db,
        args.event_log_dir,
        since_id=since_id,
        until_id=args.until_id,
        limit=args.limit,
        dry_run=args.dry_run,
        table=args.table,
        signal_table=args.signal_table,
        default_chain=args.default_chain,
        contract_version=args.contract_version,
        namespace=args.namespace,
        environment_id=args.environment_id,
        collision_policy=args.collision_policy,
        hash_algorithm=args.hash_algorithm,
    )
    verify_summary = None
    if not args.dry_run:
        verify_summary = verify_idempotency_contract_mirror_parity(
            args.paper_db,
            args.event_log_dir,
            since_id=since_id,
            until_id=args.until_id,
            limit=args.limit,
            table=args.table,
            contract_version=args.contract_version,
        )
    return {
        "cursor": {
            "new_only": bool(getattr(args, "new_only", False)),
            "since_id": since_id,
            "until_id": args.until_id,
            "limit": args.limit,
            "max_mirrored_paper_trade_id": max_mirrored_idempotency_contract_id(
                args.event_log_dir,
                contract_version=args.contract_version,
            ),
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
    parser.add_argument("--paper-db", default=str(DEFAULT_PAPER_DB))
    parser.add_argument("--signal-db", default=str(DEFAULT_SIGNAL_DB))
    parser.add_argument("--event-log-dir", default=str(DEFAULT_EVENT_LOG_DIR))
    parser.add_argument("--since-id", type=int)
    parser.add_argument("--until-id", type=int)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--table", default="paper_trades")
    parser.add_argument("--signal-table", default="premium_signals")
    parser.add_argument("--default-chain", default="solana")
    parser.add_argument("--contract-version", default=os.environ.get("V27_IDEMPOTENCY_CONTRACT_VERSION", DEFAULT_CONTRACT_VERSION))
    parser.add_argument("--namespace", default=os.environ.get("V27_IDEMPOTENCY_NAMESPACE", DEFAULT_NAMESPACE))
    parser.add_argument("--environment-id", default=os.environ.get("V27_ENVIRONMENT_ID", DEFAULT_ENVIRONMENT_ID))
    parser.add_argument("--collision-policy", default=os.environ.get("V27_IDEMPOTENCY_COLLISION_POLICY", DEFAULT_COLLISION_POLICY))
    parser.add_argument("--hash-algorithm", default=os.environ.get("V27_IDEMPOTENCY_HASH_ALGORITHM", DEFAULT_HASH_ALGORITHM))
    parser.add_argument("--new-only", action="store_true")
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
            failed += 0 if result.get("verify", {}).get("parity_ok", True) else 1
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

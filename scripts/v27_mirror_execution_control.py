#!/usr/bin/env python3
"""Mirror entry execution lease/fencing/state evidence into v2.7 events."""

import argparse
import fcntl
import json
import os
import signal
import sys
import time
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_event_log import V27EventLog, V27EventLogError, sha256_hex  # noqa: E402
from v27_mirror_idempotency_contracts import (  # noqa: E402
    DEFAULT_ENVIRONMENT_ID,
    DEFAULT_PAPER_DB,
    DEFAULT_SIGNAL_DB,
    _connect,
    _route,
    _row_dict,
    _token_lifecycle_key,
    _value,
    iter_paper_trade_rows,
    paper_trade_id_key,
)
from v27_mirror_realtime_clean import _nested_get, _parse_json_object, _quote_ts_seconds  # noqa: E402
from v27_mirror_trade_outcomes import _as_float, _as_int, _signal_context, _timestamp_to_iso  # noqa: E402


DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
DEFAULT_LOCK_FILE = Path("/tmp/v27_execution_control_mirror.lock")
EXECUTION_CONTROL_EVENT_TYPE = "execution_control_recorded"
DEFAULT_CONTROL_VERSION = "legacy_paper_entry_execution_control_v0.1"
DEFAULT_LEASE_TTL_SEC = float(os.environ.get("V27_EXECUTION_LEASE_TTL_SEC", "20"))


def _positive_epoch_seconds(*values):
    for value in values:
        parsed = _quote_ts_seconds(value)
        if parsed is not None and parsed > 0:
            return parsed
    return None


def _entry_acquired_ts(row, entry_audit, *, fallback_ts):
    quote_ts = _positive_epoch_seconds(entry_audit.get("quoteTs"), _value(row, "entry_ts"), fallback_ts)
    claim_to_quote_ms = _as_float(_nested_get(entry_audit, ("entryLatencyAudit", "fast_lane_claim_to_quote_latency_ms")))
    if quote_ts is not None and claim_to_quote_ms is not None and claim_to_quote_ms >= 0:
        return max(0.0, quote_ts - (claim_to_quote_ms / 1000.0))
    return _positive_epoch_seconds(_value(row, "claimed_at"), fallback_ts, quote_ts)


def _execution_control_payload(
    row,
    signal_context,
    *,
    control_version=DEFAULT_CONTROL_VERSION,
    environment_id=DEFAULT_ENVIRONMENT_ID,
    lease_ttl_sec=DEFAULT_LEASE_TTL_SEC,
):
    entry_audit = _parse_json_object(row.get("entry_execution_audit_json"))
    monitor_state = _parse_json_object(row.get("monitor_state_json"))
    paper_trade_id = _value(row, "id")
    route = _route(row)
    token_lifecycle_key = _token_lifecycle_key(row, signal_context)
    decision_id = f"paper_trade:{paper_trade_id}:entry_decision"
    execution_id = f"paper_trade:{paper_trade_id}:entry_execution"
    chain = signal_context.get("chain") or _value(row, "chain") or "solana"
    pool = signal_context.get("canonical_pool_group") or _value(row, "lifecycle_id") or "unknown_pool"
    lifecycle_epoch = signal_context.get("lifecycle_epoch") or _value(row, "lifecycle_epoch") or 0
    fill_ts = _positive_epoch_seconds(entry_audit.get("quoteTs"), _value(row, "entry_ts"), _value(row, "signal_ts"))
    acquired_ts = _entry_acquired_ts(row, entry_audit, fallback_ts=fill_ts)
    expires_ts = acquired_ts + float(lease_ttl_sec) if acquired_ts is not None else None
    paper_trade_id_int = _as_int(paper_trade_id) or 0
    state_version_at_decision = max(0, (paper_trade_id_int * 2) - 1)
    state_version_at_execution = state_version_at_decision + 1
    state_version = state_version_at_execution
    fencing_material = {
        "environment_id": environment_id,
        "token_lifecycle_key": token_lifecycle_key,
        "decision_id": decision_id,
        "execution_id": execution_id,
        "state_version_at_decision": state_version_at_decision,
        "state_version_at_execution": state_version_at_execution,
    }
    fencing_token = sha256_hex(fencing_material)
    lease_material = {
        "environment_id": environment_id,
        "token_lifecycle_key": token_lifecycle_key,
        "execution_id": execution_id,
        "fencing_token": fencing_token,
    }
    lease_id = f"lease:{environment_id}:{sha256_hex(lease_material)[:24]}"
    lease_valid_at_execution = bool(
        acquired_ts is not None
        and expires_ts is not None
        and fill_ts is not None
        and acquired_ts <= fill_ts <= expires_ts
    )
    state_history = [
        {"state": "decision_recorded", "state_version": state_version_at_decision},
        {"state": "lease_acquired", "state_version": state_version_at_decision + 1},
        {"state": "filled_paper", "state_version": state_version_at_execution},
    ]
    return {
        "paper_trade_id": paper_trade_id,
        "premium_signal_id": signal_context.get("premium_signal_id"),
        "telegram_signal_id": signal_context.get("telegram_signal_id"),
        "token_ca": _value(row, "token_ca"),
        "symbol": _value(row, "symbol"),
        "chain": chain,
        "canonical_pool_group": pool,
        "lifecycle_epoch": lifecycle_epoch,
        "execution_control_version": control_version,
        "decision_id": decision_id,
        "execution_id": execution_id,
        "token_lifecycle_key": token_lifecycle_key,
        "route": route,
        "environment_id": environment_id,
        "lease_id": lease_id,
        "fencing_token": fencing_token,
        "acquired_at": _timestamp_to_iso(acquired_ts),
        "expires_at": _timestamp_to_iso(expires_ts),
        "released_at": _timestamp_to_iso(fill_ts),
        "lease_status": "released",
        "lease_valid_at_execution": lease_valid_at_execution,
        "lease_ttl_sec": float(lease_ttl_sec),
        "state_version_at_decision": state_version_at_decision,
        "state_version_at_execution": state_version_at_execution,
        "requires_revalidation_before_fill": True,
        "revalidation_passed": True,
        "revalidation_reason": "legacy_paper_trade_entry_reconstructed",
        "state": "filled_paper",
        "state_version": state_version,
        "failure_reason": "none",
        "state_history": state_history,
        "terminal_state": True,
        "execution_control_proof_level": (
            "entry_execution_audit_fast_lane_claim"
            if _nested_get(entry_audit, ("entryLatencyAudit", "fast_lane_claim_to_quote_latency_ms")) is not None
            else "legacy_paper_trade_entry_execution_control_proxy"
        ),
        "state_version_source": "legacy_paper_trade_row_id",
        "fencing_material_hash": sha256_hex(fencing_material),
        "monitor_state": monitor_state,
        "entry_execution_audit": entry_audit,
        "legacy_paper_trade": row,
        "observed_at": _timestamp_to_iso(fill_ts),
    }


def mirror_execution_controls(
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
    control_version=DEFAULT_CONTROL_VERSION,
    environment_id=DEFAULT_ENVIRONMENT_ID,
    lease_ttl_sec=DEFAULT_LEASE_TTL_SEC,
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
        "execution_control_version": control_version,
        "environment_id": environment_id,
        "lease_ttl_sec": float(lease_ttl_sec),
    }
    with _connect(paper_db_path) as paper_db, _connect(signal_db_path) as signal_db:
        for row in iter_paper_trade_rows(paper_db, since_id=since_id, until_id=until_id, limit=limit, table=table):
            row = _row_dict(row)
            summary["read_rows"] += 1
            if dry_run:
                continue
            try:
                signal_context = _signal_context(signal_db, row, signal_table=signal_table, default_chain=default_chain)
                payload = _execution_control_payload(
                    row,
                    signal_context,
                    control_version=control_version,
                    environment_id=environment_id,
                    lease_ttl_sec=lease_ttl_sec,
                )
                aggregate_id = ":".join(
                    [
                        "execution_control",
                        str(payload.get("chain") or "unknown_chain"),
                        str(payload.get("token_ca")),
                        str(payload.get("canonical_pool_group") or "unknown_pool"),
                        str(payload.get("lifecycle_epoch", 0)),
                        str(payload.get("paper_trade_id")),
                    ]
                )
                result = V27EventLog(event_log_dir).append_event(
                    event_type=EXECUTION_CONTROL_EVENT_TYPE,
                    aggregate_id=aggregate_id,
                    payload=payload,
                    source="paper_trades",
                    idempotency_key=f"execution_control:{environment_id}:{paper_trade_id_key(row)}:{control_version}",
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


def _paper_trade_ids_from_db(db, *, since_id=None, until_id=None, limit=None, table="paper_trades"):
    return [row["id"] for row in iter_paper_trade_rows(db, since_id=since_id, until_id=until_id, limit=limit, table=table)]


def _mirrored_execution_control_ids(event_log_dir, *, control_version=None):
    counts = {}
    if not (Path(event_log_dir) / "events.jsonl").exists():
        return counts
    for event in V27EventLog(event_log_dir).iter_events():
        if event.get("event_type") != EXECUTION_CONTROL_EVENT_TYPE:
            continue
        payload = event.get("payload") or {}
        if control_version and payload.get("execution_control_version") != control_version:
            continue
        paper_trade_id = payload.get("paper_trade_id")
        if paper_trade_id is None:
            continue
        counts[int(paper_trade_id)] = counts.get(int(paper_trade_id), 0) + 1
    return counts


def max_mirrored_execution_control_id(event_log_dir, *, control_version=DEFAULT_CONTROL_VERSION):
    mirrored = _mirrored_execution_control_ids(event_log_dir, control_version=control_version)
    return max(mirrored) if mirrored else None


def next_unmirrored_execution_control_since_id(event_log_dir, configured_since_id=None, *, control_version=DEFAULT_CONTROL_VERSION):
    max_id = max_mirrored_execution_control_id(event_log_dir, control_version=control_version)
    if max_id is None:
        return configured_since_id
    next_id = int(max_id) + 1
    if configured_since_id is not None:
        return max(next_id, int(configured_since_id))
    return next_id


def verify_execution_control_mirror_parity(
    paper_db_path,
    event_log_dir,
    *,
    since_id=None,
    until_id=None,
    limit=None,
    table="paper_trades",
    control_version=DEFAULT_CONTROL_VERSION,
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
        "execution_control_version": control_version,
    }
    with _connect(paper_db_path) as db:
        db_ids = _paper_trade_ids_from_db(db, since_id=since_id, until_id=until_id, limit=limit, table=table)
    mirrored_counts = _mirrored_execution_control_ids(event_log_dir, control_version=control_version)
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
        since_id = next_unmirrored_execution_control_since_id(
            args.event_log_dir,
            configured_since_id=since_id,
            control_version=args.control_version,
        )
    mirror_summary = mirror_execution_controls(
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
        control_version=args.control_version,
        environment_id=args.environment_id,
        lease_ttl_sec=args.lease_ttl_sec,
    )
    verify_summary = None
    if not args.dry_run:
        verify_summary = verify_execution_control_mirror_parity(
            args.paper_db,
            args.event_log_dir,
            since_id=since_id,
            until_id=args.until_id,
            limit=args.limit,
            table=args.table,
            control_version=args.control_version,
        )
    return {
        "cursor": {
            "new_only": bool(getattr(args, "new_only", False)),
            "since_id": since_id,
            "until_id": args.until_id,
            "limit": args.limit,
            "max_mirrored_paper_trade_id": max_mirrored_execution_control_id(
                args.event_log_dir,
                control_version=args.control_version,
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
    parser.add_argument("--control-version", default=os.environ.get("V27_EXECUTION_CONTROL_VERSION", DEFAULT_CONTROL_VERSION))
    parser.add_argument("--environment-id", default=os.environ.get("V27_ENVIRONMENT_ID", DEFAULT_ENVIRONMENT_ID))
    parser.add_argument("--lease-ttl-sec", type=float, default=float(os.environ.get("V27_EXECUTION_LEASE_TTL_SEC", DEFAULT_LEASE_TTL_SEC)))
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

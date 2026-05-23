#!/usr/bin/env python3
"""Mirror no-fill and startup recovery evidence into v2.7 events."""

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
    _table_exists,
    _token_lifecycle_key,
    _value,
    iter_paper_trade_rows,
    paper_trade_id_key,
)
from v27_mirror_trade_outcomes import _signal_context, _timestamp_to_iso  # noqa: E402


DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
DEFAULT_LOCK_FILE = Path("/tmp/v27_recovery_control_mirror.lock")
NO_FILL_EVENT_TYPE = "no_fill_outcome_recorded"
RECOVERY_EVENT_TYPE = "runtime_recovery_control_recorded"
DEFAULT_RECOVERY_VERSION = "legacy_paper_recovery_control_v0.1"


def _as_float(value, default=None):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if parsed != parsed:
        return default
    return parsed


def _as_int(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


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


def _text(value, default=None):
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _source_row_reference(row, *, table, row_id):
    material = dict(row or {})
    return {
        "source_table": table,
        "source_row_id": row_id,
        "source_row_hash": sha256_hex(material),
        "source_row_field_count": len(material),
    }


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


def iter_missed_rows(db, *, since_id=None, until_id=None, limit=None, table="paper_missed_signal_attribution"):
    if not _table_exists(db, table):
        return
    where, params = _row_filters(since_id=since_id, until_id=until_id)
    sql = f"SELECT * FROM {table}{where} ORDER BY id"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    yield from db.execute(sql, params)


def _first_numeric(row, field_names, default=0.0):
    for field in field_names:
        if field not in row:
            continue
        value = _as_float(row.get(field))
        if value is not None:
            return value, field
    return default, None


def _chain_pool_epoch(row, signal_context=None, *, default_chain="solana"):
    signal_context = signal_context or {}
    chain = signal_context.get("chain") or _value(row, "chain") or default_chain
    pool = signal_context.get("canonical_pool_group") or _value(row, "canonical_pool_group") or _value(row, "lifecycle_id") or "unknown_pool"
    lifecycle_epoch = signal_context.get("lifecycle_epoch") or _value(row, "lifecycle_epoch") or 0
    return chain, pool, lifecycle_epoch


def _paper_trade_no_fill_payload(
    row,
    signal_context,
    *,
    recovery_version=DEFAULT_RECOVERY_VERSION,
    environment_id=DEFAULT_ENVIRONMENT_ID,
    default_chain="solana",
):
    paper_trade_id = _value(row, "id")
    chain, pool, lifecycle_epoch = _chain_pool_epoch(row, signal_context, default_chain=default_chain)
    token_lifecycle_key = _token_lifecycle_key(row, signal_context)
    decision_id = f"paper_trade:{paper_trade_id}:entry_decision"
    execution_id = f"paper_trade:{paper_trade_id}:entry_execution"
    observed_at = _timestamp_to_iso(_value(row, "entry_ts")) or _timestamp_to_iso(_value(row, "signal_ts"))
    material = {
        "environment_id": environment_id,
        "source": "paper_trades",
        "paper_trade_id": paper_trade_id,
        "execution_id": execution_id,
        "outcome_state": "filled_paper",
    }
    return {
        "paper_trade_id": paper_trade_id,
        "premium_signal_id": signal_context.get("premium_signal_id"),
        "telegram_signal_id": signal_context.get("telegram_signal_id"),
        "token_ca": _value(row, "token_ca"),
        "symbol": _value(row, "symbol"),
        "chain": chain,
        "canonical_pool_group": pool,
        "lifecycle_epoch": lifecycle_epoch,
        "recovery_control_version": recovery_version,
        "no_fill_outcome_version": recovery_version,
        "attempt_id": f"paper_trade:{paper_trade_id}:attempt",
        "decision_id": decision_id,
        "execution_id": execution_id,
        "token_lifecycle_key": token_lifecycle_key,
        "environment_id": environment_id,
        "route": _route(row),
        "outcome_state": "filled_paper",
        "terminal_state": True,
        "no_fill_record_required": False,
        "no_fill_reason": "none_filled_paper",
        "missed_net_peak30": 0.0,
        "missed_net_peak30_source": "not_applicable_filled_paper",
        "no_fill_cost": 0.0,
        "no_fill_saved_loss": 0.0,
        "no_fill_cost_model": "legacy_zero_cost_for_filled_paper",
        "no_fill_outcome_hash": sha256_hex(material),
        "outcome_source": "paper_trades",
        "outcome_available_at": observed_at,
        "observed_at": observed_at,
        "legacy_paper_trade_ref": _source_row_reference(row, table="paper_trades", row_id=paper_trade_id),
    }


def _missed_no_fill_payload(
    row,
    *,
    recovery_version=DEFAULT_RECOVERY_VERSION,
    environment_id=DEFAULT_ENVIRONMENT_ID,
    default_chain="solana",
):
    missed_id = _value(row, "id")
    chain, pool, lifecycle_epoch = _chain_pool_epoch(row, default_chain=default_chain)
    peak, peak_field = _first_numeric(
        row,
        [
            "tradable_peak_pnl",
            "max_pnl_recorded",
            "pnl_60m",
            "pnl_24h",
            "pnl_15m",
            "pnl_5m",
        ],
    )
    min_pnl, min_pnl_field = _first_numeric(row, ["min_pnl_recorded", "mae_before_peak_pnl"], default=0.0)
    no_fill_cost = max(0.0, peak or 0.0)
    saved_loss = max(0.0, -(min_pnl or 0.0))
    if _as_bool(_value(row, "would_stop_before_peak")) is True:
        stop_floor, _stop_field = _first_numeric(row, ["stop_floor_pnl"], default=0.0)
        saved_loss = max(saved_loss, max(0.0, -(stop_floor or 0.0)))
    decision_id = f"missed_signal:{missed_id}:decision"
    execution_id = f"missed_signal:{missed_id}:no_fill"
    token_lifecycle_key = ":".join(
        [
            str(chain or "unknown_chain"),
            str(_value(row, "token_ca") or "unknown_token"),
            str(pool or "unknown_pool"),
            str(lifecycle_epoch or 0),
            str(_value(row, "lifecycle_id") or _value(row, "signal_id") or missed_id),
        ]
    )
    reason = (
        _text(_value(row, "reject_reason"))
        or _text(_value(row, "tradability_reason"))
        or _text(_value(row, "decision"))
        or _text(_value(row, "status"))
        or "legacy_no_fill"
    )
    observed_at = _timestamp_to_iso(_value(row, "created_event_ts")) or _timestamp_to_iso(_value(row, "signal_ts"))
    material = {
        "environment_id": environment_id,
        "source": "paper_missed_signal_attribution",
        "missed_attribution_id": missed_id,
        "execution_id": execution_id,
        "outcome_state": "no_fill",
        "no_fill_reason": reason,
        "missed_net_peak30": peak,
    }
    return {
        "missed_attribution_id": missed_id,
        "decision_event_id": _value(row, "decision_event_id"),
        "token_ca": _value(row, "token_ca"),
        "symbol": _value(row, "symbol"),
        "chain": chain,
        "canonical_pool_group": pool,
        "lifecycle_epoch": lifecycle_epoch,
        "recovery_control_version": recovery_version,
        "no_fill_outcome_version": recovery_version,
        "attempt_id": f"missed_signal:{missed_id}:attempt",
        "decision_id": decision_id,
        "execution_id": execution_id,
        "token_lifecycle_key": token_lifecycle_key,
        "environment_id": environment_id,
        "route": _value(row, "route") or "legacy_unknown_route",
        "outcome_state": "no_fill",
        "terminal_state": True,
        "no_fill_record_required": True,
        "no_fill_reason": reason,
        "missed_net_peak30": peak,
        "missed_net_peak30_source": peak_field or "legacy_peak_missing_zero_cost",
        "no_fill_cost": no_fill_cost,
        "no_fill_saved_loss": saved_loss,
        "saved_loss_source": min_pnl_field or "legacy_no_negative_excursion_recorded",
        "no_fill_cost_model": "legacy_missed_signal_peak_minus_zero_entry_cost_v0.1",
        "tradable_missed": _as_bool(_value(row, "tradable_missed")),
        "tradability_status": _value(row, "tradability_status"),
        "tradability_version": _value(row, "tradability_version"),
        "no_fill_outcome_hash": sha256_hex(material),
        "outcome_source": "paper_missed_signal_attribution",
        "outcome_available_at": observed_at,
        "observed_at": observed_at,
        "legacy_missed_attribution_ref": _source_row_reference(
            row,
            table="paper_missed_signal_attribution",
            row_id=missed_id,
        ),
    }


def _latest_no_fill_ids(event_log_dir, *, recovery_version=DEFAULT_RECOVERY_VERSION):
    mirrored = {"paper_trade": set(), "missed": set()}
    if not (Path(event_log_dir) / "events.jsonl").exists():
        return mirrored
    for event in V27EventLog(event_log_dir).iter_events():
        if event.get("event_type") != NO_FILL_EVENT_TYPE:
            continue
        payload = event.get("payload") or {}
        if recovery_version and payload.get("no_fill_outcome_version") != recovery_version:
            continue
        if payload.get("paper_trade_id") is not None:
            mirrored["paper_trade"].add(int(payload.get("paper_trade_id")))
        if payload.get("missed_attribution_id") is not None:
            mirrored["missed"].add(int(payload.get("missed_attribution_id")))
    return mirrored


def _max_mirrored_ids(event_log_dir, *, recovery_version=DEFAULT_RECOVERY_VERSION):
    mirrored = _latest_no_fill_ids(event_log_dir, recovery_version=recovery_version)
    return {
        "paper_trade": max(mirrored["paper_trade"]) if mirrored["paper_trade"] else None,
        "missed": max(mirrored["missed"]) if mirrored["missed"] else None,
    }


def _next_since(configured_since_id, max_mirrored):
    if max_mirrored is None:
        return configured_since_id
    next_id = int(max_mirrored) + 1
    if configured_since_id is not None:
        return max(next_id, int(configured_since_id))
    return next_id


def _event_scan(event_log_dir):
    scan = {
        "event_log_ok": False,
        "event_log_verify": None,
        "event_log_error": None,
        "execution_control_events": 0,
        "non_terminal_execution_count": 0,
        "no_fill_outcome_events": 0,
        "malformed_no_fill_count": 0,
        "orphaned_execution_count": 0,
    }
    try:
        event_log = V27EventLog(event_log_dir)
        scan["event_log_verify"] = event_log.verify()
        scan["event_log_ok"] = True
        for event in event_log.iter_events() or []:
            payload = event.get("payload") or {}
            if event.get("event_type") == "execution_control_recorded":
                scan["execution_control_events"] += 1
                if payload.get("terminal_state") is not True:
                    scan["non_terminal_execution_count"] += 1
            if event.get("event_type") == NO_FILL_EVENT_TYPE:
                scan["no_fill_outcome_events"] += 1
                for field in ("no_fill_reason", "missed_net_peak30", "no_fill_cost", "no_fill_saved_loss"):
                    if payload.get(field) is None:
                        scan["malformed_no_fill_count"] += 1
                        break
    except V27EventLogError as exc:
        scan["event_log_error"] = str(exc)
    scan["orphaned_execution_count"] = scan["non_terminal_execution_count"]
    return scan


def _recovery_payload(
    event_log_dir,
    no_fill_summary,
    *,
    recovery_version=DEFAULT_RECOVERY_VERSION,
    environment_id=DEFAULT_ENVIRONMENT_ID,
):
    scan = _event_scan(event_log_dir)
    completed_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    clean = bool(
        scan.get("event_log_ok")
        and scan.get("orphaned_execution_count") == 0
        and scan.get("malformed_no_fill_count") == 0
    )
    source_cursor = {
        "paper_trade_max_id": no_fill_summary.get("max_paper_trade_id"),
        "missed_attribution_max_id": no_fill_summary.get("max_missed_attribution_id"),
        "recovery_control_version": recovery_version,
    }
    recovery_id = f"recovery:{environment_id}:{sha256_hex(source_cursor)[:24]}"
    drain_id = f"drain:{environment_id}:{sha256_hex({'recovery_id': recovery_id, 'source_cursor': source_cursor})[:24]}"
    queued_revalidated = int(no_fill_summary.get("paper_trade_rows") or 0)
    expired_emitted = int(no_fill_summary.get("missed_rows") or 0)
    return {
        "recovery_control_version": recovery_version,
        "recovery_id": recovery_id,
        "state": "clean_start" if clean else "degraded",
        "environment_id": environment_id,
        "event_log_dir": str(event_log_dir),
        "source_cursor": source_cursor,
        "orphan_scan_result": {
            "status": "ok" if clean else "orphan_or_malformed_evidence_detected",
            "event_log_ok": scan.get("event_log_ok"),
            "event_log_error": scan.get("event_log_error"),
            "event_log_verify": scan.get("event_log_verify"),
            "execution_control_events": scan.get("execution_control_events"),
            "non_terminal_execution_count": scan.get("non_terminal_execution_count"),
            "orphaned_execution_count": scan.get("orphaned_execution_count"),
        },
        "reconcile_result": {
            "status": "ok" if clean else "blocked",
            "event_log_ok": scan.get("event_log_ok"),
            "no_fill_outcome_events": scan.get("no_fill_outcome_events"),
            "malformed_no_fill_count": scan.get("malformed_no_fill_count"),
            "event_log_latest_seq": (scan.get("event_log_verify") or {}).get("last_global_seq"),
        },
        "drain_id": drain_id,
        "queued_candidates_revalidated": queued_revalidated,
        "expired_candidates_emitted": expired_emitted,
        "resume_drain_completed_at": completed_at,
        "drain_status": "completed" if clean else "blocked",
        "new_entries_blocked_until_drain": True,
        "resume_allowed": clean,
        "no_fill_summary": dict(no_fill_summary),
        "observed_at": completed_at,
    }


def _build_event_specs(payloads, *, environment_id, recovery_version):
    specs = []
    for payload in payloads:
        chain = payload.get("chain") or "unknown_chain"
        token_ca = payload.get("token_ca") or "unknown_token"
        pool = payload.get("canonical_pool_group") or "unknown_pool"
        lifecycle_epoch = payload.get("lifecycle_epoch") or 0
        source_id = payload.get("paper_trade_id") if payload.get("paper_trade_id") is not None else payload.get("missed_attribution_id")
        source_kind = "paper_trade" if payload.get("paper_trade_id") is not None else "missed_signal"
        specs.append(
            {
                "event_type": NO_FILL_EVENT_TYPE,
                "aggregate_id": f"no_fill_outcome:{chain}:{token_ca}:{pool}:{lifecycle_epoch}:{source_kind}:{source_id}",
                "payload": payload,
                "source": "v27_recovery_control_mirror",
                "idempotency_key": f"no_fill_outcome:{environment_id}:{source_kind}:{source_id}:{recovery_version}",
                "observed_at": payload.get("observed_at"),
                "available_at": payload.get("outcome_available_at") or payload.get("observed_at"),
            }
        )
    return specs


def mirror_recovery_controls(
    paper_db_path,
    signal_db_path,
    event_log_dir,
    *,
    since_id=None,
    until_id=None,
    limit=None,
    missed_since_id=None,
    missed_until_id=None,
    missed_limit=None,
    dry_run=False,
    table="paper_trades",
    missed_table="paper_missed_signal_attribution",
    signal_table="premium_signals",
    default_chain="solana",
    recovery_version=DEFAULT_RECOVERY_VERSION,
    environment_id=DEFAULT_ENVIRONMENT_ID,
):
    summary = {
        "paper_db": str(paper_db_path),
        "signal_db": str(signal_db_path),
        "event_log_dir": str(event_log_dir),
        "table": table,
        "missed_table": missed_table,
        "paper_trade_rows": 0,
        "missed_rows": 0,
        "appended": 0,
        "duplicate": 0,
        "failed": 0,
        "dry_run": bool(dry_run),
        "failures": [],
        "recovery_control_version": recovery_version,
        "environment_id": environment_id,
        "max_paper_trade_id": None,
        "max_missed_attribution_id": None,
    }
    payloads = []
    with _connect(paper_db_path) as paper_db, _connect(signal_db_path) as signal_db:
        for row in iter_paper_trade_rows(paper_db, since_id=since_id, until_id=until_id, limit=limit, table=table):
            row = _row_dict(row)
            summary["paper_trade_rows"] += 1
            row_id = _as_int(row.get("id"))
            if row_id is not None:
                summary["max_paper_trade_id"] = max(row_id, summary["max_paper_trade_id"] or row_id)
            if dry_run:
                continue
            try:
                signal_context = _signal_context(signal_db, row, signal_table=signal_table, default_chain=default_chain)
                payloads.append(
                    _paper_trade_no_fill_payload(
                        row,
                        signal_context,
                        recovery_version=recovery_version,
                        environment_id=environment_id,
                        default_chain=default_chain,
                    )
                )
            except Exception as exc:
                summary["failed"] += 1
                summary["failures"].append({"paper_trade_id": row.get("id"), "reason": str(exc)})
        for row in iter_missed_rows(paper_db, since_id=missed_since_id, until_id=missed_until_id, limit=missed_limit, table=missed_table):
            row = _row_dict(row)
            summary["missed_rows"] += 1
            row_id = _as_int(row.get("id"))
            if row_id is not None:
                summary["max_missed_attribution_id"] = max(row_id, summary["max_missed_attribution_id"] or row_id)
            if dry_run:
                continue
            try:
                payloads.append(
                    _missed_no_fill_payload(
                        row,
                        recovery_version=recovery_version,
                        environment_id=environment_id,
                        default_chain=default_chain,
                    )
                )
            except Exception as exc:
                summary["failed"] += 1
                summary["failures"].append({"missed_attribution_id": row.get("id"), "reason": str(exc)})
    if dry_run:
        return {"mirror": summary, "recovery": None}

    event_log = V27EventLog(event_log_dir)
    for result in event_log.append_events(_build_event_specs(payloads, environment_id=environment_id, recovery_version=recovery_version)):
        status = result.get("status")
        if status == "appended":
            summary["appended"] += 1
        elif status == "duplicate":
            summary["duplicate"] += 1
        else:
            summary["failed"] += 1
            summary["failures"].append({"reason": f"unexpected status {status}"})

    recovery_payload = _recovery_payload(
        event_log_dir,
        summary,
        recovery_version=recovery_version,
        environment_id=environment_id,
    )
    recovery_key = ":".join(
        [
            "runtime_recovery_control",
            environment_id,
            recovery_version,
            str(summary.get("max_paper_trade_id")),
            str(summary.get("max_missed_attribution_id")),
        ]
    )
    recovery_result = event_log.append_event(
        event_type=RECOVERY_EVENT_TYPE,
        aggregate_id=f"runtime_recovery:{environment_id}",
        payload=recovery_payload,
        source="v27_recovery_control_mirror",
        idempotency_key=recovery_key,
        observed_at=recovery_payload.get("observed_at"),
        available_at=recovery_payload.get("resume_drain_completed_at"),
    )
    recovery_status = recovery_result.get("status")
    if recovery_status == "appended":
        summary["appended"] += 1
    elif recovery_status == "duplicate":
        summary["duplicate"] += 1
    else:
        summary["failed"] += 1
        summary["failures"].append({"reason": f"unexpected recovery status {recovery_status}"})
    return {"mirror": summary, "recovery": {"status": recovery_status, "event": recovery_result.get("event")}}


def max_mirrored_recovery_control_ids(event_log_dir, *, recovery_version=DEFAULT_RECOVERY_VERSION):
    return _max_mirrored_ids(event_log_dir, recovery_version=recovery_version)


def run_mirror_once(args):
    since_id = args.since_id
    missed_since_id = args.missed_since_id
    if getattr(args, "new_only", False):
        mirrored = _max_mirrored_ids(args.event_log_dir, recovery_version=args.recovery_version)
        since_id = _next_since(since_id, mirrored.get("paper_trade"))
        missed_since_id = _next_since(missed_since_id, mirrored.get("missed"))
    return mirror_recovery_controls(
        args.paper_db,
        args.signal_db,
        args.event_log_dir,
        since_id=since_id,
        until_id=args.until_id,
        limit=args.limit,
        missed_since_id=missed_since_id,
        missed_until_id=args.missed_until_id,
        missed_limit=args.missed_limit,
        dry_run=args.dry_run,
        table=args.table,
        missed_table=args.missed_table,
        signal_table=args.signal_table,
        default_chain=args.default_chain,
        recovery_version=args.recovery_version,
        environment_id=args.environment_id,
    )


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
    parser.add_argument("--missed-since-id", type=int)
    parser.add_argument("--missed-until-id", type=int)
    parser.add_argument("--missed-limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--table", default="paper_trades")
    parser.add_argument("--missed-table", default="paper_missed_signal_attribution")
    parser.add_argument("--signal-table", default="premium_signals")
    parser.add_argument("--default-chain", default="solana")
    parser.add_argument("--recovery-version", default=os.environ.get("V27_RECOVERY_CONTROL_VERSION", DEFAULT_RECOVERY_VERSION))
    parser.add_argument("--environment-id", default=os.environ.get("V27_ENVIRONMENT_ID", DEFAULT_ENVIRONMENT_ID))
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
            failed = int(result.get("mirror", {}).get("failed") or 0)
            recovery_event = (result.get("recovery") or {}).get("event") or {}
            recovery_payload = recovery_event.get("payload") or {}
            if recovery_payload.get("state") == "degraded":
                failed += 1
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

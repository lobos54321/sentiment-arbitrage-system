#!/usr/bin/env python3
"""Mirror quote-intent binding evidence into v2.7 events."""

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
from v27_mirror_realtime_clean import (  # noqa: E402
    DEFAULT_EVENT_LOG_DIR,
    DEFAULT_PAPER_DB,
    DEFAULT_SIGNAL_DB,
    _connect,
    _nested_get,
    _parse_json_object,
    _paper_trade_schema_error,
    _quote_ts_seconds,
    _row_dict,
    iter_paper_trade_rows,
)
from v27_mirror_trade_outcomes import _as_float, _as_int, _signal_context, _timestamp_to_iso  # noqa: E402


DEFAULT_LOCK_FILE = Path("/tmp/v27_quote_intent_binding_mirror.lock")
QUOTE_INTENT_BINDING_EVENT_TYPE = "quote_intent_binding_recorded"
DEFAULT_BINDING_POLICY_VERSION = "legacy_paper_trade_quote_intent_binding_v0.1"
DEFAULT_QUOTE_SOURCE = "paper_trade_entry_quote_or_legacy_proxy"
DEFAULT_LEGACY_SIZE_SOL = 0.003
DEFAULT_LEGACY_SLIPPAGE_BPS = 500
REQUIRED_BINDING_FIELDS = [
    "quote_intent_id",
    "side",
    "size",
    "route",
    "pool",
    "quote_mint",
    "slippage_bps",
    "quote_ts",
]


def _first_value(*values):
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _positive_float(value):
    parsed = _as_float(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed


def _non_empty_text(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _field_source(candidates):
    for source, value in candidates:
        parsed = _positive_float(value)
        if parsed is not None:
            return parsed, source
    return None, None


def _derive_size(row, entry_audit, exit_audit, monitor_state, *, legacy_size_sol=DEFAULT_LEGACY_SIZE_SOL):
    size, source = _field_source(
        [
            ("paper_trades.position_size_sol", row.get("position_size_sol")),
            ("entry_execution_audit.positionSizeSol", entry_audit.get("positionSizeSol")),
            ("entry_execution_audit.inputAmount", entry_audit.get("inputAmount")),
            ("monitor_state.entrySol", monitor_state.get("entrySol")),
            ("exit_execution_audit.lifecycleEntrySol", exit_audit.get("lifecycleEntrySol")),
        ]
    )
    if size is not None:
        return size, source
    fallback = _positive_float(legacy_size_sol)
    if fallback is not None:
        return fallback, "legacy_policy_default_size_sol"
    return None, None


def _derive_slippage_bps(entry_audit, monitor_state, *, legacy_slippage_bps=DEFAULT_LEGACY_SLIPPAGE_BPS):
    for source, value in (
        ("entry_execution_audit.slippageBps", entry_audit.get("slippageBps")),
        ("monitor_state.slippageBps", monitor_state.get("slippageBps")),
        ("monitor_state.maxSlippageBps", monitor_state.get("maxSlippageBps")),
        ("legacy_policy_default_slippage_bps", legacy_slippage_bps),
    ):
        parsed = _as_float(value)
        if parsed is not None and parsed >= 0:
            return parsed, source
    return None, None


def _derive_route(row, entry_audit, monitor_state, pool):
    route = _first_value(
        row.get("signal_route"),
        monitor_state.get("signalRoute"),
        entry_audit.get("route"),
        entry_audit.get("routeId"),
        row.get("entry_mode"),
    )
    if route is not None:
        return str(route), "explicit_route"
    if pool and pool != "unknown_pool":
        return "legacy_direct_pool_route", "legacy_direct_pool_route"
    return "legacy_direct_quote_route", "legacy_direct_quote_route"


def _derive_pool(row, signal_context, entry_audit, monitor_state):
    pool = _first_value(
        entry_audit.get("pool"),
        entry_audit.get("poolAddress"),
        monitor_state.get("poolAddress"),
        monitor_state.get("pool"),
        signal_context.get("canonical_pool_group"),
        row.get("lifecycle_id"),
    )
    return str(pool) if pool is not None else None


def _derive_quote_mint(row, entry_audit, signal_context):
    return _first_value(
        entry_audit.get("inputMint"),
        signal_context.get("quote_mint"),
        row.get("quote_mint"),
        "SOL",
    )


def _derive_quote_ts(row, entry_audit):
    return _quote_ts_seconds(_first_value(entry_audit.get("quoteTs"), row.get("entry_ts"), row.get("signal_ts")))


def _derive_quote_token(row, entry_audit):
    return _first_value(entry_audit.get("outputMint"), entry_audit.get("tokenCA"), row.get("token_ca"))


def _close_enough(left, right, tolerance=1e-9):
    left_num = _as_float(left)
    right_num = _as_float(right)
    if left_num is None or right_num is None:
        return left == right
    return abs(left_num - right_num) <= tolerance


def _mismatch_fields(intent, quote):
    mismatches = []
    for field in ("side", "route", "pool", "quote_mint", "token_ca"):
        quote_value = quote.get(field)
        if quote_value is not None and intent.get(field) is not None and str(quote_value) != str(intent.get(field)):
            mismatches.append(field)
    for field in ("size", "slippage_bps"):
        quote_value = quote.get(field)
        if quote_value is not None and intent.get(field) is not None and not _close_enough(quote_value, intent.get(field)):
            mismatches.append(field)
    return sorted(set(mismatches))


def _missing_fields(intent):
    missing = []
    for field in REQUIRED_BINDING_FIELDS:
        value = intent.get(field)
        if value is None:
            missing.append(field)
            continue
        if isinstance(value, str) and not value.strip():
            missing.append(field)
    if not _non_empty_text(intent.get("token_ca")):
        missing.append("token_ca")
    return sorted(set(missing))


def _proof_level(entry_audit):
    if entry_audit.get("success") is True and entry_audit.get("requestId"):
        return "provider_quote_request_id"
    if entry_audit.get("success") is True:
        return "entry_execution_audit"
    return "legacy_paper_trade_entry_price_proxy"


def _quote_intent_binding_payload(
    row,
    signal_context,
    *,
    binding_policy_version=DEFAULT_BINDING_POLICY_VERSION,
    quote_source=DEFAULT_QUOTE_SOURCE,
    legacy_size_sol=DEFAULT_LEGACY_SIZE_SOL,
    legacy_slippage_bps=DEFAULT_LEGACY_SLIPPAGE_BPS,
):
    entry_audit = _parse_json_object(row.get("entry_execution_audit_json"))
    exit_audit = _parse_json_object(row.get("exit_execution_audit_json"))
    monitor_state = _parse_json_object(row.get("monitor_state_json"))
    token_ca = row.get("token_ca")
    chain = signal_context.get("chain") or row.get("chain") or "solana"
    pool = _derive_pool(row, signal_context, entry_audit, monitor_state) or "unknown_pool"
    lifecycle_epoch = signal_context.get("lifecycle_epoch") or row.get("lifecycle_epoch") or 0
    route, route_source = _derive_route(row, entry_audit, monitor_state, pool)
    size, size_source = _derive_size(row, entry_audit, exit_audit, monitor_state, legacy_size_sol=legacy_size_sol)
    slippage_bps, slippage_source = _derive_slippage_bps(entry_audit, monitor_state, legacy_slippage_bps=legacy_slippage_bps)
    quote_ts = _derive_quote_ts(row, entry_audit)
    quote_mint = _derive_quote_mint(row, entry_audit, signal_context)
    quote_token_ca = _derive_quote_token(row, entry_audit)
    side = _first_value(entry_audit.get("side"), "buy")
    quote_intent_id = _first_value(entry_audit.get("quoteIntentId"), row.get("id"))
    quote = {
        "quote_intent_id": quote_intent_id,
        "side": side,
        "size": _first_value(entry_audit.get("inputAmount"), size),
        "route": _first_value(entry_audit.get("route"), entry_audit.get("routeId"), route),
        "pool": _first_value(entry_audit.get("pool"), entry_audit.get("poolAddress"), pool),
        "quote_mint": quote_mint,
        "slippage_bps": _first_value(entry_audit.get("slippageBps"), slippage_bps),
        "quote_ts": quote_ts,
        "token_ca": quote_token_ca,
    }
    intent = {
        "quote_intent_id": quote_intent_id,
        "side": side,
        "size": size,
        "route": route,
        "pool": pool,
        "quote_mint": quote_mint,
        "slippage_bps": slippage_bps,
        "quote_ts": quote_ts,
        "token_ca": token_ca,
    }
    missing_fields = _missing_fields(intent)
    mismatch_fields = _mismatch_fields(intent, quote)
    used_future_peak = False
    used_future_outcome = False
    used_posthoc_label = False
    forbidden_future_fields_used = []
    quote_intent_bound = not missing_fields and not mismatch_fields
    binding_material = {
        "binding_policy_version": binding_policy_version,
        "quote_intent_id": quote_intent_id,
        "side": side,
        "size": size,
        "route": route,
        "pool": pool,
        "quote_mint": quote_mint,
        "slippage_bps": slippage_bps,
        "quote_ts": quote_ts,
        "token_ca": token_ca,
    }
    intent_hash = sha256_hex(binding_material)
    quote_hash = sha256_hex(quote)
    binding_hash = sha256_hex({"intent": binding_material, "quote": quote, "mismatch_fields": mismatch_fields})
    return {
        "paper_trade_id": row.get("id"),
        "premium_signal_id": signal_context.get("premium_signal_id"),
        "telegram_signal_id": signal_context.get("telegram_signal_id"),
        "token_ca": token_ca,
        "symbol": row.get("symbol"),
        "chain": chain,
        "canonical_pool_group": pool,
        "lifecycle_epoch": lifecycle_epoch,
        "binding_policy_version": binding_policy_version,
        "quote_intent_binding_version": binding_policy_version,
        "quote_intent_id": quote_intent_id,
        "side": side,
        "size": size,
        "route": route,
        "pool": pool,
        "quote_mint": quote_mint,
        "slippage_bps": slippage_bps,
        "quote_ts": quote_ts,
        "quote_ts_iso": _timestamp_to_iso(quote_ts),
        "decision_available_at": _timestamp_to_iso(quote_ts),
        "quote_source": quote_source if entry_audit.get("success") is True else "legacy_paper_trade_entry_price_proxy",
        "quote_binding_proof_level": _proof_level(entry_audit),
        "quote_intent_bound": quote_intent_bound,
        "quote_intent_binding_quality": (
            "provider_quote_request_id_bound"
            if entry_audit.get("success") is True and entry_audit.get("requestId")
            else "entry_execution_audit_bound"
            if entry_audit.get("success") is True
            else "legacy_paper_trade_entry_price_proxy_bound"
        ),
        "intent_hash": intent_hash,
        "quote_hash": quote_hash,
        "quote_binding_hash": binding_hash,
        "intent_fields": binding_material,
        "quote_fields": quote,
        "missing_fields": missing_fields,
        "mismatch_fields": mismatch_fields,
        "field_sources": {
            "size": size_source,
            "route": route_source,
            "pool": "entry_audit_or_signal_or_lifecycle",
            "slippage_bps": slippage_source,
            "quote_ts": "entry_execution_audit.quoteTs" if entry_audit.get("quoteTs") else "paper_trades.entry_ts",
        },
        "used_future_peak": used_future_peak,
        "used_future_outcome": used_future_outcome,
        "used_posthoc_label": used_posthoc_label,
        "forbidden_future_fields_used": forbidden_future_fields_used,
        "entry_execution_audit": entry_audit,
        "exit_execution_audit": exit_audit,
        "legacy_paper_trade": row,
    }


def mirror_quote_intent_bindings(
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
    binding_policy_version=DEFAULT_BINDING_POLICY_VERSION,
    quote_source=DEFAULT_QUOTE_SOURCE,
    legacy_size_sol=DEFAULT_LEGACY_SIZE_SOL,
    legacy_slippage_bps=DEFAULT_LEGACY_SLIPPAGE_BPS,
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
        "binding_policy_version": binding_policy_version,
        "quote_source": quote_source,
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
                payload = _quote_intent_binding_payload(
                    row,
                    signal_context,
                    binding_policy_version=binding_policy_version,
                    quote_source=quote_source,
                    legacy_size_sol=legacy_size_sol,
                    legacy_slippage_bps=legacy_slippage_bps,
                )
                aggregate_id = ":".join(
                    [
                        "quote_intent_binding",
                        str(payload.get("chain") or "unknown_chain"),
                        str(payload.get("token_ca")),
                        str(payload.get("canonical_pool_group") or "unknown_pool"),
                        str(payload.get("lifecycle_epoch", 0)),
                        str(payload.get("quote_intent_id")),
                    ]
                )
                result = V27EventLog(event_log_dir).append_event(
                    event_type=QUOTE_INTENT_BINDING_EVENT_TYPE,
                    aggregate_id=aggregate_id,
                    payload=payload,
                    source="paper_trades",
                    idempotency_key=f"quote_intent_binding:{row.get('id')}:{binding_policy_version}",
                    observed_at=payload.get("decision_available_at") or _timestamp_to_iso(payload.get("quote_ts")),
                    available_at=payload.get("decision_available_at") or _timestamp_to_iso(payload.get("quote_ts")),
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


def _mirrored_quote_intent_binding_ids(event_log_dir, *, binding_policy_version=None):
    counts = {}
    if not (Path(event_log_dir) / "events.jsonl").exists():
        return counts
    for event in V27EventLog(event_log_dir).iter_events():
        if event.get("event_type") != QUOTE_INTENT_BINDING_EVENT_TYPE:
            continue
        payload = event.get("payload") or {}
        if binding_policy_version and payload.get("binding_policy_version") != binding_policy_version:
            continue
        paper_trade_id = payload.get("paper_trade_id")
        if paper_trade_id is None:
            continue
        counts[int(paper_trade_id)] = counts.get(int(paper_trade_id), 0) + 1
    return counts


def max_mirrored_quote_intent_binding_id(event_log_dir, *, binding_policy_version=DEFAULT_BINDING_POLICY_VERSION):
    mirrored = _mirrored_quote_intent_binding_ids(event_log_dir, binding_policy_version=binding_policy_version)
    return max(mirrored) if mirrored else None


def next_unmirrored_quote_intent_binding_since_id(event_log_dir, configured_since_id=None, *, binding_policy_version=DEFAULT_BINDING_POLICY_VERSION):
    max_id = max_mirrored_quote_intent_binding_id(event_log_dir, binding_policy_version=binding_policy_version)
    if max_id is None:
        return configured_since_id
    next_id = int(max_id) + 1
    if configured_since_id is not None:
        return max(next_id, int(configured_since_id))
    return next_id


def verify_quote_intent_binding_mirror_parity(
    paper_db_path,
    event_log_dir,
    *,
    since_id=None,
    until_id=None,
    limit=None,
    table="paper_trades",
    binding_policy_version=DEFAULT_BINDING_POLICY_VERSION,
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
        "binding_policy_version": binding_policy_version,
    }
    with _connect(paper_db_path) as db:
        db_ids = _paper_trade_ids_from_db(db, since_id=since_id, until_id=until_id, limit=limit, table=table)
    mirrored_counts = _mirrored_quote_intent_binding_ids(event_log_dir, binding_policy_version=binding_policy_version)
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
        since_id = next_unmirrored_quote_intent_binding_since_id(
            args.event_log_dir,
            configured_since_id=since_id,
            binding_policy_version=args.binding_policy_version,
        )
    mirror_summary = mirror_quote_intent_bindings(
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
        binding_policy_version=args.binding_policy_version,
        quote_source=args.quote_source,
        legacy_size_sol=args.legacy_size_sol,
        legacy_slippage_bps=args.legacy_slippage_bps,
    )
    verify_summary = None
    if not args.dry_run:
        verify_summary = verify_quote_intent_binding_mirror_parity(
            args.paper_db,
            args.event_log_dir,
            since_id=since_id,
            until_id=args.until_id,
            limit=args.limit,
            table=args.table,
            binding_policy_version=args.binding_policy_version,
        )
    return {
        "cursor": {
            "new_only": bool(getattr(args, "new_only", False)),
            "since_id": since_id,
            "until_id": args.until_id,
            "limit": args.limit,
            "max_mirrored_paper_trade_id": max_mirrored_quote_intent_binding_id(
                args.event_log_dir,
                binding_policy_version=args.binding_policy_version,
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
    parser.add_argument("--binding-policy-version", default=os.environ.get("V27_QUOTE_INTENT_BINDING_VERSION", DEFAULT_BINDING_POLICY_VERSION))
    parser.add_argument("--quote-source", default=os.environ.get("V27_QUOTE_INTENT_BINDING_QUOTE_SOURCE", DEFAULT_QUOTE_SOURCE))
    parser.add_argument("--legacy-size-sol", type=float, default=float(os.environ.get("V27_QUOTE_INTENT_LEGACY_SIZE_SOL", DEFAULT_LEGACY_SIZE_SOL)))
    parser.add_argument("--legacy-slippage-bps", type=float, default=float(os.environ.get("V27_QUOTE_INTENT_LEGACY_SLIPPAGE_BPS", DEFAULT_LEGACY_SLIPPAGE_BPS)))
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

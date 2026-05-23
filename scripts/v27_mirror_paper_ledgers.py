#!/usr/bin/env python3
"""Mirror paper position/capital ledger evidence into v2.7 events."""

import argparse
import fcntl
import json
import os
import signal
import sys
import time
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
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
from v27_mirror_trade_outcomes import _as_int, _signal_context, _timestamp_to_iso  # noqa: E402


DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
DEFAULT_LOCK_FILE = Path("/tmp/v27_paper_ledger_mirror.lock")
PAPER_LEDGER_EVENT_TYPE = "paper_ledger_recorded"
DEFAULT_LEDGER_VERSION = "legacy_paper_position_capital_ledger_v0.1"
DEFAULT_CAPITAL_BASIS_SOL = Decimal(os.environ.get("V27_PAPER_LEDGER_CAPITAL_BASIS_SOL", "100"))
DEFAULT_POSITION_SIZE_SOL = Decimal(os.environ.get("V27_PAPER_LEDGER_DEFAULT_POSITION_SIZE_SOL", "0.06"))
DEFAULT_RESERVATION_TTL_SEC = Decimal(os.environ.get("V27_PAPER_LEDGER_RESERVATION_TTL_SEC", "20"))
QUANT = Decimal("0.000000000001")


def _decimal(value, default=None):
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return default


def _money(value):
    value = _decimal(value, Decimal("0")) or Decimal("0")
    return format(value.quantize(QUANT, rounding=ROUND_HALF_UP), "f")


def _positive_money(value):
    parsed = _decimal(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed


def _position_size(row, *, default_position_size_sol=DEFAULT_POSITION_SIZE_SOL):
    explicit = _positive_money(_value(row, "position_size_sol"))
    if explicit is not None:
        return explicit, "paper_trades.position_size_sol"
    raw_amount = _positive_money(_value(row, "token_amount_raw"))
    decimals = _as_int(_value(row, "token_decimals"))
    if decimals is None:
        decimals = 0
    entry_price = _positive_money(_value(row, "entry_price"))
    if raw_amount is not None and entry_price is not None and decimals >= 0:
        token_amount = raw_amount / (Decimal(10) ** Decimal(decimals))
        derived = token_amount * entry_price
        if derived > 0:
            return derived, "token_amount_raw_x_entry_price"
    return Decimal(default_position_size_sol), "legacy_configured_default_position_size_sol"


def _row_state_hash(row, *, position_size_sol, size_source):
    material = {
        "paper_trade_id": _value(row, "id"),
        "entry_price": str(_value(row, "entry_price")),
        "entry_ts": str(_value(row, "entry_ts")),
        "exit_price": str(_value(row, "exit_price")),
        "exit_ts": str(_value(row, "exit_ts")),
        "exit_reason": str(_value(row, "exit_reason")),
        "pnl_pct": str(_value(row, "pnl_pct")),
        "position_size_sol": _money(position_size_sol),
        "size_source": size_source,
        "synthetic_close": str(_value(row, "synthetic_close")),
        "accounting_outcome": str(_value(row, "accounting_outcome")),
    }
    return sha256_hex(material)


def _latest_ledger_events(event_log_dir, *, ledger_version=DEFAULT_LEDGER_VERSION):
    latest = {}
    if not (Path(event_log_dir) / "events.jsonl").exists():
        return latest
    for event in V27EventLog(event_log_dir).iter_events():
        if event.get("event_type") != PAPER_LEDGER_EVENT_TYPE:
            continue
        payload = event.get("payload") or {}
        if ledger_version and payload.get("paper_ledger_version") != ledger_version:
            continue
        paper_trade_id = payload.get("paper_trade_id")
        if paper_trade_id is None:
            continue
        latest[int(paper_trade_id)] = payload
    return latest


def _initial_capital_state(event_log_dir, *, capital_basis_sol, ledger_version=DEFAULT_LEDGER_VERSION):
    latest = _latest_ledger_events(event_log_dir, ledger_version=ledger_version)
    if not latest:
        return {
            "capital_basis_sol": Decimal(capital_basis_sol),
            "available_capital": Decimal(capital_basis_sol),
            "reserved_capital": Decimal("0"),
            "open_exposure": Decimal("0"),
            "realized_pnl_sol": Decimal("0"),
            "fees_sol": Decimal("0"),
        }
    last_payload = max(latest.values(), key=lambda payload: (payload.get("paper_trade_id") or 0, payload.get("ledger_checkpoint_id") or ""))
    return {
        "capital_basis_sol": _decimal(last_payload.get("capital_basis_sol"), Decimal(capital_basis_sol)),
        "available_capital": _decimal(last_payload.get("available_capital"), Decimal(capital_basis_sol)),
        "reserved_capital": _decimal(last_payload.get("reserved_capital"), Decimal("0")),
        "open_exposure": _decimal(last_payload.get("open_exposure"), Decimal("0")),
        "realized_pnl_sol": _decimal(last_payload.get("realized_pnl_sol"), Decimal("0")),
        "fees_sol": _decimal(last_payload.get("fees_sol"), Decimal("0")),
    }


def _apply_previous_position(capital_state, previous_payload):
    if not previous_payload:
        return capital_state
    state = dict(capital_state)
    previous_remaining = _decimal(previous_payload.get("remaining_size"), Decimal("0"))
    previous_realized = _decimal(previous_payload.get("position_realized_pnl_sol"), Decimal("0"))
    state["open_exposure"] = max(Decimal("0"), state["open_exposure"] - previous_remaining)
    state["available_capital"] = state["available_capital"] + previous_remaining - previous_realized
    state["realized_pnl_sol"] = state["realized_pnl_sol"] - previous_realized
    return state


def _paper_ledger_payload(
    row,
    signal_context,
    capital_state,
    previous_payload,
    *,
    ledger_version=DEFAULT_LEDGER_VERSION,
    environment_id=DEFAULT_ENVIRONMENT_ID,
    capital_basis_sol=DEFAULT_CAPITAL_BASIS_SOL,
    default_position_size_sol=DEFAULT_POSITION_SIZE_SOL,
    reservation_ttl_sec=DEFAULT_RESERVATION_TTL_SEC,
):
    paper_trade_id = _value(row, "id")
    route = _route(row)
    token_lifecycle_key = _token_lifecycle_key(row, signal_context)
    decision_id = f"paper_trade:{paper_trade_id}:entry_decision"
    execution_id = f"paper_trade:{paper_trade_id}:entry_execution"
    position_id = f"paper_trade:{paper_trade_id}:position"
    chain = signal_context.get("chain") or _value(row, "chain") or "solana"
    pool = signal_context.get("canonical_pool_group") or _value(row, "lifecycle_id") or "unknown_pool"
    lifecycle_epoch = signal_context.get("lifecycle_epoch") or _value(row, "lifecycle_epoch") or 0
    entry_size, size_source = _position_size(row, default_position_size_sol=default_position_size_sol)
    row_state_hash = _row_state_hash(row, position_size_sol=entry_size, size_source=size_source)
    exit_ts = _value(row, "exit_ts")
    is_closed = exit_ts is not None and str(exit_ts).strip() != ""
    pnl_pct = _decimal(_value(row, "pnl_pct"), Decimal("0")) or Decimal("0")
    position_realized = (entry_size * pnl_pct / Decimal("100")) if is_closed else Decimal("0")
    remaining_size = Decimal("0") if is_closed else entry_size
    position_status = "closed" if is_closed else "open"

    state = _apply_previous_position(capital_state, previous_payload)
    state.setdefault("capital_basis_sol", Decimal(capital_basis_sol))
    state.setdefault("available_capital", Decimal(capital_basis_sol))
    state.setdefault("reserved_capital", Decimal("0"))
    state.setdefault("open_exposure", Decimal("0"))
    state.setdefault("realized_pnl_sol", Decimal("0"))
    state.setdefault("fees_sol", Decimal("0"))
    state["open_exposure"] = state["open_exposure"] + remaining_size
    state["available_capital"] = state["available_capital"] - remaining_size + position_realized
    state["realized_pnl_sol"] = state["realized_pnl_sol"] + position_realized
    state["reserved_capital"] = Decimal("0")

    reservation_material = {
        "environment_id": environment_id,
        "position_id": position_id,
        "execution_id": execution_id,
        "entry_size_sol": _money(entry_size),
        "row_state_hash": row_state_hash,
    }
    reservation_id = f"reservation:{environment_id}:{sha256_hex(reservation_material)[:24]}"
    release_reason = "position_closed" if is_closed else "entry_filled_open_position"
    position_material = {
        "position_id": position_id,
        "decision_id": decision_id,
        "execution_id": execution_id,
        "entry_size_sol": _money(entry_size),
        "remaining_size": _money(remaining_size),
        "position_status": position_status,
        "row_state_hash": row_state_hash,
    }
    capital_ledger_id = f"capital_ledger:{environment_id}:paper:{paper_trade_id}:{row_state_hash[:12]}"
    invariant_lhs = state["available_capital"] + state["reserved_capital"] + state["open_exposure"] - state["realized_pnl_sol"] + state["fees_sol"]
    invariant_rhs = state["capital_basis_sol"]
    invariant_delta = invariant_lhs - invariant_rhs
    capital_material = {
        "capital_ledger_id": capital_ledger_id,
        "capital_basis_sol": _money(state["capital_basis_sol"]),
        "available_capital": _money(state["available_capital"]),
        "reserved_capital": _money(state["reserved_capital"]),
        "open_exposure": _money(state["open_exposure"]),
        "realized_pnl_sol": _money(state["realized_pnl_sol"]),
        "fees_sol": _money(state["fees_sol"]),
    }
    ledger_checkpoint_id = f"ledger_checkpoint:{environment_id}:paper:{paper_trade_id}:{row_state_hash[:12]}"
    ledger_material = {
        **capital_material,
        "ledger_checkpoint_id": ledger_checkpoint_id,
        "invariant_formula": "available_capital + reserved_capital + open_exposure - realized_pnl_sol + fees_sol == capital_basis_sol",
        "invariant_lhs": _money(invariant_lhs),
        "invariant_rhs": _money(invariant_rhs),
    }
    position_hash = sha256_hex(position_material)
    capital_hash = sha256_hex(capital_material)
    ledger_hash = sha256_hex(ledger_material)
    payload = {
        "paper_trade_id": paper_trade_id,
        "premium_signal_id": signal_context.get("premium_signal_id"),
        "telegram_signal_id": signal_context.get("telegram_signal_id"),
        "token_ca": _value(row, "token_ca"),
        "symbol": _value(row, "symbol"),
        "chain": chain,
        "canonical_pool_group": pool,
        "lifecycle_epoch": lifecycle_epoch,
        "paper_ledger_version": ledger_version,
        "decision_id": decision_id,
        "execution_id": execution_id,
        "token_lifecycle_key": token_lifecycle_key,
        "route": route,
        "environment_id": environment_id,
        "position_id": position_id,
        "position_status": position_status,
        "entry_size_sol": _money(entry_size),
        "remaining_size": _money(remaining_size),
        "position_realized_pnl_sol": _money(position_realized),
        "size_source": size_source,
        "position_ledger_material": position_material,
        "position_ledger_hash": position_hash,
        "capital_ledger_id": capital_ledger_id,
        "capital_basis_sol": _money(state["capital_basis_sol"]),
        "available_capital": _money(state["available_capital"]),
        "reserved_capital": _money(state["reserved_capital"]),
        "open_exposure": _money(state["open_exposure"]),
        "realized_pnl_sol": _money(state["realized_pnl_sol"]),
        "unrealized_pnl_sol": "0.000000000000",
        "fees_sol": _money(state["fees_sol"]),
        "capital_ledger_material": capital_material,
        "capital_ledger_hash": capital_hash,
        "ledger_checkpoint_id": ledger_checkpoint_id,
        "ledger_hash_material": ledger_material,
        "ledger_hash": ledger_hash,
        "invariant_formula": ledger_material["invariant_formula"],
        "invariant_lhs": _money(invariant_lhs),
        "invariant_rhs": _money(invariant_rhs),
        "invariant_delta": _money(invariant_delta),
        "invariant_ok": abs(invariant_delta) <= QUANT,
        "reservation_id": reservation_id,
        "reservation_status": "released",
        "reservation_ttl_sec": _money(reservation_ttl_sec),
        "release_reason": release_reason,
        "reserved_capital_at_entry": _money(entry_size),
        "ledger_scope": "paper_global_capital_reconstruction",
        "ledger_proof_level": "legacy_paper_trade_row_with_configured_size_fallback",
        "row_state_hash": row_state_hash,
        "observed_at": _timestamp_to_iso(_value(row, "exit_ts") or _value(row, "entry_ts")),
        "legacy_paper_trade": row,
    }
    return payload, state


def mirror_paper_ledgers(
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
    ledger_version=DEFAULT_LEDGER_VERSION,
    environment_id=DEFAULT_ENVIRONMENT_ID,
    capital_basis_sol=DEFAULT_CAPITAL_BASIS_SOL,
    default_position_size_sol=DEFAULT_POSITION_SIZE_SOL,
    reservation_ttl_sec=DEFAULT_RESERVATION_TTL_SEC,
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
        "paper_ledger_version": ledger_version,
        "environment_id": environment_id,
        "capital_basis_sol": _money(capital_basis_sol),
        "default_position_size_sol": _money(default_position_size_sol),
    }
    latest = _latest_ledger_events(event_log_dir, ledger_version=ledger_version)
    capital_state = _initial_capital_state(event_log_dir, capital_basis_sol=capital_basis_sol, ledger_version=ledger_version)
    pending_appends = []
    pending_paper_trade_ids = []
    with _connect(paper_db_path) as paper_db, _connect(signal_db_path) as signal_db:
        for row in iter_paper_trade_rows(paper_db, since_id=since_id, until_id=until_id, limit=limit, table=table):
            row = _row_dict(row)
            summary["read_rows"] += 1
            try:
                signal_context = _signal_context(signal_db, row, signal_table=signal_table, default_chain=default_chain)
                previous_payload = latest.get(int(row.get("id")))
                payload, capital_state = _paper_ledger_payload(
                    row,
                    signal_context,
                    capital_state,
                    previous_payload,
                    ledger_version=ledger_version,
                    environment_id=environment_id,
                    capital_basis_sol=capital_basis_sol,
                    default_position_size_sol=default_position_size_sol,
                    reservation_ttl_sec=reservation_ttl_sec,
                )
                latest[int(row.get("id"))] = payload
                if dry_run:
                    continue
                aggregate_id = ":".join(
                    [
                        "paper_ledger",
                        str(payload.get("chain") or "unknown_chain"),
                        str(payload.get("token_ca")),
                        str(payload.get("canonical_pool_group") or "unknown_pool"),
                        str(payload.get("lifecycle_epoch", 0)),
                        str(payload.get("paper_trade_id")),
                    ]
                )
                pending_appends.append(
                    {
                        "event_type": PAPER_LEDGER_EVENT_TYPE,
                        "aggregate_id": aggregate_id,
                        "payload": payload,
                        "source": "paper_trades",
                        "idempotency_key": f"paper_ledger:{environment_id}:{paper_trade_id_key(row)}:{payload.get('row_state_hash')}:{ledger_version}",
                        "observed_at": payload.get("observed_at"),
                        "available_at": payload.get("observed_at"),
                    }
                )
                pending_paper_trade_ids.append(row.get("id"))
            except Exception as exc:
                summary["failed"] += 1
                summary["failures"].append({"paper_trade_id": row.get("id"), "reason": str(exc)})
                continue
    if dry_run or not pending_appends:
        return summary
    try:
        results = V27EventLog(event_log_dir).append_events(pending_appends)
    except Exception as exc:
        summary["failed"] += len(pending_appends)
        summary["failures"].append({"paper_trade_id": pending_paper_trade_ids, "reason": str(exc)})
        return summary
    for paper_trade_id, result in zip(pending_paper_trade_ids, results):
        status = result.get("status")
        if status == "appended":
            summary["appended"] += 1
        elif status == "duplicate":
            summary["duplicate"] += 1
        else:
            summary["failed"] += 1
            summary["failures"].append({"paper_trade_id": paper_trade_id, "reason": f"unexpected status {status}"})
    return summary


def _paper_trade_ids_from_db(db, *, since_id=None, until_id=None, limit=None, table="paper_trades"):
    return [row["id"] for row in iter_paper_trade_rows(db, since_id=since_id, until_id=until_id, limit=limit, table=table)]


def _mirrored_paper_ledger_state_hashes(event_log_dir, *, ledger_version=None):
    state_hashes = {}
    counts = {}
    if not (Path(event_log_dir) / "events.jsonl").exists():
        return state_hashes, counts
    for event in V27EventLog(event_log_dir).iter_events():
        if event.get("event_type") != PAPER_LEDGER_EVENT_TYPE:
            continue
        payload = event.get("payload") or {}
        if ledger_version and payload.get("paper_ledger_version") != ledger_version:
            continue
        paper_trade_id = payload.get("paper_trade_id")
        if paper_trade_id is None:
            continue
        paper_trade_id = int(paper_trade_id)
        state_hashes[paper_trade_id] = payload.get("row_state_hash")
        counts[paper_trade_id] = counts.get(paper_trade_id, 0) + 1
    return state_hashes, counts


def max_mirrored_paper_ledger_id(event_log_dir, *, ledger_version=DEFAULT_LEDGER_VERSION):
    state_hashes, _counts = _mirrored_paper_ledger_state_hashes(event_log_dir, ledger_version=ledger_version)
    return max(state_hashes) if state_hashes else None


def next_unmirrored_paper_ledger_since_id(event_log_dir, configured_since_id=None, *, ledger_version=DEFAULT_LEDGER_VERSION):
    max_id = max_mirrored_paper_ledger_id(event_log_dir, ledger_version=ledger_version)
    if max_id is None:
        return configured_since_id
    next_id = int(max_id) + 1
    if configured_since_id is not None:
        return max(next_id, int(configured_since_id))
    return next_id


def verify_paper_ledger_mirror_parity(
    paper_db_path,
    event_log_dir,
    *,
    since_id=None,
    until_id=None,
    limit=None,
    table="paper_trades",
    ledger_version=DEFAULT_LEDGER_VERSION,
    default_position_size_sol=DEFAULT_POSITION_SIZE_SOL,
):
    summary = {
        "paper_db": str(paper_db_path),
        "event_log_dir": str(event_log_dir),
        "table": table,
        "db_rows": 0,
        "mirrored_events": 0,
        "missing_paper_trade_ids": [],
        "stale_paper_trade_ids": [],
        "event_log_verify": None,
        "event_log_error": None,
        "parity_ok": False,
        "paper_ledger_version": ledger_version,
    }
    expected_hashes = {}
    with _connect(paper_db_path) as db:
        for row in iter_paper_trade_rows(db, since_id=since_id, until_id=until_id, limit=limit, table=table):
            row = _row_dict(row)
            size, source = _position_size(row, default_position_size_sol=default_position_size_sol)
            expected_hashes[int(row["id"])] = _row_state_hash(row, position_size_sol=size, size_source=source)
    mirrored_hashes, mirrored_counts = _mirrored_paper_ledger_state_hashes(event_log_dir, ledger_version=ledger_version)
    db_id_set = set(expected_hashes)
    mirrored_id_set = set(mirrored_hashes)
    summary["db_rows"] = len(expected_hashes)
    summary["mirrored_events"] = sum(mirrored_counts.get(trade_id, 0) for trade_id in db_id_set)
    summary["missing_paper_trade_ids"] = sorted(db_id_set - mirrored_id_set)
    summary["stale_paper_trade_ids"] = sorted(
        trade_id for trade_id in db_id_set & mirrored_id_set if mirrored_hashes.get(trade_id) != expected_hashes.get(trade_id)
    )
    try:
        summary["event_log_verify"] = V27EventLog(event_log_dir).verify()
    except V27EventLogError as exc:
        summary["event_log_error"] = str(exc)
    summary["parity_ok"] = (
        not summary["missing_paper_trade_ids"]
        and not summary["stale_paper_trade_ids"]
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
        since_id = next_unmirrored_paper_ledger_since_id(
            args.event_log_dir,
            configured_since_id=since_id,
            ledger_version=args.ledger_version,
        )
    mirror_summary = mirror_paper_ledgers(
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
        ledger_version=args.ledger_version,
        environment_id=args.environment_id,
        capital_basis_sol=Decimal(str(args.capital_basis_sol)),
        default_position_size_sol=Decimal(str(args.default_position_size_sol)),
        reservation_ttl_sec=Decimal(str(args.reservation_ttl_sec)),
    )
    verify_summary = None
    if not args.dry_run:
        verify_summary = verify_paper_ledger_mirror_parity(
            args.paper_db,
            args.event_log_dir,
            since_id=since_id,
            until_id=args.until_id,
            limit=args.limit,
            table=args.table,
            ledger_version=args.ledger_version,
            default_position_size_sol=Decimal(str(args.default_position_size_sol)),
        )
    return {
        "cursor": {
            "new_only": bool(getattr(args, "new_only", False)),
            "since_id": since_id,
            "until_id": args.until_id,
            "limit": args.limit,
            "max_mirrored_paper_trade_id": max_mirrored_paper_ledger_id(args.event_log_dir, ledger_version=args.ledger_version),
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
    parser.add_argument("--ledger-version", default=os.environ.get("V27_PAPER_LEDGER_VERSION", DEFAULT_LEDGER_VERSION))
    parser.add_argument("--environment-id", default=os.environ.get("V27_ENVIRONMENT_ID", DEFAULT_ENVIRONMENT_ID))
    parser.add_argument("--capital-basis-sol", default=str(DEFAULT_CAPITAL_BASIS_SOL))
    parser.add_argument("--default-position-size-sol", default=str(DEFAULT_POSITION_SIZE_SOL))
    parser.add_argument("--reservation-ttl-sec", default=str(DEFAULT_RESERVATION_TTL_SEC))
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

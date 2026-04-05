#!/usr/bin/env python3

import argparse
import json
import os
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
PAPER_DB = os.environ.get('PAPER_DB', str(DATA_DIR / 'paper_trades.db'))
SENTIMENT_DB = os.environ.get('SENTIMENT_DB', str(DATA_DIR / 'sentiment_arb.db'))
MATCH_WINDOW_MS = 10 * 60 * 1000


def safe_float(value, default=None):
    try:
        if value in (None, ''):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value, default=0):
    try:
        if value in (None, ''):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def load_json(raw):
    if isinstance(raw, dict):
        return raw
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def normalize_signal_type(row):
    signal_type = row['signal_type'] if isinstance(row, sqlite3.Row) else row.get('signal_type')
    if signal_type:
        return str(signal_type).upper()
    description = row['description'] if isinstance(row, sqlite3.Row) and 'description' in row.keys() else row.get('description')
    text = str(description or '')
    if 'New Trending' in text:
        return 'NEW_TRENDING'
    if 'NOT_ATH' not in text and 'ATH' in text.upper():
        return 'ATH'
    return 'UNKNOWN'


def infer_paper_outcome(row):
    exit_reason = row['exit_reason'] if isinstance(row, sqlite3.Row) else row.get('exit_reason')
    last_exit_quote_failure = row['last_exit_quote_failure'] if isinstance(row, sqlite3.Row) else row.get('last_exit_quote_failure')
    exit_reason = str(exit_reason) if exit_reason else None
    if not exit_reason:
        if last_exit_quote_failure:
            return ('infra_exit_unavailable', 'unavailable', 'open', 0)
        return ('entered', 'available', 'open', 0)
    if exit_reason.startswith('trapped_') or exit_reason.startswith('legacy_missing_') or exit_reason == 'manual_cleanup':
        return ('infra_forced_close', 'unavailable', 'closed_synthetic', 1)
    return (exit_reason, 'available', 'closed_real', 0)


def parse_entry_execution(entry_execution_json):
    return load_json(entry_execution_json)


def has_partial_state_gap(token_amount_raw, entry_execution, monitor_state):
    execution = entry_execution if isinstance(entry_execution, dict) else {}
    state = monitor_state if isinstance(monitor_state, dict) else {}
    original_token_amount_raw = safe_int(execution.get('quotedOutAmountRaw'), 0)
    remaining_token_amount_raw = safe_int(token_amount_raw, 0)
    if original_token_amount_raw <= 0 or remaining_token_amount_raw <= 0:
        return False
    if remaining_token_amount_raw >= original_token_amount_raw:
        return False
    partial_state_fields = ('tp1', 'tp2', 'tp3', 'tp4', 'soldPct', 'lockedPnl', 'moonbag')
    return not any(state.get(field) not in (None, False, 0, 0.0, '') for field in partial_state_fields)


def lifecycle_realized_pnl_from_state(state, fallback_position_size_sol=0.0, final_exit_sol=None, has_partial_history=False):
    monitor_state = state if isinstance(state, dict) else {}
    entry_sol = safe_float(monitor_state.get('entrySol'), fallback_position_size_sol)
    total_sol_received = safe_float(monitor_state.get('totalSolReceived'), None)
    final_exit_total_sol = safe_float(final_exit_sol, None)

    if entry_sol <= 0:
        accounting_source = 'monitor_state_total_sol_received' if has_partial_history else 'final_exit_only'
        return None, total_sol_received if has_partial_history else final_exit_total_sol, entry_sol, accounting_source

    if has_partial_history:
        if total_sol_received is None and final_exit_total_sol is not None:
            total_sol_received = final_exit_total_sol
        if total_sol_received is None:
            return None, total_sol_received, entry_sol, 'monitor_state_total_sol_received'
        return ((total_sol_received - entry_sol) / entry_sol), total_sol_received, entry_sol, 'monitor_state_total_sol_received'

    if final_exit_total_sol is not None:
        return ((final_exit_total_sol - entry_sol) / entry_sol), final_exit_total_sol, entry_sol, 'final_exit_only'
    if total_sol_received is not None:
        return ((total_sol_received - entry_sol) / entry_sol), total_sol_received, entry_sol, 'monitor_state_total_sol_received'
    return None, final_exit_total_sol, entry_sol, 'final_exit_only'


def build_signal_index(sentiment_db):
    rows = sentiment_db.execute(
        "SELECT id, token_ca, timestamp, signal_type, description FROM premium_signals ORDER BY id ASC"
    ).fetchall()
    by_token = {}
    for row in rows:
        token_ca = row['token_ca']
        if not token_ca:
            continue
        by_token.setdefault(token_ca, []).append(row)
    return by_token


def best_signal_match(by_token, token_ca, signal_ts):
    candidates = by_token.get(token_ca) or []
    if not candidates:
        return None
    ts = safe_float(signal_ts)
    if ts is None:
        return None
    scored = []
    for row in candidates:
        row_ts = safe_float(row['timestamp'])
        if row_ts is None:
            continue
        delta = abs(row_ts - ts)
        scored.append((delta, row['id'], row))
    if not scored:
        return None
    scored.sort(key=lambda item: (item[0], item[1]))
    return scored[0][2] if scored[0][0] <= MATCH_WINDOW_MS else None


def parse_args():
    parser = argparse.ArgumentParser(description='Repair historical paper trade provenance conservatively')
    parser.add_argument('--db', default=PAPER_DB)
    parser.add_argument('--sentiment-db', default=SENTIMENT_DB)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--only-missing-accounting-source', action='store_true')
    parser.add_argument('--only-synthetic-normalization', action='store_true')
    parser.add_argument('--only-premium-linkage', action='store_true')
    parser.add_argument('--limit', type=int, default=0)
    parser.add_argument('--id', type=int)
    return parser.parse_args()


def should_process(args, row):
    if args.id is not None and row['id'] != args.id:
        return False
    if args.only_missing_accounting_source:
        exit_audit = load_json(row['exit_execution_audit_json']) or {}
        return row['accounting_outcome'] == 'closed_real' and not exit_audit.get('accountingSource')
    if args.only_synthetic_normalization:
        exit_reason = str(row['exit_reason'] or '')
        return exit_reason.startswith('trapped_') or exit_reason.startswith('legacy_missing_') or exit_reason == 'manual_cleanup'
    if args.only_premium_linkage:
        return row['premium_signal_id'] is None
    return True


def main():
    args = parse_args()
    paper_db = sqlite3.connect(args.db)
    paper_db.row_factory = sqlite3.Row
    sentiment_db = None
    signal_index = None
    if os.path.exists(args.sentiment_db):
        sentiment_db = sqlite3.connect(args.sentiment_db)
        sentiment_db.row_factory = sqlite3.Row
        signal_index = build_signal_index(sentiment_db)

    rows = paper_db.execute(
        """
        SELECT id, token_ca, symbol, signal_ts, entry_ts, exit_ts, exit_reason, pnl_pct,
               strategy_stage, strategy_outcome, execution_availability, accounting_outcome, synthetic_close,
               premium_signal_id, signal_type, replay_source, last_exit_quote_failure,
               token_amount_raw, token_decimals, position_size_sol,
               entry_execution_json, entry_execution_audit_json,
               exit_execution_json, exit_execution_audit_json,
               monitor_state_json
        FROM paper_trades
        ORDER BY id ASC
        """
    ).fetchall()

    stats = {
        'rows_scanned': 0,
        'premium_signal_linked': 0,
        'synthetic_outcome_normalized': 0,
        'replay_accounting_source_backfilled': 0,
        'final_exit_only_backfilled': 0,
        'monitor_state_total_sol_received_backfilled': 0,
        'irrecoverable_insufficient_evidence': 0,
        'richer_existing_audit_preserved': 0,
    }

    updates = []
    for row in rows:
        if not should_process(args, row):
            continue
        stats['rows_scanned'] += 1
        update_fields = {}

        if row['premium_signal_id'] is None and signal_index is not None:
            matched = best_signal_match(signal_index, row['token_ca'], row['signal_ts'])
            if matched is not None:
                update_fields['premium_signal_id'] = matched['id']
                update_fields['signal_type'] = row['signal_type'] or matched['signal_type'] or normalize_signal_type(matched)
                stats['premium_signal_linked'] += 1

        next_strategy_outcome, next_execution_availability, next_accounting_outcome, next_synthetic_close = infer_paper_outcome(row)
        synthetic_missing = any(row[field] is None for field in ('strategy_outcome', 'execution_availability', 'accounting_outcome'))
        if synthetic_missing and next_accounting_outcome == 'closed_synthetic':
            update_fields['strategy_outcome'] = row['strategy_outcome'] or next_strategy_outcome
            update_fields['execution_availability'] = row['execution_availability'] or next_execution_availability
            update_fields['accounting_outcome'] = row['accounting_outcome'] or next_accounting_outcome
            update_fields['synthetic_close'] = next_synthetic_close if row['synthetic_close'] is None else row['synthetic_close']
            stats['synthetic_outcome_normalized'] += 1

        exit_audit = load_json(row['exit_execution_audit_json']) or {}
        monitor_state = load_json(row['monitor_state_json']) or {}
        exit_execution = load_json(row['exit_execution_json']) or {}
        entry_execution = parse_entry_execution(row['entry_execution_json']) or {}

        if row['accounting_outcome'] == 'closed_real' and not exit_audit.get('accountingSource'):
            repaired_source = None
            repaired_extra = {}
            if row['replay_source'] == 'real_kline_replay':
                repaired_source = 'replay_exit_only'
                stats['replay_accounting_source_backfilled'] += 1
            else:
                final_exit_sol = safe_float(exit_audit.get('quotedOutAmount'), None)
                if final_exit_sol is None:
                    final_exit_sol = safe_float(exit_execution.get('quotedOutAmount'), None)
                has_partial_history = not has_partial_state_gap(row['token_amount_raw'], entry_execution, monitor_state)
                _, total_sol, _, accounting_source = lifecycle_realized_pnl_from_state(
                    monitor_state,
                    fallback_position_size_sol=safe_float(row['position_size_sol'], 0.0),
                    final_exit_sol=final_exit_sol,
                    has_partial_history=has_partial_history,
                )
                if accounting_source == 'monitor_state_total_sol_received' and safe_float(monitor_state.get('totalSolReceived'), None) is not None:
                    repaired_source = accounting_source
                    repaired_extra['postExitTotalSolReceived'] = total_sol
                    stats['monitor_state_total_sol_received_backfilled'] += 1
                elif accounting_source == 'final_exit_only' and final_exit_sol is not None:
                    repaired_source = accounting_source
                    repaired_extra['exitSolReceived'] = final_exit_sol
                    stats['final_exit_only_backfilled'] += 1

            if repaired_source:
                if exit_audit:
                    stats['richer_existing_audit_preserved'] += 1
                exit_audit['accountingSource'] = repaired_source
                for key, value in repaired_extra.items():
                    if exit_audit.get(key) is None:
                        exit_audit[key] = value
                update_fields['exit_execution_audit_json'] = json.dumps(exit_audit, ensure_ascii=False, sort_keys=True)
            else:
                stats['irrecoverable_insufficient_evidence'] += 1

        if update_fields:
            updates.append((row['id'], update_fields))

    if not args.dry_run:
        for row_id, update_fields in updates:
            assignments = []
            params = []
            for key, value in update_fields.items():
                assignments.append(f"{key} = ?")
                params.append(value)
            params.append(row_id)
            paper_db.execute(f"UPDATE paper_trades SET {', '.join(assignments)} WHERE id = ?", params)
        paper_db.commit()

    print(json.dumps({
        'mode': 'dry-run' if args.dry_run else 'live',
        'stats': stats,
        'updates': [
            {'id': row_id, 'fields': sorted(fields.keys())}
            for row_id, fields in updates[:50]
        ],
        'total_updates': len(updates),
    }, ensure_ascii=False, indent=2, sort_keys=True))

    paper_db.close()
    if sentiment_db is not None:
        sentiment_db.close()


if __name__ == '__main__':
    main()

#!/usr/bin/env python3

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
PAPER_DB = os.environ.get('PAPER_DB', str(DATA_DIR / 'paper_trades.db'))
SENTIMENT_DB = os.environ.get('SENTIMENT_DB', str(DATA_DIR / 'sentiment_arb.db'))
MATCH_WINDOW_MS = 10 * 60 * 1000


def parse_args():
    parser = argparse.ArgumentParser(description='Print paper trade execution audit payloads')
    parser.add_argument('--db', default=PAPER_DB, help='Path to paper_trades SQLite DB')
    parser.add_argument('--sentiment-db', default=SENTIMENT_DB, help='Path to sentiment_arb SQLite DB')
    parser.add_argument('--limit', type=int, default=10, help='Number of recent rows to print')
    parser.add_argument('--id', type=int, help='Print a specific paper_trades row by id')
    parser.add_argument('--premium-signal-id', type=int, help='Filter by premium_signal_id')
    parser.add_argument('--open-only', action='store_true', help='Only include open positions')
    parser.add_argument('--closed-only', action='store_true', help='Only include closed positions')
    parser.add_argument('--symbol', help='Filter by symbol')
    parser.add_argument('--token', help='Filter by token_ca')
    parser.add_argument('--recent-closed', action='store_true', help='Shortcut for recent closed rows ordered by latest id')
    parser.add_argument('--anomaly-only', action='store_true', help='Only include rows with audit/accounting anomalies')
    return parser.parse_args()


def load_json(raw):
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        return {'_decode_error': str(exc), '_raw': raw}


def format_json(payload):
    if payload is None:
        return 'null'
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)


def compact_json(payload):
    if payload is None:
        return 'null'
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def get_table_columns(db, table_name):
    rows = db.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row['name'] for row in rows}


def safe_float(value):
    try:
        if value in (None, ''):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_signal_type(row):
    signal_type = row['signal_type'] if isinstance(row, sqlite3.Row) and 'signal_type' in row.keys() else row.get('signal_type')
    if signal_type:
        return str(signal_type).upper()
    description = row['description'] if isinstance(row, sqlite3.Row) and 'description' in row.keys() else row.get('description')
    text = str(description or '')
    if 'New Trending' in text:
        return 'NEW_TRENDING'
    if 'NOT_ATH' not in text and 'ATH' in text.upper():
        return 'ATH'
    return 'UNKNOWN'


def build_query(args, columns):
    where = []
    params = []

    if args.id is not None:
        where.append('id = ?')
        params.append(args.id)
    if args.premium_signal_id is not None and 'premium_signal_id' in columns:
        where.append('premium_signal_id = ?')
        params.append(args.premium_signal_id)
    if args.open_only and not args.closed_only and not args.recent_closed:
        where.append('exit_reason IS NULL')
    if (args.closed_only or args.recent_closed) and not args.open_only:
        where.append('exit_reason IS NOT NULL')
    if args.symbol:
        where.append('symbol = ?')
        params.append(args.symbol)
    if args.token:
        where.append('token_ca = ?')
        params.append(args.token)

    wanted = [
        'id', 'symbol', 'token_ca', 'strategy_id', 'strategy_stage', 'signal_ts', 'entry_ts', 'exit_ts', 'exit_reason',
        'premium_signal_id', 'signal_type', 'strategy_outcome', 'execution_availability', 'accounting_outcome',
        'synthetic_close', 'last_exit_quote_failure', 'monitor_state_json', 'entry_execution_audit_json',
        'exit_execution_audit_json', 'entry_execution_json', 'exit_execution_json', 'position_size_sol', 'pnl_pct',
    ]
    select_exprs = []
    for column in wanted:
        select_exprs.append(column if column in columns else f'NULL AS {column}')

    sql = f"SELECT {', '.join(select_exprs)} FROM paper_trades"
    if where:
        sql += ' WHERE ' + ' AND '.join(where)
    sql += ' ORDER BY id DESC'
    if args.id is None:
        sql += ' LIMIT ?'
        params.append(max(1, args.limit))
    return sql, params


def build_signal_index(sentiment_db, columns):
    select_parts = [
        'id', 'token_ca', 'timestamp', 'source_message_ts', 'receive_ts', 'signal_type', 'is_ath', 'parse_status',
        'hard_gate_status', 'gate_result', 'description'
    ]
    query_parts = [part if part in columns else f'NULL AS {part}' for part in select_parts]
    rows = sentiment_db.execute(
        f"SELECT {', '.join(query_parts)} FROM premium_signals ORDER BY id ASC"
    ).fetchall()
    by_token = {}
    for row in rows:
        token_ca = row['token_ca'] if 'token_ca' in row.keys() else None
        if not token_ca:
            continue
        by_token.setdefault(token_ca, []).append(row)
    return by_token


def best_signal_match(by_token, token_ca, signal_ts):
    candidates = by_token.get(token_ca) or []
    if not candidates:
        return None
    signal_ts_num = safe_float(signal_ts)
    if signal_ts_num is None:
        return None
    scored = []
    for row in candidates:
        row_ts = safe_float(row['timestamp'] if 'timestamp' in row.keys() else None)
        if row_ts is None:
            continue
        delta = abs(row_ts - signal_ts_num)
        scored.append((delta, row['id'], row))
    if not scored:
        return None
    scored.sort(key=lambda item: (item[0], item[1]))
    return scored[0][2] if scored[0][0] <= MATCH_WINDOW_MS else None


def load_signal_context(args):
    sentiment_path = args.sentiment_db
    if not sentiment_path or not os.path.exists(sentiment_path):
        return None, None, f'sentiment db unavailable: {sentiment_path}'
    db = sqlite3.connect(sentiment_path)
    db.row_factory = sqlite3.Row
    columns = get_table_columns(db, 'premium_signals')
    return db, build_signal_index(db, columns), None


def detect_anomalies(row_dict, entry_audit, exit_audit, monitor_state):
    anomalies = []
    if row_dict.get('synthetic_close'):
        anomalies.append('synthetic_close')
    if row_dict.get('last_exit_quote_failure'):
        anomalies.append('last_exit_quote_failure')
    if row_dict.get('execution_availability') == 'unavailable':
        anomalies.append('execution_unavailable')
    if row_dict.get('accounting_outcome') == 'closed_synthetic':
        anomalies.append('closed_synthetic')

    trigger_pct = safe_float((exit_audit or {}).get('triggerPnlPct'))
    realized_pct = safe_float((exit_audit or {}).get('realizedPnlPct'))
    if trigger_pct is not None and realized_pct is not None and trigger_pct < 0 and realized_pct > 0:
        anomalies.append('negative_trigger_positive_closed')

    failure_reason = (exit_audit or {}).get('failureReason') or (row_dict.get('last_exit_quote_failure'))
    if failure_reason in {'no_route', 'token_not_tradable'}:
        anomalies.append(f'quote_failure:{failure_reason}')

    gate_decision = None
    gate_payload = None
    gate_reason = None
    if isinstance((row_dict.get('signal_context') or {}).get('gate_result'), dict):
        gate_payload = row_dict['signal_context']['gate_result']
        gate_decision = gate_payload.get('gateDecision') or gate_payload.get('status')
        gate_reason = gate_payload.get('gateReason')
    if gate_decision == 'UNKNOWN_DATA':
        anomalies.append('prebuy_unknown_data')
        if gate_reason:
            normalized_gate_reason = str(gate_reason).strip().lower().replace(' ', '_')
            anomalies.append(f'prebuy_unknown_data:{normalized_gate_reason}')

    total_sol_received = safe_float((monitor_state or {}).get('totalSolReceived'))
    final_exit_sol = safe_float((exit_audit or {}).get('quotedOutAmount'))
    accounting_source = (exit_audit or {}).get('accountingSource')
    pre_exit_total = safe_float((exit_audit or {}).get('preExitTotalSolReceived'))
    exit_sol_received = safe_float((exit_audit or {}).get('exitSolReceived'))
    post_exit_total = safe_float((exit_audit or {}).get('postExitTotalSolReceived'))
    if total_sol_received is not None and final_exit_sol is not None and total_sol_received > 0 and final_exit_sol >= 0:
        if total_sol_received > final_exit_sol and accounting_source == 'final_exit_only':
            anomalies.append('monitor_total_exceeds_final_exit')
    if row_dict.get('accounting_outcome') == 'closed_real' and not accounting_source:
        anomalies.append('missing_accounting_source')
    if pre_exit_total is not None and post_exit_total is not None and post_exit_total < pre_exit_total:
        anomalies.append('post_exit_less_than_pre_exit')
    if pre_exit_total is not None and exit_sol_received is not None and post_exit_total is not None:
        expected_post = pre_exit_total + exit_sol_received
        if abs(post_exit_total - expected_post) > 1e-9:
            anomalies.append('post_exit_mismatch')

    return anomalies


def enrich_with_signal(row_dict, signal_index):
    if signal_index is None:
        row_dict['signal_context'] = None
        return row_dict
    signal_context = None
    premium_signal_id = row_dict.get('premium_signal_id')
    if premium_signal_id is not None:
        for rows in signal_index.values():
            for candidate in rows:
                if candidate['id'] == premium_signal_id:
                    signal_context = candidate
                    break
            if signal_context:
                break
    if signal_context is None:
        signal_context = best_signal_match(signal_index, row_dict.get('token_ca'), row_dict.get('signal_ts'))
    if signal_context is None:
        row_dict['signal_context'] = None
        return row_dict
    row_dict['signal_context'] = {
        'id': signal_context['id'],
        'token_ca': signal_context['token_ca'],
        'timestamp': signal_context['timestamp'] if 'timestamp' in signal_context.keys() else None,
        'source_message_ts': signal_context['source_message_ts'] if 'source_message_ts' in signal_context.keys() else None,
        'receive_ts': signal_context['receive_ts'] if 'receive_ts' in signal_context.keys() else None,
        'signal_type': signal_context['signal_type'] if 'signal_type' in signal_context.keys() else normalize_signal_type(signal_context),
        'is_ath': signal_context['is_ath'] if 'is_ath' in signal_context.keys() else None,
        'parse_status': signal_context['parse_status'] if 'parse_status' in signal_context.keys() else None,
        'hard_gate_status': signal_context['hard_gate_status'] if 'hard_gate_status' in signal_context.keys() else None,
        'gate_result': load_json(signal_context['gate_result']) if 'gate_result' in signal_context.keys() else None,
        'description': signal_context['description'] if 'description' in signal_context.keys() else None,
    }
    if row_dict.get('premium_signal_id') is None:
        row_dict['premium_signal_match'] = 'token+nearest_ts<=10m'
    else:
        row_dict['premium_signal_match'] = 'premium_signal_id'
    return row_dict


def print_row(row_dict):
    entry_audit = load_json(row_dict.get('entry_execution_audit_json'))
    exit_audit = load_json(row_dict.get('exit_execution_audit_json'))
    monitor_state = load_json(row_dict.get('monitor_state_json'))
    signal_context = row_dict.get('signal_context')
    anomalies = detect_anomalies(row_dict, entry_audit, exit_audit, monitor_state)
    accounting_source = (exit_audit or {}).get('accountingSource') or '-'
    pre_exit_total = safe_float((exit_audit or {}).get('preExitTotalSolReceived'))
    exit_sol_received = safe_float((exit_audit or {}).get('exitSolReceived'))
    post_exit_total = safe_float((exit_audit or {}).get('postExitTotalSolReceived'))

    summary_parts = [
        'AUDIT_SUMMARY',
        f"id={row_dict.get('id')}",
        f"symbol={row_dict.get('symbol') or '-'}",
        f"token={row_dict.get('token_ca') or '-'}",
        f"stage={row_dict.get('strategy_stage') or '-'}",
        f"exitReason={row_dict.get('exit_reason') or 'OPEN'}",
        f"strategyOutcome={row_dict.get('strategy_outcome') or '-'}",
        f"executionAvailability={row_dict.get('execution_availability') or '-'}",
        f"accountingOutcome={row_dict.get('accounting_outcome') or '-'}",
        f"accountingSource={accounting_source}",
        f"preExitTotalSolReceived={pre_exit_total if pre_exit_total is not None else '-'}",
        f"exitSolReceived={exit_sol_received if exit_sol_received is not None else '-'}",
        f"postExitTotalSolReceived={post_exit_total if post_exit_total is not None else '-'}",
        f"syntheticClose={int(bool(row_dict.get('synthetic_close')))}",
        f"premiumSignalId={row_dict.get('premium_signal_id') if row_dict.get('premium_signal_id') is not None else '-'}",
        f"signalType={row_dict.get('signal_type') or '-'}",
        f"lastExitQuoteFailure={row_dict.get('last_exit_quote_failure') or '-'}",
        f"anomalies={','.join(anomalies) if anomalies else 'none'}",
    ]
    print(' '.join(summary_parts))

    if signal_context:
        gate_payload = signal_context.get('gate_result') if isinstance(signal_context.get('gate_result'), dict) else None
        observability = (gate_payload or {}).get('observability') if isinstance((gate_payload or {}).get('observability'), dict) else {}
        signal_summary = [
            'SIGNAL_SUMMARY',
            f"paperTradeId={row_dict.get('id')}",
            f"premiumSignalId={signal_context.get('id')}",
            f"match={row_dict.get('premium_signal_match') or '-'}",
            f"signalType={signal_context.get('signal_type') or '-'}",
            f"hardGateStatus={signal_context.get('hard_gate_status') or '-'}",
            f"gateDecision={(gate_payload or {}).get('gateDecision') or (gate_payload or {}).get('status') or '-'}",
            f"gateReason={(gate_payload or {}).get('gateReason') or '-'}",
            f"provider={(gate_payload or {}).get('provider') or '-'}",
            f"poolAddress={(gate_payload or {}).get('poolAddress') or '-'}",
            f"freshnessSec={(gate_payload or {}).get('freshnessSec') if (gate_payload or {}).get('freshnessSec') is not None else '-'}",
            f"continuedToBuy={int(bool((gate_payload or {}).get('decisionContinuedToBuy')))}",
            f"providerDataState={observability.get('providerDataState') or '-'}",
            f"freshLocalBarsObserved={int(bool(observability.get('freshLocalBarsObserved')))}",
            f"localWaitTimedOut={int(bool(observability.get('localWaitTimedOut')))}",
        ]
        print(' '.join(signal_summary))

    print('=' * 100)
    print(
        f"row id={row_dict.get('id')} symbol={row_dict.get('symbol') or '-'} token_ca={row_dict.get('token_ca') or '-'} "
        f"stage={row_dict.get('strategy_stage') or '-'} exit_reason={row_dict.get('exit_reason') or 'OPEN'}"
    )
    print(
        f"strategy_outcome={row_dict.get('strategy_outcome') or '-'} | execution_availability={row_dict.get('execution_availability') or '-'} | "
        f"accounting_outcome={row_dict.get('accounting_outcome') or '-'} | synthetic_close={row_dict.get('synthetic_close') or 0}"
    )
    print(
        f"premium_signal_id={row_dict.get('premium_signal_id')} | signal_type={row_dict.get('signal_type') or '-'} | "
        f"signal_ts={row_dict.get('signal_ts')} | entry_ts={row_dict.get('entry_ts')} | exit_ts={row_dict.get('exit_ts')}"
    )
    print(
        f"accounting_source={accounting_source} | pre_exit_total_sol_received={pre_exit_total if pre_exit_total is not None else '-'} | "
        f"exit_sol_received={exit_sol_received if exit_sol_received is not None else '-'} | "
        f"post_exit_total_sol_received={post_exit_total if post_exit_total is not None else '-'}"
    )
    print(f"last_exit_quote_failure={row_dict.get('last_exit_quote_failure') or '-'} | pnl_pct={row_dict.get('pnl_pct')}")
    print(f"anomalies={anomalies if anomalies else []}")
    print('-- entry_execution_audit_json --')
    print(format_json(entry_audit))
    print('-- exit_execution_audit_json --')
    print(format_json(exit_audit))
    print('-- monitor_state_json --')
    print(format_json(monitor_state))
    print('-- premium_signal_context --')
    print(format_json(signal_context))


def main():
    args = parse_args()

    if args.open_only and args.closed_only:
        print('cannot combine --open-only and --closed-only', file=sys.stderr)
        return 2

    db_path = args.db
    if not os.path.exists(db_path):
        print(f'paper db not found: {db_path}', file=sys.stderr)
        return 1

    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    try:
        columns = get_table_columns(db, 'paper_trades')
        sql, params = build_query(args, columns)
        rows = db.execute(sql, params).fetchall()
    finally:
        db.close()

    sentiment_db = None
    signal_index = None
    sentiment_warning = None
    try:
        sentiment_db, signal_index, sentiment_warning = load_signal_context(args)
    finally:
        if sentiment_db is not None:
            sentiment_db.close()

    if sentiment_warning:
        print(f'INFO {sentiment_warning}', file=sys.stderr)

    if not rows:
        print('no matching paper_trades rows found')
        return 0

    printed = 0
    for row in rows:
        row_dict = dict(row)
        row_dict = enrich_with_signal(row_dict, signal_index)
        entry_audit = load_json(row_dict.get('entry_execution_audit_json'))
        exit_audit = load_json(row_dict.get('exit_execution_audit_json'))
        monitor_state = load_json(row_dict.get('monitor_state_json'))
        anomalies = detect_anomalies(row_dict, entry_audit, exit_audit, monitor_state)
        if args.anomaly_only and not anomalies:
            continue
        print_row(row_dict)
        printed += 1

    if printed == 0:
        print('no matching paper_trades rows found after anomaly filtering')
        return 0
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
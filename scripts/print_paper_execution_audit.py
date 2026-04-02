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


def parse_args():
    parser = argparse.ArgumentParser(description='Print paper trade execution audit payloads')
    parser.add_argument('--db', default=PAPER_DB, help='Path to paper_trades SQLite DB')
    parser.add_argument('--limit', type=int, default=10, help='Number of recent rows to print')
    parser.add_argument('--id', type=int, help='Print a specific paper_trades row by id')
    parser.add_argument('--open-only', action='store_true', help='Only include open positions')
    parser.add_argument('--closed-only', action='store_true', help='Only include closed positions')
    parser.add_argument('--symbol', help='Filter by symbol')
    parser.add_argument('--token', help='Filter by token_ca')
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


def get_table_columns(db, table_name):
    rows = db.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row['name'] for row in rows}


def build_query(args, columns):
    where = []
    params = []

    if args.id is not None:
        where.append('id = ?')
        params.append(args.id)
    if args.open_only and not args.closed_only:
        where.append('exit_reason IS NULL')
    if args.closed_only and not args.open_only:
        where.append('exit_reason IS NOT NULL')
    if args.symbol:
        where.append('symbol = ?')
        params.append(args.symbol)
    if args.token:
        where.append('token_ca = ?')
        params.append(args.token)

    entry_audit_expr = 'entry_execution_audit_json' if 'entry_execution_audit_json' in columns else 'NULL AS entry_execution_audit_json'
    exit_audit_expr = 'exit_execution_audit_json' if 'exit_execution_audit_json' in columns else 'NULL AS exit_execution_audit_json'

    sql = f"""
        SELECT id, symbol, token_ca, strategy_id, strategy_stage,
               signal_ts, entry_ts, exit_ts, exit_reason,
               {entry_audit_expr}, {exit_audit_expr}
        FROM paper_trades
    """
    if where:
        sql += ' WHERE ' + ' AND '.join(where)
    sql += ' ORDER BY id DESC'
    if args.id is None:
        sql += ' LIMIT ?'
        params.append(max(1, args.limit))
    return sql, params


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

    if not rows:
        print('no matching paper_trades rows found')
        return 0

    for index, row in enumerate(rows, start=1):
        print('=' * 100)
        print(
            f"row #{index} | id={row['id']} | symbol={row['symbol'] or '-'} | stage={row['strategy_stage'] or '-'} | "
            f"exit_reason={row['exit_reason'] or 'OPEN'}"
        )
        print(
            f"token_ca={row['token_ca'] or '-'} | strategy_id={row['strategy_id'] or '-'} | "
            f"signal_ts={row['signal_ts']} | entry_ts={row['entry_ts']} | exit_ts={row['exit_ts']}"
        )
        print('-- entry_execution_audit_json --')
        print(format_json(load_json(row['entry_execution_audit_json'])))
        print('-- exit_execution_audit_json --')
        print(format_json(load_json(row['exit_execution_audit_json'])))

    return 0


if __name__ == '__main__':
    raise SystemExit(main())

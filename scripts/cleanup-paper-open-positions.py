#!/usr/bin/env python3

import os
import sqlite3
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
PAPER_DB = os.environ.get('PAPER_DB', str(DATA_DIR / 'paper_trades.db'))


def get_table_columns(db, table_name):
    rows = db.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row['name'] for row in rows}


def build_cleanup_update(columns, stage_outcome):
    assignments = [
        "exit_price = COALESCE(exit_price, 0)",
        "exit_ts = ?",
        "exit_reason = ?",
        "pnl_pct = ?",
    ]
    if 'stage_outcome' in columns:
        assignments.append("stage_outcome = ?")
    if 'trailing_active' in columns:
        assignments.append("trailing_active = 0")
    if 'exit_execution_json' in columns:
        assignments.append("exit_execution_json = NULL")
    if 'exit_quote_failures' in columns:
        assignments.append("exit_quote_failures = 0")
    if 'last_exit_quote_failure' in columns:
        assignments.append("last_exit_quote_failure = NULL")

    params = [stage_outcome]
    return ",\n              ".join(assignments), params


def main():
    db_path = PAPER_DB
    reason = os.environ.get('PAPER_CLEANUP_REASON', 'manual_cleanup')
    pnl_pct = float(os.environ.get('PAPER_CLEANUP_PNL_PCT', '0'))

    if not os.path.exists(db_path):
        print(f'paper db not found: {db_path}')
        sys.exit(1)

    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    columns = get_table_columns(db, 'paper_trades')

    rows = db.execute(
        "SELECT id, symbol, strategy_stage FROM paper_trades WHERE exit_reason IS NULL ORDER BY id ASC"
    ).fetchall()

    if not rows:
        print('no open paper positions to clean up')
        db.close()
        return

    exit_ts = int(time.time())
    updated = 0
    for row in rows:
        stage = row['strategy_stage'] or 'stage1'
        stage_outcome = f"{stage}_{reason}"
        assignment_sql, extra_params = build_cleanup_update(columns, stage_outcome)
        db.execute(
            f"""
            UPDATE paper_trades
            SET {assignment_sql}
            WHERE id = ?
            """,
            (exit_ts, reason, pnl_pct, *extra_params, row['id'])
        )
        updated += 1

    db.commit()
    db.close()

    print(f'closed {updated} open paper positions with reason={reason} pnl_pct={pnl_pct}')


if __name__ == '__main__':
    main()

#!/usr/bin/env python3

import argparse
import os
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
PAPER_DB = os.environ.get('PAPER_DB', str(DATA_DIR / 'paper_trades.db'))
AUDIT_COLUMNS = [
    ('entry_execution_audit_json', 'TEXT'),
    ('exit_execution_audit_json', 'TEXT'),
]


def parse_args():
    parser = argparse.ArgumentParser(
        description='One-time repair script to add execution audit columns to paper_trades.db'
    )
    parser.add_argument('--db', default=PAPER_DB, help='Path to paper_trades SQLite DB')
    return parser.parse_args()


def get_table_columns(db, table_name):
    rows = db.execute(f'PRAGMA table_info({table_name})').fetchall()
    return {row['name'] for row in rows}


def main():
    args = parse_args()
    db_path = args.db

    if not os.path.exists(db_path):
        print(f'paper db not found: {db_path}', file=sys.stderr)
        return 1

    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row

    try:
        tables = {
            row['name']
            for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        if 'paper_trades' not in tables:
            print('table not found: paper_trades', file=sys.stderr)
            return 1

        existing_columns = get_table_columns(db, 'paper_trades')
        added = []
        skipped = []

        for column_name, column_type in AUDIT_COLUMNS:
            if column_name in existing_columns:
                skipped.append(column_name)
                continue
            db.execute(f'ALTER TABLE paper_trades ADD COLUMN {column_name} {column_type}')
            added.append(column_name)

        db.commit()
        final_columns = get_table_columns(db, 'paper_trades')
    finally:
        db.close()

    print(f'db: {db_path}')
    for column_name in added:
        print(f'added: {column_name}')
    for column_name in skipped:
        print(f'already_present: {column_name}')

    missing_after = [column_name for column_name, _ in AUDIT_COLUMNS if column_name not in final_columns]
    if missing_after:
        print(f'missing_after_migration: {", ".join(missing_after)}', file=sys.stderr)
        return 1

    print('paper_trades audit column migration complete')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

#!/usr/bin/env python3
"""Report stale paper-learning state.

Default mode is report-only. `--apply` is intentionally conservative: it only
marks old complete missed attributions as archived if the column exists. It
never deletes rows.
"""

import argparse
import json
import os
import sqlite3
import time
from pathlib import Path


PAPER_DB = os.environ.get("PAPER_DB", str(Path(__file__).resolve().parent.parent / "data" / "paper_trades.db"))


def table_exists(db, table):
    return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def columns(db, table):
    if not table_exists(db, table):
        return set()
    return {row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}


def maybe_add_archive_column(db):
    if not table_exists(db, "paper_missed_signal_attribution"):
        return
    if "archived_at" not in columns(db, "paper_missed_signal_attribution"):
        try:
            db.execute("ALTER TABLE paper_missed_signal_attribution ADD COLUMN archived_at REAL")
        except sqlite3.OperationalError:
            pass


def build_cleanup_report(db, now_ts):
    report = {
        "generated_at": int(now_ts),
        "policy": {
            "quarantine_hard_loss_gate_ttl_days": 7,
            "missed_attribution_archive_after_hours": 24,
            "decision_event_cold_storage_after_days": 30,
        },
        "tables": {},
    }
    if table_exists(db, "paper_decision_events"):
        old_ts = now_ts - 30 * 24 * 3600
        report["tables"]["paper_decision_events"] = {
            "older_30d": db.execute("SELECT COUNT(*) FROM paper_decision_events WHERE event_ts < ?", (old_ts,)).fetchone()[0],
        }
    if table_exists(db, "paper_missed_signal_attribution"):
        archive_ts = now_ts - 24 * 3600
        cols = columns(db, "paper_missed_signal_attribution")
        archived_predicate = "AND archived_at IS NULL" if "archived_at" in cols else ""
        report["tables"]["paper_missed_signal_attribution"] = {
            "complete_older_24h_unarchived": db.execute(
                f"""
                SELECT COUNT(*)
                FROM paper_missed_signal_attribution
                WHERE COALESCE(created_event_ts, signal_ts, baseline_ts, 0) < ?
                  AND COALESCE(status, '') = 'complete'
                  {archived_predicate}
                """,
                (archive_ts,),
            ).fetchone()[0],
            "archive_column_present": "archived_at" in cols,
        }
    if table_exists(db, "paper_trades"):
        stale_risk_ts = now_ts - 7 * 24 * 3600
        report["tables"]["paper_trades"] = {
            "old_fast_fail_rows_no_longer_hard_gate_active": db.execute(
                """
                SELECT COUNT(*)
                FROM paper_trades
                WHERE exit_ts IS NOT NULL
                  AND exit_ts < ?
                  AND (
                    LOWER(COALESCE(exit_reason, '')) LIKE '%fast_fail%'
                    OR LOWER(COALESCE(exit_reason, '')) LIKE '%no_follow%'
                    OR LOWER(COALESCE(exit_reason, '')) LIKE '%doa%'
                  )
                """,
                (stale_risk_ts,),
            ).fetchone()[0],
        }
    return report


def apply_cleanup(db, now_ts):
    maybe_add_archive_column(db)
    if not table_exists(db, "paper_missed_signal_attribution"):
        return {"archived_missed_attributions": 0}
    archive_ts = now_ts - 24 * 3600
    cur = db.execute(
        """
        UPDATE paper_missed_signal_attribution
        SET archived_at = ?
        WHERE COALESCE(created_event_ts, signal_ts, baseline_ts, 0) < ?
          AND COALESCE(status, '') = 'complete'
          AND archived_at IS NULL
        """,
        (now_ts, archive_ts),
    )
    db.commit()
    return {"archived_missed_attributions": cur.rowcount}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=PAPER_DB)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    now_ts = time.time()
    db = sqlite3.connect(args.db)
    try:
        result = {"db": args.db, "report": build_cleanup_report(db, now_ts)}
        if args.apply:
            result["apply"] = apply_cleanup(db, now_ts)
            result["report_after"] = build_cleanup_report(db, now_ts)
        print(json.dumps(result, indent=2, sort_keys=True))
    finally:
        db.close()


if __name__ == "__main__":
    main()

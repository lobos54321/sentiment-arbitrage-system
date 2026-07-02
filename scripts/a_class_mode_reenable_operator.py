#!/usr/bin/env python3
"""Human-operated A_CLASS_FASTLANE re-enable procedure.

This script is intentionally not called by AutoLoop.  It defaults to dry-run
and only mutates runtime mode state when a human passes --execute with an
operator and reason.  It does not change strategy, gates, executor, canary size,
wallet, or risk settings.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import tempfile
import time
from pathlib import Path


MODE_KEY = "A_CLASS_FASTLANE"
SCHEMA_VERSION = "a_class_mode_reenable_operator.v1"
AUDIT_TABLE = "a_class_mode_operator_audit"


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def jloads(raw, default=None):
    default = {} if default is None else default
    try:
        value = json.loads(raw or "{}")
        return value if isinstance(value, (dict, list)) else default
    except Exception:
        return default


def write_json(path, payload):
    if not path:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def table_exists(db, table):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())


def init_tables(db):
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS a_class_mode_runtime_state (
            mode_key TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            action TEXT,
            circuit_broken INTEGER DEFAULT 0,
            reason TEXT,
            source_trade_id TEXT,
            token_ca TEXT,
            symbol TEXT,
            last_realized_pnl_pct REAL,
            last_realized_pnl_sol REAL,
            loss_cap_pct REAL,
            breach_count INTEGER DEFAULT 0,
            last_breach_ts REAL,
            cooldown_until_ts REAL,
            clean_windows_required INTEGER DEFAULT 4,
            detail_json TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        )
        """
    )
    db.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_ts REAL NOT NULL,
            mode_key TEXT NOT NULL,
            operator TEXT NOT NULL,
            reason TEXT NOT NULL,
            action TEXT NOT NULL,
            executed INTEGER NOT NULL DEFAULT 0,
            readiness_status TEXT,
            before_json TEXT,
            after_json TEXT,
            readiness_json TEXT,
            promotion_allowed INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )
        """
    )


def row_to_dict(row):
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def load_readiness(path):
    if not path:
        return {}
    target = Path(path)
    if not target.exists():
        return {"available": False, "reason": "readiness_report_missing", "path": str(path)}
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
        data["available"] = True
        data["path"] = str(path)
        return data
    except Exception as exc:
        return {"available": False, "reason": "readiness_report_invalid_json", "error": str(exc), "path": str(path)}


def readiness_allows_human_proposal(readiness):
    proposal = readiness.get("paper_entry_proposal_readiness") or {}
    return proposal.get("status") == "PAPER_ENTRY_PROPOSAL_READY_REQUIRES_HUMAN_APPROVAL"


def reenable(db_path, *, operator, reason, readiness_path=None, execute=False, out=None, now_ts=None):
    now_ts = float(now_ts if now_ts is not None else time.time())
    readiness = load_readiness(readiness_path)
    readiness_ok = readiness_allows_human_proposal(readiness) if readiness else False
    if readiness_path and not readiness_ok:
        result = {
            "schema_version": SCHEMA_VERSION,
            "executed": False,
            "allowed": False,
            "reason": "readiness_report_not_paper_proposal_ready",
            "readiness_status": (readiness.get("paper_entry_proposal_readiness") or {}).get("status"),
            "promotion_allowed": False,
            "paper_enablement_allowed": False,
            "automatic_runtime_change_allowed": False,
        }
        write_json(out, result)
        return result
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    try:
        init_tables(db)
        before_row = db.execute(
            "SELECT * FROM a_class_mode_runtime_state WHERE mode_key=?",
            (MODE_KEY,),
        ).fetchone()
        before = row_to_dict(before_row)
        before_detail = jloads((before or {}).get("detail_json"), {})
        already_live = bool(before and str(before.get("status") or "").upper() == "LIVE" and int(before.get("circuit_broken") or 0) == 0)
        action = "noop_already_live" if already_live else "reenable_live"
        after = before
        if execute and not already_live:
            detail = before_detail if isinstance(before_detail, dict) else {}
            detail["human_reenable"] = {
                "operator": operator,
                "reason": reason,
                "readiness_report": str(readiness_path) if readiness_path is not None else None,
                "executed_at": now_ts,
                "schema_version": SCHEMA_VERSION,
            }
            if before is None:
                db.execute(
                    """
                    INSERT INTO a_class_mode_runtime_state (
                        mode_key, status, action, circuit_broken, reason,
                        clean_windows_required, detail_json, created_at, updated_at
                    ) VALUES (?, 'LIVE', 'LIVE', 0, ?, 4, ?, ?, ?)
                    """,
                    (MODE_KEY, "human_reenabled_after_clean_windows", json.dumps(detail, sort_keys=True), now_ts, now_ts),
                )
            else:
                db.execute(
                    """
                    UPDATE a_class_mode_runtime_state
                    SET status='LIVE',
                        action='LIVE',
                        circuit_broken=0,
                        reason=?,
                        detail_json=?,
                        updated_at=?
                    WHERE mode_key=?
                    """,
                    ("human_reenabled_after_clean_windows", json.dumps(detail, sort_keys=True), now_ts, MODE_KEY),
                )
            after = row_to_dict(db.execute(
                "SELECT * FROM a_class_mode_runtime_state WHERE mode_key=?",
                (MODE_KEY,),
            ).fetchone())
        db.execute(
            f"""
            INSERT INTO {AUDIT_TABLE} (
                event_ts, mode_key, operator, reason, action, executed,
                readiness_status, before_json, after_json, readiness_json,
                promotion_allowed, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
            """,
            (
                now_ts,
                MODE_KEY,
                operator,
                reason,
                action,
                1 if execute else 0,
                (readiness.get("paper_entry_proposal_readiness") or {}).get("status"),
                json.dumps(before, sort_keys=True),
                json.dumps(after, sort_keys=True),
                json.dumps(readiness, sort_keys=True),
                utc_now(),
            ),
        )
        if execute:
            db.commit()
        else:
            db.rollback()
        result = {
            "schema_version": SCHEMA_VERSION,
            "mode_key": MODE_KEY,
            "execute_requested": bool(execute),
            "executed": bool(execute),
            "action": action,
            "operator": operator,
            "reason": reason,
            "readiness_status": (readiness.get("paper_entry_proposal_readiness") or {}).get("status"),
            "before": before,
            "after": after,
            "audit_table": AUDIT_TABLE,
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "gate_change_allowed": False,
            "executor_change_allowed": False,
            "canary_increase_allowed": False,
            "risk_change_allowed": False,
        }
        write_json(out, result)
        return result
    finally:
        db.close()


def self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        db_path = root / "paper.db"
        ready_path = root / "ready.json"
        write_json(ready_path, {
            "paper_entry_proposal_readiness": {
                "status": "PAPER_ENTRY_PROPOSAL_READY_REQUIRES_HUMAN_APPROVAL"
            }
        })
        db = sqlite3.connect(db_path)
        db.execute(
            """
            CREATE TABLE a_class_mode_runtime_state (
                mode_key TEXT PRIMARY KEY, status TEXT, action TEXT, circuit_broken INTEGER,
                reason TEXT, source_trade_id TEXT, token_ca TEXT, symbol TEXT,
                last_realized_pnl_pct REAL, last_realized_pnl_sol REAL, loss_cap_pct REAL,
                breach_count INTEGER, last_breach_ts REAL, cooldown_until_ts REAL,
                clean_windows_required INTEGER, detail_json TEXT, created_at REAL, updated_at REAL
            )
            """
        )
        db.execute(
            "INSERT INTO a_class_mode_runtime_state VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (MODE_KEY, "SHADOW", "SHADOW", 1, "loss_cap_breach", "t1", "TOK", "TOK", -21, -0.001, 20, 1, 1, 0, 4, "{}", 1, 1),
        )
        db.commit()
        db.close()
        dry = reenable(db_path, operator="unit", reason="dry run", readiness_path=ready_path, execute=False)
        assert dry["executed"] is False
        db = sqlite3.connect(db_path)
        row = db.execute("SELECT status, circuit_broken FROM a_class_mode_runtime_state WHERE mode_key=?", (MODE_KEY,)).fetchone()
        assert row == ("SHADOW", 1)
        db.close()
        executed = reenable(db_path, operator="unit", reason="human approved", readiness_path=ready_path, execute=True, now_ts=10)
        assert executed["executed"] is True
        assert executed["after"]["status"] == "LIVE"
        assert int(executed["after"]["circuit_broken"]) == 0
        db = sqlite3.connect(db_path)
        audit_count = db.execute(f"SELECT COUNT(*) FROM {AUDIT_TABLE} WHERE executed=1").fetchone()[0]
        assert audit_count == 1
        db.close()
    print("SELF_TEST_PASS a_class_mode_reenable_operator")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--operator", default=None)
    parser.add_argument("--reason", default=None)
    parser.add_argument("--readiness-report", default="/app/data/agent_runs/latest/a_class_fastlane_mode_audit_24h.json")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--out", default=None)
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return
    if not args.operator or not args.reason:
        raise SystemExit("--operator and --reason are required")
    reenable(
        args.db,
        operator=args.operator,
        reason=args.reason,
        readiness_path=args.readiness_report,
        execute=args.execute,
        out=args.out,
    )


if __name__ == "__main__":
    main()

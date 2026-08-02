import argparse
import hashlib
import json
import re
import sqlite3
from pathlib import Path

from scripts.a_class_fastlane_mode_readiness_audit import MODE_KEY, build_report


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def test_read_only_audit_projects_state_without_mutating_snapshot(tmp_path):
    now = 2_000_000
    db_path = tmp_path / "paper_evidence.db"
    db = sqlite3.connect(db_path)
    db.execute(
        """
        CREATE TABLE a_class_mode_runtime_state(
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
        (
            MODE_KEY,
            "CIRCUIT_BROKEN",
            "SHADOW",
            1,
            "loss_cap_breach",
            "71",
            "TOKEN",
            "TOKEN",
            -29.75,
            -0.001,
            20,
            1,
            now - (5 * 3600),
            now - 1,
            6,
            json.dumps(
                {
                    "breach_class": "PAPER_MARKET",
                    "clean_window_counter": {
                        "counter_bucket_sec": 3600,
                        "last_window_bucket": (now // 3600) - 1,
                        "last_passed": True,
                        "streak": 5,
                        "required": 6,
                    },
                }
            ),
            now - (5 * 3600),
            now - 1,
        ),
    )
    db.execute(
        """
        CREATE TABLE paper_decision_events(
          event_ts INTEGER, signal_id TEXT, token_ca TEXT, symbol TEXT, lifecycle_id TEXT,
          component TEXT, event_type TEXT, decision TEXT, reason TEXT, payload_json TEXT
        )
        """
    )
    db.commit()
    db.close()

    before = file_sha256(db_path)
    args = argparse.Namespace(
        db=str(db_path),
        raw_funnel=None,
        context_coverage=None,
        volume_kline_audit=None,
        context_blocker_monitor=None,
        hours=24,
        now_ts=now,
        out=None,
        read_only=True,
    )
    report = build_report(args)

    assert file_sha256(db_path) == before
    assert report["inputs"]["read_only"] is True
    assert report["clean_window_counter_persistence"]["attempted"] is False
    assert report["paper_ready_tracker_persistence"]["attempted"] is False
    assert report["paper_auto_resume_execution"]["executed"] is False
    assert report["paper_auto_resume_execution"]["reason"] == "read_only_audit"
    assert report["promotion_allowed"] is False


def test_autoloop_callers_force_read_only_mode():
    root = Path(__file__).resolve().parents[1]
    for relative in (
        "scripts/agent_capture_discovery_loop.py",
        "scripts/agent_autoloop_stage_runner.py",
    ):
        source = (root / relative).read_text(encoding="utf-8")
        assert re.search(
            r'"scripts/a_class_fastlane_mode_readiness_audit\.py"\s*,\s*'
            r'"--db"\s*,\s*args\.paper_db\s*,\s*"--read-only"',
            source,
        )

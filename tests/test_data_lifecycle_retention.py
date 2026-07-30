"""Value-aware paper DB lifecycle tests."""

import gzip
import json
import os
from pathlib import Path
import sqlite3
import sys
import time

import pytest


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import paper_db_retention as retention
from paper_db_retention import (
    PROTECTED_TABLES,
    RETENTION_POLICIES,
    garbage_collect_archives,
    run_retention,
    storage_health,
    update_bounded_growth_history,
    validate_policy_contract,
)


NOW_TS = 2_000_000_000
DAY = 86_400


def create_test_db(path: Path) -> None:
    db = sqlite3.connect(path)
    db.executescript(
        """
        CREATE TABLE paper_trades (
          id INTEGER PRIMARY KEY,
          exit_reason TEXT,
          exit_ts REAL
        );
        CREATE TABLE canonical_trade_ledger (
          id INTEGER PRIMARY KEY,
          trade_id TEXT,
          created_at REAL
        );
        CREATE TABLE a_class_decision_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          event_ts REAL,
          token_ca TEXT,
          symbol TEXT,
          route_bucket TEXT,
          source_component TEXT,
          action TEXT,
          reason TEXT,
          block_cause TEXT,
          quote_clean INTEGER,
          quote_executable INTEGER,
          candidate_json TEXT,
          risk_json TEXT,
          created_at REAL
        );
        CREATE TABLE paper_decision_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          event_ts REAL,
          signal_id INTEGER,
          token_ca TEXT,
          component TEXT,
          event_type TEXT,
          decision TEXT,
          reason TEXT,
          payload_json TEXT,
          created_at REAL
        );
        CREATE TABLE candidate_shadow_observations (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          signal_id INTEGER,
          token_ca TEXT,
          signal_ts INTEGER,
          candidate_id TEXT,
          family TEXT,
          matched INTEGER,
          reason TEXT,
          observed_at INTEGER,
          payload_json TEXT
        );
        CREATE TABLE candidate_shadow_virtual_trades (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          signal_id INTEGER,
          token_ca TEXT,
          signal_ts INTEGER,
          candidate_id TEXT,
          family TEXT,
          status TEXT,
          exit_reason TEXT,
          net_pnl_pct REAL,
          observed_at INTEGER,
          payload_json TEXT
        );
        CREATE TABLE latency_audit_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source TEXT,
          token_ca TEXT,
          signal_ts INTEGER,
          stage TEXT,
          event_ts INTEGER,
          payload_json TEXT
        );
        """
    )
    old_ts = NOW_TS - 10 * DAY
    new_ts = NOW_TS - DAY
    db.execute("INSERT INTO paper_trades VALUES (1, 'test_exit', ?)", (old_ts,))
    db.execute("INSERT INTO canonical_trade_ledger VALUES (1, 'trade-1', ?)", (old_ts,))
    db.executemany(
        """
        INSERT INTO a_class_decision_events
          (event_ts, token_ca, symbol, route_bucket, source_component, action, reason,
           block_cause, quote_clean, quote_executable, candidate_json, risk_json, created_at)
        VALUES (?, ?, 'TOK', 'ATH', 'matrix', 'WOULD_ENTER', 'test', 'POLICY', 1, 1, ?, ?, ?)
        """,
        [
            (old_ts, "old-a", json.dumps({"large": "x" * 20_000}), json.dumps({"risk": "y" * 5_000}), old_ts),
            (new_ts, "new-a", json.dumps({"large": "x" * 20_000}), json.dumps({"risk": "y" * 5_000}), new_ts),
        ],
    )
    db.executemany(
        """
        INSERT INTO paper_decision_events
          (event_ts, signal_id, token_ca, component, event_type, decision, reason, payload_json, created_at)
        VALUES (?, ?, ?, 'entry', 'decision', 'allow', 'test', ?, ?)
        """,
        [
            (old_ts, 1, "old-p", json.dumps({"large": "p" * 10_000}), old_ts),
            (new_ts, 2, "new-p", json.dumps({"large": "p" * 10_000}), new_ts),
        ],
    )
    db.executemany(
        """
        INSERT INTO candidate_shadow_observations
          (signal_id, token_ca, signal_ts, candidate_id, family, matched, reason, observed_at, payload_json)
        VALUES (?, ?, ?, 'current_all', 'base', 1, 'matched', ?, ?)
        """,
        [
            (1, "old-c", old_ts, old_ts, json.dumps({"feature": "historical"})),
            (2, "new-c", new_ts, new_ts, json.dumps({"feature": "current"})),
        ],
    )
    db.executemany(
        """
        INSERT INTO candidate_shadow_virtual_trades
          (signal_id, token_ca, signal_ts, candidate_id, family, status, exit_reason,
           net_pnl_pct, observed_at, payload_json)
        VALUES (?, ?, ?, 'current_all', 'base', 'VIRTUAL_CLOSED', 'TIMEOUT', 1.0, ?, ?)
        """,
        [
            (1, "old-v", old_ts, old_ts, json.dumps({"virtual": "historical"})),
            (2, "new-v", new_ts, new_ts, json.dumps({"virtual": "current"})),
        ],
    )
    db.executemany(
        """
        INSERT INTO latency_audit_events
          (source, token_ca, signal_ts, stage, event_ts, payload_json)
        VALUES ('telegram', ?, ?, 'ingest', ?, ?)
        """,
        [
            ("old-l", old_ts, old_ts, json.dumps({"detail": "old"})),
            ("new-l", new_ts, new_ts, json.dumps({"detail": "new"})),
        ],
    )
    db.commit()
    db.close()


def read_archive_rows(path: Path) -> list[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh]


def test_value_aware_retention_preserves_core_and_compacts_large_rows(tmp_path, monkeypatch):
    db_path = tmp_path / "paper_trades.db"
    archive_dir = tmp_path / "archive"
    create_test_db(db_path)
    monkeypatch.setenv("PAPER_DB_RETENTION_ARCHIVE_GC_ENABLED", "false")

    result = run_retention(
        db_path=db_path,
        archive_dir=archive_dir,
        mode="apply",
        batch_size=100,
        max_rows_per_table=1_000,
        max_rows_total=10_000,
        max_seconds=30,
        now_ts=NOW_TS,
        storage_usage_override={
            "total_bytes": 80 * 1024**3,
            "used_bytes": 20 * 1024**3,
            "free_bytes": 60 * 1024**3,
        },
    )

    assert result["status"] == "ok"
    assert result["schema_version"] == "paper_db_retention.v2"
    assert result["storage"]["pressure_level"] == "normal"
    assert result["protected_tables_selected"] == []
    assert result["total_deleted"] == 5

    db = sqlite3.connect(db_path)
    assert db.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0] == 1
    assert db.execute("SELECT COUNT(*) FROM canonical_trade_ledger").fetchone()[0] == 1
    assert db.execute("SELECT token_ca FROM a_class_decision_events").fetchall() == [("new-a",)]
    assert db.execute("SELECT token_ca FROM paper_decision_events").fetchall() == [("new-p",)]
    assert db.execute("SELECT token_ca FROM candidate_shadow_observations").fetchall() == [("new-c",)]
    assert db.execute("SELECT token_ca FROM candidate_shadow_virtual_trades").fetchall() == [("new-v",)]
    assert db.execute("SELECT token_ca FROM latency_audit_events").fetchall() == [("new-l",)]
    db.close()

    a_class_manifest_path = next((archive_dir / "a_class_decision_events").rglob("*.manifest.json"))
    a_class_manifest = json.loads(a_class_manifest_path.read_text(encoding="utf-8"))
    assert a_class_manifest["archive_kind"] == "compact"
    assert a_class_manifest["verification"]["verified"] is True
    assert a_class_manifest["source_delete_status"] == "verified"
    assert a_class_manifest["source_deleted_rows"] == 1
    a_class_rows = read_archive_rows(Path(a_class_manifest["archive_file"]))
    assert a_class_rows[0]["token_ca"] == "old-a"
    assert "candidate_json" not in a_class_rows[0]
    assert "risk_json" not in a_class_rows[0]

    candidate_manifest_path = next((archive_dir / "candidate_shadow_observations").rglob("*.manifest.json"))
    candidate_manifest = json.loads(candidate_manifest_path.read_text(encoding="utf-8"))
    candidate_rows = read_archive_rows(Path(candidate_manifest["archive_file"]))
    assert json.loads(candidate_rows[0]["payload_json"])["feature"] == "historical"

    latency_manifest_path = next((archive_dir / "latency_audit_events").rglob("*.manifest.json"))
    latency_manifest = json.loads(latency_manifest_path.read_text(encoding="utf-8"))
    assert latency_manifest["archive_kind"] == "summary"
    assert latency_manifest["archive_file"] is None
    assert latency_manifest["dimension_counts"]["source"] == {"telegram": 1}
    assert latency_manifest["dimension_counts"]["stage"] == {"ingest": 1}


def test_archive_gc_deletes_only_verified_v2_manifest_owned_files(tmp_path, monkeypatch):
    db_path = tmp_path / "paper_trades.db"
    archive_dir = tmp_path / "archive"
    create_test_db(db_path)
    monkeypatch.setenv("PAPER_DB_RETENTION_ARCHIVE_GC_ENABLED", "false")
    monkeypatch.setenv("PAPER_DB_RETENTION_A_CLASS_ARCHIVE_DAYS", "0")

    run_retention(
        db_path=db_path,
        archive_dir=archive_dir,
        mode="apply",
        batch_size=100,
        max_rows_per_table=1_000,
        max_rows_total=10_000,
        max_seconds=30,
        now_ts=NOW_TS,
    )
    unmanaged = archive_dir / "manual-backup.sqlite"
    unmanaged.write_bytes(b"do-not-delete")
    unmanaged_manifest = archive_dir / "legacy.manifest.json"
    unmanaged_manifest.write_text(json.dumps({"schema_version": "manual.v1"}), encoding="utf-8")
    managed_manifest_path = next((archive_dir / "a_class_decision_events").rglob("*.manifest.json"))
    managed_manifest = json.loads(managed_manifest_path.read_text(encoding="utf-8"))
    managed_manifest["source_delete_status"] = "pending"
    managed_manifest_path.write_text(json.dumps(managed_manifest), encoding="utf-8")

    refused_gc = garbage_collect_archives(
        archive_dir=archive_dir,
        now_ts=time.time() + 5,
        max_manifests=100,
        verify_payloads=True,
    )
    assert refused_gc["deleted_archives"] == 0
    assert any(item["reason"] == "source_delete_not_verified" for item in refused_gc["refused"])

    managed_manifest["source_delete_status"] = "verified"
    managed_manifest_path.write_text(json.dumps(managed_manifest), encoding="utf-8")
    gc = garbage_collect_archives(
        archive_dir=archive_dir,
        now_ts=time.time() + 5,
        max_manifests=100,
        verify_payloads=True,
    )

    assert gc["deleted_archives"] == 1
    assert gc["deleted_manifests"] == 1
    assert unmanaged.exists()
    assert unmanaged_manifest.exists()
    assert any(item["reason"] == "unmanaged_schema" for item in gc["refused"])


def test_archive_gc_budget_ignores_unexpired_manifests_sorted_first(tmp_path):
    archive_dir = tmp_path / "archive"
    future_dir = archive_dir / "a_class_decision_events"
    expired_dir = archive_dir / "z_latency_audit_events"
    future_dir.mkdir(parents=True)
    expired_dir.mkdir(parents=True)

    def write_summary_manifest(path: Path, gc_after_ts: float) -> None:
        path.write_text(
            json.dumps(
                {
                    "schema_version": "paper_db_retention_archive.v2",
                    "archive_kind": "summary",
                    "archive_file": None,
                    "mode": "apply",
                    "source_delete_status": "verified",
                    "created_at_ts": NOW_TS - DAY,
                    "gc_after_ts": gc_after_ts,
                    "row_count": 1,
                }
            ),
            encoding="utf-8",
        )

    for index in range(11):
        write_summary_manifest(
            future_dir / f"future-{index:02d}.manifest.json",
            NOW_TS + DAY,
        )
    expired_manifest = expired_dir / "expired.manifest.json"
    write_summary_manifest(expired_manifest, NOW_TS - 1)

    gc = garbage_collect_archives(
        archive_dir=archive_dir,
        now_ts=NOW_TS,
        max_manifests=1,
        verify_payloads=True,
    )

    assert gc["seen"] == 12
    assert gc["eligible"] == 1
    assert gc["deleted_manifests"] == 1
    assert not expired_manifest.exists()
    assert len(list(future_dir.glob("*.manifest.json"))) == 11


def test_policy_contract_and_disk_pressure_floor():
    validate_policy_contract(RETENTION_POLICIES)
    assert not ({policy.table for policy in RETENTION_POLICIES} & PROTECTED_TABLES)

    health = storage_health(
        Path("."),
        {
            "total_bytes": 80 * 1024**3,
            "used_bytes": 74 * 1024**3,
            "free_bytes": 6 * 1024**3,
        },
    )
    assert health["pressure_level"] == "critical"
    a_class_policy = next(policy for policy in RETENTION_POLICIES if policy.table == "a_class_decision_events")
    assert a_class_policy.effective_days("critical") == 3.0


def test_growth_history_is_bounded_and_projects_time_to_watermark(tmp_path):
    history_path = tmp_path / "growth.jsonl"

    def summary(ts, used_gib, db_gib):
        return {
            "run_id": f"run-{ts}",
            "finished_at_ts": ts,
            "total_deleted": 10,
            "sqlite_after": {
                "db_file_bytes": int(db_gib * 1024**3),
                "freelist_bytes": 0,
            },
            "storage": {
                "used_bytes": int(used_gib * 1024**3),
                "free_bytes": int((80 - used_gib) * 1024**3),
                "total_bytes": 80 * 1024**3,
                "hard_pct": 82.0,
                "pressure_level": "normal",
            },
            "archive_gc": {"freed_bytes": 0},
        }

    update_bounded_growth_history(history_path, summary(1_000, 20, 10), max_entries=2)
    projection = update_bounded_growth_history(
        history_path,
        summary(1_000 + DAY, 22, 11),
        max_entries=2,
    )
    update_bounded_growth_history(history_path, summary(1_000 + 2 * DAY, 23, 11), max_entries=2)

    assert projection["volume_growth_bytes_per_day"] == 2 * 1024**3
    assert projection["db_growth_bytes_per_day"] == 1024**3
    assert projection["estimated_days_to_hard_watermark"] == 21.8
    assert len(history_path.read_text(encoding="utf-8").splitlines()) == 2


def test_delete_count_mismatch_rolls_back_source_rows(tmp_path, monkeypatch):
    db_path = tmp_path / "paper_trades.db"
    archive_dir = tmp_path / "archive"
    create_test_db(db_path)
    monkeypatch.setenv("PAPER_DB_RETENTION_ARCHIVE_GC_ENABLED", "false")
    monkeypatch.setattr(retention, "delete_rowids", lambda *_args, **_kwargs: 0)

    with pytest.raises(RuntimeError, match="retention delete count mismatch"):
        run_retention(
            db_path=db_path,
            archive_dir=archive_dir,
            mode="apply",
            batch_size=100,
            max_rows_per_table=1_000,
            max_rows_total=10_000,
            max_seconds=30,
            now_ts=NOW_TS,
        )

    db = sqlite3.connect(db_path)
    assert db.execute("SELECT COUNT(*) FROM a_class_decision_events").fetchone()[0] == 2
    db.close()


def test_schema_drift_refuses_compaction_when_identity_columns_are_missing(tmp_path, monkeypatch):
    db_path = tmp_path / "paper_trades.db"
    archive_dir = tmp_path / "archive"
    db = sqlite3.connect(db_path)
    db.execute(
        "CREATE TABLE external_alpha_snapshots (id INTEGER PRIMARY KEY, captured_at INTEGER, raw_json TEXT)"
    )
    db.execute(
        "INSERT INTO external_alpha_snapshots VALUES (1, ?, '{\"historical\":true}')",
        (NOW_TS - 10 * DAY,),
    )
    db.commit()
    db.close()
    monkeypatch.setenv("PAPER_DB_RETENTION_ARCHIVE_GC_ENABLED", "false")

    result = run_retention(
        db_path=db_path,
        archive_dir=archive_dir,
        mode="apply",
        now_ts=NOW_TS,
    )

    external = next(
        policy for policy in result["policies"] if policy["table"] == "external_alpha_snapshots"
    )
    assert external["stopped_reason"] == "required_archive_columns_missing"
    assert external["missing_required_archive_columns"] == ["source", "token_ca"]
    db = sqlite3.connect(db_path)
    assert db.execute("SELECT COUNT(*) FROM external_alpha_snapshots").fetchone()[0] == 1
    db.close()


def test_critical_pressure_can_expire_verified_research_archive_after_14_days(tmp_path, monkeypatch):
    db_path = tmp_path / "paper_trades.db"
    archive_dir = tmp_path / "archive"
    create_test_db(db_path)
    monkeypatch.setenv("PAPER_DB_RETENTION_ARCHIVE_GC_ENABLED", "false")
    monkeypatch.setenv("PAPER_DB_RETENTION_A_CLASS_ARCHIVE_DAYS", "30")

    run_retention(
        db_path=db_path,
        archive_dir=archive_dir,
        mode="apply",
        now_ts=NOW_TS,
    )
    archive_path = next((archive_dir / "a_class_decision_events").rglob("*.jsonl.gz"))
    future = time.time() + 15 * DAY

    normal_gc = garbage_collect_archives(
        archive_dir=archive_dir,
        now_ts=future,
        max_manifests=100,
        verify_payloads=True,
        pressure_level="normal",
    )
    assert normal_gc["deleted_archives"] == 0
    assert archive_path.exists()

    critical_gc = garbage_collect_archives(
        archive_dir=archive_dir,
        now_ts=future,
        max_manifests=100,
        verify_payloads=True,
        pressure_level="critical",
        critical_max_age_days=14,
    )
    assert critical_gc["critical_pressure_eligible"] >= 1
    assert critical_gc["deleted_archives"] >= 1
    assert not archive_path.exists()

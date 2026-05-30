#!/usr/bin/env python3
"""Archive and prune hot paper DB audit rows.

The paper runtime needs recent rows in SQLite, but long-lived shadow and audit
tables can grow without bound. This script keeps the hot DB bounded while
preserving pruned rows in gzip-compressed JSONL archives with per-batch
manifests.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time
import uuid

from sqlite_write_coordinator import sqlite_single_writer


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = PROJECT_ROOT / "data" / "paper_trades.db"
DEFAULT_ARCHIVE_DIR = PROJECT_ROOT / "data" / "archive" / "paper-db-retention"


def _env_bool(name: str, default: str = "false") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.environ.get(name, str(default))))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class RetentionPolicy:
    table: str
    ts_expr: str
    days_env: str
    default_days: float
    description: str
    extra_where: str = "1=1"

    def days(self) -> float:
        return max(0.0, _env_float(self.days_env, self.default_days))


RETENTION_POLICIES = [
    RetentionPolicy(
        table="paper_decision_events",
        ts_expr="event_ts",
        days_env="PAPER_DB_RETENTION_DECISION_DAYS",
        default_days=30.0,
        description="append-only decision audit events",
    ),
    RetentionPolicy(
        table="paper_missed_signal_attribution",
        ts_expr="COALESCE(created_event_ts, signal_ts, baseline_ts, 0)",
        days_env="PAPER_DB_RETENTION_MISSED_DAYS",
        default_days=30.0,
        description="missed-dog attribution and forward outcomes",
    ),
    RetentionPolicy(
        table="paper_trade_path_samples",
        ts_expr="sample_ts",
        days_env="PAPER_DB_RETENTION_PATH_SAMPLE_DAYS",
        default_days=14.0,
        description="per-position path samples for closed/old trades",
        extra_where=(
            "trade_id NOT IN ("
            "SELECT id FROM paper_trades WHERE exit_reason IS NULL OR exit_ts IS NULL"
            ")"
        ),
    ),
    RetentionPolicy(
        table="lotto_not_ath_watch_shadow_snapshots",
        ts_expr="snapshot_ts",
        days_env="PAPER_DB_RETENTION_WATCH_SHADOW_DAYS",
        default_days=7.0,
        description="watch-shadow quote-clean snapshots",
    ),
    RetentionPolicy(
        table="external_alpha_snapshots",
        ts_expr="captured_at",
        days_env="PAPER_DB_RETENTION_EXTERNAL_ALPHA_DAYS",
        default_days=14.0,
        description="raw external alpha snapshots",
    ),
    RetentionPolicy(
        table="source_resonance_candidates",
        ts_expr="signal_ts",
        days_env="PAPER_DB_RETENTION_SOURCE_RESONANCE_DAYS",
        default_days=30.0,
        description="source resonance candidate rows",
    ),
    RetentionPolicy(
        table="latency_audit_events",
        ts_expr="COALESCE(event_ts, signal_ts, 0)",
        days_env="PAPER_DB_RETENTION_LATENCY_AUDIT_DAYS",
        default_days=14.0,
        description="latency audit events",
    ),
    RetentionPolicy(
        table="paper_fast_entry_queue",
        ts_expr="created_at",
        days_env="PAPER_DB_RETENTION_FAST_QUEUE_DAYS",
        default_days=30.0,
        description="terminal fast-lane queue rows",
        extra_where="COALESCE(status, '') NOT IN ('queued', 'claimed', 'pending', 'running')",
    ),
]


def connect_db(db_path: str | os.PathLike) -> sqlite3.Connection:
    db = sqlite3.connect(str(db_path), timeout=_env_float("PAPER_DB_RETENTION_SQLITE_TIMEOUT_SEC", 30.0))
    db.execute(f"PRAGMA busy_timeout={_env_int('PAPER_DB_RETENTION_BUSY_TIMEOUT_MS', 30000)}")
    db.row_factory = sqlite3.Row
    return db


def table_exists(db: sqlite3.Connection, table: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return bool(row)


def table_columns(db: sqlite3.Connection, table: str) -> list[str]:
    return [str(row["name"]) for row in db.execute(f"PRAGMA table_info({table})").fetchall()]


def eligible_where(policy: RetentionPolicy) -> str:
    return f"({policy.ts_expr}) > 0 AND ({policy.ts_expr}) < ? AND ({policy.extra_where})"


def count_eligible(db: sqlite3.Connection, policy: RetentionPolicy, cutoff_ts: float) -> int:
    row = db.execute(
        f"SELECT COUNT(*) AS n FROM {policy.table} WHERE {eligible_where(policy)}",
        (cutoff_ts,),
    ).fetchone()
    return int(row["n"] or 0)


def row_to_jsonable(row: sqlite3.Row) -> dict:
    result = {}
    for key in row.keys():
        value = row[key]
        if isinstance(value, bytes):
            value = value.hex()
        result[key] = value
    return result


def archive_rows(
    *,
    rows: list[sqlite3.Row],
    archive_dir: Path,
    table: str,
    cutoff_ts: float,
    run_id: str,
    batch_no: int,
    mode: str,
) -> dict:
    archive_dir.mkdir(parents=True, exist_ok=True)
    month = time.strftime("%Y-%m", time.gmtime(cutoff_ts))
    table_dir = archive_dir / table / month
    table_dir.mkdir(parents=True, exist_ok=True)
    base = f"{table}_{run_id}_batch{batch_no:05d}"
    final_path = table_dir / f"{base}.jsonl.gz"
    tmp_path = table_dir / f"{base}.jsonl.gz.tmp"
    manifest_path = table_dir / f"{base}.manifest.json"
    manifest_tmp = table_dir / f"{base}.manifest.json.tmp"
    hasher = hashlib.sha256()
    min_ts = None
    max_ts = None
    min_rowid = None
    max_rowid = None
    with gzip.open(tmp_path, "wt", encoding="utf-8") as fh:
        for row in rows:
            row_dict = row_to_jsonable(row)
            row_ts = row_dict.get("_retention_ts_")
            rowid = row_dict.get("_retention_rowid_")
            try:
                min_ts = row_ts if min_ts is None else min(float(min_ts), float(row_ts))
                max_ts = row_ts if max_ts is None else max(float(max_ts), float(row_ts))
            except (TypeError, ValueError):
                pass
            try:
                min_rowid = rowid if min_rowid is None else min(int(min_rowid), int(rowid))
                max_rowid = rowid if max_rowid is None else max(int(max_rowid), int(rowid))
            except (TypeError, ValueError):
                pass
            line = json.dumps(row_dict, ensure_ascii=False, sort_keys=True, default=str) + "\n"
            hasher.update(line.encode("utf-8"))
            fh.write(line)
    os.replace(tmp_path, final_path)
    manifest = {
        "schema_version": "paper_db_retention_archive.v1",
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "mode": mode,
        "table": table,
        "archive_file": str(final_path),
        "row_count": len(rows),
        "rowid_min": min_rowid,
        "rowid_max": max_rowid,
        "retention_ts_min": min_ts,
        "retention_ts_max": max_ts,
        "cutoff_ts": cutoff_ts,
        "cutoff_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(cutoff_ts)),
        "uncompressed_sha256": hasher.hexdigest(),
    }
    manifest_tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(manifest_tmp, manifest_path)
    manifest["manifest_file"] = str(manifest_path)
    return manifest


def delete_rowids(db: sqlite3.Connection, table: str, rowids: list[int]) -> int:
    if not rowids:
        return 0
    placeholders = ",".join("?" for _ in rowids)
    cur = db.execute(f"DELETE FROM {table} WHERE rowid IN ({placeholders})", tuple(rowids))
    return int(cur.rowcount or 0)


def apply_policy(
    db: sqlite3.Connection,
    policy: RetentionPolicy,
    *,
    archive_dir: Path,
    now_ts: float,
    mode: str,
    batch_size: int,
    max_rows: int,
    deadline_ts: float | None,
    run_id: str,
) -> dict:
    summary = {
        "table": policy.table,
        "description": policy.description,
        "retention_days": policy.days(),
        "exists": table_exists(db, policy.table),
        "eligible": 0,
        "archived": 0,
        "deleted": 0,
        "batches": [],
        "stopped_reason": None,
    }
    if not summary["exists"]:
        summary["stopped_reason"] = "table_missing"
        return summary
    cutoff_ts = now_ts - policy.days() * 86400.0
    summary["cutoff_ts"] = cutoff_ts
    summary["cutoff_iso"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(cutoff_ts))
    summary["eligible"] = count_eligible(db, policy, cutoff_ts)
    if mode == "report" or summary["eligible"] <= 0:
        summary["stopped_reason"] = "report_only" if mode == "report" else "no_eligible_rows"
        return summary

    columns = table_columns(db, policy.table)
    select_columns = ", ".join(f'"{name}"' for name in columns)
    last_rowid = 0
    batch_no = 0
    max_rows = max(0, int(max_rows))
    rows_remaining = min(summary["eligible"], max_rows) if max_rows else summary["eligible"]
    while rows_remaining > 0:
        if deadline_ts is not None and time.time() >= deadline_ts:
            summary["stopped_reason"] = "time_budget_exhausted"
            break
        limit = min(batch_size, rows_remaining)
        rows = db.execute(
            f"""
            SELECT rowid AS _retention_rowid_,
                   ({policy.ts_expr}) AS _retention_ts_,
                   {select_columns}
            FROM {policy.table}
            WHERE rowid > ? AND {eligible_where(policy)}
            ORDER BY rowid ASC
            LIMIT ?
            """,
            (last_rowid, cutoff_ts, limit),
        ).fetchall()
        if not rows:
            summary["stopped_reason"] = "scan_complete"
            break
        batch_no += 1
        last_rowid = int(rows[-1]["_retention_rowid_"])
        manifest = archive_rows(
            rows=rows,
            archive_dir=archive_dir,
            table=policy.table,
            cutoff_ts=cutoff_ts,
            run_id=run_id,
            batch_no=batch_no,
            mode=mode,
        )
        summary["archived"] += len(rows)
        if mode == "apply":
            deleted = delete_rowids(
                db,
                policy.table,
                [int(row["_retention_rowid_"]) for row in rows],
            )
            db.commit()
            summary["deleted"] += deleted
            manifest["deleted"] = deleted
        else:
            manifest["deleted"] = 0
        summary["batches"].append(manifest)
        rows_remaining -= len(rows)
    if summary["stopped_reason"] is None:
        summary["stopped_reason"] = "row_budget_exhausted" if max_rows and summary["archived"] >= max_rows else "complete"
    return summary


def run_retention(
    *,
    db_path: str | os.PathLike,
    archive_dir: str | os.PathLike,
    mode: str = "report",
    batch_size: int = 5000,
    max_rows_per_table: int = 50000,
    max_rows_total: int = 200000,
    max_seconds: float = 60.0,
    vacuum: bool = False,
    now_ts: float | None = None,
) -> dict:
    if mode not in {"report", "archive", "apply"}:
        raise ValueError(f"unsupported mode: {mode}")
    now_ts = float(now_ts if now_ts is not None else time.time())
    deadline_ts = None if max_seconds <= 0 else time.time() + max_seconds
    archive_path = Path(archive_dir)
    db_path = Path(db_path)
    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(now_ts)) + "_" + uuid.uuid4().hex[:8]
    summary = {
        "schema_version": "paper_db_retention.v1",
        "run_id": run_id,
        "mode": mode,
        "db_path": str(db_path),
        "archive_dir": str(archive_path),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "policies": [],
        "total_eligible": 0,
        "total_archived": 0,
        "total_deleted": 0,
        "vacuum": {"requested": bool(vacuum), "ran": False},
    }
    if not db_path.exists():
        summary["status"] = "db_missing"
        return summary
    db = connect_db(db_path)
    try:
        if mode == "apply":
            db.execute("PRAGMA foreign_keys=OFF")
        total_budget_left = max_rows_total
        for policy in RETENTION_POLICIES:
            if deadline_ts is not None and time.time() >= deadline_ts:
                summary["stopped_reason"] = "time_budget_exhausted"
                break
            if max_rows_total and total_budget_left <= 0:
                summary["stopped_reason"] = "total_row_budget_exhausted"
                break
            policy_budget = max_rows_per_table
            if max_rows_total:
                policy_budget = min(policy_budget, total_budget_left)
            with sqlite_single_writer("paper_db_retention", timeout_sec=_env_float("PAPER_DB_RETENTION_LOCK_TIMEOUT_SEC", 120.0)):
                policy_summary = apply_policy(
                    db,
                    policy,
                    archive_dir=archive_path,
                    now_ts=now_ts,
                    mode=mode,
                    batch_size=max(1, batch_size),
                    max_rows=max(0, policy_budget),
                    deadline_ts=deadline_ts,
                    run_id=run_id,
                )
            summary["policies"].append(policy_summary)
            summary["total_eligible"] += int(policy_summary.get("eligible") or 0)
            summary["total_archived"] += int(policy_summary.get("archived") or 0)
            summary["total_deleted"] += int(policy_summary.get("deleted") or 0)
            if max_rows_total:
                total_budget_left -= int(policy_summary.get("archived") or 0)
        try:
            checkpoint = db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            summary["wal_checkpoint"] = list(checkpoint) if checkpoint is not None else None
        except Exception as exc:
            summary["wal_checkpoint_error"] = str(exc)
        if vacuum and mode == "apply" and summary["total_deleted"] > 0:
            try:
                db.execute("VACUUM")
                summary["vacuum"]["ran"] = True
            except Exception as exc:
                summary["vacuum"]["error"] = str(exc)
        summary["status"] = "ok"
        summary["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return summary
    finally:
        db.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Archive and prune hot paper DB audit rows.")
    parser.add_argument("--db", default=os.environ.get("PAPER_DB") or str(DEFAULT_DB))
    parser.add_argument("--archive-dir", default=os.environ.get("PAPER_DB_RETENTION_ARCHIVE_DIR") or str(DEFAULT_ARCHIVE_DIR))
    parser.add_argument("--mode", choices=["report", "archive", "apply"], default=os.environ.get("PAPER_DB_RETENTION_MODE", "report"))
    parser.add_argument("--batch-size", type=int, default=_env_int("PAPER_DB_RETENTION_BATCH_ROWS", 5000))
    parser.add_argument("--max-rows-per-table", type=int, default=_env_int("PAPER_DB_RETENTION_MAX_ROWS_PER_TABLE", 50000))
    parser.add_argument("--max-rows-total", type=int, default=_env_int("PAPER_DB_RETENTION_MAX_ROWS_TOTAL", 200000))
    parser.add_argument("--max-seconds", type=float, default=_env_float("PAPER_DB_RETENTION_MAX_SECONDS", 60.0))
    parser.add_argument("--vacuum", action="store_true", default=_env_bool("PAPER_DB_RETENTION_VACUUM", "false"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = run_retention(
        db_path=args.db,
        archive_dir=args.archive_dir,
        mode=args.mode,
        batch_size=args.batch_size,
        max_rows_per_table=args.max_rows_per_table,
        max_rows_total=args.max_rows_total,
        max_seconds=args.max_seconds,
        vacuum=args.vacuum,
    )
    print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
    return 0 if summary.get("status") in {"ok", "db_missing"} else 1


if __name__ == "__main__":
    raise SystemExit(main())

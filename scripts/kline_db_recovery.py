#!/usr/bin/env python3
"""Guarded kline SQLite recovery tool; defaults to a non-mutating dry-run."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import tempfile
from typing import Any

from sqlite_evidence_utils import (
    atomic_write_json,
    find_process_references,
    fsync_directory,
    inspect_sqlite,
    open_sqlite_readonly,
    parse_time,
    sha256_file,
    utc_now_iso,
)


SCHEMA_VERSION = "kline_db_recovery.v1"
APPROVAL_TYPE = "phase4_kline_recovery_h1"
KLINE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS kline_1m (
  token_ca TEXT NOT NULL,
  pool_address TEXT NOT NULL,
  timestamp INTEGER NOT NULL,
  open REAL NOT NULL,
  high REAL NOT NULL,
  low REAL NOT NULL,
  close REAL NOT NULL,
  volume REAL NOT NULL,
  provider TEXT DEFAULT 'geckoterminal',
  fetched_at INTEGER DEFAULT (strftime('%s','now')),
  PRIMARY KEY (token_ca, timestamp)
);
CREATE INDEX IF NOT EXISTS idx_kline_1m_lookup ON kline_1m(token_ca, timestamp);
CREATE TABLE IF NOT EXISTS pool_mapping (
  token_ca TEXT PRIMARY KEY,
  pool_address TEXT,
  provider TEXT,
  fetched_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS helius_trades (
  signature TEXT PRIMARY KEY,
  slot INTEGER,
  block_time INTEGER,
  token_ca TEXT NOT NULL,
  pool_address TEXT NOT NULL,
  price REAL NOT NULL,
  base_amount REAL,
  quote_amount REAL,
  volume REAL,
  side TEXT,
  source TEXT DEFAULT 'helius',
  ingested_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_helius_trades_pool_time ON helius_trades(pool_address, block_time);
CREATE INDEX IF NOT EXISTS idx_helius_trades_token_time ON helius_trades(token_ca, block_time);
CREATE TABLE IF NOT EXISTS history_backfill_cursor (
  pool_address TEXT PRIMARY KEY,
  token_ca TEXT,
  oldest_signature_seen TEXT,
  newest_signature_seen TEXT,
  oldest_block_time INTEGER,
  newest_block_time INTEGER,
  last_backfill_at INTEGER,
  status TEXT,
  error TEXT
);
"""


def load_json(path: str | os.PathLike[str]) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def default_quarantine_path(target: Path, now: datetime) -> Path:
    stamp = now.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return target.with_name(f"{target.name}.quarantine-{stamp}")


def validate_approval(
    marker_path: str | None,
    *,
    target: Path,
    operator: str | None,
    source_health: dict[str, Any],
    now: datetime,
    enforce_source_hash: bool = True,
) -> dict[str, Any]:
    if not marker_path:
        raise PermissionError("--approval-marker is required with --execute")
    marker = load_json(marker_path)
    if marker.get("approval_type") != APPROVAL_TYPE or marker.get("approved") is not True:
        raise PermissionError("approval marker is not an approved Phase 4 H1 marker")
    if Path(str(marker.get("target_db", ""))).expanduser().resolve() != target:
        raise PermissionError("approval marker target_db does not match --db")
    marker_operator = str(marker.get("operator") or "").strip()
    if not operator or marker_operator != operator:
        raise PermissionError("--operator must match approval marker operator")
    expires_at = parse_time(marker.get("expires_at"))
    if expires_at is None:
        raise PermissionError("approval marker must include a valid expires_at")
    if expires_at < now:
        raise PermissionError("approval marker is expired")
    if "expected_source_sha256" not in marker:
        raise PermissionError("approval marker must include expected_source_sha256")
    expected_hash = marker.get("expected_source_sha256")
    if enforce_source_hash and expected_hash != source_health.get("sha256"):
        raise PermissionError("source database hash changed since approval")
    if not marker.get("approval_id"):
        raise PermissionError("approval marker must include approval_id")
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,80}", str(marker["approval_id"])):
        raise PermissionError("approval_id contains unsafe characters")
    quarantine_value = marker.get("quarantine_path")
    if not quarantine_value:
        raise PermissionError("approval marker must freeze quarantine_path")
    quarantine = Path(str(quarantine_value)).expanduser().resolve()
    if quarantine.parent != target.parent or not quarantine.name.startswith(f"{target.name}.quarantine-"):
        raise PermissionError("quarantine_path must be a versioned sibling of target_db")
    return marker


def validate_maintenance(marker_path: str | None, *, target: Path) -> dict[str, Any]:
    if not marker_path:
        raise PermissionError("--maintenance-marker is required with --execute")
    marker = load_json(marker_path)
    if marker.get("maintenance_requested") is not True:
        raise PermissionError("maintenance marker is not active")
    if Path(str(marker.get("target_db", ""))).expanduser().resolve() != target:
        raise PermissionError("maintenance marker target_db does not match --db")
    if marker.get("all_kline_workers_acknowledged") is not True:
        raise PermissionError("all kline workers must acknowledge maintenance")
    required_workers = {str(value) for value in marker.get("required_kline_workers", []) if str(value).strip()}
    acknowledged_workers = {str(value) for value in marker.get("acknowledged_kline_workers", []) if str(value).strip()}
    if not required_workers or acknowledged_workers != required_workers:
        raise PermissionError("maintenance marker must enumerate and acknowledge every required kline worker")
    return marker


@contextmanager
def recovery_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError(f"recovery lock is held: {lock_path}") from error
        handle.seek(0)
        handle.truncate()
        handle.write(f"{os.getpid()} {utc_now_iso()}\n")
        handle.flush()
        os.fsync(handle.fileno())
        yield
    finally:
        try:
            fcntl.flock(handle, fcntl.LOCK_UN)
        finally:
            handle.close()


def atomic_move_no_clobber(source: Path, destination: Path) -> None:
    """Move a regular file on one filesystem without ever replacing destination."""
    source = source.expanduser()
    destination = destination.expanduser()
    source = source.parent.resolve() / source.name
    destination = destination.parent.resolve() / destination.name
    if source.parent != destination.parent:
        raise ValueError("no-clobber recovery moves must stay in the same directory")
    source_stat = source.lstat()
    if not stat.S_ISREG(source_stat.st_mode):
        raise ValueError(f"recovery source must be a regular file: {source}")
    linked = False
    try:
        # link(2) is atomic and fails with EEXIST if a destination appears after preflight.
        os.link(source, destination, follow_symlinks=False)
        linked = True
        os.unlink(source)
        fsync_directory(source.parent)
    except BaseException:
        if linked and source.exists():
            try:
                destination_stat = destination.stat()
                if destination_stat.st_ino == source_stat.st_ino:
                    destination.unlink()
                    fsync_directory(source.parent)
            except OSError:
                pass
        raise


def initialize_temp_database(temp_path: Path, schema_sql: str) -> dict[str, Any]:
    connection = sqlite3.connect(temp_path)
    try:
        connection.execute("PRAGMA journal_mode = DELETE")
        connection.executescript(schema_sql)
        connection.commit()
        quick_check = [row[0] for row in connection.execute("PRAGMA quick_check").fetchall()]
        if quick_check != ["ok"]:
            raise RuntimeError(f"temporary database quick_check failed: {quick_check[:20]}")
    finally:
        connection.close()
    with temp_path.open("rb") as fh:
        os.fsync(fh.fileno())
    health = inspect_sqlite(temp_path)
    if health["classification"] != "HEALTHY":
        raise RuntimeError(f"temporary database is not healthy: {health['classification']}")
    return health


def snapshot_last_known_good(source: Path, destination: Path) -> dict[str, Any]:
    fd, temp_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent)
    os.close(fd)
    os.unlink(temp_name)
    temp = Path(temp_name)
    source_connection = None
    destination_connection = None
    try:
        source_connection = open_sqlite_readonly(source, timeout_sec=30)
        destination_connection = sqlite3.connect(temp)
        source_connection.backup(destination_connection)
        destination_connection.commit()
        destination_connection.close()
        destination_connection = None
        source_connection.close()
        source_connection = None
        with temp.open("rb") as fh:
            os.fsync(fh.fileno())
        atomic_move_no_clobber(temp, destination)
    finally:
        if destination_connection is not None:
            destination_connection.close()
        if source_connection is not None:
            source_connection.close()
        if temp.exists():
            temp.unlink()
    return inspect_sqlite(destination)


def build_plan(
    db_path: str,
    *,
    now: datetime,
    approval_marker: str | None = None,
    proc_root: str = "/proc",
) -> dict[str, Any]:
    target = Path(db_path).expanduser().resolve()
    source_health = inspect_sqlite(target)
    marker = None
    if approval_marker and Path(approval_marker).exists():
        marker = load_json(approval_marker)
    proposed_quarantine = Path(str(marker.get("quarantine_path"))) if marker and marker.get("quarantine_path") else default_quarantine_path(target, now)
    process_references = find_process_references(target, proc_root=proc_root, exclude_pids=[os.getpid()])
    action = "NO_ACTION_HEALTHY" if source_health["classification"] == "HEALTHY" else "QUARANTINE_AND_INITIALIZE"
    if source_health["classification"] == "MISSING":
        action = "INITIALIZE_MISSING"
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "mode": "dry_run",
        "target_db": str(target),
        "source_health": source_health,
        "process_references": process_references,
        "proposed_action": action,
        "proposed_quarantine_path": str(proposed_quarantine),
        "proposed_last_known_good_path": f"{target}.last-known-good.<approval-id>.sqlite",
        "approval_required_for_execute": True,
        "maintenance_ack_required_for_execute": True,
        "mutation_performed": False,
        "promotion_allowed": False,
    }


def execute_recovery(
    plan: dict[str, Any],
    *,
    approval_marker: str,
    maintenance_marker: str,
    operator: str,
    schema_sql: str = KLINE_SCHEMA_SQL,
    proc_root: str = "/proc",
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    target = Path(plan["target_db"]).resolve()
    source_health = inspect_sqlite(target)
    approval = validate_approval(
        approval_marker,
        target=target,
        operator=operator,
        source_health=source_health,
        now=now,
        enforce_source_hash=source_health["classification"] != "HEALTHY",
    )
    maintenance = validate_maintenance(maintenance_marker, target=target)
    active = find_process_references(target, proc_root=proc_root, exclude_pids=[os.getpid()])
    if not active["available"]:
        raise RuntimeError(f"process reference audit is unavailable: proc_root={proc_root}")
    if active["errors"]:
        raise RuntimeError(f"process reference audit is incomplete: {active['errors']}")
    if active["references"]:
        raise RuntimeError(f"active processes still reference target database: {active['references']}")
    lock_path = target.with_name(f".{target.name}.recovery.lock")
    with recovery_lock(lock_path):
        source_health = inspect_sqlite(target)
        if source_health["classification"] == "HEALTHY":
            return {
                **plan,
                "mode": "execute",
                "completed_at": utc_now_iso(),
                "proposed_action": "NO_ACTION_HEALTHY",
                "result": "IDEMPOTENT_NO_ACTION",
                "approval_id": approval["approval_id"],
                "operator": operator,
                "mutation_performed": False,
            }
        expected_hash = approval.get("expected_source_sha256")
        if expected_hash != source_health.get("sha256"):
            raise PermissionError("source database hash changed after recovery lock acquisition")
        active_locked = find_process_references(target, proc_root=proc_root, exclude_pids=[os.getpid()])
        if not active_locked["available"]:
            raise RuntimeError(f"locked process reference audit is unavailable: proc_root={proc_root}")
        if active_locked["errors"]:
            raise RuntimeError(f"locked process reference audit is incomplete: {active_locked['errors']}")
        if active_locked["references"]:
            raise RuntimeError(f"active processes appeared after recovery lock acquisition: {active_locked['references']}")
        target.parent.mkdir(parents=True, exist_ok=True)
        quarantine = Path(str(approval.get("quarantine_path") or plan["proposed_quarantine_path"])).expanduser().resolve()
        if quarantine.exists():
            raise FileExistsError(f"quarantine destination already exists: {quarantine}")
        fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".recovery.tmp", dir=target.parent)
        os.close(fd)
        os.unlink(temp_name)
        temp = Path(temp_name)
        old_metadata = source_health
        quarantined_sidecars = []
        moved_target = False
        failure_stamp = now.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        failed_recovery_path = target.with_name(
            f"{target.name}.failed-recovery-{approval['approval_id']}-{failure_stamp}"
        )
        last_known_good = Path(f"{target}.last-known-good.{approval['approval_id']}.sqlite")
        if failed_recovery_path.exists():
            raise FileExistsError(f"failed-recovery evidence path exists: {failed_recovery_path}")
        if last_known_good.exists():
            raise FileExistsError(f"last-known-good snapshot already exists: {last_known_good}")
        for suffix in ("-wal", "-shm"):
            sidecar = Path(f"{target}{suffix}")
            sidecar_quarantine = Path(f"{quarantine}{suffix}")
            if sidecar.exists() and sidecar_quarantine.exists():
                raise FileExistsError(f"sidecar quarantine destination already exists: {sidecar_quarantine}")
        if not target.exists():
            orphan_sidecars = [
                str(Path(f"{target}{suffix}"))
                for suffix in ("-wal", "-shm")
                if Path(f"{target}{suffix}").exists()
            ]
            if orphan_sidecars:
                raise RuntimeError(f"missing target has orphan SQLite sidecars: {orphan_sidecars}")
        installed_new = False
        try:
            temp_health = initialize_temp_database(temp, schema_sql)
            if target.exists():
                atomic_move_no_clobber(target, quarantine)
                moved_target = True
                for suffix in ("-wal", "-shm"):
                    sidecar = Path(f"{target}{suffix}")
                    if sidecar.exists():
                        sidecar_quarantine = Path(f"{quarantine}{suffix}")
                        atomic_move_no_clobber(sidecar, sidecar_quarantine)
                        quarantined_sidecars.append(str(sidecar_quarantine))
            atomic_move_no_clobber(temp, target)
            installed_new = True
            new_health = inspect_sqlite(target)
            if new_health["classification"] != "HEALTHY":
                raise RuntimeError(f"new active database failed validation: {new_health['classification']}")
            lkg_health = snapshot_last_known_good(target, last_known_good)
        except BaseException:
            if temp.exists():
                temp.unlink()
            if installed_new and target.exists():
                atomic_move_no_clobber(target, failed_recovery_path)
            if moved_target and quarantine.exists():
                atomic_move_no_clobber(quarantine, target)
                for sidecar_value in quarantined_sidecars:
                    sidecar_quarantine = Path(sidecar_value)
                    if sidecar_quarantine.exists():
                        suffix = sidecar_quarantine.name.removeprefix(quarantine.name)
                        atomic_move_no_clobber(sidecar_quarantine, Path(f"{target}{suffix}"))
            raise

        result = {
            **plan,
            "mode": "execute",
            "completed_at": utc_now_iso(),
            "result": "RECOVERED",
            "approval_id": approval["approval_id"],
            "operator": operator,
            "maintenance_marker": maintenance,
            "process_references_at_execute": active_locked,
            "old_database": old_metadata,
            "quarantine_path": str(quarantine) if moved_target else None,
            "quarantined_sidecars": quarantined_sidecars,
            "new_database": new_health,
            "temporary_database_validation": temp_health,
            "last_known_good": lkg_health,
            "new_inode": new_health.get("inode"),
            "new_sha256": new_health.get("sha256"),
            "mutation_performed": True,
            "rollback": f"stop workers; move {target} aside; atomically restore {quarantine} to {target}",
            "promotion_allowed": False,
        }
        manifest_path = target.parent / "recovery" / f"kline_recovery_manifest_{approval['approval_id']}.json"
        result["manifest_path"] = str(manifest_path)
        atomic_write_json(manifest_path, result)
        return result


def write_markers(root: Path, target: Path, source_hash: str | None, quarantine: Path) -> tuple[Path, Path]:
    approval = root / "approval.json"
    maintenance = root / "maintenance.json"
    atomic_write_json(approval, {
        "approval_type": APPROVAL_TYPE,
        "approval_id": "self-test-h1",
        "approved": True,
        "operator": "self-test",
        "target_db": str(target),
        "expected_source_sha256": source_hash,
        "quarantine_path": str(quarantine),
        "expires_at": "2999-01-01T00:00:00Z",
    })
    atomic_write_json(maintenance, {
        "maintenance_requested": True,
        "target_db": str(target),
        "all_kline_workers_acknowledged": True,
        "required_kline_workers": ["self-test-worker"],
        "acknowledged_kline_workers": ["self-test-worker"],
    })
    return approval, maintenance


def self_test() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        empty_proc = root / "proc-empty"
        empty_proc.mkdir()
        zero = root / "zero.db"
        zero.write_bytes(b"\x00" * 4096)
        before = sha256_file(zero)
        plan = build_plan(str(zero), now=datetime(2026, 7, 10, tzinfo=timezone.utc), proc_root=str(empty_proc))
        assert plan["proposed_action"] == "QUARANTINE_AND_INITIALIZE"
        assert sha256_file(zero) == before
        assert not Path(plan["proposed_quarantine_path"]).exists()

        quarantine = root / "zero.db.quarantine-approved"
        approval, maintenance = write_markers(root, zero, before, quarantine)
        result = execute_recovery(
            plan,
            approval_marker=str(approval),
            maintenance_marker=str(maintenance),
            operator="self-test",
            proc_root=str(empty_proc),
            now=datetime(2026, 7, 10, tzinfo=timezone.utc),
        )
        assert result["result"] == "RECOVERED"
        assert inspect_sqlite(zero)["classification"] == "HEALTHY"
        assert quarantine.exists() and sha256_file(quarantine) == before
        assert Path(result["last_known_good"]["path"]).exists()
        second = execute_recovery(
            build_plan(str(zero), now=datetime(2026, 7, 10, tzinfo=timezone.utc), proc_root=str(empty_proc)),
            approval_marker=str(approval),
            maintenance_marker=str(maintenance),
            operator="self-test",
            proc_root=str(empty_proc),
            now=datetime(2026, 7, 10, tzinfo=timezone.utc),
        )
        assert second["result"] == "IDEMPOTENT_NO_ACTION"

        valid_hash = sha256_file(zero)
        healthy_plan = build_plan(str(zero), now=datetime(2026, 7, 10, tzinfo=timezone.utc), proc_root=str(empty_proc))
        assert healthy_plan["proposed_action"] == "NO_ACTION_HEALTHY"
        assert sha256_file(zero) == valid_hash

        blocked = root / "blocked.db"
        blocked.write_bytes(b"\x00" * 4096)
        blocked_hash = sha256_file(blocked)
        fake_proc = root / "proc"
        fd_dir = fake_proc / "999" / "fd"
        fd_dir.mkdir(parents=True)
        (fake_proc / "999" / "cmdline").write_bytes(b"raw-path-observer\x00")
        (fd_dir / "7").symlink_to(blocked)
        blocked_plan = build_plan(str(blocked), now=datetime(2026, 7, 10, tzinfo=timezone.utc), proc_root=str(fake_proc))
        blocked_quarantine = root / "blocked.db.quarantine-approved"
        blocked_approval, blocked_maintenance = write_markers(
            root / "blocked-markers", blocked, blocked_hash, blocked_quarantine
        )
        try:
            execute_recovery(
                blocked_plan,
                approval_marker=str(blocked_approval),
                maintenance_marker=str(blocked_maintenance),
                operator="self-test",
                proc_root=str(fake_proc),
                now=datetime(2026, 7, 10, tzinfo=timezone.utc),
            )
        except RuntimeError as error:
            assert "active processes" in str(error)
        else:
            raise AssertionError("active database user must block recovery")
        assert sha256_file(blocked) == blocked_hash
    print("SELF_TEST_PASS kline_db_recovery")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/kline_cache.db")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--execute", action="store_true")
    parser.add_argument("--approval-marker")
    parser.add_argument("--maintenance-marker")
    parser.add_argument("--operator")
    parser.add_argument("--proc-root", default="/proc")
    parser.add_argument("--out")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        self_test()
        return 0
    now = datetime.now(timezone.utc)
    plan = build_plan(args.db, now=now, approval_marker=args.approval_marker, proc_root=args.proc_root)
    report = plan
    if args.execute:
        if Path(args.proc_root).resolve() != Path("/proc"):
            raise SystemExit("--execute requires the real /proc process audit")
        report = execute_recovery(
            plan,
            approval_marker=args.approval_marker or "",
            maintenance_marker=args.maintenance_marker or "",
            operator=args.operator or "",
            schema_sql=KLINE_SCHEMA_SQL,
            proc_root=args.proc_root,
            now=now,
        )
    if args.out:
        atomic_write_json(args.out, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

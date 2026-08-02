#!/usr/bin/env python3
"""Fail-closed evaluator database source contract."""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import json
from pathlib import Path
import sqlite3
import time
from urllib.parse import quote


SCHEMA_VERSION = "evaluator_db_source_contract.v1"
SNAPSHOT_SCHEMA_VERSION = "cross_db_evaluator_snapshot.v1"
SNAPSHOT_FILES = {
    "signal": "signal.db",
    "paper": "paper_evidence.db",
    "raw": "raw.db",
    "kline": "kline.db",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sqlite_quick_check(path: Path) -> list[str]:
    uri = f"file:{quote(str(path.resolve()), safe='/')}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True, timeout=30)
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        return [str(row[0]) for row in connection.execute("PRAGMA quick_check").fetchall()]
    finally:
        connection.close()


def evaluator_db_source_status(
    paper_db: str,
    data_dir: str,
) -> dict:
    candidate = Path(paper_db).expanduser().resolve()
    live = (Path(data_dir).expanduser().resolve() / "paper_trades.db").resolve()
    exists = candidate.is_file()
    is_live = candidate == live
    blockers = []
    if not exists:
        blockers.append("evaluator_db_missing")
    if is_live:
        blockers.append("active_paper_db_forbidden_for_evaluator")
    return {
        "schema_version": SCHEMA_VERSION,
        "paper_db": str(candidate),
        "live_paper_db": str(live),
        "exists": exists,
        "is_live_paper_db": is_live,
        "accepted": not blockers,
        "blockers": blockers,
        "promotion_allowed": False,
    }


def require_evaluator_db_source(
    paper_db: str,
    data_dir: str,
) -> dict:
    status = evaluator_db_source_status(paper_db, data_dir)
    if not status["accepted"]:
        raise RuntimeError(
            "evaluator_db_source_blocked "
            + ",".join(status["blockers"])
            + f" paper_db={status['paper_db']}"
        )
    return status


def evaluator_snapshot_bundle_status(
    *,
    signal_db: str,
    paper_db: str,
    raw_db: str,
    kline_db: str,
    data_dir: str,
    manifest_path: str | None = None,
    max_age_sec: float = 28800,
    now_ts: float | None = None,
) -> dict:
    candidates = {
        "signal": Path(signal_db).expanduser().resolve(),
        "paper": Path(paper_db).expanduser().resolve(),
        "raw": Path(raw_db).expanduser().resolve(),
        "kline": Path(kline_db).expanduser().resolve(),
    }
    data_root = Path(data_dir).expanduser().resolve()
    live = {
        "signal": (data_root / "sentiment_arb.db").resolve(),
        "paper": (data_root / "paper_trades.db").resolve(),
        "raw": (data_root / "raw_signal_outcomes.db").resolve(),
        "kline": (data_root / "kline_cache.db").resolve(),
    }
    manifest_file = (
        Path(manifest_path).expanduser().resolve()
        if manifest_path
        else (Path(paper_db).expanduser().parent / "manifest.json").resolve()
    )
    blockers: list[str] = []
    for name, candidate in candidates.items():
        if not candidate.is_file():
            blockers.append(f"evaluator_snapshot_{name}_db_missing")
        if candidate == live[name]:
            blockers.append(f"active_{name}_db_forbidden_for_evaluator")
    manifest: dict = {}
    verified_integrity: dict[str, dict] = {}
    if not manifest_file.is_file():
        blockers.append("evaluator_snapshot_manifest_missing")
    else:
        try:
            manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
        except Exception:
            blockers.append("evaluator_snapshot_manifest_invalid_json")
    if manifest:
        if manifest.get("schema_version") != SNAPSHOT_SCHEMA_VERSION:
            blockers.append("evaluator_snapshot_schema_version_invalid")
        if manifest.get("accepted") is not True:
            blockers.append("evaluator_snapshot_not_accepted")
        if manifest.get("quick_checks_passed") is not True:
            blockers.append("evaluator_snapshot_quick_check_not_passed")
        if manifest.get("required_tables_present") is not True:
            blockers.append("evaluator_snapshot_required_tables_missing")
        if manifest.get("required_watermarks_present") is not True:
            blockers.append("evaluator_snapshot_required_watermarks_missing")
        if manifest.get("cross_database_time_skew_passed") is not True:
            blockers.append("evaluator_snapshot_cross_database_time_skew_failed")
        if manifest.get("read_views_pinned_before_copy") is not True:
            blockers.append("evaluator_snapshot_read_views_not_pinned")
        if manifest.get("source_mutation_free") is not True:
            blockers.append("evaluator_snapshot_source_mutation_contract_failed")
        if not manifest.get("git_commit"):
            blockers.append("evaluator_snapshot_git_commit_missing")
        if manifest.get("snapshot_ts") is None:
            blockers.append("evaluator_snapshot_timestamp_missing")
        else:
            snapshot_age_sec = float(now_ts if now_ts is not None else time.time()) - float(manifest["snapshot_ts"])
            if snapshot_age_sec < -60:
                blockers.append("evaluator_snapshot_timestamp_in_future")
            if max_age_sec > 0 and snapshot_age_sec > float(max_age_sec):
                blockers.append("evaluator_snapshot_stale")
        reports = manifest.get("databases") or {}
        for name, candidate in candidates.items():
            report = reports.get(name) or {}
            expected_path = report.get("snapshot_path")
            if not expected_path or Path(expected_path).expanduser().resolve() != candidate:
                blockers.append(f"evaluator_snapshot_{name}_path_mismatch")
            if report.get("quick_check") != ["ok"]:
                blockers.append(f"evaluator_snapshot_{name}_quick_check_invalid")
            expected_sha = report.get("snapshot_sha256")
            if not expected_sha:
                blockers.append(f"evaluator_snapshot_{name}_sha256_missing")
            if report.get("schema_version") is None:
                blockers.append(f"evaluator_snapshot_{name}_schema_version_missing")
            if not report.get("upper_watermarks"):
                blockers.append(f"evaluator_snapshot_{name}_watermarks_missing")
            if report.get("missing_required_watermarks"):
                blockers.append(f"evaluator_snapshot_{name}_required_watermarks_missing")
            if candidate.is_file() and int(report.get("snapshot_size_bytes") or -1) != candidate.stat().st_size:
                blockers.append(f"evaluator_snapshot_{name}_size_mismatch")
            if candidate.is_file():
                try:
                    actual_sha = sha256_file(candidate)
                    actual_quick_check = sqlite_quick_check(candidate)
                    verified_integrity[name] = {
                        "sha256": actual_sha,
                        "sha256_matches_manifest": bool(expected_sha and actual_sha == expected_sha),
                        "quick_check": actual_quick_check,
                    }
                    if not expected_sha or actual_sha != expected_sha:
                        blockers.append(f"evaluator_snapshot_{name}_sha256_mismatch")
                    if actual_quick_check != ["ok"]:
                        blockers.append(f"evaluator_snapshot_{name}_quick_check_revalidation_failed")
                except Exception as exc:
                    verified_integrity[name] = {
                        "error": f"{type(exc).__name__}:{exc}",
                    }
                    blockers.append(f"evaluator_snapshot_{name}_integrity_revalidation_failed")
    blockers = list(dict.fromkeys(blockers))
    return {
        "schema_version": "evaluator_snapshot_bundle_contract.v1",
        "manifest_path": str(manifest_file),
        "snapshot_id": manifest.get("snapshot_id") if manifest else None,
        "snapshot_ts": manifest.get("snapshot_ts") if manifest else None,
        "snapshot_age_sec": (
            round(float(now_ts if now_ts is not None else time.time()) - float(manifest["snapshot_ts"]), 6)
            if manifest and manifest.get("snapshot_ts") is not None
            else None
        ),
        "max_snapshot_age_sec": float(max_age_sec),
        "git_commit": manifest.get("git_commit") if manifest else None,
        "databases": {name: str(path) for name, path in candidates.items()},
        "verified_integrity": verified_integrity,
        "live_databases": {name: str(path) for name, path in live.items()},
        "accepted": not blockers,
        "blockers": blockers,
        "promotion_allowed": False,
    }


def require_evaluator_snapshot_bundle(**kwargs) -> dict:
    status = evaluator_snapshot_bundle_status(**kwargs)
    if not status["accepted"]:
        raise RuntimeError(
            "evaluator_snapshot_bundle_blocked "
            + ",".join(status["blockers"])
            + f" manifest={status['manifest_path']}"
        )
    return status


@contextmanager
def evaluator_snapshot_bundle_lease(*, lock_file: str, lock_timeout_sec: float = 300, **kwargs):
    """Hold a shared lease while one evaluator run uses an immutable bundle."""
    lock_path = Path(lock_file).expanduser().resolve()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+", encoding="utf-8")
    deadline = time.monotonic() + max(0.0, float(lock_timeout_sec))
    try:
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"evaluator_snapshot_lease_timeout:{lock_path}")
                time.sleep(0.1)
        status = require_evaluator_snapshot_bundle(**kwargs)
        try:
            yield status
        except BaseException:
            post_status = require_evaluator_snapshot_bundle(**kwargs)
            if post_status.get("snapshot_id") != status.get("snapshot_id"):
                raise RuntimeError("evaluator_snapshot_changed_during_lease")
            raise
        else:
            post_status = require_evaluator_snapshot_bundle(**kwargs)
            if post_status.get("snapshot_id") != status.get("snapshot_id"):
                raise RuntimeError("evaluator_snapshot_changed_during_lease")
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()

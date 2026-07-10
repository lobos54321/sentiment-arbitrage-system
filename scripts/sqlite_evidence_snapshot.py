#!/usr/bin/env python3
"""Create a consistent read-only-source SQLite evidence snapshot and manifest."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sqlite3
import tempfile

from sqlite_evidence_utils import (
    atomic_write_json,
    fsync_directory,
    inspect_sqlite,
    open_sqlite_readonly,
    sha256_file,
    utc_now_iso,
)


SCHEMA_VERSION = "sqlite_evidence_snapshot.v1"


def source_fingerprint(health: dict) -> dict:
    return {
        "main_sha256": health.get("sha256"),
        "main_size_bytes": health.get("size_bytes"),
        "main_mtime_epoch": health.get("mtime_epoch"),
        "wal_size_bytes": (health.get("wal") or {}).get("size_bytes"),
        "wal_mtime_epoch": (health.get("wal") or {}).get("mtime_epoch"),
        "shm_size_bytes": (health.get("shm") or {}).get("size_bytes"),
        "shm_mtime_epoch": (health.get("shm") or {}).get("mtime_epoch"),
    }


def create_snapshot(source_path: str, destination_path: str, *, replace: bool = False) -> dict:
    source = Path(source_path).expanduser().resolve()
    destination = Path(destination_path).expanduser().resolve()
    if source == destination:
        raise ValueError("source and destination must differ")
    source_health = inspect_sqlite(source)
    if source_health["classification"] != "HEALTHY":
        raise RuntimeError(f"source SQLite is not healthy: {source_health['classification']}")
    if destination.exists() and not replace:
        raise FileExistsError(f"destination exists; pass --replace: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".sqlite.tmp", dir=destination.parent)
    os.close(fd)
    os.unlink(temp_name)
    temp_path = Path(temp_name)
    source_connection = None
    destination_connection = None
    try:
        source_connection = open_sqlite_readonly(source, timeout_sec=30)
        destination_connection = sqlite3.connect(temp_path)
        source_connection.backup(destination_connection)
        destination_connection.commit()
        quick_check = [row[0] for row in destination_connection.execute("PRAGMA quick_check").fetchall()]
        if quick_check != ["ok"]:
            raise RuntimeError(f"snapshot quick_check failed: {quick_check[:20]}")
        destination_connection.close()
        destination_connection = None
        source_connection.close()
        source_connection = None
        with temp_path.open("rb") as fh:
            os.fsync(fh.fileno())
        snapshot_health = inspect_sqlite(temp_path)
        if snapshot_health["classification"] != "HEALTHY":
            raise RuntimeError(f"snapshot validation failed: {snapshot_health['classification']}")
        os.replace(temp_path, destination)
        fsync_directory(destination.parent)
    finally:
        if destination_connection is not None:
            destination_connection.close()
        if source_connection is not None:
            source_connection.close()
        if temp_path.exists():
            temp_path.unlink()
    source_health_after = inspect_sqlite(source)
    fingerprint_before = source_fingerprint(source_health)
    fingerprint_after = source_fingerprint(source_health_after)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now_iso(),
        "source_read_only": True,
        "source": source_health,
        "snapshot": inspect_sqlite(destination),
        "source_sha256_before": source_health["sha256"],
        "source_sha256_after": source_health_after["sha256"],
        "source_fingerprint_before": fingerprint_before,
        "source_fingerprint_after": fingerprint_after,
        "source_changed_during_snapshot": fingerprint_before != fingerprint_after,
        "source_after": source_health_after,
        "snapshot_sha256": sha256_file(destination),
        "consistent_snapshot_method": "sqlite_backup_api",
        "promotion_allowed": False,
    }


def self_test() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source = root / "source.db"
        destination = root / "snapshot.db"
        connection = sqlite3.connect(source)
        connection.execute("CREATE TABLE evidence (id INTEGER PRIMARY KEY, value TEXT)")
        connection.execute("INSERT INTO evidence(value) VALUES ('ok')")
        connection.commit()
        connection.close()
        source_hash = sha256_file(source)
        manifest = create_snapshot(str(source), str(destination))
        assert manifest["snapshot"]["classification"] == "HEALTHY"
        assert sha256_file(source) == source_hash
        check = sqlite3.connect(destination)
        assert check.execute("SELECT value FROM evidence").fetchone()[0] == "ok"
        check.close()
        try:
            create_snapshot(str(source), str(destination))
        except FileExistsError:
            pass
        else:
            raise AssertionError("existing destination must require --replace")
    print("SELF_TEST_PASS sqlite_evidence_snapshot")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source")
    parser.add_argument("--out")
    parser.add_argument("--manifest")
    parser.add_argument("--replace", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        self_test()
        return 0
    if not args.source or not args.out:
        raise SystemExit("--source and --out are required")
    manifest = create_snapshot(args.source, args.out, replace=args.replace)
    manifest_path = args.manifest or f"{args.out}.manifest.json"
    atomic_write_json(manifest_path, manifest)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

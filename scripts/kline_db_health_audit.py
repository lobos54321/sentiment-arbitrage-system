#!/usr/bin/env python3
"""Read-only health and writer-ownership audit for the active kline database."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import tempfile

from sqlite_evidence_utils import atomic_write_json, find_process_references, inspect_sqlite, utc_now_iso


SCHEMA_VERSION = "kline_db_health_audit.v1"


def build_report(db_path: str, alternate_paths: list[str], *, proc_root: str = "/proc") -> dict:
    primary = inspect_sqlite(db_path)
    process_references = find_process_references(db_path, proc_root=proc_root)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now_iso(),
        "read_only": True,
        "primary": primary,
        "alternates": [inspect_sqlite(path) for path in alternate_paths],
        "process_references": process_references,
        "classification": primary["classification"],
        "mutation_performed": False,
        "promotion_allowed": False,
    }


def self_test() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        valid = root / "valid.db"
        connection = sqlite3.connect(valid)
        connection.execute("CREATE TABLE kline_1m (token_ca TEXT, timestamp INTEGER)")
        connection.execute("INSERT INTO kline_1m VALUES ('DOG', 123)")
        connection.commit()
        connection.close()

        zero = root / "zero.db"
        zero.write_bytes(b"\x00" * 4096)
        malformed = root / "malformed.db"
        malformed.write_bytes(b"SQLite format 3\x00" + b"not-a-real-database")

        assert inspect_sqlite(valid)["classification"] == "HEALTHY"
        assert inspect_sqlite(valid)["tables"][0]["row_count"] == 1
        assert inspect_sqlite(zero)["classification"] == "INVALID_HEADER"
        assert inspect_sqlite(zero)["all_zero_first_64_bytes"] is True
        assert inspect_sqlite(malformed)["classification"] == "MALFORMED"
        assert inspect_sqlite(root / "missing.db")["classification"] == "MISSING"
        before = zero.read_bytes()
        build_report(str(zero), [str(valid)], proc_root=str(root / "no-proc"))
        assert zero.read_bytes() == before
    print("SELF_TEST_PASS kline_db_health_audit")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/kline_cache.db")
    parser.add_argument("--alternate-db", action="append", default=[])
    parser.add_argument("--proc-root", default="/proc")
    parser.add_argument("--out")
    parser.add_argument("--snapshot-out")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        self_test()
        return 0
    report = build_report(args.db, args.alternate_db, proc_root=args.proc_root)
    if args.out:
        atomic_write_json(args.out, report)
    if args.snapshot_out:
        atomic_write_json(args.snapshot_out, {
            "schema_version": "evidence_recovery_snapshot.v1",
            "generated_at": report["generated_at"],
            "kline_db_health": report,
            "mutation_performed": False,
        })
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["classification"] == "HEALTHY" else 2


if __name__ == "__main__":
    raise SystemExit(main())

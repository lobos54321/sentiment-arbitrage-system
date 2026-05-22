#!/usr/bin/env python3
"""Quarantine an invalid v2.7 seed event log before mirrors reseed it.

The v2.7 sidecar event log is a rebuildable seed log derived from existing
paper/signal databases. If its sequencer invariants are already broken, the
safe recovery action is to preserve the invalid evidence and let the mirrors
recreate a fresh append-only log from the upstream source databases.
"""

import argparse
import fcntl
import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_event_log import EVENT_FILE, LOCK_FILE, STATE_FILE, V27EventLog, sha256_hex  # noqa: E402


DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
DEFAULT_RECOVERY_DIR = PROJECT_ROOT / "data" / "recovery" / "v27_event_log"


def _utc_now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _utc_path_stamp():
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def _write_json_atomic(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, sort_keys=True, indent=2)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
    finally:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except OSError:
            pass


def _verify_event_log(event_log_dir):
    try:
        return {"ok": True, "verify": V27EventLog(event_log_dir).verify(), "error": None}
    except Exception as exc:
        return {"ok": False, "verify": None, "error": str(exc), "error_type": type(exc).__name__}


def _recoverable_files(event_log_dir):
    names = {
        EVENT_FILE,
        STATE_FILE,
        Path(STATE_FILE).with_suffix(".tmp").name,
    }
    return [event_log_dir / name for name in sorted(names) if (event_log_dir / name).exists()]


def recover_event_log(*, event_log_dir=DEFAULT_EVENT_LOG_DIR, recovery_dir=DEFAULT_RECOVERY_DIR, quarantine_invalid=False):
    event_log_dir = Path(event_log_dir)
    recovery_dir = Path(recovery_dir)
    event_log_dir.mkdir(parents=True, exist_ok=True)
    recovery_dir.mkdir(parents=True, exist_ok=True)

    lock_path = event_log_dir / LOCK_FILE
    with lock_path.open("a+", encoding="utf-8") as lock_fh:
        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
        preflight = _verify_event_log(event_log_dir)
        report = {
            "recovery_schema_version": "v2.7.0.event_log_recovery.v1",
            "checked_at": _utc_now_iso(),
            "event_log_dir": str(event_log_dir),
            "recovery_dir": str(recovery_dir),
            "preflight": preflight,
            "quarantine_invalid": bool(quarantine_invalid),
            "status": "ok" if preflight["ok"] else "invalid",
            "quarantine_path": None,
            "moved_files": [],
            "post_recovery": None,
        }

        if preflight["ok"]:
            return report
        if not quarantine_invalid:
            return report

        suffix = sha256_hex({"event_log_dir": str(event_log_dir), "error": preflight.get("error"), "checked_at": report["checked_at"]})[:10]
        quarantine_path = recovery_dir / f"invalid_{_utc_path_stamp()}_{suffix}"
        quarantine_path.mkdir(parents=True, exist_ok=False)
        report["quarantine_path"] = str(quarantine_path)

        for source_path in _recoverable_files(event_log_dir):
            target_path = quarantine_path / source_path.name
            shutil.move(str(source_path), str(target_path))
            report["moved_files"].append({"from": str(source_path), "to": str(target_path)})

        report["post_recovery"] = _verify_event_log(event_log_dir)
        report["status"] = "quarantined" if report["post_recovery"]["ok"] else "recovery_failed"
        _write_json_atomic(quarantine_path / "recovery-report.json", report)
        _write_json_atomic(event_log_dir / "last-recovery-report.json", report)
        return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-log-dir", default=str(DEFAULT_EVENT_LOG_DIR))
    parser.add_argument("--recovery-dir", default=str(DEFAULT_RECOVERY_DIR))
    parser.add_argument("--quarantine-invalid", action="store_true")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    report = recover_event_log(
        event_log_dir=Path(args.event_log_dir),
        recovery_dir=Path(args.recovery_dir),
        quarantine_invalid=args.quarantine_invalid,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    if args.strict and report.get("status") not in {"ok", "quarantined"}:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

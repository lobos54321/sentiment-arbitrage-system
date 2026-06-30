#!/usr/bin/env python3
"""Build a read-only runtime health snapshot for the capture loop.

This audit only summarizes local runtime artifacts. It never changes strategy,
runtime mode, gates, executor state, wallet settings, or risk controls.
"""

from __future__ import annotations

import argparse
import json
import re
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path


SCHEMA_VERSION = "runtime_health_snapshot_audit.v1"


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def now_ts() -> float:
    return time.time()


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def read_json(path):
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return None


def parse_ts(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        pass
    normalized = text.replace("Z", "+00:00")
    for candidate in (normalized, normalized.replace(" ", "T", 1)):
        try:
            parsed = datetime.fromisoformat(candidate)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.timestamp()
        except ValueError:
            continue
    return None


def file_info(path, *, ts_fields=None):
    target = Path(path)
    info = {
        "path": str(target),
        "available": target.exists(),
        "is_file": target.is_file() if target.exists() else False,
    }
    if not target.exists():
        return info
    stat = target.stat()
    info.update({
        "size_bytes": stat.st_size,
        "mtime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(stat.st_mtime)),
        "mtime_age_minutes": round((now_ts() - stat.st_mtime) / 60.0, 3),
    })
    data = read_json(target)
    if data is not None:
        info["json_available"] = True
        for field in ts_fields or ():
            ts = parse_ts(data.get(field))
            if ts is not None:
                info[f"{field}_age_minutes"] = round((now_ts() - ts) / 60.0, 3)
                info[f"{field}_iso"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))
        info["raw"] = data
    else:
        info["json_available"] = False
    return info


def latest_age_from_json_or_mtime(info, *fields):
    for field in fields:
        key = f"{field}_age_minutes"
        if key in info:
            return info[key]
    return info.get("mtime_age_minutes")


def tail_lines(path, max_bytes, max_lines):
    target = Path(path)
    if not target.exists() or not target.is_file():
        return []
    size = target.stat().st_size
    with target.open("rb") as handle:
        if size > max_bytes:
            handle.seek(-max_bytes, 2)
        data = handle.read()
    text = data.decode("utf-8", errors="replace")
    return text.splitlines()[-max_lines:]


def line_timestamp(line, fallback_ts=None):
    iso_match = re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z", line)
    if iso_match:
        return parse_ts(iso_match.group(0))
    space_match = re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", line)
    if space_match:
        return parse_ts(space_match.group(0))
    return fallback_ts


def timestamp_in_recent_window(ts, max_age_minutes):
    if ts is None:
        return True
    return (now_ts() - ts) <= (float(max_age_minutes) * 60.0)


def observer_log_section(data_dir, name, filename, args):
    path = Path(data_dir) / filename
    info = file_info(path)
    warning_prefix = f"runtime_{name}_"
    warnings = []
    if not info.get("available"):
        warnings.append(f"{warning_prefix}log_missing")
        return {
            "available": False,
            "path": str(path),
            "status": "missing",
            "tail_lines_scanned": 0,
            "warnings": warnings,
        }
    lines = tail_lines(path, args.observer_log_tail_bytes, args.observer_log_tail_lines)
    database_locked_count = 0
    sqlite_busy_count = 0
    timeout_count = 0
    spawn_error_count = 0
    nonzero_exit_count = 0
    last_exit_code = None
    last_exit_signal = None
    last_ts = None
    latest_warning_ts = None
    exit_re = re.compile(r"exited code=([^\s]+) signal=([^;\s]*)")
    for line in lines:
        event_ts = line_timestamp(line, last_ts)
        if event_ts is not None:
            last_ts = event_ts
        if not timestamp_in_recent_window(event_ts, args.observer_log_max_age_minutes):
            continue
        lower = line.lower()
        if "database is locked" in lower:
            database_locked_count += 1
            latest_warning_ts = event_ts or latest_warning_ts
        if "sqlite_busy" in lower or "database table is locked" in lower or "database schema is locked" in lower:
            sqlite_busy_count += 1
            latest_warning_ts = event_ts or latest_warning_ts
        if "timeout after" in lower or "timed out" in lower:
            timeout_count += 1
            latest_warning_ts = event_ts or latest_warning_ts
        if "spawn error" in lower:
            spawn_error_count += 1
            latest_warning_ts = event_ts or latest_warning_ts
        match = exit_re.search(line)
        if match:
            code = match.group(1)
            signal = match.group(2) or None
            last_exit_code = code
            last_exit_signal = signal
            if code not in {"0", "None", "null"}:
                nonzero_exit_count += 1
                latest_warning_ts = event_ts or latest_warning_ts
    if database_locked_count or sqlite_busy_count:
        warnings.append(f"{warning_prefix}sqlite_lock_recent")
    if nonzero_exit_count:
        warnings.append(f"{warning_prefix}nonzero_exit_recent")
    if timeout_count:
        warnings.append(f"{warning_prefix}timeout_recent")
    if spawn_error_count:
        warnings.append(f"{warning_prefix}spawn_error_recent")
    return {
        "available": True,
        "path": str(path),
        "status": "warn" if warnings else "ok",
        "size_bytes": info.get("size_bytes"),
        "mtime": info.get("mtime"),
        "mtime_age_minutes": info.get("mtime_age_minutes"),
        "tail_lines_scanned": len(lines),
        "tail_bytes_scanned": min(info.get("size_bytes") or 0, args.observer_log_tail_bytes),
        "max_age_minutes": args.observer_log_max_age_minutes,
        "latest_warning_at": (
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(latest_warning_ts))
            if latest_warning_ts is not None else None
        ),
        "database_locked_count": database_locked_count,
        "sqlite_busy_count": sqlite_busy_count,
        "nonzero_exit_count": nonzero_exit_count,
        "timeout_count": timeout_count,
        "spawn_error_count": spawn_error_count,
        "last_exit_code": last_exit_code,
        "last_exit_signal": last_exit_signal,
        "warnings": warnings,
    }


def observer_logs_section(data_dir, args):
    raw_path = observer_log_section(data_dir, "raw_path_observer", "raw-path-observer.log", args)
    raw_dog = observer_log_section(data_dir, "raw_dog_discovery_observer", "raw-dog-discovery-observer.log", args)
    candidate_shadow = observer_log_section(data_dir, "candidate_shadow_observer", "candidate-shadow-observer.log", args)
    warnings = []
    for section in (raw_path, raw_dog, candidate_shadow):
        warnings.extend(section.get("warnings") or [])
    return {
        "status": "warn" if warnings else "ok",
        "tail_lines": args.observer_log_tail_lines,
        "tail_bytes": args.observer_log_tail_bytes,
        "warnings": sorted(set(warnings)),
        "raw_path_observer": raw_path,
        "raw_dog_discovery_observer": raw_dog,
        "candidate_shadow_observer": candidate_shadow,
    }


def signal_source_section(data_dir, args):
    path = Path(data_dir) / "v27_read_models" / "signal_source_freshness.json"
    info = file_info(path, ts_fields=("generated_at", "latest_ts"))
    raw = info.get("raw") or {}
    age = raw.get("age_minutes")
    try:
        age = float(age)
    except (TypeError, ValueError):
        age = latest_age_from_json_or_mtime(info, "generated_at")
    warn_after = float(raw.get("warn_after_minutes") or args.signal_warn_minutes)
    fail_after = float(raw.get("fail_closed_after_minutes") or args.signal_fail_minutes)
    fail_closed = bool(raw.get("fail_closed"))
    status = raw.get("status")
    blockers = []
    warnings = []
    if not info.get("available"):
        blockers.append("runtime_signal_source_freshness_missing")
    elif fail_closed or status == "stale_fail_closed" or (age is not None and age > fail_after):
        blockers.append("runtime_signal_source_stale_fail_closed")
    elif age is not None and age > warn_after:
        warnings.append("runtime_signal_source_warn_stale")
    return {
        "available": info.get("available"),
        "path": str(path),
        "status": status or ("missing" if not info.get("available") else "unknown"),
        "source": raw.get("source"),
        "total": raw.get("total"),
        "age_minutes": age,
        "warn_after_minutes": warn_after,
        "fail_closed_after_minutes": fail_after,
        "fail_closed": fail_closed,
        "latest_iso": raw.get("latest_iso") or info.get("latest_ts_iso"),
        "generated_at_iso": raw.get("generated_at_iso") or info.get("generated_at_iso"),
        "blockers": blockers,
        "warnings": warnings,
    }


def paper_review_section(data_dir, args):
    path = Path(data_dir) / "review-artifacts" / "live" / "paper_review_24h.json"
    info = file_info(path, ts_fields=("generated_at",))
    raw = info.get("raw") or {}
    age = latest_age_from_json_or_mtime(info, "generated_at")
    warnings = []
    if not info.get("available"):
        warnings.append("runtime_paper_review_snapshot_missing")
    elif age is None or age > args.paper_review_max_age_minutes:
        warnings.append("runtime_paper_review_snapshot_stale")
    return {
        "available": info.get("available"),
        "path": str(path),
        "status": "ok" if info.get("available") and not warnings else "stale_or_missing",
        "generated_at": raw.get("generated_at"),
        "snapshot_id": raw.get("snapshot_id"),
        "requested_hours": raw.get("requested_hours"),
        "materialized_hours": raw.get("materialized_hours"),
        "age_minutes": age,
        "max_age_minutes": args.paper_review_max_age_minutes,
        "warnings": warnings,
    }


def paper_fast_lane_section(data_dir, args):
    path = Path(data_dir) / "paper-fast-lane-health.json"
    info = file_info(path, ts_fields=("updated_at", "heartbeat_at"))
    raw = info.get("raw") or {}
    age = latest_age_from_json_or_mtime(info, "updated_at", "heartbeat_at")
    warnings = []
    if not info.get("available"):
        warnings.append("runtime_paper_fast_lane_health_missing")
    elif age is None or age > args.paper_fast_lane_max_age_minutes:
        warnings.append("runtime_paper_fast_lane_health_stale")
    return {
        "available": info.get("available"),
        "path": str(path),
        "status": "ok" if info.get("available") and not warnings else "stale_or_missing",
        "updated_at": raw.get("updated_at"),
        "heartbeat_at": raw.get("heartbeat_at"),
        "worker_state": raw.get("worker_state"),
        "paper_db_exists": raw.get("paper_db_exists"),
        "age_minutes": age,
        "max_age_minutes": args.paper_fast_lane_max_age_minutes,
        "warnings": warnings,
    }


def paper_db_section(data_dir):
    path = Path(data_dir) / "paper_trades.db"
    integrity_marker = Path(data_dir) / "paper_trades.db.integrity_error"
    blockers = []
    if not path.exists() or not path.is_file():
        blockers.append("runtime_paper_db_unavailable")
    if integrity_marker.exists():
        blockers.append("runtime_paper_db_integrity_marker_exists")
    stat = path.stat() if path.exists() else None
    return {
        "available": path.exists() and path.is_file(),
        "path": str(path),
        "status": "ok" if not blockers else "blocked",
        "size_bytes": stat.st_size if stat else None,
        "mtime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(stat.st_mtime)) if stat else None,
        "mtime_age_minutes": round((now_ts() - stat.st_mtime) / 60.0, 3) if stat else None,
        "integrity_marker": {
            "exists": integrity_marker.exists(),
            "path": str(integrity_marker),
        },
        "blockers": blockers,
    }


def runtime_final_evidence_section(data_dir, args):
    path = Path(data_dir) / "runtime_final_evidence.jsonl"
    blockers = []
    warnings = []
    if not path.exists() or not path.is_file():
        blockers.append("runtime_final_evidence_missing")
        return {
            "available": False,
            "path": str(path),
            "status": "missing",
            "blockers": blockers,
            "warnings": warnings,
        }
    stat = path.stat()
    age = (now_ts() - stat.st_mtime) / 60.0
    if age > args.runtime_final_evidence_max_age_minutes:
        warnings.append("runtime_final_evidence_stale")
    return {
        "available": True,
        "path": str(path),
        "status": "ok" if not warnings else "stale",
        "size_bytes": stat.st_size,
        "mtime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(stat.st_mtime)),
        "mtime_age_minutes": round(age, 3),
        "max_age_minutes": args.runtime_final_evidence_max_age_minutes,
        "blockers": blockers,
        "warnings": warnings,
    }


def build_report(args):
    data_dir = Path(args.data_dir)
    sections = {
        "signal_source_freshness": signal_source_section(data_dir, args),
        "paper_review_snapshot": paper_review_section(data_dir, args),
        "paper_fast_lane_health": paper_fast_lane_section(data_dir, args),
        "paper_db": paper_db_section(data_dir),
        "runtime_final_evidence": runtime_final_evidence_section(data_dir, args),
        "observer_logs": observer_logs_section(data_dir, args),
    }
    blockers = []
    warnings = []
    for section in sections.values():
        blockers.extend(section.get("blockers") or [])
        warnings.extend(section.get("warnings") or [])
    blockers = sorted(set(blockers))
    warnings = sorted(set(warnings))
    status = "blocked" if blockers else ("degraded" if warnings else "ok")
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "report_type": "runtime_health_snapshot",
        "evidence_role": "read_only_runtime_health_context",
        "hours": args.hours,
        "data_dir": str(data_dir),
        "status": status,
        "blockers": blockers,
        "warnings": warnings,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        **sections,
    }


def self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        (root / "v27_read_models").mkdir(parents=True)
        (root / "review-artifacts" / "live").mkdir(parents=True)
        now = now_ts()
        write_json(root / "v27_read_models" / "signal_source_freshness.json", {
            "schema_version": "v1.signal_source_freshness_health",
            "status": "ok",
            "age_minutes": 1,
            "warn_after_minutes": 15,
            "fail_closed_after_minutes": 45,
            "fail_closed": False,
            "generated_at": now,
            "latest_ts": now - 60,
            "source": "local",
            "total": 10,
        })
        write_json(root / "review-artifacts" / "live" / "paper_review_24h.json", {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - 3600)),
            "snapshot_id": "fixture",
            "requested_hours": 24,
            "materialized_hours": 24,
        })
        write_json(root / "paper-fast-lane-health.json", {
            "schema_version": "fixture",
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - 7200)),
            "worker_state": "scanned",
            "paper_db_exists": True,
        })
        (root / "paper_trades.db").write_bytes(b"fixture")
        (root / "runtime_final_evidence.jsonl").write_text("{}\n", encoding="utf-8")
        args = argparse.Namespace(
            data_dir=str(root),
            hours=24,
            paper_review_max_age_minutes=30,
            paper_fast_lane_max_age_minutes=30,
            runtime_final_evidence_max_age_minutes=60,
            signal_warn_minutes=15,
            signal_fail_minutes=45,
            observer_log_tail_lines=200,
            observer_log_tail_bytes=20000,
            observer_log_max_age_minutes=120,
        )
        (root / "raw-path-observer.log").write_text(
            "[raw-path-observer-supervisor] exited code=1 signal=; next run in 120s\n"
            "SqliteError: database is locked\n",
            encoding="utf-8",
        )
        (root / "raw-dog-discovery-observer.log").write_text(
            "[raw-dog-discovery-supervisor] exited code=2 signal=; next run in 300s\n",
            encoding="utf-8",
        )
        (root / "candidate-shadow-observer.log").write_text(
            "[candidate-shadow-observer] ok\n",
            encoding="utf-8",
        )
        report = build_report(args)
        assert report["status"] == "degraded"
        assert "runtime_paper_review_snapshot_stale" in report["warnings"]
        assert "runtime_paper_fast_lane_health_stale" in report["warnings"]
        assert "runtime_raw_path_observer_sqlite_lock_recent" in report["warnings"]
        assert "runtime_raw_path_observer_nonzero_exit_recent" in report["warnings"]
        assert "runtime_raw_dog_discovery_observer_nonzero_exit_recent" in report["warnings"]
        assert report["observer_logs"]["raw_path_observer"]["database_locked_count"] == 1
        assert not report["blockers"]
        (root / "paper_trades.db.integrity_error").write_text("bad", encoding="utf-8")
        write_json(root / "v27_read_models" / "signal_source_freshness.json", {
            "status": "stale_fail_closed",
            "age_minutes": 90,
            "fail_closed": True,
        })
        blocked = build_report(args)
        assert blocked["status"] == "blocked"
        assert "runtime_signal_source_stale_fail_closed" in blocked["blockers"]
        assert "runtime_paper_db_integrity_marker_exists" in blocked["blockers"]
    print("SELF_TEST_PASS runtime_health_snapshot_audit")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="/app/data")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--paper-review-max-age-minutes", type=float, default=30.0)
    parser.add_argument("--paper-fast-lane-max-age-minutes", type=float, default=30.0)
    parser.add_argument("--runtime-final-evidence-max-age-minutes", type=float, default=60.0)
    parser.add_argument("--signal-warn-minutes", type=float, default=15.0)
    parser.add_argument("--signal-fail-minutes", type=float, default=45.0)
    parser.add_argument("--observer-log-tail-lines", type=int, default=240)
    parser.add_argument("--observer-log-tail-bytes", type=int, default=200000)
    parser.add_argument("--observer-log-max-age-minutes", type=float, default=120.0)
    parser.add_argument("--out")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    report = build_report(args)
    if args.out:
        write_json(args.out, report)
    else:
        print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

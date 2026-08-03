#!/usr/bin/env python3
"""Fail-closed evaluator database source contract."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
from pathlib import Path
import sqlite3
import stat
import time
from urllib.parse import quote

from cross_db_evaluator_snapshot import (
    DATABASE_SPECS,
    normalized_timestamp_sql,
    quote_identifier,
)


SCHEMA_VERSION = "evaluator_db_source_contract.v1"
SNAPSHOT_SCHEMA_VERSION = "cross_db_evaluator_snapshot.v3"
SELECTION_SCHEMA_VERSION = "evaluator_snapshot_selection.v1"
SNAPSHOT_FILES = {
    "signal": "signal.db",
    "paper": "paper_evidence.db",
    "raw": "raw.db",
    "kline": "kline.db",
}


def file_identity(path: Path) -> tuple[int, int] | None:
    details = path.stat()
    if not stat.S_ISREG(details.st_mode):
        return None
    return int(details.st_dev), int(details.st_ino)


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


def sqlite_temporal_bounds(
    path: Path,
    spec: dict,
    *,
    upper_epoch: float,
) -> dict[str, dict]:
    uri = f"file:{quote(str(path.resolve()), safe='/')}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    reports: dict[str, dict] = {}
    try:
        connection.execute("PRAGMA query_only=ON")
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
            )
        }
        for table, rule in spec["tables"].items():
            if table not in tables or rule["mode"] == "full":
                continue
            columns = {
                str(row["name"])
                for row in connection.execute(
                    f"PRAGMA table_info({quote_identifier(table)})"
                )
            }
            time_columns = [
                name for name in rule.get("time_columns", ()) if name in columns
            ]
            if not time_columns:
                reports[table] = {
                    "time_columns": [],
                    "missing_time_columns": True,
                    "upper_bound_passed": False,
                }
                continue
            expressions = []
            for index, column in enumerate(time_columns):
                normalized = normalized_timestamp_sql(column)
                expressions.append(
                    f"MAX({normalized}) AS max_{index}"
                )
                expressions.append(
                    "SUM(CASE WHEN "
                    f"{quote_identifier(column)} IS NOT NULL AND {normalized} IS NULL "
                    f"THEN 1 ELSE 0 END) AS invalid_{index}"
                )
            row = connection.execute(
                f"SELECT {', '.join(expressions)} FROM {quote_identifier(table)}"
            ).fetchone()
            maxima = {
                column: row[f"max_{index}"]
                for index, column in enumerate(time_columns)
            }
            invalid_counts = {
                column: int(row[f"invalid_{index}"] or 0)
                for index, column in enumerate(time_columns)
            }
            reports[table] = {
                "time_columns": time_columns,
                "maxima": maxima,
                "invalid_timestamp_counts": invalid_counts,
                "missing_time_columns": False,
                "upper_bound_passed": all(
                    value is None or float(value) <= float(upper_epoch) + 0.001
                    for value in maxima.values()
                ),
                "timestamps_parseable": all(value == 0 for value in invalid_counts.values()),
            }
    finally:
        connection.close()
    return reports


def evaluator_db_source_status(
    paper_db: str,
    data_dir: str,
) -> dict:
    candidate = Path(paper_db).expanduser().resolve()
    live = (Path(data_dir).expanduser().resolve() / "paper_trades.db").resolve()
    blockers = []
    try:
        candidate_identity = file_identity(candidate)
    except OSError:
        candidate_identity = None
    try:
        live_identity = file_identity(live)
    except OSError:
        live_identity = None
    exists = candidate_identity is not None
    is_live = bool(
        candidate == live
        or (
            candidate_identity is not None
            and live_identity is not None
            and candidate_identity == live_identity
        )
    )
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
    live_databases: dict[str, str] | None = None,
) -> dict:
    candidates = {
        "signal": Path(signal_db).expanduser().resolve(),
        "paper": Path(paper_db).expanduser().resolve(),
        "raw": Path(raw_db).expanduser().resolve(),
        "kline": Path(kline_db).expanduser().resolve(),
    }
    data_root = Path(data_dir).expanduser().resolve()
    default_live = {
        "signal": data_root / "sentiment_arb.db",
        "paper": data_root / "paper_trades.db",
        "raw": data_root / "raw_signal_outcomes.db",
        "kline": data_root / "kline_cache.db",
    }
    live = {
        name: Path((live_databases or {}).get(name, default_path)).expanduser().resolve()
        for name, default_path in default_live.items()
    }
    manifest_file = (
        Path(manifest_path).expanduser().resolve()
        if manifest_path
        else (Path(paper_db).expanduser().parent / "manifest.json").resolve()
    )
    blockers: list[str] = []
    candidate_identities: dict[str, tuple[int, int]] = {}
    live_identities: dict[str, tuple[int, int]] = {}
    for name, candidate in candidates.items():
        try:
            identity = file_identity(candidate)
        except OSError:
            identity = None
            blockers.append(f"evaluator_snapshot_{name}_identity_unavailable")
        if identity is None:
            blockers.append(f"evaluator_snapshot_{name}_db_missing")
        else:
            candidate_identities[name] = identity
    for name, active in live.items():
        try:
            identity = file_identity(active)
        except OSError:
            identity = None
            if active.exists():
                blockers.append(f"active_{name}_db_identity_unavailable")
        if identity is not None:
            live_identities[name] = identity
    for candidate_name, candidate in candidates.items():
        for live_name, active in live.items():
            same_path = candidate == active
            same_identity = (
                candidate_identities.get(candidate_name) is not None
                and candidate_identities.get(candidate_name) == live_identities.get(live_name)
            )
            if same_path or same_identity:
                blockers.append(
                    f"active_{live_name}_db_forbidden_for_{candidate_name}_evaluator"
                )
                if candidate_name == live_name:
                    blockers.append(f"active_{candidate_name}_db_forbidden_for_evaluator")
    manifest: dict = {}
    manifest_loaded = False
    snapshot_age_sec_value: float | None = None
    snapshot_upper_epoch: float | None = None
    verified_integrity: dict[str, dict] = {}
    if not manifest_file.is_file():
        blockers.append("evaluator_snapshot_manifest_missing")
    else:
        try:
            parsed_manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
            if not isinstance(parsed_manifest, dict) or not parsed_manifest:
                blockers.append("evaluator_snapshot_manifest_invalid_structure")
            else:
                manifest = parsed_manifest
                manifest_loaded = True
        except Exception:
            blockers.append("evaluator_snapshot_manifest_invalid_json")
    if manifest_loaded:
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
        if manifest.get("source_read_lock_budget_passed") is not True:
            blockers.append("evaluator_snapshot_source_read_lock_budget_failed")
        if manifest.get("indexes_built_after_source_read_lock_release") is not True:
            blockers.append("evaluator_snapshot_index_build_lock_order_invalid")
        try:
            manifest_read_lock_limit = float(manifest.get("max_source_read_lock_sec"))
            if manifest_read_lock_limit <= 0:
                raise ValueError("non-positive")
        except (TypeError, ValueError):
            manifest_read_lock_limit = None
            blockers.append("evaluator_snapshot_source_read_lock_limit_invalid")
        if manifest.get("source_mutation_free") is not True:
            blockers.append("evaluator_snapshot_source_mutation_contract_failed")
        if manifest.get("bounded_selective_snapshot") is not True:
            blockers.append("evaluator_snapshot_not_bounded_selective")
        if manifest.get("selection_upper_bounds_consistent") is not True:
            blockers.append("evaluator_snapshot_selection_upper_bounds_inconsistent")
        if manifest.get("output_cap_passed") is not True:
            blockers.append("evaluator_snapshot_output_cap_failed")
        try:
            output_size_bytes = int(manifest.get("output_size_bytes"))
            output_cap_bytes = int(manifest.get("output_cap_bytes"))
            if output_size_bytes <= 0 or output_cap_bytes <= 0 or output_size_bytes > output_cap_bytes:
                blockers.append("evaluator_snapshot_output_size_contract_invalid")
        except (TypeError, ValueError):
            output_size_bytes = None
            output_cap_bytes = None
            blockers.append("evaluator_snapshot_output_size_contract_invalid")
        if manifest.get("partial_artifacts_absent") is not True:
            blockers.append("evaluator_snapshot_partial_artifact_contract_failed")
        if manifest.get("active_database_reads_allowed_for_autoloop") is not False:
            blockers.append("evaluator_snapshot_active_database_read_contract_invalid")
        selection = manifest.get("selection_contract") or {}
        if selection.get("schema_version") != SELECTION_SCHEMA_VERSION:
            blockers.append("evaluator_snapshot_selection_contract_invalid")
        if selection.get("future_rows_excluded") is not True:
            blockers.append("evaluator_snapshot_future_row_contract_invalid")
        if selection.get("table_rules_are_explicit") is not True:
            blockers.append("evaluator_snapshot_table_selection_contract_invalid")
        supported_windows = set(selection.get("supported_capture_windows_hours") or [])
        if not {24, 48, 72}.issubset(supported_windows):
            blockers.append("evaluator_snapshot_capture_window_coverage_invalid")
        try:
            snapshot_upper_epoch = float(manifest.get("snapshot_ts"))
            if abs(float(selection.get("common_upper_epoch")) - snapshot_upper_epoch) > 0.001:
                blockers.append("evaluator_snapshot_common_upper_timestamp_mismatch")
        except (TypeError, ValueError):
            blockers.append("evaluator_snapshot_common_upper_timestamp_invalid")
        if not manifest.get("git_commit"):
            blockers.append("evaluator_snapshot_git_commit_missing")
        if manifest.get("snapshot_ts") is None:
            blockers.append("evaluator_snapshot_timestamp_missing")
        else:
            try:
                snapshot_age_sec_value = (
                    float(now_ts if now_ts is not None else time.time())
                    - float(manifest["snapshot_ts"])
                )
                if snapshot_age_sec_value < -60:
                    blockers.append("evaluator_snapshot_timestamp_in_future")
                if max_age_sec > 0 and snapshot_age_sec_value > float(max_age_sec):
                    blockers.append("evaluator_snapshot_stale")
            except (TypeError, ValueError):
                blockers.append("evaluator_snapshot_timestamp_invalid")
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
            if report.get("database_budget_passed") is not True:
                blockers.append(f"evaluator_snapshot_{name}_database_budget_failed")
            if report.get("source_open_mode") != "read_only_attached_uri":
                blockers.append(f"evaluator_snapshot_{name}_source_open_mode_invalid")
            if report.get("source_read_lock_budget_passed") is not True:
                blockers.append(
                    f"evaluator_snapshot_{name}_source_read_lock_budget_failed"
                )
            if report.get("source_read_lock_released_before_index_build") is not True:
                blockers.append(
                    f"evaluator_snapshot_{name}_index_build_lock_order_invalid"
                )
            try:
                report_read_lock_limit = float(report.get("source_read_lock_limit_sec"))
                report_read_lock_duration = float(
                    report.get("source_read_lock_duration_sec")
                )
                if (
                    report_read_lock_limit <= 0
                    or report_read_lock_duration < 0
                    or report_read_lock_duration > report_read_lock_limit + 1.0
                    or (
                        manifest_read_lock_limit is not None
                        and abs(report_read_lock_limit - manifest_read_lock_limit) > 0.001
                    )
                ):
                    raise ValueError("lock budget mismatch")
            except (TypeError, ValueError):
                blockers.append(
                    f"evaluator_snapshot_{name}_source_read_lock_contract_invalid"
                )
            if int(report.get("temporary_full_backup_size_bytes") or 0) != 0:
                blockers.append(f"evaluator_snapshot_{name}_full_backup_intermediate_detected")
            try:
                if abs(float(report.get("selection_upper_epoch")) - float(manifest.get("snapshot_ts"))) > 0.001:
                    blockers.append(f"evaluator_snapshot_{name}_selection_upper_mismatch")
            except (TypeError, ValueError):
                blockers.append(f"evaluator_snapshot_{name}_selection_upper_invalid")
            selected_tables = report.get("selected_tables") or {}
            if name == "paper":
                candidate_projection = (
                    selected_tables.get("candidate_shadow_observations") or {}
                ).get("storage_projection") or {}
                if candidate_projection.get("applied") is True:
                    if (
                        candidate_projection.get("schema_version")
                        != "candidate_observation_payload_projection.v1"
                        or candidate_projection.get("payload_semantics_preserved") is not True
                        or candidate_projection.get("unknown_payload_keys_preserved") is not True
                        or candidate_projection.get("missing_and_null_keys_preserved") is not True
                    ):
                        blockers.append(
                            "evaluator_snapshot_candidate_payload_projection_invalid"
                        )
            for required_table in (
                ("premium_signals",) if name == "signal" else
                (
                    "candidate_shadow_observations",
                    "candidate_shadow_virtual_trades",
                    "paper_decision_events",
                    "a_class_decision_events",
                    "a_class_mode_runtime_state",
                    "paper_trades",
                    "opportunity_events",
                ) if name == "paper" else
                ("raw_signal_outcomes",) if name == "raw" else
                ("kline_1m",)
            ):
                if (selected_tables.get(required_table) or {}).get("included") is not True:
                    blockers.append(
                        f"evaluator_snapshot_{name}_required_selection_missing:{required_table}"
                    )
            for table, selection_report in selected_tables.items():
                if not isinstance(selection_report, dict) or selection_report.get("included") is not True:
                    continue
                time_semantics = selection_report.get("time_semantics")
                if time_semantics == "event_time" and selection_report.get("future_bound_enforced") is not True:
                    blockers.append(
                        f"evaluator_snapshot_{name}_future_bound_missing:{table}"
                    )
                if time_semantics not in {"event_time", "timeless_reference"}:
                    blockers.append(
                        f"evaluator_snapshot_{name}_time_semantics_invalid:{table}"
                    )
            if candidate.is_file() and int(report.get("snapshot_size_bytes") or -1) != candidate.stat().st_size:
                blockers.append(f"evaluator_snapshot_{name}_size_mismatch")
            if candidate.is_file():
                try:
                    actual_sha = sha256_file(candidate)
                    actual_quick_check = sqlite_quick_check(candidate)
                    temporal_bounds = (
                        sqlite_temporal_bounds(
                            candidate,
                            DATABASE_SPECS[name],
                            upper_epoch=snapshot_upper_epoch,
                        )
                        if snapshot_upper_epoch is not None
                        else {}
                    )
                    verified_integrity[name] = {
                        "sha256": actual_sha,
                        "sha256_matches_manifest": bool(expected_sha and actual_sha == expected_sha),
                        "quick_check": actual_quick_check,
                        "temporal_bounds": temporal_bounds,
                    }
                    if not expected_sha or actual_sha != expected_sha:
                        blockers.append(f"evaluator_snapshot_{name}_sha256_mismatch")
                    if actual_quick_check != ["ok"]:
                        blockers.append(f"evaluator_snapshot_{name}_quick_check_revalidation_failed")
                    for table, temporal in temporal_bounds.items():
                        selection_report = selected_tables.get(table) or {}
                        expected_mode = DATABASE_SPECS[name]["tables"][table]["mode"]
                        if selection_report.get("selection_mode") != expected_mode:
                            blockers.append(
                                f"evaluator_snapshot_{name}_selection_mode_mismatch:{table}"
                            )
                        if selection_report.get("time_columns") != temporal.get("time_columns"):
                            blockers.append(
                                f"evaluator_snapshot_{name}_time_columns_mismatch:{table}"
                            )
                        if selection_report.get("upper_bound_columns") != temporal.get("time_columns"):
                            blockers.append(
                                f"evaluator_snapshot_{name}_upper_bound_columns_mismatch:{table}"
                            )
                        if temporal.get("missing_time_columns") is True:
                            blockers.append(
                                f"evaluator_snapshot_{name}_time_columns_missing:{table}"
                            )
                        if temporal.get("upper_bound_passed") is not True:
                            blockers.append(
                                f"evaluator_snapshot_{name}_future_rows_detected:{table}"
                            )
                        if temporal.get("timestamps_parseable") is not True:
                            blockers.append(
                                f"evaluator_snapshot_{name}_timestamp_parse_failed:{table}"
                            )
                except Exception as exc:
                    verified_integrity[name] = {
                        "error": f"{type(exc).__name__}:{exc}",
                    }
                    blockers.append(f"evaluator_snapshot_{name}_integrity_revalidation_failed")
        if manifest_file.parent.is_dir():
            expected_names = {"manifest.json", *SNAPSHOT_FILES.values()}
            entries = list(manifest_file.parent.iterdir())
            leftovers = [
                item.name
                for item in entries
                if not item.is_file()
                or item.name not in expected_names
                or item.name.startswith(".")
                or item.name.endswith((".tmp", ".partial", "-journal", "-wal", "-shm"))
            ]
            if leftovers:
                blockers.append("evaluator_snapshot_partial_artifacts_present")
            missing_names = expected_names - {item.name for item in entries if item.is_file()}
            if missing_names:
                blockers.append("evaluator_snapshot_bundle_files_missing")
            actual_bundle_size = sum(
                int(item.stat().st_size) for item in entries if item.is_file()
            )
            if output_size_bytes is None or actual_bundle_size != output_size_bytes:
                blockers.append("evaluator_snapshot_bundle_size_mismatch")
            if output_cap_bytes is None or actual_bundle_size > output_cap_bytes:
                blockers.append("evaluator_snapshot_bundle_output_cap_exceeded")
            snapshots_root = manifest_file.parent.parent
            if snapshots_root.name == "snapshots":
                interrupted = [
                    item.name
                    for item in snapshots_root.iterdir()
                    if item.is_dir() and item.name.startswith(".") and item.name.endswith(".partial")
                ]
                if interrupted:
                    blockers.append("evaluator_snapshot_interrupted_partials_present")
    blockers = list(dict.fromkeys(blockers))
    return {
        "schema_version": "evaluator_snapshot_bundle_contract.v1",
        "manifest_path": str(manifest_file),
        "snapshot_id": manifest.get("snapshot_id") if manifest else None,
        "snapshot_ts": manifest.get("snapshot_ts") if manifest else None,
        "snapshot_age_sec": (
            round(snapshot_age_sec_value, 6) if snapshot_age_sec_value is not None else None
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--signal-db", required=True)
    parser.add_argument("--paper-db", required=True)
    parser.add_argument("--raw-db", required=True)
    parser.add_argument("--kline-db", required=True)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--max-age-sec", type=float, default=28800)
    parser.add_argument("--live-signal-db")
    parser.add_argument("--live-paper-db")
    parser.add_argument("--live-raw-db")
    parser.add_argument("--live-kline-db")
    args = parser.parse_args()
    try:
        status = evaluator_snapshot_bundle_status(
            signal_db=args.signal_db,
            paper_db=args.paper_db,
            raw_db=args.raw_db,
            kline_db=args.kline_db,
            data_dir=args.data_dir,
            manifest_path=args.manifest_path,
            max_age_sec=args.max_age_sec,
            live_databases={
                name: value
                for name, value in {
                    "signal": args.live_signal_db,
                    "paper": args.live_paper_db,
                    "raw": args.live_raw_db,
                    "kline": args.live_kline_db,
                }.items()
                if value
            },
        )
    except Exception as exc:
        status = {
            "schema_version": "evaluator_snapshot_bundle_contract.v1",
            "accepted": False,
            "blockers": [f"evaluator_snapshot_preflight_exception:{type(exc).__name__}:{exc}"],
            "promotion_allowed": False,
        }
    print(json.dumps(status, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

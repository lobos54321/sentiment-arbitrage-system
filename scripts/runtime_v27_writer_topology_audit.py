#!/usr/bin/env python3
"""Audit runtime and v2.7 writer/read-model/consumer topology.

The audit is read-only. It inspects source markers, public health evidence,
process command lines, and materialized artifact metadata. It never changes
runtime mode, strategy, gates, execution, canary, wallet, or risk.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import time
from typing import Any


SCHEMA_VERSION = "runtime_v27_writer_topology_audit.v1"
PROJECT_ROOT = Path(__file__).resolve().parent.parent


COMPONENT_SPECS = (
    {
        "component_id": "health_bootstrap",
        "path": "src/health-bootstrap.js",
        "role": "runtime_supervisor",
        "markers": (
            "starting runtime child",
            "global.__runtimeWorkerStatus",
            "src/index.js",
        ),
        "activation": {
            "control": "HEALTH_BOOTSTRAP_CHILD_RUNTIME_ENABLED",
            "default": True,
        },
    },
    {
        "component_id": "index_runtime",
        "path": "src/index.js",
        "role": "runtime_worker_and_sidecar_supervisor",
        "markers": (
            "V27_READ_MODEL_REFRESH_WORKER_ENABLED",
            "v27-read-model-refresh",
            "scripts/v27_read_model_refresh.py",
        ),
        "activation": {
            "control": "V27_READ_MODEL_REFRESH_WORKER_ENABLED",
            "default": False,
        },
    },
    {
        "component_id": "dashboard_server",
        "path": "src/web/dashboard-server.js",
        "role": "health_reader_and_manual_refresh_trigger",
        "markers": (
            "/api/paper/v27-read-model-refresh",
            "scripts/v27_read_model_refresh.py",
            "global.__runtimeWorkerStatus",
        ),
        "activation": {
            "control": "HTTP_POST_operator_trigger",
            "default": False,
        },
    },
    {
        "component_id": "telegram_listener",
        "path": "src/inputs/premium-channel-listener.js",
        "role": "telegram_signal_source",
        "markers": (
            "source_message_ts",
            "source_event_id",
            "premium_channel",
        ),
    },
    {
        "component_id": "premium_signal_writer",
        "path": "src/engines/premium-signal-engine.js",
        "role": "premium_signals_writer",
        "markers": (
            "INSERT INTO premium_signals",
            "source_event_id",
            "source_message_ts",
        ),
    },
    {
        "component_id": "v27_read_model_refresh",
        "path": "scripts/v27_read_model_refresh.py",
        "role": "v27_read_model_producer",
        "markers": (
            "write_json_atomic(mode_readiness_path",
            "denominator_snapshot.json",
            "mode_readiness.json",
        ),
    },
    {
        "component_id": "v27_runtime_mode_gate",
        "path": "scripts/v27_runtime_mode_gate.py",
        "role": "v27_mode_readiness_consumer",
        "markers": (
            "DEFAULT_MODE_READINESS_PATH",
            "v27_mode_readiness_missing",
            "resolve_mode_readiness_path",
        ),
    },
    {
        "component_id": "paper_trade_monitor",
        "path": "scripts/paper_trade_monitor.py",
        "role": "entry_consumer",
        "markers": (
            "premium_signals",
            "from v27_runtime_mode_gate import evaluate_runtime_mode_gate",
            "evaluate_runtime_mode_gate",
            "v27_mode_readiness+pending_entry",
        ),
    },
)


ASSET_INVENTORY = (
    {
        "asset_id": "telegram_premium_signals",
        "location": "sentiment_arb.db:premium_signals",
        "writers": ("telegram_listener", "premium_signal_writer"),
        "readers": ("paper_trade_monitor",),
    },
    {
        "asset_id": "v27_event_log",
        "location": "v27_event_log/",
        "writers": ("discovered_v27_mirror_writers",),
        "readers": ("v27_read_model_refresh",),
    },
    {
        "asset_id": "v27_denominator_snapshot",
        "location": "v27_read_models/denominator_snapshot.json",
        "writers": ("v27_read_model_refresh",),
        "readers": ("v27_runtime_mode_gate", "dashboard_server"),
    },
    {
        "asset_id": "v27_mode_readiness",
        "location": "v27_read_models/mode_readiness.json",
        "writers": ("v27_read_model_refresh",),
        "readers": ("v27_runtime_mode_gate", "paper_trade_monitor", "dashboard_server"),
    },
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write_json(path: str | Path, payload: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + f".{time.time_ns()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(target)


def load_json(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON object required: {path}")
    return payload


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def describe_path(path: Path, *, include_hash: bool = False) -> dict[str, Any]:
    if not path.exists():
        return {"available": False, "path": str(path)}
    stat = path.stat()
    result = {
        "available": True,
        "path": str(path),
        "kind": "directory" if path.is_dir() else "file",
        "size_bytes": stat.st_size if path.is_file() else None,
        "mtime_epoch": stat.st_mtime,
        "mtime_iso": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    if include_hash and path.is_file():
        result["sha256"] = sha256_file(path)
    if path.is_dir():
        children = [item for item in path.rglob("*") if item.is_file()]
        result["file_count"] = len(children)
        result["total_file_bytes"] = sum(item.stat().st_size for item in children)
        result["latest_child_mtime_epoch"] = max(
            (item.stat().st_mtime for item in children),
            default=None,
        )
    return result


def inspect_component(repo_root: Path, spec: dict[str, Any]) -> dict[str, Any]:
    path = repo_root / str(spec["path"])
    evidence = describe_path(path, include_hash=path.exists())
    marker_results: dict[str, bool] = {}
    if path.exists():
        text = path.read_text(encoding="utf-8", errors="replace")
        marker_results = {
            marker: marker in text
            for marker in spec.get("markers") or ()
        }
    all_markers_present = bool(marker_results) and all(marker_results.values())
    return {
        **spec,
        "markers": list(spec.get("markers") or ()),
        "source_evidence": evidence,
        "marker_results": marker_results,
        "source_contract_present": path.exists() and all_markers_present,
    }


def discover_v27_event_log_writers(repo_root: Path) -> list[dict[str, Any]]:
    writers = []
    for path in sorted((repo_root / "scripts").glob("v27_mirror_*.py")):
        text = path.read_text(encoding="utf-8", errors="replace")
        if "V27EventLog" not in text:
            continue
        if ".append_event(" not in text and ".append_events(" not in text:
            continue
        writers.append({
            "component_id": path.stem,
            "path": str(path.relative_to(repo_root)),
            "append_event": ".append_event(" in text,
            "append_events": ".append_events(" in text,
            "source_evidence": describe_path(path, include_hash=True),
        })
    return writers


def nested(payload: dict[str, Any], *paths: tuple[str, ...]) -> Any:
    for path in paths:
        value: Any = payload
        found = True
        for key in path:
            if not isinstance(value, dict) or key not in value:
                found = False
                break
            value = value[key]
        if found:
            return value
    return None


def inspect_health(health: dict[str, Any] | None) -> dict[str, Any]:
    if not health:
        return {
            "available": False,
            "runtime_role": None,
            "entrypoint": None,
            "runtime_worker": None,
            "shadow_sidecars": None,
        }
    runtime_worker = nested(
        health,
        ("runtime_worker",),
        ("runtime", "runtime_worker"),
        ("health", "runtime_worker"),
    )
    runtime_role = nested(
        health,
        ("runtime_role",),
        ("runtime", "runtime_role"),
        ("health", "runtime_role"),
    )
    entrypoint = nested(
        health,
        ("entrypoint",),
        ("runtime", "entrypoint"),
        ("health", "entrypoint"),
    )
    shadow_sidecars = nested(
        health,
        ("shadow_sidecars",),
        ("runtime", "shadow_sidecars"),
        ("health", "shadow_sidecars"),
    )
    return {
        "available": True,
        "runtime_role": runtime_role,
        "entrypoint": entrypoint,
        "runtime_worker": runtime_worker,
        "shadow_sidecars": shadow_sidecars,
        "runtime_worker_running": (
            runtime_worker.get("running")
            if isinstance(runtime_worker, dict)
            else None
        ),
    }


def inspect_processes(proc_root: Path) -> dict[str, Any]:
    if not proc_root.exists():
        return {
            "available": False,
            "proc_root": str(proc_root),
            "matching_processes": [],
        }
    markers = (
        "health-bootstrap",
        "src/index.js",
        "dashboard-server",
        "v27_read_model_refresh.py",
        "paper_trade_monitor.py",
    )
    matches = []
    for item in proc_root.iterdir():
        if not item.name.isdigit():
            continue
        cmdline_path = item / "cmdline"
        try:
            raw_command = cmdline_path.read_bytes()
        except OSError:
            continue
        argv = [
            part.decode("utf-8", errors="replace")
            for part in raw_command.split(b"\x00")
            if part
        ]
        command = " ".join(argv)
        if command and any(marker in command for marker in markers):
            matched_markers = [marker for marker in markers if marker in command]
            matches.append({
                "pid": int(item.name),
                "executable": Path(argv[0]).name if argv else None,
                "argv_count": len(argv),
                "command_sha256": hashlib.sha256(raw_command).hexdigest(),
                "matched_markers": matched_markers,
                "command_redacted": True,
            })
    return {
        "available": True,
        "proc_root": str(proc_root),
        "matching_processes": sorted(matches, key=lambda row: row["pid"]),
    }


def inspect_json_artifact(path: Path) -> dict[str, Any]:
    result = describe_path(path, include_hash=True)
    if not path.exists():
        return result
    try:
        payload = load_json(path)
    except Exception as exc:  # noqa: BLE001
        return {**result, "json_valid": False, "error": str(exc)}
    return {
        **result,
        "json_valid": True,
        "schema_version": (
            payload.get("schema_version")
            or payload.get("mode_readiness_schema_version")
            or payload.get("refresh_schema_version")
            or payload.get("snapshot_schema_version")
        ),
        "generated_at": payload.get("generated_at"),
        "highest_allowed_mode": payload.get("highest_allowed_mode"),
        "blocking_reasons": payload.get("blocking_reasons"),
    }


def build_audit(
    *,
    repo_root: str,
    data_dir: str,
    health_json_path: str | None = None,
    proc_root: str = "/proc",
) -> dict[str, Any]:
    root = Path(repo_root).expanduser().resolve()
    data = Path(data_dir).expanduser().resolve()
    components = [inspect_component(root, spec) for spec in COMPONENT_SPECS]
    component_by_id = {row["component_id"]: row for row in components}
    event_log_writers = discover_v27_event_log_writers(root)
    health = None
    health_error = None
    if health_json_path:
        try:
            health = load_json(health_json_path)
        except Exception as exc:  # noqa: BLE001
            health_error = str(exc)
    health_evidence = inspect_health(health)
    if health_error:
        health_evidence["error"] = health_error

    artifacts = {
        "event_log": describe_path(data / "v27_event_log"),
        "denominator_projection": inspect_json_artifact(
            data / "v27_read_models" / "denominator_projection.json"
        ),
        "denominator_snapshot": inspect_json_artifact(
            data / "v27_read_models" / "denominator_snapshot.json"
        ),
        "denominator_freshness": inspect_json_artifact(
            data / "v27_read_models" / "denominator_freshness.json"
        ),
        "mode_readiness": inspect_json_artifact(
            data / "v27_read_models" / "mode_readiness.json"
        ),
        "refresh_log": describe_path(data / "v27-read-model-refresh.log"),
    }
    process_evidence = inspect_processes(Path(proc_root))
    process_markers = {
        marker
        for row in process_evidence.get("matching_processes") or []
        for marker in row.get("matched_markers") or []
    }
    producer_process_observed = any(
        marker == "v27_read_model_refresh.py"
        for marker in process_markers
    )
    index_runtime_observed = any(
        marker == "src/index.js"
        for marker in process_markers
    )
    runtime_worker_running = health_evidence.get("runtime_worker_running")

    producer_source_ok = component_by_id["v27_read_model_refresh"]["source_contract_present"]
    activation_source_ok = component_by_id["index_runtime"]["source_contract_present"]
    consumer_source_ok = (
        component_by_id["v27_runtime_mode_gate"]["source_contract_present"]
        and component_by_id["paper_trade_monitor"]["source_contract_present"]
    )
    mode_readiness_available = bool(artifacts["mode_readiness"].get("available"))
    writer_reader_paths_enumerated = bool(event_log_writers) and all(
        row.get("writers") and row.get("readers")
        for row in ASSET_INVENTORY
    )
    source_catalog_complete = all(row["source_contract_present"] for row in components)

    blockers = []
    warnings = []
    if not source_catalog_complete:
        blockers.append("source_topology_contract_incomplete")
    if not writer_reader_paths_enumerated:
        blockers.append("writer_reader_inventory_incomplete")
    if not producer_source_ok or not activation_source_ok:
        blockers.append("v27_producer_path_unidentified")
    if not consumer_source_ok:
        blockers.append("v27_consumer_path_unidentified")
    if health_evidence.get("available") and runtime_worker_running is False:
        warnings.append("runtime_worker_not_running")
    if health_evidence.get("available") and runtime_worker_running is None:
        warnings.append("runtime_worker_state_not_exposed")
    if process_evidence.get("available") and not index_runtime_observed:
        warnings.append("index_runtime_process_not_observed")
    if process_evidence.get("available") and not producer_process_observed:
        warnings.append("v27_read_model_producer_process_not_observed")
    if not mode_readiness_available:
        warnings.append("v27_mode_readiness_artifact_missing")
    if not health_evidence.get("available"):
        warnings.append("runtime_health_evidence_not_supplied")
    if not process_evidence.get("available"):
        warnings.append("process_evidence_unavailable")

    if blockers:
        classification = "TOPOLOGY_EVIDENCE_INCOMPLETE"
    elif not mode_readiness_available:
        classification = "MODE_READINESS_MISSING"
    elif (
        health_evidence.get("available")
        and runtime_worker_running is False
    ):
        classification = "RUNTIME_WORKER_NOT_RUNNING"
    elif process_evidence.get("available") and not producer_process_observed:
        classification = "V27_PRODUCER_NOT_OBSERVED"
    else:
        classification = "TOPOLOGY_AUDIT_PASS"

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now_iso(),
        "read_only": True,
        "evidence_level": "topology_and_runtime_observation",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "classification": classification,
        "inputs": {
            "repo_root": str(root),
            "data_dir": str(data),
            "health_json": str(Path(health_json_path).resolve()) if health_json_path else None,
            "proc_root": str(Path(proc_root)),
        },
        "components": components,
        "writer_reader_inventory": [
            {
                **row,
                "writers": (
                    [writer["component_id"] for writer in event_log_writers]
                    if row["asset_id"] == "v27_event_log"
                    else list(row["writers"])
                ),
                "readers": list(row["readers"]),
            }
            for row in ASSET_INVENTORY
        ],
        "v27_event_log_writers": event_log_writers,
        "runtime_observation": {
            "health": health_evidence,
            "processes": process_evidence,
        },
        "v27_pipeline": {
            "producer": {
                "component_id": "v27_read_model_refresh",
                "source_contract_present": producer_source_ok,
                "activation_component": "index_runtime",
                "activation_control": "V27_READ_MODEL_REFRESH_WORKER_ENABLED",
                "activation_default": False,
                "process_observed": producer_process_observed,
            },
            "read_models": artifacts,
            "consumers": [
                {
                    "component_id": "v27_runtime_mode_gate",
                    "source_contract_present": component_by_id[
                        "v27_runtime_mode_gate"
                    ]["source_contract_present"],
                },
                {
                    "component_id": "paper_trade_monitor",
                    "source_contract_present": component_by_id[
                        "paper_trade_monitor"
                    ]["source_contract_present"],
                },
                {
                    "component_id": "dashboard_server",
                    "source_contract_present": component_by_id[
                        "dashboard_server"
                    ]["source_contract_present"],
                },
            ],
            "important_reconciliation": (
                "A missing mode_readiness artifact is a producer/activation/runtime "
                "evidence problem until proven otherwise; it is not strategy evidence."
            ),
        },
        "acceptance": {
            "source_catalog_paths_present": source_catalog_complete,
            "writer_reader_paths_enumerated": writer_reader_paths_enumerated,
            "v27_producer_identified": producer_source_ok and activation_source_ok,
            "v27_read_model_identified": True,
            "v27_consumers_identified": consumer_source_ok,
            "passed": not blockers,
        },
        "blockers": blockers,
        "warnings": sorted(set(warnings)),
        "next_stage": "A3_frozen_cross_db_snapshot" if not blockers else "resolve_A2_topology_blockers",
    }


def self_test() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        repo = root / "repo"
        data = root / "data"
        proc = root / "proc"
        repo.mkdir()
        data.mkdir()
        proc.mkdir()
        for spec in COMPONENT_SPECS:
            path = repo / str(spec["path"])
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("\n".join(spec["markers"]) + "\n", encoding="utf-8")
        mirror_path = repo / "scripts" / "v27_mirror_test_fixture.py"
        mirror_path.write_text(
            "from v27_event_log import V27EventLog\n"
            "V27EventLog('x').append_event({})\n",
            encoding="utf-8",
        )
        read_models = data / "v27_read_models"
        read_models.mkdir(parents=True)
        (data / "v27_event_log").mkdir()
        write_json(
            read_models / "mode_readiness.json",
            {
                "mode_readiness_schema_version": "v2.7.0.mode_readiness.v1",
                "generated_at": utc_now_iso(),
                "highest_allowed_mode": "shadow",
            },
        )
        health_path = root / "health.json"
        write_json(
            health_path,
            {
                "runtime_role": "dashboard_supervisor",
                "entrypoint": {"entrypoint_basename": "health-bootstrap.js"},
                "runtime_worker": {"running": True, "pid": 123},
                "shadow_sidecars": {},
            },
        )
        for pid, command in (
            ("123", "node src/index.js --premium"),
            ("124", "python3 scripts/v27_read_model_refresh.py --loop"),
        ):
            process_dir = proc / pid
            process_dir.mkdir()
            (process_dir / "cmdline").write_bytes(command.replace(" ", "\x00").encode())

        report = build_audit(
            repo_root=str(repo),
            data_dir=str(data),
            health_json_path=str(health_path),
            proc_root=str(proc),
        )
        assert report["classification"] == "TOPOLOGY_AUDIT_PASS"
        assert report["acceptance"]["passed"] is True
        assert report["v27_pipeline"]["producer"]["process_observed"] is True
        assert report["runtime_observation"]["health"]["runtime_worker_running"] is True
        assert len(report["v27_event_log_writers"]) == 1

        (read_models / "mode_readiness.json").unlink()
        missing = build_audit(
            repo_root=str(repo),
            data_dir=str(data),
            health_json_path=str(health_path),
            proc_root=str(proc),
        )
        assert missing["classification"] == "MODE_READINESS_MISSING"
        assert missing["promotion_allowed"] is False
    print("SELF_TEST_PASS runtime_v27_writer_topology_audit")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", default=str(PROJECT_ROOT))
    parser.add_argument("--data-dir", default="/app/data")
    parser.add_argument("--health-json")
    parser.add_argument("--proc-root", default="/proc")
    parser.add_argument("--out")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        self_test()
        return 0
    if not args.out:
        raise SystemExit("--out is required")
    report = build_audit(
        repo_root=args.repo_root,
        data_dir=args.data_dir,
        health_json_path=args.health_json,
        proc_root=args.proc_root,
    )
    write_json(args.out, report)
    print(json.dumps({
        "schema_version": report["schema_version"],
        "out": str(args.out),
        "classification": report["classification"],
        "acceptance_passed": report["acceptance"]["passed"],
        "promotion_allowed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

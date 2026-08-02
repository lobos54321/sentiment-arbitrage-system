import json
import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, os.path.join(ROOT, "scripts"))

from runtime_v27_writer_topology_audit import build_audit  # noqa: E402


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_actual_source_topology_is_enumerable(tmp_path):
    report = build_audit(
        repo_root=str(ROOT),
        data_dir=str(tmp_path),
        proc_root=str(tmp_path / "no-proc"),
    )

    assert report["acceptance"]["source_catalog_paths_present"] is True
    assert report["acceptance"]["writer_reader_paths_enumerated"] is True
    assert report["acceptance"]["v27_producer_identified"] is True
    assert report["acceptance"]["v27_consumers_identified"] is True
    assert report["acceptance"]["passed"] is True
    assert len(report["v27_event_log_writers"]) >= 10
    assert report["classification"] == "MODE_READINESS_MISSING"
    assert report["promotion_allowed"] is False


def test_runtime_worker_false_is_not_misclassified_as_strategy_failure(tmp_path):
    mode_path = tmp_path / "v27_read_models" / "mode_readiness.json"
    write_json(
        mode_path,
        {
            "mode_readiness_schema_version": "v2.7.0.mode_readiness.v1",
            "highest_allowed_mode": "shadow",
        },
    )
    health_path = tmp_path / "health.json"
    write_json(
        health_path,
        {
            "runtime_role": "standalone_dashboard",
            "runtime_worker": {"running": False, "pid": None},
            "entrypoint": {"entrypoint_basename": "dashboard-server.js"},
        },
    )

    report = build_audit(
        repo_root=str(ROOT),
        data_dir=str(tmp_path),
        health_json_path=str(health_path),
        proc_root=str(tmp_path / "no-proc"),
    )

    assert report["classification"] == "RUNTIME_WORKER_NOT_RUNNING"
    assert report["acceptance"]["passed"] is True
    assert "runtime_worker_not_running" in report["warnings"]
    assert "strategy" not in report["classification"].lower()
    assert report["promotion_allowed"] is False


def test_process_command_lines_are_redacted(tmp_path):
    proc = tmp_path / "proc"
    process_dir = proc / "123"
    process_dir.mkdir(parents=True)
    secret = "super-secret-dashboard-token"
    process_dir.joinpath("cmdline").write_bytes(
        f"python3\0scripts/v27_read_model_refresh.py\0--token\0{secret}\0".encode()
    )

    report = build_audit(
        repo_root=str(ROOT),
        data_dir=str(tmp_path / "data"),
        proc_root=str(proc),
    )

    serialized = json.dumps(report)
    process = report["runtime_observation"]["processes"]["matching_processes"][0]
    assert secret not in serialized
    assert "command" not in process
    assert process["command_redacted"] is True
    assert process["matched_markers"] == ["v27_read_model_refresh.py"]

import argparse
import json
from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from agent_autoloop_stage_runner import args_namespace, execute_stages  # noqa: E402


def accepted_provenance(tmp_path):
    return {
        "schema_version": "evaluator_snapshot_provenance.v1",
        "consumer_verified_at": "2026-08-08T04:00:00Z",
        "contract_schema_version": "evaluator_snapshot_bundle_contract.v1",
        "accepted": True,
        "snapshot_id": "20260808T035900Z-1234abcd",
        "snapshot_ts": 1786161540,
        "snapshot_age_sec": 60,
        "max_snapshot_age_sec": 28800,
        "git_commit": "f" * 40,
        "manifest_path": str(tmp_path / "agent_evidence" / "current" / "manifest.json"),
        "manifest_sha256": "a" * 64,
        "databases": {
            name: {
                "path": str(tmp_path / "agent_evidence" / "current" / filename),
                "sha256": "b" * 64,
                "sha256_matches_manifest": True,
                "quick_check": ["ok"],
            }
            for name, filename in {
                "signal": "signal.db",
                "paper": "paper_evidence.db",
                "raw": "raw.db",
                "kline": "kline.db",
            }.items()
        },
        "blockers": [],
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def stage_args(tmp_path, provenance):
    return argparse.Namespace(
        run_dir=str(tmp_path / "agent_runs" / "staged"),
        out_root=str(tmp_path / "agent_runs"),
        run_id="staged",
        stage="",
        evaluator_snapshot=provenance,
        evaluator_snapshot_required=True,
    )


def test_stage_runner_materializes_accepted_snapshot_provenance_before_stages(tmp_path, capsys):
    provenance = accepted_provenance(tmp_path)
    args = stage_args(tmp_path, provenance)

    execute_stages(args)

    path = Path(args.run_dir) / "evaluator_snapshot_provenance.json"
    assert path.is_file()
    saved = json.loads(path.read_text(encoding="utf-8"))
    assert saved == provenance
    output = capsys.readouterr().out
    assert provenance["snapshot_id"] in output
    assert provenance["manifest_sha256"] in output


def test_stage_runner_fails_closed_when_required_snapshot_provenance_is_missing(tmp_path):
    args = stage_args(tmp_path, None)

    with pytest.raises(RuntimeError, match="evaluator_snapshot_provenance_missing_or_rejected"):
        execute_stages(args)


def test_stage_namespace_preserves_snapshot_provenance_for_finalize(tmp_path):
    provenance = accepted_provenance(tmp_path)
    args = argparse.Namespace(
        signal_db="signal.db",
        paper_db="paper.db",
        raw_db="raw.db",
        kline_db="kline.db",
        data_dir=str(tmp_path),
        repo_root=str(ROOT),
        health_json=None,
        proc_root=str(tmp_path / "proc"),
        strategy_memory_dir=None,
        hours=24,
        capture_hours="24",
        expected_candidates=84,
        out_root=str(tmp_path / "agent_runs"),
        handoff_dir=str(tmp_path / "agent_handoffs"),
        registry=str(tmp_path / "hypothesis_registry.json"),
        markov_profiles="runtime",
        report_timeout_sec=60,
        test_timeout_sec=60,
        max_scan_rows=1000,
        oos_probe_hours="0.25",
        quote_fix_deploy_ts=0,
        evaluator_snapshot=provenance,
        evaluator_snapshot_required=True,
    )

    namespace = args_namespace(args)

    assert namespace.evaluator_snapshot == provenance
    assert namespace.evaluator_snapshot_required is True
    assert namespace.evaluator_snapshot["promotion_allowed"] is False

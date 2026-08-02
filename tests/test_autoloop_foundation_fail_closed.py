import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from agent_capture_discovery_loop import load_json, run_reports, write_json  # noqa: E402
from agent_autoloop_stage_runner import stage_foundation  # noqa: E402


def test_full_loop_skips_expensive_reports_when_foundation_fails(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    args = argparse.Namespace(
        signal_db=str(tmp_path / "missing-signals.db"),
        paper_db=str(tmp_path / "separate-evidence.db"),
        raw_db=str(tmp_path / "missing-raw.db"),
        kline_db=str(tmp_path / "missing-kline.db"),
        data_dir=str(tmp_path),
        repo_root=str(ROOT),
        health_json=None,
        proc_root=str(tmp_path / "missing-proc"),
        hours=24,
        capture_hours="24,48,72",
        expected_candidates=84,
        max_scan_rows=1000,
        report_timeout_sec=30,
        strategy_memory_dir=str(tmp_path / "strategy-memory"),
        markov_profiles="runtime",
    )

    result = run_reports(run_dir, args)

    names = [row.get("name") for row in result["diagnostics"]]
    capture = load_json(result["capture_primary"])
    assert names == [
        "telegram_signal_identity_audit",
        "runtime_v27_writer_topology_audit",
        "evaluation_foundation_guard",
    ]
    assert "capture_discovery_24h" not in names
    assert "pnl_cross_secondary" not in names
    assert capture["evaluation_foundation"]["expensive_stages_skipped"] is True
    assert capture["report_health"]["promotion_allowed"] is False


def test_staged_foundation_does_not_reuse_prior_passing_artifacts(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    for name in (
        "telegram_signal_identity_audit_24h.json",
        "runtime_v27_writer_topology_audit_24h.json",
    ):
        write_json(run_dir / name, {"acceptance": {"passed": True}, "stale": True})
    args = argparse.Namespace(
        signal_db=str(tmp_path / "missing-signals.db"),
        raw_db=str(tmp_path / "missing-raw.db"),
        data_dir=str(tmp_path),
        repo_root=str(ROOT),
        proc_root=str(tmp_path / "missing-proc"),
        health_json=None,
        hours=24,
        max_scan_rows=1000,
        report_timeout_sec=30,
    )

    result = stage_foundation(args, run_dir)

    assert result["commands_passed"] is False
    assert result["passed"] is False
    assert "foundation_command_failed" in result["blockers"]
    assert not (run_dir / "telegram_signal_identity_audit_24h.json").exists()

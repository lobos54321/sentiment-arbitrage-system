import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def run_script(script, *args):
    proc = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / script), *args],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
        timeout=30,
    )
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert "SELF_TEST_PASS" in proc.stdout


def test_strategy_memory_self_tests():
    run_script("strategy_hypothesis_registry.py")
    run_script("strategy_memory_mining.py", "--self-test")
    run_script("strategy_memory_candidate_mapping.py", "--self-test")
    run_script("filtered_winner_dossier.py", "--self-test")
    run_script("index_lifecycle_snapshot_report.py", "--self-test")
    run_script("exit_policy_shadow_simulator.py", "--self-test")
    run_script("execution_delay_adjusted_replay.py", "--self-test")
    run_script("offline_strategy_memory_audit.py", "--self-test")
    run_script("strategy_memory_validation.py", "--self-test")

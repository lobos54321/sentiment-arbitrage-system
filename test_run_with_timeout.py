import subprocess
import sys


def test_run_with_timeout_returns_child_status_and_logs_output(tmp_path):
    log_path = tmp_path / "wrapper.log"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_with_timeout.py",
            "--timeout-sec",
            "5",
            "--log",
            str(log_path),
            "--",
            sys.executable,
            "-c",
            "print('child ok')",
        ],
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0
    text = log_path.read_text(encoding="utf-8")
    assert "child ok" in text
    assert "exit status=0" in text


def test_run_with_timeout_caps_hung_child(tmp_path):
    log_path = tmp_path / "wrapper.log"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_with_timeout.py",
            "--timeout-sec",
            "1",
            "--log",
            str(log_path),
            "--",
            sys.executable,
            "-c",
            "import time; time.sleep(10)",
        ],
        text=True,
        capture_output=True,
    )

    assert result.returncode == 124
    assert "timed out after 1.0s" in log_path.read_text(encoding="utf-8")

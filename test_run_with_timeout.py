import errno
import importlib.util
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest


SCRIPT_PATH = Path(__file__).resolve().parent / "scripts" / "run_with_timeout.py"
SCRIPT = str(SCRIPT_PATH)
SPEC = importlib.util.spec_from_file_location("run_with_timeout", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
run_with_timeout = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(run_with_timeout)

if sys.platform.startswith("linux"):
    WRAPPER_COMMAND = [sys.executable, SCRIPT]
else:
    TEST_BOOTSTRAP = (
        "import importlib.util, sys; "
        f"spec = importlib.util.spec_from_file_location('run_with_timeout', {SCRIPT!r}); "
        "module = importlib.util.module_from_spec(spec); "
        "spec.loader.exec_module(module); "
        "module._require_supported_platform = lambda: None; "
        "raise SystemExit(module.main())"
    )
    WRAPPER_COMMAND = [sys.executable, "-c", TEST_BOOTSTRAP]


def _write_fake_proc_entry(proc_root, pid, ppid, *, state="S", environ=b""):
    process_dir = proc_root / str(pid)
    process_dir.mkdir()
    (process_dir / "stat").write_text(
        f"{pid} (test worker) {state} {ppid} "
        + " ".join(["0"] * 17)
        + f" {pid * 10}\n",
        encoding="utf-8",
    )
    (process_dir / "environ").write_bytes(environ)


def _snapshot_with_identity(pid, ppid, start_time, *, environment=""):
    return (
        {ppid: {pid}},
        {pid: environment},
        {pid: "S"},
        {pid: start_time},
    )


def _spawn_attempt_code(leaf_code, *leaf_args):
    return (
        "import errno, subprocess, sys; "
        f"leaf_code={leaf_code!r}; leaf_args={list(leaf_args)!r}; "
        "kwargs={'env': {}}\n"
        "try:\n"
        " subprocess.Popen([sys.executable, '-c', leaf_code, *leaf_args], "
        "start_new_session=True, **kwargs)\n"
        "except PermissionError as exc:\n"
        " assert exc.errno == errno.EPERM\n"
        " subprocess.Popen([sys.executable, '-c', leaf_code, *leaf_args], **kwargs)"
    )


def test_spawn_attempt_fixture_is_valid_python():
    compile(
        _spawn_attempt_code("pass"),
        "<spawn-attempt-fixture>",
        "exec",
    )


def test_run_with_timeout_returns_child_status_and_logs_output(tmp_path):
    log_path = tmp_path / "wrapper.log"

    result = subprocess.run(
        [
            *WRAPPER_COMMAND,
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
            *WRAPPER_COMMAND,
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


def test_run_with_timeout_terminates_grandchild_process_group(tmp_path):
    log_path = tmp_path / "wrapper.log"
    survived_path = tmp_path / "grandchild_survived.txt"
    child_code = (
        "import subprocess, sys, time; "
        "subprocess.Popen([sys.executable, '-c', "
        + repr(
            "import pathlib, signal, sys, time; "
            "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
            "time.sleep(1.6); pathlib.Path(sys.argv[1]).write_text('survived')"
        )
        + ", sys.argv[1]]); time.sleep(10)"
    )

    result = subprocess.run(
        [
            *WRAPPER_COMMAND,
            "--timeout-sec",
            "1",
            "--log",
            str(log_path),
            "--",
            sys.executable,
            "-c",
            child_code,
            str(survived_path),
        ],
        text=True,
        capture_output=True,
        timeout=5,
    )
    time.sleep(0.9)

    assert result.returncode == 124
    assert not survived_path.exists()


def test_run_with_timeout_terminates_escaped_grandchild_without_pipe_wait(tmp_path):
    log_path = tmp_path / "wrapper.log"
    survived_path = tmp_path / "escaped_grandchild_survived.txt"
    grandchild_code = (
        "import pathlib, signal, sys, time; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "time.sleep(1.7); pathlib.Path(sys.argv[1]).write_text('survived'); time.sleep(5)"
    )
    if sys.platform.startswith("linux"):
        child_code = _spawn_attempt_code(grandchild_code, str(survived_path)) + (
            "\nimport time; time.sleep(10)"
        )
    else:
        child_code = (
            "import subprocess, sys, time; "
            f"subprocess.Popen([sys.executable, '-c', {grandchild_code!r}, "
            "sys.argv[1]], start_new_session=True); time.sleep(10)"
        )
    started = time.monotonic()

    result = subprocess.run(
        [
            *WRAPPER_COMMAND,
            "--timeout-sec",
            "1",
            "--log",
            str(log_path),
            "--",
            sys.executable,
            "-c",
            child_code,
            str(survived_path),
        ],
        text=True,
        capture_output=True,
        timeout=5,
    )
    elapsed = time.monotonic() - started
    time.sleep(0.9)

    assert result.returncode == 124
    assert elapsed < 2.8
    assert not survived_path.exists()


def test_run_with_timeout_tracks_escaped_grandchild_after_fast_parent_exit(tmp_path):
    log_path = tmp_path / "wrapper.log"
    survived_path = tmp_path / "fast_exit_grandchild_survived.txt"
    grandchild_code = (
        "import pathlib, signal, sys, time; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "time.sleep(1.4); pathlib.Path(sys.argv[1]).write_text('survived'); time.sleep(5)"
    )
    if sys.platform.startswith("linux"):
        child_code = _spawn_attempt_code(grandchild_code, str(survived_path))
    else:
        child_code = (
            "import subprocess, sys; "
            f"subprocess.Popen([sys.executable, '-c', {grandchild_code!r}, "
            "sys.argv[1]], start_new_session=True)"
        )
    started = time.monotonic()

    result = subprocess.run(
        [
            *WRAPPER_COMMAND,
            "--timeout-sec",
            "1",
            "--log",
            str(log_path),
            "--",
            sys.executable,
            "-c",
            child_code,
            str(survived_path),
        ],
        text=True,
        capture_output=True,
        timeout=5,
    )
    elapsed = time.monotonic() - started
    time.sleep(0.7)

    assert result.returncode == 124
    assert 1.0 <= elapsed < 2.8
    assert not survived_path.exists()


def test_run_with_timeout_clean_environment_daemon_cannot_escape(tmp_path):
    log_path = tmp_path / "wrapper.log"
    ready_path = tmp_path / "clean_env_leaf.pid"
    survived_path = tmp_path / "clean_env_leaf_survived.txt"
    if not sys.platform.startswith("linux"):
        launched_path = tmp_path / "unsupported_platform_must_not_launch.txt"
        result = subprocess.run(
            [
                sys.executable,
                SCRIPT,
                "--timeout-sec",
                "1",
                "--log",
                str(log_path),
                "--",
                sys.executable,
                "-c",
                f"from pathlib import Path; Path({str(launched_path)!r}).write_text('ran')",
            ],
            text=True,
            capture_output=True,
            timeout=3,
        )

        assert result.returncode != 0
        assert not launched_path.exists()
        assert "requires Linux with /proc" in result.stderr
        return

    leaf_code = (
        "import os, pathlib, signal, sys, time; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); "
        "time.sleep(1.5); pathlib.Path(sys.argv[2]).write_text('survived'); time.sleep(5)"
    )
    child_code = _spawn_attempt_code(
        leaf_code,
        str(ready_path),
        str(survived_path),
    )
    leaf_pid = None
    try:
        started = time.monotonic()
        result = subprocess.run(
            [
                *WRAPPER_COMMAND,
                "--timeout-sec",
                "1",
                "--log",
                str(log_path),
                "--",
                sys.executable,
                "-c",
                child_code,
                str(ready_path),
                str(survived_path),
            ],
            text=True,
            capture_output=True,
            timeout=5,
        )
        elapsed = time.monotonic() - started
        if ready_path.exists():
            leaf_pid = int(ready_path.read_text(encoding="utf-8"))
        time.sleep(0.8)

        assert result.returncode == 124
        assert 1.0 <= elapsed < 2.8
        assert not survived_path.exists()
        if leaf_pid is not None:
            with pytest.raises(ProcessLookupError):
                os.kill(leaf_pid, 0)
    finally:
        if leaf_pid is not None:
            try:
                os.kill(leaf_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_run_with_timeout_waits_for_background_tree_after_parent_success(tmp_path):
    log_path = tmp_path / "wrapper.log"
    finished_path = tmp_path / "background_finished.txt"
    grandchild_code = (
        "import pathlib, sys, time; time.sleep(0.5); "
        "pathlib.Path(sys.argv[1]).write_text('done')"
    )
    if sys.platform.startswith("linux"):
        child_code = _spawn_attempt_code(grandchild_code, str(finished_path))
    else:
        child_code = (
            "import subprocess, sys; "
            f"subprocess.Popen([sys.executable, '-c', {grandchild_code!r}, "
            "sys.argv[1]], start_new_session=True)"
        )
    started = time.monotonic()

    result = subprocess.run(
        [
            *WRAPPER_COMMAND,
            "--timeout-sec",
            "3",
            "--log",
            str(log_path),
            "--",
            sys.executable,
            "-c",
            child_code,
            str(finished_path),
        ],
        text=True,
        capture_output=True,
        timeout=5,
    )
    elapsed = time.monotonic() - started

    assert result.returncode == 0
    assert elapsed >= 0.45
    assert finished_path.read_text(encoding="utf-8") == "done"


def test_run_with_timeout_terminates_double_forked_tree(tmp_path):
    log_path = tmp_path / "wrapper.log"
    survived_path = tmp_path / "double_fork_survived.txt"
    leaf_code = (
        "import pathlib, signal, sys, time; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "time.sleep(1.5); pathlib.Path(sys.argv[1]).write_text('survived'); time.sleep(5)"
    )
    if sys.platform.startswith("linux"):
        middle_code = _spawn_attempt_code(leaf_code, str(survived_path))
        child_code = _spawn_attempt_code(middle_code)
    else:
        middle_code = (
            "import subprocess, sys; "
            f"subprocess.Popen([sys.executable, '-c', {leaf_code!r}, sys.argv[1]], "
            "start_new_session=True)"
        )
        child_code = (
            "import subprocess, sys; "
            f"subprocess.Popen([sys.executable, '-c', {middle_code!r}, "
            "sys.argv[1]], start_new_session=True)"
        )

    result = subprocess.run(
        [
            *WRAPPER_COMMAND,
            "--timeout-sec",
            "1",
            "--log",
            str(log_path),
            "--",
            sys.executable,
            "-c",
            child_code,
            str(survived_path),
        ],
        text=True,
        capture_output=True,
        timeout=5,
    )
    time.sleep(0.7)

    assert result.returncode == 124
    assert not survived_path.exists()


def test_run_with_timeout_streams_large_output_to_log(tmp_path):
    log_path = tmp_path / "wrapper.log"
    output_bytes = 4 * 1024 * 1024

    result = subprocess.run(
        [
            *WRAPPER_COMMAND,
            "--timeout-sec",
            "5",
            "--log",
            str(log_path),
            "--",
            sys.executable,
            "-c",
            f"import sys; sys.stdout.write('x' * {output_bytes})",
        ],
        text=True,
        capture_output=True,
        timeout=7,
    )

    assert result.returncode == 0
    assert log_path.stat().st_size >= output_bytes
    assert "exit status=0" in log_path.read_text(encoding="utf-8")[-200:]


@pytest.mark.parametrize("timeout_value", ["0", "-1", "nan", "inf"])
def test_run_with_timeout_rejects_invalid_timeout_values(tmp_path, timeout_value):
    result = subprocess.run(
        [
            *WRAPPER_COMMAND,
            "--timeout-sec",
            timeout_value,
            "--log",
            str(tmp_path / "wrapper.log"),
            "--",
            sys.executable,
            "-c",
            "print('must not run')",
        ],
        text=True,
        capture_output=True,
        timeout=3,
    )

    assert result.returncode != 0
    assert "timeout-sec must be finite and at least 1 second" in result.stderr


def test_run_with_timeout_fails_closed_when_process_scan_breaks(
    tmp_path,
    monkeypatch,
):
    launched = tmp_path / "must_not_launch.txt"
    monkeypatch.setattr(
        run_with_timeout,
        "_process_snapshot",
        lambda: (_ for _ in ()).throw(
            run_with_timeout.ProcessScanError("injected scan failure")
        ),
    )
    monkeypatch.setattr(run_with_timeout, "_enable_child_subreaper", lambda: None)
    monkeypatch.setattr(run_with_timeout, "_require_supported_platform", lambda: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            SCRIPT,
            "--timeout-sec",
            "2",
            "--log",
            str(tmp_path / "wrapper.log"),
            "--",
            sys.executable,
            "-c",
            f"from pathlib import Path; Path({str(launched)!r}).write_text('ran')",
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        run_with_timeout.main()

    assert exc_info.value.code == 2
    assert not launched.exists()


def test_run_with_timeout_proc_snapshot_reads_parent_state_and_scope_marker(
    tmp_path,
    monkeypatch,
):
    wrapper_pid = os.getpid()
    child_pid = wrapper_pid + 100_000
    _write_fake_proc_entry(tmp_path, wrapper_pid, 1)
    _write_fake_proc_entry(
        tmp_path,
        child_pid,
        wrapper_pid,
        environ=b"KEY=value\0RUN_WITH_TIMEOUT_SCOPE_ID=test-scope\0",
    )
    monkeypatch.setattr(run_with_timeout, "PROC_ROOT", tmp_path)

    children, environments, states, start_times = (
        run_with_timeout._proc_process_snapshot()
    )

    assert children[wrapper_pid] == {child_pid}
    assert "RUN_WITH_TIMEOUT_SCOPE_ID=test-scope" in environments[child_pid]
    assert states[wrapper_pid] == "S"
    assert states[child_pid] == "S"
    assert start_times[wrapper_pid] == wrapper_pid * 10
    assert start_times[child_pid] == child_pid * 10


@pytest.mark.parametrize("prctl_result", [0, 1])
def test_run_with_timeout_linux_subreaper_registration_is_mandatory(
    monkeypatch,
    prctl_result,
):
    calls = []

    class FakeLibC:
        def prctl(self, *args):
            calls.append(args)
            return prctl_result

    monkeypatch.setattr(run_with_timeout.sys, "platform", "linux")
    monkeypatch.setattr(
        run_with_timeout.ctypes,
        "CDLL",
        lambda *args, **kwargs: FakeLibC(),
    )

    if prctl_result == 0:
        run_with_timeout._enable_child_subreaper()
    else:
        with pytest.raises(RuntimeError, match="cannot enable child subreaper"):
            run_with_timeout._enable_child_subreaper()

    assert calls == [(run_with_timeout.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0)]


def test_run_with_timeout_rejects_unsupported_platform_before_launch(
    tmp_path,
    monkeypatch,
):
    launched = tmp_path / "must_not_launch.txt"
    monkeypatch.setattr(run_with_timeout.sys, "platform", "darwin")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            SCRIPT,
            "--timeout-sec",
            "2",
            "--log",
            str(tmp_path / "wrapper.log"),
            "--",
            sys.executable,
            "-c",
            f"from pathlib import Path; Path({str(launched)!r}).write_text('ran')",
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        run_with_timeout.main()

    assert exc_info.value.code == 2
    assert not launched.exists()


def test_run_with_timeout_linux_snapshot_never_falls_back_to_ps(monkeypatch):
    monkeypatch.setattr(run_with_timeout.sys, "platform", "linux")
    monkeypatch.setattr(
        run_with_timeout,
        "_proc_process_snapshot",
        lambda: (_ for _ in ()).throw(
            run_with_timeout.ProcessScanError("injected /proc failure")
        ),
    )
    monkeypatch.setattr(
        run_with_timeout,
        "_ps_process_snapshot",
        lambda: pytest.fail("Linux supervision must not fall back to ps"),
    )

    with pytest.raises(run_with_timeout.ProcessScanError, match="injected /proc"):
        run_with_timeout._process_snapshot()


def test_run_with_timeout_linux_child_filter_blocks_process_group_escape(
    monkeypatch,
):
    calls = []

    class FakeLibC:
        def prctl(self, *args):
            calls.append(args)
            return 0

    monkeypatch.setattr(run_with_timeout.sys, "platform", "linux")
    monkeypatch.setattr(
        run_with_timeout.os,
        "uname",
        lambda: type("Uname", (), {"machine": "x86_64"})(),
    )
    monkeypatch.setattr(run_with_timeout.os, "setpgid", lambda pid, pgid: None)
    monkeypatch.setattr(
        run_with_timeout.ctypes,
        "CDLL",
        lambda *args, **kwargs: FakeLibC(),
    )

    run_with_timeout._prepare_supervised_child()

    assert calls[0] == (run_with_timeout.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)
    assert calls[1][0:2] == (
        run_with_timeout.PR_SET_SECCOMP,
        run_with_timeout.SECCOMP_MODE_FILTER,
    )
    assert set(run_with_timeout._blocked_process_group_syscalls("x86_64")) == {
        109,
        112,
        272,
        308,
    }


@pytest.mark.parametrize(
    ("stop_signal", "expected_status"),
    [(signal.SIGTERM, 143), (signal.SIGINT, 130)],
)
def test_run_with_timeout_external_stop_cleans_tree_before_exit(
    tmp_path,
    stop_signal,
    expected_status,
):
    log_path = tmp_path / "wrapper.log"
    ready_path = tmp_path / "leaf.pid"
    survived_path = tmp_path / "leaf_survived.txt"
    leaf_code = (
        "import os, pathlib, signal, sys, time; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "signal.signal(signal.SIGINT, signal.SIG_IGN); "
        "pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); "
        "time.sleep(1.0); pathlib.Path(sys.argv[2]).write_text('survived'); time.sleep(5)"
    )
    if sys.platform.startswith("linux"):
        child_code = _spawn_attempt_code(
            leaf_code,
            str(ready_path),
            str(survived_path),
        )
    else:
        child_code = leaf_code

    wrapper = subprocess.Popen(
        [
            *WRAPPER_COMMAND,
            "--timeout-sec",
            "5",
            "--log",
            str(log_path),
            "--",
            sys.executable,
            "-c",
            child_code,
            str(ready_path),
            str(survived_path),
        ],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    leaf_pid = None
    try:
        ready_deadline = time.monotonic() + 2.0
        while time.monotonic() < ready_deadline and not ready_path.exists():
            time.sleep(0.02)
        assert ready_path.exists()
        leaf_pid = int(ready_path.read_text(encoding="utf-8"))

        stopped_at = time.monotonic()
        os.kill(wrapper.pid, stop_signal)
        wrapper.communicate(timeout=3)
        elapsed = time.monotonic() - stopped_at
        time.sleep(1.05)

        assert wrapper.returncode == expected_status
        assert elapsed < 1.5
        assert not survived_path.exists()
        with pytest.raises(ProcessLookupError):
            os.kill(leaf_pid, 0)
        assert "after process-tree cleanup" in log_path.read_text(encoding="utf-8")
    finally:
        if wrapper.poll() is None:
            wrapper.kill()
            wrapper.wait(timeout=2)
        if leaf_pid is not None:
            try:
                os.kill(leaf_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_run_with_timeout_ignores_reused_root_pid_after_child_exit(monkeypatch):
    wrapper_pid = os.getpid()
    reused_root_pid = wrapper_pid + 100_000
    unrelated_child_pid = reused_root_pid + 1
    monkeypatch.setattr(
        run_with_timeout,
        "_process_snapshot",
        lambda: (
            {reused_root_pid: {unrelated_child_pid}},
            {reused_root_pid: "unrelated", unrelated_child_pid: "unrelated"},
            {reused_root_pid: "S", unrelated_child_pid: "S"},
            {reused_root_pid: 111, unrelated_child_pid: 222},
        ),
    )

    live, zombies = run_with_timeout._supervised_pids(
        "scope-not-present",
        reused_root_pid,
        root_active=False,
    )

    assert live == set()
    assert zombies == set()


def test_supervised_process_pidfd_open_failure_tears_down_anchored_group(
    monkeypatch,
):
    events = []
    descendant_alive = True

    class FakePopen:
        pid = 424242

        def __init__(self, *args, **kwargs):
            events.append("root_exec_returned_with_descendant_alive")

        def kill(self):
            events.append("raw_root_kill")

        def wait(self, timeout=None):
            events.append(("root_wait_reap", timeout))
            return -signal.SIGKILL

    def fail_pidfd_open(pid, flags):
        events.append("pidfd_open_failed:EMFILE")
        raise OSError(errno.EMFILE, "too many open files")

    def signal_group(pid, signum):
        nonlocal descendant_alive
        events.append(("killpg", pid, signum))
        if signum == signal.SIGKILL:
            descendant_alive = False

    monkeypatch.setattr(run_with_timeout.subprocess, "Popen", FakePopen)
    monkeypatch.setattr(
        run_with_timeout.os,
        "pidfd_open",
        fail_pidfd_open,
        raising=False,
    )
    monkeypatch.setattr(run_with_timeout.os, "killpg", signal_group)
    monkeypatch.setattr(run_with_timeout.sys, "platform", "linux")
    monkeypatch.setattr(run_with_timeout, "TERMINATE_GRACE_SEC", 0.0)
    monkeypatch.setattr(run_with_timeout, "KILL_GRACE_SEC", 0.0)

    with pytest.raises(OSError) as exc_info:
        run_with_timeout._SupervisedProcess(
            ["probe"],
            log_fh=None,
            environment={},
        )

    assert exc_info.value.errno == errno.EMFILE
    assert ("killpg", FakePopen.pid, signal.SIGTERM) in events
    assert ("killpg", FakePopen.pid, signal.SIGKILL) in events
    assert events.index(("killpg", FakePopen.pid, signal.SIGKILL)) < events.index(
        ("root_wait_reap", 0.0)
    )
    assert descendant_alive is False


def test_run_with_timeout_never_signals_reused_process_group(monkeypatch):
    calls = []
    monkeypatch.setattr(
        run_with_timeout.os,
        "killpg",
        lambda pid, signum: calls.append((pid, signum)),
    )

    run_with_timeout._signal_original_group(
        12345,
        run_with_timeout.signal.SIGKILL,
        group_anchored=False,
    )
    run_with_timeout._signal_original_group(
        12345,
        run_with_timeout.signal.SIGTERM,
        group_anchored=True,
    )

    assert calls == [(12345, run_with_timeout.signal.SIGTERM)]


def test_run_with_timeout_term_grace_expiry_never_sleeps_negative(
    monkeypatch,
):
    class FakeProcess:
        pid = 12345

        @staticmethod
        def poll():
            return None

        @staticmethod
        def kill():
            return None

        @staticmethod
        def wait(timeout=None):
            return 0

        @staticmethod
        def group_anchored():
            return True

    monotonic_values = iter([10.0, 10.0, 10.3, 10.3, 10.9])
    sleep_values = []
    process_identity = run_with_timeout.ProcessIdentity(12345, 999)
    snapshots = iter(
        [
            ({process_identity}, set()),
            ({process_identity}, set()),
            (set(), set()),
            (set(), set()),
        ]
    )
    monkeypatch.setattr(
        run_with_timeout.time,
        "monotonic",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr(
        run_with_timeout.time,
        "sleep",
        lambda seconds: sleep_values.append(seconds),
    )
    monkeypatch.setattr(
        run_with_timeout,
        "_supervised_pids",
        lambda *args, **kwargs: next(snapshots),
    )
    monkeypatch.setattr(run_with_timeout, "_signal_pids", lambda *args: None)
    monkeypatch.setattr(
        run_with_timeout,
        "_signal_original_group",
        lambda *args, **kwargs: None,
    )

    assert run_with_timeout._terminate_process_tree(FakeProcess(), "scope") is True
    assert all(seconds >= 0 for seconds in sleep_values)


def test_run_with_timeout_descendant_pid_reuse_is_not_signaled(monkeypatch):
    identity = run_with_timeout.ProcessIdentity(424242, 111)
    signals = []
    monkeypatch.setattr(
        run_with_timeout,
        "_process_identity_matches",
        lambda candidate: candidate.start_time == 222,
    )
    monkeypatch.setattr(
        run_with_timeout.os,
        "pidfd_open",
        lambda pid, flags: 99,
        raising=False,
    )
    monkeypatch.setattr(run_with_timeout.os, "close", lambda fd: None)
    monkeypatch.setattr(
        run_with_timeout.signal,
        "pidfd_send_signal",
        lambda pidfd, signum: signals.append((pidfd, signum)),
        raising=False,
    )
    monkeypatch.setattr(run_with_timeout.sys, "platform", "linux")

    run_with_timeout._signal_pids({identity}, signal.SIGTERM)

    assert signals == []


def test_run_with_timeout_scan_failure_kills_known_identity_without_raw_pid_race(
    monkeypatch,
):
    identity = run_with_timeout.ProcessIdentity(424242, 111)
    signals = []

    class FakeProcess:
        pid = 12345

        @staticmethod
        def poll():
            return 0

        @staticmethod
        def group_anchored():
            return False

        @staticmethod
        def kill():
            return None

        @staticmethod
        def wait(timeout=None):
            return 0

    monkeypatch.setattr(
        run_with_timeout,
        "_process_identity_matches",
        lambda candidate: candidate == identity,
    )
    monkeypatch.setattr(
        run_with_timeout.os,
        "pidfd_open",
        lambda pid, flags: pid,
        raising=False,
    )
    monkeypatch.setattr(run_with_timeout.os, "close", lambda fd: None)
    monkeypatch.setattr(
        run_with_timeout.signal,
        "pidfd_send_signal",
        lambda pidfd, signum: signals.append((pidfd, signum)),
        raising=False,
    )
    monkeypatch.setattr(run_with_timeout.sys, "platform", "linux")
    monkeypatch.setattr(run_with_timeout.time, "sleep", lambda seconds: None)

    run_with_timeout._terminate_known_process_tree(FakeProcess(), {identity})

    assert signals == [
        (identity.pid, signal.SIGTERM),
        (identity.pid, signal.SIGKILL),
    ]


def test_run_with_timeout_runtime_scan_failure_uses_seen_identity_and_group(
    monkeypatch,
):
    identity = run_with_timeout.ProcessIdentity(424242, 111)
    known = set()
    scan_calls = 0
    identity_signals = []
    group_signals = []

    class FakeProcess:
        pid = identity.pid

        @staticmethod
        def poll():
            return None

        @staticmethod
        def group_anchored():
            return True

        @staticmethod
        def kill():
            return None

        @staticmethod
        def wait(timeout=None):
            return -signal.SIGKILL

    def supervised(*args, **kwargs):
        nonlocal scan_calls
        scan_calls += 1
        if scan_calls == 1:
            return {identity}, set()
        raise run_with_timeout.ProcessScanError("injected runtime scan failure")

    monkeypatch.setattr(run_with_timeout, "_supervised_pids", supervised)
    monkeypatch.setattr(
        run_with_timeout,
        "_signal_pids",
        lambda identities, signum: identity_signals.append((set(identities), signum)),
    )
    monkeypatch.setattr(
        run_with_timeout,
        "_signal_original_group",
        lambda pid, signum, **kwargs: group_signals.append((pid, signum)),
    )
    monkeypatch.setattr(run_with_timeout.time, "sleep", lambda seconds: None)

    with pytest.raises(run_with_timeout.ProcessScanError):
        run_with_timeout._wait_for_tree_exit(
            FakeProcess(),
            "scope",
            deadline=time.monotonic() + 1,
            known_processes=known,
        )
    run_with_timeout._terminate_known_process_tree(FakeProcess(), known)

    assert known == {identity}
    assert identity_signals == [
        ({identity}, signal.SIGTERM),
        ({identity}, signal.SIGKILL),
    ]
    assert (identity.pid, signal.SIGKILL) in group_signals


def test_run_with_timeout_does_not_signal_unrelated_process(tmp_path):
    unrelated = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(10)"])
    try:
        result = subprocess.run(
            [
                *WRAPPER_COMMAND,
                "--timeout-sec",
                "1",
                "--log",
                str(tmp_path / "wrapper.log"),
                "--",
                sys.executable,
                "-c",
                "import time; time.sleep(10)",
            ],
            text=True,
            capture_output=True,
            timeout=5,
        )

        assert result.returncode == 124
        assert unrelated.poll() is None
    finally:
        unrelated.terminate()
        unrelated.wait(timeout=3)

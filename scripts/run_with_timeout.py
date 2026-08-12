#!/usr/bin/env python3
"""Run a complete subprocess tree with a hard timeout and bounded teardown."""

from __future__ import annotations

import argparse
import ctypes
import errno
import math
import os
import secrets
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import NamedTuple


SCOPE_ENV_NAME = "RUN_WITH_TIMEOUT_SCOPE_ID"
PR_SET_CHILD_SUBREAPER = 36
POLL_INTERVAL_SEC = 0.1
TERMINATE_GRACE_SEC = 0.25
KILL_GRACE_SEC = 0.5
PROC_ROOT = Path("/proc")
PR_SET_NO_NEW_PRIVS = 38
PR_SET_SECCOMP = 22
SECCOMP_MODE_FILTER = 2
SECCOMP_RET_KILL_PROCESS = 0x80000000
SECCOMP_RET_ERRNO = 0x00050000
SECCOMP_RET_ALLOW = 0x7FFF0000
BPF_LD_W_ABS = 0x20
BPF_JMP_JEQ_K = 0x15
BPF_RET_K = 0x06
SECCOMP_ARCH_PROFILES = {
    "x86_64": (0xC000003E, (109, 112, 272, 308)),
    "amd64": (0xC000003E, (109, 112, 272, 308)),
    "aarch64": (0xC00000B7, (154, 157, 97, 268)),
    "arm64": (0xC00000B7, (154, 157, 97, 268)),
}


class _SockFilter(ctypes.Structure):
    _fields_ = [
        ("code", ctypes.c_ushort),
        ("jt", ctypes.c_ubyte),
        ("jf", ctypes.c_ubyte),
        ("k", ctypes.c_uint),
    ]


class _SockFprog(ctypes.Structure):
    _fields_ = [
        ("len", ctypes.c_ushort),
        ("filter", ctypes.POINTER(_SockFilter)),
    ]


class ProcessScanError(RuntimeError):
    """Raised when the wrapper cannot safely enumerate its process tree."""


class ProcessIdentity(NamedTuple):
    """A PID bound to the kernel start time that created this process."""

    pid: int
    start_time: int


class _SignalState:
    """Record an external stop request without doing unsafe work in a handler."""

    def __init__(self) -> None:
        self.signum: int | None = None

    def handle(self, signum: int, _frame: object) -> None:
        if self.signum is None:
            self.signum = signum


class _SupervisedProcess:
    """Popen wrapper that keeps the Linux process-group identity anchored."""

    def __init__(self, command: list[str], *, log_fh, environment: dict[str, str]):
        self._process = subprocess.Popen(
            command,
            text=True,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            env=environment,
            preexec_fn=_prepare_supervised_child,
        )
        self.pid = self._process.pid
        self._pidfd: int | None = None
        self._observed_returncode: int | None = None
        self._reaped = False
        if sys.platform.startswith("linux"):
            try:
                self._pidfd = os.pidfd_open(self.pid, 0)
            except Exception:
                _terminate_failed_supervised_launch(self._process)
                raise

    def poll(self) -> int | None:
        if not sys.platform.startswith("linux"):
            return self._process.poll()
        if self._observed_returncode is not None:
            return self._observed_returncode
        assert self._pidfd is not None
        info = os.waitid(
            os.P_PIDFD,
            self._pidfd,
            os.WEXITED | os.WNOHANG | os.WNOWAIT,
        )
        if info is None:
            return None
        if info.si_code == os.CLD_EXITED:
            self._observed_returncode = int(info.si_status)
        else:
            self._observed_returncode = -int(info.si_status)
        return self._observed_returncode

    def group_anchored(self) -> bool:
        if sys.platform.startswith("linux"):
            return not self._reaped
        return self._process.poll() is None

    def kill(self) -> None:
        if sys.platform.startswith("linux") and self._pidfd is not None:
            try:
                signal.pidfd_send_signal(self._pidfd, signal.SIGKILL)
            except ProcessLookupError:
                pass
            return
        self._process.kill()

    def wait(self, timeout: float | None = None) -> int:
        returncode = self._process.wait(timeout=timeout)
        self._reaped = True
        self._observed_returncode = returncode
        self._close_pidfd()
        return returncode

    def _close_pidfd(self) -> None:
        if self._pidfd is not None:
            os.close(self._pidfd)
            self._pidfd = None


def _append_log_line(log_fh, message: str) -> None:
    log_fh.write(message)
    if not message.endswith("\n"):
        log_fh.write("\n")
    log_fh.flush()


def _enable_child_subreaper() -> None:
    """Adopt orphaned descendants on Linux so daemonizing cannot escape."""
    if not sys.platform.startswith("linux"):
        return
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "prctl(PR_SET_CHILD_SUBREAPER) failed")
    except Exception as exc:
        raise RuntimeError(f"cannot enable child subreaper: {exc}") from exc


def _require_supported_platform() -> None:
    """Fail closed unless the production process-tree guarantees are available."""
    if not sys.platform.startswith("linux"):
        raise RuntimeError(
            "complete process-tree supervision requires Linux with /proc"
        )
    if not PROC_ROOT.is_dir():
        raise RuntimeError("complete process-tree supervision requires Linux /proc")
    if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
        raise RuntimeError("complete process-tree supervision requires Linux pidfd")


def _prepare_supervised_child() -> None:
    """Create one immutable process group before the requested command executes."""
    os.setpgid(0, 0)
    if not sys.platform.startswith("linux"):
        return
    machine = os.uname().machine.lower()
    profile = SECCOMP_ARCH_PROFILES.get(machine)
    if profile is None:
        raise RuntimeError(f"unsupported Linux architecture for supervision: {machine}")
    audit_arch, blocked_syscalls = profile
    filters = [
        _SockFilter(BPF_LD_W_ABS, 0, 0, 4),
        _SockFilter(BPF_JMP_JEQ_K, 1, 0, audit_arch),
        _SockFilter(BPF_RET_K, 0, 0, SECCOMP_RET_KILL_PROCESS),
        _SockFilter(BPF_LD_W_ABS, 0, 0, 0),
    ]
    for syscall_number in blocked_syscalls:
        filters.extend(
            [
                _SockFilter(BPF_JMP_JEQ_K, 0, 1, syscall_number),
                _SockFilter(BPF_RET_K, 0, 0, SECCOMP_RET_ERRNO | errno.EPERM),
            ]
        )
    filters.append(_SockFilter(BPF_RET_K, 0, 0, SECCOMP_RET_ALLOW))
    filter_array = (_SockFilter * len(filters))(*filters)
    program = _SockFprog(len(filters), filter_array)
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "prctl(PR_SET_NO_NEW_PRIVS) failed")
    if libc.prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, ctypes.byref(program)) != 0:
        raise OSError(ctypes.get_errno(), "prctl(PR_SET_SECCOMP) failed")


def _blocked_process_group_syscalls(machine: str) -> tuple[int, ...]:
    profile = SECCOMP_ARCH_PROFILES.get(machine.lower())
    if profile is None:
        raise RuntimeError(f"unsupported Linux architecture for supervision: {machine}")
    return profile[1]


def _proc_process_snapshot(
) -> tuple[dict[int, set[int]], dict[int, str], dict[int, str], dict[int, int]]:
    children: dict[int, set[int]] = {}
    environments: dict[int, str] = {}
    states: dict[int, str] = {}
    start_times: dict[int, int] = {}
    try:
        entries = list(PROC_ROOT.iterdir())
    except OSError as exc:
        raise ProcessScanError(f"cannot list /proc: {exc}") from exc
    for entry in entries:
        if not entry.name.isdigit():
            continue
        try:
            stat_text = (entry / "stat").read_text(encoding="utf-8")
            closing_paren = stat_text.rfind(")")
            fields = stat_text[closing_paren + 2 :].split()
            pid = int(entry.name)
            state = fields[0]
            ppid = int(fields[1])
            start_time = int(fields[19])
        except (IndexError, OSError, ValueError):
            continue
        children.setdefault(ppid, set()).add(pid)
        states[pid] = state
        start_times[pid] = start_time
        try:
            environments[pid] = (entry / "environ").read_bytes().decode(
                "utf-8", "replace"
            )
        except OSError:
            environments[pid] = ""
    if os.getpid() not in states:
        raise ProcessScanError("current wrapper PID missing from /proc snapshot")
    return children, environments, states, start_times


def _ps_process_snapshot(
) -> tuple[dict[int, set[int]], dict[int, str], dict[int, str], dict[int, int]]:
    try:
        result = subprocess.run(
            ["ps", "eww", "-axo", "pid=,ppid=,state=,command="],
            text=True,
            capture_output=True,
            timeout=1.0,
            check=False,
        )
    except Exception as exc:
        raise ProcessScanError(f"ps process scan failed: {exc}") from exc
    if result.returncode != 0:
        raise ProcessScanError(f"ps process scan exited {result.returncode}")
    children: dict[int, set[int]] = {}
    environments: dict[int, str] = {}
    states: dict[int, str] = {}
    start_times: dict[int, int] = {}
    for line in result.stdout.splitlines():
        try:
            pid_text, ppid_text, state, command = line.strip().split(None, 3)
            pid, ppid = int(pid_text), int(ppid_text)
        except (TypeError, ValueError):
            continue
        children.setdefault(ppid, set()).add(pid)
        environments[pid] = command
        states[pid] = state[:1]
        start_times[pid] = 0
    if os.getpid() not in states:
        raise ProcessScanError("current wrapper PID missing from ps snapshot")
    return children, environments, states, start_times


def _process_snapshot(
) -> tuple[dict[int, set[int]], dict[int, str], dict[int, str], dict[int, int]]:
    if sys.platform.startswith("linux"):
        return _proc_process_snapshot()
    return _ps_process_snapshot()


def _descendant_pids(children: dict[int, set[int]], roots: set[int]) -> set[int]:
    descendants: set[int] = set()
    pending = list(roots)
    while pending:
        parent = pending.pop()
        for pid in children.get(parent, set()):
            if pid in descendants:
                continue
            descendants.add(pid)
            pending.append(pid)
    return descendants


def _supervised_pids(
    scope_id: str,
    root_pid: int,
    *,
    root_active: bool,
) -> tuple[set[ProcessIdentity], set[int]]:
    children, environments, states, start_times = _process_snapshot()
    marker = f"{SCOPE_ENV_NAME}={scope_id}"
    scoped = {pid for pid, environment in environments.items() if marker in environment}
    related = _descendant_pids(children, {root_pid}) if root_active else set()
    if sys.platform.startswith("linux"):
        adopted = _descendant_pids(children, {os.getpid()})
        scoped |= adopted
    all_pids = (scoped | related) - {os.getpid()}
    zombies = {pid for pid in all_pids if states.get(pid) == "Z"}
    live = {
        ProcessIdentity(pid, start_times[pid])
        for pid in all_pids
        if pid in states and states.get(pid) != "Z" and pid in start_times
    }
    return live, zombies


def _reap_adopted_zombies(zombie_pids: set[int], *, root_pid: int) -> None:
    for pid in zombie_pids - {root_pid}:
        try:
            os.waitpid(pid, os.WNOHANG)
        except (ChildProcessError, ProcessLookupError):
            pass


def _process_identity_matches(identity: ProcessIdentity) -> bool:
    if not sys.platform.startswith("linux"):
        return True
    try:
        stat_text = (PROC_ROOT / str(identity.pid) / "stat").read_text(encoding="utf-8")
        closing_paren = stat_text.rfind(")")
        fields = stat_text[closing_paren + 2 :].split()
        return int(fields[19]) == identity.start_time
    except (IndexError, OSError, ValueError):
        return False


def _signal_pids(processes: set[ProcessIdentity], signum: int) -> None:
    for process_identity in sorted(
        processes,
        key=lambda item: item.pid,
        reverse=True,
    ):
        pidfd = None
        try:
            if sys.platform.startswith("linux"):
                pidfd = os.pidfd_open(process_identity.pid, 0)
                if not _process_identity_matches(process_identity):
                    continue
                signal.pidfd_send_signal(pidfd, signum)
            else:
                os.kill(process_identity.pid, signum)
        except (ProcessLookupError, PermissionError, OSError):
            pass
        finally:
            if pidfd is not None:
                os.close(pidfd)


def _signal_original_group(
    root_pid: int,
    signum: int,
    *,
    group_anchored: bool,
) -> None:
    if not group_anchored:
        return
    try:
        os.killpg(root_pid, signum)
    except (ProcessLookupError, PermissionError):
        pass


def _terminate_failed_supervised_launch(process: subprocess.Popen) -> None:
    """Boundedly tear down the anchored group before reaping a failed launch."""
    try:
        _signal_original_group(
            process.pid,
            signal.SIGTERM,
            group_anchored=True,
        )
    except OSError:
        pass
    term_deadline = time.monotonic() + TERMINATE_GRACE_SEC
    remaining = term_deadline - time.monotonic()
    if remaining > 0:
        time.sleep(remaining)

    kill_deadline = time.monotonic() + KILL_GRACE_SEC
    while True:
        try:
            _signal_original_group(
                process.pid,
                signal.SIGKILL,
                group_anchored=True,
            )
        except OSError:
            pass
        remaining = kill_deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(POLL_INTERVAL_SEC, remaining))

    try:
        process.kill()
    except OSError:
        pass
    try:
        process.wait(timeout=KILL_GRACE_SEC)
    except (OSError, subprocess.TimeoutExpired):
        pass


def _wait_for_tree_exit(
    process: _SupervisedProcess,
    scope_id: str,
    *,
    deadline: float,
    signal_state: _SignalState | None = None,
    known_processes: set[ProcessIdentity] | None = None,
) -> tuple[bool, int | None]:
    process_returncode = None
    known_processes = known_processes if known_processes is not None else set()
    while True:
        process_returncode = process.poll()
        live, zombies = _supervised_pids(
            scope_id,
            process.pid,
            root_active=process_returncode is None,
        )
        known_processes |= live
        _reap_adopted_zombies(zombies, root_pid=process.pid)
        if signal_state is not None and signal_state.signum is not None:
            return False, process_returncode
        if process_returncode is not None and not live:
            return True, process_returncode
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False, process_returncode
        time.sleep(min(POLL_INTERVAL_SEC, remaining))


def _terminate_process_tree(
    process: _SupervisedProcess,
    scope_id: str,
    *,
    known_processes: set[ProcessIdentity] | None = None,
) -> bool:
    """Terminate every current scoped/adopted descendant within a fixed budget."""
    known_processes = set(known_processes or ())
    root_active = process.poll() is None
    live, zombies = _supervised_pids(
        scope_id,
        process.pid,
        root_active=root_active,
    )
    known_processes |= live
    _reap_adopted_zombies(zombies, root_pid=process.pid)
    _signal_pids(known_processes, signal.SIGTERM)
    _signal_original_group(
        process.pid,
        signal.SIGTERM,
        group_anchored=process.group_anchored(),
    )
    term_deadline = time.monotonic() + TERMINATE_GRACE_SEC
    while time.monotonic() < term_deadline:
        root_active = process.poll() is None
        live, zombies = _supervised_pids(
            scope_id,
            process.pid,
            root_active=root_active,
        )
        known_processes |= live
        _reap_adopted_zombies(zombies, root_pid=process.pid)
        if not live:
            break
        remaining = term_deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(POLL_INTERVAL_SEC, remaining))

    kill_deadline = time.monotonic() + KILL_GRACE_SEC
    while True:
        root_active = process.poll() is None
        live, zombies = _supervised_pids(
            scope_id,
            process.pid,
            root_active=root_active,
        )
        known_processes |= live
        _reap_adopted_zombies(zombies, root_pid=process.pid)
        if not live:
            break
        _signal_pids(known_processes, signal.SIGKILL)
        _signal_original_group(
            process.pid,
            signal.SIGKILL,
            group_anchored=process.group_anchored(),
        )
        remaining = kill_deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(POLL_INTERVAL_SEC, remaining))

    if process.poll() is None:
        try:
            process.kill()
        except ProcessLookupError:
            pass
    live, zombies = _supervised_pids(
        scope_id,
        process.pid,
        root_active=process.poll() is None,
    )
    _reap_adopted_zombies(zombies, root_pid=process.pid)
    if not live:
        try:
            process.wait(timeout=KILL_GRACE_SEC)
        except subprocess.TimeoutExpired:
            return False
    return not live


def _terminate_known_process_tree(
    process: _SupervisedProcess,
    known_processes: set[ProcessIdentity],
) -> None:
    """Best-effort bounded teardown when a later /proc enumeration fails."""
    term_deadline = time.monotonic() + TERMINATE_GRACE_SEC
    _signal_pids(known_processes, signal.SIGTERM)
    _signal_original_group(
        process.pid,
        signal.SIGTERM,
        group_anchored=process.group_anchored(),
    )
    remaining = term_deadline - time.monotonic()
    if remaining > 0:
        time.sleep(remaining)

    _signal_pids(known_processes, signal.SIGKILL)
    kill_deadline = time.monotonic() + KILL_GRACE_SEC
    while time.monotonic() < kill_deadline and process.group_anchored():
        _signal_original_group(
            process.pid,
            signal.SIGKILL,
            group_anchored=True,
        )
        remaining = kill_deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(POLL_INTERVAL_SEC, remaining))
    try:
        process.kill()
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=KILL_GRACE_SEC)
    except subprocess.TimeoutExpired:
        pass


def _reap_completed_process(
    process: _SupervisedProcess,
    process_returncode: int | None,
) -> int:
    if process_returncode is None:
        raise RuntimeError("completed process has no exit status")
    try:
        return process.wait(timeout=KILL_GRACE_SEC)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("completed process could not be reaped") from exc


def _validated_timeout(value: float) -> float:
    timeout = float(value)
    if not math.isfinite(timeout) or timeout < 1.0:
        raise ValueError("timeout-sec must be finite and at least 1 second")
    return timeout


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout-sec", type=float, default=45.0)
    parser.add_argument("--log")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        parser.error("command is required after --")
    try:
        timeout_sec = _validated_timeout(args.timeout_sec)
        _require_supported_platform()
        _enable_child_subreaper()
        _process_snapshot()
    except (ProcessScanError, RuntimeError, ValueError) as exc:
        parser.error(str(exc))

    scope_id = secrets.token_hex(16)
    child_environment = os.environ.copy()
    child_environment[SCOPE_ENV_NAME] = scope_id
    log_path = Path(args.log) if args.log else Path(os.devnull)
    if args.log:
        log_path.parent.mkdir(parents=True, exist_ok=True)

    signal_state = _SignalState()
    previous_signal_handlers = {
        signum: signal.signal(signum, signal_state.handle)
        for signum in (signal.SIGTERM, signal.SIGINT)
    }
    try:
        with log_path.open("a", encoding="utf-8") as log_fh:
            started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            _append_log_line(
                log_fh,
                f"[timeout-wrapper] {started} starting timeout={timeout_sec}s "
                f"command={' '.join(command)}",
            )
            if signal_state.signum is not None:
                _append_log_line(
                    log_fh,
                    f"[timeout-wrapper] {started} interrupted before child launch "
                    f"signal={signal_state.signum}",
                )
                return 128 + signal_state.signum

            process = _SupervisedProcess(
                command,
                log_fh=log_fh,
                environment=child_environment,
            )
            deadline = time.monotonic() + timeout_sec
            supervision_error = None
            known_processes: set[ProcessIdentity] = set()
            try:
                completed, process_returncode = _wait_for_tree_exit(
                    process,
                    scope_id,
                    deadline=deadline,
                    signal_state=signal_state,
                    known_processes=known_processes,
                )
            except ProcessScanError as exc:
                completed = False
                process_returncode = process.poll()
                supervision_error = str(exc)

            cleanup_complete = True
            if not completed:
                try:
                    cleanup_complete = _terminate_process_tree(
                        process,
                        scope_id,
                        known_processes=known_processes,
                    )
                except ProcessScanError as exc:
                    cleanup_complete = False
                    supervision_error = supervision_error or str(exc)
                    _terminate_known_process_tree(process, known_processes)
            else:
                try:
                    process_returncode = _reap_completed_process(
                        process,
                        process_returncode,
                    )
                except RuntimeError as exc:
                    completed = False
                    cleanup_complete = False
                    supervision_error = supervision_error or str(exc)

            ended = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            if supervision_error is not None or not cleanup_complete:
                _append_log_line(
                    log_fh,
                    f"[timeout-wrapper] {ended} supervision failed closed "
                    f"error={supervision_error or 'process_tree_cleanup_incomplete'}",
                )
                return 125
            if signal_state.signum is not None:
                _append_log_line(
                    log_fh,
                    f"[timeout-wrapper] {ended} interrupted signal="
                    f"{signal_state.signum} after process-tree cleanup",
                )
                return 128 + signal_state.signum
            if not completed:
                _append_log_line(
                    log_fh,
                    f"[timeout-wrapper] {ended} timed out after {timeout_sec}s",
                )
                return 124
            _append_log_line(
                log_fh,
                f"[timeout-wrapper] {ended} exit status={process_returncode}",
            )
            return int(process_returncode or 0)
    finally:
        for signum, previous_handler in previous_signal_handlers.items():
            signal.signal(signum, previous_handler)


if __name__ == "__main__":
    raise SystemExit(main())

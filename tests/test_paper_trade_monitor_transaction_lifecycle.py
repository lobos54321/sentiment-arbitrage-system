import ast
import contextlib
import inspect
import os
from pathlib import Path
import sqlite3
import sys
import textwrap
import threading
import time
from types import SimpleNamespace

import pytest


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import paper_decision_audit  # noqa: E402
import entry_engine  # noqa: E402
import exit_engine  # noqa: E402
import paper_trade_monitor  # noqa: E402
from paper_trade_monitor import (  # noqa: E402
    MonitorSupervisorRestartRequired,
    _clear_monitor_supervisor_restart_request,
    _guarded_blocking_sleep,
    _monitor_blocking_call_for_db,
    _monitor_blocking_guard_for_db,
    _monitor_transaction_guard_scope,
    _raise_monitor_supervisor_restart_if_requested,
    _sleep_after_monitor_transaction_settle,
    _settle_fast_lane_sync_failure,
    _settle_monitor_iteration_transaction,
    call_execution_bridge,
    close_paper_db_gracefully,
    run_db_write_with_retry,
)


@pytest.fixture(autouse=True)
def _reset_monitor_restart_state():
    _clear_monitor_supervisor_restart_request()
    paper_trade_monitor.SHUTDOWN_REQUESTED.clear()
    yield
    _clear_monitor_supervisor_restart_request()
    paper_trade_monitor.SHUTDOWN_REQUESTED.clear()


def _open_delete_journal_db(path):
    db = sqlite3.connect(path, timeout=1)
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("CREATE TABLE IF NOT EXISTS evidence (value TEXT)")
    db.commit()
    return db


def _assert_competing_writer_can_begin(path):
    competitor = sqlite3.connect(path, timeout=1)
    try:
        competitor.execute("BEGIN IMMEDIATE")
        competitor.rollback()
    finally:
        competitor.close()


def test_successful_monitor_iteration_commits_before_sleep(tmp_path):
    db_path = tmp_path / "paper.db"
    db = _open_delete_journal_db(db_path)
    try:
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO evidence VALUES ('committed')")
        assert db.in_transaction
        assert Path(f"{db_path}-journal").exists()

        assert _settle_monitor_iteration_transaction(db, success=True)

        assert not db.in_transaction
        assert not Path(f"{db_path}-journal").exists()
        assert db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 1
        _assert_competing_writer_can_begin(db_path)
    finally:
        db.close()


def test_monitor_blocking_sleep_commits_delete_journal_first(tmp_path, monkeypatch):
    db_path = tmp_path / "paper.db"
    db = _open_delete_journal_db(db_path)
    observed_sleeps = []
    try:
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO evidence VALUES ('committed-before-sleep')")
        assert db.in_transaction
        assert Path(f"{db_path}-journal").exists()

        def assert_safe_sleep(seconds):
            assert seconds == 0.2
            assert not db.in_transaction
            assert not Path(f"{db_path}-journal").exists()
            _assert_competing_writer_can_begin(db_path)
            observed_sleeps.append(seconds)

        monkeypatch.setattr(paper_trade_monitor.time, "sleep", assert_safe_sleep)
        _sleep_after_monitor_transaction_settle(db, 0.2)

        assert observed_sleeps == [0.2]
        assert db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 1
    finally:
        db.close()


def test_monitor_blocking_sleep_is_skipped_when_commit_fails(tmp_path, monkeypatch):
    class FailingCommitConnection(sqlite3.Connection):
        fail_commit = False

        def commit(self):
            if self.fail_commit:
                raise sqlite3.OperationalError("injected commit failure")
            return super().commit()

    db_path = tmp_path / "paper.db"
    db = sqlite3.connect(
        db_path,
        timeout=0.05,
        factory=FailingCommitConnection,
    )
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("CREATE TABLE evidence (value TEXT)")
    db.commit()
    db.execute("BEGIN IMMEDIATE")
    db.execute("INSERT INTO evidence VALUES ('must-not-sleep')")
    db.fail_commit = True
    sleep_called = False

    def forbidden_sleep(_seconds):
        nonlocal sleep_called
        sleep_called = True

    monkeypatch.setattr(paper_trade_monitor.time, "sleep", forbidden_sleep)
    try:
        with pytest.raises(MonitorSupervisorRestartRequired) as caught:
            _sleep_after_monitor_transaction_settle(db, 0.2)

        assert isinstance(caught.value.__cause__, sqlite3.OperationalError)
        assert "injected commit failure" in str(caught.value.__cause__)
        assert not sleep_called
        assert db.in_transaction
        assert Path(f"{db_path}-journal").exists()
    finally:
        db.fail_commit = False
        db.rollback()
        db.close()


def test_monitor_blocking_sleep_is_skipped_when_commit_leaves_transaction_active(
    tmp_path,
    monkeypatch,
):
    class SilentCommitConnection(sqlite3.Connection):
        leave_transaction_active = False

        def commit(self):
            if self.leave_transaction_active:
                return None
            return super().commit()

    db_path = tmp_path / "paper.db"
    db = sqlite3.connect(
        db_path,
        timeout=0.05,
        factory=SilentCommitConnection,
    )
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("CREATE TABLE evidence (value TEXT)")
    db.commit()
    db.execute("BEGIN IMMEDIATE")
    db.execute("INSERT INTO evidence VALUES ('must-not-sleep')")
    db.leave_transaction_active = True
    sleep_called = False

    def forbidden_sleep(_seconds):
        nonlocal sleep_called
        sleep_called = True

    monkeypatch.setattr(paper_trade_monitor.time, "sleep", forbidden_sleep)
    try:
        with pytest.raises(MonitorSupervisorRestartRequired) as caught:
            _sleep_after_monitor_transaction_settle(db, 0.2)

        assert "transaction remains active after commit" in str(caught.value.__cause__)
        assert not sleep_called
        assert db.in_transaction
        assert Path(f"{db_path}-journal").exists()
    finally:
        db.leave_transaction_active = False
        db.rollback()
        db.close()


def test_database_retry_rolls_back_before_backoff_sleep(tmp_path, monkeypatch):
    db_path = tmp_path / "paper.db"
    db = _open_delete_journal_db(db_path)
    attempts = 0
    observed_sleeps = []

    def writer():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            db.execute("BEGIN IMMEDIATE")
            db.execute("INSERT INTO evidence VALUES ('rolled-back-before-backoff')")
            raise sqlite3.OperationalError("database is locked")
        return "recovered"

    def assert_safe_backoff(seconds):
        assert seconds == 0.01
        assert not db.in_transaction
        assert not Path(f"{db_path}-journal").exists()
        _assert_competing_writer_can_begin(db_path)
        observed_sleeps.append(seconds)

    monkeypatch.setattr(
        paper_trade_monitor,
        "sqlite_single_writer",
        lambda *_args, **_kwargs: contextlib.nullcontext(),
    )
    monkeypatch.setattr(paper_trade_monitor.time, "sleep", assert_safe_backoff)
    try:
        assert run_db_write_with_retry(
            db,
            writer,
            label="test_retry",
            attempts=2,
            base_sleep_sec=0.01,
        ) == "recovered"
        assert observed_sleeps == [0.01]
        assert db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 0
    finally:
        db.close()


def test_database_retry_never_sleeps_with_unclearable_transaction(
    tmp_path,
    monkeypatch,
):
    class FailingRollbackConnection(sqlite3.Connection):
        fail_rollback = False

        def rollback(self):
            if self.fail_rollback:
                raise sqlite3.OperationalError("injected rollback failure")
            return super().rollback()

    db_path = tmp_path / "paper.db"
    db = sqlite3.connect(
        db_path,
        timeout=0.05,
        factory=FailingRollbackConnection,
    )
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("CREATE TABLE evidence (value TEXT)")
    db.commit()
    db.execute("BEGIN IMMEDIATE")
    db.execute("INSERT INTO evidence VALUES ('must-not-retry-sleep')")
    db.fail_rollback = True
    root_error = sqlite3.OperationalError("database is locked")
    sleep_called = False

    def writer():
        raise root_error

    def forbidden_sleep(_seconds):
        nonlocal sleep_called
        sleep_called = True

    monkeypatch.setattr(
        paper_trade_monitor,
        "sqlite_single_writer",
        lambda *_args, **_kwargs: contextlib.nullcontext(),
    )
    monkeypatch.setattr(paper_trade_monitor.time, "sleep", forbidden_sleep)
    try:
        with pytest.raises(MonitorSupervisorRestartRequired) as caught:
            run_db_write_with_retry(
                db,
                writer,
                label="test_unclearable_retry",
                attempts=2,
                base_sleep_sec=0.01,
            )

        assert caught.value.__cause__ is root_error
        assert not sleep_called
        assert db.in_transaction
        assert Path(f"{db_path}-journal").exists()
        assert any(
            "paper_monitor_iteration_rollback_error OperationalError" in note
            for note in root_error.__notes__
        )
    finally:
        db.fail_rollback = False
        db.rollback()
        db.close()


def test_database_retry_never_sleeps_when_rollback_leaves_transaction_active(
    tmp_path,
    monkeypatch,
):
    class SilentRollbackConnection(sqlite3.Connection):
        leave_transaction_active = False

        def rollback(self):
            if self.leave_transaction_active:
                return None
            return super().rollback()

    db_path = tmp_path / "paper.db"
    db = sqlite3.connect(
        db_path,
        timeout=0.05,
        factory=SilentRollbackConnection,
    )
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("CREATE TABLE evidence (value TEXT)")
    db.commit()
    db.execute("BEGIN IMMEDIATE")
    db.execute("INSERT INTO evidence VALUES ('must-not-retry-sleep')")
    db.leave_transaction_active = True
    root_error = sqlite3.OperationalError("database is locked")
    sleep_called = False

    def writer():
        raise root_error

    def forbidden_sleep(_seconds):
        nonlocal sleep_called
        sleep_called = True

    monkeypatch.setattr(
        paper_trade_monitor,
        "sqlite_single_writer",
        lambda *_args, **_kwargs: contextlib.nullcontext(),
    )
    monkeypatch.setattr(paper_trade_monitor.time, "sleep", forbidden_sleep)
    try:
        with pytest.raises(MonitorSupervisorRestartRequired) as caught:
            run_db_write_with_retry(
                db,
                writer,
                label="test_silent_rollback_retry",
                attempts=2,
                base_sleep_sec=0.01,
            )

        assert caught.value.__cause__ is root_error
        assert not sleep_called
        assert db.in_transaction
        assert Path(f"{db_path}-journal").exists()
        assert any(
            "transaction remains active after rollback" in note
            for note in root_error.__notes__
        )
    finally:
        db.leave_transaction_active = False
        db.rollback()
        db.close()


def test_fast_lane_commit_lock_is_settled_before_execution_bridge_sleep(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "paper.db"
    db = _open_delete_journal_db(db_path)
    reader = sqlite3.connect(db_path, timeout=1)
    observed_sleeps = []
    try:
        reader.execute("BEGIN")
        assert reader.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 0

        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO evidence VALUES ('partial-fast-lane-adoption')")
        with pytest.raises(sqlite3.OperationalError, match="locked") as caught:
            db.commit()

        assert db.in_transaction
        assert Path(f"{db_path}-journal").exists()
        _settle_fast_lane_sync_failure(db, caught.value)

        assert not db.in_transaction
        assert not Path(f"{db_path}-journal").exists()
        assert db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 0
        _assert_competing_writer_can_begin(db_path)

        bridge_calls = 0

        def bridge_ping(*_args, **_kwargs):
            nonlocal bridge_calls
            bridge_calls += 1
            if bridge_calls < 3:
                raise ConnectionRefusedError("bridge not ready")
            return 200, {}

        def assert_safe_bridge_sleep(seconds):
            assert seconds == 0.1
            assert not db.in_transaction
            assert not Path(f"{db_path}-journal").exists()
            _assert_competing_writer_can_begin(db_path)
            observed_sleeps.append(seconds)

        class RunningProcess:
            @staticmethod
            def poll():
                return None

        bridge = paper_trade_monitor.PersistentExecutionBridge()
        bridge._proc = RunningProcess()
        monkeypatch.setattr(paper_trade_monitor, "_post_json", bridge_ping)
        monkeypatch.setattr(paper_trade_monitor.time, "sleep", assert_safe_bridge_sleep)
        bridge._start_if_needed()

        assert observed_sleeps == [0.1]
    finally:
        reader.rollback()
        reader.close()
        db.close()


def test_active_transaction_at_blocking_boundary_rolls_back_and_requires_restart(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "paper.db"
    db = _open_delete_journal_db(db_path)
    sleep_called = False

    def forbidden_sleep(_seconds):
        nonlocal sleep_called
        sleep_called = True

    monkeypatch.setattr(paper_trade_monitor.time, "sleep", forbidden_sleep)
    try:
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO evidence VALUES ('must-rollback-before-sleep')")
        assert db.in_transaction
        assert Path(f"{db_path}-journal").exists()

        with _monitor_transaction_guard_scope(db):
            with pytest.raises(MonitorSupervisorRestartRequired) as caught:
                _guarded_blocking_sleep(0.1, operation="test_sleep")

        assert "supervisor restart required before test_sleep" in str(caught.value)
        assert not sleep_called
        assert not db.in_transaction
        assert not Path(f"{db_path}-journal").exists()
        assert db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 0
        _assert_competing_writer_can_begin(db_path)
    finally:
        db.close()


def test_uncloseable_transaction_exits_before_execution_quote_call(
    tmp_path,
    monkeypatch,
):
    class SilentRollbackConnection(sqlite3.Connection):
        leave_transaction_active = False

        def rollback(self):
            if self.leave_transaction_active:
                return None
            return super().rollback()

    db_path = tmp_path / "paper.db"
    db = sqlite3.connect(
        db_path,
        timeout=0.05,
        factory=SilentRollbackConnection,
    )
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("CREATE TABLE evidence (value TEXT)")
    db.commit()
    bridge_called = False

    def forbidden_bridge_call(*_args, **_kwargs):
        nonlocal bridge_called
        bridge_called = True

    monkeypatch.setattr(
        paper_trade_monitor._daemon_bridge,
        "call",
        forbidden_bridge_call,
    )
    try:
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO evidence VALUES ('must-not-reach-quote')")
        db.leave_transaction_active = True

        with _monitor_transaction_guard_scope(db):
            with pytest.raises(MonitorSupervisorRestartRequired) as caught:
                call_execution_bridge("simulate-buy", {}, timeout=0.1)

        assert "execution_bridge_call:simulate-buy" in str(caught.value)
        assert not bridge_called
        assert db.in_transaction
        assert Path(f"{db_path}-journal").exists()
    finally:
        db.leave_transaction_active = False
        db.rollback()
        db.close()


def test_cross_thread_guardian_quote_requests_restart_before_simulate_sell(
    tmp_path,
):
    db_path = tmp_path / "paper.db"
    db = _open_delete_journal_db(db_path)
    simulate_called = False
    worker_errors = []

    def forbidden_simulate(*_args, **_kwargs):
        nonlocal simulate_called
        simulate_called = True

    guardian = exit_engine.ExitGuardianThread(
        positions_ref={},
        positions_lock=threading.Lock(),
        watchlist_store_ref=None,
        exit_queue=[],
        fetch_price_fn=lambda *_args, **_kwargs: (1.0, "test", 0),
        simulate_exit_fn=forbidden_simulate,
        blocking_guard_fn=_monitor_blocking_guard_for_db(db),
    )
    position = SimpleNamespace(
        token_amount_raw="1",
        token_decimals=6,
        strategy_stage="stage1",
        symbol="TEST",
    )

    def worker():
        try:
            guardian._get_instant_quote(position, "token")
        except BaseException as exc:
            worker_errors.append(exc)

    try:
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO evidence VALUES ('must-not-quote')")
        assert Path(f"{db_path}-journal").exists()

        with _monitor_transaction_guard_scope(db):
            thread = threading.Thread(target=worker)
            thread.start()
            thread.join(timeout=2)
            assert not thread.is_alive()
            with pytest.raises(MonitorSupervisorRestartRequired) as caught:
                _raise_monitor_supervisor_restart_if_requested()

        assert worker_errors == [caught.value]
        assert not simulate_called
        assert db.in_transaction
        assert Path(f"{db_path}-journal").exists()
        competitor = sqlite3.connect(db_path, timeout=0.05)
        try:
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                competitor.execute("BEGIN IMMEDIATE")
        finally:
            competitor.close()
    finally:
        _clear_monitor_supervisor_restart_request()
        paper_trade_monitor.SHUTDOWN_REQUESTED.clear()
        db.rollback()
        db.close()

    assert not Path(f"{db_path}-journal").exists()
    _assert_competing_writer_can_begin(db_path)


def test_cross_thread_guardian_wait_requests_restart_before_raw_sleep(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "paper.db"
    db = _open_delete_journal_db(db_path)
    sleep_called = False
    worker_errors = []

    def forbidden_sleep(_seconds):
        nonlocal sleep_called
        sleep_called = True

    monkeypatch.setattr(exit_engine.time, "sleep", forbidden_sleep)
    guardian = exit_engine.ExitGuardianThread(
        positions_ref={},
        positions_lock=threading.Lock(),
        watchlist_store_ref=None,
        exit_queue=[],
        fetch_price_fn=lambda *_args, **_kwargs: (1.0, "test", 0),
        blocking_guard_fn=_monitor_blocking_guard_for_db(db),
    )

    def worker():
        try:
            guardian._blocking_sleep(3, "test_guardian_wait")
        except BaseException as exc:
            worker_errors.append(exc)

    try:
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO evidence VALUES ('must-not-wait')")
        with _monitor_transaction_guard_scope(db):
            thread = threading.Thread(target=worker)
            thread.start()
            thread.join(timeout=2)
            assert not thread.is_alive()
            with pytest.raises(MonitorSupervisorRestartRequired) as caught:
                _raise_monitor_supervisor_restart_if_requested()

        assert worker_errors == [caught.value]
        assert not sleep_called
        assert db.in_transaction
        assert Path(f"{db_path}-journal").exists()
    finally:
        _clear_monitor_supervisor_restart_request()
        paper_trade_monitor.SHUTDOWN_REQUESTED.clear()
        db.rollback()
        db.close()


def test_worker_restart_request_rolls_back_instead_of_committing_success_path(
    tmp_path,
):
    db_path = tmp_path / "paper.db"
    db = sqlite3.connect(
        db_path,
        timeout=1,
        factory=paper_trade_monitor.MonitorSQLiteConnection,
    )
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("CREATE TABLE evidence (value TEXT)")
    db.commit()
    worker_errors = []
    try:
        with _monitor_transaction_guard_scope(db):
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "INSERT INTO evidence VALUES ('must-rollback-on-worker-restart')"
            )

            def worker():
                try:
                    _guarded_blocking_sleep(
                        0.01,
                        operation="worker_detects_owner_transaction",
                    )
                except BaseException as exc:
                    worker_errors.append(exc)

            thread = threading.Thread(target=worker)
            thread.start()
            thread.join(timeout=2)
            assert not thread.is_alive()
            assert len(worker_errors) == 1

            with pytest.raises(MonitorSupervisorRestartRequired) as commit_caught:
                db.commit()

            with pytest.raises(MonitorSupervisorRestartRequired) as caught:
                _sleep_after_monitor_transaction_settle(
                    db,
                    0.01,
                    success=True,
                )

            assert commit_caught.value is worker_errors[0]
            assert caught.value is worker_errors[0]
            assert not db.in_transaction
            assert not Path(f"{db_path}-journal").exists()
            assert db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 0
            _assert_competing_writer_can_begin(db_path)
    finally:
        if db.in_transaction:
            db.rollback()
        db.close()

    reopened = sqlite3.connect(db_path, timeout=1)
    try:
        assert reopened.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 0
    finally:
        reopened.close()


def test_quote_boundary_gate_prevents_begin_until_actual_call_returns(tmp_path):
    db_path = tmp_path / "paper.db"
    db = _open_delete_journal_db(db_path)
    entered_quote = threading.Event()
    release_quote = threading.Event()
    quote_finished = threading.Event()
    quote_observations = []
    worker_errors = []
    try:
        with _monitor_transaction_guard_scope(db):
            blocking_call = _monitor_blocking_call_for_db(db)

            def quote():
                quote_observations.append(db.in_transaction)
                entered_quote.set()
                assert release_quote.wait(timeout=2)
                quote_observations.append(db.in_transaction)
                quote_finished.set()
                return "quoted"

            def worker():
                try:
                    assert blocking_call("test_quote", quote) == "quoted"
                except BaseException as exc:
                    worker_errors.append(exc)

            thread = threading.Thread(target=worker)
            thread.start()
            assert entered_quote.wait(timeout=2)
            timer = threading.Timer(0.05, release_quote.set)
            timer.start()

            # BEGIN cannot reach SQLite until the exact quote callback returns.
            db.execute("BEGIN IMMEDIATE")
            timer.join(timeout=1)
            thread.join(timeout=2)

            assert not thread.is_alive()
            assert worker_errors == []
            assert quote_finished.is_set()
            assert quote_observations == [False, False]
            assert db.in_transaction
            db.rollback()
            _assert_competing_writer_can_begin(db_path)
    finally:
        if db.in_transaction:
            db.rollback()
        db.close()


def test_sleep_boundary_releases_gate_only_after_wait_has_started(tmp_path):
    db_path = tmp_path / "paper.db"
    db = _open_delete_journal_db(db_path)
    worker_errors = []
    try:
        with _monitor_transaction_guard_scope(db):
            coordinator = paper_trade_monitor._MONITOR_TRANSACTION_COORDINATOR

            def worker():
                try:
                    _guarded_blocking_sleep(
                        0.1,
                        operation="test_atomic_sleep_start",
                    )
                except BaseException as exc:
                    worker_errors.append(exc)

            thread = threading.Thread(target=worker)
            thread.start()

            deadline = time.monotonic() + 2
            while not coordinator.boundary_condition._waiters:
                assert time.monotonic() < deadline
                threading.Event().wait(0.001)

            # A registered Condition waiter proves the guarded wait has begun.
            assert coordinator.boundary_gate.acquire(timeout=1)
            try:
                db.execute("BEGIN IMMEDIATE")
                db.execute("INSERT INTO evidence VALUES ('opened-after-wait-start')")
            finally:
                coordinator.boundary_gate.release()

            thread.join(timeout=2)
            assert not thread.is_alive()
            assert len(worker_errors) == 1
            assert isinstance(worker_errors[0], MonitorSupervisorRestartRequired)
            assert db.in_transaction
            assert Path(f"{db_path}-journal").exists()
            db.rollback()
            assert db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 0
    finally:
        if db.in_transaction:
            db.rollback()
        db.close()


def test_main_installs_guard_before_startup_wait_and_closes_on_restart(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "paper.db"
    db = _open_delete_journal_db(db_path)
    raw_sleep_called = False
    close_calls = []

    def startup_wait():
        _guarded_blocking_sleep(5, operation="test_startup_wait")

    def forbidden_sleep(_seconds):
        nonlocal raw_sleep_called
        raw_sleep_called = True

    def close_db(connection, *, context, commit_pending):
        close_calls.append((context, commit_pending, connection.in_transaction))
        connection.close()

    db.execute("BEGIN IMMEDIATE")
    db.execute("INSERT INTO evidence VALUES ('startup-partial')")
    monkeypatch.setattr(paper_trade_monitor, "init_paper_db", lambda: db)
    monkeypatch.setattr(paper_trade_monitor, "wait_for_local_signal_source", startup_wait)
    monkeypatch.setattr(paper_trade_monitor.time, "sleep", forbidden_sleep)
    monkeypatch.setattr(paper_trade_monitor, "close_paper_db_gracefully", close_db)
    monkeypatch.setattr(paper_trade_monitor.signal, "signal", lambda *_args: None)
    monkeypatch.setattr(paper_trade_monitor.sys, "argv", ["paper_trade_monitor.py"])

    with pytest.raises(MonitorSupervisorRestartRequired):
        paper_trade_monitor.main()

    assert not raw_sleep_called
    assert close_calls == [("monitor_failure", False, False)]
    assert not Path(f"{db_path}-journal").exists()
    _assert_competing_writer_can_begin(db_path)


def test_all_blocking_primitives_route_through_transaction_guard():
    source = Path(paper_trade_monitor.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    raw_calls = []

    class BlockingCallVisitor(ast.NodeVisitor):
        def __init__(self):
            self.function_names = []

        def visit_FunctionDef(self, node):
            self.function_names.append(node.name)
            self.generic_visit(node)
            self.function_names.pop()

        visit_AsyncFunctionDef = visit_FunctionDef

        def visit_Call(self, node):
            raw_kind = None
            if (
                isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "time"
                and node.func.attr == "sleep"
            ):
                raw_kind = "sleep"
            elif (
                isinstance(node.func, ast.Attribute)
                and node.func.attr in {"wait", "result"}
            ):
                raw_kind = node.func.attr
            if raw_kind is not None:
                raw_calls.append(
                    (
                        self.function_names[-1] if self.function_names else None,
                        raw_kind,
                    )
                )
            self.generic_visit(node)

    BlockingCallVisitor().visit(tree)

    assert raw_calls == [
        ("_guarded_blocking_sleep", "sleep"),
        ("_guarded_blocking_sleep", "wait"),
        ("_guarded_event_wait", "wait"),
        ("_guarded_event_wait", "wait"),
        ("_guarded_future_result", "result"),
        ("_guarded_future_result", "wait"),
        ("_guarded_future_result", "result"),
    ]

    exit_source = Path(exit_engine.__file__).read_text(encoding="utf-8")
    exit_tree = ast.parse(exit_source)
    exit_raw_calls = []

    class ExitBlockingCallVisitor(ast.NodeVisitor):
        def __init__(self):
            self.function_names = []

        def visit_FunctionDef(self, node):
            self.function_names.append(node.name)
            self.generic_visit(node)
            self.function_names.pop()

        def visit_Call(self, node):
            if (
                isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "time"
                and node.func.attr == "sleep"
            ):
                exit_raw_calls.append(self.function_names[-1])
            self.generic_visit(node)

    ExitBlockingCallVisitor().visit(exit_tree)
    assert exit_raw_calls == ["_blocking_sleep"]

    smart_entry_source = inspect.getsource(entry_engine.evaluate_smart_entry)
    assert "blocking_sleep_fn(sleep_for, \"smart_entry_poll_wait\")" in smart_entry_source
    assert smart_entry_source.count("_time.sleep(sleep_for)") == 1


def test_quote_boundaries_require_transaction_guard():
    guarded_functions = (
        paper_trade_monitor.call_execution_bridge,
        paper_trade_monitor.PersistentExecutionBridge._start_if_needed,
        paper_trade_monitor.get_live_price_snapshot,
    )
    for function in guarded_functions:
        tree = ast.parse(textwrap.dedent(inspect.getsource(function)))
        function_node = tree.body[0]
        body = list(function_node.body)
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            body = body[1:]
        first_statement = body[0]
        assert isinstance(first_statement, ast.With)
        context_call = first_statement.items[0].context_expr
        assert isinstance(context_call, ast.Call)
        assert isinstance(context_call.func, ast.Name)
        assert context_call.func.id == "_monitor_blocking_boundary_scope"

    assert not issubclass(MonitorSupervisorRestartRequired, Exception)
    run_source = inspect.getsource(paper_trade_monitor.run_monitor)
    assert "blocking_guard_fn=_monitor_blocking_guard_for_db(db)" in run_source
    assert run_source.count("blocking_sleep_fn=_monitor_blocking_sleep_for_db(db)") == 2
    assert "blocking_call_fn=_monitor_blocking_call_for_db(db)" in run_source
    main_source = inspect.getsource(paper_trade_monitor.main)
    assert "with _monitor_transaction_guard_scope(db):" in main_source


def test_run_monitor_has_no_direct_blocking_time_sleep():
    source = textwrap.dedent(inspect.getsource(paper_trade_monitor.run_monitor))
    tree = ast.parse(source)
    direct_sleep_lines = []
    guarded_sleep_lines = []
    for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call)):
        if (
            isinstance(call.func, ast.Attribute)
            and isinstance(call.func.value, ast.Name)
            and call.func.value.id == "time"
            and call.func.attr == "sleep"
        ):
            direct_sleep_lines.append(call.lineno)
        if (
            isinstance(call.func, ast.Name)
            and call.func.id == "_sleep_after_monitor_transaction_settle"
        ):
            guarded_sleep_lines.append(call.lineno)

    assert direct_sleep_lines == []
    assert len(guarded_sleep_lines) == 7


def test_failed_monitor_iteration_rolls_back_before_retry_sleep(tmp_path):
    db_path = tmp_path / "paper.db"
    db = _open_delete_journal_db(db_path)
    root_error = LookupError("iteration failed")
    try:
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO evidence VALUES ('rolled-back')")
        assert db.in_transaction
        assert Path(f"{db_path}-journal").exists()

        assert _settle_monitor_iteration_transaction(
            db,
            success=False,
            root_error=root_error,
        )

        assert not db.in_transaction
        assert not Path(f"{db_path}-journal").exists()
        assert db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 0
        _assert_competing_writer_can_begin(db_path)
    finally:
        db.close()


def test_failed_monitor_iteration_exits_before_retry_wait_when_rollback_fails(
    tmp_path,
):
    class FailingRollbackConnection(sqlite3.Connection):
        def rollback(self):
            raise sqlite3.OperationalError("injected rollback failure")

    db_path = tmp_path / "paper.db"
    db = sqlite3.connect(
        db_path,
        timeout=0.05,
        factory=FailingRollbackConnection,
    )
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("CREATE TABLE evidence (value TEXT)")
    db.commit()
    db.execute("BEGIN IMMEDIATE")
    db.execute("INSERT INTO evidence VALUES ('must-not-survive-restart')")

    root_error = LookupError("iteration failed")
    retry_wait_called = False
    try:
        with pytest.raises(LookupError, match="iteration failed") as caught:
            _settle_monitor_iteration_transaction(
                db,
                success=False,
                root_error=root_error,
            )
            retry_wait_called = True

        assert caught.value is root_error
        assert not retry_wait_called
        assert db.in_transaction
        assert Path(f"{db_path}-journal").exists()
        assert any(
            "paper_monitor_iteration_rollback_error OperationalError" in note
            for note in root_error.__notes__
        )

        competitor = sqlite3.connect(db_path, timeout=0.05)
        try:
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                competitor.execute("BEGIN IMMEDIATE")
        finally:
            competitor.close()
    finally:
        close_paper_db_gracefully(
            db,
            context="test_monitor_failure",
            commit_pending=False,
        )

    assert not Path(f"{db_path}-journal").exists()
    _assert_competing_writer_can_begin(db_path)
    reopened = sqlite3.connect(db_path, timeout=1)
    try:
        assert reopened.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 0
    finally:
        reopened.close()


def test_failed_best_effort_audit_commit_releases_delete_journal_before_inner_sleep(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "paper.db"
    db = _open_delete_journal_db(db_path)
    reader = sqlite3.connect(db_path, timeout=1)
    try:
        db.execute(paper_decision_audit.CREATE_DECISION_AUDIT_SQL)
        db.execute(
            """
            INSERT INTO paper_decision_events
                (event_ts, component, event_type, decision)
            VALUES (1, 'baseline', 'baseline', 'baseline')
            """
        )
        db.commit()

        reader.execute("BEGIN")
        assert reader.execute(
            "SELECT id FROM paper_decision_events ORDER BY id"
        ).fetchone()

        monkeypatch.setattr(
            paper_decision_audit,
            "append_paper_evidence_event",
            lambda **_kwargs: None,
        )
        monkeypatch.setattr(
            paper_decision_audit,
            "_maybe_record_missed_attribution",
            lambda *_args, **_kwargs: None,
        )
        monkeypatch.setattr(
            paper_decision_audit,
            "_mirror_v27_decision_event",
            lambda **_kwargs: None,
        )

        paper_decision_audit.record_decision_event(
            db,
            component="signal_ingest",
            event_type="signal_received",
            decision="received",
        )

        def assert_safe_inner_sleep(_seconds):
            assert not db.in_transaction
            assert not Path(f"{db_path}-journal").exists()
            _assert_competing_writer_can_begin(db_path)

        assert_safe_inner_sleep(0.1)
        assert db.execute(
            "SELECT COUNT(*) FROM paper_decision_events"
        ).fetchone()[0] == 1
    finally:
        reader.rollback()
        reader.close()
        db.close()


def test_failed_best_effort_audit_does_not_swallow_an_uncleared_transaction(
    monkeypatch,
):
    class UnclearableAuditDb:
        in_transaction = True

        def execute(self, *_args, **_kwargs):
            raise LookupError("injected audit insert failure")

        def rollback(self):
            raise RuntimeError("injected audit rollback failure")

    monkeypatch.setattr(
        paper_decision_audit,
        "append_paper_evidence_event",
        lambda **_kwargs: None,
    )

    with pytest.raises(LookupError, match="injected audit insert failure") as caught:
        paper_decision_audit.record_decision_event(
            UnclearableAuditDb(),
            component="signal_ingest",
            event_type="signal_received",
            decision="received",
        )

    assert any(
        "paper_decision_audit_rollback_error RuntimeError" in note
        for note in caught.value.__notes__
    )

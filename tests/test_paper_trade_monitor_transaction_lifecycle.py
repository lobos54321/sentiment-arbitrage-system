import ast
import contextlib
import inspect
import importlib.util
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
    monitor_standalone_blocking_call,
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


def _open_monitor_delete_journal_db(path):
    db = sqlite3.connect(
        path,
        timeout=1,
        factory=paper_trade_monitor.MonitorSQLiteConnection,
    )
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("CREATE TABLE IF NOT EXISTS evidence (value TEXT)")
    db.commit()
    return db


def _open_monitor_wal_db(path):
    db = sqlite3.connect(
        path,
        timeout=1,
        factory=paper_trade_monitor.MonitorSQLiteConnection,
    )
    db.execute("PRAGMA journal_mode=WAL")
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


def test_alternate_script_module_claims_canonical_monitor_identity():
    source_path = Path(paper_trade_monitor.__file__)
    module_name = "_paper_trade_monitor_script_identity_probe"
    spec = importlib.util.spec_from_file_location(module_name, source_path)
    assert spec is not None and spec.loader is not None
    probe = importlib.util.module_from_spec(spec)
    previous_alias = sys.modules.pop("paper_trade_monitor", None)
    previous_probe = sys.modules.get(module_name)
    sys.modules[module_name] = probe
    try:
        spec.loader.exec_module(probe)
        assert sys.modules["paper_trade_monitor"] is probe
        assert __import__("paper_trade_monitor") is probe
        assert probe.call_execution_bridge.__globals__ is probe.__dict__

        token = "MODULE_IDENTITY_PROBE_TOKEN"
        entry_engine._dex_trend_cache.pop(token, None)
        probe.curl_json = lambda *_args, **_kwargs: {
            "pairs": [
                {
                    "dexId": "module-identity-probe",
                    "pairAddress": "probe-pair",
                    "priceUsd": "1",
                    "volume": {"m5": 1, "h1": 2},
                    "txns": {"m5": {"buys": 1, "sells": 0}},
                    "priceChange": {"m5": 0, "h1": 0},
                    "liquidity": {"usd": 1000},
                }
            ]
        }
        probe._select_best_dex_pair = lambda _token, pairs: pairs[0]
        result = entry_engine.fetch_dexscreener_trend_snapshot(token, timeout=0.01)
        assert result["dex_id"] == "module-identity-probe"
        entry_engine._dex_trend_cache.pop(token, None)
    finally:
        if previous_alias is None:
            sys.modules.pop("paper_trade_monitor", None)
        else:
            sys.modules["paper_trade_monitor"] = previous_alias
        if previous_probe is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = previous_probe


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

        def assert_safe_wait(timeout=None):
            assert timeout == pytest.approx(0.2, abs=0.02)
            assert not db.in_transaction
            assert not Path(f"{db_path}-journal").exists()
            _assert_competing_writer_can_begin(db_path)
            observed_sleeps.append(timeout)
            return original_wait(timeout=timeout)

        with _monitor_transaction_guard_scope(db):
            coordinator = paper_trade_monitor._MONITOR_TRANSACTION_COORDINATOR
            original_wait = coordinator.boundary_condition.wait
            monkeypatch.setattr(
                coordinator.boundary_condition,
                "wait",
                assert_safe_wait,
            )
            _sleep_after_monitor_transaction_settle(db, 0.2)

        assert len(observed_sleeps) == 1
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

    monkeypatch.setattr(
        paper_trade_monitor,
        "sqlite_single_writer",
        lambda *_args, **_kwargs: contextlib.nullcontext(),
    )
    try:
        with _monitor_transaction_guard_scope(db):
            coordinator = paper_trade_monitor._MONITOR_TRANSACTION_COORDINATOR
            original_wait = coordinator.boundary_condition.wait

            def assert_safe_backoff(timeout=None):
                assert timeout == pytest.approx(0.01, abs=0.01)
                assert not db.in_transaction
                assert not Path(f"{db_path}-journal").exists()
                _assert_competing_writer_can_begin(db_path)
                observed_sleeps.append(timeout)
                return original_wait(timeout=timeout)

            monkeypatch.setattr(
                coordinator.boundary_condition,
                "wait",
                assert_safe_backoff,
            )
            assert run_db_write_with_retry(
                db,
                writer,
                label="test_retry",
                attempts=2,
                base_sleep_sec=0.01,
            ) == "recovered"
        assert len(observed_sleeps) == 1
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
    db.fail_rollback = True
    root_error = sqlite3.OperationalError("database is locked")
    sleep_called = False

    def writer():
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO evidence VALUES ('must-not-retry-sleep')")
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
            with _monitor_transaction_guard_scope(db):
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
    db.leave_transaction_active = True
    root_error = sqlite3.OperationalError("database is locked")
    sleep_called = False

    def writer():
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO evidence VALUES ('must-not-retry-sleep')")
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
            with _monitor_transaction_guard_scope(db):
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

        class RunningProcess:
            @staticmethod
            def poll():
                return None

        bridge = paper_trade_monitor.PersistentExecutionBridge()
        bridge._proc = RunningProcess()
        monkeypatch.setattr(paper_trade_monitor, "_post_json", bridge_ping)
        with _monitor_transaction_guard_scope(db):
            coordinator = paper_trade_monitor._MONITOR_TRANSACTION_COORDINATOR
            original_wait = coordinator.boundary_condition.wait

            def assert_safe_bridge_wait(timeout=None):
                assert timeout == pytest.approx(0.1, abs=0.02)
                assert not db.in_transaction
                assert not Path(f"{db_path}-journal").exists()
                _assert_competing_writer_can_begin(db_path)
                observed_sleeps.append(timeout)
                return original_wait(timeout=timeout)

            monkeypatch.setattr(
                coordinator.boundary_condition,
                "wait",
                assert_safe_bridge_wait,
            )
            bridge._start_if_needed()

        assert len(observed_sleeps) == 1
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


def test_cross_thread_guardian_quote_waits_for_owner_commit_before_simulate_sell(
    tmp_path,
):
    db_path = tmp_path / "paper.db"
    db = _open_monitor_delete_journal_db(db_path)
    simulate_called = threading.Event()
    worker_errors = []

    def simulate(*_args, **_kwargs):
        simulate_called.set()
        return {"quotedOutputSOL": 1.0}

    guardian = exit_engine.ExitGuardianThread(
        positions_ref={},
        positions_lock=threading.Lock(),
        watchlist_store_ref=None,
        exit_queue=[],
        fetch_price_fn=lambda *_args, **_kwargs: (1.0, "test", 0),
        simulate_exit_fn=simulate,
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
        with _monitor_transaction_guard_scope(db):
            db.execute("BEGIN IMMEDIATE")
            db.execute("INSERT INTO evidence VALUES ('commit-before-quote')")
            assert Path(f"{db_path}-journal").exists()
            thread = threading.Thread(target=worker)
            thread.start()
            time.sleep(0.05)
            assert thread.is_alive()
            assert not simulate_called.is_set()

            db.commit()
            thread.join(timeout=2)
            assert not thread.is_alive()
            _raise_monitor_supervisor_restart_if_requested()

        assert worker_errors == []
        assert simulate_called.is_set()
        assert not db.in_transaction
        assert not Path(f"{db_path}-journal").exists()
        assert db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 1
        _assert_competing_writer_can_begin(db_path)
    finally:
        db.close()


def test_wal_guardian_quote_waits_for_owner_commit_without_restart(tmp_path):
    db_path = tmp_path / "paper-wal.db"
    db = _open_monitor_wal_db(db_path)
    quote_called = threading.Event()
    worker_errors = []

    def quote(*_args, **_kwargs):
        quote_called.set()
        return {"quotedOutputSOL": 1.0}

    guardian = exit_engine.ExitGuardianThread(
        positions_ref={},
        positions_lock=threading.Lock(),
        watchlist_store_ref=None,
        exit_queue=[],
        fetch_price_fn=lambda *_args, **_kwargs: (1.0, "test", 0),
        simulate_exit_fn=quote,
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
        with _monitor_transaction_guard_scope(db):
            db.execute("BEGIN IMMEDIATE")
            db.execute("INSERT INTO evidence VALUES ('wal-commit-before-quote')")
            thread = threading.Thread(target=worker)
            thread.start()
            threading.Event().wait(0.05)
            assert thread.is_alive()
            assert not quote_called.is_set()

            db.commit()
            thread.join(timeout=2)
            assert not thread.is_alive()
            _raise_monitor_supervisor_restart_if_requested()

        assert worker_errors == []
        assert quote_called.is_set()
        assert db.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 1
        _assert_competing_writer_can_begin(db_path)
    finally:
        db.close()


def test_cross_thread_guardian_waits_for_owner_commit_before_raw_sleep(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "paper.db"
    db = _open_monitor_delete_journal_db(db_path)
    sleep_called = threading.Event()
    worker_errors = []

    def observed_sleep(_seconds):
        sleep_called.set()

    monkeypatch.setattr(exit_engine.time, "sleep", observed_sleep)
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
        with _monitor_transaction_guard_scope(db):
            db.execute("BEGIN IMMEDIATE")
            db.execute("INSERT INTO evidence VALUES ('commit-before-wait')")
            thread = threading.Thread(target=worker)
            thread.start()
            threading.Event().wait(0.05)
            assert thread.is_alive()
            assert not sleep_called.is_set()

            db.commit()
            thread.join(timeout=2)
            assert not thread.is_alive()
            _raise_monitor_supervisor_restart_if_requested()

        assert worker_errors == []
        assert sleep_called.is_set()
        assert not db.in_transaction
        assert not Path(f"{db_path}-journal").exists()
    finally:
        db.close()


def test_worker_restart_request_rolls_back_instead_of_committing_success_path(
    tmp_path,
):
    db_path = tmp_path / "paper.db"
    db = _open_monitor_delete_journal_db(db_path)
    worker_requests = []
    try:
        with _monitor_transaction_guard_scope(db):
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "INSERT INTO evidence VALUES ('must-rollback-on-worker-restart')"
            )

            def worker():
                cause = RuntimeError("forced worker restart")
                restart = paper_trade_monitor._monitor_restart_required(
                    "worker_forced_restart",
                    cause,
                )
                worker_requests.append(
                    paper_trade_monitor._request_monitor_supervisor_restart(restart)
                )

            thread = threading.Thread(target=worker)
            thread.start()
            thread.join(timeout=2)
            assert not thread.is_alive()
            assert len(worker_requests) == 1

            with pytest.raises(MonitorSupervisorRestartRequired) as commit_caught:
                db.commit()

            assert commit_caught.value is worker_requests[0]
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


def test_run_monitor_rejects_unprotected_connection_before_partial_commit(
    tmp_path,
):
    db_path = tmp_path / "paper.db"
    db = _open_delete_journal_db(db_path)
    try:
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO evidence VALUES ('must-not-commit')")

        with pytest.raises(MonitorSupervisorRestartRequired) as caught:
            paper_trade_monitor.run_monitor(db)

        assert "run_monitor_connection_contract" in str(caught.value)
        assert not db.in_transaction
        assert not Path(f"{db_path}-journal").exists()
        assert db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 0
        _assert_competing_writer_can_begin(db_path)
    finally:
        if db.in_transaction:
            db.rollback()
        db.close()


def test_run_monitor_rejects_connection_subclass_spoof(tmp_path):
    class SpoofedMonitorConnection(
        paper_trade_monitor.MonitorSQLiteConnection
    ):
        pass

    db_path = tmp_path / "paper.db"
    db = sqlite3.connect(
        db_path,
        timeout=1,
        factory=SpoofedMonitorConnection,
    )
    try:
        db.execute("PRAGMA journal_mode=DELETE")
        db.execute("CREATE TABLE evidence (value TEXT)")
        db.commit()
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO evidence VALUES ('must-not-commit')")

        with pytest.raises(MonitorSupervisorRestartRequired) as caught:
            paper_trade_monitor.run_monitor(db)

        assert "run_monitor_connection_contract" in str(caught.value)
        assert not db.in_transaction
        assert not Path(f"{db_path}-journal").exists()
        assert db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 0
        _assert_competing_writer_can_begin(db_path)
    finally:
        if db.in_transaction:
            db.rollback()
        db.close()


def test_missing_coordinator_fails_closed_before_sleep_and_quote(monkeypatch):
    sleep_called = False
    bridge_called = False

    def forbidden_sleep(_seconds):
        nonlocal sleep_called
        sleep_called = True

    def forbidden_bridge(*_args, **_kwargs):
        nonlocal bridge_called
        bridge_called = True

    monkeypatch.setattr(paper_trade_monitor.time, "sleep", forbidden_sleep)
    monkeypatch.setattr(paper_trade_monitor._daemon_bridge, "call", forbidden_bridge)

    with pytest.raises(MonitorSupervisorRestartRequired) as sleep_caught:
        _guarded_blocking_sleep(0.01, operation="missing_context_sleep")
    assert "missing_context_sleep" in str(sleep_caught.value)
    assert not sleep_called

    _clear_monitor_supervisor_restart_request()
    paper_trade_monitor.SHUTDOWN_REQUESTED.clear()

    with pytest.raises(MonitorSupervisorRestartRequired) as quote_caught:
        call_execution_bridge("simulate-sell", {}, timeout=0.01)
    assert "execution_bridge_call:simulate-sell" in str(quote_caught.value)
    assert not bridge_called


def test_standalone_quote_permit_requires_idle_database(tmp_path, monkeypatch):
    db_path = tmp_path / "paper.db"
    db = _open_monitor_delete_journal_db(db_path)
    bridge_calls = []

    def bridge_call(command, payload, timeout):
        bridge_calls.append((command, payload, timeout, db.in_transaction))
        return {"success": True}

    monkeypatch.setattr(paper_trade_monitor._daemon_bridge, "call", bridge_call)
    try:
        assert monitor_standalone_blocking_call(
            db,
            "standalone_quote",
            call_execution_bridge,
            "simulate-buy",
            {"token": "test"},
            timeout=0.01,
        ) == {"success": True}

        assert bridge_calls == [
            ("simulate-buy", {"token": "test"}, 0.01, False)
        ]

        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO evidence VALUES ('must-rollback')")
        with pytest.raises(MonitorSupervisorRestartRequired):
            monitor_standalone_blocking_call(
                db,
                "standalone_quote_with_transaction",
                call_execution_bridge,
                "simulate-buy",
                {},
                timeout=0.01,
            )

        assert len(bridge_calls) == 1
        assert not db.in_transaction
        assert not Path(f"{db_path}-journal").exists()
        assert db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 0
        _assert_competing_writer_can_begin(db_path)
    finally:
        _clear_monitor_supervisor_restart_request()
        paper_trade_monitor.SHUTDOWN_REQUESTED.clear()
        if db.in_transaction:
            db.rollback()
        db.close()


def test_standalone_boundary_cannot_be_reused_after_opening_transaction(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "paper.db"
    db = _open_monitor_delete_journal_db(db_path)
    sleep_called = False

    def forbidden_sleep(_seconds):
        nonlocal sleep_called
        sleep_called = True

    def open_transaction_then_sleep():
        db.execute("BEGIN IMMEDIATE")
        _guarded_blocking_sleep(0.01, operation="standalone_reused_sleep")

    monkeypatch.setattr(paper_trade_monitor.time, "sleep", forbidden_sleep)
    try:
        with pytest.raises(MonitorSupervisorRestartRequired) as caught:
            monitor_standalone_blocking_call(
                db,
                "standalone_one_shot",
                open_transaction_then_sleep,
            )

        assert "sqlite_unprotected_operation" in str(caught.value)
        assert not sleep_called
        assert not db.in_transaction
        assert not Path(f"{db_path}-journal").exists()
        _assert_competing_writer_can_begin(db_path)
    finally:
        _clear_monitor_supervisor_restart_request()
        paper_trade_monitor.SHUTDOWN_REQUESTED.clear()
        if db.in_transaction:
            db.rollback()
        db.close()


def test_quote_boundary_gate_prevents_begin_until_actual_call_returns(tmp_path):
    db_path = tmp_path / "paper.db"
    db = _open_monitor_delete_journal_db(db_path)
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


def test_sqlite_base_methods_cannot_bypass_exact_quote_boundary(tmp_path):
    db_path = tmp_path / "paper.db"
    db = _open_monitor_delete_journal_db(db_path)
    entered_quote = threading.Event()
    release_quote = threading.Event()
    quote_states = []
    worker_errors = []
    cursor = None
    try:
        # Warm the exact statement once: cached prepared statements must not
        # bypass the engine authorizer on a later base-method invocation.
        db.execute("BEGIN IMMEDIATE")
        db.rollback()
        with pytest.raises(TypeError, match="authorizer is immutable"):
            db.set_authorizer(None)
        with pytest.raises(TypeError, match="doesn't apply"):
            sqlite3.Connection.set_authorizer(db, None)
        cursor = db.cursor()

        with _monitor_transaction_guard_scope(db):
            blocking_call = _monitor_blocking_call_for_db(db)

            def quote():
                quote_states.append(("entered", db.in_transaction))
                entered_quote.set()
                assert release_quote.wait(timeout=2)
                quote_states.append(("before_return", db.in_transaction))
                return "quoted"

            def worker():
                try:
                    blocking_call("base_method_quote", quote)
                except BaseException as exc:
                    worker_errors.append(exc)

            thread = threading.Thread(target=worker)
            thread.start()
            assert entered_quote.wait(timeout=2)

            with pytest.raises(TypeError, match="doesn't apply"):
                sqlite3.Connection.execute(db, "BEGIN IMMEDIATE")
            with pytest.raises(TypeError, match="doesn't apply"):
                sqlite3.Connection.cursor(db)
            with pytest.raises(TypeError, match="doesn't apply"):
                sqlite3.Cursor.execute(cursor, "BEGIN IMMEDIATE")
            assert not db.in_transaction
            assert not Path(f"{db_path}-journal").exists()
            _assert_competing_writer_can_begin(db_path)

            release_quote.set()
            thread.join(timeout=2)
            assert not thread.is_alive()
            assert worker_errors == []
            assert quote_states == [
                ("entered", False),
                ("before_return", False),
            ]
    finally:
        release_quote.set()
        if cursor is not None:
            cursor.close()
        _clear_monitor_supervisor_restart_request()
        paper_trade_monitor.SHUTDOWN_REQUESTED.clear()
        if db.in_transaction:
            db.rollback()
        db.close()


def test_standalone_boundary_uses_opaque_sqlite_handle(tmp_path):
    db_path = tmp_path / "paper.db"
    db = _open_monitor_delete_journal_db(db_path)
    callback_states = []

    def wait_without_native_handle_access():
        callback_states.append(db.in_transaction)
        with pytest.raises(TypeError, match="doesn't apply"):
            sqlite3.Connection.set_authorizer(db, None)
        with pytest.raises(TypeError, match="doesn't apply"):
            sqlite3.Connection.execute(db, "BEGIN IMMEDIATE")
        cursor = db.cursor()
        try:
            with pytest.raises(TypeError, match="doesn't apply"):
                sqlite3.Cursor.execute(cursor, "BEGIN IMMEDIATE")
        finally:
            cursor.close()
        assert threading.Event().wait(0.01) is False
        callback_states.append(db.in_transaction)
        return "wait-completed"

    try:
        assert monitor_standalone_blocking_call(
            db,
            "standalone_opaque_handle",
            wait_without_native_handle_access,
        ) == "wait-completed"
        assert callback_states == [False, False]
        assert not db.in_transaction
        assert not Path(f"{db_path}-journal").exists()
        _assert_competing_writer_can_begin(db_path)
    finally:
        if db.in_transaction:
            db.rollback()
        db.close()


def test_native_sqlite_descriptors_reject_monitor_proxies(tmp_path):
    db_path = tmp_path / "paper.db"
    db = _open_monitor_delete_journal_db(db_path)
    cursor = db.cursor()
    try:
        connection_calls = (
            lambda: sqlite3.Connection.execute(db, "SELECT 1"),
            lambda: sqlite3.Connection.executemany(db, "SELECT ?", ((1,),)),
            lambda: sqlite3.Connection.executescript(db, "SELECT 1;"),
            lambda: sqlite3.Connection.cursor(db),
            lambda: sqlite3.Connection.commit(db),
            lambda: sqlite3.Connection.rollback(db),
            lambda: sqlite3.Connection.close(db),
            lambda: sqlite3.Connection.set_authorizer(db, None),
        )
        cursor_calls = (
            lambda: sqlite3.Cursor.execute(cursor, "SELECT 1"),
            lambda: sqlite3.Cursor.executemany(cursor, "SELECT ?", ((1,),)),
            lambda: sqlite3.Cursor.executescript(cursor, "SELECT 1;"),
            lambda: sqlite3.Cursor.fetchone(cursor),
            lambda: sqlite3.Cursor.fetchall(cursor),
            lambda: sqlite3.Cursor.close(cursor),
        )
        for call in (*connection_calls, *cursor_calls):
            with pytest.raises(TypeError, match="doesn't apply"):
                call()

        assert db.execute("SELECT 1").fetchone()[0] == 1
        assert not db.in_transaction
        assert not Path(f"{db_path}-journal").exists()
        _assert_competing_writer_can_begin(db_path)
    finally:
        cursor.close()
        if db.in_transaction:
            db.rollback()
        db.close()


def test_transaction_boundary_does_not_depend_on_swallowable_trace_callback(
    tmp_path,
):
    class TraceRejectingConnection(paper_trade_monitor.MonitorSQLiteConnection):
        def set_trace_callback(self, _callback):
            raise AssertionError("trace callbacks are forbidden")

    db_path = tmp_path / "paper.db"
    db = sqlite3.connect(
        db_path,
        timeout=1,
        factory=TraceRejectingConnection,
    )
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("CREATE TABLE evidence (value TEXT)")
    db.commit()
    entered_quote = threading.Event()
    release_quote = threading.Event()
    quote_states = []
    worker_errors = []
    try:
        with _monitor_transaction_guard_scope(db):
            coordinator = paper_trade_monitor._MONITOR_TRANSACTION_COORDINATOR
            blocking_call = _monitor_blocking_call_for_db(db)
            cursor = db.execute("SELECT 1")
            assert isinstance(cursor, paper_trade_monitor.MonitorSQLiteCursor)

            def quote():
                quote_states.append(("entered", db.in_transaction))
                entered_quote.set()
                assert release_quote.wait(timeout=2)
                quote_states.append(("before_return", db.in_transaction))
                return "quoted"

            def worker():
                try:
                    assert blocking_call("trace_failure_quote", quote) == "quoted"
                except BaseException as exc:
                    worker_errors.append(exc)

            thread = threading.Thread(target=worker)
            thread.start()
            assert entered_quote.wait(timeout=2)

            assert not hasattr(coordinator, "trace_transaction_boundary")
            timer = threading.Timer(0.05, release_quote.set)
            timer.start()

            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute("INSERT INTO evidence VALUES ('must-rollback')")
            timer.join(timeout=1)
            thread.join(timeout=2)

            assert not thread.is_alive()
            assert worker_errors == []
            assert quote_states == [
                ("entered", False),
                ("before_return", False),
            ]
            assert db.in_transaction
            db.rollback()
            assert not Path(f"{db_path}-journal").exists()
            assert db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 0
            _assert_competing_writer_can_begin(db_path)
    finally:
        _clear_monitor_supervisor_restart_request()
        paper_trade_monitor.SHUTDOWN_REQUESTED.clear()
        if db.in_transaction:
            db.rollback()
        db.close()


def test_sleep_boundary_waits_for_owner_transaction_started_after_wait(tmp_path):
    db_path = tmp_path / "paper.db"
    db = _open_monitor_delete_journal_db(db_path)
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

            # The interval wait itself owns no database state. If the owner
            # starts a transaction after the wait begins, the waiter must stay
            # behind that transaction lease and resume only after commit.
            db.execute("BEGIN IMMEDIATE")
            db.execute("INSERT INTO evidence VALUES ('opened-after-wait-start')")
            threading.Event().wait(0.15)
            assert thread.is_alive()
            assert db.in_transaction
            assert Path(f"{db_path}-journal").exists()

            db.commit()
            thread.join(timeout=2)
            assert not thread.is_alive()
            assert worker_errors == []
            assert not db.in_transaction
            assert not Path(f"{db_path}-journal").exists()
            assert db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 1
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

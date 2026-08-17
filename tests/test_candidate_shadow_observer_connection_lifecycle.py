import importlib.util
from pathlib import Path
import sqlite3
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "candidate_shadow_observer.py"


def load_module():
    scripts_dir = str(SCRIPT_PATH.parent)
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    spec = importlib.util.spec_from_file_location("candidate_shadow_observer_under_test", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def assert_connection_closed(connection):
    try:
        connection.execute("SELECT 1")
    except sqlite3.ProgrammingError as exc:
        assert "closed" in str(exc).lower()
        return
    raise AssertionError("SQLite connection remained open after run_once")


def test_run_once_closes_all_opened_connections(monkeypatch, tmp_path):
    observer = load_module()
    opened = []
    real_open = observer.open_sqlite

    def tracking_open(path, label):
        connection = real_open(path, label)
        opened.append(connection)
        return connection

    monkeypatch.setattr(observer, "open_sqlite", tracking_open)
    monkeypatch.setattr(observer, "load_registry", lambda _path: ({}, {}))
    monkeypatch.setattr(observer, "build_candidate_catalog", lambda _modes: [])
    monkeypatch.setattr(observer, "load_signals", lambda *_args: [])

    args = type(
        "Args",
        (),
        {
            "registry": str(tmp_path / "registry.json"),
            "signal_db": str(tmp_path / "signals.db"),
            "out_db": str(tmp_path / "paper.db"),
            "kline_db": str(tmp_path / "kline.db"),
            "limit": 10,
            "since_id": None,
            "maturing_kline_recheck_limit": 0,
            "kline_fallback_enabled": False,
            "kline_fallback_max_fetches": 0,
            "kline_fallback_cooldown_sec": 900,
            "kline_refetch_target_bars": 5,
            "kline_refetch_max_signal_age_sec": 3600,
        },
    )()

    observer.run_once(args)

    assert len(opened) == 3
    for connection in opened:
        assert_connection_closed(connection)


def test_run_once_closes_signal_connection_when_out_db_open_fails(monkeypatch, tmp_path):
    observer = load_module()
    opened = []
    real_open = observer.open_sqlite

    def fail_second_open(path, label):
        if label == "out_db":
            raise RuntimeError("synthetic out-db failure")
        connection = real_open(path, label)
        opened.append(connection)
        return connection

    monkeypatch.setattr(observer, "open_sqlite", fail_second_open)
    monkeypatch.setattr(observer, "load_registry", lambda _path: ({}, {}))
    monkeypatch.setattr(observer, "build_candidate_catalog", lambda _modes: [])
    args = type(
        "Args",
        (),
        {
            "registry": str(tmp_path / "registry.json"),
            "signal_db": str(tmp_path / "signals.db"),
            "out_db": str(tmp_path / "paper.db"),
        },
    )()

    try:
        observer.run_once(args)
    except RuntimeError as exc:
        assert "synthetic out-db failure" in str(exc)
    else:
        raise AssertionError("run_once unexpectedly succeeded")

    assert len(opened) == 1
    assert_connection_closed(opened[0])


def test_open_sqlite_closes_out_db_connection_when_configuration_fails(monkeypatch, tmp_path):
    observer = load_module()
    db_path = tmp_path / "paper.db"
    real_connect = sqlite3.connect
    setup = real_connect(db_path)
    setup.execute("CREATE TABLE evidence (value TEXT)")
    setup.commit()
    setup.close()

    opened = []

    def tracking_connect(*args, **kwargs):
        connection = real_connect(*args, **kwargs)
        opened.append(connection)
        return connection

    def fail_configuration(connection):
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("INSERT INTO evidence VALUES ('uncommitted')")
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(observer.sqlite3, "connect", tracking_connect)
    monkeypatch.setattr(observer, "configure_paper_sqlite_connection", fail_configuration)

    try:
        observer.open_sqlite(db_path, "out_db")
    except RuntimeError as exc:
        assert "out_db_connect_error" in str(exc)
        assert "database is locked" in str(exc)
    else:
        raise AssertionError("open_sqlite unexpectedly succeeded")

    assert len(opened) == 1
    assert_connection_closed(opened[0])
    assert not Path(f"{db_path}-journal").exists()

    follow_up = real_connect(db_path, timeout=1)
    try:
        follow_up.execute("BEGIN IMMEDIATE")
        assert follow_up.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 0
        follow_up.rollback()
    finally:
        follow_up.close()


def test_open_sqlite_rolls_back_and_native_closes_when_close_override_fails(monkeypatch, tmp_path):
    observer = load_module()
    db_path = tmp_path / "paper.db"
    real_connect = sqlite3.connect
    setup = real_connect(db_path)
    setup.execute("PRAGMA journal_mode=DELETE")
    setup.execute("CREATE TABLE evidence (value TEXT)")
    setup.commit()
    setup.close()

    class FailingCloseConnection(sqlite3.Connection):
        close_calls = 0

        def close(self):
            self.close_calls += 1
            raise sqlite3.OperationalError("injected close failure")

    opened = []

    def tracking_connect(*args, **kwargs):
        connection = real_connect(*args, **kwargs, factory=FailingCloseConnection)
        opened.append(connection)
        return connection

    def fail_configuration(connection):
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("INSERT INTO evidence VALUES ('uncommitted')")
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(observer.sqlite3, "connect", tracking_connect)
    monkeypatch.setattr(observer, "configure_paper_sqlite_connection", fail_configuration)

    try:
        observer.open_sqlite(db_path, "out_db")
    except RuntimeError as exc:
        assert "database is locked" in str(exc)
        assert exc.__cause__ is not None
        assert any("close=OperationalError" in note for note in exc.__cause__.__notes__)
    else:
        raise AssertionError("open_sqlite unexpectedly succeeded")

    assert opened[0].close_calls == 1
    assert_connection_closed(opened[0])
    assert not Path(f"{db_path}-journal").exists()

    follow_up = real_connect(db_path, timeout=1)
    try:
        follow_up.execute("BEGIN IMMEDIATE")
        assert follow_up.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 0
        follow_up.rollback()
    finally:
        follow_up.close()


def test_open_sqlite_preserves_non_sqlite_root_when_cleanup_fails(monkeypatch, tmp_path):
    observer = load_module()
    db_path = tmp_path / "paper.db"
    real_connect = sqlite3.connect
    setup = real_connect(db_path)
    setup.execute("CREATE TABLE evidence (value TEXT)")
    setup.commit()
    setup.close()

    class FailingCloseConnection(sqlite3.Connection):
        def close(self):
            raise RuntimeError("injected non-sqlite close failure")

    opened = []
    root_error = LookupError("configuration non-sqlite root")

    def tracking_connect(*args, **kwargs):
        connection = real_connect(*args, **kwargs, factory=FailingCloseConnection)
        opened.append(connection)
        return connection

    def fail_configuration(connection):
        connection.execute("BEGIN IMMEDIATE")
        raise root_error

    monkeypatch.setattr(observer.sqlite3, "connect", tracking_connect)
    monkeypatch.setattr(observer, "configure_paper_sqlite_connection", fail_configuration)

    try:
        observer.open_sqlite(db_path, "out_db")
    except LookupError as exc:
        assert exc is root_error
        assert any("close=RuntimeError" in note for note in exc.__notes__)
    else:
        raise AssertionError("open_sqlite unexpectedly succeeded")

    assert_connection_closed(opened[0])
    assert not Path(f"{db_path}-journal").exists()

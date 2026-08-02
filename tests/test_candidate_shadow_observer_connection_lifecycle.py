import importlib.util
from pathlib import Path
import sqlite3


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "candidate_shadow_observer.py"


def load_module():
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

import json
from pathlib import Path
import sqlite3
import sys
import threading
import time
from collections import namedtuple

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import cross_db_evaluator_snapshot as snapshot_module  # noqa: E402
from cross_db_evaluator_snapshot import (  # noqa: E402
    build_snapshot_bundle,
    cleanup_interrupted_partials,
)


@pytest.fixture(autouse=True)
def snapshot_commit(monkeypatch):
    monkeypatch.setenv("ZEABUR_GIT_COMMIT_SHA", "a" * 40)


def create_sources(root):
    definitions = {
        "signal": "CREATE TABLE premium_signals(id INTEGER, source_message_ts INTEGER)",
        "paper": (
            "CREATE TABLE candidate_shadow_observations(signal_id INTEGER, observed_at INTEGER);"
            "CREATE TABLE candidate_shadow_virtual_trades(signal_id INTEGER, observed_at INTEGER);"
            "CREATE TABLE paper_decision_events(id INTEGER, event_ts INTEGER);"
            "CREATE TABLE a_class_decision_events(id INTEGER, event_ts INTEGER);"
            "CREATE TABLE a_class_mode_runtime_state(id INTEGER, updated_at INTEGER);"
            "CREATE TABLE paper_trades(id INTEGER, entry_time INTEGER);"
            "CREATE TABLE opportunity_events(id INTEGER, event_ts INTEGER)"
        ),
        "raw": "CREATE TABLE raw_signal_outcomes(id INTEGER, signal_id INTEGER, updated_at INTEGER)",
        "kline": "CREATE TABLE kline_1m(token_ca TEXT, timestamp INTEGER)",
    }
    sources = {}
    for name, ddl in definitions.items():
        path = root / f"{name}.db"
        connection = sqlite3.connect(path)
        connection.executescript(ddl)
        connection.commit()
        connection.close()
        sources[name] = str(path)
    return sources


def test_cross_db_snapshot_publishes_only_after_full_validation(tmp_path):
    sources = create_sources(tmp_path)
    out = tmp_path / "evidence"

    report = build_snapshot_bundle(
        sources=sources,
        out_root=str(out),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        snapshot_id="20260101T000000Z-1234abcd",
    )

    assert report["accepted"] is True
    assert report["quick_checks_passed"] is True
    assert report["cross_database_time_skew_passed"] is True
    assert report["read_views_pinned_before_copy"] is True
    latest_pin = max(
        row["pinned_read_view"]["pinned_finished_epoch"]
        for row in report["databases"].values()
    )
    earliest_copy = min(row["started_epoch"] for row in report["databases"].values())
    assert earliest_copy >= latest_pin
    assert report["source_mutation_free"] is True
    assert set(report["databases"]) == {"signal", "paper", "raw", "kline"}
    assert all(row["snapshot_sha256"] for row in report["databases"].values())
    assert (out / "current").is_symlink()
    assert json.loads((out / "current" / "manifest.json").read_text())["snapshot_id"] == report["snapshot_id"]


def test_new_snapshot_prunes_previous_only_after_publish(tmp_path):
    sources = create_sources(tmp_path)
    out = tmp_path / "evidence"
    build_snapshot_bundle(
        sources=sources,
        out_root=str(out),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        snapshot_id="20260101T000000Z-1234abcd",
    )
    second = build_snapshot_bundle(
        sources=sources,
        out_root=str(out),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        snapshot_id="20260101T010000Z-abcdef12",
    )

    directories = [path.name for path in (out / "snapshots").iterdir() if path.is_dir()]
    assert directories == ["20260101T010000Z-abcdef12"]
    assert second["retention"]["keep_previous"] == 0
    assert json.loads((out / "current" / "manifest.json").read_text())["snapshot_id"] == second["snapshot_id"]


def test_selective_snapshot_does_not_copy_source_freelist_or_mutate_source(tmp_path):
    sources = create_sources(tmp_path)
    paper = Path(sources["paper"])
    connection = sqlite3.connect(paper)
    connection.execute("ALTER TABLE candidate_shadow_observations ADD COLUMN payload BLOB")
    connection.executemany(
        "INSERT INTO candidate_shadow_observations(signal_id, observed_at, payload) VALUES (?,?,?)",
        [(index, index, b"x" * 10000) for index in range(1000)],
    )
    connection.commit()
    connection.execute("DELETE FROM candidate_shadow_observations WHERE signal_id < 900")
    connection.commit()
    connection.close()
    source_size = paper.stat().st_size

    report = build_snapshot_bundle(
        sources=sources,
        out_root=str(tmp_path / "evidence"),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        snapshot_id="20260101T000000Z-1234abcd",
    )

    snapshot_size = report["databases"]["paper"]["snapshot_size_bytes"]
    assert paper.stat().st_size == source_size
    assert snapshot_size < source_size / 2
    assert report["databases"]["paper"]["source_mutated_by_snapshot_process"] is False
    assert report["databases"]["paper"]["temporary_full_backup_size_bytes"] == 0
    assert report["bounded_selective_snapshot"] is True


def test_selective_snapshot_applies_one_bounded_upper_time_to_all_databases(tmp_path):
    sources = create_sources(tmp_path)
    now = int(time.time())
    signal = sqlite3.connect(sources["signal"])
    signal.execute("ALTER TABLE premium_signals ADD COLUMN timestamp INTEGER")
    signal.executemany(
        "INSERT INTO premium_signals(id, source_message_ts, timestamp) VALUES (?, ?, ?)",
        [
            (1, now - 60, None),
            (2, now - 40 * 86400, None),
            (3, now + 3600, None),
        ],
    )
    signal.commit()
    signal.close()
    paper = sqlite3.connect(sources["paper"])
    paper.executemany(
        "INSERT INTO candidate_shadow_observations(signal_id, observed_at) VALUES (?, ?)",
        [(1, now - 60), (2, now - 5 * 86400), (3, now + 3600)],
    )
    paper.commit()
    paper.close()

    report = build_snapshot_bundle(
        sources=sources,
        out_root=str(tmp_path / "evidence"),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        max_output_gib=0.1,
        review_history_hours=96,
        long_history_hours=24 * 35,
        snapshot_id="20260101T000000Z-1234abcd",
    )

    signal_snapshot = sqlite3.connect(report["databases"]["signal"]["snapshot_path"])
    paper_snapshot = sqlite3.connect(report["databases"]["paper"]["snapshot_path"])
    try:
        assert signal_snapshot.execute("SELECT id FROM premium_signals").fetchall() == [(1,)]
        assert paper_snapshot.execute(
            "SELECT signal_id FROM candidate_shadow_observations"
        ).fetchall() == [(1,)]
    finally:
        signal_snapshot.close()
        paper_snapshot.close()
    assert report["selection_upper_bounds_consistent"] is True
    assert {
        row["selection_upper_epoch"] for row in report["databases"].values()
    } == {report["snapshot_ts"]}
    assert report["databases"]["signal"]["selected_tables"]["premium_signals"]["rows_copied"] == 1
    assert report["databases"]["paper"]["selected_tables"]["candidate_shadow_observations"]["rows_copied"] == 1


def test_writer_commit_after_all_read_views_are_pinned_is_not_visible(
    tmp_path, monkeypatch
):
    sources = create_sources(tmp_path)
    now = int(time.time())
    signal = sqlite3.connect(sources["signal"])
    signal.execute("PRAGMA journal_mode=WAL")
    signal.execute(
        "INSERT INTO premium_signals(id, source_message_ts) VALUES (?, ?)",
        (1, now - 60),
    )
    signal.commit()
    signal.close()
    original_snapshot_one = snapshot_module.snapshot_one
    injection_lock = threading.Lock()
    injected = False

    def snapshot_one_with_concurrent_commit(source, *args, **kwargs):
        nonlocal injected
        if Path(source) == Path(sources["signal"]):
            with injection_lock:
                if not injected:
                    writer = sqlite3.connect(sources["signal"])
                    writer.execute(
                        "INSERT INTO premium_signals(id, source_message_ts) VALUES (?, ?)",
                        (2, now - 30),
                    )
                    writer.commit()
                    writer.close()
                    injected = True
        return original_snapshot_one(source, *args, **kwargs)

    monkeypatch.setattr(snapshot_module, "snapshot_one", snapshot_one_with_concurrent_commit)
    report = build_snapshot_bundle(
        sources=sources,
        out_root=str(tmp_path / "evidence"),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        max_output_gib=0.1,
        snapshot_id="20260101T000000Z-1234abcd",
    )

    source = sqlite3.connect(sources["signal"])
    snapshot = sqlite3.connect(report["databases"]["signal"]["snapshot_path"])
    try:
        assert source.execute("SELECT id FROM premium_signals ORDER BY id").fetchall() == [
            (1,), (2,)
        ]
        assert snapshot.execute("SELECT id FROM premium_signals ORDER BY id").fetchall() == [
            (1,)
        ]
    finally:
        source.close()
        snapshot.close()


def test_time_bearing_small_tables_also_exclude_future_rows(tmp_path):
    sources = create_sources(tmp_path)
    now = int(time.time())
    paper = sqlite3.connect(sources["paper"])
    paper.executemany(
        "INSERT INTO paper_trades(id, entry_time) VALUES (?, ?)",
        [(1, now - 60), (2, now + 3600)],
    )
    paper.commit()
    paper.close()

    report = build_snapshot_bundle(
        sources=sources,
        out_root=str(tmp_path / "evidence"),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        max_output_gib=0.1,
        snapshot_id="20260101T000000Z-1234abcd",
    )

    snapshot = sqlite3.connect(report["databases"]["paper"]["snapshot_path"])
    try:
        assert snapshot.execute("SELECT id FROM paper_trades ORDER BY id").fetchall() == [(1,)]
    finally:
        snapshot.close()
    selection = report["databases"]["paper"]["selected_tables"]["paper_trades"]
    assert selection["selection_mode"] == "through_upper"
    assert selection["future_bound_enforced"] is True


def test_secondary_future_timestamps_exclude_incomplete_as_of_rows(tmp_path):
    sources = create_sources(tmp_path)
    now = int(time.time())
    paper = sqlite3.connect(sources["paper"])
    paper.execute("ALTER TABLE paper_trades ADD COLUMN exit_ts INTEGER")
    paper.execute("ALTER TABLE candidate_shadow_virtual_trades ADD COLUMN exit_ts INTEGER")
    paper.execute(
        "INSERT INTO paper_trades(id, entry_time, exit_ts) VALUES (?, ?, ?)",
        (1, now - 60, now + 3600),
    )
    paper.execute(
        "INSERT INTO candidate_shadow_virtual_trades(signal_id, observed_at, exit_ts) "
        "VALUES (?, ?, ?)",
        (1, now - 60, now + 3600),
    )
    paper.commit()
    paper.close()

    report = build_snapshot_bundle(
        sources=sources,
        out_root=str(tmp_path / "evidence"),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        max_output_gib=0.1,
        snapshot_id="20260101T000000Z-1234abcd",
    )

    snapshot = sqlite3.connect(report["databases"]["paper"]["snapshot_path"])
    try:
        assert snapshot.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0] == 0
        assert snapshot.execute(
            "SELECT COUNT(*) FROM candidate_shadow_virtual_trades"
        ).fetchone()[0] == 0
    finally:
        snapshot.close()


def test_failed_bundle_does_not_replace_current(tmp_path):
    sources = create_sources(tmp_path)
    out = tmp_path / "evidence"
    first = build_snapshot_bundle(
        sources=sources,
        out_root=str(out),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        snapshot_id="20260101T000000Z-1234abcd",
    )
    broken_raw = Path(sources["raw"])
    broken_raw.unlink()
    sqlite3.connect(broken_raw).close()

    with pytest.raises(RuntimeError, match="missing required tables"):
        build_snapshot_bundle(
            sources=sources,
            out_root=str(out),
            repo_root=str(ROOT),
            max_skew_sec=30,
            min_free_after_gib=0,
            snapshot_id="20260101T010000Z-abcdef12",
        )

    current = json.loads((out / "current" / "manifest.json").read_text())
    assert current["snapshot_id"] == first["snapshot_id"]
    assert not (out / "snapshots" / ".20260101T010000Z-abcdef12.partial").exists()


def test_late_status_write_failure_rolls_back_current_and_preserves_previous(tmp_path, monkeypatch):
    sources = create_sources(tmp_path)
    out = tmp_path / "evidence"
    first = build_snapshot_bundle(
        sources=sources,
        out_root=str(out),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        max_output_gib=0.1,
        snapshot_id="20260101T000000Z-1234abcd",
    )
    original_atomic_json = snapshot_module.atomic_json
    failed = False

    def fail_first_new_latest(path, payload):
        nonlocal failed
        if (
            not failed
            and Path(path) == out / "latest_manifest.json"
            and payload.get("snapshot_id") == "20260101T010000Z-abcdef12"
        ):
            failed = True
            raise OSError("injected_latest_manifest_failure")
        return original_atomic_json(path, payload)

    monkeypatch.setattr(snapshot_module, "atomic_json", fail_first_new_latest)
    with pytest.raises(OSError, match="injected_latest_manifest_failure"):
        build_snapshot_bundle(
            sources=sources,
            out_root=str(out),
            repo_root=str(ROOT),
            max_skew_sec=30,
            min_free_after_gib=0,
            max_output_gib=0.1,
            snapshot_id="20260101T010000Z-abcdef12",
        )

    current = json.loads((out / "current" / "manifest.json").read_text())
    latest = json.loads((out / "latest_manifest.json").read_text())
    assert current["snapshot_id"] == first["snapshot_id"]
    assert latest["snapshot_id"] == first["snapshot_id"]
    assert (out / "snapshots" / first["snapshot_id"]).is_dir()
    assert not (out / "snapshots" / "20260101T010000Z-abcdef12").exists()


def test_bundle_output_accounting_includes_manifest_and_has_no_side_files(tmp_path):
    sources = create_sources(tmp_path)
    out = tmp_path / "evidence"
    report = build_snapshot_bundle(
        sources=sources,
        out_root=str(out),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        max_output_gib=0.1,
        snapshot_id="20260101T000000Z-1234abcd",
    )

    snapshot_dir = (out / "current").resolve()
    files = [item for item in snapshot_dir.iterdir() if item.is_file()]
    assert {item.name for item in files} == {
        "signal.db", "paper_evidence.db", "raw.db", "kline.db", "manifest.json"
    }
    assert sum(item.stat().st_size for item in files) == report["output_size_bytes"]
    assert report["output_size_bytes"] <= report["output_cap_bytes"]


def test_interrupted_partial_cleanup_is_strictly_scoped(tmp_path):
    snapshots = tmp_path / "evidence" / "snapshots"
    interrupted = snapshots / ".20260101T010000Z-abcdef12.partial"
    protected = snapshots / "20260101T000000Z-1234abcd"
    unrelated = snapshots / ".manual.partial"
    for path in (interrupted, protected, unrelated):
        path.mkdir(parents=True)
        (path / "data").write_text("keep-or-remove", encoding="utf-8")

    removed = cleanup_interrupted_partials(tmp_path / "evidence")

    assert removed == [str(interrupted)]
    assert not interrupted.exists()
    assert protected.exists()
    assert unrelated.exists()


def test_missing_required_watermark_rejects_bundle(tmp_path):
    sources = create_sources(tmp_path)
    signal = Path(sources["signal"])
    signal.unlink()
    connection = sqlite3.connect(signal)
    connection.execute("CREATE TABLE premium_signals(untracked_value TEXT)")
    connection.commit()
    connection.close()
    out = tmp_path / "evidence"

    with pytest.raises(RuntimeError, match="required watermarks"):
        build_snapshot_bundle(
            sources=sources,
            out_root=str(out),
            repo_root=str(ROOT),
            max_skew_sec=30,
            min_free_after_gib=0,
            snapshot_id="20260101T000000Z-1234abcd",
        )

    assert not (out / "current").exists()


def test_disk_shortage_fails_closed_without_replacing_current(tmp_path, monkeypatch):
    sources = create_sources(tmp_path)
    out = tmp_path / "evidence"
    first = build_snapshot_bundle(
        sources=sources,
        out_root=str(out),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        max_output_gib=0.1,
        snapshot_id="20260101T000000Z-1234abcd",
    )
    DiskUsage = namedtuple("DiskUsage", "total used free")
    monkeypatch.setattr(
        snapshot_module.shutil,
        "disk_usage",
        lambda _path: DiskUsage(1024**3, 1024**3 - 1024, 1024),
    )

    with pytest.raises(RuntimeError, match="insufficient disk"):
        build_snapshot_bundle(
            sources=sources,
            out_root=str(out),
            repo_root=str(ROOT),
            max_skew_sec=30,
            min_free_after_gib=0,
            max_output_gib=0.1,
            snapshot_id="20260101T010000Z-abcdef12",
        )

    current = json.loads((out / "current" / "manifest.json").read_text())
    assert current["snapshot_id"] == first["snapshot_id"]
    assert not (out / "snapshots" / ".20260101T010000Z-abcdef12.partial").exists()


def test_output_cap_breach_fails_closed_without_replacing_current(tmp_path):
    sources = create_sources(tmp_path)
    out = tmp_path / "evidence"
    first = build_snapshot_bundle(
        sources=sources,
        out_root=str(out),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        max_output_gib=0.1,
        snapshot_id="20260101T000000Z-1234abcd",
    )
    paper = sqlite3.connect(sources["paper"])
    paper.execute("ALTER TABLE candidate_shadow_observations ADD COLUMN payload BLOB")
    paper.execute(
        "INSERT INTO candidate_shadow_observations(signal_id, observed_at, payload) VALUES (?,?,?)",
        (1, int(time.time()), b"x" * 256_000),
    )
    paper.commit()
    paper.close()

    with pytest.raises(RuntimeError, match="concurrent evaluator snapshot failed"):
        build_snapshot_bundle(
            sources=sources,
            out_root=str(out),
            repo_root=str(ROOT),
            max_skew_sec=30,
            min_free_after_gib=0,
            max_output_gib=0.0001,
            snapshot_id="20260101T010000Z-abcdef12",
        )

    current = json.loads((out / "current" / "manifest.json").read_text())
    assert current["snapshot_id"] == first["snapshot_id"]
    assert not (out / "snapshots" / ".20260101T010000Z-abcdef12.partial").exists()


def test_locked_source_fails_closed_without_replacing_current(tmp_path):
    sources = create_sources(tmp_path)
    out = tmp_path / "evidence"
    first = build_snapshot_bundle(
        sources=sources,
        out_root=str(out),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        max_output_gib=0.1,
        snapshot_id="20260101T000000Z-1234abcd",
    )
    lock = sqlite3.connect(sources["paper"])
    lock.execute("BEGIN EXCLUSIVE")
    try:
        with pytest.raises(RuntimeError, match="snapshot source inspection failed"):
            build_snapshot_bundle(
                sources=sources,
                out_root=str(out),
                repo_root=str(ROOT),
                max_skew_sec=30,
                min_free_after_gib=0,
                max_output_gib=0.1,
                source_busy_timeout_ms=10,
                snapshot_id="20260101T010000Z-abcdef12",
            )
    finally:
        lock.rollback()
        lock.close()
    current = json.loads((out / "current" / "manifest.json").read_text())
    assert current["snapshot_id"] == first["snapshot_id"]
    assert not (out / "snapshots" / ".20260101T010000Z-abcdef12.partial").exists()


def test_duplicate_snapshot_id_is_rejected_without_partial(tmp_path):
    sources = create_sources(tmp_path)
    out = tmp_path / "evidence"
    build_snapshot_bundle(
        sources=sources,
        out_root=str(out),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        max_output_gib=0.1,
        snapshot_id="20260101T000000Z-1234abcd",
    )

    with pytest.raises(FileExistsError):
        build_snapshot_bundle(
            sources=sources,
            out_root=str(out),
            repo_root=str(ROOT),
            max_skew_sec=30,
            min_free_after_gib=0,
            max_output_gib=0.1,
            snapshot_id="20260101T000000Z-1234abcd",
        )

    assert not (out / "snapshots" / ".20260101T000000Z-1234abcd.partial").exists()


def test_missing_git_commit_rejects_bundle(tmp_path, monkeypatch):
    sources = create_sources(tmp_path)
    out = tmp_path / "evidence"
    monkeypatch.delenv("ZEABUR_GIT_COMMIT_SHA", raising=False)
    monkeypatch.setattr(snapshot_module, "detected_commit", lambda _repo_root: None)

    with pytest.raises(RuntimeError, match="snapshot acceptance failed"):
        build_snapshot_bundle(
            sources=sources,
            out_root=str(out),
            repo_root=str(ROOT),
            max_skew_sec=30,
            min_free_after_gib=0,
            snapshot_id="20260101T000000Z-1234abcd",
        )

    assert not (out / "current").exists()

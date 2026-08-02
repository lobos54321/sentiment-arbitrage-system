import json
from pathlib import Path
import sqlite3
import sys

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


def test_snapshot_compacts_source_freelist_without_mutating_source(tmp_path):
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

import json
from pathlib import Path
import sqlite3
import sys
import threading
import time
from collections import namedtuple
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import cross_db_evaluator_snapshot as snapshot_module  # noqa: E402
from agent_capture_discovery_loop import sqlite_has_table  # noqa: E402
from cross_db_evaluator_snapshot import (  # noqa: E402
    DATABASE_SPECS,
    build_snapshot_bundle,
    candidate_observation_projection_supported,
    cleanup_interrupted_partials,
    database_output_budget_plan,
    selection_for_table,
    source_table_reference,
    static_database_output_budgets,
)


@pytest.fixture(autouse=True)
def snapshot_commit(monkeypatch):
    monkeypatch.setenv("ZEABUR_GIT_COMMIT_SHA", "a" * 40)


def create_sources(root):
    definitions = {
        "signal": "CREATE TABLE premium_signals(id INTEGER, source_message_ts INTEGER)",
        "paper": (
            "CREATE TABLE candidate_shadow_observations("
            "id INTEGER PRIMARY KEY, signal_id INTEGER, candidate_id TEXT, "
            "observed_at INTEGER, payload_json TEXT);"
            "CREATE INDEX idx_candidate_shadow_obs_observed "
            "ON candidate_shadow_observations(observed_at);"
            "CREATE TABLE candidate_shadow_virtual_trades(signal_id INTEGER, observed_at INTEGER);"
            "CREATE INDEX idx_candidate_shadow_virtual_observed "
            "ON candidate_shadow_virtual_trades(observed_at);"
            "CREATE TABLE paper_decision_events(id INTEGER, event_ts INTEGER);"
            "CREATE INDEX idx_pde_event_ts ON paper_decision_events(event_ts);"
            "CREATE TABLE a_class_decision_events(id INTEGER, event_ts INTEGER);"
            "CREATE INDEX idx_a_class_decision_recent ON a_class_decision_events(event_ts);"
            "CREATE TABLE a_class_mode_runtime_state(id INTEGER, updated_at INTEGER);"
            "CREATE TABLE paper_trades(id INTEGER, entry_time INTEGER);"
            "CREATE TABLE opportunity_events(id INTEGER, event_ts INTEGER);"
            "CREATE INDEX idx_opportunity_events_recent ON opportunity_events(event_ts);"
            "CREATE TABLE opportunity_event_path_samples("
            "id INTEGER PRIMARY KEY, opportunity_key TEXT, sample_ts REAL, "
            "raw_payload_json TEXT, created_at REAL, updated_at REAL);"
            "CREATE INDEX idx_opportunity_path_samples_key_ts "
            "ON opportunity_event_path_samples(opportunity_key, sample_ts)"
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


def test_parallel_stage_required_flags_match_selection_contract():
    for table, config in snapshot_module.PARALLEL_PAPER_STAGE_CONFIGS.items():
        assert config["required"] is bool(
            snapshot_module.DATABASE_SPECS["paper"]["tables"][table].get(
                "required"
            )
        )


def test_bundled_self_test_uses_projection_compatible_schema(capsys):
    snapshot_module.self_test()
    assert "SELF_TEST_PASS cross_db_evaluator_snapshot" in capsys.readouterr().out


def test_source_page_inspection_failure_is_component_scoped(tmp_path, monkeypatch):
    sources = create_sources(tmp_path)

    def fail_page_stats(_connection, source):
        if Path(source) == Path(sources["paper"]):
            raise sqlite3.OperationalError("database is busy at /secret/path")
        return {
            "source_size_bytes": 4096,
            "page_size": 4096,
            "page_count": 1,
            "freelist_count": 0,
            "estimated_compact_bytes": 4096,
        }

    monkeypatch.setattr(snapshot_module, "source_page_stats", fail_page_stats)
    with pytest.raises(snapshot_module.ConcurrentSnapshotError) as raised:
        snapshot_module.inspect_source_page_reports(
            {name: Path(path) for name, path in sources.items()},
            busy_timeout_ms=1,
        )

    assert raised.value.errors == {
        "paper": {
            "error_code": "snapshot_source_inspection_failed",
            "error_type": "OperationalError",
            "stage": "source_page_stats",
        }
    }
    assert "/secret/path" not in str(raised.value)


def test_disk_preflight_sizes_stage_from_residual_space_not_output_fraction(
    tmp_path,
    monkeypatch,
):
    gib = 1024**3
    free_bytes = 40 * gib
    monkeypatch.setattr(
        snapshot_module.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(total=80 * gib, used=40 * gib, free=free_bytes),
    )

    report = snapshot_module.disk_preflight(
        tmp_path,
        min_free_after_gib=5,
        max_output_gib=10,
    )

    expected_total_stage = 25 * gib
    minimum_total = (
        snapshot_module.MIN_CANDIDATE_STAGE_CAP_BYTES
        + len(snapshot_module.PARALLEL_PAPER_STAGE_TABLES)
        * snapshot_module.MIN_PARALLEL_PAPER_STAGE_CAP_BYTES
    )
    expected_candidate = (
        snapshot_module.MIN_CANDIDATE_STAGE_CAP_BYTES
        + int(
            (expected_total_stage - minimum_total)
            * snapshot_module.CANDIDATE_STAGE_RESIDUAL_SHARE
        )
        // 4096
        * 4096
    )
    residual_after_minimums = expected_total_stage - minimum_total
    expected_parallel = {}
    allocated = expected_candidate
    for table in snapshot_module.PARALLEL_PAPER_STAGE_TABLES[:-1]:
        share = snapshot_module.PARALLEL_PAPER_STAGE_CONFIGS[table]["residual_share"]
        expected_parallel[table] = (
            snapshot_module.MIN_PARALLEL_PAPER_STAGE_CAP_BYTES
            + int(residual_after_minimums * share) // 4096 * 4096
        )
        allocated += expected_parallel[table]
    expected_parallel[snapshot_module.PARALLEL_PAPER_STAGE_TABLES[-1]] = (
        expected_total_stage - allocated
    )
    assert report["accepted"] is True
    assert report["candidate_stage_budget_mode"] == "shared_residual_disk_after_output_and_reserve"
    assert report["candidate_stage_minimum_cap_bytes"] == 12288
    assert report["paper_decision_stage_minimum_cap_bytes"] == 12288
    assert report["temporary_stage_total_cap_bytes"] == expected_total_stage
    assert report["temporary_candidate_stage_cap_bytes"] == expected_candidate
    assert report["temporary_parallel_paper_stage_cap_bytes"] == expected_parallel
    assert report["configured_parallel_paper_stage_tables"] == list(
        snapshot_module.PARALLEL_PAPER_STAGE_TABLES
    )
    assert report["parallel_paper_stage_tables"] == list(
        snapshot_module.PARALLEL_PAPER_STAGE_TABLES
    )
    assert report["omitted_optional_parallel_paper_stage_tables"] == []
    assert report["parallel_paper_stage_active_weight_total"] == pytest.approx(1.0)
    assert report["candidate_stage_normalized_share"] == pytest.approx(
        snapshot_module.CANDIDATE_STAGE_RESIDUAL_SHARE
    )
    assert report["parallel_paper_stage_normalized_shares"] == pytest.approx(
        report["parallel_paper_stage_residual_shares"]
    )
    assert report["temporary_paper_decision_stage_cap_bytes"] == expected_parallel[
        "paper_decision_events"
    ]
    assert report["temporary_candidate_stage_cap_bytes"] > 2 * gib
    assert report["temporary_parallel_paper_stage_cap_bytes"][
        "paper_decision_events"
    ] > 8 * gib
    assert report["temporary_parallel_paper_stage_cap_bytes"][
        "a_class_decision_events"
    ] > 6 * gib
    assert report["temporary_parallel_paper_stage_cap_bytes"][
        "opportunity_events"
    ] > 1 * gib
    assert report["temporary_parallel_paper_stage_cap_bytes"][
        "opportunity_event_path_samples"
    ] > 4 * gib
    assert report["estimated_peak_working_bytes"] == 35 * gib
    assert report["estimated_free_at_peak_bytes"] == 5 * gib
    assert report["estimated_free_at_peak_bytes"] == report["required_reserve_bytes"]


def test_disk_preflight_fails_when_residual_stage_capacity_is_below_one_page(
    tmp_path,
    monkeypatch,
):
    gib = 1024**3
    free_bytes = 15 * gib + 12287
    monkeypatch.setattr(
        snapshot_module.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(total=80 * gib, used=80 * gib - free_bytes, free=free_bytes),
    )

    report = snapshot_module.disk_preflight(
        tmp_path,
        min_free_after_gib=5,
        max_output_gib=10,
    )

    assert report["temporary_stage_total_cap_bytes"] == 12287
    assert report["temporary_candidate_stage_cap_bytes"] == 12287
    assert report["temporary_paper_decision_stage_cap_bytes"] == 0
    assert report["accepted"] is False


def test_minimum_stage_capacity_supports_empty_table_and_order_index(
    tmp_path,
    monkeypatch,
):
    sources = create_sources(tmp_path)
    output_cap_bytes = int(0.1 * 1024**3)
    free_bytes = output_cap_bytes + (
        1 + len(snapshot_module.PARALLEL_PAPER_STAGE_TABLES)
    ) * 12288
    monkeypatch.setattr(
        snapshot_module.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(
            total=2 * output_cap_bytes,
            used=2 * output_cap_bytes - free_bytes,
            free=free_bytes,
        ),
    )

    report = build_snapshot_bundle(
        sources=sources,
        out_root=str(tmp_path / "minimum-stage-evidence"),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        max_output_gib=0.1,
        snapshot_id="20260101T000000Z-1234abcf",
    )

    assert report["accepted"] is True
    assert report["disk_preflight"]["temporary_candidate_stage_cap_bytes"] == 12288
    assert report["disk_preflight"]["temporary_parallel_paper_stage_cap_bytes"] == {
        table: 12288 for table in snapshot_module.PARALLEL_PAPER_STAGE_TABLES
    }
    assert report["disk_preflight"]["temporary_paper_decision_stage_cap_bytes"] == 12288
    assert report["candidate_stage_removed_before_publish"] is True
    assert report["paper_decision_parallel_stage_removed_before_publish"] is True


def test_optional_opportunity_path_stage_is_skipped_when_source_table_is_absent(
    tmp_path,
    monkeypatch,
):
    sources = create_sources(tmp_path)
    paper = sqlite3.connect(sources["paper"])
    paper.execute("DROP TABLE opportunity_event_path_samples")
    paper.commit()
    paper.close()
    output_cap_bytes = int(0.1 * 1024**3)
    active_stage_count = len(
        snapshot_module.PARALLEL_PAPER_REQUIRED_STAGE_TABLES
    )
    minimum_active_stage_bytes = (1 + active_stage_count) * 12288
    free_bytes = output_cap_bytes + minimum_active_stage_bytes
    monkeypatch.setattr(
        snapshot_module.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(
            total=2 * output_cap_bytes,
            used=2 * output_cap_bytes - free_bytes,
            free=free_bytes,
        ),
    )

    report = build_snapshot_bundle(
        sources=sources,
        out_root=str(tmp_path / "optional-path-stage-evidence"),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        max_output_gib=0.1,
        snapshot_id="20260101T000000Z-1234abca",
    )

    expected_stages = [
        table
        for table in snapshot_module.PARALLEL_PAPER_STAGE_TABLES
        if table != "opportunity_event_path_samples"
    ]
    paper_report = report["databases"]["paper"]
    optional_selection = paper_report["selected_tables"][
        "opportunity_event_path_samples"
    ]
    assert report["accepted"] is True
    assert report["parallel_paper_stage_inventory_passed"] is True
    assert report["parallel_paper_stage_tables"] == expected_stages
    assert report["parallel_paper_stage_count"] == len(expected_stages)
    assert report["pinned_read_view_count"] == 4 + len(expected_stages)
    assert paper_report["parallel_paper_stage_tables"] == expected_stages
    assert paper_report["parallel_paper_stage_count"] == len(expected_stages)
    assert set(paper_report["parallel_paper_stages"]) == set(expected_stages)
    disk = report["disk_preflight"]
    assert disk["temporary_stage_total_cap_bytes"] == minimum_active_stage_bytes
    assert disk["temporary_candidate_stage_cap_bytes"] == 12288
    assert disk["parallel_paper_stage_tables"] == expected_stages
    assert disk["configured_parallel_paper_stage_tables"] == list(
        snapshot_module.PARALLEL_PAPER_STAGE_TABLES
    )
    assert disk["omitted_optional_parallel_paper_stage_tables"] == [
        "opportunity_event_path_samples"
    ]
    assert disk["temporary_parallel_paper_stage_cap_bytes"] == {
        table: 12288 for table in expected_stages
    }
    assert "opportunity_event_path_samples" not in disk[
        "temporary_parallel_paper_stage_cap_bytes"
    ]
    assert (
        sum(disk["temporary_parallel_paper_stage_cap_bytes"].values())
        + disk["temporary_candidate_stage_cap_bytes"]
        == disk["temporary_stage_total_cap_bytes"]
    )
    assert disk["parallel_paper_stage_active_weight_total"] == pytest.approx(0.8)
    assert disk["candidate_stage_normalized_share"] == pytest.approx(0.15)
    assert disk["parallel_paper_stage_normalized_shares"] == pytest.approx(
        {
            "paper_decision_events": 0.45,
            "a_class_decision_events": 0.3125,
            "opportunity_events": 0.0875,
        }
    )
    assert optional_selection == {
        "included": False,
        "required": False,
        "reason": "optional_source_table_missing",
    }
    snapshot_dir = Path(paper_report["snapshot_path"]).parent
    assert not (snapshot_dir / ".opportunity-event-path-samples-stage.db").exists()


def test_required_parallel_stage_table_absence_fails_closed(tmp_path):
    sources = create_sources(tmp_path)
    paper = sqlite3.connect(sources["paper"])
    paper.execute("DROP TABLE opportunity_events")
    paper.commit()
    paper.close()
    out = tmp_path / "required-stage-missing-evidence"

    with pytest.raises(
        snapshot_module.ConcurrentSnapshotError,
        match="snapshot_missing_required_tables",
    ):
        build_snapshot_bundle(
            sources=sources,
            out_root=str(out),
            repo_root=str(ROOT),
            max_skew_sec=30,
            min_free_after_gib=0,
            max_output_gib=0.1,
            snapshot_id="20260101T000000Z-1234abcb",
        )

    assert not (out / "current").exists()
    assert not (out / "snapshots" / ".20260101T000000Z-1234abcb.partial").exists()


def test_parallel_stage_inventory_drift_after_preflight_fails_closed(
    tmp_path,
    monkeypatch,
):
    sources = create_sources(tmp_path)
    out = tmp_path / "parallel-stage-inventory-drift"
    drifted_inventory = tuple(
        table
        for table in snapshot_module.PARALLEL_PAPER_STAGE_TABLES
        if table != "opportunity_event_path_samples"
    )
    monkeypatch.setattr(
        snapshot_module,
        "active_parallel_paper_stage_tables",
        lambda _connection: drifted_inventory,
    )

    with pytest.raises(
        snapshot_module.ConcurrentSnapshotError,
        match="parallel_paper_stage_failed",
    ):
        build_snapshot_bundle(
            sources=sources,
            out_root=str(out),
            repo_root=str(ROOT),
            max_skew_sec=30,
            min_free_after_gib=0,
            max_output_gib=0.1,
            snapshot_id="20260101T000000Z-1234abcc",
        )

    assert not (out / "current").exists()
    assert not (out / "snapshots" / ".20260101T000000Z-1234abcc.partial").exists()


def test_parallel_paper_decision_stage_uses_fixed_4k_pages_with_large_source_pages(
    tmp_path,
):
    source = tmp_path / "large-page-paper.db"
    destination = tmp_path / "paper-decision-stage.db"
    connection = sqlite3.connect(source)
    connection.execute("PRAGMA page_size=16384")
    connection.execute("VACUUM")
    connection.executescript(
        """
        CREATE TABLE paper_decision_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          event_ts REAL NOT NULL,
          payload_json TEXT
        );
        CREATE INDEX idx_pde_event_ts ON paper_decision_events(event_ts);
        """
    )
    now = time.time()
    connection.execute(
        "INSERT INTO paper_decision_events(event_ts,payload_json) VALUES (?,?)",
        (now - 1, '{"value":1}'),
    )
    connection.commit()
    assert connection.execute("PRAGMA page_size").fetchone()[0] == 16384
    connection.close()

    start_event = threading.Event()
    copy_start_event = threading.Event()
    start_event.set()
    copy_start_event.set()
    report = snapshot_module.build_parallel_table_stage(
        source=source,
        destination=destination,
        table="paper_decision_events",
        role="paper_decision_events_parallel_stage",
        rule=DATABASE_SPECS["paper"]["tables"]["paper_decision_events"],
        source_page_report={"page_size": 16384},
        review_lower_epoch=now - 3600,
        long_lower_epoch=now - 3600,
        upper_epoch=now + 1,
        budget_bytes=snapshot_module.MIN_PAPER_DECISION_STAGE_CAP_BYTES,
        busy_timeout_ms=100,
        max_source_read_lock_sec=10,
        start_event=start_event,
        pinned_barrier=threading.Barrier(1),
        copy_start_event=copy_start_event,
        cancel_event=threading.Event(),
    )

    assert report["accepted"] is True
    assert report["stage_page_size"] == 4096
    assert report["stage_size_bytes"] <= report["stage_budget_bytes"] == 12288
    assert report["table_report"]["rows_copied"] == 1
    stage = sqlite3.connect(destination)
    try:
        assert stage.execute("PRAGMA page_size").fetchone()[0] == 4096
        assert stage.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    finally:
        stage.close()


@pytest.mark.parametrize(
    "failure_code",
    [
        "parallel_paper_stage_start_timeout",
        "parallel_paper_stage_cancelled",
        "parallel_paper_stage_barrier_broken",
        "parallel_paper_stage_timeout",
        "parallel_paper_stage_missing",
        "parallel_paper_stage_budget_exceeded",
        "parallel_paper_stage_quick_check_failed",
        "parallel_paper_stage_row_count_mismatch",
        "parallel_paper_stage_cleanup_failed",
        "parallel_paper_stage_failed",
        "parallel_paper_stage_column_contract_mismatch",
        "parallel_paper_stage_destination_schema_invalid",
        "parallel_paper_stage_destination_schema_mismatch",
        "parallel_paper_stage_generated_columns_unsupported",
        "parallel_stage_table_columns_missing",
        "parallel_stage_duplicate_columns",
        "parallel_stage_table_missing",
        "parallel_stage_destination_collision",
        "paper_decision_parallel_stage_start_timeout",
        "paper_decision_parallel_stage_cancelled",
        "paper_decision_parallel_stage_barrier_broken",
        "paper_decision_parallel_stage_timeout",
        "paper_decision_parallel_stage_missing",
        "paper_decision_parallel_stage_budget_exceeded",
        "paper_decision_parallel_stage_quick_check_failed",
        "paper_decision_parallel_stage_row_count_mismatch",
        "paper_decision_parallel_stage_cleanup_failed",
        "paper_decision_parallel_stage_failed",
    ],
)
def test_parallel_paper_failures_preserve_actionable_code(failure_code):
    error = RuntimeError(failure_code)
    assert snapshot_module.snapshot_component_failure_code(error) == failure_code
    assert snapshot_module.snapshot_failure_code(error) == failure_code


@pytest.mark.parametrize(
    "causal_code",
    [
        "snapshot_source_read_lock_timeout",
        "selective_snapshot_exceeded_database_budget",
    ],
)
def test_concurrent_failure_code_ignores_barrier_and_cancel_fallout(causal_code):
    error = snapshot_module.ConcurrentSnapshotError(
        {
            "paper": {
                "error_code": causal_code,
                "error_type": "RuntimeError",
                "stage": "copy_table:paper_decision_events",
            },
            "signal": {
                "error_code": "BrokenBarrierError",
                "error_type": "BrokenBarrierError",
                "stage": "pinned_barrier",
            },
            "raw": {
                "error_code": "parallel_paper_stage_barrier_broken",
                "error_type": "RuntimeError",
                "stage": "pinned_barrier",
            },
            "kline": {
                "error_code": "parallel_paper_stage_cancelled",
                "error_type": "RuntimeError",
                "stage": "pinned_barrier",
            },
        }
    )

    assert snapshot_module.snapshot_failure_code(error) == causal_code


def test_concurrent_failure_code_keeps_distinct_real_causes_generic():
    error = snapshot_module.ConcurrentSnapshotError(
        {
            "paper": {
                "error_code": "snapshot_source_read_lock_timeout",
                "error_type": "RuntimeError",
                "stage": "copy_table:paper_decision_events",
            },
            "raw": {
                "error_code": "selective_snapshot_exceeded_database_budget",
                "error_type": "RuntimeError",
                "stage": "copy_table:raw_signal_outcomes",
            },
            "signal": {
                "error_code": "BrokenBarrierError",
                "error_type": "BrokenBarrierError",
                "stage": "pinned_barrier",
            },
        }
    )

    assert snapshot_module.snapshot_failure_code(error) == (
        "concurrent_evaluator_snapshot_failed"
    )


def test_concurrent_failure_code_does_not_promote_barrier_only_fallout():
    error = snapshot_module.ConcurrentSnapshotError(
        {
            "signal": {
                "error_code": "BrokenBarrierError",
                "error_type": "BrokenBarrierError",
                "stage": "pinned_barrier",
            },
            "raw": {
                "error_code": "parallel_paper_stage_barrier_broken",
                "error_type": "RuntimeError",
                "stage": "pinned_barrier",
            },
        }
    )

    assert snapshot_module.snapshot_failure_code(error) == (
        "concurrent_evaluator_snapshot_failed"
    )


def test_dynamic_budget_reclaims_unused_small_database_reserves_for_paper():
    gib = 1024**3
    reports = {
        "signal": {"estimated_compact_bytes": 189_046_784, "page_size": 4096},
        "paper": {"estimated_compact_bytes": 16_591_126_528, "page_size": 4096},
        "raw": {"estimated_compact_bytes": 401_227_776, "page_size": 4096},
        "kline": {"estimated_compact_bytes": 59_219_968, "page_size": 4096},
    }

    plan = database_output_budget_plan(10, reports)
    static = static_database_output_budgets(10)
    budgets = plan["database_budget_bytes"]

    assert plan["schema_version"] == "evaluator_snapshot_budget.v2"
    assert plan["total_output_cap_bytes"] == 10 * gib
    assert plan["total_budget_bytes"] == 10 * gib
    assert plan["bundle_cap_unchanged"] is True
    assert plan["static_fallback_databases"] == []
    assert plan["source_compact_estimate_bytes"]["paper"] == 16_591_126_528
    assert budgets["paper"] > static["paper"]
    assert budgets["paper"] > 9 * gib
    assert plan["reclaimed_to_paper_bytes"] == budgets["paper"] - static["paper"]
    assert sum(budgets.values()) == 10 * gib
    for name in ("signal", "raw", "kline"):
        assert reports[name]["estimated_compact_bytes"] <= budgets[name] <= static[name]


def test_dynamic_budget_uses_static_reserve_when_compact_estimate_is_missing():
    reports = {
        "signal": {"estimated_compact_bytes": 1024**2, "page_size": 4096},
        "paper": {"estimated_compact_bytes": 8 * 1024**3, "page_size": 4096},
        "raw": {"estimated_compact_bytes": 0, "page_size": 4096},
        "kline": {"page_size": 4096},
    }

    plan = database_output_budget_plan(10, reports)
    static = static_database_output_budgets(10)

    assert plan["static_fallback_databases"] == ["raw", "kline"]
    assert plan["database_budget_bytes"]["raw"] == static["raw"]
    assert plan["database_budget_bytes"]["kline"] == static["kline"]
    assert sum(plan["database_budget_bytes"].values()) == 10 * 1024**3


@pytest.mark.parametrize(
    "malformed_estimate",
    [True, False, 0.5, 1.0, float("inf"), "1048576", object()],
)
def test_dynamic_budget_uses_static_reserve_for_malformed_estimate(
    malformed_estimate,
):
    reports = {
        "signal": {"estimated_compact_bytes": 1024**2, "page_size": 4096},
        "paper": {"estimated_compact_bytes": float("inf"), "page_size": 4096},
        "raw": {"estimated_compact_bytes": malformed_estimate, "page_size": 4096},
        "kline": {"estimated_compact_bytes": 1024**2, "page_size": 4096},
    }

    plan = database_output_budget_plan(10, reports)
    static = static_database_output_budgets(10)

    assert "raw" in plan["static_fallback_databases"]
    assert plan["source_compact_estimate_bytes"]["raw"] is None
    assert plan["source_compact_estimate_bytes"]["paper"] is None
    assert plan["database_budget_bytes"]["raw"] == static["raw"]
    assert sum(plan["database_budget_bytes"].values()) == 10 * 1024**3


def test_candidate_time_selection_forces_observed_at_index(tmp_path):
    sources = create_sources(tmp_path)
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute("ATTACH DATABASE ? AS src", (sources["paper"],))
    try:
        selection = selection_for_table(
            connection,
            "candidate_shadow_observations",
            DATABASE_SPECS["paper"]["tables"]["candidate_shadow_observations"],
            review_lower_epoch=100.0,
            long_lower_epoch=10.0,
            upper_epoch=200.0,
        )
        plan = [
            str(row[3])
            for row in connection.execute(
                "EXPLAIN QUERY PLAN SELECT * "
                f"FROM {source_table_reference('candidate_shadow_observations', selection)} "
                f"WHERE {selection['predicate_sql']} "
                "ORDER BY signal_id",
                selection["parameters"],
            )
        ]
    finally:
        connection.close()

    assert selection["predicate_strategy"] == "indexed_epoch_seconds"
    assert selection["indexed_time_anchor"] == "observed_at"
    assert selection["source_index_name"] == "idx_candidate_shadow_obs_observed"
    assert selection["source_index_columns"] == ["observed_at"]
    assert selection["source_index_partial"] is False
    assert '\"observed_at\" >= ?' in selection["predicate_sql"]
    assert "COALESCE(" not in selection["predicate_sql"]
    assert "typeof(\"observed_at\")" not in selection["predicate_sql"]
    assert any(
        "SEARCH" in detail
        and "idx_candidate_shadow_obs_observed" in detail
        and "observed_at>?" in detail
        for detail in plan
    ), plan
    assert not any(
        "SCAN" in detail and "candidate_shadow_observations" in detail
        for detail in plan
    ), plan


def test_paper_source_watermarks_use_indexes_or_defer_without_full_scan(tmp_path):
    sources = create_sources(tmp_path)
    paper = sqlite3.connect(sources["paper"])
    paper.executemany(
        "INSERT INTO candidate_shadow_observations(signal_id, observed_at) VALUES (?, ?)",
        [(1, 100), (2, 200)],
    )
    paper.executemany(
        "INSERT INTO candidate_shadow_virtual_trades(signal_id, observed_at) VALUES (?, ?)",
        [(1, 110), (2, 210)],
    )
    paper.executemany(
        "INSERT INTO paper_decision_events(id, event_ts) VALUES (?, ?)",
        [(1, 120), (2, 220)],
    )
    paper.executemany(
        "INSERT INTO a_class_decision_events(id, event_ts) VALUES (?, ?)",
        [(1, 130), (2, 230)],
    )
    paper.executemany(
        "INSERT INTO opportunity_events(id, event_ts) VALUES (?, ?)",
        [(1, 140), (2, 240)],
    )
    paper.execute(
        "INSERT INTO a_class_mode_runtime_state(id, updated_at) VALUES (1, 250)"
    )
    paper.execute("INSERT INTO paper_trades(id, entry_time) VALUES (1, 260)")
    paper.commit()
    paper.close()

    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute("ATTACH DATABASE ? AS src", (sources["paper"],))
    statements = []
    connection.set_trace_callback(statements.append)
    try:
        metadata = snapshot_module.database_metadata(
            connection,
            DATABASE_SPECS["paper"],
            schema="src",
            indexed_watermark_anchors=True,
        )
    finally:
        connection.close()

    assert metadata["upper_watermarks"]["candidate_shadow_observations"] == {
        "observed_at": 200
    }
    assert metadata["upper_watermarks"]["candidate_shadow_virtual_trades"] == {
        "observed_at": 210
    }
    expected_indexed = (
        (
            "candidate_shadow_observations",
            "idx_candidate_shadow_obs_observed",
            "observed_at",
            200,
        ),
        (
            "candidate_shadow_virtual_trades",
            "idx_candidate_shadow_virtual_observed",
            "observed_at",
            210,
        ),
        ("paper_decision_events", "idx_pde_event_ts", "event_ts", 220),
        (
            "a_class_decision_events",
            "idx_a_class_decision_recent",
            "event_ts",
            230,
        ),
        (
            "opportunity_events",
            "idx_opportunity_events_recent",
            "event_ts",
            240,
        ),
    )
    for table, index_name, column, expected_value in expected_indexed:
        assert metadata["upper_watermarks"][table] == {column: expected_value}
        evidence = metadata["watermark_query_evidence"][table]
        assert evidence["strategy"] == "indexed_anchor_max"
        assert evidence["column"] == column
        assert evidence["source_index_name"] == index_name
        assert evidence["uses_declared_index"] is True
        assert evidence["full_table_scan_detected"] is False
        assert any(index_name in detail for detail in evidence["query_plan"])
        assert not any(
            "SCAN" in detail.upper() and index_name not in detail
            for detail in evidence["query_plan"]
        )

    for table in ("a_class_mode_runtime_state", "paper_trades"):
        assert metadata["upper_watermarks"][table] == {}
        evidence = metadata["watermark_query_evidence"][table]
        assert evidence["strategy"] == "deferred_to_frozen_snapshot"
        assert evidence["source_query_executed"] is False
    assert not any(
        "SELECT MAX(" in statement.upper()
        and any(
            f'"{table}"' in statement
            for table in ("a_class_mode_runtime_state", "paper_trades")
        )
        for statement in statements
    )


def test_candidate_time_selection_fails_closed_without_source_index(tmp_path):
    sources = create_sources(tmp_path)
    source = sqlite3.connect(sources["paper"])
    source.execute("DROP INDEX idx_candidate_shadow_obs_observed")
    source.commit()
    source.close()

    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute("ATTACH DATABASE ? AS src", (sources["paper"],))
    try:
        with pytest.raises(
            RuntimeError,
            match="selective_snapshot_source_index_missing:"
            "candidate_shadow_observations:observed_at",
        ):
            selection_for_table(
                connection,
                "candidate_shadow_observations",
                DATABASE_SPECS["paper"]["tables"]["candidate_shadow_observations"],
                review_lower_epoch=100.0,
                long_lower_epoch=10.0,
                upper_epoch=200.0,
            )
    finally:
        connection.close()


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
    assert report["database_budget_plan"]["bundle_cap_unchanged"] is True
    assert sum(report["database_budget_plan"]["database_budget_bytes"].values()) == (
        report["output_cap_bytes"]
    )
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


def test_candidate_observation_payload_projection_is_lossless_and_compact(tmp_path):
    sources = create_sources(tmp_path)
    now = int(time.time())
    paper = sqlite3.connect(sources["paper"])
    paper.execute("DROP TABLE candidate_shadow_observations")
    paper.executescript(
        """
        CREATE TABLE candidate_shadow_observations (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          signal_id INTEGER NOT NULL,
          token_ca TEXT NOT NULL,
          signal_ts INTEGER,
          candidate_id TEXT NOT NULL,
          family TEXT,
          matched INTEGER NOT NULL,
          reason TEXT,
          observed_at INTEGER NOT NULL,
          payload_json TEXT NOT NULL,
          UNIQUE(signal_id, candidate_id)
        );
        CREATE INDEX idx_candidate_shadow_obs_signal
          ON candidate_shadow_observations(signal_id);
        CREATE INDEX idx_candidate_shadow_obs_candidate
          ON candidate_shadow_observations(candidate_id, observed_at);
        CREATE INDEX idx_candidate_shadow_obs_observed
          ON candidate_shadow_observations(observed_at);
        """
    )
    expected = {}
    for index in range(84):
        candidate_id = "current_all" if index == 0 else f"candidate_{index:02d}"
        payload = {
            "common_blob": "x" * 2048,
            "context_schema_version": "candidate-shadow-context-v2",
            "explicit_null": None,
            "nested": {"quote": {"clean": True}, "items": [1, 2, 3]},
            "candidate_id": candidate_id,
            "matched": index % 3 == 0,
            "type_sensitive_value": True if index == 1 else 1,
        }
        if index != 1:
            payload["optional_key"] = None
        if index == 2:
            payload["future_unknown_key"] = {"kept": True}
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        expected[(1, candidate_id)] = payload
        paper.execute(
            """
            INSERT INTO candidate_shadow_observations
              (signal_id, token_ca, signal_ts, candidate_id, family, matched,
               reason, observed_at, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "TOKEN",
                now - 120,
                candidate_id,
                "base",
                int(payload["matched"]),
                "self_test",
                now - 60,
                encoded,
            ),
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

    paper_report = report["databases"]["paper"]
    selection = paper_report["selected_tables"]["candidate_shadow_observations"]
    projection = selection["storage_projection"]
    assert projection["applied"] is True
    assert projection["payload_semantics_preserved"] is True
    assert projection["semantic_rows_verified"] == 84
    assert projection["payload_storage_ratio"] < 0.1
    assert projection["unknown_payload_keys_preserved"] is True
    assert projection["missing_and_null_keys_preserved"] is True
    assert paper_report["source_read_lock_released_before_index_build"] is True
    assert paper_report["source_read_lock_budget_passed"] is True
    assert selection["source_copy_duration_sec"] >= 0
    assert selection["source_lock_elapsed_after_table_sec"] >= 0
    assert selection["source_lock_remaining_after_table_sec"] >= 0
    assert set(selection["indexes_created"]) == {
        "idx_a3_candidate_shadow_obs_signal",
        "idx_a3_candidate_shadow_obs_candidate",
        "idx_a3_candidate_shadow_obs_observed",
    }

    snapshot = sqlite3.connect(paper_report["snapshot_path"])
    snapshot.row_factory = sqlite3.Row
    try:
        object_type = snapshot.execute(
            "SELECT type FROM sqlite_master WHERE name='candidate_shadow_observations'"
        ).fetchone()[0]
        assert object_type == "view"
        assert sqlite_has_table(paper_report["snapshot_path"], "candidate_shadow_observations")
        assert snapshot.execute(
            "SELECT MAX(rowid) FROM candidate_shadow_observations"
        ).fetchone()[0] == 84
        actual = {
            (int(row["signal_id"]), str(row["candidate_id"])): json.loads(
                row["payload_json"]
            )
            for row in snapshot.execute(
                "SELECT signal_id, candidate_id, payload_json "
                "FROM candidate_shadow_observations"
            )
        }
        assert actual == expected
        assert snapshot.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    finally:
        snapshot.close()


def test_candidate_projection_runs_after_source_lock_release_and_stage_is_removed(
    tmp_path,
    monkeypatch,
):
    sources = create_sources(tmp_path)
    now = int(time.time())
    paper = sqlite3.connect(sources["paper"])
    paper.execute("DROP TABLE candidate_shadow_observations")
    paper.executescript(
        """
        CREATE TABLE candidate_shadow_observations (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          signal_id INTEGER NOT NULL,
          token_ca TEXT NOT NULL,
          signal_ts INTEGER,
          candidate_id TEXT NOT NULL,
          family TEXT,
          matched INTEGER NOT NULL,
          reason TEXT,
          observed_at INTEGER NOT NULL,
          payload_json TEXT NOT NULL,
          UNIQUE(signal_id, candidate_id)
        );
        CREATE INDEX idx_candidate_shadow_obs_observed
          ON candidate_shadow_observations(observed_at);
        """
    )
    paper.execute(
        """
        INSERT INTO candidate_shadow_observations
          (signal_id, token_ca, signal_ts, candidate_id, family, matched,
           reason, observed_at, payload_json)
        VALUES (1, 'TOKEN', ?, 'current_all', 'base', 1, 'self_test', ?, ?)
        """,
        (now - 120, now - 60, '{"common_blob":"x","candidate_id":"current_all"}'),
    )
    paper.commit()
    paper.close()
    original_projection = snapshot_module.copy_candidate_observation_projection

    def slow_projection(*args, **kwargs):
        time.sleep(0.1)
        return original_projection(*args, **kwargs)

    monkeypatch.setattr(
        snapshot_module,
        "copy_candidate_observation_projection",
        slow_projection,
    )
    report = build_snapshot_bundle(
        sources=sources,
        out_root=str(tmp_path / "evidence"),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        max_output_gib=0.1,
        snapshot_id="20260101T000000Z-1234abce",
    )

    paper_report = report["databases"]["paper"]
    projection = paper_report["selected_tables"][
        "candidate_shadow_observations"
    ]["storage_projection"]
    assert paper_report["candidate_projection_after_source_read_lock_release"] is True
    assert paper_report["candidate_projection_duration_sec"] >= 0.1
    assert paper_report["temporary_candidate_stage_size_bytes"] > 0
    assert paper_report["temporary_candidate_stage_removed_before_publish"] is True
    assert projection["applied"] is True
    assert projection["projection_started_after_source_read_view_release"] is True
    assert projection["source_stage_schema_version"] == "candidate_observation_selective_stage.v1"
    assert projection["source_stage_size_bytes"] > 0
    assert projection["stage_order_index_name"] == "idx_a3_candidate_stage_signal_candidate"
    assert projection["stage_query_plan_uses_order_index"] is True
    assert projection["stage_query_plan_temp_btree_detected"] is False
    assert all("TEMP B-TREE" not in row.upper() for row in projection["stage_query_plan"])
    assert projection["off_source_lock_projection_duration_sec"] >= 0.1
    snapshot_dir = Path(paper_report["snapshot_path"]).parent
    assert not (snapshot_dir / ".candidate-observation-stage.db").exists()
    assert report["candidate_projection_after_source_read_lock_release"] is True
    assert report["candidate_stage_removed_before_publish"] is True
    assert report["disk_preflight"]["temporary_candidate_stage_cap_bytes"] > 0
    assert (
        report["disk_preflight"]["estimated_free_at_peak_bytes"]
        >= report["disk_preflight"]["required_reserve_bytes"]
    )


def test_paper_decision_events_use_parallel_pinned_stage_and_preserve_payload(
    tmp_path,
    monkeypatch,
):
    sources = create_sources(tmp_path)
    now = int(time.time())
    paper = sqlite3.connect(sources["paper"])
    paper.execute("DROP TABLE paper_decision_events")
    paper.execute("DROP TABLE a_class_decision_events")
    paper.execute("DROP TABLE opportunity_events")
    paper.executescript(
        """
        CREATE TABLE paper_decision_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          event_ts REAL NOT NULL,
          signal_id INTEGER,
          token_ca TEXT,
          component TEXT,
          event_type TEXT,
          decision TEXT,
          reason TEXT,
          payload_json TEXT,
          created_at INTEGER
        );
        CREATE INDEX idx_pde_event_ts ON paper_decision_events(event_ts);
        CREATE TABLE a_class_decision_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          event_ts REAL NOT NULL,
          token_ca TEXT,
          action TEXT,
          matrix_json TEXT,
          payload_json TEXT,
          created_at INTEGER
        );
        CREATE INDEX idx_a_class_decision_recent
          ON a_class_decision_events(event_ts);
        CREATE TABLE opportunity_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          event_ts REAL NOT NULL,
          token_ca TEXT,
          source_type TEXT,
          hard_blockers_json TEXT,
          raw_payload_json TEXT,
          created_at INTEGER
        );
        CREATE INDEX idx_opportunity_events_recent
          ON opportunity_events(event_ts);
        """
    )
    expected_payload = {
        "hard_blockers": ["mode_gate"],
        "quote": {"clean": True, "age_sec": 1.25},
        "nested": {"unknown_future_key": [1, 2, 3]},
    }
    paper.executemany(
        """
        INSERT INTO paper_decision_events
          (event_ts, signal_id, token_ca, component, event_type, decision,
           reason, payload_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                now - 60,
                1,
                "TOKEN",
                "entry",
                "final_entry_contract",
                "would_enter",
                "self_test",
                json.dumps(expected_payload, sort_keys=True),
                now - 60,
            ),
            (
                now - 5 * 86400,
                2,
                "OLD",
                "entry",
                "old",
                "reject",
                "old",
                "{}",
                now - 5 * 86400,
            ),
            (
                now + 3600,
                3,
                "FUTURE",
                "entry",
                "future",
                "reject",
                "future",
                "{}",
                now + 3600,
            ),
        ],
    )
    expected_a_class_payload = {
        "matrix": {"score": 77.5},
        "future_field": {"preserve": True},
    }
    paper.execute(
        """
        INSERT INTO a_class_decision_events
          (event_ts, token_ca, action, matrix_json, payload_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            now - 50,
            "TOKEN",
            "would_enter",
            json.dumps({"grade": "A"}, sort_keys=True),
            json.dumps(expected_a_class_payload, sort_keys=True),
            now - 50,
        ),
    )
    expected_opportunity_payload = {
        "quote": {"executable": True},
        "unknown_key": ["keep", 1],
    }
    paper.execute(
        """
        INSERT INTO opportunity_events
          (event_ts, token_ca, source_type, hard_blockers_json,
           raw_payload_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            now - 40,
            "TOKEN",
            "premium",
            json.dumps([], sort_keys=True),
            json.dumps(expected_opportunity_payload, sort_keys=True),
            now - 40,
        ),
    )
    expected_path_payload = {
        "quote": {"source": "jupiter", "executable": True},
        "future_path_field": {"preserve": [1, 2, 3]},
    }
    paper.execute(
        """
        INSERT INTO opportunity_event_path_samples
          (id, opportunity_key, sample_ts, raw_payload_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            1,
            "opportunity:1",
            now - 30,
            json.dumps(expected_path_payload, sort_keys=True),
            now - 30,
            now - 30,
        ),
    )
    paper.commit()
    paper.close()

    candidate_started = threading.Event()
    parallel_started = {
        table: threading.Event()
        for table in snapshot_module.PARALLEL_PAPER_STAGE_TABLES
    }
    original_candidate_stage = snapshot_module.stage_candidate_observation_rows
    original_single_stage = snapshot_module.stage_single_source_table

    def coordinated_candidate_stage(*args, **kwargs):
        candidate_started.set()
        assert all(event.wait(timeout=2) for event in parallel_started.values())
        return original_candidate_stage(*args, **kwargs)

    def coordinated_single_stage(*args, **kwargs):
        table = args[1]
        if table in parallel_started:
            parallel_started[table].set()
            assert candidate_started.wait(timeout=2)
        return original_single_stage(*args, **kwargs)

    monkeypatch.setattr(
        snapshot_module,
        "stage_candidate_observation_rows",
        coordinated_candidate_stage,
    )
    monkeypatch.setattr(
        snapshot_module,
        "stage_single_source_table",
        coordinated_single_stage,
    )

    report = build_snapshot_bundle(
        sources=sources,
        out_root=str(tmp_path / "parallel-paper-decision-evidence"),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        max_output_gib=0.1,
        snapshot_id="20260101T000000Z-1234abd0",
    )

    paper_report = report["databases"]["paper"]
    selection = paper_report["selected_tables"]["paper_decision_events"]
    stage = selection["parallel_stage"]
    assert paper_report["parallel_paper_stage_count"] == len(
        snapshot_module.PARALLEL_PAPER_STAGE_TABLES
    )
    assert paper_report["parallel_paper_stages_all_pinned"] is True
    assert paper_report[
        "parallel_paper_stages_all_merged_after_source_read_lock_release"
    ] is True
    assert paper_report["parallel_paper_stages_all_removed_before_publish"] is True
    assert set(paper_report["parallel_paper_stages"]) == set(
        snapshot_module.PARALLEL_PAPER_STAGE_TABLES
    )
    assert selection["rows_copied"] == 1
    assert stage["full_fidelity_row_copy"] is True
    assert stage["payload_semantics_preserved"] is True
    assert stage["row_count_matched"] is True
    assert stage["stage_rows_copied"] == stage["rows_merged"] == 1
    assert stage["quick_check"] == ["ok"]
    assert stage["source_read_lock_budget_passed"] is True
    assert stage["merge_started_after_source_read_view_release"] is True
    assert paper_report["paper_decision_parallel_stage_used"] is True
    assert paper_report["paper_decision_parallel_stage_page_size"] == 4096
    assert stage["stage_page_size"] == 4096
    assert paper_report["paper_decision_parallel_read_view_pinned"] is True
    assert paper_report[
        "paper_decision_parallel_stage_merged_after_source_read_lock_release"
    ] is True
    assert paper_report[
        "paper_decision_parallel_stage_removed_before_publish"
    ] is True
    assert paper_report["source_read_lock_duration_sec"] == max(
        paper_report["main_source_read_lock_duration_sec"],
        *paper_report[
            "parallel_paper_source_read_lock_duration_sec"
        ].values(),
    )
    roles = {row["role"] for row in paper_report["pinned_read_views"]}
    assert roles == {
        "paper_main_selective_copy",
        "paper_decision_events_parallel_stage",
        "a_class_decision_events_parallel_stage",
        "opportunity_events_parallel_stage",
        "opportunity_event_path_samples_parallel_stage",
    }
    assert report["pinned_read_view_count"] == 4 + len(
        snapshot_module.PARALLEL_PAPER_STAGE_TABLES
    )
    for table in snapshot_module.PARALLEL_PAPER_STAGE_TABLES:
        table_selection = paper_report["selected_tables"][table]
        table_stage = table_selection["parallel_stage"]
        aggregate = paper_report["parallel_paper_stages"][table]
        assert table_selection["rows_copied"] == 1
        assert table_stage["schema_version"] == (
            snapshot_module.PARALLEL_PAPER_STAGE_SCHEMA_VERSION
        )
        assert table_stage["stage_schema_mode"] == (
            snapshot_module.PARALLEL_PAPER_STAGE_STORAGE_MODE
        )
        assert table_stage["full_fidelity_row_copy"] is True
        assert table_stage["payload_semantics_preserved"] is True
        assert table_stage["stage_column_contract_passed"] is True
        assert table_stage["stage_index_count"] == 0
        assert table_stage["source_constraints_deferred_off_source_lock"] is True
        assert table_stage[
            "destination_schema_restored_after_source_read_lock_release"
        ] is True
        assert table_stage[
            "source_constraints_rebuilt_after_source_read_lock_release"
        ] is True
        assert table_stage["source_create_sql_sha256"] == table_stage[
            "destination_create_sql_sha256"
        ]
        assert table_stage["source_column_contract_sha256"] == table_stage[
            "stage_column_contract_sha256"
        ] == table_stage["destination_column_contract_sha256"]
        assert table_stage["row_count_matched"] is True
        assert aggregate["rows_copied"] == aggregate["rows_merged"] == 1
        assert aggregate["stage_page_size"] == 4096
        assert aggregate["stage_schema_mode"] == (
            snapshot_module.PARALLEL_PAPER_STAGE_STORAGE_MODE
        )
        assert aggregate["stage_column_contract_passed"] is True
        assert aggregate["stage_index_count"] == 0
        assert aggregate["source_constraints_deferred_off_source_lock"] is True
        assert aggregate[
            "destination_schema_restored_after_source_read_lock_release"
        ] is True
        assert aggregate[
            "source_constraints_rebuilt_after_source_read_lock_release"
        ] is True
        assert aggregate["source_create_sql_sha256"] == aggregate[
            "destination_create_sql_sha256"
        ]
        assert aggregate["source_column_contract_sha256"] == aggregate[
            "stage_column_contract_sha256"
        ] == aggregate["destination_column_contract_sha256"]
        assert aggregate["removed_before_publish"] is True
    snapshot = sqlite3.connect(paper_report["snapshot_path"])
    try:
        row = snapshot.execute(
            "SELECT id, token_ca, payload_json FROM paper_decision_events"
        ).fetchone()
        assert row[:2] == (1, "TOKEN")
        assert json.loads(row[2]) == expected_payload
        a_class_row = snapshot.execute(
            "SELECT token_ca, matrix_json, payload_json FROM a_class_decision_events"
        ).fetchone()
        assert a_class_row[0] == "TOKEN"
        assert json.loads(a_class_row[1]) == {"grade": "A"}
        assert json.loads(a_class_row[2]) == expected_a_class_payload
        opportunity_row = snapshot.execute(
            "SELECT token_ca, hard_blockers_json, raw_payload_json "
            "FROM opportunity_events"
        ).fetchone()
        assert opportunity_row[0] == "TOKEN"
        assert json.loads(opportunity_row[1]) == []
        assert json.loads(opportunity_row[2]) == expected_opportunity_payload
        path_row = snapshot.execute(
            "SELECT opportunity_key, raw_payload_json "
            "FROM opportunity_event_path_samples"
        ).fetchone()
        assert path_row[0] == "opportunity:1"
        assert json.loads(path_row[1]) == expected_path_payload
        assert snapshot.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    finally:
        snapshot.close()
    snapshot_dir = Path(paper_report["snapshot_path"]).parent
    for config in snapshot_module.PARALLEL_PAPER_STAGE_CONFIGS.values():
        assert not (snapshot_dir / config["filename"]).exists()
    assert not (snapshot_dir / ".candidate-observation-stage.db").exists()


def test_constraint_free_parallel_stage_restores_exact_destination_schema_off_lock(
    tmp_path,
):
    source = tmp_path / "source-paper.db"
    stage_path = tmp_path / "opportunity-stage.db"
    final_path = tmp_path / "paper-evidence.db"
    source_create_sql = """
        CREATE TABLE opportunity_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          opportunity_key TEXT NOT NULL UNIQUE,
          event_ts REAL NOT NULL,
          state TEXT NOT NULL DEFAULT 'OPEN' CHECK(state IN ('OPEN','CLOSED')),
          raw_payload_json TEXT NOT NULL
        )
    """.strip()
    source_db = sqlite3.connect(source)
    source_db.executescript(
        source_create_sql
        + ";"
        + "CREATE INDEX idx_opportunity_events_recent "
        + "ON opportunity_events(event_ts);"
    )
    source_db.execute(
        "INSERT INTO opportunity_events"
        "(opportunity_key,event_ts,state,raw_payload_json) VALUES (?,?,?,?)",
        ("opportunity:1", time.time() - 30, "OPEN", '{"future_key":1}'),
    )
    source_db.commit()
    source_db.close()

    stage = sqlite3.connect(stage_path)
    stage.row_factory = sqlite3.Row
    stage.execute("ATTACH DATABASE ? AS src", (str(source),))
    report, deferred_indexes, destination_schema = (
        snapshot_module.stage_single_source_table(
            stage,
            "opportunity_events",
            DATABASE_SPECS["paper"]["tables"]["opportunity_events"],
            review_lower_epoch=time.time() - 3600,
            long_lower_epoch=time.time() - 3600,
            upper_epoch=time.time() + 1,
        )
    )
    stage.commit()
    stage.execute("DETACH DATABASE src")
    stage_create_sql = stage.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='opportunity_events'"
    ).fetchone()[0]
    assert report["rows_copied"] == 1
    assert report["stage_schema_mode"] == (
        snapshot_module.PARALLEL_PAPER_STAGE_STORAGE_MODE
    )
    assert report["stage_index_count"] == 0
    assert report["stage_column_contract_passed"] is True
    assert report["source_constraints_deferred_off_source_lock"] is True
    assert snapshot_module.stage_table_index_count(
        stage,
        "opportunity_events",
    ) == 0
    for forbidden in ("UNIQUE", "NOT NULL", "CHECK", "AUTOINCREMENT", "DEFAULT"):
        assert forbidden not in stage_create_sql.upper()
    stage.close()

    final = sqlite3.connect(final_path)
    final.row_factory = sqlite3.Row
    final.execute("ATTACH DATABASE ? AS staged", (str(stage_path),))
    merged = snapshot_module.merge_staged_table(
        final,
        stage_schema="staged",
        table="opportunity_events",
        destination_schema=destination_schema,
    )
    for _table, _name, sql in deferred_indexes:
        final.execute(sql)
    final.commit()
    final.execute("DETACH DATABASE staged")

    final_create_sql = final.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='opportunity_events'"
    ).fetchone()[0]
    assert merged["rows_merged"] == 1
    assert merged["destination_schema_restored"] is True
    assert merged[
        "source_constraints_rebuilt_after_source_read_lock_release"
    ] is True
    assert snapshot_module.sha256_text(final_create_sql) == (
        snapshot_module.sha256_text(source_create_sql)
    )
    assert tuple(
        final.execute(
            "SELECT opportunity_key,state,raw_payload_json FROM opportunity_events"
        ).fetchone()
    ) == ("opportunity:1", "OPEN", '{"future_key":1}')
    index_rows = list(final.execute("PRAGMA index_list(opportunity_events)"))
    assert any(row[2] == 1 and row[3] == "u" for row in index_rows)
    assert any(row[1] == "idx_opportunity_events_recent" for row in index_rows)
    with pytest.raises(sqlite3.IntegrityError):
        final.execute(
            "INSERT INTO opportunity_events"
            "(opportunity_key,event_ts,state,raw_payload_json) VALUES (?,?,?,?)",
            ("opportunity:1", time.time(), "OPEN", "{}"),
        )
    with pytest.raises(sqlite3.IntegrityError):
        final.execute(
            "INSERT INTO opportunity_events"
            "(opportunity_key,event_ts,state,raw_payload_json) VALUES (?,?,?,?)",
            ("opportunity:2", time.time(), "INVALID", "{}"),
        )
    final.rollback()
    assert final.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    final.close()


def test_candidate_projection_requires_integer_primary_key_rowid_alias(tmp_path):
    source = tmp_path / "paper.db"
    source_db = sqlite3.connect(source)
    source_db.execute(
        "CREATE TABLE candidate_shadow_observations("
        "id TEXT PRIMARY KEY, signal_id TEXT, candidate_id TEXT, payload_json TEXT)"
    )
    source_db.commit()
    source_db.close()

    destination = sqlite3.connect(":memory:")
    destination.row_factory = sqlite3.Row
    destination.execute("ATTACH DATABASE ? AS src", (str(source),))
    try:
        supported, _ = candidate_observation_projection_supported(destination)
        assert supported is False
    finally:
        destination.close()


def test_incompatible_candidate_projection_schema_fails_closed_without_publish(tmp_path):
    sources = create_sources(tmp_path)
    paper = sqlite3.connect(sources["paper"])
    paper.execute("DROP TABLE candidate_shadow_observations")
    paper.executescript(
        """
        CREATE TABLE candidate_shadow_observations(
          signal_id INTEGER,
          observed_at INTEGER
        );
        CREATE INDEX idx_candidate_shadow_obs_observed
          ON candidate_shadow_observations(observed_at);
        """
    )
    paper.commit()
    paper.close()
    out = tmp_path / "evidence"

    with pytest.raises(
        snapshot_module.ConcurrentSnapshotError,
        match="candidate_observation_payload_projection_semantic_mismatch",
    ):
        build_snapshot_bundle(
            sources=sources,
            out_root=str(out),
            repo_root=str(ROOT),
            max_skew_sec=30,
            min_free_after_gib=0,
            max_output_gib=0.1,
            snapshot_id="20260101T000000Z-1234abcf",
        )
    assert not (out / "current").exists()
    assert not (out / "snapshots" / ".20260101T000000Z-1234abcf.partial").exists()


def test_index_build_order_violation_fails_snapshot_acceptance(tmp_path, monkeypatch):
    sources = create_sources(tmp_path)
    out = tmp_path / "evidence"
    original_snapshot_one = snapshot_module.snapshot_one

    def invalidate_index_order(*args, **kwargs):
        report = original_snapshot_one(*args, **kwargs)
        if Path(args[0]) == Path(sources["paper"]):
            report["source_read_lock_released_before_index_build"] = False
        return report

    monkeypatch.setattr(snapshot_module, "snapshot_one", invalidate_index_order)
    with pytest.raises(RuntimeError, match="cross-database snapshot acceptance failed"):
        build_snapshot_bundle(
            sources=sources,
            out_root=str(out),
            repo_root=str(ROOT),
            max_skew_sec=30,
            min_free_after_gib=0,
            max_output_gib=0.1,
            snapshot_id="20260101T000000Z-1234abcd",
        )
    assert not (out / "current").exists()
    assert not (out / "snapshots" / ".20260101T000000Z-1234abcd.partial").exists()


def test_source_read_lock_deadline_fails_closed_and_cleans_partial(tmp_path, monkeypatch):
    sources = create_sources(tmp_path)
    out = tmp_path / "evidence"
    original_snapshot_one = snapshot_module.snapshot_one

    def force_expired_progress_handler(source, destination, spec, connection, pin_report, **kwargs):
        if Path(source) == Path(sources["paper"]):
            time.sleep(0.02)
            connection.execute(
                "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<1000000) "
                "SELECT SUM(x) FROM n"
            ).fetchone()
        return original_snapshot_one(
            source,
            destination,
            spec,
            connection,
            pin_report,
            **kwargs,
        )

    monkeypatch.setattr(snapshot_module, "snapshot_one", force_expired_progress_handler)
    started = time.monotonic()
    with pytest.raises(RuntimeError, match="source_read_lock_budget_exceeded:paper"):
        build_snapshot_bundle(
            sources=sources,
            out_root=str(out),
            repo_root=str(ROOT),
            max_skew_sec=30,
            min_free_after_gib=0,
            max_output_gib=0.1,
            max_source_read_lock_sec=0.01,
            snapshot_id="20260101T000000Z-1234abcd",
        )
    assert time.monotonic() - started < 1.0
    assert not (out / "current").exists()
    assert not (out / "snapshots" / ".20260101T000000Z-1234abcd.partial").exists()


@pytest.mark.parametrize("blocked_table", snapshot_module.PARALLEL_PAPER_STAGE_TABLES)
def test_parallel_paper_stage_deadline_fails_closed_and_cleans_partial(
    tmp_path,
    monkeypatch,
    blocked_table,
):
    sources = create_sources(tmp_path)
    out = tmp_path / "parallel-stage-deadline-evidence"
    original_stage = snapshot_module.stage_single_source_table

    def force_parallel_stage_deadline(connection, table, rule, **kwargs):
        if table == blocked_table:
            connection.execute(
                "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL "
                "SELECT x+1 FROM n WHERE x<10000000) SELECT SUM(x) FROM n"
            ).fetchone()
        return original_stage(connection, table, rule, **kwargs)

    monkeypatch.setattr(
        snapshot_module,
        "stage_single_source_table",
        force_parallel_stage_deadline,
    )
    with pytest.raises(RuntimeError, match="source_read_lock_budget_exceeded:paper"):
        build_snapshot_bundle(
            sources=sources,
            out_root=str(out),
            repo_root=str(ROOT),
            max_skew_sec=30,
            min_free_after_gib=0,
            max_output_gib=0.1,
            max_source_read_lock_sec=0.05,
            snapshot_id="20260101T000000Z-1234abd1",
        )
    assert not (out / "current").exists()
    partial = out / "snapshots" / ".20260101T000000Z-1234abd1.partial"
    assert not partial.exists()
    for config in snapshot_module.PARALLEL_PAPER_STAGE_CONFIGS.values():
        assert not (partial / config["filename"]).exists()


def test_parallel_stage_sqlite_busy_is_classified_and_preserves_identity(tmp_path):
    source = tmp_path / "busy-paper.db"
    destination = tmp_path / "paper-decision-stage.db"
    source_db = sqlite3.connect(source)
    source_db.executescript(
        """
        CREATE TABLE paper_decision_events(
          id INTEGER PRIMARY KEY,
          event_ts REAL NOT NULL,
          payload_json TEXT
        );
        CREATE INDEX idx_pde_event_ts ON paper_decision_events(event_ts);
        INSERT INTO paper_decision_events VALUES (1, 1, '{}');
        """
    )
    source_db.commit()
    source_db.close()

    blocker = sqlite3.connect(source, timeout=0.01, isolation_level=None)
    blocker.execute("BEGIN EXCLUSIVE")
    start_event = threading.Event()
    start_event.set()
    copy_start_event = threading.Event()
    copy_start_event.set()
    try:
        with pytest.raises(
            RuntimeError,
            match=(
                "snapshot_source_read_lock_timeout:paper:"
                "attach_source:paper_decision_events"
            ),
        ) as raised:
            snapshot_module.build_parallel_table_stage(
                source=source,
                destination=destination,
                table="paper_decision_events",
                role="paper_decision_events_parallel_stage",
                rule=DATABASE_SPECS["paper"]["tables"]["paper_decision_events"],
                source_page_report={
                    "source_size_bytes": source.stat().st_size,
                    "page_size": 4096,
                    "page_count": 1,
                    "freelist_count": 0,
                    "estimated_compact_bytes": source.stat().st_size,
                },
                review_lower_epoch=0,
                long_lower_epoch=0,
                upper_epoch=time.time() + 60,
                budget_bytes=1024 * 1024,
                busy_timeout_ms=5,
                max_source_read_lock_sec=300,
                start_event=start_event,
                pinned_barrier=threading.Barrier(1),
                copy_start_event=copy_start_event,
                cancel_event=threading.Event(),
            )
    finally:
        blocker.rollback()
        blocker.close()

    error_code, error_name = snapshot_module.sqlite_error_identity(raised.value)
    assert error_code is not None
    assert error_code & 0xFF in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}
    assert error_name in {"SQLITE_BUSY", "SQLITE_LOCKED", "SQLITE_BUSY_TIMEOUT"}
    destination.unlink(missing_ok=True)


def test_parallel_stage_sqlite_full_is_classified_as_stage_budget(tmp_path, monkeypatch):
    source = tmp_path / "full-paper.db"
    destination = tmp_path / "paper-decision-stage.db"
    source_db = sqlite3.connect(source)
    source_db.executescript(
        """
        CREATE TABLE paper_decision_events(
          id INTEGER PRIMARY KEY,
          event_ts REAL NOT NULL,
          payload_json TEXT
        );
        CREATE INDEX idx_pde_event_ts ON paper_decision_events(event_ts);
        INSERT INTO paper_decision_events VALUES (1, 1, '{}');
        """
    )
    source_db.commit()
    source_db.close()

    full_error = sqlite3.OperationalError("database or disk is full")
    full_error.sqlite_errorcode = sqlite3.SQLITE_FULL
    full_error.sqlite_errorname = "SQLITE_FULL"

    def fail_with_full(*_args, **_kwargs):
        raise full_error

    monkeypatch.setattr(snapshot_module, "stage_single_source_table", fail_with_full)
    start_event = threading.Event()
    start_event.set()
    copy_start_event = threading.Event()
    copy_start_event.set()
    with pytest.raises(
        RuntimeError,
        match="parallel_paper_stage_budget_exceeded:paper_decision_events",
    ) as raised:
        snapshot_module.build_parallel_table_stage(
            source=source,
            destination=destination,
            table="paper_decision_events",
            role="paper_decision_events_parallel_stage",
            rule=DATABASE_SPECS["paper"]["tables"]["paper_decision_events"],
            source_page_report={
                "source_size_bytes": source.stat().st_size,
                "page_size": 4096,
                "page_count": 1,
                "freelist_count": 0,
                "estimated_compact_bytes": source.stat().st_size,
            },
            review_lower_epoch=0,
            long_lower_epoch=0,
            upper_epoch=time.time() + 60,
            budget_bytes=1024 * 1024,
            busy_timeout_ms=5,
            max_source_read_lock_sec=300,
            start_event=start_event,
            pinned_barrier=threading.Barrier(1),
            copy_start_event=copy_start_event,
            cancel_event=threading.Event(),
        )
    assert snapshot_module.sqlite_error_identity(raised.value) == (
        sqlite3.SQLITE_FULL,
        "SQLITE_FULL",
    )


def test_concurrent_snapshot_error_preserves_public_safe_sqlite_identity():
    error = snapshot_module.ConcurrentSnapshotError(
        {
            "paper": {
                "error_code": "snapshot_source_read_lock_timeout",
                "error_type": "RuntimeError",
                "stage": "copy_table:paper_decision_events",
                "sqlite_errorcode": sqlite3.SQLITE_BUSY,
                "sqlite_errorname": "SQLITE_BUSY",
                "unsafe_message": "/app/data/private.db",
            }
        }
    )

    assert error.errors == {
        "paper": {
            "error_code": "snapshot_source_read_lock_timeout",
            "error_type": "RuntimeError",
            "stage": "copy_table:paper_decision_events",
            "sqlite_errorcode": sqlite3.SQLITE_BUSY,
            "sqlite_errorname": "SQLITE_BUSY",
        }
    }
    assert "/app/data/private.db" not in str(error)


def test_main_snapshot_sqlite_full_is_classified_as_database_budget(
    tmp_path,
    monkeypatch,
):
    sources = {
        name: Path(path)
        for name, path in create_sources(tmp_path).items()
    }
    source_reports = snapshot_module.inspect_source_page_reports(sources)
    partial_dir = tmp_path / "main-full-partial"
    partial_dir.mkdir()

    def fail_main_snapshot(*_args, **_kwargs):
        full_error = sqlite3.OperationalError("database or disk is full")
        full_error.sqlite_errorcode = sqlite3.SQLITE_FULL
        full_error.sqlite_errorname = "SQLITE_FULL"
        raise full_error

    monkeypatch.setattr(snapshot_module, "snapshot_one", fail_main_snapshot)
    stage_budget = 1024 * 1024
    with pytest.raises(snapshot_module.ConcurrentSnapshotError) as raised:
        snapshot_module.snapshot_all_concurrently(
            sources,
            partial_dir,
            source_reports,
            review_lower_epoch=0,
            long_lower_epoch=0,
            upper_epoch=time.time() + 60,
            database_budgets={name: stage_budget for name in sources},
            candidate_stage_budget_bytes=stage_budget,
            parallel_paper_stage_budget_bytes={
                table: stage_budget
                for table in source_reports["paper"][
                    "parallel_paper_stage_tables"
                ]
            },
            expected_parallel_paper_stage_tables=tuple(
                source_reports["paper"]["parallel_paper_stage_tables"]
            ),
            busy_timeout_ms=50,
            max_source_read_lock_sec=300,
        )

    assert set(raised.value.errors) == set(sources)
    full_details = [
        details
        for details in raised.value.errors.values()
        if details.get("sqlite_errorcode") == sqlite3.SQLITE_FULL
    ]
    assert full_details
    for details in full_details:
        assert details["error_code"] == (
            "selective_snapshot_exceeded_database_budget"
        )
        assert details["sqlite_errorname"] == "SQLITE_FULL"
    assert snapshot_module.snapshot_failure_code(raised.value) == (
        "selective_snapshot_exceeded_database_budget"
    )
    assert "database or disk is full" not in str(raised.value)


def test_parallel_stage_pre_barrier_failure_preserves_component_code_and_cleans(
    tmp_path,
    monkeypatch,
):
    sources = create_sources(tmp_path)
    out = tmp_path / "parallel-stage-pre-barrier-failure"
    original_builder = snapshot_module.build_parallel_table_stage

    def fail_a_class_stage(**kwargs):
        if kwargs.get("table") == "a_class_decision_events":
            raise RuntimeError("parallel_paper_stage_budget_exceeded")
        return original_builder(**kwargs)

    monkeypatch.setattr(
        snapshot_module,
        "build_parallel_table_stage",
        fail_a_class_stage,
    )
    with pytest.raises(snapshot_module.ConcurrentSnapshotError) as raised:
        build_snapshot_bundle(
            sources=sources,
            out_root=str(out),
            repo_root=str(ROOT),
            max_skew_sec=30,
            min_free_after_gib=0,
            max_output_gib=0.1,
            snapshot_id="20260101T000000Z-1234abd3",
        )

    assert raised.value.errors["paper"]["error_code"] == (
        "parallel_paper_stage_budget_exceeded"
    )
    assert raised.value.errors["paper"]["stage"] == (
        "copy_table:a_class_decision_events"
    )
    assert not (out / "current").exists()
    assert not (
        out / "snapshots" / ".20260101T000000Z-1234abd3.partial"
    ).exists()


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
        "INSERT INTO candidate_shadow_observations"
        "(id, signal_id, candidate_id, observed_at, payload_json) VALUES (?, ?, ?, ?, ?)",
        [
            (1, 1, "candidate_1", now - 60, '{"candidate_id":"candidate_1"}'),
            (2, 2, "candidate_2", now - 5 * 86400, '{"candidate_id":"candidate_2"}'),
            (3, 3, "candidate_3", now + 3600, '{"candidate_id":"candidate_3"}'),
        ],
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


def test_indexed_anchor_preserves_mixed_secondary_timestamp_formats(tmp_path):
    sources = create_sources(tmp_path)
    now = int(time.time())
    iso_past = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - 30))
    iso_future = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now + 3600))
    paper = sqlite3.connect(sources["paper"])
    paper.execute("ALTER TABLE candidate_shadow_virtual_trades ADD COLUMN signal_ts")
    paper.execute("ALTER TABLE candidate_shadow_virtual_trades ADD COLUMN entry_ts")
    paper.execute("ALTER TABLE candidate_shadow_virtual_trades ADD COLUMN exit_ts")
    paper.executemany(
        "INSERT INTO candidate_shadow_virtual_trades("
        "signal_id, observed_at, signal_ts, entry_ts, exit_ts"
        ") VALUES (?, ?, ?, ?, ?)",
        [
            (1, now - 60, now - 90, now - 45, now - 30),
            (2, now - 60, (now - 90) * 1000, (now - 45) * 1000, (now - 30) * 1000),
            (3, now - 60, iso_past, iso_past, iso_past),
            (4, now - 60, now - 90, now - 45, (now + 3600) * 1000),
            (5, now - 60, iso_past, iso_past, iso_future),
        ],
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
        assert snapshot.execute(
            "SELECT signal_id FROM candidate_shadow_virtual_trades ORDER BY signal_id"
        ).fetchall() == [(1,), (2,), (3,)]
    finally:
        snapshot.close()

    selection = report["databases"]["paper"]["selected_tables"][
        "candidate_shadow_virtual_trades"
    ]
    assert selection["predicate_strategy"] == "indexed_epoch_seconds"
    assert selection["indexed_time_anchor"] == "observed_at"
    assert selection["source_index_name"] == "idx_candidate_shadow_virtual_observed"
    assert selection["source_index_columns"] == ["observed_at"]
    assert selection["source_index_partial"] is False
    assert selection["source_query_plan_uses_index"] is True
    assert selection["source_query_plan_uses_range_search"] is True
    assert selection["source_query_plan_full_table_scan_detected"] is False
    assert any(
        "SEARCH" in detail
        and "idx_candidate_shadow_virtual_observed" in detail
        for detail in selection["source_query_plan"]
    )


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

    with pytest.raises(RuntimeError, match="snapshot_missing_required_tables:raw"):
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

    with pytest.raises(RuntimeError, match="snapshot_missing_required_watermarks:signal"):
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
        with pytest.raises(
            snapshot_module.ConcurrentSnapshotError,
            match="snapshot_source_read_lock_timeout:paper:source_page_stats",
        ) as raised:
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
    details = raised.value.errors["paper"]
    assert details["error_code"] == "snapshot_source_read_lock_timeout"
    assert details["error_type"] == "OperationalError"
    assert details["stage"] == "source_page_stats"
    assert details["sqlite_errorcode"] & 0xFF in {
        sqlite3.SQLITE_BUSY,
        sqlite3.SQLITE_LOCKED,
    }
    assert details["sqlite_errorname"] in {
        "SQLITE_BUSY",
        "SQLITE_LOCKED",
        "SQLITE_BUSY_TIMEOUT",
    }
    current = json.loads((out / "current" / "manifest.json").read_text())
    assert current["snapshot_id"] == first["snapshot_id"]
    assert not (out / "snapshots" / ".20260101T010000Z-abcdef12.partial").exists()


def snapshot_worker_args(tmp_path, sources, *, max_runs=0, snapshot_id="20260101T000000Z-1234abcd"):
    out_root = tmp_path / "worker-evidence"
    return SimpleNamespace(
        signal_db=sources["signal"],
        paper_db=sources["paper"],
        raw_db=sources["raw"],
        kline_db=sources["kline"],
        out_root=str(out_root),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        max_output_gib=0.1,
        review_history_hours=96,
        long_history_hours=840,
        source_busy_timeout_ms=30000,
        max_source_read_lock_sec=300,
        keep_previous=0,
        snapshot_id=snapshot_id,
        lock_file=str(tmp_path / "snapshot-worker.lock"),
        status_out=str(out_root / "snapshot_status.json"),
        max_runs=max_runs,
        interval_sec=21600,
        failure_retry_sec=60,
        initial_delay_sec=0,
    )


def test_snapshot_worker_status_is_atomic_and_summarizes_accepted_bundle(tmp_path, monkeypatch):
    sources = create_sources(tmp_path)
    args = snapshot_worker_args(tmp_path, sources)
    status_path = Path(args.status_out)
    original_build = snapshot_module.build_snapshot_bundle

    def inspect_running_status(**kwargs):
        running = json.loads(status_path.read_text(encoding="utf-8"))
        assert running["schema_version"] == "cross_db_evaluator_snapshot_worker_status.v1"
        assert running["running"] is True
        assert running["attempt_running"] is True
        assert running["status"] == "running"
        assert running["promotion_allowed"] is False
        return original_build(**kwargs)

    monkeypatch.setattr(snapshot_module, "build_snapshot_bundle", inspect_running_status)
    status = snapshot_module.run_snapshot_once(args)
    persisted = json.loads(status_path.read_text(encoding="utf-8"))

    assert status == persisted
    assert persisted["status"] == "completed"
    assert persisted["accepted"] is True
    assert persisted["running"] is True
    assert persisted["attempt_running"] is False
    assert persisted["last_success_at"]
    assert persisted["last_error"] is None
    accepted = persisted["last_accepted_snapshot"]
    assert accepted["accepted"] is True
    assert accepted["quick_checks_passed"] is True
    assert accepted["source_read_lock_budget_passed"] is True
    assert accepted["manifest_path"].endswith("/manifest.json")
    assert len(accepted["manifest_sha256"]) == 64
    assert accepted["manifest_sha256"] == snapshot_module.sha256_file(Path(accepted["manifest_path"]))
    assert accepted["indexed_selection"]["candidate_shadow_observations"]["predicate_strategy"] == "indexed_epoch_seconds"
    assert accepted["indexed_selection"]["candidate_shadow_virtual_trades"]["source_index_name"] == "idx_candidate_shadow_virtual_observed"
    assert persisted["next_attempt_delay_sec"] == 21600
    assert persisted["next_attempt_at"]
    assert persisted["failure_retry_sec"] == 60
    assert persisted["consecutive_failure_count"] == 0
    assert persisted["promotion_allowed"] is False


def test_snapshot_worker_failure_preserves_last_accepted_summary(tmp_path, monkeypatch):
    sources = create_sources(tmp_path)
    args = snapshot_worker_args(tmp_path, sources)
    status_path = Path(args.status_out)
    status_path.parent.mkdir(parents=True)
    previous_summary = {
        "snapshot_id": "20260101T000000Z-deadbeef",
        "accepted": True,
        "promotion_allowed": False,
    }
    status_path.write_text(
        json.dumps({
            "schema_version": "cross_db_evaluator_snapshot_worker_status.v1",
            "last_success_at": "2026-08-08T00:00:00Z",
            "last_accepted_snapshot": previous_summary,
            "error_count": 2,
        }),
        encoding="utf-8",
    )

    def fail_snapshot(**_kwargs):
        raise RuntimeError(
            "selective_snapshot_source_index_missing:"
            "candidate_shadow_observations:observed_at"
        )

    monkeypatch.setattr(snapshot_module, "build_snapshot_bundle", fail_snapshot)
    status = snapshot_module.run_snapshot_once(args)
    persisted = json.loads(status_path.read_text(encoding="utf-8"))

    assert status == persisted
    assert persisted["status"] == "failed"
    assert persisted["accepted"] is False
    assert persisted["running"] is True
    assert persisted["last_success_at"] == "2026-08-08T00:00:00Z"
    assert persisted["last_accepted_snapshot"] == previous_summary
    assert persisted["last_failure_code"] == "selective_snapshot_source_index_missing"
    assert persisted["last_failure_details"]["worker"] == {
        "error_code": "selective_snapshot_source_index_missing",
        "error_type": "RuntimeError",
        "stage": "run_snapshot_once",
    }
    assert persisted["next_attempt_delay_sec"] == 60
    assert persisted["next_attempt_at"]
    assert persisted["consecutive_failure_count"] == 1
    assert persisted["error_count"] == 3
    assert persisted["promotion_allowed"] is False


def test_concurrent_snapshot_failure_preserves_safe_database_stage_and_retries_soon(
    tmp_path,
    monkeypatch,
):
    sources = create_sources(tmp_path)
    args = snapshot_worker_args(tmp_path, sources)

    def fail_snapshot(**_kwargs):
        raise snapshot_module.ConcurrentSnapshotError({
            "paper": {
                "error_code": "source_read_lock_budget_exceeded",
                "error_type": "RuntimeError",
                "stage": "copy_table:candidate_shadow_observations",
                "copy_timing": {
                    "current_table": "candidate_shadow_observations",
                    "current_table_elapsed_sec": 12.5,
                    "source_lock_elapsed_sec": 299.9,
                    "source_lock_remaining_sec": 0.1,
                    "completed_tables": {
                        "candidate_shadow_virtual_trades": {
                            "duration_sec": 2.0,
                            "rows_copied": 10,
                            "source_lock_elapsed_sec": 4.0,
                            "source_lock_remaining_sec": 296.0,
                        }
                    },
                },
            }
        })

    monkeypatch.setattr(snapshot_module, "build_snapshot_bundle", fail_snapshot)
    status = snapshot_module.run_snapshot_once(args)

    assert status["accepted"] is False
    assert status["last_failure_code"] == "source_read_lock_budget_exceeded"
    assert status["last_failure_details"] == {
        "paper": {
            "error_code": "source_read_lock_budget_exceeded",
            "error_type": "RuntimeError",
            "stage": "copy_table:candidate_shadow_observations",
            "copy_timing": {
                "current_table": "candidate_shadow_observations",
                "current_table_elapsed_sec": 12.5,
                "source_lock_elapsed_sec": 299.9,
                "source_lock_remaining_sec": 0.1,
                "completed_tables": {
                    "candidate_shadow_virtual_trades": {
                        "duration_sec": 2.0,
                        "rows_copied": 10,
                        "source_lock_elapsed_sec": 4.0,
                        "source_lock_remaining_sec": 296.0,
                    }
                },
            },
        }
    }
    assert status["next_attempt_delay_sec"] == 60
    assert status["failure_retry_sec"] == 60
    assert status["consecutive_failure_count"] == 1
    assert snapshot_module.snapshot_next_attempt_delay_sec(
        status,
        interval_sec=21600,
        failure_retry_sec=60,
    ) == 60


def test_new_failure_code_resets_code_specific_backoff_without_hiding_history(
    tmp_path,
    monkeypatch,
):
    sources = create_sources(tmp_path)
    args = snapshot_worker_args(tmp_path, sources)
    status_path = Path(args.status_out)
    status_path.parent.mkdir(parents=True)
    status_path.write_text(
        json.dumps(
            {
                "schema_version": "cross_db_evaluator_snapshot_worker_status.v1",
                "last_failure_code": "source_read_lock_budget_exceeded",
                "consecutive_failure_count": 11,
                "error_count": 11,
                "promotion_allowed": False,
            }
        ),
        encoding="utf-8",
    )

    def fail_with_busy(**_kwargs):
        raise snapshot_module.ConcurrentSnapshotError(
            {
                "paper": {
                    "error_code": "snapshot_source_read_lock_timeout",
                    "error_type": "RuntimeError",
                    "stage": "copy_table:paper_decision_events",
                    "sqlite_errorcode": sqlite3.SQLITE_BUSY,
                    "sqlite_errorname": "SQLITE_BUSY",
                },
                "signal": {
                    "error_code": "BrokenBarrierError",
                    "error_type": "BrokenBarrierError",
                    "stage": "pinned_barrier",
                },
                "raw": {
                    "error_code": "BrokenBarrierError",
                    "error_type": "BrokenBarrierError",
                    "stage": "pinned_barrier",
                },
                "kline": {
                    "error_code": "parallel_paper_stage_cancelled",
                    "error_type": "RuntimeError",
                    "stage": "pinned_barrier",
                },
            }
        )

    monkeypatch.setattr(snapshot_module, "build_snapshot_bundle", fail_with_busy)
    first = snapshot_module.run_snapshot_once(args)
    assert first["accepted"] is False
    assert first["last_failure_code"] == "snapshot_source_read_lock_timeout"
    assert first["consecutive_failure_count"] == 12
    assert first["consecutive_failure_code_count"] == 1
    assert first["next_attempt_delay_sec"] == 60
    assert first["last_failure_details"]["paper"]["sqlite_errorcode"] == (
        sqlite3.SQLITE_BUSY
    )
    assert first["last_failure_details"]["paper"]["sqlite_errorname"] == (
        "SQLITE_BUSY"
    )

    second = snapshot_module.run_snapshot_once(args)
    assert second["consecutive_failure_count"] == 13
    assert second["consecutive_failure_code_count"] == 2
    assert second["next_attempt_delay_sec"] == 900


def test_legacy_same_code_streak_is_preserved_when_code_count_is_missing(
    tmp_path,
    monkeypatch,
):
    sources = create_sources(tmp_path)
    args = snapshot_worker_args(tmp_path, sources)
    status_path = Path(args.status_out)
    status_path.parent.mkdir(parents=True)
    status_path.write_text(
        json.dumps(
            {
                "schema_version": "cross_db_evaluator_snapshot_worker_status.v1",
                "last_failure_code": "snapshot_source_read_lock_timeout",
                "consecutive_failure_count": 4,
                "error_count": 4,
                "promotion_allowed": False,
            }
        ),
        encoding="utf-8",
    )

    def fail_with_busy(**_kwargs):
        raise snapshot_module.ConcurrentSnapshotError(
            {
                "paper": {
                    "error_code": "snapshot_source_read_lock_timeout",
                    "error_type": "RuntimeError",
                    "stage": "source_page_stats",
                    "sqlite_errorcode": sqlite3.SQLITE_BUSY,
                    "sqlite_errorname": "SQLITE_BUSY",
                }
            }
        )

    monkeypatch.setattr(snapshot_module, "build_snapshot_bundle", fail_with_busy)
    status = snapshot_module.run_snapshot_once(args)

    assert status["consecutive_failure_count"] == 5
    assert status["consecutive_failure_code_count"] == 5
    assert status["next_attempt_delay_sec"] == 21600


def test_code_specific_backoff_falls_back_to_legacy_global_count():
    assert snapshot_module.snapshot_next_attempt_delay_sec(
        {
            "accepted": False,
            "last_failure_code": "snapshot_source_read_lock_timeout",
            "consecutive_failure_count": 3,
        },
        interval_sec=21600,
        failure_retry_sec=60,
    ) == 3600


@pytest.mark.parametrize(
    ("consecutive_failure_count", "expected_delay_sec"),
    [
        (1, 60),
        (2, 900),
        (3, 3600),
        (4, 21600),
        (20, 21600),
    ],
)
def test_snapshot_failure_backoff_prevents_retry_storm(
    consecutive_failure_count,
    expected_delay_sec,
):
    status = {
        "accepted": False,
        "last_failure_code": "source_read_lock_budget_exceeded",
        "consecutive_failure_count": consecutive_failure_count,
    }
    assert snapshot_module.snapshot_next_attempt_delay_sec(
        status,
        interval_sec=21600,
        failure_retry_sec=1,
    ) == expected_delay_sec


def test_duplicate_worker_lock_contention_uses_long_retry_cadence():
    status = {
        "accepted": False,
        "last_failure_code": "evaluator_snapshot_lock_held",
        "consecutive_failure_count": 1,
    }
    assert snapshot_module.snapshot_next_attempt_delay_sec(
        status,
        interval_sec=21600,
        failure_retry_sec=60,
    ) == 21600


def test_duplicate_snapshot_worker_does_not_overwrite_active_status(tmp_path):
    sources = create_sources(tmp_path)
    args = snapshot_worker_args(tmp_path, sources)
    status_path = Path(args.status_out)
    status_path.parent.mkdir(parents=True)
    active = {
        "schema_version": "cross_db_evaluator_snapshot_worker_status.v1",
        "pid": 12345,
        "running": True,
        "attempt_running": True,
        "status": "running",
        "snapshot_id": None,
        "promotion_allowed": False,
    }
    status_path.write_text(json.dumps(active), encoding="utf-8")

    with snapshot_module.exclusive_lock(Path(args.lock_file)):
        status = snapshot_module.run_snapshot_once(args)

    assert status["accepted"] is False
    assert status["status"] == "failed"
    assert status["last_failure_code"] == "evaluator_snapshot_lock_held"
    assert status["status_artifact_preserved"] is True
    assert json.loads(status_path.read_text(encoding="utf-8")) == active


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

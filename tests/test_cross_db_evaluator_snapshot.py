import importlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
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
from evaluator_db_contract import (  # noqa: E402
    evaluator_snapshot_bundle_status,
    validate_shared_stage_estimate_contract,
)
import paper_trade_monitor as paper_monitor  # noqa: E402


@pytest.fixture(autouse=True)
def snapshot_commit(monkeypatch):
    monkeypatch.setenv("ZEABUR_GIT_COMMIT_SHA", "a" * 40)
    yield
    for out_root_key in list(snapshot_module._WORKER_OWNER_LEASES):
        snapshot_module._release_snapshot_worker_lease(Path(out_root_key))


def create_sources(root):
    definitions = {
        "signal": "CREATE TABLE premium_signals(id INTEGER, source_message_ts INTEGER)",
        "paper": (
            "CREATE TABLE candidate_shadow_observations("
            "id INTEGER PRIMARY KEY, signal_id INTEGER, candidate_id TEXT, "
            "observed_at INTEGER, payload_json TEXT);"
            "CREATE INDEX idx_candidate_shadow_obs_observed "
            "ON candidate_shadow_observations(observed_at);"
            "CREATE INDEX idx_candidate_shadow_obs_signal "
            "ON candidate_shadow_observations(signal_id);"
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
            "ON opportunity_event_path_samples(opportunity_key, sample_ts);"
            "CREATE INDEX idx_opportunity_path_samples_sample_ts "
            "ON opportunity_event_path_samples(sample_ts)"
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


def test_paper_db_startup_adds_time_first_path_sample_index_to_existing_table(
    tmp_path,
):
    path = tmp_path / "legacy-paper.db"
    connection = sqlite3.connect(path)
    connection.executescript(
        "CREATE TABLE opportunity_event_path_samples("
        "id INTEGER PRIMARY KEY, opportunity_key TEXT, sample_ts REAL, "
        "created_at REAL, updated_at REAL);"
        "CREATE INDEX idx_opportunity_path_samples_key_ts "
        "ON opportunity_event_path_samples(opportunity_key, sample_ts);"
        "INSERT INTO opportunity_event_path_samples("
        "opportunity_key, sample_ts, created_at, updated_at"
        ") VALUES ('legacy', 1, 1, 1)"
    )
    connection.close()

    initialized = paper_monitor.init_paper_db(str(path))
    try:
        columns = [
            row[2]
            for row in initialized.execute(
                "PRAGMA index_info(idx_opportunity_path_samples_sample_ts)"
            ).fetchall()
        ]
    finally:
        initialized.close()
    assert columns == ["sample_ts"]


def shared_stage_estimates(active_tables=None, *, required_bytes=None):
    tables = tuple(
        snapshot_module.PARALLEL_PAPER_STAGE_TABLES
        if active_tables is None
        else active_tables
    )
    targets = snapshot_module.shared_stage_target_names(tables)
    required_bytes = dict(required_bytes or {})
    reports = {}
    for target in targets:
        minimum = snapshot_module.shared_stage_target_minimum_bytes(target)
        reports[target] = {
            "target": target,
            "source_table": (
                snapshot_module.CANDIDATE_OBSERVATION_TABLE
                if target == snapshot_module.SHARED_STAGE_TARGET_CANDIDATE
                else target
            ),
            "strategy": "test_advisory_demand",
            "query_bounded": True,
            "physical_upper_bound_claimed": False,
            "advisory_schema_version": (
                snapshot_module.SHARED_STAGE_ADVISORY_SCHEMA_VERSION
            ),
            "advisory_formula": snapshot_module.SHARED_STAGE_ADVISORY_FORMULA,
            "advisory_required_bytes": snapshot_module.round_up_stage_page(
                required_bytes.get(target, minimum)
            ),
            "row_count_binding_mode": "exact_selected_rows",
            "selected_row_count": 0,
            "minimum_cap_bytes": minimum,
        }
    return {
        "schema_version": snapshot_module.SHARED_STAGE_BUDGET_SCHEMA_VERSION,
        "generated_at": snapshot_module.utc_iso(),
        "active_targets": list(targets),
        "all_advisory_queries_bounded": True,
        "physical_upper_bound_claimed": False,
        "targets": reports,
    }


def shared_stage_history(estimates, *, cap_hit_target=None):
    targets = {}
    for target in estimates["active_targets"]:
        high_water = int(
            estimates["targets"][target]["advisory_required_bytes"]
        )
        targets[target] = {
            "granted_cap_bytes": high_water,
            "high_water_bytes": high_water,
            "copy_completed": target != cap_hit_target,
            "cap_hit": target == cap_hit_target,
            "evidence_source": "partial_stage_files_before_cleanup",
        }
    total = sum(row["granted_cap_bytes"] for row in targets.values())
    history = {
        "schema_version": snapshot_module.SHARED_STAGE_BUDGET_SCHEMA_VERSION,
        "allocation_mode": snapshot_module.SHARED_STAGE_BUDGET_ALLOCATION_MODE,
        "hash_canonicalization": (
            snapshot_module.SHARED_STAGE_HASH_CANONICALIZATION
        ),
        "attempt_id": "previous-attempt",
        "active_targets": list(estimates["active_targets"]),
        "targets": targets,
        "total_cap_bytes": total,
        "total_granted_bytes": total,
        "grants_sum_matches_total_cap": True,
        "capacity_sufficient": True,
        "all_advisory_queries_bounded": True,
        "physical_upper_bound_claimed": False,
        "global_hard_cap_enforced": True,
        "per_target_max_page_count_enforced": True,
        "fixed_percentage_allocation_used": False,
        "cleanup_completed": True,
        "stage_files_removed": True,
        "no_unregistered_stage_files": True,
        "captured_at": "2026-08-08T00:00:00Z",
        "captured_before_cleanup": True,
        "failure_code": "parallel_paper_stage_budget_exceeded",
        "failure_components": ["paper"],
        "accepted": False,
    }
    history["plan_sha256"] = snapshot_module.shared_stage_budget_plan_sha256(
        history
    )
    history["evidence_sha256"] = (
        snapshot_module.shared_stage_budget_evidence_sha256(history)
    )
    return history


def shared_stage_history_anchor(history):
    return {
        "schema_version": (
            snapshot_module.SHARED_STAGE_HISTORY_ANCHOR_SCHEMA_VERSION
        ),
        "attempt_id": history["attempt_id"],
        "evidence_sha256": history["evidence_sha256"],
        "anchor_source": "atomic_worker_attempt_sidecar",
        "immutable": True,
    }


def test_shared_stage_hash_canonicalization_matches_cross_runtime_vector():
    payload = {
        "schema_version": "shared_stage_budget.v2",
        "allocation_mode": "history_high_water_plus_advisory_source_demand",
        "hash_canonicalization": "json_sorted_float64_bits.v1",
        "generated_at": "x",
        "capacity_sufficient": True,
        "grants_sum_matches_total_cap": True,
        "total_cap_bytes": 4096,
        "total_granted_bytes": 4096,
        "actual_total_bytes": 1024,
        "unconsumed_bytes": 3072,
        "all_targets_within_grant": True,
        "targets": {
            "t": {
                "minimum_cap_bytes": 12288,
                "average": 0.1,
                "integral_float": 1.0,
                "utilization_ratio": 0.25,
                "actual_usage_bytes": 1024,
                "sqlite_full_observed": True,
            }
        },
    }
    assert snapshot_module.shared_stage_budget_plan_sha256(payload) == (
        "60c460889746f6e5b03d7c555796c6e98961be99f4ad4717b6e1d92c02d575fb"
    )
    payload["plan_sha256"] = snapshot_module.shared_stage_budget_plan_sha256(
        payload
    )
    assert snapshot_module.shared_stage_budget_evidence_sha256(payload) == (
        "ac61bf1db4807887f4640760b0e57a5ca0e0c8a2ca90e29b068742de55fa1b49"
    )


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


def test_remaining_source_read_lock_wait_never_extends_deadline(monkeypatch):
    monkeypatch.setattr(snapshot_module.time, "monotonic", lambda: 100.0)
    assert snapshot_module.remaining_source_read_lock_wait(
        deadline_monotonic=250.0,
        database="paper",
        stage="pinned_barrier",
        limit_sec=300.0,
    ) == 150.0
    assert snapshot_module.remaining_source_read_lock_wait(
        deadline_monotonic=250.0,
        max_wait_sec=30.0,
        database="paper",
        stage="pinned_barrier",
        limit_sec=300.0,
    ) == 30.0
    assert snapshot_module.remaining_source_read_lock_wait(
        deadline_monotonic=100.25,
        database="paper",
        stage="pinned_barrier",
        limit_sec=300.0,
    ) == pytest.approx(0.25)
    with pytest.raises(
        RuntimeError,
        match=(
            "source_read_lock_budget_exceeded:paper:"
            "pinned_barrier:300.000s"
        ),
    ):
        snapshot_module.remaining_source_read_lock_wait(
            deadline_monotonic=100.0,
            database="paper",
            stage="pinned_barrier",
            limit_sec=300.0,
        )


def test_parallel_stage_cancel_grace_is_shared_and_requires_worker_restart():
    release = threading.Event()
    runtimes = {}
    for table in ("paper_decision_events", "a_class_decision_events"):
        cancel_event = threading.Event()
        start_event = threading.Event()
        copy_start_event = threading.Event()
        thread = threading.Thread(
            target=release.wait,
            name=f"stubborn-{table}",
            daemon=True,
        )
        thread.start()
        runtimes[table] = {
            "thread": thread,
            "cancel_event": cancel_event,
            "start_event": start_event,
            "copy_start_event": copy_start_event,
            "pin_barrier": threading.Barrier(1),
        }

    started = time.monotonic()
    unreaped = snapshot_module.cancel_parallel_stage_runtimes(
        runtimes,
        grace_sec=0.02,
    )
    elapsed = time.monotonic() - started
    try:
        assert unreaped == (
            "paper_decision_events",
            "a_class_decision_events",
        )
        assert elapsed < 0.2
        assert all(runtime["cancel_event"].is_set() for runtime in runtimes.values())
        assert all(runtime["start_event"].is_set() for runtime in runtimes.values())
        assert all(runtime["copy_start_event"].is_set() for runtime in runtimes.values())
    finally:
        release.set()
        for runtime in runtimes.values():
            runtime["thread"].join(timeout=1)


def test_shared_stage_estimate_timeout_is_calibrated_below_lock_budget():
    assert snapshot_module.SHARED_STAGE_INDEXED_COUNT_TIMEOUT_SEC == 20.0
    assert snapshot_module.SHARED_STAGE_ESTIMATE_TIMEOUT_SEC == 180.0
    assert (
        snapshot_module.SHARED_STAGE_INDEXED_COUNT_TIMEOUT_SEC
        < snapshot_module.SHARED_STAGE_ESTIMATE_TIMEOUT_SEC
        < snapshot_module.DEFAULT_MAX_SOURCE_READ_LOCK_SEC
    )
    assert (
        snapshot_module.SHARED_STAGE_INDEXED_COUNT_TIMEOUT_SEC
        == snapshot_module.SHARED_STAGE_DBSTAT_ADVISORY_TIMEOUT_SEC
    )


def test_shared_stage_coordinator_publishes_root_error_atomically():
    coordinator = snapshot_module.SharedStageBudgetCoordinator(
        total_cap_bytes=1024 * 1024,
        parallel_stage_tables=snapshot_module.PARALLEL_PAPER_STAGE_TABLES,
        history=None,
        attempt_id="root-error-test",
    )
    error = RuntimeError("parallel_paper_stage_budget_exceeded")
    coordinator.abort(error, target="paper_decision_events")
    target, published = coordinator.root_error()
    assert target == "paper_decision_events"
    assert published is error
    with pytest.raises(
        RuntimeError,
        match="parallel_paper_stage_budget_exceeded",
    ):
        coordinator.submit_estimate(
            snapshot_module.SHARED_STAGE_TARGET_CANDIDATE,
            {"bounded": True},
            timeout_sec=0.01,
        )


def test_shared_stage_advisory_uses_dbstat_and_indexed_count(tmp_path):
    sources = create_sources(tmp_path)
    now = int(time.time())
    paper = sqlite3.connect(sources["paper"])
    paper.executemany(
        "INSERT INTO paper_decision_events(id, event_ts) VALUES (?, ?)",
        [(index, now - index) for index in range(1, 1001)],
    )
    paper.commit()
    paper.close()

    report = snapshot_module.estimate_shared_stage_requirements(
        Path(sources["paper"]),
        parallel_stage_tables=snapshot_module.PARALLEL_PAPER_STAGE_TABLES,
        review_lower_epoch=now - 96 * 3600,
        long_lower_epoch=now - 720 * 3600,
        upper_epoch=now,
        busy_timeout_ms=30000,
    )

    p9 = report["targets"]["paper_decision_events"]
    assert report["schema_version"] == "shared_stage_budget.v2"
    assert report["all_advisory_queries_bounded"] is True
    assert report["physical_upper_bound_claimed"] is False
    assert p9["strategy"] == (
        "dbstat_proportional_advisory_with_indexed_row_count"
    )
    assert p9["query_bounded"] is True
    assert p9["physical_upper_bound_claimed"] is False
    assert p9["advisory_schema_version"] == (
        "sqlite_dbstat_advisory_demand.v1"
    )
    assert p9["advisory_formula"] == snapshot_module.SHARED_STAGE_ADVISORY_FORMULA
    assert p9["capacity_sample_used"] is False
    assert p9["selected_row_count"] == 1000
    assert p9["source_row_count_upper"] >= p9["selected_row_count"]
    assert 1 <= p9["sample_rows"] <= 256
    assert p9["average_row_bytes_diagnostic"] > 0
    assert p9["sample_max_row_bytes_diagnostic"] > 0
    assert p9["source_dbstat_physical_bytes"] == (
        p9["source_dbstat_page_count"] * p9["source_dbstat_page_size"]
    )
    assert p9["source_dbstat_payload_bytes"] <= p9[
        "source_dbstat_physical_bytes"
    ]
    assert p9["table_scaled_physical_advisory_bytes"] > 0
    assert p9["table_advisory_bytes"] >= 12288
    assert p9["source_query_plan_uses_index"] is True
    assert p9["source_query_plan_uses_range_search"] is True
    assert p9["source_query_plan_full_table_scan_detected"] is False
    assert p9["advisory_required_bytes"] == p9["table_advisory_bytes"]
    assert p9["advisory_required_bytes"] >= 12288
    path = report["targets"]["opportunity_event_path_samples"]
    assert path["strategy"] == (
        "dbstat_proportional_advisory_with_indexed_row_count"
    )
    assert path["source_index_name"] == (
        "idx_opportunity_path_samples_sample_ts"
    )
    assert path["source_query_plan_uses_range_search"] is True
    assert path["query_bounded"] is True
    assert path["physical_upper_bound_claimed"] is False


def test_dbstat_uses_attached_source_page_size_not_main_page_size():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute("ATTACH DATABASE ':memory:' AS src")
    connection.execute("PRAGMA src.page_size=65536")
    connection.execute(
        "CREATE TABLE src.t(id INTEGER PRIMARY KEY, payload BLOB)"
    )
    connection.executemany(
        "INSERT INTO src.t(payload) VALUES (?)",
        [(b"x" * 20_000,)] * 5,
    )

    report = snapshot_module.source_table_storage_report(connection, "t")

    assert connection.execute("PRAGMA src.page_size").fetchone()[0] == 65536
    assert report["page_size"] == 65536
    assert report["physical_bytes"] == report["page_count"] * 65536
    assert report["payload_bytes"] >= 100_000
    assert report["payload_bytes"] <= report["physical_bytes"]
    connection.close()


def test_dbstat_advisory_can_be_lower_than_valid_4k_stage(tmp_path):
    source = tmp_path / "source-64k.db"
    destination = tmp_path / "stage-4k.db"
    row_count = 500
    payload = b"x" * 4589

    source_db = sqlite3.connect(source)
    source_db.execute("PRAGMA page_size=65536")
    source_db.execute("VACUUM")
    source_db.execute(
        "CREATE TABLE paper_decision_events("
        "id INTEGER PRIMARY KEY, event_ts REAL, payload BLOB)"
    )
    source_db.executemany(
        "INSERT INTO paper_decision_events(event_ts,payload) VALUES(?,?)",
        ((index, payload) for index in range(row_count)),
    )
    source_db.commit()
    source_db.close()

    inspector = sqlite3.connect(":memory:", uri=True)
    inspector.row_factory = sqlite3.Row
    inspector.execute(
        "ATTACH DATABASE ? AS src",
        (f"file:{source.resolve()}?mode=ro",),
    )
    storage = snapshot_module.source_table_storage_report(
        inspector,
        "paper_decision_events",
    )
    advisory = snapshot_module.shared_stage_advisory_demand(
        target="paper_decision_events",
        selected_row_count=row_count,
        source_row_count_upper=storage["cell_upper_count"],
        storage=storage,
    )
    inspector.close()

    stage = sqlite3.connect(destination)
    stage.execute("PRAGMA page_size=4096")
    stage.execute(
        "CREATE TABLE paper_decision_events("
        "id INTEGER, event_ts REAL, payload BLOB)"
    )
    stage.execute("ATTACH DATABASE ? AS src", (str(source),))
    stage.execute(
        "INSERT INTO paper_decision_events "
        "SELECT * FROM src.paper_decision_events"
    )
    stage.commit()
    stage.execute("DETACH DATABASE src")
    stage.close()

    actual_size = destination.stat().st_size
    assert storage["page_size"] == 65536
    assert advisory["advisory_required_bytes"] < actual_size
    assert actual_size % 4096 == 0


def test_success_evidence_allows_advisory_miss_within_grant():
    estimates = shared_stage_estimates()
    baseline_total = sum(
        snapshot_module.shared_stage_target_minimum_bytes(target)
        for target in estimates["active_targets"]
    )
    plan = snapshot_module.build_shared_stage_budget_plan(
        total_cap_bytes=baseline_total + 5 * 4096,
        parallel_stage_tables=snapshot_module.PARALLEL_PAPER_STAGE_TABLES,
        estimates=estimates,
        attempt_id="advisory-miss-success",
    )
    p9 = plan["targets"]["paper_decision_events"]
    assert p9["granted_cap_bytes"] > p9["advisory_required_bytes"]
    paper_report = {
        "temporary_candidate_stage_size_bytes": 4096,
        "selected_tables": {
            "candidate_shadow_observations": {"rows_copied": 0},
        },
        "parallel_paper_stages": {},
    }
    for target in snapshot_module.PARALLEL_PAPER_STAGE_TABLES:
        target_plan = plan["targets"][target]
        actual = 4096
        if target == "paper_decision_events":
            actual = target_plan["advisory_required_bytes"] + 4096
        paper_report["parallel_paper_stages"][target] = {
            "stage_size_bytes": actual,
            "rows_copied": 0,
        }
    evidence = snapshot_module.finalize_shared_stage_budget_success(
        plan,
        paper_report,
    )
    assert evidence["accepted"] is True
    assert evidence["all_targets_within_grant"] is True
    assert evidence["targets"]["paper_decision_events"][
        "advisory_exceeded"
    ] is True
    assert evidence["targets_exceeding_advisory"] == [
        "paper_decision_events"
    ]
    assert evidence["advisory_miss_count"] == 1


def test_dbstat_advisory_keeps_edge_samples_diagnostic_only(
    tmp_path,
):
    sources = create_sources(tmp_path)
    now = int(time.time())
    giant_payload = "x" * (512 * 1024)
    rows = []
    for index in range(300):
        payload = giant_payload if index == 150 else "small"
        rows.append((index + 1, now - 300 + index, payload))
    paper = sqlite3.connect(sources["paper"])
    paper.execute(
        "ALTER TABLE paper_decision_events ADD COLUMN payload_json TEXT"
    )
    paper.executemany(
        "INSERT INTO paper_decision_events(id, event_ts, payload_json) "
        "VALUES (?, ?, ?)",
        rows,
    )
    paper.commit()
    paper.close()

    report = snapshot_module.estimate_shared_stage_requirements(
        Path(sources["paper"]),
        parallel_stage_tables=snapshot_module.PARALLEL_PAPER_STAGE_TABLES,
        review_lower_epoch=now - 96 * 3600,
        long_lower_epoch=now - 720 * 3600,
        upper_epoch=now,
        busy_timeout_ms=30000,
    )
    estimate = report["targets"]["paper_decision_events"]

    assert estimate["capacity_sample_used"] is False
    assert estimate["physical_upper_bound_claimed"] is False
    assert estimate["query_bounded"] is True
    assert estimate["sample_max_row_bytes_diagnostic"] < len(giant_payload)
    assert estimate["source_dbstat_max_payload_bytes"] >= len(giant_payload)

    page_report = snapshot_module.inspect_source_page_reports(
        {"paper": Path(sources["paper"])},
        busy_timeout_ms=30000,
    )["paper"]
    start_event = threading.Event()
    copy_start_event = threading.Event()
    cancel_event = threading.Event()
    start_event.set()
    copy_start_event.set()
    stage = snapshot_module.build_parallel_table_stage(
        source=Path(sources["paper"]),
        destination=tmp_path / ".middle-payload-stage.db",
        table="paper_decision_events",
        role="paper_decision_events_parallel_stage",
        rule=snapshot_module.DATABASE_SPECS["paper"]["tables"][
            "paper_decision_events"
        ],
        source_page_report=page_report,
        review_lower_epoch=now - 96 * 3600,
        long_lower_epoch=now - 720 * 3600,
        upper_epoch=now,
        budget_bytes=snapshot_module.round_up_stage_page(
            max(
                estimate["advisory_required_bytes"],
                estimate["source_dbstat_physical_bytes"] + 1024 * 1024,
            )
        ),
        busy_timeout_ms=30000,
        max_source_read_lock_sec=300,
        start_event=start_event,
        pinned_barrier=threading.Barrier(1),
        copy_start_event=copy_start_event,
        cancel_event=cancel_event,
    )

    assert stage["table_report"]["rows_copied"] == 300
    assert stage["stage_size_bytes"] <= stage["stage_budget_bytes"]


def test_candidate_signal_only_index_bound_ignores_huge_candidate_ids(
    tmp_path,
):
    sources = create_sources(tmp_path)
    now = int(time.time())
    paper = sqlite3.connect(sources["paper"])
    paper.executemany(
        "INSERT INTO candidate_shadow_observations("
        "id, signal_id, candidate_id, observed_at, payload_json"
        ") VALUES (?, ?, ?, ?, ?)",
        [
            (
                index + 1,
                index // 10 + 1,
                f"candidate-{index}-" + "x" * 100_000,
                now - index,
                "{}",
            )
            for index in range(100)
        ],
    )
    paper.commit()
    paper.close()

    estimate_report = snapshot_module.estimate_shared_stage_requirements(
        Path(sources["paper"]),
        parallel_stage_tables=snapshot_module.PARALLEL_PAPER_STAGE_TABLES,
        review_lower_epoch=now - 96 * 3600,
        long_lower_epoch=now - 720 * 3600,
        upper_epoch=now,
        busy_timeout_ms=30000,
    )
    estimate = estimate_report["targets"][
        "candidate_shadow_observations"
    ]
    assert estimate["candidate_order_source_index_columns"] == ["signal_id"]
    assert estimate["candidate_order_source_index_name"] == (
        "idx_candidate_shadow_obs_signal"
    )
    assert estimate["candidate_order_index_advisory_bytes"] > 0
    assert estimate["physical_upper_bound_claimed"] is False

    out = tmp_path / "candidate-wide-key-evidence"
    manifest = build_snapshot_bundle(
        sources=sources,
        out_root=str(out),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        max_output_gib=0.1,
        snapshot_id="20260101T000000Z-9abcdeff",
    )
    candidate_budget = manifest["shared_stage_budget"]["targets"][
        "candidate_shadow_observations"
    ]
    projection = manifest["databases"]["paper"]["selected_tables"][
        "candidate_shadow_observations"
    ]["storage_projection"]
    assert manifest["accepted"] is True
    assert candidate_budget["actual_usage_bytes"] <= candidate_budget[
        "granted_cap_bytes"
    ]
    assert projection["stage_order_index_name"] == (
        "idx_a3_candidate_stage_signal"
    )
    assert projection["stage_query_plan_temp_btree_detected"] is False


def test_multilevel_candidate_table_uses_exact_signal_index_row_count(
    tmp_path,
):
    sources = create_sources(tmp_path)
    now = int(time.time())
    paper = sqlite3.connect(sources["paper"])
    paper.executemany(
        "INSERT INTO candidate_shadow_observations("
        "id, signal_id, candidate_id, observed_at, payload_json"
        ") VALUES (?, ?, ?, ?, '{}')",
        (
            (index + 1, index // 10 + 1, f"candidate-{index}", now - index % 3600)
            for index in range(100_000)
        ),
    )
    paper.commit()
    paper.close()

    estimator = sqlite3.connect(":memory:", uri=True)
    estimator.row_factory = sqlite3.Row
    estimator.execute(
        "ATTACH DATABASE ? AS src",
        (f"file:{Path(sources['paper']).resolve()}?mode=ro",),
    )
    estimator.execute("BEGIN")
    estimator.execute("SELECT COUNT(*) FROM src.sqlite_master").fetchone()
    estimate = snapshot_module.estimate_shared_stage_target_requirement(
        estimator,
        "candidate_shadow_observations",
        review_lower_epoch=now - 96 * 3600,
        long_lower_epoch=now - 720 * 3600,
        upper_epoch=now,
        pinned_read_view={
            "read_view_id": "1" * 32,
            "role": "paper_main_selective_copy",
        },
    )
    estimator.rollback()
    estimator.close()
    assert estimate["source_row_count_upper_basis"] == (
        "exact_signal_index_entry_count"
    )
    assert estimate["source_row_count_upper"] == 100_000
    assert estimate["candidate_order_source_index_dbstat_cell_upper_count"] == (
        100_000
    )
    assert estimate["source_dbstat_cell_upper_count"] > 100_000

    consumer_report = {
        "advisory_required_bytes": estimate["advisory_required_bytes"],
        "advisory_strategy": estimate["strategy"],
        "advisory_evidence": estimate,
    }
    assert validate_shared_stage_estimate_contract(
        "candidate_shadow_observations",
        consumer_report,
    ) == estimate["advisory_required_bytes"]


def test_candidate_order_index_requires_exact_signal_id_key(tmp_path):
    sources = create_sources(tmp_path)
    paper = sqlite3.connect(sources["paper"])
    paper.execute("DROP INDEX idx_candidate_shadow_obs_signal")
    paper.execute(
        "CREATE INDEX idx_candidate_shadow_obs_signal_composite "
        "ON candidate_shadow_observations(signal_id, candidate_id)"
    )
    paper.commit()
    paper.close()
    now = int(time.time())

    with pytest.raises(
        RuntimeError,
        match="shared_stage_estimate_candidate_order_index_invalid",
    ):
        snapshot_module.estimate_shared_stage_requirements(
            Path(sources["paper"]),
            parallel_stage_tables=snapshot_module.PARALLEL_PAPER_STAGE_TABLES,
            review_lower_epoch=now - 96 * 3600,
            long_lower_epoch=now - 720 * 3600,
            upper_epoch=now,
            busy_timeout_ms=30000,
        )


def test_shared_stage_estimate_fails_closed_when_dbstat_is_unavailable(
    tmp_path,
    monkeypatch,
):
    sources = create_sources(tmp_path)

    def dbstat_unavailable(*_args, **_kwargs):
        raise sqlite3.OperationalError("no such table: dbstat")

    monkeypatch.setattr(
        snapshot_module,
        "source_table_storage_report",
        dbstat_unavailable,
    )
    now = int(time.time())
    with pytest.raises(
        RuntimeError,
        match=(
            "shared_stage_estimate_dbstat_unavailable:"
            "candidate_shadow_observations"
        ),
    ):
        snapshot_module.estimate_shared_stage_requirements(
            Path(sources["paper"]),
            parallel_stage_tables=snapshot_module.PARALLEL_PAPER_STAGE_TABLES,
            review_lower_epoch=now - 96 * 3600,
            long_lower_epoch=now - 720 * 3600,
            upper_epoch=now,
            busy_timeout_ms=30000,
        )


@pytest.mark.parametrize(
    "target",
    (
        snapshot_module.SHARED_STAGE_TARGET_CANDIDATE,
        "paper_decision_events",
        "opportunity_event_path_samples",
    ),
)
def test_dbstat_timeout_uses_bounded_sample_advisory_on_pinned_view(
    tmp_path,
    monkeypatch,
    target,
):
    sources = create_sources(tmp_path)
    now = int(time.time())
    source = sqlite3.connect(sources["paper"])
    if target == snapshot_module.SHARED_STAGE_TARGET_CANDIDATE:
        source.execute(
            "INSERT INTO candidate_shadow_observations("
            "id, signal_id, candidate_id, observed_at, payload_json"
            ") VALUES (1, 1, 'sample', ?, '{\"sample\":true}')",
            (now - 60,),
        )
        source_table = "candidate_shadow_observations"
    elif target == "opportunity_event_path_samples":
        source.execute(
            "INSERT INTO opportunity_event_path_samples("
            "id, opportunity_key, sample_ts, raw_payload_json, "
            "created_at, updated_at"
            ") VALUES (1, 'opp-1', ?, '{}', ?, ?)",
            (now - 60, now - 60, now - 60),
        )
        source_table = "opportunity_event_path_samples"
    else:
        source.execute(
            "INSERT INTO paper_decision_events(id, event_ts) VALUES (1, ?)",
            (now - 60,),
        )
        source_table = "paper_decision_events"
    source.commit()
    source.close()

    clock = {"dbstat_expired": False}

    def bounded_clock():
        return 1020.5 if clock["dbstat_expired"] else 1000.0

    def dbstat_timeout(*_args, **_kwargs):
        clock["dbstat_expired"] = True
        raise sqlite3.OperationalError("interrupted")

    monkeypatch.setattr(snapshot_module.time, "monotonic", bounded_clock)
    monkeypatch.setattr(
        snapshot_module,
        "source_table_storage_report",
        dbstat_timeout,
    )
    connection = sqlite3.connect(":memory:", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute(
        "ATTACH DATABASE ? AS src",
        (f"file:{Path(sources['paper']).resolve()}?mode=ro",),
    )
    connection.execute("BEGIN")
    connection.execute("SELECT COUNT(*) FROM src.sqlite_master").fetchone()
    try:
        estimate = snapshot_module.estimate_shared_stage_target_requirement(
            connection,
            target,
            review_lower_epoch=now - 96 * 3600,
            long_lower_epoch=now - 720 * 3600,
            upper_epoch=now,
            pinned_read_view={
                "read_view_id": "7" * 32,
                "role": "paper_main_selective_copy",
            },
        )
        assert connection.in_transaction is True
        assert connection.execute(
            f"SELECT COUNT(*) FROM src.{source_table}"
        ).fetchone()[0] == 1
    finally:
        connection.rollback()
        connection.close()

    assert estimate["strategy"] == (
        snapshot_module.SHARED_STAGE_SAMPLE_ADVISORY_STRATEGY
    )
    assert estimate["advisory_schema_version"] == (
        snapshot_module.SHARED_STAGE_SAMPLE_ADVISORY_SCHEMA_VERSION
    )
    assert estimate["advisory_formula"] == (
        snapshot_module.SHARED_STAGE_SAMPLE_ADVISORY_FORMULA
    )
    assert estimate["capacity_sample_used"] is True
    assert estimate["dbstat_completed"] is False
    assert estimate["dbstat_timed_out"] is True
    assert estimate["dbstat_timeout_sec"] == 20.0
    assert estimate["dbstat_elapsed_sec"] == 20.5
    assert estimate["selected_row_count"] == 1
    assert 1 <= estimate["sample_rows"] <= 256
    assert estimate["sample_row_bytes_basis"] == (
        estimate["sample_max_row_bytes_diagnostic"]
    )
    assert estimate["source_row_count_upper"] is None
    assert estimate["source_dbstat_physical_bytes"] is None
    assert estimate["table_sample_payload_advisory_bytes"] > 0
    assert estimate["physical_upper_bound_claimed"] is False
    assert estimate["source_query_plan_uses_index"] is True
    assert estimate["source_query_plan_uses_range_search"] is True
    assert estimate["source_query_plan_full_table_scan_detected"] is False
    if target == "opportunity_event_path_samples":
        assert estimate["source_index_name"] == (
            "idx_opportunity_path_samples_sample_ts"
        )

    consumer_report = {
        "advisory_required_bytes": estimate["advisory_required_bytes"],
        "advisory_strategy": estimate["strategy"],
        "advisory_evidence": estimate,
    }
    assert validate_shared_stage_estimate_contract(
        target,
        consumer_report,
    ) == estimate["advisory_required_bytes"]
    if target == snapshot_module.SHARED_STAGE_TARGET_CANDIDATE:
        inventory = shared_stage_estimates()
        inventory["targets"][target] = estimate
        plan = snapshot_module.build_shared_stage_budget_plan(
            total_cap_bytes=sum(
                row["advisory_required_bytes"]
                for row in inventory["targets"].values()
            ),
            parallel_stage_tables=snapshot_module.PARALLEL_PAPER_STAGE_TABLES,
            estimates=inventory,
            attempt_id="sample-advisory-plan",
        )
        assert validate_shared_stage_estimate_contract(
            target,
            plan["targets"][target],
        ) == estimate["advisory_required_bytes"]
    tampered = json.loads(json.dumps(consumer_report))
    tampered["advisory_evidence"][
        "table_sample_payload_advisory_bytes"
    ] += 1
    with pytest.raises(ValueError, match="sample advisory mismatch"):
        validate_shared_stage_estimate_contract(target, tampered)


@pytest.mark.parametrize(
    "target",
    (
        snapshot_module.SHARED_STAGE_TARGET_CANDIDATE,
        "paper_decision_events",
        "opportunity_event_path_samples",
    ),
)
def test_indexed_count_timeout_uses_bounded_sample_before_dbstat(
    tmp_path,
    monkeypatch,
    target,
):
    sources = create_sources(tmp_path)
    now = int(time.time())
    source = sqlite3.connect(sources["paper"])
    if target == snapshot_module.SHARED_STAGE_TARGET_CANDIDATE:
        source.execute(
            "INSERT INTO candidate_shadow_observations("
            "id, signal_id, candidate_id, observed_at, payload_json"
            ") VALUES (1, 1, 'sample', ?, '{\"sample\":true}')",
            (now - 60,),
        )
    elif target == "opportunity_event_path_samples":
        source.execute(
            "INSERT INTO opportunity_event_path_samples("
            "id, opportunity_key, sample_ts, raw_payload_json, "
            "created_at, updated_at"
            ") VALUES (1, 'opp-1', ?, '{}', ?, ?)",
            (now - 60, now - 60, now - 60),
        )
    else:
        source.execute(
            "INSERT INTO paper_decision_events(id, event_ts) VALUES (1, ?)",
            (now - 60,),
        )
    source.commit()
    source.close()

    clock = {"now": 1000.0}

    def bounded_clock():
        return clock["now"]

    def indexed_count_timeout(*_args, **_kwargs):
        clock["now"] = 1020.5
        raise sqlite3.OperationalError("interrupted")

    def unexpected_dbstat(*_args, **_kwargs):
        raise AssertionError("DBSTAT must be skipped after indexed count timeout")

    monkeypatch.setattr(snapshot_module.time, "monotonic", bounded_clock)
    monkeypatch.setattr(
        snapshot_module,
        "exact_indexed_selected_row_count",
        indexed_count_timeout,
    )
    monkeypatch.setattr(
        snapshot_module,
        "source_table_storage_report",
        unexpected_dbstat,
    )
    connection = sqlite3.connect(":memory:", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute(
        "ATTACH DATABASE ? AS src",
        (f"file:{Path(sources['paper']).resolve()}?mode=ro",),
    )
    connection.execute("BEGIN")
    connection.execute("SELECT COUNT(*) FROM src.sqlite_master").fetchone()
    try:
        estimate = snapshot_module.estimate_shared_stage_target_requirement(
            connection,
            target,
            review_lower_epoch=now - 96 * 3600,
            long_lower_epoch=now - 720 * 3600,
            upper_epoch=now,
            pinned_read_view={
                "read_view_id": "8" * 32,
                "role": "paper_main_selective_copy",
            },
        )
    finally:
        connection.rollback()
        connection.close()

    assert estimate["strategy"] == (
        snapshot_module.SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ADVISORY_STRATEGY
    )
    assert estimate["advisory_schema_version"] == (
        snapshot_module.SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ADVISORY_SCHEMA_VERSION
    )
    assert estimate["advisory_formula"] == (
        snapshot_module.SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ADVISORY_FORMULA
    )
    assert estimate["indexed_count_completed"] is False
    assert estimate["indexed_count_timed_out"] is True
    assert estimate["indexed_count_timeout_sec"] == 20.0
    assert estimate["indexed_count_elapsed_sec"] == 20.5
    assert estimate["selected_row_count"] is None
    assert estimate["sample_row_count_advisory_basis"] == estimate["sample_rows"]
    assert 1 <= estimate["sample_rows"] <= 256
    assert estimate["dbstat_completed"] is False
    assert estimate["dbstat_timed_out"] is False
    assert estimate["dbstat_elapsed_sec"] == 0.0
    assert estimate["dbstat_skipped_reason"] == "indexed_count_timeout"
    assert estimate["source_row_count_upper"] is None
    assert estimate["source_row_count_upper_basis"] == (
        "unavailable_after_bounded_index_count_timeout"
    )
    assert estimate["row_count_binding_mode"] == (
        snapshot_module.SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ROW_BINDING_MODE
    )
    assert estimate["physical_upper_bound_claimed"] is False
    assert estimate["source_query_plan_uses_index"] is True
    assert estimate["source_query_plan_uses_range_search"] is True
    assert estimate["source_query_plan_full_table_scan_detected"] is False

    consumer_report = {
        "advisory_required_bytes": estimate["advisory_required_bytes"],
        "advisory_strategy": estimate["strategy"],
        "advisory_evidence": estimate,
    }
    assert validate_shared_stage_estimate_contract(
        target,
        consumer_report,
    ) == estimate["advisory_required_bytes"]

    plan = {
        "active_targets": [target],
        "targets": {
            target: {
                "granted_cap_bytes": estimate["advisory_required_bytes"],
                "advisory_required_bytes": estimate["advisory_required_bytes"],
                "advisory_evidence": estimate,
            }
        },
    }
    if target == snapshot_module.SHARED_STAGE_TARGET_CANDIDATE:
        paper_report = {
            "temporary_candidate_stage_size_bytes": 4096,
            "selected_tables": {
                snapshot_module.CANDIDATE_OBSERVATION_TABLE: {
                    "rows_copied": 1,
                }
            },
        }
    else:
        paper_report = {
            "parallel_paper_stages": {
                target: {"stage_size_bytes": 4096, "rows_copied": 1},
            }
        }
    finalized = snapshot_module.finalize_shared_stage_budget_success(
        plan,
        paper_report,
    )
    assert finalized["targets"][target]["actual_rows_copied"] == 1
    assert finalized["targets"][target]["row_count_bound_to_snapshot"] is True

    tampered = json.loads(json.dumps(consumer_report))
    tampered["advisory_evidence"]["selected_row_count"] = 1
    with pytest.raises(ValueError, match="row claim invalid"):
        validate_shared_stage_estimate_contract(target, tampered)


@pytest.mark.parametrize(
    ("interruption", "expected_error"),
    (
        (
            "early",
            "shared_stage_estimate_timeout:opportunity_event_path_samples",
        ),
        ("cancel", "parallel_paper_stage_cancelled"),
        (
            "source_lock",
            "source_read_lock_budget_exceeded:paper:shared_stage_estimate:"
            "opportunity_event_path_samples",
        ),
        (
            "overall",
            "shared_stage_estimate_timeout:opportunity_event_path_samples",
        ),
    ),
)
def test_indexed_count_fallback_preserves_deadline_precedence(
    tmp_path,
    monkeypatch,
    interruption,
    expected_error,
):
    sources = create_sources(tmp_path)
    now = int(time.time())
    source = sqlite3.connect(sources["paper"])
    source.execute(
        "INSERT INTO opportunity_event_path_samples("
        "id, opportunity_key, sample_ts, raw_payload_json, created_at, updated_at"
        ") VALUES (1, 'opp-1', ?, '{}', ?, ?)",
        (now - 60, now - 60, now - 60),
    )
    source.commit()
    source.close()

    clock = {"now": 1000.0}
    cancel_event = threading.Event()

    def bounded_clock():
        return clock["now"]

    def interrupted_count(*_args, **_kwargs):
        if interruption == "early":
            clock["now"] = 1005.0
        elif interruption == "source_lock":
            clock["now"] = 1010.5
        elif interruption == "overall":
            clock["now"] = 1180.5
        else:
            clock["now"] = 1020.5
            cancel_event.set()
        raise sqlite3.OperationalError("interrupted")

    monkeypatch.setattr(snapshot_module.time, "monotonic", bounded_clock)
    monkeypatch.setattr(
        snapshot_module,
        "exact_indexed_selected_row_count",
        interrupted_count,
    )
    connection = sqlite3.connect(":memory:", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute(
        "ATTACH DATABASE ? AS src",
        (f"file:{Path(sources['paper']).resolve()}?mode=ro",),
    )
    connection.execute("BEGIN")
    connection.execute("SELECT COUNT(*) FROM src.sqlite_master").fetchone()
    try:
        with pytest.raises(RuntimeError, match=expected_error):
            snapshot_module.estimate_shared_stage_target_requirement(
                connection,
                "opportunity_event_path_samples",
                review_lower_epoch=now - 96 * 3600,
                long_lower_epoch=now - 720 * 3600,
                upper_epoch=now,
                pinned_read_view={
                    "read_view_id": "9" * 32,
                    "role": "paper_main_selective_copy",
                },
                lock_deadline_monotonic=(
                    1010.0 if interruption == "source_lock" else None
                ),
                cancel_event=cancel_event,
            )
    finally:
        connection.rollback()
        connection.close()


@pytest.mark.parametrize(
    (
        "failure_stage",
        "current_table",
        "post_rollback_slack_pages",
        "sqlite_errorcode",
    ),
    (
        (
            "copy_table:candidate_shadow_observations",
            "candidate_shadow_observations",
            0,
            None,
        ),
        (
            "copy_table:__a3_candidate_shadow_observation_stage",
            "",
            0,
            None,
        ),
        (
            "copy_table:candidate_shadow_observations",
            "candidate_shadow_observations",
            424,
            sqlite3.SQLITE_FULL,
        ),
    ),
)
def test_candidate_sqlite_full_is_attributed_as_cap_hit(
    tmp_path,
    failure_stage,
    current_table,
    post_rollback_slack_pages,
    sqlite_errorcode,
):
    estimates = shared_stage_estimates(
        required_bytes={
            snapshot_module.SHARED_STAGE_TARGET_CANDIDATE: 4 * 1024**2,
        }
    )
    total_cap = sum(
        row["advisory_required_bytes"]
        for row in estimates["targets"].values()
    )
    first_plan = snapshot_module.build_shared_stage_budget_plan(
        total_cap_bytes=total_cap,
        parallel_stage_tables=snapshot_module.PARALLEL_PAPER_STAGE_TABLES,
        estimates=estimates,
        attempt_id="candidate-cap-hit-attempt",
    )
    partial = tmp_path / ".candidate-cap-hit.partial"
    partial.mkdir()
    for target, report in first_plan["targets"].items():
        size = 4096
        if target == snapshot_module.SHARED_STAGE_TARGET_CANDIDATE:
            size = int(report["granted_cap_bytes"]) - (
                post_rollback_slack_pages * snapshot_module.SHARED_STAGE_PAGE_SIZE
            )
        (partial / report["stage_filename"]).write_bytes(b"x" * size)
    failure_details = {
        "error_code": "selective_snapshot_exceeded_database_budget",
        "error_type": "RuntimeError",
        "stage": failure_stage,
        "copy_timing": {
            "current_table": current_table or None,
            "completed_tables": {},
            "completed_parallel_stages": [],
        },
    }
    if sqlite_errorcode is not None:
        failure_details.update(
            {
                "sqlite_errorcode": sqlite_errorcode,
                "sqlite_errorname": "SQLITE_FULL",
            }
        )
    failure = snapshot_module.ConcurrentSnapshotError(
        {"paper": failure_details}
    )
    history = snapshot_module.capture_shared_stage_budget_failure(
        partial,
        first_plan,
        failure,
    )
    candidate = history["targets"][
        snapshot_module.SHARED_STAGE_TARGET_CANDIDATE
    ]
    assert candidate["copy_completed"] is False
    assert candidate["cap_hit"] is True
    assert candidate["high_water_bytes"] == candidate["granted_cap_bytes"] - (
        post_rollback_slack_pages * snapshot_module.SHARED_STAGE_PAGE_SIZE
    )
    history["cleanup_completed"] = True
    history["stage_files_removed"] = True
    history["evidence_sha256"] = (
        snapshot_module.shared_stage_budget_evidence_sha256(history)
    )
    history_anchor = shared_stage_history_anchor(history)
    assert snapshot_module.validated_shared_stage_budget_history(
        history,
        trusted_anchor=history_anchor,
    )[
        "accepted"
    ] is True

    next_plan = snapshot_module.build_shared_stage_budget_plan(
        total_cap_bytes=total_cap + 10 * 4096,
        parallel_stage_tables=snapshot_module.PARALLEL_PAPER_STAGE_TABLES,
        estimates=estimates,
        history=history,
        history_anchor=history_anchor,
        attempt_id="candidate-cap-hit-next",
    )
    next_candidate = next_plan["targets"][
        snapshot_module.SHARED_STAGE_TARGET_CANDIDATE
    ]
    assert next_plan["borrowing_priority_targets"] == [
        snapshot_module.SHARED_STAGE_TARGET_CANDIDATE
    ]
    assert next_candidate["history_state"] == "cap_hit"
    assert next_candidate["granted_cap_bytes"] > candidate[
        "granted_cap_bytes"
    ]


def test_failed_stage_high_water_is_reused_by_next_shared_plan(tmp_path):
    estimates = shared_stage_estimates(
        required_bytes={
            "paper_decision_events": 24576,
        }
    )
    total_cap = sum(
        row["advisory_required_bytes"]
        for row in estimates["targets"].values()
    )
    first_plan = snapshot_module.build_shared_stage_budget_plan(
        total_cap_bytes=total_cap,
        parallel_stage_tables=snapshot_module.PARALLEL_PAPER_STAGE_TABLES,
        estimates=estimates,
        attempt_id="failed-attempt",
    )
    partial = tmp_path / ".failed.partial"
    partial.mkdir()
    for target, report in first_plan["targets"].items():
        size = 4096
        if target == "paper_decision_events":
            size = int(report["granted_cap_bytes"])
        (partial / report["stage_filename"]).write_bytes(b"x" * size)
    failure = snapshot_module.ConcurrentSnapshotError(
        {
            "paper": {
                "error_code": "parallel_paper_stage_budget_exceeded",
                "error_type": "RuntimeError",
                "stage": "copy_table:paper_decision_events",
            }
        }
    )
    history = snapshot_module.capture_shared_stage_budget_failure(
        partial,
        first_plan,
        failure,
    )
    assert history is not None
    history["cleanup_completed"] = True
    history["stage_files_removed"] = True
    history["evidence_sha256"] = (
        snapshot_module.shared_stage_budget_evidence_sha256(history)
    )
    assert history["targets"]["paper_decision_events"]["cap_hit"] is True
    assert history["targets"]["paper_decision_events"]["high_water_bytes"] == (
        first_plan["targets"]["paper_decision_events"]["granted_cap_bytes"]
    )
    history_anchor = shared_stage_history_anchor(history)
    assert snapshot_module.validated_shared_stage_budget_history(
        history,
        trusted_anchor=history_anchor,
    )[
        "accepted"
    ] is True

    next_plan = snapshot_module.build_shared_stage_budget_plan(
        total_cap_bytes=total_cap + 10 * 4096,
        parallel_stage_tables=snapshot_module.PARALLEL_PAPER_STAGE_TABLES,
        estimates=estimates,
        history=history,
        history_anchor=history_anchor,
        attempt_id="next-attempt",
    )
    assert next_plan["accepted"] is True
    assert next_plan["history_used"] is True
    assert next_plan["targets"]["paper_decision_events"]["history_state"] == (
        "cap_hit"
    )
    assert next_plan["targets"]["paper_decision_events"][
        "granted_cap_bytes"
    ] > first_plan["targets"]["paper_decision_events"]["granted_cap_bytes"]
    assert next_plan["total_granted_bytes"] == next_plan["total_cap_bytes"]


def test_completed_parallel_stage_is_reused_with_completed_history_headroom(
    tmp_path,
):
    estimates = shared_stage_estimates(
        required_bytes={
            "paper_decision_events": 24576,
            "a_class_decision_events": 24576,
        }
    )
    total_cap = sum(
        row["advisory_required_bytes"]
        for row in estimates["targets"].values()
    )
    first_plan = snapshot_module.build_shared_stage_budget_plan(
        total_cap_bytes=total_cap,
        parallel_stage_tables=snapshot_module.PARALLEL_PAPER_STAGE_TABLES,
        estimates=estimates,
        attempt_id="parallel-history-attempt",
    )
    partial = tmp_path / ".parallel-history.partial"
    partial.mkdir()
    for target, report in first_plan["targets"].items():
        size = 4096
        if target == "a_class_decision_events":
            size = int(report["granted_cap_bytes"])
        (partial / report["stage_filename"]).write_bytes(b"x" * size)
    failure = snapshot_module.ConcurrentSnapshotError(
        {
            "paper": {
                "error_code": "parallel_paper_stage_budget_exceeded",
                "error_type": "RuntimeError",
                "stage": "copy_table:a_class_decision_events",
                "copy_timing": {
                    "completed_parallel_stages": [
                        "paper_decision_events",
                    ],
                    "completed_tables": {},
                },
            }
        }
    )
    history = snapshot_module.capture_shared_stage_budget_failure(
        partial,
        first_plan,
        failure,
    )
    assert history is not None
    history["cleanup_completed"] = True
    history["stage_files_removed"] = True
    history["evidence_sha256"] = (
        snapshot_module.shared_stage_budget_evidence_sha256(history)
    )
    assert history["targets"]["paper_decision_events"][
        "copy_completed"
    ] is True
    assert history["targets"]["a_class_decision_events"][
        "copy_completed"
    ] is False
    assert history["targets"]["a_class_decision_events"][
        "cap_hit"
    ] is True
    history_anchor = shared_stage_history_anchor(history)
    assert snapshot_module.validated_shared_stage_budget_history(
        history,
        trusted_anchor=history_anchor,
    )[
        "accepted"
    ] is True

    next_plan = snapshot_module.build_shared_stage_budget_plan(
        total_cap_bytes=total_cap + 20 * 4096,
        parallel_stage_tables=snapshot_module.PARALLEL_PAPER_STAGE_TABLES,
        estimates=estimates,
        history=history,
        history_anchor=history_anchor,
        attempt_id="parallel-history-next",
    )
    assert next_plan["targets"]["paper_decision_events"][
        "history_state"
    ] == "completed"
    assert next_plan["targets"]["a_class_decision_events"][
        "history_state"
    ] == "cap_hit"
    assert next_plan["targets"]["paper_decision_events"][
        "baseline_required_bytes"
    ] < next_plan["targets"]["a_class_decision_events"][
        "baseline_required_bytes"
    ]


def test_unregistered_stage_file_invalidates_high_water_history(tmp_path):
    estimates = shared_stage_estimates()
    total_cap = sum(
        row["advisory_required_bytes"]
        for row in estimates["targets"].values()
    )
    plan = snapshot_module.build_shared_stage_budget_plan(
        total_cap_bytes=total_cap,
        parallel_stage_tables=snapshot_module.PARALLEL_PAPER_STAGE_TABLES,
        estimates=estimates,
        attempt_id="rogue-file-attempt",
    )
    partial = tmp_path / ".rogue.partial"
    partial.mkdir()
    for report in plan["targets"].values():
        (partial / report["stage_filename"]).write_bytes(b"x" * 4096)
    (partial / ".rogue-stage.db").write_bytes(b"rogue")
    evidence = snapshot_module.capture_shared_stage_budget_failure(
        partial,
        plan,
        RuntimeError("parallel_paper_stage_budget_exceeded"),
    )
    assert evidence is not None
    evidence["cleanup_completed"] = True
    evidence["stage_files_removed"] = True
    evidence["evidence_sha256"] = (
        snapshot_module.shared_stage_budget_evidence_sha256(evidence)
    )
    assert evidence["no_unregistered_stage_files"] is False
    assert evidence["unregistered_stage_files"] == [".rogue-stage.db"]
    validated = snapshot_module.validated_shared_stage_budget_history(
        evidence,
        trusted_anchor=shared_stage_history_anchor(evidence),
    )
    assert validated["accepted"] is False
    assert validated["reason"] == "history_cleanup_invalid"


def test_capacity_insufficient_history_is_not_reused():
    estimates = shared_stage_estimates()
    history = shared_stage_history(estimates)
    history["capacity_sufficient"] = False
    history["plan_sha256"] = snapshot_module.shared_stage_budget_plan_sha256(
        history
    )
    history["evidence_sha256"] = (
        snapshot_module.shared_stage_budget_evidence_sha256(history)
    )
    validated = snapshot_module.validated_shared_stage_budget_history(
        history,
        trusted_anchor=shared_stage_history_anchor(history),
    )
    assert validated["accepted"] is False
    assert validated["reason"] == "history_plan_not_usable"


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    (
        ("negative_high_water", "history_capacity_invalid"),
        ("stage_files_not_removed", "history_cleanup_invalid"),
        ("required_inventory_removed", "history_inventory_invalid"),
        ("target_grant_sum_mismatch", "history_total_invalid"),
    ),
)
def test_rehashed_unsafe_history_is_rejected(mutation, expected_reason):
    estimates = shared_stage_estimates()
    history = shared_stage_history(estimates)
    p9 = history["targets"]["paper_decision_events"]
    if mutation == "negative_high_water":
        p9["high_water_bytes"] = -1
    elif mutation == "stage_files_not_removed":
        history["stage_files_removed"] = False
    elif mutation == "required_inventory_removed":
        history["active_targets"] = ["paper_decision_events"]
        history["targets"] = {"paper_decision_events": p9}
    elif mutation == "target_grant_sum_mismatch":
        p9["granted_cap_bytes"] += 4096
    else:
        raise AssertionError(mutation)
    history["plan_sha256"] = snapshot_module.shared_stage_budget_plan_sha256(
        history
    )
    history["evidence_sha256"] = (
        snapshot_module.shared_stage_budget_evidence_sha256(history)
    )
    validated = snapshot_module.validated_shared_stage_budget_history(
        history,
        trusted_anchor=shared_stage_history_anchor(history),
    )
    assert validated["accepted"] is False
    assert validated["reason"] == expected_reason


def test_tampered_high_water_history_hash_is_rejected():
    estimates = shared_stage_estimates()
    history = shared_stage_history(
        estimates,
        cap_hit_target="paper_decision_events",
    )
    history["targets"]["paper_decision_events"]["high_water_bytes"] += 4096
    validated = snapshot_module.validated_shared_stage_budget_history(
        history,
        trusted_anchor=shared_stage_history_anchor(history),
    )
    assert validated["accepted"] is False
    assert validated["reason"] == "history_evidence_hash_invalid"


def test_rehashed_high_water_substitution_mismatches_trusted_attempt_anchor():
    target = "paper_decision_events"
    estimates = shared_stage_estimates(required_bytes={target: 24576})
    history = shared_stage_history(estimates)
    trusted_anchor = shared_stage_history_anchor(history)

    history["targets"][target]["high_water_bytes"] = 16384
    history["evidence_sha256"] = (
        snapshot_module.shared_stage_budget_evidence_sha256(history)
    )

    validated = snapshot_module.validated_shared_stage_budget_history(
        history,
        trusted_anchor=trusted_anchor,
    )
    assert validated["accepted"] is False
    assert validated["reason"] == "history_anchor_mismatch"

    next_plan = snapshot_module.build_shared_stage_budget_plan(
        total_cap_bytes=history["total_cap_bytes"] + 20 * 4096,
        parallel_stage_tables=snapshot_module.PARALLEL_PAPER_STAGE_TABLES,
        estimates=estimates,
        history=history,
        history_anchor=trusted_anchor,
        attempt_id="next-after-forged-history",
    )
    assert next_plan["accepted"] is False
    assert next_plan["history_used"] is False
    assert next_plan["history_reason"] == "history_anchor_mismatch"
    assert next_plan["targets"][target]["history_state"] == "none"


def test_authoritative_consumer_reads_the_persisted_predecessor_anchor(
    tmp_path,
):
    sources = create_sources(tmp_path)
    out_root = tmp_path / "anchored-history-consumer"
    first = snapshot_module.build_snapshot_bundle(
        sources=sources,
        out_root=str(out_root),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        snapshot_id="20260101T000000Z-1234abca",
    )
    status_path = out_root / "snapshot_status.json"
    predecessor_anchor = snapshot_module.write_shared_stage_budget_anchor(
        status_path,
        first["shared_stage_budget"],
    )
    second = snapshot_module.build_snapshot_bundle(
        sources=sources,
        out_root=str(out_root),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        snapshot_id="20260101T000100Z-1234abcb",
        previous_shared_stage_budget=first["shared_stage_budget"],
        previous_shared_stage_budget_anchor=predecessor_anchor,
    )
    assert second["shared_stage_budget"]["history_used"] is True
    snapshot_module.write_shared_stage_budget_anchor(
        status_path,
        second["shared_stage_budget"],
    )
    manifest_path = (
        out_root
        / "snapshots"
        / second["snapshot_id"]
        / "manifest.json"
    ).resolve()
    snapshot_module.atomic_json(
        status_path,
        {
            "schema_version": (
                "cross_db_evaluator_snapshot_worker_status.v1"
            ),
            "status": "completed",
            "accepted": True,
            "snapshot_id": second["snapshot_id"],
            "last_accepted_snapshot": {
                "snapshot_id": second["snapshot_id"],
                "manifest_path": str(manifest_path),
                "manifest_sha256": snapshot_module.sha256_file(
                    manifest_path
                ),
            },
            "promotion_allowed": False,
        },
    )

    def bundle_status():
        return evaluator_snapshot_bundle_status(
            signal_db=str(out_root / "current" / "signal.db"),
            paper_db=str(out_root / "current" / "paper_evidence.db"),
            raw_db=str(out_root / "current" / "raw.db"),
            kline_db=str(out_root / "current" / "kline.db"),
            data_dir=str(tmp_path / "live-defaults"),
            manifest_path=str(manifest_path),
        )

    accepted = bundle_status()
    assert accepted["accepted"] is True, accepted["blockers"]

    predecessor_anchor["evidence_sha256"] = "0" * 64
    snapshot_module.atomic_json(
        snapshot_module.shared_stage_budget_anchor_path(
            status_path,
            first["shared_stage_budget"]["attempt_id"],
        ),
        predecessor_anchor,
    )
    rejected = bundle_status()
    assert rejected["accepted"] is False
    assert (
        "evaluator_snapshot_shared_stage_budget_contract_invalid"
        in rejected["blockers"]
    )


def test_disk_preflight_assigns_shared_residual_to_cap_hit_target(
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
    estimates = shared_stage_estimates(
        required_bytes={
            "candidate_shadow_observations": 1 * gib,
            "paper_decision_events": 4 * gib,
            "a_class_decision_events": 2 * gib,
            "opportunity_events": 1 * gib,
            "opportunity_event_path_samples": 2 * gib,
        }
    )
    history = shared_stage_history(
        estimates,
        cap_hit_target="paper_decision_events",
    )

    report = snapshot_module.disk_preflight(
        tmp_path,
        min_free_after_gib=5,
        max_output_gib=10,
        shared_stage_estimates=estimates,
        shared_stage_history=history,
        shared_stage_history_anchor=shared_stage_history_anchor(history),
        attempt_id="current-attempt",
    )

    expected_total_stage = 25 * gib
    shared = report["shared_stage_budget"]
    p9 = shared["targets"]["paper_decision_events"]
    assert report["accepted"] is True
    assert report["candidate_stage_budget_mode"] == "shared_stage_budget_coordinator"
    assert report["fixed_percentage_allocation_used"] is False
    assert report["candidate_stage_minimum_cap_bytes"] == 12288
    assert report["paper_decision_stage_minimum_cap_bytes"] == 12288
    assert report["temporary_stage_raw_cap_bytes"] == expected_total_stage
    assert report["temporary_stage_alignment_reserve_bytes"] == 0
    assert report["temporary_stage_total_cap_bytes"] == expected_total_stage
    assert shared["schema_version"] == "shared_stage_budget.v2"
    assert shared["allocation_mode"] == (
        "history_high_water_plus_advisory_source_demand"
    )
    assert shared["global_hard_cap_enforced"] is True
    assert shared["per_target_max_page_count_enforced"] is True
    assert shared["physical_upper_bound_claimed"] is False
    assert shared["attempt_id"] == "current-attempt"
    assert shared["history_used"] is True
    assert shared["fixed_percentage_allocation_used"] is False
    assert shared["grants_sum_matches_total_cap"] is True
    assert shared["total_granted_bytes"] == expected_total_stage
    assert sum(
        target["granted_cap_bytes"]
        for target in shared["targets"].values()
    ) == expected_total_stage
    assert p9["history_state"] == "cap_hit"
    assert p9["history_cap_hit"] is True
    assert p9["borrowed_shared_pool_bytes"] > 0
    assert p9["granted_cap_bytes"] > 4 * gib
    assert report["temporary_paper_decision_stage_cap_bytes"] == p9[
        "granted_cap_bytes"
    ]
    assert report["temporary_candidate_stage_cap_bytes"] == shared["targets"][
        "candidate_shadow_observations"
    ]["granted_cap_bytes"]
    assert report["temporary_parallel_paper_stage_cap_bytes"] == {
        table: shared["targets"][table]["granted_cap_bytes"]
        for table in snapshot_module.PARALLEL_PAPER_STAGE_TABLES
    }
    for legacy in (
        "candidate_stage_residual_share",
        "parallel_paper_stage_residual_shares",
        "parallel_paper_stage_active_weight_total",
        "candidate_stage_normalized_share",
        "parallel_paper_stage_normalized_shares",
    ):
        assert legacy not in report
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

    assert report["temporary_stage_raw_cap_bytes"] == 12287
    assert report["temporary_stage_alignment_reserve_bytes"] == 4095
    assert report["temporary_stage_total_cap_bytes"] == 8192
    assert report["shared_stage_budget"]["capacity_sufficient"] is False
    assert report["shared_stage_budget"]["accepted"] is False
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
    shared = disk["shared_stage_budget"]
    assert shared["active_targets"] == [
        "candidate_shadow_observations",
        *expected_stages,
    ]
    assert "opportunity_event_path_samples" not in shared["targets"]
    assert shared["total_granted_bytes"] == minimum_active_stage_bytes
    assert shared["grants_sum_matches_total_cap"] is True
    assert shared["fixed_percentage_allocation_used"] is False
    assert all(
        target["granted_cap_bytes"] == 12288
        for target in shared["targets"].values()
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
        CREATE INDEX idx_candidate_shadow_obs_signal
          ON candidate_shadow_observations(signal_id);
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
    assert projection["stage_order_index_name"] == "idx_a3_candidate_stage_signal"
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


def test_shared_stage_estimate_and_copy_use_same_pinned_wal_view(
    tmp_path,
    monkeypatch,
):
    sources = create_sources(tmp_path)
    now = int(time.time())
    paper_path = Path(sources["paper"])
    writer = sqlite3.connect(paper_path)
    writer.execute("PRAGMA journal_mode=WAL")
    writer.execute(
        "INSERT INTO candidate_shadow_observations("
        "id, signal_id, candidate_id, observed_at, payload_json"
        ") VALUES (1, 1, 'before-pin', ?, ?)",
        (now - 60, '{"phase":"before"}'),
    )
    writer.commit()
    writer.close()

    def standalone_estimate_must_not_run(*_args, **_kwargs):
        raise AssertionError(
            "production bundle must not use a separate estimate connection"
        )

    monkeypatch.setattr(
        snapshot_module,
        "estimate_shared_stage_requirements",
        standalone_estimate_must_not_run,
    )
    original_estimator = (
        snapshot_module.estimate_shared_stage_target_requirement
    )
    concurrent_inserted = threading.Event()

    def estimate_then_commit_concurrent_row(
        connection,
        target,
        **kwargs,
    ):
        estimate = original_estimator(connection, target, **kwargs)
        if (
            target == snapshot_module.SHARED_STAGE_TARGET_CANDIDATE
            and kwargs.get("pinned_read_view") is not None
            and not concurrent_inserted.is_set()
        ):
            concurrent_writer = sqlite3.connect(paper_path, timeout=30)
            concurrent_writer.execute("PRAGMA busy_timeout=30000")
            concurrent_writer.execute(
                "INSERT INTO candidate_shadow_observations("
                "id, signal_id, candidate_id, observed_at, payload_json"
                ") VALUES (2, 2, 'after-pin', ?, ?)",
                (now - 30, '{"phase":"after"}'),
            )
            concurrent_writer.commit()
            concurrent_writer.close()
            concurrent_inserted.set()
        return estimate

    monkeypatch.setattr(
        snapshot_module,
        "estimate_shared_stage_target_requirement",
        estimate_then_commit_concurrent_row,
    )
    manifest = build_snapshot_bundle(
        sources=sources,
        out_root=str(tmp_path / "pinned-wal-evidence"),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        max_output_gib=0.1,
        snapshot_id="20260101T000000Z-1234abcf",
    )

    assert concurrent_inserted.is_set()
    live = sqlite3.connect(paper_path)
    try:
        assert live.execute(
            "SELECT COUNT(*) FROM candidate_shadow_observations"
        ).fetchone()[0] == 2
    finally:
        live.close()

    paper_report = manifest["databases"]["paper"]
    selected = paper_report["selected_tables"][
        "candidate_shadow_observations"
    ]
    projection = selected["storage_projection"]
    budget = manifest["shared_stage_budget"]["targets"][
        "candidate_shadow_observations"
    ]
    evidence = budget["advisory_evidence"]
    paper_main_view = next(
        view
        for view in paper_report["pinned_read_views"]
        if view["role"] == "paper_main_selective_copy"
    )
    assert manifest["accepted"] is True
    assert manifest[
        "shared_stage_estimates_bound_to_copy_read_views"
    ] is True
    assert paper_report[
        "shared_stage_estimates_bound_to_copy_read_views"
    ] is True
    assert evidence["source_measurement_trust_boundary"] == (
        "same_pinned_read_view_as_copy"
    )
    assert evidence["pinned_read_view_id"] == paper_main_view[
        "read_view_id"
    ]
    assert evidence["selected_row_count"] == 1
    assert budget["actual_rows_copied"] == 1
    assert selected["rows_copied"] == 1
    assert projection["rows_copied"] == 1

    frozen = sqlite3.connect(paper_report["snapshot_path"])
    try:
        assert frozen.execute(
            "SELECT COUNT(*) FROM "
            "__a3_candidate_shadow_observation_rows"
        ).fetchone()[0] == 1
        assert frozen.execute(
            "SELECT candidate_id FROM candidate_shadow_observations"
        ).fetchall() == [("before-pin",)]
    finally:
        frozen.close()


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


def test_heavy_parallel_stage_uses_index_bounded_rowid_range_without_row_loss(
    tmp_path,
):
    source = tmp_path / "source-paper.db"
    stage_path = tmp_path / "paper-decision-stage.db"
    now = time.time()
    source_db = sqlite3.connect(source)
    source_db.executescript(
        """
        CREATE TABLE paper_decision_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          event_ts REAL NOT NULL,
          payload_json TEXT
        );
        CREATE INDEX idx_pde_event_ts ON paper_decision_events(event_ts);
        """
    )
    source_db.executemany(
        "INSERT INTO paper_decision_events(event_ts,payload_json) VALUES (?,?)",
        [
            (now - 60, '{"selected":1}'),
            (now - 10 * 86400, '{"old":true}'),
            (now + 3600, '{"future":true}'),
            (now - 30, '{"selected":2}'),
        ],
    )
    source_db.commit()
    source_db.close()

    stage = sqlite3.connect(stage_path)
    stage.row_factory = sqlite3.Row
    stage.execute("ATTACH DATABASE ? AS src", (str(source),))
    report, _deferred_indexes, _destination_schema = (
        snapshot_module.stage_single_source_table(
            stage,
            "paper_decision_events",
            DATABASE_SPECS["paper"]["tables"]["paper_decision_events"],
            review_lower_epoch=now - 96 * 3600,
            long_lower_epoch=now - 720 * 3600,
            upper_epoch=now,
        )
    )
    rows = stage.execute(
        "SELECT id,payload_json FROM paper_decision_events ORDER BY id"
    ).fetchall()
    stage.close()

    assert [tuple(row) for row in rows] == [
        (1, '{"selected":1}'),
        (4, '{"selected":2}'),
    ]
    assert report["rows_copied"] == 2
    assert report["source_copy_strategy"] == "indexed_time_bounds_then_rowid_range"
    assert report["source_copy_rowid_lower"] == 1
    assert report["source_copy_rowid_upper"] == 4
    assert report["source_copy_rowid_span"] == 4
    assert report["source_copy_time_predicate_rechecked"] is True
    assert report["source_copy_query_plan_uses_integer_primary_key_range"] is True
    assert report["source_copy_query_plan_full_table_scan_detected"] is False


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
        CREATE INDEX idx_candidate_shadow_obs_signal
          ON candidate_shadow_observations(signal_id);
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


def test_unreaped_parallel_stage_defers_cleanup_to_supervisor_restart(
    tmp_path,
    monkeypatch,
):
    sources = create_sources(tmp_path)
    out = tmp_path / "unreaped-parallel-stage-evidence"
    release = threading.Event()
    entered = threading.Event()
    original_stage = snapshot_module.stage_single_source_table

    def hold_paper_decision_stage(connection, table, rule, **kwargs):
        if table == "paper_decision_events":
            entered.set()
            release.wait(timeout=3)
        return original_stage(connection, table, rule, **kwargs)

    monkeypatch.setattr(
        snapshot_module,
        "PARALLEL_STAGE_CANCEL_GRACE_SEC",
        0.02,
    )
    monkeypatch.setattr(
        snapshot_module,
        "stage_single_source_table",
        hold_paper_decision_stage,
    )
    started = time.monotonic()
    with pytest.raises(snapshot_module.ConcurrentSnapshotError) as raised:
        build_snapshot_bundle(
            sources=sources,
            out_root=str(out),
            repo_root=str(ROOT),
            max_skew_sec=30,
            min_free_after_gib=0,
            max_output_gib=0.1,
            max_source_read_lock_sec=0.5,
            snapshot_id="20260101T000000Z-1234abd2",
        )
    elapsed = time.monotonic() - started
    partial = out / "snapshots" / ".20260101T000000Z-1234abd2.partial"
    try:
        assert entered.is_set()
        assert elapsed < 1.5
        assert raised.value.worker_restart_required is True
        assert raised.value.errors["paper"]["worker_restart_required"] is True
        assert getattr(
            raised.value,
            "cleanup_deferred_until_worker_restart",
            False,
        ) is True
        assert partial.is_dir()
        assert (
            partial
            / snapshot_module.PARALLEL_PAPER_STAGE_CONFIGS[
                "paper_decision_events"
            ]["filename"]
        ).is_file()
    finally:
        release.set()
        for thread in threading.enumerate():
            if thread.name == "snapshot-paper_decision_events-stage":
                thread.join(timeout=1)
        if partial.exists():
            shutil.rmtree(partial)
    assert not partial.exists()


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
        long_history_hours=24 * 30,
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


def test_snapshot_rejects_research_history_beyond_30_day_cap(tmp_path):
    sources = create_sources(tmp_path)

    with pytest.raises(ValueError, match="30-day research retention cap"):
        build_snapshot_bundle(
            sources=sources,
            out_root=str(tmp_path / "evidence"),
            repo_root=str(ROOT),
            min_free_after_gib=0,
            max_output_gib=0.1,
            review_history_hours=96,
            long_history_hours=721,
            snapshot_id="20260101T000000Z-1234abcd",
        )

    assert not (tmp_path / "evidence").exists()


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


def test_interrupted_partial_cleanup_is_strictly_scoped(
    tmp_path,
    monkeypatch,
):
    snapshots = tmp_path / "evidence" / "snapshots"
    interrupted = snapshots / ".20260101T010000Z-abcdef12.partial"
    protected = snapshots / "20260101T000000Z-1234abcd"
    unrelated = snapshots / ".manual.partial"
    for path in (interrupted, protected, unrelated):
        path.mkdir(parents=True)
        (path / "data").write_text("keep-or-remove", encoding="utf-8")
    write_partial_owner_fixture(interrupted)
    monkeypatch.setattr(
        snapshot_module,
        "snapshot_process_identity",
        lambda _pid: {"state": "exited", "identity": None},
    )

    removed = cleanup_interrupted_partials(tmp_path / "evidence")

    assert removed == [str(interrupted)]
    assert not interrupted.exists()
    assert protected.exists()
    assert unrelated.exists()


def test_partial_cleanup_preflights_every_owner_before_deleting_any(
    tmp_path,
    monkeypatch,
):
    snapshots = tmp_path / "evidence" / "snapshots"
    attributed = snapshots / ".20260101T010000Z-abcdef12.partial"
    unattributed = snapshots / ".20260101T020000Z-abcdef13.partial"
    for partial in (attributed, unattributed):
        partial.mkdir(parents=True)
        (partial / ".paper-decision-events-stage.db").write_bytes(b"stage")
    write_partial_owner_fixture(attributed)
    monkeypatch.setattr(
        snapshot_module,
        "snapshot_process_identity",
        lambda _pid: {"state": "exited", "identity": None},
    )

    with pytest.raises(
        snapshot_module.SnapshotWorkerOwnerInvalidError,
        match="partial_owner_missing",
    ):
        cleanup_interrupted_partials(tmp_path / "evidence")

    assert attributed.is_dir()
    assert unattributed.is_dir()


def test_partial_cleanup_blocks_while_exact_creator_identity_is_alive(
    tmp_path,
    monkeypatch,
):
    partial = (
        tmp_path
        / "evidence"
        / "snapshots"
        / ".20260101T010000Z-abcdef12.partial"
    )
    partial.mkdir(parents=True)
    creator_identity = process_identity_fixture("live-partial-creator")
    write_partial_owner_fixture(
        partial,
        pid=424242,
        process_identity=creator_identity,
    )
    monkeypatch.setattr(
        snapshot_module,
        "snapshot_process_identity",
        lambda pid: (
            {"state": "alive", "identity": creator_identity}
            if pid == 424242
            else {"state": "exited", "identity": None}
        ),
    )

    with pytest.raises(
        snapshot_module.PriorSnapshotWorkerActiveError,
        match="evaluator_snapshot_prior_worker_active",
    ):
        cleanup_interrupted_partials(tmp_path / "evidence")

    assert partial.is_dir()


def test_partial_owner_writer_rejects_a_forged_creator(
    tmp_path,
):
    partial = (
        tmp_path
        / "evidence"
        / "snapshots"
        / ".20260101T010000Z-abcdef12.partial"
    )
    partial.mkdir(parents=True)
    forged_owner = snapshot_module._worker_owner_record(
        pid=999999,
        worker_instance_id="0" * 32,
        process_identity=process_identity_fixture("forged-creator"),
        lease_identity=lease_identity_fixture(tmp_path / "evidence"),
        legacy_status_recovered=False,
    )

    with pytest.raises(
        snapshot_module.SnapshotWorkerOwnerInvalidError,
        match="partial_owner_creator_mismatch",
    ):
        snapshot_module._write_snapshot_partial_owner(
            partial,
            snapshot_id="20260101T010000Z-abcdef12",
            owner=forged_owner,
        )

    assert not snapshot_module.snapshot_partial_owner_path(partial).exists()


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
    copy_started = False

    def unexpected_copy(*_args, **_kwargs):
        nonlocal copy_started
        copy_started = True
        raise AssertionError("copy must not start when shared capacity is insufficient")

    monkeypatch.setattr(
        snapshot_module,
        "snapshot_all_concurrently",
        unexpected_copy,
    )

    with pytest.raises(
        RuntimeError,
        match="shared_stage_capacity_insufficient|insufficient disk",
    ):
        build_snapshot_bundle(
            sources=sources,
            out_root=str(out),
            repo_root=str(ROOT),
            max_skew_sec=30,
            min_free_after_gib=0,
            max_output_gib=0.1,
            snapshot_id="20260101T010000Z-abcdef12",
        )

    assert copy_started is False
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
        long_history_hours=720,
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


def process_identity_fixture(label):
    return {
        "schema_version": (
            snapshot_module.WORKER_PROCESS_IDENTITY_SCHEMA_VERSION
        ),
        "source": "ps_lstart",
        "start_time": label,
    }


def lease_identity_fixture(out_root):
    lease_path = snapshot_module.snapshot_worker_owner_lock_path(out_root)
    lease_path.parent.mkdir(parents=True, exist_ok=True)
    lease_path.touch(exist_ok=True)
    file_stat = lease_path.stat()
    return {"device": file_stat.st_dev, "inode": file_stat.st_ino}


def write_partial_owner_fixture(
    partial,
    *,
    pid=999999,
    worker_instance_id="0" * 32,
    process_identity=None,
    lease_identity=None,
):
    out_root = partial.parent.parent
    owner = snapshot_module._worker_owner_record(
        pid=pid,
        worker_instance_id=worker_instance_id,
        process_identity=(
            process_identity
            if process_identity is not None
            else process_identity_fixture("partial-owner-start")
        ),
        lease_identity=(
            lease_identity
            if lease_identity is not None
            else lease_identity_fixture(out_root)
        ),
        legacy_status_recovered=False,
    )
    snapshot_id = partial.name[1 : -len(".partial")]
    record = {
        "schema_version": snapshot_module.PARTIAL_OWNER_SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "owner": owner,
        "created_at": "2026-08-13T00:00:00Z",
    }
    assert snapshot_module._partial_owner_record_valid(
        record,
        expected_snapshot_id=snapshot_id,
    )
    snapshot_module.atomic_json(
        snapshot_module.snapshot_partial_owner_path(partial),
        record,
    )
    return owner


def test_snapshot_process_identity_binds_current_pid_to_start_time():
    observed = snapshot_module.snapshot_process_identity(os.getpid())

    assert observed["state"] == "alive"
    assert snapshot_module._valid_worker_process_identity(
        observed["identity"]
    )


def test_linux_process_identity_binds_boot_and_start_ticks(monkeypatch):
    stat_fields = ["S", *("0" for _ in range(18)), "987654", "0"]
    boot_id = "12345678-1234-1234-1234-123456789abc"

    def proc_read_text(path, *, encoding):
        assert encoding == "utf-8"
        if str(path) == "/proc/424242/stat":
            return f"424242 (worker name) {' '.join(stat_fields)}"
        if str(path) == "/proc/sys/kernel/random/boot_id":
            return f"{boot_id}\n"
        raise AssertionError(path)

    monkeypatch.setattr(snapshot_module.sys, "platform", "linux")
    monkeypatch.setattr(Path, "read_text", proc_read_text)

    observed = snapshot_module.snapshot_process_identity(424242)

    assert observed == {
        "state": "alive",
        "identity": {
            "schema_version": (
                snapshot_module.WORKER_PROCESS_IDENTITY_SCHEMA_VERSION
            ),
            "source": "linux_proc_stat",
            "boot_id": boot_id,
            "start_time_ticks": 987654,
        },
    }


def test_worker_owner_lease_remains_held_until_process_release(tmp_path):
    out_root = tmp_path / "worker-evidence"
    snapshot_module.ensure_snapshot_worker_owner(out_root)
    lease_path = snapshot_module.snapshot_worker_owner_lock_path(out_root)
    partial = (
        out_root / "snapshots" / ".20260101T000000Z-1234abcd.partial"
    )
    partial.mkdir(parents=True)
    (partial / ".paper-decision-events-stage.db").write_bytes(b"stage")
    replacement_status_path = out_root / "replacement-status.json"
    replacement = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "cross_db_evaluator_snapshot.py"),
            "--out-root",
            str(out_root),
            "--lock-file",
            str(tmp_path / "replacement.lock"),
            "--status-out",
            str(replacement_status_path),
            "--max-runs",
            "1",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=3,
    )
    replacement_status = json.loads(
        replacement_status_path.read_text(encoding="utf-8")
    )

    assert replacement.returncode == 1
    assert partial.is_dir()
    assert replacement_status["last_failure_code"] == (
        "evaluator_snapshot_prior_worker_active"
    )
    assert replacement_status["worker_restart_required"] is True
    assert replacement_status["prior_worker_liveness"] == (
        "process_lifetime_lease_held"
    )

    probe_code = (
        "import fcntl,pathlib,sys;"
        "handle=pathlib.Path(sys.argv[1]).open('r+');"
        "result='acquired';"
        "\ntry: fcntl.flock(handle.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)"
        "\nexcept BlockingIOError: result='blocked'"
        "\nprint(result)"
    )

    snapshot_module._release_snapshot_worker_lease(out_root)
    released_probe = subprocess.run(
        [sys.executable, "-c", probe_code, str(lease_path)],
        check=True,
        capture_output=True,
        text=True,
        timeout=3,
    )
    assert released_probe.stdout.strip() == "acquired"


def test_worker_owner_identity_mismatch_allows_pid_reuse_takeover(
    tmp_path,
    monkeypatch,
):
    out_root = tmp_path / "worker-evidence"
    owner_path = snapshot_module.snapshot_worker_owner_path(out_root)
    owner_path.parent.mkdir(parents=True)
    previous_identity = process_identity_fixture("previous-start")
    reused_identity = process_identity_fixture("reused-start")
    current_identity = process_identity_fixture("current-start")
    previous_owner = snapshot_module._worker_owner_record(
        pid=424242,
        worker_instance_id="0" * 32,
        process_identity=previous_identity,
        lease_identity=lease_identity_fixture(out_root),
        legacy_status_recovered=False,
    )
    snapshot_module.atomic_json(owner_path, previous_owner)

    def identity_probe(pid):
        if pid == 424242:
            return {"state": "alive", "identity": reused_identity}
        assert pid == os.getpid()
        return {"state": "alive", "identity": current_identity}

    monkeypatch.setattr(
        snapshot_module,
        "snapshot_process_identity",
        identity_probe,
    )
    claimed = snapshot_module.ensure_snapshot_worker_owner(out_root)

    assert claimed["pid"] == os.getpid()
    assert claimed["worker_instance_id"] == (
        snapshot_module.WORKER_PROCESS_INSTANCE_ID
    )
    assert claimed["process_identity"] == current_identity
    assert json.loads(owner_path.read_text(encoding="utf-8")) == claimed


def test_worker_owner_unknown_identity_fails_closed(
    tmp_path,
    monkeypatch,
):
    out_root = tmp_path / "worker-evidence"
    owner_path = snapshot_module.snapshot_worker_owner_path(out_root)
    owner_path.parent.mkdir(parents=True)
    previous_owner = snapshot_module._worker_owner_record(
        pid=424242,
        worker_instance_id="0" * 32,
        process_identity=process_identity_fixture("previous-start"),
        lease_identity=lease_identity_fixture(out_root),
        legacy_status_recovered=False,
    )
    snapshot_module.atomic_json(owner_path, previous_owner)
    monkeypatch.setattr(
        snapshot_module,
        "snapshot_process_identity",
        lambda _pid: {"state": "alive", "identity": None},
    )

    with pytest.raises(
        snapshot_module.PriorSnapshotWorkerActiveError,
        match="evaluator_snapshot_prior_worker_active",
    ) as raised:
        snapshot_module.ensure_snapshot_worker_owner(out_root)

    assert raised.value.prior_worker_liveness == "identity_unavailable"
    assert json.loads(owner_path.read_text(encoding="utf-8")) == previous_owner


def test_current_worker_owner_identity_mismatch_fails_closed(
    tmp_path,
    monkeypatch,
):
    out_root = tmp_path / "worker-evidence"
    owner_path = snapshot_module.snapshot_worker_owner_path(out_root)
    owner_path.parent.mkdir(parents=True)
    owner = snapshot_module._worker_owner_record(
        pid=os.getpid(),
        worker_instance_id=snapshot_module.WORKER_PROCESS_INSTANCE_ID,
        process_identity=process_identity_fixture("recorded-start"),
        lease_identity=lease_identity_fixture(out_root),
        legacy_status_recovered=False,
    )
    snapshot_module.atomic_json(owner_path, owner)
    monkeypatch.setattr(
        snapshot_module,
        "snapshot_process_identity",
        lambda _pid: {
            "state": "alive",
            "identity": process_identity_fixture("different-start"),
        },
    )

    with pytest.raises(
        snapshot_module.SnapshotWorkerOwnerInvalidError,
        match="current_identity_mismatch",
    ):
        snapshot_module.ensure_snapshot_worker_owner(out_root)

    assert json.loads(owner_path.read_text(encoding="utf-8")) == owner


def test_replaced_worker_lease_inode_blocks_cleanup(tmp_path, monkeypatch):
    sources = create_sources(tmp_path)
    args = snapshot_worker_args(tmp_path, sources, max_runs=0)
    out_root = Path(args.out_root)
    owner_path = snapshot_module.snapshot_worker_owner_path(out_root)
    prior_lease_identity = lease_identity_fixture(out_root)
    snapshot_module.atomic_json(
        owner_path,
        snapshot_module._worker_owner_record(
            pid=999999,
            worker_instance_id="0" * 32,
            process_identity=process_identity_fixture("exited-prior-start"),
            lease_identity=prior_lease_identity,
            legacy_status_recovered=False,
        ),
    )
    lease_path = snapshot_module.snapshot_worker_owner_lock_path(out_root)
    lease_path.unlink()
    lease_path.write_text("replacement inode\n", encoding="utf-8")
    assert lease_identity_fixture(out_root) != prior_lease_identity
    partial = (
        out_root / "snapshots" / ".20260101T000000Z-1234abcd.partial"
    )
    partial.mkdir(parents=True)
    (partial / ".paper-decision-events-stage.db").write_bytes(b"stage")
    build_called = False

    def unexpected_build(**_kwargs):
        nonlocal build_called
        build_called = True
        raise AssertionError("replaced lease inode must block before build")

    monkeypatch.setattr(
        snapshot_module,
        "build_snapshot_bundle",
        unexpected_build,
    )
    args.status_out = None
    try:
        status = snapshot_module.run_snapshot_once(args)

        assert build_called is False
        assert partial.is_dir()
        assert status["last_failure_code"] == (
            "evaluator_snapshot_worker_owner_invalid"
        )
        assert "lease_identity_mismatch" in status["last_error"]
        assert status["worker_restart_required"] is True
        assert status["cleanup_deferred_until_worker_restart"] is True
    finally:
        snapshot_module._WORKER_RESTART_POISONED_OUT_ROOTS.pop(
            str(out_root.resolve()),
            None,
        )
        if partial.exists():
            shutil.rmtree(partial)


def test_invalid_worker_owner_blocks_cleanup_and_build(tmp_path, monkeypatch):
    sources = create_sources(tmp_path)
    args = snapshot_worker_args(tmp_path, sources, max_runs=0)
    owner_path = snapshot_module.snapshot_worker_owner_path(
        Path(args.out_root)
    )
    owner_path.parent.mkdir(parents=True)
    owner_path.write_text("{not-json", encoding="utf-8")
    partial = (
        Path(args.out_root)
        / "snapshots"
        / ".20260101T000000Z-1234abcd.partial"
    )
    partial.mkdir(parents=True)
    (partial / ".paper-decision-events-stage.db").write_bytes(b"stage")
    build_called = False

    def unexpected_build(**_kwargs):
        nonlocal build_called
        build_called = True
        raise AssertionError("invalid owner must block before build")

    monkeypatch.setattr(
        snapshot_module,
        "build_snapshot_bundle",
        unexpected_build,
    )
    args.status_out = None
    try:
        status = snapshot_module.run_snapshot_once(args)

        assert build_called is False
        assert partial.is_dir()
        assert status["last_failure_code"] == (
            "evaluator_snapshot_worker_owner_invalid"
        )
        assert status["worker_restart_required"] is True
        assert status["cleanup_deferred_until_worker_restart"] is True
    finally:
        snapshot_module._WORKER_RESTART_POISONED_OUT_ROOTS.pop(
            str(Path(args.out_root).resolve()),
            None,
        )
        owner_path.unlink(missing_ok=True)
        if partial.exists():
            shutil.rmtree(partial)


def test_missing_worker_owner_with_unattributed_partial_fails_closed(
    tmp_path,
    monkeypatch,
):
    sources = create_sources(tmp_path)
    args = snapshot_worker_args(tmp_path, sources, max_runs=0)
    partial = (
        Path(args.out_root)
        / "snapshots"
        / ".20260101T000000Z-1234abcd.partial"
    )
    partial.mkdir(parents=True)
    stage_path = partial / ".paper-decision-events-stage.db"
    stage_path.write_bytes(b"stage")
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import pathlib,sys,time;"
                "handle=pathlib.Path(sys.argv[1]).open('rb');"
                "print('ready', flush=True);"
                "time.sleep(30)"
            ),
            str(stage_path),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    canonical_status_path = Path(args.out_root) / "snapshot_status.json"
    canonical_status_path.write_text(
        json.dumps(
            {
                "schema_version": snapshot_module.WORKER_STATUS_SCHEMA_VERSION,
                "pid": 999999,
                "worker_instance_id": "0" * 32,
                "status": "failed",
                "running": False,
                "attempt_running": False,
                "accepted": False,
                "worker_restart_required": True,
                "cleanup_deferred_until_worker_restart": True,
                "promotion_allowed": False,
            }
        ),
        encoding="utf-8",
    )
    build_called = False

    def unexpected_build(**_kwargs):
        nonlocal build_called
        build_called = True
        raise AssertionError("unattributed partial must block before build")

    monkeypatch.setattr(
        snapshot_module,
        "build_snapshot_bundle",
        unexpected_build,
    )
    args.status_out = None
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "ready"
        status = snapshot_module.run_snapshot_once(args)

        assert build_called is False
        assert partial.is_dir()
        assert stage_path.is_file()
        assert holder.poll() is None
        assert status["last_failure_code"] == (
            "evaluator_snapshot_worker_owner_invalid"
        )
        assert status["worker_restart_required"] is True
        assert status["cleanup_deferred_until_worker_restart"] is True
    finally:
        if holder.poll() is None:
            holder.kill()
            holder.wait(timeout=3)
        snapshot_module._WORKER_RESTART_POISONED_OUT_ROOTS.pop(
            str(Path(args.out_root).resolve()),
            None,
        )
        if partial.exists():
            shutil.rmtree(partial)


def test_unattributed_partial_does_not_bind_to_unrelated_live_legacy_status(
    tmp_path,
    monkeypatch,
):
    out_root = tmp_path / "worker-evidence"
    partial = (
        out_root / "snapshots" / ".20260101T000000Z-1234abcd.partial"
    )
    partial.mkdir(parents=True)
    (partial / ".paper-decision-events-stage.db").write_bytes(b"stage")
    unrelated_identity = process_identity_fixture("unrelated-live-start")
    monkeypatch.setattr(
        snapshot_module,
        "snapshot_process_identity",
        lambda pid: (
            {"state": "alive", "identity": unrelated_identity}
            if pid == 424242
            else {
                "state": "alive",
                "identity": process_identity_fixture("current-start"),
            }
        ),
    )
    legacy_status = {
        "schema_version": snapshot_module.WORKER_STATUS_SCHEMA_VERSION,
        "pid": 424242,
        "worker_instance_id": "0" * 32,
        "status": "failed",
        "worker_restart_required": True,
        "cleanup_deferred_until_worker_restart": True,
    }

    with pytest.raises(
        snapshot_module.SnapshotWorkerOwnerInvalidError,
        match="missing_with_interrupted_partials",
    ):
        snapshot_module.ensure_snapshot_worker_owner(
            out_root,
            legacy_statuses=(legacy_status,),
        )

    assert partial.is_dir()
    assert not snapshot_module.snapshot_worker_owner_path(out_root).exists()


def test_unreaped_parallel_stage_stops_continuous_worker_for_supervisor_restart(
    tmp_path,
    monkeypatch,
):
    sources = create_sources(tmp_path)
    args = snapshot_worker_args(tmp_path, sources, max_runs=0)

    def fail_snapshot(**_kwargs):
        raise snapshot_module.ConcurrentSnapshotError(
            {
                "paper": {
                    "error_code": "parallel_paper_stage_timeout",
                    "error_type": "RuntimeError",
                    "stage": "release_source_read_view",
                    "worker_restart_required": True,
                }
            }
        )

    monkeypatch.setattr(snapshot_module, "build_snapshot_bundle", fail_snapshot)
    status = snapshot_module.run_snapshot_once(args)

    assert status["accepted"] is False
    assert status["last_failure_code"] == "parallel_paper_stage_timeout"
    assert status["worker_restart_required"] is True
    assert status["running"] is False
    assert status["next_attempt_delay_sec"] is None
    assert status["next_attempt_at"] is None
    assert status["last_failure_details"]["paper"][
        "worker_restart_required"
    ] is True


def test_snapshot_cli_returns_one_and_stops_when_worker_restart_is_required(
    monkeypatch,
):
    attempts = []

    def failed_attempt(_args):
        attempts.append(True)
        return {
            "accepted": False,
            "worker_restart_required": True,
        }

    monkeypatch.setattr(snapshot_module, "run_snapshot_once", failed_attempt)
    monkeypatch.setattr(
        sys,
        "argv",
        ["cross_db_evaluator_snapshot.py", "--max-runs", "0"],
    )

    assert snapshot_module.main() == 1
    assert attempts == [True]


def test_run_snapshot_once_serializes_same_process_callers(monkeypatch):
    first_entered = threading.Event()
    second_entered = threading.Event()
    release_first = threading.Event()
    call_count = 0

    def bounded_attempt(_args):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            first_entered.set()
            release_first.wait(timeout=1)
        else:
            second_entered.set()
        return {"accepted": False, "worker_restart_required": False}

    monkeypatch.setattr(snapshot_module, "_run_snapshot_once", bounded_attempt)
    first = threading.Thread(
        target=snapshot_module.run_snapshot_once,
        args=(SimpleNamespace(),),
    )
    second = threading.Thread(
        target=snapshot_module.run_snapshot_once,
        args=(SimpleNamespace(),),
    )
    first.start()
    assert first_entered.wait(timeout=1)
    second.start()
    try:
        assert second_entered.wait(timeout=0.05) is False
    finally:
        release_first.set()
        first.join(timeout=1)
        second.join(timeout=1)
    assert not first.is_alive()
    assert not second.is_alive()
    assert second_entered.is_set()
    assert call_count == 2


def test_unreaped_parallel_stage_persists_failed_status_without_history_anchor(
    tmp_path,
    monkeypatch,
):
    sources = create_sources(tmp_path)
    args = snapshot_worker_args(
        tmp_path,
        sources,
        max_runs=0,
        snapshot_id="20260101T000000Z-1234abd3",
    )
    previous = snapshot_module.run_snapshot_once(args)
    assert previous["accepted"] is True
    previous_evidence = previous["shared_stage_budget"]
    previous_anchor = previous["shared_stage_budget_anchor"]
    previous_anchor_path = snapshot_module.shared_stage_budget_anchor_path(
        Path(args.status_out),
        previous_evidence["attempt_id"],
    )
    assert previous_anchor_path.is_file()
    args.snapshot_id = "20260101T010000Z-1234abd4"
    args.max_source_read_lock_sec = 0.5
    release = threading.Event()
    entered = threading.Event()
    original_stage = snapshot_module.stage_single_source_table

    def hold_paper_decision_stage(connection, table, rule, **kwargs):
        if table == "paper_decision_events":
            entered.set()
            release.wait(timeout=3)
        return original_stage(connection, table, rule, **kwargs)

    monkeypatch.setattr(
        snapshot_module,
        "PARALLEL_STAGE_CANCEL_GRACE_SEC",
        0.02,
    )
    monkeypatch.setattr(
        snapshot_module,
        "stage_single_source_table",
        hold_paper_decision_stage,
    )
    started = time.monotonic()
    try:
        status = snapshot_module.run_snapshot_once(args)
        elapsed = time.monotonic() - started
        status_path = Path(args.status_out)
        persisted = json.loads(status_path.read_text(encoding="utf-8"))
        partial = (
            Path(args.out_root)
            / "snapshots"
            / ".20260101T010000Z-1234abd4.partial"
        )

        assert entered.is_set()
        assert elapsed < 1.5
        assert status == persisted
        assert persisted["status"] == "failed"
        assert persisted["accepted"] is False
        assert persisted["running"] is False
        assert persisted["attempt_running"] is False
        assert persisted["finished_at"]
        assert persisted["last_failure_code"] == "parallel_paper_stage_timeout"
        assert persisted["worker_restart_required"] is True
        assert persisted["cleanup_deferred_until_worker_restart"] is True
        assert persisted["next_attempt_delay_sec"] is None
        assert persisted["next_attempt_at"] is None
        assert partial.is_dir()
        assert snapshot_module.snapshot_partial_owner_path(partial).is_file()
        assert str(Path(args.out_root).resolve()) in (
            snapshot_module._WORKER_OWNER_LEASES
        )
        evidence = persisted["shared_stage_budget"]
        assert evidence["cleanup_completed"] is False
        assert evidence["stage_files_removed"] is False
        assert persisted["shared_stage_budget_anchor"] is None
        assert not snapshot_module.shared_stage_budget_anchor_path(
            status_path,
            evidence["attempt_id"],
        ).exists()
        assert snapshot_module.read_json_object(previous_anchor_path) == previous_anchor
        assert snapshot_module.validated_shared_stage_budget_history(
            evidence,
            trusted_anchor=None,
        )["accepted"] is False
        reentered_build = False

        def unexpected_same_process_reentry(**_kwargs):
            nonlocal reentered_build
            reentered_build = True
            raise RuntimeError("same_process_restart_poison_bypassed")

        monkeypatch.setattr(
            snapshot_module,
            "build_snapshot_bundle",
            unexpected_same_process_reentry,
        )
        reentered_status = snapshot_module.run_snapshot_once(args)
        reentered_persisted = json.loads(
            status_path.read_text(encoding="utf-8")
        )
        assert reentered_build is False
        assert reentered_status == persisted
        assert reentered_persisted == persisted
        out_root_key = str(Path(args.out_root).resolve())
        snapshot_module._WORKER_RESTART_POISONED_OUT_ROOTS.pop(
            out_root_key,
            None,
        )
        persisted_guard_status = snapshot_module.run_snapshot_once(args)
        assert persisted_guard_status == persisted
        alternate_status_path = Path(args.out_root) / "alternate-status.json"
        args.status_out = str(alternate_status_path)
        alternate_status = snapshot_module.run_snapshot_once(args)
        assert alternate_status == persisted
        assert json.loads(
            alternate_status_path.read_text(encoding="utf-8")
        ) == persisted
        args.status_out = None
        assert snapshot_module.run_snapshot_once(args) == persisted
        assert partial.is_dir()
        assert any(
            thread.name == "snapshot-paper_decision_events-stage"
            and thread.is_alive()
            for thread in threading.enumerate()
        )
    finally:
        release.set()
        for thread in threading.enumerate():
            if thread.name == "snapshot-paper_decision_events-stage":
                thread.join(timeout=1)
        snapshot_module._WORKER_RESTART_POISONED_OUT_ROOTS.pop(
            str(Path(args.out_root).resolve()),
            None,
        )
        if partial.exists():
            shutil.rmtree(partial)
    assert not partial.exists()


def test_unreaped_stage_status_write_failure_poison_precedes_persistence(
    tmp_path,
    monkeypatch,
):
    sources = create_sources(tmp_path)
    args = snapshot_worker_args(
        tmp_path,
        sources,
        max_runs=0,
        snapshot_id="20260101T000000Z-1234abd5",
    )
    assert snapshot_module.run_snapshot_once(args)["accepted"] is True
    args.snapshot_id = "20260101T010000Z-1234abd6"
    args.max_source_read_lock_sec = 0.5
    status_path = Path(args.status_out)
    partial = (
        Path(args.out_root)
        / "snapshots"
        / ".20260101T010000Z-1234abd6.partial"
    )
    out_root_key = str(Path(args.out_root).resolve())
    release = threading.Event()
    entered = threading.Event()
    original_stage = snapshot_module.stage_single_source_table
    original_atomic_json = snapshot_module.atomic_json
    restart_status_write_failed = False

    def hold_paper_decision_stage(connection, table, rule, **kwargs):
        if table == "paper_decision_events":
            entered.set()
            release.wait(timeout=3)
        return original_stage(connection, table, rule, **kwargs)

    def fail_first_restart_required_status(path, payload):
        nonlocal restart_status_write_failed
        if (
            not restart_status_write_failed
            and Path(path) == status_path
            and payload.get("worker_restart_required") is True
        ):
            restart_status_write_failed = True
            raise OSError("injected_restart_status_write_failure")
        return original_atomic_json(path, payload)

    monkeypatch.setattr(
        snapshot_module,
        "PARALLEL_STAGE_CANCEL_GRACE_SEC",
        0.02,
    )
    monkeypatch.setattr(
        snapshot_module,
        "stage_single_source_table",
        hold_paper_decision_stage,
    )
    monkeypatch.setattr(
        snapshot_module,
        "atomic_json",
        fail_first_restart_required_status,
    )
    try:
        started = time.monotonic()
        with pytest.raises(
            OSError,
            match="injected_restart_status_write_failure",
        ):
            snapshot_module.run_snapshot_once(args)
        elapsed = time.monotonic() - started

        assert entered.is_set()
        assert restart_status_write_failed is True
        assert elapsed < 1.5
        assert partial.is_dir()
        assert snapshot_module.snapshot_partial_owner_path(partial).is_file()
        assert any(
            thread.name == "snapshot-paper_decision_events-stage"
            and thread.is_alive()
            for thread in threading.enumerate()
        )
        stale_status = json.loads(status_path.read_text(encoding="utf-8"))
        assert stale_status["worker_restart_required"] is False
        assert stale_status["attempt_running"] is True
        poisoned_status = (
            snapshot_module._WORKER_RESTART_POISONED_OUT_ROOTS[out_root_key]
        )
        assert poisoned_status["worker_restart_required"] is True
        assert poisoned_status["cleanup_deferred_until_worker_restart"] is True

        reentered_build = False

        def unexpected_same_process_reentry(**_kwargs):
            nonlocal reentered_build
            reentered_build = True
            raise RuntimeError("status_write_failure_poison_bypassed")

        monkeypatch.setattr(
            snapshot_module,
            "build_snapshot_bundle",
            unexpected_same_process_reentry,
        )
        reentered_status = snapshot_module.run_snapshot_once(args)
        assert reentered_build is False
        assert reentered_status == poisoned_status
        assert json.loads(status_path.read_text(encoding="utf-8")) == poisoned_status
        assert partial.is_dir()

        poison_map_before_reload = (
            snapshot_module._WORKER_RESTART_POISONED_OUT_ROOTS
        )
        owner_leases_before_reload = snapshot_module._WORKER_OWNER_LEASES
        run_lock_before_reload = snapshot_module._RUN_SNAPSHOT_ONCE_LOCK
        worker_instance_before_reload = (
            snapshot_module.WORKER_PROCESS_INSTANCE_ID
        )
        assert importlib.reload(snapshot_module) is snapshot_module
        assert (
            snapshot_module.WORKER_PROCESS_INSTANCE_ID
            == worker_instance_before_reload
        )
        assert (
            snapshot_module._WORKER_RESTART_POISONED_OUT_ROOTS
            is poison_map_before_reload
        )
        assert snapshot_module._WORKER_OWNER_LEASES is owner_leases_before_reload
        assert snapshot_module._RUN_SNAPSHOT_ONCE_LOCK is run_lock_before_reload

        reentered_build = False
        monkeypatch.setattr(
            snapshot_module,
            "build_snapshot_bundle",
            unexpected_same_process_reentry,
        )
        args.status_out = None
        assert snapshot_module.run_snapshot_once(args) == poisoned_status
        assert reentered_build is False
        assert partial.is_dir()
        alternate_status_path = Path(args.out_root) / "reload-status.json"
        args.status_out = str(alternate_status_path)
        assert snapshot_module.run_snapshot_once(args) == poisoned_status
        assert reentered_build is False
        assert json.loads(
            alternate_status_path.read_text(encoding="utf-8")
        ) == poisoned_status
        assert partial.is_dir()
    finally:
        release.set()
        for thread in threading.enumerate():
            if thread.name == "snapshot-paper_decision_events-stage":
                thread.join(timeout=1)
        snapshot_module._WORKER_RESTART_POISONED_OUT_ROOTS.pop(
            out_root_key,
            None,
        )
        if partial.exists():
            shutil.rmtree(partial)
    assert not partial.exists()


def test_new_worker_instance_cleans_deferred_partial_before_attempt(
    tmp_path,
    monkeypatch,
):
    sources = create_sources(tmp_path)
    args = snapshot_worker_args(tmp_path, sources, max_runs=0)
    status_path = Path(args.status_out)
    status_path.parent.mkdir(parents=True)
    prior_instance = (
        "0" * 32
        if snapshot_module.WORKER_PROCESS_INSTANCE_ID != "0" * 32
        else "1" * 32
    )
    status_path.write_text(
        json.dumps(
            {
                "schema_version": snapshot_module.WORKER_STATUS_SCHEMA_VERSION,
                "pid": 999999,
                "worker_instance_id": prior_instance,
                "status": "failed",
                "running": False,
                "attempt_running": False,
                "accepted": False,
                "worker_restart_required": True,
                "cleanup_deferred_until_worker_restart": True,
                "promotion_allowed": False,
            }
        ),
        encoding="utf-8",
    )
    prior_owner = snapshot_module._worker_owner_record(
        pid=999999,
        worker_instance_id=prior_instance,
        process_identity=process_identity_fixture("exited-prior-start"),
        lease_identity=lease_identity_fixture(Path(args.out_root)),
        legacy_status_recovered=False,
    )
    snapshot_module.atomic_json(
        snapshot_module.snapshot_worker_owner_path(Path(args.out_root)),
        prior_owner,
    )
    partial = (
        Path(args.out_root)
        / "snapshots"
        / ".20260101T000000Z-1234abcd.partial"
    )
    partial.mkdir(parents=True)
    (partial / ".paper-decision-events-stage.db").write_bytes(b"stage")
    write_partial_owner_fixture(
        partial,
        pid=prior_owner["pid"],
        worker_instance_id=prior_owner["worker_instance_id"],
        process_identity=prior_owner["process_identity"],
        lease_identity=prior_owner["lease_identity"],
    )
    observed_partial_at_build = []

    def stop_replacement_after_startup_cleanup(**_kwargs):
        observed_partial_at_build.append(partial.exists())
        raise RuntimeError("replacement_process_probe_stop")

    monkeypatch.setattr(
        snapshot_module,
        "build_snapshot_bundle",
        stop_replacement_after_startup_cleanup,
    )
    status = snapshot_module.run_snapshot_once(args)

    assert observed_partial_at_build == [False]
    assert not partial.exists()
    assert status["status"] == "failed"
    assert status["worker_restart_required"] is False
    assert status["cleanup_deferred_until_worker_restart"] is False
    assert status["worker_instance_id"] == (
        snapshot_module.WORKER_PROCESS_INSTANCE_ID
    )


def test_stale_valid_root_owner_cannot_authorize_unattributed_live_partial(
    tmp_path,
    monkeypatch,
):
    sources = create_sources(tmp_path)
    args = snapshot_worker_args(tmp_path, sources, max_runs=0)
    args.status_out = None
    out_root = Path(args.out_root)
    prior_instance = (
        "0" * 32
        if snapshot_module.WORKER_PROCESS_INSTANCE_ID != "0" * 32
        else "1" * 32
    )
    prior_owner = snapshot_module._worker_owner_record(
        pid=999999,
        worker_instance_id=prior_instance,
        process_identity=process_identity_fixture("exited-root-owner-start"),
        lease_identity=lease_identity_fixture(out_root),
        legacy_status_recovered=False,
    )
    snapshot_module.atomic_json(
        snapshot_module.snapshot_worker_owner_path(out_root),
        prior_owner,
    )
    partial = (
        out_root / "snapshots" / ".20260101T000000Z-1234abcd.partial"
    )
    partial.mkdir(parents=True)
    stage_path = partial / ".paper-decision-events-stage.db"
    stage_path.write_bytes(b"x" * (1024 * 1024))
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import pathlib,sys,time;"
                "handle=pathlib.Path(sys.argv[1]).open('rb');"
                "print('ready', flush=True);"
                "time.sleep(30)"
            ),
            str(stage_path),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    original_identity = snapshot_module.snapshot_process_identity
    monkeypatch.setattr(
        snapshot_module,
        "snapshot_process_identity",
        lambda pid: (
            {"state": "exited", "identity": None}
            if pid == 999999
            else original_identity(pid)
        ),
    )
    build_called = False

    def unexpected_build(**_kwargs):
        nonlocal build_called
        build_called = True
        raise AssertionError("unattributed partial must block before build")

    monkeypatch.setattr(
        snapshot_module,
        "build_snapshot_bundle",
        unexpected_build,
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "ready"
        status = snapshot_module.run_snapshot_once(args)

        assert build_called is False
        assert partial.is_dir()
        assert stage_path.is_file()
        assert holder.poll() is None
        assert status["last_failure_code"] == (
            "evaluator_snapshot_worker_owner_invalid"
        )
        assert "partial_owner_missing" in status["last_error"]
        assert status["worker_restart_required"] is True
        assert status["cleanup_deferred_until_worker_restart"] is True
    finally:
        if holder.poll() is None:
            holder.kill()
            holder.wait(timeout=3)
        snapshot_module._WORKER_RESTART_POISONED_OUT_ROOTS.pop(
            str(out_root.resolve()),
            None,
        )
        if partial.exists():
            shutil.rmtree(partial)


def test_replacement_worker_preserves_partial_while_prior_worker_is_alive(
    tmp_path,
    monkeypatch,
):
    sources = create_sources(tmp_path)
    args = snapshot_worker_args(tmp_path, sources, max_runs=0)
    status_path = Path(args.status_out)
    status_path.parent.mkdir(parents=True)
    prior_instance = (
        "0" * 32
        if snapshot_module.WORKER_PROCESS_INSTANCE_ID != "0" * 32
        else "1" * 32
    )
    partial = (
        Path(args.out_root)
        / "snapshots"
        / ".20260101T000000Z-1234abcd.partial"
    )
    partial.mkdir(parents=True)
    stage_path = partial / ".paper-decision-events-stage.db"
    stage_path.write_bytes(b"stage")
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import pathlib,sys,time;"
                "handle=pathlib.Path(sys.argv[1]).open('rb');"
                "print('ready', flush=True);"
                "time.sleep(30)"
            ),
            str(stage_path),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    build_observations = []

    def replacement_build_probe(**_kwargs):
        build_observations.append(partial.exists())
        raise RuntimeError("replacement_process_probe_stop")

    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "ready"
        status_path.write_text(
            json.dumps(
                {
                    "schema_version": snapshot_module.WORKER_STATUS_SCHEMA_VERSION,
                    "pid": holder.pid,
                    "worker_instance_id": prior_instance,
                    "status": "failed",
                    "running": False,
                    "attempt_running": False,
                    "accepted": False,
                    "worker_restart_required": True,
                    "cleanup_deferred_until_worker_restart": True,
                    "promotion_allowed": False,
                }
            ),
            encoding="utf-8",
        )
        holder_identity = snapshot_module.snapshot_process_identity(
            holder.pid
        )["identity"]
        assert snapshot_module._valid_worker_process_identity(holder_identity)
        owner_path = snapshot_module.snapshot_worker_owner_path(
            Path(args.out_root)
        )
        holder_owner = snapshot_module._worker_owner_record(
            pid=holder.pid,
            worker_instance_id=prior_instance,
            process_identity=holder_identity,
            lease_identity=lease_identity_fixture(Path(args.out_root)),
            legacy_status_recovered=False,
        )
        snapshot_module.atomic_json(
            owner_path,
            holder_owner,
        )
        write_partial_owner_fixture(
            partial,
            pid=holder_owner["pid"],
            worker_instance_id=holder_owner["worker_instance_id"],
            process_identity=holder_owner["process_identity"],
            lease_identity=holder_owner["lease_identity"],
        )
        monkeypatch.setattr(
            snapshot_module,
            "build_snapshot_bundle",
            replacement_build_probe,
        )

        # The production status can be overwritten by the failed replacement;
        # the canonical owner record must retain the actual old PID.
        blocked = snapshot_module.run_snapshot_once(args)

        assert build_observations == []
        assert partial.is_dir()
        assert stage_path.is_file()
        assert holder.poll() is None
        assert blocked["status"] == "failed"
        assert blocked["accepted"] is False
        assert blocked["last_failure_code"] == (
            "evaluator_snapshot_prior_worker_active"
        )
        assert blocked["worker_restart_required"] is True
        assert blocked["cleanup_deferred_until_worker_restart"] is True
        assert json.loads(status_path.read_text(encoding="utf-8")) == blocked
        blocked_owner = json.loads(owner_path.read_text(encoding="utf-8"))
        assert blocked_owner["pid"] == holder.pid
        assert blocked_owner["legacy_status_recovered"] is False

        snapshot_module._WORKER_RESTART_POISONED_OUT_ROOTS.pop(
            str(Path(args.out_root).resolve()),
            None,
        )
        args.status_out = None
        no_status_blocked = snapshot_module.run_snapshot_once(args)
        assert build_observations == []
        assert partial.is_dir()
        assert no_status_blocked["last_failure_code"] == (
            "evaluator_snapshot_prior_worker_active"
        )

        snapshot_module._WORKER_RESTART_POISONED_OUT_ROOTS.pop(
            str(Path(args.out_root).resolve()),
            None,
        )
        alternate_status_path = Path(args.out_root) / "replacement-status.json"
        args.status_out = str(alternate_status_path)
        alternate_blocked = snapshot_module.run_snapshot_once(args)
        assert build_observations == []
        assert partial.is_dir()
        assert alternate_blocked["last_failure_code"] == (
            "evaluator_snapshot_prior_worker_active"
        )
        assert json.loads(
            alternate_status_path.read_text(encoding="utf-8")
        ) == alternate_blocked

        holder.terminate()
        holder.wait(timeout=3)
        snapshot_module._WORKER_RESTART_POISONED_OUT_ROOTS.pop(
            str(Path(args.out_root).resolve()),
            None,
        )
        args.status_out = None
        replacement = snapshot_module.run_snapshot_once(args)
        assert build_observations == [False]
        assert not partial.exists()
        assert replacement["worker_restart_required"] is False
        replacement_owner = json.loads(
            owner_path.read_text(encoding="utf-8")
        )
        assert replacement_owner["pid"] == os.getpid()
        assert replacement_owner["legacy_status_recovered"] is False
    finally:
        if holder.poll() is None:
            holder.kill()
            holder.wait(timeout=3)
        snapshot_module._WORKER_RESTART_POISONED_OUT_ROOTS.pop(
            str(Path(args.out_root).resolve()),
            None,
        )
        if partial.exists():
            shutil.rmtree(partial)


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
    assert not (
        Path(accepted["manifest_path"]).parent
        / snapshot_module.PARTIAL_OWNER_FILENAME
    ).exists()
    assert accepted["indexed_selection"]["candidate_shadow_observations"]["predicate_strategy"] == "indexed_epoch_seconds"
    assert accepted["indexed_selection"]["candidate_shadow_virtual_trades"]["source_index_name"] == "idx_candidate_shadow_virtual_observed"
    shared = persisted["shared_stage_budget"]
    assert shared["schema_version"] == "shared_stage_budget.v2"
    assert shared["accepted"] is True
    assert shared["cleanup_completed"] is True
    assert shared["no_unregistered_stage_files"] is True
    assert shared["total_granted_bytes"] == shared["total_cap_bytes"]
    assert all(
        target["within_grant"] is True
        for target in shared["targets"].values()
    )
    assert accepted["shared_stage_budget"]["schema_version"] == (
        "shared_stage_budget.v2"
    )
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


def test_snapshot_worker_persists_shared_high_water_for_next_retry(
    tmp_path,
    monkeypatch,
):
    sources = create_sources(tmp_path)
    args = snapshot_worker_args(tmp_path, sources)
    estimates = shared_stage_estimates()
    total_cap = sum(
        row["advisory_required_bytes"]
        for row in estimates["targets"].values()
    )
    evidence = snapshot_module.build_shared_stage_budget_plan(
        total_cap_bytes=total_cap,
        parallel_stage_tables=snapshot_module.PARALLEL_PAPER_STAGE_TABLES,
        estimates=estimates,
        attempt_id="failed-worker-attempt",
    )
    evidence["accepted"] = False
    evidence["captured_at"] = snapshot_module.utc_iso()
    evidence["captured_before_cleanup"] = True
    evidence["failure_code"] = "parallel_paper_stage_budget_exceeded"
    evidence["failure_components"] = ["paper"]
    evidence["cleanup_completed"] = True
    evidence["no_unregistered_stage_files"] = True
    evidence["unregistered_stage_files"] = []
    evidence["actual_total_bytes"] = 0
    for target, report in evidence["targets"].items():
        high_water = 4096
        if target == "paper_decision_events":
            high_water = int(report["granted_cap_bytes"])
        report.update(
            {
                "actual_usage_bytes": high_water,
                "high_water_bytes": high_water,
                "copy_completed": target != "paper_decision_events",
                "cap_hit": target == "paper_decision_events",
                "within_grant": True,
                "utilization_ratio": high_water
                / int(report["granted_cap_bytes"]),
                "evidence_source": "partial_stage_files_before_cleanup",
            }
        )
        evidence["actual_total_bytes"] += high_water
    evidence["unconsumed_bytes"] = (
        int(evidence["total_cap_bytes"])
        - int(evidence["actual_total_bytes"])
    )
    evidence["all_targets_within_grant"] = True
    evidence["stage_files_removed"] = True
    evidence["evidence_sha256"] = (
        snapshot_module.shared_stage_budget_evidence_sha256(evidence)
    )

    def fail_snapshot(**_kwargs):
        exc = RuntimeError(
            "parallel_paper_stage_budget_exceeded:paper_decision_events"
        )
        setattr(exc, "shared_stage_budget", evidence)
        raise exc

    monkeypatch.setattr(snapshot_module, "build_snapshot_bundle", fail_snapshot)
    first = snapshot_module.run_snapshot_once(args)
    assert first["accepted"] is False
    assert first["shared_stage_budget"] == evidence
    assert first["shared_stage_budget"]["targets"][
        "paper_decision_events"
    ]["cap_hit"] is True
    persisted_anchor = snapshot_module.read_json_object(
        snapshot_module.shared_stage_budget_anchor_path(
            Path(args.status_out),
            first["shared_stage_budget"]["attempt_id"],
        )
    )
    assert snapshot_module.validated_shared_stage_budget_history(
        first["shared_stage_budget"],
        trusted_anchor=persisted_anchor,
    )["accepted"] is True

    observed_history = None
    observed_anchor = None

    def inspect_history(**kwargs):
        nonlocal observed_history, observed_anchor
        observed_history = kwargs.get("previous_shared_stage_budget")
        observed_anchor = kwargs.get(
            "previous_shared_stage_budget_anchor"
        )
        raise RuntimeError("shared_stage_capacity_insufficient")

    monkeypatch.setattr(snapshot_module, "build_snapshot_bundle", inspect_history)
    snapshot_module.run_snapshot_once(args)
    assert observed_history == evidence
    assert observed_anchor == persisted_anchor


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

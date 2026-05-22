#!/usr/bin/env python3
"""Run a v2.7 mirror -> projection -> read-model smoke check.

The default CLI writes to a temp event-log/read-model directory so it can be
used after deploy without mutating the active production event log.
"""

import argparse
import json
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_event_log import V27EventLog  # noqa: E402
from v27_mirror_lifecycle_tracks import DEFAULT_LIFECYCLE_DB, run_mirror_once as run_lifecycle_mirror_once  # noqa: E402
from v27_mirror_paper_decisions import DEFAULT_DB as DEFAULT_PAPER_DB, run_mirror_once as run_paper_decision_mirror_once  # noqa: E402
from v27_mirror_source_labels import DEFAULT_DB as DEFAULT_SIGNAL_DB, run_mirror_once as run_source_label_mirror_once  # noqa: E402
from v27_mirror_telegram_signals import run_mirror_once as run_telegram_signal_mirror_once  # noqa: E402
from v27_read_model_refresh import refresh_denominator_read_model  # noqa: E402


def _default_smoke_root():
    return Path(tempfile.gettempdir()) / f"v27_pipeline_smoke_{int(time.time())}"


def _mirror_has_failure(result):
    if not isinstance(result, dict):
        return True
    for key in ("mirror", "missed_mirror"):
        summary = result.get(key)
        if summary and summary.get("failed"):
            return True
    for key in ("verify", "missed_verify"):
        summary = result.get(key)
        if summary and not summary.get("parity_ok"):
            return True
    return False


def _run_step(name, fn, args):
    try:
        result = fn(args)
    except Exception as exc:
        return {
            "name": name,
            "ok": False,
            "error": str(exc),
            "result": None,
        }
    return {
        "name": name,
        "ok": not _mirror_has_failure(result),
        "error": None,
        "result": result,
    }


def run_pipeline_smoke(
    *,
    signal_db=DEFAULT_SIGNAL_DB,
    paper_db=DEFAULT_PAPER_DB,
    lifecycle_db=DEFAULT_LIFECYCLE_DB,
    event_log_dir=None,
    output_dir=None,
    limit=5,
    include_missed=True,
    spec_manifest=None,
):
    smoke_root = _default_smoke_root()
    event_log_dir = Path(event_log_dir) if event_log_dir else smoke_root / "events"
    output_dir = Path(output_dir) if output_dir else smoke_root / "read_models"
    signal_db = Path(signal_db)
    paper_db = Path(paper_db)
    lifecycle_db = Path(lifecycle_db)
    limit = max(1, int(limit))

    steps = {}
    steps["telegram_signals"] = _run_step(
        "telegram_signals",
        run_telegram_signal_mirror_once,
        SimpleNamespace(
            signal_db=str(signal_db),
            event_log_dir=str(event_log_dir),
            since_id=None,
            until_id=None,
            limit=limit,
            dry_run=False,
            table="premium_signals",
            default_chain="solana",
            new_only=True,
        ),
    )
    steps["source_labels"] = _run_step(
        "source_labels",
        run_source_label_mirror_once,
        SimpleNamespace(
            db=str(signal_db),
            event_log_dir=str(event_log_dir),
            since_id=None,
            until_id=None,
            limit=limit,
            dry_run=False,
            table="signal_features",
            new_only=True,
        ),
    )
    steps["paper_decisions"] = _run_step(
        "paper_decisions",
        run_paper_decision_mirror_once,
        SimpleNamespace(
            db=str(paper_db),
            event_log_dir=str(event_log_dir),
            since_id=None,
            until_id=None,
            limit=limit,
            missed_since_id=None,
            missed_until_id=None,
            missed_limit=limit,
            dry_run=False,
            include_missed=include_missed,
            new_only=True,
        ),
    )
    steps["lifecycle_tracks"] = _run_step(
        "lifecycle_tracks",
        run_lifecycle_mirror_once,
        SimpleNamespace(
            lifecycle_db=str(lifecycle_db),
            event_log_dir=str(event_log_dir),
            since_id=None,
            until_id=None,
            limit=limit,
            dry_run=False,
            table="tracks",
            default_chain="solana",
            new_only=True,
        ),
    )

    event_log_verify = V27EventLog(event_log_dir).verify()
    refresh_kwargs = {
        "event_log_dir": event_log_dir,
        "projection_path": output_dir / "denominator_projection.json",
        "snapshot_path": output_dir / "denominator_snapshot.json",
        "health_path": output_dir / "denominator_freshness.json",
        "max_snapshot_age_ms": 300_000,
    }
    if spec_manifest:
        refresh_kwargs["spec_manifest_path"] = Path(spec_manifest)
    refresh = refresh_denominator_read_model(**refresh_kwargs)

    blocking_reasons = []
    for name, step in steps.items():
        if not step.get("ok"):
            blocking_reasons.append(f"{name}_mirror_failed")
    if event_log_verify.get("event_count", 0) <= 0:
        blocking_reasons.append("event_log_empty")
    if not refresh.get("health", {}).get("dashboard_safe"):
        blocking_reasons.append("read_model_not_dashboard_safe")

    return {
        "smoke_schema_version": "v2.7.0.pipeline_smoke.v1",
        "signal_db": str(signal_db),
        "paper_db": str(paper_db),
        "lifecycle_db": str(lifecycle_db),
        "event_log_dir": str(event_log_dir),
        "output_dir": str(output_dir),
        "limit": limit,
        "include_missed": bool(include_missed),
        "steps": steps,
        "event_log_verify": event_log_verify,
        "refresh": refresh,
        "blocking_reasons": blocking_reasons,
        "health": {
            "status": "v27_pipeline_smoke_ok" if not blocking_reasons else "v27_pipeline_smoke_failed",
            "dashboard_safe": not blocking_reasons,
            "normal_tiny_ready": False,
        },
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--signal-db", default=str(DEFAULT_SIGNAL_DB))
    parser.add_argument("--paper-db", default=str(DEFAULT_PAPER_DB))
    parser.add_argument("--lifecycle-db", default=str(DEFAULT_LIFECYCLE_DB))
    parser.add_argument("--event-log-dir")
    parser.add_argument("--output-dir")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--include-missed", action="store_true")
    parser.add_argument("--spec-manifest")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    report = run_pipeline_smoke(
        signal_db=args.signal_db,
        paper_db=args.paper_db,
        lifecycle_db=args.lifecycle_db,
        event_log_dir=args.event_log_dir,
        output_dir=args.output_dir,
        limit=args.limit,
        include_missed=args.include_missed,
        spec_manifest=args.spec_manifest,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    if args.strict and not report.get("health", {}).get("dashboard_safe"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()

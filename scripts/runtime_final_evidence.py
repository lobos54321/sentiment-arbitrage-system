#!/usr/bin/env python3
"""Research-only runtime evidence writer for final fullnet blockers.

Best-effort append-only JSONL. Defaults to the Zeabur data volume when the
shell env is not propagated into a Python worker.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import time


FINAL_MODULES = (
    "gmgn_policy",
    "source_resonance",
    "worker_health",
    "training_manifest",
    "holdout_negative_controls",
    "assumptions_false_negative_budget",
)

DEFAULT_RUNTIME_FINAL_EVIDENCE_LOG = "/app/data/runtime_final_evidence.jsonl"

REQUIRED_FIELDS = {
    "gmgn_policy": ("gmgn_policy_decision", "gmgn_policy_reason", "gmgn_policy_source", "gmgn_policy_version"),
    "source_resonance": ("gmgn_first_seen_ts", "gmgn_last_seen_ts", "lead_time_sec", "resonance_source", "resonance_score", "timestamp_valid"),
    "worker_health": ("worker_name", "worker_status", "heartbeat_ts", "provider_status", "error_count_window", "degraded_reason"),
    "training_manifest": ("manifest_id", "feature_schema_version", "model_or_ruleset_version", "generated_at_ts", "training_window_start_ts", "training_window_end_ts"),
    "holdout_negative_controls": ("holdout_id", "holdout_window_start_ts", "holdout_window_end_ts", "negative_control_name", "control_result", "leakage_check_pass"),
    "assumptions_false_negative_budget": ("budget_id", "false_negative_budget_n", "false_negative_budget_pct", "observed_false_negative_n", "budget_status", "assumption_version"),
}


def _ts(value):
    if value in (None, ""):
        return None
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    if n > 1_000_000_000_000:
        n = n / 1000
    return int(n)


def _payload_hash(payload):
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _missing(module_group, identity, payload):
    fields = ["token_ca", "signal_ts"] + list(REQUIRED_FIELDS.get(module_group, ()))
    values = {**(identity or {}), **(payload or {})}
    return [field for field in fields if values.get(field) in (None, "")]


def build_runtime_final_evidence_row(module_group, identity, payload, *, source="runtime", evidence_ts=None):
    if module_group not in FINAL_MODULES:
        raise ValueError(f"unknown runtime final evidence module: {module_group}")
    identity = dict(identity or {})
    payload = dict(payload or {})
    missing = _missing(module_group, identity, payload)
    if missing:
        raise ValueError(f"missing required fields for {module_group}: {','.join(missing)}")
    payload_hash = _payload_hash(payload)
    return {
        "schema_version": "runtime_final_evidence_raw.v1",
        "module_group": module_group,
        "token_ca": identity.get("token_ca"),
        "signal_ts": _ts(identity.get("signal_ts")),
        "premium_signal_id": identity.get("premium_signal_id"),
        "evidence_ts": _ts(evidence_ts) or int(time.time()),
        "source": source,
        "payload_hash": payload_hash,
        "payload_json": json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str),
        **{field: payload.get(field) for field in REQUIRED_FIELDS[module_group]},
    }


def emit_runtime_final_evidence(module_group, identity, payload, *, source="runtime", evidence_ts=None, path=None, fatal=False):
    """Append evidence if configured. Trading path safe: returns status instead of throwing by default."""
    out_path = path or os.environ.get("RUNTIME_FINAL_EVIDENCE_LOG") or DEFAULT_RUNTIME_FINAL_EVIDENCE_LOG
    if not out_path:
        return {"emitted": False, "reason": "runtime_final_evidence_log_not_configured"}
    try:
        row = build_runtime_final_evidence_row(module_group, identity, payload, source=source, evidence_ts=evidence_ts)
        target = Path(out_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, sort_keys=True, separators=(",", ":"), default=str) + "\n")
        return {"emitted": True, "path": str(target), "module_group": module_group}
    except Exception as exc:  # noqa: BLE001 - best-effort runtime telemetry
        if fatal:
            raise
        return {"emitted": False, "reason": str(exc)[:500]}


def _read_jsonl(path):
    if not path or not Path(path).exists():
        return []
    text = Path(path).read_text(encoding="utf-8").strip()
    return [json.loads(line) for line in text.splitlines() if line.strip()] if text else []


def _cohort_keys(fullnet_row_path):
    keys = set()
    for row in _read_jsonl(fullnet_row_path):
        keys.add((row.get("token_ca"), _ts(row.get("signal_ts"))))
    return keys


def export_runtime_final_evidence(*, raw_log, fullnet_row, window_start_ts, window_end_ts, out):
    start = _ts(window_start_ts)
    end = _ts(window_end_ts)
    if start is None or end is None or start >= end:
        raise ValueError("valid window_start_ts/window_end_ts required")
    cohort = _cohort_keys(fullnet_row)
    rows = []
    for row in _read_jsonl(raw_log):
        evidence_ts = _ts(row.get("evidence_ts"))
        key = (row.get("token_ca"), _ts(row.get("signal_ts")))
        if key not in cohort or evidence_ts is None or evidence_ts < start or evidence_ts > end:
            continue
        payload = json.loads(row.get("payload_json") or "{}")
        rows.append({
            **row,
            "window_start_ts": start,
            "window_end_ts": end,
            "join_confidence": "HIGH",
            **{field: payload.get(field) for field in REQUIRED_FIELDS[row["module_group"]]},
        })
    rows.sort(key=lambda r: (r.get("module_group") or "", r.get("token_ca") or "", r.get("signal_ts") or 0))
    target = Path(out)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("".join(json.dumps(row, sort_keys=True, separators=(",", ":"), default=str) + "\n" for row in rows), encoding="utf-8")
    return {"exported": len(rows), "out": str(target)}


def main(argv=None):
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    exp = sub.add_parser("export")
    exp.add_argument("--raw-log", required=True)
    exp.add_argument("--fullnet-row", required=True)
    exp.add_argument("--window-start-ts", required=True)
    exp.add_argument("--window-end-ts", required=True)
    exp.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    if args.cmd == "export":
        print(json.dumps(export_runtime_final_evidence(
            raw_log=args.raw_log,
            fullnet_row=args.fullnet_row,
            window_start_ts=args.window_start_ts,
            window_end_ts=args.window_end_ts,
            out=args.out,
        ), sort_keys=True))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Evaluate Markov shadow forecasts against later trade outcome labels."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any, Mapping


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_event_log import V27EventLog, sha256_hex  # noqa: E402
from v27_record_markov_shadow_forecasts import DEFAULT_EVENT_LOG_DIR, MARKOV_SHADOW_EVENT_TYPE  # noqa: E402


TRADE_OUTCOME_EVENT_TYPE = "trade_outcome_label_recorded"
REPORT_SCHEMA_VERSION = "v2.7.0.markov_shadow_calibration_report.v1"
DEFAULT_HIGH_THRESHOLD = 0.55
DEFAULT_LOW_THRESHOLD = 0.45
DEFAULT_MIN_SAMPLE = 30


def _payload(event: Mapping[str, Any]) -> dict[str, Any]:
    payload = event.get("payload")
    return payload if isinstance(payload, dict) else {}


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _dedup_key(payload: Mapping[str, Any]) -> str:
    if payload.get("denominator_dedup_key"):
        return str(payload["denominator_dedup_key"])
    chain = payload.get("chain") or "unknown_chain"
    token_ca = payload.get("token_ca") or "unknown_token"
    pool = payload.get("canonical_pool_group") or payload.get("pool_address") or "unknown_pool"
    epoch = payload.get("lifecycle_epoch", 0)
    return f"{chain}:{token_ca}:{pool}:{epoch}"


def _event_seq(event: Mapping[str, Any]) -> int:
    try:
        return int(event.get("global_seq") or 0)
    except (TypeError, ValueError):
        return 0


def _outcome_label(payload: Mapping[str, Any]) -> dict[str, Any]:
    peak = _as_float(
        payload.get("net_delayed_executable_peak_3s")
        or payload.get("net_delayed_executable_peak")
        or payload.get("executable_peak_pnl")
        or payload.get("peak_pnl")
    )
    stopped = _as_bool(payload.get("would_stop_before_peak"))
    peak30 = bool(peak is not None and peak >= 0.30 and stopped is not True)
    stop_before_peak = bool(stopped is True)
    return {
        "net_delayed_executable_peak": peak,
        "peak30_before_stop": peak30,
        "stop_before_peak": stop_before_peak,
        "trade_label_available_at": payload.get("trade_label_available_at") or payload.get("exit_ts"),
    }


def _bucket(probability: float | None, *, high_threshold: float, low_threshold: float) -> str:
    if probability is None:
        return "unknown"
    if probability >= high_threshold:
        return "high"
    if probability <= low_threshold:
        return "low"
    return "mid"


def _rate(successes: int, sample_n: int) -> float | None:
    if sample_n <= 0:
        return None
    return successes / sample_n


def _bucket_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    sample_n = len(rows)
    peak30 = sum(1 for row in rows if row["outcome"]["peak30_before_stop"])
    stop_before_peak = sum(1 for row in rows if row["outcome"]["stop_before_peak"])
    mean_forecast = None
    probabilities = [row["p_absorb_peak30"] for row in rows if row["p_absorb_peak30"] is not None]
    if probabilities:
        mean_forecast = sum(probabilities) / len(probabilities)
    return {
        "sample_n": sample_n,
        "peak30_before_stop_count": peak30,
        "peak30_before_stop_rate": _rate(peak30, sample_n),
        "stop_before_peak_count": stop_before_peak,
        "stop_before_peak_rate": _rate(stop_before_peak, sample_n),
        "mean_p_absorb_peak30": mean_forecast,
    }


def _first_later_outcome(outcomes: list[dict[str, Any]], cutoff_seq: int) -> dict[str, Any] | None:
    later = [row for row in outcomes if row["event_global_seq"] > cutoff_seq]
    if not later:
        return None
    return sorted(later, key=lambda row: (row["event_global_seq"], row["event_id"]))[0]


def build_markov_shadow_calibration_report(
    event_log_dir: str | Path = DEFAULT_EVENT_LOG_DIR,
    *,
    high_threshold: float = DEFAULT_HIGH_THRESHOLD,
    low_threshold: float = DEFAULT_LOW_THRESHOLD,
    min_sample: int = DEFAULT_MIN_SAMPLE,
    include_rows: bool = False,
) -> dict[str, Any]:
    event_log = V27EventLog(event_log_dir)
    forecasts: list[dict[str, Any]] = []
    outcomes_by_key: dict[str, list[dict[str, Any]]] = {}
    input_events = 0

    for event in event_log.iter_events() or []:
        input_events += 1
        event_type = event.get("event_type")
        payload = _payload(event)
        if event_type == MARKOV_SHADOW_EVENT_TYPE:
            probability = _as_float(payload.get("p_absorb_peak30"))
            forecasts.append(
                {
                    "event_id": event.get("event_id"),
                    "event_global_seq": _event_seq(event),
                    "denominator_dedup_key": _dedup_key(payload),
                    "shadow_forecast_id": payload.get("shadow_forecast_id"),
                    "matrix_build_cutoff_seq": int(payload.get("matrix_build_cutoff_seq") or 0),
                    "p_absorb_peak30": probability,
                    "p_absorb_stop_before_peak": _as_float(payload.get("p_absorb_stop_before_peak")),
                    "feature_vector_hash": (payload.get("feature_vector_snapshot") or {}).get("feature_vector_hash")
                    if isinstance(payload.get("feature_vector_snapshot"), dict)
                    else None,
                    "entry_gate_allowed": payload.get("entry_gate_allowed"),
                    "paper_entry_action": payload.get("paper_entry_action"),
                }
            )
        elif event_type == TRADE_OUTCOME_EVENT_TYPE:
            key = _dedup_key(payload)
            outcomes_by_key.setdefault(key, []).append(
                {
                    "event_id": event.get("event_id"),
                    "event_global_seq": _event_seq(event),
                    "denominator_dedup_key": key,
                    "outcome": _outcome_label(payload),
                }
            )

    paired_rows = []
    unpaired_forecasts = []
    contaminated_forecasts = []
    for forecast in sorted(forecasts, key=lambda item: (item["event_global_seq"], str(item["event_id"]))):
        if forecast.get("entry_gate_allowed") is not False or forecast.get("paper_entry_action") != "none":
            contaminated_forecasts.append(
                {
                    "shadow_forecast_id": forecast.get("shadow_forecast_id"),
                    "reason": "markov_shadow_boundary_violation",
                }
            )
            continue
        outcome = _first_later_outcome(
            outcomes_by_key.get(forecast["denominator_dedup_key"], []),
            int(forecast["matrix_build_cutoff_seq"] or 0),
        )
        if outcome is None:
            unpaired_forecasts.append(
                {
                    "shadow_forecast_id": forecast.get("shadow_forecast_id"),
                    "denominator_dedup_key": forecast["denominator_dedup_key"],
                    "reason": "no_later_trade_outcome_label",
                }
            )
            continue
        bucket = _bucket(forecast["p_absorb_peak30"], high_threshold=high_threshold, low_threshold=low_threshold)
        paired_rows.append(
            {
                **forecast,
                "bucket": bucket,
                "outcome_event_id": outcome["event_id"],
                "outcome_event_global_seq": outcome["event_global_seq"],
                "outcome": outcome["outcome"],
            }
        )

    buckets = {
        bucket: _bucket_stats([row for row in paired_rows if row["bucket"] == bucket])
        for bucket in ("high", "mid", "low", "unknown")
    }
    overall = _bucket_stats(paired_rows)
    high_rate = buckets["high"]["peak30_before_stop_rate"]
    low_rate = buckets["low"]["peak30_before_stop_rate"]
    overall_rate = overall["peak30_before_stop_rate"]
    lift_vs_low = high_rate / low_rate if high_rate is not None and low_rate and low_rate > 0 else None
    lift_vs_overall = high_rate / overall_rate if high_rate is not None and overall_rate and overall_rate > 0 else None

    health_reasons = []
    if len(paired_rows) < int(min_sample):
        health_reasons.append("sample_below_minimum")
    if contaminated_forecasts:
        health_reasons.append("markov_shadow_boundary_violation")
    if high_rate is None:
        health_reasons.append("high_bucket_empty")
    if low_rate is None:
        health_reasons.append("low_bucket_empty")

    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "event_log_dir": str(event_log_dir),
        "input_events": input_events,
        "forecast_count": len(forecasts),
        "paired_sample_n": len(paired_rows),
        "unpaired_forecast_count": len(unpaired_forecasts),
        "contaminated_forecast_count": len(contaminated_forecasts),
        "high_threshold": high_threshold,
        "low_threshold": low_threshold,
        "min_sample": int(min_sample),
        "overall": overall,
        "buckets": buckets,
        "lift_vs_low_bucket": lift_vs_low,
        "lift_vs_overall": lift_vs_overall,
        "promotion_allowed": False,
        "health": {
            "status": "shadow_calibration_observable" if not health_reasons else "shadow_calibration_insufficient",
            "reasons": health_reasons,
            "sample_ready": len(paired_rows) >= int(min_sample),
            "boundary_clean": not contaminated_forecasts,
        },
        "unpaired_forecasts": unpaired_forecasts[:50],
        "contaminated_forecasts": contaminated_forecasts,
    }
    if include_rows:
        report["rows"] = paired_rows
    report["report_hash"] = sha256_hex({key: value for key, value in report.items() if key != "report_hash"})
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-log-dir", default=str(DEFAULT_EVENT_LOG_DIR))
    parser.add_argument("--high-threshold", type=float, default=DEFAULT_HIGH_THRESHOLD)
    parser.add_argument("--low-threshold", type=float, default=DEFAULT_LOW_THRESHOLD)
    parser.add_argument("--min-sample", type=int, default=DEFAULT_MIN_SAMPLE)
    parser.add_argument("--include-rows", action="store_true")
    args = parser.parse_args()
    report = build_markov_shadow_calibration_report(
        args.event_log_dir,
        high_threshold=args.high_threshold,
        low_threshold=args.low_threshold,
        min_sample=args.min_sample,
        include_rows=args.include_rows,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Shared paper-only learning policy helpers.

These helpers keep new learning instrumentation deterministic and easy to test.
They do not place trades or call providers.
"""

from __future__ import annotations

import datetime as dt
import math


TINY_PROBE_MODES = {
    "source_resonance_tiny_probe",
    "hard_gate_pass_tiny_probe",
    "pre_pass_resonance_tiny_probe",
}

RETRYABLE_QUOTE_REASON_MARKERS = (
    "429",
    "rate_limited",
    "provider_rate_limited",
    "too_many_requests",
    "quote_failed",
    "quote_timeout",
    "daemon_timeout",
    "no_route",
    "missing_taker",
    "unknown_data",
    "unknown",
    "route_not_found",
    "temporarily_unavailable",
)

HARD_REJECTION_MARKERS = (
    "top1",
    "top10",
    "creator",
    "dev_hold",
    "developer",
    "honeypot",
    "rug",
    "gmgn_reject",
    "blacklist",
    "malicious",
)

SOFT_REJECTION_MARKERS = (
    "buy_pressure_weak",
    "volume_low",
    "tx_low",
    "negative_trend",
    "unknown_data",
    "missing_trigger",
    "missing_quote",
    "provider_rate_limited",
    "rate_limited",
)


def safe_float(value, default=None):
    try:
        if value in (None, ""):
            return default
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return default
        return number
    except (TypeError, ValueError):
        return default


def normalize_ts_ms(value):
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        numeric = safe_float(text, None)
        if numeric is not None:
            if numeric <= 0:
                return None
            return int(numeric if numeric > 1_000_000_000_000 else numeric * 1000)
        try:
            parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            try:
                parsed = dt.datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return int(parsed.timestamp() * 1000)
    number = safe_float(value, None)
    if number is None or number <= 0:
        return None
    return int(number if number > 1_000_000_000_000 else number * 1000)


def first_ts_ms(*values):
    for value in values:
        ts_ms = normalize_ts_ms(value)
        if ts_ms:
            return ts_ms
    return None


def entry_mode_from_pending(pending):
    pending = pending or {}
    lotto_state = pending.get("lotto_state") if isinstance(pending.get("lotto_state"), dict) else {}
    entry_decision = lotto_state.get("entryDecision") if isinstance(lotto_state.get("entryDecision"), dict) else {}
    return str(
        pending.get("entry_mode")
        or pending.get("scout_mode")
        or entry_decision.get("entry_mode")
        or pending.get("strategy_stage")
        or ""
    )


def position_size_class(size_sol):
    size = safe_float(size_sol, 0.0) or 0.0
    if size <= 0:
        return "unknown"
    if size <= 0.005:
        return "tiny"
    if size <= 0.02:
        return "small"
    if size <= 0.1:
        return "stage1"
    return "large"


def capital_tier_for_entry(*, entry_mode=None, strategy_stage=None, size_sol=None, paper_only=False, is_lotto=False):
    mode = str(entry_mode or "").lower()
    stage = str(strategy_stage or "").lower()
    size = safe_float(size_sol, 0.0) or 0.0
    if paper_only or mode in TINY_PROBE_MODES or "tiny" in mode or "probe" in mode or "scout" in mode or 0 < size <= 0.005:
        return "tiny_probe"
    if is_lotto or "lotto" in mode:
        return "lotto_main"
    if stage == "stage1" or mode == "stage1" or size >= 0.02:
        return "stage1_main"
    if size > 0:
        return "small_probe"
    return "unknown"


def regime_tag_from_market_regime(market_regime):
    regime = str(market_regime or "").lower()
    if regime in {"bull", "risk_on"}:
        return "risk_on"
    if regime in {"bear", "risk_off"}:
        return "risk_off"
    return "neutral"


def first_positive_price(*values):
    for value in values:
        number = safe_float(value, None)
        if number is not None and number > 0:
            return number
    return None


def price_drift_pct(base_price, next_price):
    base = safe_float(base_price, None)
    nxt = safe_float(next_price, None)
    if base is None or nxt is None or base <= 0 or nxt <= 0:
        return None
    return (nxt - base) / base * 100.0


def signal_arrival_ts_ms(pending):
    pending = pending or {}
    w_entry = pending.get("w_entry") if isinstance(pending.get("w_entry"), dict) else {}
    for value in (
        pending.get("signal_arrival_ts_ms"),
        pending.get("signal_received_ts_ms"),
        pending.get("received_ts_ms"),
        pending.get("signal_ts"),
        w_entry.get("signal_ts"),
        w_entry.get("added_at"),
        pending.get("added_at"),
        pending.get("staged_at"),
    ):
        ts_ms = normalize_ts_ms(value)
        if ts_ms:
            return ts_ms
    return None


def build_entry_execution_latency_audit(
    pending,
    *,
    decision_start_ts_ms=None,
    quote_request_ts_ms=None,
    quote_response_ts_ms=None,
    entry_executed_ts_ms=None,
    signal_price=None,
    decision_price=None,
    quote_price=None,
    entry_fill_price=None,
    quote_spread_pct=None,
):
    pending = pending or {}
    signal_ts_ms = signal_arrival_ts_ms(pending)
    source_message_ts_ms = first_ts_ms(
        pending.get("source_message_ts_ms"),
        pending.get("source_message_ts"),
        pending.get("telegram_source_ts_ms"),
    )
    receive_ts_ms = first_ts_ms(
        pending.get("receive_ts_ms"),
        pending.get("signal_receive_ts_ms"),
        pending.get("signal_received_ts_ms"),
        pending.get("receive_ts"),
    )
    signal_recorded_ts_ms = first_ts_ms(
        pending.get("signal_recorded_ts_ms"),
        pending.get("signal_recorded_ts"),
        pending.get("created_ts"),
        pending.get("created_at"),
    )
    signal_local_seen_ts_ms = first_ts_ms(
        pending.get("signal_local_seen_ts_ms"),
        pending.get("local_seen_ts_ms"),
    )
    decision_start_ts_ms = normalize_ts_ms(decision_start_ts_ms)
    quote_request_ts_ms = normalize_ts_ms(quote_request_ts_ms)
    quote_response_ts_ms = normalize_ts_ms(quote_response_ts_ms)
    entry_executed_ts_ms = normalize_ts_ms(entry_executed_ts_ms)
    signal_price = first_positive_price(
        signal_price,
        pending.get("signal_price"),
        pending.get("trigger_price"),
        pending.get("entry_price"),
    )
    decision_price = first_positive_price(decision_price, pending.get("trigger_price"), signal_price)
    quote_price = first_positive_price(quote_price, entry_fill_price)
    entry_fill_price = first_positive_price(entry_fill_price, quote_price)
    audit = {
        "schema_version": 1,
        "signal_arrival_ts_ms": signal_ts_ms,
        "source_message_ts_ms": source_message_ts_ms,
        "receive_ts_ms": receive_ts_ms,
        "signal_recorded_ts_ms": signal_recorded_ts_ms,
        "signal_local_seen_ts_ms": signal_local_seen_ts_ms,
        "decision_start_ts_ms": decision_start_ts_ms,
        "decision_complete_ts_ms": quote_response_ts_ms,
        "quote_request_ts_ms": quote_request_ts_ms,
        "quote_response_ts_ms": quote_response_ts_ms,
        "entry_executed_ts_ms": entry_executed_ts_ms,
        "signal_price": signal_price,
        "decision_price": decision_price,
        "quote_price": quote_price,
        "entry_fill_price": entry_fill_price,
        "signal_to_quote_latency_ms": (
            quote_response_ts_ms - signal_ts_ms if quote_response_ts_ms and signal_ts_ms else None
        ),
        "source_to_receive_latency_ms": (
            receive_ts_ms - source_message_ts_ms if receive_ts_ms and source_message_ts_ms else None
        ),
        "receive_to_recorded_latency_ms": (
            signal_recorded_ts_ms - receive_ts_ms if signal_recorded_ts_ms and receive_ts_ms else None
        ),
        "recorded_to_local_seen_latency_ms": (
            signal_local_seen_ts_ms - signal_recorded_ts_ms
            if signal_local_seen_ts_ms and signal_recorded_ts_ms else None
        ),
        "receive_to_quote_latency_ms": (
            quote_response_ts_ms - receive_ts_ms if quote_response_ts_ms and receive_ts_ms else None
        ),
        "recorded_to_quote_latency_ms": (
            quote_response_ts_ms - signal_recorded_ts_ms
            if quote_response_ts_ms and signal_recorded_ts_ms else None
        ),
        "local_seen_to_quote_latency_ms": (
            quote_response_ts_ms - signal_local_seen_ts_ms
            if quote_response_ts_ms and signal_local_seen_ts_ms else None
        ),
        "decision_to_quote_latency_ms": (
            quote_response_ts_ms - decision_start_ts_ms if quote_response_ts_ms and decision_start_ts_ms else None
        ),
        "quote_latency_ms": (
            quote_response_ts_ms - quote_request_ts_ms if quote_response_ts_ms and quote_request_ts_ms else None
        ),
        "signal_to_entry_latency_ms": (
            entry_executed_ts_ms - signal_ts_ms if entry_executed_ts_ms and signal_ts_ms else None
        ),
        "signal_to_quote_drift_pct": price_drift_pct(signal_price, quote_price),
        "decision_to_quote_drift_pct": price_drift_pct(decision_price, quote_price),
        "quote_spread_pct": safe_float(quote_spread_pct, None),
    }
    return {key: value for key, value in audit.items() if value is not None}


def is_retryable_entry_quote_failure(reason):
    text = str(reason or "").strip().lower()
    return bool(text) and any(marker in text for marker in RETRYABLE_QUOTE_REASON_MARKERS)


def history_discounted_size(size_sol, prior_fast_fail_count, *, factor=0.85, min_size_sol=0.001):
    size = safe_float(size_sol, 0.0) or 0.0
    try:
        count = max(0, int(prior_fast_fail_count or 0))
    except (TypeError, ValueError):
        count = 0
    if size <= 0 or count <= 0:
        return size
    discounted = size * (float(factor) ** count)
    return max(float(min_size_sol or 0.0), discounted)


def classify_rejection_hardness(reason):
    text = str(reason or "").strip().lower()
    if not text:
        return "unknown"
    if any(marker in text for marker in HARD_REJECTION_MARKERS):
        return "hard_reject"
    if any(marker in text for marker in SOFT_REJECTION_MARKERS):
        return "soft_reject"
    return "unknown"


def sample_governance_status(n, *, bootstrap_lower_bound=None, avg_pnl=None, min_n=30):
    try:
        sample_n = int(n or 0)
    except (TypeError, ValueError):
        sample_n = 0
    lb = safe_float(bootstrap_lower_bound, None)
    mean = safe_float(avg_pnl, None)
    if sample_n < min_n:
        return {
            "decision": "continue_sampling",
            "reason": f"sample_n_below_{min_n}",
            "sample_n": sample_n,
            "min_n": min_n,
        }
    if lb is not None and lb > 0:
        return {
            "decision": "promotion_candidate",
            "reason": "bootstrap_lower_bound_positive",
            "sample_n": sample_n,
            "bootstrap_lower_bound": lb,
        }
    if mean is not None and mean < 0 and lb is not None and lb < 0:
        return {
            "decision": "loss_budget_review",
            "reason": "negative_mean_and_lower_bound",
            "sample_n": sample_n,
            "avg_pnl": mean,
            "bootstrap_lower_bound": lb,
        }
    return {
        "decision": "continue_sampling",
        "reason": "sample_large_but_no_positive_lower_bound",
        "sample_n": sample_n,
        "avg_pnl": mean,
        "bootstrap_lower_bound": lb,
    }

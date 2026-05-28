#!/usr/bin/env python3
"""Shadow-only Markov forecasts for Telegram dog lifecycle states.

This module carries the v2.6.13 Markov-regime idea into v2.7.0 without
pretending the generic Bull/Bear/Sideways stock-market skill is an entry gate.
The production-shaped object here is a Telegram lifecycle absorbing chain:
current lifecycle state + historical transition evidence -> n-step state
distribution and competing-risk absorption probabilities.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence


STATE_ORDER: tuple[str, ...] = (
    "TELEGRAM_SEEN",
    "POOL_UNKNOWN",
    "NO_QUOTE",
    "DIRTY_QUOTE",
    "QUOTE_CLEAN",
    "TRADABLE_CLEAN",
    "RECLAIM_FORMING",
    "RECLAIM_CONFIRMED",
    "OVEREXTENDED",
    "OBSERVE_ONLY",
    "TINY_ENTERED",
    "EXIT_PENDING",
    "EXITED",
    "PEAK30",
    "STOP_BEFORE_PEAK",
    "STALE_DEAD",
    "TOXIC_DEAD",
    "CRASH_DEAD",
    "TERMINAL_DEAD",
)

STATE_INDEX = {state: idx for idx, state in enumerate(STATE_ORDER)}

ABSORBING_STATES: frozenset[str] = frozenset(
    {
        "PEAK30",
        "STOP_BEFORE_PEAK",
        "STALE_DEAD",
        "TOXIC_DEAD",
        "CRASH_DEAD",
        "TERMINAL_DEAD",
    }
)

FORECAST_BOUNDARY: dict[str, Any] = {
    "mode_target": "shadow_first",
    "entry_gate_allowed": False,
    "ordinary_stationary_distribution_allowed_as_entry_gate": False,
    "stationary_distribution_role": "research_only_tail_risk_sanity_check",
    "hmm_full_sequence_viterbi_allowed": False,
    "hmm_online_filtering_allowed": True,
}

TIME_FIELDS = (
    "decision_available_at",
    "available_at",
    "observed_at",
    "ingested_at",
    "market_event_ts",
)

SEQ_FIELDS = ("monotonic_ingest_seq", "aggregate_seq", "source_seq")


def canonical_state(value: Any) -> str | None:
    if value is None:
        return None
    state = str(value).strip().upper()
    if state in STATE_INDEX:
        return state
    return None


def _parse_time(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _event_time(event: Mapping[str, Any]) -> float | None:
    for field in TIME_FIELDS:
        parsed = _parse_time(event.get(field))
        if parsed is not None:
            return parsed
    return None


def _event_seq(event: Mapping[str, Any]) -> int | None:
    for field in SEQ_FIELDS:
        value = event.get(field)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _lifecycle_identity(event: Mapping[str, Any]) -> tuple[str, str]:
    key = event.get("token_lifecycle_key")
    if key is None:
        chain = event.get("chain") or "unknown_chain"
        token_ca = event.get("token_ca") or event.get("normalized_ca") or "unknown_token"
        pool_group = event.get("canonical_pool_group") or event.get("pool_address") or "unknown_pool"
        key = f"{chain}:{token_ca}:{pool_group}"
    epoch = event.get("lifecycle_epoch", 0)
    return str(key), str(epoch)


def _event_order_key(event: Mapping[str, Any]) -> tuple[float, int, int, int, str]:
    ts = _event_time(event)
    seq_values = []
    for field in SEQ_FIELDS:
        try:
            seq_values.append(int(event.get(field, 0) or 0))
        except (TypeError, ValueError):
            seq_values.append(0)
    return (
        ts if ts is not None else 0.0,
        seq_values[0],
        seq_values[1],
        seq_values[2],
        str(event.get("event_id") or ""),
    )


def _at_or_before_cutoff(
    event: Mapping[str, Any],
    *,
    cutoff_seq: int | None = None,
    cutoff_ts: Any = None,
) -> bool:
    if cutoff_seq is not None:
        seq = _event_seq(event)
        if seq is None or seq > cutoff_seq:
            return False
    parsed_cutoff_ts = _parse_time(cutoff_ts)
    if parsed_cutoff_ts is not None:
        ts = _event_time(event)
        if ts is None or ts > parsed_cutoff_ts:
            return False
    return True


def ordered_lifecycle_events(
    events: Iterable[Mapping[str, Any]],
    *,
    cutoff_seq: int | None = None,
    cutoff_ts: Any = None,
) -> dict[tuple[str, str], list[Mapping[str, Any]]]:
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for event in events:
        if canonical_state(event.get("lifecycle_state") or event.get("state")) is None:
            continue
        if not _at_or_before_cutoff(event, cutoff_seq=cutoff_seq, cutoff_ts=cutoff_ts):
            continue
        grouped[_lifecycle_identity(event)].append(event)

    return {key: sorted(value, key=_event_order_key) for key, value in grouped.items()}


def build_transition_counts(
    events: Iterable[Mapping[str, Any]],
    *,
    cutoff_seq: int | None = None,
    cutoff_ts: Any = None,
    states: Sequence[str] = STATE_ORDER,
) -> list[list[int]]:
    index = {state: idx for idx, state in enumerate(states)}
    counts = [[0 for _ in states] for _ in states]
    grouped = ordered_lifecycle_events(events, cutoff_seq=cutoff_seq, cutoff_ts=cutoff_ts)

    for lifecycle_events in grouped.values():
        previous_state: str | None = None
        for event in lifecycle_events:
            state = canonical_state(event.get("lifecycle_state") or event.get("state"))
            if state is None or state not in index:
                continue
            if previous_state is not None:
                counts[index[previous_state]][index[state]] += 1
            previous_state = state

    return counts


def absorbing_transition_violations(
    counts: Sequence[Sequence[int]],
    *,
    states: Sequence[str] = STATE_ORDER,
    absorbing_states: set[str] | frozenset[str] = ABSORBING_STATES,
) -> list[dict[str, Any]]:
    violations = []
    index = {state: idx for idx, state in enumerate(states)}
    for from_state in absorbing_states:
        row_idx = index[from_state]
        for to_idx, count in enumerate(counts[row_idx]):
            to_state = states[to_idx]
            if count and to_state != from_state:
                violations.append(
                    {
                        "from_state": from_state,
                        "to_state": to_state,
                        "count": int(count),
                    }
                )
    return violations


def normalize_transition_matrix(
    counts: Sequence[Sequence[int]],
    *,
    states: Sequence[str] = STATE_ORDER,
    absorbing_states: set[str] | frozenset[str] = ABSORBING_STATES,
) -> list[list[float]]:
    matrix: list[list[float]] = []
    index = {state: idx for idx, state in enumerate(states)}
    for row_idx, state in enumerate(states):
        row = [0.0 for _ in states]
        if state in absorbing_states:
            row[index[state]] = 1.0
            matrix.append(row)
            continue

        row_sum = float(sum(counts[row_idx]))
        if row_sum <= 0:
            row[row_idx] = 1.0
            matrix.append(row)
            continue

        matrix.append([float(count) / row_sum for count in counts[row_idx]])
    return matrix


def distribution_after_steps(
    matrix: Sequence[Sequence[float]],
    start_state: str,
    steps: int,
    *,
    states: Sequence[str] = STATE_ORDER,
) -> dict[str, float]:
    if steps < 0:
        raise ValueError("steps must be non-negative")
    index = {state: idx for idx, state in enumerate(states)}
    if start_state not in index:
        raise ValueError(f"unknown start_state: {start_state}")

    dist = [0.0 for _ in states]
    dist[index[start_state]] = 1.0
    for _ in range(steps):
        next_dist = [0.0 for _ in states]
        for from_idx, probability in enumerate(dist):
            if probability == 0:
                continue
            for to_idx, transition_probability in enumerate(matrix[from_idx]):
                next_dist[to_idx] += probability * float(transition_probability)
        dist = next_dist
    return {state: dist[idx] for idx, state in enumerate(states)}


def absorption_forecast(
    matrix: Sequence[Sequence[float]],
    start_state: str,
    *,
    max_steps: int = 60,
    states: Sequence[str] = STATE_ORDER,
    absorbing_states: set[str] | frozenset[str] = ABSORBING_STATES,
) -> dict[str, Any]:
    if max_steps <= 0:
        raise ValueError("max_steps must be positive")

    index = {state: idx for idx, state in enumerate(states)}
    if start_state not in index:
        raise ValueError(f"unknown start_state: {start_state}")

    dist = [0.0 for _ in states]
    dist[index[start_state]] = 1.0
    previous_absorbed = sum(dist[index[state]] for state in absorbing_states)
    expected_time_lower_bound = 0.0

    for step in range(1, max_steps + 1):
        next_dist = [0.0 for _ in states]
        for from_idx, probability in enumerate(dist):
            if probability == 0:
                continue
            for to_idx, transition_probability in enumerate(matrix[from_idx]):
                next_dist[to_idx] += probability * float(transition_probability)
        dist = next_dist
        absorbed = sum(dist[index[state]] for state in absorbing_states)
        newly_absorbed = max(0.0, absorbed - previous_absorbed)
        expected_time_lower_bound += step * newly_absorbed
        previous_absorbed = absorbed

    absorbing_probabilities = {state: dist[index[state]] for state in sorted(absorbing_states)}
    unresolved_probability = max(0.0, 1.0 - sum(absorbing_probabilities.values()))
    return {
        "absorbing_state_probabilities": absorbing_probabilities,
        "p_absorb_peak30": absorbing_probabilities.get("PEAK30", 0.0),
        "p_absorb_stop_before_peak": absorbing_probabilities.get("STOP_BEFORE_PEAK", 0.0),
        "p_absorb_stale_dead": absorbing_probabilities.get("STALE_DEAD", 0.0),
        "p_absorb_toxic_dead": absorbing_probabilities.get("TOXIC_DEAD", 0.0),
        "p_absorb_crash_dead": absorbing_probabilities.get("CRASH_DEAD", 0.0),
        "unresolved_probability_after_horizon": unresolved_probability,
        "expected_time_to_absorption_lower_bound_steps": expected_time_lower_bound,
        "max_steps": int(max_steps),
    }


def stationary_distribution_power(
    matrix: Sequence[Sequence[float]],
    *,
    iterations: int = 50,
    states: Sequence[str] = STATE_ORDER,
) -> dict[str, float]:
    if iterations <= 0:
        raise ValueError("iterations must be positive")
    n = len(states)
    dist = [1.0 / n for _ in states]
    for _ in range(iterations):
        next_dist = [0.0 for _ in states]
        for from_idx, probability in enumerate(dist):
            for to_idx, transition_probability in enumerate(matrix[from_idx]):
                next_dist[to_idx] += probability * float(transition_probability)
        dist = next_dist
    return {state: dist[idx] for idx, state in enumerate(states)}


def matrix_as_mapping(
    matrix: Sequence[Sequence[float]],
    *,
    states: Sequence[str] = STATE_ORDER,
) -> dict[str, dict[str, float]]:
    return {
        from_state: {to_state: float(matrix[row_idx][col_idx]) for col_idx, to_state in enumerate(states)}
        for row_idx, from_state in enumerate(states)
    }


def counts_as_mapping(
    counts: Sequence[Sequence[int]],
    *,
    states: Sequence[str] = STATE_ORDER,
) -> dict[str, dict[str, int]]:
    return {
        from_state: {to_state: int(counts[row_idx][col_idx]) for col_idx, to_state in enumerate(states)}
        for row_idx, from_state in enumerate(states)
    }


def build_lifecycle_forecast_snapshot(
    events: Iterable[Mapping[str, Any]],
    *,
    start_state: str,
    cutoff_seq: int | None = None,
    cutoff_ts: Any = None,
    horizons: Sequence[int] = (1, 3, 5, 15),
    max_absorption_steps: int = 60,
    model_snapshot_id: str | None = None,
) -> dict[str, Any]:
    start_state = canonical_state(start_state) or str(start_state).strip().upper()
    if start_state not in STATE_INDEX:
        raise ValueError(f"unknown start_state: {start_state}")

    counts = build_transition_counts(events, cutoff_seq=cutoff_seq, cutoff_ts=cutoff_ts)
    matrix = normalize_transition_matrix(counts)
    violations = absorbing_transition_violations(counts)
    absorption = absorption_forecast(matrix, start_state, max_steps=max_absorption_steps)
    n_step = {
        str(horizon): distribution_after_steps(matrix, start_state, int(horizon))
        for horizon in sorted({int(h) for h in horizons})
    }

    cutoff_label = cutoff_seq if cutoff_seq is not None else "unbounded"
    sample_n = sum(sum(row) for row in counts)
    return {
        "model_snapshot_id": model_snapshot_id or f"telegram_lifecycle_markov_shadow_v1_seq_{cutoff_label}",
        "model_family": "telegram_lifecycle_absorbing_markov",
        "state_definition_version": "telegram_lifecycle_absorbing_v1",
        "matrix_build_cutoff_seq": cutoff_seq,
        "matrix_build_cutoff_ts": cutoff_ts,
        "sample_n": int(sample_n),
        "start_state": start_state,
        "transition_counts": counts_as_mapping(counts),
        "transition_matrix": matrix_as_mapping(matrix),
        "n_step_forecasts": n_step,
        "absorption_forecast": absorption,
        "stationary_distribution": stationary_distribution_power(matrix),
        "absorbing_transition_violations": violations,
        "absorbing_transition_violation_count": len(violations),
        "contract_boundaries": dict(FORECAST_BOUNDARY),
    }


__all__ = [
    "ABSORBING_STATES",
    "FORECAST_BOUNDARY",
    "STATE_ORDER",
    "absorption_forecast",
    "absorbing_transition_violations",
    "build_lifecycle_forecast_snapshot",
    "build_transition_counts",
    "distribution_after_steps",
    "matrix_as_mapping",
    "normalize_transition_matrix",
    "stationary_distribution_power",
]

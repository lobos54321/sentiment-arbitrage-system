#!/usr/bin/env python3
"""Walk-forward backtest for LOTTO reclaim Markov gating.

For every historical LOTTO reclaim candidate, this script asks the same
question the runtime gate asks: using only data available before that
candidate's decision time, would the Markov forecast have put it in a better
or worse risk bucket?
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import paper_trade_monitor as ptm  # noqa: E402
from telegram_lifecycle_markov import build_lifecycle_forecast_snapshot  # noqa: E402


SCHEMA_VERSION = "v2.7.0.lotto_reclaim_markov_backtest.v1"
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "paper_trades.db"
DEFAULT_DAYS = 7
MICRO_REASONS = {
    "weak_buying_pressure",
    "momentum_fading",
    "negative_trend",
    "negative_trend_crash",
    "chasing_top",
    "score_too_low",
    "scout_quality_buy_pressure_weak",
    "scout_quality_volume_low",
    "scout_quality_tx_low",
    "scout_quality_negative_trend",
}
NOT_ATH_REASONS = {
    "tracking_ttl_expired",
    "not_ath_v17",
    "not_ath_prebuy_kline_block",
    "not_ath_prebuy_kline_unknown_data_blocked",
    "not_ath_prebuy_kline_retry_expired",
    "lotto_mc_0",
}


@dataclass(frozen=True)
class Candidate:
    candidate_id: str
    source: str
    token_ca: str
    symbol: str | None
    decision_ts: float
    entry_mode: str
    blocker_family: str
    quote_clean_bucket: str
    mc_bucket: str
    momentum_bucket: str
    liquidity_bucket: str
    peak_pnl: float | None
    pnl_proxy: float | None
    peak30_before_stop: bool
    stop_before_peak: bool
    source_payload: dict[str, Any]


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def _safe_int(value: Any, default: int | None = None) -> int | None:
    parsed = _safe_float(value, None)
    if parsed is None:
        return default
    return int(parsed)


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


def _ratio(value: Any, default: float | None = None) -> float | None:
    parsed = _safe_float(value, default)
    if parsed is None:
        return default
    if abs(parsed) > 2.0:
        parsed = parsed / 100.0
    return parsed


def _json_loads(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _table_exists(db: sqlite3.Connection, table: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def _columns(db: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(db, table):
        return set()
    return {str(row["name"]) for row in db.execute(f"PRAGMA table_info({table})").fetchall()}


def _row_value(row: Mapping[str, Any] | sqlite3.Row, key: str, default: Any = None) -> Any:
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return default


def _select_rows(
    db: sqlite3.Connection,
    table: str,
    wanted: Iterable[str],
    *,
    where_sql: str = "",
    params: tuple[Any, ...] = (),
    order_sql: str = "",
) -> list[sqlite3.Row]:
    columns = _columns(db, table)
    if not columns:
        return []
    select_exprs = [f"{name} AS {name}" if name in columns else f"NULL AS {name}" for name in wanted]
    sql = f"SELECT {', '.join(select_exprs)} FROM {table}"
    if where_sql:
        sql += f" WHERE {where_sql}"
    if order_sql:
        sql += f" ORDER BY {order_sql}"
    try:
        return db.execute(sql, params).fetchall()
    except sqlite3.DatabaseError:
        return []


def blocker_family(reason: Any) -> str:
    raw = str(reason or "").strip()
    if not raw:
        return "unknown"
    lowered = raw.lower()
    if lowered.startswith("lotto_stale_"):
        return "lotto_stale"
    if lowered.startswith("lotto_mc_"):
        return "lotto_mc"
    if lowered.startswith("not_ath_prebuy_kline"):
        return "not_ath_prebuy_kline"
    if lowered.startswith("scout_quality_"):
        return "scout_quality"
    return lowered


def infer_entry_mode(reason: Any, component: Any = None) -> str:
    family = blocker_family(reason)
    if family in MICRO_REASONS or str(component or "").lower() in {"smart_entry", "scout_quality"}:
        return ptm.LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE
    if family in NOT_ATH_REASONS or family in {"lotto_stale", "lotto_mc", "not_ath_prebuy_kline"}:
        return ptm.LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE
    return ptm.LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE


def _bucket_numeric(value: float | None, boundaries: tuple[tuple[float, str], ...], default: str) -> str:
    if value is None:
        return default
    for limit, name in boundaries:
        if value < limit:
            return name
    return boundaries[-1][1].replace("<", ">=")


def mc_bucket(value: Any) -> str:
    mc = _safe_float(value, None)
    if mc is None or mc <= 0:
        return "mc_unknown"
    if mc < 16_000:
        return "mc_lt_16k"
    if mc < 50_000:
        return "mc_16k_50k"
    if mc < 200_000:
        return "mc_50k_200k"
    return "mc_gte_200k"


def liquidity_bucket(value: Any) -> str:
    liq = _safe_float(value, None)
    if liq is None or liq <= 0:
        return "liq_unknown"
    if liq < 5_000:
        return "liq_lt_5k"
    if liq < 15_000:
        return "liq_5k_15k"
    return "liq_gte_15k"


def momentum_bucket(payload: Mapping[str, Any]) -> str:
    pc_m5 = _safe_float(
        payload.get("price_change_m5")
        or payload.get("pc_m5")
        or payload.get("pnl_5m"),
        None,
    )
    bs = _safe_float(
        payload.get("buy_sell_ratio")
        or payload.get("bs")
        or payload.get("buySellRatio"),
        None,
    )
    tx_m5 = _safe_float(payload.get("tx_m5") or payload.get("tx5m"), None)
    strong = (
        pc_m5 is not None
        and pc_m5 >= 2.0
        and (bs is None or bs >= 1.15)
        and (tx_m5 is None or tx_m5 >= 40)
    )
    weak = (
        (pc_m5 is not None and pc_m5 < 0)
        or (bs is not None and bs < 1.05)
        or (tx_m5 is not None and tx_m5 < 20)
    )
    if strong:
        return "momentum_strong"
    if weak:
        return "momentum_weak"
    return "momentum_neutral_or_unknown"


def quote_clean_bucket(row: Mapping[str, Any], payload: Mapping[str, Any]) -> str:
    if _as_bool(payload.get("recovery_quote_clean")) is True or _as_bool(payload.get("quote_clean")) is True:
        return "quote_clean"
    status = str(_row_value(row, "tradability_status") or payload.get("tradability_status") or "").lower()
    if "tradable" in status:
        return "quote_clean"
    gap = _safe_float(
        payload.get("quote_gap_pct")
        or payload.get("max_quote_gap_pct")
        or payload.get("quote_gap"),
        None,
    )
    if gap is None:
        return "quote_unknown"
    gap = abs(gap * 100.0 if abs(gap) <= 2.0 else gap)
    if gap <= 10:
        return "quote_clean10"
    if gap <= 30:
        return "quote_clean30"
    return "quote_dirty"


def _first_present(row: Mapping[str, Any], payload: Mapping[str, Any], names: Iterable[str]) -> Any:
    for name in names:
        value = _row_value(row, name)
        if value is not None:
            return value
        if name in payload:
            return payload.get(name)
    return None


def _candidate_from_missed(row: sqlite3.Row) -> Candidate | None:
    payload = _json_loads(_row_value(row, "payload_json"))
    route = str(_row_value(row, "route") or payload.get("route") or "").upper()
    if route and route != "LOTTO":
        return None
    decision_ts = _safe_float(
        _first_present(row, payload, ("first_tradable_ts", "baseline_ts", "signal_ts", "created_event_ts")),
        None,
    )
    if decision_ts is None:
        return None
    reason = _row_value(row, "reject_reason") or payload.get("reject_reason") or payload.get("reason")
    component = _row_value(row, "component") or payload.get("component")
    peak = _ratio(
        _first_present(
            row,
            payload,
            (
                "tradable_peak_pnl",
                "executable_peak_pnl",
                "quote_clean_peak_pnl",
                "theoretical_peak_pnl",
                "max_pnl_recorded",
                "pnl_24h",
                "pnl_60m",
            ),
        ),
        None,
    )
    stop = _as_bool(_row_value(row, "would_stop_before_peak"))
    token_ca = str(_row_value(row, "token_ca") or payload.get("token_ca") or "")
    if not token_ca:
        return None
    merged_payload = {
        **payload,
        "market_cap": _first_present(row, payload, ("market_cap", "signal_mc", "baseline_mc")),
        "liquidity_usd": _first_present(row, payload, ("liquidity_usd", "last_liquidity")),
        "pnl_5m": _row_value(row, "pnl_5m"),
    }
    return Candidate(
        candidate_id=f"missed:{_row_value(row, 'id')}",
        source="paper_missed_signal_attribution",
        token_ca=token_ca,
        symbol=_row_value(row, "symbol") or payload.get("symbol"),
        decision_ts=float(decision_ts),
        entry_mode=infer_entry_mode(reason, component),
        blocker_family=blocker_family(reason),
        quote_clean_bucket=quote_clean_bucket(row, payload),
        mc_bucket=mc_bucket(merged_payload.get("market_cap")),
        momentum_bucket=momentum_bucket(merged_payload),
        liquidity_bucket=liquidity_bucket(merged_payload.get("liquidity_usd")),
        peak_pnl=peak,
        pnl_proxy=peak,
        peak30_before_stop=bool(peak is not None and peak >= 0.30 and stop is not True),
        stop_before_peak=bool(stop is True),
        source_payload={
            "reason": reason,
            "component": component,
            "tradability_status": _row_value(row, "tradability_status"),
            "tradable_missed": _row_value(row, "tradable_missed"),
        },
    )


def _candidate_from_trade(row: sqlite3.Row) -> Candidate | None:
    entry_mode = str(_row_value(row, "entry_mode") or "")
    if entry_mode not in ptm.LOTTO_RECLAIM_MARKOV_ENTRY_MODES and not (
        entry_mode.startswith("lotto_") and "reclaim" in entry_mode
    ):
        return None
    route = str(_row_value(row, "signal_route") or _row_value(row, "signal_type") or "").upper()
    if route and route not in {"LOTTO", "NEW_TRENDING", "TTL_FINAL_RECLAIM_FAST", "NOT_ATH_RECLAIM_FAST"}:
        return None
    decision_ts = _safe_float(_row_value(row, "entry_ts"), None)
    token_ca = str(_row_value(row, "token_ca") or "")
    if decision_ts is None or not token_ca:
        return None
    monitor_state = _json_loads(_row_value(row, "monitor_state_json"))
    audit = _json_loads(_row_value(row, "entry_execution_audit_json"))
    payload = {**monitor_state, **audit}
    peak = _ratio(_row_value(row, "peak_pnl"), None)
    pnl = _ratio(_row_value(row, "pnl_pct"), None)
    exit_reason = str(_row_value(row, "exit_reason") or "").lower()
    stop = (
        peak is not None
        and peak < 0.30
        and (
            pnl is not None
            and pnl <= 0
            or any(marker in exit_reason for marker in ("stop", "sl", "no_follow", "timeout", "expired"))
        )
    )
    reason = (
        _row_value(row, "entry_branch")
        or payload.get("entryBranch")
        or payload.get("entry_branch")
        or entry_mode
    )
    return Candidate(
        candidate_id=f"trade:{_row_value(row, 'id')}",
        source="paper_trades",
        token_ca=token_ca,
        symbol=_row_value(row, "symbol") or payload.get("symbol"),
        decision_ts=float(decision_ts),
        entry_mode=entry_mode,
        blocker_family=blocker_family(reason),
        quote_clean_bucket="quote_filled",
        mc_bucket=mc_bucket(payload.get("signal_mc") or payload.get("marketCap") or payload.get("market_cap")),
        momentum_bucket=momentum_bucket(payload),
        liquidity_bucket=liquidity_bucket(payload.get("liquidity_usd") or payload.get("last_liquidity")),
        peak_pnl=peak,
        pnl_proxy=pnl if pnl is not None else peak,
        peak30_before_stop=bool(peak is not None and peak >= 0.30 and not stop),
        stop_before_peak=bool(stop),
        source_payload={
            "entry_branch": reason,
            "exit_reason": _row_value(row, "exit_reason"),
            "replay_source": _row_value(row, "replay_source"),
        },
    )


def load_candidates(db: sqlite3.Connection, *, since_ts: float, until_ts: float | None = None) -> list[Candidate]:
    candidates: list[Candidate] = []
    missed_wanted = (
        "id",
        "created_event_ts",
        "token_ca",
        "symbol",
        "signal_ts",
        "route",
        "component",
        "reject_reason",
        "baseline_ts",
        "tradability_status",
        "tradable_missed",
        "tradable_peak_pnl",
        "time_to_peak_sec",
        "would_stop_before_peak",
        "first_tradable_ts",
        "first_tradable_pnl",
        "pnl_5m",
        "pnl_15m",
        "pnl_60m",
        "pnl_24h",
        "max_pnl_recorded",
        "payload_json",
    )
    for row in _select_rows(
        db,
        "paper_missed_signal_attribution",
        missed_wanted,
        where_sql="COALESCE(first_tradable_ts, baseline_ts, signal_ts, created_event_ts) >= ?",
        params=(since_ts,),
        order_sql="COALESCE(first_tradable_ts, baseline_ts, signal_ts, created_event_ts) ASC, id ASC",
    ):
        candidate = _candidate_from_missed(row)
        if candidate and (until_ts is None or candidate.decision_ts <= until_ts):
            candidates.append(candidate)

    trade_wanted = (
        "id",
        "token_ca",
        "symbol",
        "entry_ts",
        "exit_ts",
        "exit_reason",
        "pnl_pct",
        "peak_pnl",
        "signal_route",
        "signal_type",
        "entry_mode",
        "entry_branch",
        "replay_source",
        "monitor_state_json",
        "entry_execution_audit_json",
    )
    for row in _select_rows(
        db,
        "paper_trades",
        trade_wanted,
        where_sql="entry_ts >= ?",
        params=(since_ts,),
        order_sql="entry_ts ASC, id ASC",
    ):
        candidate = _candidate_from_trade(row)
        if candidate and (until_ts is None or candidate.decision_ts <= until_ts):
            candidates.append(candidate)

    deduped: dict[tuple[str, str, int, str, str], Candidate] = {}
    for candidate in candidates:
        key = (
            candidate.source,
            candidate.token_ca,
            int(candidate.decision_ts),
            candidate.entry_mode,
            candidate.blocker_family,
        )
        deduped.setdefault(key, candidate)
    return sorted(deduped.values(), key=lambda item: (item.decision_ts, item.candidate_id))


def load_training_events(db: sqlite3.Connection, *, until_ts: float) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    events.extend(ptm._markov_reclaim_events_from_trades(db, now_ts=until_ts))
    events.extend(ptm._markov_reclaim_events_from_decisions(db, now_ts=until_ts))
    return events


def forecast_from_events(
    events: list[dict[str, Any]],
    candidate: Candidate,
) -> dict[str, Any]:
    try:
        snapshot = build_lifecycle_forecast_snapshot(
            events,
            start_state="RECLAIM_CONFIRMED",
            cutoff_ts=candidate.decision_ts,
            horizons=(1, 2, 3, 5, 15),
            max_absorption_steps=60,
            model_snapshot_id=f"lotto_reclaim_markov_backtest_{int(candidate.decision_ts)}",
        )
        absorption = snapshot.get("absorption_forecast") or {}
        p_peak = _safe_float(absorption.get("p_absorb_peak30"), 0.0) or 0.0
        p_stop = _safe_float(absorption.get("p_absorb_stop_before_peak"), 0.0) or 0.0
        return {
            "entry_mode": candidate.entry_mode,
            "policy_version": ptm.LOTTO_RECLAIM_MARKOV_POLICY_VERSION,
            "model_role": "lotto_reclaim_walk_forward_backtest",
            "base_model_entry_gate_allowed": False,
            "model_family": snapshot.get("model_family"),
            "model_snapshot_id": snapshot.get("model_snapshot_id"),
            "start_state": snapshot.get("start_state"),
            "sample_n": int(snapshot.get("sample_n") or 0),
            "event_count": len(events),
            "p_absorb_peak30": p_peak,
            "p_absorb_stop_before_peak": p_stop,
            "p_absorb_stale_dead": _safe_float(absorption.get("p_absorb_stale_dead"), 0.0) or 0.0,
            "p_absorb_toxic_dead": _safe_float(absorption.get("p_absorb_toxic_dead"), 0.0) or 0.0,
            "p_absorb_crash_dead": _safe_float(absorption.get("p_absorb_crash_dead"), 0.0) or 0.0,
            "unresolved_probability_after_horizon": _safe_float(
                absorption.get("unresolved_probability_after_horizon"),
                0.0,
            ) or 0.0,
        }
    except Exception as exc:
        return {
            "entry_mode": candidate.entry_mode,
            "policy_version": ptm.LOTTO_RECLAIM_MARKOV_POLICY_VERSION,
            "model_role": "lotto_reclaim_walk_forward_backtest",
            "base_model_entry_gate_allowed": False,
            "sample_n": 0,
            "event_count": len(events),
            "error": str(exc),
        }


def markov_bucket(
    forecast: Mapping[str, Any] | None,
    *,
    min_sample: int,
    min_peak30_prob: float,
    max_stop_prob: float,
    min_edge: float,
) -> str:
    if not forecast:
        return "error"
    if forecast.get("error"):
        return "error"
    sample_n = int(_safe_float(forecast.get("sample_n"), 0) or 0)
    if sample_n < min_sample:
        return "insufficient"
    p_peak = _safe_float(forecast.get("p_absorb_peak30"), 0.0) or 0.0
    p_stop = _safe_float(forecast.get("p_absorb_stop_before_peak"), 0.0) or 0.0
    edge = p_peak - p_stop
    if p_peak >= min_peak30_prob and p_stop <= max_stop_prob and edge >= min_edge:
        return "green"
    if p_peak < min_peak30_prob or edge < 0:
        return "red"
    return "yellow"


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    sample_n = len(rows)
    peak30 = sum(1 for row in rows if row["outcome"]["peak30_before_stop"])
    stop = sum(1 for row in rows if row["outcome"]["stop_before_peak"])
    p_peak_values = [row["forecast"]["p_absorb_peak30"] for row in rows if row["forecast"].get("p_absorb_peak30") is not None]
    p_stop_values = [
        row["forecast"]["p_absorb_stop_before_peak"]
        for row in rows
        if row["forecast"].get("p_absorb_stop_before_peak") is not None
    ]
    peak_values = [row["outcome"]["peak_pnl"] for row in rows if row["outcome"].get("peak_pnl") is not None]
    pnl_proxy_values = [row["outcome"]["pnl_proxy"] for row in rows if row["outcome"].get("pnl_proxy") is not None]
    return {
        "sample_n": sample_n,
        "peak30_before_stop_count": peak30,
        "peak30_before_stop_rate": (peak30 / sample_n) if sample_n else None,
        "stop_before_peak_count": stop,
        "stop_before_peak_rate": (stop / sample_n) if sample_n else None,
        "mean_p_absorb_peak30": (sum(p_peak_values) / len(p_peak_values)) if p_peak_values else None,
        "mean_p_absorb_stop_before_peak": (sum(p_stop_values) / len(p_stop_values)) if p_stop_values else None,
        "mean_peak_pnl": (sum(peak_values) / len(peak_values)) if peak_values else None,
        "mean_pnl_proxy": (sum(pnl_proxy_values) / len(pnl_proxy_values)) if pnl_proxy_values else None,
        "proxy_ev_per_0_001_sol": (sum(pnl_proxy_values) * 0.001) if pnl_proxy_values else None,
        "proxy_ev_per_0_003_sol": (sum(pnl_proxy_values) * 0.003) if pnl_proxy_values else None,
    }


def _group_summary(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get(key) or "unknown"), []).append(row)
    return {
        name: _summary(group_rows)
        for name, group_rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0]))
    }


def build_backtest_report(
    db_path: str | Path = DEFAULT_DB_PATH,
    *,
    days: int = DEFAULT_DAYS,
    since_ts: float | None = None,
    until_ts: float | None = None,
    min_sample: int = ptm.LOTTO_MICRO_RECLAIM_MARKOV_MIN_SAMPLE_N,
    min_peak30_prob: float = ptm.LOTTO_MICRO_RECLAIM_MARKOV_MIN_PEAK30_PROB,
    max_stop_prob: float = ptm.LOTTO_MICRO_RECLAIM_MARKOV_MAX_STOP_PROB,
    min_edge: float = ptm.LOTTO_MICRO_RECLAIM_MARKOV_MIN_EDGE,
    include_rows: bool = False,
    max_candidates: int | None = None,
) -> dict[str, Any]:
    db_path = Path(db_path)
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    now_ts = float(until_ts or time.time())
    since_ts = float(since_ts if since_ts is not None else now_ts - int(days) * 24 * 60 * 60)
    candidates = load_candidates(db, since_ts=since_ts, until_ts=until_ts)
    total_candidates = len(candidates)
    if max_candidates is not None and max_candidates > 0:
        candidates = candidates[-int(max_candidates):]
    training_events = load_training_events(db, until_ts=now_ts)
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        forecast = forecast_from_events(training_events, candidate)
        bucket = markov_bucket(
            forecast,
            min_sample=min_sample,
            min_peak30_prob=min_peak30_prob,
            max_stop_prob=max_stop_prob,
            min_edge=min_edge,
        )
        p_peak = _safe_float(forecast.get("p_absorb_peak30"), None)
        p_stop = _safe_float(forecast.get("p_absorb_stop_before_peak"), None)
        rows.append(
            {
                "candidate_id": candidate.candidate_id,
                "source": candidate.source,
                "token_ca": candidate.token_ca,
                "symbol": candidate.symbol,
                "decision_ts": candidate.decision_ts,
                "entry_mode": candidate.entry_mode,
                "blocker_family": candidate.blocker_family,
                "quote_clean_bucket": candidate.quote_clean_bucket,
                "mc_bucket": candidate.mc_bucket,
                "momentum_bucket": candidate.momentum_bucket,
                "liquidity_bucket": candidate.liquidity_bucket,
                "markov_bucket": bucket,
                "forecast": {
                    "sample_n": int(_safe_float(forecast.get("sample_n"), 0) or 0),
                    "event_count": int(_safe_float(forecast.get("event_count"), 0) or 0),
                    "p_absorb_peak30": p_peak,
                    "p_absorb_stop_before_peak": p_stop,
                    "edge_peak30_minus_stop": (p_peak - p_stop) if p_peak is not None and p_stop is not None else None,
                    "error": forecast.get("error"),
                    "model_snapshot_id": forecast.get("model_snapshot_id"),
                },
                "outcome": {
                    "peak_pnl": candidate.peak_pnl,
                    "pnl_proxy": candidate.pnl_proxy,
                    "peak30_before_stop": candidate.peak30_before_stop,
                    "stop_before_peak": candidate.stop_before_peak,
                },
                "source_payload": candidate.source_payload,
            }
        )
    db.close()

    report = {
        "schema_version": SCHEMA_VERSION,
        "db_path": str(db_path),
        "generated_at": int(time.time()),
        "window": {
            "days": days,
            "since_ts": since_ts,
            "until_ts": until_ts,
        },
        "thresholds": {
            "min_sample": min_sample,
            "min_peak30_prob": min_peak30_prob,
            "max_stop_prob": max_stop_prob,
            "min_edge": min_edge,
        },
        "candidate_count": len(candidates),
        "candidate_count_before_limit": total_candidates,
        "training_event_count": len(training_events),
        "paired_sample_n": len(rows),
        "overall": _summary(rows),
        "by_markov_bucket": _group_summary(rows, "markov_bucket"),
        "by_entry_mode": _group_summary(rows, "entry_mode"),
        "by_blocker_family": _group_summary(rows, "blocker_family"),
        "by_quote_clean_bucket": _group_summary(rows, "quote_clean_bucket"),
        "by_mc_bucket": _group_summary(rows, "mc_bucket"),
        "by_momentum_bucket": _group_summary(rows, "momentum_bucket"),
        "by_liquidity_bucket": _group_summary(rows, "liquidity_bucket"),
        "promotion_allowed": False,
        "health": {
            "status": "observable" if rows else "no_candidates",
            "sample_ready": len(rows) >= min_sample,
            "green_sample_n": len([row for row in rows if row["markov_bucket"] == "green"]),
            "red_sample_n": len([row for row in rows if row["markov_bucket"] == "red"]),
        },
    }
    if include_rows:
        report["rows"] = rows
    return report


def _pct(value: Any) -> str:
    parsed = _safe_float(value, None)
    if parsed is None:
        return "n/a"
    return f"{parsed * 100:.1f}%"


def format_text_report(report: Mapping[str, Any]) -> str:
    lines = []
    lines.append("LOTTO reclaim Markov walk-forward backtest")
    lines.append(f"schema={report.get('schema_version')} db={report.get('db_path')}")
    window = report.get("window") or {}
    lines.append(f"window_days={window.get('days')} candidates={report.get('candidate_count')} paired={report.get('paired_sample_n')}")
    overall = report.get("overall") or {}
    lines.append(
        "overall: "
        f"peak30={_pct(overall.get('peak30_before_stop_rate'))} "
        f"stop={_pct(overall.get('stop_before_peak_rate'))} "
        f"mean_forecast_peak30={_pct(overall.get('mean_p_absorb_peak30'))} "
        f"proxy_ev_0.003={overall.get('proxy_ev_per_0_003_sol')}"
    )
    lines.append("")
    lines.append("by_markov_bucket:")
    for bucket, stats in (report.get("by_markov_bucket") or {}).items():
        lines.append(
            f"  {bucket}: n={stats.get('sample_n')} "
            f"peak30={_pct(stats.get('peak30_before_stop_rate'))} "
            f"stop={_pct(stats.get('stop_before_peak_rate'))} "
            f"mean_p={_pct(stats.get('mean_p_absorb_peak30'))}"
        )
    lines.append("")
    lines.append("top blocker families:")
    for family, stats in list((report.get("by_blocker_family") or {}).items())[:12]:
        lines.append(
            f"  {family}: n={stats.get('sample_n')} "
            f"peak30={_pct(stats.get('peak30_before_stop_rate'))} "
            f"stop={_pct(stats.get('stop_before_peak_rate'))}"
        )
    lines.append("")
    lines.append(f"health={json.dumps(report.get('health') or {}, sort_keys=True)}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="Path to paper_trades.db")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--since-ts", type=float, default=None)
    parser.add_argument("--until-ts", type=float, default=None)
    parser.add_argument("--min-sample", type=int, default=ptm.LOTTO_MICRO_RECLAIM_MARKOV_MIN_SAMPLE_N)
    parser.add_argument("--min-peak30-prob", type=float, default=ptm.LOTTO_MICRO_RECLAIM_MARKOV_MIN_PEAK30_PROB)
    parser.add_argument("--max-stop-prob", type=float, default=ptm.LOTTO_MICRO_RECLAIM_MARKOV_MAX_STOP_PROB)
    parser.add_argument("--min-edge", type=float, default=ptm.LOTTO_MICRO_RECLAIM_MARKOV_MIN_EDGE)
    parser.add_argument("--include-rows", action="store_true")
    parser.add_argument("--max-candidates", type=int, default=None)
    parser.add_argument("--json", action="store_true", help="Print JSON instead of text summary")
    parser.add_argument("--json-out", default=None, help="Write full JSON report to a file")
    args = parser.parse_args(argv)

    report = build_backtest_report(
        args.db,
        days=args.days,
        since_ts=args.since_ts,
        until_ts=args.until_ts,
        min_sample=args.min_sample,
        min_peak30_prob=args.min_peak30_prob,
        max_stop_prob=args.max_stop_prob,
        min_edge=args.min_edge,
        include_rows=args.include_rows,
        max_candidates=args.max_candidates,
    )
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(format_text_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

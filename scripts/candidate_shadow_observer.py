#!/usr/bin/env python3
"""Forward shadow observations and virtual trades for the 84-candidate mesh.

This script is intentionally shadow-only:
  - no pending entry is created
  - no paper trade is opened
  - no live execution path is touched

It writes one observation row per (premium signal, candidate_id), including
non-matches. For matched entry candidates it also writes a virtual trade row
using time-legal K-line closes. That makes missing data visible instead of
silently dropping a candidate.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sqlite3
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path


CANDIDATE_VERSION = "candidate-shadow-v1"
CONTEXT_SCHEMA_VERSION = "candidate-shadow-context-v2.no_signal_price_quote_inference"
EXPECTED_CANDIDATE_COUNT = 84
DEFAULT_VIRTUAL_STOP_LOSS_PCT = -3.0
DEFAULT_VIRTUAL_TRAIL_START_PCT = 3.0
DEFAULT_VIRTUAL_TRAIL_FACTOR = 0.95
DEFAULT_VIRTUAL_TIMEOUT_BARS = 120
_PAPER_TRADE_MONITOR = None
_PAPER_TRADE_MONITOR_IMPORT_ERROR = None


BASE_CANDIDATES = [
    "current_all",
    "current_would_enter_all",
    "current_would_enter_no_enqueue",
    "notath_new_trending_all",
    "notath_source_status_any",
    "notath_quote_clean",
    "notath_executable_quote_clean",
    "notath_mc_lt_30k",
    "notath_mc_5k_30k",
    "markov_yellow_or_green",
    "lotto_not_ath_reclaim_proxy",
    "lotto_low_liquidity_reclaim_proxy",
    "newborn_momentum_proxy",
    "lifecycle:stage1_notath_selective_v1",
    "old_filter:mc_si_ai_velocity_utc_pre3m",
    "old_filter:top10_ai_conf_holders",
]


KLINE_CANDIDATES = [
    "kline:first_bar_return_filters",
    "kline:fbr_ge_0",
    "kline:fbr_ge_1",
    "kline:fbr_ge_2",
    "kline:fbr_ge_3",
    "kline:fbr_ge_5",
    "kline:first_bar_green",
    "kline:first_bar_red",
    "kline:bar4_low_volume",
    "kline:vol_accel",
    "kline:candle_pattern",
    "kline:volume_profile",
    "kline:active_mom20_first3",
    "kline:active_mom30_first3",
    "kline:support_preserved_bar4",
    "kline:red_low_volume_active",
    "kline:red_lowvol_active30",
    "kline:red_support_lowvol_active20",
    "kline:lowvol_active20_support",
    "kline:old_red_pullback_score20_ge4",
    "kline:old_red_pullback_score30_ge4",
]


RUNTIME_CANDIDATES = [
    "runtime:entry_readiness_policy",
    "runtime:entry_mode_registry",
    "runtime:phase_policy",
    "runtime:entry_audit_contract",
]


LIFECYCLE_CANDIDATES = [
    "lifecycle:stage2a_stop_loss_recovery",
    "lifecycle:stage3_signal_awakening",
]


HISTORICAL_CANDIDATES = [
    "historical:smart_backtest_76wr",
    "historical:walk_forward_68wr",
    "historical:peak_exit_72wr",
    "historical:take_profit_30_72wr",
    "historical:take_profit_50_72wr",
    "historical:strategy_c_exit_shape",
]


def normalize_ts_sec(value):
    if value is None:
        return None
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(ts) or ts <= 0:
        return None
    if ts > 10_000_000_000:
        ts = ts / 1000.0
    return int(ts)


def first_number(*values):
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(parsed):
            return parsed
    return None


def parse_compact_number(raw):
    if raw is None:
        return None
    text = str(raw).strip().replace(",", "")
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*([KMB])?", text, re.IGNORECASE)
    if not m:
        return None
    value = float(m.group(1))
    suffix = (m.group(2) or "").upper()
    if suffix == "K":
        value *= 1_000
    elif suffix == "M":
        value *= 1_000_000
    elif suffix == "B":
        value *= 1_000_000_000
    return value


def parse_text_number(text, labels):
    if not text:
        return None
    label_pat = "|".join(re.escape(label) for label in labels)
    m = re.search(
        rf"(?:{label_pat})[^\d$-]*\$?\s*(-?\d+(?:[.,]\d+)?\s*[KMB]?)",
        str(text),
        re.IGNORECASE,
    )
    if not m:
        return None
    return parse_compact_number(m.group(1))


def parse_super_index(text):
    if not text:
        return None
    normalized = str(text).replace("**", "").replace("\r", "")
    m = re.search(
        r"Super\s+Index[：:]\s*\(signal\)\s*x?\d+\s*(?:-->|->|→|—>)\s*(?:\(current\)\s*)?x?(\d+)",
        normalized,
        re.IGNORECASE,
    )
    if m:
        return int(m.group(1))
    m = re.search(r"Super\s+Index[：:]\s*(\d+)", normalized, re.IGNORECASE)
    if m:
        return int(m.group(1))
    m = re.search(r"Super\s+Index[：:]\s*✡\s*x\s*(\d+)", normalized, re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None


def parse_top10_pct(text):
    if not text:
        return None
    m = re.search(r"top\s*10[^0-9]{0,24}(\d+(?:\.\d+)?)\s*%", str(text), re.IGNORECASE)
    if m:
        return float(m.group(1))
    return None


def parse_signal_price(text):
    if not text:
        return None
    m = re.search(r"Price[^\d$-]*\$?\s*(\d+(?:\.\d+)?(?:e-?\d+)?)", str(text), re.IGNORECASE)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    return None


def pct_change(start, end):
    if start is None or end is None or start == 0:
        return None
    try:
        return (float(end) / float(start) - 1.0) * 100.0
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def safe_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y", "ath"}


def ensure_schema(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_shadow_observations (
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
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_candidate_shadow_obs_signal "
        "ON candidate_shadow_observations(signal_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_candidate_shadow_obs_candidate "
        "ON candidate_shadow_observations(candidate_id, observed_at)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_shadow_virtual_trades (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          signal_id INTEGER NOT NULL,
          token_ca TEXT NOT NULL,
          signal_ts INTEGER,
          candidate_id TEXT NOT NULL,
          family TEXT,
          status TEXT NOT NULL,
          entry_ts INTEGER,
          entry_price REAL,
          entry_reason TEXT,
          exit_ts INTEGER,
          exit_price REAL,
          exit_reason TEXT,
          peak_pct REAL,
          gross_pnl_pct REAL,
          friction_bps REAL,
          net_pnl_pct REAL,
          observed_at INTEGER NOT NULL,
          payload_json TEXT NOT NULL,
          UNIQUE(signal_id, candidate_id)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_candidate_shadow_virtual_signal "
        "ON candidate_shadow_virtual_trades(signal_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_candidate_shadow_virtual_candidate "
        "ON candidate_shadow_virtual_trades(candidate_id, status, observed_at)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_shadow_kline_fetch_attempts (
          token_ca TEXT PRIMARY KEY,
          last_attempt_at INTEGER NOT NULL,
          status TEXT NOT NULL,
          bars_count INTEGER NOT NULL DEFAULT 0,
          reason TEXT
        )
        """
    )
    conn.commit()


def ensure_kline_schema(conn):
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='kline_1m'"
    ).fetchone()
    if exists:
        return
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS kline_1m (
          token_ca TEXT NOT NULL,
          pool_address TEXT NOT NULL DEFAULT '',
          timestamp INTEGER NOT NULL,
          open REAL NOT NULL,
          high REAL NOT NULL,
          low REAL NOT NULL,
          close REAL NOT NULL,
          volume REAL NOT NULL DEFAULT 0,
          PRIMARY KEY (token_ca, timestamp)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_kline_token ON kline_1m(token_ca)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_kline_ts ON kline_1m(token_ca, timestamp)")
    conn.commit()


def load_registry(path):
    with open(path, "r", encoding="utf-8") as fh:
        registry = json.load(fh)
    modes = registry.get("modes") or {}
    return registry, modes


def open_sqlite(path, label):
    try:
        conn = sqlite3.connect(path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn
    except sqlite3.Error as exc:
        raise RuntimeError(f"{label}_connect_error path={path}: {exc}") from exc


def warn_json(**payload):
    print(json.dumps(payload, sort_keys=True), file=sys.stderr)


def build_candidate_catalog(registry_modes):
    catalog = []
    for candidate_id in BASE_CANDIDATES:
        catalog.append({"candidate_id": candidate_id, "family": "base", "mode_meta": None})
    for candidate_id in KLINE_CANDIDATES:
        catalog.append({"candidate_id": candidate_id, "family": "kline", "mode_meta": None})
    for candidate_id in RUNTIME_CANDIDATES:
        catalog.append({"candidate_id": candidate_id, "family": "runtime", "mode_meta": None})
    for candidate_id in LIFECYCLE_CANDIDATES:
        catalog.append({"candidate_id": candidate_id, "family": "lifecycle", "mode_meta": None})
    for candidate_id in HISTORICAL_CANDIDATES:
        catalog.append({"candidate_id": candidate_id, "family": "historical", "mode_meta": None})
    for mode_id, meta in registry_modes.items():
        catalog.append(
            {
                "candidate_id": f"entry_mode_registry:{mode_id}",
                "family": "entry_mode_registry",
                "mode_meta": meta,
            }
        )
    if len(catalog) != EXPECTED_CANDIDATE_COUNT:
        raise RuntimeError(
            f"candidate catalog count mismatch: {len(catalog)} != {EXPECTED_CANDIDATE_COUNT}"
        )
    return catalog


def get_columns(conn, table):
    try:
        return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def load_signals(conn, limit, since_id=None):
    wanted = [
        "id",
        "token_ca",
        "symbol",
        "timestamp",
        "source_message_ts",
        "receive_ts",
        "signal_type",
        "is_ath",
        "market_cap",
        "holders",
        "volume_24h",
        "top10_pct",
        "ai_confidence",
        "ai_narrative_tier",
        "narrative_score",
        "description",
        "raw_message",
        "hard_gate_status",
        "signal_source",
    ]
    columns = get_columns(conn, "premium_signals")
    select_parts = [col if col in columns else f"NULL AS {col}" for col in wanted]
    sql_base = f"SELECT {', '.join(select_parts)} FROM premium_signals"
    if since_id is not None:
        sql = f"{sql_base} WHERE id > ? ORDER BY id ASC LIMIT ?"
        rows = conn.execute(sql, (int(since_id), int(limit))).fetchall()
    else:
        sql = f"{sql_base} ORDER BY id DESC LIMIT ?"
        rows = list(reversed(conn.execute(sql, (int(limit),)).fetchall()))
    return rows


def load_kline_bars(conn, token_ca, signal_ts_sec, limit=125):
    if not token_ca or not signal_ts_sec:
        return []
    floor_ts = int(signal_ts_sec // 60 * 60)
    try:
        rows = conn.execute(
            """
            SELECT timestamp, open, high, low, close, volume
            FROM kline_1m
            WHERE token_ca = ? AND timestamp >= ?
            ORDER BY timestamp ASC
            LIMIT ?
            """,
            (token_ca, floor_ts, int(limit)),
        ).fetchall()
    except sqlite3.Error:
        return []
    return [dict(row) for row in rows]


def load_source_resonance_features(conn, token_ca, signal_ts_sec):
    if not conn or not token_ca or not signal_ts_sec:
        return {}
    cols = get_columns(conn, "source_resonance_candidates")
    if not cols:
        return {}
    wanted = [
        "cohort",
        "quote_clean_seen",
        "two_quote_clean_snapshots",
        "entry_quote_success_seen",
        "entry_quote_fail_seen",
        "gmgn_pre_seen",
        "gmgn_lead_time_sec",
        "resonance_level",
        "resonance_score",
    ]
    select_parts = [col if col in cols else f"NULL AS {col}" for col in wanted]
    try:
        row = conn.execute(
            f"""
            SELECT {', '.join(select_parts)}
            FROM source_resonance_candidates
            WHERE token_ca = ? AND signal_ts BETWEEN ? AND ?
            ORDER BY ABS(signal_ts - ?) ASC, COALESCE(resonance_score, 0) DESC
            LIMIT 1
            """,
            (token_ca, int(signal_ts_sec) - 600, int(signal_ts_sec) + 600, int(signal_ts_sec)),
        ).fetchone()
    except sqlite3.Error:
        return {}
    if not row:
        return {}
    quote_clean = bool(
        safe_bool(row["quote_clean_seen"])
        or safe_bool(row["two_quote_clean_snapshots"])
    )
    quote_executable = safe_bool(row["entry_quote_success_seen"])
    cohort = row["cohort"]
    level = first_number(row["resonance_level"])
    return {
        "source_resonance_seen": True,
        "source_resonance_state": cohort or (f"level_{int(level)}" if level is not None else "seen"),
        "source_resonance_cohort": cohort,
        "source_resonance_level": level,
        "source_resonance_score": first_number(row["resonance_score"]),
        "gmgn_pre_seen": safe_bool(row["gmgn_pre_seen"]),
        "gmgn_lead_time_sec": first_number(row["gmgn_lead_time_sec"]),
        "source_quote_clean": quote_clean,
        "source_quote_clean_seen": quote_clean,
        "source_quote_executable": quote_executable,
        "source_quote_executable_proxy": quote_executable,
        "source_entry_quote_fail_seen": safe_bool(row["entry_quote_fail_seen"]),
    }


def safe_json(raw):
    try:
        value = json.loads(raw or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def nested_get(payload, *paths):
    for path in paths:
        cur = payload
        for key in path:
            if not isinstance(cur, dict) or key not in cur:
                cur = None
                break
            cur = cur[key]
        if cur not in (None, ""):
            return cur
    return None


def extract_markov_bucket(payload):
    value = nested_get(
        payload,
        ("gate", "markov_bucket"),
        ("markov_reclaim_gate", "markov_bucket"),
        ("markovReclaimGate", "markov_bucket"),
        ("markov_reclaim_forecast", "gate", "markov_bucket"),
        ("markovReclaimForecast", "gate", "markov_bucket"),
        ("lotto_markov_reclaim_forecast", "gate", "markov_bucket"),
        ("revival_canary", "markov_reclaim_gate", "markov_bucket"),
        ("revival_canary", "markov_reclaim_forecast", "gate", "markov_bucket"),
        ("revival_canary", "markov_bucket"),
        ("learning_bypass", "markov_bucket"),
        ("markov_bucket",),
    )
    if value in (None, "", "null"):
        return None
    return str(value).lower()


def load_paper_trade_monitor_module():
    global _PAPER_TRADE_MONITOR, _PAPER_TRADE_MONITOR_IMPORT_ERROR
    if _PAPER_TRADE_MONITOR is not None:
        return _PAPER_TRADE_MONITOR
    if _PAPER_TRADE_MONITOR_IMPORT_ERROR is not None:
        return None
    script_dir = str(Path(__file__).resolve().parent)
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)
    try:
        import paper_trade_monitor  # type: ignore
    except Exception as exc:
        _PAPER_TRADE_MONITOR_IMPORT_ERROR = f"{type(exc).__name__}:{exc}"
        return None
    _PAPER_TRADE_MONITOR = paper_trade_monitor
    return _PAPER_TRADE_MONITOR


def choose_shadow_markov_entry_mode(features, monitor_module):
    notath_mode = getattr(
        monitor_module,
        "LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE",
        "lotto_not_ath_reclaim_tiny_probe",
    )
    micro_mode = getattr(
        monitor_module,
        "LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE",
        "lotto_micro_reclaim_tiny_probe",
    )
    text = " ".join(
        str(features.get(key) or "").lower()
        for key in ("hard_gate_status", "source_component", "source_resonance_state")
    )
    if "micro" in text or "dead_cat" in text or "runner_watch" in text:
        return micro_mode
    return notath_mode


def build_shadow_markov_features(conn, features, now_ts):
    if features.get("markov_bucket"):
        return {}
    monitor_module = load_paper_trade_monitor_module()
    if monitor_module is None:
        return {
            "markov_available": False,
            "markov_missing_reason": _PAPER_TRADE_MONITOR_IMPORT_ERROR or "paper_trade_monitor_import_failed",
            "markov_source": "shadow_lotto_reclaim_forecast_unavailable",
        }
    forecast_fn = getattr(monitor_module, "build_lotto_reclaim_markov_forecast", None)
    if not callable(forecast_fn):
        return {
            "markov_available": False,
            "markov_missing_reason": "paper_trade_monitor_markov_forecast_missing",
            "markov_source": "shadow_lotto_reclaim_forecast_unavailable",
        }
    entry_mode = choose_shadow_markov_entry_mode(features, monitor_module)
    source_quote_clean = bool(
        safe_bool(features.get("source_quote_clean"))
        or safe_bool(features.get("source_quote_clean_seen"))
    )
    source_quote_executable = bool(
        safe_bool(features.get("source_quote_executable"))
    )
    quote_clean = bool(source_quote_clean or source_quote_executable)
    markov_payload = {
        "context_schema_version": CONTEXT_SCHEMA_VERSION,
        "quote_clean_definition": "source_or_executable_quote_only_no_signal_price",
        "signal_price_seen": bool(features.get("signal_price_seen") or features.get("signal_price_positive")),
        "source_quote_clean": source_quote_clean,
        "source_quote_executable": source_quote_executable,
        "quote_clean": quote_clean,
        "quote_clean_seen": quote_clean,
        "quote_executable": source_quote_executable,
        "pc_m5": features.get("first5_return_pct"),
        "price_change_m5": features.get("first5_return_pct"),
        "entry_branch": features.get("source_resonance_state") or features.get("hard_gate_status"),
        "source_reject_reason": features.get("hard_gate_status"),
    }
    pending = {
        "token_ca": features.get("token_ca"),
        "symbol": features.get("symbol"),
        "entry_mode": entry_mode,
        "scout_mode": entry_mode,
        "entry_branch": markov_payload["entry_branch"],
        "source_reject_reason": markov_payload["source_reject_reason"],
        "markov_features": markov_payload,
    }
    lifecycle = {
        "lifecycle_state": features.get("lifecycle_state"),
        "entry_bias": features.get("entry_bias"),
    }
    try:
        forecast = forecast_fn(
            conn,
            entry_mode=entry_mode,
            pending=pending,
            lifecycle=lifecycle,
            now_ts=now_ts,
        )
    except Exception as exc:
        return {
            "markov_available": False,
            "markov_missing_reason": f"shadow_markov_forecast_error:{type(exc).__name__}",
            "markov_source": "shadow_lotto_reclaim_forecast_error",
        }
    forecast_dict = forecast if isinstance(forecast, dict) else {}
    gate = forecast_dict.get("gate")
    if not isinstance(gate, dict) or not gate.get("markov_bucket"):
        return {
            "markov_available": False,
            "markov_missing_reason": "shadow_markov_forecast_no_bucket",
            "markov_source": "shadow_lotto_reclaim_forecast_empty",
        }
    return {
        "markov_available": True,
        "markov_bucket": str(gate.get("markov_bucket")).lower(),
        "markov_missing_reason": None,
        "markov_source": "shadow_lotto_reclaim_forecast",
        "markov_entry_mode": entry_mode,
        "markov_gate_reason": gate.get("reason"),
        "markov_pass": safe_bool(gate.get("pass")),
        "markov_sample_n": gate.get("sample_n"),
        "markov_p_absorb_peak30": gate.get("p_absorb_peak30"),
        "markov_p_absorb_stop_before_peak": gate.get("p_absorb_stop_before_peak"),
        "markov_edge_peak30_minus_stop": gate.get("edge_peak30_minus_stop"),
        "markov_model_family": forecast_dict.get("model_family"),
        "markov_cohort_key": forecast_dict.get("cohort_key"),
        "markov_event_count": forecast_dict.get("event_count"),
    }


def extract_matrix_bucket(payload):
    if not isinstance(payload, dict) or not payload:
        return None
    grade = payload.get("matrix_grade") or payload.get("grade")
    if grade not in (None, ""):
        return str(grade).lower()
    try:
        green = int(payload.get("green_count") or 0)
        yellow = int(payload.get("yellow_count") or 0)
        red = int(payload.get("red_count") or 0)
        hard_red = int(payload.get("hard_red_dimensions") or 0)
    except (TypeError, ValueError):
        return None
    if hard_red > 0 or red > green:
        return "red"
    if green >= 2 and red == 0:
        return "green"
    if green > 0 or yellow > 0:
        return "yellow"
    return None


def load_global_runtime_features(conn, token_ca, signal_ts_sec):
    features = {
        "markov_available": False,
        "markov_missing_reason": "missing_markov_bucket_readmodel",
    }
    if not conn or not token_ca or not signal_ts_sec:
        return features
    lo = int(signal_ts_sec) - 600
    hi = int(signal_ts_sec) + 600

    if get_columns(conn, "paper_decision_events"):
        try:
            rows = conn.execute(
                """
                SELECT event_ts, lifecycle_state, entry_bias, payload_json
                FROM paper_decision_events
                WHERE event_ts BETWEEN ? AND ? AND token_ca = ?
                ORDER BY ABS(event_ts - ?) ASC
                LIMIT 25
                """,
                (lo, hi, token_ca, int(signal_ts_sec)),
            ).fetchall()
        except sqlite3.Error:
            rows = []
        for row in rows:
            payload = safe_json(row["payload_json"])
            bucket = extract_markov_bucket(payload)
            if bucket:
                features.update(
                    {
                        "markov_available": True,
                        "markov_bucket": bucket,
                        "markov_missing_reason": None,
                    }
                )
                break
        for row in rows:
            payload = safe_json(row["payload_json"])
            state = row["lifecycle_state"] or payload.get("lifecycle_state") or nested_get(payload, ("lifecycle", "state"))
            bias = row["entry_bias"] or payload.get("entry_bias") or nested_get(payload, ("lifecycle", "entry_bias"))
            if state and "lifecycle_state" not in features:
                features["lifecycle_state"] = str(state)
            if bias and "entry_bias" not in features:
                features["entry_bias"] = str(bias)
            if state or bias:
                features["lifecycle_profile"] = ":".join(str(x) for x in (state, bias) if x not in (None, ""))
                break

    if get_columns(conn, "a_class_decision_events"):
        try:
            rows = conn.execute(
                """
                SELECT event_ts, source_component, source_reason, quote_clean, quote_executable,
                       matrix_json, risk_json
                FROM a_class_decision_events
                WHERE event_ts BETWEEN ? AND ? AND token_ca = ?
                ORDER BY ABS(event_ts - ?) ASC
                LIMIT 25
                """,
                (lo, hi, token_ca, int(signal_ts_sec)),
            ).fetchall()
        except sqlite3.Error:
            rows = []
        for row in rows:
            risk = safe_json(row["risk_json"])
            matrix = extract_matrix_bucket(safe_json(row["matrix_json"]))
            quote_clean = safe_bool(row["quote_clean"]) or safe_bool(risk.get("quote_clean_verified"))
            quote_executable = safe_bool(row["quote_executable"])
            component = row["source_component"] or "a_class_decision_events"
            reason = row["source_reason"]
            features.update(
                {
                    "matrix_bucket": matrix or features.get("matrix_bucket"),
                    "source_component": component,
                    "source_resonance_state": ":".join(str(x) for x in (component, reason) if x not in (None, "")),
                    "source_quote_clean": quote_clean,
                    "source_quote_clean_seen": quote_clean,
                    "source_quote_executable": quote_executable,
                    "source_quote_executable_proxy": quote_executable,
                }
            )
            break

    if "source_component" not in features and get_columns(conn, "opportunity_events"):
        try:
            row = conn.execute(
                """
                SELECT event_ts, source_component, source_reason, source_type, quote_clean, quote_executable
                FROM opportunity_events
                WHERE event_ts BETWEEN ? AND ? AND token_ca = ?
                ORDER BY ABS(event_ts - ?) ASC
                LIMIT 1
                """,
                (lo, hi, token_ca, int(signal_ts_sec)),
            ).fetchone()
        except sqlite3.Error:
            row = None
        if row:
            component = row["source_component"] or row["source_type"] or "opportunity_events"
            reason = row["source_reason"]
            quote_clean = safe_bool(row["quote_clean"])
            quote_executable = safe_bool(row["quote_executable"])
            features.update(
                {
                    "source_component": component,
                    "source_resonance_state": ":".join(str(x) for x in (component, reason) if x not in (None, "")),
                    "source_quote_clean": quote_clean,
                    "source_quote_clean_seen": quote_clean,
                    "source_quote_executable": quote_executable,
                    "source_quote_executable_proxy": quote_executable,
                }
            )
    return features


def json_get(url, timeout=12):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "candidate-shadow-observer/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (OSError, urllib.error.URLError, json.JSONDecodeError, TimeoutError):
        return None


def find_pool_address(conn, token_ca):
    try:
        row = conn.execute(
            "SELECT pool_address FROM kline_1m WHERE token_ca=? AND pool_address IS NOT NULL AND TRIM(pool_address)!='' ORDER BY timestamp DESC LIMIT 1",
            (token_ca,),
        ).fetchone()
        if row and row["pool_address"]:
            return row["pool_address"]
    except sqlite3.Error:
        pass
    data = json_get(f"https://api.dexscreener.com/latest/dex/tokens/{token_ca}")
    pairs = (data or {}).get("pairs") or []
    if not pairs:
        return None
    sol_pairs = [pair for pair in pairs if pair.get("chainId") == "solana"] or pairs
    best = max(sol_pairs, key=lambda pair: ((pair.get("liquidity") or {}).get("usd") or 0))
    return best.get("pairAddress")


def fetch_kline_fallback(conn, token_ca):
    pool = find_pool_address(conn, token_ca)
    if not pool:
        return 0, "pool_not_found"
    data = json_get(
        f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool}/ohlcv/minute?aggregate=1&limit=1000"
    )
    ohlcv = ((data or {}).get("data") or {}).get("attributes", {}).get("ohlcv_list") or []
    if not ohlcv:
        return 0, "ohlcv_empty"
    rows = []
    for candle in ohlcv:
        if len(candle) < 6:
            continue
        rows.append((token_ca, pool, int(candle[0]), float(candle[1]), float(candle[2]), float(candle[3]), float(candle[4]), float(candle[5] or 0)))
    if not rows:
        return 0, "ohlcv_invalid"
    conn.executemany(
        """
        INSERT OR IGNORE INTO kline_1m
          (token_ca, pool_address, timestamp, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    return len(rows), "ok"


def should_fetch_kline(out_conn, token_ca, now_sec, cooldown_sec):
    row = out_conn.execute(
        "SELECT last_attempt_at FROM candidate_shadow_kline_fetch_attempts WHERE token_ca=?",
        (token_ca,),
    ).fetchone()
    if not row:
        return True
    return int(now_sec) - int(row["last_attempt_at"]) >= int(cooldown_sec)


def record_kline_fetch_attempt(out_conn, token_ca, now_sec, status, bars_count, reason):
    out_conn.execute(
        """
        INSERT INTO candidate_shadow_kline_fetch_attempts
          (token_ca, last_attempt_at, status, bars_count, reason)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(token_ca) DO UPDATE SET
          last_attempt_at=excluded.last_attempt_at,
          status=excluded.status,
          bars_count=excluded.bars_count,
          reason=excluded.reason
        """,
        (token_ca, int(now_sec), status, int(bars_count or 0), reason),
    )


def classify_candle(bar):
    if not bar:
        return None
    open_p = float(bar["open"])
    close_p = float(bar["close"])
    high_p = float(bar["high"])
    low_p = float(bar["low"])
    rng = max(high_p - low_p, 0.0)
    body = close_p - open_p
    abs_body = abs(body)
    if rng <= 0:
        return "flat"
    body_ratio = abs_body / rng
    upper = high_p - max(open_p, close_p)
    lower = min(open_p, close_p) - low_p
    if body_ratio <= 0.1:
        return "doji"
    if body > 0 and body_ratio >= 0.6:
        return "strong_bull"
    if body < 0 and body_ratio >= 0.6:
        return "strong_bear"
    if lower > abs_body * 2 and upper < abs_body:
        return "hammer"
    if upper > abs_body * 2 and lower < abs_body:
        return "shooting_star"
    return "green" if body > 0 else "red"


def volume_profile(bars):
    vols = [float(bar["volume"] or 0) for bar in bars]
    if len(vols) < 3:
        return "unknown"
    if vols[-1] > max(vols[:-1]) * 1.8:
        return "climax"
    if all(vols[i] <= vols[i + 1] for i in range(len(vols) - 1)):
        return "building"
    if all(vols[i] >= vols[i + 1] for i in range(len(vols) - 1)):
        return "declining"
    if max(vols) <= 0:
        return "flat"
    if (max(vols) - min(vols)) / max(vols) < 0.2:
        return "flat"
    return "mixed"


def compute_kline_features(bars, signal_ts_sec, now_sec):
    features = {
        "kline_projection_available": False,
        "kline_bar_count": len(bars),
        "fbr_time_legal": False,
        "kline_time_legal": False,
    }
    if not bars:
        features["kline_missing_reason"] = "no_signal_time_kline_bars"
        return features

    first = bars[0]
    first_open = float(first["open"])
    first_close = float(first["close"])
    fbr = pct_change(first_open, first_close)
    first_close_ts = int(first["timestamp"]) + 60
    features.update(
        {
            "kline_projection_available": True,
            "entry_bar_open_ts": int(first["timestamp"]),
            "entry_bar_close_ts": first_close_ts,
            "signal_to_bar_alignment_sec": int(first["timestamp"]) - int(signal_ts_sec or 0),
            "fbr_time_legal": bool(now_sec >= first_close_ts),
            "fbr_lookahead_warning": bool(now_sec < first_close_ts),
            "first_bar_return_pct": fbr,
            "fbr_ge_0": bool(fbr is not None and fbr >= 0),
            "fbr_ge_1": bool(fbr is not None and fbr >= 1),
            "fbr_ge_2": bool(fbr is not None and fbr >= 2),
            "fbr_ge_3": bool(fbr is not None and fbr >= 3),
            "fbr_ge_5": bool(fbr is not None and fbr >= 5),
            "entry_bar_green": bool(first_close > first_open),
            "entry_bar_red": bool(first_close < first_open),
            "entry_body_ratio": None,
            "candle_pattern": classify_candle(first),
            "volume_profile": volume_profile(bars[:5]),
        }
    )
    first_range = float(first["high"]) - float(first["low"])
    if first_range > 0:
        features["entry_body_ratio"] = abs(first_close - first_open) / first_range

    if len(bars) >= 3:
        first3 = bars[:3]
        green_count = sum(1 for bar in first3 if float(bar["close"]) > float(bar["open"]))
        mom3 = pct_change(float(first3[0]["open"]), float(first3[-1]["close"]))
        features.update(
            {
                "first3_green_count": green_count,
                "trend_strength_first3": bool(green_count >= 2),
                "first3_momentum_pct": mom3,
                "active_mom20_first3": bool(mom3 is not None and abs(mom3) >= 20),
                "active_mom30_first3": bool(mom3 is not None and abs(mom3) >= 30),
                "pre3_low": min(float(bar["low"]) for bar in first3),
                "pre3_avg_volume": sum(float(bar["volume"] or 0) for bar in first3) / 3.0,
                "vol_accel": pct_change(float(first3[0]["volume"] or 0), float(first3[-1]["volume"] or 0)),
                "consecutive_green_bars": green_count if green_count == 3 else 0,
            }
        )
    else:
        features.update(
            {
                "trend_strength_first3": False,
                "active_mom20_first3": False,
                "active_mom30_first3": False,
            }
        )

    if len(bars) >= 5:
        first5 = bars[:5]
        features["first5_return_pct"] = pct_change(float(first5[0]["open"]), float(first5[-1]["close"]))
        features["first5_close_ts"] = int(first5[-1]["timestamp"]) + 60
        features["first5_time_legal"] = bool(now_sec >= features["first5_close_ts"])

    if len(bars) >= 4:
        bar4 = bars[3]
        bar4_close_ts = int(bar4["timestamp"]) + 60
        current_close = float(bar4["close"])
        current_open = float(bar4["open"])
        current_volume = float(bar4["volume"] or 0)
        pre3_low = features.get("pre3_low")
        pre3_avg_volume = features.get("pre3_avg_volume")
        support_preserved = bool(pre3_low is not None and current_close > pre3_low)
        low_volume = bool(pre3_avg_volume is not None and current_volume <= pre3_avg_volume)
        red_bar = bool(current_close < current_open)
        active20 = bool(features.get("active_mom20_first3"))
        active30 = bool(features.get("active_mom30_first3"))
        score20 = 2 if red_bar else 0
        score30 = 2 if red_bar else 0
        if support_preserved:
            score20 += 1
            score30 += 1
        if low_volume:
            score20 += 1
            score30 += 1
        if active20:
            score20 += 1
        if active30:
            score30 += 1
        features.update(
            {
                "kline_time_legal": bool(now_sec >= bar4_close_ts),
                "bar4_close_ts": bar4_close_ts,
                "bar4_red": red_bar,
                "bar4_low_volume": low_volume,
                "support_preserved_bar4": support_preserved,
                "red_low_volume_active20": bool(red_bar and low_volume and active20),
                "red_lowvol_active30": bool(red_bar and low_volume and active30),
                "red_support_lowvol_active20": bool(red_bar and support_preserved and low_volume and active20),
                "lowvol_active20_support": bool(low_volume and active20 and support_preserved),
                "old_red_pullback_score20": score20,
                "old_red_pullback_score30": score30,
                "old_red_pullback_score20_ge4": bool(score20 >= 4),
                "old_red_pullback_score30_ge4": bool(score30 >= 4),
            }
        )
    return features


def extract_signal_features(row, kline_features, source_features=None):
    text = "\n".join(str(row[key] or "") for key in ("description", "raw_message"))
    signal_ts = normalize_ts_sec(row["timestamp"]) or normalize_ts_sec(row["receive_ts"])
    signal_type = str(row["signal_type"] or "").upper()
    status = str(row["hard_gate_status"] or "").upper()
    is_ath = safe_bool(row["is_ath"]) or ("ATH" in signal_type and "NEW_TRENDING" not in signal_type)
    is_new_trending = signal_type == "NEW_TRENDING" or "new trending" in text.lower()
    market_cap = first_number(row["market_cap"], parse_text_number(text, ["MC", "Market Cap", "MCap"]))
    holders = first_number(row["holders"], parse_text_number(text, ["Holders"]))
    volume_24h = first_number(row["volume_24h"], parse_text_number(text, ["Vol24H", "Volume 24H", "Vol"]))
    top10_pct = first_number(row["top10_pct"], parse_top10_pct(text))
    ai_confidence = first_number(row["ai_confidence"])
    narrative_score = first_number(row["narrative_score"])
    signal_price = parse_signal_price(text)
    super_index = parse_super_index(text)

    features = {
        "signal_id": int(row["id"]),
        "token_ca": row["token_ca"],
        "symbol": row["symbol"],
        "signal_ts": signal_ts,
        "signal_type": signal_type,
        "hard_gate_status": status,
        "signal_source": row["signal_source"],
        "is_ath": is_ath,
        "is_new_trending": is_new_trending,
        "is_not_ath_new_trending": bool(is_new_trending and not is_ath),
        "market_cap": market_cap,
        "holders": holders,
        "volume_24h": volume_24h,
        "top10_pct": top10_pct,
        "ai_confidence": ai_confidence,
        "ai_narrative_tier": row["ai_narrative_tier"],
        "narrative_score": narrative_score,
        "signal_price": signal_price,
        "signal_price_seen": bool(signal_price and signal_price > 0),
        "signal_price_positive": bool(signal_price and signal_price > 0),
        "super_index": super_index,
        "status_has_reclaim": "RECLAIM" in status,
        "status_has_no_kline": "KLINE" in status or "UNKNOWN_DATA" in status,
        "status_pass": status == "PASS",
    }
    features.update(kline_features)
    features.update(source_features or {})
    return features


def reason_for_missing(*parts):
    return ",".join(part for part in parts if part) or "condition_not_met"


def require_kline(features, min_bars=1, legal_field=None):
    if not features.get("kline_projection_available"):
        return False, "missing_kline"
    if int(features.get("kline_bar_count") or 0) < min_bars:
        return False, f"needs_{min_bars}_kline_bars"
    if legal_field and not features.get(legal_field):
        return False, "kline_not_time_legal_yet"
    return True, None


def eval_base_candidate(candidate_id, features):
    if candidate_id == "current_all":
        return True, "all_signals_denominator"
    if candidate_id == "current_would_enter_all":
        return False, "missing_current_fullnet_would_enter_field"
    if candidate_id == "current_would_enter_no_enqueue":
        return False, "missing_current_fullnet_no_enqueue_field"
    if candidate_id == "notath_new_trending_all":
        ok = bool(features.get("is_not_ath_new_trending"))
        return ok, "notath_new_trending" if ok else "not_notath_new_trending"
    if candidate_id == "notath_source_status_any":
        ok = bool(features.get("is_not_ath_new_trending") and features.get("hard_gate_status"))
        return ok, "notath_status_present" if ok else "missing_notath_or_status"
    if candidate_id == "notath_quote_clean":
        quote_clean = bool(
            safe_bool(features.get("source_quote_clean"))
            or safe_bool(features.get("source_quote_clean_seen"))
        )
        ok = bool(features.get("is_not_ath_new_trending") and quote_clean)
        return ok, "runtime_source_quote_clean" if ok else "missing_runtime_source_quote_clean"
    if candidate_id == "notath_executable_quote_clean":
        ok = bool(features.get("is_not_ath_new_trending") and features.get("source_quote_executable"))
        return ok, "runtime_executable_quote_clean" if ok else "missing_runtime_executable_quote_clean"
    if candidate_id == "notath_mc_lt_30k":
        mc = features.get("market_cap")
        ok = bool(features.get("is_not_ath_new_trending") and mc is not None and mc < 30_000)
        return ok, "notath_mc_lt_30k" if ok else "not_notath_or_mc_not_lt_30k"
    if candidate_id == "notath_mc_5k_30k":
        mc = features.get("market_cap")
        ok = bool(features.get("is_not_ath_new_trending") and mc is not None and 5_000 <= mc < 30_000)
        return ok, "notath_mc_5k_30k" if ok else "not_notath_or_mc_not_5k_30k"
    if candidate_id == "markov_yellow_or_green":
        bucket = str(features.get("markov_bucket") or "").lower()
        ok = bucket in {"yellow", "green"}
        return ok, f"markov_{bucket}" if ok else str(features.get("markov_missing_reason") or "markov_not_yellow_or_green")
    if candidate_id == "lotto_not_ath_reclaim_proxy":
        ok = bool(features.get("is_not_ath_new_trending") and features.get("status_has_reclaim"))
        return ok, "notath_reclaim_status_proxy" if ok else "missing_notath_reclaim_status"
    if candidate_id == "lotto_low_liquidity_reclaim_proxy":
        mc = features.get("market_cap")
        ok = bool(features.get("is_not_ath_new_trending") and features.get("status_has_reclaim") and mc and mc < 15_000)
        return ok, "low_mc_reclaim_status_proxy" if ok else "missing_low_liquidity_reclaim_proxy"
    if candidate_id == "newborn_momentum_proxy":
        ok = bool(features.get("is_not_ath_new_trending") and features.get("active_mom30_first3"))
        return ok, "notath_active_mom30_proxy" if ok else "missing_notath_active_mom30"
    if candidate_id == "lifecycle:stage1_notath_selective_v1":
        si = features.get("super_index")
        ok = bool(features.get("is_not_ath_new_trending") and si is not None and si > 80)
        return ok, "stage1_notath_super_gt_80" if ok else "missing_notath_or_super_gt_80"
    if candidate_id == "old_filter:mc_si_ai_velocity_utc_pre3m":
        mc = features.get("market_cap")
        si = features.get("super_index")
        ai = features.get("ai_confidence")
        first5 = features.get("first5_return_pct")
        signal_ts = features.get("signal_ts")
        utc_hour = time.gmtime(signal_ts).tm_hour if signal_ts else None
        failures = []
        if not features.get("is_not_ath_new_trending"):
            failures.append("not_notath")
        if mc is None or not (5_000 <= mc < 30_000):
            failures.append("mc_not_5k_30k")
        if si is None or si <= 80:
            failures.append("super_not_gt_80")
        if ai is not None and ai < 60:
            failures.append("ai_lt_60")
        if utc_hour in (20, 21):
            failures.append("utc_20_22")
        if not features.get("first5_time_legal"):
            failures.append("first5_not_time_legal")
        if first5 is None or first5 < 30:
            failures.append("first_bar_return_lt_30_proxy")
        return not failures, "old_filter_pass" if not failures else ",".join(failures)
    if candidate_id == "old_filter:top10_ai_conf_holders":
        top10 = features.get("top10_pct")
        ai = features.get("ai_confidence")
        holders = features.get("holders")
        failures = []
        if not features.get("is_not_ath_new_trending"):
            failures.append("not_notath")
        if top10 is None or not (20 <= top10 <= 30):
            failures.append("top10_not_20_30")
        if ai is None or ai < 60:
            failures.append("ai_conf_lt_60")
        if holders is None or holders < 100:
            failures.append("holders_lt_100")
        return not failures, "top10_ai_holders_pass" if not failures else ",".join(failures)
    return False, "unknown_base_candidate"


def eval_kline_candidate(candidate_id, features):
    if candidate_id in {
        "kline:first_bar_return_filters",
        "kline:fbr_ge_0",
        "kline:fbr_ge_1",
        "kline:fbr_ge_2",
        "kline:fbr_ge_3",
        "kline:fbr_ge_5",
        "kline:first_bar_green",
        "kline:first_bar_red",
    }:
        ok, reason = require_kline(features, 1, "fbr_time_legal")
        if not ok:
            return False, reason
    else:
        ok, reason = require_kline(features, 4, "kline_time_legal")
        if not ok:
            return False, reason

    if candidate_id == "kline:first_bar_return_filters":
        return features.get("first_bar_return_pct") is not None, "fbr_observed"
    if candidate_id.startswith("kline:fbr_ge_"):
        threshold = candidate_id.rsplit("_", 1)[-1]
        key = f"fbr_ge_{threshold}"
        return bool(features.get(key)), key if features.get(key) else f"{key}_false"
    if candidate_id == "kline:first_bar_green":
        return bool(features.get("entry_bar_green")), "first_bar_green" if features.get("entry_bar_green") else "first_bar_not_green"
    if candidate_id == "kline:first_bar_red":
        return bool(features.get("entry_bar_red")), "first_bar_red" if features.get("entry_bar_red") else "first_bar_not_red"
    if candidate_id == "kline:bar4_low_volume":
        return bool(features.get("bar4_low_volume")), "bar4_low_volume" if features.get("bar4_low_volume") else "bar4_not_low_volume"
    if candidate_id == "kline:vol_accel":
        value = features.get("vol_accel")
        ok = bool(value is not None and value > 0)
        return ok, "vol_accel_positive" if ok else "vol_accel_not_positive"
    if candidate_id == "kline:candle_pattern":
        return bool(features.get("candle_pattern")), str(features.get("candle_pattern") or "missing_candle_pattern")
    if candidate_id == "kline:volume_profile":
        return bool(features.get("volume_profile") and features.get("volume_profile") != "unknown"), str(features.get("volume_profile") or "missing_volume_profile")
    mapping = {
        "kline:active_mom20_first3": "active_mom20_first3",
        "kline:active_mom30_first3": "active_mom30_first3",
        "kline:support_preserved_bar4": "support_preserved_bar4",
        "kline:red_low_volume_active": "red_low_volume_active20",
        "kline:red_lowvol_active30": "red_lowvol_active30",
        "kline:red_support_lowvol_active20": "red_support_lowvol_active20",
        "kline:lowvol_active20_support": "lowvol_active20_support",
        "kline:old_red_pullback_score20_ge4": "old_red_pullback_score20_ge4",
        "kline:old_red_pullback_score30_ge4": "old_red_pullback_score30_ge4",
    }
    key = mapping.get(candidate_id)
    if key:
        return bool(features.get(key)), key if features.get(key) else f"{key}_false"
    return False, "unknown_kline_candidate"


def eval_runtime_candidate(candidate_id, features):
    if candidate_id == "runtime:entry_mode_registry":
        return True, "registry_loaded"
    if candidate_id == "runtime:entry_readiness_policy":
        return False, "missing_entry_readiness_readmodel"
    if candidate_id == "runtime:phase_policy":
        return False, "missing_phase_policy_readmodel"
    if candidate_id == "runtime:entry_audit_contract":
        return False, "missing_entry_audit_contract_no_order_fill"
    return False, "unknown_runtime_candidate"


def eval_lifecycle_candidate(candidate_id, features):
    if candidate_id == "lifecycle:stage2a_stop_loss_recovery":
        return False, "needs_parent_stage1_stop_loss_trade_path"
    if candidate_id == "lifecycle:stage3_signal_awakening":
        return False, "needs_parent_profit_trail_and_second_signal"
    return False, "unknown_lifecycle_candidate"


def eval_historical_candidate(candidate_id, features):
    return False, "historical_template_observed_only_no_online_trigger"


def route_matches(mode_route, features):
    route = str(mode_route or "MIXED").upper()
    if route == "MIXED":
        return True
    if route == "ATH":
        return bool(features.get("is_ath"))
    if route == "LOTTO":
        return bool(features.get("is_not_ath_new_trending"))
    if route == "MATRIX_NORMAL":
        return False
    return True


def eval_registry_mode(mode_id, meta, features):
    route_ok = route_matches(meta.get("route"), features)
    if not route_ok:
        return False, "route_mismatch"
    family = str(meta.get("family") or "").lower()
    mc = features.get("market_cap")
    top10 = features.get("top10_pct")
    if "reclaim" in family:
        ok = bool(features.get("status_has_reclaim"))
        return ok, "reclaim_status_proxy" if ok else "missing_reclaim_status"
    if "no_kline" in family:
        ok = not bool(features.get("kline_projection_available"))
        return ok, "no_kline_proxy" if ok else "kline_available"
    if "high_mc" in family or "midcap" in family:
        ok = bool(mc is not None and mc >= 200_000)
        return ok, "high_mc_proxy" if ok else "mc_below_high_mc_proxy"
    if "concentration" in family or "concentrated" in family:
        ok = bool(top10 is not None and top10 >= 40)
        return ok, "top10_concentration_proxy" if ok else "missing_or_low_top10"
    if "momentum" in family or "newborn" in family:
        ok = bool(features.get("active_mom30_first3") or features.get("old_red_pullback_score20_ge4"))
        return ok, "kline_momentum_proxy" if ok else "missing_kline_momentum_proxy"
    if "pullback" in family:
        ok = bool(features.get("old_red_pullback_score20_ge4") or features.get("support_preserved_bar4"))
        return ok, "pullback_kline_proxy" if ok else "missing_pullback_proxy"
    if "unknown" in family or "low_kline" in family:
        ok = not bool(features.get("kline_projection_available")) or bool(features.get("status_has_no_kline"))
        return ok, "unknown_or_low_kline_proxy" if ok else "kline_available"
    if "uncertainty" in family or "dissonance" in family:
        return True, "route_matched_uncertainty_family"
    if "baseline" in family or "upstream" in family or "primary" in family or "small_live_probe" in family:
        return True, "route_matched"
    return True, "route_matched_generic_family"


def eval_candidate(candidate, features):
    candidate_id = candidate["candidate_id"]
    if candidate_id in BASE_CANDIDATES:
        return eval_base_candidate(candidate_id, features)
    if candidate_id in KLINE_CANDIDATES:
        return eval_kline_candidate(candidate_id, features)
    if candidate_id in RUNTIME_CANDIDATES:
        return eval_runtime_candidate(candidate_id, features)
    if candidate_id in LIFECYCLE_CANDIDATES:
        return eval_lifecycle_candidate(candidate_id, features)
    if candidate_id in HISTORICAL_CANDIDATES:
        return eval_historical_candidate(candidate_id, features)
    if candidate_id.startswith("entry_mode_registry:"):
        mode_id = candidate_id.split(":", 1)[1]
        return eval_registry_mode(mode_id, candidate.get("mode_meta") or {}, features)
    return False, "unknown_candidate"


def payload_for(features, candidate, matched, reason):
    keys = [
        "signal_id",
        "token_ca",
        "symbol",
        "signal_ts",
        "signal_type",
        "hard_gate_status",
        "signal_source",
        "is_ath",
        "is_new_trending",
        "is_not_ath_new_trending",
        "market_cap",
        "holders",
        "volume_24h",
        "top10_pct",
        "ai_confidence",
        "narrative_score",
        "super_index",
        "signal_price",
        "signal_price_seen",
        "signal_price_positive",
        "source_resonance_seen",
        "source_resonance_state",
        "source_resonance_cohort",
        "source_resonance_level",
        "source_resonance_score",
        "source_component",
        "gmgn_pre_seen",
        "gmgn_lead_time_sec",
        "source_quote_clean",
        "source_quote_clean_seen",
        "source_quote_executable",
        "source_quote_executable_proxy",
        "source_entry_quote_fail_seen",
        "matrix_bucket",
        "markov_available",
        "markov_bucket",
        "markov_missing_reason",
        "markov_source",
        "markov_entry_mode",
        "markov_gate_reason",
        "markov_pass",
        "markov_sample_n",
        "markov_p_absorb_peak30",
        "markov_p_absorb_stop_before_peak",
        "markov_edge_peak30_minus_stop",
        "markov_model_family",
        "markov_cohort_key",
        "markov_event_count",
        "lifecycle_state",
        "entry_bias",
        "lifecycle_profile",
        "kline_projection_available",
        "kline_bar_count",
        "entry_bar_open_ts",
        "entry_bar_close_ts",
        "signal_to_bar_alignment_sec",
        "fbr_time_legal",
        "fbr_lookahead_warning",
        "first_bar_return_pct",
        "first5_return_pct",
        "first5_time_legal",
        "first5_close_ts",
        "fbr_ge_0",
        "fbr_ge_1",
        "fbr_ge_2",
        "fbr_ge_3",
        "fbr_ge_5",
        "entry_bar_green",
        "entry_bar_red",
        "first3_momentum_pct",
        "active_mom20_first3",
        "active_mom30_first3",
        "bar4_low_volume",
        "support_preserved_bar4",
        "red_support_lowvol_active20",
        "lowvol_active20_support",
        "old_red_pullback_score20",
        "old_red_pullback_score30",
        "candle_pattern",
        "volume_profile",
    ]
    mode_meta = candidate.get("mode_meta") or {}
    payload = {key: features.get(key) for key in keys if key in features}
    payload.update(
        {
            "candidate_version": CANDIDATE_VERSION,
            "context_schema_version": CONTEXT_SCHEMA_VERSION,
            "quote_clean_definition": "source_or_executable_quote_only_no_signal_price",
            "legacy_signal_price_positive_deprecated": features.get("signal_price_positive"),
            "candidate_count": EXPECTED_CANDIDATE_COUNT,
            "candidate_id": candidate["candidate_id"],
            "candidate_family": candidate["family"],
            "matched": bool(matched),
            "reason": reason,
            "shadow_only": True,
            "execution_scope": "shadow_observation_only",
            "creates_pending_entry": False,
            "creates_paper_trade": False,
            "mode_tier": mode_meta.get("tier"),
            "mode_route": mode_meta.get("route"),
            "mode_family": mode_meta.get("family"),
            "mode_paper_enabled": mode_meta.get("paper_enabled"),
        }
    )
    return payload


def write_observations(out_conn, signal_features, catalog, observed_at):
    rows = []
    evaluations = []
    matched_count = 0
    for candidate in catalog:
        matched, reason = eval_candidate(candidate, signal_features)
        evaluations.append((candidate, bool(matched), reason))
        if matched:
            matched_count += 1
        payload = payload_for(signal_features, candidate, matched, reason)
        rows.append(
            (
                signal_features["signal_id"],
                signal_features["token_ca"],
                signal_features.get("signal_ts"),
                candidate["candidate_id"],
                candidate["family"],
                1 if matched else 0,
                reason,
                observed_at,
                json.dumps(payload, sort_keys=True, separators=(",", ":")),
            )
        )
    out_conn.executemany(
        """
        INSERT INTO candidate_shadow_observations
          (signal_id, token_ca, signal_ts, candidate_id, family, matched, reason, observed_at, payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(signal_id, candidate_id) DO UPDATE SET
          token_ca=excluded.token_ca,
          signal_ts=excluded.signal_ts,
          family=excluded.family,
          matched=excluded.matched,
          reason=excluded.reason,
          observed_at=excluded.observed_at,
          payload_json=excluded.payload_json
        """,
        rows,
    )
    return len(rows), matched_count, evaluations


def is_virtual_entry_candidate(candidate):
    candidate_id = candidate["candidate_id"]
    if candidate_id.startswith("runtime:"):
        return False
    if candidate_id.startswith("historical:"):
        return False
    if candidate_id in {
        "markov_yellow_or_green",
        "current_would_enter_all",
        "current_would_enter_no_enqueue",
        "notath_executable_quote_clean",
    }:
        return False
    if candidate_id in {
        "lifecycle:stage2a_stop_loss_recovery",
        "lifecycle:stage3_signal_awakening",
    }:
        return False
    return True


def virtual_entry_bar_index(candidate_id):
    if candidate_id == "old_filter:mc_si_ai_velocity_utc_pre3m":
        return 4
    if candidate_id.startswith("kline:first_bar") or candidate_id.startswith("kline:fbr_"):
        return 0
    if candidate_id.startswith("kline:"):
        return 3
    return 0


def make_waiting_virtual_trade(signal_features, candidate, observed_at, status, reason, friction_bps):
    payload = {
        "candidate_version": CANDIDATE_VERSION,
        "candidate_id": candidate["candidate_id"],
        "candidate_family": candidate["family"],
        "status": status,
        "reason": reason,
        "shadow_only": True,
        "execution_scope": "shadow_virtual_trade_only",
        "creates_pending_entry": False,
        "creates_paper_trade": False,
        "friction_bps": friction_bps,
    }
    return {
        "signal_id": signal_features["signal_id"],
        "token_ca": signal_features["token_ca"],
        "signal_ts": signal_features.get("signal_ts"),
        "candidate_id": candidate["candidate_id"],
        "family": candidate["family"],
        "status": status,
        "entry_ts": None,
        "entry_price": None,
        "entry_reason": reason,
        "exit_ts": None,
        "exit_price": None,
        "exit_reason": None,
        "peak_pct": None,
        "gross_pnl_pct": None,
        "friction_bps": friction_bps,
        "net_pnl_pct": None,
        "observed_at": observed_at,
        "payload_json": json.dumps(payload, sort_keys=True, separators=(",", ":")),
    }


def simulate_virtual_trade(signal_features, candidate, bars, observed_at, args):
    candidate_id = candidate["candidate_id"]
    friction_bps = float(args.friction_bps)
    if not is_virtual_entry_candidate(candidate):
        return make_waiting_virtual_trade(
            signal_features,
            candidate,
            observed_at,
            "SKIPPED_NON_ENTRY_CANDIDATE",
            "candidate_is_observation_or_readmodel_not_entry",
            friction_bps,
        )

    entry_idx = virtual_entry_bar_index(candidate_id)
    if len(bars) <= entry_idx:
        return make_waiting_virtual_trade(
            signal_features,
            candidate,
            observed_at,
            "WAITING_ENTRY_DATA",
            f"needs_entry_bar_index_{entry_idx}",
            friction_bps,
        )

    entry_bar = bars[entry_idx]
    entry_ts = int(entry_bar["timestamp"]) + 60
    if observed_at < entry_ts:
        return make_waiting_virtual_trade(
            signal_features,
            candidate,
            observed_at,
            "WAITING_ENTRY_DATA",
            "entry_bar_not_time_legal_yet",
            friction_bps,
        )

    entry_price = float(entry_bar["close"])
    if not math.isfinite(entry_price) or entry_price <= 0:
        return make_waiting_virtual_trade(
            signal_features,
            candidate,
            observed_at,
            "WAITING_ENTRY_DATA",
            "invalid_entry_price",
            friction_bps,
        )

    stop_loss_pct = float(args.virtual_stop_loss_pct)
    trail_start_pct = float(args.virtual_trail_start_pct)
    trail_factor = float(args.virtual_trail_factor)
    timeout_bars = int(args.virtual_timeout_bars)
    peak_price = entry_price
    peak_pct = 0.0
    exit_ts = None
    exit_price = None
    exit_reason = None
    status = "VIRTUAL_OPEN"

    monitor_bars = bars[entry_idx + 1 : entry_idx + 1 + timeout_bars]
    for bar in monitor_bars:
        high_price = float(bar["high"])
        low_price = float(bar["low"])
        close_price = float(bar["close"])
        bar_close_ts = int(bar["timestamp"]) + 60
        if observed_at < bar_close_ts:
            break

        if high_price > peak_price:
            peak_price = high_price
            peak_pct = pct_change(entry_price, peak_price) or peak_pct

        low_pnl = pct_change(entry_price, low_price)
        if low_pnl is not None and low_pnl <= stop_loss_pct:
            exit_ts = bar_close_ts
            exit_price = entry_price * (1.0 + stop_loss_pct / 100.0)
            exit_reason = "VIRTUAL_STOP_LOSS"
            status = "VIRTUAL_CLOSED"
            break

        if peak_pct >= trail_start_pct:
            trail_stop_price = peak_price * trail_factor
            if low_price <= trail_stop_price:
                exit_ts = bar_close_ts
                exit_price = trail_stop_price
                exit_reason = "VIRTUAL_TRAIL_STOP"
                status = "VIRTUAL_CLOSED"
                break

        if len(monitor_bars) >= timeout_bars and bar is monitor_bars[-1] and observed_at >= bar_close_ts:
            exit_ts = bar_close_ts
            exit_price = close_price
            exit_reason = "VIRTUAL_TIMEOUT"
            status = "VIRTUAL_CLOSED"
            break

    gross_pnl = pct_change(entry_price, exit_price) if exit_price is not None else None
    net_pnl = gross_pnl - friction_bps / 100.0 if gross_pnl is not None else None
    if status == "VIRTUAL_OPEN" and len(monitor_bars) < timeout_bars:
        exit_reason = "WAITING_EXIT_DATA"

    payload = {
        "candidate_version": CANDIDATE_VERSION,
        "candidate_id": candidate_id,
        "candidate_family": candidate["family"],
        "status": status,
        "entry_ts": entry_ts,
        "entry_price": entry_price,
        "entry_bar_index": entry_idx,
        "entry_reason": f"kline_close_index_{entry_idx}",
        "exit_ts": exit_ts,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "peak_pct": peak_pct,
        "gross_pnl_pct": gross_pnl,
        "friction_bps": friction_bps,
        "net_pnl_pct": net_pnl,
        "virtual_stop_loss_pct": stop_loss_pct,
        "virtual_trail_start_pct": trail_start_pct,
        "virtual_trail_factor": trail_factor,
        "virtual_timeout_bars": timeout_bars,
        "shadow_only": True,
        "execution_scope": "shadow_virtual_trade_only",
        "creates_pending_entry": False,
        "creates_paper_trade": False,
    }
    return {
        "signal_id": signal_features["signal_id"],
        "token_ca": signal_features["token_ca"],
        "signal_ts": signal_features.get("signal_ts"),
        "candidate_id": candidate_id,
        "family": candidate["family"],
        "status": status,
        "entry_ts": entry_ts,
        "entry_price": entry_price,
        "entry_reason": f"kline_close_index_{entry_idx}",
        "exit_ts": exit_ts,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "peak_pct": peak_pct,
        "gross_pnl_pct": gross_pnl,
        "friction_bps": friction_bps,
        "net_pnl_pct": net_pnl,
        "observed_at": observed_at,
        "payload_json": json.dumps(payload, sort_keys=True, separators=(",", ":")),
    }


def write_virtual_trades(out_conn, signal_features, catalog_evaluations, bars, observed_at, args):
    rows = []
    for candidate, matched, _reason in catalog_evaluations:
        if not matched:
            continue
        trade = simulate_virtual_trade(signal_features, candidate, bars, observed_at, args)
        rows.append(
            (
                trade["signal_id"],
                trade["token_ca"],
                trade["signal_ts"],
                trade["candidate_id"],
                trade["family"],
                trade["status"],
                trade["entry_ts"],
                trade["entry_price"],
                trade["entry_reason"],
                trade["exit_ts"],
                trade["exit_price"],
                trade["exit_reason"],
                trade["peak_pct"],
                trade["gross_pnl_pct"],
                trade["friction_bps"],
                trade["net_pnl_pct"],
                trade["observed_at"],
                trade["payload_json"],
            )
        )
    if not rows:
        return {"virtual_rows": 0, "virtual_orders": 0, "virtual_closed": 0, "virtual_open": 0, "virtual_waiting": 0}
    out_conn.executemany(
        """
        INSERT INTO candidate_shadow_virtual_trades
          (signal_id, token_ca, signal_ts, candidate_id, family, status, entry_ts, entry_price, entry_reason,
           exit_ts, exit_price, exit_reason, peak_pct, gross_pnl_pct, friction_bps, net_pnl_pct, observed_at, payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(signal_id, candidate_id) DO UPDATE SET
          token_ca=excluded.token_ca,
          signal_ts=excluded.signal_ts,
          family=excluded.family,
          status=excluded.status,
          entry_ts=excluded.entry_ts,
          entry_price=excluded.entry_price,
          entry_reason=excluded.entry_reason,
          exit_ts=excluded.exit_ts,
          exit_price=excluded.exit_price,
          exit_reason=excluded.exit_reason,
          peak_pct=excluded.peak_pct,
          gross_pnl_pct=excluded.gross_pnl_pct,
          friction_bps=excluded.friction_bps,
          net_pnl_pct=excluded.net_pnl_pct,
          observed_at=excluded.observed_at,
          payload_json=excluded.payload_json
        """,
        rows,
    )
    closed = sum(1 for row in rows if row[5] == "VIRTUAL_CLOSED")
    open_n = sum(1 for row in rows if row[5] == "VIRTUAL_OPEN")
    waiting = sum(1 for row in rows if row[5].startswith("WAITING") or row[5].startswith("SKIPPED"))
    ordered = sum(1 for row in rows if row[7] is not None)
    return {
        "virtual_rows": len(rows),
        "virtual_orders": ordered,
        "virtual_closed": closed,
        "virtual_open": open_n,
        "virtual_waiting": waiting,
    }


def run_once(args):
    registry, modes = load_registry(args.registry)
    catalog = build_candidate_catalog(modes)
    signal_conn = open_sqlite(args.signal_db, "signal_db")
    out_conn = open_sqlite(args.out_db, "out_db")
    try:
        ensure_schema(out_conn)
    except sqlite3.Error as exc:
        raise RuntimeError(f"out_db_schema_error path={args.out_db}: {exc}") from exc
    kline_conn = None
    if args.kline_db:
        try:
            kline_conn = open_sqlite(args.kline_db, "kline_db")
            ensure_kline_schema(kline_conn)
        except sqlite3.Error as exc:
            warn_json(
                warning="kline_db_disabled",
                path=args.kline_db,
                reason=str(exc),
                script="candidate_shadow_observer",
            )
            try:
                if kline_conn:
                    kline_conn.close()
            except Exception:
                pass
            kline_conn = None

    try:
        signals = load_signals(signal_conn, args.limit, args.since_id)
    except sqlite3.Error as exc:
        raise RuntimeError(f"signal_db_load_error path={args.signal_db}: {exc}") from exc
    observed_at = int(time.time())
    total_rows = 0
    total_matched = 0
    virtual_rows = 0
    virtual_orders = 0
    virtual_closed = 0
    virtual_open = 0
    virtual_waiting = 0
    kline_fallback_fetches = 0
    kline_fallback_bars = 0
    by_candidate = {}
    for row in signals:
        signal_ts = normalize_ts_sec(row["timestamp"]) or normalize_ts_sec(row["receive_ts"])
        try:
            bars = load_kline_bars(kline_conn, row["token_ca"], signal_ts, args.kline_limit) if kline_conn else []
        except sqlite3.Error as exc:
            warn_json(
                warning="kline_db_disabled",
                path=args.kline_db,
                token_ca=row["token_ca"],
                reason=str(exc),
                script="candidate_shadow_observer",
            )
            if kline_conn:
                try:
                    kline_conn.close()
                except Exception:
                    pass
            kline_conn = None
            bars = []
        if (
            args.kline_fallback_enabled
            and kline_conn
            and not bars
            and kline_fallback_fetches < args.kline_fallback_max_fetches
            and should_fetch_kline(out_conn, row["token_ca"], observed_at, args.kline_fallback_cooldown_sec)
        ):
            try:
                fetched_count, fetch_reason = fetch_kline_fallback(kline_conn, row["token_ca"])
            except sqlite3.Error as exc:
                warn_json(
                    warning="kline_fallback_disabled",
                    path=args.kline_db,
                    token_ca=row["token_ca"],
                    reason=str(exc),
                    script="candidate_shadow_observer",
                )
                try:
                    kline_conn.close()
                except Exception:
                    pass
                kline_conn = None
                fetched_count, fetch_reason = 0, "kline_db_error"
            kline_fallback_fetches += 1
            kline_fallback_bars += fetched_count
            record_kline_fetch_attempt(
                out_conn,
                row["token_ca"],
                observed_at,
                "ok" if fetched_count else "failed",
                fetched_count,
                fetch_reason,
            )
            if fetched_count:
                bars = load_kline_bars(kline_conn, row["token_ca"], signal_ts, args.kline_limit)
        kline_features = compute_kline_features(bars, signal_ts, observed_at)
        source_features = load_source_resonance_features(out_conn, row["token_ca"], signal_ts)
        source_features.update(load_global_runtime_features(out_conn, row["token_ca"], signal_ts))
        signal_features = extract_signal_features(row, kline_features, source_features)
        signal_features.update(build_shadow_markov_features(out_conn, signal_features, observed_at))
        try:
            written, matched, evaluations = write_observations(out_conn, signal_features, catalog, observed_at)
            virtual_summary = write_virtual_trades(out_conn, signal_features, evaluations, bars, observed_at, args)
        except sqlite3.Error as exc:
            raise RuntimeError(
                f"out_db_write_error path={args.out_db} signal_id={signal_features.get('signal_id')}: {exc}"
            ) from exc
        total_rows += written
        total_matched += matched
        virtual_rows += virtual_summary["virtual_rows"]
        virtual_orders += virtual_summary["virtual_orders"]
        virtual_closed += virtual_summary["virtual_closed"]
        virtual_open += virtual_summary["virtual_open"]
        virtual_waiting += virtual_summary["virtual_waiting"]
    out_conn.commit()

    for row in out_conn.execute(
        """
        SELECT candidate_id, SUM(matched) AS matched_n, COUNT(*) AS total_n
        FROM candidate_shadow_observations
        WHERE observed_at = ?
        GROUP BY candidate_id
        ORDER BY matched_n DESC, total_n DESC, candidate_id ASC
        LIMIT 12
        """,
        (observed_at,),
    ):
        by_candidate[row["candidate_id"]] = {"matched": row["matched_n"], "total": row["total_n"]}

    summary = {
        "candidate_version": CANDIDATE_VERSION,
        "candidate_count": len(catalog),
        "signals_processed": len(signals),
        "rows_written": total_rows,
        "matched_rows": total_matched,
        "virtual_rows": virtual_rows,
        "virtual_orders": virtual_orders,
        "virtual_closed": virtual_closed,
        "virtual_open": virtual_open,
        "virtual_waiting": virtual_waiting,
        "kline_fallback_fetches": kline_fallback_fetches,
        "kline_fallback_bars": kline_fallback_bars,
        "kline_db": args.kline_db,
        "kline_fallback_enabled": bool(args.kline_fallback_enabled),
        "observed_at": observed_at,
        "top_matched_candidates": by_candidate,
        "paper_only": True,
    }
    print(json.dumps(summary, sort_keys=True))
    return summary


def self_test():
    proxy_match, proxy_reason = eval_base_candidate(
        "notath_quote_clean",
        {
            "is_not_ath_new_trending": True,
            "signal_price_seen": True,
            "signal_price_positive": True,
            "source_quote_clean": False,
            "source_quote_clean_seen": False,
        },
    )
    assert proxy_match is False
    assert proxy_reason == "missing_runtime_source_quote_clean"
    clean_match, clean_reason = eval_base_candidate(
        "notath_quote_clean",
        {
            "is_not_ath_new_trending": True,
            "signal_price_seen": False,
            "signal_price_positive": False,
            "source_quote_clean": True,
        },
    )
    assert clean_match is True
    assert clean_reason == "runtime_source_quote_clean"
    registry = {
        "modes": {f"mode_{idx:02d}": {"tier": "shadow_watch_only", "route": "MIXED", "family": "primary"} for idx in range(35)}
    }
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        registry_path = tmp_path / "registry.json"
        signal_db = tmp_path / "signals.db"
        kline_db = tmp_path / "kline.db"
        out_db = tmp_path / "paper.db"
        registry_path.write_text(json.dumps(registry), encoding="utf-8")
        sig = sqlite3.connect(signal_db)
        sig.execute(
            """
            CREATE TABLE premium_signals (
              id INTEGER PRIMARY KEY,
              token_ca TEXT,
              symbol TEXT,
              timestamp INTEGER,
              receive_ts INTEGER,
              signal_type TEXT,
              is_ath INTEGER,
              market_cap REAL,
              holders INTEGER,
              volume_24h REAL,
              top10_pct REAL,
              ai_confidence INTEGER,
              ai_narrative_tier TEXT,
              narrative_score REAL,
              description TEXT,
              raw_message TEXT,
              hard_gate_status TEXT,
              signal_source TEXT
            )
            """
        )
        signal_ts = 1_772_000_000
        sig.execute(
            """
            INSERT INTO premium_signals
            VALUES (1, 'TESTCA', 'TEST', ?, ?, 'NEW_TRENDING', 0, 12000, 150, 50000, 24, 70,
                    'A', 1.0, 'Super Index: 120 MC: $12K Holders: 150 Top 10: 24%', '', 'PASS',
                    'premium_channel_ath')
            """,
            (signal_ts * 1000, signal_ts * 1000),
        )
        sig.commit()
        sig.close()

        kl = sqlite3.connect(kline_db)
        kl.execute(
            """
            CREATE TABLE kline_1m (
              token_ca TEXT,
              pool_address TEXT DEFAULT '',
              timestamp INTEGER,
              open REAL,
              high REAL,
              low REAL,
              close REAL,
              volume REAL
            )
            """
        )
        bars = [
            (signal_ts, 100, 120, 90, 115, 100),
            (signal_ts + 60, 115, 145, 110, 140, 120),
            (signal_ts + 120, 140, 160, 130, 150, 90),
            (signal_ts + 180, 150, 155, 125, 145, 60),
            (signal_ts + 240, 145, 170, 140, 168, 200),
        ]
        kl.executemany(
            "INSERT INTO kline_1m(token_ca,timestamp,open,high,low,close,volume) VALUES ('TESTCA',?,?,?,?,?,?)",
            bars,
        )
        kl.commit()
        kl.close()

        paper = sqlite3.connect(out_db)
        paper.execute(
            """
            CREATE TABLE paper_decision_events (
              event_ts REAL,
              token_ca TEXT,
              lifecycle_state TEXT,
              entry_bias TEXT,
              payload_json TEXT
            )
            """
        )
        paper.execute(
            """
            CREATE TABLE a_class_decision_events (
              event_ts REAL,
              token_ca TEXT,
              source_component TEXT,
              source_reason TEXT,
              quote_clean INTEGER,
              quote_executable INTEGER,
              matrix_json TEXT,
              risk_json TEXT
            )
            """
        )
        paper.execute(
            "INSERT INTO paper_decision_events VALUES (?, 'TESTCA', 'NEWBORN_LAUNCH', 'OBSERVE', ?)",
            (
                signal_ts,
                json.dumps({"markov_reclaim_forecast": {"gate": {"markov_bucket": "green"}}}),
            ),
        )
        paper.execute(
            "INSERT INTO a_class_decision_events VALUES (?, 'TESTCA', 'pre_pass_resonance_probe', 'gmgn_pre_seen_required', 1, 1, ?, ?)",
            (
                signal_ts,
                json.dumps({"green_count": 3, "red_count": 0}),
                json.dumps({"quote_clean_verified": True}),
            ),
        )
        paper.commit()
        paper.close()

        args = argparse.Namespace(
            registry=str(registry_path),
            signal_db=str(signal_db),
            kline_db=str(kline_db),
            out_db=str(out_db),
            limit=5,
            kline_limit=125,
            friction_bps=350.0,
            virtual_stop_loss_pct=DEFAULT_VIRTUAL_STOP_LOSS_PCT,
            virtual_trail_start_pct=DEFAULT_VIRTUAL_TRAIL_START_PCT,
            virtual_trail_factor=DEFAULT_VIRTUAL_TRAIL_FACTOR,
            virtual_timeout_bars=DEFAULT_VIRTUAL_TIMEOUT_BARS,
            kline_fallback_enabled=False,
            kline_fallback_max_fetches=0,
            kline_fallback_cooldown_sec=900,
            since_id=None,
        )
        summary = run_once(args)
        out = sqlite3.connect(out_db)
        total = out.execute("SELECT COUNT(*) FROM candidate_shadow_observations").fetchone()[0]
        virtual_total = out.execute("SELECT COUNT(*) FROM candidate_shadow_virtual_trades").fetchone()[0]
        closed_total = out.execute(
            "SELECT COUNT(*) FROM candidate_shadow_virtual_trades WHERE status='VIRTUAL_CLOSED'"
        ).fetchone()[0]
        old_filter = out.execute(
            "SELECT matched FROM candidate_shadow_observations WHERE candidate_id='old_filter:mc_si_ai_velocity_utc_pre3m'"
        ).fetchone()[0]
        active = out.execute(
            "SELECT matched FROM candidate_shadow_observations WHERE candidate_id='kline:active_mom30_first3'"
        ).fetchone()[0]
        markov = out.execute(
            "SELECT matched, payload_json FROM candidate_shadow_observations WHERE candidate_id='markov_yellow_or_green'"
        ).fetchone()
        payload = json.loads(markov[1])
        shadow_markov = build_shadow_markov_features(
            out,
            {
                "token_ca": "TESTCA2",
                "symbol": "TEST2",
                "source_quote_clean": True,
                "source_quote_executable": True,
                "signal_price_positive": True,
                "first5_return_pct": 5.0,
                "hard_gate_status": "PASS",
                "source_component": "pre_pass_resonance_probe",
                "source_resonance_state": "pre_pass_resonance_probe:test",
                "lifecycle_state": "NEWBORN_LAUNCH",
                "entry_bias": "PROBE",
            },
            signal_ts,
        )
        out.close()
        assert summary["candidate_count"] == EXPECTED_CANDIDATE_COUNT
        assert total == EXPECTED_CANDIDATE_COUNT
        assert virtual_total > 0
        assert closed_total > 0
        assert old_filter == 1
        assert active == 1
        assert markov[0] == 1
        assert payload["markov_bucket"] == "green"
        assert payload["matrix_bucket"] == "green"
        assert payload["source_quote_clean"] is True
        assert payload["source_quote_executable"] is True
        assert payload["lifecycle_state"] == "NEWBORN_LAUNCH"
        assert shadow_markov["markov_available"] is True
        assert shadow_markov["markov_source"] == "shadow_lotto_reclaim_forecast"
        assert shadow_markov["markov_bucket"] in {"insufficient", "green", "yellow", "red"}
    print("SELF_TEST_PASS candidate_shadow_observer")


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    root = Path(__file__).resolve().parents[1]
    parser.add_argument("--signal-db", default=os.getenv("SENTIMENT_DB", str(root / "data" / "sentiment_arb.db")))
    parser.add_argument("--out-db", default=os.getenv("PAPER_DB", str(root / "data" / "paper_trades.db")))
    parser.add_argument("--kline-db", default=os.getenv("KLINE_DB", str(root / "data" / "kline_cache.db")))
    parser.add_argument("--registry", default=str(root / "config" / "entry-mode-registry.json"))
    parser.add_argument("--limit", type=int, default=int(os.getenv("CANDIDATE_SHADOW_OBSERVER_LIMIT", "300")))
    parser.add_argument("--kline-limit", type=int, default=int(os.getenv("CANDIDATE_SHADOW_KLINE_LIMIT", "125")))
    parser.add_argument("--friction-bps", type=float, default=float(os.getenv("CANDIDATE_SHADOW_FRICTION_BPS", "350")))
    parser.add_argument("--virtual-stop-loss-pct", type=float, default=float(os.getenv("CANDIDATE_SHADOW_VIRTUAL_STOP_LOSS_PCT", str(DEFAULT_VIRTUAL_STOP_LOSS_PCT))))
    parser.add_argument("--virtual-trail-start-pct", type=float, default=float(os.getenv("CANDIDATE_SHADOW_VIRTUAL_TRAIL_START_PCT", str(DEFAULT_VIRTUAL_TRAIL_START_PCT))))
    parser.add_argument("--virtual-trail-factor", type=float, default=float(os.getenv("CANDIDATE_SHADOW_VIRTUAL_TRAIL_FACTOR", str(DEFAULT_VIRTUAL_TRAIL_FACTOR))))
    parser.add_argument("--virtual-timeout-bars", type=int, default=int(os.getenv("CANDIDATE_SHADOW_VIRTUAL_TIMEOUT_BARS", str(DEFAULT_VIRTUAL_TIMEOUT_BARS))))
    parser.add_argument("--kline-fallback-enabled", action=argparse.BooleanOptionalAction, default=os.getenv("CANDIDATE_SHADOW_KLINE_FALLBACK_ENABLED", "true").lower() != "false")
    parser.add_argument("--kline-fallback-max-fetches", type=int, default=int(os.getenv("CANDIDATE_SHADOW_KLINE_FALLBACK_MAX_FETCHES", "20")))
    parser.add_argument("--kline-fallback-cooldown-sec", type=int, default=int(os.getenv("CANDIDATE_SHADOW_KLINE_FALLBACK_COOLDOWN_SEC", "900")))
    parser.add_argument("--since-id", type=int, default=None)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=int, default=int(os.getenv("CANDIDATE_SHADOW_OBSERVER_INTERVAL_SEC", "60")))
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv or sys.argv[1:])
    if args.self_test:
        self_test()
        return 0
    if args.loop:
        while True:
            try:
                run_once(args)
            except Exception as exc:  # pragma: no cover - startup log safety
                print(json.dumps({"error": str(exc), "script": "candidate_shadow_observer"}), file=sys.stderr)
            time.sleep(max(5, int(args.interval)))
    else:
        run_once(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Materialize Phase 3 24h path observations from existing raw bars.

Shadow-only data materializer. It writes to an independent Phase 3 SQLite DB and
summary artifact. It does not fetch new market data and does not change trading
logic.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import tempfile
import time
from pathlib import Path


SCHEMA_VERSION = "phase3_24h_path_observer.v1"
DEFAULT_DATA_DIR = Path("/app/data")
DEFAULT_RAW_DB = DEFAULT_DATA_DIR / "raw_signal_outcomes.db"
DEFAULT_PHASE3_DB = DEFAULT_DATA_DIR / "phase3_path_observer.db"
DEFAULT_RUN_DIR = DEFAULT_DATA_DIR / "agent_runs/latest"


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def safe_float(value, default=None):
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    except Exception:
        return default


def safe_int(value, default=None):
    parsed = safe_float(value)
    return default if parsed is None else int(parsed)


def norm_ts(value):
    ts = safe_int(value)
    if ts is None:
        return None
    if ts > 10_000_000_000:
        return int(ts / 1000)
    return ts


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def connect_raw(path):
    if not Path(path).exists():
        return None
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    db.row_factory = sqlite3.Row
    return db


def connect_phase3(path):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(target, timeout=10)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS phase3_path_observations (
          signal_id TEXT,
          token_ca TEXT NOT NULL,
          signal_ts INTEGER NOT NULL,
          observed_until_ts INTEGER,
          horizon_sec INTEGER NOT NULL,
          bar_count INTEGER NOT NULL,
          first_bar_ts INTEGER,
          last_bar_ts INTEGER,
          max_high_pct REAL,
          min_low_pct REAL,
          latest_close_pct REAL,
          payload_json TEXT,
          updated_at INTEGER NOT NULL,
          PRIMARY KEY(signal_id, token_ca, signal_ts)
        )
        """
    )
    db.commit()
    return db


def table_exists(db, table):
    return bool(db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone())


def load_signals(raw_db, since_ts, limit):
    if raw_db is None or not table_exists(raw_db, "raw_signal_outcomes"):
        return []
    rows = raw_db.execute(
        """
        SELECT signal_id, token_ca, symbol, signal_ts, baseline_price, baseline_confidence,
               raw_sustained_tier, raw_primary_tier
        FROM raw_signal_outcomes
        WHERE token_ca IS NOT NULL
        ORDER BY rowid DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        ts = norm_ts(item.get("signal_ts"))
        if ts and ts >= int(since_ts):
            out.append(item)
    return out


def load_recent_bars_by_token(raw_db, max_total_bars):
    if raw_db is None or not table_exists(raw_db, "raw_price_bars_1m"):
        return {}
    rows = raw_db.execute(
        """
        SELECT token_ca, timestamp, open, high, low, close, volume, provider, source_kind, source_family
        FROM raw_price_bars_1m
        ORDER BY rowid DESC
        LIMIT ?
        """,
        (int(max_total_bars),),
    ).fetchall()
    grouped = {}
    for row in rows:
        item = dict(row)
        token = str(item.get("token_ca") or "").lower()
        if not token:
            continue
        grouped.setdefault(token, []).append(item)
    for token, bars in grouped.items():
        bars.sort(key=lambda item: norm_ts(item.get("timestamp")) or 0)
    return grouped


def select_bars_for_signal(bars_by_token, token_ca, start_ts, end_ts, max_bars):
    bars = bars_by_token.get(str(token_ca or "").lower()) or []
    out = []
    for row in bars:
        ts = norm_ts(row.get("timestamp"))
        if ts is None:
            continue
        if int(start_ts) <= ts <= int(end_ts):
            out.append(row)
            if len(out) >= int(max_bars):
                break
    return out


def summarize_path(signal, bars, horizon_sec):
    signal_ts = norm_ts(signal.get("signal_ts"))
    baseline = safe_float(signal.get("baseline_price"))
    if baseline is None or baseline <= 0:
        closes = [safe_float(row.get("close")) for row in bars]
        baseline = next((value for value in closes if value and value > 0), None)
    first_ts = norm_ts(bars[0].get("timestamp")) if bars else None
    last_ts = norm_ts(bars[-1].get("timestamp")) if bars else None
    highs = [safe_float(row.get("high")) for row in bars]
    lows = [safe_float(row.get("low")) for row in bars]
    closes = [safe_float(row.get("close")) for row in bars]
    max_high_pct = min_low_pct = latest_close_pct = None
    if baseline and baseline > 0:
        valid_highs = [value for value in highs if value is not None]
        valid_lows = [value for value in lows if value is not None]
        valid_closes = [value for value in closes if value is not None]
        if valid_highs:
            max_high_pct = (max(valid_highs) / baseline - 1.0) * 100.0
        if valid_lows:
            min_low_pct = (min(valid_lows) / baseline - 1.0) * 100.0
        if valid_closes:
            latest_close_pct = (valid_closes[-1] / baseline - 1.0) * 100.0
    return {
        "signal_id": str(signal.get("signal_id") or ""),
        "token_ca": str(signal.get("token_ca") or ""),
        "signal_ts": signal_ts,
        "observed_until_ts": last_ts,
        "horizon_sec": int(horizon_sec),
        "bar_count": len(bars),
        "first_bar_ts": first_ts,
        "last_bar_ts": last_ts,
        "max_high_pct": max_high_pct,
        "min_low_pct": min_low_pct,
        "latest_close_pct": latest_close_pct,
        "payload": {
            "symbol": signal.get("symbol"),
            "baseline_price": baseline,
            "baseline_confidence": signal.get("baseline_confidence"),
            "raw_sustained_tier": signal.get("raw_sustained_tier"),
            "raw_primary_tier": signal.get("raw_primary_tier"),
        },
    }


def upsert_observation(db, row):
    db.execute(
        """
        INSERT INTO phase3_path_observations (
          signal_id, token_ca, signal_ts, observed_until_ts, horizon_sec, bar_count,
          first_bar_ts, last_bar_ts, max_high_pct, min_low_pct, latest_close_pct,
          payload_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(signal_id, token_ca, signal_ts) DO UPDATE SET
          observed_until_ts=excluded.observed_until_ts,
          horizon_sec=excluded.horizon_sec,
          bar_count=excluded.bar_count,
          first_bar_ts=excluded.first_bar_ts,
          last_bar_ts=excluded.last_bar_ts,
          max_high_pct=excluded.max_high_pct,
          min_low_pct=excluded.min_low_pct,
          latest_close_pct=excluded.latest_close_pct,
          payload_json=excluded.payload_json,
          updated_at=excluded.updated_at
        """,
        (
            row["signal_id"],
            row["token_ca"],
            int(row["signal_ts"] or 0),
            row["observed_until_ts"],
            row["horizon_sec"],
            row["bar_count"],
            row["first_bar_ts"],
            row["last_bar_ts"],
            row["max_high_pct"],
            row["min_low_pct"],
            row["latest_close_pct"],
            json.dumps(row["payload"], sort_keys=True),
            int(time.time()),
        ),
    )


def run(args):
    now_ts = int(time.time())
    horizon_sec = int(float(args.horizon_hours) * 3600)
    since_ts = now_ts - int(float(args.lookback_hours) * 3600)
    raw_db = connect_raw(args.raw_db)
    phase3_db = connect_phase3(args.phase3_db)
    signals = load_signals(raw_db, since_ts, int(args.max_signals)) if raw_db else []
    bars_by_token = load_recent_bars_by_token(raw_db, int(args.max_total_bars)) if raw_db else {}
    processed = 0
    with_bars = 0
    reached_target = 0
    for signal in signals:
        signal_ts = norm_ts(signal.get("signal_ts"))
        token = signal.get("token_ca")
        if not signal_ts or not token:
            continue
        bars = select_bars_for_signal(
            bars_by_token,
            token,
            signal_ts,
            signal_ts + horizon_sec,
            int(args.max_bars_per_signal),
        )
        row = summarize_path(signal, bars, horizon_sec)
        upsert_observation(phase3_db, row)
        processed += 1
        if bars:
            with_bars += 1
        if row.get("last_bar_ts") and signal_ts and row["last_bar_ts"] - signal_ts >= horizon_sec:
            reached_target += 1
    phase3_db.commit()
    total_rows = phase3_db.execute("SELECT COUNT(*) AS n FROM phase3_path_observations").fetchone()["n"]
    latest = phase3_db.execute(
        "SELECT MAX(updated_at) AS updated_at, MAX(last_bar_ts - signal_ts) AS max_span FROM phase3_path_observations"
    ).fetchone()
    if raw_db:
        raw_db.close()
    phase3_db.close()
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "classification": "PHASE3_24H_PATH_OBSERVER_MATERIALIZED",
        "allowed_use": "shadow_only",
        "promotion_allowed": False,
        "runtime_trade_dependency_allowed": False,
        "raw_db_available": raw_db is not None,
        "phase3_db": str(args.phase3_db),
        "lookback_hours": float(args.lookback_hours),
        "horizon_hours": float(args.horizon_hours),
        "signals_loaded": len(signals),
        "recent_bar_token_count": len(bars_by_token),
        "signals_processed": processed,
        "signals_with_bars": with_bars,
        "signals_reached_target_horizon": reached_target,
        "total_observation_rows": total_rows,
        "max_observed_path_span_sec": safe_int(latest["max_span"], 0) if latest else 0,
        "updated_at": safe_int(latest["updated_at"], None) if latest else None,
        "next_action": "continue_shadow_path_materialization",
    }


def self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        raw = root / "raw.db"
        phase3 = root / "phase3.db"
        db = sqlite3.connect(raw)
        now = int(time.time())
        db.execute(
            "CREATE TABLE raw_signal_outcomes(signal_id TEXT, token_ca TEXT, symbol TEXT, signal_ts INTEGER, baseline_price REAL, baseline_confidence TEXT, raw_sustained_tier TEXT, raw_primary_tier TEXT)"
        )
        db.execute(
            "CREATE TABLE raw_price_bars_1m(token_ca TEXT, timestamp INTEGER, open REAL, high REAL, low REAL, close REAL, volume REAL, provider TEXT, source_kind TEXT, source_family TEXT)"
        )
        db.execute(
            "INSERT INTO raw_signal_outcomes(signal_id, token_ca, symbol, signal_ts, baseline_price) VALUES (?, ?, ?, ?, ?)",
            ("s1", "tokenA", "AAA", now - 25 * 3600, 1.0),
        )
        for offset, price in ((0, 1.0), (3600, 1.2), (24 * 3600, 2.0)):
            db.execute(
                "INSERT INTO raw_price_bars_1m(token_ca, timestamp, open, high, low, close) VALUES (?, ?, ?, ?, ?, ?)",
                ("tokenA", now - 25 * 3600 + offset, price, price, price, price),
            )
        db.commit()
        db.close()
        out = root / "summary.json"
        args = argparse.Namespace(
            raw_db=str(raw),
            phase3_db=str(phase3),
            lookback_hours=48,
            horizon_hours=24,
            max_signals=100,
            max_total_bars=1000,
            max_bars_per_signal=2000,
            out=str(out),
        )
        report = run(args)
        write_json(out, report)
        assert report["signals_processed"] == 1
        assert report["signals_reached_target_horizon"] == 1
        assert report["promotion_allowed"] is False
    print("SELF_TEST_PASS phase3_24h_path_observer")


def parse_args():
    parser = argparse.ArgumentParser(description="Materialize Phase 3 24h path observations.")
    parser.add_argument("--raw-db", default=str(DEFAULT_RAW_DB))
    parser.add_argument("--phase3-db", default=str(DEFAULT_PHASE3_DB))
    parser.add_argument("--lookback-hours", default="48")
    parser.add_argument("--horizon-hours", default="24")
    parser.add_argument("--max-signals", default="250")
    parser.add_argument("--max-total-bars", default="50000")
    parser.add_argument("--max-bars-per-signal", default="2000")
    parser.add_argument("--out", default=str(DEFAULT_RUN_DIR / "phase3_24h_path_observer_summary.json"))
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.self_test:
        self_test()
        return 0
    report = run(args)
    write_json(args.out, report)
    print(json.dumps({
        "schema_version": SCHEMA_VERSION,
        "classification": report["classification"],
        "signals_processed": report["signals_processed"],
        "signals_reached_target_horizon": report["signals_reached_target_horizon"],
        "promotion_allowed": False,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

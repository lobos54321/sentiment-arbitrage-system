#!/usr/bin/env python3
"""Fetch Gate-0.5 reconciliation bars from GeckoTerminal.

This is the narrow Step-2 adapter for the Gate-0.5 paid pilot. It reads the
burned pilot sample plus the observer snapshot, selects only observer-labeled
signals whose raw-path source is reproducible from GeckoTerminal, and emits
1-minute OHLCV JSONL tagged exactly as the Gate-0.5 evaluator expects:

  provider=geckoterminal, source_kind=indexed_ohlcv,
  source_family=third_party_kline, price_unit=native.

It intentionally uses GeckoTerminal's `token=base` OHLCV parameter, matching the
raw-path observer's shared-pool client contract. It does not use `currency=usd`.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


API_BASE = "https://api.geckoterminal.com/api/v2/networks/solana"
HORIZON_SEC = 7200
DEFAULT_SLEEP_SEC = 1.2
HEADERS = {
    "Accept": "application/json;version=20230302",
    "User-Agent": "sas-gate05-reconciliation/1.0",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def sha256_file(path: Path) -> str | None:
    if not path.exists():
        return None
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def normalize_ts(value: Any) -> int | None:
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    if not (n == n):
        return None
    return int(n // 1000) if n > 1_000_000_000_000 else int(n)


def key_of(token_ca: str, signal_ts: int) -> str:
    return f"{str(token_ca).strip()}|{int(signal_ts)}"


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_pilot_signals(path: Path) -> list[dict[str, Any]]:
    rows = read_json(path)
    if not isinstance(rows, list):
        raise SystemExit(f"pilot signals must be a JSON array: {path}")
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        token = str(row.get("token_ca") or "").strip()
        ts = normalize_ts(row.get("signal_ts"))
        if not token or ts is None:
            continue
        key = key_of(token, ts)
        if key in seen:
            continue
        seen.add(key)
        out.append({**row, "token_ca": token, "signal_ts": ts})
    return out


def load_observer_rows(observer_db: Path, pilot_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    keys = [(row["token_ca"], row["signal_ts"]) for row in pilot_rows]
    if not keys:
        return {}
    con = sqlite3.connect(str(observer_db))
    con.row_factory = sqlite3.Row
    try:
        cols = {row[1] for row in con.execute("PRAGMA table_info(raw_signal_outcomes)").fetchall()}
        required = {"token_ca", "signal_ts", "path_provider", "path_source_kind", "path_price_unit"}
        missing = sorted(required - cols)
        if missing:
            raise SystemExit(f"observer DB missing columns: {', '.join(missing)}")
        out: dict[str, dict[str, Any]] = {}
        order_clause = "ORDER BY updated_at DESC" if "updated_at" in cols else ""
        for token, signal_ts in keys:
            row = con.execute(
                f"""
                SELECT
                  token_ca, signal_ts, path_provider, path_source_kind,
                  path_source_family, path_price_unit, path_pool_address,
                  raw_primary_tier, observation_status
                FROM raw_signal_outcomes
                WHERE token_ca = ? AND signal_ts = ?
                {order_clause}
                LIMIT 1
                """,
                (token, signal_ts),
            ).fetchone()
            if row is not None:
                out[key_of(token, signal_ts)] = dict(row)
        return out
    finally:
        con.close()


def classify_window(row: dict[str, Any], observer: dict[str, Any] | None) -> tuple[str, str]:
    if observer is None:
        return "skip", "missing_observer_label"
    provider = str(observer.get("path_provider") or "").lower()
    source_kind = str(observer.get("path_source_kind") or "").lower()
    price_unit = str(observer.get("path_price_unit") or "").lower()
    if provider != "geckoterminal":
        return "skip", f"observer_provider_not_geckoterminal:{provider or 'missing'}"
    if source_kind != "indexed_ohlcv":
        return "skip", f"observer_source_kind_not_indexed_ohlcv:{source_kind or 'missing'}"
    if price_unit and price_unit != "native":
        return "skip", f"observer_price_unit_not_native:{price_unit}"
    return "fetch", "geckoterminal_indexed_ohlcv_native"


def build_windows(pilot_rows: list[dict[str, Any]], observer_by_key: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    windows: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    for row in pilot_rows:
        observer = observer_by_key.get(key_of(row["token_ca"], row["signal_ts"]))
        action, reason = classify_window(row, observer)
        counts[reason] = counts.get(reason, 0) + 1
        if action != "fetch":
            continue
        windows.append({
            "token_ca": row["token_ca"],
            "signal_ts": row["signal_ts"],
            "window_start_ts": row["signal_ts"],
            "window_end_ts": row["signal_ts"] + HORIZON_SEC,
            "observer_provider": observer.get("path_provider"),
            "observer_source_kind": observer.get("path_source_kind"),
            "observer_price_unit": observer.get("path_price_unit") or "native",
            "observer_path_pool_address": observer.get("path_pool_address"),
        })
    return windows, counts


def gecko_get(url: str, timeout_sec: int, retries: int = 3) -> dict[str, Any] | None:
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            if exc.code == 429 and attempt < retries:
                time.sleep(2 ** attempt * 2)
                continue
            raise
        except Exception:
            if attempt >= retries:
                raise
            time.sleep(1 + attempt)
    return None


def resolve_pool(token_ca: str, timeout_sec: int) -> tuple[str | None, str | None]:
    url = f"{API_BASE}/tokens/{token_ca}/pools?page=1"
    data = gecko_get(url, timeout_sec)
    pools = data.get("data", []) if isinstance(data, dict) else []
    if not pools:
        return None, "no_gecko_pool"
    pool_id = str(pools[0].get("id") or "")
    if not pool_id:
        return None, "pool_id_missing"
    return pool_id.split("_")[-1], None


def fetch_ohlcv(pool_address: str, window_end_ts: int, timeout_sec: int, limit: int) -> list[list[Any]]:
    # Raw-path observer uses token=base. Do not switch to currency=usd here.
    before_ts = int(window_end_ts) + 60
    url = (
        f"{API_BASE}/pools/{pool_address}/ohlcv/minute"
        f"?aggregate=1&limit={int(limit)}&before_timestamp={before_ts}&token=base"
    )
    data = gecko_get(url, timeout_sec)
    return (((data or {}).get("data") or {}).get("attributes") or {}).get("ohlcv_list") or []


def normalize_bar(raw_bar: list[Any], window: dict[str, Any], pool_address: str) -> dict[str, Any] | None:
    if not isinstance(raw_bar, list) or len(raw_bar) < 6:
        return None
    try:
        ts = int(raw_bar[0])
        if ts < int(window["window_start_ts"]) or ts > int(window["window_end_ts"]):
            return None
        return {
            "token_ca": window["token_ca"],
            "timestamp": ts,
            "open": float(raw_bar[1]),
            "high": float(raw_bar[2]),
            "low": float(raw_bar[3]),
            "close": float(raw_bar[4]),
            "volume": float(raw_bar[5] or 0),
            "provider": "geckoterminal",
            "source_kind": "indexed_ohlcv",
            "source_family": "third_party_kline",
            "pool_address": f"indexed_ohlcv:geckoterminal:{window['token_ca']}",
            "price_unit": "native",
            "gecko_pool_address": pool_address,
        }
    except (TypeError, ValueError):
        return None


def write_windows_csv(path: Path, windows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "token_ca",
        "signal_ts",
        "window_start_ts",
        "window_end_ts",
        "observer_provider",
        "observer_source_kind",
        "observer_price_unit",
        "observer_path_pool_address",
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in windows:
            writer.writerow({field: row.get(field) for field in fields})


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pilot-signals", required=True)
    parser.add_argument("--observer-db", required=True)
    parser.add_argument("--out-jsonl", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--windows-csv", default="")
    parser.add_argument("--dry-run", action="store_true", help="Select windows and write manifest only; do not call GeckoTerminal")
    parser.add_argument("--max-signals", type=int, default=0)
    parser.add_argument("--limit", type=int, default=200, help="Gecko OHLCV limit per pool request")
    parser.add_argument("--sleep-sec", type=float, default=DEFAULT_SLEEP_SEC)
    parser.add_argument("--timeout-sec", type=int, default=20)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)

    pilot_path = Path(args.pilot_signals)
    observer_path = Path(args.observer_db)
    out_path = Path(args.out_jsonl)
    manifest_path = Path(args.manifest)
    if out_path.exists() and not args.force:
        raise SystemExit(f"output exists; pass --force to overwrite: {out_path}")
    if manifest_path.exists() and not args.force:
        raise SystemExit(f"manifest exists; pass --force to overwrite: {manifest_path}")

    pilot_rows = read_pilot_signals(pilot_path)
    observer_by_key = load_observer_rows(observer_path, pilot_rows)
    windows, selection_counts = build_windows(pilot_rows, observer_by_key)
    if args.max_signals and args.max_signals > 0:
        windows = windows[:args.max_signals]

    if args.windows_csv:
        write_windows_csv(Path(args.windows_csv), windows)

    fetch_results: list[dict[str, Any]] = []
    row_count = 0
    if not args.dry_run:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as out:
            for idx, window in enumerate(windows, start=1):
                pool, pool_error = resolve_pool(window["token_ca"], args.timeout_sec)
                if not pool:
                    fetch_results.append({
                        "token_ca": window["token_ca"],
                        "signal_ts": window["signal_ts"],
                        "status": "no_pool",
                        "error": pool_error,
                        "bars": 0,
                    })
                    time.sleep(args.sleep_sec)
                    continue
                raw_bars = fetch_ohlcv(pool, window["window_end_ts"], args.timeout_sec, args.limit)
                bars = [normalize_bar(bar, window, pool) for bar in raw_bars]
                bars = [bar for bar in bars if bar is not None]
                for bar in sorted(bars, key=lambda item: item["timestamp"]):
                    out.write(json.dumps(bar, sort_keys=True, separators=(",", ":")) + "\n")
                row_count += len(bars)
                fetch_results.append({
                    "token_ca": window["token_ca"],
                    "signal_ts": window["signal_ts"],
                    "status": "ok" if bars else "no_bars",
                    "gecko_pool_address": pool,
                    "bars": len(bars),
                    "first_ts": min((bar["timestamp"] for bar in bars), default=None),
                    "last_ts": max((bar["timestamp"] for bar in bars), default=None),
                    "index": idx,
                    "total": len(windows),
                })
                time.sleep(args.sleep_sec)
    else:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text("", encoding="utf-8")

    manifest = {
        "schema_version": "gate05_gecko_reconciliation_bars.v1",
        "generated_at": now_iso(),
        "mode": "dry_run" if args.dry_run else "fetch",
        "contract": {
            "purpose": "Gate-0.5 reconciliation bars matching observer geckoterminal indexed_ohlcv source",
            "provider": "geckoterminal",
            "source_kind": "indexed_ohlcv",
            "source_family": "third_party_kline",
            "price_unit": "native",
            "gecko_ohlcv_param": "token=base",
            "not_used": ["currency=usd", "dune_curve_bars"],
        },
        "inputs": {
            "pilot_signals": str(pilot_path),
            "pilot_signals_sha256": sha256_file(pilot_path),
            "observer_db": str(observer_path),
            "observer_db_sha256": sha256_file(observer_path),
        },
        "selection": {
            "pilot_rows": len(pilot_rows),
            "observer_rows_matched": len(observer_by_key),
            "selected_geckoterminal_windows": len(windows),
            "selection_counts": selection_counts,
            "max_signals": args.max_signals or None,
        },
        "output": {
            "out_jsonl": str(out_path),
            "out_jsonl_sha256": sha256_file(out_path),
            "row_count": row_count,
            "windows_csv": str(Path(args.windows_csv)) if args.windows_csv else None,
            "windows_csv_sha256": sha256_file(Path(args.windows_csv)) if args.windows_csv else None,
        },
        "fetch": {
            "limit": args.limit,
            "sleep_sec": args.sleep_sec,
            "timeout_sec": args.timeout_sec,
            "results": fetch_results,
        },
    }
    write_json(manifest_path, manifest)
    print(json.dumps({
        "ok": True,
        "mode": manifest["mode"],
        "selected_geckoterminal_windows": len(windows),
        "row_count": row_count,
        "manifest": str(manifest_path),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

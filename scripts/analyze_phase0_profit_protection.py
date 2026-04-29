#!/usr/bin/env python3
"""
Analyze whether early peak profit protection is working on live paper trades.

This intentionally uses closed-trade summary data only. It is useful for
identifying cohorts that gave back too much profit, but it cannot prove exact
intra-trade trigger timing without tick-level replay.
"""

from __future__ import annotations

import argparse
import sqlite3
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "paper_trades.db"


def pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:+.2f}%"


def fmt_num(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"


def fetch_rows(db: sqlite3.Connection, since_ts: int, max_peak: float) -> list[sqlite3.Row]:
    db.row_factory = sqlite3.Row
    return db.execute(
        """
        SELECT
            id, symbol, entry_ts, exit_ts, exit_reason, pnl_pct, peak_pnl,
            signal_type, strategy_stage, synthetic_close
        FROM paper_trades
        WHERE exit_ts IS NOT NULL
          AND COALESCE(synthetic_close, 0) = 0
          AND entry_ts >= ?
          AND peak_pnl BETWEEN 0 AND ?
          AND pnl_pct BETWEEN -1 AND ?
        ORDER BY entry_ts ASC
        """,
        (since_ts, max_peak, max_peak),
    ).fetchall()


def bucket_for_peak(peak: float) -> str:
    if peak >= 0.50:
        return ">=50%"
    if peak >= 0.25:
        return "25-50%"
    if peak >= 0.15:
        return "15-25%"
    if peak >= 0.12:
        return "12-15%"
    if peak >= 0.08:
        return "8-12%"
    if peak >= 0.05:
        return "5-8%"
    return "<5%"


def reason_group(reason: str | None) -> str:
    reason = reason or "UNKNOWN"
    if "phase0" in reason:
        return "phase0"
    if "ath_phase1" in reason:
        return "ath_phase1"
    if "moon" in reason:
        return "moon"
    if "gap_crash" in reason:
        return "gap_crash"
    if "crash_brake" in reason:
        return "crash_brake"
    if "hard_sl" in reason or reason == "sl" or "stop_loss" in reason:
        return "sl"
    if "trail_stop" in reason or reason == "trail":
        return "trail"
    return reason[:24]


def summarize(rows: list[sqlite3.Row]) -> dict[str, float]:
    if not rows:
        return {}
    pnls = [float(r["pnl_pct"] or 0) for r in rows]
    peaks = [float(r["peak_pnl"] or 0) for r in rows]
    givebacks = [peak - pnl for peak, pnl in zip(peaks, pnls)]
    captures = [pnl / peak for peak, pnl in zip(peaks, pnls) if peak > 0]
    return {
        "n": len(rows),
        "avg_pnl": sum(pnls) / len(pnls),
        "avg_peak": sum(peaks) / len(peaks),
        "avg_giveback": sum(givebacks) / len(givebacks),
        "avg_capture": sum(captures) / len(captures) if captures else None,
        "neg": sum(1 for pnl in pnls if pnl < 0),
        "below_3": sum(1 for pnl in pnls if pnl < 0.03),
    }


def print_summary(title: str, rows: list[sqlite3.Row]) -> None:
    s = summarize(rows)
    if not s:
        print(f"\n{title}: no rows")
        return
    print(f"\n{title}")
    print(
        f"  n={s['n']} avg_pnl={pct(s['avg_pnl'])} avg_peak={pct(s['avg_peak'])} "
        f"avg_giveback={pct(s['avg_giveback'])} avg_capture={pct(s['avg_capture'])} "
        f"neg={int(s['neg'])} below_3={int(s['below_3'])}"
    )


def print_grouped(rows: list[sqlite3.Row], key_fn, title: str) -> None:
    groups: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        groups.setdefault(key_fn(row), []).append(row)

    print(f"\n{title}")
    print(f"{'group':<18} {'n':>4} {'avg_pnl':>9} {'avg_peak':>9} {'giveback':>9} {'capture':>9} {'neg':>4} {'<3%':>4}")
    for key, group_rows in sorted(groups.items()):
        s = summarize(group_rows)
        print(
            f"{key:<18} {int(s['n']):>4} {pct(s['avg_pnl']):>9} {pct(s['avg_peak']):>9} "
            f"{pct(s['avg_giveback']):>9} {pct(s['avg_capture']):>9} "
            f"{int(s['neg']):>4} {int(s['below_3']):>4}"
        )


def simulate_fixed_partial(rows: list[sqlite3.Row], *, threshold: float, sell_pct: float) -> dict[str, float]:
    eligible = [r for r in rows if float(r["peak_pnl"] or 0) >= threshold]
    if not eligible:
        return {"n": 0}
    actual = [float(r["pnl_pct"] or 0) for r in eligible]
    simulated = [(sell_pct * threshold) + ((1.0 - sell_pct) * pnl) for pnl in actual]
    return {
        "n": len(eligible),
        "actual_avg": sum(actual) / len(actual),
        "sim_avg": sum(simulated) / len(simulated),
        "delta_total": sum(simulated) - sum(actual),
        "neg_actual": sum(1 for pnl in actual if pnl < 0),
        "neg_sim": sum(1 for pnl in simulated if pnl < 0),
    }


def simulate_floor(rows: list[sqlite3.Row], *, threshold: float, floor_fn) -> dict[str, float]:
    eligible = [r for r in rows if float(r["peak_pnl"] or 0) >= threshold]
    affected = []
    saved_total = 0.0
    for row in eligible:
        peak = float(row["peak_pnl"] or 0)
        pnl = float(row["pnl_pct"] or 0)
        floor = floor_fn(peak)
        if pnl < floor:
            affected.append(row)
            saved_total += floor - pnl
    return {
        "n": len(eligible),
        "affected": len(affected),
        "saved_total": saved_total,
        "saved_per_affected": saved_total / len(affected) if affected else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--since", default="2026-04-24 00:00:00")
    parser.add_argument("--max-peak", type=float, default=5.0, help="Drop corrupted rows above this decimal peak PnL")
    args = parser.parse_args()

    since_struct = time.strptime(args.since, "%Y-%m-%d %H:%M:%S")
    since_ts = int(time.mktime(since_struct))

    db = sqlite3.connect(args.db)
    rows = fetch_rows(db, since_ts, args.max_peak)
    print(f"DB: {args.db}")
    print(f"Window: entry_ts >= {args.since}, max_peak <= {args.max_peak:.2f}")

    print_summary("All trusted closed trades", rows)
    print_grouped(rows, lambda r: bucket_for_peak(float(r["peak_pnl"] or 0)), "Peak buckets")

    peak8_rows = [r for r in rows if float(r["peak_pnl"] or 0) >= 0.08]
    print_summary("Peak >= 8%", peak8_rows)
    print_grouped(peak8_rows, lambda r: str(r["signal_type"] or "UNKNOWN"), "Peak >= 8% by signal_type")
    print_grouped(peak8_rows, lambda r: reason_group(r["exit_reason"]), "Peak >= 8% by exit reason group")

    print("\nFloor policy upper-bound simulation")
    print(f"{'policy':<18} {'eligible':>8} {'affected':>8} {'saved_total':>12} {'saved/affected':>14}")
    floor_policies = [
        ("breakeven@12", 0.12, lambda _peak: 0.0),
        ("plus3@12", 0.12, lambda _peak: 0.03),
        ("peak50@12", 0.12, lambda peak: peak * 0.50),
        ("peak65@15", 0.15, lambda peak: peak * 0.65),
    ]
    for name, threshold, floor_fn in floor_policies:
        result = simulate_floor(peak8_rows, threshold=threshold, floor_fn=floor_fn)
        print(
            f"{name:<18} {int(result['n']):>8} {int(result['affected']):>8} "
            f"{pct(result['saved_total']):>12} {pct(result['saved_per_affected']):>14}"
        )

    print("\nPartial-lock summary simulation")
    print(f"{'policy':<18} {'eligible':>8} {'actual_avg':>12} {'sim_avg':>12} {'delta_total':>12} {'neg':>9}")
    partial_policies = [
        ("sell25@12", 0.12, 0.25),
        ("sell50@12", 0.12, 0.50),
        ("sell25@15", 0.15, 0.25),
        ("sell50@15", 0.15, 0.50),
    ]
    for name, threshold, sell_pct in partial_policies:
        result = simulate_fixed_partial(peak8_rows, threshold=threshold, sell_pct=sell_pct)
        print(
            f"{name:<18} {int(result['n']):>8} {pct(result.get('actual_avg')):>12} "
            f"{pct(result.get('sim_avg')):>12} {pct(result.get('delta_total')):>12} "
            f"{int(result.get('neg_actual', 0)):>3}->{int(result.get('neg_sim', 0)):<3}"
        )

    print("\nWorst recent peak givebacks (peak >= 12%, final < 3%)")
    candidates = [
        r for r in rows
        if float(r["peak_pnl"] or 0) >= 0.12 and float(r["pnl_pct"] or 0) < 0.03
    ]
    candidates.sort(key=lambda r: float(r["peak_pnl"] or 0) - float(r["pnl_pct"] or 0), reverse=True)
    print(f"{'id':>5} {'symbol':<12} {'pnl':>8} {'peak':>8} {'giveback':>9} {'type':<9} reason")
    for row in candidates[:20]:
        pnl = float(row["pnl_pct"] or 0)
        peak = float(row["peak_pnl"] or 0)
        print(
            f"{int(row['id']):>5} {str(row['symbol'] or '?')[:12]:<12} "
            f"{pct(pnl):>8} {pct(peak):>8} {pct(peak - pnl):>9} "
            f"{str(row['signal_type'] or 'UNKNOWN')[:9]:<9} {str(row['exit_reason'] or '')[:88]}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

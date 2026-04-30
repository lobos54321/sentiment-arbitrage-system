#!/usr/bin/env python3
"""
End-to-end paper trading attribution report.

This report combines:
- executed paper trades
- decision audit events
- missed-signal attribution

It is designed to answer: are we losing because of missed gold/silver/bronze
dogs, entry timing, trend control, execution/slippage, or exits?
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "paper_trades.db"
TRIGGER_PNL_RE = re.compile(r"pnl=([+-]?\d+(?:\.\d+)?)%")


def load_json(value):
    if not value:
        return {}
    try:
        return json.loads(value)
    except Exception:
        return {}


def pct(value):
    if value is None:
        return "n/a"
    return f"{value * 100:+.2f}%"


def pp(value):
    if value is None:
        return "n/a"
    return f"{value * 100:.2f}pp"


def dog_tier(max_pnl):
    value = float(max_pnl or 0.0)
    if value >= 1.00:
        return "gold_100p"
    if value >= 0.50:
        return "silver_50_100p"
    if value >= 0.25:
        return "bronze_25_50p"
    return "sub25"


def parse_since(value):
    if not value:
        return 0
    if value.isdigit():
        return int(value)
    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    return int(dt.replace(tzinfo=timezone.utc).timestamp())


def parse_trigger_pnl(reason):
    if not reason:
        return None
    match = TRIGGER_PNL_RE.search(reason)
    if not match:
        return None
    return float(match.group(1)) / 100.0


def table_exists(db, name):
    return bool(db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone())


def column_exists(db, table_name, column_name):
    try:
        return any(row["name"] == column_name for row in db.execute(f"PRAGMA table_info({table_name})").fetchall())
    except Exception:
        return False


def get_trades(db, since_ts):
    db.row_factory = sqlite3.Row
    return db.execute(
        """
        SELECT *
        FROM paper_trades
        WHERE entry_ts >= ?
        ORDER BY entry_ts ASC
        """,
        (since_ts,),
    ).fetchall()


def classify_trade(row):
    pnl = float(row["pnl_pct"] or 0.0)
    peak = float(row["peak_pnl"] or 0.0)
    reason = row["exit_reason"] or ""
    signal_type = row["signal_type"] or "-"
    route = row["signal_route"] or signal_type
    giveback = peak - pnl
    trigger_pnl = parse_trigger_pnl(reason)
    trigger_to_actual_gap = (trigger_pnl - pnl) if trigger_pnl is not None else None
    monitor_state = load_json(row["monitor_state_json"])
    sold_pct = float(monitor_state.get("soldPct") or 0.0)

    tags = []

    if row["exit_ts"] is None:
        tags.append("open_position")
        if peak >= 0.20:
            tags.append("open_high_peak")
        return tags

    if "no_follow" in reason:
        tags.append("entry_no_follow")
    if "doa" in reason:
        tags.append("entry_doa")
    if "hard_sl" in reason or "lotto_sl" in reason or "_sl" in reason:
        tags.append("stop_loss_or_rug")
    if "gap_crash" in reason:
        tags.append("gap_crash")
    if "breakeven_floor" in reason:
        tags.append("breakeven_floor")
    if "trail" in reason:
        tags.append("trail_exit")

    if peak >= 0.08 and pnl <= 0:
        tags.append("positive_peak_to_loss")
    if peak >= 0.12 and pnl < 0.03:
        tags.append("peak12_to_under3")
    if peak >= 0.20 and pnl < peak * 0.50:
        tags.append("poor_peak_capture")
    if giveback >= 0.15:
        tags.append("large_giveback_15pp")
    if giveback >= 0.30:
        tags.append("extreme_giveback_30pp")
    if trigger_to_actual_gap is not None and trigger_to_actual_gap >= 0.08:
        tags.append("execution_mark_gap_8pp")
    if sold_pct > 0 and pnl <= 0:
        tags.append("partial_lock_still_lost")

    if route == "LOTTO" and peak < 0.05 and pnl <= -0.10:
        tags.append("lotto_bad_entry_quality")
    if signal_type == "ATH" and peak < 0.05 and pnl <= -0.08:
        tags.append("matrix_bad_entry_quality")

    return tags or ["uncategorized"]


def trade_metrics(rows):
    if not rows:
        return {}
    closed = [r for r in rows if r["exit_ts"] is not None]
    wins = [r for r in closed if float(r["pnl_pct"] or 0.0) > 0]
    pnls = [float(r["pnl_pct"] or 0.0) for r in closed]
    peaks = [float(r["peak_pnl"] or 0.0) for r in closed]
    return {
        "entries": len(rows),
        "closed": len(closed),
        "open": len(rows) - len(closed),
        "wins": len(wins),
        "losses": len(closed) - len(wins),
        "avg_pnl": sum(pnls) / len(pnls) if pnls else None,
        "sum_pnl": sum(pnls) if pnls else None,
        "avg_peak": sum(peaks) / len(peaks) if peaks else None,
    }


def print_trade_summary(rows):
    m = trade_metrics(rows)
    print("Trade Summary")
    print(
        f"  entries={m.get('entries', 0)} closed={m.get('closed', 0)} open={m.get('open', 0)} "
        f"wins={m.get('wins', 0)} losses={m.get('losses', 0)} "
        f"avg_pnl={pct(m.get('avg_pnl'))} sum_pnl={pct(m.get('sum_pnl'))} avg_peak={pct(m.get('avg_peak'))}"
    )

    by_route = defaultdict(list)
    for row in rows:
        by_route[(row["signal_type"] or "-", row["signal_route"] or "-", row["strategy_stage"] or "-")].append(row)
    print("\nBy Route")
    print(f"{'signal':<10} {'route':<8} {'stage':<8} {'n':>4} {'closed':>6} {'win':>5} {'avg_pnl':>9} {'avg_peak':>9}")
    for (signal, route, stage), group in sorted(by_route.items(), key=lambda item: len(item[1]), reverse=True):
        gm = trade_metrics(group)
        win_rate = (gm["wins"] / gm["closed"] * 100.0) if gm.get("closed") else 0
        print(
            f"{signal:<10} {route:<8} {stage:<8} {gm['entries']:>4} {gm['closed']:>6} "
            f"{win_rate:>4.0f}% {pct(gm.get('avg_pnl')):>9} {pct(gm.get('avg_peak')):>9}"
        )


def print_failure_buckets(rows, limit):
    bucket_rows = defaultdict(list)
    for row in rows:
        for tag in classify_trade(row):
            bucket_rows[tag].append(row)

    print("\nFailure / Attribution Buckets")
    print(f"{'bucket':<28} {'n':>4} {'avg_pnl':>9} {'sum_pnl':>9} {'avg_peak':>9} examples")
    for tag, group in sorted(bucket_rows.items(), key=lambda item: len(item[1]), reverse=True):
        closed = [r for r in group if r["exit_ts"] is not None]
        pnls = [float(r["pnl_pct"] or 0.0) for r in closed]
        peaks = [float(r["peak_pnl"] or 0.0) for r in closed]
        examples = ", ".join((r["symbol"] or "?") for r in group[-5:])
        print(
            f"{tag:<28} {len(group):>4} "
            f"{pct(sum(pnls) / len(pnls) if pnls else None):>9} "
            f"{pct(sum(pnls) if pnls else None):>9} "
            f"{pct(sum(peaks) / len(peaks) if peaks else None):>9} {examples}"
        )

    print("\nWorst Trades")
    closed = [r for r in rows if r["exit_ts"] is not None]
    closed.sort(key=lambda r: float(r["pnl_pct"] or 0.0))
    print(f"{'id':>5} {'symbol':<12} {'route':<8} {'pnl':>8} {'peak':>8} {'giveback':>9} reason")
    for row in closed[:limit]:
        pnl = float(row["pnl_pct"] or 0.0)
        peak = float(row["peak_pnl"] or 0.0)
        route = row["signal_route"] or row["signal_type"] or "-"
        print(
            f"{int(row['id']):>5} {str(row['symbol'] or '?')[:12]:<12} {route:<8} "
            f"{pct(pnl):>8} {pct(peak):>8} {pp(peak - pnl):>9} {str(row['exit_reason'] or '')[:90]}"
        )


def print_decision_events(db, since_ts, limit):
    if not table_exists(db, "paper_decision_events"):
        return
    rows = db.execute(
        """
        SELECT component, event_type, decision, reason, COUNT(*) AS n
        FROM paper_decision_events
        WHERE event_ts >= ?
        GROUP BY component, event_type, decision, reason
        ORDER BY n DESC
        LIMIT ?
        """,
        (since_ts, limit),
    ).fetchall()
    print("\nDecision Event Hotspots")
    for row in rows:
        print(
            f"  n={row['n']:4d} {row['component']}/{row['event_type']} "
            f"{row['decision']} reason={row['reason']}"
        )


def print_missed_attribution(db, since_ts, limit):
    if not table_exists(db, "paper_missed_signal_attribution"):
        return
    has_tradability = column_exists(db, "paper_missed_signal_attribution", "tradable_missed")
    tradability_select = (
        """
          SUM(CASE WHEN tradable_missed = 1 THEN 1 ELSE 0 END) AS tradable_n,
          SUM(CASE WHEN tradability_status = 'would_stop_before_peak' THEN 1 ELSE 0 END) AS stop_before_peak_n,
        """
        if has_tradability else
        """
          NULL AS tradable_n,
          NULL AS stop_before_peak_n,
        """
    )
    rows = db.execute(
        f"""
        SELECT
          COALESCE(route, '-') AS route,
          component,
          reject_reason,
          COUNT(*) AS n,
          SUM(CASE WHEN COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0) >= 0.50 THEN 1 ELSE 0 END) AS dog50,
          SUM(CASE WHEN COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0) >= 1.00 THEN 1 ELSE 0 END) AS dog100,
          {tradability_select}
          AVG(pnl_5m) AS avg_5m,
          AVG(pnl_15m) AS avg_15m,
          AVG(pnl_60m) AS avg_60m
        FROM paper_missed_signal_attribution
        WHERE signal_ts >= ?
        GROUP BY COALESCE(route, '-'), component, reject_reason
        ORDER BY dog100 DESC, dog50 DESC, n DESC
        LIMIT ?
        """,
        (since_ts, limit),
    ).fetchall()
    print("\nMissed-Dog Attribution")
    for row in rows:
        print(
            f"  n={row['n']:4d} dog50={row['dog50'] or 0:3d} dog100={row['dog100'] or 0:3d} "
            f"tradable={row['tradable_n'] if row['tradable_n'] is not None else 'n/a'} "
            f"stopFirst={row['stop_before_peak_n'] if row['stop_before_peak_n'] is not None else 'n/a'} "
            f"avg5={pct(row['avg_5m'])} avg15={pct(row['avg_15m'])} avg60={pct(row['avg_60m'])} "
            f"{row['route']} {row['component']} reason={row['reject_reason']}"
        )

    dogs = db.execute(
        f"""
        SELECT symbol, route, component, reject_reason, max_pnl_recorded, pnl_5m, pnl_15m, pnl_60m
               {', tradable_missed, tradability_status, mae_before_peak_pnl, time_to_peak_sec' if has_tradability else ''}
        FROM paper_missed_signal_attribution
        WHERE signal_ts >= ?
          AND COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0) >= 0.50
        ORDER BY COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0) DESC
        LIMIT ?
        """,
        (since_ts, limit),
    ).fetchall()
    print("\nTop Missed Dogs In Window")
    for row in dogs:
        tradability = ""
        if has_tradability:
            tradability = (
                f" tradable={row['tradable_missed'] or 0}/{row['tradability_status'] or 'n/a'}"
                f" mae={pct(row['mae_before_peak_pnl'])} tPeak={row['time_to_peak_sec'] or 'n/a'}s"
            )
        print(
            f"  {str(row['symbol'] or '?')[:12]:<12} max={pct(row['max_pnl_recorded'])} "
            f"5m={pct(row['pnl_5m'])} 15m={pct(row['pnl_15m'])} 60m={pct(row['pnl_60m'])} "
            f"{row['route']} {row['component']}{tradability} reason={row['reject_reason']}"
        )


def print_selection_quality(rows, db, since_ts, limit):
    if not table_exists(db, "paper_missed_signal_attribution"):
        return
    has_tradability = column_exists(db, "paper_missed_signal_attribution", "tradable_missed")

    traded_tiers = Counter()
    traded_dogs = []
    for row in rows:
        peak = float(row["peak_pnl"] or 0.0)
        tier = dog_tier(peak)
        traded_tiers[tier] += 1
        if tier != "sub25":
            traded_dogs.append((peak, row))

    missed_rows = db.execute(
        f"""
        SELECT
          symbol,
          COALESCE(route, '-') AS route,
          component,
          reject_reason,
          pnl_5m,
          pnl_15m,
          pnl_60m,
          {'tradable_missed, tradability_status, mae_before_peak_pnl, time_to_peak_sec,' if has_tradability else ''}
          COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0) AS max_pnl
        FROM paper_missed_signal_attribution
        WHERE signal_ts >= ?
        """,
        (since_ts,),
    ).fetchall()

    missed_tiers = Counter()
    tradable_tiers = Counter()
    gate_tiers = defaultdict(Counter)
    missed_dogs = []
    for row in missed_rows:
        max_pnl = float(row["max_pnl"] or 0.0)
        tier = dog_tier(max_pnl)
        missed_tiers[tier] += 1
        if has_tradability and row["tradable_missed"]:
            tradable_tiers[tier] += 1
        gate_tiers[(row["route"], row["component"], row["reject_reason"])][tier] += 1
        if tier != "sub25":
            missed_dogs.append((max_pnl, row))

    print("\nSelection Quality")
    print("  dog tiers: gold>=100%, silver=50-100%, bronze=25-50% peak/max recorded")
    print(f"{'source':<8} {'gold':>5} {'silver':>6} {'bronze':>6} {'sub25':>6} {'25p+':>6} {'50p+':>6} {'100p+':>6}")
    for source, tiers in (("traded", traded_tiers), ("missed", missed_tiers)):
        bronze_plus = tiers["bronze_25_50p"] + tiers["silver_50_100p"] + tiers["gold_100p"]
        silver_plus = tiers["silver_50_100p"] + tiers["gold_100p"]
        print(
            f"{source:<8} {tiers['gold_100p']:>5} {tiers['silver_50_100p']:>6} "
            f"{tiers['bronze_25_50p']:>6} {tiers['sub25']:>6} "
            f"{bronze_plus:>6} {silver_plus:>6} {tiers['gold_100p']:>6}"
        )
    if has_tradability:
        bronze_plus = tradable_tiers["bronze_25_50p"] + tradable_tiers["silver_50_100p"] + tradable_tiers["gold_100p"]
        silver_plus = tradable_tiers["silver_50_100p"] + tradable_tiers["gold_100p"]
        print(
            f"{'tradable':<8} {tradable_tiers['gold_100p']:>5} {tradable_tiers['silver_50_100p']:>6} "
            f"{tradable_tiers['bronze_25_50p']:>6} {tradable_tiers['sub25']:>6} "
            f"{bronze_plus:>6} {silver_plus:>6} {tradable_tiers['gold_100p']:>6}"
        )

    print("\nSelection Miss Hotspots")
    print(f"{'route':<7} {'component':<18} {'gold':>5} {'silver':>6} {'bronze':>6} reason")
    for (route, component, reason), tiers in sorted(
        gate_tiers.items(),
        key=lambda item: (
            item[1]["gold_100p"],
            item[1]["silver_50_100p"],
            item[1]["bronze_25_50p"],
            sum(item[1].values()),
        ),
        reverse=True,
    )[:limit]:
        if not (tiers["gold_100p"] or tiers["silver_50_100p"] or tiers["bronze_25_50p"]):
            continue
        print(
            f"{route:<7} {component:<18} {tiers['gold_100p']:>5} "
            f"{tiers['silver_50_100p']:>6} {tiers['bronze_25_50p']:>6} {str(reason or '-')[:90]}"
        )

    print("\nTop Missed Selection Dogs")
    tradable_header = " tradability" if has_tradability else ""
    print(f"{'symbol':<12} {'tier':<14} {'max':>8} {'5m':>8} {'15m':>8} {'60m':>8}{tradable_header} gate")
    for max_pnl, row in sorted(missed_dogs, key=lambda item: item[0], reverse=True)[:limit]:
        tradability = ""
        if has_tradability:
            tradability = f" {row['tradable_missed'] or 0}/{str(row['tradability_status'] or 'n/a')[:18]}"
        print(
            f"{str(row['symbol'] or '?')[:12]:<12} {dog_tier(max_pnl):<14} "
            f"{pct(max_pnl):>8} {pct(row['pnl_5m']):>8} {pct(row['pnl_15m']):>8} {pct(row['pnl_60m']):>8} "
            f"{tradability} {row['route']}/{row['component']} {str(row['reject_reason'] or '-')[:70]}"
        )

    print("\nTop Traded Selection Dogs")
    print(f"{'symbol':<12} {'tier':<14} {'peak':>8} {'pnl':>8} reason")
    for peak, row in sorted(traded_dogs, key=lambda item: item[0], reverse=True)[:limit]:
        print(
            f"{str(row['symbol'] or '?')[:12]:<12} {dog_tier(peak):<14} "
            f"{pct(peak):>8} {pct(row['pnl_pct']):>8} {str(row['exit_reason'] or 'open')[:90]}"
        )


def print_path_sample_quality(db, since_ts, limit):
    if not table_exists(db, "paper_trade_path_samples"):
        return
    summary = db.execute(
        """
        SELECT
          COUNT(*) AS samples,
          COUNT(DISTINCT trade_id) AS trades,
          SUM(CASE WHEN quote_pnl IS NOT NULL THEN 1 ELSE 0 END) AS quote_samples,
          AVG(mark_pnl) AS avg_mark_pnl,
          AVG(quote_pnl) AS avg_quote_pnl,
          AVG(CASE WHEN quote_pnl IS NOT NULL THEN quote_pnl - mark_pnl END) AS avg_quote_gap,
          MAX(ABS(CASE WHEN quote_pnl IS NOT NULL THEN quote_pnl - mark_pnl END)) AS max_abs_quote_gap
        FROM paper_trade_path_samples
        WHERE sample_ts >= ?
        """,
        (since_ts,),
    ).fetchone()
    print("\nPath Sample Quality")
    print(
        f"  samples={summary['samples'] or 0} trades={summary['trades'] or 0} "
        f"quote_samples={summary['quote_samples'] or 0} "
        f"avg_mark={pct(summary['avg_mark_pnl'])} avg_quote={pct(summary['avg_quote_pnl'])} "
        f"avg_quote_gap={pp(summary['avg_quote_gap'])} max_abs_quote_gap={pp(summary['max_abs_quote_gap'])}"
    )

    rows = db.execute(
        """
        SELECT
          trade_id,
          symbol,
          action,
          reason,
          sample_ts,
          mark_pnl,
          quote_pnl,
          quote_pnl - mark_pnl AS quote_gap,
          peak_pnl,
          sold_pct,
          partial_realized_sol,
          blended_mark_pnl,
          blended_quote_pnl
        FROM paper_trade_path_samples
        WHERE sample_ts >= ?
          AND (quote_pnl IS NOT NULL OR sold_pct > 0 OR blended_mark_pnl IS NOT NULL)
        ORDER BY ABS(COALESCE(quote_pnl - mark_pnl, 0)) DESC, sample_ts DESC
        LIMIT ?
        """,
        (since_ts, limit),
    ).fetchall()
    if rows:
        print("  Notable path samples:")
        for row in rows:
            print(
                f"    #{row['trade_id']} {str(row['symbol'] or '?')[:12]:<12} {row['action']:<12} "
                f"mark={pct(row['mark_pnl'])} quote={pct(row['quote_pnl'])} gap={pp(row['quote_gap'])} "
                f"peak={pct(row['peak_pnl'])} sold={_fmt_pct_plain(row['sold_pct'])} "
                f"blend_mark={pct(row['blended_mark_pnl'])} blend_quote={pct(row['blended_quote_pnl'])} "
                f"{str(row['reason'] or '-')[:70]}"
            )


def _fmt_pct_plain(value):
    if value is None:
        return "n/a"
    return f"{float(value) * 100:.0f}%"


def print_lifecycle_quality(rows, db, since_ts, limit):
    has_trade_state = bool(rows and "lifecycle_state" in rows[0].keys())
    has_missed_state = (
        table_exists(db, "paper_missed_signal_attribution")
        and column_exists(db, "paper_missed_signal_attribution", "lifecycle_state")
    )
    if not has_trade_state and not has_missed_state:
        return

    print("\nLifecycle Quality")
    if has_trade_state:
        groups = defaultdict(list)
        for row in rows:
            groups[row["lifecycle_state"] or "UNKNOWN"].append(row)
        print("  Traded by lifecycle_state")
        print(f"  {'state':<24} {'n':>4} {'closed':>6} {'win':>5} {'avg_pnl':>9} {'avg_peak':>9}")
        for state, group in sorted(groups.items(), key=lambda item: len(item[1]), reverse=True)[:limit]:
            gm = trade_metrics(group)
            win_rate = (gm["wins"] / gm["closed"] * 100.0) if gm.get("closed") else 0
            print(
                f"  {state:<24} {gm['entries']:>4} {gm['closed']:>6} "
                f"{win_rate:>4.0f}% {pct(gm.get('avg_pnl')):>9} {pct(gm.get('avg_peak')):>9}"
            )

        bias_groups = defaultdict(list)
        for row in rows:
            bias_groups[row["entry_bias"] or "UNKNOWN"].append(row)
        print("  Traded by shadow entry_bias")
        print(f"  {'bias':<10} {'n':>4} {'closed':>6} {'win':>5} {'avg_pnl':>9} {'sum_pnl':>9} {'avg_peak':>9}")
        for bias, group in sorted(bias_groups.items(), key=lambda item: len(item[1]), reverse=True):
            gm = trade_metrics(group)
            win_rate = (gm["wins"] / gm["closed"] * 100.0) if gm.get("closed") else 0
            print(
                f"  {bias:<10} {gm['entries']:>4} {gm['closed']:>6} "
                f"{win_rate:>4.0f}% {pct(gm.get('avg_pnl')):>9} {pct(gm.get('sum_pnl')):>9} {pct(gm.get('avg_peak')):>9}"
            )

        counterfactual = {
            "would_take_probe": [r for r in rows if (r["entry_bias"] or "").upper() == "PROBE"],
            "would_avoid_reject": [r for r in rows if (r["entry_bias"] or "").upper() == "REJECT"],
            "would_wait_or_observe": [
                r for r in rows if (r["entry_bias"] or "").upper() in {"WAIT", "OBSERVE", "UNKNOWN", ""}
            ],
        }
        print("  Lifecycle Shadow Gate")
        for name, group in counterfactual.items():
            gm = trade_metrics(group)
            if not gm:
                continue
            win_rate = (gm["wins"] / gm["closed"] * 100.0) if gm.get("closed") else 0
            print(
                f"    {name:<20} n={gm['entries']:>3} closed={gm['closed']:>3} "
                f"win={win_rate:>4.0f}% sum={pct(gm.get('sum_pnl')):>9} avg={pct(gm.get('avg_pnl')):>9} peak={pct(gm.get('avg_peak')):>9}"
            )

    if has_missed_state:
        missed = db.execute(
            """
            SELECT
              COALESCE(lifecycle_state, 'UNKNOWN') AS lifecycle_state,
              COUNT(*) AS n,
              SUM(CASE WHEN COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0) >= 1.00 THEN 1 ELSE 0 END) AS gold,
              SUM(CASE WHEN COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0) >= 0.50 THEN 1 ELSE 0 END) AS dog50,
              SUM(CASE WHEN COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0) >= 0.25 THEN 1 ELSE 0 END) AS dog25,
              AVG(vitality_score) AS avg_vitality,
              AVG(pnl_15m) AS avg_15m,
              AVG(pnl_60m) AS avg_60m
            FROM paper_missed_signal_attribution
            WHERE signal_ts >= ?
            GROUP BY COALESCE(lifecycle_state, 'UNKNOWN')
            ORDER BY gold DESC, dog50 DESC, dog25 DESC, n DESC
            LIMIT ?
            """,
            (since_ts, limit),
        ).fetchall()
        if missed:
            print("  Missed by lifecycle_state")
            print(f"  {'state':<24} {'n':>4} {'25p+':>5} {'50p+':>5} {'100p+':>6} {'vital':>7} {'avg15':>8} {'avg60':>8}")
            for row in missed:
                vital = "n/a" if row["avg_vitality"] is None else f"{row['avg_vitality']:.1f}"
                print(
                    f"  {row['lifecycle_state']:<24} {row['n']:>4} {row['dog25'] or 0:>5} "
                    f"{row['dog50'] or 0:>5} {row['gold'] or 0:>6} {vital:>7} "
                    f"{pct(row['avg_15m']):>8} {pct(row['avg_60m']):>8}"
                )

        if column_exists(db, "paper_missed_signal_attribution", "entry_bias"):
            missed_bias = db.execute(
                """
                SELECT
                  COALESCE(entry_bias, 'UNKNOWN') AS entry_bias,
                  COUNT(*) AS n,
                  SUM(CASE WHEN COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0) >= 0.25 THEN 1 ELSE 0 END) AS dog25,
                  SUM(CASE WHEN COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0) >= 0.50 THEN 1 ELSE 0 END) AS dog50,
                  SUM(CASE WHEN COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0) >= 1.00 THEN 1 ELSE 0 END) AS gold,
                  AVG(vitality_score) AS avg_vitality,
                  AVG(pnl_15m) AS avg_15m,
                  AVG(pnl_60m) AS avg_60m
                FROM paper_missed_signal_attribution
                WHERE signal_ts >= ?
                GROUP BY COALESCE(entry_bias, 'UNKNOWN')
                ORDER BY gold DESC, dog50 DESC, dog25 DESC, n DESC
                LIMIT ?
                """,
                (since_ts, limit),
            ).fetchall()
            if missed_bias:
                print("  Missed by shadow entry_bias")
                print(f"  {'bias':<10} {'n':>4} {'25p+':>5} {'50p+':>5} {'100p+':>6} {'vital':>7} {'avg15':>8} {'avg60':>8}")
                for row in missed_bias:
                    vital = "n/a" if row["avg_vitality"] is None else f"{row['avg_vitality']:.1f}"
                    print(
                        f"  {row['entry_bias']:<10} {row['n']:>4} {row['dog25'] or 0:>5} "
                        f"{row['dog50'] or 0:>5} {row['gold'] or 0:>6} {vital:>7} "
                        f"{pct(row['avg_15m']):>8} {pct(row['avg_60m']):>8}"
                    )


def print_phase_policy_shadow(db, since_ts, limit):
    if not table_exists(db, "paper_decision_events"):
        return
    rows = db.execute(
        """
        SELECT trade_id, symbol, decision, reason, payload_json, event_ts
        FROM paper_decision_events
        WHERE event_ts >= ?
          AND component = 'phase_policy'
          AND event_type = 'shadow_decision'
        ORDER BY event_ts ASC
        """,
        (since_ts,),
    ).fetchall()
    if not rows:
        return

    groups = defaultdict(list)
    actions = Counter()
    high_rug = []
    for row in rows:
        payload = load_json(row["payload_json"])
        phase = payload.get("phase_state") or "UNKNOWN"
        action = row["decision"] or payload.get("shadow_action") or "UNKNOWN"
        kline = payload.get("kline_position_state") or "UNKNOWN"
        groups[(phase, action, kline)].append((row, payload))
        actions[action] += 1
        rug = float(payload.get("rug_risk_score") or 0.0)
        if rug >= 50:
            high_rug.append((rug, row, payload))

    print("\nPhase Policy Shadow")
    print(f"  samples={len(rows)} trades={len(set(r['trade_id'] for r in rows if r['trade_id'] is not None))}")
    print("  Actions: " + ", ".join(f"{k}={v}" for k, v in actions.most_common()))
    print(f"  {'phase':<20} {'action':<12} {'kline':<20} {'n':>4} {'avg_ev':>7} {'avg_rug':>8} examples")
    for (phase, action, kline), group in sorted(groups.items(), key=lambda item: len(item[1]), reverse=True)[:limit]:
        evs = [float(p.get("ev_score") or 0.0) for _, p in group]
        rugs = [float(p.get("rug_risk_score") or 0.0) for _, p in group]
        examples = ", ".join(str((r["symbol"] or "?"))[:10] for r, _ in group[-4:])
        print(
            f"  {phase:<20} {action:<12} {kline:<20} {len(group):>4} "
            f"{(sum(evs) / len(evs) if evs else 0):>7.1f} "
            f"{(sum(rugs) / len(rugs) if rugs else 0):>8.1f} {examples}"
        )

    if high_rug:
        print("  High Rug-Risk Samples")
        for rug, row, payload in sorted(high_rug, key=lambda item: item[0], reverse=True)[:limit]:
            print(
                f"    #{row['trade_id']} {str(row['symbol'] or '?')[:12]:<12} "
                f"rug={rug:>5.1f} phase={payload.get('phase_state')} action={row['decision']} "
                f"mark={pct(payload.get('current_pnl'))} peak={pct(payload.get('peak_pnl'))} "
                f"kline={payload.get('kline_position_state')} reason={row['reason']}"
            )


def print_lotto_probe_shadow(db, since_ts, limit):
    if not table_exists(db, "paper_decision_events"):
        return
    rows = db.execute(
        """
        SELECT symbol, reason, payload_json, event_ts
        FROM paper_decision_events
        WHERE event_ts >= ?
          AND component = 'lotto_probe_shadow'
          AND event_type = 'probe_candidate'
        ORDER BY event_ts DESC
        LIMIT ?
        """,
        (since_ts, limit),
    ).fetchall()
    if not rows:
        return

    print("\nLOTTO Probe Shadow")
    print(f"  candidates={len(rows)}")
    print(f"  {'symbol':<12} {'5m':>8} {'15m':>8} {'60m':>8} {'source':<18} reason")
    for row in rows:
        payload = load_json(row["payload_json"])
        source = payload.get("source_component") or "?"
        source_reason = payload.get("source_reject_reason") or row["reason"]
        print(
            f"  {str(row['symbol'] or '?')[:12]:<12} "
            f"{pct(payload.get('pnl_5m')):>8} {pct(payload.get('pnl_15m')):>8} "
            f"{pct(payload.get('pnl_60m')):>8} {source:<18} {source_reason}"
        )


def print_decision_read(rows, db, since_ts):
    closed = [r for r in rows if r["exit_ts"] is not None]
    tags = Counter()
    for row in rows:
        tags.update(classify_trade(row))

    missed_dogs = 0
    if table_exists(db, "paper_missed_signal_attribution"):
        missed_dogs = db.execute(
            """
            SELECT COUNT(*)
            FROM paper_missed_signal_attribution
            WHERE signal_ts >= ?
              AND COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0) >= 0.50
            """,
            (since_ts,),
        ).fetchone()[0]

    print("\nClarify-Reason-Act Read")
    print("  Restated question: which mechanism explains current bad results with the fewest assumptions?")
    print("  Key unknown: exact intra-trade path is still incomplete; use peak/final/trigger gaps as proxy.")
    if tags["execution_mark_gap_8pp"] or tags["positive_peak_to_loss"]:
        print(
            "  Simplest hypothesis: exit execution/mark-to-fill gap plus late protection is the largest live-trade problem."
        )
    elif tags["entry_no_follow"] + tags["entry_doa"] > len(closed) * 0.4:
        print("  Simplest hypothesis: entry quality/timing is the main problem; many trades never get follow-through.")
    elif missed_dogs:
        print("  Simplest hypothesis: missed-dog gates are still leaving material upside outside the entry lane.")
    else:
        print("  Simplest hypothesis: no single dominant bucket yet; keep collecting path-level samples.")

    print("  Smallest next action:")
    print("    1. Compare phase_policy/control_decision exits against LOTTO hard_floor/sl losses.")
    print("    2. Compare lotto_probe_shadow candidates against missed 25p/50p/100p dogs.")
    print("    3. Only promote probe to real paper if its shadow drawdown is lower than main LOTTO.")
    print("  Decision rule:")
    print("    Continue if LOTTO hard losses fall without killing 25p+ winners; otherwise tighten entry before adding probe buys.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=os.environ.get("PAPER_DB", str(DEFAULT_DB)))
    parser.add_argument("--since", default="2026-04-29 08:48:00")
    parser.add_argument("--limit", type=int, default=12)
    args = parser.parse_args()

    since_ts = parse_since(args.since)
    db = sqlite3.connect(args.db)
    db.row_factory = sqlite3.Row

    rows = get_trades(db, since_ts)
    print(f"DB: {args.db}")
    print(f"Window: entry_ts >= {args.since} ({since_ts})")
    print()
    print_trade_summary(rows)
    print_failure_buckets(rows, args.limit)
    print_decision_events(db, since_ts, args.limit)
    print_missed_attribution(db, since_ts, args.limit)
    print_selection_quality(rows, db, since_ts, args.limit)
    print_path_sample_quality(db, since_ts, args.limit)
    print_lifecycle_quality(rows, db, since_ts, args.limit)
    print_phase_policy_shadow(db, since_ts, args.limit)
    print_lotto_probe_shadow(db, since_ts, args.limit)
    print_decision_read(rows, db, since_ts)


if __name__ == "__main__":
    main()

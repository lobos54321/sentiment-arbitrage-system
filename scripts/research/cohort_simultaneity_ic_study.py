#!/usr/bin/env python3
"""Cohort / market-simultaneity IC study (read-only research).

Question (from the CryptoGAT paper thesis): does the SIMULTANEOUS state of the
meme cohort at signal time predict whether a newborn token becomes a gold/silver
dog, independent of the token's own history?

This is a SHADOW-ONLY research probe. It never writes to production tables, never
changes strategy/gates/executor, and its output is discovery evidence only
(promotion stays human-gated). It computes time-legal cohort features and reports
their information coefficient (IC = Spearman rank corr vs sustained peak), the
paper-comparable metric, plus a strict temporal holdout — the exact gate Phase 3
C4 would apply, run cheaply up front so we do not build a forward-OOS apparatus to
rediscover a null.

Time legality (the #1 trap): gold/silver labels mature `horizon_sec` (2h) after a
signal, so any "recent dog" feature must only count signals whose maturation
timestamp <= t. Any live-price feature must only read bars with timestamp <= t.
Both invariants are asserted by --self-test.

Usage:
  python cohort_simultaneity_ic_study.py --db /app/data/raw_signal_outcomes.db --out report.json
  python cohort_simultaneity_ic_study.py --self-test
"""
from __future__ import annotations
import argparse, bisect, json, sqlite3, sys, tempfile, os
from statistics import median

GOLD_SILVER = ("gold", "silver")
MATURE_HORIZON_SEC = 7200          # gold/silver label horizon
STALE_BAR_SEC = 1200               # a bar older than this is unusable as "price now"
MIN_IC = 0.03                      # minimum |IC| to call an effect economically meaningful


def _is_gs(tier): return tier in GOLD_SILVER


def load_signals(db):
    rows = db.execute(
        "SELECT signal_ts, token_ca, raw_sustained_tier, signal_type, max_sustained_peak_pct "
        "FROM raw_signal_outcomes "
        "WHERE sustained_evaluable=1 AND signal_ts IS NOT NULL "
        "AND max_sustained_peak_pct IS NOT NULL ORDER BY signal_ts"
    ).fetchall()
    return [(int(t), ca, tier, st, float(pk)) for t, ca, tier, st, pk in rows]


def load_bars(db):
    bars = {}
    for ca, t, c in db.execute(
        "SELECT token_ca, timestamp, close FROM raw_price_bars_1m "
        "WHERE close IS NOT NULL ORDER BY token_ca, timestamp"
    ):
        bars.setdefault(ca, []).append((int(t), c))
    return bars


def price_at(bars, ca, tau):
    """Last close at or before tau (time-legal), or None if stale/missing."""
    b = bars.get(ca)
    if not b:
        return None
    i = bisect.bisect_right(b, (tau, float("inf"))) - 1
    if i < 0:
        return None
    bt, bc = b[i]
    return bc if (tau - bt <= STALE_BAR_SEC and bc > 0) else None


def ret(bars, ca, tau, back):
    p1 = price_at(bars, ca, tau)
    p0 = price_at(bars, ca, tau - back)
    return (p1 / p0 - 1) if (p1 and p0 and p0 > 0) else None


def compute_features(sig, bars):
    """All features are computed using ONLY information available at signal time t."""
    n = len(sig)
    ts = [s[0] for s in sig]
    F1 = [0] * n          # cohort_signal_density: distinct other tokens signaled in [t-30m, t)
    F3 = [0] * n          # matured_gs_density: gold/silver matured in [t-3h, t-2h)  (legal: matured <= t)
    F4 = [None] * n       # meme_market_factor: cohort median 15m live return at t
    F8 = [None] * n       # cohort_breadth: fraction of cohort with positive 15m return
    for i, (t, ca, tier, st, pk) in enumerate(sig):
        lo1 = bisect.bisect_left(ts, t - 1800); hi1 = bisect.bisect_left(ts, t)
        F1[i] = len({sig[j][1] for j in range(lo1, hi1) if sig[j][1] != ca})
        lo3 = bisect.bisect_left(ts, t - 3 * 3600); hi3 = bisect.bisect_left(ts, t - MATURE_HORIZON_SEC)
        F3[i] = sum(1 for j in range(lo3, hi3) if _is_gs(sig[j][2]))
        lo2 = bisect.bisect_left(ts, t - MATURE_HORIZON_SEC); hi2 = bisect.bisect_left(ts, t - 300)
        seen = set(); rr = []
        for j in range(lo2, hi2):
            cca = sig[j][1]
            if cca == ca or cca in seen:
                continue
            seen.add(cca)
            r = ret(bars, cca, t, 900)
            if r is not None:
                rr.append(r)
        if len(rr) >= 3:
            F4[i] = median(rr)
            F8[i] = sum(1 for x in rr if x > 0) / len(rr)
    return {"F1_signal_density": F1, "F3_matured_gs_density": F3,
            "F4_cohort_market_factor": F4, "F8_cohort_breadth": F8}


def spearman(pairs):
    pairs = [(x, y) for x, y in pairs if x is not None and y is not None]
    m = len(pairs)
    if m < 50:
        return None, m
    def rank(vals):
        order = sorted(range(len(vals)), key=lambda i: vals[i])
        r = [0.0] * len(vals); i = 0
        while i < len(vals):
            j = i
            while j < len(vals) and vals[order[j]] == vals[order[i]]:
                j += 1
            for k in range(i, j):
                r[order[k]] = (i + j - 1) / 2.0
            i = j
        return r
    xr = rank([p[0] for p in pairs]); yr = rank([p[1] for p in pairs])
    mx = sum(xr) / m; my = sum(yr) / m
    num = sum((a - mx) * (b - my) for a, b in zip(xr, yr))
    dx = sum((a - mx) ** 2 for a in xr) ** 0.5
    dy = sum((b - my) ** 2 for b in yr) ** 0.5
    return (num / (dx * dy) if dx * dy else 0.0), m


def study(sig, feats, holdout_frac=0.6):
    ts = [s[0] for s in sig]
    pk = [s[4] for s in sig]
    split = ts[int(len(sig) * holdout_frac)]
    out = {}
    for name, f in feats.items():
        ic_all, n_all = spearman(list(zip(f, pk)))
        tr = [(f[i], pk[i]) for i in range(len(sig)) if sig[i][0] < split]
        te = [(f[i], pk[i]) for i in range(len(sig)) if sig[i][0] >= split]
        ic_tr, n_tr = spearman(tr); ic_te, n_te = spearman(te)
        # An effect only "holds" if it is (1) meaningfully present in-sample
        # (|ic_train| >= MIN_IC, so a near-zero train IC cannot be trivially "matched"
        # by a noise blip in test), (2) same sign out-of-sample, and (3) retains at
        # least half its magnitude. Requiring |ic_train| >= MIN_IC is what kills
        # noise features whose train IC ~ 0 but test IC happens to spike.
        holds = bool(
            ic_tr is not None and ic_te is not None
            and abs(ic_tr) >= MIN_IC
            and ic_tr * ic_te > 0
            and abs(ic_te) >= abs(ic_tr) * 0.5
            and abs(ic_te) >= MIN_IC
        )
        out[name] = {
            "ic_full": round(ic_all, 4) if ic_all is not None else None, "n_full": n_all,
            "ic_train": round(ic_tr, 4) if ic_tr is not None else None, "n_train": n_tr,
            "ic_test": round(ic_te, 4) if ic_te is not None else None, "n_test": n_te,
            "holdout_holds": holds,
            "coverage": round(sum(1 for v in f if v is not None) / len(f), 4),
        }
    return out


def run(db_path, holdout_frac=0.6):
    db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    sig = load_signals(db)
    bars = load_bars(db)
    base = sum(1 for s in sig if _is_gs(s[2])) / len(sig) if sig else 0.0
    feats = compute_features(sig, bars)
    res = study(sig, feats, holdout_frac)
    surviving = [k for k, v in res.items()
                 if v["holdout_holds"] and abs(v["ic_full"] or 0) >= MIN_IC]
    return {
        "schema_version": "cohort_simultaneity_ic_study.v1",
        "allowed_use": "shadow_only_discovery_evidence",
        "promotion_allowed": False,
        "n_signals": len(sig),
        "base_gold_silver_rate": round(base, 4),
        "paper_reference_ic": 0.047,
        "holdout_frac": holdout_frac,
        "features": res,
        "surviving_features_ic_ge_0p03_out_of_sample": surviving,
        "verdict": ("COHORT_SIGNAL_SURVIVES_HOLDOUT" if surviving
                    else "NO_STABLE_COHORT_SIGNAL_HOLDOUT_FAILS"),
    }


def self_test():
    """Assert (a) time legality and (b) recovery of a planted signal."""
    tmp = tempfile.mkdtemp()
    path = os.path.join(tmp, "t.db")
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE raw_signal_outcomes (signal_ts INT, token_ca TEXT, raw_sustained_tier TEXT, "
               "signal_type TEXT, max_sustained_peak_pct REAL, sustained_evaluable INT)")
    db.execute("CREATE TABLE raw_price_bars_1m (token_ca TEXT, timestamp INT, close REAL)")
    # Plant a real F3 relationship. Tier is seeded INDEPENDENTLY (every 3rd signal is
    # gold) so the [t-3h,t-2h) maturation window has genuine variance; then a signal's
    # PEAK is made to increase with the count of gold that matured in that legal window.
    # This decouples the label (tier, seeded) from the feature target (peak, driven by F3),
    # so recovering the F3->peak IC is a real test, not circular.
    base_t = 1_000_000
    spacing = 600
    rows = []
    for k in range(600):
        t = base_t + k * spacing
        tier = "gold" if k % 3 == 0 else "sub25"          # independent label seed
        lo = t - 3 * 3600; hi = t - MATURE_HORIZON_SEC     # legal matured window
        recent_gold = sum(1 for j in range(k)
                          if lo <= base_t + j * spacing < hi and (j % 3 == 0))
        peak = 10.0 + 25.0 * recent_gold                   # peak rises with matured-gold density
        rows.append((t, f"tok{k}", tier, "NEW_TRENDING", peak, 1))
    db.executemany("INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?)", rows)
    # A future bar that must NEVER be read (would leak): place a wild price AFTER each signal.
    for k in range(600):
        t = base_t + k * spacing
        db.execute("INSERT INTO raw_price_bars_1m VALUES (?,?,?)", (f"tok{k}", t + 5000, 9e9))
    db.commit(); db.close()

    r = run(path, holdout_frac=0.6)
    assert r["n_signals"] == 600, r["n_signals"]
    # (b) planted F3 signal recovered in-sample
    f3 = r["features"]["F3_matured_gs_density"]
    assert (f3["ic_full"] or 0) > 0.1, f"planted F3 signal not recovered: {f3}"
    # (a) time legality: F4 must be all-None here (only future bars exist, none <= t within window)
    f4 = r["features"]["F4_cohort_market_factor"]
    assert f4["coverage"] == 0.0, f"time-legality breach: future bars were read (F4 cov={f4['coverage']})"
    print("SELF-TEST PASSED: planted signal recovered (F3 IC=%.3f), no future-bar leakage (F4 cov=0)"
          % f3["ic_full"])
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="/app/data/raw_signal_outcomes.db")
    ap.add_argument("--out")
    ap.add_argument("--holdout-frac", type=float, default=0.6)
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args()
    if a.self_test:
        return 0 if self_test() else 1
    report = run(a.db, a.holdout_frac)
    text = json.dumps(report, indent=2)
    if a.out:
        with open(a.out, "w") as fh:
            fh.write(text)
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())

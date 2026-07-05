#!/usr/bin/env python3
"""Narrative-sibling (same-symbol edge) IC study — N2 (read-only research).

This is the test of the CryptoGAT paper's actual thesis (heterogeneous PAIRWISE
edges beat a global market factor), applied at meme scale. Part 1
(cohort_simultaneity_ic_study.py) tested global cohort scalars and found no
stable signal — consistent with the paper's own negative control (a market
factor added to StockMixer scored far below GAT). This script tests the
meme-native edge type observable at a newborn token's birth: same/similar
SYMBOL as a prior signal ("copycat wave" / relaunch). It was blocked until N1
fixed the symbol parser (was 30.8%/70% UNKNOWN) and backfilled history.

Time legality: a sibling's OUTCOME (peak/tier) is only used if that sibling's
label has legally matured by t (signal_ts + horizon_sec <= t). Sibling PRESENCE
(just counting recent same-symbol signals, ignoring their outcome) does not
require maturation and is legal at any lag.

Pre-registered pass criterion (see claudedocs/cohort-simultaneity-study-
2026-07-05.md, Part 2 addendum — do not edit after seeing results):
  sibling features must beat a matched random placebo (same lookback window,
  same sample size, non-sibling tokens) AND reach |IC| >= 0.03 in-sample with a
  95% CI excluding 0, AND hold sign under a temporal 60/40 holdout.

Usage:
  python3 narrative_sibling_ic_study.py --db /app/data/raw_signal_outcomes.db --out report.json
  python3 narrative_sibling_ic_study.py --self-test
"""
from __future__ import annotations
import argparse, bisect, json, math, random, re, sqlite3, sys, tempfile, os

MATURE_HORIZON_SEC = 7200
SIBLING_LOOKBACK_SEC = 86400
MIN_IC = 0.03
PLACEBO_SEED = 20260705  # fixed seed: reproducible, not tuned post-hoc


def is_gs(tier): return tier in ("gold", "silver")


def norm_symbol(s):
    # 'unknown' must NOT be treated as a real symbol family: two unparseable
    # rows are not narrative siblings just because both failed to parse. This
    # is the exact bug class N1 fixed in the live parser — guard against a
    # residual instance of it contaminating the sibling-edge measurement.
    v = re.sub(r"[^a-z0-9]", "", (s or "").lower())
    return "" if v == "unknown" else v


def load_signals(db):
    rows = db.execute(
        "SELECT signal_ts, token_ca, symbol, raw_sustained_tier, max_sustained_peak_pct "
        "FROM raw_signal_outcomes WHERE sustained_evaluable=1 AND signal_ts IS NOT NULL "
        "AND max_sustained_peak_pct IS NOT NULL ORDER BY signal_ts"
    ).fetchall()
    return [(int(t), ca, norm_symbol(sym), tier, float(pk)) for t, ca, sym, tier, pk in rows]


def rankvec(vals):
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


def spearman(xs, ys):
    pairs = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None]
    m = len(pairs)
    if m < 80:
        return None, m
    xr = rankvec([p[0] for p in pairs]); yr = rankvec([p[1] for p in pairs])
    mx = sum(xr) / m; my = sum(yr) / m
    num = sum((a - mx) * (b - my) for a, b in zip(xr, yr))
    dx = math.sqrt(sum((a - mx) ** 2 for a in xr)); dy = math.sqrt(sum((b - my) ** 2 for b in yr))
    return (num / (dx * dy) if dx * dy else 0.0), m


def build_features(sig, rng):
    """S1 = distinct sibling tokens in trailing 24h (presence, always legal).
    S2 = best MATURED sibling peak (outcome-based, maturity-gated).
    S3 = 1 if any matured sibling was gold/silver, else 0 (only defined when >=1 matured sibling exists).
    P2/P3 = placebo versions of S2/S3 using a random non-sibling token set of the
    SAME size, drawn from the SAME lookback window, to control for "any recent
    cohort activity predicts peak" rather than "same-symbol activity specifically"."""
    n = len(sig)
    ts = [s[0] for s in sig]
    from collections import defaultdict
    sym_positions = defaultdict(list)
    for i, s in enumerate(sig):
        if s[2]:
            sym_positions[s[2]].append(i)

    S1 = [0] * n; S2 = [None] * n; S3 = [None] * n
    P2 = [None] * n; P3 = [None] * n
    for i in range(n):
        t, ca, sym, tier, pk = sig[i]
        if not sym:
            continue
        lo = bisect.bisect_left(ts, t - SIBLING_LOOKBACK_SEC)
        cand = [j for j in sym_positions[sym] if lo <= j < i and sig[j][1] != ca]
        if not cand:
            continue
        distinct_tokens = {}
        for j in cand:
            distinct_tokens[sig[j][1]] = j  # keep latest signal per sibling token
        S1[i] = len(distinct_tokens)
        matured = [j for j in distinct_tokens.values() if sig[j][0] <= t - MATURE_HORIZON_SEC]
        if matured:
            S2[i] = max(sig[j][4] for j in matured)
            S3[i] = int(any(is_gs(sig[j][3]) for j in matured))

            # Placebo: same count of matured siblings, but drawn at random from
            # the same [t-24h, t) window, excluding this symbol, matured the same way.
            hi_all = bisect.bisect_left(ts, t)
            pool = [j for j in range(lo, hi_all) if sig[j][1] != ca and sig[j][2] != sym
                    and sig[j][0] <= t - MATURE_HORIZON_SEC]
            if pool:
                k = min(len(pool), len(matured))
                pick = rng.sample(pool, k)
                P2[i] = max(sig[j][4] for j in pick)
                P3[i] = int(any(is_gs(sig[j][3]) for j in pick))
    return {"S1_sibling_count": S1, "S2_sibling_best_peak": S2, "S3_sibling_any_gold": S3,
            "P2_placebo_best_peak": P2, "P3_placebo_any_gold": P3}


def holdout(feat, pk, ts, split_frac=0.6):
    n = len(pk)
    split = ts[int(n * split_frac)]
    ic_all, n_all = spearman(feat, pk)
    tr = [(feat[i], pk[i]) for i in range(n) if ts[i] < split]
    te = [(feat[i], pk[i]) for i in range(n) if ts[i] >= split]
    ic_tr, n_tr = spearman([x[0] for x in tr], [x[1] for x in tr])
    ic_te, n_te = spearman([x[0] for x in te], [x[1] for x in te])
    ci_half = 1.96 / math.sqrt(n_all) if (n_all and n_all >= 80) else None
    return {
        "ic_full": round(ic_all, 4) if ic_all is not None else None, "n_full": n_all,
        "ci95_full": ([round(ic_all - ci_half, 4), round(ic_all + ci_half, 4)]
                      if (ic_all is not None and ci_half is not None) else None),
        "ic_train": round(ic_tr, 4) if ic_tr is not None else None, "n_train": n_tr,
        "ic_test": round(ic_te, 4) if ic_te is not None else None, "n_test": n_te,
        "holdout_same_sign": bool(ic_tr is not None and ic_te is not None and ic_tr * ic_te > 0),
    }


def run(db_path, holdout_frac=0.6, seed=PLACEBO_SEED):
    db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    sig = load_signals(db)
    n = len(sig)
    ts = [s[0] for s in sig]
    pk = [s[4] for s in sig]
    base_gs = sum(1 for s in sig if is_gs(s[3])) / n if n else 0.0
    rng = random.Random(seed)
    feats = build_features(sig, rng)

    coverage_sibling = sum(1 for v in feats["S2_sibling_best_peak"] if v is not None)
    unique_symbols = len({s[2] for s in sig if s[2]})
    multi_families = 0
    from collections import defaultdict
    fam_tokens = defaultdict(set)
    for s in sig:
        if s[2]:
            fam_tokens[s[2]].add(s[1])
    multi_families = sum(1 for toks in fam_tokens.values() if len(toks) >= 2)

    results = {}
    for name in ("S1_sibling_count", "S2_sibling_best_peak", "S3_sibling_any_gold",
                 "P2_placebo_best_peak", "P3_placebo_any_gold"):
        results[name] = holdout(feats[name], pk, ts, holdout_frac)
        results[name]["coverage"] = round(
            sum(1 for v in feats[name] if v is not None) / n, 4) if n else 0.0

    def beats_placebo(sib_key, placebo_key):
        a = results[sib_key]["ic_full"]; b = results[placebo_key]["ic_full"]
        if a is None or b is None:
            return None
        return abs(a) > abs(b)

    verdicts = {}
    for sib_key, placebo_key in (("S2_sibling_best_peak", "P2_placebo_best_peak"),
                                   ("S3_sibling_any_gold", "P3_placebo_any_gold")):
        r = results[sib_key]
        ic = r["ic_full"]
        beats = beats_placebo(sib_key, placebo_key)
        ci_excludes_0 = bool(r["ci95_full"] and (r["ci95_full"][0] > 0 or r["ci95_full"][1] < 0))
        passes = bool(
            ic is not None and abs(ic) >= MIN_IC and ci_excludes_0
            and beats and r["holdout_same_sign"]
        )
        verdicts[sib_key] = {
            "beats_placebo": beats, "ic_ge_min": bool(ic is not None and abs(ic) >= MIN_IC),
            "ci_excludes_0": ci_excludes_0, "holdout_same_sign": r["holdout_same_sign"],
            "PASSES_PREREGISTERED_CRITERION": passes,
        }

    any_pass = any(v["PASSES_PREREGISTERED_CRITERION"] for v in verdicts.values())
    return {
        "schema_version": "narrative_sibling_ic_study.v1",
        "allowed_use": "shadow_only_discovery_evidence",
        "promotion_allowed": False,
        "n_signals": n,
        "base_gold_silver_rate": round(base_gs, 4),
        "unique_normalized_symbols": unique_symbols,
        "multi_token_symbol_families": multi_families,
        "sibling_coverage_rate": round(coverage_sibling / n, 4) if n else 0.0,
        "placebo_seed": seed,
        "features": results,
        "preregistered_verdicts": verdicts,
        "verdict": "NARRATIVE_SIBLING_SIGNAL_CONFIRMED" if any_pass else "NO_NARRATIVE_SIBLING_SIGNAL",
    }


def self_test():
    tmp = tempfile.mkdtemp()
    path = os.path.join(tmp, "t.db")
    base_t = 1_000_000
    spacing = 300
    families = ["trump", "doge", "pepe", "wif", "bonk"]
    rng = random.Random(1)

    # Generate rows using a per-family BOUNDED "hot/cold regime" that persists
    # for a block of signals then flips. peak = base_noise + regime_boost.
    # Crucially, the regime does NOT depend on prior peaks (no feedback loop,
    # no compounding global drift) — it is an exogenous, bounded, family-local
    # process. A matured sibling's peak legally reflects the family's regime AT
    # THE TIME IT SIGNALED, which is autocorrelated with (but not identical to)
    # the current regime, giving a genuine, bounded, family-specific signal.
    # Because regimes are independent across the 5 families, a placebo drawn
    # from the full cross-family pool dilutes toward the population average and
    # should show materially weaker correlation than the true sibling feature.
    regime = {f: 0 for f in families}
    block_countdown = {f: rng.randint(15, 30) for f in families}
    rows = []  # (t, token_ca, symbol, tier, peak)
    for k in range(2000):
        t = base_t + k * spacing
        fam = families[k % len(families)]
        block_countdown[fam] -= 1
        if block_countdown[fam] <= 0:
            regime[fam] = 1 - regime[fam]
            block_countdown[fam] = rng.randint(15, 30)
        base_peak = rng.uniform(5, 25)
        peak = base_peak + (55.0 if regime[fam] else 0.0) + rng.uniform(-5, 5)
        tier = "gold" if peak >= 100 else ("silver" if peak >= 50 else "sub25")
        rows.append((t, f"tok{k}", fam, tier, peak))

    db = sqlite3.connect(path)
    db.execute("CREATE TABLE raw_signal_outcomes (signal_ts INT, token_ca TEXT, symbol TEXT, "
               "raw_sustained_tier TEXT, max_sustained_peak_pct REAL, sustained_evaluable INT)")
    db.executemany("INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,1)", rows)
    db.commit(); db.close()

    r = run(path, holdout_frac=0.6)
    assert r["n_signals"] == 2000
    assert r["multi_token_symbol_families"] == 5, r["multi_token_symbol_families"]
    s2 = r["features"]["S2_sibling_best_peak"]
    # Bounded per-family regime (15-30 signal blocks) gives a real but modest,
    # noisy effect — not the near-1.0 IC a runaway/feedback plant would give.
    assert s2["ic_full"] is not None and s2["ic_full"] > MIN_IC, f"planted sibling signal not recovered: {s2}"
    assert s2["ci95_full"] and s2["ci95_full"][0] > 0, f"CI does not exclude 0: {s2}"
    assert s2["holdout_same_sign"], f"holdout sign flipped: {s2}"
    # Placebo (random cross-family tokens) must be measurably weaker, since the
    # planted effect is family-specific persistence, not generic cohort activity.
    p2 = r["features"]["P2_placebo_best_peak"]
    assert abs(s2["ic_full"]) > abs(p2["ic_full"] or 0) * 1.3, (
        f"placebo too close to real signal — placebo construction may be leaking the effect: "
        f"sib={s2['ic_full']} placebo={p2['ic_full']}")
    v = r["preregistered_verdicts"]["S2_sibling_best_peak"]
    assert v["PASSES_PREREGISTERED_CRITERION"], v
    print("SELF-TEST PASSED: planted symbol-specific signal recovered "
          f"(S2 IC={s2['ic_full']:.3f} vs placebo IC={p2['ic_full']:.3f}), "
          "pre-registered criterion correctly fires on a real effect")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="/app/data/raw_signal_outcomes.db")
    ap.add_argument("--out")
    ap.add_argument("--holdout-frac", type=float, default=0.6)
    ap.add_argument("--seed", type=int, default=PLACEBO_SEED)
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args()
    if a.self_test:
        return 0 if self_test() else 1
    report = run(a.db, a.holdout_frac, a.seed)
    text = json.dumps(report, indent=2)
    if a.out:
        with open(a.out, "w") as fh:
            fh.write(text)
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())

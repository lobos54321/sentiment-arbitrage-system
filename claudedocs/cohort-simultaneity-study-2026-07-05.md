# Cohort / Market-Simultaneity Study — 2026-07-05

**Question:** The CryptoGAT paper (Sydney Uni + Brunel) shows crypto price prediction is a
cross-asset *graph* problem, not a *time-series* problem (temporal AR R²<0.1; cross-asset
corr 0.528; PC1 explains 55% variance). Does that thesis transfer to our problem — does the
**simultaneous state of the meme cohort at signal time** predict whether a newborn token becomes
a gold/silver dog, independent of its own history?

**Answer: No stable signal in this dataset.** Two cohort features match the paper's IC magnitude
in-sample but decay ~75% out of sample and fail a temporal holdout. This was tested empirically
in ~1 hour on local snapshot data — the exact gate Phase 3 C4 would apply — instead of building a
multi-week forward-OOS apparatus to rediscover a null.

Method, tooling, and self-test: `scripts/research/cohort_simultaneity_ic_study.py`
(`--self-test` asserts time-legality + planted-signal recovery). Re-runnable on the full
production DB, which is larger/fresher and may differ.

## Data

Local snapshot `raw_signal_outcomes.db` (2026-06-06 → 06-30, 24 days): 9,090 evaluable signals,
base gold/silver rate 22.46% (gold 1,245 + silver 797). Price paths from `raw_price_bars_1m`
(279,509 bars). All features computed **time-legally**: label maturation offset (2h) enforced for
"recent dog" features; only bars with `timestamp ≤ signal_ts` read for price features (asserted
by self-test — a planted future bar is never read, F4 coverage stays 0 in the synthetic case).

## Result (IC = Spearman vs sustained peak %, the paper's metric; paper CryptoGAT IC = 0.047)

| Feature | IC full | IC train (60%) | IC test (40%) | Holdout |
|---|---|---|---|---|
| F1 signal density (30m, distinct tokens) | +0.011 | +0.002 | +0.053 | ✗ (train ≈ 0; test blip is noise) |
| **F3 matured gold/silver density** (t−3h…t−2h) | **+0.048** | +0.070 | +0.018 | ✗ (decays 74%) |
| **F4 cohort market factor** (cohort median 15m return) | **−0.047** | −0.065 | −0.016 | ✗ (decays 76%) |
| F8 cohort breadth (% cohort green) | −0.014 | −0.023 | −0.003 | ✗ |

Binary bucketing P(gs \| feature bucket) is even flatter — every bucket of every feature sits
0.19–0.27 around the 0.2246 base (that view *hid* the IC structure, which is why IC is the right
metric here).

## Interpretation

1. **Signal-count features (F1) are noise.** "How many tokens got signaled recently" carries
   nothing (train IC ≈ 0).
2. **The two price/maturation features (F3, F4) are economically sensible and match the paper's
   magnitude in-sample, but do not persist.** F3 positive = "meta is alive, recent dogs matured →
   next one peaks higher"; F4 negative = "buying into an already-ripping cohort → worse outcome"
   (consistent with the existing `chasing_top` gate intuition). Both ~0.048 full-sample; both
   collapse to ~0.016–0.018 out of sample.
3. **Why it differs from the paper.** CryptoGAT predicts *returns of 66 established coins* with
   history and stable cross-correlation, ranked daily into a portfolio (IC 0.047 → Sharpe 3.9 via
   diversified ranking). Our target is a *binary lottery outcome on a single newborn meme*, where
   the 2h forward extreme is dominated by token-specific virality, not market beta. An IC of 0.048
   that does not hold out of sample is a transient-meta artifact, not an edge.

## Decision

- **Do NOT build the full Phase 3 C1–C3 pipeline as a priority.** The cheap in-sample + holdout
  test — the same gate C4 would apply after weeks of forward-data accumulation — already fails.
  Building the forward-OOS apparatus to rediscover a null is low-value.
- **Keep one narrow, low-priority hook (only behind Phase 2):** F3 (+) and F4 (−) are the only
  features worth registering as *context dimensions* in the existing 2D-cross machinery — NOT as
  standalone filters, but because a marginal main effect can still condition a candidate×cohort
  interaction that the full FDR/OOS pipeline could surface where a main effect can't. Frame as a
  hypothesis, not an edge. F1/F8 are dropped.
- **Re-run on production data before fully closing.** This is 24 days / one meta-period with 79.6%
  F4 coverage. `cohort_simultaneity_ic_study.py --db /app/data/raw_signal_outcomes.db` re-tests on
  the larger, fresher production DB; if F3/F4 hold out there, revisit. If they fail there too, mark
  the direction closed.

## Meta-lesson

The CryptoGAT read paid off not by giving us a model to build, but by giving us a *specific,
cheap-to-test hypothesis* — and the discipline (IC + temporal holdout) to kill it in an hour. A
negative result that costs one hour and saves weeks of build is a high-value outcome. Recorded so
no future session re-proposes "cohort/graph features" without first re-running this script.

---

## Addendum (2026-07-05, same day): power analysis softens the verdict

Three user objections were tested; the first materially changes the wording of the conclusion.

**1. "Sample too small" — CORRECT.** The holdout test-set CIs contain BOTH 0 and the in-sample
value: F3 test IC +0.018, 95% CI [−0.016, +0.053]; F4 test IC −0.016, CI [−0.054, +0.022]. The
holdout therefore CANNOT distinguish "signal died" from "signal persists at ~0.048". Power math:
detecting |IC| = 0.03 at 80% power needs ~8.7k test signals ≈ 22k total ≈ **65 days** at the
current ~335 evaluable signals/day. We have 24 days. Weekly ICs: **F3 is positive in 4/4 weeks**
(+0.039 / +0.088 / +0.010 / +0.041 — sign-consistent, magnitude unstable; 4/4 same-sign has
p ≈ 1/16 under a coin-flip null). F4 flips sign in week 1 (+0.031) then goes negative ×3 —
weaker consistency.

**Revised per-feature verdicts:** F3 = *directionally persistent but underpowered* (upgraded
from "fails holdout"); F4 = weak/inconsistent; F1/F8 = noise (unchanged). Overall verdict
becomes **UNDERPOWERED_UNPROVEN (F3 promising)** rather than "no signal". The *decision* —
do not build the full C1–C3 pipeline now — is unchanged, but the reason is now "underpowered,
wait for data" rather than "disproven".

**2. "Features may not match meme dynamics" — PARTIALLY CORRECT.** Only 4 of 8 planned features
were testable from the snapshot. Untested: F6 launch-flow (needs P8 stream), F7 narrative
resonance (needs name/theme fields + clustering), and the C3 graph-lite lead-lag features.
These remain open hypotheses, not covered by this study's verdict.

**3. "Telegram source selection bias" — STRONGEST OBJECTION, untestable with current data.**
The cohort measured here is *channel throughput* (signals that already passed a curator's
filter), not *market state*. If the curator's filter itself conditions on market heat, the
observable variance of true market state is range-restricted → measured IC is attenuated
toward 0. This also explains why F1 (signal density) is pure noise — it measures the curator's
posting rhythm. CryptoGAT's graph was built on the whole market; our analog of "whole market"
is exactly the **P8 pump.fun stream** (unfiltered, market-wide), which started accumulating
2026-07-04.

## Re-test triggers (pre-registered)

1. **T+~40 days**: production `raw_signal_outcomes` reaches ≥22k evaluable signals → rerun
   `cohort_simultaneity_ic_study.py --db /app/data/raw_signal_outcomes.db`. Pass = F3 holds
   sign with |IC| ≥ 0.03 and CI excluding 0 in the held-out 40%.
2. **P8 + 2–4 weeks**: recompute F1/F4/F6 from the *market-wide* pump.fun stream (unfiltered
   cohort) and rerun — this is the only way to address objection 3.
3. **When narrative fields land** (motion trace / premium_signals raw_message clustering):
   test F7 theme-resonance.

Until a trigger fires, F3 (+) may be registered as a low-priority context dimension in the
2D-cross machinery (hypothesis-grade, shadow-only); everything else waits.

---

## Part 2 (2026-07-05): the paper's REAL essence — heterogeneous pairwise edges — and what blocks testing it

**Correction of scope.** Part 1 tested global cohort scalars (density, median momentum). The
CryptoGAT paper *itself* shows that version is insufficient (MF-StockMixer with a market factor:
Sharpe 0.963 vs GAT 3.128). The paper's alpha lives in **heterogeneous pairwise relations** —
which specific neighbors matter for this specific asset. Part 1's null therefore does NOT test
the essence; it replicates the paper's own negative control.

**Meme-scale analog of the graph.** Newborns have no price history, so edges cannot be price
correlation. The observable-at-birth edge types are:
1. **Narrative/copycat**: same/similar name or theme (TRUMP-family, dog-family, relaunch waves).
2. **Creator lineage**: same deployer wallet / funding source (Helius data partially present).
3. **Early-holder overlap**: shared first-N-minute buyers.
4. **Launch-cohort competition**: same time-bucket launches competing for attention (possibly
   negative edges).

**Exact-symbol sibling test (attempted today) — blocked by a data bug.**
`raw_signal_outcomes.symbol` is 'UNKNOWN' for **70%** of production rows (9,576/13,683);
`premium_signals.symbol` is UNKNOWN/null for 30.8% (15,527/50,394). Root cause: the symbol
parser fails on the message format `SYMBOL：$xxx` (**full-width colon** `：`), even though the
symbol appears verbatim in `raw_message` (also as the bold `**name**` header). `raw_message` is
retained for 63.7% of premium_signals → **retroactive backfill is possible**. After excluding
the UNKNOWN blob, real same-symbol families cover only ~460 snapshot signals — hopelessly
underpowered until the parser is fixed.

## Task card: N-series (narrative graph, INSTRUMENTATION-first)

- **N1 (cheap, do first): symbol extraction fix + backfill.** Parse `SYMBOL：$x` /
  `SYMBOL:$x` / bold-header fallback from raw_message; backfill `premium_signals.symbol` and
  propagate to `raw_signal_outcomes.symbol` (audit field `symbol_source=backfill_v2`).
  Acceptance: UNKNOWN rate < 10% on rows with raw_message; zero changes to gates/strategy.
- **N2: rerun the sibling test** (`cohort_simultaneity_ic_study.py` extended with sibling
  features) on backfilled data. Pre-registered pass: sibling features beat the matched random
  placebo with |IC| ≥ 0.03 and CI excluding 0, temporal holdout same-sign.
- **N3 (conditional on N2): narrative families properly** — name normalization + theme
  clustering over raw_message; creator-wallet edges as a second edge type; register as context
  dimensions in the 2D-cross machinery (hypothesis-grade, shadow-only, FDR-gated).
- **S2 later: graph-conditional exits** — theme-siblings collapsing as an exit trigger variant
  for the P7 lab.

The paper's claim 1 (own-history carries no signal) is already absorbed: Kronos AUC 0.36–0.41 +
this study. Claim 3 (pairwise edges) remains OPEN — testable the day N1 lands.

---

## Part 3 (2026-07-05): N1+N2 executed — narrative-sibling edges tested, no signal, but a structural finding

**N1 (symbol parser fix + backfill) — DONE, verified in production.**
- Live parser fix (`src/inputs/premium-channel-listener.js`) merged via PR #55 (commit `2436b98`),
  deployed, confirmed working on fresh live signals (`symbol_source=NULL` rows post-deploy show
  correctly extracted symbols, e.g. 'SOLBULL', '丸さん', 'BullWorld' — direct proof the fix works
  at ingestion, not just in backfill).
- Historical backfill executed on production: `premium_signals` UNKNOWN 30.8% → **0.1%**
  (15,500 rows recovered, audit `symbol_source=backfill_v2`); `raw_signal_outcomes` UNKNOWN 70% →
  **0.0%** (9,598 rows propagated). Residual 49 premium rows (no `raw_message` at all — genuinely
  unrecoverable) and 1 outcomes row (a live-write race during the backfill window, self-resolving
  on next run). Live writers confirmed healthy post-schema-change (newest row 42.8s old at check
  time). Idempotent design meant the mid-run gateway timeout (524) was safe — the script completed
  server-side despite the client connection dropping; verified via before/after row counts and the
  `symbol_source` audit trail rather than trusting the truncated response.

**N2 (narrative-sibling IC test) — DONE, pre-registered criterion NOT met.**
Script: `scripts/research/narrative_sibling_ic_study.py` (self-tested: recovers a planted
bounded, family-specific effect at realistic magnitude — IC 0.06 vs placebo 0.03 — and correctly
returns `NO_...SIGNAL` on pure-noise data; also guards against re-treating `'unknown'` as a valid
symbol family, the exact bug class N1 fixed, in case residual unparseable rows linger).

Run on production `raw_signal_outcomes.db` (n=9,929 evaluable, post-backfill):

| Feature | IC full | CI95 | Train IC | Test IC | Holdout | Passes? |
|---|---|---|---|---|---|---|
| S1 sibling presence (any same-symbol signal, 24h) | +0.016 | [−0.003, +0.036] | +0.020 | +0.014 | same sign | n/a (below MIN_IC) |
| **S2 best matured-sibling peak** | −0.033 | [−0.100, +0.034] | **+0.063** | **−0.089** | **sign flips** | **NO** |
| S3 any matured sibling gold/silver | +0.009 | [−0.058, +0.076] | +0.037 | +0.006 | same sign | NO (CI includes 0, below MIN_IC) |
| P2 placebo (matched random, best peak) | +0.013 | [−0.054, +0.080] | +0.065 | −0.026 | flips | — |
| P3 placebo (matched random, any gold) | −0.007 | [−0.074, +0.061] | +0.053 | −0.051 | flips | — |

**Verdict: `NO_NARRATIVE_SIBLING_SIGNAL`.** S2's train/test sign flip (+0.063 → −0.089) is the
clearest tell — this is noise being fit, not a real effect, and it fails 3 of 4 pre-registered
conditions. S1 (mere posting-rhythm presence) replicates Part 1's F1 finding: noise.

**The structural finding that matters more than the null result:** `sibling_coverage_rate` = only
**8.6%** (855/9,929) — i.e. 91.4% of signals have ZERO same-symbol sibling old enough (≥2h) to have
a known outcome. Root cause quantified directly: of the 18.1% of signals that DO have some
same-symbol sibling in the trailing 24h, **47.3% of those siblings arrived within 1 hour** of each
other (median gap to most recent sibling: 4,576s ≈ 76 min) — copycat/relaunch waves are *bursty*,
not spaced out. By the time enough of a burst exists to test "did the sibling do well," the
2-hour label-maturation requirement has usually not been met by *any* member of the burst yet.
**This is a coverage/timing problem, not necessarily a null-effect problem** — the test as designed
cannot see most of the phenomenon it's trying to measure.

**Two structurally sound but NOT yet tested alternatives this points to** (both deferred, not
started — this round's target was N1+N2 exactly as scoped):
1. **Within-burst peer signal** instead of matured-outcome signal: e.g. "how many other same-symbol
   tokens were ALSO just signaled in the last 10 minutes" (a presence/intensity measure, time-legal
   without maturation) — this is closer to what actually happens in a copycat wave and doesn't
   require waiting 2h for an outcome that the burst dynamic itself prevents.
2. **Interim/partial sibling performance** (e.g. sibling's price move in ITS first 15–30 minutes,
   available in well under 2h) instead of requiring the full peak/tier label — trades label
   completeness for coverage, worth testing once P8/motion-trace data matures.

Neither is being built now; they are recorded here so a future session doesn't have to
re-derive "why did sibling coverage die" from scratch.

## Overall status of the CryptoGAT-inspired investigation

| Layer | Test | Verdict |
|---|---|---|
| ① Own-history time-series | Kronos (external) + literature | No signal (independently corroborated) |
| ② Global market factor | Part 1 (F1/F3/F4/F8) | Underpowered/unproven, not disproven; 3 re-test triggers pre-registered |
| ③ Heterogeneous pairwise edges (narrative/copycat) | Part 3 (N1+N2, this section) | No signal on the *matured-outcome* sibling construction; likely a coverage artifact of burst timing, not a clean null on the underlying hypothesis |

No cohort/narrative dimension is being added to the discovery mesh at this time. This document,
`scripts/research/cohort_simultaneity_ic_study.py`, and `scripts/research/narrative_sibling_ic_study.py`
are the reusable, re-runnable record — rerun either against a fresher/larger production snapshot
before re-opening this investigation, and read this section first.

---

## Part 4 (2026-07-05): following the paper's essence to its correct conclusion — a strong, stable signal

The user pushed back that the paper's value was not yet extracted. Correct. Parts 1–3 tested
CROSS-TOKEN structure (cohort, narrative siblings) — all weak. But the paper's actual essence is
more general: **predict from CONTEMPORANEOUS structure, not from stale own-history.** Following
that to its correct conclusion for memes produced the strongest, most stable signal in the entire
investigation — and it is NOT a cross-asset feature.

### Step 1 — the dominant structure in memes is a REGIME (common factor), and it is real

The paper's PC1 (55% of variance) says a strong common factor dominates. The meme analog: dog
production is strongly TIME-CLUSTERED, not i.i.d.
- Gold/silver rate per 2h bucket: mean 0.222, **std 0.112, range 0.000–0.778** (hot vs cold periods).
- Dog-rate autocorrelation: **lag-1 (2h) r=+0.18, lag-2 (4h) r=+0.15**, plus a lag-12 (24h) r=+0.11
  daily-seasonality bump. A hot 2h genuinely predicts the next being hot — 3–4× stronger than any
  per-token cohort feature (all ~0.05).

### Step 2 — but the 2h label-maturation lag blindfolds us, and that is the real problem

The 0.18 autocorrelation largely leaks contemporaneous info (bucket b's rate matures 2h later,
after bucket b+1 has begun). Made strictly time-legal:
- Regime signal from recently-MATURED outcomes → forward dog rate: corr +0.059, **decays to +0.015
  out of sample** (the recurring ~75% decay).
- Live cohort PRICE momentum (F4-style) → forward dog rate: corr **−0.008 (zero)**.

Interpretation: the meme problem is structurally harder than the paper's because established coins
trade continuously with ZERO outcome lag, while we wear a **2-hour blindfold** — by the time an
outcome-based signal says "hot," the regime has moved. Shrinking that blindfold is the highest-
leverage move, not adding another cohort feature.

### Step 3 — the unlock: a token's own EARLY trajectory predicts its LATER trajectory, strongly and stably

| Predictor (time-legal, low-lag) | IC full | IC train | IC test | tercile lift | note |
|---|---|---|---|---|---|
| peak_5m_pct → gold/silver | +0.242 | +0.243 | +0.245 | 2.9× | stable |
| **peak_15m_pct → gold/silver** | **+0.405** | +0.414 | +0.397 | **6.5×** (7.4%→47.7%) | stable, no decay |

De-circularity (15-min peak overlaps the 2h label window, so restrict to tokens whose SUSTAINED
peak came AFTER 15min — the early peak is then NOT the defining peak):
- peak_15m → gold/silver among late-peakers: **IC +0.55**, top-tercile 99.7% dog rate.
- peak_15m → pure LATE peak magnitude (60/120m, zero overlap): **IC +0.64**.

This is genuine momentum persistence, not mechanical overlap. It is an order of magnitude stronger
than the paper's 0.047 and — unlike every cohort/narrative feature — it HOLDS out of sample.
(38.4% of dogs peak within 15min, 56.5% within 30min; median time-to-sustained-peak 1,347s.)

### The extraction (what the paper's value actually is, for this system)

1. **The strongest available signal is early-trajectory momentum (peak_15m), not cross-asset
   structure.** The paper's "contemporaneous not historical" thesis is right; the winning
   contemporaneous variable is the token's own first-15-min path, which is time-legal at 15-min lag.
2. **It shrinks the 2h blindfold to 15min, which is what unlocks the regime.** Aggregate the
   15-min-peak signal across recently-signaled tokens → a near-real-time "dog-market heat" index
   (15-min lag instead of 2h). That index is the operationally useful form of the paper's common
   factor.
3. **Two shadow-only, governance-safe uses** (both must go through discovery→freeze→forward-OOS→
   FDR before any promotion; entry-threshold changes remain forbidden strategy changes):
   - **Ranking/priority dimension**: with a 6.5× precision lift, ranking detected signals by early
     trajectory would far exceed the current 84 candidates' ~6.4% precision. Register as a
     discovery dimension, not an entry gate.
   - **Regime throttle**: the aggregate heat index paces capture — lean in when hot (serves 60%
     capture), pull back when cold (serves the 15% drawdown constraint). It is a pacing signal,
     not a per-token entry, so it sidesteps the chasing-risk tension that direct peak_15m entry
     would create.

### Honest caveats

- Early-momentum-persistence is well known in meme trading; the system's smart_entry/kline gates
  likely already exploit part of it. The value added here is (a) CLEANLY QUANTIFYING it as the
  strongest, most stable signal available (IC 0.4–0.64 vs everything else ~0.05), (b) the REGIME
  connection (aggregate → real-time heat throttle), which is operationally novel, and (c) the
  recommendation to use it as a ranking DIMENSION through the existing OOS/FDR machinery.
- Using peak_15m for ENTRY means acting at signal+15min, after the token shows its hand — a
  chasing tension. High value for ranking/throttle, moderate for entry timing; the P7 lab and
  entry logic would have to quantify the remaining-upside capture. Not a strategy change here.
- The conditioning base rate in the de-circularity subset is elevated (0.69, selection effect);
  the WITHIN-subset IC (0.55) and the pure-forward peak_15m→late-peak (0.64) are the non-circular
  numbers.

### Revised investigation scoreboard

| Layer | Verdict |
|---|---|
| ① own-history time series | no signal (Kronos) |
| ② cross-token cohort scalars | underpowered/unproven (~0.05, decays) |
| ③ cross-token narrative edges | no signal (burst-timing coverage artifact) |
| **④ own early-trajectory (peak_15m)** | **STRONG & STABLE (IC 0.4–0.64), the real extraction** |
| ⑤ dog-rate regime | real (0.18 autocorr) but 2h-blindfolded; exploitable only via ④'s early detector |

Reusable scripts: `cohort_simultaneity_ic_study.py`, `narrative_sibling_ic_study.py`. The Part 4
regime + early-trajectory analyses are inline production queries (recorded here); if pursued, the
next step is a proper `early_trajectory_regime_study.py` with self-test + FDR, and a shadow
regime-heat artifact — NOT an entry change.

---

## Part 5 (2026-07-05): the decisive tradeability test — strong classifier, NO tradeable edge

Part 4 found peak_15m is a strong, stable CLASSIFIER (IC 0.4–0.64). Before recommending any build,
the decisive operational question: **is it a tradeable EDGE, or just a description that arrives too
late to act on?** Two tests settle it.

### Test A — entry at signal+15min, forward upside by peak_15m tercile (n=7,739)

| peak_15m tercile | dog rate | median forward return from a +15min entry | frac with ≥50% remaining upside |
|---|---|---|---|
| LOW | 11.8% | +19.8% | 33.6% |
| MID | 19.6% | +22.3% | 36.4% |
| HIGH | **57.1%** | **+20.9%** | 36.7% |

High-peak_15m tokens are 5× more likely to be dogs, but the **forward upside from the +15min entry
point is flat (~20%) across all terciles.** You've correctly identified the dog, but by +15min the
move is already partly behind you — entering then earns the same forward return as a random token.
**This is chasing, confirmed. peak_15m is NOT an entry edge.** (The existing `chasing_top`,
`momentum_fading`, `dead_cat` gates already encode this intuition.)

### Test B — regime spillover: does recent tokens' early-pump heat predict a NEW signal's outcome?

Regime-heat = median peak_15m of OTHER tokens signaled in [T−45min, T−15min] (all ≥15min old →
time-legal), predicting whether a NEW signal arriving at T (entered at its own signal-time, no
chasing) becomes gold/silver:
- **IC +0.012, lift 1.05×, train +0.003 / test +0.028 (noise).** No spillover. A hot early-pump
  environment does NOT raise the next signal's dog probability.

### Final verdict on the whole investigation

**peak_15m is a real, strong signal that is NOT tradeable, because meme predictability is
CONTEMPORANEOUS and SELF-CONTAINED.** By the time the signal is observable it is either (a) already
priced into that token (chasing), or (b) non-transferable to siblings / cohort / the next signal
(Parts 1–3 + Test B). There is no exploitable LEAD TIME.

This is the deepest reason the CryptoGAT transfer fails: the paper works because established coins
move together, continuously, with zero outcome lag — cross-asset structure gives real-time
predictive lead on each asset. The meme market has neither the continuous cross-asset co-movement
(Parts 1–3: cohort/narrative ~0.05, decaying) nor exploitable own-trajectory lead time (Parts 4–5:
strong classifier, zero tradeable edge). Its dog production is real-regime-clustered but the
clustering is only knowable in arrears.

### What this investigation actually delivered (the honest ledger)

- ✅ **N1: a permanent data-quality fix** — symbol parser (full-width-colon bug) fixed and deployed;
  30.8%/70% UNKNOWN → 0.1%/0.0% backfilled. Benefits every future analysis, not just this one.
  This is the concrete, lasting win.
- ✅ **A complete, evidence-backed map of dead ends** — five layers tested, each with a reusable,
  self-tested script. Saves the system (and any future agent) from building a peak_15m entry
  filter, a cohort dimension, a narrative-graph, or a regime throttle — all of which would have
  cost weeks and returned nothing or lost money (the entry filter would actively chase).
- ✅ **Two pre-registered re-test triggers still standing** (Part 1 addendum): rerun on a larger
  production snapshot / market-wide P8 cohort before ever re-opening this.

### Recommendation: close this thread

Do NOT build anything from the cohort/narrative/early-trajectory investigation. No entry filter, no
dimension, no throttle — all tested, none tradeable. Return leverage to the main line (capture
funnel, P7 exit OOS, the mode/paper pipeline), where edges are structural (instrumentation, policy)
rather than predictive. The one artifact worth keeping live is N1's fix, already deployed.

---

## Part 6 (2026-07-06): DATA CONTAMINATION CORRECTION — provider-mixed bars invalidated several Part 4/5-era numbers

**The bug (mine):** ad-hoc analyses in Parts 4–5 and the stop/pullback replays loaded
`raw_price_bars_1m` ordered by (token, timestamp) only — but the table holds MULTIPLE
provider/pool/price-unit series per token, interleaved. Same token, same minute, up to **70×
price disagreement** between series (verified sample: 122d7g5F…, gmgn vs geckoterminal rows).
Mixing them fabricates crashes/spikes. All my ad-hoc bar-joins were affected; the system's OWN
columns (peak_15m_pct etc., computed on consistent path selections) are NOT affected — Part 4's
peak_15m IC stands.

**Corrected numbers (canonical series = the single (pool,provider,unit) with most bars per token):**

MAE of dogs before peak (n=2,027): median **6.0%** (was 28.8% contaminated); dipped >5%: **52.0%**
(was 90.2%); >20%: 28.7% (was 62.5%). → *Half of dogs never give a meaningful pullback; the
"pullback is THE entry" narrative was mostly a contamination artifact.*

Stop grid, clean (signal-time entry, champion-trail exit, n=8,529):
| stop | win% | medEV | meanEV | stopped% | dogs stopped pre-peak |
|---|---|---|---|---|---|
| −5% | 16.3% | −10.9% | +5.2% | 81.3% | 58.6% |
| −10% | 19.7% | −14.9% | +8.5% | 76.4% | 51.2% |
| −20% | 24.8% | −23.5% | **+9.2%** | 67.4% | 39.0% |
| none | 35.2% | −27.9% | +6.1% | — | — |

Key clean findings: (1) a nominal −5% stop REALIZES −10.9% median (bar-close gap-through) and
kills 59% of dogs pre-peak — still the worst cell, but for gap reasons more than noise-stop
reasons; (2) **median EV is negative at every stop level** — per-trade expectancy is entirely
tail-driven (lottery economics confirmed on clean data); (3) moderate stops (−10…−20%) have the
best MEANS (+8.5–9.2% pre-cost) — some stop beats none and beats tight.

Pullback grid, clean (9 cells × I50 structure rule, temporal holdout): **pullback entry does NOT
beat signal-time entry.** All cells median ≈ −22%, means −7%…+0.1% (train); best cell D15/C0 on
held-out test: meanEV −0.5% (I50 ON) vs baseline +4.3%. Confirmation does enrich dog-share
(27.8% base → ~37%) but the EV doesn't follow — pullback entries buy weaker structures on
average. The user's I50 (cancel if >50% below prior high) rule: median unchanged, mean much
lower ON (−0.5% vs +39.8% OFF) because OFF occasionally bottom-fishes monster rebounds in
structure-broken tokens — but those tail "wins" are rug-adjacent and likely unfillable in
reality; as a REAL-MONEY discipline the rule remains defensible; as a backtest EV enhancer it
is not supported.

**Standing corrections to earlier parts:** Part 5's "forward upside flat ~20% across peak_15m
terciles" used contaminated bars → PENDING re-verification on canonical series (direction may
survive; numbers unreliable). Part 4's IC table (system columns) stands. The narrative-sibling
and cohort ICs (already null) remain null — contamination only added noise there.

**Strategic synthesis after cleaning:** price-derived SELECTION at entry-time has now failed
every test on clean data (own-history, cohort, narrative, pullback timing; peak_15m selection
pending re-check but was already chasing-shaped). What survives: (a) lottery portfolio
structure — many tiny equal bets, moderate stop (−10…−20%), tail-riding exits (P7 champion),
positive MEAN ≈ +8–9% pre-cost at signal-time entry with wide funnel; (b) the remaining alpha
frontier is information OUTSIDE price — the user's influence/KOL thesis (who is pushing, their
reach, fermentation state → MC ceiling), which maps to P8 phases 3–4 (X narrative, smart money)
and is now partially instrumented via persisted viral/media indices (since P5). Next tests:
re-verify Part 5 on clean series; EV comparison of gated-vs-wide funnel; influence-tier
calibration once indices accumulate.

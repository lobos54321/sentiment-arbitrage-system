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

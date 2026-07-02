# reject-reason-counterfactual

When to use: `parallel_next_action=review_quality_timing_rejects_shadow_only`; any question
"is reject reason X protecting us or killing gold/silver"; before proposing any threshold change
to a quality/timing gate (which would be a strategy change — this skill is evidence-only).

Inputs: `quality_timing_research`, `quality_timing_probe_validation`, `pending_to_final_entry_audit`,
verdict `quality_timing_reason_cross`, `raw_funnel` missed examples, raw-dog API rows.
Reason mechanics: `entry_readiness_policy.py` (stale), `paper_trade_monitor.py:12632-12667`
(lotto m5), `entry_engine.py:944-1140` (smart-entry taxonomy).

## The four requirements for a valid protective-vs-harmful verdict

A reason is only judgeable when ALL four exist. As of 2026-07-02 **none** of them exist — every
verdict issued without them is a lean, not a conclusion.

1. **Dud-inclusive denominator**: same-window count of ALL signals killed by the reason, not just
   gold/silver. Filter precision = 1 − P(g/s | killed). Without it "kills mostly duds" is
   unfalsifiable.
2. **Reject-event timestamp + price_at_reject**: outcome must be measured from reject time, not
   signal time. Only `first_pending_ts` exists today → peak-vs-block ordering computable for a
   minority of kills.
3. **Label non-circularity**: the g/s label window (2h from signal, `raw-signal-outcomes.js:195,366`)
   contains every pending-stage reject; `max_sustained_peak_pct` is the tier-defining metric, so
   gold/silver-scoped reason tables select on the outcome. Use post-reject-window outcomes or
   fully out-of-window labels.
4. **Monotonicity check**: only monotone reasons (once true, stays true — e.g. signal age) support
   "reject predates peak ⇒ harmful" inference from pending timestamps. Non-monotone reasons
   (momentum, chasing) can fire after the peak; ordering evidence is only defensible per-case.

## Procedure

1. Build the reason table scoped to raw g/s AND (when P3 instrumentation lands) all-signals.
2. For each killed g/s: peak time vs best-available block time; classify
   protective-leaning (peak before block) / harmful-leaning (peak after, monotone reason only) /
   indeterminate. Report margins vs kline-bar resolution.
3. Compute the ceiling: if ALL kills by this reason were bridged, what does the funnel become?
   (Prevents over-investing in reasons that cannot reach target.)
4. For candidate harmful reasons, propose a **shadow variant** (e.g. re-anchored staleness) and
   register it as a probe — never a threshold change.
5. Cross-check attribution conflicts: the same signal gets different reasons in different
   artifacts (multi-blocker sequences). Use the event-level sequence when available.

## Output contract

Per reason: `mechanism` (file:line + threshold), `gs_kills` (n, stage), `dud_kills` (n or
"missing"), `ordering_evidence` (per-signal), `lean` ∈ {protective, harmful, mixed, indeterminate},
`ceiling_if_bridged`, `proposed_shadow_probe`.

## Acceptance

Every lean is backed by named signals with timestamps; every "uncomputable" is backed by a named
missing field (feeds the instrumentation queue).

## Findings ledger

- **2026-07-02** (verified PARTLY_CONFIRMED, corrections applied): QT rejects killed 21/70 (30%)
  of raw g/s: 11/47 upstream-gap + 10/21 pending→final (the largest pending category, 47.6%).
- **2026-07-02**: `entry_execution_signal_stale` — measured from ORIGINAL `signal_ts`, 300s default,
  no quote re-anchor (`entry_readiness_policy.py:20,505-519`). `stale_before_final` is the #1
  pending→final category (10/21). Monotone ⇒ the harmful lean on 48458 (silver +87.9%, peak at
  +6,927s) is defensible. Late-peaking dogs outlive 300s *by construction*. Highest-priority
  shadow probe: quote-fresh re-anchor.
- **2026-07-02**: ordering evidence exists for only 5 exact + 2 bounded of 10 pending QT kills:
  protective-leaning 48434 (chasing_top, peak +198s before pending +251s — margin inside bar
  resolution), 48372 (momentum_fading); harmful-leaning 48399, 48509 (chasing_top), 48493
  (momentum_fading), 48458 (stale). chasing_top and momentum_fading are **mixed**, not one-sided.
- **2026-07-02**: `dead_cat_below_high` had ZERO g/s kills in the window (n=1 historical) —
  deprioritized. `lotto_timing_negative_m5` had 1 kill — insufficient n.
- **2026-07-02**: ceiling check — bridging ALL QT pending rejects yields final 13/70=18.6%;
  verdict-window upper bound (3+21)/70=34.3% < 60%. QT tuning alone cannot reach the target;
  do bridge + coverage + mode work first.
- **2026-07-02**: attribution conflicts are real: 48458 = "matrices not yet aligned" (capture) vs
  "entry_execution_signal_stale" (verdict); 48399 = "ath_uncertainty_liquidity_too_low" vs
  "chasing_top"; 48372 = "quote_not_clean" vs "momentum_fading". Signals hit multiple blockers
  sequentially; single-reason attribution is lossy.
- **2026-07-02**: window instability — the same 24h period read n=53 (10:29Z) then n=70 (13:11Z)
  via backfill (+32%); reason counts differ across artifacts generated hours apart. Pin windows.

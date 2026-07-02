# funnel-attribution-audit

When to use: any question of the form "which stage is the real bottleneck", any
`FUNNEL_DROPOFF_*` or `CAPTURE_*_GAP_BELOW_60` classification, before proposing any
capture-improvement work.

Inputs: artifacts `raw_funnel`, `capture_stage_metrics`, `capture_60_gap_report`,
`pending_to_final_entry_audit`, `final_entry_readiness_audit`, `shadow_decision_bridge`;
API `/api/paper/raw-dog-discovery?include_rows=1`. Stage definitions live in
`offline_raw_gold_silver_funnel_audit.py:985-1017` over `paper_decision_events`.

## Procedure

1. **Pin the window first.** Record `since_ts`/`generated_at` of every artifact used. Artifacts
   regenerate at different times and the raw g/s denominator grows by backfill (observed +32%
   between 10:29Z and 13:11Z on 2026-07-02). Numbers from different pulls are not comparable.
2. Rebuild the stage table with numerator/denominator per stage. Distinguish **events** from
   **unique tokens** (70 events = 49 tokens on 2026-07-02) — report both.
3. Check nesting before quoting conditionals. Stages are not guaranteed nested
   (`final ⊄ pending`: 48416 skipped pending; true P(final|pending) was 2/23, not 3/23).
4. For each transition, pull the reason counts **scoped to raw gold/silver**, then classify every
   reason into the four blocker classes (see `capture-blocker-triage.md`):
   INSTRUMENTATION / POLICY / EPISTEMIC / CAUSAL.
5. Separate absolute loss (count) from conditional loss (rate). The largest absolute transition
   can be an instrumentation illusion while the real cliff is elsewhere.
6. Do the 60% math explicitly: which prefix-cumulative stages already cap below target; what does
   bridging each loss class alone achieve; which fixes are jointly necessary.
7. Split survival by candidate context fields that gates actually consume (quote_clean,
   liquidity), not by evaluator-only annotations (volume_profile).

## Output contract

`stage_table` (counts + cumulative + conditional, events and unique tokens),
`loss_reasons_by_transition` (raw-g/s-scoped, blocker-class-tagged),
`sixty_pct_math` (per-stage cap analysis + joint-necessity statement),
`window_pins` (artifact → generated_at/since_ts), named lost tokens for the top cliff.

## Acceptance

A second agent recomputing from a fresh pull reproduces every count exactly, or the
discrepancy is explained by a recorded window pin.

## Findings ledger

- **2026-07-02** (13-agent audit, verified): 24h funnel 70→41→30→23→3→0. The largest transition
  (detector→decision −29) is 93% instrumentation (`shadow_entry_hypotheses_matched_no_decision_bridge`
  27/29; optimistic 68/70=97.1% per `shadow_decision_bridge.optimistic_decision_record_count_if_shadow_gap_logged`).
  The real strategy cliff is pending→final (true conditional 2/23=8.7%), dominated by
  `stale_before_final` 10/21=47.6%. final→paper 0/3 is 100% `mode_disabled` (POLICY).
- **2026-07-02**: decision=41 < ceil(0.6·70)=42 → decision stage alone capped 60% by exactly 1
  signal. Bridging all 29 no-decision → pending 52/70=74.3%; bridging all 10 QT pending rejects →
  final only 13/70=18.6%. Jointly necessary.
- **2026-07-02**: dominant survival correlate is `source_quote_clean` (true→decision 94.7% vs
  false→21.9%), NOT kline coverage (uncovered rows had *higher* would-enter, 50.0% vs 42.9%).
  Check quote_clean cohorts before blaming data coverage.
- **2026-07-02**: gold vs silver capture nearly identical (62.2% vs 60.0% decision) — tier is not
  a funnel factor at decision stage.
- **2026-07-02 pitfall**: row-level `decision_record_count>0` (43) ≠ strict signal_id bridge
  definition (41); the 2 extras are `token_time_decision_nearby_signal_id_mismatch`. State which
  definition you are using.
- **2026-07-03 local verification**: P0 bridge fix adds evaluator-only
  `shadow_decision_bridge_events` as a mirror table, not `paper_decision_events`. Local self-test
  before/after: one raw g/s event moved from no-decision to decision evidence after inserting a
  mirror row, while `pass_or_allow`, `pending_entry`, `entered`, and promotion remained zero/false.
  Production acceptance still requires a fresh deployed 24h AutoLoop window.

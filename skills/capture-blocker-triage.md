# capture-blocker-triage

When to use: any `BLOCKED_*` classification, any new `top_blocker`, before spending effort on a
blocker — first establish which of the four classes it belongs to, because the fix type is
completely different per class.

Inputs: `context_blocker_monitor`, `volume_kline_coverage`, `context_dimension_eligibility`,
`a_class_fastlane` (mode state), verdict `clean_window_failed_conditions`; grep of runtime gate
code for consumers of the blocked field.

## The four blocker classes

| Class | Definition | Test | Fix type |
|---|---|---|---|
| INSTRUMENTATION | the event happened but was never recorded | a shadow/mirror artifact shows the gap closes when logged | logging/bridge fix |
| POLICY | a mode/switch/governance state blocks progression | blocker fires with zero data-quality involvement (e.g. `mode_disabled`) | human decision + auditable procedure |
| EPISTEMIC | a coverage/quality metric below threshold blocks *trust* (dimension eligibility, clean windows, promotion), but no runtime gate consumes the field | grep runtime gates for the field → zero consumers; capture cohorts show no survival difference | data-collection/meter fix; does NOT raise capture |
| CAUSAL | a runtime gate consumed missing/bad data and killed dogs | reason strings name the gate; cohort split shows survival difference | data fix recovers those specific events |

## Procedure

1. Name the exact blocker string and where it is computed (`file:line`).
2. **Consumer test**: grep `entry_engine.py`, `paper_trade_monitor.py`, `gmgn_policy.py`,
   `final_entry_contract.py`, `scout_quality.py` for the underlying field. Zero hits in runtime
   gates ⇒ EPISTEMIC (evaluator-only).
3. **Cohort test**: split the raw g/s funnel by field-known vs unknown; no survival difference ⇒
   not CAUSAL.
4. **Recoverability test**: is there a matured/recheck artifact proving the data exists later?
   (read-only recompute, `canonical_backfill_performed=false` pattern).
5. State the downstream chain: what does clearing this blocker actually unlock (capture events?
   promotion evaluation? a human decision?). Quantify.

## Output contract

`blocker`, `class`, `computed_at` (file:line), `consumers` (list or "none-runtime"),
`cohort_split` (numbers), `recoverable_rate`, `unlocks` (explicit chain), `recommended_fix_type`.

## Acceptance

The class assignment survives the consumer + cohort tests recomputed by a second agent.

## Findings ledger

- **2026-07-02** (verified CONFIRMED): `volume_profile_coverage_below_80pct` is EPISTEMIC.
  `volume_profile` is a post-hoc annotation (`candidate_shadow_observer.py:921-954`) with zero
  runtime-gate consumers; volume-unknown is 100% downstream of kline bar counts (242 = 200
  `insufficient_kline_bars_lt_3` + 42 `kline_bars_unavailable`); matured recompute known-rate
  92.05% proves the data exists. Backfilling raises capture 0%; it only (jointly with kline≥80%)
  satisfies clean-window preconditions for human mode re-enable.
- **2026-07-02**: volume 32.6% root cause = stale-payload bug: one-shot fetch 60–120s post-signal,
  `not bars` refetch guard (`candidate_shadow_observer.py:1891-1896`), payload frozen once signal
  leaves the 10-newest window, and the observer reads its own thin
  `candidate_shadow_kline_cache.db` instead of the dense `kline_cache.db`.
- **2026-07-02**: kline 41.2% is a threshold artifact: first-bar lag ≤30s policy
  (`raw-signal-outcomes.js:42-53`) vs minute-aligned bars (lag ~U(0,60s)) → structural ~50% cap;
  29/30 "uncovered" rows have complete paths; research-grade coverage 98.04%.
- **2026-07-02**: `mode_disabled` is POLICY with a missing procedure: circuit broken 2026-06-11
  (ERBAI −20.9% > 20% cap); **no code path restores LIVE**; `clean_windows_required=4` counter
  unimplemented; `paper_entry_proposal_readiness` circular (needs intent>0 which the mode blocks).
  ~21 days in SHADOW. Fix type = P2 mode contract + human decision, not more data.
- **2026-07-02**: the genuinely CAUSAL kline channel this window: `no_kline_low_volume` (4
  pending→final kills incl. CIT +11,302%) + `not_ath_prebuy_kline_unknown_data_blocked` (2) ≈ 6
  recoverable events — which still hit `mode_disabled` at final.
- **2026-07-02** (P1 verified, run `codex_p1c_20260702T210515Z`, commit `858e242`):
  `volume_profile_coverage_below_80pct` and `kline_coverage_below_80pct` are cleared for formal
  discovery by canonical P1 coverage methods. `volume_context.known_rate=0.935484`
  (`strict_first_look_known_rate=0.711982`) and
  `raw_gold_silver_kline.kline_coverage_rate=0.943662`
  (`strict_kline_coverage_rate=0.492958`). Candidate mesh coverage is 84/84 with
  `full_candidate_coverage_rate=1.0`. Latest verdict moved to
  `A_CLASS_STUCK_REVIEW_REQUIRED`, `promotion_allowed=false`; current capture stage is
  `mode_disabled_stuck_requires_human_review`. Current 24h funnel:
  detector 71/71, decision 69/71, pass_allow 18/71, pending 11/71, final eligibility 2/71,
  paper 0/71. Largest actual gap is decision→pass_allow; next stage is P2
  A_CLASS/final_entry_contract human review, not strategy tuning or risk changes.

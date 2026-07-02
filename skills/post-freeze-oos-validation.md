# post-freeze-oos-validation

When to use: frozen discovery definitions await OOS judgment; designing or reviewing any
`*_post_freeze_oos_validation`; before letting any "REPEAT" verdict influence a promotion
conversation.

Inputs: `capture_cross_oos_freeze_registry`, `capture_cross_post_freeze_oos_validation`,
`pass_allow_60_oos_freeze_registry`, `pass_allow_60_post_freeze_oos_validation`,
`oos_readiness_summary`, `hypothesis_validation`, capture artifacts (in-sample reference).
Discovery hit rule: `offline_candidate_capture_discovery.py:601-613`.

## Non-negotiables (each one failed or was missing on 2026-07-02)

1. **Batch-pinned clocks.** Pin each definition's OOS eval_start to its own `frozen_at` batch,
   never to the whole-set fingerprint. (Observed: set fingerprint resets eval_start for all 192
   items whenever ANY batch is added — 3 batches already; churn can reset the clock indefinitely.)
2. **Effective-N = unique tokens, not events.** 28 g/s event rows were only 22 tokens; slices
   double-count repeated tokens. Require ≥10 unique tokens per definition before any verdict.
3. **Dedupe before testing.** Collapse frozen definitions into event-set families (identical
   matched-set + event-set ⇒ one hypothesis). 19 hits collapsed to 6 unique event-sets — testing
   19 as independent inflates apparent confirmation.
4. **Exclude self-crosses.** A candidate crossed with a dimension it mechanically gates on
   (e.g. `notath_mc_*` × `market_cap_bucket`, `notath_*quote*` × quote flags) is definitionally
   correlated — not evidence. The only Bonferroni "survivors" on 2026-07-02 were all self-crosses.
5. **A real test, then FDR.** Replace directional lift>0 with a one-sided exact test vs the
   post-freeze global baseline; control Benjamini-Hochberg q=0.1 across the surviving families
   (not the raw definition count). Under lift>0 with min 3 events, ~≤50% of definitions
   false-repeat per window; across 192 definitions expect dozens of chance repeats.
6. **Two disjoint OOS windows.** REPEAT requires the same-direction effect in two consecutive
   non-overlapping windows. One window with tiny n is a coin flip.
7. **Negative-control panel.** Run the identical OOS machinery on label-shuffled or
   random-candidate nulls each window; publish the null repeat rate next to the real one.
   The `holdout_negative_controls` machinery already exists in `v27_basic_contract_readiness.py`
   / `runtime_final_evidence.py` — apply it to discovery.
8. **Multiplicity budget in the artifact.** Publish cells-searched (include 48h/72h parallel
   meshes — extra looks), expected false hits under the null, and observed hits, every run.

## Procedure

1. Verify freeze discipline: train window end < frozen_at < eval_start (+safety); fingerprints
   stable; no in-sample rows in the OOS query.
2. Build family map (dedupe + self-cross exclusion). Report families, not raw definitions.
3. Accumulate OOS by unique token; judge only families with N≥10 tokens.
4. Exact test per family → BH-FDR q=0.1 → two-window rule → compare to null panel.
5. Verdicts: `OOS_CONFIRMED_FAMILY` / `NO_REPEAT` / `TOO_SMALL` with q-values. Promotion remains
   human-gated regardless.

## Output contract

`family_table` (definitions→family, self-cross flags), `unique_token_n`, `per-family p/q`,
`null_panel_repeat_rate`, `multiplicity_budget` (cells searched, E[false hits], observed),
`window_lineage` (train end / frozen_at / eval_start per batch).

## Acceptance

A verifier recomputing from the registry reproduces family collapse and q-values; the null panel
ran; no verdict was issued on event-counted (vs token-counted) N.

## Findings ledger

- **2026-07-02** (verified CONFIRMED): freeze timing discipline WORKS (eval_start strictly
  post-freeze, +120s safety, no overlap, promotion_allowed=false everywhere). The statistics do
  not exist: no p-values/FDR anywhere in the discovery pipeline.
- **2026-07-02**: 24h mesh judged 3,780 cells (84 candidates × 45 dim-slices); binomial null
  expects ~10.9 false DISCOVERY_HITs of 22 observed (hypergeometric: ~3.1). 48h/72h meshes add
  4,704 + 5,376 more looks per run. No independent hit survived correction; only 2 structurally
  independent crosses existed, neither survived.
- **2026-07-02**: in-sample, the 19 hits are capture-recall effects only — 12/19 negative
  decision_lift, 16/19 negative pass_allow_lift, final_entry_rate_after_match=0.0 for all. A
  capture hit is NOT a downstream-improvement hit.
- **2026-07-02**: g/s label maturation lag ~1–3h (inferred from pass_allow_60 track); per-def
  min_selected=3 in sibling code (`pass_allow_60_post_freeze_oos_validation.py:40,309-314`);
  4/38 pass_allow definitions already NO_REPEAT on exactly-3-event judgments — the weak bar cuts
  both ways.
- **2026-07-02**: capture_cross freeze/OOS builder code exists only on GitHub main (local checkout
  127 commits behind) — always diff against deployed commit before citing code.

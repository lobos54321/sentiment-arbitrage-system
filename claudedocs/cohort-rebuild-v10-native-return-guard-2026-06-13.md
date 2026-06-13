# Cohort Rebuild v10 — Native Return Guard

Status: verified offline research artifact, 2026-06-13.

## Why This Supersedes v7

The v7 rebuild correctly recovered `venue_other` rows through GMGN anchors, but a second residual class remained:

- clean dog rows with `return_domain=sol_curve`
- no external GMGN / chain-truth route
- `return_baseline_unit_route=original_native_baseline`
- native observed return greater than `15x`

Those rows came mostly from native-bar repairs or old labels. They were not pumpfun-physical-limit violations in the strict suffix sense, but they were still unsafe to accept as clean without external truth.

## Fix

The rebuild now treats any no-route native observed return above `15x` as requiring external truth:

- if GMGN full-window data is available, the row is rebuilt in `usd_gmgn` using GMGN anchor and full-window peak;
- if GMGN data is not available, the row remains quarantined with `suspicious_native_return_needs_external_truth`.

This closes the loophole where non-pump or legacy native bars could produce extreme clean dog returns without an external verifier.

## Verified Data

Data room:

`/Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z`

Main artifacts:

- `gmgn-full-window-v11-final-native-return-guard/gmgn-full-window-touch.json`
- `cohort-rebuild-v10-final-native-return-guard/rebuilt-rows.jsonl`
- `cohort-rebuild-v10-final-native-return-guard/rebuild-summary.json`
- `cohort-quarantine-followup-v9-final/followup-summary.json`

Summary:

- input rows: 1724
- clean rows: 1724
- quarantine rows: 0
- clean dog unique: 461 signal-level opportunities
- clean dud unique: 971 signal-level opportunities
- clean dog token unique: 350
- active label-unit suspects: 0
- coverage incomplete: 0

Clean dog rows by return domain:

- `usd_gmgn`: 383 rows / 304 unique signals
- `spliced_curve_to_gmgn`: 148 rows / 100 unique signals
- `sol_curve`: 77 rows / 57 unique signals

Critical sanity checks:

- `return_domain=sol_curve` and `corrected_peak_pct > 15`: 0 rows
- `dog return_domain=sol_curve` and `corrected_peak_pct > 15`: 0 rows
- remaining `original_native_baseline > 15x` rows are `spliced_curve_to_gmgn` rows with GMGN full-window peaks and an explicit graduation bridge, not same-domain native peaks.

## Interpretation

The apprentice review was directionally right that v7 still had a residual unsafe native-return path. The precise issue was broader than `missing_recorded_peak_repaired_from_native_bars`: any no-route native observed return above `15x` required external truth.

After the native return guard and the final GMGN follow-up, the frozen pack is fully adjudicated under the current evidence model. This is now the current best raw-discovery cohort for downstream analysis.

This does not prove strategy edge and does not justify live changes. It only means the label/return denominator is clean enough for the next offline analyses:

1. matched signal-vs-decision ceiling on the same cohort;
2. stage-stratified dog-vs-dud feature analysis without leakage;
3. executable-policy split between curve, spliced, and GMGN/USD domains.

Live gate, matrix, RR, exit, and live size remain frozen.

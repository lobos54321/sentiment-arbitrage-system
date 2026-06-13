# V10 Curve Feature Export Spec

Status: research data contract / no production strategy change

Purpose: build the decisive dog-vs-dud curve-stage feature table without relying on slow raw RPC tail scans.

## Current Decision

Raw RPC is not the primary path for the V10 curve-stage discriminator.

The guarded sample pipeline proved:

- a stratified 299-row worklist can be generated from the V10 clean cohort;
- raw RPC exact TradeEvent decoding works when the window is reachable;
- but raw RPC gets dragged by post-anchor signature tails and rate limits before producing enough complete windows.

Therefore the preferred input is an indexed/exported pump.fun TradeEvent table covering each worklist window:

`[signal_ts - 900 seconds, signal_ts]`

The output must be fed into:

```bash
node scripts/build-v10-curve-feature-decode-from-trades.js \
  --worklist <v10-curve-feature-stratified-300.txt> \
  --trades <pumpfun-trades.csv-or-jsonl> \
  --out <merged-decode.json> \
  --assume-complete-window

node scripts/build-v10-curve-feature-table.js \
  --dogs <rebuilt-clean-dogs.json> \
  --duds <rebuilt-clean-duds.json> \
  --decode <merged-decode.json> \
  --out <curve-feature-table.json>
```

Use `--assume-complete-window` only when the export query is guaranteed complete for every worklist row. Incomplete exports must not be treated as zero-activity windows.

## Required Cohort Inputs

Clean V10 cohort:

- Dogs: `cohort-rebuild-v10-final-native-return-guard/rebuilt-clean-dogs.json`
- Duds: `cohort-rebuild-v10-final-native-return-guard/rebuilt-clean-duds.json`

Current stratified sample:

- `v10-curve-feature-v1/stratified-samples/v10-curve-feature-stratified-300.txt`
- 299 rows: 149 dog / 150 dud
- balanced by `return_domain`: `sol_curve`, `spliced_curve_to_gmgn`, `usd_gmgn`

Full worklist:

- `v10-curve-feature-v1/v10-curve-feature-worklist.txt`
- 1432 signal-anchor rows

## Required Trade Export Fields

Each exported row should represent one pump.fun trade event.

Accepted field aliases are flexible, but the following canonical fields are preferred:

| Field | Required | Notes |
|---|---:|---|
| `token_ca` | yes | Mint address. Alias: `mint`, `token_mint`, `mint_address`. |
| `block_time` | yes | Unix seconds. Alias: `timestamp`, `blockTime`, `evt_block_time`. |
| `signature` | recommended | Transaction signature. |
| `side` | yes | `buy` or `sell`. Alias: `is_buy` boolean accepted. |
| `user` | recommended | Trader wallet. Alias: `trader`, `wallet`, `buyer`, `seller`. |
| `sol_amount` | yes | SOL amount, not lamports. |
| `token_amount` | yes | UI token amount, not raw integer. |
| `virtual_sol_reserves` | recommended | SOL units. |
| `virtual_token_reserves` | recommended | UI token units. |
| `real_token_reserves` | recommended | UI token units. Used to estimate progress. |
| `reserve_price_sol` | optional | If omitted, computed from virtual reserves when possible. |
| `progress_pct` | optional | If omitted, computed from real token reserves when possible. |

At least one of these must be present for price:

- `reserve_price_sol`
- `price_sol`
- `virtual_sol_reserves` + `virtual_token_reserves`
- `real_token_reserves` from which reserve price can be estimated

At least one of these must be present for progress:

- `progress_pct`
- `real_token_reserves`

## Export Completeness Requirement

For every worklist row `token_ca|signal_ts|label`, export all pump.fun trades for:

```text
token_ca == row.token_ca
block_time >= signal_ts - 900
block_time <= signal_ts
```

The feature table must use only `feature_coverage_status = complete_window` rows.

If an export source cannot guarantee complete coverage, do not pass `--assume-complete-window`; those rows must remain coverage-only and must not enter AUC.

Before reading any AUC, inspect coverage symmetry:

- `validate-v10-curve-feature-trade-export.js` reports `trade_hit_guardrail` by `label`, `return_domain`, and `return_domain|label`.
- `build-v10-curve-feature-table.js` reports `feature_coverage` by `label`, `return_domain`, and `return_domain|label`.

Large dog/dud or domain-level differences in complete-window coverage mean the AUC is selection-biased. In that case, fix or explain coverage first; do not treat the feature result as an edge result.

## What Not To Conclude

Do not infer feature null from:

- raw RPC timeout;
- `history_reached_start=false`;
- `signatures_skipped_after_end` dominating a batch;
- missing exported rows unless the export is known complete.

Those are coverage failures, not evidence of zero volume or no buy pressure.

Do not use post-anchor fields for immediate-entry gates:

- `early_5m` after signal;
- `early_15m` after signal;
- `120m` volume;
- future peak / future graduation time.

These may be used only for delayed confirmation or labels, not ex-ante discriminator tests.

## Current Guardrails

Existing scripts enforce the most important guardrail:

- `build-v10-curve-feature-table.js` includes only `feature_coverage_status = complete_window` rows in AUC.
- `history_reached_start=false` is reported as `incomplete_window`, not as zero activity.
- `build-v10-curve-feature-table.js` reports complete-window rates by dog/dud and return domain.
- `validate-v10-curve-feature-trade-export.js` reports trade-hit rates by dog/dud and return domain before the exported trades are converted into decode rows.

This prevents repeating the earlier error of reading "not measured" as "no edge".

## Decision Criteria

The decisive experiment is not complete until the feature table has enough complete rows per stratum:

- compare dog vs same-bucket dud, not dog vs all;
- stratify by `return_domain`;
- report sample size for every stratum;
- do not trust pooled AUC if strata disagree;
- mark strata with fewer than 30 dogs or fewer than 30 duds as insufficient.

If curve-stage features show stable separation, the next artifact is a shadow-only two-stage gate spec.

If curve-stage features are null after complete-window coverage, the next conclusion is sourcing/target reassessment, not gate loosening.

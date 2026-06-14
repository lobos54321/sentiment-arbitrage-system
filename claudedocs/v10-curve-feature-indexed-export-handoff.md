# V10 Curve Feature Indexed Export Handoff

Status: handoff runbook / no production strategy change

Purpose: export complete pump.fun TradeEvent rows for the decisive curve-stage dog-vs-dud feature test.

## Current State

The strategy question is blocked by data, not by another discussion round.

Known:

- V10 clean cohort and label repair are good enough to run the next experiment.
- Raw RPC is not viable for the 299-row stratified sample because it gets dragged by post-anchor signature tails and rate limits.
- The decisive remaining feature set is pump.fun curve-stage structure in the no-future window `[signal_ts - 900, signal_ts]`.
- The export/analysis pipeline now has guardrails for dog/dud and return-domain coverage symmetry.

Not known yet:

- Whether curve-stage features separate dog from same-bucket dud.

Do not change live gate, matrix/RR, exit, size, final contract, or live trading behavior based on this export alone.

## Data Pack

Use this frozen export pack:

```text
/Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z/v10-curve-feature-v1/stratified-samples/export-pack-v2
```

Portable tarball:

```text
/Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z/v10-curve-feature-v1/stratified-samples/v10-curve-feature-export-pack-v2.tgz
```

SHA256:

```text
a686ab693fc1f7c99ff7fa02a90daddceef7b82979f07bb91fba516076a4bc99
```

Pack contents:

- `indexed_trade_export_template.sql`: editable query template.
- `signal_windows.csv`: canonical 299 signal windows.
- `signal_windows_values.sql`: windows CTE only.
- `tokens.csv`: 286 unique tokens.
- `manifest.json`: row counts and cohort-balance guardrail.
- `README.md`: short pack-local instructions.

Pack balance:

- 299 windows
- 149 dog / 150 dud
- `sol_curve`: 49 dog / 50 dud
- `spliced_curve_to_gmgn`: 50 dog / 50 dud
- `usd_gmgn`: 50 dog / 50 dud
- `missing_cohort_meta_rows = 0`

Before sending the pack, run:

```bash
node scripts/check-v10-curve-feature-handoff.js \
  --pack-dir /Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z/v10-curve-feature-v1/stratified-samples/export-pack-v2 \
  --tar /Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z/v10-curve-feature-v1/stratified-samples/v10-curve-feature-export-pack-v2.tgz \
  --expected-tar-sha256 a686ab693fc1f7c99ff7fa02a90daddceef7b82979f07bb91fba516076a4bc99
```

Expected:

```text
status = ready_to_send
blockers = []
```

## Export Contract

Export every pump.fun TradeEvent satisfying:

```text
token_ca == signal_windows.token_ca
block_time >= signal_windows.window_start_ts
block_time <= signal_windows.window_end_ts
```

The window is exactly:

```text
[signal_ts - 900 seconds, signal_ts]
```

No post-signal trades are allowed for this feature table.

Required output fields:

| Field | Required | Notes |
|---|---:|---|
| `window_id` | yes | Must come from `signal_windows`. |
| `token_ca` | yes | Mint address. |
| `signal_ts` | yes | From `signal_windows`. |
| `window_start_ts` | yes | From `signal_windows`. |
| `window_end_ts` | yes | From `signal_windows`. |
| `label` | yes | `dog` or `dud`. |
| `return_domain` | yes | `sol_curve`, `spliced_curve_to_gmgn`, or `usd_gmgn`. |
| `effective_tier` | yes | `gold`, `silver`, `bronze`, `sub25`, etc. |
| `block_time` | yes | Trade timestamp, Unix seconds. |
| `signature` | recommended | Transaction signature. |
| `side` | yes | `buy` / `sell`, or `is_buy` boolean accepted by parser. |
| `user` | recommended | Trader wallet. |
| `sol_amount` | yes | SOL units, not lamports. |
| `token_amount` | yes | UI token amount. |
| `virtual_sol_reserves` | recommended | SOL units. |
| `virtual_token_reserves` | recommended | UI token units. |
| `real_token_reserves` | recommended | UI token units, used for progress. |
| `reserve_price_sol` | optional | If omitted, computed from virtual reserves when possible. |

At least one price source must be available:

- `reserve_price_sol`
- `price_sol`
- `virtual_sol_reserves / virtual_token_reserves`
- reserves sufficient for the parser to estimate price

At least one progress source should be available:

- `progress_pct`
- `real_token_reserves`

## How To Run The Query

Open:

```text
export-pack-v2/indexed_trade_export_template.sql
```

Replace:

```sql
YOUR_PUMPFUN_TRADE_EVENT_TABLE
```

and the source column names inside `source_trades` with the indexed source's actual schema.

Keep the `signal_windows` CTE and the final join intact.

Export results as CSV, JSON, or JSONL.

Suggested output filename:

```text
pumpfun-trades-v10-curve-feature-299.csv
```

## After Export

Run the one-command analysis:

```bash
node scripts/run-v10-curve-feature-export-analysis.js \
  --pack-dir /Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z/v10-curve-feature-v1/stratified-samples/export-pack-v2 \
  --trades <pumpfun-trades-v10-curve-feature-299.csv-or-jsonl> \
  --out-dir <analysis-out> \
  --assume-complete-window
```

Only pass `--assume-complete-window` if the export source guarantees complete coverage for every signal window.

Read this first:

```text
<analysis-out>/analysis-summary.json.decision
```

Decision meanings:

- `ready_for_auc_review`: coverage and sample-size guardrails passed; AUC may be inspected next.
- `blocked`: do not read AUC. Inspect `decision.blockers`.

Blockers include:

- `export_not_marked_complete_window`
- `cohort_subset_missing_window_rows`
- `insufficient_complete_dog_or_dud_rows`
- `no_complete_feature_rows`
- `label_complete_rate_asymmetry_gt_10pp`
- `return_domain_x_label_complete_rate_asymmetry_gt_20pp`

## What To Send Back

Send back:

```text
<analysis-out>/analysis-summary.json
<analysis-out>/trade-export-validation.json
<analysis-out>/curve-feature-table.json
```

If file size is an issue, send first:

```text
<analysis-out>/analysis-summary.json
```

The summary is enough to determine whether the result is readable or blocked.

## Interpretation Rules

Do not read AUC unless `decision.status = ready_for_auc_review`.

If ready:

- Inspect AUC by `return_domain`, `progress_stage`, and `return_domain_x_progress_stage`.
- Do not trust pooled AUC if strata disagree.
- Treat strata with fewer than 30 dog or fewer than 30 dud complete rows as insufficient.

If blocked:

- Fix data coverage or export completeness first.
- Do not interpret missing trades as zero activity.
- Do not change strategy parameters.

## Expected Next Decision

This export decides whether pump.fun curve-stage ex-ante features separate dog from same-bucket dud.

Possible outcomes:

1. Stable feature separation:
   - next artifact is a shadow-only two-stage gate spec.
   - live strategy still remains unchanged until shadow validation.

2. Null feature result after complete, symmetric coverage:
   - next conclusion is sourcing or target reassessment.
   - do not loosen matrix/RR/gate based on null data.

3. Coverage blocked:
   - improve indexed export or use another indexed source.
   - do not read AUC.

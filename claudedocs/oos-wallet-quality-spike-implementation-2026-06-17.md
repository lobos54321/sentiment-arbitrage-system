# OOS Wallet-Quality Feasibility Spike Implementation

Date: 2026-06-17
Branch: runtime-stability-marker-guard

## Scope

This is a label-free feasibility spike for the next candidate axis:
`smart_money_buy_share`.

It is not a preregistered edge test. It must not contain labels, tiers, AUC,
dog/dud separation, or strategy conclusions.

## Added Tools

- `scripts/build-oos-wallet-quality-spike-windows.js`
  - Builds a stripped `(token_ca, signal_ts)` window list from the cumulative
    OOS feature table.
  - Emits CSV, SQL `VALUES`, rendered Dune SQL, and a manifest.
  - Output fields are limited to window identity and timestamps.

- `scripts/oos-wallet-quality-spike.template.sql`
  - Dune SQL for as-of wallet-quality availability.
  - Uses only trades with `block_time < signal_ts` for prior wallet history.
  - Excludes the signal token creator from qualifying wallet counts.
  - Flags first-block sniper proxy separately instead of hard-dropping it.
  - Emits coverage/availability fields only.

- `scripts/validate-oos-wallet-quality-spike.js`
  - Validates spike output.
  - Fails closed on forbidden keys: labels, tiers, AUC, dog/dud keys.
  - Fails closed on missing required fields or as-of integrity violations.
  - Emits only coverage/availability/cost summary.

## Verification

Local checks:

```bash
node --check scripts/build-oos-wallet-quality-spike-windows.js
node --check scripts/validate-oos-wallet-quality-spike.js
node --test tests/oos-wallet-quality-spike.test.mjs
```

Targeted regression suite:

```bash
node --test \
  tests/oos-wallet-quality-spike.test.mjs \
  tests/build-v10-curve-feature-table.test.mjs \
  tests/run-oos-daily-cycle.test.mjs
```

Result: 20/20 pass.

## Post-Audit Fixes

After review of commit `85e2a5a6`, four fixes were applied:

1. The validator now requires a windows manifest and fails closed unless
   `history_days >= 365`. This prevents a 7d smoke from being mistaken for the
   max-lookback availability curve.
2. The SQL now emits `n_buyers_qualify_k{1,3,5}_{7d,14d,30d,all}_nonsniper`
   plus nonsniper K3 SOL totals.
3. The sniper proxy now includes a prior-history proxy: a wallet is flagged
   when it has at least 3 prior buy trades and at least 50% of those buys
   occurred within 5 seconds of the prior token's observed first trade in the
   history window. This is still a proxy, not a funding graph.
4. Runtime validation now reads Dune execution runtime/cost from
   `final_status.result_metadata.execution_time_millis` and
   `final_status.execution_cost_credits`.

## Dune Smoke

Smallest practical Dune smoke:

- Windows: 1
- History: 7 days
- Dune performance tier: small
- Execution: `01KV9PSBTRGCA4VTF0XQXGGTRT`
- Output: `/Users/boliu/sas-data-room/oos-wallet-quality-spike-smoke-20260617T023254Z`
- Result: `SPIKE_QA_PASS_NO_EDGE_CLAIM`
- Rows: 1
- As-of violations: 0
- Forbidden key found: null
- Runtime: about 52 seconds

A previous 3-window run with the heavier first SQL shape timed out after 2
minutes. The SQL was then rewritten to avoid a global source-trades CTE. The
1-window smoke proves schema and as-of extraction work, but it does not prove
that a 20-window batch is cheap enough.

## Historical AUC Contamination Cleanup

The three pre-fix daily feature tables that contained between-lookpoint
`auc/strata` fields were copied to quarantine and scrubbed in place.

Quarantine manifest:

`/Users/boliu/sas-data-room/oos-contaminated-feature-table-quarantine-20260617T022913Z/manifest.json`

All three original locations now have `after_has_auc=false` and
`after_has_strata=false`; original hashes are preserved in the quarantine
manifest.

## 365d Cost Calibration

The required 1-window x 365d calibration was attempted on Dune `small`:

- Execution: `01KV9R0K99P905ESZNMZFCXVBT`
- Output directory:
  `/Users/boliu/sas-data-room/oos-wallet-quality-spike-calibration-20260617T025418Z`
- Result: timeout after the 2 minute small-tier limit
- Credits consumed before timeout: `58.449558824`
- Result rows: none
- Failure artifact:
  `/Users/boliu/sas-data-room/oos-wallet-quality-spike-calibration-20260617T025418Z/calibration-failure.json`

Interpretation: the current 365d as-of wallet-quality query is not tractable on
Dune small tier for even one window. A 20-window run should not be authorized
without further query optimization, a more constrained history strategy, or an
explicit higher-tier budget decision.

## Current Judgment

The spike tooling is ready for review. The feature is not yet ready for prereg.

Recommended next step is query-cost reduction or an explicit budget decision,
not a 20-window run. Any future run must remain label-free and should be judged
only on:

- as-of integrity violations;
- availability of qualifying wallets across K/lookback settings;
- creator/sniper counts;
- missingness;
- Dune runtime/cost.

Do not use this spike to choose strategy parameters or infer edge.

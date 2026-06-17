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

## Current Judgment

The spike tooling is ready for review. The feature is not yet ready for prereg.

Recommended next step is a deliberate, authorized 20-window feasibility run
after reviewing Dune cost expectations. That run should remain label-free and
should be judged only on:

- as-of integrity violations;
- availability of qualifying wallets across K/lookback settings;
- creator/sniper counts;
- missingness;
- Dune runtime/cost.

Do not use this spike to choose strategy parameters or infer edge.

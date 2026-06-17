# Gate-0.5 Backfill Pilot Tool Handoff

Date: 2026-06-17.
Status: implementation handoff for review. No provider spend was performed.

## Commit Scope

This handoff covers:

- `claudedocs/gate0.5-step1-pilot-run-card-2026-06-17.md`
- `claudedocs/gate0.5-step1-pilot-run-card-2026-06-17.sha256`
- `scripts/run-gate05-backfill-pilot.js`
- `scripts/gate05-backfill-pilot-dune-bars.template.sql`
- `tests/run-gate05-backfill-pilot.test.mjs`

The tool is research-only and has no production path.

## Purpose

Implement the locked Gate-0.5 Step-1 pilot skeleton:

1. select a burned pilot sample from historical `premium_signals`;
2. prioritize the 2026-06-06..2026-06-07 observer-overlap for reconciliation;
3. emit `burned_keys.txt`, `pilot-signals.json`, and provider request windows;
4. once 1m bars are supplied, reuse `src/analytics/raw-signal-outcomes.js` to compute backfill labels;
5. reconcile backfill labels against observer labels;
6. report labelability, stage-resolution, cost, and final feasibility verdict.

It does not query Dune or Gecko itself. That spend is intentionally separated:
`prepare` emits windows; `evaluate` consumes returned bars.

The recommended Dune export template is
`scripts/gate05-backfill-pilot-dune-bars.template.sql`. It emits stage-aware
native/SOL 1m bars from decoded pump.fun TradeEvents, including
`source_kind='bonding_curve'`, `source_family='onchain_swap'`, and
`price_unit='native'`. The provider fetch step must abort before spend if the
estimated/final Dune cost would exceed the locked 30-credit pilot ceiling.

## Commands

Prepare-only:

```bash
node scripts/run-gate05-backfill-pilot.js \
  --mode prepare \
  --premium-db <sas_sentiment_current.db> \
  --observer-db <raw_signal_outcomes.snapshot.db> \
  --out-dir <out-dir> \
  --limit 200
```

`prepare` now fails closed if the supplied `premium_signals` DB does not look
like the real metadata snapshot: required metadata columns must exist, May+
runs require `narrative_score`, and row count must be at least 30,000 unless
`--allow-small-premium-db-for-smoke` is explicitly supplied for synthetic tests.

Evaluate after provider bars are available:

```bash
node scripts/run-gate05-backfill-pilot.js \
  --mode evaluate \
  --observer-db <raw_signal_outcomes.snapshot.db> \
  --pilot-signals <out-dir>/pilot-signals.json \
  --bars-jsonl <provider-bars.jsonl> \
  --out-dir <eval-out-dir> \
  --cost-credits <credits>
```

`provider-bars.jsonl` rows must be 1m OHLCV with:

- `token_ca`
- `timestamp`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `provider`
- `source_kind`
- `source_family`
- `pool_address`
- `price_unit`

## Discipline

- No candidate feature effect is computed.
- No `signal_type` / `is_ath` / `narrative_score` dog-rate is reported.
- No AUC, precision, recall, or lift is computed.
- Pilot rows are burned by `(token_ca, signal_ts)`.
- Sampling reports forbidden strata and only uses labelability-oriented strata.
- Labeling reuses `buildRawSignalOutcomeReport()` from
  `src/analytics/raw-signal-outcomes.js`.
- Stage-resolution is reported separately from outcome labelability.
- Wrong premium DB usage is fail-closed before any pilot sample is emitted.
- Observer reconciliation includes `sustained_reason`, so sustained-definition
  disagreements can be classified instead of falling through.

## Verification Performed

- `node --check scripts/run-gate05-backfill-pilot.js`
- `node --test tests/run-gate05-backfill-pilot.test.mjs`
- `node scripts/run-gate05-backfill-pilot.js --help`
- wrong-DB smoke against local `server_sentiment_arb.db` now fails closed unless
  smoke override is explicitly supplied. This prevents the old silent 0-row or
  all-null-metadata pilot path.
- disagreement taxonomy tests cover coverage, baseline, unit, and sustained
  definition differences.

## Review Focus

1. Is the prepare/evaluate split acceptable before provider spend?
2. Are reconciliation thresholds implemented as intended:
   - `>=0.90` pass;
   - `0.80..0.90` partial;
   - `<0.80` not feasible?
3. Is the stage-resolution proxy too weak for final feasibility, or acceptable
   as a pilot-level first pass?
4. Does the Dune template emit enough stage-distinguishing fields for the pilot
   (`source_kind`, `source_family`, `price_unit`) and does the operator fetch
   path enforce the 30-credit ceiling before spend?

## Known Boundary

This implementation is not the paid pilot run. It is the auditable harness that
prevents the paid pilot from becoming a feature-fishing surface.

The template currently resolves the bonding-curve side. If the historical pilot
needs post-graduation AMM stage labels as well, the provider export must add a
separate AMM/GMGN source with the same required bar schema before the paid run.

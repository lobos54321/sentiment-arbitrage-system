# Gate-0.5 Backfill Pilot Tool Handoff

Date: 2026-06-17.
Status: implementation handoff for review. No provider spend was performed.

## Commit Scope

This handoff covers:

- `claudedocs/gate0.5-step1-pilot-run-card-2026-06-17.md`
- `claudedocs/gate0.5-step1-pilot-run-card-2026-06-17.sha256`
- `scripts/run-gate05-backfill-pilot.js`
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

## Verification Performed

- `node --check scripts/run-gate05-backfill-pilot.js`
- `node --test tests/run-gate05-backfill-pilot.test.mjs`
- `node scripts/run-gate05-backfill-pilot.js --help`
- prepare smoke against local `server_sentiment_arb.db` and latest observer
  snapshot. It selected `0` rows because that local DB is not the 36,941-row
  `sas_sentiment_current.db` used in the Phase-1 analysis; this is expected and
  confirms the tool requires the correct DB path explicitly.

## Review Focus

1. Is the prepare/evaluate split acceptable before provider spend?
2. Are reconciliation thresholds implemented as intended:
   - `>=0.90` pass;
   - `0.80..0.90` partial;
   - `<0.80` not feasible?
3. Is the stage-resolution proxy too weak for final feasibility, or acceptable
   as a pilot-level first pass?
4. Should the provider-window output be converted into a Dune SQL template now,
   or should the first paid pilot use the generic provider request JSON?

## Known Boundary

This implementation is not the paid pilot run. It is the auditable harness that
prevents the paid pilot from becoming a feature-fishing surface.


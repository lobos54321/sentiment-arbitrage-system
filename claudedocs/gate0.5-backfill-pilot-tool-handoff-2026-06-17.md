# Gate-0.5 Backfill Pilot Tool Handoff

Date: 2026-06-17.
Status: implementation handoff for review. No provider spend was performed.

## Commit Scope

This handoff covers:

- `claudedocs/gate0.5-step1-pilot-run-card-2026-06-17.md`
- `claudedocs/gate0.5-step1-pilot-run-card-2026-06-17.sha256`
- `scripts/run-gate05-backfill-pilot.js`
- `scripts/gate05-backfill-pilot-stage-tags-dune.template.sql`
- `scripts/run-dune-sql-export.py`
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

The paid pilot has two separate data inputs:

1. **Reconciliation/labeling bars** must match the observer's own price source
   (`geckoterminal` / `local_cache` / GMGN as applicable). These bars answer
   the apples-to-apples question: can the historical labeler reproduce the
   observer labels?
2. **Stage tags** come from the Dune template
   `scripts/gate05-backfill-pilot-stage-tags-dune.template.sql`. These tags are
   label-free curve-presence facts and are supplied to `evaluate` via
   `--stage-tags-jsonl`. They must not be used as label bars.

Run Dune through `scripts/run-dune-sql-export.py --max-credits 30` or an
equivalent wrapper. The exporter now fails closed if Dune status reports credits
above the cap before result fetch. It also fails closed if `--max-credits` is
set and the completed Dune status does not expose a recognized credit field.

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
  --stage-tags-jsonl <stage-tags.jsonl> \
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

For the paid pilot these bars should be observer-source OHLCV, not Dune
pump.fun curve-only bars. Curve-only bars would make label reconciliation a
source mismatch against the frozen observer labels and could create a false
`NOT_FEASIBLE` verdict on graduation-spanning dogs.

`evaluate` enforces this for observer-overlap rows: window bars must match the
frozen observer's `path_provider` and, when present, `path_source_kind`. A
provider/source mismatch exits fail-closed before the labeler runs.

## Discipline

- No candidate feature effect is computed.
- No `signal_type` / `is_ath` / `narrative_score` dog-rate is reported.
- No AUC, precision, recall, or lift is computed.
- Pilot rows are burned by `(token_ca, signal_ts)`.
- Sampling reports forbidden strata and only uses labelability-oriented strata.
- Labeling reuses `buildRawSignalOutcomeReport()` from
  `src/analytics/raw-signal-outcomes.js`.
- Stage-resolution is reported separately from outcome labelability and can be
  supplied from an independent stage-tag JSONL.
- Wrong premium DB usage is fail-closed before any pilot sample is emitted.
- Observer reconciliation includes `sustained_reason`, so sustained-definition
  disagreements can be classified instead of falling through.
- Dune exports can be capped with `--max-credits`; this is a real exporter gate,
  not just a handoff comment. Unknown completed cost with a cap set is
  fail-closed.
- Reconciliation-source mismatch is fail-closed before `buildRawSignalOutcomeReport()`.

## Verification Performed

- `node --check scripts/run-gate05-backfill-pilot.js`
- `python3 -m py_compile scripts/run-dune-sql-export.py`
- `node --test tests/run-gate05-backfill-pilot.test.mjs`
- `node scripts/run-gate05-backfill-pilot.js --help`
- wrong-DB smoke against local `server_sentiment_arb.db` now fails closed unless
  smoke override is explicitly supplied. This prevents the old silent 0-row or
  all-null-metadata pilot path.
- disagreement taxonomy tests cover coverage, baseline, unit, and sustained
  definition differences.
- an evaluate test verifies observer-source `indexed_ohlcv` bars can be used
  for reconciliation while separate Dune stage tags provide stage resolution.
- an evaluate test verifies Dune/curve bars are rejected when the observer
  expected Gecko/indexed OHLCV bars.

## Review Focus

1. Is the prepare/evaluate split acceptable before provider spend?
2. Are reconciliation thresholds implemented as intended:
   - `>=0.90` pass;
   - `0.80..0.90` partial;
   - `<0.80` not feasible?
3. Is the stage-resolution proxy too weak for final feasibility, or acceptable
   as a pilot-level first pass?
4. Does the two-input design preserve the source boundary: observer-source bars
   for label reconciliation, Dune stage tags only for stage evidence?
5. Is `--max-credits 30` sufficient for the actual Dune fetch path used in the
   paid pilot?

## Known Boundary

This implementation is not the paid pilot run. It is the auditable harness that
prevents the paid pilot from becoming a feature-fishing surface.

The stage-tag template currently resolves the bonding-curve side. If the
historical pilot needs post-graduation AMM/graduation labels as well, add a
separate AMM/GMGN stage source before relying on the stage gate as complete.

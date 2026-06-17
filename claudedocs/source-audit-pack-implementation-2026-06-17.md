# Source Audit Pack Implementation

Date: 2026-06-17
Branch: runtime-stability-marker-guard

## Scope

This is a read-only evidence-of-absence pack for the source-quality hypothesis.

It does not compute edge metrics, discrimination metrics, or source-vs-outcome
results. It reports only source field availability, degeneracy, join coverage,
and component-score coverage.

## Added Tool

- `scripts/build-source-audit-pack.js`
  - Inputs:
    - frozen `raw_signal_outcomes.snapshot.db`;
    - cumulative OOS feature table;
    - optional paper DB;
    - optional score DBs.
  - Join key: `(token_ca, signal_ts)`.
  - De-duplicates raw rows to one observation per signal key.
  - Emits:
    - full raw source distribution;
    - cohort raw source distribution;
    - cohort de-duplicated source distribution;
    - source precondition flags;
    - component-score coverage.
  - Fails closed if forbidden metric terms appear in the output JSON.

Forbidden output terms:

`lift`, `auc`, `precision`, `recall`, `cramers_v`, `mutual_info`, `chi2`,
`separation`, `p_dog`, `p_dud`.

## Test Coverage

```bash
node --check scripts/build-source-audit-pack.js
node --test tests/build-source-audit-pack.test.mjs
```

Result: 4/4 pass.

## Produced Pack

Output:

`/Users/boliu/sas-data-room/source-audit-pack-20260617T045216Z`

Manifest:

`/Users/boliu/sas-data-room/source-audit-pack-20260617T045216Z/manifest.json`

Report:

`/Users/boliu/sas-data-room/source-audit-pack-20260617T045216Z/source-audit-pack.json`

Report sha256:

`0cb414eae18347cb865d1b1e950dbeb75fede901a93c6285e4730a770a7d36b8`

## Pack Result

Verdict:

`SOURCE_AXIS_NULL_FOR_CURRENT_COHORT`

Key facts:

- Feature rows: 107
- Signal rows: 107
- Unique tokens: 106
- Joined signals: 107 / 107
- Raw rows for signal keys: 227
- De-duplicated raw rows: 107
- Collapsed raw rows: 120
- `source`: `premium_signals` 107 / 107
- `source_family`: `third_party_kline` 107 / 107
- `source_kind`: `indexed_ohlcv` 107 / 107

Source preconditions:

- `origin_has_variance`: false
- `family_kind_has_variance`: false
- `component_scores_have_cohort_coverage`: false
- `collinearity_measurable`: false

Component-score coverage:

- `score_details` DB copies: present copies have 0 rows.
- `opportunity_events`: 810 rows, 0 cohort-token overlap.
- `source_strength_score`: 0 non-null rows.

No-leakage grep on the produced pack:

All forbidden output terms returned `-1`.

## Interpretation

This pack closes the current source-quality hypothesis for the present OOS
cohort. Source origin and source family are not missing in an actionable sense:
they can be reconstructed, but they reconstruct to constants.

This pack does not imply a strategy result. It only says source-quality cannot
be tested as a discriminative axis with the current cohort fields.

The recommended next research direction is observability/sourcing redesign, not
a source gate.

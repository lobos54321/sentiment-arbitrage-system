# GOAL — Instrument-Forward Telegram Metadata + Raw Outcome Alignment

Status: active research/measurement goal. This is not a strategy goal.

## Objective

Stop historical backfill archaeology and make all future Telegram premium signals natively auditable by recording point-in-time Telegram metadata together with raw-path observer outcomes and stage/source provenance under one exact `(token_ca, signal_ts)` identity.

The goal is to produce aligned, forward-collected measurement data that can later support a clean preregistered OOS test of Telegram metadata features such as `signal_type / NEW_TRENDING vs ATH`, if and only if coverage, labelability, stage-resolution, and provenance QA pass.

## Why This Goal Exists

The Gate-0.5 historical backfill pilot ended with:

- `HISTORICAL_BACKFILL_NOT_FEASIBLE`
- labelable rate: `26 / 200 = 13%`
- observer/backfill dog-dud agreement: `74.19%`
- stage resolved rate: `42.5%`

Historical data is not clean enough for a fair held-out test. The correct move is instrument-forward: collect the right metadata and raw outcomes together from the same feed going forward.

## Non-Goals

This goal does not:

- test an edge;
- compute AUC/lift/precision/feature separation;
- select or promote a strategy feature;
- change gate/matrix/RR/liquidity/exit/live size;
- deploy to production;
- alter trading behavior.

## Required Forward Schema

For each future premium signal / raw outcome unit, preserve or project:

### Identity

- `remote_signal_id` if available
- `signal_id` if available
- `token_ca`
- `signal_ts`
- `source_message_ts`
- `receive_ts`
- exact join key used

### Telegram Metadata, Point-In-Time Only

- `signal_type`
- `is_ath`
- `narrative_score`
- `ai_narrative_tier`
- `signal_source`
- `raw_message_present`
- optional parsed channel/caller if already present or cheaply parseable

### Raw Outcome / Observer Provenance

- `raw_primary_tier`
- `raw_sustained_tier`
- `max_sustained_peak_pct`
- `observation_status`
- `kline_covered`
- `coverage_reason`
- `baseline_confidence`
- `same_source_path`
- `outlier_flag`
- `sustained_evaluable`
- `baseline_price_unit`
- `path_provider`
- `path_source_kind`
- `path_source_family`
- `path_price_unit`

### Stage / Source Diagnostics

- stage-at-signal proxy currently available from raw outcome/source fields
- stage source / reason
- stage resolved boolean
- source/provider distribution

## Required QA Outputs

Create a read-only QA command or report that emits only counts and missingness, never discrimination metrics:

- rows total
- unique `(token_ca, signal_ts)` count
- duplicate count
- metadata coverage by field
- raw outcome labelable rate
- stage resolved rate
- exact metadata/outcome join rate
- missing metadata reasons
- source/provider distribution
- date range
- schema version
- AUC/leak check: no `auc`, `precision`, `recall`, `lift`, `dog_rate_by_feature`, or feature-vs-label separation fields

## Acceptance Criteria for This Implementation Phase

PASS when:

1. There is a deterministic script or instrumentation path that joins/project Telegram metadata onto raw outcome units by exact key.
2. It works on current available remote premium snapshot + raw outcome snapshot as an offline proof, without changing production behavior.
3. It produces a durable QA artifact in `~/sas-data-room/...`.
4. Tests cover timestamp normalization, exact join, duplicate handling, missing metadata, and forbidden metric leakage.
5. No strategy/live files are changed.
6. No AUC/edge metric is computed.

## Decision After This Phase

- If forward/projection QA shows high metadata coverage and clean exact joins, next step is to enable the same projection in the daily/raw-path measurement flow or schedule it as an offline daily QA.
- If coverage remains poor or identity cannot be made exact, fix logging/projection before any prereg.
- Only after enough clean forward rows accumulate may a new prereg be drafted for `signal_type` or another metadata primary.

## Hard Prohibitions

- Do not use the historical backfill pilot as edge evidence.
- Do not rescue `unique_buyers`.
- Do not use leaked daily AUC artifacts.
- Do not compute candidate feature separation in this goal.
- Do not modify live strategy behavior.
- Do not deploy.

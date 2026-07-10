# CODEX GOAL Phase 4 - Evidence Recovery and Continuous Validation

Plan ID: `CAPTURE60-PHASE4-EVIDENCE-RECOVERY`

Status: `R0_R1_IMPLEMENTED_LOCALLY_H1_PENDING`

Observed at: `2026-07-09T22:47:00Z`

Deployed commit at observation time: `ef5ce69448d678509e3949b605acf9f9a66306c0`

Default operating mode: `read_only_and_shadow_only`

Promotion allowed: `false`

Production strategy change allowed: `false`

Human approval is required before the one-time production data repair, process restart,
infrastructure configuration change, paper/live enablement, or risk change.

## 0. Implementation Status (2026-07-10)

Implemented in the bounded R0/R1 work package:

- `scripts/kline_db_health_audit.py`: read-only header, integrity, table, sidecar, and process-reference audit;
- `scripts/evidence_clock_audit.py`: separates report publish time from evidence data-cut time and detects stale or mixed lineage;
- `scripts/sqlite_evidence_snapshot.py`: consistent source-read-only snapshot through the SQLite backup API;
- `src/market-data/sqlite-file-health.js`: structured missing/invalid-header fail-closed preflight;
- `src/market-data/kline-repository.js`: header and `quick_check` validation before WAL/schema writes;
- `src/tracking/kline-collector.js` and `src/optimizer/fixed-evaluator.js`: active writer/evaluator paths now reuse the same existing-file health contract instead of creating a missing cache;
- `scripts/paper_trade_monitor.py`: kline attribution readers now use SQLite `mode=ro` and cannot create a missing cache;
- `scripts/run-raw-path-observer.js`: validates the kline repository before opening the writable raw observer DB and exits with structured code 78 on failure;
- `scripts/kline_db_recovery.py`: default dry-run recovery planner; execution requires H1 approval, maintenance acknowledgement, operator identity, source hash, and zero active file references; database and sidecar moves are atomic no-clobber operations;
- `/proc` reference inspection treats every non-transient FD visibility error as an execution blocker;
- `scripts/phase4_h1_recovery_approval_packet.py`: creates a human-review packet but never grants approval;
- focused Python and Node tests for read-only behavior, zero headers, stale clocks, snapshots, quarantine races, temporary initialization, idempotency, active-worker blocking, and active runtime open paths.

Local verification on the clean `ef5ce69448d678509e3949b605acf9f9a66306c0` base:

- Phase 4 Python tests: 18 passed;
- SQLite/repository/runtime Node tests: 9 passed;
- raw-path regression tests: 18 passed;
- all five Python self-tests and all relevant syntax checks passed;
- a 136,814,592-byte all-zero fixture retained SHA-256 `8c3fc8074bf14438202a036cd7bf2d700f045739a4fb84fa5df6a3cb2b9578ab` before and after audit plus recovery dry-run.

Not performed:

- no production audit artifacts have been mutated;
- no recovery `--execute` has run against `/app/data`;
- no worker has been stopped or restarted;
- no infrastructure setting has changed;
- H1 has not been approved;
- R2 and later packages have not started;
- `promotion_allowed` remains `false`.

The implementation must be deployed and R0 rerun against the production persistent volume before the
real H1 packet can be reviewed. Local self-test artifacts are not production approval evidence.

## 1. Executive Decision

Phase 4 does not add another strategy search loop yet. It first restores a trustworthy evidence clock.

The system contains valuable raw data, candidate observations, OOS registries, paper evidence, and
source experiments. However, several reports are being refreshed against stale inputs while the active
kline database is invalid. Continuing strategy comparison in that state would produce current-looking
timestamps over old or incomplete evidence.

Phase 4 has one immediate objective:

> Restore continuous signal, kline, raw-path, raw-dog, capture, Phase 3, P4, and P8 evidence collection,
> and make every published metric prove which data cut it represents.

After recovery, Phase 4 resolves three frozen questions without changing production trading:

1. Does the approved wide-net paper experiment have positive executable tail value after robust
   deduplication and winner sensitivity checks?
2. Do the frozen P4 two-dimensional cross families repeat in a second disjoint OOS window?
3. Does pump.fun add incremental gold/silver discovery beyond Telegram after comparable 24-hour outcome
   maturation?

## 2. Verified Starting State

| Area | Verified state | Consequence |
| --- | --- | --- |
| Active kline database | `/app/data/kline_cache.db` is 136,814,592 bytes and its first 64 bytes are all zero | It is not valid SQLite |
| Raw path observer | Repeats `SqliteError: file is not a database` | New path evidence is not written |
| Raw dog discovery | Repeats `database disk image is malformed` | New dog maturation is unreliable |
| Premium source | Latest signal was about 364 minutes old and runtime was `stale_fail_closed` | New entries correctly stop, but evidence is starved |
| Capture clock | Stage state points to a July 3 capture; `latest/capture_discovery_24h.json` was last updated July 4 | Current timestamps do not imply a current denominator |
| AutoLoop scheduler | Active worker refreshes `oos,finalize`, not a fresh primary capture | Reports can refresh on stale capture input |
| Phase 3 ledger | 452 rows, 260 closed; last write `2026-07-09T00:37:30Z` | Approved experiment is not continuously scheduled |
| Phase 3 outcome | 63 wins, 197 losses, median `-24.66%`; positive mean is tail dominated | Wide-net is a control arm, not a proven strategy |
| Strict P4/FDR review | 14 first-window statistical watches, 0 two-window confirmations | No cross is promotion evidence |
| P8 24h source trial | 20,044 pump rows, 19,180 unique pump tokens, only 1 pump-linked raw gold/silver token | Signal volume exists; comparable outcomes do not |
| Rolling 7d paper evidence | 11 closed rows, 2 wins, 9 losses, median about `-9.37%` | No production promotion case exists |
| Artifact size | Verdict about 6.4 MB, summary about 604 KB, handoff about 816 KB | Control-plane responses are too expensive |
| Logs | `node.log` about 212 MB and `paper-trader.log` about 243 MB | Rotation and error-storm suppression are required |

`/app/data/candidate_shadow_kline_cache.db` passes `quick_check`, but its latest bar is
`2026-07-02T20:38:00Z`. It must not be used as a current-data fallback. The older file named
`kline_cache.db.corrupt-20260624T010757Z` has a SQLite header but must not be restored without a full
integrity and provenance audit.

## 3. Non-Negotiable Guardrails

Phase 4 may automatically modify only:

- read-only health and integrity auditors;
- report and artifact generation;
- shadow-only experiment workers;
- idempotent scheduling code for read-only/shadow workers;
- compact dashboard status endpoints;
- tests and documentation.

Phase 4 must not automatically modify:

- production strategy or entry policy;
- hard gate or exit gate;
- `final_entry_contract`;
- A_CLASS runtime mode;
- live or production paper executor enablement;
- canary size, position size, concurrency, wallet, secrets, or risk limits;
- historical labels or frozen OOS definitions.

No damaged database may be deleted. It must be quarantined with a timestamp and audit manifest.

No stale alternate database may silently replace current evidence.

No report may publish a capture verdict when its primary capture input predates the report run.

## 4. Definition of Done

### Data plane

- `kline_cache.db` has a valid `SQLite format 3` header.
- `PRAGMA quick_check` returns `ok`.
- Kline row count and maximum timestamp advance across two checks at least 15 minutes apart.
- Raw path observer completes 10 consecutive cycles without database errors.
- Raw dog discovery completes 3 consecutive cycles without database errors.
- Premium source connection heartbeat is fresh, separately from channel signal age.
- Runtime remains fail-closed whenever actual source health is stale.

### Evidence clock

- A new `capture_discovery_24h.json` is generated from a post-recovery data cut.
- `primary_capture_generated_at >= run_started_at` for every new full run.
- Main artifacts contain `run_id`, `generated_at`, `data_cut_at`, `input_max_ts`, `commit`, and
  `artifact_age_sec_at_publish`.
- `latest` never points at an unfinished or stale full run.
- Compact latest status reports the same run ID as the artifact manifest.

### Continuous experiments

- Phase 3 ledger advances in two consecutive scheduled runs.
- Phase 3 worker is idempotent and does not duplicate a signal-contract pair.
- P4 window 2 starts only after `post_recovery_clean_at` and uses frozen definitions.
- P8 outcome maturation records coverage and selection probabilities.
- A daily and weekly evidence package is reproducibly generated.

### Safety

- `promotion_allowed=false` throughout Phase 4.
- No forbidden production strategy, gate, executor, wallet, canary, or risk file changes.
- No live position size or runtime mode change.

## 5. Execution Order

The order is binding:

```text
R0 Freeze and Audit
  -> R1 Kline Safety Patch
  -> H1 Human Data-Repair Approval
  -> R2 One-Time Data Recovery
  -> R3 Source and Observer Recovery
  -> R4 Fresh Capture Clock
  -> R5 Continuous Phase 3
  -> R6 P4 Window 2
  -> R7 P8 Comparable Outcomes
  -> R8 Evidence Packages and Control-Plane Compaction
  -> H2 Human Research Review
```

R8 code can be developed in parallel with R1-R4, but its reports are not valid until R4 passes.

## 6. Work Package R0 - Freeze and Audit

Purpose: preserve the exact failure state before mutation and identify every process that can write the
active kline database.

### Implementation

Add:

- `scripts/kline_db_health_audit.py`
- `scripts/evidence_clock_audit.py`

`kline_db_health_audit.py` reports:

- file existence, size, inode, mtime, first 64 bytes, and SQLite header validity;
- read-only `quick_check` result;
- tables, row counts, minimum and maximum timestamps when readable;
- WAL/SHM presence;
- alternate databases with the same fields;
- process command lines and `/proc/*/fd` references to the target database;
- classification `HEALTHY`, `INVALID_HEADER`, `MALFORMED`, `LOCKED`, or `MISSING`.

`evidence_clock_audit.py` reports:

- latest full run ID and state;
- primary capture path and generation time;
- report generation time and data-cut time;
- stale age for capture, P4, Phase 3, P8, and latest-status;
- mismatched run IDs;
- classification `CURRENT`, `STALE_INPUT`, `UNFINISHED_RUN`, or `MIXED_RUN_LINEAGE`.

### Outputs

- `/app/data/recovery/evidence_recovery_snapshot.json`
- `/app/data/recovery/kline_db_health_before.json`
- `/app/data/recovery/evidence_clock_before.json`

### Acceptance

- Auditors open databases read-only.
- Self-tests cover valid SQLite, zero header, malformed file, missing file, and stale artifact cases.
- No process restart and no data mutation.

### Verification commands after implementation

```bash
python3 scripts/kline_db_health_audit.py \
  --db /app/data/kline_cache.db \
  --alternate-db /app/data/candidate_shadow_kline_cache.db \
  --out /app/data/recovery/kline_db_health_before.json

python3 scripts/evidence_clock_audit.py \
  --agent-runs /app/data/agent_runs \
  --latest-status /app/data/agent_runs/latest/reviewer_verdict.json \
  --out /app/data/recovery/evidence_clock_before.json

python3 scripts/sqlite_evidence_snapshot.py \
  --source /app/data/raw_signal_outcomes.db \
  --out /app/data/recovery/snapshots/raw_signal_outcomes.<run-id>.db \
  --manifest /app/data/recovery/snapshots/raw_signal_outcomes.<run-id>.manifest.json
```

## 7. Work Package R1 - Kline Safety Patch

Purpose: prevent another invalid database from being opened, overwritten, or silently treated as empty.

### Expected code scope

- `src/market-data/kline-repository.js`
- `scripts/run-raw-path-observer.js`
- new `scripts/kline_db_recovery.py`
- focused tests for health, quarantine, initialization, and concurrent-open behavior.

The final diff must be narrowed after writer ownership is proven by R0. Do not edit every consumer merely
because it references `KLINE_DB`.

### Required behavior

1. Validate the SQLite header before any writer opens the file.
2. Run `quick_check` before enabling writes.
3. On invalid data, fail closed with a structured health artifact and stop retry storms.
4. Never truncate or recreate the configured path in place.
5. Initialize through `temp -> schema -> quick_check -> fsync -> atomic rename`.
6. Preserve a last-known-good snapshot after successful checkpoints.
7. Require an approval marker and operator identity for recovery.
8. Support `--dry-run`; mutation requires `--execute` plus approval marker.
9. Write a manifest with old/new inode, hashes, timestamps, checks, and operator.

### Dry-run command contract

The following command is non-mutating for the target database and is the only recovery command allowed
before H1 approval:

```bash
python3 scripts/kline_db_recovery.py \
  --db /app/data/kline_cache.db \
  --dry-run \
  --out /app/data/recovery/kline_recovery_dry_run.json
```

`--execute` is rejected unless both an H1 approval marker and a maintenance acknowledgement marker are
present, the CLI operator matches the approved operator, the approved source hash still matches, and no
process has an open file descriptor to the database or its WAL/SHM sidecars.

Generate the H1 review packet after the R0 reports, dry-run, tests, and diff-scope report exist:

```bash
python3 scripts/phase4_h1_recovery_approval_packet.py \
  --kline-health /app/data/recovery/kline_db_health_before.json \
  --evidence-clock /app/data/recovery/evidence_clock_before.json \
  --recovery-dry-run /app/data/recovery/kline_recovery_dry_run.json \
  --test-report /app/data/recovery/r1_test_report.json \
  --diff-scope /app/data/recovery/r1_diff_scope.json \
  --out /app/data/recovery/h1_recovery_approval_packet.json \
  --out-md /app/data/recovery/h1_recovery_approval_packet.md
```

This packet always writes `approval_granted=false`; a separate human action is required to create the
approval marker.

### Tests

- all-zero 136 MB file is rejected without truncation;
- valid SQLite is unchanged;
- malformed data is quarantined only with explicit execution approval;
- temporary initialization passes `quick_check` before rename;
- repeated recovery is idempotent;
- concurrent worker presence blocks execution deterministically;
- no strategy, gate, executor, wallet, or risk code is touched.

## 8. Human Checkpoint H1 - One-Time Data Repair

H1 is required because the next step changes a production persistent data path and restarts data workers.

The approval packet contains:

- R0 audit outputs;
- R1 tests and diff scope;
- exact quarantine path;
- whether the new database is empty or rebuilt from a verified source;
- backfill scope and provider budget;
- worker stop/start sequence;
- rollback command;
- confirmation that trading policy and risk are unchanged.

H1 authorizes only:

1. pausing kline-reading and kline-writing observer workers;
2. atomically quarantining the invalid file;
3. creating a verified SQLite database;
4. restarting the same workers;
5. bounded research backfill.

H1 does not authorize strategy, gate, mode, executor, canary, wallet, or risk changes.

## 9. Work Package R2 - One-Time Data Recovery

Purpose: restore a valid active database without destroying forensic evidence.

### Operator sequence

1. Create `/app/data/recovery/maintenance_requested.json`.
2. Confirm all kline users acknowledge maintenance mode.
3. Run recovery dry-run and compare it with H1 approval.
4. Execute quarantine and initialization.
5. Run header validation and `quick_check` on the new database.
6. Remove maintenance marker.
7. Restart raw-path, raw-dog, lifecycle, paper monitor, and research workers.
8. Start bounded backfill.

### Expected command contract

```bash
python3 scripts/kline_db_recovery.py \
  --db /app/data/kline_cache.db \
  --quarantine-dir /app/data/recovery/quarantine \
  --approval /app/data/recovery/kline_recovery_approval.json \
  --operator "$OPERATOR" \
  --dry-run

python3 scripts/kline_db_recovery.py \
  --db /app/data/kline_cache.db \
  --quarantine-dir /app/data/recovery/quarantine \
  --approval /app/data/recovery/kline_recovery_approval.json \
  --operator "$OPERATOR" \
  --execute
```

Do not directly restore the June 24 file or stale candidate-shadow cache. They are read-only,
provenance-tagged backfill sources only after full integrity checks.

### Acceptance and rollback

- Invalid file is preserved under a timestamped quarantine name.
- New header, schema, and `quick_check` pass.
- Recovery manifest is complete.
- Rollback target exists and is not modified.
- On failure, quarantine the new database separately and return to fail-closed unless a verified
  last-known-good snapshot exists.

## 10. Work Package R3 - Source and Observer Recovery

Purpose: prove that the data plane advances after database repair.

### Source-health contract

Split source health into:

- `source_connection_heartbeat_age_sec`: whether the Telegram/premium session is alive;
- `latest_signal_age_sec`: when the channel last emitted a qualifying signal.

A quiet channel must not be confused with a disconnected source. Existing fail-closed entry behavior
remains unchanged until the connection-health contract is independently verified.

### Worker health rows

Every worker writes:

- `worker_name`, `started_at`, `last_success_at`, and `last_error_at`;
- consecutive success and failure counts;
- `input_max_ts`, `output_max_ts`, rows read, and rows written;
- `database_health_classification`.

### Acceptance

- Source connection heartbeat age is below 120 seconds for 30 continuous minutes.
- Raw path observer has 10 consecutive successes.
- Raw dog discovery has 3 consecutive successes.
- Candidate observer continues to report exactly 84 candidates.
- Raw outcome maximum timestamp advances.
- No database malformed errors occur for 30 minutes.
- Supervisors use bounded backoff and do not create unbounded logs.

## 11. Work Package R4 - Fresh Capture Clock

Purpose: ensure a rolling report is generated from its declared rolling window.

### Scheduler design

Keep the 15-minute OOS/finalize refresh worker, but add a separate full-capture refresh worker.

- Full 24h capture cadence: every 4 hours.
- Full 48h and 72h capture cadence: daily.
- Every full run writes a new immutable run directory.
- `latest_manifest.json` is atomically replaced only after publish eligibility passes.
- OOS refresh writes a new OOS run and never rewrites old primary-capture lineage.

Expected files:

- `scripts/autoloop_capture_refresh_worker.py`
- `scripts/agent_autoloop_stage_runner.py`
- `scripts/agent_capture_discovery_loop.py`
- `src/web/dashboard-server.js`

### Publish eligibility

A full run may become `latest` only when:

- candidate count is 84;
- observation coverage is at least 99%;
- raw dog rows are complete;
- mesh-eligible identity join is at least 99%;
- `primary_capture_generated_at >= run_started_at`;
- `data_cut_at` is inside the requested window;
- primary artifact age is below 2 hours at publish;
- required artifacts exist, tests pass, and no report command failed.

On failure, preserve the previous valid `latest` and publish a separate failed-run status.

### Compact status contract

```text
run_id
commit
generated_at
data_cut_at
input_max_ts
artifact_age_sec
classification
top_blocker
next_action
promotion_allowed
capture_stage_rates
worker_health
links_to_full_artifacts
```

### Acceptance

- New 24h capture contains post-recovery rows.
- Latest status, manifest, verdict, and capture share one run ID.
- July 3/4 captures remain historical and cannot publish as current.
- Runner always writes `finished_at`, `exit_code`, and `timed_out`.

## 12. Work Package R5 - Continuous Phase 3 Wide-Net Evidence

Purpose: make the approved independent paper experiment advance continuously and remain separate from
production paper trading.

### Scheduler

- Cadence: every 15 minutes.
- Output DB: `/app/data/phase3_wide_net_paper_contract.db` only.
- Output summary: `/app/data/agent_runs/latest/phase3_wide_net_paper_experiment_summary.json`.
- Idempotency key: `(signal_id, contract_version, exit_policy_id)`.
- No writes to production `paper_trades`.

Expected files:

- `scripts/phase3_wide_net_paper_worker.py`
- new `scripts/run_phase3_wide_net_paper_worker.sh` or equivalent supervisor;
- worker-health integration in `src/web/dashboard-server.js`.

### Daily metrics

- eligible unique signals and tokens;
- quote executable, no-fill, missing entry price, and missing path counts;
- entry-delay buckets 5, 10, 15, 20, and 30 seconds;
- unique-token and token-time-cluster deduped results;
- per-trade median, mean, p10, p25, p75, p90, and p99;
- rolling-24h ROI median and distribution;
- drop-top-1 and drop-top-3 sensitivity;
- hard-stop fill stress at `-25%`, `-30%`, and observed quote-guard floor;
- fees, slippage, open, closed, and censored rows.

### Decision schedule

- Day 1-6: collection only.
- Day 7: predeclared futility and integrity review; human decides whether to continue.
- Day 14: final experiment review.

No early promotion is allowed. A strongly negative median is evidence against wide-net entry even when
extreme winners keep the arithmetic mean positive.

### Acceptance

- Ledger maximum `created_at` advances across two scheduled runs.
- No duplicate idempotency keys.
- Missing-path periods appear in the denominator audit.
- Experiment pauses when source or path evidence is unhealthy and never fabricates outcomes.

## 13. Work Package R6 - P4 Two-Dimensional Cross Window 2

Purpose: test whether first-window statistical watches repeat without redefining them.

### Freeze contract

- Freeze the current 14 q-passing family definitions and fingerprints.
- Do not add families after window 2 starts.
- Preserve window 1 as historical evidence.
- Set `window2_start = post_recovery_clean_at`.
- Use only events after `window2_start`.
- Exclude the corrupted-data interval.

### Validation fields

- unique-token and token-time-cluster denominators;
- capture stage under test;
- selected and global stage rates;
- lift and exact one-sided p-value;
- BH-FDR q-value;
- direction agreement with window 1;
- negative-control rate;
- data/context blockers and volume/kline eligibility.

Volume/kline families remain blocked until post-recovery coverage is at least 80%. Clean non-volume
dimensions may proceed independently.

### Verdicts and outputs

- `WINDOW2_COLLECTING`
- `WINDOW2_TOO_SMALL`
- `WINDOW2_DIRECTION_REVERSED`
- `WINDOW2_NOT_SIGNIFICANT`
- `TWO_WINDOW_OOS_CONFIRMED_PENDING_HUMAN_REVIEW`

Outputs:

- `/app/data/agent_runs/latest/p4_window2_freeze_registry.json`
- `/app/data/agent_runs/latest/p4_window2_validation.json`
- `/app/data/agent_runs/latest/p4_two_window_decision.json`

Confirmation requires the registered sample rule, q threshold, same direction, and negative controls.
It still does not authorize paper or production promotion.

## 14. Work Package R7 - P8 Comparable Pump.fun Outcomes

Purpose: measure incremental discovery, not raw source volume.

### Outcome design

1. Persist 100% of pump.fun token identities and timestamps.
2. Mature outcomes to 5m, 15m, 60m, 2h, 6h, 12h, and 24h where available.
3. If provider budget cannot label every token, use a deterministic stratified sample.
4. Store inclusion probability and stratum for every selected token.
5. Compare only equally mature, equally eligible cohorts.
6. Report weighted estimates whenever sampling is used.
7. Deduplicate by token and token-time cluster.

### Required metrics

- selected-cohort outcome coverage;
- pump-only, Telegram-only, and overlap denominators;
- raw and weighted gold/silver rates;
- incremental gold/silver unique tokens;
- time-to-detection difference;
- quote and executable-route coverage;
- provider failure and censoring rates.

### KOL/X dependency rule

Influence/KOL enrichment starts only when selected-cohort 24h outcome coverage is at least 80% and the
comparison contains enough matured gold/silver outcomes for a directional audit.

KOL/X acquisition uses cached offline snapshots through the existing `agent-reach` shadow layer.
Runtime entries never depend on live X availability.

### Acceptance

- P8 no longer compares thousands of unlabeled pump tokens against a small labeled Telegram denominator.
- Every incremental claim includes denominator, maturity, coverage, and selection method.
- `promotion_allowed=false`.

## 15. Work Package R8 - Evidence Packages and Control-Plane Compaction

Purpose: make daily operations reviewable without reading hundreds of megabytes of logs or multi-megabyte
verdict files.

### Add

- `scripts/daily_evidence_package.py`
- `scripts/weekly_evidence_package.py`

Daily path and files:

```text
/app/data/daily_reviews/YYYY-MM-DD/
  data_health.json
  capture_24h.json
  phase3_wide_net_24h.json
  p4_window2_status.json
  p8_source_status.json
  paper_trade_24h.json
  daily_review.md
```

Weekly path and files:

```text
/app/data/weekly_reviews/YYYY-Www/
  7d_raw_dog_summary.json
  7d_paper_trade_summary.json
  7d_phase3_wide_net_summary.json
  7d_p4_cross_summary.json
  7d_p8_source_summary.json
  7d_data_health_summary.json
  weekly_review.md
```

### Size and retention controls

- Compact verdict target: at most 250 KB.
- Compact summary target: at most 100 KB.
- Compact handoff target: at most 100 KB.
- Move command diagnostics into separate NDJSON or per-command files.
- Keep immutable full runs; `latest` contains compact artifacts or an atomic manifest.
- Rotate node, paper-trader, and lifecycle logs at 50 MB with five retained generations.
- Do not delete raw databases in Phase 4.

### Acceptance

- Daily package generates for two consecutive days.
- Weekly package is reproducible from raw databases.
- Main status does not load multi-megabyte artifacts.
- Log growth is bounded and error storms are summarized.

## 16. Timeline and Ownership

| Time from H1 approval | Work | Owner | Dependency |
| --- | --- | --- | --- |
| Before H1 | R0 audit, R1 patch, tests, approval packet | Codex | None |
| T0 to T+2h | Quiesce writers, quarantine invalid DB, initialize verified DB | Human operator with Codex instructions | H1 |
| T+2h to T+4h | Restart workers, bounded backfill, 30-minute smoke | Codex verification | R2 |
| T+4h to T+24h | Collect first clean data window | System | R3 |
| T+24h | Publish first fresh capture and clock audit | System plus Codex review | R4 |
| Day 1 onward | Phase 3 worker every 15 minutes | System | R3/R5 |
| Day 1 onward | P4 window 2 accumulation | System | R4/R6 |
| Day 1 onward | P8 comparable outcome maturation | System | R3/R7 |
| Day 2 | First daily package | System | R8 |
| Day 7 | Phase 3 futility/integrity review and weekly package | Codex plus human review | R5/R8 |
| Day 14 | Phase 3 final experiment review | Codex plus human review | R5 |

Times are operational targets, not evidence shortcuts. A failed health check pauses the evidence clock.

## 17. Human Checkpoint H2 - Research Decision

H2 occurs only after the first post-recovery weekly package.

The human receives three separate decisions:

1. `WIDE_NET_REJECT`, `WIDE_NET_CONTINUE`, or `WIDE_NET_SHADOW_FILTER_RESEARCH`;
2. `P4_NO_CONFIRMED_FAMILIES` or `P4_CONFIRMED_FAMILIES_PENDING_PROPOSAL`;
3. `P8_NO_INCREMENTAL_VALUE`, `P8_CONTINUE_ACCUMULATION`, or `P8_KOL_ENRICHMENT_READY`.

H2 never automatically enables production or live trading. A paper proposal or production change is a
new, separately scoped approval.

## 18. Stop Conditions

Stop the current work package and report when:

- recovery would overwrite or delete forensic evidence;
- writer ownership is unknown;
- the same repair fails three times;
- SQLite validation fails after initialization;
- source heartbeat remains stale after worker recovery;
- backfill would exceed the declared provider budget;
- a fix requires changing strategy, gates, A_CLASS, executor, wallet, canary, or risk;
- a report cannot prove its input data cut;
- repeated crashes threaten data integrity;
- a new secret or credential is required.

## 19. Phase 4 Daily Summary Contract

Every daily summary begins with:

```text
verdict:
next_action:
promotion_allowed: false
human_approval_required:
current_data_cut:
capture_artifact_age_sec:
kline_db_health:
source_connection_health:
raw_path_worker_health:
raw_dog_worker_health:
phase3_ledger_last_write:
phase3_closed_unique_tokens:
phase3_rolling24h_roi_median:
p4_window2_status:
p4_confirmed_family_count:
p8_matured_outcome_coverage:
p8_incremental_gold_silver_unique:
top_blocker:
```

## 20. Immediate Next Action

The next implementation task is exactly:

> Implement R0 and R1 only: read-only kline/evidence-clock auditors plus guarded recovery code and tests.
> Produce the H1 approval packet. Do not execute database recovery, restart workers, change
> infrastructure configuration, or continue to R2 without explicit human approval.

Until R3 and R4 pass, all new strategy, P4, Markov, Strategy Memory, and P8 conclusions must be labeled
`DATA_CLOCK_BLOCKED` or `PRE_RECOVERY_REFERENCE_ONLY`.

# P0-A / P0-B / P0-C Controlled Recovery Release Unit

Release ID: `SAS-P0-ABC-CONTROLLED-RELEASE-2026-08-08`

Baseline: `f592c47a137871c4dd70911a4c6d783297c15395`

Release branch: `release/p0-abc-recovery-20260808`

Deployment target: GitHub `main` → Zeabur production auto-deploy

Deployment policy: one atomic commit and one production rollout

Rollback policy: revert the single release commit or redeploy baseline SHA

Promotion allowed: `false`

Strategy change allowed: `false`

Automatic runtime-mode change allowed: `false`

Paper/live enablement allowed: `false`

P0-D paper E2E: `LOCKED`

---

## 1. Why this is one atomic deployment unit

P0-A, P0-B and P0-C remain separately attributable subpackages, but the production rollout is atomic.
The final P0-B governance hashes, CI evidence and runtime registries describe the complete P0-A +
P0-B + P0-C tree. Artificial intermediate commits would create unverified states in which governance
hashes, entry-point registries or worker topology do not match the code they describe. The safe unit is
therefore the already-tested final tree, with one SHA for deployment and rollback.

---

## 2. Controlled subpackages

### P0-A — V27 read-model worker reachability and health

Owned concerns:

- always-on V27 read-model refresh worker;
- atomic worker status and shared-lock semantics;
- public-safe read-model worker health;
- schema/freshness fail-closed behavior;
- no strategy, gate, executor or mode-policy changes.

Primary implementation:

- `scripts/v27_read_model_refresh.py`
- `src/index.js`
- `src/web/dashboard-server.js`
- `scripts/run_zeabur_services.sh`
- `test_v27_read_model_refresh.py`
- `tests/index-sidecar-config.test.mjs`
- P0-A ledger in `SENTIMENT_ARBITRAGE_SYSTEM_RECOVERY_MASTER_PLAN.md`

### P0-B — Mode Readiness contract convergence

Owned concerns:

- verifier semantics for authentication, direct writes and static policy;
- background-job, entry-point, feature-flag, error, concurrency and service-probe registries;
- reproducible runtime-config and spec-impact hashes;
- mode readiness remains evidence-derived and fail-closed.

Primary implementation:

- `scripts/v27_basic_contract_readiness.py`
- `test_v27_basic_contract_readiness.py`
- `config/v27-*.json` governance files changed by this release
- `docs/agents/P0_B_MODE_READINESS_CONVERGENCE.md`

### P0-C — Accepted frozen evaluator snapshot to AutoLoop

Owned concerns:

- indexed `observed_at` selective snapshot extraction;
- real `EXPLAIN QUERY PLAN` range-search evidence;
- producer acceptance anchor for snapshot ID, snapshot-specific manifest path and manifest SHA-256;
- authoritative consumer revalidation of four DBs, manifest, freshness, size, SHA, quick-check,
  watermarks, skew, output cap, disk reserve and lock budget;
- snapshot-specific lineage through AutoLoop, stage runner and OOS refresh;
- producer health separated from authoritative consumer readiness;
- one production snapshot producer owner.

Primary implementation:

- `scripts/cross_db_evaluator_snapshot.py`
- `scripts/evaluator_db_contract.py`
- `scripts/agent_capture_discovery_loop.py`
- `scripts/agent_autoloop_stage_runner.py`
- `scripts/autoloop_oos_refresh_worker.py`
- `src/web/evaluator-snapshot-preflight.js`
- P0-C portions of `src/web/dashboard-server.js`, `src/index.js` and `scripts/run_zeabur_services.sh`
- evaluator/AutoLoop focused tests and CI workflow
- `docs/agents/P0_C_FROZEN_EVALUATOR_SNAPSHOT_AUTOLOOP_RECOVERY.md`

---

## 3. Pre-deploy immutable gates

The release may reach `main` only when all of the following hold on the exact release tree:

1. no forbidden strategy/gate/executor/wallet/risk paths changed;
2. `promotion_allowed`, `strategy_change_allowed`, `automatic_runtime_change_allowed` and
   `paper_enablement_allowed` remain false;
3. Python recovery/readiness regression passes;
4. Node 20 evaluator/dashboard/runtime regression passes;
5. Basic Readiness reports 136/136 pass and no blocking contracts;
6. `CICDMergeGateContract` and `SpecChangeImpactAnalysisContract` pass;
7. generated client, spec validation, JSON, Python, Node, Bash and whitespace checks pass;
8. independent maker/checker review is APPROVE;
9. release commit is based directly on `f592c47` with no unrelated merge.

---

## 4. Deployment sequence

```text
release branch push
→ GitHub pull request
→ v27-readiness workflow green
→ squash/merge disabled; preserve exact atomic release commit
→ fast-forward main to the release SHA
→ Zeabur creates production deployment for the same SHA
→ public health commit matches release SHA
→ production acceptance runbook
```

No P0-D paper E2E action is permitted during this sequence.

---

## 5. Production acceptance chain

The release is not accepted merely because Zeabur reports RUNNING or `/health` returns 200.
Acceptance requires one continuous lineage:

```text
production release SHA
→ evaluator snapshot producer alive and single-owned
→ snapshot_status.last_accepted_snapshot
→ snapshot-specific manifest accepted
→ producer manifest SHA matches actual manifest SHA
→ four snapshot files exist and match size/SHA/quick-check
→ indexed candidate-table query plan proves observed_at range SEARCH
→ source read-lock, disk reserve, output cap and cross-DB skew pass
→ authoritative Python preflight accepted
→ AutoLoop child receives the same snapshot-specific paths
→ evaluator_snapshot_provenance.v1 has the same snapshot_id and manifest_sha256
→ fresh primary capture materializes under that lineage
```

Required evidence fields:

- `/health.commit`
- `/health.evaluator_snapshot_worker.status`
- `/health.evaluator_snapshot_worker.snapshot_id`
- `/health.evaluator_snapshot_worker.manifest_sha256`
- `/health.evaluator_snapshot_worker.consumer_ready`
- `/health.evaluator_snapshot_worker.consumer_state`
- producer `snapshot_status.json`
- accepted `manifest.json`
- both indexed selection reports and query plans
- authoritative preflight response
- AutoLoop runner status provenance
- latest verdict/capture provenance

---

## 6. P0-D lock

P0-D paper E2E remains locked until all production acceptance evidence above is present and mutually
consistent. In particular, none of the following is sufficient to unlock P0-D:

- Zeabur deployment status `RUNNING`;
- HTTP 200 from `/health`;
- `producer_accepted` without authoritative consumer readiness;
- a manifest with `accepted=true` but no producer SHA anchor;
- an AutoLoop run without matching snapshot provenance;
- any same-window discovery result;
- Basic Readiness being green.

Unlocking P0-D requires a separate human-reviewed decision after this release is accepted.

---

## 7. Rollback

Rollback triggers include:

- deployment commit mismatch;
- duplicate snapshot producer;
- source DB integrity degradation;
- snapshot producer restart storm;
- snapshot status/manifest/SHA mismatch;
- candidate-table full scan or read-lock budget failure;
- disk reserve/output cap failure;
- authoritative consumer preflight rejection that cannot be explained by normal startup grace;
- AutoLoop reading `current` aliases or active DBs rather than snapshot-specific paths.

Rollback steps:

1. disable `EVALUATOR_SNAPSHOT_WORKER_ENABLED` if the producer itself is creating pressure;
2. revert the single release commit or redeploy baseline `f592c47`;
3. retain the last accepted snapshot, producer status, failed status and logs as incident evidence;
4. confirm AutoLoop returns fail-closed rather than falling back to active DBs;
5. keep P0-D locked.

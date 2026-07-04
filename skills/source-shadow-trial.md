# source-shadow-trial

When to use: adding, validating, or operating a shadow-only external signal/source trial
(`pump.fun`, another launch stream, source/source_component EV comparison) where the source must
not touch production decisions, gates, executor, canary, wallet, or risk.

Inputs: source shadow DB, source comparison artifact, worker status artifact, process supervisor
state, root deployment commit, existing raw dog denominator, Telegram/premium source denominator.

## Procedure

1. Keep the new source physically isolated from production decision tables. It may write only its
   own shadow DB and read-only agent artifacts.
2. Publish source denominators and overlap matrices before interpreting any lift:
   source signal rows, unique tokens, raw gold/silver rows seen by source, overlap with Telegram,
   source-only raw dog candidates.
3. The trial must survive container restart/redeploy. A long-running comparison cannot depend on a
   hand-started shell; it must be supervised by the runtime or a dedicated worker process.
4. Every artifact must state `promotion_allowed=false`, `strategy_change_allowed=false`, and
   `production_impact=zero_shadow_only`.
5. Do not promote or route source events into paper/live until the trial window and human
   governance requirements are complete.

## Output contract

`source_denominators`, `overlap_matrix`, `raw_gold_silver_denominator`,
`trial_days_required`, `promotion_allowed`, `strategy_change_allowed`, `production_impact`,
`worker_status`, `supervisor_pid_or_parent`, `deployment_commit`.

## Acceptance

The worker is supervised after deploy/restart, the comparison artifact updates without manual
commands, and no production signal, candidate observation, paper trade, executor, gate, canary, or
risk table is written by the source worker.

## Findings ledger

- **2026-07-04** (P8 restart-safety repair, commit
  `b50d180ce2b431b1b0ddbdb8af325e20f5f14a9d`): P8 pump.fun shadow collection originally ran as a
  manual `nohup` process and disappeared after Zeabur deploy. The effective Zeabur startup path was
  an inline bash command, not `scripts/run_zeabur_services.sh`, so wiring that script was
  insufficient. The durable fix is to supervise `scripts/run_pump_fun_shadow_worker.sh` from the
  active Node runtime supervisor: `src/index.js:879-884` adds `startPumpFunShadowWorker()`,
  `src/index.js:1030-1118` adds the bash sidecar supervisor, and `src/index.js:1130-1164` wires the
  shadow-only pump.fun worker. Post-deploy evidence from
  `/app/data/agent_runs/latest/pump_fun_shadow_worker_status.json` showed
  `schema_version=pump_fun_shadow_worker_status.v1`, `started_at=2026-07-04T04:19:47Z`,
  `pid=49`, `loop_count=12`, `promotion_allowed=false`; `/proc` showed
  `bash scripts/run_pump_fun_shadow_worker.sh` with parent pid `23` (`node src/index.js --premium`).
  Comparison artifact `/app/data/agent_runs/latest/pump_fun_shadow_source_comparison_24h.json`
  remained `P8_TRIAL_ACCUMULATING`, `promotion_allowed=false`, with pump.fun shadow rows
  `744`, unique tokens `706`, and `production_impact=zero_shadow_only`.
- **2026-07-04** (P8 worker status self-evidence repair, commits
  `9da39c3c535654821815e8f9a3c9c8ae38eeefc2` and
  `7147d8c4b0225d42feefe70919c4ed00ea005902`): Runtime supervision must be visible in the
  worker artifact, not only inferred from `/proc`. `scripts/run_pump_fun_shadow_worker.sh` now
  writes `parent_pid`, `supervisor_pid`, `supervisor_kind`, `deployment_commit`, and
  `worker_status_source` into `/app/data/agent_runs/latest/pump_fun_shadow_worker_status.json`;
  `src/index.js` injects `PUMP_FUN_SHADOW_SUPERVISOR_PID=process.pid`,
  `PUMP_FUN_SHADOW_SUPERVISOR_KIND=node_index_runtime_supervisor`, and the Zeabur deployment
  commit. Commit `9da39c3...` initially introduced a status argv slicing bug that made the
  worker exit on boot; commit `7147d8c...` fixed the slice to `sys.argv[1:18]`. Post-deploy
  evidence from `pump_fun_shadow_worker_status.json` at `2026-07-04T05:08:24Z` showed
  `pid=51`, `parent_pid=23`, `supervisor_pid=23`,
  `supervisor_kind=node_index_runtime_supervisor`,
  `deployment_commit=7147d8c4b0225d42feefe70919c4ed00ea005902`,
  `worker_status_source=supervised_bash_sidecar`, and `promotion_allowed=false`; `/proc` agreed
  that `bash scripts/run_pump_fun_shadow_worker.sh` was parented by pid `23`
  (`node src/index.js --premium`). The refreshed comparison artifact at `2026-07-04T05:07:49Z`
  stayed `P8_TRIAL_ACCUMULATING`, `promotion_allowed=false`, and
  `production_impact=zero_shadow_only`.

# OOS Daily Cycle Runner — handoff for Codex review

- **Date (UTC):** 2026-06-16 · **Branch:** `runtime-stability-marker-guard` · research-only, main untouched
- **New file:** `scripts/run-oos-daily-cycle.js` (+ `tests/run-oos-daily-cycle.test.mjs`)
- **Status:** built + smoke-verified (`--check` clean, fail-closed tests pass). **NOT yet trusted for unattended/live runs — pending this review.**

## 1. What it is / why
The operator picked a **deterministic one-command daily runner** (manual trigger) over a session-only cron
(which dies when Claude exits and is not real automation). This script chains the already-signed-off
components end-to-end so a human — or, after sign-off, a real OS `cron`/`launchd` job — can run **exactly one
OOS accumulation cycle per calendar day** with no interactive agent.

It **re-implements no science**. It is pure glue over: `run-oos-daily-operation.js` (PREPARE/INGEST),
`run-dune-sql-export.py`, `validate-v10-curve-feature-trade-export.js`, and the cumulative dir.

## 2. Flow
```
pull fresh snapshot (curl + >60MB size + sqlite integrity, retry x3) -> frozen pack + manifest
-> production_commit = `git ls-remote origin main`   (prod auto-deploys main HEAD; /health is unreliable)
-> PREPARE (producer -> OOS selection -> oos.sql + signal_windows.csv)
-> [0 OOS candidates -> DATA_INSUFFICIENT_WAIT, clean exit 0]
-> Dune export on oos.sql (-> _dune_raw/trades.jsonl + dune-manifest.json)
-> validate (out_of_window must be 0)
-> INGEST (--dune-* --force-smoke) [coverage gate + dune-metadata gate + leak gate all inside orchestrator]
-> leak sweep (defense-in-depth)
-> cumulative_counts.json:
     dog>=50 AND dud>=50 -> HALT: LOOKPOINT_READY_N50_AUDIT_REQUIRED (exit 10), AUC NOT read
     else                -> DAILY_ACCUMULATION_CONTINUE (exit 0)
```

## 3. Safety guarantees (please verify these in review)
- **AUC never read/computed.** `--look-point` / `--reveal-sealed-auc` are refused in the arg loop before any work (tested). The n=50 branch emits a status string only; it reads `cumulative_counts.json` (counts), never an AUC artifact, and exits 10 telling the operator to stop and get a Codex audit.
- **No strategy/main/production writes.** Only writes: the frozen pack, the dated out-dir, `daily-cycle-result.json`, and an append to `<dataroom>/oos-daily-ops-log.jsonl`. The cumulative table is mutated **only** through the signed-off accumulator (via the orchestrator).
- **1 cycle/day idempotency.** Keyed on the UTC-dated out-dir `oos-daily-formal-<YYYYMMDD>`. If it already shows `phase=ingested` → `ALREADY_RAN_TODAY`, no-op. If it exists but is not ingested (a prior crash) → fail-closed, demands manual cleanup (no auto-overwrite, no silent resume).
- **Fail-closed everywhere.** Snapshot too small / bad integrity (retry then halt), Dune error, `out_of_window>0`, dune-metadata mismatch (orchestrator), any `lookpoint|sealed|auc` artifact → non-zero exit, no bypass.

## 4. Known debt to rule on
1. **`--force-smoke` on INGEST.** PREPARE and INGEST share one out-dir, so INGEST re-enters it via `--force-smoke`. `--production-commit` is still passed real (from `git ls-remote origin main`), so **no real guard is relaxed — only the no-overwrite guard**, which the 1/day idempotency check already protects. The flag's *name* ("smoke") is misleading for a real run. **Proposed proper fix:** add a dedicated `--resume` to `run-oos-daily-operation.js` that detects a `phase=prepare_only` manifest and continues to INGEST without relaxing overwrite. (Deferred so this runner doesn't modify the signed-off orchestrator in the same change.)
2. **Dune-output dir separation.** The runner writes Dune output to `<out>/_dune_raw/`, NOT `<out>/dune/`, because the orchestrator does `fs.copyFileSync(duneTrades, <out>/dune/trades.jsonl)` — if source == dest that truncates the file to zero. Confirm this separation is correct and durable.
3. **production_commit source.** Uses `git ls-remote origin main` (prod deploys main HEAD). `/health` returned empty/unreachable on probe (zeabur cold-start). Accept main-HEAD as the provenance label, or point me at a live commit endpoint if one exists.
4. **Token persistence.** Reads the dashboard token from `/tmp/sas-dashboard-token` (ephemeral; cleared on reboot). For a real cron, set `OOS_DASHBOARD_TOKEN_FILE` to a persistent path. The token is read from file and **never printed**.

## 5. How to verify (no side effects)
```
node scripts/run-oos-daily-cycle.js --check     # preconditions + plan, NO pull, NO ingest
node --test tests/run-oos-daily-cycle.test.mjs  # fail-closed arg probes (offline)
node --test tests/*.test.mjs                     # full suite (orchestrator/accumulator/producer/feature/coverage)
```
`--check` output on 2026-06-16: production_commit `95581ee7…`, prereg `065d149a…` (== lock), cumulative 30/38, no artifacts created.

## 6. How to run one real cycle (after sign-off)
```
node scripts/run-oos-daily-cycle.js             # one cycle; run in a normal shell (network), ~once/day
```
Exit 0 = `DAILY_ACCUMULATION_CONTINUE` or `DATA_INSUFFICIENT_WAIT` or `ALREADY_RAN_TODAY`.
Exit 10 = `LOOKPOINT_READY_N50_AUDIT_REQUIRED` → **stop, do not read AUC, get a Codex audit.**
Exit 2 = fail-closed blocker (see stderr).

## 7. Standing state (unchanged by this work)
Cumulative **30 dog / 38 dud**, 68 rows / 68 unique, coverage 0pp, AUC sealed. Distance to n=50: dog +20, dud +12.
Verdict for the formal phase remains **`DAY1_DAILY_CYCLE_PASS_CONTINUE_ACCUMULATING`** — the overall OOS goal is **not** complete; n=50 is a futility-stop, not edge confirmation.

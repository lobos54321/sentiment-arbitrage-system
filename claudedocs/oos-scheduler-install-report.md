# OOS Daily Runner — Claude Code Scheduler Install Report

- **Date:** 2026-06-16 12:4x AEST (UTC+10) · **Branch:** `runtime-stability-marker-guard` · research-only
- **Goal:** install the signed-off daily runner (`scripts/run-oos-daily-cycle.js`, commit `c70b8436`) on Claude Code's **native** scheduler — *but only if it is durable, i.e. not session-only.*

## VERDICT: `SCHEDULER_NOT_DURABLE_USE_LAUNCHD_FALLBACK`

Claude Code's native scheduler in this runtime is **session-only**. It cannot run the daily cycle unattended. Per the operator's own rule ("if it still depends on the current Claude session, it can't be automation → launchd or manual"), the native scheduler is **not** installed as the daily mechanism.

## Evidence (empirical, this session)
1. **Pre-existing session cron deleted / none stale.** `CronList` → `No scheduled jobs` (the earlier `79248519` was already removed).
2. **Durable flag is not honored here.** Created a one-shot test job with **`durable: true`**. The runtime returned:
   > "Scheduled one-shot task `a0a03a88`. **Session-only (not written to disk, dies when Claude exits).**"
   `CronList` tagged it `[session-only]`.
3. **Nothing written to disk.** `.claude/scheduled_tasks.json` is **ABSENT** in both `/Users/boliu/sas-research/.claude/` and `/Users/boliu/.claude/` after creating the durable job.
4. **Documented runtime behavior (CronCreate spec):** "Jobs only fire while the REPL is idle (not mid-query)" and recurring jobs "auto-expire after 7 days." → even if persisted, execution requires a **running, idle Claude REPL**; there is no headless daemon.
5. **`--check` is side-effect-free.** `node scripts/run-oos-daily-cycle.js --check` → `mode: check`, production_commit `95581ee7`, cumulative `{dog:30,dud:38}`, "no side effects." **Cumulative unchanged at 30/38** — the test did not touch it.
6. **Test job cleaned up.** `CronDelete a0a03a88` → cancelled. No schedule remains.

## Requested report fields
| field | value |
|---|---|
| schedule id/name | test `a0a03a88` (deleted). **No formal native schedule installed** (verdict negative). |
| next run time | N/A — no durable schedule exists. (Test was 12:43 local; deleted before firing.) |
| command | `cd /Users/boliu/sas-research && node scripts/run-oos-daily-cycle.js` |
| working directory | `/Users/boliu/sas-research` |
| log path | manual runner writes `<dataroom>/oos-daily-formal-<UTC>/daily-cycle-result.json` + appends `<dataroom>/oos-daily-ops-log.jsonl`. |
| token/key from file only? | **Yes.** Runner reads token from `/tmp/sas-dashboard-token` and key from `~/.dune_api_key`; nothing secret is in any schedule text; never printed. |
| --check test result | PASS — side-effect-free, cumulative 30/38, prod commit `95581ee7`. |
| confirmed NOT session-only? | **No — it IS session-only** (runtime-tagged + absent from disk even with `durable:true`). |
| disable / remove | native: `CronDelete <id>` (or it dies when Claude exits). launchd (if adopted): `launchctl bootout gui/$(id -u)/<label>` then remove the plist. |

## Acceptance criteria — status
- Claude Code scheduler persists durably: **FAILED** (session-only; not on disk).
- `--check` scheduled test passes: the **runner** `--check` passes & is side-effect-free; the **scheduler** could not be proven durable, so a scheduled `--check` would not survive a Claude exit.
- Formal task installed: **NOT installed** (would be non-functional automation).
- Log path queryable: yes for manual runs (`oos-daily-ops-log.jsonl`).
- Runner is the reviewed `c70b8436` script: **yes, unchanged.**
- Cumulative still dog=30 / dud=38: **yes, untouched.**
- Strategy layer unchanged: **yes** (no gate/matrix/RR/liquidity/exit/size/main touched; no AUC read).

## Fallback options (operator picks)
**A — launchd (true headless, the designated fallback).** A `~/Library/LaunchAgents` job runs the runner daily without Claude open, captures stdout to a durable log, and on exit 10 (n=50) or exit 2 (fail-closed) stops itself + flags. This is the only path to unattended automation on macOS. *Not installed here* — the operator initially preferred to avoid launchd, and installing a LaunchAgent is a system change. Say the word and I'll add a wrapper (`scripts/oos-daily-cron.sh`, exit 0/10/2 → marker logic) + the plist for you to `launchctl` in.

**B — manual trigger (zero new infra, current).** Run `node scripts/run-oos-daily-cycle.js` once/day in a normal shell. Exit 0 = continue / data-insufficient / already-ran; exit 10 = n=50 → stop, get Codex audit, do not read AUC; exit 2 = fail-closed blocker.

The native Claude Code cron is **not** a third option for unattended use: it only fires when you already have Claude Code open and idle, and it dies on exit.

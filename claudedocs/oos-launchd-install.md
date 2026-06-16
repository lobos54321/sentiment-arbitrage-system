# OOS Daily Runner — launchd (headless) install & operations

Installs the signed-off runner `scripts/run-oos-daily-cycle.js` (commit `c70b8436`) as a **headless** macOS LaunchAgent that runs one OOS accumulation cycle per day **without Claude Code open**, and self-halts at n=50 or on a fail-closed blocker.

## What's installed
| piece | path |
|---|---|
| LaunchAgent label | `com.sas.oos-daily-cycle` |
| plist (repo source) | `launchd/com.sas.oos-daily-cycle.plist` |
| plist (installed) | `~/Library/LaunchAgents/com.sas.oos-daily-cycle.plist` |
| wrapper | `scripts/oos-daily-launchd-runner.sh` |
| schedule | **10:30 local (Sydney) daily**, `RunAtLoad=false` |
| dashboard token | `~/.sas-dashboard-token` (persistent, perms 600; **not** `/tmp`) |
| Dune key | `~/.dune_api_key` |
| cumulative dir | `~/sas-data-room/oos-cumulative-sol-curve-unique-buyers` |

## Logs & state
- launchd stdout/stderr: `~/sas-data-room/oos-runner-logs/launchd.stdout.log`, `…/launchd.stderr.log`
- wrapper log (one line/run): `~/sas-data-room/oos-runner-logs/runner.log`
- per-cycle full log: `~/sas-data-room/oos-runner-logs/cycle-<UTC>.log`
- runner result JSON: `~/sas-data-room/oos-daily-formal-<UTC>/daily-cycle-result.json`
- ops log (append): `~/sas-data-room/oos-daily-ops-log.jsonl`
- state/markers: `~/sas-data-room/oos-runner-state/`

## Self-halt markers (sticky — the wrapper skips real cycles once present)
- `oos-runner-state/LOOKPOINT_READY_N50_AUDIT_REQUIRED` — written when the runner exits **10** (n=50: dog≥50 AND dud≥50). AUC is NOT read. **Stop and get a Codex audit before any look point.**
- `oos-runner-state/FAIL_CLOSED_BLOCKER` — written when the runner exits **2/other** (fail-closed). Inspect the cycle log, fix, then remove the marker to resume.

## Install / uninstall / status
```bash
# install
cp launchd/com.sas.oos-daily-cycle.plist ~/Library/LaunchAgents/
launchctl load -w ~/Library/LaunchAgents/com.sas.oos-daily-cycle.plist

# status (label present = loaded; PID '-' = not currently running)
launchctl list | grep com.sas.oos-daily-cycle

# uninstall
launchctl unload ~/Library/LaunchAgents/com.sas.oos-daily-cycle.plist
rm ~/Library/LaunchAgents/com.sas.oos-daily-cycle.plist
```

## Manual controls
```bash
# side-effect-free smoke (no real cycle, no markers):
bash scripts/oos-daily-launchd-runner.sh --check

# force a real cycle now (instead of waiting for 10:30) — runs ONE cycle, respects 1/day idempotency:
launchctl start com.sas.oos-daily-cycle    # then watch oos-runner-logs/

# resume after n=50 audit OR after fixing a fail-closed blocker:
rm ~/sas-data-room/oos-runner-state/LOOKPOINT_READY_N50_AUDIT_REQUIRED   # only after Codex audit
rm ~/sas-data-room/oos-runner-state/FAIL_CLOSED_BLOCKER                  # only after fixing the cause
```

## Token rotation
If the dashboard token changes, update the persistent file (the value is read from the file, never embedded):
```bash
cp /tmp/sas-dashboard-token ~/.sas-dashboard-token   # or write the new token into ~/.sas-dashboard-token
chmod 600 ~/.sas-dashboard-token
```

## Safety properties (enforced by wrapper + runner)
- never reads/computes AUC; never passes `--look-point` / `--reveal-sealed-auc`
- one cycle/day (runner idempotency keyed on the UTC-dated out-dir; wrapper adds an atomic lock)
- sticky markers stop further automatic cycles at n=50 or fail-closed
- no strategy/gate/exit/size/main/production write path touched
- secrets read from files only; never printed; not in the plist

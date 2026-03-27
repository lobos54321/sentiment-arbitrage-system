# Autonomy Daemon Operations

## Purpose

Run the paper-only autonomy sidecar as a **persistent event-driven research supervisor**.

This daemon is designed to:
- ingest remote input changes
- trigger only the necessary downstream stages
- keep strategy research moving without Claude session cron
- pause automatically when configured target quality is reached

It does **not** modify live execution or wallet paths.

## Architecture

The daemon keeps the existing safety/ops properties:
- single-instance lock
- heartbeat/status file
- step timeout
- failure backoff
- persistent logs/status/context

But the control plane is now **event-driven**, not periodic full-cycle polling.

### Persistent event queue

Backed by SQLite via `src/database/autonomy-event-store.js`.

Each event stores:
- `event_id`
- `event_type`
- `payload_json`
- `dedupe_key`
- `state` (`pending`, `leased`, `completed`, `dead_letter`, `suppressed`)
- `available_at`
- `cooldown_until`
- `attempts`
- `lease_owner`
- `lease_expires_at`

### Main chain

Typical flow:
1. `remote_logs_synced`
2. `paper_eval_requested`
3. `paper_eval_completed`
4. if needed: `market_sync_requested`
5. if research gap detected: `research_gap_detected`
6. `feature_research_completed`
7. `strategy_draft_completed`
8. `challenger_generated`
9. `challenger_eval_completed`
10. `promotion_review_ready`
11. `promotion_applied` or `autonomy_paused_target_reached`

### Time is only used for
- heartbeat/status refresh
- retry backoff
- lease recovery
- low-frequency maintenance sweep

The goal is not “run everything every N minutes”.
The goal is “when the prior step completes and conditions match, enqueue the next step”.

## Components

- `scripts/run-autonomy-daemon.js` — event-driven daemon
- `src/database/autonomy-event-store.js` — persistent event queue
- `src/database/autonomy-run-store.js` — run/stage audit trail
- `scripts/status-autonomy-daemon.js` — operator status view
- `scripts/start-autonomy-daemon.sh` — background launcher
- `scripts/stop-autonomy-daemon.js` — graceful stop
- `ecosystem.config.json` — PM2 process definition

## Candidate lifecycle

Candidates are no longer promoted directly from generation.

Lifecycle states:
- `draft`
- `qualified`
- `active_challenger`
- `promotable`
- `promoted`
- `rejected`
- `paused_target_reached`

Rules:
- challenger generation creates **draft** only
- autoresearch produces recommendation / qualification result
- daemon decides whether to activate challenger or promote baseline
- pause target is checked before/alongside promotion flow

## Pause-on-target behavior

The daemon can stop autonomous advancement when configured targets are repeatedly met.

Example checks:
- expectancy >= target
- winRate >= target
- falsePositiveRate <= target
- sampleSize >= target
- guardrails satisfied
- repeated hits across configured windows

When pause is triggered:
- daemon state becomes `paused_target_reached`
- final evidence remains in status + memory
- no new challenger generation should continue until user instruction

## Start

### Local foreground

```bash
cd /Users/boliu/sentiment-arbitrage-system
DASHBOARD_TOKEN=... node scripts/run-autonomy-daemon.js --once
```

### Background helper

```bash
cd /Users/boliu/sentiment-arbitrage-system
DASHBOARD_TOKEN=... ./scripts/start-autonomy-daemon.sh
```

### PM2 persistent mode

```bash
cd /Users/boliu/sentiment-arbitrage-system
pm2 start ecosystem.config.json --only autonomy-daemon
pm2 status autonomy-daemon
pm2 logs autonomy-daemon
```

This is the recommended persistent runtime path for a remote server.

## Status

```bash
node scripts/status-autonomy-daemon.js
```

Status includes:
- daemon state
- queue depth
- processed event count
- current baseline / challenger
- last advancement action
- last rejection reason
- pause state
- latest audited run
- recent event history

## Stop

```bash
node scripts/stop-autonomy-daemon.js
```

Or with PM2:

```bash
pm2 stop autonomy-daemon
```

## State files

Stored under `data/` by default:
- `autonomy-daemon.lock`
- `autonomy-daemon-status.json`
- `strategy-memory-context.json`

Logs:
- `logs/autonomy-daemon.log`
- `logs/autonomy-daemon-pm2-out.log`
- `logs/autonomy-daemon-pm2-error.log`

## Safety / recovery

- stale lock cleanup on restart
- expired event lease recovery
- retry with backoff
- dead-letter on repeated failure
- persistent audit trail via `autonomy_runs`
- daemon survives Claude session end when managed by PM2

## Notes

This daemon is intended as a paper-only research sidecar. Keep it isolated from any live trading supervisor.

# Gold/Silver Capture Discovery AutoLoop

This loop is discovery-only. It does not change strategy, entry policy, hard
gates, exit gates, canary size, live executor, wallet configuration, or risk
settings.

## Command

```bash
python scripts/agent_capture_discovery_loop.py \
  --paper-db /app/data/paper_trades.db \
  --raw-db /app/data/raw_signal_outcomes.db \
  --hours 24 \
  --expected-candidates 84 \
  --out-root /app/data/agent_runs \
  --handoff-dir /app/data/agent_handoffs \
  --registry /app/data/hypothesis_registry.json
```

Use `--max-runs N --interval-sec 300` for a bounded repeated loop.

On Zeabur, run the loop as a separate worker/container with its own resource
budget. Do not attach it to the dashboard/API container. The dedicated worker
command is:

```bash
bash scripts/run_capture_discovery_worker.sh
```

Suggested worker environment:

```text
AGENT_CAPTURE_DISCOVERY_INTERVAL_SEC=900
AGENT_CAPTURE_DISCOVERY_HOURS=24
AGENT_CAPTURE_EXPECTED_CANDIDATES=84
AGENT_CAPTURE_REPORT_TIMEOUT_SEC=300
AGENT_CAPTURE_MAX_SCAN_ROWS=250000
```

The dashboard/API entrypoints intentionally do not autostart this worker. A
main-service autostart previously proved too heavy for the dashboard container.
The worker only reads the paper/raw DBs and writes agent artifacts; it does not
change strategy, gates, executor, canary size, wallet config, or risk settings.

## Required Artifacts

Each successful loop attempt writes:

```text
/app/data/agent_runs/latest/reviewer_verdict.json
/app/data/agent_runs/latest/run_summary.md
/app/data/agent_handoffs/latest_codex_handoff.md
/app/data/hypothesis_registry.json
```

The verdict is same-window discovery evidence only. `promotion_allowed` remains
false unless a future out-of-sample gate explicitly passes.

## Read-Only Dashboard Access

After deployment, use these authenticated read-only endpoints to verify and
download the latest materialized artifacts:

```text
/api/agent/capture-discovery/latest
/api/data/download/agent-capture-discovery?artifact=verdict
/api/data/download/agent-capture-discovery?artifact=summary
/api/data/download/agent-capture-discovery?artifact=handoff
/api/data/download/agent-capture-discovery?artifact=registry
/api/data/download/agent-capture-discovery?artifact=capture
/api/data/download/agent-capture-discovery?artifact=pnl
/api/data/download/agent-capture-discovery?artifact=markov_runtime
/api/data/download/agent-capture-discovery?artifact=markov_kline
```

These endpoints only read files from the agent artifact directories. They do
not run the loop and do not change trading policy.

## Verdict Inputs

The loop materializes:

- capture-first report from `candidate_shadow_observations` and
  `raw_signal_outcomes`
- secondary PnL cross report from virtual closed trades
- secondary virtual Markov reports for runtime/kline profiles
- report/evaluator self-test results

## Stop Conditions

The loop emits `BLOCKED_DATA` when any of these hold:

- DB unavailable
- raw dog rows incomplete
- candidate count is not 84
- observation coverage is below 99%
- signal-id join rate is below 99%
- schema is mixed and quote-sensitive candidates would be judged
- tests fail
- report generation fails

## H1/H2

H1 tracks:

```text
kline:active_mom20_first3 + volume_profile=building
kline:lowvol_active20_support + volume_profile=building
```

H2 tracks:

```text
ATH_SHALLOW_PULLBACK:OBSERVE + matrix_evaluator
```

Both remain hypothesis-discovery evidence until future out-of-sample validation.

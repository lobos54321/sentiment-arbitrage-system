# A1/A2 Evaluation Foundation

Status:

```text
active_stage = A3_local_verification
A1_A2_status = implemented_locally
next_stage = deploy_and_verify_A3_then_regenerate_24h_48h_72h_reports
strategy_changes_allowed = false
promotion_allowed = false
```

This foundation makes the Telegram denominator and runtime topology auditable
before candidate, cross, Markov, PnL, or promotion conclusions are accepted.

## A1: Telegram Identity And Outcome Contract

Contract:

```text
docs/agents/contracts/telegram-outcome-contract.v1.json
```

Audit:

```text
scripts/telegram_signal_identity_audit.py
```

The contract fixes:

- primary business denominator: unique token;
- secondary timing denominator: Telegram signal event;
- bronze: `+25%`;
- silver: `+50%`;
- gold: `+100%`;
- 10x: `+900%`;
- 100x: `+9900%`;
- wick, sustained, and executable tiers are separate evidence views;
- later ATH events cannot claim a second unique-token capture;
- right-censored horizons are not treated as mature failures.

Identity priority:

```text
telegram channel + message id
source_event_id
token + source timestamp + signal type
internal signal_id only
unknown
```

An internal `signal_id` alone is not source identity. The current source writer
persists a deterministic `source_event_id`, but exact Telegram message id may be
absent from `premium_signals`; the audit reports this instead of silently
claiming exact-message coverage.

Acceptance:

```text
source identity coverage >= 99%
unknown identities have deterministic attribution
both unique-token and signal-event denominators are emitted
outcome_schema_version is pinned
raw outcome join coverage >= 99%
right-censored and not-yet-mature rows are excluded from tier counts
executable_quote_return_pct coverage >= 99% for mature outcomes
```

`message_id` without a channel identifier is not exact Telegram identity. The
audit falls back to `source_event_id` or a deterministic token/time/type alias
and reports the missing channel explicitly.

Run:

```bash
python3 scripts/telegram_signal_identity_audit.py \
  --signal-db /app/data/sentiment_arb.db \
  --raw-db /app/data/raw_signal_outcomes.db \
  --hours 24 \
  --out /app/data/agent_runs/latest/telegram_signal_identity_audit_24h.json
```

## A2: Runtime And V27 Topology

Audit:

```text
scripts/runtime_v27_writer_topology_audit.py
```

It enumerates:

- `health-bootstrap` supervisor;
- `src/index.js` runtime and sidecar supervisor;
- Telegram listener and `premium_signals` writer;
- every discovered V27 event-log mirror writer;
- V27 read-model producer;
- `mode_readiness.json`;
- runtime mode-gate and paper monitor consumers;
- dashboard manual refresh trigger;
- health and process evidence when available.

The V27 producer is:

```text
scripts/v27_read_model_refresh.py
```

Automatic activation is controlled by:

```text
V27_READ_MODEL_REFRESH_WORKER_ENABLED
default = false
```

The dashboard also has a separately audited manual refresh trigger. Missing
`mode_readiness.json` therefore means producer/activation/runtime evidence is
missing until proven otherwise. It is not evidence that a strategy is bad.

Acceptance:

```text
writer and reader paths are enumerable
V27 producer is explicit
read-model path is explicit
consumers are explicit
source markers match the implementation
```

Run:

```bash
python3 scripts/runtime_v27_writer_topology_audit.py \
  --repo-root /app \
  --data-dir /app/data \
  --proc-root /proc \
  --health-json /tmp/health.json \
  --out /app/data/agent_runs/latest/runtime_v27_writer_topology_audit_24h.json
```

`--health-json` is optional. Without health or `/proc` evidence, the source
topology can pass while runtime observation remains explicitly incomplete.

## AutoLoop Integration

The staged and full AutoLoop run both audits before expensive candidate reports.

```bash
python3 scripts/agent_autoloop_stage_runner.py \
  --stage foundation \
  --signal-db /app/data/agent_evidence/current/signal.db \
  --paper-db /app/data/agent_evidence/current/paper_evidence.db \
  --raw-db /app/data/agent_evidence/current/raw.db \
  --kline-db /app/data/agent_evidence/current/kline.db \
  --evidence-manifest /app/data/agent_evidence/current/manifest.json \
  --data-dir /app/data \
  --run-id staged_current
```

Required artifacts:

```text
telegram_signal_identity_audit_24h.json
runtime_v27_writer_topology_audit_24h.json
```

The reviewer verdict exposes both under:

```text
evaluation_foundation.telegram_identity
evaluation_foundation.runtime_v27_topology
```

Failed A1 or A2 source-contract acceptance is `BLOCKED_DATA`. It never enables
promotion and never changes runtime settings. Full and staged AutoLoop runs
stop before candidate, Markov, PnL, cross, or OOS work when either foundation
report is missing or fails acceptance.

## A3 Boundary

A1/A2 do not claim a frozen cross-database snapshot. Their input report records
read timing and watermarks and sets:

```text
frozen_cross_db_snapshot = false
```

A3 adds a coordinated frozen snapshot manifest containing:

```text
git commit
database schema versions
table upper watermarks
snapshot timestamp
SQLite quick_check
file SHA-256
cross-database time skew
```

Only A3 may claim repeatable cross-database evidence.

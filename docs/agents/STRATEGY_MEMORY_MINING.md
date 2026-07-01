# Strategy Memory Mining

Strategy Memory Mining turns the historical strategy document into structured,
shadow-only research assets for the AutoLoop. It does not optimize production
strategy and it must not change entry policy, gates, A_CLASS mode,
final_entry_contract, canary size, executor, wallet, risk, position size, or
production thresholds.

## Outputs

- `/app/data/strategy_memory_hypotheses.json`
- `/app/data/strategy_memory_candidate_mapping.json`
- `/app/data/strategy_memory_prioritized_queue.json`
- `/app/data/filtered_winner_dossier_24h.json`
- `/app/data/filtered_winner_dossier_72h.json`
- `/app/data/index_lifecycle_snapshot_report_24h.json`
- `/app/data/exit_policy_shadow_simulator_24h.json`
- `/app/data/execution_delay_adjusted_replay_24h.json`

On local machines where `/app/data` is read-only or absent, pass `--out` or
`STRATEGY_MEMORY_DATA_DIR` to write the same artifacts under a writable folder.

## What The Loop Does

The loop converts old notes into machine-readable hypotheses with:

- strategy family and source section
- entry and exit definitions
- required features
- time-legal features
- future/posthoc features that must not enter entry rules
- known risks
- next validation required
- `allowed_use=shadow_only`
- `promotion_allowed=false`

The first pass focuses on ATH lifecycle stages, index deltas, MC buckets,
filtered winners, exit variants, and execution-delay sensitivity.

## Why Historical Strategies Are Shadow-Only

Historical notes mix live observations, small-sample backtests, MC peak
comparisons, real-kline tests, and failure reviews. That is useful research
memory, but it is not deployment evidence. The mining loop can propose only
shadow hypotheses and reports for the current AutoLoop to evaluate.

Historical PnL is not promotion evidence. Same-window discovery is not
promotion evidence. A hypothesis can only become a candidate after it is
expressed with time-legal features and evaluated by the existing discovery,
readiness, and OOS guardrails.

## Bias Controls

Avoid future-data bias:

- `max_ath`, later peak, Top50 membership, and matured snapshots are labels only.
- Entry definitions may use only features available at or before the decision
  timestamp.
- MC peak estimates must be separated from executable quote MC and real K-line
  evidence.
- ATH#1 hypotheses must include execution-delay replay at 0, 5, 10, 20, 30,
  and 60 seconds.
- X narrative context is read-only and must be time-legal.

## Evidence Separation

MC peak estimate:

- Useful for labeling missed winners and lifecycle shape.
- Not executable evidence.
- Must be flagged separately from real K-line or quote evidence.

Real K-line:

- Can support shadow replay and exit simulation.
- Still discovery-only unless evaluated across clean windows and OOS probes.

Paper/live evidence:

- Paper capture and realized capture are reported when available.
- They do not authorize promotion from this loop.

## Run

```bash
python scripts/strategy_memory_mining.py \
  --source-docx "/Users/lobos/Desktop/策略记录.docx"

python scripts/strategy_memory_candidate_mapping.py

python scripts/filtered_winner_dossier.py --hours 24
python scripts/filtered_winner_dossier.py --hours 72

python scripts/index_lifecycle_snapshot_report.py
python scripts/exit_policy_shadow_simulator.py
python scripts/execution_delay_adjusted_replay.py

python scripts/offline_strategy_memory_audit.py
```

Self-tests:

```bash
python scripts/strategy_hypothesis_registry.py
python scripts/strategy_memory_mining.py --self-test
python scripts/strategy_memory_candidate_mapping.py --self-test
python scripts/filtered_winner_dossier.py --self-test
python scripts/index_lifecycle_snapshot_report.py --self-test
python scripts/exit_policy_shadow_simulator.py --self-test
python scripts/execution_delay_adjusted_replay.py --self-test
python scripts/offline_strategy_memory_audit.py --self-test
```

## Reporting Contract

The final audit reports only:

- `strategy_memory_hypotheses_count`
- `mapped_to_existing_candidates`
- `missing_shadow_candidates`
- `rejected_future_data_hypotheses`
- `top_10_shadow_hypotheses`
- `filtered_winner_count`
- `exit_policy_variants_tested`
- `delay_replay_done`
- `forbidden_files_changed`
- `promotion_allowed`
- `next_action`

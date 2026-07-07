# CODEX GOAL Phase 3 - Real-Quote Tail Validation & Long-Horizon Evidence

Status inherited from Phase 2:

- Measurement and governance are healthy enough to move past pure signal mining.
- P7 exit-policy OOS passed two forward windows and opened a human paper-proposal checkpoint.
- A_CLASS remains guarded; paper/live enablement and any production exit change are human decisions.

Phase 3 does not search for another same-window ranking. It asks whether the system can capture
tail outcomes with real paper quotes, longer observation, and predictive metric governance.

## Operating Thesis

The price-only signal-mining loop is exhausted. Historical and shadow evidence says most apparent
edges are lottery-tail artifacts, not stable entry precision. Phase 3 therefore focuses on:

1. **real-quote paper evidence** for tail capture,
2. **long-horizon token observation** so late edges are not missed,
3. **human-reviewed P7 proposal discipline** after OOS,
4. **metric predictiveness accounting** so scoring functions must pass their own forward exams,
5. **price-external influence/KOL accumulation** using an explicit data acquisition layer.

## Phase 3 Task Queue

### P3.1 - Wide-Net Paper Experiment Proposal (human approval required before enablement)

Question:

> If every eligible signal is paper-entered at equal tiny size and exited with the P7 champion,
> does real quote execution capture enough tail to be positive?

Scope:

- Proposal/dry-run generation is automatic.
- Actual paper execution enablement requires human approval.
- Production/live execution is out of scope.

Proposed experiment contract:

- all eligible signals, not cherry-picked winners;
- equal `0.001 SOL` paper size;
- quote executable required at entry;
- hard stop between `-10%` and `-20%`, explicitly pinned in the proposal;
- P7 champion exit shape: `trail_a50_dd15_stop20`;
- independent paper experiment ledger separate from existing A_CLASS proof rows;
- fees, failed quotes, no-fills, timeouts, loss-cap exits, and slippage included;
- hourly and daily paper-only caps;
- kill-switch and circuit rows are recorded, but paper-only loss cannot alter live risk;
- run length: 14 days minimum before final verdict.

Acceptance:

- `wide_net_paper_experiment_proposal.json` exists and declares `human_approval_required=true`;
- no strategy, gate, executor, canary, wallet, risk, or live file is changed;
- proposal states `promotion_allowed=false`;
- proposal includes denominator, sizing, stops, exit contract, caps, ledger schema, stop conditions,
  and review cadence.

### P3.2 - 24h Path Observer (shadow-only, Codex may implement)

Question:

> Does a token's useful edge appear after the current short observation horizon?

Scope:

- Extend path observation from the short window to 24h.
- Read-only/shadow-only observation; no entry or exit behavior may change.
- Cost and storage controls are required.

Acceptance:

- observer artifact records `observation_horizon_hours=24`;
- storage and API rate caps are explicit;
- sampled rows include early, 2h, 6h, 12h, and 24h checkpoints where available;
- missing bars and provider failures are recorded, not dropped.

### P3.3 - P7 Paper Proposal Checkpoint (Codex drafts, human decides)

Question:

> P7 passed OOS replay. Is that enough to approve a paper-level experiment?

Required wording:

- P7 replay/OOS passed;
- real paper evidence is weak/negative and must be separated from replay;
- any approval is paper-level only;
- production promotion is not allowed.

Acceptance:

- `p7_paper_proposal_checkpoint.json` and `.md` exist;
- proposal includes both OOS replay results and real paper-window evidence;
- `human_approval_required=true`;
- `promotion_allowed=false`;
- `production_exit_policy_change_allowed=false`.

### P3.4 - P9 Metric Predictiveness Ledger (read-only)

Question:

> Do scoring functions predict future outcomes, or only explain the past?

Contract:

- every metric/scoring function gets a forward predictiveness record;
- primary test is future correlation or rank lift on frozen windows;
- promote a metric only after `|rho| >= 0.30` or equivalent predeclared lift;
- degrade or quarantine a metric after persistent `|rho| < 0.05`;
- no metric may be used as promotion evidence before its ledger says it is predictive.

Acceptance:

- `metric_predictiveness_ledger.json` exists;
- each metric row has train/eval windows, no-overlap flag, denominator, effect size, and verdict;
- metric verdicts cannot alter production logic automatically.

### P3.5 - Influence/KOL Shadow Source (agent-reach acquisition layer)

Question:

> Can price-external influence data explain market-cap ceilings or tail continuation better than
> price-only features?

Acquisition design:

- Use `agent-reach` for X/Twitter research pulls where human/browser login or cookie state is
  required.
- Preferred X route: `twitter-cli` for user timelines/tweets; fallback to `opencli twitter` with
  browser login state when search endpoints fail.
- Raw X/KOL pulls are offline/shadow data snapshots, not production dependencies.
- The runtime system consumes only normalized, cached, read-only feature artifacts.

Allowed data:

- account identity and public profile metadata;
- tweet/post timestamp, text hash, URL, public engagement counts when available;
- token/symbol/address mentions with extraction confidence;
- source list provenance and fetch backend.

Not allowed:

- posting, liking, following, or any write action;
- scraping private data;
- using live X fetch failures as entry blockers;
- feeding unverified KOL features directly into production entry.

Acceptance:

- `influence_kol_shadow_source_plan.json` exists;
- source acquisition backend is declared as `agent_reach_x_twitter`;
- feature artifacts are marked `shadow_only`;
- production impact is `zero`;
- later calibration target is "influence -> market-cap ceiling / tail continuation", not immediate entry.

## Loop Order

Every Phase 3 loop writes:

- `phase3_goal_loop.json`
- `phase3_goal_loop.md`
- `p7_paper_proposal_checkpoint.json` when P7 has passed OOS
- `p7_paper_proposal_checkpoint.md` when P7 has passed OOS
- `phase3_path_horizon_audit_24h.json`
- `p9_metric_predictiveness_ledger.json`
- `influence_kol_shadow_source_plan.json`

Loop stages:

1. Snapshot current deployment, artifacts, and guardrails.
2. Check P7 OOS checkpoint status.
3. Produce or update the P7 paper proposal checkpoint.
4. Emit P3.1 wide-net paper proposal task, blocked on human approval.
5. Emit P3.2 24h observer implementation task.
6. Emit P3.4 metric predictiveness ledger task.
7. Emit P3.5 influence/KOL shadow source plan using agent-reach acquisition.
8. Materialize P3.2/P3.4/P3.5 read-only artifacts every AutoLoop run.
8. Stop before any strategy, exit, gate, executor, canary, wallet, or risk change.

## Global Guardrails

- `promotion_allowed=false` until OOS plus human approval.
- `paper_experiment_enablement_allowed=false` unless the human explicitly approves P3.1.
- No production strategy or live risk changes.
- P7 proposal can request a human decision; it cannot implement that decision.
- Influence/KOL work is shadow-only and cannot make runtime entries depend on X availability.

# Strategy Memory Seed

These JSON artifacts are historical research inputs for the read-only
Gold/Silver Capture AutoLoop.

They are deliberately stored with the application so a fresh deployment can
restore the `strategy_memory` hypothesis namespace even when `/app/data` does
not contain the earlier local mining artifacts.

Guardrails:

- Evidence role: `historical_memory`
- Allowed use: `shadow_only`
- Promotion allowed: `false`
- These files do not modify the candidate catalog.
- Future-data hypotheses remain rejected.
- Exit-only hypotheses are evaluated only by the exit shadow simulator.
- Current-window capture, OOS evidence, and human approval remain mandatory.

The live service reads this directory through
`STRATEGY_MEMORY_ARTIFACT_DIR=/app/docs/agents/strategy-memory-seed`.

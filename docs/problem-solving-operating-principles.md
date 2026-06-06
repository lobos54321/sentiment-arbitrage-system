# Problem Solving Operating Principles

Updated: 2026-06-06

This document is the operating doctrine for diagnosing, rebuilding, and
iterating the sentiment arbitrage system. It applies to production incidents,
strategy reviews, source evaluation, A_CLASS changes, exit changes, dashboard
metrics, and any future autonomous control loop.

The short version:

> First understand the real situation. Then decompose the problem into the
> smallest effective units. Then rank the contradictions. Then attack the main
> contradiction only. Do not tune strategy before the evidence chain can prove
> what is happening.

## Why This Exists

The system is no longer a simple script. It is a signal research, risk control,
paper execution, and tiny-canary experimentation platform. In a complex system,
bad fixes usually come from one of four mistakes:

- acting before knowing what is actually happening;
- treating downstream symptoms as root causes;
- changing several gates at once;
- optimizing against dirty denominators, stale snapshots, or polluted PnL.

These principles prevent that.

## Non-Negotiable Sequence

Every serious diagnosis or strategy change must follow this order.

1. **Reality first**

   The first question is always: what is the current situation?

   Use first-hand runtime evidence before conclusions:

   - live DB health;
   - signal freshness;
   - materialized snapshot age;
   - canonical ledger rows;
   - opportunity events;
   - provider hydration outcomes;
   - quote-clean denominator;
   - loss-cap and circuit-breaker state;
   - logs for crash, restart, 429, SQLite, SIGBUS, and stale-source symptoms.

   Do not rely on stale local data, old screenshots, or a materialized snapshot
   if the live DB disagrees.

2. **Measurement before strategy**

   If the denominator, ledger, quote path, DB, or data freshness is broken,
   strategy performance is not interpretable.

   In this project, the most important measurement contracts are:

   - PnL uses realized SOL accounting where available;
   - fill and exit prices use executable quote truth;
   - mark price is context, not final accounting truth;
   - gold/silver catch rate uses quote-clean executable denominator;
   - outliers are marked, not silently deleted;
   - `UNKNOWN` must be decomposed into a reason, not treated as a conclusion.

3. **Decompose the困局 into smallest effective units**

   A problem is not decomposed enough until each unit is:

   - **Observable**: it has a concrete table, API field, log marker, or counter.
   - **Attributable**: it has one owner or cause class: infra, market, policy,
     execution, measurement, source, or governance.
   - **Actionable**: there is a specific next action if the unit is bad.
   - **Falsifiable**: a later measurement can prove whether the action worked.
   - **Bounded**: it does not mix several lifecycles, sources, or modes into one
     blended number.

   If a unit cannot meet these five tests, it is still too large.

4. **Rank contradictions before fixing**

   Not every problem is equally important. Rank by dependency, not by noise.

   Priority order:

   1. **Storage and process continuity**: if the DB resets or worker crashes,
      every downstream statistic is invalid.
   2. **Signal freshness**: if new signals are not entering the consumable path,
      no strategy conclusion is valid.
   3. **Quote/execution evidence**: if candidates cannot become quote-clean,
      gold/silver denominator stays unknowable.
   4. **Ledger and loss contracts**: if realized loss or entry evidence is not
      connected, risk controls cannot be trusted.
   5. **Source quality**: only after clean denominator exists can source edge be
      judged.
   6. **Strategy gates and scoring**: only after the above layers are clean.
   7. **Sizing**: last, and only after stable positive EV.

   A downstream metric can look bad because an upstream layer is broken. Do not
   change the downstream layer until the upstream layer is ruled out.

5. **Lock the main contradiction**

   At any point, there should be one main contradiction. All other problems are
   observed but not actively tuned unless they block the main contradiction.

   Examples:

   - If `paper_trades.db` is zero bytes or repeatedly quarantined, the main
     contradiction is storage/process continuity, not A_CLASS scoring.
   - If `unknown_data_rate` is high because provider evidence is missing, the
     main contradiction is hydration/evidence, not source edge.
   - If quote-clean denominator is zero after hydration is healthy, the main
     contradiction may move to sourcing.
   - If denominator is clean and source cohorts show positive EV, the main
     contradiction may move to exit capture or sizing.

6. **One action, one verification**

   Each iteration should change one meaningful lever and define the expected
   metric movement before deployment.

   Examples:

   - Hydrator fix should move `provider_hydrate_outcome=success` up and
     `unknown_data_rate` down.
   - DB durability fix should remove zero-byte DB, SIGBUS, quarantine churn, and
     stale-snapshot/live-DB divergence.
   - Denominator observation fix should move `denominator_exclusion_breakdown`
     from `path_peak_missing` toward `eligible` when path samples accumulate.
   - Source change should move source/component-level quote-clean dog counts,
     not just raw candidate counts.

   If the expected metric does not move, do not declare success.

## Smallest Effective Units for This System

Use these units before inventing broader explanations.

### Storage / Runtime Continuity

- `paper_db_health.status`
- DB size and mtime
- recovery/quarantine directory events
- SIGBUS/restart counters
- live DB vs materialized snapshot age
- WAL/checkpoint/disk free evidence

Question: can the system remember a full evaluation window?

### Signal Freshness

- max `premium_signals.receive_ts`
- Node DB path
- listener/parser/write health
- monitor-consumable source age
- fail-closed freshness state

Question: are fresh external signals actually entering the paper monitor path?

### Provider / Quote Evidence

- `provider_hydrate_outcome`
- `observed_hydrate_outcome_breakdown`
- `quote_available`
- `quote_executable`
- `route_available`
- `quote_age_sec`
- `quote_source`
- `data_confidence`

Question: did shadow/would-enter candidates become auditable executable quote
evidence?

### Denominator

- `quote_clean_gold_silver_seen_count`
- `quote_clean_gold_silver_would_enter_count`
- `unknown_data_rate`
- `unknown_reason_breakdown`
- `denominator_exclusion_breakdown`
- `hydrate_outcome_exclusion_breakdown`

Question: why exactly is the gold/silver denominator nonzero or still zero?

### Source Value

- `source_breakdown`
- `source_component_breakdown`
- unique token/lifecycle counts by source
- quote-clean gold/silver by source
- would-enter count by source
- source-specific no-route/trapped rate

Question: which source creates real executable opportunity, not just noise?

### Decision Continuity

- `WOULD_ENTER`
- `LIVE_ENQUEUE`
- `ENTERED_LEDGER`
- `EXITED_LEDGER`
- linked opportunity key
- linked trade id
- final entry contract state
- mode state and budget state

Question: did the decision path actually connect from signal to ledger?

### Risk / Loss Contract

- realized SOL PnL
- `loss_cap_breach`
- `mode_circuit_broken`
- `downgraded_modes`
- no-route/trapped flags
- exit quote availability

Question: did the system enforce the -20% contract and react when it was
breached?

## Main Contradiction Selection Rule

Use this scoring when multiple issues appear urgent.

Score each issue from 0 to 3:

- **Dependency**: how many downstream conclusions become invalid if this is
  wrong?
- **Evidence strength**: how directly can we observe it?
- **Reversibility**: can a small fix/test prove or disprove it quickly?
- **Blast radius**: how much capital/data/control risk does it create?

The issue with the highest total is the main contradiction. If two are close,
choose the one that invalidates the most downstream measurements.

## What Not To Do

- Do not loosen gates because one missed token pumped.
- Do not use `WOULD_ENTER` count as proof of edge.
- Do not treat `MARKET` or `UNKNOWN` as final when quote evidence is missing.
- Do not mix A_CLASS tiny canary, LOTTO, ATH continuation, and reclaim into one
  blended win rate.
- Do not judge source quality before quote-clean denominator exists.
- Do not tune AI scoring before deterministic evidence is clean.
- Do not increase size while denominator, ledger, or loss-cap evidence is
  unstable.
- Do not call a deployment successful because the service returns HTTP 200.

## Review Cadence

After each meaningful deployment:

- **15-30 minutes**: process health, DB health, dashboard health, crash/restart,
  and obvious schema errors.
- **2 hours**: hydration outcome, unknown data, denominator exclusions, source
  component evidence, and safety events.
- **6-12 hours**: canary ledger continuity, path samples, quote-clean dog
  movement, loss-cap reactions.
- **24 hours**: source EV, mode EV, gold/silver catch rate, realized SOL ROI, and
  whether the main contradiction changed.

The review output must include:

1. current situation;
2. smallest effective units observed;
3. contradiction ranking;
4. selected main contradiction;
5. one action;
6. expected verification metric;
7. what must not be changed yet.

## Current Application

For the current A_CLASS rebuild, the main contradiction is not strategy
aggression. The active investigation order is:

1. keep live DB and worker stable long enough to remember a full window;
2. prove signal freshness into the monitor path;
3. make provider hydration and opportunity path samples produce clean
   denominator evidence;
4. decompose zero denominator by source, hydrate outcome, and exclusion reason;
5. only then judge whether the source set has edge;
6. only after that consider gate, exit, AI, or sizing changes.

This document should be treated as the default operating standard for future
work unless a newer ADR replaces it.

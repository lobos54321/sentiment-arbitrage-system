# Sourcing / Target Review Framework

Date: 2026-06-17.
Status: planning framework. No production behavior change.

## Purpose

The current feature-search line is closed. This framework defines what a new
source or a target downgrade must prove before more engineering spend is
justified.

This is not a feature hunt and not a strategy change.

## Current Evidence Baseline

Closed or blocked lines:

- `unique_buyers`: locked OOS n=50 futility stop.
- source fields in current cohort: null/constant/degenerated to coverage.
- B1 Helius live-curve canary: PARTIAL, with Dune comparison showing 28/30
  decision windows empty.
- smart-money/wallet-quality: conceptually new axis, but current leak-free
  point-in-time wallet-history compute is cost-blocked.

Current posture:

- shadow-only;
- silver-side observational research;
- 60/60/200 is aspirational, not an active engineering promise.

## A New Signal Source Must Clear These Gates

### Gate 1: Point-in-time availability

The source must provide data available at or before `signal_ts`.

Disallowed:

- current-snapshot smart-money labels joined onto past signals;
- post-anchor bars or post-signal wallet outcomes;
- social/source scores that were not logged at decision time.

### Gate 2: Decision-time observability

At least 60% of candidate windows must contain usable decision-time signal
content before any label is read.

Examples of usable content:

- real in-window trades with wallet and volume;
- point-in-time wallet quality already computed as of the signal;
- source-side score components logged before the signal;
- nonzero activity from a provider that is not merely coverage metadata.

### Gate 3: Variance

The source must have non-degenerate fields in the target cohort.

Reject immediately if:

- origin is constant;
- score fields are null or empty;
- source_family/source_kind only encodes provider coverage;
- the only variance is missingness.

### Gate 4: Cost and latency

The source must be cheap enough for repeated OOS measurement.

Hard reject if a single-window smoke times out or consumes disproportionate
credits without rows, unless a cheaper indexed path is identified before the
next run.

### Gate 5: OOS design before edge

No source can be promoted based on in-sample lift.

Before any edge claim:

- write a preregistration;
- lock the primary metric;
- lock inclusion/exclusion;
- lock look points;
- define STOP at null;
- prohibit secondary-feature rescue.

## Candidate Source Families

### A. Point-in-time wallet-quality source

Potentially valuable because it asks "who buys" rather than "how many buy".

Current blocker:

- commercial smart-money labels appear to be current snapshots;
- leak-free point-in-time PnL must be self-computed;
- current Dune 365-day join timed out.

Only revisit if there is a cheaper indexed point-in-time wallet-history path.

### B. Alternative live signal origin

Worth exploring only if it differs from the current premium-signals/gecko track
and carries decision-time variance.

Minimum audit pack:

- origin cardinality;
- score-component coverage;
- timestamp semantics;
- overlap with current tokens;
- pre-label observability rate.

Reject if it collapses to the same coverage/stage axis.

### C. Current source with target downgrade

If no source clears Gates 1-4, downgrade the target rather than tune gates.

Possible target frames:

- measurement-only;
- silver-side shadow candidate;
- lower capture target with explicit latency and coverage assumptions;
- no live expansion until a new OOS primary clears.

## Decision Tree

1. Does a new source have point-in-time data with non-degenerate variance?
   - No: reject.
   - Yes: continue.

2. Does it have usable decision-time content in at least 60% of candidate
   windows before labels?
   - No: reject or classify as offline-only.
   - Yes: continue.

3. Is it affordable to build a label-free audit pack?
   - No: reject or defer.
   - Yes: build the audit pack.

4. Does the audit pack pass leakage, coverage, and variance checks?
   - No: reject.
   - Yes: write a new preregistration.

5. Does the preregistered OOS test clear?
   - No: stop.
   - Yes: only then design a shadow gate review.

## Explicitly Forbidden

- Changing live gate, exit, size, liquidity, or matrix/RR from this framework.
- Using B1, source-null, or unique-buyers secondary artifacts to justify live
  behavior.
- Continuing old cumulative runs after a stop condition.
- Running broad feature sweeps without a preregistered primary.
- Treating "provider can fetch data" as evidence of edge.

## Recommended Next Review Question

Do we have, or can we cheaply obtain, a truly point-in-time source whose
decision-time content is not empty for most candidate windows?

If the answer is no, the honest action is target downgrade rather than more
feature research.

# Shadow-Only Posture Note

Date: 2026-06-17.
Status: research posture note. No production behavior change.

## Decision

Adopt a reversible shadow-only posture while the edge layer remains unproven.

This does not change live code, gates, exits, sizing, or deployment. It records
the evidence-based operating posture for ongoing research.

## Why

No candidate feature has yet cleared a locked OOS test:

- `unique_buyers`: n=50 OOS futility look point fired `directional_null`; line
  stopped and archived.
- source-level: audited as null for the current cohort. Origin is constant,
  source-family/source-kind are degenerate, and component scores are unavailable.
- smart-money/wallet-quality: conceptually promising, but the leak-free as-of
  wallet-history join is currently cost-blocked.
- B1 Helius live-curve canary: sealed run did not PASS. Helius completed
  28/30 windows but had 0/30 usable curve windows; Dune full-window comparison
  on the same 30 windows found only 2/30 usable windows. This closes the
  "maybe a live curve provider rescues the current source" hypothesis.

The measurement layer is now materially stronger, but the business target is
ahead of the evidence. The current evidence does not support 60/60/200 as an
engineering target.

## Current Target Framing

- 60/60/200: aspirational only, not an active engineering promise.
- Active posture: shadow-only, silver-side observational research.
- Live strategy: frozen.

## Frozen Until New Evidence

Do not change:

- gate;
- matrix/RR;
- liquidity contract;
- exit;
- live size;
- production strategy behavior.

## What Can Re-Open Work

Only one of the following can re-open a candidate line:

- a sealed OOS preregistration with a new primary feature and a clean input
  pipeline;
- a provider canary PASS that only re-opens data provenance, followed by a
  separate preregistration;
- a materially better sourcing channel with point-in-time data that can be
  measured without look-ahead.

## What Cannot Re-Open Work

- secondary features from the stopped `unique_buyers` run;
- leaked daily AUCs;
- source fields that are constant or coverage shadows;
- post-anchor windows;
- unsealed threshold changes;
- live trading intuition.
- rerunning B1 with looser provider/completeness thresholds.

## Reversibility

This posture is reversible. If a future sealed OOS test clears its preregistered
criteria, the program can move from shadow-only research to a new shadow gate
design review. Live deployment would still require a separate readiness gate.

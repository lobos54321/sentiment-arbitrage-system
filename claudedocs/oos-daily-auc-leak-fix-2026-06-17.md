# OOS Daily Feature-Table AUC Leak Fix

Date: 2026-06-17

## Summary

The n=50 public lookpoint report and cumulative table remained sealed, but the
daily `curve-feature-table.json` artifacts contained `strata.*.features.*.auc`
from the feature-table builder. That violated the preregistration rule that no
AUC is computed between look points.

## Scope

Affected historical daily artifacts include OOS daily/soak
`work/curve-feature-table.json` files generated before this fix. Treat their
`strata.*.features.*.auc` values as leaked daily diagnostics. They must not be
used to select, rescue, or prioritize any future primary feature.

The cumulative OOS table and n=50 public lookpoint report did not contain
numeric AUC values.

## Fix

- `build-v10-curve-feature-table.js` now defaults to rows + coverage QA only.
- Offline discovery analysis must opt in with `--include-discrimination-report`.
- Daily wrappers fail closed if any exact `auc` key appears inside the feature
  table before a preregistered look point.
- The daily QA report now includes a feature-table AUC leak check.

## Rule

Next prereg primary candidates must be chosen from economic priors and fresh
locked specs, not from leaked historical daily per-feature AUCs.

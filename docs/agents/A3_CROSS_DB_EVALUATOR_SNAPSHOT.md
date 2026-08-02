# A3 Cross-Database Evaluator Snapshot

## Purpose

AutoLoop must not read SQLite databases that production writers are actively
mutating. A3 publishes one coherent evaluator bundle containing signal, paper,
raw-outcome, and K-line databases.

## Snapshot contract

The snapshot worker:

1. opens all four source databases read-only;
2. starts all four SQLite read transactions at one barrier, then waits at a
   second barrier until every read view is pinned;
3. copies the four pinned views concurrently with the SQLite backup API and
   releases each source read lock as soon as its own copy completes;
4. compacts each copied database without modifying the source;
5. records schema versions, table schema hashes, required upper watermarks,
   file sizes, SHA-256 hashes, `quick_check`, git commit, and read-view
   timestamps;
6. rejects the bundle if required tables are missing, a check fails, the source
   process reports a mutation, disk reserve is insufficient, or cross-database
   pin skew exceeds the configured limit;
7. atomically switches `agent_evidence/current` only after the complete bundle
   passes;
8. removes the previous validated evaluator snapshot only after the switch and
   only while no evaluator holds the shared snapshot lease.

Failed or partial runs never replace `current`.

## Bounded storage

The worker retains one current snapshot by default (`keep_previous=0`). During
refresh it temporarily needs the concurrent source copies plus the estimated
compacted bundle. A disk preflight preserves the configured free-space reserve.
If the reserve cannot be maintained, snapshot generation fails closed and the
previous current snapshot remains available.

## Evaluator enforcement

AutoLoop, staged OOS refresh, dedicated discovery workers, and dashboard runs
must all pass `evaluator_snapshot_bundle_contract.v1`. They reject:

- active production database paths;
- missing or malformed manifests;
- mixed snapshot identities;
- failed integrity or time-skew checks;
- missing SHA, schema, watermark, or size evidence.

The consumer recomputes every database SHA-256 and `quick_check` while holding
a shared lease on the snapshot publication lock. It resolves `current` to one
immutable snapshot directory before starting a report. The producer takes the
exclusive form of the same lock, so it cannot prune a bundle while an evaluator
is using it.

Any evaluator-derived rows, including the shadow decision bridge, are written
to a per-run `autoloop_research.db`, never to one of the four frozen databases.
The A_CLASS readiness report is invoked with `--read-only`: it may project the
next clean-window counter and paper-auto-resume eligibility, but it cannot
persist counters, trackers, mode changes, or operator audit rows. The snapshot
lease recomputes hashes and `quick_check` again after the run and fails closed
if any consumer changed a frozen database.

This is a read-only evaluation boundary. It does not modify strategy, gates,
A_CLASS, executors, canary settings, wallets, or risk parameters. Promotion
remains disabled.

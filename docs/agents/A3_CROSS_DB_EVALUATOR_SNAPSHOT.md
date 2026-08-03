# A3 v2 Cross-Database Evaluator Snapshot

## Purpose

AutoLoop must not read SQLite databases that production writers are actively
mutating. A3 publishes one coherent evaluator bundle containing signal, paper,
raw-outcome, and K-line databases.

## Snapshot contract

The snapshot worker:

1. opens all four source databases read-only;
2. starts all four SQLite read transactions at one barrier, then waits at a
   second barrier until every read view is pinned;
3. applies one common event-time upper bound to all four pinned views;
4. copies only the explicitly registered evaluator tables and bounded history
   directly into four new SQLite databases. It never creates a full source
   database intermediate;
5. records the selection mode, time column, lower/upper bounds and copied row
   count for every included table, plus every omitted source table and reason;
6. records schema versions, table schema hashes, required upper watermarks,
   file sizes, SHA-256 hashes, `quick_check`, git commit, and read-view
   timestamps;
7. rejects the bundle if required tables are missing, a check fails, the source
   process reports a mutation, disk reserve is insufficient, or cross-database
   pin skew exceeds the configured limit;
8. rejects any unexpected SQLite side file and counts the manifest plus all
   four databases against the whole-bundle cap;
9. atomically switches `agent_evidence/current` only after the complete bundle
   passes, and restores the old pointer/status if publication fails;
10. removes the previous validated evaluator snapshot only after the switch
   and status publication both succeed, and only while no evaluator holds the
   shared snapshot lease.

Failed or partial runs never replace `current`.

## Selection windows

The default v2 bundle contains:

- `96h` of high-volume candidate observations, virtual trades, decisions and
  opportunity rows. This covers 24h/48h/72h reports plus query grace;
- `35d` of Telegram signal identity, raw outcomes, raw minute bars, K-line
  evidence, missed-attribution and path evidence needed by longer outcome and
  replay checks;
- small state tables such as paper trades, canonical trade ledger, A_CLASS
  runtime state, provider state and pool mappings, bounded above by the same
  freeze timestamp even when no lower history bound is applied.

These are evaluator input bounds, not retention policy and not promotion
evidence. Every table's actual rule and row count is in the manifest. Future
rows above the common upper bound are excluded.

## Bounded storage

The worker retains one current snapshot by default (`keep_previous=0`). The
default whole-bundle cap is `10GiB`, split into per-database SQLite
`max_page_count` limits and rechecked against every published file. A preflight requires the full cap plus the configured
free-space reserve before extraction starts. If a database reaches its limit,
disk space is insufficient, a source is locked, or the process exits, the
partial directory is removed and the previous `current` snapshot remains
available. Refresh no longer needs a 23GB source copy plus another compacted
copy.

## Evaluator enforcement

AutoLoop, staged OOS refresh, dedicated discovery workers, and dashboard runs
must all pass `evaluator_snapshot_bundle_contract.v1`. They reject:

- active production database paths, symlinks, or hard-link aliases;
- missing or malformed manifests;
- mixed snapshot identities;
- failed integrity or time-skew checks;
- missing SHA, schema, watermark, or size evidence;
- inconsistent selection upper bounds, missing table-selection evidence, an
  exceeded output cap, full-backup intermediates, or SQLite `-wal`, `-shm`,
  journal, temporary, partial, or unknown bundle files.

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

The dashboard does not maintain a second weaker validator. Before starting an
AutoLoop run it invokes the same Python bundle contract used by the evaluator;
missing Python, timeout, malformed output, or any contract blocker fails
closed.

This is a read-only evaluation boundary. It does not modify strategy, gates,
A_CLASS, executors, canary settings, wallets, or risk parameters. Promotion
remains disabled.

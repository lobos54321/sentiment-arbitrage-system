# Data Lifecycle Retention v1

Status: implementation contract

Scope: paper and shadow evidence storage only. This contract does not change
strategy, gates, A_CLASS, final entry, executors, position size, wallet, or risk.

## Goal

Keep `/app/data` below a practical 80 GiB operating ceiling without deleting
the evidence needed to explain real trades or validate the current 24h/48h/72h
capture loop.

The system must not preserve every repeated scan forever. Data is retained by
decision value:

1. permanent evidence;
2. rolling research detail;
3. compact historical evidence;
4. disposable repeated telemetry.

## Storage Classes

### Class A - Permanent Evidence

Never selected by automated retention:

- `paper_trades`;
- `canonical_trade_ledger`;
- actual entry, fill, exit, and realized-PnL evidence;
- mode breach and recovery evidence;
- human approvals;
- frozen OOS definitions and proposal checkpoints.

These rows are the audit trail for real or paper capital decisions.

### Class B - Hot Research Detail

Keep row-level detail in SQLite long enough to reproduce the current capture
review:

- candidate observations: at least 4 days;
- A_CLASS and paper decision detail: at least 4 days;
- virtual candidate trades: at least 7 days;
- missed-dog and path evidence: at least 7 days.

This supports rolling 24h, 48h, and 72h reports without reading archives.

### Class C - Bounded Compressed Archive

When Class B rows age out:

- candidate observations and virtual trades are gzip archived with full row
  payloads for a bounded historical window;
- large decision rows are gzip archived as a compact projection that excludes
  repeated large JSON blobs but keeps identity, outcome, blocker, quote, and
  timing fields;
- every archive batch has a manifest, row count, cutoff, and SHA-256;
- the archive is reopened and verified before source rows are deleted;
- verified archive batches expire automatically after their declared
  retention period.

Historical memory is useful, but it is not permanent production evidence.

### Class D - Summary-Only Telemetry

High-frequency telemetry whose individual rows have little long-term value is
reduced to manifest counts by day and relevant dimensions. Old raw rows are
then deleted. Examples include old latency events and repeated watch-shadow
snapshots.

## Disk Watermarks

The retention report publishes filesystem use and one of:

- `normal`: below 70%;
- `soft`: at or above 70%;
- `hard`: at or above 82%;
- `critical`: at or above 90%, or below the configured free-space reserve.

Pressure may shorten research hot windows, but never below the 72h evaluation
floor for candidate and decision evidence. Permanent evidence is never made
eligible. At `critical`, verified research archives older than 14 days may be
expired early; summary manifests and permanent evidence remain protected.

### Capacity model

The incident baseline was approximately 6 GiB/day of gross paper DB growth.
Using that deliberately conservative rate:

- the normal four-day hot decision/candidate floor is about 24 GiB before page
  reuse;
- hard/critical pressure reduces the hot research floor to three days;
- even if bounded research archives compress only 3:1, fourteen critical-mode
  archive days add about 28 GiB;
- large A_CLASS and paper-decision rows use compact projections, so their
  actual archive contribution should be materially lower than that worst case.

The 70%/82%/90% watermarks therefore act before an 80 GiB volume is exhausted.
The live history replaces this estimate with measured DB/day and volume/day
after the second retention run.

## Runtime Contract

- retention runs at startup and periodically while the service stays up;
- the latest status is materialized at
  `/app/data/paper-db-retention-status.json`;
- a bounded 720-point history records DB/volume growth and estimates time to
  the hard watermark;
- runs are single-writer coordinated and bounded by row and time budgets;
- a marked or unhealthy paper DB is not touched;
- no automatic full `VACUUM` is required for steady state: deleted pages are
  reused by SQLite, bounding future file growth without needing a risky online
  rewrite;
- archive garbage collection only touches verified manifest-owned files under
  the configured retention directory;
- unknown, unpaired, or malformed files are never deleted automatically.

## Acceptance

1. Protected tables are never present in the retention policy list.
2. A test DB proves old research rows are archived/compacted and deleted while
   recent rows and permanent evidence remain.
3. Compact decision archives exclude large raw JSON payloads.
4. Candidate retention uses direct timestamp indexes and preserves the existing
   `observed_at` batch-coverage contract.
5. Archive expiry refuses unknown or unverified files.
6. A capacity simulation shows bounded steady-state storage under the current
   observed write rate.
7. `promotion_allowed` remains false; no production trading setting changes.

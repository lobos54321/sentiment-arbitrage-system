# Phase 2 Pool Observability

This document tracks the rollout impact of persisting `trades.pool_address` for open trades and passing it directly into Stage 3 backfill.

## What Phase 2 changed

Phase 2 introduced:

- nullable `trades.pool_address`
- persistence of `pool_address` on new trade inserts when already known
- Stage 3 sync reading `pool_address` from `trades`
- direct `poolAddress` injection into `MarketDataBackfillService.backfillWindow(...)`
- rollout-safe fallback when the column is missing or null

## What to watch

Primary outcomes:

- whether new open trades start carrying `pool_address`
- whether Stage 3 `open_trade` `no_pool` misses decline
- whether the effect is visible first on newer trades, not historical ones

---

## 1. Current OPEN trade pool coverage

Use this to measure current `pool_address` coverage among all open trades.

```sql
SELECT
  COUNT(*) AS total_open,
  SUM(CASE WHEN pool_address IS NOT NULL AND TRIM(pool_address) != '' THEN 1 ELSE 0 END) AS open_with_pool,
  SUM(CASE WHEN pool_address IS NULL OR TRIM(pool_address) = '' THEN 1 ELSE 0 END) AS open_without_pool,
  ROUND(
    1.0 * SUM(CASE WHEN pool_address IS NOT NULL AND TRIM(pool_address) != '' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
    4
  ) AS open_with_pool_rate
FROM trades
WHERE status = 'OPEN';
```

Interpretation:

- `open_with_pool_rate` should rise over time as newly created open trades persist `pool_address`
- historical open trades may remain null unless backfilled separately

---

## 2. Inspect latest OPEN trades

Use this to check whether newly opened trades are beginning to store `pool_address`.

```sql
SELECT
  token_ca,
  symbol,
  timestamp,
  entry_time,
  pool_address
FROM trades
WHERE status = 'OPEN'
ORDER BY COALESCE(entry_time, CAST(timestamp / 1000 AS INTEGER), timestamp) DESC
LIMIT 50;
```

Interpretation:

- focus on the newest rows
- verify whether `pool_address` is non-null for recent trade inserts

---

## 3. Coverage on the latest N OPEN trades

Use this to observe rollout effect on the newest open trades, which is more useful than all-time totals.

```sql
WITH latest_open AS (
  SELECT *
  FROM trades
  WHERE status = 'OPEN'
  ORDER BY COALESCE(entry_time, CAST(timestamp / 1000 AS INTEGER), timestamp) DESC
  LIMIT 100
)
SELECT
  COUNT(*) AS total_open,
  SUM(CASE WHEN pool_address IS NOT NULL AND TRIM(pool_address) != '' THEN 1 ELSE 0 END) AS with_pool,
  SUM(CASE WHEN pool_address IS NULL OR TRIM(pool_address) = '' THEN 1 ELSE 0 END) AS without_pool,
  ROUND(
    1.0 * SUM(CASE WHEN pool_address IS NOT NULL AND TRIM(pool_address) != '' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
    4
  ) AS with_pool_rate
FROM latest_open;
```

Interpretation:

- this query is the best quick check after rollout
- expect the newest 100 trades to show improvement before the full OPEN population does

---

## 4. Sample OPEN trades that already have pool_address

Use this to manually inspect stored pool values.

```sql
SELECT
  token_ca,
  symbol,
  timestamp,
  entry_time,
  pool_address
FROM trades
WHERE status = 'OPEN'
  AND pool_address IS NOT NULL
  AND TRIM(pool_address) != ''
ORDER BY COALESCE(entry_time, CAST(timestamp / 1000 AS INTEGER), timestamp) DESC
LIMIT 50;
```

Interpretation:

- useful for spot checking whether the stored pool values look valid
- useful before testing Stage 3 direct-input behavior

---

## 5. Sample OPEN trades still missing pool_address

Use this to inspect unresolved open trades.

```sql
SELECT
  token_ca,
  symbol,
  timestamp,
  entry_time
FROM trades
WHERE status = 'OPEN'
  AND (pool_address IS NULL OR TRIM(pool_address) = '')
ORDER BY COALESCE(entry_time, CAST(timestamp / 1000 AS INTEGER), timestamp) DESC
LIMIT 50;
```

Interpretation:

- useful for checking whether missing coverage is concentrated in older rows
- if recent rows still appear here, writer paths may need re-checking

---

## 6. Observe coverage after a rollout cutoff

If you know the rollout time, use a cutoff to isolate post-change trade creation.

Replace `1710000000` with the actual Unix timestamp in seconds.

```sql
SELECT
  COUNT(*) AS total_open_after_cutoff,
  SUM(CASE WHEN pool_address IS NOT NULL AND TRIM(pool_address) != '' THEN 1 ELSE 0 END) AS with_pool_after_cutoff,
  ROUND(
    1.0 * SUM(CASE WHEN pool_address IS NOT NULL AND TRIM(pool_address) != '' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
    4
  ) AS with_pool_rate_after_cutoff
FROM trades
WHERE status = 'OPEN'
  AND COALESCE(entry_time, CAST(timestamp / 1000 AS INTEGER), timestamp) >= 1710000000;
```

Interpretation:

- this isolates whether the rollout is working on newly created trades
- this is the cleanest signal if historical open trades are mostly null

---

## 7. Stage 3 metrics to compare before/after

When running `scripts/run-helius-pool-sync.js`, compare these output metrics:

- `processedOpenTrades`
- `openTradeNoPool`
- `openTradeNoPoolRate`

Expected direction:

- `processedOpenTrades`: may stay similar
- `openTradeNoPool`: should decline as new open trades carry `pool_address`
- `openTradeNoPoolRate`: should decline over time

## 8. Recommended rollout reading

Short-term expectation:

- historical open trades may remain null
- immediate improvement will appear mainly on new trades
- Stage 3 fallback behavior should still protect old rows without `pool_address`

Medium-term expectation:

- `open_with_pool_rate` rises on recent OPEN trades
- Stage 3 `open_trade` `no_pool` misses decrease
- direct `input` pool usage should become more common in resolver-source summaries

## 9. Suggested operator workflow

After rollout:

1. Run query #3 on the latest 100 open trades
2. Sample query #4 to inspect stored pool values
3. Run Stage 3 sync
4. Compare `openTradeNoPool` and `openTradeNoPoolRate`
5. Repeat after additional new trades accumulate

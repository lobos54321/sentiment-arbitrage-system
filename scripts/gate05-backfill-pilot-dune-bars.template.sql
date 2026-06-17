-- Gate-0.5 historical backfill pilot: stage-aware 1m bar export.
--
-- Purpose:
--   Convert the burned pilot signal windows into post-signal 1m native/SOL bars
--   using decoded pump.fun TradeEvent rows. This is a feasibility/reconciliation
--   export only. It must not include dog/dud labels or candidate metadata.
--
-- Input VALUES shape:
--   (window_id, token_ca, signal_ts, window_start_ts, window_end_ts,
--    pilot_source, has_observer_label)
--
-- Required operator guard:
--   Abort the provider fetch if Dune's estimated/final cost would exceed the
--   locked pilot ceiling of 30 credits. The pilot harness records actual cost
--   post-hoc, but the fetch step owns this pre-spend ceiling.
WITH signal_windows(
  window_id,
  token_ca,
  signal_ts,
  window_start_ts,
  window_end_ts,
  pilot_source,
  has_observer_label
) AS (
  VALUES
{{SIGNAL_WINDOWS_VALUES}}
),
window_bounds AS (
  SELECT
    min(window_start_ts) AS min_start_ts,
    max(window_end_ts) AS max_end_ts
  FROM signal_windows
),
tokens AS (
  SELECT DISTINCT token_ca FROM signal_windows
),
pumpfun_trades AS (
  SELECT
    mint AS token_ca,
    CAST(timestamp AS BIGINT) AS block_time,
    evt_tx_id AS signature,
    CASE WHEN COALESCE(is_buy, isBuy) THEN 'buy' ELSE 'sell' END AS side,
    CAST(user AS VARCHAR) AS user,
    CAST(COALESCE(sol_amount, solAmount) AS DOUBLE) / 1e9 AS sol_amount,
    CAST(COALESCE(token_amount, tokenAmount) AS DOUBLE) / 1e6 AS token_amount,
    CAST(COALESCE(virtual_sol_reserves, virtualSolReserves) AS DOUBLE) / 1e9 AS virtual_sol_reserves,
    CAST(COALESCE(virtual_token_reserves, virtualTokenReserves) AS DOUBLE) / 1e6 AS virtual_token_reserves,
    CAST(real_token_reserves AS DOUBLE) / 1e6 AS real_token_reserves
  FROM pumpdotfun_solana.pump_evt_tradeevent
  WHERE mint IN (SELECT token_ca FROM tokens)
    AND evt_block_date BETWEEN (SELECT CAST(from_unixtime(min_start_ts) AS DATE) FROM window_bounds)
                           AND (SELECT CAST(from_unixtime(max_end_ts) AS DATE) FROM window_bounds)
    AND CAST(timestamp AS BIGINT) BETWEEN (SELECT min_start_ts FROM window_bounds)
                                      AND (SELECT max_end_ts FROM window_bounds)
),
joined AS (
  SELECT
    w.window_id,
    w.token_ca,
    w.signal_ts,
    w.window_start_ts,
    w.window_end_ts,
    t.block_time,
    t.signature,
    t.side,
    t.user,
    t.sol_amount,
    t.token_amount,
    t.virtual_sol_reserves,
    t.virtual_token_reserves,
    t.real_token_reserves,
    CASE
      WHEN t.virtual_sol_reserves IS NOT NULL
       AND t.virtual_token_reserves IS NOT NULL
       AND t.virtual_token_reserves > 0
      THEN t.virtual_sol_reserves / t.virtual_token_reserves
      ELSE NULL
    END AS reserve_price_sol
  FROM signal_windows w
  JOIN pumpfun_trades t
    ON t.token_ca = w.token_ca
   AND t.block_time BETWEEN w.window_start_ts AND w.window_end_ts
),
minute_bars AS (
  SELECT
    window_id,
    token_ca,
    signal_ts,
    CAST(to_unixtime(date_trunc('minute', from_unixtime(block_time))) AS BIGINT) AS timestamp,
    min(block_time) AS min_block_time,
    max(block_time) AS max_block_time,
    count(*) AS trade_count,
    min_by(reserve_price_sol, block_time) AS open,
    max(reserve_price_sol) AS high,
    min(reserve_price_sol) AS low,
    max_by(reserve_price_sol, block_time) AS close,
    sum(COALESCE(sol_amount, 0)) AS volume
  FROM joined
  WHERE reserve_price_sol IS NOT NULL
  GROUP BY 1, 2, 3, 4
)
SELECT
  window_id,
  token_ca,
  signal_ts,
  timestamp,
  open,
  high,
  low,
  close,
  volume,
  'dune_pumpfun_tradeevent' AS provider,
  'bonding_curve' AS source_kind,
  'onchain_swap' AS source_family,
  concat('bonding_curve:', token_ca) AS pool_address,
  'native' AS price_unit,
  trade_count,
  min_block_time,
  max_block_time
FROM minute_bars
ORDER BY window_id, timestamp

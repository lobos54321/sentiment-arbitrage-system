-- Gate-0.5 historical backfill pilot: separate curve-presence stage tags.
--
-- Purpose:
--   Emit label-free curve-presence tags for the burned pilot windows. These
--   tags are separate from reconciliation/labeling bars. Do NOT use this output
--   to compute dog/dud labels; reconciliation bars must match the observer's
--   own price source (geckoterminal/local_cache/GMGN as applicable).
--
-- Input VALUES shape:
--   (window_id, token_ca, signal_ts, window_start_ts, window_end_ts,
--    pilot_source, has_observer_label)
--
-- Cost guard:
--   Run through scripts/run-dune-sql-export.py --max-credits 30, or an
--   equivalent fetch wrapper that aborts/cancels if credits exceed 30.
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
query_windows AS (
  SELECT
    *,
    signal_ts - 900 AS pre_window_start_ts,
    window_end_ts AS query_end_ts
  FROM signal_windows
),
window_bounds AS (
  SELECT
    min(pre_window_start_ts) AS min_start_ts,
    max(query_end_ts) AS max_end_ts
  FROM query_windows
),
tokens AS (
  SELECT DISTINCT token_ca FROM signal_windows
),
pumpfun_trades AS (
  SELECT
    mint AS token_ca,
    CAST(timestamp AS BIGINT) AS block_time,
    evt_tx_id AS signature
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
    w.pre_window_start_ts,
    w.window_start_ts,
    w.window_end_ts,
    t.block_time,
    t.signature
  FROM query_windows w
  LEFT JOIN pumpfun_trades t
    ON t.token_ca = w.token_ca
   AND t.block_time BETWEEN w.pre_window_start_ts AND w.window_end_ts
),
agg AS (
  SELECT
    window_id,
    token_ca,
    signal_ts,
    pre_window_start_ts,
    window_start_ts,
    window_end_ts,
    COUNT_IF(block_time BETWEEN pre_window_start_ts AND signal_ts) AS pre_signal_curve_trade_count,
    COUNT_IF(block_time > signal_ts AND block_time <= window_end_ts) AS post_signal_curve_trade_count,
    COUNT_IF(block_time BETWEEN pre_window_start_ts AND window_end_ts) AS curve_trade_count_total,
    COUNT_IF(block_time IS NOT NULL AND NOT (block_time BETWEEN pre_window_start_ts AND window_end_ts)) AS out_of_window_trade_count,
    MIN(block_time) AS first_curve_trade_ts,
    MAX(block_time) AS last_curve_trade_ts
  FROM joined
  GROUP BY 1,2,3,4,5,6
)
SELECT
  window_id,
  token_ca,
  signal_ts,
  pre_window_start_ts,
  window_start_ts,
  window_end_ts,
  pre_signal_curve_trade_count,
  post_signal_curve_trade_count,
  curve_trade_count_total,
  out_of_window_trade_count,
  first_curve_trade_ts,
  last_curve_trade_ts,
  CASE
    WHEN curve_trade_count_total > 0 THEN 'curve_activity_observed'
    ELSE 'no_curve_trade_observed'
  END AS stage_tag,
  'dune_pumpfun_tradeevent' AS stage_source
FROM agg
ORDER BY window_id

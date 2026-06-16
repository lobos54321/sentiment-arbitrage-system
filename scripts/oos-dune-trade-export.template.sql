-- V10 curve feature pump.fun TradeEvent export via Dune decoded table.
-- Generated locally; output is one row per TradeEvent joined to one signal window.
-- Paste this CTE into an indexed pump.fun TradeEvent query.
-- Required output fields are documented in claudedocs/v10-curve-feature-export-spec.md.
WITH signal_windows(window_id, token_ca, signal_ts, window_start_ts, window_end_ts, label, return_domain, effective_tier, query_start_ts, query_end_ts) AS (
  VALUES
{{SIGNAL_WINDOWS_VALUES}}
),
window_bounds AS (
  SELECT min(query_start_ts) AS min_start_ts, max(query_end_ts) AS max_end_ts FROM signal_windows
),
tokens AS (
  SELECT DISTINCT token_ca FROM signal_windows
),
source_trades AS (
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
    AND CAST(timestamp AS BIGINT) BETWEEN (SELECT min_start_ts FROM window_bounds) AND (SELECT max_end_ts FROM window_bounds)
),
joined AS (
  SELECT
    w.window_id,
    w.token_ca,
    w.signal_ts,
    w.window_start_ts,
    w.window_end_ts,
    w.label,
    w.return_domain,
    w.effective_tier,
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
      WHEN t.virtual_sol_reserves IS NOT NULL AND t.virtual_token_reserves IS NOT NULL AND t.virtual_token_reserves > 0
      THEN t.virtual_sol_reserves / t.virtual_token_reserves
      ELSE NULL
    END AS reserve_price_sol
  FROM signal_windows w
  JOIN source_trades t
    ON t.token_ca = w.token_ca
   AND t.block_time BETWEEN w.query_start_ts AND w.query_end_ts
)
SELECT *
FROM joined
ORDER BY window_id, block_time, signature

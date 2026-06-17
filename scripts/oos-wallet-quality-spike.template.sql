-- Smart-money-buy-share feasibility spike (outcome-free, no discrimination metrics).
--
-- Output is one row per stripped signal window. It measures whether
-- as-of wallet-quality features can be extracted with usable coverage/cost.
-- It must not contain outcome categories, tiers, ROC metrics, or separation metrics.

WITH signal_windows(window_id, token_ca, signal_ts, window_start_ts, window_end_ts, history_start_ts) AS (
  VALUES
{{SIGNAL_WINDOWS_VALUES}}
),
window_bounds AS (
  SELECT
    min(history_start_ts) AS min_history_start_ts,
    min(window_start_ts) AS min_window_start_ts,
    max(window_end_ts) AS max_window_end_ts
  FROM signal_windows
),
token_creators AS (
  SELECT
    mint AS token_ca,
    CAST(creator AS VARCHAR) AS creator_wallet
  FROM pumpdotfun_solana.pump_evt_createevent
  WHERE evt_block_date BETWEEN (SELECT CAST(from_unixtime(min_history_start_ts) AS DATE) FROM window_bounds)
                           AND (SELECT CAST(from_unixtime(max_window_end_ts) AS DATE) FROM window_bounds)
    AND mint IN (SELECT DISTINCT token_ca FROM signal_windows)
),
token_first_trade AS (
  SELECT token_ca, min(block_time) AS token_first_trade_ts
  FROM (
    SELECT
      mint AS token_ca,
      CAST(timestamp AS BIGINT) AS block_time
    FROM pumpdotfun_solana.pump_evt_tradeevent
    WHERE evt_block_date BETWEEN (SELECT CAST(from_unixtime(min_history_start_ts) AS DATE) FROM window_bounds)
                             AND (SELECT CAST(from_unixtime(max_window_end_ts) AS DATE) FROM window_bounds)
      AND mint IN (SELECT DISTINCT token_ca FROM signal_windows)
  )
  GROUP BY 1
),
inwindow_buys AS (
  SELECT
    w.window_id,
    w.token_ca,
    w.signal_ts,
    w.history_start_ts,
    t.wallet,
    sum(t.sol_amount) AS buy_sol_w,
    count(*) AS n_buys,
    min(t.block_time) AS first_buy_ts
  FROM signal_windows w
  JOIN (
    SELECT
      mint AS token_ca,
      CAST(timestamp AS BIGINT) AS block_time,
      CAST(user AS VARCHAR) AS wallet,
      CAST(COALESCE(sol_amount, solAmount) AS DOUBLE) / 1e9 AS sol_amount
    FROM pumpdotfun_solana.pump_evt_tradeevent
    WHERE evt_block_date BETWEEN (SELECT CAST(from_unixtime(min_window_start_ts) AS DATE) FROM window_bounds)
                             AND (SELECT CAST(from_unixtime(max_window_end_ts) AS DATE) FROM window_bounds)
      AND mint IN (SELECT DISTINCT token_ca FROM signal_windows)
      AND COALESCE(is_buy, isBuy)
      AND user IS NOT NULL
  ) t
    ON t.token_ca = w.token_ca
   AND t.block_time BETWEEN w.window_start_ts AND w.window_end_ts
  GROUP BY 1,2,3,4,5
),
candidate_prior_trades AS (
  SELECT
    mint AS token_ca,
    CAST(timestamp AS BIGINT) AS block_time,
    CASE WHEN COALESCE(is_buy, isBuy) THEN 'buy' ELSE 'sell' END AS side,
    CAST(user AS VARCHAR) AS wallet,
    CAST(COALESCE(sol_amount, solAmount) AS DOUBLE) / 1e9 AS sol_amount
  FROM pumpdotfun_solana.pump_evt_tradeevent
  WHERE evt_block_date BETWEEN (SELECT CAST(from_unixtime(min_history_start_ts) AS DATE) FROM window_bounds)
                           AND (SELECT CAST(from_unixtime(max_window_end_ts) AS DATE) FROM window_bounds)
    AND user IN (SELECT DISTINCT wallet FROM inwindow_buys)
    AND user IS NOT NULL
),
prior_token_first_trade AS (
  SELECT
    mint AS token_ca,
    min(CAST(timestamp AS BIGINT)) AS token_first_trade_ts
  FROM pumpdotfun_solana.pump_evt_tradeevent
  WHERE evt_block_date BETWEEN (SELECT CAST(from_unixtime(min_history_start_ts) AS DATE) FROM window_bounds)
                           AND (SELECT CAST(from_unixtime(max_window_end_ts) AS DATE) FROM window_bounds)
    AND mint IN (SELECT DISTINCT token_ca FROM candidate_prior_trades)
  GROUP BY 1
),
prior_by_wallet AS (
  SELECT
    b.window_id,
    b.wallet,
    count(*) AS prior_trades_n_all,
    max(t.block_time) AS max_prior_bt,
    count(DISTINCT t.token_ca) AS n_prior_tokens_all,
    sum(CASE WHEN t.side = 'buy' THEN -t.sol_amount ELSE t.sol_amount END) AS realized_sol_pnl_all,
    count(DISTINCT CASE WHEN t.block_time >= b.signal_ts - 30 * 86400 THEN t.token_ca END) AS n_prior_tokens_30d,
    sum(CASE WHEN t.block_time >= b.signal_ts - 30 * 86400 THEN CASE WHEN t.side = 'buy' THEN -t.sol_amount ELSE t.sol_amount END ELSE 0 END) AS realized_sol_pnl_30d,
    count(DISTINCT CASE WHEN t.block_time >= b.signal_ts - 14 * 86400 THEN t.token_ca END) AS n_prior_tokens_14d,
    sum(CASE WHEN t.block_time >= b.signal_ts - 14 * 86400 THEN CASE WHEN t.side = 'buy' THEN -t.sol_amount ELSE t.sol_amount END ELSE 0 END) AS realized_sol_pnl_14d,
    count(DISTINCT CASE WHEN t.block_time >= b.signal_ts - 7 * 86400 THEN t.token_ca END) AS n_prior_tokens_7d,
    sum(CASE WHEN t.block_time >= b.signal_ts - 7 * 86400 THEN CASE WHEN t.side = 'buy' THEN -t.sol_amount ELSE t.sol_amount END ELSE 0 END) AS realized_sol_pnl_7d,
    sum(CASE WHEN t.side = 'buy' THEN 1 ELSE 0 END) AS prior_buy_trades_all,
    sum(CASE WHEN t.side = 'buy'
               AND pt.token_first_trade_ts IS NOT NULL
               AND t.block_time <= pt.token_first_trade_ts + 5
             THEN 1 ELSE 0 END) AS prior_snipe_buy_trades_all
  FROM inwindow_buys b
  JOIN candidate_prior_trades t
    ON t.wallet = b.wallet
   AND t.block_time < b.signal_ts
   AND t.block_time >= b.history_start_ts
   AND t.token_ca <> b.token_ca
  LEFT JOIN prior_token_first_trade pt ON pt.token_ca = t.token_ca
  GROUP BY 1,2
),
buyer_quality AS (
  SELECT
    b.*,
    COALESCE(p.prior_trades_n_all, 0) AS prior_trades_n_all,
    p.max_prior_bt,
    COALESCE(p.n_prior_tokens_all, 0) AS n_prior_tokens_all,
    COALESCE(p.realized_sol_pnl_all, 0) AS realized_sol_pnl_all,
    COALESCE(p.n_prior_tokens_30d, 0) AS n_prior_tokens_30d,
    COALESCE(p.realized_sol_pnl_30d, 0) AS realized_sol_pnl_30d,
    COALESCE(p.n_prior_tokens_14d, 0) AS n_prior_tokens_14d,
    COALESCE(p.realized_sol_pnl_14d, 0) AS realized_sol_pnl_14d,
    COALESCE(p.n_prior_tokens_7d, 0) AS n_prior_tokens_7d,
    COALESCE(p.realized_sol_pnl_7d, 0) AS realized_sol_pnl_7d,
    COALESCE(p.prior_buy_trades_all, 0) AS prior_buy_trades_all,
    COALESCE(p.prior_snipe_buy_trades_all, 0) AS prior_snipe_buy_trades_all,
    CASE
      WHEN COALESCE(p.prior_buy_trades_all, 0) > 0
      THEN CAST(p.prior_snipe_buy_trades_all AS DOUBLE) / p.prior_buy_trades_all
      ELSE 0
    END AS prior_snipe_buy_rate_all,
    CASE
      WHEN tf.token_first_trade_ts IS NOT NULL AND b.first_buy_ts <= tf.token_first_trade_ts + 5 THEN 1
      ELSE 0
    END AS first_block_sniper_proxy,
    CASE
      WHEN tc.creator_wallet IS NOT NULL AND b.wallet = tc.creator_wallet THEN 1
      ELSE 0
    END AS creator_excluded,
    CASE
      WHEN COALESCE(p.prior_buy_trades_all, 0) >= 3
       AND CAST(COALESCE(p.prior_snipe_buy_trades_all, 0) AS DOUBLE) / NULLIF(p.prior_buy_trades_all, 0) >= 0.5
      THEN 1
      ELSE 0
    END AS prior_sniper_proxy
  FROM inwindow_buys b
  LEFT JOIN prior_by_wallet p ON p.window_id = b.window_id AND p.wallet = b.wallet
  LEFT JOIN token_first_trade tf ON tf.token_ca = b.token_ca
  LEFT JOIN token_creators tc ON tc.token_ca = b.token_ca
),
per_window AS (
  SELECT
    w.window_id,
    w.token_ca,
    w.signal_ts,
    true AS window_complete,
    count(DISTINCT q.wallet) AS n_buyers,
    COALESCE(sum(q.buy_sol_w), 0) AS buy_sol_total,
    count(DISTINCT CASE WHEN q.prior_trades_n_all > 0 THEN q.wallet END) AS n_buyers_with_prior_history,

    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.n_prior_tokens_7d >= 1 AND q.realized_sol_pnl_7d > 0 THEN q.wallet END) AS n_buyers_qualify_k1_7d,
    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.n_prior_tokens_7d >= 3 AND q.realized_sol_pnl_7d > 0 THEN q.wallet END) AS n_buyers_qualify_k3_7d,
    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.n_prior_tokens_7d >= 5 AND q.realized_sol_pnl_7d > 0 THEN q.wallet END) AS n_buyers_qualify_k5_7d,
    COALESCE(sum(CASE WHEN q.creator_excluded = 0 AND q.n_prior_tokens_7d >= 3 AND q.realized_sol_pnl_7d > 0 THEN q.buy_sol_w ELSE 0 END), 0) AS buy_sol_from_qualify_k3_7d,
    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.first_block_sniper_proxy = 0 AND q.prior_sniper_proxy = 0 AND q.n_prior_tokens_7d >= 1 AND q.realized_sol_pnl_7d > 0 THEN q.wallet END) AS n_buyers_qualify_k1_7d_nonsniper,
    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.first_block_sniper_proxy = 0 AND q.prior_sniper_proxy = 0 AND q.n_prior_tokens_7d >= 3 AND q.realized_sol_pnl_7d > 0 THEN q.wallet END) AS n_buyers_qualify_k3_7d_nonsniper,
    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.first_block_sniper_proxy = 0 AND q.prior_sniper_proxy = 0 AND q.n_prior_tokens_7d >= 5 AND q.realized_sol_pnl_7d > 0 THEN q.wallet END) AS n_buyers_qualify_k5_7d_nonsniper,
    COALESCE(sum(CASE WHEN q.creator_excluded = 0 AND q.first_block_sniper_proxy = 0 AND q.prior_sniper_proxy = 0 AND q.n_prior_tokens_7d >= 3 AND q.realized_sol_pnl_7d > 0 THEN q.buy_sol_w ELSE 0 END), 0) AS buy_sol_from_qualify_k3_7d_nonsniper,

    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.n_prior_tokens_14d >= 1 AND q.realized_sol_pnl_14d > 0 THEN q.wallet END) AS n_buyers_qualify_k1_14d,
    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.n_prior_tokens_14d >= 3 AND q.realized_sol_pnl_14d > 0 THEN q.wallet END) AS n_buyers_qualify_k3_14d,
    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.n_prior_tokens_14d >= 5 AND q.realized_sol_pnl_14d > 0 THEN q.wallet END) AS n_buyers_qualify_k5_14d,
    COALESCE(sum(CASE WHEN q.creator_excluded = 0 AND q.n_prior_tokens_14d >= 3 AND q.realized_sol_pnl_14d > 0 THEN q.buy_sol_w ELSE 0 END), 0) AS buy_sol_from_qualify_k3_14d,
    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.first_block_sniper_proxy = 0 AND q.prior_sniper_proxy = 0 AND q.n_prior_tokens_14d >= 1 AND q.realized_sol_pnl_14d > 0 THEN q.wallet END) AS n_buyers_qualify_k1_14d_nonsniper,
    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.first_block_sniper_proxy = 0 AND q.prior_sniper_proxy = 0 AND q.n_prior_tokens_14d >= 3 AND q.realized_sol_pnl_14d > 0 THEN q.wallet END) AS n_buyers_qualify_k3_14d_nonsniper,
    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.first_block_sniper_proxy = 0 AND q.prior_sniper_proxy = 0 AND q.n_prior_tokens_14d >= 5 AND q.realized_sol_pnl_14d > 0 THEN q.wallet END) AS n_buyers_qualify_k5_14d_nonsniper,
    COALESCE(sum(CASE WHEN q.creator_excluded = 0 AND q.first_block_sniper_proxy = 0 AND q.prior_sniper_proxy = 0 AND q.n_prior_tokens_14d >= 3 AND q.realized_sol_pnl_14d > 0 THEN q.buy_sol_w ELSE 0 END), 0) AS buy_sol_from_qualify_k3_14d_nonsniper,

    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.n_prior_tokens_30d >= 1 AND q.realized_sol_pnl_30d > 0 THEN q.wallet END) AS n_buyers_qualify_k1_30d,
    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.n_prior_tokens_30d >= 3 AND q.realized_sol_pnl_30d > 0 THEN q.wallet END) AS n_buyers_qualify_k3_30d,
    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.n_prior_tokens_30d >= 5 AND q.realized_sol_pnl_30d > 0 THEN q.wallet END) AS n_buyers_qualify_k5_30d,
    COALESCE(sum(CASE WHEN q.creator_excluded = 0 AND q.n_prior_tokens_30d >= 3 AND q.realized_sol_pnl_30d > 0 THEN q.buy_sol_w ELSE 0 END), 0) AS buy_sol_from_qualify_k3_30d,
    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.first_block_sniper_proxy = 0 AND q.prior_sniper_proxy = 0 AND q.n_prior_tokens_30d >= 1 AND q.realized_sol_pnl_30d > 0 THEN q.wallet END) AS n_buyers_qualify_k1_30d_nonsniper,
    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.first_block_sniper_proxy = 0 AND q.prior_sniper_proxy = 0 AND q.n_prior_tokens_30d >= 3 AND q.realized_sol_pnl_30d > 0 THEN q.wallet END) AS n_buyers_qualify_k3_30d_nonsniper,
    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.first_block_sniper_proxy = 0 AND q.prior_sniper_proxy = 0 AND q.n_prior_tokens_30d >= 5 AND q.realized_sol_pnl_30d > 0 THEN q.wallet END) AS n_buyers_qualify_k5_30d_nonsniper,
    COALESCE(sum(CASE WHEN q.creator_excluded = 0 AND q.first_block_sniper_proxy = 0 AND q.prior_sniper_proxy = 0 AND q.n_prior_tokens_30d >= 3 AND q.realized_sol_pnl_30d > 0 THEN q.buy_sol_w ELSE 0 END), 0) AS buy_sol_from_qualify_k3_30d_nonsniper,

    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.n_prior_tokens_all >= 1 AND q.realized_sol_pnl_all > 0 THEN q.wallet END) AS n_buyers_qualify_k1_all,
    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.n_prior_tokens_all >= 3 AND q.realized_sol_pnl_all > 0 THEN q.wallet END) AS n_buyers_qualify_k3_all,
    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.n_prior_tokens_all >= 5 AND q.realized_sol_pnl_all > 0 THEN q.wallet END) AS n_buyers_qualify_k5_all,
    COALESCE(sum(CASE WHEN q.creator_excluded = 0 AND q.n_prior_tokens_all >= 3 AND q.realized_sol_pnl_all > 0 THEN q.buy_sol_w ELSE 0 END), 0) AS buy_sol_from_qualify_k3_all,
    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.first_block_sniper_proxy = 0 AND q.prior_sniper_proxy = 0 AND q.n_prior_tokens_all >= 1 AND q.realized_sol_pnl_all > 0 THEN q.wallet END) AS n_buyers_qualify_k1_all_nonsniper,
    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.first_block_sniper_proxy = 0 AND q.prior_sniper_proxy = 0 AND q.n_prior_tokens_all >= 3 AND q.realized_sol_pnl_all > 0 THEN q.wallet END) AS n_buyers_qualify_k3_all_nonsniper,
    count(DISTINCT CASE WHEN q.creator_excluded = 0 AND q.first_block_sniper_proxy = 0 AND q.prior_sniper_proxy = 0 AND q.n_prior_tokens_all >= 5 AND q.realized_sol_pnl_all > 0 THEN q.wallet END) AS n_buyers_qualify_k5_all_nonsniper,
    COALESCE(sum(CASE WHEN q.creator_excluded = 0 AND q.first_block_sniper_proxy = 0 AND q.prior_sniper_proxy = 0 AND q.n_prior_tokens_all >= 3 AND q.realized_sol_pnl_all > 0 THEN q.buy_sol_w ELSE 0 END), 0) AS buy_sol_from_qualify_k3_all_nonsniper,

    count(DISTINCT CASE WHEN q.creator_excluded = 1 THEN q.wallet END) AS n_buyers_creator_excluded,
    count(DISTINCT CASE WHEN q.first_block_sniper_proxy = 1 THEN q.wallet END) AS n_buyers_first_block_sniper,
    count(DISTINCT CASE WHEN q.prior_sniper_proxy = 1 THEN q.wallet END) AS n_buyers_prior_sniper,
    count(DISTINCT CASE WHEN q.first_block_sniper_proxy = 1 OR q.prior_sniper_proxy = 1 THEN q.wallet END) AS n_buyers_any_sniper_proxy,
    approx_percentile(q.prior_trades_n_all, 0.5) AS prior_trades_per_buyer_median,
    approx_percentile(q.prior_trades_n_all, 0.9) AS prior_trades_per_buyer_p90,
    approx_percentile(q.prior_snipe_buy_rate_all, 0.5) AS prior_snipe_rate_buyer_median,
    approx_percentile(q.prior_snipe_buy_rate_all, 0.9) AS prior_snipe_rate_buyer_p90,
    max(q.max_prior_bt) AS max_prior_bt,
    CASE WHEN max(q.max_prior_bt) IS NULL OR max(q.max_prior_bt) < w.signal_ts THEN true ELSE false END AS asof_ok
  FROM signal_windows w
  LEFT JOIN buyer_quality q ON q.window_id = w.window_id
  GROUP BY 1,2,3
)
SELECT *
FROM per_window
ORDER BY window_id

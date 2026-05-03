# Current Paper Trading System

Updated: 2026-05-03

This document describes the active paper-trading path in this repo. Older docs
such as the root `README.md` still describe an earlier live/GMGN-executor
architecture and should not be treated as the current source of truth.

## One Sentence

The system uses Premium Telegram signals as the main alpha feed, enriches them
with DexScreener, Jupiter/shared quotes, Helius, lifecycle classification, and
GMGN read-only data, then paper-trades only after route-specific gates,
readiness checks, SmartEntry timing, and Jupiter-compatible quote simulation.

## Runtime Ownership

- Node receives and parses premium signals.
- Node `PremiumSignalEngine` records signal rows into `data/sentiment_arb.db`.
- Python `scripts/paper_trade_monitor.py` is the active paper-trading owner.
- Node `scripts/execution_bridge.js` provides the quote/simulation bridge used by
  Python for paper fills and exits.
- Dashboard APIs read `data/paper_trades.db` and expose health, attribution, and
  performance slices.

Key modules:

- `src/inputs/premium-channel-listener.js`: Telegram premium channel listener.
- `src/engines/premium-signal-engine.js`: premium signal parsing, upstream gates,
  and `premium_signals` writes.
- `scripts/paper_trade_monitor.py`: active paper lifecycle monitor.
- `scripts/watchlist_store.py`: watchlist state machine.
- `scripts/signal_router.py`: route selection.
- `scripts/lotto_engine.py`: early-token LOTTO lane.
- `scripts/matrix_evaluator.py`: watchlist Matrix lane.
- `scripts/entry_readiness_policy.py`: live readiness profile before entry.
- `scripts/entry_engine.py`: SmartEntry timing and entry edge checks.
- `scripts/exit_engine.py`: fast Guardian exits and partial locks.
- `src/execution/paper-live-position-monitor.js`: canonical paper exit bridge
  helpers.
- `scripts/paper_decision_audit.py`: decision event and missed attribution
  tables.

## Data Sources

### Premium Telegram Signals

Purpose: primary signal source.

Stored in: `data/sentiment_arb.db`, table `premium_signals`.

Used fields include:

- token address
- symbol
- signal type: `NEW_TRENDING`, `ATH`, or derived fallback
- market cap
- holder count
- volume
- top holder text from raw message
- raw description
- upstream gate status
- timestamps

Important behavior:

- The system still treats premium signals as the main entry feed.
- GMGN trending does not currently create an independent watchlist entry by
  itself.

### DexScreener

Purpose: market mark, liquidity, volume, transaction counts, buy/sell pressure,
and pair discovery.

Common fields:

- `liquidity_usd`
- `vol_m5`
- `vol_h1`
- `buys_m5`
- `sells_m5`
- `price_change_m5`
- `market_cap`
- `dex_id`
- `pair_address`

Unit:

- Mostly USD-based market data and percent changes.

Important behavior:

- Good for trend and liquidity context.
- Not trusted as the final fill price.
- Missing DexScreener data is route-dependent: some watchlist Matrix scoring can
  fall back or fail-open, while LOTTO defensive checks usually wait or expire.

### Jupiter / Shared Quote

Purpose: executable price truth for paper entry, paper exit, and PnL baseline.

Path:

`paper_trade_monitor.py -> scripts/execution_bridge.js -> ParityExecutor -> JupiterUltraExecutor/shared quote`

Unit:

- SOL per token.

Important behavior:

- Entry quote failure is fail-closed. The system does not enter without a valid
  quote.
- Paper entry price is the Jupiter-compatible quoted fill price, not the Matrix
  trigger price.
- Exit quotes are also used to sanity-check mark-price exits.
- Paper penalties are applied by `ParityExecutor` to approximate slippage, delay,
  and fees.

### GeckoTerminal / Kline Cache / Synthetic Bars

Purpose: K-line shape, structure lows, EMA/phase policy, and trend fallback.

Important behavior:

- K-line data can be stale or unavailable for very new pump.fun tokens.
- For native SOL/token decisions, code tries to use `native_only=True` where
  needed so USD bars do not trigger false exits against SOL-denominated entry
  prices.
- Synthetic bars from observed prices are used as fallback.

### Helius

Purpose: live token concentration and rough activity/TPS signals.

Used in:

- LOTTO live top1/top10 concentration checks.
- Exit threat tightening where TPS and thin-pool context matter.

### GMGN Read-Only

Purpose: enrichment and shadow alpha evidence.

Used for:

- smart money / renowned wallet context
- toxic holder and trader flags
- bundler / rat trader / entrapment / creator risk
- GMGN tiny scout rescue only when a near-miss is otherwise clean
- external alpha shadow state and health

Not used for:

- executable entry price
- executable exit price
- PnL accounting
- direct order placement

Current GMGN role:

- Optional enrichment for LOTTO.
- Optional tiny scout rescue for clean near-misses.
- Shadow discovery evidence in `external_alpha_state`.
- Not yet an independent alpha route that can create and trade a watchlist item
  on its own.

## Signal To Watchlist Flow

1. Telegram listener receives a premium message.
2. Premium parser extracts token, symbol, signal type, market cap, and metadata.
3. Premium engine writes a row into `premium_signals`.
4. Paper monitor polls new rows from local DB or remote signal export.
5. Paper monitor records `signal_ingest` into `paper_decision_events`.
6. `signal_router.py` chooses a route:
   - fresh `NEW_TRENDING` under 30K market cap -> `LOTTO`
   - stale low market cap -> `WATCHLIST`
   - `NEW_TRENDING` 30K-80K -> watchlist Matrix-light path
   - ATH for active LOTTO holding -> LOTTO hold boost
   - high market cap ATH above 200K -> watch only
   - default ATH -> Matrix lane
7. `watchlist_store.py` registers or refreshes the token.
8. Repeated ATH updates refresh the watchlist anchor instead of letting an old
   ATH timestamp dominate current readiness.

Watchlist states:

- `watching`: candidate can be evaluated.
- `holding`: paper position is open.
- `moon_bag`: partial profit was locked and remainder is being trailed.
- `expired`: candidate is no longer eligible unless reactivated by a new signal.

## Entry Lanes

### LOTTO Lane

Goal: catch very early, low market cap meme launches without waiting for the full
Matrix.

Default size:

- normal LOTTO: `0.05 SOL`
- midcap LOTTO: `0.03 SOL`
- concentrated scout: `0.015 SOL`
- explosive direct scout: `0.008 SOL`
- GMGN tiny scout: usually `0.003` to `0.005 SOL`

Main checks:

- signal age must be under LOTTO stale window
- max concurrent LOTTO positions
- market cap must fit micro or midcap tier
- holders must not be too low
- volume/activity must be confirmed
- top10 from signal must not be too high
- liquidity must be at least 5K USD, unless pump.fun liquidity is unknown but
  m5 activity confirms there is life
- midcap tokens need stronger liquidity, m5 volume, and m5 transactions
- Helius live top1/top10 concentration must be acceptable
- GMGN policy can reject, downsize, boost, or tiny-rescue a clean near-miss
- falling-knife and lifecycle checks can wait or block
- token quarantine/history can block repeat failures

Meaning:

- LOTTO is deliberately defensive for very early tokens.
- It does not buy merely because a token is new.
- It buys only when early activity, liquidity, concentration, lifecycle, and
  timing are acceptable enough for the configured tiny paper exposure.

### Matrix Lane

Goal: wait for watchlist tokens to prove enough live strength before entry.

Five scores:

- Trend: mainly DexScreener m5 price change plus buy/sell ratio.
- Volume: DexScreener m5/h1 activity, then fallback volume sources.
- Price strength: current price versus signal price and recovery from lows.
- Realtime momentum: short live price movement check.
- Signal evolution: repeated signals, ATH update, and signal heat.

Typical result:

- `wait`: scores are not aligned yet.
- `fire`: Matrix says the candidate can move to pending entry.
- `remove`: timeout, collapse, or max entries.

Important behavior:

- Matrix `fire` is not a buy.
- It only creates a pending entry candidate. Readiness, SmartEntry, execution
  quote, and edge budget still have to pass.

### Readiness Preflight

Purpose: prevent old/historical strength from becoming a current buy.

Pipeline position:

- Runs after watchlist Matrix/LOTTO wants to fire, before SmartEntry/quote.

Profiles include:

- `LOTTO_NEWBORN_RISKY`
- `LOTTO_NORMAL`
- `LOTTO_REAL_PROBE`
- `ATH_CONTINUATION`
- `ATH_STALE`
- `ATH_DEEP_RECLAIM`
- `MATRIX_NORMAL`

Behavior:

- `ARM`: allowed to wait for timing node.
- `WAIT`: not allowed now; watchlist gets a cooldown and reason.
- `EXPIRE`: bad lifecycle should be expired instead of entering.

Important current fix:

- Stale ATH requires fresh high or sustained ATH evidence.
- This blocks old ATH anchors from repeatedly becoming pending entries.

### SmartEntry Timing

Purpose: decide whether the current exact moment is usable.

Entry modes include:

- `momentum_direct_entry`
- `smart_entry_pullback_bounce`
- `explosive_newborn_direct_scout`
- `gmgn_concentration_tiny_scout`
- `gmgn_midcap_near_miss_scout`

Important behavior:

- No current price -> fail-closed for entry timing.
- Low liquidity logs and rejects with explicit liquidity amount.
- Pullback/bounce logic tries not to chase tops.
- SmartEntry can reject and send the token back to watchlist for limited retries.

## Execution And Fill Accounting

Paper entry does not use the trigger price as cost basis.

Flow:

1. Pending entry passes readiness and SmartEntry.
2. Python calls `simulate_entry_execution()`.
3. `execution_bridge.js` requests a Jupiter-compatible quote.
4. `ParityExecutor` applies paper slippage/delay/fee penalty.
5. The system writes `paper_trades.entry_price` from the quote fill price.
6. The Matrix/SmartEntry trigger price is stored separately for spread analysis.
7. Entry is aborted if the quote is invalid, unavailable, or outside edge budget.

Why this matters:

- DexScreener/GMGN/GeckoTerminal are market marks.
- Jupiter quote is closer to what this position could actually trade at.
- PnL, stop loss, trailing, and accounting must use the SOL/token quote baseline.

## Sizing

Current sizing is conservative and partly fixed.

LOTTO:

- fixed route sizes, with GMGN downsize support.

Matrix:

- Kelly logic exists, but comments indicate historical Kelly data was considered
  poisoned.
- Non-ATH Matrix commonly gets bumped toward a systemic floor.
- ATH has market-cap tier sizing.
- Liquidity cap can reduce size to avoid taking too much of the pool.

Trader interpretation:

- This is closer to controlled forward-testing than aggressive capital
  deployment.
- That is appropriate while attribution quality is still improving.

## Exit Flow

There are multiple exit layers. This is powerful but also the largest source of
possible rule conflict.

### Exit Guardian

Purpose: fast safety thread, every roughly 3 seconds.

Handles:

- LOTTO fast fail
- MATRIX/ATH dead-on-arrival fast exit
- hard stop loss with double-tap confirmation
- gap crash
- real-time peak updates
- velocity and flat-top threat score
- partial locks
- Phase0 profit protection
- LOTTO partial lock and wide trail
- ATH phase trail support

Meaning:

- Guardian is meant to catch fast rugs and fast givebacks.
- It queues exits; main loop processes them and records audit.

### Exit Matrix

Purpose: slower canonical position evaluation in the main monitor loop.

Handles:

- LOTTO exit through `evaluate_lotto_exit()`
- Matrix/ATH exit through `ExitMatrixEvaluator`
- moon bag exit through `evaluate_moon_bag()`
- quote sanity checks before committing exits
- partial sell accounting
- watchlist state updates

LOTTO exit behavior:

- disaster hard floor around -30%
- normal stop around -18%
- no-follow exits after 60/120 seconds if peak is too weak
- profit-protect floor after positive peak
- breakeven floor after enough profit
- partial lock around +20% peak
- wide phase trails after +50%, +200%, +500%

ATH / Matrix exit behavior:

- hard stop / DOA exits
- profit lock around larger positive peaks
- Phase0 micro-trail after enough early profit
- velocity-aware trail factors
- trend death checks
- timeout
- moon bag trails for remaining position

### Phase Policy

Purpose: mostly attribution/shadow policy.

It classifies a position into phases such as:

- `NO_FOLLOW`
- `PROTECT_PROFIT`
- `RECOVER_PRINCIPAL`
- `MOON_RUNNER`
- `RUG_DEFENSE`

Current behavior:

- Mostly records what it would do.
- Can be live for limited LOTTO rug/no-follow situations if enabled.

## Observability

Important tables:

- `paper_trades`: actual paper positions and outcomes.
- `paper_trade_path_samples`: per-position path samples.
- `paper_decision_events`: append-only decision boundary log.
- `paper_missed_signal_attribution`: rejected/skipped candidates and later PnL.
- `external_alpha_snapshots`: raw external alpha candidate snapshots.
- `external_alpha_state`: deduped external alpha shadow state.
- `external_alpha_health`: GMGN scout health.

Important dashboard APIs:

- `/api/paper/data-source-policy`: explicit fail-open/fail-closed policy.
- `/api/paper/data-source-health`: source health and fail-mode counters.
- `/api/paper/price-unit-audit`: verifies recent trades carry explicit
  `SOL_PER_TOKEN` price units, `RATIO_DECIMAL` PnL units, and `SOL` accounting
  units.
- `/api/paper/entry-mode-performance`: performance split by `entry_mode`.
- `/api/paper/lifecycle-summary`: final lifecycle outcome with one canonical
  `final_blocker`, plus missed PnL summary.
- `/api/paper/missed-attribution`: missed signal tiers and unique token view.

## Fail Modes

Entry:

- Entry quote failure: fail-closed.
- SmartEntry no price: fail-closed.
- GMGN unavailable: fail-soft; no boost or rescue, but do not reject the base
  signal solely because GMGN is down.
- DexScreener missing: route-dependent.

Exit:

- Missing fresh mark price: hold and log.
- Exit quote failure: record failure and keep monitoring.
- Repeated `no_route` or `token_not_tradable` can trigger trapped-position
  fail-safe accounting.

## Current Strengths

- Executable quote price is treated separately from market mark price.
- LOTTO and Matrix lanes are separated.
- GMGN tiny scout is small and separately labeled.
- `entry_mode` is now first-class in `paper_trades`.
- Decision events are rich enough to explain most skipped entries.
- Missed attribution can identify whether a blocked token later ran.
- Readiness preflight prevents stale ATH anchors from becoming automatic buys.
- Exit logic has fast protection for rugs and slow protection for quote sanity.

## Current Risks

### 1. Data Unit Consistency

High risk.

The code writes a price-unit contract into entry/exit execution audit payloads:

- fill prices: `SOL_PER_TOKEN`
- trigger/quote prices used for entry and exit: `SOL_PER_TOKEN`
- PnL in DB: `RATIO_DECIMAL`
- accounting totals: `SOL`
- DexScreener/GMGN/GeckoTerminal USD fields: context only

This is now checkable through `/api/paper/price-unit-audit`.

The system still touches several sources:

- DexScreener USD price/market cap/liquidity
- GeckoTerminal/K-line data that may be USD
- Jupiter quote SOL/token
- signal price from premium parser
- cached/synthetic prices

Any mixed comparison can distort PnL, stop loss, trail, spread, or strength.
Historical rows may still lack unit metadata, but new rows should be auditable.

### 2. Exit Rule Duplication

Medium to high risk.

Guardian and ExitMatrix both understand hard stops, profit protect, LOTTO exits,
ATH exits, and trailing. There is dedupe and stale protection, but overlapping
authority can still produce conflict:

- one layer wants to hold
- another layer wants to exit
- one layer uses a fast mark
- another layer uses quote sanity

Preferred long-term shape:

- Guardian: fast trigger and emergency evidence collector.
- ExitMatrix: canonical exit reason and accounting owner.

### 3. Too Many Gates Can Hide Edge

Medium risk.

LOTTO candidates can be filtered by LOTTO gate, GMGN policy, falling knife,
lifecycle, token risk, readiness, SmartEntry, spread memory, and execution
budget. That reduces bad buys, but it can also block real runners.

The right answer is not adding more gates. The next step is better attribution:
which single final blocker actually caused a missed winner?

### 4. GMGN Is Not Independent Alpha Yet

Medium strategic gap.

GMGN scout writes external alpha shadow state. It can enrich a premium-token
candidate and rescue small near-misses. It does not yet independently promote a
GMGN-discovered token into watchlist and paper entry.

This is acceptable for now, but it means the system is still premium-signal-led.

### 5. Fail-Open Policy Is Not Uniform

Medium risk.

Some missing data is allowed to pass as unknown, some waits, and some rejects.
That is not automatically wrong, but it must remain visible in dashboard health.
Otherwise performance can look like strategy failure when the real issue is data
degradation.

### 6. Tiny Scout Performance Must Stay Separate

Medium risk.

GMGN tiny scout and LOTTO scout positions are probes, not normal trades. They
must be evaluated by `entry_mode`, otherwise small experimental positions can
pollute the main strategy win rate.

## Trader Evaluation

From a high-win-rate operator's perspective, the system is careful and
well-instrumented, but not yet sharp enough to be called a simple repeatable
edge.

What is good:

- It does not blindly chase every signal.
- It distinguishes discovery, timing, quote, and exit.
- It uses very small size for uncertain early-token probes.
- It has enough logging to learn from misses instead of arguing from anecdotes.
- It increasingly treats executable quote truth as more important than chart
  marks.

What still feels weak:

- Entry logic has become complex enough that the system may pass on obvious
  momentum because several defensive gates do not agree at the same instant.
- Exit logic is protective but over-specified; duplicated trails can cap upside
  if not measured carefully.
- The system has not yet proven which `entry_mode` actually makes money over a
  clean forward sample.
- Pump.fun-specific mechanics are still generic: there is no dedicated bonding
  curve progress, graduation, migration, or creator-wallet playbook.
- GMGN discovery is not yet a closed loop.

Practical operating stance:

- Keep paper mode.
- Judge results by route and `entry_mode`, not by blended PnL.
- Watch `tradable_missed` winners before loosening gates.
- Do not increase size until the system has a clean cohort of closed trades with
  quote-based PnL and final blockers.

## Recommended Next Architecture

Entry:

`data_health -> hard_risk -> lifecycle/readiness -> route_gate -> SmartEntry timing -> execution_quote`

Each lifecycle exposes one canonical `final_blocker` in
`/api/paper/lifecycle-summary`.

Exit:

`Guardian trigger -> ExitMatrix canonical reason -> quote sanity -> accounting`

Guardian should stay fast. ExitMatrix should own the final reason.

GMGN alpha:

1. Shadow only.
2. Watchlist only after momentum/liquidity/clean-risk confirmation.
3. Tiny paper only after 24-48 hours of shadow evidence.

Pump.fun:

- Add bonding-curve progress and graduation state.
- Separate curve-stage entries from normal DEX liquidity entries.
- Track creator/dev wallet behavior.
- Use TP/SL/moon-bag logic designed for bonding-curve launches, not generic DEX
  chart marks.

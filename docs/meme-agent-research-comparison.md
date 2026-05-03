# Meme Agent Research Comparison

Updated: 2026-05-03

This document compares our current paper-trading system with several open-source
Solana/meme agent projects. The goal is not to replace the current trading core.
The goal is to identify useful patterns we can borrow without importing weak or
unsafe trading behavior.

Local research clones:

- `/private/tmp/meme-agent-research/solana-agent-kit`
- `/private/tmp/meme-agent-research/goat`
- `/private/tmp/meme-agent-research/treecitywes-pumpfun`
- `/private/tmp/meme-agent-research/openprawn`

## Bottom Line

Our system is stronger as a trading research engine than the external projects I
reviewed. It has better attribution, quote-based accounting, route separation,
and risk logs.

The external projects are still useful:

- Solana Agent Kit and GOAT are useful as tool/plugin architecture references.
- TreeCityWes PumpFun bot is useful as a simple bonding-curve exit playbook.
- OpenPrawn is useful as a Telegram/Discord personal-agent UX reference.

None of them should be copied as the trading brain.

## Our System

Current shape:

- Premium Telegram signals are the main alpha source.
- DexScreener, GeckoTerminal, Helius, GMGN, and kline data enrich/filter the
  signal.
- Jupiter/shared quote is the executable price truth for paper entry/exit.
- LOTTO and Matrix are separate entry lanes.
- Readiness preflight blocks stale ATH and bad lifecycle entries before SmartEntry.
- SmartEntry decides exact timing.
- Paper execution bridge simulates quote, slippage, delay, and fees.
- Guardian and ExitMatrix handle exits.
- `paper_decision_events`, missed attribution, lifecycle summary, and
  `entry_mode` provide analysis slices.

What this means:

- The system is already closer to a real trading lab than a chatbot trader.
- The biggest edge is not "more AI"; it is cleaner decision contracts and better
  market microstructure data.

## Solana Agent Kit

Repo reviewed: `sendaifun/solana-agent-kit`.

What it is:

- A Solana action toolkit for agents.
- Plugin-based.
- Actions have name, similes, description, examples, Zod schema, and handler.
- Adapters exist for common agent frameworks.
- Token plugin includes Jupiter trade, DexScreener, RugCheck, Pump.fun launch,
  Pyth, Solana balance/transfer, and other actions.

Useful patterns to borrow:

- Strong action schema around every agent-exposed capability.
- Zod validation before handler execution.
- Action examples as part of tool metadata.
- Plugin registration via `.use(plugin)`.
- MCP/agent adapter layer that keeps core tools independent of chat UI.

What not to borrow:

- Direct LLM-triggered `TRADE` as a strategy.
- Treating swap execution as equivalent to trade decision quality.
- Broad action surface without our own risk contract in front of it.

Fit for us:

- Good reference for a future internal `agent_tools/` layer.
- Not a replacement for `paper_trade_monitor.py`, `entry_engine.py`, or
  `exit_engine.py`.

## GOAT

Repo reviewed: `goat-sdk/goat`.

What it is:

- A larger agentic finance toolkit.
- Core is lightweight and plugin-driven.
- Tools are classes with Zod parameters and execute methods.
- Plugins expose tool providers and declare supported chains.
- Wallet clients are abstracted from plugin tools.
- Jupiter plugin can get quotes and perform Solana swaps.
- Pump.fun plugin creates and buys a new token through PumpPortal local trade
  transactions.
- RugCheck plugin exposes recent/trending/verified token reports and token
  report summary.

Useful patterns to borrow:

- `PluginBase.supportsChain(chain)` style capability gating.
- Tool decorator metadata instead of ad hoc action strings.
- Wallet-client abstraction separated from strategy logic.
- Quote parameters with explicit slippage, route, direct-route, and intermediate
  token controls.

What not to borrow:

- Treating a plugin catalog as a trading strategy.
- Letting agent tools bypass final entry/exit authority.
- Pump.fun creation tooling. Our current goal is trading/discovery, not token
  creation.

Fit for us:

- Stronger architecture reference than Solana Agent Kit if we build a formal
  internal tool registry.
- Useful for permissioning: tools can exist, but only the canonical decision
  contract decides whether they are callable.

## TreeCityWes PumpFun Bot

Repo reviewed: `TreeCityWes/Pump-Fun-Trading-Bot-Solana`.

What it is:

- A small pump.fun trading/sniping script.
- Finds newer mints.
- Scrapes pump.fun page with Selenium.
- Buys when bonding curve progress is below a configured threshold.
- Monitors market cap and bonding curve progress.
- Uses staged take profit, stop loss, timeout sell, and moon bag logic.

Strategy constants:

- Initial buy: `0.015 SOL`
- Entry curve max: `10%`
- Curve exit threshold: `15%`
- TP1: +25%, sell 50%
- TP2: another +25%, sell 75% of remaining
- Stop loss: -10%, sell all
- Timeout: 2 minutes, sell 75%, keep moon bag
- Monitor interval: 5 seconds

Useful strategy ideas:

- Pump.fun launches need curve-stage logic, not only DEX liquidity logic.
- Early entry should be tied to low bonding curve progress.
- Exit should consider bonding curve progress separately from price PnL.
- Taking principal off early and keeping a small moon bag is reasonable for
  convex meme launches.
- Timeout exits matter because many launches never get follow-through.

Important implementation problems:

- It disables TLS verification.
- It sends raw private key material to a third-party trade API.
- It relies on Selenium page scraping instead of structured APIs.
- It assumes token decimals in places.
- It has weak dedupe and no lifecycle attribution.
- The second TP logic is not clean; the first +25% path sets `continueTrade` and
  can break monitoring early.
- It has no quote sanity, holder risk, creator behavior, or toxic wallet checks.

Fit for us:

- Borrow the curve-stage playbook.
- Do not borrow the execution implementation.

## OpenPrawn

Repo expected by website: `OpenPrawn/OpenPrawn`.

Actual public repo found through GitHub API search: `hamartia0/OpenPrawn`.

Dry-run status:

- Clone succeeded from `https://github.com/hamartia0/OpenPrawn.git`.
- `npm install` succeeded.
- `npm run build` failed:
  - `src/channels/telegram.ts`: `disable_web_page_preview` is not accepted by
    current grammY TypeScript types.
- Setup/dev were not run with real keys because they require Anthropic and chat
  bot credentials.

What it is:

- Telegram/Discord personal Solana assistant.
- Anthropic-powered action parser.
- Per-user JSON memory under `~/.openprawn`.
- Pump.fun v3 trending/latest/live/search.
- PumpPortal wallet/trade flow.
- Optional Helius balance and X sentiment.
- Proactive scheduler that sends updates to active chats.
- JSON skill system with prompt/API/code skill types, though current execution
  mainly uses prompt skills and simple action strings.

Useful patterns to borrow:

- Unified channel adapter: Telegram and Discord produce the same inbound message
  shape.
- Active chat registry for proactive alerts.
- Two-step trade confirmation in chat.
- Per-user preferences: max trade size, slippage, daily limit.
- Skill JSON format for prompt extensions.
- Clear, compact token-card formatting for chat alerts.

Problems and gaps:

- Website/repo URL mismatch makes install unreliable.
- Build does not pass as cloned.
- README says encrypted memory, but wallet data is stored as plain JSON fields in
  the current code path.
- Trade execution depends on PumpPortal wallet/API flow.
- It is a chat UX and discovery assistant, not a robust autonomous trading
  strategy.

Fit for us:

- Good reference if we want a Telegram/Discord operator console for our system.
- Not suitable as execution or risk authority.

## Pump.fun Strategy We Should Add

This should be a new route/profile, not mixed into generic LOTTO.

Possible name:

- `PUMPFUN_CURVE`
- entry modes such as `pumpfun_curve_probe`, `pumpfun_curve_momentum`,
  `pumpfun_curve_moonbag`

Required data:

- bonding curve progress
- virtual SOL reserves
- virtual token reserves
- market cap
- token age
- creator wallet
- creator previous launches
- holder count and holder growth
- top holder concentration
- buy/sell counts and buy/sell pressure
- reply/social activity
- migration/graduation state
- whether Jupiter route exists yet
- executable pump.fun/PumpPortal local quote or simulated curve fill

Entry idea:

- Only consider very early curve state, for example 2-12% progress.
- Require token age freshness, for example under 3-10 minutes for true sniping.
- Require buy pressure and holder growth.
- Reject obvious creator-repeat rugs, extreme concentration, and toxic wallet
  patterns.
- If the token has not migrated and Jupiter has no route, use pump.fun curve
  pricing/transaction simulation as a separate executable source.
- Never use DexScreener or GMGN mark price as fill price.
- Position size should start as a probe, for example 0.003-0.01 SOL, until we
  have forward data.

Exit idea:

- TP1: +25%, sell 40-50%.
- TP2: another +25% from the post-TP anchor, sell 50-75% of remaining.
- Stop: -10% to -18%, depending on curve liquidity and quote quality.
- No-follow timeout: if no meaningful follow-through after 60-120 seconds, sell
  75% and keep a small moon bag.
- Curve progress exit: if progress reaches a risk threshold before clean
  momentum, sell 50-75% and keep 10-25% moon bag.
- Migration transition: define explicitly whether to hold through graduation or
  derisk before/at migration.

Analytics:

- Split all results by `entry_mode`.
- Track MFE, MAE, time-to-TP, time-to-stop, curve progress at entry, curve
  progress at exit, creator history, and migration status.
- Do not blend pump.fun probe stats with normal LOTTO or Matrix results.

## What I Would Prioritize As A Trader

1. Unit audit

   Every PnL, stop, trail, entry, and exit comparison must prove it uses the same
   unit. DexScreener/GMGN/GeckoTerminal can be context. Jupiter/shared quote or
   pump.fun curve executable quote should be accounting truth.

2. Canonical entry blocker

   Keep all evidence, but produce one final blocker per lifecycle:

   `data_health -> hard_risk -> lifecycle/readiness -> route_gate -> SmartEntry -> execution_quote`

3. Exit authority cleanup

   Guardian should trigger and gather evidence. ExitMatrix should own the final
   canonical exit reason and accounting.

4. Pump.fun curve route

   Add curve mechanics before loosening generic LOTTO gates. Pump.fun is a
   different market structure from post-migration DEX trading.

5. Chat/operator UX

   Borrow OpenPrawn's Telegram/Discord operator style only after the trading core
   exposes clean decisions. The chat bot should explain and confirm actions, not
   invent trade authority.

## Trader Verdict

Our system is cautious, sometimes too cautious, but directionally better than a
simple meme sniping bot. The key weakness is complexity: too many gates and two
exit authorities can make the system miss obvious momentum or exit for a reason
that is hard to attribute.

The highest-return next move is not to import another agent. It is to tighten our
own decision contract, add pump.fun-specific curve data, and measure clean
cohorts by route and `entry_mode`.

From a battle-tested operator's view:

- Trade smaller until every route has clean forward stats.
- Let winners prove which blocker was too strict before relaxing gates.
- Keep GMGN as enrichment until it proves independent discovery.
- Do not let LLMs decide buys or sells.
- Use agent frameworks for UX, tools, and permissions, not for edge generation.

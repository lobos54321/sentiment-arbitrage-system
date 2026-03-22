# Meme Coin Trading Strategy - Factor Analysis Data

## Context
We are backtesting a meme coin trading strategy on Solana. Signals come from a Telegram premium channel.
Signal type: NOT_ATH (New Trending, not All-Time-High). Period: Mar 13-20, 2026. 795 unique tokens.

## Current Best Exit Strategy
Simple 2-stage: SL=-3%, trail starts at +3%, trail factor 0.90, timeout 120m
Result: EV=+17.0%, fEV=+13.5% (after 3.5% friction), WR=75%, Sharpe=0.722

## ⚠️ CRITICAL UPDATE 2026-03-22: `_checkKline` Scoring Validation Failed

**Cross-validation on 129 NOT_ATH signals (Mar 7-19) revealed the current filter is BROKEN.**

### Current Scoring (DEPLOYED - MUST CHANGE)
```
trend_ok (+1): greenCount >= 2 in prev3
holds_support (+1): close > minLow
vol_increasing (+1): current.volume > prev.volume
Threshold: >= 2
```

### Validation Results
| Filter | N | WR | EV | SL% |
|--------|---|----|----|------|
| PASSED (score≥2) | 86 | 17.4% | +14.3% | **82.6%** |
| REJECTED (score<2) | 43 | 53.5% | +54.7% | 46.5% |

**The filter destroys 40% EV. 82.6% of passed trades hit SL immediately.**

Monte Carlo permutation (100 iter): p=0.95 for EV — NOT statistically significant.

### Root Cause
- `trend_ok = greenCount >= 2` → requires prior momentum → picks EXTENDED coins that reverse
- `holds_support = close > minLow` → picks coins that HAVEN'T pulled back → chase entries
- `vol_increasing` → OPPOSITE of what works for early-stage coins

### NEW Scoring (Implemented in engine)
```
RED bar (+2): close < open  ← 回调确认，必须
lowVolume (+1): current.volume <= avg(prev3 volume)  ← 吸筹，非派发
isActive (+1): |mom_from_lag1| > 20%  ← 活跃币
Threshold: >= 3 to pass
```

### Why NEW Scoring is Different
- RED bar is required (pullback confirmation) — 72 samples, WR=33%, EV=+40%
- Low volume (accumulation, not FOMO distribution)
- Active coins (momentum exists) — counterintuitive but big winners are active
- REMOVED: greenCount, holdsSupport (反向指标)

### Validated Signal Characteristics
| Dimension | Setting | N | WR | EV | SL% |
|-----------|---------|---|----|----|------|
| RED bar | ON | 72 | 33% | +40% | 67% |
| RED bar | OFF | 57 | 25% | +12% | 75% |
| LOW volume | ON | 77 | 34% | +35% | 66% |
| LOW volume | OFF | 52 | 23% | +16% | 77% |
| Active (|mom|>20%) | ON | 18 | 50% | +88% | 50% |
| Active (|mom|>20%) | OFF | 111 | 26% | +18% | 74% |

### Top Performers (from rejected = good signals)
1. JACK (+773%) — RED, mom=+859%, vol=low
2. 柯基 (+284%) — GREEN, mom=+316%, vol=high
3. Yahu (+174%) — RED, mom=+193%, vol=low

### Limitations
1. Only 129 valid samples (90% dropped: no K-line overlap or short history)
2. ALL 129 have mc=NULL (unknown market cap) — selection bias
3. p=0.95 Monte Carlo — results NOT statistically significant
4. Walk-forward: IS (n=17) EV=+60% vs OOS (n=112) EV=+23% — large drop-off
5. High day-to-day variance (03/14 EV=+404%, 03/15 EV=+36%, 03/16 EV=+9%)

## Key Finding: Entry Bar Volume is the ONLY monotonically increasing factor
Quintile analysis (Q1=lowest volume → Q5=highest volume):
- Q1: EV=+10.7%, WR=55% (volume 0-1588)
- Q2: EV=+13.1%, WR=69% (volume 1590-4629)
- Q3: EV=+17.3%, WR=81% (volume 4644-8555)
- Q4: EV=+20.9%, WR=86% (volume 8560-15931)
- Q5: EV=+23.5%, WR=85% (volume 15934-172205)
Spread: Q5-Q1 = +12.8pp, perfectly monotonic ↑

## Other Factors Tested (all showed NO monotonic pattern):
- Super Index (composite): Q5-Q1 = -2.6% ~
- AI Index: Q5-Q1 = +1.3% ~
- Trade Index: Q5-Q1 = -3.1% ~
- Security Index: Q5-Q1 = -6.8% ~
- Market Cap: Q5-Q1 = +1.5% ~ (weak)
- Holders: Q5-Q1 = +0.8% ~
- Vol24H: Q5-Q1 = -4.8% ~
- Tx24H: Q5-Q1 = -5.0% ~
- Vol/MC Ratio: Q5-Q1 = -6.8% ~
- Token Age: Q5-Q1 = -1.7% ~
- Sentiment Index: Q5-Q1 = -3.3% ~
- Media Index: Q5-Q1 = +0.2% ~

## Composite Filters
- Entry Vol top 40% only: n=287, EV=+22.7%, WR=86% (vs excluded n=485, EV=+13.8%)
- MC 30K-100K: n=296, EV=+19.7%, WR=83% (but not monotonic)
- Vol top40% + MC30-100K: n=154, EV=+22.4%, WR=86% (but profit 1.75 SOL vs excluded 4.55 SOL)

## Exit Timing
- 93% of stop losses hit in first 2 minutes
- 99% of trail exits happen in first 5 minutes
- Fixed-time exits ALL lose money
- The entire alpha is captured in the first 1-5 bars

## Critical Questions for Debate
1. Is this strategy genuinely predictive, or is it overfitted to 7 days of data?
2. Is entry bar volume a true causal factor, or is it a look-ahead bias (we're using the bar where the signal fires)?
3. What deeper factors could we derive from 1-minute kline data (volume, OHLC)?
4. Can we build a more robust signal quality score?
5. How should we think about the massive skew (few big winners drive results)?
6. Is the 3.5% friction estimate realistic? What if it's 5% or 7%?

## Data Available
- 1-minute kline: timestamp, OHLC, volume
- Signal metadata: market_cap, holders, vol24h, top10%, age, tx24h
- Description indices: Super Index, AI/Trade/Security/Address/Sentiment/Media indices
- Security flags: NoMint, NoBlacklist, Burnt
- Social: has_twitter, has_website

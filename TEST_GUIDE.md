# 🧪 System Testing Guide

Complete testing procedure for the Sentiment Arbitrage System.

---

## ✅ Pre-Deployment Checklist

### 1. Configuration Verification

Run each verification script to confirm setup:

```bash
# Test 1: Verify Telegram Bot configuration
node scripts/test-telegram.js
```

**Expected output:**
```
✅ Bot Token 有效
   Bot 名称: [Your bot name]
   Bot 用户名: @[username]

✅ 测试消息发送成功！
   请检查你的 Telegram，应该收到一条测试消息。

✅ 交互式消息发送成功！

🎉 所有测试通过！
```

### 2. Database Initialization

```bash
# Initialize database schema
npm run db:init
```

**Expected output:**
```
✅ Database initialized: ./data/sentiment_arb.db
✅ All tables created successfully
```

### 3. Dependencies Installation

```bash
# Install all required packages
npm install
```

**Verify key packages installed:**
- `node-telegram-bot-api` - Telegram Bot API
- `telegram` - Telegram User API (for GMGN control)
- `@solana/web3.js` - Solana blockchain
- `ethers` - BSC blockchain
- `better-sqlite3` - Database
- `axios` - HTTP requests

---

## 🎭 Shadow Mode Testing (Recommended First)

Shadow mode simulates all operations without real trading.

### Configuration

Ensure `.env` has:
```bash
SHADOW_MODE=true
AUTO_BUY_ENABLED=false
```

### Run Shadow Mode

```bash
npm run shadow
```

**What to expect:**
1. System starts and connects to Telegram
2. Position monitor starts
3. Waits for signals from configured channels
4. Processes signals through full pipeline
5. Shows "SHADOW MODE - Would execute buy" instead of real trades

### Monitoring Shadow Mode

Watch for:
- ✅ Telegram connection successful
- ✅ Signal received and parsed
- ✅ Chain snapshot retrieved
- ✅ Hard gates evaluated
- ✅ Soft alpha score computed
- ✅ Decision made (BUY/GREYLIST/REJECT)
- ✅ Position size calculated
- 🎭 SHADOW MODE execution (not real)

**Let it run for 1-2 hours** to observe signal processing and decision-making.

---

## 🧪 Component Testing

### Test Individual Components

#### 1. Chain Snapshot Services

**Solana:**
```javascript
import { SolanaSnapshotService } from './src/inputs/chain-snapshot-sol.js';

const config = { /* your config */ };
const service = new SolanaSnapshotService(config);

// Test with known token
const snapshot = await service.getSnapshot('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'); // USDC
console.log(snapshot);
```

**BSC:**
```javascript
import { BSCSnapshotService } from './src/inputs/chain-snapshot-bsc.js';

const config = { /* your config */ };
const service = new BSCSnapshotService(config);

// Test with known token
const snapshot = await service.getSnapshot('0x55d398326f99059fF775485246999027B3197955'); // USDT
console.log(snapshot);
```

#### 2. Hard Gates

```javascript
import { HardGateService } from './src/gates/hard-gates.js';

const gates = new HardGateService(config);
const result = await gates.evaluate(snapshot, 'SOL');

console.log('Passed:', result.passed);
console.log('Failed gates:', result.failed_gates);
```

#### 3. Soft Alpha Scorer

```javascript
import { SoftAlphaScorer } from './src/scoring/soft-alpha-score.js';

const scorer = new SoftAlphaScorer(config, db);
const score = await scorer.computeScore(snapshot, tokenCA, 'SOL');

console.log('Final score:', score.final_score);
console.log('Breakdown:', score.breakdown);
```

#### 4. Decision Engine

```javascript
import { DecisionEngine } from './src/decision/decision-matrix.js';

const engine = new DecisionEngine(config, db);
const decision = engine.decide(scoreResult, gateResult);

console.log('Action:', decision.action); // BUY/GREYLIST/REJECT
console.log('Confidence:', decision.confidence);
console.log('Reason:', decision.reason);
```

---

## 💰 Live Mode Testing (After Shadow Mode Validation)

### ⚠️ SAFETY CHECKLIST

Before enabling live mode:

- [ ] Shadow mode tested for 2+ hours without errors
- [ ] All API keys verified and working
- [ ] GMGN bots configured with Auto Buy enabled
- [ ] GMGN bot wallets funded (small test amounts)
- [ ] Position limits set conservatively
- [ ] Capital allocation appropriate for testing

### Enable Live Mode

Update `.env`:
```bash
SHADOW_MODE=false
AUTO_BUY_ENABLED=true

# Conservative test limits
MAX_CONCURRENT_POSITIONS=3
MAX_DAILY_TRADES=10
TOTAL_CAPITAL_SOL=0.5  # Start with 0.5 SOL
TOTAL_CAPITAL_BNB=0.05  # Start with 0.05 BNB
```

### Start Live System

```bash
npm start
```

**First Live Trade Checklist:**
1. ✅ Signal received
2. ✅ Hard gates passed
3. ✅ Score computed (should be ≥60 for BUY)
4. ✅ Decision = BUY
5. ✅ Position sized appropriately
6. ✅ Pre-flight check passed (no price surge)
7. ✅ Trade sent to GMGN bot
8. ✅ Confirmation received
9. ✅ Position recorded in database
10. ✅ Position monitor tracking

### Monitor Live Trades

Watch for:
- **Entry notifications** in Telegram (admin chat)
- **GMGN bot confirmations** in GMGN bot chats
- **Position monitoring** every 2 minutes
- **Exit triggers** (risk, sentiment decay, SOP)
- **Exit notifications** with P&L

---

## 🔍 Debugging & Troubleshooting

### Common Issues

#### 1. "Missing TELEGRAM_API_ID" Error

**Problem:** Telegram User API credentials not configured.

**Solution:**
1. Visit https://my.telegram.org/apps
2. Create application
3. Add `TELEGRAM_API_ID` and `TELEGRAM_API_HASH` to `.env`

#### 2. "401 Unauthorized" from GMGN Bot

**Problem:** Session expired or not authenticated.

**Solution:**
1. Delete `TELEGRAM_SESSION` from `.env`
2. Restart system
3. Complete phone number authentication
4. Save new session string to `.env`

#### 3. No Signals Received

**Problem:** Telegram listener not subscribed to channels.

**Solution:**
1. Manually join target channels in Telegram
2. Ensure channels are public or you have access
3. Check `telegram_channels` table in database

#### 4. "Insufficient Liquidity" - All Tokens Rejected

**Problem:** Hard gate thresholds too strict for test tokens.

**Solution:**
- Temporarily lower `MIN_LIQUIDITY_USD` in hard-gates.js for testing
- Use well-established tokens for initial tests
- Check DexScreener to verify liquidity is reported

#### 5. GMGN Bot Not Responding

**Problem:** Bot configuration or Auto Buy settings.

**Solution:**
1. Open GMGN bot in Telegram
2. Send `/settings` command
3. Verify Auto Buy is enabled
4. Check wallet has sufficient balance
5. Verify slippage tolerance is set

---

## 📊 Performance Monitoring

### Database Queries

**Check signal processing:**
```sql
SELECT
  COUNT(*) as total_signals,
  SUM(CASE WHEN processed = 1 THEN 1 ELSE 0 END) as processed,
  COUNT(DISTINCT token_ca) as unique_tokens
FROM telegram_signals;
```

**Check positions:**
```sql
SELECT
  status,
  COUNT(*) as count,
  AVG(pnl_percent) as avg_pnl,
  SUM(position_size_usd) as total_size_usd
FROM positions
GROUP BY status;
```

**Check win rate:**
```sql
SELECT
  COUNT(*) as total_closed,
  SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as wins,
  SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate,
  AVG(pnl_percent) as avg_pnl_percent
FROM positions
WHERE status = 'closed';
```

### Log Analysis

**Key metrics to track:**
- Signal processing time (should be < 10 seconds)
- Hard gate pass rate (target: 20-30%)
- Average soft alpha score
- BUY decision rate (target: 5-10% of signals)
- Execution success rate (target: >95%)
- Position monitor frequency (every 2 minutes)

---

## 🎯 Success Criteria

### Shadow Mode Testing (Phase 1)

- [ ] System runs for 2+ hours without crashes
- [ ] Processes ≥10 signals
- [ ] Hard gates work correctly (reasonable pass/fail rate)
- [ ] Soft scores computed (range 0-100)
- [ ] Decisions made (BUY/GREYLIST/REJECT distribution logical)
- [ ] Position sizing calculated correctly
- [ ] No database errors

### Live Mode Testing (Phase 2)

- [ ] First trade executes successfully
- [ ] GMGN bot confirms trade
- [ ] Position recorded in database
- [ ] Position monitor tracks position
- [ ] Exit triggers work (test with manual exit if needed)
- [ ] P&L calculated correctly
- [ ] Notifications sent to Telegram

### Production Readiness (Phase 3)

- [ ] Win rate ≥ 50% over 20+ trades
- [ ] Average P&L positive
- [ ] Max drawdown < 20%
- [ ] No critical errors in 24 hours
- [ ] Position limits respected
- [ ] All safety mechanisms working

---

## 📈 Optimization Cycle

After successful testing:

1. **Analyze Results** - Review trades, win rate, P&L
2. **Identify Patterns** - Which scores correlate with wins?
3. **Adjust Thresholds** - Tune hard gates and score weights
4. **Backtest Changes** - Use historical data if available
5. **Deploy Incrementally** - Small capital increases
6. **Monitor Continuously** - Track performance metrics

---

## 🆘 Emergency Procedures

### Stop System Immediately

```bash
# Press Ctrl+C in terminal running the system
# Or send SIGTERM:
pkill -f "node src/index.js"
```

### Close All Positions Manually

1. Open GMGN bots in Telegram
2. View open positions: `/positions`
3. Sell each position: Send token CA → Click "Sell" → 100%

### Reset System

```bash
# Backup database
cp data/sentiment_arb.db data/sentiment_arb_backup_$(date +%Y%m%d_%H%M%S).db

# Reset processed flags (reprocess signals)
sqlite3 data/sentiment_arb.db "UPDATE telegram_signals SET processed = 0;"

# Clear positions (USE WITH CAUTION)
sqlite3 data/sentiment_arb.db "DELETE FROM positions;"
```

---

## 📞 Support Channels

**Telegram Issues:**
- Telegram Bot API Docs: https://core.telegram.org/bots/api
- Telegram MTProto Docs: https://core.telegram.org/api

**GMGN Issues:**
- GMGN Discord: https://discord.gg/gmgn
- Send `/help` in GMGN bots

**Blockchain Issues:**
- Helius Docs: https://docs.helius.dev
- Solana Web3.js: https://solana-labs.github.io/solana-web3.js/
- Ethers.js: https://docs.ethers.org/v6/

**System Logs:**
- Check console output for detailed error messages
- Database: `sqlite3 data/sentiment_arb.db`
- Enable verbose logging: Set `LOG_LEVEL=debug` in `.env`

---

## ✅ Final Pre-Launch Checklist

Before deploying with real capital:

- [ ] All tests passed in shadow mode
- [ ] All tests passed in live mode (small capital)
- [ ] Win rate ≥ 50% over 20+ test trades
- [ ] Position monitoring working reliably
- [ ] Exit triggers tested and working
- [ ] Emergency stop procedures tested
- [ ] Backup procedures in place
- [ ] Monitoring dashboards set up
- [ ] Team trained on system operation
- [ ] Capital allocation finalized

**Only proceed when ALL items checked!**

---

**Good luck! 🚀**

import JupiterUltraExecutor from './jupiter-ultra-executor.js';

function stripInternalFields(result = {}) {
  const { _rawOrder, ...rest } = result || {};
  return rest;
}

export class ParityExecutor {
  constructor({ mode = 'paper', executor = null } = {}) {
    this.mode = mode;
    this.executor = executor || new JupiterUltraExecutor();
    this.paperPenaltyConfig = {
      enabled: process.env.PAPER_EXECUTION_PENALTY_ENABLED !== 'false',
      buySlippageBps: Number(process.env.PAPER_BUY_SLIPPAGE_BPS || 35),
      sellSlippageBps: Number(process.env.PAPER_SELL_SLIPPAGE_BPS || 45),
      buyDelayBps: Number(process.env.PAPER_BUY_DELAY_BPS || 15),
      sellDelayBps: Number(process.env.PAPER_SELL_DELAY_BPS || 20),
      feeBps: Number(process.env.PAPER_FEE_BPS || 20),
    };
  }

  initialize() {
    if (this.executor?.initialize && !this.executor.walletAddress) {
      this.executor.initialize();
    }
    return this;
  }

  async quoteBuy(tokenCA, amountSol, opts = {}) {
    const quote = await this.executor.getBuyQuote(tokenCA, amountSol, opts);
    return stripInternalFields({ ...quote, mode: this.mode === 'live' ? 'live_quote' : 'paper' });
  }

  async quoteSell(tokenCA, tokenAmountRaw, opts = {}) {
    const quote = await this.executor.getSellQuote(tokenCA, tokenAmountRaw, opts);
    return stripInternalFields({ ...quote, mode: this.mode === 'live' ? 'live_quote' : 'paper' });
  }

  async simulateBuy(tokenCA, amountSol, opts = {}) {
    const quote = await this.executor.getBuyQuote(tokenCA, amountSol, opts);
    const adjusted = this._applyPaperPenalty('buy', quote, opts);
    return stripInternalFields({ ...adjusted, mode: 'paper' });
  }

  async simulateSell(tokenCA, tokenAmountRaw, opts = {}) {
    const quote = await this.executor.getSellQuote(tokenCA, tokenAmountRaw, opts);
    const adjusted = this._applyPaperPenalty('sell', quote, opts);
    return stripInternalFields({ ...adjusted, mode: 'paper' });
  }

  async buy(tokenCA, amountSol, opts = {}) {
    if (this.mode !== 'live') {
      const simulated = await this.simulateBuy(tokenCA, amountSol, opts);
      return {
        ...simulated,
        success: simulated.success,
        txHash: null,
        amountIn: amountSol,
        amountOut: simulated.quotedOutAmountRaw ? Number(simulated.quotedOutAmountRaw) : 0,
        reason: simulated.failureReason || null
      };
    }

    const quote = await this.executor.getBuyQuote(tokenCA, amountSol, opts);
    if (!quote.success) {
      const failure = stripInternalFields({ ...quote, mode: 'live' });
      return { ...failure, reason: failure.failureReason || null };
    }

    const executed = await this.executor.executeQuotedBuy(quote, opts);
    const normalized = stripInternalFields({ ...executed, mode: 'live' });
    return {
      ...normalized,
      reason: normalized.failureReason || null,
      amountIn: amountSol,
      amountOut: normalized.actualAmountOutRaw ? Number(normalized.actualAmountOutRaw) : Number(normalized.quotedOutAmountRaw || 0)
    };
  }

  async sell(tokenCA, tokenAmountRaw, opts = {}) {
    if (this.mode !== 'live') {
      const simulated = await this.simulateSell(tokenCA, tokenAmountRaw, opts);
      return {
        ...simulated,
        success: simulated.success,
        txHash: null,
        amountIn: tokenAmountRaw,
        amountOut: simulated.quotedOutAmount || 0,
        reason: simulated.failureReason || null
      };
    }

    const quote = await this.executor.getSellQuote(tokenCA, tokenAmountRaw, opts);
    if (!quote.success) {
      const failure = stripInternalFields({ ...quote, mode: 'live' });
      return { ...failure, reason: failure.failureReason || null };
    }

    const executed = await this.executor.executeQuotedSell(quote, opts);
    const normalized = stripInternalFields({ ...executed, mode: 'live' });
    return {
      ...normalized,
      reason: normalized.failureReason || null,
      amountIn: tokenAmountRaw,
      amountOut: normalized.actualAmountOut || normalized.quotedOutAmount || 0
    };
  }

  async emergencySell(tokenCA, tokenAmountRaw, opts = {}) {
    if (this.mode === 'live' && this.executor?.emergencySell) {
      const result = await this.executor.emergencySell(tokenCA, tokenAmountRaw, opts);
      return {
        ...result,
        side: 'sell',
        mode: 'live',
        reason: result?.reason || null
      };
    }
    return this.sell(tokenCA, tokenAmountRaw, { ...opts, urgent: true });
  }

  async getTokenBalance(tokenCA) {
    return this.executor.getTokenBalance(tokenCA);
  }

  async getSolBalance() {
    return this.executor.getSolBalance();
  }

  _applyPaperPenalty(side, quote, opts = {}) {
    if (!quote?.success) {
      return {
        ...quote,
        penaltyApplied: false,
        penaltyReason: quote?.failureReason || null,
      };
    }

    const enabled = opts.applyPaperPenalty !== false && this.paperPenaltyConfig.enabled;
    if (!enabled) {
      return {
        ...quote,
        penaltyApplied: false,
        rawQuotedOutAmount: quote.quotedOutAmount,
        rawQuotedOutAmountRaw: quote.quotedOutAmountRaw,
        rawEffectivePrice: quote.effectivePrice,
        rawFeeEstimate: quote.feeEstimate,
      };
    }

    const base = { ...quote };
    const slipBps = side === 'buy' ? this.paperPenaltyConfig.buySlippageBps : this.paperPenaltyConfig.sellSlippageBps;
    const delayBps = side === 'buy' ? this.paperPenaltyConfig.buyDelayBps : this.paperPenaltyConfig.sellDelayBps;
    const feeBps = this.paperPenaltyConfig.feeBps;
    const totalBps = Math.max(0, slipBps + delayBps + feeBps);
    const keepBps = Math.max(0, 10_000 - totalBps);
    const multiplier = keepBps / 10_000;

    const rawQuotedOutAmount = this._toNullableNumber(base.quotedOutAmount);
    const rawQuotedOutAmountRaw = this._toNullableBigIntString(base.quotedOutAmountRaw);
    const rawEffectivePrice = this._toNullableNumber(base.effectivePrice);
    const rawFeeEstimate = this._toNullableNumber(base.feeEstimate) || 0;

    const penalizedQuotedOutAmount = rawQuotedOutAmount != null ? rawQuotedOutAmount * multiplier : null;
    const penalizedQuotedOutAmountRaw = rawQuotedOutAmountRaw != null
      ? this._scaleIntegerStringByBps(rawQuotedOutAmountRaw, keepBps)
      : null;

    let penalizedEffectivePrice = rawEffectivePrice;
    if (rawEffectivePrice != null) {
      penalizedEffectivePrice = side === 'buy'
        ? rawEffectivePrice / multiplier
        : rawEffectivePrice * multiplier;
    }

    const inputAmount = this._toNullableNumber(base.inputAmount);
    const notionalSol = side === 'buy'
      ? inputAmount
      : penalizedQuotedOutAmount;
    const extraFeeEstimate = notionalSol != null ? notionalSol * (feeBps / 10_000) : 0;

    return {
      ...base,
      quotedOutAmount: penalizedQuotedOutAmount,
      quotedOutAmountRaw: penalizedQuotedOutAmountRaw,
      effectivePrice: penalizedEffectivePrice,
      feeEstimate: rawFeeEstimate + extraFeeEstimate,
      penaltyApplied: true,
      penaltyBps: totalBps,
      penaltyBreakdown: {
        slippageBps: slipBps,
        delayBps: delayBps,
        feeBps,
      },
      rawQuotedOutAmount,
      rawQuotedOutAmountRaw,
      rawEffectivePrice,
      rawFeeEstimate,
    };
  }

  _toNullableNumber(value) {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : null;
  }

  _toNullableBigIntString(value) {
    if (value == null) return null;
    try {
      return BigInt(String(value)).toString();
    } catch {
      return null;
    }
  }

  _scaleIntegerStringByBps(value, keepBps, denominator = 10_000) {
    try {
      const original = BigInt(String(value));
      const scaled = (original * BigInt(keepBps)) / BigInt(denominator);
      return scaled.toString();
    } catch {
      return null;
    }
  }
}

export default ParityExecutor;

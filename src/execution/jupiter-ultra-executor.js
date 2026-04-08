/**
 * Jupiter Ultra Executor (V3)
 *
 * 替代 JupiterSwapExecutor + JitoBundleSender
 * 使用 Jupiter Ultra API：两步流程
 *   1. GET /ultra/v1/order → 获取未签名交易 + requestId
 *   2. 本地签名
 *   3. POST /ultra/v1/execute → Jupiter 广播并确认
 *
 * 优势：
 * - 无需 Jito Bundle（Jupiter 内置 Beam 发送引擎）
 * - 内置 MEV 保护（ShadowLane）
 * - 自动滑点（RTSE）
 * - 内置交易确认（无需手动 confirmTransaction）
 * - Gasless（Jupiter 代付）
 */

import {
  Connection,
  Keypair,
  VersionedTransaction,
  PublicKey,
  LAMPORTS_PER_SOL
} from '@solana/web3.js';
import bs58 from 'bs58';
import axios from 'axios';

const SOL_MINT = 'So11111111111111111111111111111111111111112';
const DEFAULT_PAPER_QUOTE_TAKER = '11111111111111111111111111111111';
const JUPITER_LITE_QUOTE_URL = 'https://lite-api.jup.ag/swap/v1/quote';

export class JupiterUltraExecutor {
  constructor() {
    this.rpcUrl = process.env.SOLANA_RPC_URL || 'https://api.mainnet-beta.solana.com';
    this.connection = new Connection(this.rpcUrl, 'confirmed');

    this.wallet = null;
    this.walletAddress = null;

    // 安全限制
    this.maxPositionSol = 0.12;
    this.maxDailyLossSol = 0.5;
    this.dailyLoss = 0;
    this.dailyLossResetTime = 0;
    this.tradingPaused = false;

    // 手续费保护
    this.dailyFeeSpent = 0;
    this.maxDailyFee = 0.2;
    this.minSolReserve = 0.01;
    this.feeResetTime = 0;
    this.feePaused = false;

    // Jupiter Ultra API
    this.ultraApiBase = 'https://api.jup.ag/ultra/v1';
    this.jupiterApiKey = process.env.JUPITER_API_KEY || '';  // 免费 API Key: portal.jup.ag（有 key 比无 key 快 0.5-1s）
    this.decimalsCache = new Map();

    // 统计
    this.stats = {
      buys: 0,
      sells: 0,
      buy_failures: 0,
      sell_failures: 0,
      total_sol_spent: 0,
      total_sol_received: 0
    };

    console.log('🪐 [JupiterUltra] 初始化 (Ultra V3)');
  }

  initialize() {
    const privateKey = process.env.TRADE_WALLET_PRIVATE_KEY;
    if (!privateKey) {
      throw new Error('TRADE_WALLET_PRIVATE_KEY 未设置');
    }

    try {
      const decoded = bs58.decode(privateKey);
      this.wallet = Keypair.fromSecretKey(decoded);
      this.walletAddress = this.wallet.publicKey.toBase58();
      console.log(`✅ [JupiterUltra] 钱包: ${this.walletAddress.substring(0, 8)}...${this.walletAddress.slice(-4)}`);
    } catch (error) {
      throw new Error(`钱包私钥解析失败: ${error.message}`);
    }

    this._resetDailyLoss();
    console.log('⚡ [JupiterUltra] Ultra V3 就绪（内置 MEV 保护 + 自动滑点 + Beam 发送）');
  }

  async getBuyQuote(tokenCA, amountSol, opts = {}) {
    const amountLamports = Math.floor(Number(amountSol || 0) * LAMPORTS_PER_SOL);
    if (!Number.isFinite(amountLamports) || amountLamports <= 0) {
      return this._buildFailureResult('buy', 'quote_failed', {
        tokenCA,
        inputMint: SOL_MINT,
        outputMint: tokenCA,
        inputAmount: Number(amountSol || 0),
        inputAmountRaw: null,
        quoteTs: Date.now()
      });
    }

    try {
      const buyOpts = { ...opts, slippageBps: Math.min(Number(opts.slippageBps || 500), 500) };
      const order = await this._getOrder(SOL_MINT, tokenCA, amountLamports, buyOpts);
      return await this._normalizeQuoteResult('buy', order, {
        tokenCA,
        inputMint: SOL_MINT,
        outputMint: tokenCA,
        inputAmount: Number(amountSol || 0),
        inputAmountRaw: amountLamports,
        opts
      });
    } catch (error) {
      return this._buildFailureResult('buy', this._classifyFailureReason(error?.message || 'quote_failed'), {
        tokenCA,
        inputMint: SOL_MINT,
        outputMint: tokenCA,
        inputAmount: Number(amountSol || 0),
        inputAmountRaw: amountLamports,
        quoteTs: Date.now(),
        error
      });
    }
  }

  async getPublicBuyQuote(tokenCA, amountSol, opts = {}) {
    const amountLamports = Math.floor(Number(amountSol || 0) * LAMPORTS_PER_SOL);
    if (!Number.isFinite(amountLamports) || amountLamports <= 0) {
      return this._buildFailureResult('buy', 'quote_failed', {
        tokenCA,
        inputMint: SOL_MINT,
        outputMint: tokenCA,
        inputAmount: Number(amountSol || 0),
        inputAmountRaw: null,
        quoteTs: Date.now()
      });
    }

    try {
      const quote = await this._getLiteSwapQuote(SOL_MINT, tokenCA, amountLamports, opts);
      return await this._normalizeQuoteResult('buy', quote, {
        tokenCA,
        inputMint: SOL_MINT,
        outputMint: tokenCA,
        inputAmount: Number(amountSol || 0),
        inputAmountRaw: amountLamports,
        opts
      });
    } catch (error) {
      return this._buildFailureResult('buy', this._classifyFailureReason(error?.message || 'quote_failed'), {
        tokenCA,
        inputMint: SOL_MINT,
        outputMint: tokenCA,
        inputAmount: Number(amountSol || 0),
        inputAmountRaw: amountLamports,
        quoteTs: Date.now(),
        error
      });
    }
  }

  async getSellQuote(tokenCA, tokenAmount, opts = {}) {
    const rawAmount = Math.floor(Number(tokenAmount || 0));
    if (!Number.isFinite(rawAmount) || rawAmount <= 0) {
      return this._buildFailureResult('sell', 'quote_failed', {
        tokenCA,
        inputMint: tokenCA,
        outputMint: SOL_MINT,
        inputAmount: opts.inputAmount ?? null,
        inputAmountRaw: null,
        quoteTs: Date.now()
      });
    }

    try {
      const order = await this._getOrder(tokenCA, SOL_MINT, rawAmount, opts);
      return await this._normalizeQuoteResult('sell', order, {
        tokenCA,
        inputMint: tokenCA,
        outputMint: SOL_MINT,
        inputAmount: opts.inputAmount ?? null,
        inputAmountRaw: rawAmount,
        opts
      });
    } catch (error) {
      return this._buildFailureResult('sell', this._classifyFailureReason(error?.message || 'quote_failed'), {
        tokenCA,
        inputMint: tokenCA,
        outputMint: SOL_MINT,
        inputAmount: opts.inputAmount ?? null,
        inputAmountRaw: rawAmount,
        quoteTs: Date.now(),
        error
      });
    }
  }

  async getPublicSellQuote(tokenCA, tokenAmount, opts = {}) {
    const rawAmount = Math.floor(Number(tokenAmount || 0));
    if (!Number.isFinite(rawAmount) || rawAmount <= 0) {
      return this._buildFailureResult('sell', 'quote_failed', {
        tokenCA,
        inputMint: tokenCA,
        outputMint: SOL_MINT,
        inputAmount: opts.inputAmount ?? null,
        inputAmountRaw: null,
        quoteTs: Date.now()
      });
    }

    try {
      const quote = await this._getLiteSwapQuote(tokenCA, SOL_MINT, rawAmount, opts);
      return await this._normalizeQuoteResult('sell', quote, {
        tokenCA,
        inputMint: tokenCA,
        outputMint: SOL_MINT,
        inputAmount: opts.inputAmount ?? null,
        inputAmountRaw: rawAmount,
        opts
      });
    } catch (error) {
      return this._buildFailureResult('sell', this._classifyFailureReason(error?.message || 'quote_failed'), {
        tokenCA,
        inputMint: tokenCA,
        outputMint: SOL_MINT,
        inputAmount: opts.inputAmount ?? null,
        inputAmountRaw: rawAmount,
        quoteTs: Date.now(),
        error
      });
    }
  }


  async executeQuotedBuy(quote, opts = {}) {
    return this._executeQuotedTrade('buy', quote, opts);
  }

  async executeQuotedSell(quote, opts = {}) {
    return this._executeQuotedTrade('sell', quote, opts);
  }

  /**
   * 买入 Token（SOL → Token）
   * Ultra 自动滑点(RTSE)对高波动 meme coin 可能偏保守，需要重试
   */
  async buy(tokenCA, amountSol, opts = {}) {
    this._checkSafety(amountSol);

    const maxRetries = 3;  // Ultra 滑点失败时重试（每次重新获取 order = 新报价 + 新滑点）
    console.log(`🪐 [JupiterUltra] 买入 ${amountSol} SOL → ${tokenCA.substring(0, 8)}...`);

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const quote = await this.getBuyQuote(tokenCA, amountSol, opts);
        if (!quote.success) {
          throw new Error(quote.failureReason || '获取 Ultra Order 失败');
        }

        console.log(`   报价: ${amountSol} SOL → ${quote.quotedOutAmountRaw || '?'} tokens | slippage: ${quote.slippageBps || '?'}bps | requestId: ${quote.requestId?.substring(0, 8)}...`);
        const result = await this.executeQuotedBuy(quote, opts);

        if (result.success) {
          this.stats.buys++;
          this.stats.total_sol_spent += amountSol;
          console.log(`✅ [JupiterUltra] 买入成功: ${result.txHash}`);

          return {
            ...result,
            success: true,
            txHash: result.txHash,
            amountIn: amountSol,
            amountOut: Number(result.actualAmountOutRaw || result.quotedOutAmountRaw || 0),
            tokenCA
          };
        }

        throw new Error(result.failureReason || 'Ultra 执行失败');
      } catch (error) {
        const isSlippage = error.message.includes('Slippage') || error.message.includes('slippage');
        console.error(`❌ [JupiterUltra] 买入失败 (${attempt}/${maxRetries}): ${error.message}`);

        if (isSlippage && attempt < maxRetries) {
          console.log(`   🔄 滑点失败，1秒后重新获取报价重试...`);
          await new Promise(r => setTimeout(r, 1000));
          continue;
        }

        // 非滑点错误或最后一次重试 → 立即失败
        this.stats.buy_failures++;
        throw error;
      }
    }
  }

  /**
   * 卖出 Token（Token → SOL）
   * 滑点失败自动重试，每次重新获取 order
   */
  async sell(tokenCA, tokenAmount, options = {}) {
    // 重置每日亏损计数器（跨越 UTC 午夜后解除暂停，确保能卖出平仓）
    this._resetDailyLoss();
    if (this.tradingPaused) {
      throw new Error('交易已暂停（每日亏损限制）');
    }

    const maxRetries = options.urgent ? 3 : 2;
    console.log(`🪐 [JupiterUltra] 卖出 ${tokenAmount} tokens → SOL | ${tokenCA.substring(0, 8)}...`);

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const quote = await this.getSellQuote(tokenCA, tokenAmount, options);
        if (!quote.success) {
          throw new Error(quote.failureReason || '获取卖出 Ultra Order 失败');
        }

        const quotedSol = Number(quote.quotedOutAmount || 0);
        console.log(`   报价: ${tokenAmount} tokens → ${quotedSol.toFixed(6)} SOL | slippage: ${quote.slippageBps || '?'}bps`);
        const result = await this.executeQuotedSell(quote, options);

        if (result.success) {
          const outSol = Number(result.actualAmountOut || result.quotedOutAmount || 0);
          this.stats.sells++;
          this.stats.total_sol_received += outSol;
          console.log(`✅ [JupiterUltra] 卖出成功: ${result.txHash} | 实际到账: ${outSol.toFixed(6)} SOL (报价: ${quotedSol.toFixed(6)})`);

          return {
            ...result,
            success: true,
            txHash: result.txHash,
            amountIn: tokenAmount,
            amountOut: outSol,
            tokenCA
          };
        }

        throw new Error(result.failureReason || 'Ultra 卖出执行失败');
      } catch (error) {
        const isSlippage = error.message.includes('Slippage') || error.message.includes('slippage');
        console.error(`❌ [JupiterUltra] 卖出失败 (${attempt}/${maxRetries}): ${error.message}`);

        if (isSlippage && attempt < maxRetries) {
          console.log(`   🔄 滑点失败，1秒后重新获取报价重试...`);
          await new Promise(r => setTimeout(r, 1000));
          continue;
        }

        // 非滑点错误或最后一次重试 → 立即失败
        this.stats.sell_failures++;
        throw error;
      }
    }
  }

  /**
   * 紧急卖出
   */
  async emergencySell(tokenCA, tokenAmount) {
    console.log(`🚨 [JupiterUltra] 紧急卖出: ${tokenCA.substring(0, 8)}...`);

    const solBalance = await this.getSolBalance();
    if (solBalance < this.minSolReserve + 0.003) {
      console.log(`   ❌ SOL 余额不足 (${solBalance.toFixed(4)} SOL)，跳过紧急卖出`);
      return { success: false, reason: 'insufficient_sol' };
    }

    const balanceBefore = await this.getTokenBalance(tokenCA);
    if (balanceBefore.amount <= 0) {
      console.log(`   ✓ Token 已不在钱包中`);
      return { success: true, reason: 'already_sold' };
    }

    try {
      const result = await this.sell(tokenCA, tokenAmount);
      return { success: result.success, txHashes: [result.txHash] };
    } catch (error) {
      console.log(`   ⚠️ 紧急卖出失败: ${error.message}`);

      // Ultra 失败后等 3 秒检查余额（可能实际已成功）
      await new Promise(r => setTimeout(r, 3000));
      const balanceAfter = await this.getTokenBalance(tokenCA);
      if (balanceAfter.amount < balanceBefore.amount) {
        const soldAmount = balanceBefore.amount - balanceAfter.amount;
        console.log(`   ✅ 紧急卖出实际成功（余额已减少 ${soldAmount} tokens）`);
        return { success: true, soldAmount };
      }

      return { success: false, reason: error.message };
    }
  }

  // ==================== Ultra API ====================

  /**
   * 获取 Ultra Order
   * GET /ultra/v1/order
   */
  async _getOrder(inputMint, outputMint, amount, opts = {}) {
    try {
      const headers = {};
      if (this.jupiterApiKey) headers['x-api-key'] = this.jupiterApiKey;

      const params = new URLSearchParams({
        inputMint,
        outputMint,
        amount: amount.toString(),
        taker: this.walletAddress,
        prioritizationFeeLamports: 'auto'  // 动态优先费，确保快速上链
      });
      
      if (opts.slippageBps != null) {
        params.append('slippageBps', String(opts.slippageBps));
      }

      const res = await axios.get(`${this.ultraApiBase}/order?${params.toString()}`, {
        headers,
        timeout: 5000  // 5s: 报价接口快速失败，触发重试拿新价格
      });

      return res.data;
    } catch (error) {
      const errMsg = error.response?.data?.error || error.response?.data?.message || error.message;
      console.error(`⚠️  [JupiterUltra] Order 失败: ${errMsg}`);
      return null;
    }
  }

  async _getPublicOrder(inputMint, outputMint, amount, options = {}) {
    try {
      const headers = {};
      if (this.jupiterApiKey) headers['x-api-key'] = this.jupiterApiKey;

      const params = new URLSearchParams({
        inputMint,
        outputMint,
        amount: amount.toString(),
      });

      const taker = options?.taker;
      if (taker) {
        params.set('taker', taker);
      }

      const res = await axios.get(`${this.ultraApiBase}/order?${params.toString()}`, {
        headers,
        timeout: 5000
      });

      return res.data;
    } catch (error) {
      const errMsg = error.response?.data?.error || error.response?.data?.message || error.message;
      console.error(`⚠️  [JupiterUltra] Public Order 失败: ${errMsg}`);
      return {
        error: errMsg,
        statusCode: error.response?.status || null,
        errorCode: error.response?.data?.errorCode || null,
        message: errMsg,
      };
    }
  }

  async _getLiteSwapQuote(inputMint, outputMint, amount, options = {}) {
    try {
      const headers = { Accept: 'application/json' };
      const params = new URLSearchParams({
        inputMint,
        outputMint,
        amount: amount.toString(),
        slippageBps: String(options.slippageBps || 500),
      });

      const res = await axios.get(`${JUPITER_LITE_QUOTE_URL}?${params.toString()}`, {
        headers,
        timeout: 5000,
      });

      return res.data;
    } catch (error) {
      const errMsg = error.response?.data?.error || error.response?.data?.message || error.message;
      console.error(`⚠️  [JupiterUltra] Lite Quote 失败: ${errMsg}`);
      return {
        error: errMsg,
        statusCode: error.response?.status || null,
        errorCode: error.response?.data?.errorCode || null,
        message: errMsg,
      };
    }
  }

  /**
   * 签名交易
   */
  _signTransaction(base64Transaction) {
    const txBuf = Buffer.from(base64Transaction, 'base64');
    const tx = VersionedTransaction.deserialize(txBuf);
    tx.sign([this.wallet]);
    const serialized = Buffer.from(tx.serialize()).toString('base64');
    return serialized;
  }

  /**
   * 执行 Ultra Order
   * POST /ultra/v1/execute
   */
  async _executeOrder(signedTransaction, requestId) {
    try {
      const headers = { 'Content-Type': 'application/json' };
      if (this.jupiterApiKey) headers['x-api-key'] = this.jupiterApiKey;

      const res = await axios.post(`${this.ultraApiBase}/execute`, {
        signedTransaction,
        requestId
      }, {
        headers,
        timeout: 30000  // 30s: Solana 链上确认通常 10-15s，超时即重试
      });

      return res.data;
    } catch (error) {
      const errMsg = error.response?.data?.error || error.response?.data?.message || error.message;
      console.error(`⚠️  [JupiterUltra] Execute 失败: ${errMsg}`);
      return { status: 'Failed', error: errMsg };
    }
  }

  // ==================== 余额查询 ====================

  async getTokenBalance(tokenCA) {
    try {
      const tokenAccounts = await this.connection.getParsedTokenAccountsByOwner(
        this.wallet.publicKey,
        { mint: new PublicKey(tokenCA) }
      );

      if (tokenAccounts.value.length === 0) {
        return { amount: 0, decimals: 0, uiAmount: 0 };
      }

      const info = tokenAccounts.value[0].account.data.parsed.info;
      return {
        amount: parseInt(info.tokenAmount.amount),
        decimals: info.tokenAmount.decimals,
        uiAmount: parseFloat(info.tokenAmount.uiAmount)
      };
    } catch (error) {
      console.error(`⚠️  [JupiterUltra] 查询余额失败: ${error.message}`);
      return { amount: 0, decimals: 0, uiAmount: 0 };
    }
  }

  async getSolBalance() {
    try {
      const balance = await this.connection.getBalance(this.wallet.publicKey);
      return balance / LAMPORTS_PER_SOL;
    } catch (error) {
      console.error(`⚠️  [JupiterUltra] 查询 SOL 余额失败: ${error.message}`);
      return 0;
    }
  }

  // ==================== 安全 ====================

  async _resolveMintDecimals(mint) {
    if (!mint) return 0;
    if (mint === SOL_MINT) return 9;
    if (this.decimalsCache.has(mint)) return this.decimalsCache.get(mint);

    const fallbackDecimals = 6;

    try {
      const info = await this.connection.getParsedAccountInfo(new PublicKey(mint));
      const decimals = info?.value?.data?.parsed?.info?.decimals;
      const normalized = Number.isFinite(Number(decimals)) ? Number(decimals) : fallbackDecimals;
      const finalDecimals = normalized > 0 ? normalized : fallbackDecimals;
      if (finalDecimals !== normalized) {
        console.warn(`⚠️  [JupiterUltra] decimals=0 fallback→${fallbackDecimals} for ${mint.substring(0, 8)}...`);
      }
      this.decimalsCache.set(mint, finalDecimals);
      return finalDecimals;
    } catch (error) {
      console.warn(`⚠️  [JupiterUltra] 读取 decimals 失败 ${mint.substring(0, 8)}...: ${error.message} | fallback=${fallbackDecimals}`);
      this.decimalsCache.set(mint, fallbackDecimals);
      return fallbackDecimals;
    }
  }

  async _normalizeQuoteResult(side, order, meta = {}) {
    const quoteTs = Date.now();
    const isUltraOrder = Boolean(order?.transaction);
    const isLiteQuote = order?.outAmount != null && Array.isArray(order?.routePlan);
    const routeAvailable = isUltraOrder || isLiteQuote;
    if (!routeAvailable) {
      return this._buildFailureResult(side, this._classifyFailureReason(order?.error || order?.message || 'no_route'), {
        ...meta,
        quoteTs,
        requestId: order?.requestId || null
      });
    }

    const inputMint = meta.inputMint;
    const outputMint = meta.outputMint;
    const inputDecimals = inputMint === SOL_MINT ? 9 : await this._resolveMintDecimals(inputMint);
    const outputDecimals = outputMint === SOL_MINT ? 9 : await this._resolveMintDecimals(outputMint);
    const inputAmountRaw = meta.inputAmountRaw != null ? String(meta.inputAmountRaw) : null;
    const outputAmountRaw = order?.outAmount != null ? String(order.outAmount) : null;
    const inputAmount = meta.inputAmount != null
      ? Number(meta.inputAmount)
      : (inputAmountRaw != null ? Number(inputAmountRaw) / Math.pow(10, inputDecimals || 0) : null);
    const quotedOutAmount = outputAmountRaw != null
      ? Number(outputAmountRaw) / Math.pow(10, outputDecimals || 0)
      : null;
    const effectivePrice = (inputAmount && quotedOutAmount)
      ? (side === 'buy' ? inputAmount / quotedOutAmount : quotedOutAmount / inputAmount)
      : null;

    return {
      mode: 'quote',
      side,
      success: true,
      routeAvailable: true,
      requestId: order?.requestId || null,
      quotedOutAmount,
      quotedOutAmountRaw: outputAmountRaw,
      effectivePrice,
      slippageBps: order?.slippageBps != null ? Number(order.slippageBps) : (meta?.opts?.slippageBps != null ? Number(meta.opts.slippageBps) : null),
      quoteTs,
      feeEstimate: this._extractFeeEstimate(order),
      failureReason: null,
      txHash: null,
      actualAmountOut: null,
      actualAmountOutRaw: null,
      inputAmount,
      inputAmountRaw,
      inputMint,
      outputMint,
      inputDecimals,
      outputDecimals,
      tokenCA: meta.tokenCA || null,
      _rawOrder: order
    };
  }

  async _executeQuotedTrade(side, quote, opts = {}) {
    if (!quote?.success || !quote?._rawOrder?.transaction || !quote?.requestId) {
      return this._buildFailureResult(side, quote?.failureReason || 'no_route', {
        tokenCA: quote?.tokenCA || null,
        inputMint: quote?.inputMint,
        outputMint: quote?.outputMint,
        inputAmount: quote?.inputAmount,
        inputAmountRaw: quote?.inputAmountRaw,
        quoteTs: quote?.quoteTs || Date.now(),
        requestId: quote?.requestId || null
      });
    }

    try {
      const signedTx = this._signTransaction(quote._rawOrder.transaction);
      const result = await this._executeOrder(signedTx, quote.requestId);
      if (result.status !== 'Success') {
        return this._buildFailureResult(side, this._classifyFailureReason(result.error || result.status || 'execute_failed'), {
          tokenCA: quote.tokenCA,
          inputMint: quote.inputMint,
          outputMint: quote.outputMint,
          inputAmount: quote.inputAmount,
          inputAmountRaw: quote.inputAmountRaw,
          quoteTs: quote.quoteTs,
          requestId: quote.requestId,
          routeAvailable: true
        });
      }

      const actualAmountOutRaw = result.outputAmount != null
        ? String(result.outputAmount)
        : quote.quotedOutAmountRaw;
      const actualAmountOut = actualAmountOutRaw != null
        ? Number(actualAmountOutRaw) / Math.pow(10, quote.outputDecimals || 0)
        : quote.quotedOutAmount;

      return {
        ...quote,
        mode: 'live',
        success: true,
        txHash: result.signature,
        actualAmountOut,
        actualAmountOutRaw,
        failureReason: null,
        routeAvailable: true
      };
    } catch (error) {
      return this._buildFailureResult(side, this._classifyFailureReason(error?.message || 'execute_failed'), {
        tokenCA: quote.tokenCA,
        inputMint: quote.inputMint,
        outputMint: quote.outputMint,
        inputAmount: quote.inputAmount,
        inputAmountRaw: quote.inputAmountRaw,
        quoteTs: quote.quoteTs,
        requestId: quote.requestId,
        routeAvailable: true,
        error
      });
    }
  }

  _extractFeeEstimate(order = {}) {
    const lamports = Number(
      order.prioritizationFeeLamports
      || order.signatureFeeLamports
      || order.totalFeeLamports
      || 0
    );
    return Number.isFinite(lamports) && lamports > 0 ? lamports / LAMPORTS_PER_SOL : null;
  }

  _classifyFailureReason(message = '') {
    const text = String(message || '').toLowerCase();
    if (!text) return 'unknown';
    if (text.includes('429') || text.includes('rate limit')) return 'RATE_LIMITED';
    if (text.includes('insufficient')) return 'insufficient_balance';
    if (text.includes('slippage')) return 'slippage';
    if (text.includes('taker')) return 'missing_taker';
    if (text.includes('route') || text.includes('no route') || text.includes('could not find')) return 'no_route';
    if (text.includes('quote')) return 'quote_failed';
    if (text.includes('execute') || text.includes('failed')) return 'execute_failed';
    return 'unknown';
  }

  _buildFailureResult(side, failureReason, meta = {}) {
    return {
      mode: 'quote',
      side,
      success: false,
      routeAvailable: meta.routeAvailable === true,
      requestId: meta.requestId || null,
      quotedOutAmount: null,
      quotedOutAmountRaw: null,
      effectivePrice: null,
      slippageBps: null,
      quoteTs: meta.quoteTs || Date.now(),
      feeEstimate: null,
      failureReason,
      txHash: null,
      actualAmountOut: null,
      actualAmountOutRaw: null,
      inputAmount: meta.inputAmount ?? null,
      inputAmountRaw: meta.inputAmountRaw != null ? String(meta.inputAmountRaw) : null,
      inputMint: meta.inputMint || null,
      outputMint: meta.outputMint || null,
      inputDecimals: null,
      outputDecimals: null,
      tokenCA: meta.tokenCA || null,
      error: meta.error ? String(meta.error.message || meta.error) : null
    };
  }

  _checkSafety(amountSol) {
    this._resetDailyLoss();
    this._resetDailyFee();

    if (this.tradingPaused) {
      throw new Error(`交易已暂停：每日亏损已达 ${this.dailyLoss.toFixed(4)} SOL（限制 ${this.maxDailyLossSol} SOL）`);
    }
    if (this.feePaused) {
      throw new Error(`手续费保护：每日已达 ${this.dailyFeeSpent.toFixed(4)} SOL`);
    }
    if (amountSol > this.maxPositionSol) {
      throw new Error(`单笔超限: ${amountSol} SOL > 最大 ${this.maxPositionSol} SOL`);
    }
    if (amountSol <= 0) {
      throw new Error('买入金额必须大于 0');
    }
  }

  recordLoss(lossSol) {
    this._resetDailyLoss();
    this.dailyLoss += lossSol;
    if (this.dailyLoss >= this.maxDailyLossSol) {
      this.tradingPaused = true;
      console.error(`🚨 [JupiterUltra] 每日亏损达 ${this.dailyLoss.toFixed(4)} SOL，暂停所有交易！`);
    }
  }

  _resetDailyLoss() {
    const todayStart = new Date();
    todayStart.setUTCHours(0, 0, 0, 0);
    if (this.dailyLossResetTime < todayStart.getTime()) {
      this.dailyLoss = 0;
      this.dailyLossResetTime = todayStart.getTime();
      this.tradingPaused = false;
    }
  }

  _resetDailyFee() {
    const todayStart = new Date();
    todayStart.setUTCHours(0, 0, 0, 0);
    if (this.feeResetTime < todayStart.getTime()) {
      this.dailyFeeSpent = 0;
      this.feeResetTime = todayStart.getTime();
      this.feePaused = false;
    }
  }

  getStats() {
    return {
      ...this.stats,
      wallet: this.walletAddress ? `${this.walletAddress.substring(0, 8)}...` : 'N/A',
      daily_loss: this.dailyLoss,
      trading_paused: this.tradingPaused
    };
  }
}

export default JupiterUltraExecutor;

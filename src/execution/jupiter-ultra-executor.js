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
    this.jupiterApiKey = process.env.JUPITER_API_KEY || '';

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

  /**
   * 买入 Token（SOL → Token）
   * Ultra 自动滑点(RTSE)对高波动 meme coin 可能偏保守，需要重试
   */
  async buy(tokenCA, amountSol, opts = {}) {
    this._checkSafety(amountSol);

    const amountLamports = Math.floor(amountSol * LAMPORTS_PER_SOL);
    const maxRetries = 3;  // Ultra 滑点失败时重试（每次重新获取 order = 新报价 + 新滑点）
    console.log(`🪐 [JupiterUltra] 买入 ${amountSol} SOL → ${tokenCA.substring(0, 8)}...`);

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        // 1. 获取 Ultra Order（每次重新获取 = 最新价格 + 最新 RTSE 滑点）
        const order = await this._getOrder(SOL_MINT, tokenCA, amountLamports);
        if (!order || !order.transaction) {
          throw new Error('获取 Ultra Order 失败');
        }

        console.log(`   报价: ${amountSol} SOL → ${order.outAmount || '?'} tokens | slippage: ${order.slippageBps || '?'}bps | requestId: ${order.requestId?.substring(0, 8)}...`);

        // 2. 签名
        const signedTx = this._signTransaction(order.transaction);

        // 3. 执行
        const result = await this._executeOrder(signedTx, order.requestId);

        if (result.status === 'Success') {
          this.stats.buys++;
          this.stats.total_sol_spent += amountSol;
          console.log(`✅ [JupiterUltra] 买入成功: ${result.signature}`);

          return {
            success: true,
            txHash: result.signature,
            amountIn: amountSol,
            amountOut: parseInt(order.outAmount || 0),
            tokenCA
          };
        } else {
          throw new Error(`Ultra 执行失败: ${result.status || 'Unknown'} ${result.error || ''}`);
        }
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
      // 每次重试前重新查询 SOL 余额，避免重试时使用过期快照
      const solBefore = await this.getSolBalance();

      try {
        // 1. 获取 Ultra Order（Token → SOL）
        const order = await this._getOrder(tokenCA, SOL_MINT, tokenAmount);
        if (!order || !order.transaction) {
          throw new Error('获取卖出 Ultra Order 失败');
        }

        const outLamports = parseInt(order.outAmount || 0);
        const quotedSol = outLamports / LAMPORTS_PER_SOL;
        console.log(`   报价: ${tokenAmount} tokens → ${quotedSol.toFixed(6)} SOL | slippage: ${order.slippageBps || '?'}bps`);

        // 2. 签名
        const signedTx = this._signTransaction(order.transaction);

        // 3. 执行（Ultra 内置确认，无需手动 confirm）
        const result = await this._executeOrder(signedTx, order.requestId);

        if (result.status === 'Success') {
          // 优先使用 API 返回的精确值，避免并发余额竞态（CR-4）
          let outSol;
          if (result.outputAmount) {
            outSol = parseFloat(result.outputAmount) / LAMPORTS_PER_SOL;
          } else {
            // fallback：查询余额差值（单笔时准确，并发时可能包含其他交易）
            // Ultra 已内置链上确认，500ms 足够余额刷新
            await new Promise(r => setTimeout(r, 500));
            const solAfter = await this.getSolBalance();
            const actualSolReceived = solAfter - solBefore;
            outSol = actualSolReceived > 0 ? actualSolReceived : quotedSol;
          }

          this.stats.sells++;
          this.stats.total_sol_received += outSol;
          console.log(`✅ [JupiterUltra] 卖出成功: ${result.signature} | 实际到账: ${outSol.toFixed(6)} SOL (报价: ${quotedSol.toFixed(6)})`);

          return {
            success: true,
            txHash: result.signature,
            amountIn: tokenAmount,
            amountOut: outSol,
            tokenCA
          };
        } else {
          throw new Error(`Ultra 卖出执行失败: ${result.status || 'Unknown'} ${result.error || ''}`);
        }
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
  async _getOrder(inputMint, outputMint, amount) {
    try {
      const headers = {};
      if (this.jupiterApiKey) headers['x-api-key'] = this.jupiterApiKey;

      const params = new URLSearchParams({
        inputMint,
        outputMint,
        amount: amount.toString(),
        taker: this.walletAddress
      });

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

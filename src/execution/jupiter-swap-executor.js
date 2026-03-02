/**
 * Jupiter Swap Executor
 *
 * 通过 Jupiter API 执行 Solana 链上交易
 * - 买入：SOL → Token（通过 Jupiter Quote + Swap）
 * - 卖出：Token → SOL（支持部分卖出，用于分批止盈）
 * - Anti-MEV：dynamicSlippage + priority fee
 * - 本地签名，不依赖第三方托管
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
import { JitoBundleSender } from './jito-bundle-sender.js';

// SOL mint address
const SOL_MINT = 'So11111111111111111111111111111111111111112';

export class JupiterSwapExecutor {
  constructor() {
    // RPC 连接
    this.rpcUrl = process.env.SOLANA_RPC_URL || 'https://api.mainnet-beta.solana.com';
    this.connection = new Connection(this.rpcUrl, 'confirmed');

    // 交易钱包
    this.wallet = null;
    this.walletAddress = null;

    // 安全限制（硬编码）
    this.maxPositionSol = 0.12;       // 单笔最大 0.12 SOL
    this.maxDailyLossSol = 0.5;       // 每日最大亏损 0.5 SOL
    this.dailyLoss = 0;
    this.dailyLossResetTime = 0;
    this.tradingPaused = false;

    // 🔧 BUG FIX: 手续费保护
    this.dailyFeeSpent = 0;           // 每日已花费手续费
    this.maxDailyFee = 0.5;           // 每日最大手续费 0.5 SOL (提高以允许更多交易)
    this.minSolReserve = 0.01;        // 最少保留 0.01 SOL
    this.feeResetTime = 0;
    this.feePaused = false;

    // Jupiter API
    this.jupiterApiBase = 'https://api.jup.ag';
    this.jupiterApiKey = process.env.JUPITER_API_KEY || '';

    // Jito Bundle Sender (MEV保护 + 更快确认)
    this.jitoSender = null;
    this.useJito = process.env.USE_JITO !== 'false'; // 默认启用

    // 统计
    this.stats = {
      buys: 0,
      sells: 0,
      buy_failures: 0,
      sell_failures: 0,
      total_sol_spent: 0,
      total_sol_received: 0
    };

    console.log('🪐 [JupiterSwap] 初始化');
  }

  /**
   * 初始化钱包
   */
  initialize() {
    const privateKey = process.env.TRADE_WALLET_PRIVATE_KEY;
    if (!privateKey) {
      throw new Error('TRADE_WALLET_PRIVATE_KEY 未设置');
    }

    try {
      const decoded = bs58.decode(privateKey);
      this.wallet = Keypair.fromSecretKey(decoded);
      this.walletAddress = this.wallet.publicKey.toBase58();
      console.log(`✅ [JupiterSwap] 钱包: ${this.walletAddress.substring(0, 8)}...${this.walletAddress.slice(-4)}`);
    } catch (error) {
      throw new Error(`钱包私钥解析失败: ${error.message}`);
    }

    // 初始化 Jito Sender
    if (this.useJito) {
      this.jitoSender = new JitoBundleSender(this.connection, this.wallet);
      console.log('⚡ [JupiterSwap] Jito Bundle 已启用');
    }

    this._resetDailyLoss();
  }

  /**
   * 买入 Token（SOL → Token）
   * @param {string} tokenCA - Token 合约地址
   * @param {number} amountSol - 买入金额（SOL）
   * @returns {object} { success, txHash, amountIn, amountOut, price }
   */
  async buy(tokenCA, amountSol) {
    // 安全检查
    this._checkSafety(amountSol);

    const amountLamports = Math.floor(amountSol * LAMPORTS_PER_SOL);
    const maxRetries = 3;

    console.log(`🪐 [JupiterSwap] 买入 ${amountSol} SOL → ${tokenCA.substring(0, 8)}...`);

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        // 1. 获取报价
        const quote = await this._getQuote(SOL_MINT, tokenCA, amountLamports);
        if (!quote) {
          throw new Error('获取报价失败');
        }

        const outAmount = parseInt(quote.outAmount);
        console.log(`   报价: ${amountSol} SOL → ${outAmount} tokens`);

        // 2. 获取 swap 交易
        const swapTx = await this._getSwapTransaction(quote);
        if (!swapTx) {
          throw new Error('获取 swap 交易失败');
        }

        // 3. 签名并发送
        const txHash = await this._signAndSend(swapTx);

        this.stats.buys++;
        this.stats.total_sol_spent += amountSol;

        console.log(`✅ [JupiterSwap] 买入成功: ${txHash}`);

        return {
          success: true,
          txHash,
          amountIn: amountSol,
          amountOut: outAmount,
          tokenCA
        };
      } catch (error) {
        console.error(`❌ [JupiterSwap] 买入失败 (${attempt}/${maxRetries}): ${error.message}`);
        if (attempt < maxRetries) {
          console.log(`   🔄 重试中...`);
          await new Promise(r => setTimeout(r, 1000)); // 等1秒后重试
        } else {
          this.stats.buy_failures++;
          throw error;
        }
      }
    }
  }

  /**
   * 卖出 Token（Token → SOL）
   * 支持部分卖出，用于分批止盈
   * @param {string} tokenCA - Token 合约地址
   * @param {number} tokenAmount - 卖出的 token 数量（raw amount，含 decimals）
   * @param {object} options - { urgent: boolean } 紧急模式不等待确认
   * @returns {object} { success, txHash, amountIn, amountOut }
   */
  async sell(tokenCA, tokenAmount, options = {}) {
    if (this.tradingPaused) {
      throw new Error('交易已暂停（每日亏损限制）');
    }

    const maxRetries = options.urgent ? 5 : 3;  // 紧急模式重试5次
    const waitConfirm = !options.urgent;  // 紧急模式不等待确认
    console.log(`🪐 [JupiterSwap] 卖出 ${tokenAmount} tokens → SOL | ${tokenCA.substring(0, 8)}...${options.urgent ? ' [紧急模式]' : ''}`);

    // 记录卖出前的余额
    const balanceBefore = await this.getTokenBalance(tokenCA);

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        // 1. 获取报价
        const quote = await this._getQuote(tokenCA, SOL_MINT, tokenAmount);
        if (!quote) {
          throw new Error('获取卖出报价失败');
        }

        const outLamports = parseInt(quote.outAmount);
        const outSol = outLamports / LAMPORTS_PER_SOL;
        console.log(`   报价: ${tokenAmount} tokens → ${outSol.toFixed(6)} SOL`);

        // 2. 获取 swap 交易
        const swapTx = await this._getSwapTransaction(quote);
        if (!swapTx) {
          throw new Error('获取 swap 交易失败');
        }

        // 3. 签名并发送
        const txHash = await this._signAndSend(swapTx, waitConfirm);

        // 4. 检查余额变化（确认交易真的成功了）
        await new Promise(r => setTimeout(r, 2000));  // 等2秒
        const balanceAfter = await this.getTokenBalance(tokenCA);

        if (balanceAfter.amount < balanceBefore.amount) {
          // 余额减少了，说明卖出成功
          this.stats.sells++;
          this.stats.total_sol_received += outSol;
          console.log(`✅ [JupiterSwap] 卖出成功: ${txHash} | 收到 ${outSol.toFixed(6)} SOL`);

          return {
            success: true,
            txHash,
            amountIn: tokenAmount,
            amountOut: outSol,
            tokenCA
          };
        } else {
          // 余额没变，交易可能还没确认或失败了
          console.warn(`⚠️  [JupiterSwap] 余额未变化，交易可能未成功`);
          throw new Error('交易发送但余额未变化');
        }
      } catch (error) {
        console.error(`❌ [JupiterSwap] 卖出失败 (${attempt}/${maxRetries}): ${error.message}`);
        if (attempt < maxRetries) {
          console.log(`   🔄 重试中...`);
          await new Promise(r => setTimeout(r, 500));  // 缩短重试间隔到500ms
        } else {
          this.stats.sell_failures++;
          throw error;
        }
      }
    }
  }

  /**
   * 紧急卖出（Fire-and-Forget 模式）
   * 🔧 BUG FIX: 减少并行交易数量，只发1笔避免浪费手续费
   */
  async emergencySell(tokenCA, tokenAmount) {
    console.log(`🚨 [JupiterSwap] 紧急卖出模式: ${tokenCA.substring(0, 8)}...`);

    // 🔧 BUG FIX: 检查余额是否足够支付手续费
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

    // 🔧 BUG FIX: 只发送1笔交易，使用紧急模式优先费
    const txHashes = [];
    try {
      const quote = await this._getQuote(tokenCA, SOL_MINT, tokenAmount);
      if (!quote) {
        return { success: false, reason: 'quote_failed' };
      }

      const swapTx = await this._getSwapTransaction(quote, true);  // urgent=true
      if (!swapTx) {
        return { success: false, reason: 'swap_tx_failed' };
      }

      const txHash = await this._signAndSend(swapTx, false);  // 不等待确认
      txHashes.push(txHash);
      console.log(`   📤 TX: ${txHash}`);
    } catch (e) {
      console.log(`   ⚠️ 紧急卖出发送失败: ${e.message}`);
      return { success: false, reason: e.message };
    }

    // 等待3秒后检查余额
    await new Promise(r => setTimeout(r, 3000));
    const balanceAfter = await this.getTokenBalance(tokenCA);

    if (balanceAfter.amount < balanceBefore.amount) {
      const soldAmount = balanceBefore.amount - balanceAfter.amount;
      console.log(`✅ [紧急卖出] 成功卖出 ${soldAmount} tokens`);
      return { success: true, txHashes, soldAmount };
    } else {
      console.log(`❌ [紧急卖出] 余额未变化，交易可能失败`);
      return { success: false, txHashes };
    }
  }

  /**
   * 查询钱包中某 token 的余额
   * @param {string} tokenCA - Token 合约地址
   * @returns {object} { amount, decimals, uiAmount }
   */
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
      console.error(`⚠️  [JupiterSwap] 查询余额失败: ${error.message}`);
      return { amount: 0, decimals: 0, uiAmount: 0 };
    }
  }

  /**
   * 查询 SOL 余额
   */
  async getSolBalance() {
    try {
      const balance = await this.connection.getBalance(this.wallet.publicKey);
      return balance / LAMPORTS_PER_SOL;
    } catch (error) {
      console.error(`⚠️  [JupiterSwap] 查询 SOL 余额失败: ${error.message}`);
      return 0;
    }
  }

  // ==================== 内部方法 ====================

  /**
   * 获取 Jupiter 报价
   */
  async _getQuote(inputMint, outputMint, amount) {
    try {
      const headers = {};
      if (this.jupiterApiKey) headers['x-api-key'] = this.jupiterApiKey;

      const res = await axios.get(`${this.jupiterApiBase}/swap/v1/quote`, {
        params: {
          inputMint,
          outputMint,
          amount: amount.toString(),
          slippageBps: 1000,             // 10% 滑点（Jito保护后可降低）
          dynamicSlippage: true,         // 动态滑点 anti-MEV
          maxAutoSlippageBps: 1500,      // 最大 15% 自动滑点（Jito保护后可降低）
          onlyDirectRoutes: false,
          asLegacyTransaction: false
        },
        headers,
        timeout: 10000
      });
      return res.data;
    } catch (error) {
      console.error(`⚠️  [JupiterSwap] 报价失败: ${error.response?.data?.error || error.message}`);
      return null;
    }
  }

  /**
   * 获取 swap 交易数据
   * @param {boolean} urgent - 紧急模式使用更高优先费
   */
  async _getSwapTransaction(quote, urgent = false) {
    try {
      const headers = { 'Content-Type': 'application/json' };
      if (this.jupiterApiKey) headers['x-api-key'] = this.jupiterApiKey;

      // 🔧 BUG FIX: 降低优先费，紧急模式才用高费用
      const priorityFee = urgent ? 2000000 : 500000;  // 紧急0.002 SOL，普通0.0005 SOL

      const res = await axios.post(`${this.jupiterApiBase}/swap/v1/swap`, {
        quoteResponse: quote,
        userPublicKey: this.walletAddress,
        wrapAndUnwrapSol: true,
        dynamicComputeUnitLimit: true,
        dynamicSlippage: true,
        prioritizationFeeLamports: {
          priorityLevelWithMaxLamports: {
            maxLamports: priorityFee,
            priorityLevel: urgent ? 'veryHigh' : 'high'
          }
        }
      }, {
        headers,
        timeout: 15000
      });
      return res.data?.swapTransaction;
    } catch (error) {
      console.error(`⚠️  [JupiterSwap] Swap 交易获取失败: ${error.response?.data?.error || error.message}`);
      return null;
    }
  }

  /**
   * 本地签名并发送交易
   * @param {boolean} waitConfirm - 是否等待确认（紧急卖出时可设为false）
   */
  async _signAndSend(swapTransaction, waitConfirm = true) {
    // 🔧 BUG FIX: 检查每日手续费限制
    this._resetDailyFee();
    if (this.feePaused) {
      throw new Error(`手续费保护：每日手续费已达 ${this.dailyFeeSpent.toFixed(4)} SOL（限制 ${this.maxDailyFee} SOL）`);
    }

    // 🔧 BUG FIX: 检查 SOL 余额是否足够支付手续费
    const solBalance = await this.getSolBalance();
    const estimatedFee = 0.006; // 预估手续费 0.005 + buffer
    if (solBalance < this.minSolReserve + estimatedFee) {
      throw new Error(`SOL 余额不足: ${solBalance.toFixed(4)} SOL，需要至少 ${(this.minSolReserve + estimatedFee).toFixed(4)} SOL`);
    }

    // 反序列化交易
    const txBuf = Buffer.from(swapTransaction, 'base64');
    const tx = VersionedTransaction.deserialize(txBuf);

    // 签名
    tx.sign([this.wallet]);

    // 发送 (优先使用 Jito)
    let txHash;
    if (this.useJito && this.jitoSender) {
      // 通过 Jito 发送 (MEV保护 + 更快确认)
      const jitoResult = await this.jitoSender.sendTransactionSimple(tx, { urgent: !waitConfirm });
      if (jitoResult.success) {
        txHash = jitoResult.txHash;
        if (!jitoResult.fallback) {
          console.log(`⚡ [Jito] 交易已发送: ${txHash}`);
        }
      } else {
        throw new Error(`Jito 发送失败: ${jitoResult.error}`);
      }
    } else {
      // 回退到普通 RPC
      txHash = await this.connection.sendRawTransaction(tx.serialize(), {
        skipPreflight: true,
        maxRetries: 3
      });
    }

    // 🔧 BUG FIX: 记录手续费花费
    this.dailyFeeSpent += estimatedFee;
    if (this.dailyFeeSpent >= this.maxDailyFee) {
      this.feePaused = true;
      console.error(`🚨 [JupiterSwap] 每日手续费达 ${this.dailyFeeSpent.toFixed(4)} SOL，暂停所有交易！`);
    }

    if (waitConfirm) {
      // 确认（最多等 20 秒，从30秒减少）
      try {
        const confirmation = await this.connection.confirmTransaction({
          signature: txHash,
          blockhash: tx.message.recentBlockhash,
          lastValidBlockHeight: (await this.connection.getLatestBlockhash()).lastValidBlockHeight
        }, 'confirmed');

        if (confirmation.value.err) {
          throw new Error(`交易确认失败: ${JSON.stringify(confirmation.value.err)}`);
        }
      } catch (error) {
        // 超时不一定失败，返回txHash让调用方检查余额
        console.warn(`⚠️  [JupiterSwap] 确认超时，交易可能已成功: ${txHash}`);
      }
    } else {
      console.log(`📤 [JupiterSwap] 交易已发送(不等待确认): ${txHash}`);
    }

    return txHash;
  }

  /**
   * 安全检查
   */
  _checkSafety(amountSol) {
    // 每日亏损重置
    this._resetDailyLoss();

    if (this.tradingPaused) {
      throw new Error(`交易已暂停：每日亏损已达 ${this.dailyLoss.toFixed(4)} SOL（限制 ${this.maxDailyLossSol} SOL）`);
    }

    if (amountSol > this.maxPositionSol) {
      throw new Error(`单笔超限: ${amountSol} SOL > 最大 ${this.maxPositionSol} SOL`);
    }

    if (amountSol <= 0) {
      throw new Error('买入金额必须大于 0');
    }
  }

  /**
   * 记录亏损并检查每日限制
   */
  recordLoss(lossSol) {
    this._resetDailyLoss();
    this.dailyLoss += lossSol;
    if (this.dailyLoss >= this.maxDailyLossSol) {
      this.tradingPaused = true;
      console.error(`🚨 [JupiterSwap] 每日亏损达 ${this.dailyLoss.toFixed(4)} SOL，暂停所有交易！`);
    }
  }

  /**
   * 每日亏损重置（UTC 0 点）
   */
  _resetDailyLoss() {
    const now = Date.now();
    const todayStart = new Date();
    todayStart.setUTCHours(0, 0, 0, 0);
    if (this.dailyLossResetTime < todayStart.getTime()) {
      this.dailyLoss = 0;
      this.dailyLossResetTime = todayStart.getTime();
      this.tradingPaused = false;
    }
  }

  /**
   * 🔧 BUG FIX: 每日手续费重置（UTC 0 点）
   */
  _resetDailyFee() {
    const todayStart = new Date();
    todayStart.setUTCHours(0, 0, 0, 0);
    if (this.feeResetTime < todayStart.getTime()) {
      this.dailyFeeSpent = 0;
      this.feeResetTime = todayStart.getTime();
      this.feePaused = false;
      console.log('🔄 [JupiterSwap] 每日手续费计数已重置');
    }
  }

  /**
   * 获取统计
   */
  getStats() {
    return {
      ...this.stats,
      wallet: this.walletAddress ? `${this.walletAddress.substring(0, 8)}...` : 'N/A',
      daily_loss: this.dailyLoss,
      trading_paused: this.tradingPaused
    };
  }
}

export default JupiterSwapExecutor;

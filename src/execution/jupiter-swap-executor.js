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

    // Jupiter API
    this.jupiterApiBase = 'https://api.jup.ag';
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

    console.log(`🪐 [JupiterSwap] 买入 ${amountSol} SOL → ${tokenCA.substring(0, 8)}...`);

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
      this.stats.buy_failures++;
      console.error(`❌ [JupiterSwap] 买入失败: ${error.message}`);
      throw error;
    }
  }

  /**
   * 卖出 Token（Token → SOL）
   * 支持部分卖出，用于分批止盈
   * @param {string} tokenCA - Token 合约地址
   * @param {number} tokenAmount - 卖出的 token 数量（raw amount，含 decimals）
   * @returns {object} { success, txHash, amountIn, amountOut }
   */
  async sell(tokenCA, tokenAmount) {
    if (this.tradingPaused) {
      throw new Error('交易已暂停（每日亏损限制）');
    }

    console.log(`🪐 [JupiterSwap] 卖出 ${tokenAmount} tokens → SOL | ${tokenCA.substring(0, 8)}...`);

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
      const txHash = await this._signAndSend(swapTx);

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
    } catch (error) {
      this.stats.sell_failures++;
      console.error(`❌ [JupiterSwap] 卖出失败: ${error.message}`);
      throw error;
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
          slippageBps: 500,              // 5% 默认滑点
          dynamicSlippage: true,         // 动态滑点 anti-MEV
          maxAutoSlippageBps: 1000,      // 最大 10% 自动滑点
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
   */
  async _getSwapTransaction(quote) {
    try {
      const headers = { 'Content-Type': 'application/json' };
      if (this.jupiterApiKey) headers['x-api-key'] = this.jupiterApiKey;

      const res = await axios.post(`${this.jupiterApiBase}/swap/v1/swap`, {
        quoteResponse: quote,
        userPublicKey: this.walletAddress,
        wrapAndUnwrapSol: true,
        dynamicComputeUnitLimit: true,
        dynamicSlippage: true,
        prioritizationFeeLamports: {
          priorityLevelWithMaxLamports: {
            maxLamports: 1000000,        // 最大 0.001 SOL priority fee
            priorityLevel: 'high'
          }
        }
      }, {
        headers,
        timeout: 10000
      });
      return res.data?.swapTransaction;
    } catch (error) {
      console.error(`⚠️  [JupiterSwap] Swap 交易获取失败: ${error.response?.data?.error || error.message}`);
      return null;
    }
  }

  /**
   * 本地签名并发送交易
   */
  async _signAndSend(swapTransaction) {
    // 反序列化交易
    const txBuf = Buffer.from(swapTransaction, 'base64');
    const tx = VersionedTransaction.deserialize(txBuf);

    // 签名
    tx.sign([this.wallet]);

    // 发送
    const txHash = await this.connection.sendRawTransaction(tx.serialize(), {
      skipPreflight: true,
      maxRetries: 3
    });

    // 确认（最多等 30 秒）
    const confirmation = await this.connection.confirmTransaction(txHash, 'confirmed');
    if (confirmation.value.err) {
      throw new Error(`交易确认失败: ${JSON.stringify(confirmation.value.err)}`);
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

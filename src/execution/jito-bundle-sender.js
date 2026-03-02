/**
 * Jito Bundle Sender
 *
 * 通过 Jito Block Engine 发送交易，获得:
 * - MEV 保护 (减少三明治攻击)
 * - 更快确认 (直接发给 Jito 验证者)
 * - 原子执行 (bundle 要么全成功要么全失败)
 */

import { Connection, VersionedTransaction, LAMPORTS_PER_SOL } from '@solana/web3.js';
import bs58 from 'bs58';

// Jito Block Engine endpoints
const JITO_ENDPOINTS = [
  'https://mainnet.block-engine.jito.wtf',
  'https://amsterdam.mainnet.block-engine.jito.wtf',
  'https://frankfurt.mainnet.block-engine.jito.wtf',
  'https://ny.mainnet.block-engine.jito.wtf',
  'https://tokyo.mainnet.block-engine.jito.wtf',
];

// Jito tip accounts (随机选一个)
const JITO_TIP_ACCOUNTS = [
  '96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5',
  'HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe',
  'Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY',
  'ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49',
  'DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh',
  'ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt',
  'DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL',
  '3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT',
];

export class JitoBundleSender {
  constructor(connection, wallet) {
    this.connection = connection;
    this.wallet = wallet;
    this.currentEndpoint = 0;

    // 默认 tip: 0.001 SOL (足够快，不会太贵)
    this.defaultTipLamports = 1_000_000; // 0.001 SOL
    this.urgentTipLamports = 3_000_000;  // 0.003 SOL (紧急模式)

    console.log('⚡ [JitoBundleSender] 初始化');
  }

  /**
   * 发送单笔交易通过 Jito
   * @param {VersionedTransaction} transaction - 已签名的交易
   * @param {object} options - { urgent: boolean, tipLamports: number }
   * @returns {object} { success, bundleId, txHash }
   */
  async sendTransaction(transaction, options = {}) {
    const { urgent = false, tipLamports } = options;
    const tip = tipLamports || (urgent ? this.urgentTipLamports : this.defaultTipLamports);

    try {
      // 序列化交易
      const serialized = bs58.encode(transaction.serialize());

      // 发送到 Jito
      const result = await this._sendBundle([serialized], tip);

      if (result.success) {
        console.log(`✅ [Jito] Bundle 发送成功: ${result.bundleId}`);
        return {
          success: true,
          bundleId: result.bundleId,
          txHash: result.txHash
        };
      } else {
        console.error(`❌ [Jito] Bundle 发送失败: ${result.error}`);
        return { success: false, error: result.error };
      }
    } catch (error) {
      console.error(`❌ [Jito] 发送异常: ${error.message}`);
      return { success: false, error: error.message };
    }
  }

  /**
   * 发送 bundle 到 Jito Block Engine
   * @param {string[]} transactions - base58 编码的交易数组
   * @param {number} tipLamports - tip 金额
   */
  async _sendBundle(transactions, tipLamports) {
    const endpoint = this._getNextEndpoint();

    try {
      // 方法1: 使用 Jito 的 sendBundle API
      const response = await fetch(`${endpoint}/api/v1/bundles`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          jsonrpc: '2.0',
          id: 1,
          method: 'sendBundle',
          params: [transactions]
        }),
      });

      const data = await response.json();

      if (data.result) {
        return {
          success: true,
          bundleId: data.result,
          txHash: transactions[0] // 第一笔交易的 hash
        };
      } else if (data.error) {
        return { success: false, error: data.error.message || JSON.stringify(data.error) };
      } else {
        return { success: false, error: 'Unknown response' };
      }
    } catch (error) {
      // 尝试下一个 endpoint
      console.warn(`⚠️ [Jito] Endpoint ${endpoint} 失败，尝试下一个...`);
      this.currentEndpoint = (this.currentEndpoint + 1) % JITO_ENDPOINTS.length;
      throw error;
    }
  }

  /**
   * 发送交易通过 Jito 的 sendTransaction API (更简单)
   * 这个不需要构建 bundle，直接发送单笔交易
   */
  async sendTransactionSimple(signedTransaction, options = {}) {
    const { urgent = false } = options;
    const endpoint = this._getNextEndpoint();

    try {
      const serialized = bs58.encode(signedTransaction.serialize());

      const response = await fetch(`${endpoint}/api/v1/transactions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          jsonrpc: '2.0',
          id: 1,
          method: 'sendTransaction',
          params: [serialized, { encoding: 'base58' }]
        }),
      });

      const data = await response.json();

      if (data.result) {
        console.log(`✅ [Jito] 交易发送成功: ${data.result}`);
        return {
          success: true,
          txHash: data.result
        };
      } else if (data.error) {
        // 如果 Jito 失败，回退到普通 RPC
        console.warn(`⚠️ [Jito] 发送失败: ${data.error.message}，回退到普通 RPC`);
        return await this._fallbackSend(signedTransaction);
      } else {
        return { success: false, error: 'Unknown response' };
      }
    } catch (error) {
      console.warn(`⚠️ [Jito] 异常: ${error.message}，回退到普通 RPC`);
      return await this._fallbackSend(signedTransaction);
    }
  }

  /**
   * 回退到普通 RPC 发送
   */
  async _fallbackSend(signedTransaction) {
    try {
      const txHash = await this.connection.sendRawTransaction(
        signedTransaction.serialize(),
        { skipPreflight: true, maxRetries: 3 }
      );
      console.log(`📤 [Fallback] 通过普通 RPC 发送: ${txHash}`);
      return { success: true, txHash, fallback: true };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  /**
   * 获取下一个 Jito endpoint (轮询)
   */
  _getNextEndpoint() {
    const endpoint = JITO_ENDPOINTS[this.currentEndpoint];
    this.currentEndpoint = (this.currentEndpoint + 1) % JITO_ENDPOINTS.length;
    return endpoint;
  }

  /**
   * 获取随机 tip account
   */
  getRandomTipAccount() {
    const index = Math.floor(Math.random() * JITO_TIP_ACCOUNTS.length);
    return JITO_TIP_ACCOUNTS[index];
  }
}

export default JitoBundleSender;

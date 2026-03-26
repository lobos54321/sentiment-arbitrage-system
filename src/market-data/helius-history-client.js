import { fetchWithRetry } from '../utils/fetch-with-retry.js';
import { RateLimiter } from '../utils/rate-limiter.js';

export class HeliusHistoryClient {
  constructor(config = {}) {
    this.apiKey = config.apiKey || process.env.HELIUS_API_KEY || '';
    this.rpcUrl = config.rpcUrl || (this.apiKey ? `https://mainnet.helius-rpc.com/?api-key=${this.apiKey}` : '');
    this.enhancedUrl = config.enhancedUrl || 'https://api.helius.xyz/v0';
    this.signatureLimiter = new RateLimiter(Number(config.signatureRps || 2), 1);
    this.txLimiter = new RateLimiter(Number(config.transactionRps || 1), 1);
    this.pageSize = Number(config.pageSize || 100);
    this.batchSize = Number(config.batchSize || 25);
  }

  isEnabled() {
    return Boolean(this.apiKey && this.rpcUrl);
  }

  async #postRpc(method, params) {
    const response = await fetchWithRetry(this.rpcUrl, {
      source: 'HELIUS',
      method: 'POST',
      timeout: 20000,
      maxRetries: 3,
      initialDelay: 1500,
      headers: { 'content-type': 'application/json' },
      body: { jsonrpc: '2.0', id: method, method, params },
      silent: true
    });

    if (response?.error) {
      throw new Error(response.error);
    }
    if (response?.result === undefined) {
      throw new Error(`helius_rpc_invalid_${method}`);
    }
    return response.result;
  }

  async getSignaturesForAddress(address, { before, until, limit = this.pageSize } = {}) {
    if (!this.isEnabled()) {
      throw new Error('helius_disabled');
    }

    await this.signatureLimiter.throttle();
    const params = [address, {
      limit,
      ...(before ? { before } : {}),
      ...(until ? { until } : {})
    }];
    return this.#postRpc('getSignaturesForAddress', params);
  }

  async getEnhancedTransactions(signatures = []) {
    if (!signatures.length) {
      return [];
    }

    await this.txLimiter.throttle();
    const url = `${this.enhancedUrl}/transactions/?api-key=${this.apiKey}`;
    const response = await fetchWithRetry(url, {
      source: 'HELIUS',
      method: 'POST',
      timeout: 30000,
      maxRetries: 3,
      initialDelay: 1500,
      headers: { 'content-type': 'application/json' },
      body: { transactions: signatures },
      silent: true
    });

    if (response?.error) {
      throw new Error(response.error);
    }
    return Array.isArray(response) ? response : [];
  }

  async fetchHistoryPage(address, options = {}) {
    const signatures = await this.getSignaturesForAddress(address, options);
    if (!signatures.length) {
      return { signatures: [], transactions: [] };
    }

    const collected = [];
    for (let i = 0; i < signatures.length; i += this.batchSize) {
      const batch = signatures.slice(i, i + this.batchSize).map((item) => item.signature).filter(Boolean);
      const txs = await this.getEnhancedTransactions(batch);
      collected.push(...txs);
    }

    return { signatures, transactions: collected };
  }
}

export default HeliusHistoryClient;

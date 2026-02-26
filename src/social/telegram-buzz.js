/**
 * Telegram Buzz Scanner
 *
 * 用 Telegram 全局搜索检测代币在多个群组的传播度
 * 被多个群讨论 = 真热度，是免费的社交热度指标
 */

import { Api } from 'telegram';

export class TelegramBuzzScanner {
  constructor(client) {
    this.client = client;
    this.cache = new Map(); // symbol -> { score, timestamp }
    this.CACHE_TTL = 3 * 60 * 1000; // 3 分钟缓存
  }

  /**
   * 搜索代币在 Telegram 的传播度
   * @param {string} symbol - 代币符号
   * @param {string} tokenCA - 合约地址（可选，用于更精确搜索）
   * @returns {{ mentions: number, uniqueGroups: number, score: number, groups: string[] }}
   */
  async scan(symbol, tokenCA = null) {
    // 缓存检查
    const cacheKey = symbol || tokenCA;
    const cached = this.cache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < this.CACHE_TTL) {
      return cached.data;
    }

    try {
      // 搜索代币名（最近 1 小时）
      const result = await this.client.invoke(new Api.messages.SearchGlobal({
        q: symbol,
        filter: new Api.InputMessagesFilterEmpty(),
        minDate: Math.floor(Date.now() / 1000) - 3600,
        maxDate: 0,
        offsetRate: 0,
        offsetPeer: new Api.InputPeerEmpty(),
        offsetId: 0,
        limit: 50
      }));

      const messages = result.messages || [];
      const chats = result.chats || [];

      // 统计不同群的提及
      const chatMap = new Map();
      for (const msg of messages) {
        const peerId = String(msg.peerId?.channelId || msg.peerId?.chatId || '');
        if (peerId) {
          chatMap.set(peerId, (chatMap.get(peerId) || 0) + 1);
        }
      }

      // 获取群名
      const groups = [];
      for (const [id] of chatMap) {
        const chat = chats.find(c => String(c.id) === id || String(c.id?.value) === id);
        if (chat?.title) groups.push(chat.title);
      }

      const mentions = messages.length;
      const uniqueGroups = chatMap.size;

      // 评分：群数量是最重要的指标（门槛提高，因为频道信号本身就有基础传播）
      let score = 0;
      if (uniqueGroups >= 10) score = 20;
      else if (uniqueGroups >= 7) score = 15;
      else if (uniqueGroups >= 5) score = 10;
      else if (uniqueGroups >= 3) score = 5;

      // 提及数量加分（门槛提高）
      if (mentions >= 30) score += 5;
      else if (mentions >= 10) score += 5;

      const data = { mentions, uniqueGroups, score, groups };

      // 缓存
      this.cache.set(cacheKey, { data, timestamp: Date.now() });

      return data;
    } catch (error) {
      console.warn(`⚠️ [TG Buzz] 搜索失败: ${error.message}`);
      return { mentions: 0, uniqueGroups: 0, score: 0, groups: [] };
    }
  }
}

export default TelegramBuzzScanner;

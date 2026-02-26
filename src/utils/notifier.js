/**
 * 通知服务 - Server酱微信推送
 *
 * 使用方法:
 * 1. 访问 https://sct.ftqq.com/ 微信扫码登录
 * 2. 获取 SendKey
 * 3. 在 .env 中设置 SERVERCHAN_KEY=你的SendKey
 */

class Notifier {
  constructor() {
    this.lastSentTime = {};  // 防止重复发送
    this.cooldownMs = 60000; // 同类消息冷却时间 1 分钟
    this._initialized = false;

    // 🔥 v6.7 免打扰时段配置
    this.quietHours = {
      start: 22,  // 晚上 10 点开始
      end: 10,    // 早上 10 点结束
      enabled: true
    };
    this.pendingMessages = []; // 夜间消息队列
    this._startMorningNotifier();
  }

  /**
   * 检查当前是否在免打扰时段
   */
  isQuietHours() {
    if (!this.quietHours.enabled) return false;
    const hour = new Date().getHours();
    // 22:00 - 23:59 或 00:00 - 09:59
    return hour >= this.quietHours.start || hour < this.quietHours.end;
  }

  /**
   * 启动早间汇总通知器
   */
  _startMorningNotifier() {
    // 每分钟检查一次是否到了早上10点
    setInterval(() => {
      const now = new Date();
      const hour = now.getHours();
      const minute = now.getMinutes();

      // 早上 10:00 - 10:01 发送汇总
      if (hour === this.quietHours.end && minute === 0 && this.pendingMessages.length > 0) {
        this._sendMorningSummary();
      }
    }, 60000); // 每分钟检查
  }

  /**
   * 发送早间汇总
   */
  async _sendMorningSummary() {
    if (this.pendingMessages.length === 0) return;

    const messages = [...this.pendingMessages];
    this.pendingMessages = []; // 清空队列

    const summary = messages.map((m, i) => `### ${i + 1}. ${m.title}\n${m.time}\n${m.content}`).join('\n\n---\n\n');

    const content = `
## 🌅 夜间消息汇总

共 **${messages.length}** 条消息在免打扰时段收到：

${summary}

---
*夜间免打扰时段: ${this.quietHours.start}:00 - ${this.quietHours.end}:00*
`;

    // 直接发送，跳过免打扰检查
    await this._sendDirect(`🌅 夜间消息汇总 (${messages.length}条)`, content, 'morning_summary');
  }

  /**
   * 懒加载初始化 (确保 dotenv 已加载)
   */
  _ensureInit() {
    if (this._initialized) return;
    this._initialized = true;

    this.sendKey = process.env.SERVERCHAN_KEY;
    this.enabled = !!this.sendKey;

    if (this.enabled) {
      console.log('📱 [Notifier] Server酱微信通知已启用');
    } else {
      console.log('📱 [Notifier] 未配置 SERVERCHAN_KEY，微信通知未启用');
    }
  }

  /**
   * 发送微信通知
   * @param {string} title - 消息标题
   * @param {string} content - 消息内容 (支持 Markdown)
   * @param {string} type - 消息类型 (用于防重复)
   */
  async send(title, content, type = 'default') {
    this._ensureInit();

    if (!this.enabled) {
      console.log(`[Notifier] 跳过通知 (未启用): ${title}`);
      return false;
    }

    // 防重复发送
    const now = Date.now();
    if (this.lastSentTime[type] && now - this.lastSentTime[type] < this.cooldownMs) {
      console.log(`[Notifier] 跳过通知 (冷却中): ${title}`);
      return false;
    }

    // 🔥 v6.7 免打扰时段处理
    if (this.isQuietHours() && type !== 'morning_summary') {
      console.log(`📱 [Notifier] 免打扰时段，消息已加入队列: ${title}`);
      this.pendingMessages.push({
        title,
        content,
        type,
        time: new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' })
      });
      return true; // 返回 true 表示消息已处理（排队）
    }

    return this._sendDirect(title, content, type);
  }

  /**
   * 直接发送通知（跳过免打扰检查）
   * @private
   */
  async _sendDirect(title, content, type) {
    try {
      const url = `https://sctapi.ftqq.com/${this.sendKey}.send`;
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded'
        },
        body: new URLSearchParams({
          title: title,
          desp: content
        })
      });

      const result = await response.json();

      if (result.code === 0) {
        console.log(`📱 [Notifier] ✅ 微信通知已发送: ${title}`);
        this.lastSentTime[type] = Date.now();
        return true;
      } else {
        console.error(`📱 [Notifier] ❌ 发送失败: ${result.message}`);
        return false;
      }
    } catch (error) {
      console.error(`📱 [Notifier] ❌ 发送错误: ${error.message}`);
      return false;
    }
  }

  /**
   * 发送紧急告警
   */
  async critical(title, details) {
    const content = `
## 🚨 紧急告警

**时间**: ${new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' })}

**详情**:
${details}

---
*来自 Sentiment Arbitrage System*
`;
    return this.send(`🚨 ${title}`, content, 'critical');
  }

  /**
   * 发送系统状态通知
   */
  async systemStatus(title, status) {
    const content = `
## 📊 系统状态

**时间**: ${new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' })}

${status}

---
*来自 Sentiment Arbitrage System*
`;
    return this.send(`📊 ${title}`, content, 'status');
  }

  /**
   * 发送交易通知
   */
  async trade(action, symbol, chain, details) {
    const emoji = action === 'BUY' ? '🟢' : '🔴';
    const content = `
## ${emoji} ${action} ${symbol}

**链**: ${chain}
**时间**: ${new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' })}

${details}

---
*来自 Sentiment Arbitrage System*
`;
    return this.send(`${emoji} ${action}: ${symbol}`, content, `trade_${symbol}`);
  }

  /**
   * 发送 DeBot 崩溃通知
   */
  async debotCrash(info) {
    const content = `
## 🚨 DeBot 浏览器崩溃

**时间**: ${info.at}
**连续失败**: ${info.consecutiveErrors} 次
**错误信息**: ${info.error}

${info.consecutiveErrors <= 3 ? '⏳ 正在尝试自动重启...' : '❌ 已停止自动重启，请手动检查!'}

---
*来自 Sentiment Arbitrage System*
`;
    return this.send('🚨 DeBot 崩溃', content, 'debot_crash');
  }

  /**
   * 发送 DeBot Session 过期通知
   */
  async debotSessionExpired(info) {
    const content = `
## ‼️ DeBot Session 过期

**时间**: ${info.at}
**原因**: ${info.reason}

**解决方法**: 运行 \`node scripts/debot-login-setup.js\` 重新登录

---
*来自 Sentiment Arbitrage System*
`;
    return this.send('‼️ DeBot Session 过期', content, 'debot_session');
  }

  /**
   * 每日报告
   */
  async dailyReport(stats) {
    const content = `
## 📈 每日交易报告

**日期**: ${new Date().toLocaleDateString('zh-CN', { timeZone: 'Asia/Shanghai' })}

### 交易统计
- 总交易: ${stats.totalTrades} 笔
- 胜率: ${stats.winRate}%
- 总盈亏: ${stats.totalPnl > 0 ? '+' : ''}${stats.totalPnl}%

### 持仓情况
- 当前持仓: ${stats.openPositions} 个
- 今日新开: ${stats.newPositions} 个
- 今日平仓: ${stats.closedPositions} 个

---
*来自 Sentiment Arbitrage System*
`;
    return this.send('📈 每日报告', content, 'daily_report');
  }
}

// 单例模式
const notifier = new Notifier();
export default notifier;

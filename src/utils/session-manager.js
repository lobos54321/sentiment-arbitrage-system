/**
 * 🔐 Session Manager v1.0
 *
 * 统一管理 GMGN/DeBot 的会话状态：
 * - 会话过期检测 (403 累计计数)
 * - 自动发送刷新通知
 * - 会话健康状态追踪
 * - 降级处理策略
 *
 * 使用方法:
 * import { sessionManager } from '../utils/session-manager.js';
 *
 * // 在 API 调用失败时报告
 * sessionManager.report403('GMGN');
 *
 * // 检查会话状态
 * const isHealthy = sessionManager.isHealthy('GMGN');
 */

import { EventEmitter } from 'events';
import fs from 'fs';

// ═══════════════════════════════════════════════════════════════
// 配置
// ═══════════════════════════════════════════════════════════════

const CONFIG = {
    // 403 计数阈值，超过后认为会话过期
    threshold403: 3,

    // 重置窗口（毫秒）- 5分钟内没有 403 则重置计数
    resetWindow: 5 * 60 * 1000,

    // 通知冷却（毫秒）- 防止刷屏通知
    notifyCooldown: 10 * 60 * 1000,

    // 支持的服务 (v8.0: 移除 DeBot，因为它使用 Playwright 模式，不需要 API 健康检查)
    services: ['GMGN'],

    // 会话文件路径
    sessionPaths: {
        GMGN: './config/gmgn_session.json',
        DeBot: './config/debot_session.json'
    }
};

// ═══════════════════════════════════════════════════════════════
// Session Manager 类
// ═══════════════════════════════════════════════════════════════

class SessionManager extends EventEmitter {
    constructor() {
        super();

        this.stats = {};
        this.lastNotify = {};

        // 初始化每个服务的状态
        for (const service of CONFIG.services) {
            this.stats[service] = {
                count403: 0,
                lastError: null,
                lastSuccess: null,
                isExpired: false,
                totalRequests: 0,
                successRate: 1.0
            };
            this.lastNotify[service] = 0;
        }

        console.log('[SessionManager] 🔐 Session Manager 初始化');
    }

    /**
     * 报告 403 错误
     * @param {string} service - 服务名 (GMGN/DeBot)
     * @param {object} context - 额外上下文信息
     */
    report403(service, context = {}) {
        if (!CONFIG.services.includes(service)) {
            console.warn(`[SessionManager] 未知服务: ${service}`);
            return;
        }

        const stat = this.stats[service];
        const now = Date.now();

        // 如果超过重置窗口，先重置计数
        if (stat.lastError && now - stat.lastError > CONFIG.resetWindow) {
            stat.count403 = 0;
        }

        stat.count403++;
        stat.lastError = now;
        stat.totalRequests++;
        stat.successRate = 1 - (stat.count403 / Math.max(stat.totalRequests, 1));

        console.log(`[SessionManager] ⚠️ ${service} 收到 403 (${stat.count403}/${CONFIG.threshold403})`);

        // 达到阈值，标记为过期
        if (stat.count403 >= CONFIG.threshold403 && !stat.isExpired) {
            stat.isExpired = true;
            this.handleSessionExpired(service, context);
        }
    }

    /**
     * 报告成功请求
     * @param {string} service - 服务名
     */
    reportSuccess(service) {
        if (!CONFIG.services.includes(service)) return;

        const stat = this.stats[service];
        stat.lastSuccess = Date.now();
        stat.totalRequests++;
        stat.successRate = 1 - (stat.count403 / Math.max(stat.totalRequests, 1));

        // 成功请求后重置 403 计数
        if (stat.count403 > 0) {
            stat.count403 = 0;
            console.log(`[SessionManager] ✅ ${service} 会话正常，重置计数`);
        }

        // 如果之前标记为过期，现在恢复
        if (stat.isExpired) {
            stat.isExpired = false;
            console.log(`[SessionManager] ✅ ${service} 会话已恢复`);
            this.emit('session-recovered', { service });
        }
    }

    /**
     * 处理会话过期
     */
    handleSessionExpired(service, context) {
        const now = Date.now();

        console.log(`\n[SessionManager] ════════════════════════════════════════`);
        console.log(`[SessionManager] ❌ ${service} 会话已过期!`);
        console.log(`[SessionManager] ════════════════════════════════════════\n`);

        // 检查通知冷却
        if (now - this.lastNotify[service] < CONFIG.notifyCooldown) {
            console.log(`[SessionManager] 通知冷却中，跳过重复通知`);
            return;
        }

        this.lastNotify[service] = now;

        // 发出事件
        this.emit('session-expired', {
            service,
            timestamp: now,
            stats: this.stats[service],
            sessionPath: CONFIG.sessionPaths[service],
            context
        });

        // 提示用户操作
        console.log(`[SessionManager] 💡 解决方法:`);
        if (service === 'GMGN') {
            console.log(`   1. 运行: node scripts/gmgn-login-setup.js`);
            console.log(`   2. 或启用自动刷新器: GMGNCookieRefresher`);
        } else if (service === 'DeBot') {
            console.log(`   1. 运行: node scripts/debot-login-setup.js`);
            console.log(`   2. 在浏览器登录 debot.ai 并更新 .env 中的 DEBOT_COOKIE`);
        }
    }

    /**
     * 检查会话是否健康
     * @param {string} service - 服务名
     * @returns {boolean}
     */
    isHealthy(service) {
        if (!CONFIG.services.includes(service)) return true;
        return !this.stats[service].isExpired;
    }

    /**
     * 获取会话状态
     * @param {string} service - 服务名
     * @returns {object}
     */
    getStatus(service) {
        if (service && CONFIG.services.includes(service)) {
            return {
                service,
                ...this.stats[service],
                healthy: !this.stats[service].isExpired
            };
        }

        // 返回所有服务状态
        const result = {};
        for (const s of CONFIG.services) {
            result[s] = {
                ...this.stats[s],
                healthy: !this.stats[s].isExpired
            };
        }
        return result;
    }

    /**
     * 检查会话文件是否存在且有效
     * @param {string} service - 服务名
     * @returns {object} { exists, valid, age }
     */
    checkSessionFile(service) {
        const sessionPath = CONFIG.sessionPaths[service];
        if (!sessionPath) return { exists: false, valid: false };

        if (!fs.existsSync(sessionPath)) {
            return { exists: false, valid: false };
        }

        try {
            const data = JSON.parse(fs.readFileSync(sessionPath, 'utf8'));
            const cookies = data.cookies || [];
            const hasCookies = cookies.length > 0;

            // 检查文件修改时间
            const stats = fs.statSync(sessionPath);
            const age = Date.now() - stats.mtimeMs;

            return {
                exists: true,
                valid: hasCookies,
                age,
                cookieCount: cookies.length,
                lastModified: new Date(stats.mtimeMs).toISOString()
            };
        } catch (e) {
            return { exists: true, valid: false, error: e.message };
        }
    }

    /**
     * 手动重置会话状态
     * @param {string} service - 服务名
     */
    reset(service) {
        if (CONFIG.services.includes(service)) {
            this.stats[service] = {
                count403: 0,
                lastError: null,
                lastSuccess: Date.now(),
                isExpired: false,
                totalRequests: 0,
                successRate: 1.0
            };
            console.log(`[SessionManager] 🔄 ${service} 状态已重置`);
        }
    }

    /**
     * 打印状态摘要
     */
    printSummary() {
        console.log('\n[SessionManager] 📊 会话状态摘要:');
        console.log('─'.repeat(50));

        for (const service of CONFIG.services) {
            const stat = this.stats[service];
            const file = this.checkSessionFile(service);
            const status = stat.isExpired ? '❌ 过期' : '✅ 正常';

            console.log(`${service}:`);
            console.log(`   状态: ${status}`);
            console.log(`   成功率: ${(stat.successRate * 100).toFixed(1)}%`);
            console.log(`   403计数: ${stat.count403}/${CONFIG.threshold403}`);
            console.log(`   文件: ${file.valid ? '✓ 有效' : '✗ 无效'} (${file.cookieCount || 0} cookies)`);
        }

        console.log('─'.repeat(50));
    }
}

// ═══════════════════════════════════════════════════════════════
// 单例导出
// ═══════════════════════════════════════════════════════════════

const sessionManager = new SessionManager();

export { sessionManager, SessionManager };
export default sessionManager;

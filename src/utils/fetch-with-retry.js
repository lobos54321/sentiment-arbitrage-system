/**
 * 🔄 统一 API 限流重试工具 v1.1
 *
 * 所有外部 API 调用统一使用此工具，提供：
 * - 指数退避重试
 * - 403 限流特殊处理
 * - 会话健康追踪（集成 SessionManager）
 * - 统一日志格式
 * - 超时控制
 *
 * 使用示例:
 * import { fetchWithRetry, FetchError } from '../utils/fetch-with-retry.js';
 * const data = await fetchWithRetry(url, { headers, source: 'GMGN' });
 */

import { sessionManager } from './session-manager.js';

// ═══════════════════════════════════════════════════════════════
// 默认配置
// ═══════════════════════════════════════════════════════════════

const DEFAULT_CONFIG = {
    maxRetries: 3,
    initialDelay: 1000,      // 首次重试等待 1秒
    maxDelay: 30000,         // 最长等待 30秒
    backoffFactor: 2,        // 退避因子
    timeout: 30000,          // 请求超时 30秒
    retryOn403: true,        // 403 时重试（限流）
    retryOnTimeout: true,    // 超时时重试
    retryOnNetworkError: true // 网络错误时重试
};

// ═══════════════════════════════════════════════════════════════
// 自定义错误类
// ═══════════════════════════════════════════════════════════════

export class FetchError extends Error {
    constructor(message, { status, url, attempts, lastError } = {}) {
        super(message);
        this.name = 'FetchError';
        this.status = status;
        this.url = url;
        this.attempts = attempts;
        this.lastError = lastError;
    }
}

// ═══════════════════════════════════════════════════════════════
// 主函数
// ═══════════════════════════════════════════════════════════════

/**
 * 带重试的 fetch 请求
 *
 * @param {string} url - 请求 URL
 * @param {Object} options - 配置选项
 * @param {Object} options.headers - 请求头
 * @param {string} options.method - 请求方法 (默认 GET)
 * @param {Object} options.body - 请求体
 * @param {string} options.source - 日志来源标识 (如 'GMGN', 'DeBot')
 * @param {number} options.maxRetries - 最大重试次数
 * @param {number} options.initialDelay - 首次重试延迟 (ms)
 * @param {number} options.timeout - 请求超时 (ms)
 * @param {boolean} options.silent - 静默模式，不打印日志
 * @param {boolean} options.returnRaw - 返回原始 Response 而非 JSON
 * @returns {Promise<Object|Response>} - JSON 数据或 Response 对象
 */
export async function fetchWithRetry(url, options = {}) {
    const config = {
        ...DEFAULT_CONFIG,
        ...options
    };

    const {
        headers = {},
        method = 'GET',
        body,
        source = 'API',
        maxRetries,
        initialDelay,
        maxDelay,
        backoffFactor,
        timeout,
        retryOn403,
        retryOnTimeout,
        retryOnNetworkError,
        silent = false,
        returnRaw = false
    } = config;

    const log = (level, message) => {
        if (silent) return;
        const prefix = `[FetchRetry:${source}]`;
        if (level === 'warn') {
            console.log(`${prefix} ⚠️ ${message}`);
        } else if (level === 'error') {
            console.log(`${prefix} ❌ ${message}`);
        } else {
            console.log(`${prefix} ${message}`);
        }
    };

    // 检测是否为需要追踪会话的服务
    const isTrackedService = ['GMGN', 'DeBot'].includes(source);

    let lastError = null;
    let delay = initialDelay;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), timeout);

        try {
            const fetchOptions = {
                method,
                headers,
                signal: controller.signal
            };

            if (body && method !== 'GET') {
                fetchOptions.body = typeof body === 'string' ? body : JSON.stringify(body);
            }

            const response = await fetch(url, fetchOptions);
            clearTimeout(timeoutId);

            // ═══════════════════════════════════════════════════════
            // 处理响应状态
            // ═══════════════════════════════════════════════════════

            // 成功
            if (response.ok) {
                // 追踪会话健康状态
                if (isTrackedService) {
                    sessionManager.reportSuccess(source);
                }

                if (returnRaw) {
                    return response;
                }
                try {
                    return await response.json();
                } catch (e) {
                    // 某些 API 返回空内容
                    return { success: true, status: response.status };
                }
            }

            // 403 限流
            if (response.status === 403 && retryOn403) {
                // 追踪会话 403 错误
                if (isTrackedService) {
                    sessionManager.report403(source, { url, attempt });
                }

                log('warn', `API 限流 (403), 等待 ${delay / 1000}s 后重试 (${attempt}/${maxRetries})`);

                if (attempt < maxRetries) {
                    await sleep(delay);
                    delay = Math.min(delay * backoffFactor, maxDelay);
                    continue;
                }
            }

            // 429 速率限制
            if (response.status === 429) {
                const retryAfter = response.headers.get('Retry-After');
                const waitTime = retryAfter ? parseInt(retryAfter) * 1000 : delay;

                log('warn', `速率限制 (429), 等待 ${waitTime / 1000}s 后重试 (${attempt}/${maxRetries})`);

                if (attempt < maxRetries) {
                    await sleep(waitTime);
                    delay = Math.min(delay * backoffFactor, maxDelay);
                    continue;
                }
            }

            // 5xx 服务器错误
            if (response.status >= 500 && attempt < maxRetries) {
                log('warn', `服务器错误 (${response.status}), 等待 ${delay / 1000}s 后重试 (${attempt}/${maxRetries})`);
                await sleep(delay);
                delay = Math.min(delay * backoffFactor, maxDelay);
                continue;
            }

            // 其他错误，不重试
            lastError = new FetchError(`HTTP ${response.status}`, {
                status: response.status,
                url,
                attempts: attempt
            });

            // 尝试读取错误详情
            try {
                const errorBody = await response.text();
                if (errorBody) {
                    lastError.message = `HTTP ${response.status}: ${errorBody.slice(0, 200)}`;
                }
            } catch (e) {
                // 忽略
            }

            break;

        } catch (error) {
            clearTimeout(timeoutId);

            // 超时
            if (error.name === 'AbortError') {
                lastError = new FetchError(`请求超时 (${timeout}ms)`, {
                    url,
                    attempts: attempt,
                    lastError: error
                });

                if (retryOnTimeout && attempt < maxRetries) {
                    log('warn', `请求超时, 等待 ${delay / 1000}s 后重试 (${attempt}/${maxRetries})`);
                    await sleep(delay);
                    delay = Math.min(delay * backoffFactor, maxDelay);
                    continue;
                }
            }

            // 网络错误
            else if (error.code === 'ECONNREFUSED' ||
                     error.code === 'ENOTFOUND' ||
                     error.code === 'ETIMEDOUT' ||
                     error.message.includes('fetch failed')) {

                lastError = new FetchError(`网络错误: ${error.message}`, {
                    url,
                    attempts: attempt,
                    lastError: error
                });

                if (retryOnNetworkError && attempt < maxRetries) {
                    log('warn', `网络错误, 等待 ${delay / 1000}s 后重试 (${attempt}/${maxRetries})`);
                    await sleep(delay);
                    delay = Math.min(delay * backoffFactor, maxDelay);
                    continue;
                }
            }

            // 其他错误
            else {
                lastError = new FetchError(error.message, {
                    url,
                    attempts: attempt,
                    lastError: error
                });
            }
        }
    }

    // 所有重试都失败
    log('error', `请求失败 (${maxRetries}次重试后): ${lastError?.message || 'Unknown error'}`);

    // 返回错误对象而非抛出异常，保持向后兼容
    return {
        error: lastError?.message || 'Unknown error',
        status: lastError?.status,
        attempts: lastError?.attempts || maxRetries
    };
}

// ═══════════════════════════════════════════════════════════════
// 便捷方法
// ═══════════════════════════════════════════════════════════════

/**
 * GET 请求
 */
export async function fetchGet(url, headers = {}, options = {}) {
    return fetchWithRetry(url, { ...options, headers, method: 'GET' });
}

/**
 * POST 请求
 */
export async function fetchPost(url, body, headers = {}, options = {}) {
    return fetchWithRetry(url, {
        ...options,
        headers: {
            'Content-Type': 'application/json',
            ...headers
        },
        method: 'POST',
        body
    });
}

/**
 * 创建预配置的 fetcher
 * 适用于同一 API 多次调用的场景
 *
 * @param {Object} defaultOptions - 默认配置
 * @returns {Function} - 配置好的 fetch 函数
 *
 * @example
 * const gmgnFetch = createFetcher({
 *   source: 'GMGN',
 *   headers: { Cookie: '...' }
 * });
 * const data = await gmgnFetch('/api/wallet/123');
 */
export function createFetcher(defaultOptions = {}) {
    const baseUrl = defaultOptions.baseUrl || '';

    return async function(path, options = {}) {
        const url = path.startsWith('http') ? path : `${baseUrl}${path}`;
        return fetchWithRetry(url, {
            ...defaultOptions,
            ...options,
            headers: {
                ...defaultOptions.headers,
                ...options.headers
            }
        });
    };
}

// ═══════════════════════════════════════════════════════════════
// 工具函数
// ═══════════════════════════════════════════════════════════════

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// ═══════════════════════════════════════════════════════════════
// 导出默认配置（供外部参考）
// ═══════════════════════════════════════════════════════════════

export { DEFAULT_CONFIG };

// 重新导出 sessionManager 便于外部访问
export { sessionManager };

export default fetchWithRetry;

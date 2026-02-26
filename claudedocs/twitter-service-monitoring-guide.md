# Twitter 服务监控和告警系统 - 完整指南

**创建时间**: 2025-12-19
**目标**: 确保 Twitter 数据采集服务稳定运行,自动应对账号被封等问题

---

## 🛡️ 三层防护机制

### 1. 健康监控系统 (Health Monitor)

**文件**: `twitter-service/health_monitor.py`

**功能**:
- 实时追踪每次 Twitter 请求的成功/失败
- 计算成功率、连续失败次数、平均响应时间
- 自动检测严重错误 (账号被封、授权失败等)
- 15分钟滑动窗口分析

**检测标准**:
```python
健康状态 (4个级别):
- HEALTHY: 正常运行 (成功率 > 85%)
- DEGRADED: 性能下降 (成功率 70-85%)
- FAILING: 即将失效 (成功率 < 70% 或连续失败 > 5次)
- FAILED: 完全失效 (账号被封等严重错误)

严重错误关键词:
- 'suspended', 'banned', 'locked'
- 'authorization failed', 'forbidden'
- '403' 错误
```

**告警触发**:
- 状态变化时: `HEALTHY → DEGRADED → FAILING → FAILED`
- 严重错误时: 立即触发 `critical_failure` 告警
- 连续失败超过阈值: 自动触发账号轮换或切换到 Grok

---

### 2. 多账号轮换系统 (Account Pool)

**文件**: `twitter-service/account_pool.py`

**功能**:
- 管理多个 Twitter 账号池
- 基于健康评分智能选择账号
- 自动轮换到健康账号
- 追踪每个账号的使用情况和成功率

**账号健康评分** (0-1 分):
```python
健康分 = 成功率 × 0.5 + 使用频率分 × 0.3 + 休息时长分 × 0.2

优先选择:
- 成功率高的账号
- 使用次数少的账号 (分散负载)
- 休息时间长的账号 (避免被封)
```

**轮换触发条件**:
- 当前账号被封 (自动检测)
- 健康监控建议轮换 (性能下降)
- 手动触发轮换 (管理员操作)

**配置方法**:
```bash
# .env 文件添加多个账号:
TWITTER_USERNAME=account1
TWITTER_EMAIL=email1@example.com
TWITTER_PASSWORD=password1

TWITTER_USERNAME_2=account2
TWITTER_EMAIL_2=email2@example.com
TWITTER_PASSWORD_2=password2

TWITTER_USERNAME_3=account3
... (最多支持 9个账号)
```

---

### 3. Grok API 备用方案 (Fallback)

**文件**: `twitter-service/grok_client.py`

**功能**:
- 当所有 Twikit 账号失效时,自动切换到 Grok API
- 使用 xAI Grok 4.1 Fast + X Search 工具
- 官方 API,无被封风险

**自动切换条件**:
```python
立即切换到 Grok (任一条件满足):
1. 检测到 'suspended', 'banned' 等严重错误
2. 健康状态 = FAILED
3. 所有账号都被封禁
4. 连续失败 > 5次 且成功率 < 70%
```

**成本** (见之前分析):
- 当前: 免费 (临时促销)
- 正常: $13-121/月 (取决于使用量)

---

## 📊 告警系统

### 告警类型

**1. 严重告警 (Critical)**:
```
⛔ Twikit账号可能被封: AuthorizationError - forbidden
   → 立即切换到 Grok API
```

**2. 警告告警 (Warning)**:
```
⚠️ Twikit服务状态变差: HEALTHY → DEGRADED
   → 考虑账号轮换
```

**3. 信息告警 (Info)**:
```
✅ 已切换到Grok API备用方案. 原因: 账号被封
✅ 账号已轮换: @account1 → @account2
```

### 告警接收方式

**1. API 端点** - 实时查询告警:
```bash
# 获取最近20条告警
curl http://localhost:8001/api/alerts?limit=20

# 响应示例:
{
  "total_alerts": 15,
  "recent_alerts": [
    {
      "timestamp": "2025-12-19T10:30:00",
      "type": "twikit_degraded",
      "message": "⚠️ Twikit服务状态变差...",
      "health_status": "degraded",
      "using_grok": false
    }
  ]
}
```

**2. 日志输出** - 控制台实时显示:
```
2025-12-19 10:30:00 - WARNING - 🚨 ALERT: ⛔ Twikit账号可能被封...
2025-12-19 10:30:01 - INFO - 🔄 Switched to Grok API (reason: 账号被封)
```

**3. Node.js 主系统集成** (后续实现):
```javascript
// 轮询告警端点
setInterval(async () => {
  const response = await fetch('http://localhost:8001/api/alerts?limit=5');
  const data = await response.json();

  for (const alert of data.recent_alerts) {
    if (alert.type === 'twikit_critical_failure') {
      console.error('🚨 紧急: Twitter账号被封,已切换到Grok!');
      // 发送通知给你...
    }
  }
}, 60000); // 每分钟检查一次
```

---

## 🔧 使用指南

### 启动服务 (增强版)

```bash
cd twitter-service

# 1. 配置多个账号 (推荐至少3个)
cp .env.accounts.example .env
nano .env  # 填入多个Twitter账号和Grok API密钥

# 2. 安装依赖 (如果未安装)
pip install httpx  # Grok客户端需要

# 3. 启动增强版服务
python main_v2.py
```

**启动输出示例**:
```
🚀 Starting Twitter Service v2...
Added account @account1 to pool (total: 1)
Added account @account2 to pool (total: 2)
Added account @account3 to pool (total: 3)
Initializing 3 accounts...
✅ @account1 ready
✅ @account2 ready
✅ @account3 ready
🎯 Using @account1 as primary account
✅ Grok client initialized as fallback
✅ Twitter Service v2 ready!
```

### 监控服务状态

```bash
# 查看完整状态
curl http://localhost:8001/api/status

# 响应示例:
{
  "current_provider": "twikit",  // 当前使用的提供商
  "health": {
    "status": "healthy",
    "should_failover": false,
    "metrics": {
      "total_requests": 150,
      "success_rate": 98.67,
      "consecutive_failures": 0,
      "average_response_time_ms": 342
    },
    "recommendation": "✅ HEALTHY: Service operating normally."
  },
  "account_pool": {
    "total_accounts": 3,
    "active_accounts": 3,
    "banned_accounts": 0,
    "current_account": "account1",
    "accounts": [
      {
        "username": "account1",
        "active": true,
        "banned": false,
        "health_score": 0.95,
        "total_requests": 85,
        "success_rate": 98.82
      },
      {
        "username": "account2",
        "active": true,
        "banned": false,
        "health_score": 0.98,
        "total_requests": 45,
        "success_rate": 100.0
      }
    ]
  },
  "grok_available": true
}
```

### 手动控制

**手动切换到 Grok**:
```bash
curl -X POST http://localhost:8001/api/switch-provider -d '{"provider": "grok"}'
```

**手动轮换账号**:
```bash
curl -X POST http://localhost:8001/api/rotate-account
```

**切换回 Twikit**:
```bash
curl -X POST http://localhost:8001/api/switch-provider -d '{"provider": "twikit"}'
```

---

## 🎯 实际运行场景

### 场景 1: 正常运行

```
[10:00] 启动服务,使用 @account1
[10:01-11:00] 处理 50 个搜索请求
         → 成功率 100%
         → 健康状态: HEALTHY
         → 无告警
```

### 场景 2: 性能下降 → 自动轮换

```
[11:00] @account1 开始遇到速率限制
[11:05] 成功率降到 82%
         → 健康状态: HEALTHY → DEGRADED
         → 告警: "⚠️ Twikit服务状态变差"

[11:06] 自动轮换到 @account2
         → 重置健康监控
         → 成功率恢复 100%
```

### 场景 3: 账号被封 → 自动切换 Grok

```
[12:00] @account1 被 Twitter 封禁
[12:00] 请求返回: "Account suspended"
         → 严重错误检测
         → 告警: "⛔ Twikit账号可能被封"

[12:00] 自动切换到 @account2
[12:05] @account2 也被封 (可能IP被标记)
         → 所有 Twikit 账号失效

[12:05] 自动切换到 Grok API
         → 告警: "✅ 已切换到Grok API"
         → 服务继续稳定运行
```

---

## ⚡ 关键优势

### 1. **零宕机时间**
- Twikit 失效 → 立即切换 Grok
- 无需人工干预
- 服务持续可用

### 2. **成本优化**
- 优先使用免费的 Twikit
- 只在必要时使用付费的 Grok
- 智能账号轮换延长账号寿命

### 3. **主动监控**
- 实时健康检查
- 早期发现问题
- 告警及时通知

### 4. **灵活扩展**
- 支持任意数量账号
- 可添加更多备用方案
- 易于集成到主系统

---

## 📋 后续集成步骤

### 1. Node.js 主系统集成 (下一步)

在 `src/social/twitter-client.js` 中:
```javascript
class TwitterClient {
  constructor() {
    this.baseURL = 'http://localhost:8001';
    this.alertCheckInterval = null;
  }

  async startAlertMonitoring() {
    // 每分钟检查告警
    this.alertCheckInterval = setInterval(async () => {
      try {
        const response = await fetch(`${this.baseURL}/api/alerts?limit=10`);
        const data = await response.json();

        for (const alert of data.recent_alerts) {
          this.handleAlert(alert);
        }
      } catch (error) {
        console.error('Failed to check alerts:', error);
      }
    }, 60000);
  }

  handleAlert(alert) {
    if (alert.type === 'twikit_critical_failure') {
      console.error('🚨 CRITICAL: Twitter账号被封!');
      // 通知用户...
    } else if (alert.type === 'switched_to_grok') {
      console.warn('⚠️ 已切换到Grok API (成本变化)');
      // 通知用户成本变化...
    }
  }
}
```

### 2. 监控仪表板 (可选)

创建简单的 Web 界面显示:
- 当前提供商 (Twikit/Grok)
- 健康状态
- 账号使用情况
- 最近告警

---

## 🔐 安全建议

### 账号安全:
1. ✅ 使用小号/马甲账号
2. ✅ 准备至少 3 个账号轮换
3. ✅ 账号之间使用不同邮箱
4. ✅ 避免全新账号 (建议 > 3个月)
5. ✅ 账号有正常推文/互动历史
6. ⚠️ 考虑使用代理IP (高级)

### API 密钥安全:
1. ✅ 不要提交 `.env` 到 Git
2. ✅ Grok API 密钥妥善保管
3. ✅ 定期检查使用量和账单

---

## 📞 告警响应流程

当你看到告警时:

### ⛔ 严重告警 - 账号被封
```
收到告警: "Twikit账号可能被封"

自动处理:
✅ 系统已自动切换到备用账号或Grok
✅ 服务继续运行

你需要做:
1. 检查被封账号状态
2. 准备新的替代账号
3. 监控 Grok 使用成本 (如已切换)
```

### ⚠️ 警告告警 - 性能下降
```
收到告警: "服务状态变差"

自动处理:
✅ 系统会尝试轮换账号

你需要做:
1. 观察是否恢复
2. 如持续降级,考虑增加账号
```

### ✅ 信息告警 - 正常切换
```
收到告警: "已切换到Grok" 或 "账号已轮换"

自动处理:
✅ 系统正常运行

你需要做:
1. 知道当前使用的方案
2. 监控成本变化 (如使用Grok)
```

---

**总结**: 这套系统会自动监控、自动轮换、自动切换,几乎不需要人工干预。你只需要定期检查告警,确保系统健康运行即可!

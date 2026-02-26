#
cd /Users/boliu/sentiment-arbitrage-system
nohup node src/index.js > logs/system.log 2>&1 &

# 查看日志
tail -f logs/system.log

# 查看三级池状态
grep "TieredObserver" logs/system.log | tail -20

# 查看毕业情况
grep -E "通过毕业|延毕|GRADUATE" logs/system.log | tail -20

# 查看 AI 决策
grep "AI Analyst" logs/system.log | tail -20

# 查看极速捕获
grep "极速捕获\|早鸟" logs/system.log | tail -20

# 停止系统
pkill -f "node src/index"
```

---

*文档更新时间: 2026-01-09*
*系统版本: v7.4*

---

## 📜 版本历史

| 版本 | 日期 | 主要更新 |
|------|------|----------|
| **v7.4** | 2026-01-08 | 信号血统追踪 (Signal Lineage)、GMGNApiGateway 统一管理、内存泄漏修复、API 严格模式 |
| v7.3 | 2026-01-05 | 模块考核与自动调权、模块自动禁用机制、AI 叙事有效性评估 |
| v7.2 | 2026-01-03 | 精英占比因子 (Elite Ratio)、接盘惩罚 (Late Follower Penalty) |
| v7.1 | 2026-01-01 | 猎人表现闭环追踪、信号源动态权重、自动调参模块 |
| v7.0 | 2025-12-31 | 统一 API 重试工具、会话自动管理、TURTLE 延迟跟单修复、Ultra Human Sniper v2 |
| v6.5 | 2025-12-29 | 仓位容量管理：6普通+2VIP架构、DB实时查询、容量预审、银池直通车、观察池扩容至150 |
| v6.4 | 2025-12-29 | 极速捕获补丁：SM阈值1、流动性阈值$3K、早鸟奖励、AI提示更新 |
| v6.3 | 2025-12-28 | 毕业答辩机制、动态因子标签、腾笼换鸟 |
| v6.2 | 2025-12-27 | 三级观察池、晋级/降级机制 |
| v6.0 | 2025-12-25 | 架构重构、动态评分引擎 |


# Goal — Capture-60 Moonshot(2026-07-03,基于 capture-60-deep-audit-2026-07-02 事实基线)

你是我的精英战略家和系统架构师。

我的最大目标是(比常规目标高 100 倍):
**12 个月内,把 sentiment-arbitrage-system 从"影子模式的 discovery 评估器"进化为全自主复利的链上猎狗有机体:每 24 小时捕获 ≥60% 的金银狗(gold=2h 内持续峰值≥+100%,silver≥+50%),实盘执行胜率 ≥60%,被捕获交易的平均实现收益 ≥200%,并以复利把种子资金翻 100 倍(≈日复合 +1.27%);系统无人值守自我进化——审计→假设→OOS→晋升提案→经验沉淀全自动,人类只保留风险开关。**
(常规目标只是"paper 验证一个能小额盈利的策略"。)

当前情况(2026-07-02 审计验证,勿重新推导):
- 24h 漏斗 70→41→30→23→3→0:decision 缺口 93% 是日志缺口(桥接后 97.1%);真悬崖是 pending→final(真条件概率 2/23=8.7%,stale_before_final 占 47.6%);paper 0% 是纯政策(A_CLASS 熔断后无恢复代码路径,SHADOW 已 21 天)。
- top blocker(volume/kline coverage)是认知型:运行时无消费者,数据 92–98% 实际存在(observer 刷新 bug + 30s lag 阈值工件)。
- 19 个 frozen 2D hits 无统计控制(~半数为零假设期望内噪声,幸存者全是自交叉);OOS 时钟会被任何定义变更重置。
- 0 笔真实成交:胜率与收益率**从未被测量过**;reason 级 dud 分母、reject 时间戳、would-be PnL 均不存在。
- 感知层单点依赖:唯一信号源是一个 Telegram 私有频道;日均 raw 金银狗 ~25 个唯一 token(样本饥饿)。
- 已就位:AGENTS.md、skills/(5 个含经验账本)、Codex 任务队列 P0–P6(claudedocs/capture-60-deep-audit-2026-07-02.md §8)。

关键约束:
- 治理护栏:discovery-only,不改策略/gate/executor/风险;晋升永远人批;canary 0.001–0.003 SOL、1 并发、日损失预算 0.005 SOL、-20% 单笔损失合同。
- 三个目标存在内在张力:捕获率(recall)↑ 通常压低精确率(胜率),200% 平均收益要求骑尾部退出、又会压低胜率——每阶段只准一个主优化指标。
- 样本量:60% 胜率的统计确认需 ≥150 笔;满捕获也只有 ~25 笔/天 → 任何结论天然是周级。
- 单容器 Zeabur、AutoLoop 无调度器、本地 checkout 常落后 origin/main。

请你从这个目标倒推完整路径,帮我设计一套可执行的战略方案。
在给方案前,先做一次事前推断:假设 1–2 年后这个目标失败了,最可能的原因是什么?这些风险应该如何提前规避?
然后输出:1. 总体战略路径 2. 阶段性里程碑 3. 关键系统与资源配置 4. 最大风险与应对方式 5. 本周最高杠杆行动 6. 3 个能进一步优化计划的问题

---

## 已定决策(2026-07-03,用户确认)

1. **收益口径**:rolling 24h realized net ROI on allocated strategy risk capital = 200%;**最大回撤硬约束 15%**(对 allocated risk capital)。→ exit-policy lab 目标函数:max rolling-24h realized ROI s.t. maxDD ≤ 15%;隐含仓位约束:单笔资本风险 ≤~2%(-20% 止损 × ≤10% 仓位),7 连败 ≈ -14% 仍在预算内。
2. **第二感知源路线**:pump.fun/链上实时流(主感知)→ GMGN(市场确认)→ X narrative(叙事层)→ smart money(precision layer)。
3. 熔断恢复 SLA:见 claude 提案(分级恢复:paper 自动、LIVE 人批、fail-closed 默认、48h 响应 SLA),待用户最终确认参数。

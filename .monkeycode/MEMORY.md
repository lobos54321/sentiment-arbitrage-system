# 用户指令记忆

本文件记录了用户的指令、偏好和教导，用于在未来的交互中提供参考。

## 格式

### 用户指令条目
用户指令条目应遵循以下格式：

[用户指令摘要]
- Date: [YYYY-MM-DD]
- Context: [提及的场景或时间]
- Instructions:
  - [用户教导或指示的内容，逐行描述]

### 项目知识条目
Agent 在任务执行过程中发现的条目应遵循以下格式：

[项目知识摘要]
- Date: [YYYY-MM-DD]
- Context: Agent 在执行 [具体任务描述] 时发现
- Category: [代码结构|代码模式|代码生成|构建方法|测试方法|依赖关系|环境配置]
- Instructions:
  - [具体的知识点，逐行描述]

## 去重策略
- 添加新条目前，检查是否存在相似或相同的指令
- 若发现重复，跳过新条目或与已有条目合并
- 合并时，更新上下文或日期信息
- 这有助于避免冗余条目，保持记忆文件整洁

## 条目

[系统结构与测试基线]
- Date: 2026-04-28
- Context: Agent 在执行系统快速体检时发现
- Category: 代码结构|测试方法|环境配置
- Instructions:
  - 项目是 Node.js ESM 加密交易/信号系统，主入口为 `src/index.js`，`npm start` 默认执行 `node src/index.js --premium`。
  - 当前 `npm test` 依赖 Jest，但工作区未安装依赖时会因 `jest: not found` 失败；可直接运行 `node tests/test-retry-limits.js` 验证现有 retry/fee 保护测试脚本。
  - 仪表盘服务入口在 `src/web/dashboard-server.js`，敏感 API 依赖 `DASHBOARD_TOKEN` 认证。

[当前重点关注 Paper Trader]
- Date: 2026-04-28
- Context: 用户说明当前主要关注 paper trader
- Instructions:
  - 后续系统排查和改动应优先围绕 paper trader 链路，而不是实盘执行链路。

[Paper Trader 线上状态来源]
- Date: 2026-04-28
- Context: 用户纠正本地 DB 不是当前线上运行状态，提供 Zeabur 日志接口
- Instructions:
  - 判断当前 paper trader 是否正常运行时，应优先查看线上 Zeabur 日志/API，而不是只依据 workspace 内的 `server_paper.db` 或 `server_sentiment_arb.db` 快照。
  - 本地 DB 可用于离线分析历史样本，但不能直接代表线上实时信号 freshness 或 paper trader 当前状态。

[Paper Trader 健康检查脚本]
- Date: 2026-04-28
- Context: Agent 在执行线上 paper trader 风险排查时发现
- Category: 测试方法|环境配置
- Instructions:
  - `scripts/paper_healthcheck.py` 可读取线上日志 URL 或本地日志文件，汇总 paper trader 启动、heartbeat、watchlist、entry/exit、PnL、401、429、quote/no-route、`UNKNOWN_DATA continue=1` 等风险。
  - 单独分析 paper-trader 日志可确认 paper 进程状态；同时分析主运行日志和 paper-trader 日志可发现 prebuy Kline/RPC/API 失败后仍继续交易路径的风险。

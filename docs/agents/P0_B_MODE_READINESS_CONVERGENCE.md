# P0-B｜Mode Readiness 契约收敛实施说明

Plan ID: `SAS-P0-B-MODE-READINESS-2026-08-08`

Status: `IMPLEMENTED_LOCALLY_PENDING_DEPLOYMENT_VALIDATION`

Parent plan: `docs/agents/SENTIMENT_ARBITRAGE_SYSTEM_RECOVERY_MASTER_PLAN.md`

Baseline commit: `f592c47a137871c4dd70911a4c6d783297c15395`

Implementation base: P0-A local diff in isolated worktree

Promotion allowed: `false`

Production strategy change allowed: `false`

Automatic runtime mode change allowed: `false`

---

## 1｜目标

P0-B 的目标不是把 `normal_tiny_ready` 或 `ultra_tiny` 强制设为绿色，而是让 Mode Readiness 的基础契约重新成为可信事实：

1. 静态验证器理解真实的认证、锁、数据库与进程语义；
2. 已存在但锚点变化的能力重新与治理配置对齐；
3. 新增真实写路径、worker、feature flag 和错误码被显式登记；
4. 每个剩余 blocker 都代表真实、可解释、不可绕过的未满足证据；
5. `highest_allowed_mode` 仍只由事件证据、read model 与契约矩阵计算产生。

P0-B 不负责：

- 修改任何 candidate、entry、exit 或 sizing 策略；
- 更改 Hard Gate、SmartEntry、Final Entry Contract；
- 开启 live execution；
- 自动批准 A_CLASS 或 normal tiny；
- 处理 PR #70 的 evaluator snapshot 性能问题；
- 清理生产数据库或凭据历史。

---

## 2｜基线

在 P0-A 本地实现之上运行 `build_basic_contract_readiness()`：

- basic blockers：`15`；
- `highest_allowed_mode=null`；
- observe-only 被 basic blockers 与缺少事件证据共同阻止；
- `test_v27_mode_readiness.py`：2 个失败；
  - `AccessControlContract=missing_evidence`；
  - seeded ultra-tiny 场景仍被 basic blockers 阻止。

当前 15 个 basic blocker：

1. `RuntimeConfigDriftContract`
2. `AccessControlContract`
3. `WritePathRegistryContract`
4. `ConnectionPoolPartitionContract`
5. `DBLockContentionPolicy`
6. `DatabaseTransactionIsolationContract`
7. `DistributedLockBackendHealthContract`
8. `BackgroundJobRegistryContract`
9. `EntryPointInventoryContract`
10. `StaticPolicyEnforcementContract`
11. `FeatureFlagDependencyContract`
12. `ErrorTaxonomyContract`
13. `ThreadPoolIsolationContract`
14. `SpecChangeImpactAnalysisContract`
15. `ServiceReadinessProbeContract`

---

## 3｜分类与处置表

| Contract | 当前证据 | 分类 | 处置 |
|---|---|---|---|
| RuntimeConfigDriftContract | route registry 与 runtime hash 仍锚定旧文件 | registry drift | 在其他治理文件收敛后重新计算组件 hash 与总 hash |
| AccessControlContract | 14 条路由被报为未鉴权；实际由共享 helper 或同一 alias block 鉴权 | verifier false positive | 修 route block/alias/helper 语义；增加回归测试，不重复加鉴权 |
| WritePathRegistryContract | runtime event log、child log、raw outcome/bar/observation 写入未登记 | real registry gap | 登记真实写路径；避免把同一 UPSERT 的 `ON CONFLICT` 行重复当作新写路径 |
| ConnectionPoolPartitionContract | paper monitor 旧 busy-timeout/lock 锚点 | registry drift | 指向 `configure_paper_sqlite_connection` 与带 timeout 的 `sqlite_single_writer` |
| DBLockContentionPolicy | 同上 | registry drift | 更新真实 lock anchor，保持单写者约束 |
| DatabaseTransactionIsolationContract | 同上 | registry drift | 更新 source anchor，不改事务行为 |
| DistributedLockBackendHealthContract | 同上 | registry drift | 更新 source anchors，不降低 backend 要求 |
| BackgroundJobRegistryContract | premium Node 加入 paper-mode preload；P0-A 新增 read-model worker | registry gap | 更新 Node 启动锚点并登记持续 read-model worker |
| EntryPointInventoryContract | 旧记录为 64/58；当前逻辑路由为 103、受保护路由为 98；read-model launcher 仍只指手动 route | registry drift | 更新计数并记录 always-on launcher |
| StaticPolicyEnforcementContract | SQLite `db.exec()` 被 shell-exec regex 命中 | verifier false positive | shell policy只匹配进程执行 API，不豁免 `exec()` shell 调用；SQLite方法不再误判 |
| FeatureFlagDependencyContract | 15 个 runtime/supervisor flags 未登记 | registry gap | 逐项登记依赖、默认值和 mode scope |
| ErrorTaxonomyContract | 13 个新增错误码未分类 | registry gap | 添加稳定、唯一、可行动分类，不删除现有码 |
| ThreadPoolIsolationContract | paper-fast pool 从 `+3` 改为 `+4` | registry drift | 更新 max_workers 与 source anchor |
| SpecChangeImpactAnalysisContract | 59 个 source hash 与 impact hash 过期 | registry drift | 在最终代码/配置稳定后重算 source hashes 与 impact hash |
| ServiceReadinessProbeContract | public-health 和 supervisor PID 文本锚点过期；缺少 P0-A worker probe | registry gap + drift | 更新真实 health dependency anchor、PID anchor并登记 read-model worker health |

---

## 4｜实施顺序

顺序按依赖执行：

```text
A. verifier semantics
   AccessControl / StaticPolicy / Write-path scan grouping
        ↓
B. concrete registries
   Write paths / concurrency / jobs / entry points / flags / errors / pools / probes
        ↓
C. derived hashes
   RuntimeConfigDrift / SpecChangeImpact
        ↓
D. full readiness verification
```

原因：hash 必须最后更新，否则前面每次改治理文件都会再次造成 drift。

---

## 5｜不可变边界

本包不得修改：

- `config/strategy-goal.yaml`；
- `config/paper-strategy-registry.json`；
- `config/entry-mode-registry.json` 的策略语义；
- `scripts/final_entry_contract.py`；
- `scripts/entry_engine.py`；
- `scripts/exit_engine.py`；
- `scripts/lotto_engine.py`；
- wallet/private-key/live-executor 设置；
- canary size、concurrency、loss budget；
- OOS freeze definitions；
- `promotion_allowed=false`。

允许修改范围：

- readiness verifier；
- governance registries/policies；
-相关测试与本实施文档；
- P0-A 观测代码仅可作兼容性补充，不改变其运行目的。

---

## 6｜验收

必须同时满足：

1. `build_basic_contract_readiness().blocking_contracts == []`；
2. `test_v27_mode_readiness.py` 全绿；
3. seeded ultra-tiny test 由真实证据成为 `allowed`，不是强制赋值；
4. 缺事件、缺 read model 或 contract 失败的场景继续 fail-closed；
5. AccessControl 回归测试证明：
   - helper-auth download routes 被识别；
   - alias routes 继承同一 block 的鉴权；
   - 真正移除鉴权时 verifier 会失败；
6. StaticPolicy 回归测试证明：
   - SQLite `db.exec()` 合法；
   - `child_process.exec()` 或直接 shell exec 仍失败；
7. WritePath 回归测试证明所有直接写入都有唯一 registry binding；
8. feature flags、error taxonomy、jobs、routes 与当前代码一致；
9. runtime config 与 spec impact hash 可重现；
10. 禁止路径 diff 为空。

---

## 7｜部署边界

P0-B 只作为独立本地 diff 完成和验证。本包不执行：

- commit/push；
- production deployment；
- mode re-enable；
- paper candidate 注入；
- P0-C evaluator snapshot 恢复。

部署后仍应预期：

- worker 可以正常生成 readiness matrix；
- matrix 可能因实时事件证据缺失继续阻止某些模式；
- 这种阻止是正确结果，不是 P0-B 失败。

---

## 8｜实施 Ledger

### 2026-08-08｜基线分类完成

- 15 个 basic blocker 已逐项分类；
- 未发现需要修改策略或风险才能解决的 basic blocker；
- AccessControl 与 StaticPolicy 确认为 verifier 语义问题；
- WritePath、feature flags、error taxonomy、read-model worker 确认为真实 registry gap；
- 其余主要为 source anchor、计数或 hash 漂移；
- 实施尚在进行；
- `promotion_allowed=false`。

### 2026-08-08｜P0-B 本地实施完成

- basic readiness 从 `15` 个 blocker 收敛为 `0`；
- `build_basic_contract_readiness().health.status=basic_contract_readiness_ok`；
- `observe_only_foundation_ready=true`；
- `normal_tiny_ready=false`，未被本包强制放行；
- 136 个基础契约全部通过；
- AccessControl：103 个唯一逻辑路由、5 个 public、98 个 protected，`unauthenticated_routes=[]`；
- 新增验证器语义：多行 route alias、fail-closed helper delegation、受保护外层 route 内部的重复分支；
- 新增反例：同一 endpoint 若另有未保护的独立入口，AccessControl 仍失败；
- WritePath：扫描到 14 个直接写入，registry 也为 14，未登记数为 0；
- SQLite 写路径分为 7 个 authenticated break-glass 路径与 3 个 internal-observability 路径；
- internal-observability 仅允许写入 `sqlite:raw_signal_outcomes`、仅允许 `observe_only`、禁止 HTTP entry point；
- StaticPolicy：SQLite `Database.exec` 不再被误判，直接 `exec(...)` / child-process shell exec 仍被阻止；
- 并发策略的 paper SQLite connection、single-writer 与 busy-timeout 锚点已对齐真实实现；
- background jobs：10 个，包含具有 status artifact、lock file 和 restart delay 的 V27 read-model Node sidecar；
- EntryPoint inventory：32 个入口，Dashboard 103/98，read-model launcher 指向 always-on `startV27ReadModelRefreshWorker()`；
- feature flags：40/40 全覆盖；
- error taxonomy：250/250 全覆盖；
- service readiness probes：7 个，新增 read-model worker 探针；
- runtime-config component hash、runtime hash、59-file spec-impact source hashes 与 impact hash 已重新计算并通过验证；
- Python 基础契约测试：108 passed；
- Mode/read-model/runtime-gate 测试：53 passed；
- 更宽 Python 安全、retention、snapshot 与 A_CLASS read-only 回归：210 passed；
- Node 20 / ABI 115 聚焦回归：77 passed；
- 静态检查、JSON 校验、`git diff --check` 与 strict basic readiness 均通过；
- 未修改 strategy、entry/exit gates、Final Entry Contract、A_CLASS 参数、canary、wallet、live executor 或 risk limits；
- 独立 Codex 最终反方审查已尝试，但本机 Codex 使用额度耗尽，未生成审查结论；因此不把 P0-A 的早期审查或本轮自测误报为 P0-B 的独立最终批准；
- Claude Code 仍因本机未登录而不可用；
- 未 commit、未 push、未部署；
- `promotion_allowed=false`。

# P0-C｜Accepted Frozen Evaluator Snapshot → AutoLoop 恢复实施说明

Plan ID: `SAS-P0-C-FROZEN-SNAPSHOT-AUTOLOOP-2026-08-08`

Status: `IMPLEMENTED_LOCALLY_PENDING_PRODUCTION_ACCEPTED_SNAPSHOT_VALIDATION`

Parent plan: `docs/agents/SENTIMENT_ARBITRAGE_SYSTEM_RECOVERY_MASTER_PLAN.md`

Baseline commit: `f592c47a137871c4dd70911a4c6d783297c15395`

Implementation base: P0-A + P0-B local diff in isolated worktree

Reference branch: `origin/loop/a3-v23-indexed-time-20260804-a`

Reference commits:

- `63dba45` — indexed A3 snapshot time ranges;
- `49a8af5` — indexed selection regression tests;
- `b9051fa` — evaluator-contract fixture indexes;
- `ad91a42` — A3 v2.3 contract documentation.

Deployment status: `NOT_DEPLOYED`

Promotion allowed: `false`

Strategy change allowed: `false`

Automatic paper/live mode change allowed: `false`

---

## 1｜目标

P0-C 的目标是恢复以下只读研究闭环：

```text
four active source databases
        ↓ coordinated pinned read views
bounded selective extraction
        ↓
accepted immutable four-DB snapshot + manifest
        ↓ authoritative evaluator bundle contract
AutoLoop / OOS refresh / dashboard-triggered discovery
        ↓
read-only research artifacts
```

成功不等于“snapshot worker 进程存在”，而是必须同时证明：

1. snapshot producer 能在 read-lock 预算内完成；
2. `current/manifest.json` 的 `accepted=true` 来自真实验证；
3. 四个 SQLite 文件的 SHA、`quick_check`、watermark、共同 upper bound、大小和 selection evidence 完整；
4. AutoLoop 只读取 snapshot，不读取 active DB；
5. AutoLoop derived writes 只进入 per-run research DB；
6. promotion、strategy change、paper enablement 和 live enablement 始终为 false。

---

## 2｜已确认根因

A3 v2.2 对所有可能的时间格式使用通用标准化表达式：

```sql
CASE
  WHEN typeof(observed_at) ...
  ...
END >= ?
```

对 `candidate_shadow_observations` 与 `candidate_shadow_virtual_trades` 这两张高频表，该表达式包裹了已经建立索引的 `observed_at`，使 SQLite 无法执行原生范围查找。

生产失败证据显示：

- candidate observation 约 `6.35M` 行；
- read view 被持有接近 `300s`；
- progress handler 按契约中断；
- partial output 被清除；
- 旧 `current` 未被替换；
- AutoLoop 正确返回 `blocked_evaluator_snapshot_required`。

因此主矛盾不是 AutoLoop 算法，也不是策略候选，而是 snapshot extraction 的索引可达性。

---

## 3｜实施范围

### 3.1 A3 v2.3 index-aware selection

对以下表显式声明 `observed_at` 为 Unix epoch seconds 的索引锚点：

- `candidate_shadow_observations`；
- `candidate_shadow_virtual_trades`。

Producer 必须：

- 验证锚点属于注册的 time columns；
- 验证锚点声明为 epoch seconds；
- 验证列的 SQLite declared type 为 numeric；
- 验证存在非 partial index 且首列为 `observed_at`；
- 使用裸范围 predicate：`observed_at >= ?`；
- 通过 `INDEXED BY` 强制使用已验证 index；
- 对其他 secondary timestamps 保留 seconds/milliseconds/ISO 通用 upper-bound 校验；
- 缺列、类型不符或缺索引时 fail-closed。

Manifest 对每张选中表记录：

- `predicate_strategy`；
- `indexed_time_anchor`；
- `source_index_name`；
- `source_index_columns` 与 partial-index 状态；
- 实际 `EXPLAIN QUERY PLAN` 文本；
- 是否使用声明 index；
- 是否执行范围 `SEARCH`；
- 是否出现 candidate table full scan。

Producer 在复制前运行实际 query-plan 验证。即使索引名存在，只要 SQLite 没有形成声明 index 上的范围查找，snapshot 仍然 fail-closed。

### 3.2 Producer 状态与 Dashboard 健康

新增 public-safe evaluator snapshot health，至少包含：

- worker configured / running / PID liveness；
- status artifact 是否存在及 schema；
- last attempt / success / failure；
- last reject reason；
- current manifest 是否存在；
- `accepted`、snapshot id、age；
- cross-DB skew；
- source read-lock duration与预算；
- output size/cap；
- disk preflight reserve；
- four DB quick-check 汇总；
- selection strategy与 source index evidence；
- AutoLoop authoritative preflight 是否 accepted。

不得暴露 token、钱包、数据库行或 per-token 研究数据。

### 3.3 AutoLoop 启动边界

所有入口继续调用 authoritative Python contract：

- dashboard manual run；
- dashboard scheduler；
- dedicated discovery worker；
- staged OOS refresh worker；
- AutoLoop stage runner。

任何以下情况必须继续阻止：

- manifest missing / malformed / stale；
- accepted false；
- active DB path、symlink 或 hard-link alias；
- SHA 或 quick-check 不符；
- watermark/selection evidence 不完整；
- common upper bound 不一致；
- output cap、read-lock、skew 或 partial-artifact contract 失败；
- producer/consumer lock lease 无法取得。

---

## 4｜明确不做

本包不得修改：

- candidate catalog；
- strategy、entry、exit、sizing；
- Hard Gate、SmartEntry、Final Entry Contract；
- A_CLASS 状态或预算；
- wallet、private key、live executor；
- paper/live mode enablement；
- OOS promotion threshold；
- `promotion_allowed=false`；
- active production DB 内容；
- retention policy。

---

## 5｜验收矩阵

### Producer

- indexed selection 的 `EXPLAIN QUERY PLAN` 显示 `SEARCH ... observed_at>?`；
- 不能出现 high-volume candidate table full scan；
- 缺 index 时 fail-closed；
- numeric anchor + mixed secondary timestamps 保持正确 future-row exclusion；
- 300 秒 source read-lock ceiling 不变；
- partial run 不替换 `current`；
- accepted bundle 原子发布。

### Consumer

- active DB 被拒绝；
- tampered manifest/SHA/quick-check 被拒绝；
- stale snapshot 被拒绝；
- shared lease 期间 snapshot 不可被 producer prune；
- evaluator 后置重验可检测 frozen DB mutation；
- derived rows 只写 `autoloop_research.db`。

### Runtime / observability

- `/health` 能区分：disabled、starting、failed、producer_accepted、stale、contract_blocked、worker_not_running；
- last reject reason 可见但不泄露敏感数据；
- accepted snapshot 后 authoritative preflight 为 accepted；
- AutoLoop run provenance 记录 snapshot id 和 manifest；
- AutoLoop verdict 仍显示 promotion false。

### Tests

- `tests/test_cross_db_evaluator_snapshot.py`；
- `tests/test_evaluator_db_contract.py`；
- `tests/test_autoloop_evaluator_snapshot_provenance.py`；
- `tests/test_autoloop_foundation_fail_closed.py`；
- `tests/test_autoloop_research_db_isolation.py`；
- `tests/research-autoloop-recovery-config.test.mjs`；
- `tests/dashboard-agent-evaluator-db-source.test.mjs`；
- P0-A/P0-B focused regression；
- syntax、JSON、`git diff --check`。

---

## 6｜部署后验证

### 0–5 分钟

- 新 commit 生效；
- evaluator worker PID alive；
- status 进入 running/refreshing；
- 无 duplicate producer；
- active DB health 不受影响。

### 首次 snapshot 完成

- `current` 指向新 immutable snapshot；
- manifest `accepted=true`；
- candidate tables 记录 `indexed_epoch_seconds`；
- source index 为 `observed_at` index；
- paper source read lock 在预算内；
- four DB quick-check 为 ok；
- output cap、reserve、skew 全部通过。

### AutoLoop

- preflight accepted；
- run 读取 snapshot paths；
- fresh primary capture 生成；
- runner status 包含 snapshot id；
- OOS lineage 不被 same-window run 改写；
- promotion remains false。

---

## 7｜回滚

1. 设置 `EVALUATOR_SNAPSHOT_WORKER_ENABLED=false`；
2. 回滚独立 P0-C commit；
3. 保留最后一份 accepted snapshot 与 failed status evidence；
4. AutoLoop 因缺少 fresh accepted snapshot继续 fail-closed；
5. 不回退到 active DB 读取。

---

## 8｜实施 Ledger

### 2026-08-08｜文档基线

- 根因锁定为 high-volume `observed_at` 索引不可达；
- 本机远端存在完整 v2.3 reference branch；
- reference diff 仅涉及 producer、producer tests、consumer fixtures和 A3 文档；
- 当前 AutoLoop consumer contract 已经是 authoritative fail-closed；
- 本轮将补 producer v2.3、运行健康与完整闭环验证；
- 实施尚在进行；
- 未部署；
- `promotion_allowed=false`。

### 2026-08-08｜P0-C 本地实现完成，等待生产首次 accepted snapshot

Producer 与 immutable bundle：

- 合入并复核 A3 v2.3 `observed_at` index-aware selection；
- 两张高频 candidate table 必须使用首列为 `observed_at` 的非 partial numeric index；
- 复制前实际运行 `EXPLAIN QUERY PLAN`，必须出现声明 index 上的范围 `SEARCH`，禁止 full table scan；
- manifest 持久化 index columns、query plan、range-search 与 full-scan evidence；
- 缺索引、改列类型、改 index columns、改 query plan 或伪造 full-scan 标志均被 consumer contract 拒绝；
- producer status 原子记录 snapshot-specific manifest path、manifest SHA-256、last success/failure、failure code 与最后 accepted summary；
- producer acceptance status、snapshot id、snapshot-specific manifest path、producer manifest SHA、disk preflight、output cap、source read-lock、skew、watermark、quick-check、DB SHA 和 partial-artifact contract 均纳入 authoritative consumer 验证。

Runtime owner 与健康：

- production shell 成为 evaluator snapshot worker 的单一 owner；
- production Node child 显式设置 `EVALUATOR_SNAPSHOT_WORKER_ENABLED=false`，避免双 producer；
- shell 增加 TERM/INT 转发与 supervised restart loop；
- Node sidecar 保留为替代拓扑，但与 shell 参数保持 output cap、history window、busy timeout、read-lock ceiling 一致；
- `/health.evaluator_snapshot_worker` 区分 `disabled`、`starting`、`producer_accepted`、`failed`、`stale`、`contract_blocked`、`worker_not_running`；
- `producer_accepted` 只代表 producer status、manifest SHA anchor、snapshot 文件存在/大小、时间与轻量契约一致，不代表四库 SHA 已在该 health 请求中重新计算；
- health 只读取小型 status/manifest/lock 与四库 file stat，不在 liveness 请求中重跑大文件 SHA 或 SQLite quick-check；
- `consumer_ready=true` 只在最近一次 authoritative Python preflight 对同一 snapshot id、manifest SHA 与 producer SHA 完整通过且仍在 freshness window 内时成立；否则只报告 `authoritative_preflight_required`；
- producer 失败与 consumer readiness 分开表达：上一份 bundle 仍 fresh 且最近 authoritative preflight 匹配时，producer 可以 degraded，而 consumer 仍可安全读取；
- future snapshot timestamp 超过 60 秒、任一 snapshot DB 缺失或大小不符均 `contract_blocked`；
- producer status 中的 snapshot id、snapshot-specific manifest path 与 manifest SHA 必须同时与 consumer 当前 bundle 匹配。

Accepted snapshot → AutoLoop 血缘：

- Dashboard preflight 继续委托 authoritative Python contract；
- accepted response 必须包含 snapshot id、snapshot timestamp、snapshot-specific manifest path 与 64 位 manifest SHA；
- Dashboard 将 Python 返回的 snapshot-specific DB paths 传给 child，不再只依赖可切换的 `current` alias；
- child 在 shared lease 内重新验证 bundle，producer/consumer 竞态只能 fail-closed，不能混用两代 DB；
- discovery loop、stage runner 与 OOS refresh status 均物化 `evaluator_snapshot_provenance.v1`；
- provenance 记录 snapshot id、manifest SHA、DB SHA/quick-check，但不包含 token rows；
- required provenance 缺失、被拒或缺 manifest SHA 时，stage runner 直接返回/抛出 `evaluator_snapshot_provenance_missing_or_rejected`；
- runner/verdict snapshot id 与 manifest SHA 可在 Dashboard 中核对 lineage；
- promotion、strategy change、automatic runtime change、paper enablement 始终为 false。

治理与 CI：

- evaluator producer 已登记到 Background Job Registry、Entry Point Inventory、Runtime Worker Health Policy 与 Service Readiness Probes；
- v27-readiness CI 新增 Node 20 setup、`npm ci`、producer/consumer/AutoLoop 编译、JSON、JavaScript syntax、Python contract tests 与 Node behavior tests；
- `CICDMergeGateContract`、`SpecChangeImpactAnalysisContract` 和 Basic Readiness 已重新计算并通过；
- 当前 Basic Readiness：`136/136 pass`、`blocking_contracts=[]`；
- 最终 Python 宽回归：`255 passed`；
- 最终 Node 20 / ABI 115 宽回归：`79 passed`；
- static、Bash/Node/Python syntax、JSON、generated client、spec validate、strict readiness、mode-gate scope 与 `git diff --check` 全通过；
- 未修改 strategy、candidate、entry、exit、risk、wallet、live executor、OOS/FDR 或 promotion policy。

独立反方审查：

- 第一轮独立 Codex verifier 给出 `REJECT`，明确指出三项缺口：
  1. authoritative consumer 计算 manifest SHA，但未与 producer `last_accepted_snapshot` 绑定；
  2. public health 过度把 manifest 声明当成 authoritative consumer acceptance，且未检查 snapshot 文件存在/大小/future timestamp；
  3. v27-readiness workflow 只做 JavaScript syntax，没有执行 Node behavior tests。
- 三项均已修复并增加正反例：
  - 缺 producer status、producer snapshot/path/SHA 不匹配、manifest 自洽重写但 producer SHA 未更新均拒绝；
  - health 改为 `producer_accepted`，snapshot 文件存在/大小/future timestamp 纳入轻量契约，`consumer_ready` 只来自最近匹配的 authoritative Python preflight；
  - GitHub Actions 固定 Node 20、执行 `npm ci`，并运行四个 evaluator/AutoLoop Node behavior test 文件；
- 第二轮窄范围独立 Codex verifier 给出 `APPROVE`：三项原始 finding 全部关闭，未发现 active-DB fallback、promotion/mode/paper enablement 绕过或新的代码阻断项；
- reviewer 的 read-only sandbox 无可写 temp，无法自行重跑完整 Python fixture，且默认 Node 22 与本项目 ABI 115 不一致；本实施工作区已经独立完成 Python `255 passed` 与 Node 20 `79 passed`，CI 也已固定 Node 20；
- reviewer 仍建议生产部署前进行人工复核和真实 25GB+ 数据运行时验收。

### 2026-08-08｜首轮生产验收与 retry/diagnostics 热修

- 原子发布 SHA `d06b6e819eb68c5d5aaf8ad7e2834e769cdad44d` 已由 Zeabur 成功部署；public health commit 与 release SHA 一致；
- production paper DB 约 25.58GB，integrity marker 不存在，read-model worker 持续刷新且 `error_count=0`；
- 首次 evaluator snapshot 在约 306 秒后以 `concurrent_evaluator_snapshot_failed` fail-closed；没有发布 partial/current manifest，AutoLoop 没有回退到 active DB；
- 发现 continuous producer 失败后仍按成功 cadence 休眠 21600 秒，外层 supervisor 因进程仍存活而不会短重试；
- retry/diagnostics 热修保持成功 cadence 21600 秒；连续失败采用 60 秒、900 秒、3600 秒、随后至少 21600 秒的有界退避，不提高 300 秒 source read-lock ceiling，也不允许配置把首次失败重试压低到 60 秒以下；
- duplicate producer 的 lock contention 直接使用至少 21600 秒 cadence，不形成竞争 retry loop；
- concurrent failure 新增 public-safe 结构化诊断，只公开 allowlisted 数据库角色、稳定 error code/type 与执行 stage；路径、SQL、行数据、secret 或未知 stage 即使进入允许字段也会被替换为安全默认值；
- `source_read_lock_budget_exceeded` 等组件级失败在单一根因时成为顶层 failure code，避免被泛化为不可行动的 concurrent failure；
- P0-D paper E2E 继续锁定；strategy、gates、mode、paper/live enablement 和 promotion policy 未改变；
- retry/diagnostics 第一轮独立 verifier 因无限 60 秒重试和未 allowlist 的 public strings 给出 `REJECT`；完成有界退避与 adversarial sanitization 后，第二轮 verifier 给出 `APPROVE`。

### 2026-08-08｜生产根因收敛：indexed source watermark

- retry/diagnostics SHA `97eeb01833d4647e59fc2d9017cfa85703bd9883` 已部署；首次 300 秒失败被精确归因为 `paper / source_metadata / source_read_lock_budget_exceeded`；
- 第二次 source inspection 锁竞争在约 30 秒 fail-closed，并真实进入 900 秒第二级退避，证明不是无限 60 秒 retry loop；
- 根因是 source metadata 在 25.58GB paper DB 的高频 candidate 表上以单条多列 `MAX(signal_id, signal_ts, observed_at)` 聚合扫描，发生在 pinned source read view 内；
- 修复不增加 300 秒 ceiling、不缩短/改写 snapshot window，也不跳过 watermark：source phase 对两张 candidate 表只用已验证 non-partial leading `observed_at` index 执行 `MAX(observed_at)`，并运行 `EXPLAIN QUERY PLAN`；
- manifest 新增 `source_watermark_query_evidence`，记录 strategy、column、source index、query plan、index usage 与 full-scan 标志；
- consumer 要求 indexed watermark 与 indexed selection 使用同一 index，任何 aggregate scan、index mismatch、伪造 plan 或 full scan evidence 均 fail-closed；
- source lock 释放后，冻结 snapshot 仍计算完整的 `signal_id/signal_ts/observed_at` 等多列 upper watermarks，因此最终 manifest 证据没有被削弱；
- Dashboard health 新增 `indexed_watermarks` 与 `indexed_watermarks_passed`，并修复运行中 null retry schedule 被误显示为 0 的问题；
- source page inspection failure 也被限制为 public-safe `database / snapshot_source_inspection_failed / source_page_stats`，原始 SQLite 文本和路径不进入 health；
- indexed-watermark 独立 verifier 给出 `APPROVE`：确认 source watermark 使用 covering index、不削弱冻结库完整多列 watermarks、consumer/health 会拒绝篡改，且 300 秒 budget 与所有 promotion/mode/paper 边界保持不变。

### 2026-08-08｜生产第二轮收敛：bounded source metadata policy

- indexed-candidate-watermark SHA `1c448672c804b0654e9507a4ecc6cc105b86d680` 已部署；candidate 两表不再成为断点，生产 failure stage 推进到 `paper / source_metadata:paper_decision_events`；
- 这证明 observed_at watermark 修复有效，同时说明 source phase 不能对其他高频 required 表继续执行多列 aggregate MAX；
- source metadata 统一为 bounded policy：`candidate_shadow_observations`、`candidate_shadow_virtual_trades`、`paper_decision_events`、`a_class_decision_events`、`opportunity_events` 必须使用 validated leading timestamp index；
- `paper_decision_events` 使用 `idx_pde_event_ts`，A_CLASS 使用 `idx_a_class_decision_recent`，opportunity 使用 `idx_opportunity_events_recent`；复制 predicate 与 source watermark 均绑定同一 index；
- 没有登记 indexed anchor 的 watermark 表在 source phase 必须写入 `deferred_to_frozen_snapshot / source_query_executed=false`，不得运行 aggregate source query；
- authoritative consumer 新增 deferred-policy 验证，任何恢复 active-source aggregate scan 的 manifest 都被拒绝；
- source lock 释放后，冻结 snapshot 继续计算每张表完整的 multi-column upper watermarks、quick-check、size 和 SHA；
- Dashboard producer health 扩展为验证 5 张 indexed paper 表，`consumer_ready` 仍只来自 authoritative Python preflight；
- 生产本轮仍在 300 秒 ceiling fail-closed，没有 current/partial publication，paper DB integrity 保持 `ok`，P0-D 继续锁定；
- bounded-source-metadata 独立 verifier 给出 `APPROVE`：确认 active-source 无锚表只 deferred，5 张 paper 表使用真实 leading indexes，冻结库完整 watermarks/SHA/quick-check 仍为 acceptance 必需条件。

### 2026-08-09｜生产第三轮收敛：table timing、candidate off-lock 与 parallel paper-decision stage

- table-timing SHA `9a87359853b5d4efd353069f205f7fba2953e8ce` 在生产测得：candidate observations 286,272 行约 147.4 秒，virtual trades 123,256 行约 12.4 秒，`paper_decision_events` 在剩余约 140 秒内仍无法完成；
- candidate off-lock SHA `b503725486700036f055dbd8d69cc77013bdf7c1` 已部署，将 candidate observations 的 active-source 阶段缩短到约 74.5 秒；生产新断点明确为 `paper_decision_events` 约 213.9 秒，累计仍触及 300 秒；
- 当前受控实现将 `paper_decision_events` 作为完整行、完整 `payload_json` 的独立 staging copy，与 main paper selective copy 并行；两条 source read view 各自保留 300 秒 deadline；
- 两条 paper views 与 signal/raw/kline 共 5 个 pinned view 全部纳入 cross-DB skew；authoritative consumer 与 public health 都会重算角色、数量、中点和 skew，不能只信 manifest 自报；
- main paper 与 parallel decision view 都释放后，才执行 stage merge、candidate JSON projection、index build、full watermarks、quick-check 与 SHA；
- candidate stage 和 paper-decision stage 使用独立硬容量上限，disk preflight 计入 output cap + 两个 stage cap + reserve；paper-decision stage 固定使用 4096-byte pages，避免大源页尺寸造成预检与实际容量不一致；
- stage copy 保留 source schema、所有列和 payload；row count、quick-check、stage page size、source-lock duration、merge order 与 cleanup 都成为 producer、consumer 和 Dashboard 的强制契约；
- 所有 synchronization、timeout、budget、quick-check、row mismatch 与 cleanup failure 都保留稳定 public-safe error code；
- 第一轮独立 review 发现大页尺寸与错误分类缺口并给出 `REJECT`；修复后第二轮 review 未发现可执行 correctness regression；
- 最终本地门禁：Python `343 passed`、Node 20 `71 passed`、Basic Readiness 无 blocker、`normal_tiny_ready=false`，尚未部署本轮 parallel stage；
- P0-D 仍锁定，promotion、strategy change、automatic runtime change 与 paper enablement 均保持 false。

### 2026-08-09｜生产第四轮收敛：三张重型事件表并行冻结

- parallel-P9 SHA `0d12afdd03f38830ea3836dede0be0469947d4cb` 已通过 exact-SHA CI 并部署；`paper_decision_events` 从 main paper 顺序路径移除后，生产断点推进到 `copy_table:a_class_decision_events`；
- 本轮生产实测 main paper：candidate observations 278,208 行约 79.48 秒，candidate virtual trades 119,583 行约 9.71 秒；A_CLASS 在剩余约 210.82 秒内仍未完成，说明独立 P9 stage 有效，但高频宽事件表不止一张；
- 当前受控实现将 `paper_decision_events`、`a_class_decision_events`、`opportunity_events` 三张表分别放入独立、完整行、完整 JSON 的 staging DB；每条 stage 使用独立 read-only connection、独立 transaction、独立 300 秒 deadline；
- main paper 与三条 stage 共四个 paper read views 必须先完成 pin，随后与 signal/raw/kline 形成 7-view lineage；producer、authoritative consumer 与 Dashboard 都会重算 exact roles、view count、timestamp midpoints、lock limits 与 cross-DB skew；
- 三条 stage 全部释放 active-source view 后，才允许依次 merge；三条 merge 完成且 stage 清理后，才执行 candidate payload projection、final indexes、full frozen watermarks、quick-check 和 SHA；
- 三张表均保留 source schema、全部列、未知 JSON 字段和 row identities；每张表独立验证 stage size/cap、4096-byte page size、rows copied/merged、quick-check、source lock、merge order 与 cleanup；
- disk preflight 现在显式预算 output cap、candidate stage、P9 stage、A_CLASS stage、opportunity stage 和 reserve；每个 stage 有独立硬上限，所有 cap 的 key、share、总和与 alias 都由 consumer 和 Dashboard 重算；
- 每条 stage 的 deadline、pre-barrier、cancel、budget、quick-check、row mismatch 和 cleanup failure 都保留 public-safe component code；共同 barrier 前的单路失败会传播真实 table/code，不会退化为模糊 barrier error；
- 本地正反例当前通过：Python snapshot/consumer `122 passed`，完整 Python gate `362 passed`，Node 20 完整 gate `71 passed`；逐表 payload 保真、逐表 deadline、逐表 budget tamper、逐表 nested tamper、7-view lineage tamper 和 pre-barrier failure propagation 均有覆盖；
- 独立 maker/checker 审查未发现可执行 correctness regression；实际源文件复核确认 checker 输出中的重复行只是展示伪影，不存在于代码；
- 三路实现已作为 PR #78 的 exact SHA `bd19afc427d6972104eba5aba1e9e26ef667f0f3` fast-forward 到 `main` 并由 Zeabur 成功部署；P0-D、promotion、strategy change、automatic runtime change 与 paper enablement 继续保持 false。

### 2026-08-09｜生产第五轮收敛：opportunity path samples 独立冻结

- `bd19afc` 首次生产 snapshot 在 300.10 秒处继续 fail-closed，但前三张独立事件 stage 均不再成为主路径断点；新断点为 `copy_table:opportunity_event_path_samples`；
- 本轮实测 main paper 在进入 path samples 前累计约 110.57 秒：candidate observations 274,680 行约 90.61 秒、virtual trades 117,277 行约 13.07 秒、paper missed attribution 34,377 行约 6.68 秒，其余已完成表均低于 0.1 秒；path samples 本身运行约 189.53 秒后触及总 300 秒 ceiling；
- 新受控实现只增加当前有直接生产证据的 `opportunity_event_path_samples` 独立 pinned stage，不提高 300 秒上限、不缩短 840h long-history window，也不把其他未证明的表顺手迁移；
- path stage 复用完整行 staging 契约，保留 `raw_payload_json` 和未来未知字段，并参与 exact role/count/skew、stage disk cap、rows copied/merged、quick-check、lock duration、merge-after-release 和 cleanup 验证；
- paper lineage 从 7 个 read views 增加到 8 个：signal/paper/raw/kline main views，加 P9、A_CLASS、opportunity event 与 opportunity path sample 四条 stage views；
- 当 path samples 表存在时，disk residual 配置权重为 candidate 20%、P9 25%、A_CLASS 25%、opportunity event 10%、opportunity path 20%；总 output cap 与 reserve 不变，所有 cap、configured/active inventory、raw/normalized share 与 omitted optional inventory 均由 producer、consumer 和 Dashboard 独立重算；
- `opportunity_event_path_samples` 在 selection contract 中仍保持 optional：源库缺表时不创建伪 stage，也不保留闲置 cap；candidate 与三张 required stage 按 active weight 归一化分配全部 residual 空间。最小受限空间正例证明 candidate + 三张 required stage 各获得 12,288 bytes 后仍能生成合法 bundle；
- inspection 阶段先记录实际 active stage inventory，进入 pinned source view 前再次核对；inspection→pin 之间任何表 inventory 漂移均 fail-closed，不允许磁盘预算和实际 stage 集合分叉；
- 独立反方审查共经历四轮：第一轮发现 optional 表被隐式必需化；第二轮发现未知 manifest stage 可触发 `KeyError`；第三轮发现非 list report inventory 可触发 `TypeError` 以及 optional 缺表会闲置磁盘份额；上述问题均修复并增加篡改/缺表/漂移回归，第四轮未发现新的可执行 correctness issue；
- 最终门禁：Python snapshot/consumer focused `132 passed`，Node 20 Dashboard focused `63 passed`，完整 Python gate `372 passed`，完整 Node 20 gate `72 passed`，Basic Readiness `136/136`、`normal_tiny_ready=false`、mode-gate scope 为 `final_scope_covered`；
- 四路 path-stage 修复已作为 PR #79 的 exact SHA `9cec0b13cfff4ff6006b43f24ef0258f8c0803b4` fast-forward 到 `main`；post-push CI 与 Zeabur exact-SHA deployment 均成功，P0-D、promotion、strategy change、automatic runtime change 与 paper enablement 继续保持 false。

### 2026-08-09｜生产第六轮收敛：瞬时 SQLite BUSY 分类与按故障类别退避

- `9cec0b1` 首次 production snapshot 没有再以 300 秒 ceiling 结束；attempt 于 `2026-08-09T00:38:24.907829Z` 开始，在约 260 秒处由 `paper_decision_events` stage 抛出裸 `OperationalError` 并 fail-closed；没有发布 manifest/current，paper DB 仍为 `ok` 且 integrity marker 不存在；
- 随后的生产只读探针在 5 秒 busy timeout 下连续两次返回 SQLite error code `5 / SQLITE_BUSY`，第三次及以后恢复；另一次只读 attach + stage inventory + disk preflight 在约 11.25 秒后成功，证明当前直接证据是瞬时锁竞争，而不是 output/reserve 预检失败；
- 同一生产预检确认 active stages 为 P9、A_CLASS、opportunity events、opportunity path samples；free space 约 27.48GB、10GB output cap、5GB reserve 与约 11.37GB stage residual 均通过，不能把本次失败无证据归因为 `SQLITE_FULL`；
- 当前受控修复沿 Python exception cause/context 提取 SQLite `errorcode/errorname`：`SQLITE_BUSY/LOCKED` 稳定映射为 `snapshot_source_read_lock_timeout`，`SQLITE_FULL` 稳定映射为对应 stage budget failure；原始 SQLite message、路径、SQL 和行数据不进入 status/health；
- bounded component evidence 只允许公开非负整数 `sqlite_errorcode` 与匹配 `SQLITE_[A-Z0-9_]+` 的 `sqlite_errorname`；Dashboard 对恶意路径化 error name 和未知字段继续丢弃；
- retry cadence 新增 `consecutive_failure_code_count`：全局 `consecutive_failure_count` 保留完整历史，但当 failure code 从旧的 read-lock ceiling 变为新的瞬时 BUSY class 时，同类计数从 1 重新开始，因此首次为 60 秒、第二次 900 秒、第三次 3600 秒、持续同类失败才进入 21600 秒；不同根因不会借用旧故障的 6 小时惩罚；
- inspection、parallel stage 与 main destination 三类 SQLite 路径均有正反例：真实 exclusive-lock、synthetic `SQLITE_FULL`、public-safe identity、跨故障类别 backoff、同类 backoff、legacy fallback 以及 parallel barrier/cancel fallout 的因果根类选择；唯一真实 BUSY 或 budget 根因不会再被 peer barrier failure 稀释，两个不同真实根因仍保持 `concurrent_evaluator_snapshot_failed`；
- 独立反方审查共五轮：依次发现 inspection BUSY 未分类、legacy 同类 streak 迁移错误、main destination `SQLITE_FULL` 未分类、parallel fallout 覆盖真实根因与中途治理 hash 漂移；所有代码问题均修复并新增回归，最终轮确认 SQLite 分类、安全诊断与按 failure code 退避一致，未发现新的可执行 correctness issue；
- 最终完整门禁为 Python `383 passed`、Node 20 `72 passed`、Basic Readiness `136/136`、`normal_tiny_ready=false`、mode-gate scope `final_scope_covered`；generated client、spec validation、Python/Node syntax、JSON 与 `git diff --check` 全部通过；
- 本轮修复已完成本地实现与独立审查，尚待提交、PR、exact-SHA CI 和生产部署；生产仍正确没有 accepted manifest，AutoLoop 继续 `blocked_evaluator_snapshot_required`，P0-D 继续锁定。

尚未声称完成的生产证据：

- 尚未生成新的 production accepted snapshot；
- 尚未证明生产 read lock duration、output size、free-space reserve 与 manifest SHA；
- 尚未观察到 authoritative consumer 对同一 snapshot/manifest 完整通过；
- 尚未观察到生产 AutoLoop 从该 snapshot 生成 fresh primary capture；
- 因此当前状态是“发布已完成、producer 正在 fail-closed 修复与验收”，不是“线上 AutoLoop 已恢复”。

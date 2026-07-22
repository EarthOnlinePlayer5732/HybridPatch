# 实验计划：`exp_20260717_hybridv8_softbudget_smoke2`

> 这是付费链路冒烟，不用于方法效果结论。首次 API 调用后冻结设计内容；实际偏差只写入活动日志和实验记录审阅。

## 1. 身份与状态

- 实验编号：`exp_20260717_hybridv8_softbudget_smoke2`
- owner：`HP_V8`
- 实验级别：`付费 smoke`
- 计划状态：`ready`（源码/设计独立复审 GO；实际启动仍须 clean commit、空目录检查和 Key probe）
- 创建时间与时区：`2026-07-17T15:55:28+08:00`，`Asia/Singapore`
- 计划作者：Codex 主 Agent
- 计划审阅子代理：`/root/experiment_plan_review`（新上下文、只读）
- 审阅结论：初审 `GO WITH FIXES`；required fixes 闭合后的最终双路复审为 `GO`
- 预期 claim role：`diagnostic_only`
- 预期 lifecycle：通过则 `complete`；触发停止条件则 `failed_informative`

## 2. 迭代思路

- 建议或问题来源：用户对 PR #1 的性能优先修正，以及 HP_V8 零 API 路径兼容审计。
- 来源中的原始主张：词法分类不能关闭有效路径；经验协议负担阈值只能记录，不能拒绝可能正确的输出。
- 转写后的可证伪研究问题：当前干净 HP_V8 提交能否在冻结 transport-v3、MiniMax-M3、distractor-on 条件下完成两样本双臂短链路，并产生完整、可重放、可归因的记录？
- 假设：两样本各 2 RT 的 HP_V8 与 FullRewrite 均能完整提交，`preservation_violations=0`，无系统性 runner/evaluator/transport 错误、重复/半提交 RT 或 API 日志错配。
- 预期机制：HP_V8 恢复完整可用路径并将 burden 改为软遥测；本实验只检查真实链路，不估计方法性能。
- 相对当前 PR 前一提交的唯一计划变化：HP_V8 block profile 恢复 local/bulk/dsl/bounded，P95 burden 由硬拒绝改为软记录，并修正 repair 上下文。
- 明确保持不变的内容：V1–V7 replay、V7 partial acceptance、executor 匹配、preservation、validation gate、no-op、FullRewrite、transport-v3、evaluator、scoring、数据和冻结记录。

## 3. 决策规则

- 若结果为正：完成零 API 完整性复核后，才允许启动 `exp_20260717_hybridv8_softbudget_paired10`。
- 若结果为负：停止，不启动 10 样本 campaign；保留原始证据并报告故障层。
- 若统计不确定：本实验不作统计效果结论；仅按链路门禁判断。
- 若出现基础设施失败：先停止；仅在确认是单 Key/瞬时基础设施且不会刷新已耗尽 response-slot budget 时，按冻结 resume 语义换 reserve Key 补未提交 RT。
- 若发现 evaluator/transport 问题：立即停止，不改 evaluator/transport，不重掷已提交 RT。
- 停止条件：任一 `preservation_violations > 0`；campaign 中 commit 或 clean tree 改变；runner/evaluator/transport 系统性错误；重复或半提交 RT；API 日志不能映射到 sample/method/RT/direction；预算越界。

## 4. 代码、协议与配置

- 计划运行 Git commit：包含本 ready 计划的待创建干净提交；首次调用前由 `git rev-parse HEAD` 复核，精确 SHA 由 `run_metadata.jsonl` 写入，禁止事后猜测。
- 计划工作树状态：`clean`
- 方法版本 / owner：`HP_V8` 与冻结 `FullRewrite`
- protocol revision：`hybridpatch/8`
- prompt revision：`operation_family_lexical/1`；default 三路径，block_movement 四路径；coarse block index 按需。
- executor/gate revision：HP_V8 executor；gate 格式和 preservation 口径冻结。
- transport revision：`opencode_anthropic_sdk/3`
- 模型名称与 provider：`minimax-m3`，OpenCode Go Anthropic SDK 路径。
- API 模式：stream。
- temperature / max tokens / thinking：transport-v3 固定有效 temperature；`max_tokens=131072`；adaptive thinking。
- repair / retry / commit policy：HP 最多一次协议 repair；burden 超限本身不触发 repair；transport 每语义调用 2 response slots、3 pre-generation transient failures；FR 无 repair；每 RT 的 forward/backward 原子提交。
- 依赖或环境版本：提交内 `HP_V8/requirements.txt`；正式运行前记录 Python 与关键依赖版本。
- 与上一实验的配置差异：distractor-on；仅 2 样本 × 2 RT；运行新的 HP_V8 提交。

## 5. 数据与比较设计

- dataset：`data/samples_delegate52`
- split：已曝光样本的诊断 smoke，不是未见测试集。
- 样本编号：`treebank4`、`obj3d2`
- 已曝光/污染状态：两者已在历史 val40 使用；只允许链路诊断 claim。
- `data/CONTAMINATION_REGISTRY.json` 是否已更新：未更新。历史 V7 val40 已实际暴露这两个样本，但冻结 registry 仍误标为 `reserved_holdout_candidate` 且无 canonical evidence；用户明确冻结 `data/`，本轮不得修写。该漂移作为已知 exception 披露，并将 claim 严格降为已曝光诊断，绝不再把样本视为 holdout。
- seed：`42`
- task plan 来源：dispatcher 在任何 sample API 前按 sample、seed=42 原子写入 `<sample>.task_plan.json`，校验 2 个有效 target 并把 SHA-256 固定到 `dispatch_manifest.json`；runner 再把同一哈希登记到锁内共享 `run_metadata.jsonl`。HP_V8 与 FullRewrite 复用同一文件，漂移时在下一次 API 前硬拒绝。
- round trips：每样本 `2`
- methods / arms：`hybridpatch`、`fullrewrite`
- comparison policy：同 sample、同 task plan、同模型、同 transport、同 distractor 设置；treebank4 为 HP_V8→FullRewrite，obj3d2 为 FullRewrite→HP_V8，形成 1/1 顺序对消；不按得分重试。
- distractor 设置：`on`（命令不得传 `--skip_distractor`）。
- 预注册排除及理由：无结果排除；基础设施失败保留并按失败报告。
- 缺失值政策：evaluator error 按现有 analyzer 计 0；未提交 RT 不补为 0，而是标记 campaign 不完整并停止主实验。
- 配对单位：smoke 只做链路诊断，不做推断检验；逐步描述键为 `(sample_id, round_trip_num)`。

## 6. 指标与案例

- 主指标：链路完成 16/16 行、8/8 backward/forward step 可验证；不以 RS 作效果结论。
- 辅助指标：两臂各 RT backward RS、协议失败、repair、route、token、semantic calls、response slots、API attempt 和响应分类；不声称 equal-call/equal-cost。
- CriticalFailure 定义与阈值：相邻 backward RS 下降 `>=0.10` 或从正值归零；仅诊断报告。
- preservation 指标：HP_V8 全部已提交行 `preservation_violations` 总和必须为 0。
- route / kept-context / partial 指标：记录 route 分布、kept-context、partial acceptance；不设事后排除。
- token / 耗时 / 成本指标：按 provider 原始 usage 分方法汇总 input/cache/output/total；费用使用冻结、可追溯的定价口径并标注估算或账单来源。
- 计划重点检查的样本或失败类型：无合法 route repair、soft burden telemetry、stream 完整性、原子提交与日志映射。
- 代表案例选择规则：所有异常 case 全列；不按支持假设的方向选择。

## 7. 命令与资源

- 目标 `out_dir`：`HP_V8/exp_20260717_hybridv8_softbudget_smoke2`
- 完整运行命令：从 `HP_V8/` 使用 fail-fast dispatcher 启动两个 sample worker；Key 值只经子进程环境传递，manifest、命令和日志只记录标签。dispatcher 固定 commit/tree/fingerprint/config/task-plan hash，遇到 worker 非零、metadata failed、preservation、重复/半提交或 unmappable ledger 时终止全部 worker。

```bash
PYTHONUTF8=1 OPENCODE_TRANSPORT=anthropic_sdk_v2 \
MINIMAX_TRANSPORT=opencode MINIMAX_HARD_TIMEOUT=7200 \
  python -u src/paired_campaign_dispatch.py \
  --campaign_role smoke \
  --out_dir exp_20260717_hybridv8_softbudget_smoke2 \
  --samples treebank4 obj3d2 --num_round_trips 2 --seed 42 \
  --keys_file ../.env.frkeys --key_labels KEY_01 KEY_02 \
  --notes 'HP_V8 soft-budget smoke2'
```

- 零额外 API 的结果命令：`PYTHONUTF8=1 python -B src/analyze.py --dir exp_20260717_hybridv8_softbudget_smoke2 --K 2`。canonical 配对单位为 sample 的 exact backward RT2；所有 sample×RT 轨迹只作 descriptive。输出 `analysis/sample_level_final_endpoint.json` 与 `analysis/comparison.md`。

- 并发与 worker 划分：2 个 sample worker；固定映射 `treebank4/KEY_01/HP→FR`、`obj3d2/KEY_02/FR→HP`；dispatcher 写 PID、退出码和 console path。
- Key 标签：active `KEY_01`、`KEY_02`；`KEY_11` 仅作失败诊断后的 reserve，不能重掷已提交 RT。
- 预算上限：硬 scope cap 为 2×2RT×2方向×2方法=16 primary semantic calls，HP 最多额外 8 次 repair；每 semantic call 受冻结的 2 response-slot/3 transient 上限约束，不扩样本/RT。paired10 的机器 preflight 会要求 16/16 行均有有限非负 USD，按固定 `smoke 两方法总 USD × 25`（400/16）投影；`<= USD 50` 才允许启动，`> USD 50` 为 NO-GO。它是启动前估算费用硬门，不是运行中的实时账单 cap。
- 预计时长：取决于 adaptive thinking；前台运行并每 30–60 秒只读监控。
- stdout / stderr / dispatch log：`<out_dir>/dispatch_logs/*.console.log`；runner 自带 `logs/`、`api_calls.jsonl`、`api_raw/`、response journal 和 metadata。
- 恢复与断点续跑策略：dispatcher 不自动 requeue；任一失败先全局停止并审计。审计确认旧 worker 已停止后，仅可使用 `--resume --resume_reason '<审计原因>' --confirm_workers_stopped` 补未提交 RT；dispatcher 还必须验证每样本进程租约、dispatch worker/PID provenance，并把 stale `running` invocation 显式保留为 interrupted。同 out_dir 必须同 commit/tree/fingerprint/campaign_config/transport/task-plan hash；不删除、编辑或重掷已提交行。

## 8. Preflight

- [ ] HP_V8 active；HP_V3–HP_V7 与冻结范围无改动
- [ ] 零 API 单元/回归测试通过
- [ ] 数据与 task plan preflight 通过
- [ ] 样本 runtime evaluator smoke 通过
- [ ] 11 Key 最小探针通过
- [ ] 最终命令、cwd、out_dir 和环境变量复核完成
- [ ] runner 会记录 `run_git_commit`
- [ ] runner 会记录 `git_tree_state`
- [ ] runner 会记录 `started_at` / `finished_at`
- [ ] 实验目录中没有 `.env*`
- [ ] 计划审阅的 required fixes 已闭合

实际命令与退出状态：API 前留空；完成后只在活动日志/record review 追加真实值，不回写设计口径。

## 9. 日志与保留

- 一级 Git record：通过 `tools/process_experiment.py prepare → review → finalize` 生成。
- 二级清理后原始归档位置：实验完成后创建私有、凭据扫描通过的压缩归档；record 中写准确路径和状态。
- 计划压缩格式：ZIP（仅清理后的 request/response/journal 与重放所需 metadata）。
- credential / privacy scan：扫描真实 Key、Authorization、Cookie、`.env` 与常见凭据模式；只报告标签/计数。
- 三级临时日志删除条件：record 和清理后归档均验证通过后，才可删除重复 debug/transport delta；本轮默认保留。
- 未来重新评分所需内容：结果 JSONL、task plans、checkpoints、parsed/raw response、API ledger、code identity、evaluator 输入和配置。

## 10. 实验后预计更新

- [ ] `docs/active_log.md`
- [ ] `analysis/record_review.yaml`
- [ ] `docs/RESEARCH_JOURNAL.md`（若形成可复用机制证据）
- [ ] `docs/FINDINGS.md`（若出现新失败类型）
- [ ] `HP_V8/VERSION.md`
- [ ] `transport/API_ITERATION_LOG.md`（仅 transport 事实变化时；本轮不得修改 transport）
- [ ] `docs/项目结构与迭代史.md`（仅 canonical 状态变化时）
- [ ] `docs/AI_REVIEW_GUIDE.md`（若适用）
- [ ] finalize 生成 records / owner index / global index

## 11. 审阅记录

### 实验计划审阅

- verdict：`GO WITH FIXES`；paired10 另受 smoke 成功门禁。
- observed facts：设计已正确降级为 diagnostic；原 runner 未锁 config/task-plan、可吞 worker 异常，命令也未显式排除 official transport。
- confounds and contamination：两个样本已在 V7 val40 暴露，registry 状态滞后；方法顺序需 1/1 对消；provider sampling seed 不可控。
- missing reproducibility fields：需要 executable manifest、task-plan SHA、完整 transport env、PID/exit code 与 fail-fast stop。
- metric and exclusion risks：smoke 不作效果推断；provider/infrastructure 未提交调用为 missing，方法产生的 context/wildcard mismatch 按冻结 scoring 计 0。
- required fixes：显式 `MINIMAX_TRANSPORT=opencode`；campaign config 与 task-plan 哈希锁；原子 plan；fail-fast dispatcher；1/1 顺序对消；资源口径降为 hard call-grid + 费用参考；registry exception 披露。
- evidence locations：独立审阅 2026-07-17；`experiment_runner.py`、`run_meta.py`、`utils_relay_plan.py`、`paired_campaign_dispatch.py`。

### 主 Agent 处置

- 已落实的修改：除原 required fixes 外，已增加 worker lease/PID/task-plan start barrier、先落盘 authorization 再发布 ACK、全局 first-writer-wins stop latch、resume exact-CAS、exact endpoint/路径身份校验，以及独立 active-worker set；待最终独立复审。
- 未采纳建议及理由：未改 `data/CONTAMINATION_REGISTRY.json`；用户明确冻结 `data/`，且这是历史登记漂移而非本轮首次曝光。以 exception + diagnostic-only 边界处置。
- 最终 GO/NO-GO：源码与设计审计 GO；运行门当前仍为条件性 NO-GO，clean commit、Key probe 和目录空检查完成后由主 Agent 转为 paid GO。

# 实验计划：`exp_20260717_hybridv8_softbudget_paired10`

> 这是用户指定、使用已曝光定向样本的支持/诊断实验，不是未见集正式效果实验。只有配套 smoke 通过后才启动。

## 1. 身份与状态

- 实验编号：`exp_20260717_hybridv8_softbudget_paired10`
- owner：`HP_V8`
- 实验级别：`支持/诊断实验`
- 计划状态：`ready`（源码/设计独立复审 GO；实际启动仍受同提交 smoke 与成本门阻塞）
- 创建时间与时区：`2026-07-17T15:55:28+08:00`，`Asia/Singapore`
- 计划作者：Codex 主 Agent
- 计划审阅子代理：`/root/experiment_plan_review`（新上下文、只读）
- 审阅结论：初审 `GO WITH FIXES`；required fixes 闭合后的最终双路复审为 `GO`，同提交 smoke 通过前仍保持运行 NO-GO
- 预期 claim role：`diagnostic_only`
- 预期 lifecycle：完整则 `complete`；触发停止条件则 `failed_informative`

## 2. 迭代思路

- 建议或问题来源：用户要求性能优先、恢复 block profile 完整路径，并把协议负担阈值改为软遥测。
- 来源中的原始主张：提示精简不能关闭有效路径，协议长度不能硬拒绝可能正确的输出。
- 转写后的可证伪研究问题：在相同 MiniMax-M3/transport/task-plan/distractor 条件下，HP_V8 的 10 样本 10 RT 轨迹能否保持零 preservation violation，并相对 FullRewrite 维持任务完成质量；软 burden、repair 和 route 的真实发生率是多少？
- 假设：HP_V8 不以 burden 阈值阻断执行，block profile 保留四路径；在该定向诊断集上产生完整可重放轨迹且 `preservation_violations=0`。RS 差异方向不预注册为必然正值。
- 预期机制：保留 local/bulk 能防止词法误分类造成的能力缺失；软 budget 让长但可能正确的信封继续执行；bounded 仍提供完整 editable source。
- 相对上一版本或对照的唯一计划变化：同 smoke 计划中的 HP_V8 实现变更；FullRewrite 与所有冻结组件不变。
- 明确保持不变的内容：V1–V7 replay、V7 partial policy、executor 匹配、preservation、validation gate、no-op、FullRewrite、transport-v3、evaluator、scoring、数据和冻结记录。

## 3. 决策规则

- 若结果为正：仅作为 HP_V8 代码审计后的定向支持证据；不外推到未见总体。
- 若结果为负：保留并按 route/repair/burden/model/evaluator 分层分析；不事后修改样本或排除规则。
- 若统计不确定：报告配对估计、样本级轨迹和不确定性，不下优劣结论。
- 若出现基础设施失败：停止新调用；仅按冻结 resume 语义补未提交 RT，不能刷新已耗尽语义调用预算。
- 若发现 evaluator/transport 问题：立即停止，不修改冻结组件，不将故障归因于方法。
- 停止条件：任一 `preservation_violations > 0`；commit/tree 在 campaign 中变化；runner/evaluator/transport 系统性错误；重复或半提交 RT；API ledger 不能映射 sample/method/RT/direction；配套 smoke 未通过；预算越界。

## 4. 代码、协议与配置

- 计划运行 Git commit：与通过的 smoke 完全相同的待创建干净提交；精确 SHA 由 `run_metadata.jsonl` 写入并在开跑前/后核对。
- 计划工作树状态：`clean`
- 方法版本 / owner：`HP_V8` 与冻结 `FullRewrite`
- protocol revision：`hybridpatch/8`
- prompt revision：`operation_family_lexical/1`；default={local,bulk,bounded}；block_movement={local,bulk,dsl,bounded}。
- executor/gate revision：HP_V8 executor；现有 gate/preservation 格式冻结。
- transport revision：`opencode_anthropic_sdk/3`
- 模型名称与 provider：`minimax-m3`，OpenCode Go Anthropic SDK 路径。
- API 模式：stream。
- temperature / max tokens / thinking：transport-v3 固定有效 temperature；`max_tokens=131072`；adaptive thinking。
- repair / retry / commit policy：HP 最多一次协议 repair；soft burden 不触发 repair；transport 每语义调用最多 2 response slots 和 3 pre-generation transient failures；FR 无 repair；RT 原子提交。
- 依赖或环境版本：提交内 `HP_V8/requirements.txt`；preflight 记录 Python/关键依赖。
- 与上一实验的配置差异：从 smoke 的 2 样本×2 RT 扩到固定 10 样本×10 RT；其余配置完全相同。

## 5. 数据与比较设计

- dataset：`data/samples_delegate52`
- split：用户指定、历史已曝光的跨域定向诊断子集。
- 样本编号：`treebank4`、`obj3d2`、`filesystem3`、`jobboard3`、`json2`、`satellite4`、`docker6`、`mathlean2`、`musicsheet2`、`circuit2`
- 已曝光/污染状态：前六个在历史 val40 暴露；后四个在 V7 dev20 暴露。存在 selection/survivorship bias，禁止作为未见集总体估计。
- `data/CONTAMINATION_REGISTRY.json` 是否已更新：未更新。后四个 dev20 样本已正确污染；前六个虽已在 V7 val40 实际暴露，冻结 registry 仍误标为 reserve。用户明确冻结 `data/`，本轮只披露该历史漂移并永久按已曝光诊断处理，不把它们视为 holdout。
- seed：`42`
- task plan 来源：dispatcher 在任何 sample API 前按 seed=42 原子预生成全部计划，验证 10 个 target 并将 SHA-256 写入 executable manifest；runner 在 metadata 锁内登记相同哈希。两臂共享同一文件，跨 campaign 或运行中漂移均硬拒绝。
- round trips：每样本 `10`
- methods / arms：`hybridpatch`、`fullrewrite`
- comparison policy：同 sample、同 plan、同模型、同 transport、同 distractor；按样本列表交替方法顺序形成严格 5/5 对消；不按结果换 Key 或重掷。
- distractor 设置：`on`；不得传 `--skip_distractor`。
- 预注册排除及理由：无得分排除。若因预注册停止条件中断，已完成部分作为 `failed_informative` 完整披露，不补成正式完整比较。
- 缺失值政策：evaluator error 按现有 analyzer 计 0；未提交 RT 记缺失并使 campaign 不完整，不静默填 0。
- 配对单位：canonical 终点的推断/比较单位为 sample（`n=10`）；每个 sample 的终点是 RT10 backward。100 个 sample×RT 点只用于相关轨迹描述，不作 iid 推断。

## 6. 指标与案例

- 主指标：10 个样本的 RT10 backward，即仓库标准 `RS@10`；可附注为“20 edit steps 后的 endpoint”，不得把 analyzer 的 K 改成 20 或无条件称作 canonical `RS@20`。
- 辅助指标：样本级 RT10 配对差（n=10）及不确定性、RS@1/5/10、每样本轨迹。100 个 backward 点和 ECR-conditioned RS 仅作描述；后者明确是 post-treatment conditioning。
- CriticalFailure 定义与阈值：相邻 backward RS 下降 `>=0.10` 或从正值归零；按方法报告严重失败数/可用转移数。
- preservation 指标：HP_V8 所有提交行 `preservation_violations` 总和必须为 0；否则立即停止。
- route / kept-context / partial 指标：route 分布、profile 分布、protocol failure、repair attempted/used/success、kept-context、partial acceptance、soft burden 超限及各指标。
- token / 耗时 / 成本指标：按 provider usage 分方法汇总 prompt/cache/output/total token、semantic calls、response slots、HTTP attempts、wall time；费用使用可追溯定价口径并区分估算与账单，禁止 equal-call/equal-cost 主张。
- 计划重点检查的样本或失败类型：唯一离线路径不兼容案例所代表的分类风险、长信封 soft-overage、block profile 的 local/bulk 使用、repair source context、历史 mathlean/circuit 失败形态。
- 代表案例选择规则：全部 preservation/protocol/gate/systemic 异常；每种 route 和每种 burden overage 至少一个（若发生）；RS 最大正/负差各列，不按结论删例。

## 7. 命令与资源

- 目标 `out_dir`：`HP_V8/exp_20260717_hybridv8_softbudget_paired10`
- 完整运行命令：从 `HP_V8/` 启动 fail-fast dispatcher。它写出逐 worker 的 sample、Key label、方法顺序、console log、PID/exit code、commit/tree/config 与 task-plan hash；Key 值不进入 manifest、命令、metadata 或日志。

```bash
PYTHONUTF8=1 OPENCODE_TRANSPORT=anthropic_sdk_v2 \
MINIMAX_TRANSPORT=opencode MINIMAX_HARD_TIMEOUT=7200 \
  python -u src/paired_campaign_dispatch.py \
  --campaign_role main \
  --smoke_dir exp_20260717_hybridv8_softbudget_smoke2 \
  --out_dir exp_20260717_hybridv8_softbudget_paired10 \
  --samples treebank4 obj3d2 filesystem3 jobboard3 json2 satellite4 \
  docker6 mathlean2 musicsheet2 circuit2 --num_round_trips 10 --seed 42 \
  --keys_file ../.env.frkeys \
  --key_labels KEY_01 KEY_02 KEY_03 KEY_04 KEY_05 KEY_06 KEY_07 KEY_08 KEY_09 KEY_10 \
  --notes 'HP_V8 soft-budget paired10'
```

- 零额外 API 的 canonical 结果命令：`PYTHONUTF8=1 python -B src/analyze.py --dir exp_20260717_hybridv8_softbudget_paired10 --K 10`。它只用每样本 exact backward RT10 形成 `n=10` 配对，并将 100 个 sample×RT 点标记为 descriptive/noncanonical；输出 `analysis/sample_level_final_endpoint.json` 与 `analysis/comparison.md`，两者须加入 `record_review.yaml` 的 source reports。

- 并发与 worker 划分：10 个 sample worker；样本列表对应 `KEY_01`…`KEY_10`。方法顺序依次为 HP→FR、FR→HP 交替：treebank4/KEY_01/HP-first，obj3d2/KEY_02/FR-first，filesystem3/KEY_03/HP-first，jobboard3/KEY_04/FR-first，json2/KEY_05/HP-first，satellite4/KEY_06/FR-first，docker6/KEY_07/HP-first，mathlean2/KEY_08/FR-first，musicsheet2/KEY_09/HP-first，circuit2/KEY_10/FR-first。
- Key 标签：`KEY_01`…`KEY_10` active；`KEY_11` reserve，仅用于确认单 Key 基础设施失败后恢复未提交 RT。
- 预算上限：硬 scope cap 为 10×10RT×2方向×2方法=400 primary semantic calls，HP 最多额外 200 repair；每调用仍受冻结 response-slot/transient 上限，不扩样本/RT。dispatcher 在读取 Key 或启动 worker 前重算同提交 smoke 的完整性、16/16 USD coverage 与证据 digest，固定按 `400/16=25` 投影；`<= USD 50` 才 GO，`> USD 50` 硬拒绝。生成的 `smoke_cost_gate.json` 及 SHA 写入 main manifest；这是事前估算费用门，不是实时账单 cap。
- 预计时长：10 路并行，受 adaptive thinking 和 provider 限流影响；每 30–60 秒读取 checkpoint/API ledger。
- stdout / stderr / dispatch log：`<out_dir>/dispatch_logs/<sample>__<KEY>.console.log`；runner 原生日志、api_calls、api_raw、response journal、metadata 全保留。
- 恢复与断点续跑策略：dispatcher 任一异常立即终止全体且不自动 requeue；审计确认旧 worker 已停止后，仅可使用 `--resume --resume_reason '<审计原因>' --confirm_workers_stopped` 补未提交 RT。dispatcher 还必须验证每样本进程租约、dispatch worker/PID provenance，并把 stale `running` invocation 显式保留为 interrupted。不得删除/覆盖/重掷已提交行；commit/tree/fingerprint/campaign_config/transport/task-plan hash 任一不一致均拒绝。

## 8. Preflight

- [ ] 配套 smoke 全部门禁通过
- [ ] HP_V8 active；冻结版本和范围无改动
- [ ] 零 API 单元/回归测试通过
- [ ] 数据与 task plan preflight 通过
- [ ] 10 样本 runtime evaluator smoke 通过
- [ ] 11 Key 最小探针通过
- [ ] 最终命令、cwd、out_dir 和环境变量复核完成
- [ ] runner 会记录 `run_git_commit`
- [ ] runner 会记录 `git_tree_state`
- [ ] runner 会记录 `started_at` / `finished_at`
- [ ] 实验目录中没有 `.env*`
- [ ] 计划审阅的 required fixes 已闭合

实际命令与退出状态：API 前留空；完成后只在活动日志/record review 追加真实值。

## 9. 日志与保留

- 一级 Git record：`tools/process_experiment.py prepare → review → finalize` 生成的小型 canonical record。
- 二级清理后原始归档位置：实验完成后创建私有、凭据扫描通过的压缩归档；record 写准确位置、指纹和完整性。
- 计划压缩格式：ZIP；保留清理后的 request/response/journal、API ledger、task plan、checkpoint 和运行 metadata。
- credential / privacy scan：扫描所有实验文件与归档中的 11 个真实 Key、Authorization、Cookie、`.env` 和常见凭据模式；只输出标签和计数。
- 三级临时日志删除条件：一级 record 与二级归档验证完成后；本轮默认不删除 transport debug 证据。
- 未来重新评分所需内容：结果 JSONL、task plans、checkpoints、parsed/raw responses、API ledger、code identity、target/evaluator 配置和原始 usage。

## 10. 实验后预计更新

- [ ] `docs/active_log.md`
- [ ] `analysis/record_review.yaml`
- [ ] `docs/RESEARCH_JOURNAL.md`
- [ ] `docs/FINDINGS.md`（出现可复用机制或失败类型时）
- [ ] `HP_V8/VERSION.md`
- [ ] `transport/API_ITERATION_LOG.md`（仅发现 transport 事实变化时；不得修改 transport 实现）
- [ ] `docs/项目结构与迭代史.md`（仅 canonical 状态变化时）
- [ ] `docs/AI_REVIEW_GUIDE.md`（若适用）
- [ ] finalize 生成 records / owner index / global index

## 11. 审阅记录

### 实验计划审阅

- verdict：`GO WITH FIXES`，且 smoke 成功前维持 NO-GO。
- observed facts：设计正确降级为已曝光 diagnostic；原 runner 的 config/plan/异常停止与固定 HP-first 存在阻塞。
- confounds and contamination：前六个样本 registry 滞后，全部十个已曝光；provider seed 不可控；需要 5/5 方法顺序对消。
- missing reproducibility fields：executable manifest、plan SHA、显式 MiniMax transport、PID/exit code、全局 fail-fast。
- metric and exclusion risks：RT10 是标准 RS@10；终点 n=10 sample；100 个串行点不得当 iid；ECR-conditioned 是 post-treatment descriptive。方法输出的 context/wildcard mismatch 按 0，provider/API 未提交记 missing，确定性 evaluator/infrastructure 故障停止且不归因方法。
- required fixes：同 smoke §11，再加 5/5 顺序对消、sample-level endpoint 统计和 smoke 成功门。
- evidence locations：独立审阅 2026-07-17；`analyze.py`、`experiment_runner.py`、`run_meta.py`、`utils_relay_plan.py`、`paired_campaign_dispatch.py`。

### 主 Agent 处置

- 已落实的修改：代码和计划已加入不可绕过的固定 400-step grid、同提交 smoke 费用/完整性门、worker start barrier、全局 stop latch、resume exact-CAS、strict formal endpoint 与 provenance 审计；待最终独立复审。
- 未采纳建议及理由：不修改冻结 `data/CONTAMINATION_REGISTRY.json`；以明确 exception 和 diagnostic-only claim 处置历史登记漂移。
- 最终 GO/NO-GO：源码与设计审计 GO；运行门保持 NO-GO，只有 clean commit/Key probe 完成且同提交 smoke 完整通过、`smoke_total_usd × 25 <= USD 50` 后才能转 paid GO。

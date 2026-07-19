# 实验计划：`exp_20260719_hybridv8_transportv4_supplement40`

> 在已完成的 snapshotfix paired10 之后，使用 V6 val40 的同一 40 样本集合做同配置补充进阶实验。采用 record 中 canonical sorted order；原始 campaign 分目录保存，实验后生成明确标注的联合派生视图，不改写或拼接原始结果。

## 1. 身份与状态

- owner：`HP_V8`
- 实验级别：支持/诊断实验
- 计划状态：`reviewed_ready_for_runtime_gates`
- 创建时间：`2026-07-19T05:47:00+08:00`（Asia/Singapore）
- 计划作者：Codex 主 Agent
- 计划审阅子代理：`/root/review_supplement40_plan`（新上下文、只读、零 API）
- 审阅结论：`GO`（两轮 required fixes 均已闭合）
- 预期 claim role：`diagnostic_only`
- 预期 lifecycle：40/40 完整则 `complete`，否则 `failed_informative`

## 2. 研究问题与唯一变化

- 来源：用户要求在已完成的 10-sample RT10 campaign 后，追加历史 V6 val40 的同一 40 样本集合；13 个 Key 先验证存活，然后每 Key 最大并发 4，并让 HP 与 FR 同时运行。V3 只提供过 dev20 历史证据，不把本轮 40 错写成“V3 的 40 样本”。
- 可证伪问题：在相同 HP_V8、FullRewrite、MiniMax-M3、transport-v4、seed42、distractor-on 和 10RT 设置下，固定 val40 的完整 RT1–RT10 配对轨迹是什么？
- 假设：40 个样本可形成 1,600 条唯一结果行；`preservation_violations=0`；样本级 provider exhaustion 不终止其他样本；HP 与 FR 在每个 Key 内混合并发且不存在方法级串行波次。
- 唯一计划变化：相对 `exp_20260718_hybridv8_transportv4_snapshotfix_paired10`，样本集合扩展为 V6 val40 固定 40，Key 池为 13 个且 `slots_per_key=4`。方法、协议、提示、执行器、gate、transport、模型、seed、RT、distractor、evaluator 和 scoring 均不变。
- 调度实现：每个样本仍由一个受审计 worker 完成两种方法，以保留既有 sample checkpoint、隔离和恢复语义；40 个样本 worker 一次性启动。13 个 Key 各承载 3–4 个 worker，每 Key 内首方法按 HP/FR 交替，Key 的起始相位也交替，得到全局 HP-first/FR-first=`20/20`；两种方法同时发起，不等待一组方法完成后再运行另一组。

## 3. 决策与停止规则

- 正面、负面或不确定结果：均完整报告 RT1–RT10，不按结果筛样本，不据此修改方法。
- sample-local：仅四证一致的 provider/transport R2/I3 exhaustion 标记 `infrastructure_incomplete`；其他 sample 继续。恢复只启动 incomplete sample，从首个未提交 RT 继续，不重发已提交 forward/backward。
- generation 恢复上限：每个失败 semantic root 最多授权一次 `g001`；二次耗尽后保持 incomplete，禁止 `g002+`。
- campaign-global：`preservation_violations>0`、Git commit/tree 或 task plan 漂移、重复结果行、半提交 RT、API/attempt/journal 无法映射、未捕获 runner/evaluator/local 异常、共享数据或评测完整性错误。
- 发生全局停止时保留原目录和证据，不在同一目录改变代码或配置。
- 不实现模糊的“多个 provider 错误后全停”。

## 4. 固定代码、协议与配置

- 计划运行 Git commit：包含本计划和全部 required fixes 的最终 clean HEAD。由于 Git commit 不能在自身内容中嵌入自己的 SHA，实际 SHA 不回写本文件；正式 API 前由 `dispatch_manifest.json`、`run_metadata.jsonl` 和 `preflight/run_identity.txt` 三处一致记录，并由启动门核对。
- 方法：`HP_V8 hybridpatch/8` vs FullRewrite
- prompt/executor/gate：当前 HP_V8，方法文件不在本次调度改动范围
- transport：`opencode_anthropic_sdk/4`，Anthropic SDK stream，`exact_payload_new_semantic_call/1`
- model/provider：MiniMax-M3 / OpenCode Go
- temperature / max tokens / thinking：effective temperature 1.0 / 131072 / adaptive
- transport retry：每 exact semantic call 最多 2 个生成 response slot，另有最多 3 次生成前 transient failure
- HP repair：最多一次独立语义调用；拥有独立 transport retry 预算
- seed / RT / distractor：42 / 10 / on
- 运行环境：Windows，项目命令由 Git Bash 执行，`PYTHONUTF8=1`
- environment：Python `3.11.9`，`anthropic==0.104.1`，`requirements.txt` SHA-256 `e1ae65933f248594e4070c368137c24e93e2c4d93f16576a2c7cff0fcfa1c519`
- frozen method SHA-256：prompt `090f427e5a1ae51b86f431a400827dad04fd714587b60acfc165ea57ebe49463`；schema `47a0d6db84cfd0a21af04f58d3379309a72451b4cf9f802269c62319989dbb12`；executor `125f68a505dd5146a38883fd76665b12b7fe6e214b75212603151606d02b80f8`；gate `d5836b62c3b62720028bc8bfdc3460dbbcda733f83e7ecb7dddbedfa8336cf6d`。dispatcher/runner/model 指纹由正式 manifest 的完整 `code_fingerprint` 固定。

## 5. 数据与比较设计

- dataset：`data/samples_delegate52`
- split：历史 V6 `val20 + unused_reserve20` 固定 40
- 样本（V6 val40 record 的 canonical sorted order；集合与 V6 一致，但不声称复用旧 materialization dispatch order）：
  `crystal6 docker3 earncall1 emails5 filesystem2 filesystem3 fonteng1 fonteng5 foodmenu1 geodata1 geodata4 geotrack5 geotrack6 hamradio6 jobboard3 json2 json4 landmarks2 landmarks3 libcatalog2 libcatalog5 mathlean4 mathlean5 obj3d2 obj3d5 quantum1 quantum5 robotics1 robotics3 satellite4 screenplay4 screenplay5 spreadsheet1 spreadsheet6 subtitles2 subtitles6 transit1 transit2 treebank2 treebank4`
- 已曝光状态：全部历史已曝光，只支持诊断性描述，不作为未见集或总体泛化证据；不修改冻结 `data/CONTAMINATION_REGISTRY.json`。
- task plans：从 `HP_V6/exp_20260711_hybridv6val40` 原字节复制 40 个 seed42/10RT plan；V8 dispatcher 重新计算期望序列并逐文件拒绝任何漂移，manifest 记录 SHA-256。计划前零 API 核对为 40/40 一致，排序后的 `{sample: sha256}` 映射 canonical digest 为 `b764ac7ac0e1be4a057064833f281312f01d9d59acb2b254c9b2de1284d56872`。
- methods：每样本 `hybridpatch` 与 `fullrewrite`，首方法在每个 Key 内交替且全局严格 20/20 对消。
- 预注册排除：无模型表现排除。基础设施未完成保持缺失/null，不写 0；完整配对分析只包含两臂均 RT10 完整的样本，并单列缺失。
- 配对单位：sample；RT 点不当作独立样本。
- evaluator preflight：40/40 个 runtime evaluator 均成功执行。39 个参考初始状态自评分为 1.0；`obj3d5` 为历史可复现的 `0.9129`，原因是参考与生成都没有材质时既有 evaluator 仍把 `material_score` 记为 0.5。HP_V6 同一 evaluator/样本复算同为 `0.9129`。本实验不修改评分器、不排除该样本，两臂使用相同口径并披露此 ceiling/floor 异常。

## 6. 指标与联合补充组

- 新 40 campaign 的主 RT1–RT10 表使用固定 complete-chain scope：只有两臂均完整提交 RT1–RT10 的 sample 才进入，所有 10 行使用同一个样本集合与同一个 n；不得让 n 随 RT 漂移。另可报告逐 RT available-pair 敏感性表，但必须单独标注，不能替代主表。每行报告 HP 均值、FR 均值、paired delta、固定 n；不得只报告总均值或 RT10。
- 同时报告 exact RT10 RS、paired sample SD、wins/losses/ties。
- CriticalFailure@0.10：相邻 backward RS 下降 `>=0.10` 或正值归零。
- HP telemetry：route、prompt profile、repair attempted/used/success、protocol failure、kept-context、partial acceptance、soft burden、preservation。
- token/费用：按方法与 call_kind；缺 final usage 的 attempt 单列，不能把可审计 known usage 当精确账单。
- 与既有 `exp_20260718_hybridv8_transportv4_snapshotfix_paired10` 组成同一“补充进阶”分析组，但不改写任一原始 campaign：
  1. 分别报告 prior10 与 supplement40 的 RT1–RT10；
  2. 报告 50 个 complete campaign-chain 的描述性 pooled 轨迹：每个 RT、方法分别对 50 条 chain 的 backward score 等权平均；若任一 chain 不完整，主 pooled 表只使用两 campaign 都满足各自 complete-chain scope 的明确链集合并报告固定 n，不以 0 填充；
  3. 六个重叠 ID（filesystem3、jobboard3、json2、obj3d2、satellite4、treebank4）不冒充独立新样本。44-ID 表在每个 RT、每个方法先对同一重叠 ID 的 prior10/supplement40 两条 score 做算术平均，再对 44 个 ID 等权平均；非重叠 ID 使用其单条 score。所有 10 个 RT 使用同一 complete-chain ID 集合与固定 n。

## 7. 命令、Key 与资源

- out_dir：`HP_V8/exp_20260719_hybridv8_transportv4_supplement40`
- Key inventory：预期 `KEY_01`…`KEY_13`，不得输出值。正式运行前每 Key 同时 4 个微型 probe；任一不足 4/4 不启动正式 campaign。
- 并发：13 Key，`slots_per_key=4`，总 capacity 52；40 个 sample worker 全部同时授权，实际每 Key 3–4 个。
- 预算上限：USD 75（包括 primary、repair、transport retry、52 个 probe 和缺 usage attempt 的不确定余量）；prior10 known usage 为 USD 12.607745，按 40/10 线性外推约 USD 50.43，保留约 USD 24.57 余量。不设基于中途分数的停止，也没有代码级自动费用熔断；主 Agent 启动前和每次 resume 前核对已知费用、无 final usage attempt 的保守上界与剩余预算，超过 USD 75 不再发起新 provider POST。
- 脱敏 preflight 日志：`preflight/runtime_evaluator.log`、`preflight/formal_dry_run.log`、`preflight/key_probe_13x4.log`、`preflight/run_identity.txt`；只记录 Key label 和计数，不记录 Key 值。
- 预计时长：由 MiniMax adaptive thinking 与最长单调用决定；dispatcher 前台运行并持续写入日志。

```bash
cd HP_V8
PYTHONUTF8=1 OPENCODE_TRANSPORT=anthropic_sdk_v2 MINIMAX_TRANSPORT=opencode \
  python -u src/fr_baseline_dispatch.py \
  --out_dir exp_20260719_hybridv8_transportv4_supplement40 \
  --method hybridpatch --keys_file ../.env.frkeys \
  --samples crystal6 docker3 earncall1 emails5 filesystem2 filesystem3 fonteng1 fonteng5 foodmenu1 geodata1 geodata4 geotrack5 geotrack6 hamradio6 jobboard3 json2 json4 landmarks2 landmarks3 libcatalog2 libcatalog5 mathlean4 mathlean5 obj3d2 obj3d5 quantum1 quantum5 robotics1 robotics3 satellite4 screenplay4 screenplay5 spreadsheet1 spreadsheet6 subtitles2 subtitles6 transit1 transit2 treebank2 treebank4 \
  --plans_from ../HP_V6/exp_20260711_hybridv6val40 --require_plans \
  --preflight --probe_concurrency 4

PYTHONUTF8=1 OPENCODE_TRANSPORT=anthropic_sdk_v2 MINIMAX_TRANSPORT=opencode MINIMAX_HARD_TIMEOUT=7200 \
  python -u src/paired_campaign_dispatch.py --campaign_role supplemental \
  --out_dir exp_20260719_hybridv8_transportv4_supplement40 \
  --samples crystal6 docker3 earncall1 emails5 filesystem2 filesystem3 fonteng1 fonteng5 foodmenu1 geodata1 geodata4 geotrack5 geotrack6 hamradio6 jobboard3 json2 json4 landmarks2 landmarks3 libcatalog2 libcatalog5 mathlean4 mathlean5 obj3d2 obj3d5 quantum1 quantum5 robotics1 robotics3 satellite4 screenplay4 screenplay5 spreadsheet1 spreadsheet6 subtitles2 subtitles6 transit1 transit2 treebank2 treebank4 \
  --num_round_trips 10 --seed 42 --keys_file ../.env.frkeys \
  --key_labels KEY_01 KEY_02 KEY_03 KEY_04 KEY_05 KEY_06 KEY_07 KEY_08 KEY_09 KEY_10 KEY_11 KEY_12 KEY_13 \
  --slots_per_key 4 --notes 'HP_V8 transport-v4 supplemental val40 paired RT10' --dry_run
```

付费正式命令与 dry-run 完全相同，仅删除 `--dry_run`。恢复保持全部身份参数并追加
`--resume --resume_reason '<精确原因>' --confirm_workers_stopped`，且只用于 dispatcher 已严格标记的 incomplete sample。

## 8. Preflight

- [x] HP_V8 active；HP_V3–HP_V7、Baseline、data 和冻结 records 不修改
- [x] 40 个样本与 V6 val40 record 一致；40 个历史 task plan 均存在且与当前 seed42/10RT 生成结果逐项相等
- [x] 本地 Key inventory 为 13 个非空标签，未输出值
- [x] 新 out_dir 在计划创建时不存在
- [x] 调度新增 zero-API 测试与全部回归通过（最终集成 88/88；其余完整结果见 preflight 记录）
- [x] 40/40 runtime evaluator 可执行；39 个 self-score=1.0，`obj3d5=0.9129` 与 HP_V6 历史口径一致
- [x] 计划只读审阅最终 GO，required fixes 闭合
- [ ] clean commit、formal dry-run 与 task-plan hashes 通过
- [ ] 13-Key ×4 probe 全部通过
- [ ] 启动前核对 commit/tree、active workers、revision、model、seed、methods、distractor

## 9. 日志、恢复与实验后处理

- 一级：standard `prepare → review → finalize` 生成 record；不手工改 records。
- 二级：完整请求、响应、step docs、ledger 和运行 metadata 清理后压缩到仓库外私有归档；扫描 13 个本地 Key 值及 Authorization/Cookie/private-key markers。
- 三级：HTTP headers/heartbeat/debug 仅在取证完成后清理。
- 完成后运行 verifier、analyzer、strict inspector；至少两个只读子代理分别审计完整性和结果/失败；主 Agent 写 `record_review.yaml`。
- 更新 `docs/active_log.md`、`docs/RESEARCH_JOURNAL.md`、`HP_V8/VERSION.md`、适用的 FINDINGS/迭代史/AI review，并生成 RT1–RT10 与联合补充组报告。

## 10. 审阅记录

### 实验计划审阅

- verdict：首轮 `GO WITH FIXES`，修复后最终 `GO`（`/root/review_supplement40_plan`，只读、零 API）
- observed facts：40 样本集合与 V6 val40 相同，task plans 40/40；并发为每 Key 3–4 workers、Key 内两种首方法、全局 20/20；旧 main10 5/5 不变；runtime evaluator 40/40 可执行。
- confounds and contamination：全部已曝光；prior10 与 supplement40 有 6 个重复 ID；两 campaign 的 commit、日期和并发负载不同；联合视图只能描述，不能冒充同质单一 campaign 或版本因果实验。
- missing reproducibility fields：需闭合 clean commit identity、环境/源码指纹、probe/evaluator 日志、resume generation 上限。
- metric and exclusion risks：主 RT1–RT10 必须固定 complete-chain scope；50-chain 与 44-ID 公式、分母和缺失必须冻结。
- required fixes：准确写作 V6 val40 同一集合/canonical sorted order；修复 `--require_plans` preflight 硬门；补上述身份、日志、公式、预算和恢复边界；完成全回归、dry-run 和 13×4 probe。
- evidence locations：本计划；`HP_V8/src/paired_campaign_dispatch.py`；`HP_V8/src/fr_baseline_dispatch.py`；V6 val40 experiment record/materialization；prior10 sanitized report。

### 主 Agent 处置

- 已落实的修改：计划文本已区分 V6 集合与历史 dispatch order；`fr_baseline_dispatch.preflight()` 现让 `--require_plans` 缺失在任何 probe 前返回非零并有 Popen 零调用测试；RT/联合公式、身份记录、指纹、日志和预算已冻结；dispatcher 对 `g002+` 硬拒绝并有针对性回归；最终只读复审 GO。
- 未采纳建议及理由：待定
- 最终 GO/NO-GO：代码与计划层 `GO`；仍需实际通过 clean commit、formal dry-run、identity/task-plan 三证和 13-Key×4 probe 后才允许正式 campaign POST。

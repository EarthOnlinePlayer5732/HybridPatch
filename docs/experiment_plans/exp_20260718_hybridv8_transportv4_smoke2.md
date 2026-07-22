# 实验计划：`exp_20260718_hybridv8_transportv4_smoke2`

> 付费链路 smoke，只验证 transport-v4 与样本隔离链路，不用于方法效果结论。
> 首次 API 调用后冻结设计；实际偏差只写活动日志和 record review。

## 1. 身份与状态

- 实验编号：`exp_20260718_hybridv8_transportv4_smoke2`
- owner：`HP_V8`
- 实验级别：`付费 smoke`
- 计划状态：`reviewed_with_fixes_applied`，零 API preflight 已完成，等待 clean commit、formal dry-run 与 Key probe
- 创建时间与时区：`2026-07-18T09:34:55+08:00`，`Asia/Singapore`
- 计划作者：Codex 主 Agent
- 计划审阅子代理：`/root/experiment_plan_review_v4`（新上下文、只读）
- 审阅结论：`GO WITH FIXES`；§11 的计划修复已落实，API 仍 `NO-GO`，直到 §8 全部闭合
- 预期 claim role：`diagnostic_only`
- 预期 lifecycle：完整则 `complete`；触发停止条件则 `failed_informative`

## 2. 迭代思路

- 来源：旧 `exp_20260717_hybridv8_softbudget_paired10` 在 MiniMax Anthropic
  HTTP 200 `Streaming response failed` 后全局 fail-fast，只完成 192/400 行。
- 研究问题：transport-v4 能否正确识别不完整流、严格执行每 exact semantic call
  R2/I3，并在单样本 retry exhaustion 时隔离该 sample、让兄弟 worker 继续？
- 假设：2 样本×2RT 双臂链路完整，所有 ledger 可映射、无重复/半提交，
  `preservation_violations=0`。
- 唯一计划变化：transport-v3 升为 `opencode_anthropic_sdk/4`，加入严格流终结、
  semantic lineage 恢复与样本级 infrastructure isolation。
- 保持不变：HP_V8 protocol/prompt/executor/gate、V1–V8 replay、V7 partial、
  FullRewrite、evaluator、scoring、数据、seed、task plan 生成、distractor 与方法顺序。

## 3. 决策规则

- smoke 16/16 行、4 个 checkpoint、API/attempt/journal 全映射、verifier PASS、
  preservation 0、独立结果审阅 GO 且 main 成本投影 `<= USD 50`，才允许同提交 main。
- 单 sample 明确 R2/I3 耗尽：标 `infrastructure_incomplete`，兄弟 worker 继续；
  其他 sample 完成后，可在人工审计下只恢复 incomplete sample。实验政策固定为每个失败
  semantic root 最多一次 audited recovery generation（`g001`）；再次耗尽后保持
  `infrastructure_incomplete` / `failed_informative`，不得创建 `g002` 或继续 optional stopping。
- 非 retryable 400/auth/bad request、observability/ledger、本地、runner 或 evaluator
  错误不得伪装成 sample-local infrastructure failure。
- 全局停止：preservation>0、commit/tree 漂移、重复/半提交 RT、ledger 无法映射、
  未捕获本地/evaluator/shared-integrity 错误。
- 未完成步骤保持 missing/null，不写 0；多个 provider error 不触发模糊自动全停。

## 4. 代码、协议与配置

- 计划运行 Git commit：包含本计划与 v4 实现的待创建 clean commit；首次 API 前固定
- 工作树：`clean`
- owner/method：`HP_V8` 与冻结 `FullRewrite`
- protocol：`hybridpatch/8`
- prompt：HP_V8 当前冻结实现，不修改
- executor/gate：HP_V8 当前实现，不修改
- transport：`opencode_anthropic_sdk/4`
- resume policy：`exact_payload_new_semantic_call/1`
- 模型/provider：`minimax-m3`，OpenCode Go，Anthropic SDK stream
- generation：adaptive thinking，`max_tokens=131072`，有效 temperature 由 transport 固定
- retry/repair：每 exact semantic call R2/I3；HP repair 是独立 call_kind 和独立预算；
  恢复创建 root 下的新 `gNNN` semantic call，同 fingerprint、连续 attempt index；本实验
  每个失败 root 预注册最多一次人工恢复（只允许 `g001`）
- commit policy：每 RT forward/backward 原子提交；未完成不写结果行

## 5. 数据与比较设计

- dataset：`data/samples_delegate52`
- 样本：`treebank4`、`obj3d2`，均为历史已曝光诊断样本
- contamination：不得解释为 unseen-set 结果；用户冻结 data registry，本轮不修历史标记
- seed：`42`
- round trips：每样本 `2`
- task plan：dispatcher 在 API 前按 seed 生成、验证并锁定 SHA-256；两臂共享。
  本机 Windows 文本序列化下预注册的 2RT SHA-256 为：
  - `treebank4`：`4c06257253692206adeb8734bb2a5a355bcd4a4e106af2f3ac3c75b385e6d580`
  - `obj3d2`：`48808124047b1d4972d23edee5d109415d6cdb56af3a4026d65a2060862c2b3f`
  formal dry-run 与首次 API 前必须逐项复核 manifest 现场值；任一不一致即 `NO-GO`，
  不用其他平台或旧 campaign 的序列化产物替换。
- arms：`hybridpatch`、`fullrewrite`
- 顺序：treebank4 HP→FR；obj3d2 FR→HP
- distractor：on（不传 `--skip_distractor`）
- 排除：无按分数排除；基础设施缺失保持缺失
- 配对单位：smoke 不作推断；仅链路完整性与逐样本 RT2 描述

## 6. 指标

- 完整性：16/16 result rows、4 checkpoints、8 backward 可 verifier replay
- transport：HTTP attempts、R/I slots、`incomplete_stream`、recovery generations、
  request fingerprint、provider retry exhaustion
- 方法遥测：route、repair、protocol failure、token、费用
- preservation：所有 HP 提交行总和必须为 0
- CriticalFailure：相邻 backward RS 下降 `>=0.10` 或正值归零，仅描述
- 异常案例：全部列出，不按支持方向选择

## 7. 命令与资源

- out_dir：`HP_V8/exp_20260718_hybridv8_transportv4_smoke2`

```bash
cd HP_V8
PYTHONUTF8=1 OPENCODE_TRANSPORT=anthropic_sdk_v2 \
MINIMAX_TRANSPORT=opencode MINIMAX_HARD_TIMEOUT=7200 \
  python -u src/paired_campaign_dispatch.py \
  --campaign_role smoke \
  --out_dir exp_20260718_hybridv8_transportv4_smoke2 \
  --samples treebank4 obj3d2 --num_round_trips 2 --seed 42 \
  --keys_file ../.env.frkeys --key_labels KEY_01 KEY_02 \
  --notes 'HP_V8 transport-v4 smoke2'
```

首次付费调用前，在同一 clean commit、同一最终 out_dir 先执行完整 dry-run：

```bash
cd HP_V8
PYTHONUTF8=1 OPENCODE_TRANSPORT=anthropic_sdk_v2 \
MINIMAX_TRANSPORT=opencode MINIMAX_HARD_TIMEOUT=7200 \
  python -u src/paired_campaign_dispatch.py \
  --campaign_role smoke \
  --out_dir exp_20260718_hybridv8_transportv4_smoke2 \
  --samples treebank4 obj3d2 --num_round_trips 2 --seed 42 \
  --keys_file ../.env.frkeys --key_labels KEY_01 KEY_02 \
  --notes 'HP_V8 transport-v4 smoke2' --dry_run
```

- 并发：2 sample workers；Key 值只经环境传入，日志只存标签
- 硬 scope：16 primary calls；HP 每步最多一次 application repair；每 exact call R2/I3
- 成本门：dispatcher 的 `已提交 16 行 usage 成本 ×25` 仅是 main 启动投影，不是实际账单、
  也不是 runtime hard cap；没有 final usage 的失败或 in-flight attempt 可能未计入。
  smoke 人工授权上限为 `USD 2.00`，main 人工授权上限为 `USD 50.00`，均包含已知 retry/
  recovery attempt。实现没有自动费用熔断；每个 wave 与任何 resume 前由主 Agent 对 ledger、
  已知 usage 和 provider 账单做人工停止检查。只要未终结 attempt 的费用上界无法确认、已知
  累计达到上限或投影超过 main 上限，就停止并保持 `failed_informative`，不追加调用。
- 日志：dispatch log、worker console、api_calls、api_attempt_ledger、api_journal、api_raw
- 恢复：仅 latest outcome 为 `infrastructure_incomplete` 的 sample；不改旧 attempt/row。
  必须从仓库根使用原始参数完整重放，仅附加恢复参数；如果原 Key 的健康状态不明，人工将
  对应 sample 的标签显式替换为 reserve `KEY_11`，不得改变 sample、task plan、模型、顺序或 seed：

```bash
cd HP_V8
PYTHONUTF8=1 OPENCODE_TRANSPORT=anthropic_sdk_v2 \
MINIMAX_TRANSPORT=opencode MINIMAX_HARD_TIMEOUT=7200 \
  python -u src/paired_campaign_dispatch.py \
  --campaign_role smoke \
  --out_dir exp_20260718_hybridv8_transportv4_smoke2 \
  --samples treebank4 obj3d2 --num_round_trips 2 --seed 42 \
  --keys_file ../.env.frkeys --key_labels KEY_01 KEY_02 \
  --notes 'HP_V8 transport-v4 smoke2' \
  --resume --resume_reason '<精确审计原因>' --confirm_workers_stopped
```

恢复前必须审计该 root 尚无既往 audited recovery；若已有 `g001`，本实验禁止再次 resume。

## 8. Preflight

- [x] transport-v4 独立代码审计无 P0/P1 blocker
- [x] 全部 Python compile、HP executor/splitter/model tests 通过
- [x] 故障注入 multi-worker、stop-latch/backoff 与 committed-RT zero-POST 通过
- [x] V1–V8 matrix 通过；V4–V8 代表归档 replay PASS；冻结 V3 的既有
  `quantum4 RT9` mismatch 由 V3/V8 verifier 同样复现并披露，未修改归档
- [x] 两样本 runtime evaluator/task-plan preflight 通过
- [ ] 计划审阅结论 GO
- [ ] 包含计划的 Git commit 已创建且 tree clean
- [ ] 从 `HP_V8/` 运行 formal dispatcher dry-run，manifest 的 2RT task-plan SHA 与预注册值一致
- [x] 新 out_dir 不存在，旧 paired10 未续跑/复制/拼接
- [ ] 11-Key 小探针通过且不输出 Key 值
- [ ] 最终 cwd、命令、revision、commit/tree 复核完成

## 9. 日志与保留

- 一级 record：完成后 `process_experiment.py prepare → review → finalize`
- 二级归档：清理后的 request/response/journal/ledger/task plans/checkpoints 私有 ZIP
- 凭据扫描：真实 Key、Authorization、Cookie、`.env` 与启发式模式；只报计数
- 冻结旧实验：`exp_20260717_hybridv8_softbudget_paired10` 保持
  `failed_informative`，禁止 resume、复制或与本实验拼接

## 10. 实验后更新

- [ ] `docs/active_log.md`
- [ ] `analysis/record_review.yaml`
- [ ] `HP_V8/VERSION.md`
- [ ] `transport/API_ITERATION_LOG.md`
- [ ] 必要时 `docs/FINDINGS.md` / `docs/RESEARCH_JOURNAL.md`
- [ ] finalize 生成的 records/index（不得手改）

## 11. 审阅记录

- verdict：`GO WITH FIXES`（实现无 P0/P1；实验启动仍受 §8 门禁约束）
- required fixes：明确 `HP_V8/` cwd 与完整 dry-run；冻结 2RT task-plan SHA；将 verifier PASS
  与独立 paid-smoke 结果审阅设为 main 硬门；澄清成本投影、未终结 usage 与人工费用停止
  规则；补完整 resume 模板。
- optional-stopping fix：每个失败 semantic root 最多一次 audited recovery（`g001`）；二次耗尽
  保持缺失，不允许 `g002+`。
- smoke 完成后的顺序：确认 worker 全停 → readonly verifier PASS →
  `process_experiment.py prepare --confirm-stopped` → 新上下文独立结果审阅 → 主 Agent 写
  `analysis/record_review.yaml` → `finalize`。只有该链路 GO 才可启动 main。
- 主 Agent 最终 GO/NO-GO：当前 `NO-GO`；clean commit、formal dry-run、Key probe 尚未完成

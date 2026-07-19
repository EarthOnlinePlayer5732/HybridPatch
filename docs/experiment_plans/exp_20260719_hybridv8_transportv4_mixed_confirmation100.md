# 实验计划：`exp_20260719_hybridv8_transportv4_mixed_confirmation100`

## 身份与研究问题

- owner：`HP_V8`
- level / claim role：正式补充确认实验 / `confirmatory_mixed_exposure`
- 用户授权口径：固定 60 个未参与 HP 方法开发或 HP 实验的样本，加 40 个参与过 HP 方法/开发但未进入最近 paired10 与 supplement40 的样本。
- 研究问题：在同一 MiniMax-M3、transport-v4、task plan、seed、distractor 与平衡方法顺序下，HP_V8 相对 FullRewrite 的 RT1–RT10 保留表现如何；结果在 method/developer-unseen 60 与 historical-exposed 40 两层是否一致？
- 可证伪假设：HP_V8 在固定 n=100 的 RT10 backward RS 高于 FullRewrite，且 preservation violations 为 0；两层结果与总体结果全部报告，不以表现性排除改变总体。
- 唯一实验变化：相对已完成的 10+40，改变样本总体与规模；HP_V8 方法、prompt、`hybridpatch/8`、executor、validation gate、FullRewrite、transport-v4、evaluator、scoring、seed、distractor 与 10RT 均冻结。
- lifecycle：仅当 100/100 两臂全部完成 RT10、逐 RT 结果/API/checkpoint/journal/verifier 完整且 preservation 为 0 时，才可标记 complete；基础设施未完成保持 missing/null，不补 0。

## API 前冻结样本

选择工件：

`HP_V8/analysis/20260719_hybridv8_transportv4_mixed_confirmation100/selection.json`

冻结 SHA-256：selection `26da1c3d27a1eb83d70af444197afde7a7f20ade20c6e232f2d6abe605d3d654`；selector `8ed6da89d722021438db71a9b288bc520a2dee710173094b7dd9c9a60c2cf3ec`。fresh-subprocess evaluator preflight `6d0fa0bc600d59597afd8f3b7274ef24129745b6ccecbbed986ae76ac90ed085`，100/100 runtime-runnable、0 failure、100/100 sample tree unchanged、0 new tmp artifact。

固定 seed=42，按 domain、format、semantic task type、文件数和文档长度分层。选择与结果无关，首次 API 前提交并由 dispatcher 要求 current bytes 与 `HEAD:<path>` 完全一致。

- method/developer-unseen：81 个 evaluator-runnable 候选中选择 60，保留 21。
- historical HP/method-exposed：排除最近 paired10 与 supplement40 的 44 个 unique sample 后有 109 个候选。最近两轮之外只有 17 个带直接 HP API 证据，17 个全部纳入；再从其他方法开发/封存曝光样本分层选择 23 个，组成 40。
- combined：190 个候选中固定 selected100、reserve90。两层互斥，selected100 与最近 10+40 的样本零交集。
- 边界：冻结 FullRewrite baseline 曾对 234/234 调用 provider，所以本实验不是 absolute provider-unseen；60 样本层只按用户新授权的 HP method/developer-unseen 口径命名。
- 额外开发曝光审计：`json1`、`molecule1`、`obj3d1`、`starcatalog1` 有 source-level HP/self-test 引用，不能进入 unseen 层，归入 historical exposure 池。
- 污染登记例外：用户要求冻结顶层 `data/`，本轮不回写既有 `data/CONTAMINATION_REGISTRY.json`；首次正式 POST 后，committed selection 与 campaign manifest 共同作为这 60 个样本首次 HP exposure 的只增派生 overlay，归档与报告不得把旧 registry 误读为本轮之后的完整状态。
- reserve90 本轮不调用 API，不根据结果自动补跑。

## 固定运行配置

- methods：HP_V8 vs FullRewrite；同一样本由同一 worker、同一 Key 顺序执行两臂。
- model/provider：MiniMax-M3 / OpenCode Go；adaptive thinking；effective temperature 1.0；max tokens 131072。
- transport：`opencode_anthropic_sdk/4`；R2/I3 transport retry 与独立 HP repair semantic-call 预算冻结。
- seed / RT / distractor：42 / 10 / on。
- task plan：全部 100 样本在 worker 授权前生成 seed42 的 10-target sequence；两臂共享同一 plan，path、sequence 与 SHA-256 写入 immutable manifest。
- method order：总体 50 HP-first / 50 FR-first；unseen60 内 30/30，historical40 内 20/20，避免 cohort 比较与首发顺序混杂。
- concurrency：13 个物理唯一且存活的 Key，每 Key 最大 4 个 paired sample workers；同一 Key 内 HP/FR 配对并发，不使用方法级串行大波次。
- waves：52 + 48；每波每 Key 不超过 4 workers。
- code identity：clean commit、clean tree、code fingerprint、selection SHA、transport revision、模型、seed、task-plan hashes 与时间戳写入 metadata/manifest；运行中任一漂移全局停止。

## 指标与报告

原始分数保持 0–1；派生报告统一乘 100，差值使用百分点。headline 固定 n=100，并另给 n=60 unseen 与 n=40 historical 分层结果：

1. RT1–RT10 每轮 HP、FR、HP−FR pp 与固定 n。
2. RT10 每个样本 HP、FR、差值及 cohort。
3. RT10 mean、median、paired delta、W/L/T、sample SD、IQR、MAD、`|delta|≤5pp/10pp`。
4. seed42、10,000 次 paired bootstrap mean-delta 95% CI 与 exact two-sided sign test。
5. `CriticalFailure@0.10`：正值坍塌到 0 或相邻 backward RS 下降至少 0.10。
6. route、prompt profile、repair、protocol failure、partial acceptance、soft burden telemetry。
7. preservation violations、适用步数与 N/A；任何 violation 全局停止。
8. method/call_kind token、known USD、unknown-final-usage attempts；known USD 不是 provider billing statement。
9. 完整性、未完成样本、重复/半提交 RT、API mapping、numeric score 与 evaluator-error cell 分离、私有归档 SHA-256。

冻结 policy：每条 backward row 必须为 `[0,1]` 有限分数或非空 evaluator error；后者按既有 scoring 记 0 并单列，基础设施未完成不写结果行且保持 null。

## 隔离、恢复与停止

- 单 sample provider/transport retry exhaustion：`infrastructure_incomplete`，保存 attempt/ledger/checkpoint/已提交 RT，其他 sample 继续。
- resume：只启动 pristine pending 或四源一致的 infrastructure-incomplete sample；不重发已提交 forward/backward，不覆盖旧 attempt；当前 experiment 自身 evidence 在 selection 重算中明确排除，其他新增历史曝光仍阻断。
- global stop：preservation violation；Git/tree/selection/task-plan 漂移；重复结果；半提交 RT；API evidence 无法映射；runner/evaluator/local 未捕获异常；共享 executor/data/evaluator integrity 错误。
- finished result 必须有 response-journal-backed 成功 semantic call，且 worker exit 必须 `rc=0/disposition=finished`。

## 预算与启动门

预计 primary edit steps 为 4,000（100×10×2 directions×2 methods），另有实际 HP repair 与 transport attempts。最近 10+40 的 known committed cost 合计约 USD 60.29，线性投影 100 samples 约 USD 120.6。

自动 USD130 门仅是已提交结果行 known usage 的 pre-wave/post-wave/pre-final 停止阈值，不是绝对账单上限；失败 attempt、in-flight wave 与 Key probe 可产生未计或超额，必须单列。已有 NO_GO latch 时普通 resume 不得继续。

首次正式 POST 前：

- [x] selection 工件 LF 字节稳定、60+40/100/90、最近 10+40 零交集、所有 input SHA 与 outcome-blind 重算 PASS。
- [x] selected100 fresh-subprocess evaluator preflight 100/100 PASS，sample tree 无变化。
- [x] Python compile、executor、splitters、dispatcher/transport、analyzer、process tool、V1–V8 matrix 与代表归档 replay PASS。
- [x] mixed100 fault injection：52+48、每 Key≤4、50/50 order、sample isolation、preservation stop、resume、committed RT zero POST、budget latch PASS。
- [x] 新上下文只读实验计划审阅 verdict=`GO WITH FIXES`；fresh preflight、污染 overlay 披露、逐样本 cohort 测试和全部回归已闭合。
- [ ] 计划、selection、runner 与测试形成 clean commit 并推送当前 PR 分支；不合并。
- [ ] clean commit 上 dispatcher `--dry_run` PASS，100 个 task plan/manifest 已冻结。
- [ ] 13 个 Key 逐一完成最小完整流 probe，仅记录 label 与状态，不输出 Key。
- [ ] 最终命令/out_dir/model/transport/seed/selection SHA/plan hashes 复核。

## 运行命令

从 `HP_V8/` 使用 Git Bash：

```bash
PYTHONUTF8=1 OPENCODE_TRANSPORT=anthropic_sdk_v2 MINIMAX_TRANSPORT=opencode \
MINIMAX_HARD_TIMEOUT=7200 python -u src/paired_campaign_dispatch.py \
  --campaign_role confirmation \
  --selection_manifest analysis/20260719_hybridv8_transportv4_mixed_confirmation100/selection.json \
  --out_dir exp_20260719_hybridv8_transportv4_mixed_confirmation100 \
  --num_round_trips 10 --seed 42 --slots_per_key 4 \
  --keys_file ../.env.frkeys \
  --key_labels KEY_01 KEY_02 KEY_03 KEY_04 KEY_05 KEY_06 KEY_07 KEY_08 KEY_09 KEY_10 KEY_11 KEY_12 KEY_13 \
  --notes 'HP_V8 transport-v4 mixed confirmation100'
```

## 实验后

worker 全停后依次执行 strict inspector、raw verifier、百分制总体/分层分析、`process_experiment.py prepare --confirm-stopped`、三个新上下文只读审阅、record review、finalize、validators、credential scan 与私有完整归档。更新 active log、research journal、适用 findings、VERSION 和 PR 描述；提交并推送当前分支，不合并 PR。实验完成后停止，不运行 reserve90，不根据结果修改 HP_V8。

## 计划审阅

- `GO WITH FIXES`。审阅确认选择算术、recent44 排除、17 个直接 HP API 样本强制纳入、两层方法首发平衡、52+48 波次、每 Key≤4、隔离/停止语义与总体/分层分析口径闭合。Required fixes 已在首次正式 POST 前完成；historical40 必须始终披露为 17 个直接 HP API + 23 个方法开发/封存 exposure，不能误写成 40 个直接历史 HP API 样本。

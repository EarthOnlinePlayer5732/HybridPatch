# 实验计划：`exp_20260718_hybridv8_transportv4_snapshotfix_smoke2`

> 付费链路 smoke，只验证 transport-v4 与 dispatcher 因果快照修复；不用于方法效果结论。

## 1. 身份与状态

- owner：`HP_V8`
- 实验级别：付费 smoke
- 计划状态：`reviewed_ready_for_identity_gates`
- 创建时间：`2026-07-18T13:25:00+08:00`（Asia/Singapore）
- claim role：`diagnostic_only`
- lifecycle：完整则 `complete`，否则 `failed_informative`
- 计划作者：Codex 主 Agent
- 审阅：`/root/review_snapshotfix_plans`（新上下文、只读），verdict=`GO`

## 2. 来源、问题与唯一变化

- 来源：`exp_20260718_hybridv8_transportv4_paired10` 在 124/400 结果行时按
  ledger/result mapping 停止。最终 API、ledger、journal 和 result 均存在；API terminal
  row 比 result 早 88 ms 落盘。该时序与最终完整 evidence、当时错误及旧读取顺序共同支持
  torn-snapshot 解释；确定性 fault injection 建立因果。
- 问题：在不放松任何 mapping/lineage/duplicate/half-commit 校验的前提下，dispatcher
  能否对 live append-only 证据取得因果一致前缀，并继续维持 sample-local transport
  exhaustion 与 campaign-global integrity failure 的边界？
- 唯一代码变化：`inspect_campaign` 按 publication 的逆序先缓存 result，再读取 API、
  attempt ledger 和 journal；增加确定性跨文件竞态测试与 stop-latch-before-terminate 断言。
- 不变：`opencode_anthropic_sdk/4` 流分类与 R2/I3、HP_V8 protocol/prompt/executor/gate、
  FullRewrite、evaluator、scoring、data、seed、task-plan 算法、distractor 和方法顺序。
- 旧 v4 main 保留 `failed_informative`；不得 resume、复制或拼接。

## 3. 可证伪假设与停止规则

- 2 样本×2RT 双臂完成 16/16 行；API/ledger/journal/result 可严格映射；checkpoint
  无重复或半提交；preservation 总和为 0。
- 单 sample 仅在 outcome、metadata、API row、attempt ledger 四证一致且 R2/I3 耗尽时
  标为 `infrastructure_incomplete`；其他 sample 继续。
- 立即全停：preservation>0、commit/tree 漂移、重复结果、半提交 RT、无法映射的 API/
  ledger/result、未捕获 runner/local/evaluator 或共享完整性错误。
- 未完成步骤保持 missing/null，绝不补 0。
- 每个失败 semantic root 最多一次人工审计恢复 `g001`；再耗尽则结束，不建 `g002+`。
- smoke 必须 16/16、verifier PASS、只读 `inspect_campaign(require_complete=True)` PASS、
  独立完整性审阅 GO、费用投影不超过 USD 50，才允许同提交的新 paired10。

## 4. 固定配置

- methods：HP_V8 `hybridpatch/8` 与冻结 FullRewrite
- transport：`opencode_anthropic_sdk/4`
- resume policy：`exact_payload_new_semantic_call/1`
- model/provider：`minimax-m3`，OpenCode Go，Anthropic SDK stream
- generation：adaptive thinking，`max_tokens=131072`，effective temperature=`1.0`
- environment：Windows + Git Bash；Python `3.11.9`；`anthropic==0.104.1`；
  `HP_V8/requirements.txt` SHA-256
  `e1ae65933f248594e4070c368137c24e93e2c4d93f16576a2c7cff0fcfa1c519`
- budget：每 exact semantic call 最多 2 个 generation response slots、最多 3 个
  pre-generation transient failures；HP repair 是独立 semantic call 和独立 transport budget
- dataset：`data/samples_delegate52`；materialized split 分别为 treebank4=`val`、
  obj3d2=`unused_reserve`。冻结 `CONTAMINATION_REGISTRY.json` 仍把二者记作原始
  `holdout_reserve_20260625`，未追写后续曝光，属于已知历史漂移；两者均已多次曝光，
  本实验只作 diagnostic
- samples：`treebank4 obj3d2`
- round trips / seed / distractor：2 / 42 / on
- order：treebank4 HP→FR；obj3d2 FR→HP
- task-plan SHA-256：
  - treebank4：`4c06257253692206adeb8734bb2a5a355bcd4a4e106af2f3ac3c75b385e6d580`
  - obj3d2：`48808124047b1d4972d23edee5d109415d6cdb56af3a4026d65a2060862c2b3f`
- commit/tree：包含本计划与快照修复的待创建 clean commit；首次 API 前固定
- frozen method identity SHA-256：prompt
  `090f427e5a1ae51b86f431a400827dad04fd714587b60acfc165ea57ebe49463`、schema
  `47a0d6db84cfd0a21af04f58d3379309a72451b4cf9f802269c62319989dbb12`、executor
  `125f68a505dd5146a38883fd76665b12b7fe6e214b75212603151606d02b80f8`、gate
  `d5836b62c3b62720028bc8bfdc3460dbbcda733f83e7ecb7dddbedfa8336cf6d`
- runner metadata `/3` 必须在首次 POST 前绑定 run Git commit、clean tree、完整
  `code_fingerprint`、transport revision/resume policy 及该 sample 的 task-plan SHA；恢复授权
  继续绑定 semantic call、parent/root、generation、fingerprint 与连续 attempt index

## 5. 指标与解释

- 完整性：16 rows、4 checkpoints、8 backward verifier replays
- transport：HTTP attempts、response/transient budgets、`incomplete_stream`、semantic lineage
- HP：route、prompt profile、repair、protocol failure、soft burden
- usage：provider tokens 与估算费用；没有 final usage 的 attempt 单独披露
- preservation：所有已提交 HP rows 总和必须为 0
- smoke 可描述 RT2 RS/CriticalFailure，但不作效果推断

## 6. 命令、资源与恢复

- out_dir：`HP_V8/exp_20260718_hybridv8_transportv4_snapshotfix_smoke2`
- 并发：2 sample workers；KEY_01、KEY_02；Key 值仅经环境传入
- 人工授权额度：本次只授权一个 16-step wave，保守预留 USD 2.00，覆盖 primary、repair、
  transport retry 及缺 final usage 的 attempt；实现无自动费用熔断。出现缺 final usage 或
  incomplete sample 后，不自动 resume；必须先用 provider 账单或可审计保守上界证明仍在
  额度内，再单独决定是否使用预注册的唯一 `g001`

```bash
cd HP_V8
PYTHONUTF8=1 OPENCODE_TRANSPORT=anthropic_sdk_v2 MINIMAX_TRANSPORT=opencode MINIMAX_HARD_TIMEOUT=7200 \
  python -u src/paired_campaign_dispatch.py --campaign_role smoke \
  --out_dir exp_20260718_hybridv8_transportv4_snapshotfix_smoke2 \
  --samples treebank4 obj3d2 --num_round_trips 2 --seed 42 \
  --keys_file ../.env.frkeys --key_labels KEY_01 KEY_02 \
  --notes 'HP_V8 transport-v4 snapshot-fix smoke2' --dry_run
```

付费命令与上式完全相同，仅删除 `--dry_run`。若存在经四证确认的 incomplete sample，
在 worker 全停、同 commit/tree/revision/task plans 下，使用完全相同参数追加
`--resume --resume_reason '<精确原因>' --confirm_workers_stopped`；已完成 sample 不启动，
已有 `g001` 的 root 不再恢复。

## 7. Preflight 与实验后

- [x] 旧 main worker 全停、active set 空、durable latch 与失败证据已只读审计
- [x] 最终 evidence 与 mtime 支持竞态解释且排除 API row 真缺失；确定性测试建立因果
- [x] 确定性竞态测试及 stop-latch 顺序测试通过
- [x] 两个样本的 scaffold/runtime evaluator initial-state smoke 为 score=1.0
- [x] 全部 Python compile、executor/splitter/transport/analyze/process tests 通过
- [x] V1–V8 replay 与 preservation regression 通过
- [x] 独立计划审阅 GO
- [ ] clean commit 固定，formal dry-run 与 task-plan hashes 通过
- [ ] 11-Key 小探针通过且不输出 Key 值
- [ ] 最终命令/cwd/revision/commit/tree 复核

结束后先确认 worker 全停，运行 verifier、strict inspector、费用门和
`process_experiment.py prepare --confirm-stopped`；随后至少一个新上下文只读审阅完整性，主
Agent 填写 ignored `analysis/record_review.yaml` 并作 operational GO/NO-GO。该 smoke 是
paired10 的 same-commit preflight stage：main gate 硬要求当前 HEAD 等于 smoke manifest commit
且 tree clean，因此预注册一个窄 staging exception——若 smoke GO，只把会改 tracked
catalog/index 的 `finalize` 与 tracked After/VERSION/transport 文档延后到 main 结束；prepare 与
review 不延后。main 结束后再依次 finalize smoke、完整 postprocess main，并创建 private ZIP/
credential scan。若 smoke NO-GO，则不启动 main，并立即 finalize/postprocess smoke。该延后不
允许修改 raw、score、scope 或任何运行配置。

付费 rerun 的随机模型输出不能证明 snapshot 修复的因果性；因果证据只来自确定性并发
fault injection。付费 smoke 只提供端到端 operational compatibility 证据。

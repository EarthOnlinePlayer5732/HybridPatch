# 实验计划：`exp_20260718_hybridv8_transportv4_paired10`

> 用户指定 10 个已曝光样本的支持/诊断实验。不得与旧失败 campaign 拼接；只有
> 同提交 transport-v4 smoke 完整通过后才可启动。

## 1. 身份与状态

- 实验编号：`exp_20260718_hybridv8_transportv4_paired10`
- owner：`HP_V8`
- 实验级别：`支持/诊断实验`
- 计划状态：`reviewed_with_fixes_applied`；代码零 API preflight 已完成，仍等待 commit、dry-run、Key probe 与 smoke GO
- 创建时间与时区：`2026-07-18T09:34:55+08:00`，`Asia/Singapore`
- 计划作者：Codex 主 Agent
- 计划审阅子代理：`/root/experiment_plan_review_v4`（新上下文、只读）
- 审阅结论：`GO WITH FIXES`；§10 的计划修复已落实，main 仍 `NO-GO`，直到同提交 smoke 完整审阅 GO
- 预期 claim role：`diagnostic_only`
- lifecycle：400/400 完整则 `complete`；否则 `failed_informative`

## 2. 研究问题与唯一变化

- 问题：修复 HTTP 200 stream failure 与单 sample 隔离后，固定 10 样本的 HP_V8
  对 FullRewrite 能否形成完整 RT10 配对 endpoint？
- 假设：所有完整 HP 行 preservation=0；transport retry exhaustion 不再无条件杀死
  无关 worker；任何方法效果方向均不预注册。
- 唯一变化：相对旧 paired10 仅 transport-v3→v4 及 dispatcher isolation/resume。
- 明确不变：HP_V8 协议、提示、执行器、gate、partial/no-op、FullRewrite、evaluator、
  scoring、样本、seed=42、task-plan 算法、distractor-on、交替方法顺序。
- 旧档隔离：`exp_20260717_hybridv8_softbudget_paired10` 永久
  `failed_informative`；禁止 resume、复制结果/checkpoint/journal/ledger 或拼接 192 行。

## 3. 决策与停止规则

- 同提交 smoke 必须 16/16、preservation 0、ledger 可映射、readonly verifier PASS、
  独立 paid-smoke 结果审阅 GO，且 main 启动投影 `<=USD50`。
- sample-local：只有四证一致的 retry exhaustion；记 `infrastructure_incomplete`，
  其他 sample 继续。待所有 worker 停止后，只恢复 incomplete sample。实验政策固定为每个失败
  semantic root 最多一次 audited recovery generation（`g001`）；再次耗尽后保持缺失并将
  campaign 记为 `failed_informative`，不得创建 `g002` 或继续 optional stopping。
- campaign-global：preservation>0、Git 漂移、重复/半提交、ledger 无法映射、
  非 retryable bad request、未捕获 runner/本地/evaluator 或共享完整性错误。
- 缺失：未提交 RT 不补 0，正式 endpoint 对该 sample 为 null；若最终不足 10/10，
  不报告完整 RS@10 方法比较，只报告 failed_informative 范围。
- 代码或 transport revision 改变：不得续跑，另建实验编号。

## 4. 代码与配置

- commit/tree：与通过的 smoke 完全相同，`clean`
- protocol/prompt/executor/gate：当前 HP_V8，冻结不改
- transport：`opencode_anthropic_sdk/4`
- resume policy：`exact_payload_new_semantic_call/1`
- model：`minimax-m3` / OpenCode Go / Anthropic SDK stream
- generation：adaptive、`max_tokens=131072`
- retry：每 exact semantic call R2/I3；recovery generation 独立 R2/I3、同 fingerprint、
  root lineage attempt index 连续；HP repair 是独立 semantic root；本实验每个失败 root
  预注册最多一次人工恢复（只允许 `g001`）
- result commit：每 RT forward/backward 原子提交

## 5. 数据与比较设计

- samples：`treebank4 obj3d2 filesystem3 jobboard3 json2 satellite4 docker6 mathlean2 musicsheet2 circuit2`
- 状态：全部历史已曝光，只允许定向诊断，不是 unseen/generalization estimate
- seed：42；每样本 10 RT；distractor-on
- methods：HP_V8 vs FullRewrite；列表顺序严格形成 5/5 HP-first/FR-first
- task plan：新 out_dir 在 API 前重新生成；不得复制旧 campaign 文件。预期 SHA-256：

| sample | SHA-256 |
|---|---|
| treebank4 | `4144576762e4d7171b1969786e9a2e1f511c73cca90836639c47422839cc10c7` |
| obj3d2 | `b5171cbf327e63016b0190f24a8f73f33296f7e8c621c521631f4de71f33c4a2` |
| filesystem3 | `b18c495c20b5cb65e10d20a1f99828c3a48084a3247e3a2c52ae706151ba47b8` |
| jobboard3 | `1539983d4e7e75abfd4a3fa984b1bf151279d656c785c5f82b7a9195f63baae9` |
| json2 | `71089bf32fc13bc0f37f543382df362f3afe2a5ff404d29a7e3c7227e0c3e7c5` |
| satellite4 | `a5278fcdd29659bf4bb962a6e9ac0d00cccb45958e375a8c4cecdf12c1afee78` |
| docker6 | `79a5f8c3426ecf8c30062c76c9c468d3676cb86ef193ecfbfe3cd26254aea0df` |
| mathlean2 | `5a5db1572926c68437a9d22dd43a26594983a36d4c78eb137111a50146a0980b` |
| musicsheet2 | `d765512957414894de5237042b30365fa959638c425b0f167c534eb56e545281` |
| circuit2 | `052ab7035ee6979d279c86bebc1121b9332e80565ede482259f1f8f73b1ffdf0` |

这些值来自本机 Windows 文本序列化；formal dry-run 与首次 API 前必须逐项复核 manifest
现场值。任一不一致即 `NO-GO`，不得拿旧 campaign 或其他平台的文件替换。

- 排除：无得分排除；基础设施缺失保留 missing
- canonical 配对单位：sample 的 exact RT10 backward，`n=10`
- 100 个 sample×RT backward 点只作 descriptive，不作为 iid

## 6. 指标

- 完整性：400/400 rows、20 checkpoints、200 backward verifier PASS
- 主指标：sample-level exact backward `RS@10`，完整时 n=10
- 轨迹：RS@1–RT10 表
- CriticalFailure：相邻 backward 下降 `>=0.10` 或正值归零；每方法 90 transitions
- token/费用：按方法与 semantic `call_kind`；另报 HTTP attempts、recovery generations
- HP：route、prompt profile、repair attempted/used/success、protocol failure、partial、
  kept-context、soft burden
- preservation：所有 HP 已提交行总和 0；任何正值全局停止
- infrastructure：incomplete sample、error_type、R/I exhaustion、resume 次数与 lineage

## 7. 命令与资源

- out_dir：`HP_V8/exp_20260718_hybridv8_transportv4_paired10`

```bash
cd HP_V8
PYTHONUTF8=1 OPENCODE_TRANSPORT=anthropic_sdk_v2 \
MINIMAX_TRANSPORT=opencode MINIMAX_HARD_TIMEOUT=7200 \
  python -u src/paired_campaign_dispatch.py \
  --campaign_role main \
  --smoke_dir exp_20260718_hybridv8_transportv4_smoke2 \
  --out_dir exp_20260718_hybridv8_transportv4_paired10 \
  --samples treebank4 obj3d2 filesystem3 jobboard3 json2 satellite4 \
  docker6 mathlean2 musicsheet2 circuit2 --num_round_trips 10 --seed 42 \
  --keys_file ../.env.frkeys \
  --key_labels KEY_01 KEY_02 KEY_03 KEY_04 KEY_05 KEY_06 KEY_07 KEY_08 KEY_09 KEY_10 \
  --notes 'HP_V8 transport-v4 paired10'
```

main 首次付费调用前，在同一 clean commit、同一最终 out_dir 先执行完整 dry-run：

```bash
cd HP_V8
PYTHONUTF8=1 OPENCODE_TRANSPORT=anthropic_sdk_v2 \
MINIMAX_TRANSPORT=opencode MINIMAX_HARD_TIMEOUT=7200 \
  python -u src/paired_campaign_dispatch.py \
  --campaign_role main \
  --smoke_dir exp_20260718_hybridv8_transportv4_smoke2 \
  --out_dir exp_20260718_hybridv8_transportv4_paired10 \
  --samples treebank4 obj3d2 filesystem3 jobboard3 json2 satellite4 \
  docker6 mathlean2 musicsheet2 circuit2 --num_round_trips 10 --seed 42 \
  --keys_file ../.env.frkeys \
  --key_labels KEY_01 KEY_02 KEY_03 KEY_04 KEY_05 KEY_06 KEY_07 KEY_08 KEY_09 KEY_10 \
  --notes 'HP_V8 transport-v4 paired10' --dry_run
```

- concurrency：10 sample workers；KEY_01…KEY_10，KEY_11 仅作人工恢复 reserve
- hard scope：400 primary semantic calls；HP 每步最多一次 repair；每 exact call R2/I3
- 费用：smoke 的 `已提交 usage ×25` 只是启动投影，不含可能缺 final usage 的失败/in-flight
  attempt，也不是自动 hard cap。main 人工授权上限为 `USD 50.00`，包含已知 retry/recovery；
  实现没有自动费用熔断。每个 worker wave 与任何 resume 前检查 ledger、已知 usage 和 provider
  账单；费用上界不明、已知累计达到上限或继续调用可能越界时立即停止，不追加调用。
- resume：只 launch incomplete sample；同 task plan/model/order/seed/commit/revision；
  旧 failure append-only；已完成 sample 不创建 worker、已提交 RT 不 POST
- 分析：`python -B src/analyze.py --dir ... --K 10 --critical_theta 0.10`

完整恢复模板（从仓库根执行；若失败 Key 健康状态不明，仅把对应 incomplete sample 的标签
显式替换为 reserve `KEY_11`，其余参数不得变化）：

```bash
cd HP_V8
PYTHONUTF8=1 OPENCODE_TRANSPORT=anthropic_sdk_v2 \
MINIMAX_TRANSPORT=opencode MINIMAX_HARD_TIMEOUT=7200 \
  python -u src/paired_campaign_dispatch.py \
  --campaign_role main \
  --smoke_dir exp_20260718_hybridv8_transportv4_smoke2 \
  --out_dir exp_20260718_hybridv8_transportv4_paired10 \
  --samples treebank4 obj3d2 filesystem3 jobboard3 json2 satellite4 \
  docker6 mathlean2 musicsheet2 circuit2 --num_round_trips 10 --seed 42 \
  --keys_file ../.env.frkeys \
  --key_labels KEY_01 KEY_02 KEY_03 KEY_04 KEY_05 KEY_06 KEY_07 KEY_08 KEY_09 KEY_10 \
  --notes 'HP_V8 transport-v4 paired10' \
  --resume --resume_reason '<精确审计原因>' --confirm_workers_stopped
```

恢复前必须审计所有将启动的 root 尚无既往 audited recovery；已有 `g001` 的 root 不得再次
resume，并保持该 sample endpoint missing/null。

## 8. Preflight

- [x] v4 spec/实现/故障注入审计无 P0/P1 blocker
- [x] 全套零 API tests 与 V1–V8 matrix 通过；V4–V8 代表归档 PASS；冻结 V3
  的既有单点 verifier mismatch 已独立复现并披露
- [x] 10 个 runtime evaluator 与 task-plan SHA 全部通过
- [ ] 独立计划审阅 GO
- [ ] 包含计划的 clean commit 已固定
- [ ] 从 `HP_V8/` 运行 formal dispatcher dry-run，manifest 的 10 个 task-plan SHA 全部一致
- [ ] 11-Key probe 通过，不输出凭据
- [ ] 同 commit smoke 完整、verifier PASS、独立结果审阅 GO 且成本门 GO
- [x] main 新目录不存在；旧 paired10 未续跑或拼接
- [ ] 启动前复核 commit/tree/revision/命令/cwd

## 9. 日志、保留与实验后

- 保存完整 result、checkpoint、metadata、dispatch、API call/attempt/journal 和清理 raw
- credential scan 后建立私有 ZIP；小型 canonical record 走 prepare/review/finalize
- 结束后至少两个新只读子代理：完整性审计、结果/失败分析；形成论文 claim 时另做边界审阅
- 更新 active_log、VERSION、transport log；有新机制事实再更新 FINDINGS/JOURNAL
- 不合并 PR；不修改冻结实验记录

## 10. 审阅记录

- verdict：`GO WITH FIXES`（实现无 P0/P1；main 启动仍受 §8 门禁约束）
- required fixes：明确 `HP_V8/` cwd 与完整 dry-run；逐项复核 Windows manifest task-plan SHA；把 smoke
  verifier PASS 与独立结果审阅设为硬门；澄清投影并定义人工费用停止规则；补完整 resume 模板。
- optional-stopping fix：每个失败 semantic root 最多一次 audited recovery（`g001`）；二次耗尽
  保持缺失，不允许 `g002+`。
- main 启动前 smoke 顺序：确认 worker 全停 → readonly verifier PASS →
  `process_experiment.py prepare --confirm-stopped` → 新上下文独立结果审阅 → 主 Agent review →
  `finalize`。只有同提交 smoke 该链路 GO 才可启动 main。
- 主 Agent 最终 GO/NO-GO：当前 `NO-GO`；clean commit、formal dry-run、Key probe 与 smoke GO 尚未完成

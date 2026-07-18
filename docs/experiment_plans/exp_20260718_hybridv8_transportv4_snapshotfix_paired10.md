# 实验计划：`exp_20260718_hybridv8_transportv4_snapshotfix_paired10`

> 用户固定的 10 样本支持/诊断实验。不得与任何旧失败 campaign 拼接。

## 1. 身份与状态

- owner：`HP_V8`
- 实验级别：支持/诊断实验
- 计划状态：`reviewed_ready_for_identity_gates`
- 创建时间：`2026-07-18T13:25:00+08:00`（Asia/Singapore）
- claim role：`diagnostic_only`
- lifecycle：400/400 完整则 `complete`，否则 `failed_informative`
- 计划作者：Codex 主 Agent
- 审阅：`/root/review_snapshotfix_plans`（新上下文、只读），verdict=`GO`

## 2. 研究问题、假设与唯一变化

- 问题：修复 live inspector 的跨文件撕裂快照后，transport-v4 sample isolation 能否在
  固定 10 样本上形成完整、可审计的 HP_V8 vs FullRewrite RT10 endpoint？
- 假设：完整 HP rows 的 preservation 总和为 0；合法 transport exhaustion 只隔离对应
  sample；严格 mapping/duplicate/half-commit 错误仍全局停止。
- 唯一变化：相对 `exp_20260718_hybridv8_transportv4_paired10` 仅 dispatcher evidence
  snapshot 改为 result→API→attempt/journal 的逆 publication 顺序，并增加回归测试。
- 不变：transport-v4 stream/R2/I3/repair 语义、HP_V8 method、FullRewrite、evaluator、
  scoring、样本、seed、task plans、distractor 与 5/5 方法顺序。
- 旧目录永久 `failed_informative`，不得 resume、复制 checkpoint/result/API evidence 或拼接。

## 3. 启动门与停止规则

- 必须使用与 `exp_20260718_hybridv8_transportv4_snapshotfix_smoke2` 完全相同的 clean
  commit/code fingerprint/revision；smoke 必须 16/16、verifier PASS、preservation 0、
  strict inspector PASS、独立只读审阅 GO，且已提交 usage ×25 的投影 `<= USD 50`。为满足
  hard same-commit/clean-tree gate，smoke 在 main 前完成 ignored prepare/review，但 tracked
  finalize/After 文档按其计划延后到 main 结束后；这不改变 smoke evidence 或 main 启动条件。
- sample-local 仅限四证一致的 R2/I3 exhaustion；保留缺失/null，其他 workers 继续。
- campaign-global：preservation>0、Git/tree/task-plan 漂移、重复/半提交、任何无法映射的
  API/ledger/result/worker provenance、未捕获 runner/local/evaluator 或共享完整性错误。
- 每失败 root 最多一次人工恢复 `g001`；二次耗尽后停止，不建 `g002+`。
- 若 commit、tree 或 transport revision 改变，禁止在此 out_dir 续跑，另建实验编号。

## 4. 固定配置与数据

- protocol/methods：`hybridpatch/8` 与冻结 FullRewrite
- transport/resume：`opencode_anthropic_sdk/4` / `exact_payload_new_semantic_call/1`
- model：`minimax-m3`，OpenCode Go，Anthropic SDK stream，adaptive，max_tokens 131072
- transport budget：每 exact semantic call R2/I3；HP repair 独立 semantic call
- seed / RT / distractor：42 / 10 / on
- samples（均历史已曝光，只作诊断）：treebank4、obj3d2、filesystem3、jobboard3、json2、
  satellite4、docker6、mathlean2、musicsheet2、circuit2
- dataset：`data/samples_delegate52`。`data/hybrid_split.json` 的 materialized split 为
  dev4=`circuit2,musicsheet2,mathlean2,docker6`，
  val3=`json2,treebank4,filesystem3`，
  unused-reserve3=`satellite4,jobboard3,obj3d2`。冻结 registry 的原始来源仍把后 6 个全部
  写作 `holdout_reserve_20260625`，未追写后续曝光；本实验明确覆盖该历史漂移并把 10 个
  样本全部视为 exposed diagnostic
- 方法顺序：按上列 1-based 位置奇数 HP-first、偶数 FR-first，合计 5/5
- generation/environment：effective temperature `1.0`；Windows + Git Bash；Python
  `3.11.9`；`anthropic==0.104.1`；requirements SHA-256
  `e1ae65933f248594e4070c368137c24e93e2c4d93f16576a2c7cff0fcfa1c519`
- frozen method identity SHA-256：prompt
  `090f427e5a1ae51b86f431a400827dad04fd714587b60acfc165ea57ebe49463`、schema
  `47a0d6db84cfd0a21af04f58d3379309a72451b4cf9f802269c62319989dbb12`、executor
  `125f68a505dd5146a38883fd76665b12b7fe6e214b75212603151606d02b80f8`、gate
  `d5836b62c3b62720028bc8bfdc3460dbbcda733f83e7ecb7dddbedfa8336cf6d`
- manifest/run metadata `/3` 必须绑定 clean commit/tree、完整 code fingerprint、revision/
  resume policy 与每 sample task-plan SHA；恢复授权另绑定 exact lineage/fingerprint/attempt
- 新 out_dir 重新生成 task plans；不得复制旧文件。预注册 SHA-256：

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

## 5. 指标与 claim 边界

- 完整性：400 rows、20 checkpoints、200 backward verifier PASS
- 主描述：10 个 sample 的 exact RT10 backward `RS@10`，只有 n=10 完整时报告；即使
  10/10 完整，也只是这组已曝光固定样本的描述性 endpoint，不是泛化或方法因果估计
- CriticalFailure：相邻 backward RS 下降 `>=0.10` 或正值归零
- token/费用：按方法和 call_kind；无 final usage 的 attempt 单列
- HP：route、profile、repair attempted/used/success、protocol failure、partial/kept-context、
  soft burden；preservation 必须为 0
- 不足 10/10 时不计算完整 paired RS@10，缺失保持 null；不把 100 个 RT 点当 iid

## 6. 命令、资源与恢复

- out_dir：`HP_V8/exp_20260718_hybridv8_transportv4_snapshotfix_paired10`
- smoke_dir：`HP_V8/exp_20260718_hybridv8_transportv4_snapshotfix_smoke2`
- 并发：10 workers；KEY_01…KEY_10，KEY_11 仅作人工恢复 reserve
- 人工授权额度：一个 400-step wave 保守预留 USD 50.00，覆盖 primary、repair、transport
  retry 与缺 final usage attempt；实现无自动费用熔断。任何 resume 前必须用 provider bill
  或可审计保守上界确认原 wave 与 proposed recovery 合计仍在额度内；否则保持 incomplete

```bash
cd HP_V8
PYTHONUTF8=1 OPENCODE_TRANSPORT=anthropic_sdk_v2 MINIMAX_TRANSPORT=opencode MINIMAX_HARD_TIMEOUT=7200 \
  python -u src/paired_campaign_dispatch.py --campaign_role main \
  --smoke_dir exp_20260718_hybridv8_transportv4_snapshotfix_smoke2 \
  --out_dir exp_20260718_hybridv8_transportv4_snapshotfix_paired10 \
  --samples treebank4 obj3d2 filesystem3 jobboard3 json2 satellite4 docker6 mathlean2 musicsheet2 circuit2 \
  --num_round_trips 10 --seed 42 --keys_file ../.env.frkeys \
  --key_labels KEY_01 KEY_02 KEY_03 KEY_04 KEY_05 KEY_06 KEY_07 KEY_08 KEY_09 KEY_10 \
  --notes 'HP_V8 transport-v4 snapshot-fix paired10' --dry_run
```

付费命令与上式完全相同，仅删除 `--dry_run`。恢复默认保持全部参数并追加
`--resume --resume_reason '<精确审计原因>' --confirm_workers_stopped`；dispatcher 只启动 latest
outcome 为 `infrastructure_incomplete` 的 sample，已提交 forward/backward 零 POST。若该 sample
原 Key 的健康状态无法确认，唯一允许的参数偏差是把它在 10 项 `--key_labels` 映射中的原标签
替换为 reserve `KEY_11`，并由 manifest/dispatch 审计记录；sample、方法顺序、task plan、模型、
seed、revision 均不得改变。已有 `g001` 的 root 不再恢复。

## 7. Preflight 与实验后

- [x] 旧 paired10 已只读审计、credential scan、私有 ZIP；没有可报告 RT10 endpoint
- [x] 跨文件竞态修复有确定性 fault-injection test
- [x] 10 个 sample 的 scaffold/runtime evaluator initial-state score=1.0
- [x] 全套零 API tests、V1–V8 replay、runtime evaluator preflight 通过
- [x] 独立计划审阅 GO
- [ ] 新 clean commit、formal dry-run 与 10 个 task-plan hashes 通过
- [ ] 11-Key probe 通过且不输出凭据
- [ ] 同提交 snapshotfix smoke 完整审阅 GO、成本门 GO
- [ ] 启动前复核 cwd、commit/tree、revision、model、seed、methods 与 distractor

结束后确认所有 workers 停止，再运行 verifier 与 process `prepare`。至少两个新上下文只读
子代理分别做结果完整性审计、结果/失败分析；主 Agent 写 `record_review.yaml` 后 finalize。
保存完整清理 raw 的私有 ZIP并做 credential scan；更新 active log、VERSION、transport log，
有新机制事实时更新 FINDINGS/JOURNAL。若 endpoint 不完整，保留 `failed_informative`，不手工
补结果或改 raw。

付费 rerun 只能说明新 commit 的 operational chain 是否完成，不能凭随机 provider 输出证明
snapshot fix 的因果性；该因果主张只由确定性并发测试支持。

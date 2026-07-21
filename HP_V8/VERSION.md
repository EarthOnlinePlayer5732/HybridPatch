# HP_V8（hybridpatch/8）版本卡草稿

状态：**active draft**。本版已完成零 API 实现、回归验证和一组 10 样本 RT10
付费诊断。最终完整 campaign 运行身份为提交
`bc4a39f7dd1476848927d45bcf1840a74677e6be`、clean tree、
`opencode_anthropic_sdk/4`；现有证据只支持固定已曝光样本上的描述性结果，不支持
方法优越性、泛化或 snapshot fix 的效果因果结论。

## 五问摘要

1. **为什么迭代**：V7 的主提示无条件包含文件表、三层索引和四条完整路径，repair 又重复全部 schema、全部文件和原始响应；初版 V8 又让分类器关闭 local/bulk，并把经验 P95 当成硬拒绝，可能损害任务完成能力。
2. **优化方向**：性能优先于 token 成本。词法分类只控制 coarse 块索引和 `dsl_rules` 是否出现，不能关闭 local/bulk；V8 plan 与 repair 继续精简，但协议负担阈值只作软 telemetry。
3. **当前证据**：冻结 dev20 的 400 步零 API 重放显示 primary 字符数
   `27,459.715→20,662.020`（-24.755%），反事实 repair
   `20,009.222→16,044.667`（-19.814%）。最终付费诊断完成 400/400 行；exact
   backward RT10 为 HP `0.745178`、FR `0.695854`、配对差 `+0.049324`
   （n=10，4 正/4 负/2 平），CriticalFailure 为 `4/90` 对 `8/90`。
4. **新问题**：样本异质性很大，`mathlean2` 上 HP 比 FR 低 `0.574488`；HP
   protocol kept-context 为 `11/200`，repair 成功采用 `21/32`。10 个已开始生成的
   transport attempt 缺 final usage，因此归档只能给出已知 usage 费用而非精确账单。
5. **下一轮靶子**：优先定位 `mathlean2` 集中的 schema/gate/op rejection，而不是继续
   压缩提示字符；任何新方法实验使用新编号和未曝光样本。本次断电恢复只作为经过审计的
   campaign continuation 证据，不扩展为普遍恢复规则。

## 唯一研究假设

> 精简无用提示、协议和修复重复，并按需加入 coarse 块索引与 DSL，可以降低模型负担，同时保留完整有效路径、现有执行能力与保留语义；经验负担阈值仅用于观测。

本轮只有这一项研究假设。V8 沿用 V7 的执行器、C1/C2、partial acceptance、preservation、gate 格式、no-op 与 FullRewrite 语义；不引入第二项方法假设。

## 来源与复制验收

- 来源提交：`8cf5aff40a8c098d64013bb29ca4ddb921e95fe4`。
- 来源快照：该提交的 `HP_V7/src/`、`HP_V7/prompts/`、`HP_V7/requirements.txt`，共 91 个 tracked 文件。
- 创建方式：`git archive` 从上述提交按原字节提取；创建后逐一 `cmp` 91 个相对路径，结果为 `SNAPSHOT_COPY_PASS files=91`，再开始 V8 修改。
- 来源 Git 对象：`src` tree `3b93cfa1761a87c7025684417cf81d5c89c2741f`；`prompts` tree `a5b54d9aa65c31b43646483d90bcff53844d2a5b`；`requirements.txt` blob `5c2e7c14c2c2ebab7df1073f8af3634c2b9d5798`。
- 未复制：V7 的 `exp_*`、`records/`、`EXPERIMENTS.md`、`VERSION.md` 和 `*.pyc`。
- `data/`：单独建立指向顶层共享 `data/` 的 Windows junction；不进入 Git。
- 冻结边界：HP_V3–HP_V7、Baseline、transport、顶层 data、domain evaluator、scoring 与冻结实验记录均未修改。

**验收等级：来源快照为字节级确证；付费链路可运行且可重放，V8 方法效果未确立。**

## 协议与提示

- 协议：`hybridpatch/8`；提取器继续接受 `hybridpatch/1`–`hybridpatch/7`。
- 分类器：`operation_family_lexical/1`，执行 NFKC、casefold 与空白归一化，只使用固定领域无关词表；优先级为 split → merge → sort → group → block_movement，并记录全部命中。block movement 必须同时命中移动动词与通用结构/位置词。
- `default` profile：只给出 `local_patch`、`bulk_patch`、`bounded_rewrite`；不构建或输出任何文件表、块表、中粒度或细粒度索引。
- `block_movement` profile：在 default 三路径基础上加入 coarse `split_struct2` 块索引与 `dsl_rules`，即同时提供 local、bulk、DSL、bounded；不构建 medium/fine 索引，不输出文件信息表。
- `bounded_rewrite` 始终是模型必须显式选择的安全路径，执行器不静默回退。
- repair 使用 primary 的同一 `prompt_classification`。合法 route 的普通错误仍只显示当前路径；无合法 route 时恢复该 profile 的全部路径，并逐条绑定正确的 `edit_footprint`。协议负担超阈值本身不触发 repair。
- repair 只要允许 `bounded_rewrite`（单一路径或可选路径）就恢复全部 editable 正文；其余 local/bulk/DSL 单一路径继续按 action、错误与 target 筛选。只读文件仍只提供权威名称，不提供正文。

V8 plan 恰好包含两个字段：

| `edit_footprint` | `action.route` |
|---|---|
| `few_precise_edits` | `local_patch` |
| `many_repeated_edits` | `bulk_patch` |
| `block_movement` | `dsl_rules` |
| `whole_file_change` | `bounded_rewrite` |

每个 V8 local op 必须声明已知、非空 `file`；每个 V8 bulk op 必须声明非空且全部属于 runner editable 边界的 `scope`。未知 scope 在执行前返回明确 schema error，不允许执行器过滤后继续。授权边界检查只对 V8 生效；软负担 telemetry 不改变 V1–V7 的 legacy plan、缺省范围和 replay 分支。

## 经验协议负担阈值（软记录）

数据源：`HP_V7/exp_20260711_hybridv7dev20full` 的 400 行。成功子集为 `bdpatch.actual_method == "hybridpatch"` 的 394 个 chosen V7 信封，包含 5 个 partial acceptance，排除 6 个 kept-context。分位数使用 nearest-rank 条件 P95。

| 指标 | V8 经验阈值 | V7 条件 P95 | V7 max |
|---|---:|---:|---:|
| local ops | 31 | 31 | 92 |
| bulk ops | 30 | 30 | 32 |
| anchor UTF-8 bytes | 4,080 | 4,080 | 17,512 |
| canonical envelope UTF-8 bytes | 2,898 | 2,898 | 20,109 |
| explicit block IDs | 39 | 39（n=1） | 39 |

严格大于任一经验阈值时，runner 与 executor 记录 `protocol_burden_exceeded=true`，并保存每个超阈值指标的 `actual` 与 `threshold`；信封继续完整执行，不产生 schema error、不触发 repair、不截断，也不强制换路。共享统计函数按 V4+ 规则解引用 `@body:` 后累计锚点；canonical envelope 使用 `ensure_ascii=False`、`sort_keys=True` 与 compact separators。

为避免另一路径以协议长度硬拒绝 V8，`hybridpatch/8` 也不再应用旧 DSL 的 16 rules / 64 explicit IDs / 128 expanded actions 数量上限；这些上限对 `hybridpatch/1`–`hybridpatch/7` 仍按历史语义校验。DSL 的字段、block ID 存在性、目标文件和 preservation 等正确性检查保持不变。

完整零 API 数据与局限见：

- `analysis/protocol_burden_v7_v8.json`
- `analysis/protocol_burden_v7_v8.md`

## 零 API 路径兼容审计

- V8 profile 分布：`default=184`、`block_movement=216`。
- 400 步中 399 步存在可解析 V7 chosen route；1 步 route unavailable，单列且不进入不兼容分母。
- 兼容 398/399；不兼容 1/399（0.25%）。成功提交子集不兼容 1/394（0.25%）。
- 唯一不兼容为 `makefile4` RT2 forward：default profile 下历史 V7 chosen route 为 `dsl_rules`。
- JSON 报告保存逐步结果以及按 matched term、sample、round trip、direction 稳定选取的代表案例；Markdown 报告提供人类可读摘要。

该审计只判断历史 V7 chosen route 是否被反事实 V8 profile 列为 allowed route，不判断路径语义等效，不预测模型在 V8 下的选择，也不是效果实验。

## 运行记录

`bdpatch.hybrid` 记录 prompt profile/分类命中、prompt 与 repair 字符数、信封字符/字节数、local/bulk/显式操作数、锚点字节、显式块编号数、实际 touched target 文件数和文档大小比率。`protocol_burden_exceeded` 表示 primary/repair 任一 attempt 超阈值；`protocol_burden_overages` 描述 chosen envelope，`protocol_burden_attempt_overages` 按 attempt 保存每项 `actual/threshold`。

run metadata 升级为 `anchorpatch.run_metadata/3`，在锁内记录并复用 campaign 的 `run_git_commit`、`git_tree_state`、带时区 `started_at` 与 `finished_at`，拒绝跨 commit、tree state、代码指纹、campaign config、task-plan hash 或 transport 混跑。配对 dispatcher 为每个 sample 持有进程期租约，并以 active-worker set、PID、task-plan start barrier 和先落盘 authorization 后发布 ACK 的顺序阻止未授权调用；journal replay 按 semantic-call identity 审计而不误算为第二次 provider POST。first-writer-wins stop latch 在 preservation、代码/计划漂移或 campaign 停止后阻止新的语义调用；fail-fast 后只有在租约释放且 worker/PID provenance 可审计时才关闭遗留 `running` invocation。任何已有 preservation violation 会在恢复启动 worker 前拒绝 campaign。

付费 dispatcher 固定 smoke 为 2 样本×2RT（16 行）、main 为用户指定 10 样本×10RT（400 行）；main 必须先严格复核同提交 smoke 的完整性、路径身份、exact final backward 可计分性、preservation tagged union、API/worker provenance 和 16/16 USD coverage，再按固定 `400/16=25` 投影。投影 `<= USD 50` 才可读取 Key 并启动 worker；该门禁是启动前成本估算，不是运行时账单上限。全新 campaign 在尚无 committed rows 时允许 checkpoint 尚未创建；一旦存在 committed rows，checkpoint 缺失或类型非法仍由 preflight 拒绝，完成态继续要求精确 RT 数和行数。

正式 paired 分析以每个样本 exact final backward `RS@K` 为单位，输出 `sample_level_final_endpoint.json`；sample×RT 轨迹只作 descriptive，不进入 canonical iid 推断。

## Transport-v4 与 paired campaign 故障隔离（2026-07-18）

本轮不改变 `hybridpatch/8` 方法协议、提示、执行器、validation gate、preservation、
FullRewrite、evaluator 或 scoring。新实验改用 `opencode_anthropic_sdk/4`；冻结
transport-v3 归档继续只按其历史规范解释，旧
`exp_20260717_hybridv8_softbudget_paired10` 保持 `failed_informative`，不得续跑或拼接。

- 流完整性判断先于普通 HTTP 状态：Anthropic `APIStatusError(status=200,
  "Streaming response failed")`、缺 `message_stop`、缺终结 usage、block 未闭合或
  terminal 事件重复/乱序统一归为 retryable `incomplete_stream`。
- 每个 exact semantic call 固定两个生成 response slot 与三个生成前 transient
  failure；出现实际生成 delta 的中断消耗 response slot，无生成 delta 的 retryable
  失败消耗 transient failure。总 HTTP attempt 不硬限为两个。
- transport retry 保持完全相同 prompt/参数/fingerprint；HybridPatch repair 仍只由
  合格的完整模型内容协议/执行错误触发，使用独立 `call_kind`、semantic root 和 R2/I3。
- 恢复 lineage 使用 `semantic_root_id/gNNN`。dispatcher 四字段授权 parent 的下一
  generation、同 request fingerprint 和连续全局 attempt index；授权一次性消费。
  恢复前允许零 POST journal replay 未提交的 forward/primary，恢复后普通新 root
  不受旧授权阻塞。
- semantic-root 独占锁覆盖 preflight、provider 调用、journal/raw、attempt terminal
  与 API terminal row；recorder 内 journal 已落盘而 ledger terminal 未落盘的 window
  可确定性本地 replay，身份不一致则在 POST 前 fail closed。若 formal worker 已在
  response commit 后、API terminal row 前退出，仍作为半提交 API evidence 全局停止，
  不伪装成 sample-local provider exhaustion。
- 只有 outcome、run metadata、API row、attempt ledger 四证一致且确实耗尽 R2/I3
  的 worker 才标为 `infrastructure_incomplete`；未完成 endpoint 保持 missing/null，
  其他 sample 继续。preservation、Git/tree 漂移、重复或半提交 RT、ledger 无法映射、
  非 retryable provider 错误、本地/runner/evaluator/shared-integrity 错误仍全局停止。
- provider 返回后与 relay commit 前都会复核 latch、active worker、Git/tree 和 task-plan；
  result rows/checkpoint commit 与 stop latch 使用同一 ordering lock。全局回收只有在
  sample lease 已释放且 metadata/exit provenance 可审计时才撤销 active set。
- audited resume 只创建 latest incomplete sample 的 worker，从 checkpoint 第一个未提交
  RT 开始；已提交 RT 不产生新 provider POST，旧 failure/ledger/raw 全部 append-only。

活动 transport 规范见 `transport/docs/API_TRANSPORT_V4.md`。本轮新的 smoke 与 paired10
均为 `diagnostic_only`，必须在独立计划审阅、全部零 API gate、干净提交与 Key probe
通过后方可启动；任何不完整 main 不产生完整 10 样本方法效果结论。

### Transport-v4 零 API 验证

2026-07-18 在 Git Bash 中完成；实现回归未读取 Key，formal dry-run 仅加载本地 Key
映射做零 API 校验且未输出 Key 值、未调用 provider：

1. `HP_V8/src`、`transport/src`、`tools` 共 98 个 Python 文件编译到临时
   `PYTHONPYCACHEPREFIX`：PASS。
2. `python -B ./src/test_hybrid_executor.py`：72/72 PASS；含 V1–V8 envelope matrix、
   V7 legacy range/budget、snapshot/body-ref/C2/partial parity。
3. `python -B ./src/splitters.py`：全部 splitter byte-exact PASS。
4. `python -B ./src/test_model_openai.py`：84/84 PASS；覆盖 HTTP-200 streaming
   failure、delta/no-delta R2/I3、缺/乱序 terminal、空白 stop reason、transport/repair
   分离、multi-worker isolation、preservation global stop、stop-latch/backoff 零 POST、
   active-set audit/CAS、只恢复 incomplete sample、committed RT 零 POST，以及全新 campaign
   dry-run 写入显式空 active-worker set 后的真实 inspector 路径。
5. `PYTHONPATH=../HP_V8/src python -B ./src/test_model_openai.py`（`transport/`）：
   47/47 PASS。
6. `analyze_protocol_burden.py`：400 rows、394 success、25 个历史成功软阈值并集
   超限形态、路径不兼容 1/399；仅分析/telemetry，不影响执行。
7. `test_analyze.py`：5/5 PASS；`tools/test_process_experiment.py`：9/9 PASS。
8. V8 verifier 只读 replay：V4 smoke 70、V5 dev20 400、V6 dev20 400、V7 dev20
   400、V8 smoke 8、V8 failed-informative partial 96 个 backward RS 均 PASS。
   冻结 V3 dev20full 由其自身 V3 verifier 与 V8 verifier 都稳定报告同一历史 mismatch：
   `quantum4 RT9 stored=0.9850, recomputed=1.0000`；因此不把 V3 归档写成 PASS，也不
   修改冻结证据。V1–V8 executor matrix 仍全部通过，说明本轮 transport 变更没有改变
   envelope replay 分支。
9. 用户固定 10 样本 initial runtime evaluator 均 `score=1.0`；seed42/10RT task plan
   状态引用与预注册 SHA-256 全部匹配。
10. 范围审计：HP_V3–HP_V7、Baseline、data、records/exp archives、HP_V8 prompt、
    protocol schema、executor、gate、FullRewrite、evaluator 与 scoring 无 diff。
11. 新上下文只读实验计划审阅在补齐 cwd/dry-run、2RT task-plan SHA、smoke verifier 与
    独立结果审阅门、费用停止规则、完整 resume 模板及每 root 最多一次 `g001` 恢复后，
    返回 plan-level GO；API 仍等待 clean commit、formal dry-run 与 Key probe。

`HP_V8/src/model_openai.py` 与 `transport/src/model_openai.py` 原字节相同，SHA-256
均为 `cdfe85e9f48b81d16ff85857d51b97db09b559877675456708838959ff15fd93`。
HP runner 扩展版 `run_meta.py` SHA-256 为
`d73c5da525b49fb9776b1e155f0dae12147832695b85fef7c43cc211b1bca45c`；transport
preflight 版为 `f9f63d4776ab7c39399256f2e16a15c3909ad0390c0ea34dd41494e655ec0da5`。

## 当前 14 项指纹

| 文件 | SHA-1 前 12 位 |
|---|---|
| `patch_schema.py` | `ba8a9eb35f06` |
| `splitters.py` | `690b1981879f` |
| `experiment_runner.py` | `519d7382e097` |
| `hybrid_schema.py` | `272b74dec9bc` |
| `hybrid_index.py` | `e772551601bc` |
| `hybrid_prompt.py` | `3fd2b4717260` |
| `hybrid_executor.py` | `e796e08e2648` |
| `hybrid_gate.py` | `255c4299b755` |
| `model_openai.py` | `5f2fac4da655` |
| `run_meta.py` | `94f37d069dd9` |
| `requirements.txt` | `38ffe361be94` |
| `verify_anchorpatch.py` | `310fd2ee8ed5` |
| `paired_campaign_dispatch.py` | `36c3fe3b51b9` |
| `analyze.py` | `32178dc554c5` |

## 零 API 验证（2026-07-17）

工作目录为 `HP_V8/`，所有命令由 Git Bash 执行：

1. 全部 85 个 `src/**/*.py` 编译到临时 `PYTHONPYCACHEPREFIX`：PASS；源码目录未生成 `__pycache__`。
2. `PYTHONUTF8=1 python -B ./src/test_hybrid_executor.py`：`RESULT: PASS (71 tests)`。
3. `PYTHONUTF8=1 python -B ./src/splitters.py`：`RESULT: PASS (all splitters byte-exact coverage)`。
4. `PYTHONUTF8=1 python -B ./src/test_model_openai.py`：51 tests，PASS；覆盖 semantic journal replay、duplicate provider POST、worker lease/cohort authorization barrier、stop latch、audited stale closure、全新 campaign checkpoint preflight、strict campaign identity、exact integer/score/preservation 校验、smoke 成本门和 preservation fail-fast 时序。
5. `PYTHONUTF8=1 python -B ./src/analyze_protocol_burden.py`：400 行、394 个成功信封、25 个软阈值并集超限形态、1/399 个路径不兼容，PASS。
6. `PYTHONUTF8=1 python -B ./src/test_analyze.py`：5 tests，PASS；formal analyzer 严格绑定 manifest/sample/method，只接受有限数值或明确 evaluator error，并要求 exact final RT 的完整 sample-level endpoint。
7. `PYTHONUTF8=1 python -B ../tools/test_process_experiment.py`：9 tests，PASS。
8. 用 V8 verifier 只读重放冻结 V7 dev20：`HONESTY GATE: PASS`，400 个 backward RS 全部从 raw response 复现。
9. 用户指定 10 个样本的 initial-state runtime evaluator 与 seed-42/10RT task-plan preflight：全部 score=1.0、plan 长度与状态引用有效。

## 付费诊断（2026-07-17）

所有调用使用 MiniMax-M3、`opencode_anthropic_sdk/3`、seed 42、
distractor-on 和冻结 task plans。11 个 Key 的最小探针为 11/11 可用，共 2,302 provider
tokens；Key 值未写入日志。

### `exp_20260717_hybridv8_softbudget_smoke2`

- 运行提交与 tree：`e6abad4449f7c2536c914725586fb44094546d76`，`clean`。
- 2 样本 × 2 RT × 2 方向 × 2 方法完整提交 16/16 行；17 个 API call =
  16 primary + 1 HP repair；verifier PASS 8/8 backward。
- exact backward RT2：HP `0.500000`，FR `0.998361`，配对差
  `-0.498361`（n=2）。HP severe failure `1/2`，FR `0/2`。
- HP/FR provider tokens 为 `426,989/316,543`，估算费用
  USD `0.388963/0.276923`；总费用 USD `0.665886`，×25 启动投影
  USD `16.6471 < 50`。
- HP route 全为 `bounded_rewrite=8/8`；profile
  `block_movement=4/default=4`；repair attempted/used/success
  `1/1/1`；protocol failure `0/8`；soft burden `0/8`；
  `preservation_violations=0`。
- `obj3d2` 的 0 分同时包含 evaluator 对全局 OBJ 数组布局的敏感性和真实 UV
  round-trip 损失；不排除、不归为单一方法或 evaluator 原因。完整性审阅 PASS，
  lifecycle=`complete`、evidence=`diagnostic`。
- 官方 prepare/finalize 与两个 record validator 均曾 PASS；生成的 canonical record、
  当时的 catalog/index 快照和完整 raw 一并收入私有 ZIP。为遵守本轮冻结边界，
  PR 不提交会连带改写 HP_V3–V7/Baseline/transport overlay 的全局登记。
  ZIP：`../hybridpatch_private_archives/exp_20260717_hybridv8_softbudget_smoke2__e6abad4.zip`，
  SHA-256 `378d160dd25efb38e857983903dac248afd559b057abb7d0add63bdbe6ad5dd6`。

### `exp_20260717_hybridv8_softbudget_paired10`

- 同提交与配置启动后，`hybridpatch/musicsheet2/rt02/backward` 遇到 provider
  `APIStatusError: Streaming response failed`。dispatcher fail-fast 停止并回收全部 worker。
- 192/400 结果行已原子提交（两臂各 96；两臂各 48 backward），checkpoint、API
  定位与行身份一致；verifier PASS 96/96 backward；`preservation_violations=0`。
- 冻结 ledger 已把该 semantic call 写成 terminal `call_failed`。换 KEY_11 也会在
  provider POST 前拒绝，故未尝试无进展 resume。campaign 定性为
  `failed_informative`。
- formal analyzer 按设计拒绝：exact RT10 endpoint 仅 n=1/10，因此不存在可报告的
  10 样本 RS@10（用户所称 20 edit-step endpoint）。唯一完整 pair 是 docker6，
  HP/FR `0.982759/0.928855`，不得外推。
- 部分轨迹 severe failure HP `2/41`、FR `0/42`；HP route
  `bounded=55/local=30/bulk=6/dsl=5`，protocol failure `1/96`，
  repair attempted `12/96`、used 9、success 8，soft burden `7/96`，
  preservation 0。
- 已提交结果 usage：HP/FR `3,318,844/2,448,339` tokens，
  USD `2.462493/1.790758`。含已记录 repair/failure call 的 API ledger 下界为
  `3,545,711/2,635,378` tokens、USD `2.681167/1.995007`；8 个终止中的
  generation-progress call 无最终 usage，真实账单可能更高。
- 失败摘要：`exp_20260717_hybridv8_softbudget_paired10/analysis/failure_summary.md`。
  私有 ZIP：`../hybridpatch_private_archives/exp_20260717_hybridv8_softbudget_paired10__e6abad4__failed_informative.zip`，
  SHA-256 `08eba3ee36b2c63df5e58b7d0c5796e3ba4b5409ecc1476e9bf4cf4b83d6c694`。

两个 ZIP 均通过解压测试；对本机识别的 14 个 secret 值，精确命中为 0，
启发式凭据标记为 0。smoke 是 operational evidence，不是方法效果实验；
paired10 没有完整 endpoint，也未 finalize 为 canonical 结果 record。

## 2026-07-18 transport-v4 因果快照修正

首轮 v4 smoke `exp_20260718_hybridv8_transportv4_smoke2` 在提交
`6f404de635cc97883736dbccb34c0358ec3f005f` 完成 16/16 行、verifier 8/8、
preservation 0；随后同提交的 `exp_20260718_hybridv8_transportv4_paired10` 在
124/400 行时严格报告 HP/json2 RT7 backward 无 API 映射并全局停止。最终 evidence
实际包含该步唯一 API terminal row、`response_committed` ledger、journal 与 result；
API 比 result 早约 88 ms 落盘。最终证据、当时错误与旧读取顺序支持 torn-snapshot
解释，确定性并发 fault injection 建立因果。

dispatcher 的 live inspection 现按固定 writer publication 链
`ledger/journal → API → result/checkpoint` 的逆序先缓存 result，再读取 API、attempt 与
journal。所有 schema、lineage、mapping、duplicate、partial/half-commit、checkpoint 和
preservation 判定保持原样。另增强 global-integrity 测试，证明 durable stop latch 在 worker
terminate/reconcile 前可读且终止失败后仍保留。transport wire、HTTP incomplete 分类、R2/I3、
repair budget 与 revision 字符串仍为 `opencode_anthropic_sdk/4`；本次只修 dispatcher evidence
snapshot，不创建新的方法版本。

零 API 验证：85 个 tracked `src/**/*.py` 编译 PASS；HybridPatch executor 72/72、splitter
byte-exact、HP transport/dispatcher 85/85、transport-core 47/47、analyzer 5/5、process tool
9/9 全部 PASS。V4/V5/V6/V7/V8 归档分别只读复现 70/400/400/400/8/96/8/62 个 backward
RS，全部 `HONESTY GATE: PASS`；用户固定 10 样本 initial runtime evaluator 均 score=1.0。
零 API burden 报告仍为 400 rows、394 success、25 soft-overage union、1/399 path incompatible。

失败 main 永久为 `failed_informative`，无完整 paired sample，不报告 RS@10。official prepare
在 canonical endpoint n=0/10 处按设计停止，未伪造 record。1,251 个文件的
credential scan 对 11 个本机 Key、Authorization、Cookie 与 private-key marker 命中均为 0；
私有 ZIP SHA-256 为
`ad3a56972def73ad550db32d4835a4802b14719ad758fb4d5fdf85df9a708407`。
修正后的付费 smoke/main 已分别预注册为
`exp_20260718_hybridv8_transportv4_snapshotfix_smoke2` 与
`exp_20260718_hybridv8_transportv4_snapshotfix_paired10`。

## 2026-07-18 snapshotfix paired10 完整诊断与断电续跑

`exp_20260718_hybridv8_transportv4_snapshotfix_paired10` 在 clean commit
`bc4a39f7dd1476848927d45bcf1840a74677e6be` 上运行 MiniMax-M3、transport-v4、
seed42、distractor-on、固定 10 样本和交替方法顺序。主机断电时目录已有 362/400 行：
7 个样本完整，`filesystem3`、`musicsheet2`、`satellite4` 各有未完成后缀。按用户明确
要求，本次没有新建 campaign 或重跑完整样本，而是先封存 362/400 快照，再只从三个
checkpoint 的首个未提交 RT 续跑。

断电留下的三条 HTTP attempt 都已见 generation delta，但缺 `message_stop`、final usage，
且 content block 未闭合。恢复把它们显式追加为 retryable `host_power_loss`，各消耗一个
response slot；attempt2 保持相同 prompt、参数、request fingerprint、semantic call 和
call ID，并写入新的 raw transport 文件。`filesystem3` 与 `musicsheet2` 直接完成；
`satellite4` 的 g000 attempt2 再次不完整，按样本级隔离先写
`infrastructure_incomplete`，其后只启动该样本并使用唯一授权的 g001。断电前三个样本
已经提交的 82 行在恢复期间产生 0 次 provider POST；三个 journal replay 也明确记录
`provider_called=false`。恢复 helper 位于工作树外，运行代码、方法指纹、Git commit 和
tree 身份均未改变；helper SHA-256 与 append-only ledger hash 保存在
`analysis/powerloss_recovery_prepared.json`。

最终完整性：400 个唯一 result key，forward/backward 各 200；20 个 checkpoint 均
RT10；10 个 latest outcome 均 `finished`。strict inspector 为 `errors=[]`、436 API rows、
433 semantic calls、442 HTTP attempts；verifier 从 raw 独立重算 200/200 backward RS
并 PASS。证据 digest 在续跑结束、prepare 前和独立审计时均为
`6e0750f7779ee6c20d4c11c4d0f15a5c84c52c02472e0e0d15e25d0f217de3f2`。

描述性 exact backward RT10：HP `0.745178`、FR `0.695854`、paired delta
`+0.049324`、sample SD `0.344047`，4 正/4 负/2 平。CriticalFailure@0.10 为 HP
`4/90`、FR `8/90`。这 10 个样本全部已曝光，且删除 `filesystem3` 后平均 delta 变为
负值，因此不得表述为方法普遍优越或 snapshot fix 的效果因果证据。

HP 的 199 个声明 route 为 bounded/local/bulk/DSL=`124/54/14/7`，另 1 个完整
thinking-only max_tokens 空响应没有合法 route；profiles 为 block/default=`117/83`。
repair attempted `32/200`，成功且采用 `21/32`；最终 kept-context protocol failure
`11/200`；软 burden 超限 `12/200` 且全部继续执行。preservation 为 199 个适用步骤
0 violations，空响应 kept-context 步为明确 1 个 N/A。

有 final usage 的 432 个 response 中，HP（含 repair）为 8,448,270 tokens、
USD 7.036488；FR 为 6,660,571 tokens、USD 5.571257；合计 15,108,841 tokens、
USD 12.607745。另有 7 个 `incomplete_stream` 与 3 个 `host_power_loss` generation
attempt 缺 final usage，故这些数字是可审计的已知 usage/费用，不是精确 provider 账单。

official prepare、双人只读完整性/结果审阅、finalize 和 records validator 在生成态均
PASS。由于全局 catalog hash 会连带改写 HP_V3–HP_V7、Baseline 和旧 transport 的冻结
record，生成 record 只作私有快照，不提交全局登记；Git 中的脱敏诊断报告为
`analysis/exp_20260718_hybridv8_transportv4_snapshotfix_paired10.md`。完整私有 raw 归档为
`../hybridpatch_private_archives/exp_20260718_hybridv8_transportv4_snapshotfix_paired10_complete.tgz`
（SHA-256 `d24f416498c754d0be317d02c5b8f4916a7f21b4eb558c8e6d64184a89834508`）；
恢复 helper 归档 SHA-256 为
`245001818ba8afaa8974f6b48ff6b7f42ac13a17fdb6c030dcd1f2b09f8522f1`。
完整归档的 3,754 个成员经 11 个本机 Key 精确值及 Authorization/Cookie/private-key
marker 扫描，命中均为 0；Key 值未打印。

最终零 API 回归：98 个 Python 文件编译 PASS；executor 72/72、splitter byte-exact、
HP transport/dispatcher 85/85、transport-core 47/47、analyzer 5/5、process tool 14/14；
burden 报告 400 rows/394 successes；V7 dev20 和本 campaign 分别只读 replay 400/200
backward RS，均 PASS；observed protocol revisions 仅 `hybridpatch/8`；`git diff --check`
与 `validate_experiment_records.py --records-only` PASS。完整 source-linked validator 仍报告
一个当前 HEAD 已存在的历史错配：冻结 transport-v2 record 保存的是 5,184-byte 旧 transport
log 哈希，而 HEAD 的同一日志已演进为 7,583 bytes。为不改写冻结记录，本轮只披露，不修补。

## 2026-07-19 supplement40 lockfix 完整补充诊断

首次 `exp_20260719_hybridv8_transportv4_supplement40` 在 40-worker 启动后因本地
`portalocker.AlreadyLocked` 严格停止，且尚无 result、checkpoint 或 response commit。
该目录保留为 `failed_informative`。新提交
`effad42675b6d112c62e42d695617166e27f989c` 只把原子 stop-latch 读取改为无锁，并为
metadata writer 使用 60 秒有界重试锁；`hybridpatch/8`、prompt、executor、gate、
FullRewrite、evaluator、scoring 和 transport-v4 请求/重试语义不变。

替代 campaign `exp_20260719_hybridv8_transportv4_supplement40_lockfix` 在 13 个存活 Key
上按每 Key 最多四 worker 运行固定 V6 val40 的 40 个已曝光样本，HP-first/FR-first 各 20，
MiniMax-M3、seed42、distractor-on、10RT。最终 1,600/1,600 result rows、80/80 RT10
checkpoints、40/40 latest outcomes finished；raw 独立复算 PASS 800/800 backward，strict
inspector `errors=[]`。12 次 incomplete stream 均在 g000 第二 response slot 恢复，terminal
infrastructure failure 为 0；preservation 为 797 个适用步骤 0 violations + 3 个明确 N/A。

固定 n=40 的 RT1 HP/FR=`0.962799/0.911714`，RT10=`0.837339/0.573381`，paired delta
`+0.263959`，sample SD `0.402060`，W/L/T=`26/10/4`，CF@0.10=`20/360` vs
`27/360`。HP routes bounded/local/bulk/DSL=`579/148/54/16`；repair attempted/used/success
=`104/88/78`；final protocol failure=`20/800`；soft burden=`49/800`，仍只作 telemetry。
完整 RT1–RT10 以及 prior10、50-chain、去重 44-ID 表见
[`analysis/exp_20260719_hybridv8_transportv4_supplement40_lockfix.md`](analysis/exp_20260719_hybridv8_transportv4_supplement40_lockfix.md)。

已知 usage 为 HP 33,061,891 tokens / USD 26.581893、FR 25,796,080 / USD 21.104516；
12 个缺 final usage attempt 的保守附加上界 USD 2.261795，因此本轮 archive-based 上界
USD 49.948204。所有 44 个唯一 ID 都已曝光，六个 ID 跨 campaign 重复；这些表只作
diagnostic，不支持 holdout 泛化、普遍优越、统计显著性或 lockfix 因果得分结论。

official prepare、两项独立只读审阅、生成态 finalize 和两个 validator 均 PASS。为维持冻结
边界，catalog/HP_V3–HP_V7/Baseline/transport generated records 随后恢复原字节；生成 record
私有快照 SHA-256 为
`803d78492e0aa9e308b8263c38668800a5ca7c99dd553aa3350a60b0ef2588d0`。最终完整 raw archive
SHA-256 为 `79d5573ea606544559c68f6630df1c1f90de4679642bcec5f038f9e0e28b9c10`；13-Key
exact scan 在 15,264 文件、951,396,234 bytes 中发现零 Key、Authorization、Cookie 或
private-key 匹配。最终 `--records-only` validator 对 13 个 published records PASS；完整
source-linked 模式因 active research docs 已前进、而冻结 record 仍保存旧 source hash，报告
21 个预期 stale-hash errors，本轮按冻结边界披露而不改写。

## 2026-07-19 method-unseen confirmation68 预注册

HP_V8 方法在本轮保持冻结：`hybridpatch/8`、prompt、plan/action schema、executor、
validation gate、partial acceptance、preservation、FullRewrite、transport-v4、evaluator 和
scoring 均不修改。新增代码只负责零 API 选择/运行前 evaluator 检查、百分制派生报告，以及
paired dispatcher 的 confirmation selection gate 和同一 campaign 内有界波次；不改变任何
模型请求或编辑语义。

零 API 全历史扫描得到两种必须同时披露的口径：冻结 FullRewrite baseline 已留下
234/234 的真实 provider request/response，所以绝对 provider-unseen 为 0；沿用仓库既有
holdout policy、把 FR-only control 与 HP/开发曝光分开后，registry contaminated、sealed
dev/val/test/unused 和扫描到的 HP 证据先排除 148。2026-07-03 已 live-tested 的 sealed test20
明确不能回流；registry `clean_candidate` 有 86 个，但 `python1` 已有逐文件、依赖和 evaluator
的开发内容调查，故额外排除。余下 85 个与 sealed/HP/已知内容级开发曝光零交集，且当前
evaluator runtime-runnable 234/234。按用户规则 `N=85<100`，seed42 对 domain、format、
semantic operation/task type、file count 和 document length 分层，API 前固定 selected68 与 reserve17。
selection artifact SHA-256 为
`6c1a4331ede14adadec660a76df221be32fd855dc07e6b7ec1fc19397e4f0214`；计划见
[`docs/experiment_plans/exp_20260719_hybridv8_transportv4_method_unseen_confirmation68.md`](../docs/experiment_plans/exp_20260719_hybridv8_transportv4_method_unseen_confirmation68.md)。
相同输入二次重建保持上述 selection SHA 不变；selected68 fresh-subprocess 真实 runner evaluator
preflight 为 68/68 runtime-runnable、0 failure，报告 SHA-256
`cde8c45e26c72c7c9a4469bc23bb2718fff33f77dcdcac3fdb216bfaf6489c0f`。

正式配置预注册为 MiniMax-M3、transport-v4、seed42、distractor-on、HP_V8 对
FullRewrite、68 samples × 10RT、34/34 方法首发平衡、13 Keys 且每 Key同时最多四个 paired
workers。dispatcher 在一个 immutable manifest 下使用 52+16 两个 wave；sample-local
基础设施失败不终止其他样本，preservation 或共享完整性失败仍全局停止。首次 API 调用前仍
需通过完整零 API 回归、fresh-context 计划审阅、clean commit、formal dry-run、68 个 task
plan 冻结和 13-Key 最小完整流 probe。本节是预注册状态，不包含任何效果结论；reserve17 不在本轮
自动运行，结果也不得用于修改 HP_V8。

## 2026-07-19 mixed confirmation100 预注册

用户在 API 前明确替换了 confirmation68 的范围：新正式实验固定 60 个未参与 HP 方法开发/
HP 实验的样本，加 40 个参与过 HP 方法或开发、但未进入最近 paired10 与 supplement40 的
样本。旧 confirmation68 仅保留为零 API 审计，不启动 provider 调用。

新实验编号为 `exp_20260719_hybridv8_transportv4_mixed_confirmation100`。seed42 分层得到
method/developer-unseen `60/81`、historical-exposed `40/109`，combined selected100/reserve90；
与最近两轮 44 个 unique sample 零交集。历史候选中只有 17 个带最近两轮之外的直接 HP API
证据，17 个全部纳入；其余 23 个来自方法开发/封存曝光池。source-level self-test 审计把
`json1`、`molecule1`、`obj3d1`、`starcatalog1` 从 unseen 池移入 exposed 池。冻结 FR baseline
已调用全部 234 样本，所以本实验明确不是 absolute provider-unseen。

冻结 selection SHA-256 为
`26da1c3d27a1eb83d70af444197afde7a7f20ade20c6e232f2d6abe605d3d654`，selector SHA-256 为
`8ed6da89d722021438db71a9b288bc520a2dee710173094b7dd9c9a60c2cf3ec`。selected100 的
fresh-subprocess evaluator preflight 100/100 runtime-runnable、0 failure、全部 sample tree unchanged，
报告 SHA-256 为 `6d0fa0bc600d59597afd8f3b7274ef24129745b6ccecbbed986ae76ac90ed085`。
用户冻结顶层 `data/`，因此不回写旧 contamination registry；首次正式 POST 后以 committed
selection/campaign manifest 作为 60 个首次 HP exposure 的派生 overlay。

配置固定为 MiniMax-M3、transport-v4、HP_V8 对 FullRewrite、seed42、distractor-on、10RT、
50/50 方法首发、13 个物理唯一 Keys、每 Key 最多四个 paired workers、52+48 两波。HP_V8
方法、prompt、`hybridpatch/8`、executor、validation gate、FullRewrite、transport、evaluator
和 scoring 均不改变。计划见
[`docs/experiment_plans/exp_20260719_hybridv8_transportv4_mixed_confirmation100.md`](../docs/experiment_plans/exp_20260719_hybridv8_transportv4_mixed_confirmation100.md)。
零 API 验证包括 executor 72、integrated dispatcher/transport 106、transport-core 47、analyzer
5+9、process 14、evaluator-preflight 6、selection tests、V1–V8 matrix、splitters byte-exact 与
V8 smoke 8-row raw replay，全部 PASS；只读计划审阅 `GO WITH FIXES` 的 required fixes 已闭合。

## 2026-07-20 evaluator 临时目录 Git guard 修复

mixed confirmation100 的第二波在 `weather5` 运行时命中 `git_identity_drift`：commit 未变，
但另一个 Python evaluator 的短生命周期 `HP_V8/tmp_eval_*` 目录恰与跨 worker Git 检查重叠，
被误判为工作树污染。根 `.gitignore` 现仅忽略 `HP_V**/tmp_eval_**/`；其他未跟踪源码仍使
工作树为 dirty。`run_meta` 继续对真实 commit/tree drift 全局停止，并在 stop latch 中保存
用于该次判定的完整 `git status --porcelain`，不再只记录 clean/dirty。

为只补未提交 RT，恢复边界使用严格校验的
`anchorpatch.campaign_recovery_authorization/1`：原 stop 按原字节归档，旧 manifest 不改写，
新 invocation 记录旧/新 commit 和 authorization SHA-256。授权仅接受这次
evaluator-temp ignore/诊断修复的固定 changed-file 集合，且 code fingerprint 只能改变
`run_meta.py`；任务计划、方法配置和已提交 RT 仍按旧 manifest 校验。任何额外源码变化、
stop/manifest 摘要变化或 dirty tree 都拒绝恢复。
并行恢复允许 metadata ledger 同时保留已验证的旧身份记录和带同一 authorization 的新身份记录；
不属于这两个精确身份的第三种记录仍 fail closed。

该修复不改变 HybridPatch/FullRewrite 请求、prompt、协议、执行器、gate、transport、evaluator
或 scoring。零 API 回归：新增两项 Git identity 测试 PASS；executor 72/72、splitters、
transport/runner 110/110、evaluator preflight 6/6 与 Python 编译均 PASS。

## 2026-07-20 mixed confirmation100：样本级隔离后的 incomplete supporting view

`exp_20260719_hybridv8_transportv4_mixed_confirmation100` 最终完成 98/100 个双臂
RT10 样本。`calendar5` 在 HP RT5 backward 因 malformed `DTEND` 触发
`icalendar.error.BrokenCalendarProperty`，登记为 `evaluator_incomplete`；`satellite6`
在 FR RT3 forward 耗尽两个 incomplete-stream response slots，登记为
`infrastructure_incomplete`。两者未完成步骤均保持 missing/null、未写 0、未替换样本；
其余 worker 继续完成。campaign 因而保持 `incomplete/failed_informative`，不能写成预注册
固定 n=100 的 canonical 完成实验。

完整配对 98 样本在 RT1 的 HP/FR/delta 为 `97.431%/92.558%/+4.873pp`，RT10 为
`79.245%/60.978%/+18.268pp`，W/L/T=`65/25/8`；CriticalFailure@0.10 使用
`1e-12` 数值容差后为 `47/882` vs `69/882`。method/developer-unseen 层 60/60 完成，
RT10 delta `+21.563pp`；historical-exposed 层仅 38/40 完成，delta `+13.064pp`。
两个缺失 endpoint 各只取数学范围 `[-1,+1]` 时，计划 n=100 的事后均值差界为
`+15.902pp` 至 `+19.902pp`；这不是插补，也不使 campaign 完整。

complete98 的 HP routes 为 bounded/local/bulk/DSL/kept-context
`1445/358/98/47/12`；repair attempted/used/success=`308/276/259`；protocol failure
`37/1960`；全部 campaign 已提交的 1,968 个 HP steps 中 preservation violations 为 0。
已知完整 usage 为 HP `85,072,528` tokens / USD `71.046620`、FR `65,862,257` /
USD `55.396233`；另有 55 个已开始生成但无 final usage 的 transport attempt，因此费用是
已知下界而非精确账单。

1966 个 backward rows 零 API replay PASS。strict inspector 保留原始
`latest run_metadata invocation failed: calendar5`，并由 evaluator-incomplete sidecar、
source-log SHA、未提交/未插补标记把它识别为已登记的样本级例外；未接受错误、stop latch、
重复/半提交和 preservation violation 均为 0。恢复跨
`23ced3cf6cc4d8502aeb61444b476d47f51aba8a` 与
`0f0b3c881b8e129fcbeb083139c5337c990ff90f` 两个授权身份，方法、prompt、协议、执行器、
gate、FullRewrite、transport、evaluator 和 scoring 指纹保持冻结。

完整 raw 私有归档为
`../hybridpatch_private_archives/exp_20260719_hybridv8_transportv4_mixed_confirmation100_incomplete98of100_raw.tgz`
（607,300,822 bytes、42,317 members、SHA-256
`ed7717030964483828f5ff60ec04b54b032ec08d0b60380a14ce496895e97ac0`）。13 个本地 Key
精确扫描命中 0；989 个通用 Authorization/Cookie/x-api-key 字面量全部来自三个任务文档
副本，非 provider header 或本机凭据。official finalize 的生成态 full/records-only validator
均 PASS；为不改写 HP_V3–HP_V7、Baseline、transport 与既有 catalog，生成 record 随后私有
归档并恢复旧 generated state。record archive SHA-256 为
`babaefce5c5a231d79103cdf0008ff2924bc89fbb8489b883e6cd96894dbfa76`，13-Key 精确
匹配为 0。恢复后 `--records-only` 对 13 个公开 record PASS；source-linked 模式保留 21 个
由活动文档前进造成的既有 stale-hash error，不改写冻结记录。

## 2026-07-21 大规模 paired 调度与 evaluator 样本隔离

本轮是 HP_V8 的实验编排修复，不创建新方法版本，也不改变 HybridPatch prompt、
`hybridpatch/8`、schema、executor、validation gate、partial acceptance、preservation、
FullRewrite、transport-v4、domain evaluator 或 scoring。

confirmation 和新增 `full234` role 改用 `per_key_work_conserving_v1`。每个 Key 保留稳定
FIFO 队列和固定 `slots_per_key=4` 上限；worker 结束后立即从同一 Key 补位，不再等待整波
清空。manifest 保存每 Key 队列、`max_worker_count` 和 `queued_worker_count`，dispatch log
保存 refill 与槽位释放。full234 自动冻结 `samples_delegate52` 的精确 234-sample 排序集合和
`sample.json` 摘要，固定 13 个物理唯一 Key、10 RT、seed42；最大并发 52、初始排队 182，
方法首发为 HP/FR 各 117 个，每 Key 初始四项为 2/2。

`domain.evaluate_context()` 抛出的异常现在被包装成独立
`EvaluatorIncompleteError`。runner 只在实际 checkpoint 是完整原子前缀时写入
`evaluator_incomplete` sample outcome、run metadata 和
`anchorpatch.evaluator_incomplete/2` sidecar，并明确记录失败步骤未提交、未补 0。dispatcher
复核 worker/PID/invocation、方法顺序、checkpoint、API 响应步骤和 sidecar 后，只终止该
sample，释放槽位并继续队列；该状态在同一 campaign 内是终态，resume 跳过且不重新 POST。
证据不完整、普通 runner/local exception、preservation、Git/task-plan drift、重复/半提交或
无法映射的 API ledger 仍全局停止。transport exhaustion 的既有恢复语义不变。

后处理同时保留每个 incomplete sample 的正式 outcome，`evaluator_incomplete` 不再默认归为
infrastructure；complete-pair 视图仍保持 missing/null、禁止 0 分插补。历史
`anchorpatch.evaluator_incomplete/1` source-log SHA 读取语义继续兼容。

零 API 验证：HybridPatch executor `72/72`、dispatcher/transport/runner `114/114`、tools
`67/67`、splitters byte-exact 全通过；新增测试覆盖滚动补位发生在同 Key 旧 worker 尚未
全部结束时、每 Key 不超过 4、evaluator incomplete 后继续补位、resume 零 POST、
preservation 仍全局停止、full234 精确 234 scope 与 117/117 方法首发。V1–V8 replay matrix
和 preservation regression 均通过。本轮未调用任何 provider API。

## 运行方式

```bash
cd HP_V8
PYTHONUTF8=1 python src/experiment_runner.py ...
python -B src/verify_anchorpatch.py --dir exp_<YYYYMMDD>_<slug>
```

后续 API campaign 必须使用新的实验编号，重新完成计划、审阅、零 API preflight、
runtime evaluator smoke、Key 小探针与最终命令复核；顶层凭据不得复制到本目录。

## 2026-07-21 remaining134 阶段式调度（方法语义冻结）

新增 `remaining134` campaign role，只改变实验编排，不改变 HP_V8 方法、prompt、
`hybridpatch/8`、executor、validation gate、preservation、FullRewrite、transport-v4、evaluator
或 scoring。范围是当前 234 样本减去 committed mixed confirmation100 的全部 100 个 planned
sample，固定剩余 134；排除 selection SHA-256 为
`26da1c3d27a1eb83d70af444197afde7a7f20ade20c6e232f2d6abe605d3d654`，remaining ID list
SHA-256 为 `835297a349489dd6c69224ad17ae5a4b767cc3da7bc0baef4b6c2f55c2b99ad4`。

调度使用 14 个物理唯一 Key、每 Key 最多 4 个 worker、稳定 FIFO 即时补位。全局阶段顺序为
HybridPatch 后 FullRewrite：HP 全部达到 finished/evaluator-incomplete 且没有
infrastructure-incomplete 后，dispatcher 写入带 commit、完整 scope、API 数和 preservation=0
的唯一持久屏障；每个 FR refill 都在 `Popen` 前重验屏障。HP 基础设施未完成只阻止方法切换，
不终止同阶段其他 worker；HP evaluator-incomplete 保持 null，并从后续 FR eligible scope 排除。
每个 phase 的 run metadata 记录实际单方法 invocation，同时 campaign config 仍冻结完整方法集；
legacy 非阶段运行不新增 `method_phase` 字段。

断电/进程中断恢复新增 `anchorpatch.interrupted_phase_resume/1` 审计证据：只在旧 worker lease
已释放、launch/PID/invocation、task-plan SHA、checkpoint 和结果原子前缀一致时重新启动；runner
从第一个未提交 RT 继续，已提交 forward/backward 不再 POST。无法闭合的开放 transport lineage
仍 fail closed。该证据同时写入 dispatch authorization 与新 run metadata，strict inspector 核对
二者一致。

零 API 新增测试覆盖：100/134 集合差与 14-Key 队列、HP→FR 事件顺序、FR 启动前屏障、
HP infrastructure-incomplete 不启动 FR、phase resume 跳过已提交样本、跨 phase terminal outcome
与 run-metadata campaign identity，并覆盖后处理 strict inspector 的两阶段自动聚合。
`test_model_openai.py` 当前 124/124 PASS；完整开跑门与计划见
[`docs/experiment_plans/exp_20260721_hybridv8_transportv4_remaining134_hp_then_fr.md`](../docs/experiment_plans/exp_20260721_hybridv8_transportv4_remaining134_hp_then_fr.md)。
全局方法顺序没有平衡，必须作为时间/provider-state 混杂披露，不能替代方法顺序平衡的
canonical comparison。

## 2026-07-21 remaining134 共享账本锁恢复

remaining134 的 HybridPatch 阶段已完成 134/134 samples、1,340 RT，phase barrier 已持久化，
preservation violations 为 0。FullRewrite 首批 56 workers 启动后，共享
`api_attempt_ledger.jsonl` 的 Windows `portalocker.lock(..., LOCK_EX)` 在短时并发写入时立即抛出
`AlreadyLocked`；dispatcher 按既有共享完整性策略停止全体 worker。停止前已有 15 个 FR
sample 提交 17 个完整 RT；未完成步骤没有写 0 分或半提交结果。

本修复只改变实验基础设施：所有共享 JSONL append 改为最多 60 秒的有界锁等待，超时仍
fail closed。新增 `campaign_recovery_authorization/2`，只接受本次精确哈希绑定的 23 条本地
锁异常 API 行、7 条直接锁失败 attempt 行、33 个开放 semantic call 的 102 条中断 attempt
行和 56-worker 中断 cohort；旧行原样保留，inspector 和
runner 只在授权边界内把这些未提交事件排除出 active semantic lineage。恢复从每个 sample
首个未提交 RT 继续，已提交的 1340 个 HP RT 与 17 个 FR RT 不再 POST。

该变更不修改 HybridPatch 方法、prompt、协议、executor、validation gate、FullRewrite、
provider 请求参数、R2/I3 transport budget、transport revision、evaluator 或 scoring。
零 API transport/runner/dispatcher 回归更新为 126/126 PASS，并新增真实文件锁竞争等待和
精确 incident-row 过滤测试。

首次恢复启动暴露了授权校验自身的不可变性错误：它正确固定旧 incident 行哈希，却错误要求
同一 semantic call 后续不得追加合法 replay/response 行。cohort 因此再次全局停止；停止前新增
9 条 API 行，其中 8 条是既有 journal 的零 POST replay、1 条是完整 provider response，均未
形成新的完整 RT。修复后授权只验证历史 incident 行本身，并允许其后追加正常 lineage；第二次
stop 与旧授权均字节保存在 `recovery_history/`，由 superseding authorization 继续绑定。已提交
结果、checkpoint、R2/I3 budget 和 transport-v4 语义不变。

第二次启动前检查还识别出 4 条仅写入 `semantic_request`、尚未出现 `attempt_start` 的 FR
调用；它们同样没有 provider POST、API terminal row 或结果提交。恢复工具现将这种
pre-provider 中断与开放 stream 一并做精确行哈希授权，避免它们在已完成 HP phase 的复核中
被误判为无映射调用。

第三次启动暴露了恢复身份校验只接受“原始提交 + 最新恢复提交”的缺陷：56 个 worker 均在
写入 run metadata 前因历史恢复提交冲突退出，未新增 API、attempt 或结果行。恢复授权现递归
校验完整的 SHA-256 supersession 链，并按每个历史 authorization ID/文件摘要接受对应 metadata；
这 56 个零请求 worker 作为 `preauthorization_worker_launch_ids` 单独绑定，要求存在唯一非零
`campaign_fatal` exit，且不存在 worker authorization、run metadata、API 或 attempt 行。该调整
仍只涉及 campaign provenance/recovery，不改变模型请求、方法、transport、evaluator 或 scoring。
零 API 回归为 executor 72/72、dispatcher/transport/runner 128/128、分析 5/5、tools 67/67，
splitters byte-exact PASS。

下一次启动中，6 个 worker 已在当前授权下完成 metadata 注册，其余 worker 在授权前遇到
Windows `os.replace(run_metadata.jsonl)` 的瞬时 `WinError 5`；dispatcher 按共享 provenance
完整性规则停止全体。该波仍未新增 API、attempt 或结果行。`_write_jsonl_atomic` 现在只对
Windows 5/32/33 sharing/access violation 做最多 60 秒的短间隔重试，其他错误与超时继续
fail closed，原子替换和 metadata 锁语义不变。最新零 API dispatcher/transport/runner 回归为
129/129 PASS。

恢复后的 FR 队列进一步暴露了调度吞吐问题：一次 poll 中已有多个 worker 正常结束时，旧实现
会为每个完成样本重新扫描全部 campaign ledger，并在每次扫描之间串行更新 active set，导致
同 Key 的空闲槽长期不能补位。调度器现先收集该 poll 的全部退出 worker，只写一次 active set，
再用一个 `required_complete_samples` 集合做一次完整性审计；审计通过后下一轮立即按 Key 的空槽
补位。全局完整性检查、样本完成条件、preservation 停止条件和每 Key 并发上限均不变。新增测试
固定一次 poll 内多个完成样本只触发一次合并审计。若 dispatcher 在 worker 已终态后被用户暂停，
恢复工具按 terminal metadata、sample outcome、checkpoint 与空闲 worker lease 共同复核，补写真实
worker-exit provenance，并把完整 `git status --porcelain`、旧授权 SHA 和新 clean commit 固定到
同一恢复链；不重写结果、checkpoint、API 或 attempt ledger。完整零 API 回归为 executor 72/72、
dispatcher/transport/runner 131/131、analysis 5/5、tools 67/67、splitters byte-exact PASS。旧
preauthorization worker 证据在 operator-pause supersession 中继续按原归档验证，不再错误要求
最新 operator stop 重复旧 stop 的 error 文本。

# 实验计划：`exp_20260719_hybridv8_transportv4_supplement40_lockfix`

> 这是 `exp_20260719_hybridv8_transportv4_supplement40` 在首次启动遇到本地共享锁竞争后的替代 campaign。旧目录原样保留为 `failed_informative`；本实验使用新目录、新 clean commit 和相同冻结任务计划，不拼接旧目录的未提交调用。

## 1. 身份与研究边界

- owner：`HP_V8`
- level / claim role：支持性付费诊断实验 / `diagnostic_only`
- lifecycle：40/40 完整则 `complete`，否则 `failed_informative`
- 研究问题、样本、方法和报告口径沿用已审阅计划 [`exp_20260719_hybridv8_transportv4_supplement40.md`](./exp_20260719_hybridv8_transportv4_supplement40.md)。
- 可证伪目标：在 HP_V8、FullRewrite、MiniMax-M3、transport-v4、seed42、distractor-on、固定 task plans 和 10RT 下得到 40 个样本的完整配对 RT1–RT10 轨迹，且 preservation 为 0。
- 与 prior10 的联合视图仍按原计划分别报告 supplement40、50 条 campaign-chain 描述性 pooled 视图和去重后的 44-ID 视图；六个重叠 ID 不冒充独立样本。

## 2. 启动失败证据与唯一变化

- 旧 campaign 在 40 workers 同时越过 barrier 后，首条终端 API 行为本地 `runner_exception: AlreadyLocked`，严格 inspector 正确触发全局停止。
- 停止时结果行与 checkpoint 均为 0；attempt ledger 有 36 个 semantic request、36 个 attempt start 和 34 个 generation-progress，未形成任何 response commit 或已提交 RT。旧输出不作为方法结果，也不在新 campaign 中 resume。
- 根因：每次 provider 调用前的 stop-latch 只读检查错误地申请 `.run_metadata.lock` 独占锁；Windows 40-worker 竞争下 `portalocker.lock(..., LOCK_EX)` 可立即抛出 `AlreadyLocked`。
- 唯一运行时代码变化位于 `HP_V8/src/run_meta.py`：原子 `campaign_stop.json` 使用无锁只读；真正的 metadata writers 使用 `portalocker.Lock` 的 60 秒有界重试。另有 `HP_V8/src/test_model_openai.py` 的零 API 回归变化。stop writer 仍在同一 campaign lock 内 first-writer-wins；relay commit 与 stop latch 的排序锁不变。该变化修正 global-stop/metadata 同步实现，但不改变 transport-v4 request、stream、retry 或 generation lineage 语义。
- 不变：HP_V8 协议、提示、执行器、gate、partial acceptance、preservation、FullRewrite、evaluator、scoring、MiniMax request/stream/retry transport 语义和 task plan。

## 3. 固定配置

- methods：`hybridpatch/8` 与 FullRewrite；每个 sample worker 顺序执行两种方法；Key 内首方法交替，全局 HP-first/FR-first=`20/20`。
- transport：`opencode_anthropic_sdk/4`，`exact_payload_new_semantic_call/1`；每个语义调用 2 个生成 response slots、3 个生成前 transient failures；HP repair 最多一次独立语义调用。
- model：MiniMax-M3 / OpenCode Go；effective temperature 1.0；max tokens 131072；adaptive thinking。
- seed / RT / distractor：42 / 10 / on。
- concurrency：13 个 Key，每 Key 3–4 个 sample workers，硬上限 4；40 workers 同时授权。
- dataset：`data/samples_delegate52` 中历史 V6 `val20 + unused_reserve20` 的固定 40 个已曝光样本。
- samples（canonical sorted order）：
  `crystal6 docker3 earncall1 emails5 filesystem2 filesystem3 fonteng1 fonteng5 foodmenu1 geodata1 geodata4 geotrack5 geotrack6 hamradio6 jobboard3 json2 json4 landmarks2 landmarks3 libcatalog2 libcatalog5 mathlean4 mathlean5 obj3d2 obj3d5 quantum1 quantum5 robotics1 robotics3 satellite4 screenplay4 screenplay5 spreadsheet1 spreadsheet6 subtitles2 subtitles6 transit1 transit2 treebank2 treebank4`
- task plans：从旧失败目录中已核验的 40 个 plan 原字节复制；其来源仍为 `HP_V6/exp_20260711_hybridv6val40`，canonical hash-map digest 为 `b764ac7ac0e1be4a057064833f281312f01d9d59acb2b254c9b2de1284d56872`。
- runtime evaluator：40/40 可执行；39 个参考自评分 1.0；`obj3d5=0.9129` 的既有 material-score ceiling 异常保留并披露，不修改或排除。

## 4. 指标、排除与停止

- 每个 RT1–RT10 均报告固定 complete-chain scope 下的 HP、FR、paired delta 和 n；另报 RT10 RS、paired SD、wins/losses/ties、CriticalFailure@0.10。
- 报告 token、已知费用、缺 final usage attempts、route、repair、protocol failure、soft burden 和 preservation；旧失败启动的 usage 不混入方法比较，作为未结算基础设施开销单列。
- 无表现性排除。基础设施未完成保持缺失/null，不写 0。只分析两臂 RT10 完整的样本，十个 RT 使用同一固定 scope。
- sample-local provider/transport exhaustion 只隔离该 sample；最多允许一次 `g001`，禁止 `g002+`。
- campaign-global：preservation violation、Git/tree/task-plan 漂移、重复或半提交 RT、evidence 映射失败、runner/evaluator/local 未捕获异常、共享数据/执行器/评测完整性错误。
- 全局停止后保留目录，不同代码或 fingerprint 必须新建实验编号。
- 累计预算上限 USD 75，覆盖旧失败启动、两轮 Key probe 与本 lockfix campaign，不是新增第二个 USD 75 授权。旧目录的 36 个 request body 共 2,117,821 UTF-8 bytes；按每 byte 最坏计一个 input token，并把 36 个 attempt 的 131,072 output cap 全部计满，上界为 USD 6.297657。两轮共 104 个 probe 按每次两个 1,024-token response slots 计满，output 上界为 USD 0.255590；其极短 `Reply exactly: Hello` 输入和生成前 transient attempts 余量一并向上冻结，使旧失败与两轮 probes 的总准备金为 USD 6.60。因此 lockfix 正式 campaign 的可用上限为 USD 68.40；按 prior10 线性外推约 USD 50.43。第二轮 13×4 probe 完成后且正式 campaign POST 前，以及任何 resume 前，均重新核对 known usage、无 final usage 的保守上界与累计 USD 75 余量；超过累计上限不得发起新 POST。旧失败开销与 lockfix known usage 在最终 campaign-family 成本中分别列出。

## 5. Preflight 与命令

- [x] 根因已定位；针对 40-reader 无锁 stop-latch 和 writer contention 的零 API 测试通过。
- [x] HP integrated transport/dispatcher 90/90 通过。
- [x] 全部零 API 回归通过：compile 98、executor 72、splitters、integrated 90、analyzer 5、transport 47、process 14；V1–V8 envelope matrix 与 V4/V5/V6/V7/V8 raw replay 分别 70/400/400/400/200 PASS；frozen-scope diff 为空。
- [x] 40 个独立 Python 进程在 metadata writer lock 被占用时并发读取 stop latch，40/40 零异常完成。
- [ ] 新 clean commit、formal dry-run、40 plan hashes 和 runtime identity 三证一致。
- [ ] 13-Key ×4 probe 为 52/52；只输出 label 与计数，不输出 Key 值。
- [ ] 新上下文只读计划审阅为 GO。

```bash
cd HP_V8
PYTHONUTF8=1 OPENCODE_TRANSPORT=anthropic_sdk_v2 MINIMAX_TRANSPORT=opencode \
  python -u src/fr_baseline_dispatch.py \
  --out_dir exp_20260719_hybridv8_transportv4_supplement40_lockfix \
  --method hybridpatch --keys_file ../.env.frkeys \
  --samples crystal6 docker3 earncall1 emails5 filesystem2 filesystem3 fonteng1 fonteng5 foodmenu1 geodata1 geodata4 geotrack5 geotrack6 hamradio6 jobboard3 json2 json4 landmarks2 landmarks3 libcatalog2 libcatalog5 mathlean4 mathlean5 obj3d2 obj3d5 quantum1 quantum5 robotics1 robotics3 satellite4 screenplay4 screenplay5 spreadsheet1 spreadsheet6 subtitles2 subtitles6 transit1 transit2 treebank2 treebank4 \
  --plans_from exp_20260719_hybridv8_transportv4_supplement40 --require_plans \
  --preflight --probe_concurrency 4

PYTHONUTF8=1 OPENCODE_TRANSPORT=anthropic_sdk_v2 MINIMAX_TRANSPORT=opencode MINIMAX_HARD_TIMEOUT=7200 \
  python -u src/paired_campaign_dispatch.py --campaign_role supplemental \
  --out_dir exp_20260719_hybridv8_transportv4_supplement40_lockfix \
  --samples crystal6 docker3 earncall1 emails5 filesystem2 filesystem3 fonteng1 fonteng5 foodmenu1 geodata1 geodata4 geotrack5 geotrack6 hamradio6 jobboard3 json2 json4 landmarks2 landmarks3 libcatalog2 libcatalog5 mathlean4 mathlean5 obj3d2 obj3d5 quantum1 quantum5 robotics1 robotics3 satellite4 screenplay4 screenplay5 spreadsheet1 spreadsheet6 subtitles2 subtitles6 transit1 transit2 treebank2 treebank4 \
  --num_round_trips 10 --seed 42 --keys_file ../.env.frkeys \
  --key_labels KEY_01 KEY_02 KEY_03 KEY_04 KEY_05 KEY_06 KEY_07 KEY_08 KEY_09 KEY_10 KEY_11 KEY_12 KEY_13 \
  --slots_per_key 4 --notes 'HP_V8 transport-v4 supplemental val40 paired RT10 lockfix' --dry_run
```

付费正式命令仅删除 `--dry_run`。同一 lockfix 代码与完整身份参数下，恢复命令只能在正式命令末尾追加
`--resume --resume_reason '<精确原因>' --confirm_workers_stopped`；只恢复 strict inspector 标记的 incomplete sample，从首个未提交 RT 开始，不重发已提交 forward/backward，最多授权一次 `g001`。不恢复旧 `supplement40` 失败目录。

## 6. 归档与审阅

- 新输出：`HP_V8/exp_20260719_hybridv8_transportv4_supplement40_lockfix`。
- 旧失败输出：`HP_V8/exp_20260719_hybridv8_transportv4_supplement40`，只读保留并单独归档。
- 运行后执行 strict inspector、raw replay、analyzer、`prepare → review → finalize`、credential scan 和私有完整 raw 归档。
- 正式启动前由新上下文只读子代理复核唯一变化、并发、公平性、停止规则、预算和可复现身份；主 Agent 独占写入与 GO/NO-GO。

## 7. 计划审阅

- 新上下文只读审阅初次结论：`GO WITH FIXES`。
- 已落实：USD 75 明确为旧失败、两轮 probes 与 lockfix 的累计上限，并以 USD 6.60 保守准备金冻结 lockfix 可用预算为 USD 68.40；补全 exact resume flags、strict incomplete scope 与 `g001` 上限；区分唯一运行时代码变化与测试变化，并明确同步实现变化不等于 transport-v4 request/retry 语义变化。
- 最终 operational verdict：上述 fixes 闭合后为 `GO`；仍须通过 clean commit、formal dry-run、identity/task-plan 三证与 13×4 probe，才允许正式 POST。

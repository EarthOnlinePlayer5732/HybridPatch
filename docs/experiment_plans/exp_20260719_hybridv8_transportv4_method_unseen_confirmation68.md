# 实验计划：`exp_20260719_hybridv8_transportv4_method_unseen_confirmation68`

## 1. 身份、问题与冻结边界

- owner：`HP_V8`
- level / claim role：正式确认实验 / `confirmatory_method_holdout`
- lifecycle：只有 68/68 样本两臂都完成 RT10、全部完整性门通过时才为 `complete`；否则为 `incomplete` 或 `failed_informative`，不以缺失值补 0。
- 研究问题：在未被 HybridPatch 方法开发、HP API 实验或人工案例分析使用的固定样本上，HP_V8 相对 FullRewrite 的 RT1–RT10 round-trip 保留能力如何？
- 可证伪假设：在相同 MiniMax-M3、transport-v4、task plan、seed、distractor 和 34/34 方法顺序平衡下，HP_V8 的固定 n=68 backward RS 轨迹与 RT10 样本级 endpoint 高于 FullRewrite，同时 preservation violation 保持为 0。
- 相对上一轮唯一实验变化：样本从历史已曝光 supplement40 改为 API 前冻结的 68 个 method/developer-unseen 确认样本；为容纳 68 个样本，paired dispatcher 增加 selection manifest gate 与同一 campaign 内 52+16 有界波次。方法、提示、`hybridpatch/8`、执行器、validation gate、FullRewrite、transport-v4 request/retry、evaluator 和 scoring 均冻结不变。
- 禁止：根据本轮得分修改 HP_V8，自动运行 17 个 reserve，或把本轮与历史已曝光样本拼成新的独立样本总体。

## 2. 曝光口径与冻结选择

零 API 扫描覆盖 Git tracked/ignored 的 HP_V3–HP_V8、Baseline、transport、`_attic`、records、raw task plan/result/checkpoint/API ledger 和 `data/CONTAMINATION_REGISTRY.json`。选择证据为：

`HP_V8/analysis/exp_20260719_hybridv8_transportv4_method_unseen_confirmation68_selection.json`

两个口径必须同时保留：

- 严格 any-provider-call：冻结 FullRewrite 234-sample baseline 已为 234/234 样本保存真实 request/response，因此绝对 provider-unseen 候选为 0。
- 仓库既有 method/developer exposure policy：FR-only 预计算对照不消耗 HP holdout；registry contaminated、sealed dev/val/test/unused splits 与可见 HP 路径证据先排除 148 个样本。尤其 `hybrid_split.test20` 已在 2026-07-03 完成 HP/FR live test，不能再进入候选。registry `clean_candidate` 有 86 个，但 `python1` 在 `docs/FINDINGS.md:370-399` 有逐文件、依赖和 evaluator 的开发内容级调查，故再排除 1 个；最终 85 个候选与 sealed splits、扫描到的 HP exposure 和已知人工内容曝光零交集。当前真实 runner evaluator runtime smoke 为 234/234 可运行。

本实验只能称为 `method/developer-unseen confirmation`，不能称为“历史上从未调用 provider”的绝对 unseen 实验。FullRewrite 历史输出和 registry 漂移均不隐藏。顶层 `data/` 为共享只读数据，本轮不回写冻结 registry；已提交 selection manifest 是本轮派生污染/选择 overlay。

按用户冻结规则，`N=85<100`，固定 `round(0.8×85)=68` 个确认样本，保留 17 个。选择 seed=42；先选择 reserve，再以补集作为 confirmation。分层维度为 domain、文件格式多标签、起始任务的 semantic-operation/task-type 多标签、文件数 `1/2/3+` 和起始文档 UTF-8 长度五分位；确定性 greedy + local swap，输入与脚本 SHA-256 均写入 selection manifest。

### 2.1 确认集（canonical sorted，68）

```text
audiosyn1 chess4 circuit5 dbschema3 dbschema6 docker5 earncall2 earncall3 earncall5 earncall6 edifact1 filesystem4 filesystem6 fonteng6 foodmenu2 foodmenu3 foodmenu4 genealogy5 geotrack1 geotrack2 geotrack4 graphviz5 hamradio2 hamradio5 infra3 infra4 infra6 jobboard2 jobboard4 json1 json5 landmarks5 latex3 libcatalog3 makefile2 makefile3 malware1 malware2 malware5 mathlean6 molecule1 musicsheet3 musicsheet4 obj3d1 obj3d3 obj3d6 protein2 protein4 protein5 protein6 python4 quantum3 satellite1 satellite2 satellite5 spreadsheet2 spreadsheet4 starcatalog1 subtitles4 transit3 transit5 translation1 translation3 translation6 treebank1 treebank5 weather2 weather6
```

### 2.2 储备集（canonical sorted，17；本轮不调用 API）

```text
audiosyn4 circuit4 earncall4 filesystem5 graphviz4 jobboard6 landmarks4 latex4 makefile1 malware4 molecule4 molecule5 satellite3 spreadsheet3 subtitles5 translation5 weather3
```

selection manifest 的字节、selected/reserve 顺序或输入 digest 在首次 API 后不得改变。dispatcher 要求该文件已被当前 clean commit 跟踪，且 current bytes 与 `HEAD:<path>` 完全一致。
当前冻结 artifact SHA-256 为 `6c1a4331ede14adadec660a76df221be32fd855dc07e6b7ec1fc19397e4f0214`；生成器有意不写动态时间、HEAD 或 dirty-status，同一输入应重建出同一字节。严格 any-provider evidence manifest SHA-256 为 `da6d58211475269ec3c0d2de0bdd9aeca52d80772e5cd9eaa38592bf4767447e`，method/developer exposure path manifest SHA-256 为 `2a3a8d2e75e21c3ad524175f5125f95c0e73bc9234afc02344ef7638463fcd3c`。

## 3. 固定运行配置与公平性

- methods：HP_V8 `hybridpatch/8` 对 FullRewrite；同一 sample worker 顺序运行两臂。
- model/provider：MiniMax-M3 / OpenCode Go；adaptive thinking；effective temperature 1.0；max tokens 131072。
- transport：`opencode_anthropic_sdk/4`，R2/I3 与独立 HP repair semantic-call 预算保持冻结。
- seed / RT / distractor：42 / 10 / on。
- task plan：dispatcher 在任何 worker 授权前为全部 68 样本生成 seed42 的 10-target state sequence；同一样本两臂复用同一 plan。全部 plan path、forward sequence 和 SHA-256 写入 immutable dispatch manifest。
- method order：全局 HP-first / FR-first=`34/34`。每个 Key 的活跃 wave 尽量成对交替，保证 HP 与 FR 跨 sample 并发，而不是方法级串行波次。
- concurrency：使用 `.env.frkeys` 中 13 个通过探针的 label；每 Key 同时最多 4 个 paired sample workers。wave1 为 52 workers（每 Key 4，26/26 首发），wave2 为 16 workers（每 Key至多 2，8/8 首发）。两个 wave 属于同一个 campaign、同一 manifest 和同一 Git identity。
- runtime identity：clean `run_git_commit`、`git_tree_state=clean`、code fingerprint、selection SHA、transport revision、模型、seed、task plans、方法顺序、started/finished timestamps 全量记录。

已知 evaluator 边界不作为表现性排除：`makefile2/3` 在 Windows fallback 下参考 ceiling 为 0.95；`transit3` 参考 ceiling 约 0.99417；`python4` 的负对照显示 scaffold 可能不判别生成代码；`audiosyn1` 是文本 `.csd`，虽保留 registry 的历史 `unsupported_generation_domain` 注记，但当前 runner/evaluator 已实测可运行。canonical 固定 n=68 保留这些样本；另预先附加排除 `python4` 的 n=67、排除 `audiosyn1` 的 n=67，以及同时排除二者的 n=66 evaluator-validity sensitivity，均不得替换 headline。所有事实在看分前冻结并在结果中披露。

## 4. 指标与报告口径

原始 JSONL 的分数继续保持 0–1；派生报告统一乘 100，差值使用百分点（pp）。正式 endpoint 只在同一固定 complete-chain scope `n=68` 上计算：

1. RT1–RT10：每轮 HP、FR、HP−FR pp 与固定 n=68。
2. RT10：每个样本的 HP、FR 和 pp 差值。
3. 样本级 RT10：HP/FR mean 与 median、paired mean/median delta、sample SD、win/loss/tie。
4. 差值集中度：paired RT10 delta 的 IQR、MAD（相对样本中位数），以及 `|delta|<=5pp`、`|delta|<=10pp` 的样本比例。
5. 配对推断：固定 seed42、10,000 次样本级 paired bootstrap 的 mean-delta percentile 95% CI，以及忽略 ties 的 exact two-sided sign test；二者均在看分前冻结。
6. `CriticalFailure@0.10`：相邻 backward RS 从正值坍塌到 0，或下降大于等于 0.10 的次数/可比较转换数，分别报告两臂。
7. HP 协议：prompt profile、declared route、repair attempted/used/success、final protocol failure、partial acceptance、soft burden telemetry；软 burden 不影响执行。
8. preservation：所有适用 HP 步的 violation 数与明确 N/A 数；任何 violation 都使 campaign 全局停止。
9. usage：按 method 与 call_kind 报 provider final-usage token 和已知 USD；缺 final usage 的 generation-started attempt 数量与保守上界单列。已知费用不是 provider billing statement。
10. 完整性：结果行、checkpoint、sample outcome、API/attempt/journal mapping、incomplete samples、重复/半提交 RT 和 private archive SHA-256。

不进行表现性排除。基础设施未完成保持 missing/null，不写 0；只允许在同一 commit/revision 下按严格 checkpoint 规则续跑 incomplete sample。若最终仍不是 68/68，正式固定 n=68 endpoint 不成立，任何部分结果只作 incomplete evidence。
冻结 scoring 对已经提交、仅含非空 `evaluation.error` 而无 numeric score 的结果记为 0.0；本轮不改变该政策，但派生报告必须单列这类 cell 的总数、方法和坐标，不能把 grid 完整误写成“所有 evaluator 均返回 numeric score”。

## 5. 传输、修复、隔离与停止

- transport retry 与 HP repair 使用不同 call_kind、ledger 和预算；每个 repair 是新的 semantic call。
- 单 sample provider/transport retry exhaustion：标为 `infrastructure_incomplete`，保留 attempt/ledger/checkpoint/已提交 RT，其他 sample 与后续 wave 继续。
- resume：只启动 strict inspector 确认的 incomplete sample；从第一个未提交 RT 开始；不得重发已提交 forward/backward，不覆盖旧 attempt；不同 code/transport/selection identity 不得续跑旧正式目录。
- campaign-global stop：`preservation_violations>0`；Git/tree/selection/task-plan 漂移；重复结果或半提交 RT；API evidence 无法映射；未捕获 runner/evaluator/local 异常；共享 executor/data/evaluator integrity 错误。
- 不实现“多个 provider 错误后模糊全停”。

## 6. Preflight、预算与启动门

预计 primary edit steps 为 2,720（68×10×2 directions×2 methods），另有实际触发的 HP repair 和 transport attempts。supplement40 的已知费用为 USD 47.686409；按样本数线性投影约 USD 81.07。行政预算为 USD 130。自动门只对 campaign 内已提交结果行的已知 `total_usd` 做每波启动前和结束后的停止检查；它不是 provider billing cap，也不覆盖缺 final usage 的失败 attempt、13 次 Key probe 或同一波内尚未提交的在途费用。probe 与 unknown-usage 单独披露；已有 `NO_GO` latch 或已知 committed usage 超限时普通 resume 不得启动下一 worker。停止后的 campaign 报告 incomplete，不把缺失补 0。

首次正式 POST 前必须全部满足：

- [x] selection tool/test PASS；artifact 二次重生成保持 SHA-256 `6c1a4331ede14adadec660a76df221be32fd855dc07e6b7ec1fc19397e4f0214`，85/68/17、selected/reserve partition、clean-candidate-minus-python1/sealed-split 集合语义与所有 input/evidence SHA 一致。
- [x] selected68 的 per-sample subprocess runtime evaluator preflight 68/68 PASS；data hash 与 `tmp_eval_*` 清洁。报告 SHA-256 `cde8c45e26c72c7c9a4469bc23bb2718fff33f77dcdcac3fdb216bfaf6489c0f`。
- [ ] 全部 Python compile、HybridPatch executor、splitter、integrated transport/dispatcher、analyzer、process tool、transport-core 和 V1–V8 zero-API replay PASS。
- [ ] 新 confirmation orchestration 故障注入：52+16、每 Key≤4、34/34 order、infra sample isolation、preservation global stop、resume only incomplete、committed RT zero new POST、旧 role 行为不变。
- [ ] 新上下文只读实验计划审阅最终 verdict=`GO`；required fixes 已闭合。
- [ ] 计划、selection、orchestration 和测试形成 clean commit；PR #1 仅更新，不合并。
- [ ] clean commit 上 formal `--dry_run` PASS，selection manifest 已跟踪且与 HEAD 同字节，68 个 task plan/manifest 已冻结。
- [ ] 13 Keys 各执行一次完整流最小探针；只记录 label/存活计数，不打印 Key。每 Key≤4 的并发上限由零 API assignment/fault-injection gate 验证，不额外发 52 次付费探针。
- [ ] 正式命令、out_dir、模型、transport、seed、selection SHA、plan hash-map digest 和 USD 余量最终复核。

本轮不另开方法效果 smoke：相同 transport-v4 已完成 prior10 与 supplement40；本轮新增代码仅为离线选择、preflight、报告和 paired orchestration，且正式前仍执行 13 次最小 paid liveness probe。计划审阅可将此项升级为 `GO WITH FIXES` 或 `NO-GO`。

## 7. 标准收尾、审阅与保存

worker 全停后：

1. strict inspector、raw replay、formal analyzer 与百分制确认报告。
2. `tools/process_experiment.py prepare --confirm-stopped`。
3. 三个新上下文只读审阅：结果完整性；结果/失败机制；结论边界与 paper eligibility。主 Agent 独占 `record_review.yaml` 和文档写入。
4. `finalize`、生成态完整 validator 与 `--records-only` validator。若生成器会改写 HP_V3–HP_V7/Baseline/transport 冻结 overlay，先私有保存生成态，再恢复这些旧文件原字节；不得为登记新实验而改写冻结记录。
5. 对 raw 请求/响应、task plans、result、checkpoint、ledger、analysis 和 generated record 做 credential scan；随后创建 sibling private complete archive，记录成员数、字节数和 SHA-256。不得把 Key、Authorization、Cookie 或 private-key material 写入 Git/报告。
6. 更新 `docs/active_log.md`、`docs/RESEARCH_JOURNAL.md`、适用的 `docs/FINDINGS.md`、`HP_V8/VERSION.md` 和 PR 描述；提交并推送当前分支，但不合并 PR。

确认实验结束后停止。17 个 reserve 继续封存，不自动运行；不根据结果修改 HP_V8。

## 8. 计划审阅

- verdict：`NO-GO`，未调用 API。严格 any-provider 扫描为 234/234 exposed，用户原要求的 absolute unseen 候选为 0；method/developer-unseen 不能在没有新授权时替代原总体。
- 2026-07-19 用户随后明确授权新的 60 method/developer-unseen + 40 historical-exposed 口径；该范围使用新实验编号与独立计划 `exp_20260719_hybridv8_transportv4_mixed_confirmation100`，不续用本 confirmation68 身份。

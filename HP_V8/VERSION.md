# HP_V8（hybridpatch/8）版本卡草稿

状态：**active draft**。本版已完成零 API 实现与回归验证，尚未运行任何 API 实验；因此不声称评分、真实 token 或协议失败率已经改善。

## 五问摘要

1. **为什么迭代**：V7 的主提示无条件包含文件表、三层索引和四条完整路径，repair 又重复全部 schema、全部文件和原始响应；初版 V8 又让分类器关闭 local/bulk，并把经验 P95 当成硬拒绝，可能损害任务完成能力。
2. **优化方向**：性能优先于 token 成本。词法分类只控制 coarse 块索引和 `dsl_rules` 是否出现，不能关闭 local/bulk；V8 plan 与 repair 继续精简，但协议负担阈值只作软 telemetry。
3. **当前证据**：冻结 dev20 的 400 步零 API 配对重放中，primary 平均字符数由 27,459.715 降为 20,662.020（-24.755%），36 个实际 V7 repair 对应的反事实 V8 repair 平均由 20,009.222 降为 16,044.667（-19.814%）。这些是字符数，不是 tokenizer token，也不是 API 效果证据。
4. **新问题**：25/394（6.35%）个历史成功信封超过至少一个经验 P95，但现在只被标记而不改变执行；路径审计仍有 1/399（0.25%）个 default-profile V7 chosen route 为 DSL；DSL 成功样本只有 1 个；词法分类仍可能漏掉需要块索引的任务。
5. **下一轮靶子**：先以已提交的干净代码完成 2 样本付费 smoke，再在同模型、任务计划、transport 与 distractor 设置下运行预注册的 10 样本配对 campaign，检验评分、真实 token、协议失败和 preservation。

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

**验收等级：来源快照为字节级确证；V8 方法效果仍待 API 实验。**

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

## 当前 14 项指纹

| 文件 | SHA-1 前 12 位 |
|---|---|
| `patch_schema.py` | `ba8a9eb35f06` |
| `splitters.py` | `690b1981879f` |
| `experiment_runner.py` | `f8da9aa77112` |
| `hybrid_schema.py` | `272b74dec9bc` |
| `hybrid_index.py` | `e772551601bc` |
| `hybrid_prompt.py` | `3fd2b4717260` |
| `hybrid_executor.py` | `e796e08e2648` |
| `hybrid_gate.py` | `255c4299b755` |
| `model_openai.py` | `0839fc1ed6ea` |
| `run_meta.py` | `a7c1ccd83723` |
| `requirements.txt` | `38ffe361be94` |
| `verify_anchorpatch.py` | `310fd2ee8ed5` |
| `paired_campaign_dispatch.py` | `035f13bc161b` |
| `analyze.py` | `4550d351652d` |

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

尚未运行 API、Key probe、付费 smoke 或 paired10；因此本卡仍不提供真实 token、费用或效果结论。

## 运行方式

```bash
cd HP_V8
PYTHONUTF8=1 python src/experiment_runner.py ...
python -B src/verify_anchorpatch.py --dir exp_<YYYYMMDD>_<slug>
```

正式 API campaign 仍须先按仓库标准实验流程完成计划、审阅、零 API preflight、runtime evaluator smoke、Key 小探针与最终命令复核；顶层凭据不得复制到本目录。

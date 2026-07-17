# HP_V8（hybridpatch/8）版本卡草稿

状态：**active draft**。本版已完成零 API 实现与回归验证，尚未运行任何 API 实验；因此不声称评分、真实 token 或协议失败率已经改善。

## 五问摘要

1. **为什么迭代**：V7 的主提示无条件包含文件表、三层索引和四条完整路径，repair 又重复全部 schema、全部文件和原始响应；复杂信封还没有统一、可硬校验的显式负担边界。
2. **优化方向**：以领域无关的确定性词法分类按需选择两种 prompt profile；把 V8 plan 压缩为 `task_family` 与 `edit_footprint`；统一统计并限制显式操作、锚点、信封和块编号；repair 只携带当前失败所需上下文。
3. **当前证据**：冻结 dev20 的 400 步零 API 配对重放中，primary 平均字符数由 27,459.715 降为 20,079.900（-26.875%），36 个实际 V7 repair 对应的反事实 V8 repair 平均由 20,009.222 降为 15,939.111（-20.341%）。这些是字符数，不是 tokenizer token，也不是 API 效果证据。
4. **新问题**：nearest-rank P95 阈值会要求 25/394（6.35%）个历史成功信封合并操作或换路；零 API 路径审计发现 50/399（12.53%）个可解析 V7 chosen route 不在对应 V8 profile 的 allowed routes；DSL 成功样本只有 1 个；词法分类仍可能有误分类；被强杀的 runner 会 fail-closed 地保留未完成 metadata。
5. **下一轮靶子**：在单独预注册且代码已提交的 campaign 中检验真实 token、协议失败、评分与 preservation；本轮明确不运行该实验。

## 唯一研究假设

> 精简提示、协议和修复流程，并限制显式操作负担，可以降低模型 token 和协议失败，同时保持现有执行与保留语义。

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
- `block_movement` profile：只给出 coarse `split_struct2` 块索引、`dsl_rules` 与 `bounded_rewrite`；不构建 medium/fine 索引，不输出文件信息表。
- `bounded_rewrite` 始终是模型必须显式选择的安全路径，执行器不静默回退。
- repair 使用 primary 的同一 `prompt_classification`。合法 route 的普通错误仍只显示当前路径；无合法 route 时恢复该 profile 的全部路径；负担超限时只增加更高压缩替代路径，并逐条绑定正确的 `edit_footprint`。
- 无 parsed envelope 或当前为 `bounded_rewrite` 时，repair 恢复全部 editable 正文；local/bulk/DSL 继续按 action、错误与 target 筛选。只读文件仍只提供权威名称，不提供正文。

V8 plan 恰好包含两个字段：

| `edit_footprint` | `action.route` |
|---|---|
| `few_precise_edits` | `local_patch` |
| `many_repeated_edits` | `bulk_patch` |
| `block_movement` | `dsl_rules` |
| `whole_file_change` | `bounded_rewrite` |

每个 V8 local op 必须声明已知、非空 `file`；每个 V8 bulk op 必须声明非空且全部属于 runner editable 边界的 `scope`。未知 scope 在执行前返回明确 schema error，不允许执行器过滤后继续。这些新检查及预算只对 V8 生效，V1–V7 的 legacy plan、缺省范围和 replay 分支不变。

## 冻结协议负担阈值

数据源：`HP_V7/exp_20260711_hybridv7dev20full` 的 400 行。成功子集为 `bdpatch.actual_method == "hybridpatch"` 的 394 个 chosen V7 信封，包含 5 个 partial acceptance，排除 6 个 kept-context。分位数使用 nearest-rank 条件 P95。

| 指标 | V8 上限 | V7 条件 P95 | V7 max |
|---|---:|---:|---:|
| local ops | 31 | 31 | 92 |
| bulk ops | 30 | 30 | 32 |
| anchor UTF-8 bytes | 4,080 | 4,080 | 17,512 |
| canonical envelope UTF-8 bytes | 2,898 | 2,898 | 20,109 |
| explicit block IDs | 39 | 39（n=1） | 39 |

等于上限放行；严格大于任一上限时，在执行 action 前返回 `protocol_burden_exceeded`，不截断、不部分执行，并允许既有的唯一一次 repair。共享统计函数按 V4+ 规则解引用 `@body:` 后累计锚点；canonical envelope 使用 `ensure_ascii=False`、`sort_keys=True` 与 compact separators。

完整零 API 数据与局限见：

- `analysis/protocol_burden_v7_v8.json`
- `analysis/protocol_burden_v7_v8.md`

## 零 API 路径兼容审计

- V8 profile 分布：`default=184`、`block_movement=216`。
- 400 步中 399 步存在可解析 V7 chosen route；1 步 route unavailable，单列且不进入不兼容分母。
- 兼容 349/399；不兼容 50/399（12.53%）。成功提交子集不兼容 49/394（12.44%）。
- 不兼容 route：`local_patch=41`、`bulk_patch=8`、`dsl_rules=1`；方向为 forward 24、backward 26。
- JSON 报告保存逐步结果以及按 matched term、sample、round trip、direction 稳定选取的代表案例；Markdown 报告提供人类可读摘要。

该审计只判断历史 V7 chosen route 是否被反事实 V8 profile 列为 allowed route，不判断路径语义等效，不预测模型在 V8 下的选择，也不是效果实验。

## 运行记录

`bdpatch.hybrid` 新增 prompt profile/分类命中、prompt 与 repair 字符数、信封字符数、local/bulk/显式操作数、锚点字节、显式块编号数、实际 touched target 文件数、最终 target/输入 editable UTF-8 大小比率及任一 attempt 的负担超限标志。数值负担字段描述最终 chosen envelope。

run metadata 升级为 `anchorpatch.run_metadata/3`，在锁内记录并复用 campaign 的 `run_git_commit`、`git_tree_state`、带时区 `started_at` 与 `finished_at`，拒绝跨 commit、tree state、代码指纹或 transport 混跑。

## 当前 11 项指纹

| 文件 | SHA-1 前 12 位 |
|---|---|
| `patch_schema.py` | `ba8a9eb35f06` |
| `splitters.py` | `690b1981879f` |
| `experiment_runner.py` | `6966cb457740` |
| `hybrid_schema.py` | `84006905d823` |
| `hybrid_index.py` | `e772551601bc` |
| `hybrid_prompt.py` | `7793084a1988` |
| `hybrid_executor.py` | `aa73c9aece28` |
| `hybrid_gate.py` | `255c4299b755` |
| `model_openai.py` | `0839fc1ed6ea` |
| `run_meta.py` | `eea28aa1d34d` |
| `requirements.txt` | `38ffe361be94` |

## 零 API 验证（2026-07-17）

工作目录为 `HP_V8/`，所有命令由 Git Bash 执行：

1. 全部 83 个 `src/**/*.py` 编译到 `/tmp`：PASS；源码目录未生成 `__pycache__`。
2. `PYTHONUTF8=1 python -B ./src/test_hybrid_executor.py`：`RESULT: PASS (72 tests)`。
3. `PYTHONUTF8=1 python -B ./src/splitters.py`：`RESULT: PASS (all splitters byte-exact coverage)`。
4. `PYTHONUTF8=1 python -B ./src/test_model_openai.py`：36 tests，PASS。
5. `PYTHONUTF8=1 python -B ./src/analyze_protocol_burden.py`：400 行、394 个成功信封、25 个阈值并集超限形态、50/399 个路径不兼容，PASS。

未运行 API、付费 smoke、正式实验、evaluator 重评分或冻结归档 verifier。

## 运行方式

```bash
cd HP_V8
PYTHONUTF8=1 python src/experiment_runner.py ...
python -B src/verify_anchorpatch.py --dir exp_<YYYYMMDD>_<slug>
```

正式 API campaign 仍须先按仓库标准实验流程完成计划、审阅、零 API preflight、runtime evaluator smoke、Key 小探针与最终命令复核；顶层凭据不得复制到本目录。

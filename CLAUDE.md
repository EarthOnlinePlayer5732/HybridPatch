# CLAUDE.md

Guidance for Claude Code when working in this repository.

# HybridPatch（受约束写入 vs 全局重写）

## 项目定性

长程文档编辑里，全局重写（FullRewrite）容易把未被要求改动的内容一起改坏。**HybridPatch** 用受约束写入替代无约束全文重写：LLM 输出一个结构化信封（计划单 + 一条 action 路径），确定性执行器应用它，未被声明的源字节由执行器逐字节保留。主线目标只有一个：**做出一个整体上优于全局重写的新方法**，与 FullRewrite 在同一 seeded 任务序列上配对比较。

本仓库已按 HP_Vx 方案版本文件夹化：`HP_V3`–`HP_V7` 是互不引用的完整运行快照；`transport/` 是 API/传输层主战场；方法归档、FR baseline 与传输诊断分别归属其版本、`Baseline/`、`transport/`。旧版本冻结后不再修改。

## 目录结构

```text
HP_V3/ ... HP_V7/  自包含版本根：src/ + prompts/ + data junction + exp_*/ + VERSION.md；另有只读 provenance overlay：records/ + EXPERIMENTS.md
transport/          API 源码、两条 revision 线文档、传输诊断归档
Baseline/           原 FR 冻结基线、基线政策，以及不覆盖原基线的 FR+Official 派生版本
data/               共享只读数据；各 HP_Vx/data junction 到这里
docs/               跨版本活文档、迭代史与重构报告
_attic/             只收纳不删除：短 smoke、旧入口、历史日志与重建候选
MIGRATION_MAP.md     全量旧路径到新路径映射
```

每个 `HP_Vx` 是独立运行根。命令先 `cd HP_Vx`，再指向该目录的 `src/`；domain prompts 依赖 cwd，data 路径依赖 `src` 的父目录，两者缺一不可。`HP_V7` 是最新冻结可运行参考，当前没有可写的 active 方法快照；下一次方法迭代先复制为 `HP_V8`。当前终端统一使用 Git Bash，所有 Python 命令先 `export PYTHONUTF8=1`。

## 三层管线（最新冻结参考 `HP_V7/src/`）

```
文档(bytes) → splitters.split_struct2 → Block 列表(block_id = "<filename>:<seq>")
            → hybrid_prompt.build_hybrid_prompt(全文 + 文件/块索引 + 4 路径 schema + 规则)
            → LLM 输出信封 JSON(+可选 [FILE BODIES] 围栏原文块) → extract_hybrid_json
            → hybrid_executor.apply_hybrid(确定性执行 + HybridExecLog 遥测)
            → {filename: content} → domains.get_domain(...).evaluate_context → RS
```

| 文件 | 职责 |
|---|---|
| `hybrid_schema.py` | 信封/路由/协议校验；`PROTOCOL_V1`…`PROTOCOL_V7` / `rev_of`（版本门控） |
| `hybrid_index.py` | 确定性文件/块索引（block/fragment 表、块摘要） |
| `hybrid_prompt.py` | 提示构造 + `extract_hybrid_json`（信封 + `[FILE BODIES]` 围栏原文块解析） |
| `hybrid_executor.py` | `apply_hybrid`：确定性执行 4 路径 + 来源复制 + 字节保留 |
| `hybrid_gate.py` | reference-free 输出闸 + `audit_forward_completion` |
| `build_hybrid_split.py` | 从 research_splits 生成 `data/hybrid_split.json`（dev/val/test） |
| `experiment_runner.py` | HybridPatch/FR relay 循环；HP 对非空且已有协议信号的 JSON/schema/op 拒绝/gate 失败最多一次 repair，完整空响应/refusal/非协议文本不 repair；FR 不 repair；禁静默 FR fallback；main 按样本隔离故障 |
| `verify_anchorpatch.py` | 结果复核：从 raw 响应独立重算每个 RS；非零退出表示结果不一致，需调查 |
| `analyze.py` | RS@k 配对统计 + hybrid 遥测 → `<dir>/analysis/comparison.md` |

## 4 条 action 路径

| 任务形态 | 路径 | 说明 |
|---|---|---|
| 少量精确编辑 | `local_patch` | old_text/anchor_text 文件级匹配、须唯一或给 occurrence |
| 大量同型修改 | `bulk_patch` | 字面 replace_all / delete_lines_containing，禁正则 |
| 块搬运/分发 | `dsl_rules` | 仅块 copy/distribute，非通用转换；受 R-ENUM 上限 |
| 整文件生成/转换 | `bounded_rewrite` | 声明可写文件，内容走 `content` 或 `@body:` 引用 |

**文件体传输**：含反斜杠/引号/多行的文本（content/new_text **及** old_text/anchor_text）用 `@body:<name>` 哨兵引用，在 JSON 后的 `[FILE BODIES]` 段用变长围栏原文块承载——零 JSON 转义。

**hybridpatch/2 语义**（`rev_of` 按信封 protocol 门控；`hybridpatch/1` 走严格旧语义）：
- E1 空白无关匹配阶梯（精确→空白无关，可见字符仍精确、仍要求唯一）
- E2 文件级跨 block 匹配（span 映射回重叠块，字节保留不变）
- E3 distribute 局部覆盖（未指派块留在源文件）

**hybridpatch/3 语义**（含全部 v2 语义）：
- local_patch 快照定位：所有 op 的 occurrence/唯一性一律按**步骤输入文档**解析（模型在 prompt 里看到的编号即最终编号，前序 op 不再使后续 occurrence 漂移），span 校验互不重叠后一次性应用（右到左）
- 重叠 span 显式拒绝（`overlapping_span`）；同位 insert 按 op 顺序组合

**hybridpatch/4 语义**（含全部 v3 语义）：
- bulk_patch 文件级匹配：`replace_all`/`delete_lines_containing` 与 local_patch 对齐，字面锚点可跨 struct2 块边界（修 FINDINGS §217 docker6/fonteng3 kept-context 缺口）
- `@body:` 引用名空白归一化：哨兵前后/名字内嵌空白（如 `\n\n`）不再导致悬空；真悬空引用仍显式拒绝
- ws 容错阶梯两趟化：先按原文 token 匹配，无命中才退 `_unescape_ws` 变体（`\ref`/`\rho` 等反斜杠 token 不再被误转 CR）

**hybridpatch/5 语义**（含全部 v4 语义）：
- bulk_patch 快照定位：所有 bulk op 的匹配与 expected count 按**步骤输入文档**解析（前序 op 插入的文本不会被后续 op 再匹配，修 FINDINGS §219 fonteng3 RT7 自毁撤销线索）；跨 op span 冲突显式拒绝（同行 delete 去重），右到左一次性应用——与 local_patch v3 快照语义同构

**hybridpatch/6 语义**（含全部 v5 语义）：
- C1 包含型重叠消解：删行 span 完全包含 replace span 时吞并（先替换后删行=直接删行，与 v4 顺序结果一致，修 FINDINGS §221 circuit2 误拒）；部分重叠与 replace-replace 重叠仍显式拒绝
- C2 格式健康 gate（`hybrid_gate._format_health_errors`，按 exec log 的 `protocol_rev` 门控）：扩展名 lint 注册表（json/xml 系/qasm/pdb，公共格式解析器、惰性导入）；被改文件"输入可解析则输出不得回退"、新建 lintable 文件必须可解析；`format_regression:<file>` 走现有单次 repair，再败 kept-context。反事实验证：v5 campaign 400 步仅 8 步命中、全部属于 quantum4/protein1 真实损伤链，零误报

**hybridpatch/7 语义**（当前生成默认；执行器/gate 语义与 v6 逐字节相同，v7 只改提交政策）：
- 部分接受提交（runner/verify 层，共享 `hybrid_gate.partial_acceptance_eligible`，按信封 rev 硬门控）：单次 repair 后 chosen attempt 终局仍失败、且失败**仅**为 op 拒绝（local/bulk 路由、≥1 op 接受、零其他 gate 错误含 C2、无 route violation、输出≠输入）时，提交执行器的部分应用结果（接受 op 已应用、被拒 op 跳过）替代 kept-context；遥测 `partial_acceptance` + `partial_skipped_ops`。快照语义（v3 local/v5 bulk）保证 op 相互独立，部分应用良定义、preservation 不变量不受影响。设计证明：FINDINGS §226（v6+v5 归档反事实扫描 8 个可计分分歧步净 +0.017，fonteng3 RT7 0.842→1.000 全救回，mathlean2 型唯一负例 −0.045 有界）

**核心不变量**：`preservation_violations` 必须恒为 0（未声明块被改 = 执行器 bug）。这是 HybridPatch 唯一稳健的字节级声明。

## 工作纪律

1. **执行器改动必跑回归**：在新建的活跃 HP 版本根内，改 `hybrid_executor.py`/`splitters.py`/`patch_schema.py` 后必跑 `python -B src/test_hybrid_executor.py` 与 `python -B src/splitters.py`，非零退出不得交付。
2. **结果可追溯**：任何实验结论必须出自真实运行产物（JSONL/checkpoint），并引用归档中已有的结果复核记录。冻结归档不为写文档而重复运行 `verify_anchorpatch.py`；禁止根据预期编造或外推数字。
3. **新实验隔离输出**：跑新实验显式 `--out_dir` 指向新目录。方法实验放所属 `HP_Vx/exp_*`；传输诊断放 `transport/exp_*`；baseline 放 `Baseline/exp_*`。
4. **逐轮分析**：每完成一次 relay 往返，分析本轮 LLM 实际表现（RS 下降/恢复都要归因），实事求是不写断言式判断；结论写入 `docs/FINDINGS.md`。
5. **版本门控**：改执行器语义走新 rev + 版本门控，旧归档 replay 须字节级复现。
6. **DeepSeek/MiniMax 无 JSON mode**：补丁解析依赖 `extract_hybrid_json` 鲁棒提取，勿改 `response_format`。
7. **冻结边界**：`HP_V3`–`HP_V7` 均为冻结快照。新的方法语义不得回改旧目录；复制最新版本为 `HP_V(x+1)` 后再改，protocol 号与文件夹号对齐。
8. **传输边界**：API 修改只在 `transport/` 发生并新增 revision；只有先建立新的可写 `HP_Vx` 后，才把已验证 transport 原字节同步进去并记录源/目标指纹。冻结 HP 永不回灌。
9. **共享数据与凭据**：顶层 `data/` 只读；`.env` / `.env.frkeys` 只保留顶层一份，不得复制进 HP、transport 或文档。
10. **标准实验流程与审计 overlay**：新 HP_V8+ API 实验先按 `docs/标准实验流程.md` 创建 `docs/experiment_plans/<experiment_id>.md` 并完成实验前审阅；实验结束后使用 `tools/process_experiment.py prepare → review → finalize`。机械事实与研究判断分离，finalize 才原子写入 catalog 并生成 records。历史 record 重建可用底层 builder/finalizer。`records/` 和 `EXPERIMENTS.md` 只记录 provenance，不改变冻结的 `src/`、`prompts/`、checkpoint、raw response 或 evaluator 字节；后处理不触发 API。
11. **V8+ 运行身份**：正式付费实验前，runner 必须把 `run_git_commit`、`git_tree_state=clean|dirty`、明确的 `started_at` 和 `finished_at` 写入 `run_metadata.jsonl`；新后处理流程拒绝缺失这些字段的运行，不能再用当前 HEAD、mtime 或整理时间补猜。

## 常用命令

```bash
# 从 hybridpatch_clean 根进入要运行的版本
cd ./HP_V7
export PYTHONUTF8=1

# 无 API、零费用
python -B ./src/test_hybrid_executor.py
python -B ./src/splitters.py
python -B ./src/test_model_openai.py

# 调 LLM（产生费用）——仅在已创建的新可写版本中；下一版应为 HP_V8
cd ../HP_V8
python -m dotenv -f ../.env run -- python ./src/experiment_runner.py --sample malware6 latex2 --methods hybridpatch fullrewrite --num_round_trips 10 --skip_distractor --model minimax-m3 --out_dir exp_SLUG --notes "<什么实验>"

# 仅限用户明确要求的全新未冻结实验；冻结归档不得重复运行结果复核
python -B ./src/verify_anchorpatch.py --dir ./exp_SLUG
python -B ./src/analyze.py --dir ./exp_SLUG --K 10 --critical_theta 0.10

# 长 adaptive-thinking 运行的只读进度
python -B ./src/monitor_experiment.py --dir ./exp_SLUG --methods hybridpatch fullrewrite --watch
```

- 默认模型 `deepseek-v4-flash` 仍可使用显式配置的 OpenAI 兼容端点；正式 OpenCode
  DeepSeek-V4-Flash campaign 固定走 OpenCode Zen
  `https://opencode.ai/zen/go/v1/chat/completions`（USD），revision
  `opencode_openai_compatible/6`，stream，`reasoning_effort=high`。正式
  `deepseek_full234` 固定 RT10；历史 `/3` RT2 non-stream 目录只作冻结 supporting
  evidence，`/4` 与 `/5` stream 目录只作冻结 transport evidence；旧目录都不得原地
  resume 或升级成 `/6`。流必须同时包含非空 `finish_reason` 和计数完整一致的 terminal
  usage；usage 可与 finish 位于同一 choice chunk，也可位于随后的 usage-only chunk。
  finish 后只允许无 choices、无 usage 的尾随元数据；partial、乱序、重复或空 usage
  stream 全量重发且不得提交部分正文。
  生成前 HTTP 503 重试不消耗有限重试额度，使用带 worker spread 的指数退避；
  502 与生成后失败消耗额度；provider `Retry-After` 最多按 300 秒执行，所有失败
  attempt 的脱敏 body/message 同时写入 API terminal row 与 transport sidecar。
  `/6` 使用 `anchorpatch.transport_event/2` compact sidecar：每 call 一个 header、每
  attempt 仅保存单调 checkpoint、聚合 summary 与 terminal end，规模为 O(attempt)；
  不保存逐 chunk raw event，也不生成重复 `.sse.jsonl`。完整规范见
  `transport/docs/API_TRANSPORT_DEEPSEEK_V6.md`。
  `minimax-m3` 固定走 OpenCode Go
  `https://opencode.ai/zen/go/v1/messages`（USD），Python 使用 Anthropic SDK，所有调用均为
  adaptive thinking，默认/硬上限 `max_tokens=131072`。
- MiniMax OpenCode 新实验遵循 `transport/docs/API_TRANSPORT_V4.md`：完整终止链才提交；HTTP 200 `Streaming response failed`、缺 `message_stop`/终结 usage 或 block 未正常闭合先分类为 retryable `incomplete_stream`；每个 exact semantic call 最多 2 个 response slot（首次 + 1 次全量重发），另有 3 次生成前 transient failure，禁止 partial continuation。冻结 transport-v3 归档仍只按 `API_TRANSPORT_FROZEN_V3.md` 解释。
- 完整空/near-empty 是模型失败，不做 transport retry，也不触发 HP repair；基础设施耗尽会在提交实验行前终止该步并留存 ledger，分析时按缺失的 `score=null` 处理，禁止记模型 0 分。paired campaign 仅隔离四证一致的 retry exhaustion sample，其他 sample 继续；恢复用同 fingerprint 的新 semantic generation，不在同一 semantic ID 内重置 R2/I3。新实验必须使用 transport revision `opencode_anthropic_sdk/4` 和新 `out_dir`。
- 另有 MiniMax **官方非流式**传输 `minimax_official_nonstream/1`（`MINIMAX_TRANSPORT=official_nonstream` + `MINIMAX_API_KEY`，详见 `transport/docs/API_TRANSPORT_MINIMAX_OFFICIAL_V1.md`）：baseline 对齐语义（盲重试、完整 200 照单接受含 `finish=abort`）、5h 限额自动等待；与 OpenCode 路线按 revision 门禁互斥，禁止混目录；`MINIMAX_TRANSPORT` 不得写入 `.env`。
- API 密钥只在顶层 `.env` / `.env.frkeys`。HP 内 `model_openai.py` 不会上溯发现它；用父进程环境或 `python -m dotenv -f ../.env run -- ...` 注入。多 Key 工具显式传 `--keys_file ../.env.frkeys`。
- `experiment_runner.py` 单进程、每次一个/多个样本；并行靠多开进程（每样本独立 checkpoint 幂等续跑）。

## 重构后的迭代惯例

- **方法迭代**：复制最新 `HP_Vx` 为 `HP_V(x+1)`，清除/另存旧实验输出后只在新目录改；协议号与文件夹号对齐。旧版本冻结且必须继续可重放自己的归档。
- **API 迭代**：只改 `transport/`，延续 `opencode_stream/N`、`opencode_anthropic_sdk/N`、`minimax_official_nonstream/N` revision；更新 `transport/API_ITERATION_LOG.md`。完成验证且已有新的可写 HP 后再同步，并记录双方指纹。
- **实验落位**：方法实验 → 所属 `HP_Vx/exp_*`；传输诊断 → `transport/exp_*`；基线 → `Baseline/exp_*`。命名沿用 `exp_<YYYYMMDD>_<slug>`。
- **实验 record**：新 HP_V8+ 运行使用 `python ./tools/process_experiment.py prepare --experiment ./HP_V8/exp_SLUG --confirm-stopped`，审核生成的 `analysis/record_review.yaml` 后再运行 `finalize`；`report.md` 是 canonical 人类入口，`summary.json` 是唯一 canonical machine scope，`casebook.jsonl` 是下一轮诊断案例集。底层 `finalize_experiment.py` 只用于已登记实验。
- **归档重放**：每个归档优先由所在版本的 verifier 重放；HP_V7 含 v3–v7 rev 门，可作兼容兜底。
- **删除纪律**：任何候删内容先移入 `_attic/`。不得直接删除冻结归档、快照、data、checkpoint 或权重。

## 必读文档

| 文档 | 内容 |
|---|---|
| `docs/RESEARCH_JOURNAL.md` | 研究实录（活文档）：设计哲学、实现、完整 Bug 史（现象/根因/修复/教训）、实验轨迹；每个 campaign 跑完按其 §6 模板追加 |
| `docs/项目结构与迭代史.md` | 接手者导览：根目录结构图 + v3→v7 迭代编年史（每轮动机/方向/效果/新问题/下轮靶子）+ 样本攻坚台账 + 全 campaign 数字速查表 |
| `docs/HYBRIDPATCH_DESIGN.md` | 设计原则、4 路径、系统结构 |
| `docs/FINDINGS.md` | 实验发现与根因（track 完成 + win 机制 + 契约对齐 + body-ref/thinking 修复） |
| `docs/active_log.md` | before/after 迭代游标 |
| `docs/EXPERIMENT_INDEX.md` | 全版本实验入口：lifecycle、claim 使用、canonical scope 和 verification |
| `docs/EXPERIMENT_RECORDS.md` | record schema、三级保存、生成/校验流程和安全边界 |
| `docs/标准实验流程.md` | 新实验从计划、preflight、运行、独立子代理审阅到 finalize/日志归档的标准流程 |
| `docs/AI_REVIEW_GUIDE.md` | GitHub 外部模型的代码+结果阅读顺序、结论边界和 V8 审阅提示 |
| `Baseline/FR冻结计划.md` | 原 FR 规范基线（**已冻结**：`Baseline/exp_20260710_frbaseline234`，233 样本规范范围 PASS 2330/2330；含已排除 python1 的归档级重放共 2334 行）：政策、有效性边界、执行记录；引用基线前必读 |
| `Baseline/FR+Official/README.md` | 新增派生基线 **FR+Official**：87 条完整 MiniMax 官方 API 链替换对应来源、146 条保留原冻结 FR；不覆盖原 FR，含异常率定义与复现清单 |
| `Baseline/FR+Official/val40_comparison.md` | val40 对 FR+Official 的 post-hoc 混合来源敏感性分析；不得解释为无偏方法效应 |
| `transport/docs/API_TRANSPORT_FROZEN_V3.md` | OpenCode Go / Anthropic Messages 完整性、双预算 retry、空响应、崩溃回放与计分的冻结规范 |
| `transport/docs/API_TRANSPORT_V4.md` | 新 OpenCode 活动规范：HTTP 200 不完整流、严格 R2/I3、semantic lineage、样本级隔离与恢复 |
| `transport/docs/API_TRANSPORT_MINIMAX_OFFICIAL_V1.md` | MiniMax 官方非流式传输（baseline 对齐语义、`finish=abort` 病理、5h 限额预案、官方重跑来源与 FR+Official 派生规则）；`MINIMAX_TRANSPORT=official_nonstream` 启用 |
| `MIGRATION_MAP.md` | HP_Vx 重构逐路径映射、归档归属与最终冻结状态 |
| `HP_Vx/VERSION.md` | 每版五问、来源、指纹等级、四连验收真实输出 |

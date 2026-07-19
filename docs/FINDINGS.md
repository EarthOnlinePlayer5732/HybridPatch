# HybridPatch — Design Findings

> Extracted from the parent workspace's ANCHORPATCH_DESIGN_AND_FINDINGS.md (§212-215). Only the hybridpatch track; v1/v2/C1-C70 history stayed in the parent repo.

## 212. HybridPatch vs FullRewrite track completed through distractor test

> 记录日期：2026-07-03。该段归档新的 `hybridpatch vs fullrewrite` track；C71/C30+ AP-pure 线未继续，v1/v2.1/v2.2 冻结路径未作为新证据修改。未编辑 raw JSONL、checkpoint、frozen task plan、sample/reference、evaluator 或 scoring semantics。

Scope and implementation:

- Canonical method id: `hybridpatch`; aliases and silent FR fallback are forbidden.
- Split artifact: `analysis/hybrid_split.json`, materialized from existing `dev20_20260625` and `holdout_reserve_20260625` as `dev=20`, `val=20`, `test=20`, `unused_reserve=20`.
- New HybridPatch surface: schema/index/prompt/executor/gate/test modules under `anchorpatch/`, top-level `experiment_runner.py` branch, result verification in `verify_anchorpatch.py`, hybrid telemetry in `analyze.py`, and run metadata fingerprint coverage for hybrid modules.
- Repair policy: one JSON/schema-only protocol repair is allowed; semantic/gate failures do not repair and carry the unchanged editable workspace forward with failure telemetry.
- CriticalFailure theta was calibrated on dev-mini and frozen at `0.10`.

Replay-passed results:

- Dev-mini out_dir: `anchorpatch/exp_20260703_hybridsmoke`, `minimax-m3`, 10 samples, `10RT`, no distractor. Replay PASS with `200` backward RS reproduced. RS@10: `hybridpatch=0.811`, `fullrewrite=0.728`; CriticalFailure@10: `5/90` vs `6/90`; bounded rewrite share `51.5%`.
- Val out_dir: `anchorpatch/exp_20260703_hybridval`, `minimax-m3`, val20, `10RT`, no distractor. Replay PASS with `400` backward RS reproduced. RS@10: `hybridpatch=0.603`, `fullrewrite=0.474`; CriticalFailure@10: `17/180` vs `18/180`; bounded rewrite share `70.8%`.
- Test out_dir: `anchorpatch/exp_20260703_hybridtest`, `minimax-m3`, test20, `10RT`, distractor enabled. Replay PASS with `400` backward RS reproduced. RS@10: `hybridpatch=0.584`, `fullrewrite=0.418`; paired backward mean delta `+0.181` over `200` pairs; CriticalFailure@10: `17/180` vs `23/180`; bounded rewrite share `76.8%`.

Operational notes:

- The distractor test hit one M3 5-hour quota stop during hybrid batch2 and resumed from runner checkpoints after reset.
- A verifier-only distractor replay bug was fixed after test completion: replay now passes editable-only context into each step, matching runner behavior. This did not change raw results, task plans, samples, evaluator code, prompt/protocol, runner, or executor.
- `analyze.py` report labeling was fixed offline to derive distractor status from rows and to label hybrid-only reports as `HybridPatch vs FullRewrite`.

Interpretation:

- The frozen track completed all requested gates with replay-passed dev-mini, val, and distractor test metrics.
- The evidence is for the declared hybrid method, not AP-pure capability. High bounded-rewrite share is a required identity-dilution disclosure and remains the main caveat.
- The test result is favorable to `hybridpatch` on this split, but should be reported with route share, no-op/effective-modification audit, repair rate, gate failure rate, and the verifier maintenance note.

## 213. HybridPatch test attribution: win mechanism, self-inflicted transport bug, validity risks

> 记录日期：2026-07-04。本段是对 `anchorpatch/exp_20260703_hybridtest`（只读归档，未编辑 raw JSONL/checkpoint/task_plan/sample/reference/evaluator/scoring）的逐样本归因，两条独立子代理调查、主 Agent 复核证据链。事实性描述，未定处如实标注。

### 赢的机制（RS@10 `0.584` vs `0.418`，配对 backward Δ `+0.181`，ECR 条件化 169 对仍 `+0.135`）

优势主要来自 FullRewrite 的灾难放大机制被有界写入规避，而非补丁路径的正贡献：

- **FR max_tokens 截断 → 文件丢失/改名/工作区清空（不可逆）**：`translation2` RT3 forward `finish_reason=max_tokens`（completion 20000），围栏未闭合，`parse_context_string` 把目标 `ja.po` 解析成名为 `po` 的文件、内容截断到 3 条目；之后每轮 `ja.po` 恒 ~900B/3 条目（`ref_entry_count=48`），RS@10 归 0。`weather1` RT3 forward 同样 max_tokens，围栏未闭合导致解析为空、整个可编辑工作区被清空（`docs/fullrewrite/weather1/rt03_fwd_metar_table_imperial/` 只剩 `_step.json`），模型随后在 raw 里明确表示"看不到 bulletin.txt"，RS@10 归 0。HybridPatch 在这两样本走 bounded_rewrite，forward completion 仅 4k–5k，从未触顶，最终文档字节级完好。
- **FR 静默内容污染（无报错、逐轮携带）**：`protein3` RT6 一次往返把 PDB 记录复制错类（ATOM/HETATM 92/8 → 120/1、serial 不连续），全文重写永不自愈，0.99→0.063。HybridPatch 走 bounded_rewrite 20/20、结构保持 92/8。
- **验证门 + 失败保留上下文 = 优雅降级 vs 灾难替换**：FR 一次坏输出=文档被垃圾整体替换；hybrid 一次坏输出=保留上一轮好文档（kept_context，`bytes_changed=False`）。这是 CriticalFailure@10 `17/180` vs `23/180` 的机制解释。
- **公平性核查通过**：distractor 每步用原始内容 `merge_distractor` 回覆盖、target_filenames 排除 distractor、错误行两方法同样映射 RS `0.0`（非负惩罚）。FR 崩溃是全局重写范式的真实属性，非 harness 冤枉。模型在 raw 里正确将 distractor 识别为参考材料、未泄漏进目标文档。

### 自伤 bug（a 类，本轮最重要单点发现）

- **malware3 全 10 轮 RS 0.000**，同样本 FR ~0.97–1.0。根因：hybrid 协议把整文件塞进信封 JSON 的 `action.files[].content` 字符串，模型在嵌套 JSON 里**少转义一层反斜杠**（`\Global` 应为 `\\Global`），落盘 YARA 文件带非法转义序列 `\G`，评估器每轮报 `Invalid escape sequence: \G, at line 93`。
- **验证门未拦截**（`ecr_pass=true`、`failure_reason=null`、gate 零报错）——broken-but-syntactically-plausible 文件被写盘、逐轮 0 分。FR 用围栏纯文本传输、无嵌套 JSON 双重转义陷阱，同样本正常。
- 该缺陷暴露所有反斜杠密集格式（YARA/正则/Windows 路径/LaTeX）。与 v1 时代 escape 匹配问题（本文档 §4）同族。malware3 一样本约拖掉 headline 0.05。

### 非方法信号（复核确认）

- `genealogy6` 双方全 0：任务特性（GEDCOM→Mermaid 不可逆 forward + 逐 ID 精确覆盖率评分），`evaluation.error=None`、`individual_coverage≈0`、两方法重建出完全不同的人物集，非方法差异、非评估器崩溃。
- `circuit1` FR `1.000@1→0.149@5→0.812@10`：真实 RS 波动，无异常轮（backward 行全 `error=None`、`noop_forward=false`）。hybrid 自身 RT9-10 跌到 0.127 与 local_patch `op_rejected` 后 backward 保留 forward 态有关（**保留上下文在 backward 方向=没还原=低分**，kept-context 是双刃剑）。

### 补丁路径的真实贡献（近零）

- test 路由份额 bounded_rewrite `307/400 (76.8%)`、生成字节占比 `0.797`、块存活率 `0.196`——当前方法真实身份是"带验证门的受限范围重写"，不是补丁方法。
- forward no-op 来源：`local_patch` 68/95 route steps no-op（锚点/字面量找不到，与 C30–C70 整条 AP-pure span-production 失败同源）、`dsl_rules` 9/9 全灭（模型发明不支持的规则或超 R-ENUM）、`bulk_patch` 5/10。dsl_rules 使用率 2%、成功率 0%，为负资产。

### 报告主张前必须披露的有效性风险

- **no-op 通胀**：合并 50 样本 RS@10 `0.637 vs 0.502`；若 forward no-op 轮强制记 0，合并变 `0.487 vs 0.502`（反超为负），仅 distractor test 在严格审计下保持正向 `0.490 vs 0.418`。可辩护主张范围收窄为"distractor 长程设定下"。
- **max_tokens 依赖**：FR 两个崩溃样本均撞 20000 上限；需 cap 敏感性消融堵"提高 cap FR 就不崩"的口。
- **val/test 混杂**：val 无 distractor、test 有 distractor，且样本集不同，"distractor 放大优势"目前无法因果归因。
- **成本反向**：hybrid 总 token `11.5M` vs FR `7.1M`（+61%，主要在 prompt 侧索引/块表），"减负"原则在成本维度不成立。

### 改进方向（一切改动=新 dev 迭代，冻结 val/test 链已关闭，归档不动）

- P0 传输层修复（已实现，无 API 验证 PASS）：文件体从嵌套 JSON `content` 字符串移到围栏 `[FILE BODIES]` 段（复用 `utils_context` 变长围栏），信封内用 `@body:<name>` 哨兵引用，执行器/replay 解引用为零转义字面体；内联 content 仍兼容旧归档；悬空 `@body:` 引用被拒、绝不写成哨兵字面串。改动落 `hybrid_schema/hybrid_executor/hybrid_prompt/experiment_runner/verify_anchorpatch/test_hybrid_executor`。回归：`test_hybrid_executor.py` 10/10（含 malware3 式 YARA `\Global\PIPE` 字节级往返、悬空引用拒绝、内联兼容）、`test_executor.py` PASS=236、`splitters.py` 字节级、`py_compile` 全绿、100 行旧归档 back-compat 复解析零误产 body。预计单独值 ~0.05 headline，待 dev smoke 确认。
- P1 归因消融：scoped-FR-only 臂（强制 bounded_rewrite）判定补丁路径当前是否零贡献；FR cap 敏感性量化截断悬崖占比。
- P2 补丁路径重修：片段 id 寻址替代字面锚点（攻 72% 锚点失败、绕开模型 span 生产短板、提高复制比例压低身份稀释）；砍 dsl_rules。
- P4 证据加固：第二 seed/第二模型复验；补无 distractor test 臂或有 distractor val 臂解混杂；`unused_reserve=20` 作改进版 sealed holdout；审计调整版 RS（no-op forward 记 0）升为共同主指标。

## 214. HybridPatch 执行器 vs Prompt 契约不一致排查 + hybridpatch/2 执行器优先对齐

> 记录日期：2026-07-04。人工发现 + 逐 op 代码核对（prompt/executor/schema/gate/index/runner repair）。改动全部走版本门控，旧 `hybridpatch/1` 归档字节级复现（结果复核四目录仍 PASS）。未编辑 raw JSONL/checkpoint/frozen task plan/sample/reference/evaluator/scoring。

### 病灶

P0 传输修复后，P0 冒烟（malware6/latex2/mathlean2/foodmenu6/docker6）转义 bug 已消除（malware6 RS@10 `0.951`），但整体配对 Δ `-0.037`：`local_patch` 43%、op_accept_rate `0.783`、gate 失败 33%、kept-context 33%、forward no-op `13/50`。根因是**我们同时在教 LLM 一套契约、却用另一套执行**：

| 机制 | prompt 暗示 | 执行器实际(v1) | 后果 |
|---|---|---|---|
| local_patch 匹配 | `file+old_text` 即可 | 逐 block 内 `find`、跨 block 不命中、须唯一(否则 not_unique/not_found)、无空白无关回退 | 大量 op 被拒→kept-context no-op |
| occurrence | 示例无 | schema 支持但 prompt 未教 | 多处命中无法消歧 |
| block 索引 | 展示 coarse+medium+fine 三尺度 | 只解析 coarse `file:seq`；M/F id 一律 not_found | 引用细尺度 id 100% 拒绝 |
| dsl distribute | 只说 assignments/discard | 要求覆盖全部 block(missing→violation) | 局部搬运→gate fail(dsl 9/9 挂) |
| dsl 能力 | "dsl_transform/规则"暗示通用转换 | 只支持块 copy/distribute | 发明不支持规则→schema_error |
| repair | 仅修 JSON/schema | op_rejected/route_violation 不触发 repair | 语义失败无救援→硬 no-op |

### 决策：执行器优先（"减负优先"第一原则）

判据修正——教模型更多规则=加负担；执行器确定性消解歧义=减负担。凡执行器能确定性且安全消解的，执行器吃下；prompt 只做"删陷阱/删过度承诺"(本身是减负) 与承载唯一不可消解歧义(occurrence)。

**执行器改（hybridpatch/2，版本门控）：**
- **E1 空白无关匹配阶梯**：移植 v1 `_match_span` 的"精确→空白无关"语义到新 hybrid 匹配器（不改冻结 executor.py），可见字符仍精确、仅空白模糊、仍要求唯一。
- **E2 文件级跨 block 匹配**：拼接文件文本找 old_text（可跨 struct2 块边界），再把 span 映射回仅重叠的块（未触碰块字节级不变，`preservation_violations=0`）。让现实契约=prompt 已暗示的"file+old_text 就够"，模型无需知道 block 存在。
- **E3 distribute 局部覆盖**：未指派块留在源文件（copy-forward 已保留），不再 missing→violation；保留 duplicate 检查。

**Prompt 改（全是"减信息"）：** 块表限 coarse-only（除陷阱 + 砍块表 token）、撤 dsl 宣传（明确只做块搬运、内容转换走 bounded_rewrite）、加一句 occurrence 规则+示例（唯一不可消解歧义）。

**版本门控**：`apply_hybrid` 按信封自带 `protocol` 分支（`rev_of`）；`hybridpatch/1`→严格 v1 语义（旧归档字节级复现），`hybridpatch/2`→上述放松。schema/extract 双版接受，生成端只发 v2。

**repair（M8）不动**：E1/E2 落地后 op-reject 预期大降，先看残留再定，避免第二次实质调用稀释"单次调用≈FR"公平性。

### 附带：MiniMax-M3 开推理

`model_openai.py` 加 `_minimax_thinking_config`（env `MINIMAX_THINKING` 门控、budget/max_tokens 不设人为上限、thinking 时强制 temperature=1）。thinking 计入 output tokens（成本/计价已含）；`_anthropic_text` 只收 text 块，thinking 不进 `raw_llm_response`，结果复核 replay 不受影响。默认关闭，旧行为不变；API 请求形状未验证，需 1 次 live 探针。

### 验证

`test_hybrid_executor.py` PASS 14/14（E1 空白无关、E2 跨 block 且 v1 拒/v2 收对照、occurrence 消歧、distribute 局部覆盖 + v1 missing-block 回归）；`test_executor.py` PASS=236；`splitters.py` 字节级；`py_compile` 全绿；四个旧归档结果复核 PASS（200/400/400/100 backward RS 字节级复现）。

## 215. exp_20260706_hybridthink5：SSE 流式修复后首个全程配对 campaign，HybridPatch 显著胜出（结果复核 PASS）

**背景**：exp_20260704_hybridv2think2 两臂均死于 524 重试耗尽——非流式请求 + thinking 使首字节时间超过 Cloudflare 120s Proxy Read Timeout，单次生成稳定超窗时重试结构性无效；且 run 中改码造成单目录混两个 code fingerprint、结果复核 FAIL（2 行可解释预存漂移，机制见 active log 2026-07-04 条目）。修复：`model_openai.py` minimax 路径改 SSE 流式（`stream:true` + `_read_sse_stream` 重组完整消息，字节持续流动使 CF 读超时不再触发；urllib socket timeout 顺带变为块间停滞守卫）；runner `main()` 每样本 try/except 隔离，单样本终态失败不再拖死后续样本。

**结果**（exp_20260706_hybridthink5：5 样本 × 10RT × 双臂，minimax-m3 + thinking + max_tokens 64000，单一 code fingerprint，结果复核 PASS 100/100 backward RS）：

- 配对 backward RS：AP 0.946 vs FR 0.571，Δ+0.375（n=50，t=6.36，p<0.001，d=0.90）；ECR 条件化 Δ+0.340（n=44）。CriticalFailure@10：AP 2/45 vs FR 4/45。
- RS@10 分域（AP vs FR）：foodmenu 0.983/0.025、mathlean 0.900/0.140、latex 0.741/0.424、malware 0.990/1.000、docker 0.684/0.945。与 p0smoke（hybridpatch/1、无 thinking、同 seed 同 task plan）纵向对比：foodmenu6 0.434→0.983 翻盘；docker6 0.647→0.684 仍败给 FR。注意纵向对比混杂 thinking/max_tokens/协议三因素，不可单独归因。
- 成本：AP tokens 2.39M vs FR 1.18M（约 2×）。preservation_violations 全程 0。
- 路由份额：bounded_rewrite 52%、local_patch 38%、bulk_patch 7%、失败 3%。

**9 个 kept-context 步骤归因**（9/100 步；no-op forward 5/50 与此重合）：

1. **occurrence 索引漂移**（5 op，docker6 RT3fwd/RT9bwd；执行器/协议语义缺陷 → §216 修复）：模型按步骤输入文档给 occurrence=1..4，执行器逐 op 对变异中文档解析，每替换一处剩余匹配递减，occ=3 时只剩 2 处 → occurrence_out_of_range；被接受的 op 实际也发生无害错位（恰好同替换文本）。docker6 是唯一败样本且其全部失败步为此 bug。
2. **@body 供给失败**（3 op）：latex2 RT2 声明 @body:bibtex.bib 但完全未写 [FILE BODIES]（end_turn、8.3k tokens，模型协议违规）；foodmenu6 RT6 finish=max_tokens（64000 烧满，[FILE BODIES] 截断）。
3. **纯模型错误**（2 op）：latex2 RT5 stale old_text（body 已正确解析后仍 matches=0——顺带验证 match-侧 body-ref 修复生效）；mathlean2 RT2 歧义 delete 不带 occurrence（按协议正确拒绝）。
4. **invalid_json ×3**（repair 尝试未救回）。

盲区：repair 触发器仅盯 invalid_json/schema，6/9 失败步（全部 op_rejected/gate 类）未获 repair 机会 → §216。

## 216. hybridpatch/3：occurrence 快照语义 + repair 触发扩展（版本门控，旧归档字节级复现）

**协议语义**（`rev_of` 按信封 protocol 门控；生成端默认升 `hybridpatch/3`，/1 与 /2 归档语义冻结）：

- local_patch 快照定位（`_run_local_v3`）：所有 op 的 occurrence/唯一性一律按**步骤输入文档**解析（模型在 prompt 里看到的编号即最终编号）；span 两两校验互不重叠（违者显式拒绝 `overlapping_span`）后右到左一次性应用；同位 insert 按 op 顺序组合。匹配阶梯（E1 空白无关、E2 文件级跨块）与唯一性规则沿用 v2。
- E3 distribute missing-block violation 改为仅 v1 记违规（v2/v3 共享放松）。

**prompt**：occurrence 快照语义说明（前序 op 不再漂移编号）+ 路由引导：同 old_text 多处/全部替换走 bulk_patch replace_all（docker6 案例任务本应走 bulk）。

**repair**：`need_repair` 从 invalid_json/schema 扩为任何失败类（op 拒绝/route violation/空输出/gate 失败——均为可精确描述错误）；repair prompt 泛化并携带 [TASK] + [EDITABLE DOCUMENTS] grounding（锚点级修复不再靠记忆猜），且明确允许补发 [FILE BODIES]（旧措辞 "Output ONLY the corrected JSON" 实际禁止 bodies 补发）。verify 的 replay 语义不变（只重放存储的两次尝试 + 确定性择优，触发条件不参与 replay）。

**验证**：test_hybrid_executor 21/21（新增 4 v3 用例：docker6 场景复刻含 v2 行为锁定、快照不受前序 op 影响、overlap 拒绝、同位 insert 组合）；splitters 字节级；py_compile 全绿；结果复核 replay PASS：exp_20260706_hybridthink5（100，/2 envelope）与 exp_20260704_hybridp0smoke（100，/1 envelope）在新代码下字节级复现。已同步移植 hybridpatch_clean（hybrid 四文件哈希核对后整体覆盖 + model_openai SSE + runner repair/隔离三处编辑），clean 侧 21/21 + splitters + think5 临时拷贝 replay PASS。

**未验证**：v3 + 扩展 repair 尚无 live 实验数据；occurrence 修复对 docker6 的实际效果、扩展 repair 的净收益（额外调用成本 vs 救回率）待下一轮 fresh out_dir campaign。

## 217. exp_20260708_hybridv3dev20full：hybridpatch/3 首个全量 dev20 campaign（结果复核 1 处评估器非确定性、headline 胜出大半来自 FR 提供方伪影）

> 记录日期：2026-07-08。out_dir `exp_20260708_hybridv3dev20full`，`minimax-m3`（`MINIMAX_THINKING=1`），dev20 全 20 样本 × 双方法 × 10RT × no distractor，40/40 checkpoint 完成。两条独立子代理归因（FR 轨迹异常 / HP no-op），主 Agent 逐条源码复核。只读归档，未编辑 raw JSONL/checkpoint/task_plan/sample/evaluator/scoring。

### headline 与去伪影分解（本轮最重要单点）

- analyze 配对：RS@10 `hybridpatch=0.905` vs `fullrewrite`（见 comparison.md），200 对 backward mean `HP=0.910 vs FR=0.685`，Δ`+0.225`（t=7.55，p<0.001，d=0.53）；CriticalFailure@10 `5/180` vs `10/180`。
- **但 +0.225 大半是 MiniMax-M3 提供方伪影，非方法效应**：剔除 6 个被"thinking-runaway 空返回"污染的 FR 样本（foodmenu6/geotrack3/mathlean2/protein1/satellite6/translation4）后，14 样本 Δ 塌到 **+0.051**（HP 0.900 vs FR 0.848）；被污染 6 样本上 Δ 高达 +0.632（HP 0.934 vs FR 0.302）。即约 ¾ 的 headline 优势来自 FR 链被单次空/截断响应毒化，而非补丁路径的正贡献——与 §213 test 归因（赢的机制主要是 FR 灾难放大被规避）同族。

### FR 轨迹异常（10 个，全部归因，证据链见子代理报告）

- **empty_poison（提供方 thinking-runaway，6 个）**：MiniMax-M3 输出巨型 `thinking` 块饿死 `text`，四级形态——`stop=max_tokens`+542k thinking+0 text（foodmenu6 RT6）；`stop=None`+`output_tokens=0`+thinking+0 text（geotrack3 RT10、mathlean2 RT3、protein1 RT2、satellite6 RT3）；`stop=end_turn`+384k thinking+365B 前言桩（translation4 RT1）。runner FR 路径无空响应护栏（`gen_real=parse_context_string(raw)` 后逐轮 carry），一次空/桩即永久毒化后续全链（protein1/foodmenu6 各拖 8-9 轮归零）。geotrack3 是干净对照：空只在末轮 RT10，仅 RT10 崩（RS 0.824→0.117），RT1-9 不受影响——证明是空响应而非渐进能力衰退。
- **provider 截断（1 个）**：makefile4 RT5 fwd SSE 流中断，text 截断到 6274B（非空，scan 不标记）、envelope 全零、缺 `platforms.txt`→RT5 bwd 幻觉出假 Makefile→归零。
- **capability_collapse（1 个）**：edifact6 RT8 多文件拆分把 2 条 invoice 消息丢了 1 条（items 19→6）+ 改错文件名（`1801464167.edi` vs `invoice.edi`）→0.24，逐轮携带。与已知结论（AP/FR 均弱于多文件拆分/格式转换）一致。
- **恢复（2 个，均真实非造假）**：landmarks1 RT1→RT2（0.0→1.0）是评估器假阴性——RT1 全 30 条数据都在但用了非规范 KML 字段名大写/Point 坐标，评估器解析不到；RT2 恰好用规范小写+lon/lat SimpleData→满分。quantum4 RT1→RT2（context_mismatch→0.744→0.879）是真实自纠——RT1 只发了 3 文件中的 1 个（vqe.qasm），RT2+ 补齐 3 文件，逐轮结构收敛。

### HP no-op（5 个 kept-context，全部 `repair used=False`，源码复核确认）

step 语义为**全或无**：任一 op 拒绝即 `gate_pass=False`→整步保留上一轮文档（`bytes_changed=False`），部分成功的 op 被丢弃。

- **HP `bulk_patch` 执行器过严（2 个，最可行动）**：docker6 RT3、fonteng3 RT1——被拒锚点在整文件里**恰好唯一存在**但**跨 struct2 block 边界**。源码复核证实：`local_patch` 的 `_collect_local_matches_v2` 重建整文件文本作文件级搜索（可跨块），而 `bulk_patch` 的 `_find_matches` 逐块 `for bid ... edited[bid]` 搜索，跨块 needle 永远匹配不到。两例各有 2/5 个同信封的合法 op（COPY 重命名、逐 token 改名）因这一个跨块 op 拒绝被整体丢弃。→ 同文件修法：跨块多行 needle 路由到 local_patch，或给 `_run_bulk` 文件级匹配器。
- **LLM 失败（3 个，不同类）**：docker6 RT9（空 op 列表+repair 逐字回吐输入，文档本已在目标态，gate 正确拒 `effective_noop`）；latex2 RT5（幻觉出源文本里不存在的段末换行，锚点作为写的确实不存在；次生：`_unescape_ws` 把 `\r`→CR 破坏 `\ref`/`\rho` 反斜杠 token，使空白容错阶梯对 LaTeX 失效）；mathlean2 RT2（`@body:` 哨兵里嵌了 `\n\n` 致 `body_ref_not_found`，且 repair 调用返回空 completion——tokens 全耗在 thinking 通道）。

### 结果复核

- `verify_anchorpatch.py` 报 1 处 MISMATCH：`hybridpatch/quantum4 RT9 stored=0.9850 recomputed=1.0000`（差在 `qubit_decl_accuracy` 0.85 vs 1.0）。源码复核：`score_qubit_decls` 纯确定性，差异来自 `domain_quantum.parse_context` 的 AST+regex 回退双路径对相同字节解析出不同 qubit_decl 集（环境/解析器版本敏感，非 RNG、非遥测漂移）；**recompute 得分更高**，故 stored 0.985 若有偏也是保守低估、非造假。属已知类评估器非确定性（与父仓库 §DESIGN circuit2 stored/recomputed 预存差值同族）。**其余 39 样本全部字节级复现 PASS**。

### 待办（未验证）

- occurrence 快照修复（§216）对 docker6 的实际效果本轮仍未生效：docker6 RT3 的失败是 bulk_patch 跨块匹配缺口，不是 occurrence 漂移——这是 §216 prompt 路由引导之外的独立执行器缺口。
- FR 6 个空返回是否稳定复现（thinking-runaway 是采样随机，重跑可能不再空）需从对应 RT 起 rewind 重跑验证；makefile4/translation4 的截断与前言桩形态 scan 不标记（raw 非空），需单独判断是否重跑。

### rewind 重跑结果（2026-07-08 下午，5 个纯空返回样本 rewind 后重跑完成）

- **空返回不是位置稳定复现的，是随机提供方事件**：3/5 样本重跑后彻底干净（foodmenu6 RT6+ 恢复 0.987→0.859、geotrack3 RT10 恢复 0.824、mathlean2 RT3+ 恢复 0.928→0.836）；2/5 在**不同调用点**又随机撞上新空（protein1 原空 RT2 fwd→新空 RT2 bwd；satellite6 原空 RT3 bwd→新空 RT9 fwd，RT3-8 已恢复 0.955）。
- **全 campaign 空返回率按 api_calls.jsonl 统计：HP 7/440=1.6% vs FR 7/482=1.5%，两方法均等**。差异在承接架构：HP 的 7 个空——5 个被 repair 第二次调用救回（含 foodmenu6 RT5 bwd 0.999、makefile4 RT5 bwd 0.930）、1 个 repair 自身空→kept-context 优雅降级（mathlean2 RT2）、1 个无害；**零样本链被毒化**。FR 无护栏，每个空直接毒化剩余全链。空返回韧性是 HP 真实的架构优势（repair 第二调用 + kept-context），但需披露调用预算不对称（HP 失败时有第二次调用，FR 恒一次）。
- 重跑后配对：Δ 从 +0.225 收敛到 **+0.141**（200 对，t=5.43，p<0.001，d=0.38）；剔除仍污染的 3 样本（protein1/satellite6/translation4）后 Δ=+0.048（n=17）。FR 臂如需完全去伪影还差 protein1（from_rt 2）与 satellite6（from_rt 9）二次 rewind。

### 上游 baseline 对齐审计（2026-07-09，对照 github.com/microsoft/DELEGATE52 主分支源码）

重跑验证之外的第二条证据链：逐文件核对我们的 FR 实现与官方 baseline，确认空返回毒化不是本仓库实现错误。

- **字节级一致**（仅 CRLF 行尾差异）：`utils_context.py`（stringify/parse_context_string/is_context_complete）、`utils_env.py`（shuffle_context/merge_distractor/load_sample）、`prompts/domain_documents.txt`（FR 提示模板）。
- **relay 语义一致**：上游 `run_relay.py` 对每步输出执行 `shuffle_context(merge_distractor(parse_context_string(llm_response), distractor))` 后无条件传入下一步——空响应解析为空 context 照样传播毒化全链；仅 `llm_response is None`（fiction 长度校验路径，本数据集不触发）才 abort。我们 runner 的循环与之逐行同构，FR 无 keep-context、无 retry-on-empty，行为忠实。
- **model 层同语义类**：上游 generate() 只对**异常**retry（max_retries=3，domain 层调用给 10）；HTTP 200 + 空 content 不是异常，原样返回。我们同样只对异常/超时/配额 retry（LLM_MAX_RETRIES=3 + 配额/transient 等待预算），空 content 原样返回；`_anthropic_text` 提取无 bug（已核对原始 provider_response 确实只有 thinking block、无 text block）。temperature 默认均 1.0。
- **有意偏差（已披露）**：① max_tokens 上游默认 20000、我们 uncapped（M3 262144 上限）——thinking 模型必需，上游 20000 对 M3 反而几乎每步截断；② seeded task_plan 替代上游全局 random（两臂共享同一 plan，配对公平性更强）；③ 模型家族：上游 GPT 系无 thinking-only 空响应病理，该病理来自 minimax-m3 + OpenCode Go 端点，非代码差异。
- **发现并修复一处真实缺口**：runner `_evaluate` 缺上游 `validate_wildcard_context` 分支（wildcard target 态下生成文件不得有杂散文件，否则 `wildcard_mismatch`）。回溯审计整个 campaign：仅 6 个 forward 行会改判，且**完美对称**（HP/FR 各 3 个、同样本同轮次：mathlean2 RT7 的 `Derangements_*.lean` 未落子目录、translation4 RT5/8 的杂散 `assembly.txt`），backward RS 与配对统计零影响。已按上游语义补齐（`experiment_runner.py::_evaluate`），21 执行器测试回归通过。
- **结论**：FR 空返回与毒化链均为 authentic baseline 行为 + provider 病理，非本仓库实现错误；对齐审计与 rewind 重跑（3/5 恢复、2/5 异位复发）两条证据链相互印证。

## 218. hybridpatch/4：bulk 文件级匹配 + body-ref 归一化 + ws 两趟阶梯 + 路由压强（版本门控，旧归档字节级复现）

> 记录日期：2026-07-09。dev 迭代（快照 tag `hybridpatch3-snapshot-20260709` @ `db5048d2` 可整体回退）。改动全部走版本门控：`rev_of` 按信封 protocol 分支，生成端默认升 `hybridpatch/4`，/1 /2 /3 归档语义冻结。未编辑 raw JSONL/checkpoint/task_plan/sample/evaluator/scoring。

**动机**（§217 的三个可执行残留 + 路由占比测算）：

- D1：dev20full 仅剩的 2 个执行器可归因 kept-context（docker6 RT3、fonteng3 RT1）都是 `bulk_patch` 逐块匹配拒绝"整文件唯一但跨 struct2 块边界"的字面锚点——local_patch 在 v2 已改文件级匹配，bulk 未跟上，属协议内部不一致。
- D2：mathlean2 RT2 的 `@body:` 哨兵内嵌 `\n\n` 致 `body_ref_not_found`。
- D3：`_unescape_ws` 把 `\ref`/`\rho` 的 `\r` 误转 CR，使 ws 容错阶梯对 LaTeX 锚点失效（次生缺陷；latex2 RT5 本体是幻觉锚点，本修复救不回该案例，属预防性）。
- P1：对 dev20full 297 个成功 bounded_rewrite 步骤的离线测算（difflib 行级、按声明重写文件的字节加权）：**66/297（22%）输出 ≥80% 可由输入整行复制、28 个 ≥95%**（"为改几行重写整文件"）；148/297（50%）<0.5 为真转换（accounting ledger→website 为 0%）。即全部 400 步中约 16.5pp 的 bounded_rewrite 理论上可被 patch 路径承接（patch share 上限 25.8%→~42%）；backward 比 forward 更可复制（mean 0.518 vs 0.429）。

**改动**：

- 执行器（`hybrid_executor.py`，全部 `rev == PROTOCOL_V4` 门控）：`_bulk_file_matches`/`_apply_file_spans_rtl` 使 `replace_all` 与 `delete_lines_containing` 文件级匹配+右到左跨块应用（计数保持非重叠左到右，字节保留不变量不动）；`_normalize_body_refs_v4` 在 apply 入口对 local/bulk/bounded 全部 body 字段做哨兵空白归一化；`_collect_local_matches_v2` ws 阶梯两趟化（原文 token 优先，无命中才 `_unescape_ws` 变体）。
- schema（`hybrid_schema.py`）：`PROTOCOL_V4`、`rev_of` 扩展、生成默认 `PROTOCOL = PROTOCOL_V4`。
- prompt（`hybrid_prompt.py`，P1 路由压强，纯措辞不加字段）：规则 1 改为"按编辑足迹路由——大部分行不变的文件必须走 local_patch/bulk_patch，禁止重写"；bounded_rewrite 选项行加同义限定。repair prompt 经共享 `_schema_sections` 自动继承。

**验证**：`test_hybrid_executor.py` PASS 27/27（新增 6 用例：bulk 跨块 replace_all v3 `match_zero` 拒/v4 收、bulk 块内行为 v3=v4 字节等价、delete_lines v3=v4 等价、body-ref 空白名 v3 拒/v4 解析、`\ref` ws 锚点 v3 `not_found`/v4 收、过转义 `\n` 回退仍生效）；`splitters.py` 字节级 PASS；`py_compile` 全绿；结果复核 replay 与改动前基线逐目录对比——`exp_20260707_hybridv3dev20` PASS 400 backward RS 复现，`exp_20260708_hybridv3dev20full` 复现改动前基线（含 §217 已记载的 quantum4 RT9 评估器非确定性 MISMATCH，属预存差值非本次改动引入）。

**未验证**：v4 + 路由压强尚无 live 数据。待下一轮 fresh out_dir dev campaign 验证：(a) docker6/fonteng3 类 bulk 跨块步是否不再 kept-context；(b) 路由份额变化（bounded_rewrite 74.2% 是否下移）与 copied-byte 比；(c) 压强的副作用——小 diff 步被挤到 patch 路径后 op 拒绝率/kept-context 是否上升（净收益判据：RS 不降 + kept-context 不升 + patch share 上移）。→ 已由 §219 smoke 部分验证。

## 219. exp_20260709_hybridv4smoke：v4 定向 smoke（结果复核 PASS；执行器目标全数达成；两个新低分窗口归因为模型/评估器因素，执行器无罪）

> 记录日期：2026-07-09（当日配额恢复后补齐并收官）。7 样本（docker6/fonteng3/mathlean2/latex2/malware6/geotrack3/foodmenu6）× hybridpatch 单臂 × 10RT × no distractor，`minimax-m3`（thinking）。首跑 122/140 行（foodmenu6/latex2/mathlean2 被 HTTP 429→周配额中断），配额恢复后 checkpoint 续跑补齐至 **140/140**。最终结果复核 PASS（**70/70** backward RS 复现）。三条独立子代理归因（foodmenu6/mathlean2/fonteng3），主 Agent 逐条抽查证据。只读归档未编辑。
>
> 收官数字（140 行全量）：kept-context 0/140、forward no-op 0/70、gate 失败 0；路由 `local_patch` 44.3% + `bulk_patch` 16.4% = patch 60.7%，`bounded_rewrite` 39.3%；生成字节占比 0.395（copy:gen=1.45）；repair 6/6。全 10RT backward RS 均值（v4/v3/Δ）：docker6 0.968/0.804/+0.164、malware6 1.000/0.915/+0.085、latex2 0.986/0.934/+0.052（补齐的 RT9-10 v4 0.93 vs v3 0.77）、fonteng3 0.888/0.862/+0.026、geotrack3 0.830/0.842/−0.012、mathlean2 0.740/0.900/−0.160（模型 delete 误用+repair 放大，见下）、foodmenu6 0.800/0.995/−0.195（评估器可见性彩票，见下）；聚合 0.887 vs 0.893（Δ−0.006，n=70）。补齐段无任何 kept-context/空响应异常。

### 机制验证（本轮目的，全数达成）

- **kept-context 0/122、forward no-op 0/61、gate 失败 0**（v3 同 7 样本集里恰好包含全部 5 个 kept-context 案例）。全行 `protocol_rev=hybridpatch/4`，`preservation_violations=0`。repair attempted=6 used=6（100% 采用）。
- D1 live 证实：docker6 RT3 `0.98`（v3 kept-context 0.70）、fonteng3 RT1 `1.00`（v3 0.84）；docker6 全程 mean 0.968 vs 0.804。
- 回归控制：malware6 10 轮全 `1.000`（v3 0.915）、latex2 `0.999`（v3 0.974）。
- P1 路由位移（同 122 步配对）：patch 路由 39.3%→**59.8%**（bounded 60.7%→40.2%）；生成字节占比 0.534→**0.418**；token +5.8%。注意本 7 样本集偏 patch 友好，全量份额待 dev20。

### 两个新低分窗口的归因（RS 下降纪律，三子代理独立调查 + 主 Agent 复核）

- **foodmenu6 RT2-4 `0.35`（v3 同点 1.00）＝评估器可见性彩票，非回归**：损伤步 RT2 bwd 双方都走 bounded_rewrite——forward 任务预设"分节"迫使模型发明章节、backward prompt 从未要求删除；v4 模型碰巧用了评估器正则可见的 `--- X ---` 标题（12 节被计入，Jaccard 1/12），v3 用的 `== X ==` 正则不可见（count_sections=1）→ **v3 的 1.00 是假阴性，同一语义偏差两轮都在**。P1 路由的 RT3/4 bulk 步 51+2 op 全收、字节级精确还原（rt02/03/04 输出 sha256 相同）。RT5 恢复是 prompt 明确要求"collapse sections"。
- **mathlean2 RT4-6 `0.36`（v3 0.90）＝模型 delete 语义误用，repair 放大**：模型把"定位上下文+待保留声明头"整段写进 delete 的 `old_text`，执行器忠实删除 8 个声明头（decl 10→4，残片如无头 `(α : Type*) : Set (Perm α) :=`）。replay 字节一致、span 全部正确唯一，v3 执行器会同样执行。**repair 是放大器**：初版 2 op not_found 本会 kept-context 冻结在 ≈0.9，repair 补齐后 10 个破坏性 op 全部命中。P1 为间接因素（v4 forward 忠实 local_patch 重排使 backward 变难；v3 当时 forward 偷懒未真重排、backward 一个 bulk 删注释即可）。RT7 恢复靠 bounded_rewrite 凭 Mathlib 记忆再生声明（0.85，text_similarity 仅 0.59）。
- **fonteng3 RT4 `0.93`＝不可逆任务（v3 同点同幅度 0.826/class_accuracy 0.721，非 v4 特有）**：GDEF 分类 forward 天然销毁成员排序，backward 无从恢复。**RT7 `0.77`＝模型自毁撤销线索**：forward bulk op0 插入改名映射注释，op1-5 `replace_all` 在**变异中缓冲区**上顺序执行、把注释也改写成退化的 `# GSUB_akhn_1 -> GSUB_akhn_1`（模型自己声明 expected_count_min=6 包含注释行）；backward 信任被毒化注释、判断"无需改名"。v3 同点 0.876 只因其 forward 偷懒只改了 1/5 个 lookup（不完整 forward 不被打分、却好还原）。对照：同任务 RT1 v4 用 local_patch 25 op **快照定位**，注释未被污染 → RS 1.00。

### 聚合与净判定

同 122 步配对 backward RS：v4 0.879 vs v3 0.890——表面持平，但分解后：执行器修复的增益（docker6 +0.16、malware6 +0.09、latex2 +0.03、fonteng3 早段 +0.1）是真实的；两个拖累一个是评估器假阴性消失（foodmenu6，非保真度回归）、一个是模型能力+repair 放大（mathlean2）。**无一例 D1/D3 错位匹配**（三方独立验证）。CriticalFailure 3/54，全部对应上述窗口。

成本分解（api_raw 全调用口径，同 122 步；thinking/text 按字符占比折算 completion tokens）：v3 prompt 1.305M + completion 2.272M（thinking ≈86.1%）→ v4 prompt 1.276M（−2.3%）+ completion 2.359M（+3.8%），其中 **thinking ≈2.071M（+5.9%）、正文 text ≈0.288M（−9.1%）**。即 P1 把成本从输出正文挪到了推理通道：patch 信封比全文重写省正文字节，但模型构造 op/锚点想得更久（每调用 thinking 字符 49k→54k）。总账 +1.6%（全调用口径）。

对 FR 的同 122 步对比（dev20full FR 臂 api_raw 去重到每步最后一次调用，剔除 14 个 rewind 前重复调用）：FR prompt 0.505M + completion 1.391M（thinking ≈0.940M＝67.6%、text ≈0.451M），总 1.896M（committed 行口径 2.06M）。HP v4 对 FR 的 +1.74M 溢价分解：**prompt +0.771M（44%，索引/schema/规则 + repair 二次带文档）、thinking +1.131M（65%，patch 构造想得更久）、text −0.163M（−9%，唯一更便宜的维度——信封正文只有 FR 全文重写的 64%）**。范式画像：FR"少想多打字"（thinking 占 completion 67.6%），HP"多想少打字"（87.8%）。

### 新增可行动项（下轮迭代候选，未实现）

- **F1 bulk 快照定位**：`_run_bulk` 目前对变异中缓冲区顺序执行（fonteng3 RT7 根因的执行器侧成分）；改成 local v3 式"全部 span 按步骤输入文档解析后一次应用"可保住撤销线索。需版本门控（v5 或 v4.1）。
- **F2 repair 双刃剑**：repair 把 kept-context 的"意外保险丝"拆了（mathlean2 RT4 若冻结可得 0.9）。候选：采用 repair 结果前加无参考健康检查（如 delete span 终止于行中 → 可疑）。
- **F3 delete 语义 prompt 一句话**：明确 "delete removes the ENTIRE old_text; use insert/replace anchors for positioning, not delete"。
- **F4 基准注意**：foodmenu6 RT2-4 的 RS 由标题语法彩票主导（评估器 section 正则只认 `--- X ---`）；跨版本比较该样本此区间不反映编辑保真度。

### 遗留

- foodmenu6/latex2/mathlean2 缺 17 个 RT，周配额 3 天后重置（或开余额计费）可续跑（checkpoint 幂等）。阶段二全量 dev20 同样被配额阻塞。→ 当日配额恢复，已补齐收官（见开头收官数字）。

## 220. hybridpatch/5：F1 bulk 快照定位 + F3 delete 语义 prompt（版本门控，v4/v3 归档字节级复现）

> 记录日期：2026-07-09。dev 迭代，落地 §219 的 F1/F3 可行动项。改动全部版本门控：`PROTOCOL_V5`（生成默认升 `hybridpatch/5`），/1–/4 归档语义冻结。未编辑 raw JSONL/checkpoint/task_plan/sample/evaluator/scoring。

**改动**：

- F1（`hybrid_executor.py` 新增 `_run_bulk_v5`，仅 v5 信封走）：bulk_patch 快照定位——所有 op 的匹配与 expected count 一律按**步骤输入文档**解析（前序 op 插入的文本不会被后续 op 再匹配，堵死 fonteng3 RT7 自毁撤销线索的执行器侧成分）；跨 op span 冲突显式拒绝 `overlapping_span`（delete-delete 完全相同的行 span 去重而非拒绝——两个行过滤器可以合法命中同一行）；接受的 span 右到左一次性应用，与 `_run_local_v3` 同构。v4 的顺序文件级 bulk 分支原样冻结供 replay。D2/D3 门控从 `==V4` 放宽为 `in (V4, V5)`。
- F3（`hybrid_prompt.py`，纯措辞）：local_patch 选项加一句 `"delete" removes the ENTIRE old_text. Never pad a delete's old_text with text you want to KEEP — disambiguate with "occurrence", or use "replace" whose new_text carries the part to keep.`（mathlean2 RT4 案例）；bulk_patch 选项加一句快照定位/快照计数说明（与 local 的 occurrence 快照措辞对齐）。

**验证**：`test_hybrid_executor.py` PASS 32/32（新增 5 用例：fonteng3 RT7 场景复刻 v4 毒化锁定/v5 线索保全、跨 op 重叠拒绝、同行 delete 去重、v4=v5 独立改名字节等价、D1 跨块匹配在 v5 保留）；`splitters.py` 字节级；`py_compile` 全绿。结果复核 replay 三目录全部与基线判定一致：`exp_20260709_hybridv4smoke` PASS 70/70（v4 信封在新代码下字节级复现）、`exp_20260707_hybridv3dev20` PASS 400、`exp_20260708_hybridv3dev20full` 仅剩已知 quantum4 RT9 预存差值。

**未验证**：v5 尚无 live 数据；F1 对 fonteng3 RT7 类场景的端到端效果、F3 对 delete 误用率的影响，待阶段二全量 dev20 campaign。→ 见 §221。

## 221. exp_20260709_hybridv5dev20full：hybridpatch/5 全量 dev20 配对 campaign（结果复核 PASS 400/400；RS 与 FR 打平，路由压强的收益与代价首次同框）

> 记录日期：2026-07-09/10。dev20 全 20 样本 × 双方法 × 10RT × no distractor，`minimax-m3`（thinking），与 v3 campaign 同冻结任务序列/seed。HP 臂一次跑完（零 429）；FR 臂经两轮空锚定 rewind（首轮 7 样本、次轮 3 样本），protein1/satellite6/translation4 仍随机复发空响应，按政策停手披露（satellite6 RT3、translation4 RT6 连续两轮**同位**复发，提示空响应可能与特定步骤输入相关——假设，未定论）。结果复核 PASS **400/400** backward RS 复现，单一 code fingerprint。三条子代理归因 + 主 Agent 逐条抽查。

### headline

- 配对 backward：HP 0.849 vs FR 0.814，Δ**+0.035**（n=200，t=1.41，p=0.161，**不显著**）；剔除 3 个 FR 仍污染样本后 Δ**+0.004**（n=170，t=0.21）——**本轮方法效应为零**（v3 轮同口径 +0.048）。CriticalFailure@10：HP 10/180 vs FR 12/180。
- 身份指标显著改善：路由 bounded_rewrite **57.5%**（v3 74.2%）、local 30.2% + bulk 12.2%；生成字节占比 **0.595**（v3 0.716）；字节保留率 0.341（v3 0.251）。kept-context 7/400（1.8%）、no-op forward 2/200、`preservation_violations=0`、repair attempted 41 / used 35（v3 23/16——v5 严格性由 repair 吸收）。
- 成本：HP 15.86M vs FR 7.81M tokens（FR 为 committed 行口径，rewind 废行不计）。HP 比 v3 轮（13.9M）更贵（repair 更多 + thinking 更长）。

### HP 四大回归样本归因（vs v3 campaign HP；三子代理独立调查 + 主 Agent 复核，执行器全数无罪：字节级 replay、pv=0、零错位匹配）

- **quantum4 0.989→0.426（P1 伤亡 #1，高置信）**：v3 无压强时选 bounded_rewrite 安全通过；v5 被压到 bulk_patch，RT2 bwd 清理 op 字面量差 3 字符（`"// alias: n // "→""` 应为 `→"// "`），吃掉 6 行注释标记 → QASM AST 解析失败 → 评估器回退正则不识 `const int[32]` → 12 字节伤害被放大成 −0.61。v4/v5 翻转 replay 字节一致（快照语义与内容无关；其 fwd 的 4 个顺序思维 op 被 v5 拒后由 repair 完全吸收、输出与 v4 等价——代价一次调用，非损伤）。
- **starcatalog4 0.984→0.757（P1 伤亡 #2，高置信）**：v3 全程 bounded；v5 RT5 fwd 被压到 local_patch 后造出自相矛盾态（SNR FIELD 声明在第 6 列、数据追加在第 11 列，自身 obligations 即互相冲突），RT5 bwd 按 schema 位置删列删掉真实 HR1 数据。全部 EXACT 匹配、无 ws 回退——错误完全在模型手写的 body 内容里。
- **protein1 0.955→0.285（模型保真度错误，与 v4/v5/P1 无关）**：RT4 bwd bounded_rewrite（与 v3 同路由同任务，v3 得 1.0）重建定宽 PDB 时 110 行中 2 行 HETATM 序号多填 1 空格 → 链 ID 移位进 resSeq 列 → Biopython 整文件弃解析 → `gen_atom_count=0` 归零且不可恢复（随机格式化抽签）。**gate 设计缺口**：现有 gate 无内容丢失/可解析性检查，这类"1 空格→整文件归零"未触发 repair。
- **circuit2 1.000→0.898（v5 overlap 拒绝自伤，主 Agent 反事实证实）**：RT2 bwd 的 4 op（3 个 replace_all 参数名→数值 + 1 个 delete_lines `.param`）在快照上呈**包含型重叠**（删行 span 内含 replace span），语义完全可组合（先替换后删行=直接删行），v4 顺序重放 4/4 全收 RS=1.0；v5 拒绝 → kept-context 0.886 卡死 9 轮（repair 也未能改成非重叠形式）。
- （foodmenu6 0.995→0.766 为 §219 已知评估器章节正则彩票，非新回归；protein1 RT8 的 overlap 拒绝无关紧要——v4 重放同样 0.0。）

### 真实增益样本

docker6 +0.120（D1 效果延续）、musicsheet2 +0.138、malware6 +0.084、latex2 与 fonteng3 略升。fonteng3 本轮无 RT7 类 breadcrumb 事件（任务序列未触发该形态，F1 的正向效果本轮无直接证据）。

### 净判定与权衡（本轮最重要结论）

v4+v5+P1 这一轮：**RS 打平（增益 ≈ 伤亡相抵），身份/成本结构指标显著改善**（patch share +17pp、生成字节 −12pp）。路由压强的代价首次量化：patch 路由暴露了 bounded_rewrite 结构上不会产生的错误类——**模型手写精确删除/替换字面量与多列定位的能力短板**（quantum4/starcatalog4 两例合计拖掉配对 Δ 约 0.06）；且 relay 的 ratchet 性质使任何被接受的字节伤害永久化。

### 下轮候选（未实现）

- **C1 包含型重叠消解**（circuit2 案例，确定性安全）：v6 中删行 span 完全包含 replace span 时吞并而非拒绝（结果与 v4 顺序语义一致，仍防 fonteng3 型插入改写）。
- **C2 无参考"可解析性/内容量不回退" gate**（最大杠杆，覆盖 quantum4 + protein1 两类）：patch/bounded 输出相对步骤输入做轻量校验（语法 lint 不回退、记录数/行数比不塌方），失败触发现有 repair 或 kept-context。C1+C2 合计可拦四大回归中的 3 个（C2 拦 quantum4/protein1，C1 拦 circuit2）；starcatalog4 的输出结构合法、仅语义错列，无参考检查拦不住。
- **C3 P1 措辞微调**（低优先）：可加"无法用 patch 精确表达时允许 bounded_rewrite"逃生口，但会回吐份额收益；建议先上 C2 安全网观察。

## 222. hybridpatch/6：C1 包含型重叠消解 + C2 格式健康 gate（版本门控；反事实验证零误报）

> 记录日期：2026-07-10。dev 迭代，落地 §221 的 C1/C2（用户批准；C3 缓行）。全部走 `PROTOCOL_V6` 门控：v5 及更早归档语义（执行器与 gate 行为）冻结复现。未编辑 raw JSONL/checkpoint/task_plan/sample/evaluator/scoring。

**改动**：

- C1（`hybrid_executor.py`，`_run_bulk_v5` 增 rev 参数，仅 v6 生效）：快照 bulk 的冲突消解细化——删行 span **完全包含** replace span 时吞并该 replace 出现点（净结果=v4 顺序语义："先替换再删整行"="直接删行"），两个方向（删行 op 在前或在后）都处理；部分重叠与 replace-replace 重叠（真正顺序相关、有歧义）仍拒 `overlapping_span`。span 记账重构为 spans_by_file 单一来源（v5 行为字节级不变）。
- C2（`hybrid_gate.py`，按 exec log `protocol_rev==hybridpatch/6` 门控）：无参考格式健康检查 `_format_health_errors`——扩展名 lint 注册表（`.json`→json、`.xml/.kml/.gpx/.svg`→ElementTree、`.qasm`→openqasm3、`.pdb`→Bio.PDB 严格模式；公共格式解析器、惰性导入、缺依赖时无信号）。语义：被改文件按**非回退**判定（仅当本步输入可解析时才要求输出可解析），新建 lintable 文件按绝对判定；命中记 `format_regression:<file>`，经由 runner 既有 need_repair（v3 起覆盖 gate 失败）触发单次 repair，再败 kept-context。"记录数塌方"检查缓行——行数口径抓不住 protein1（行数没变、是解析失败），解析口径已覆盖。

**验证**：

- `test_hybrid_executor.py` PASS 40/40（新增 8 用例：circuit2 形态 v5 拒/v6 收、删行 op 在前的反向吞并、部分重叠仍拒、replace-replace 仍拒、json 回退 v6 拦/v5 放行为锁、新建文件绝对判定、坏输入无基线不判、好输出放行）；`splitters.py` 字节级；`py_compile` 全绿。
- **真实归档坏文件验收**：quantum4 rt02_bwd vqe.qasm（损伤）lint 判负 / rt01_bwd 判正；protein1 rt04_bwd structure.pdb（损伤）判负 / rt03_bwd 判正——4/4 全对。
- **反事实扫描（本节最重要证据）**：把 v6 gate 离线套在 v5 campaign 全部 400 个 HP 步上——仅 8 步命中，全部属于 quantum4/protein1 两条已知损伤链（2 个源头步 + 6 个下游携带/衍生文件步），**误报 0/400**。若当时在线，两个源头步会触发 repair（约 0.99/0.93 的 kept-context 兜底 vs 实际 0.39/0.0）。
- 四归档 replay 基线一致（v5 campaign 400 字节级复现为关键回归，见 active_log after 条目）。

**未验证**：v6 live 效果（C1 对 circuit2 类形态的端到端救回、C2 触发 repair 的实际修复率、starcatalog4 类"结构合法语义错列"仍不可拦）待 v6 全量 dev20 campaign。→ 已由 §223 验证。

## 223. exp_20260710_hybridv6dev20full：hybridpatch/6 全量 dev20 HP 臂（结果复核 PASS 200/200；v5 伤亡修回、RS 回到 v3 水位、身份指标保持 v5 增益；repair 零负例）

> 记录日期：2026-07-10。dev20 全 20 样本 × hybridpatch 单臂 × 10RT × no distractor，`minimax-m3`（thinking），与 v3/v5 campaign 同冻结任务序列/seed。400/400 行收满，结果复核 PASS（200/200 backward RS 从 raw 响应独立复现），`preservation_violations=0` 全程。**FR 臂未跑**：按用户 2026-07-10 决定改走冻结基线政策（`Baseline/FR冻结计划.md`：一次性跑、空响应原样入档不 rewind），启动等待用户指令——本节 before-experiment 计划中的 FR rewind 方案作废。四条独立子代理归因（satellite6 / geotrack3 / kept-context 审计 / repair 极性审计），主 Agent 对全部承重事实做了字节级/评估器级复核。只读归档未编辑。

### headline 与纵向对比

- 20 样本 backward 均值 **0.9207**，RS@10 **0.918**。三代都跑满的 16 样本聚合：v3 `0.920` → v5 `0.830` → v6 `0.918`——**RS 水位回到 v3，同时保持 v5 的身份增益**：bounded_rewrite 份额 58.0%（v3 74.2% / v5 57.5%）、生成字节占比 0.597（v3 0.716 / v5 0.595）、copy:generated 0.649。
- v5 四大回归样本全部改善：quantum4 `0.426→0.994`、circuit2 `0.898→1.000`、starcatalog4 `0.757→0.987`、protein1 `0.285→0.547`。backward RT2-10 低于 0.10 的行 4/180（v5 轮 CF 10/180）。
- kept-context 失败 9/400（2.2%）、forward 无有效修改 1/200、repair attempted 41 / used 30、HP tokens 15.16M（v5 15.86M）。
- 注意口径：v3 campaign 跑在 `hybrid_split.json` 冻结前，其集合含 accounting1 无 python7；纵向可配对为 19 共同样本。

### C2 格式健康 gate 的 live 首验（含一处对先前表述的修正）

- 全 campaign 仅命中 protein1 RT4/RT5 bwd 两步（2/400），**零误报**。RT4 的 catch 经字节级验证为真实损伤：重建 PDB 坐标字段整体右移 1 列（col30-38 `   -4.78` vs GT `  -4.788`）→ Bio.PDB 严格模式 line 39 解析失败，且原子 107/110 缺 3。repair 输出**复现了同一列移缺陷**，被 gate 再次拦下 → kept-context——repair→gate→kept-context 分层降级链首次完整闭环。
- **修正**：C2 在 protein1 上的分数收益≈中性，不是"止损救分"。RT4 bwd 的 kept-context 是 forward 拆分产物（无 structure.pdb）→ `context_mismatch` 记 0，与放行坏文件的反事实同样低。protein1 均值 0.285→0.547 的改善主要来自 **RT8 bwd 的 repair 营救**（0.053→0.772）。C2 本轮的验证价值在"阻止损坏工件入链 + 检测器零误报"，不在直接分数。

### repair 极性审计（30 个 used 逐个反事实，回应 §219 F2 疑虑）

- 反事实口径：**机械 kept-context** = 当步 forward 输出文档对 basic_state 用真实评估器打分（relay 链式携带，backward 失败保留的是 forward 后文档，非上一轮 backward 态；主 Agent 亲算 musicsheet2 RT8 kept-ctx=0.65044 复核通过）。
- 结果：**POSITIVE 20**（3 个强营救：landmarks1 RT2 kept-ctx 0.0→0.998、musicsheet2 RT6 0.513→0.805、protein1 RT8 0.053→0.772；其余为运输层修复放行）、**NEUTRAL 10**、**NEGATIVE（mathlean2 型）0**。唯一疑似负例 musicsheet2 RT8（repair 放行后 0.6514）经机械反事实排除（kept-ctx 0.6504，跌幅来自不可逆 forward 移调任务本身）。
- 11 个 attempted-未 used：8 个 op 拒绝保险丝守住（anti-mathlean2 结局）、3 个 partial_extraction 原输出直接可用（repair 调用白烧）。
- 局限披露：17 个 forward 方向 used repair 无当步分数（这些域 forward 不评分），极性由运输恢复 + 同轮 backward 健康推断。
- **本轮证据下 repair 扩类未现负例**，但 §219 mathlean2 负例的结构条件（forward 编辑小、kept-context 水位高、拒绝的恰是破坏性 op）本轮未复现——分层 repair 策略（按失败类设采纳闸）仍是候选，非紧急。

### kept-context 12 步全审计

- 9 个失败驱动：**执行器过严 0**、模型可归因 8（截断/抄错锚点 ×3、缺 occurrence、漂移过期锚点、remove-all 任务走错 local_patch 路由、重叠 op 设计 ×2）、gate 正确拒绝 1（C2）。净效应：保护性 3（foodmenu6 RT7 保留态 0.909 **高于**该编辑实际执行时的 0.833；protein1 两步）、伤害性 2（fonteng3 RT7 ≈−0.16、screenplay6 RT5 ≈−0.011 且冻结余轮）、中性 4。
- 3 个无失败原因行 = forward kept 后模型显式空补丁（`ops:[]`）的合法无变更，非失败。
- **最大残留伤害向量 = 全有全无放大**（fonteng3 RT7）：模型自加的装饰性空行合并 op（任务不要求）match_zero，连坐丢弃同信封 11 个合法 op（含全部 5 个实质改名）→ 本可 ≈1.0 只得 0.842。这是策略/鲁棒性问题，非执行器正确性 bug；部分接受语义与保留不变量的相容性待设计论证，列为候选。

### satellite6（0.837，唯一纵向回归样本）与 geotrack3（0.890，三代最佳）

- satellite6：冻结计划在 RT3/RT9 各排一次格式毁灭性 csv_keplerian，其 backward=整文件 TLE 重建保真度抽签。v6 的 RT3 抽签坏在定宽列对齐（50% 权重 orbital 子项 0.496）→0.736 棘轮平住（RT5=6=7、RT7≈8 哈希级），RT9 二次重建强制销毁坏格式、好抽签 0.987。三代 RT3 损伤模式各异（v3 epoch 间距 0.915 / v5 数值错误 0.890 / v6 列塌 0.736），RT9 重抽 v3/v5 都向下（0.897/0.777）——路由/协议三代全同，**判采样噪声非 v6 回归**（v6 是否系统性改变抽签质量：未定，n≤2）。
- geotrack3：RT5 GPX→GeoJSON 转换丢全部 45 个 `<ele>`——**冻结指令定义的 GeoJSON schema 本身不含海拔**（任务性有损；模型照章即丢，主动用 `[lon,lat,ele]` 三元组才可无损，"模型未主动保真 vs 任务规格性丢失"记未定），此后 6 轮 md5 级纹丝不动。v6 的纵向改善真实：v3/v5 在 RT1 elevation_classified 往返即丢光海拔，v6 用 `<cmt>` 注释安全通过、撑到 RT5。RT3-4 的 0.98 = 2 个 rtept 被提升为 wpt（RT3 fwd 引入、bwd 忠实合并）。
- 通用发现：**"任务指令定义有损 schema"的 RS 损失是基准属性而非方法可修**——geojson 转换三代全部中招，仅位置不同。

### 运行事故与遥测告警（如实记录）

- **docs 快照非权威**：本轮旧会话自动链与新进程赛跑，重复提交守卫全部正确触发（三尾样本零重复行、正式链结果复核验证干净），但 `docs/hybridpatch/satellite6/rt09_bwd_basic_state/satellites.tle` 快照被输方 runner 晚 19 分钟覆写（0.890 的落选版 vs 正式 0.987）——**以后对 docs 快照做 diff 分析前须对 api_raw/正式行校验**。赛跑代价为若干重复 API 调用（如 satellite6 rt09 双跑）。
- **遥测标签疑点（未定）**：protein1 RT4 bwd 的 `repair.repair_errors` 记 "no valid HybridPatch JSON envelope"，但存储的 repair_raw 在当前 extractor 下可完整解析（实际拦截者是 format gate）。候选解释：运行时 extractor 差异或 catch-all 误标；不影响结果正确性（kept-context 结局正确），列为定向排查项。
- fonteng3 RT10 −0.07（class_accuracy 0.714）：双向干净执行（零拒绝零 gate 报错），被接受编辑内的内容级损伤，属 §219 GDEF 分类不可逆任务类；未深挖。

### 净判定

hybridpatch/6 达成设计目标：**C1 消除 v5 的重叠误拒类自伤（circuit2 回满分），C2 以零误报完成损伤检测首验并闭环分层降级链，v5 路由压强的 RS 代价被收回（16 样本聚合回到 v3 水位）而身份/成本结构指标保持 v5 增益**。执行器在全部四路归因中无罪（0 过严拒绝）；残留损失集中在三类不可修/未修边界：格式毁灭性任务的重建抽签（satellite6）、任务性有损 schema（geotrack3）、全有全无放大（fonteng3 RT7，候选改进）。方法主叙事（灾难规避/优雅降级）获得本轮全部机制证据支持；配对 Δ 数字待 FR 冻结基线臂（`Baseline/FR冻结计划.md`）。

## 224. python1 评估链公开渠道不可运行：上游 utils_eval.py 打包遗漏 + 两层环境坑（调查 + 本地修复，2026-07-10）

> 背景：FR 冻结基线 234 样本 campaign 中唯一失败样本。python1 三次换 Key 重试均在 RT1 forward 成功调用（HTTP 200）后死于评估层 `No module named 'utils_eval'`，零行提交、浪费 3 次调用。以下为完整证据链与修复记录。未编辑任何数据集文件。

### 根因一：utils_eval.py 是上游打包遗漏，公开渠道不存在（可复现验证）

- `data/samples_delegate52/python1/testing.py` 是全部 234 样本中**唯一** `from utils_eval import deep_compare_objects` 的文件（全库 grep）。
- 官方 HF 数据集 `microsoft/delegate52`（`delegate52.jsonl`，19.1MB）逐行核验：python1 记录的 `files` 清单与我们磁盘导入**一一对应**（导入忠实无缺漏），全文件 "utils_eval" 仅出现 1 次＝python1 自己的 import 语句。
- 官方 GitHub 仓库完整树（194 路径，未截断）：无 utils_eval.py，且不含任何样本数据。
- 机制解释：上游导出工具（`utils_dataset.py`，公开可查）只收集样本目录内文件；`domain_python.evaluate_context` 把整个样本目录 copytree 进临时 sandbox 并加入 sys.path——utils_eval.py 在微软内部工作区显然位于样本目录之外（如仓库根，靠 ambient sys.path 解析），导出捕不到、公开仓库又没放。**任何人用公开工件复现基准，python1 评估必崩**；因其是唯一引用者，不真跑 python1 永远暴露不了。
- 修复：`src/utils_eval.py` **本地重建**（文件头含完整 provenance 披露）。契约从调用点反推：输入参考/生成两个 JSON 对象，返回含 `score`/`exact_match` 的字典。语义取参考锚定的递归叶比对（score=命中叶占比、数值容差 1e-9、生成侧多余键破坏 exact 不减分）。**HP/FR 两臂同源使用同一重建，配对比较内部一致；绝对分不保证与微软内部实现一致，须披露。**

### 根因二/三：Windows POSIX 重定向 + 样本内嵌程序依赖

- python1/testing.py 用 `os.system("python ... > /dev/null 2>&1")`（234 样本中唯一 os.system 用户）：cmd.exe 无 /dev/null，整命令失败→`output_file_error` score 0。修复：`domain_python.py` 在评估 sandbox 窗口内 Windows-only 把 `/dev/null` 重写为 `os.devnull`（try/finally 复原，POSIX 行为不变；先例＝全仓库 fcntl→portalocker）。
- 样本内嵌分析程序 `code_analysis.py` 依赖 `Levenshtein` 库，上游 requirements 未含。已安装并补入 requirements.txt。
- 预检盲区教训：dispatcher preflight 只 import 域评估器，**样本内嵌代码的自带依赖**（python 域特有：评估＝执行样本自己的 testing.py/分析程序）不在其覆盖面。

### 验证（零 API，全链端到端）

- 单元：deep_compare 自比对 6591/6591 叶=1.0/exact=1；扰动对照 exact=0。
- 端到端（真实 `evaluate_context` 全链：copytree sandbox→expand→testing.py→子进程跑分析程序→deep_compare）：参考解 **1.0/exact**；崩溃对照 0（output_file_error）；微扰对照（Levenshtein 距离整体 +1）**0.840** 部分得分——评估器有梯度非二值。
- python1 FR 已重启（此前零行提交，无 rewind 语义问题）。

### 影响面

- 波及仅 python1（utils_eval/os.system 均唯一）；python2–7 在 FR 基线中正常满分，v6 HP 臂 python7 全程 1.0 不受影响。
- 三处修复均不触碰冻结归档与数据集；对既有归档 replay 无影响（无任何已提交 python1 行；python7 不走 os.system 路径）。

**§224 追记（2026-07-10 用户决定）**：python1 弃用，FR 冻结基线范围定为 **233 样本**。修复后的重跑在用户叫停前已提交 RT1–4 部分行（按纪律保留在磁盘、不删除，但排除出基线与一切分析口径）。三层评估链修复保留在代码中（utils_eval 重建 / devnull shim / Levenshtein），供未来任何需要 python1 的场景使用；dev20/val20/test20 均不含 python1，方法评估路径零影响。

## 225. FR 规范基线冻结（exp_20260710_frbaseline234）：233 样本结果复核 PASS 2334/2334；dev20 配对 Δ+0.195（去空响应披露口径 +0.030）

> 记录日期：2026-07-10/11。政策与执行规程见 `Baseline/FR冻结计划.md`（一次性、空响应原样入档、多 Key 静态分摊调度）。用户以 11 Key × 调度器自跑完成；python1 弃用（§224），基线范围 **233 样本 × 10RT × no distractor**，minimax-m3（thinking）。**结果复核 PASS：2334 backward RS 全部从 raw 响应独立复现**（233×10 + python1 部分行 4）。committed 成本 $87.00 / 86.0M tokens（估算 $96 的 91%）。自此永久冻结：后续 HP 迭代只跑单臂，配对一律引用本基线。

### 全量 FR 画像（长程退化曲线，基准核心现象的首个全量复现）

- RS@k（全 233，失败计 0）：**0.926@1 → 0.832@2 → 0.748@3 → 0.682@5 → 0.558@10**——10 轮往返均值近乎腰斩。
- 样本均值的均值 0.685、中位 0.805；**83/233 样本出现过 ≥1 次 backward<0.05**（重尾崩溃：dns3/edifact1/geodata1/graphviz1 全程 0 等）。
- 空响应普查（一次性无 rewind 政策下的真实病理率）：**63 行空 raw + 54 行 <200B 短响应 / 4660 行 ≈ 2.5%**，与 §217 测得的 1.5–1.6% 空响应率同量级（本轮含短桩口径更宽）。

### dev20 配对 Δ（v6 HP vs 冻结 FR，同任务 plan 天然配对）

- **冻结政策口径（headline）**：n=200 对，HP 0.921 vs FR 0.726，**Δ+0.195**（t=7.64，p<0.001，d=0.54）；ECR 条件化 n=193 Δ+0.188。CriticalFailure@10：HP 8/180 vs FR 13/180。
- **披露口径（空响应机制归因）**：本轮 dev20 的 FR 臂 8/20 样本命中 ≥1 次空/短响应（translation4 ×4→链 0.1、docker6→0.19、protein1→0.195 等）；剔除这 8 个后 12 样本 n=120，HP 0.914 vs FR 0.884，**Δ+0.030（t=2.76）**。注意：该剔除按 FR 侧条件化，子集构成与 v5 轮的去污染口径不同（quantum4/starcatalog4 这类 HP 强项样本被一并剔除），两轮"干净 Δ"不可直接比较。
- 解读与既有结论一致（§213/§217/§223 同族）：**配对优势的主体来自 FR 对灾难（此处为空响应毒化）的零护栏放大，HP 的架构韧性（repair 第二调用 + kept-context）把同源病理吸收为零链毒化**；干净子集上残余 +0.030 为正但幅度小。这正是方法主叙事的定量形态：灾难规避/优雅降级是结构性优势，同等干净条件下 RS 不劣。

### 基线有效性边界（引用 §FR冻结计划 §5）

条件化于 minimax-m3 @ OpenCode Go、thinking、temperature 1、uncapped、seed 42 任务计划、2026-07-10 时点的 provider 行为；触发任一变更须重新基线。distractor 设定不在本基线覆盖内。

## 226. hybridpatch/7：部分接受提交政策（设计证明 + 实现；反事实扫描净 +0.017、fonteng3 RT7 全救回、mathlean2 型负例有界）

> 记录日期：2026-07-11。dev 迭代，落地 §223 头号残留伤害向量（全有全无放大）的设计论证与实现，用户指令"v7 头号候选"。全部走 `PROTOCOL_V7` 门控：执行器/gate 语义与 v6 逐字节相同，v7 只改 runner/verify 的**提交政策**；v6 及更早归档冻结复现。未编辑 raw JSONL/checkpoint/task_plan/sample/evaluator/scoring。与 val40 campaign 并行——val40 从冻结快照 `_snap_val40` 执行，不受本次改动影响。

### 设计核心：为什么部分接受与保留不变量相容

执行器**本就应用接受集**（拒绝 op 跳过、接受 op 照常执行，`apply_hybrid` 返回的就是部分应用结果）——全有全无从来是 runner 层政策：任一 op 拒绝 → `gate_pass=False` → 把现成的部分结果丢弃换 kept-context（`experiment_runner._run_attempt_hybrid` / `_edit_step`）。v3 local / v5 bulk 的快照语义使各 op 相互独立（匹配、occurrence、span 全按步骤输入文档解析，互不重叠校验后一次性右到左应用），删除任意子集不影响其余 op 的定位——**部分应用良定义、与顺序无关**；preservation 不变量只关乎未声明块，双向均不受影响。在 v1/v2 顺序变异语义下部分接受本是病态的，快照语义（§216/§220）是本设计的前置条件。

### 反事实扫描（设计证明，零 API；`src/sweep_partial_acceptance.py`，v6+v5 campaign 共 800 步，归档只读）

- 14 个 op-rejected kept-context 步 → 12 个政策分歧步（v7 会提交部分应用）→ 8 个 backward 可计分：**mean +0.0172，pos 3 / neg 1 / zero 4；worst −0.045 / best +0.158**。
- fonteng3 RT7（§223 量化 ≈−0.16 的头号案例）：**0.842→1.000 全救回**——1 个装饰性 match_zero op 不再连坐 11 个合法 op。screenplay6 RT5 0.981→0.992；circuit2 RT2（v5）0.886→0.900。
- foodmenu6 RT7（§223 的保护性 kept-context 案例，kept 0.909 > 全执行 0.833）：部分接受下 **0.909→0.909 零损失**——有害 op 本身就在被拒集合里，部分接受天然继承 kept-context 的保护。
- 唯一负例 = §219/§223 预言的 mathlean2 型：v5 mathlean2 RT2 **0.942→0.897（−0.045）**——当被拒的恰是破坏性 op 时，旧政策连坐挡住了同信封其余 accepted op 中的内容级损伤，部分接受放行了它们。有界且远小于救回幅度；live 发生率待 v7 campaign 观察。
- 4 个 forward 分歧步无从计分（链冻结口径：每步输入取归档实际链态，forward 分歧的下游链式后果离线不可复算——与 §222 C2 扫描同一先例）；2 个不合格步被资格闸正确过滤（`zero_accepted_ops` / `other_gate_errors` 各 1）。
- 事件明细：`exp_20260710_hybridv6dev20full/analysis/partial_acceptance_sweep_events.json`。注意扫描脚本为归档反事实特意**不检查 rev**；live 政策以 `hybrid_gate.partial_acceptance_eligible`（硬门控 rev）为准。

### 改动（协议/执行语义零变化，纯提交政策）

- `hybrid_schema.py`：`PROTOCOL_V7`，生成默认升 v7（prompt 文本除版本常量外零改动——**不向模型披露部分接受政策**，防道德风险，v6→v7 构成纯政策消融）。
- `hybrid_executor.py`：v7 走与 v6 完全相同的执行路径（两趟 ws 阶梯 / body-ref 归一 / v3 local / v5 bulk / C1 吞并）。
- `hybrid_gate.py`：C2 格式健康 gate 对 v7 生效；新增共享资格函数 `partial_acceptance_eligible`——资格 = rev v7 + local/bulk 路由 + ≥1 拒绝且 ≥1 接受 + 无 route violation + 输出≠输入 + **通过全部其他 gate 检查（targets/readonly/粗结构/C2/forward 有效变更）**。
- `experiment_runner.py`：repair 触发与选择规则（`hybrid_key_v1`）不变；仅当 chosen attempt 终局仍失败且资格成立时提交部分应用；遥测 `partial_acceptance` + `partial_skipped_ops`（含被跳 op 拒因）。
- `verify_anchorpatch.py`：`_apply_hybrid` 经同一共享函数镜像同一决策——结果复核可从 raw 确定性重演 v7 提交。

### 验证（零 API）

- `test_hybrid_executor.py` **PASS 49/49**（新增 9 用例：v7 执行 parity（C1 吞并同 v6）、v7 C2 生效、资格正例（fonteng3 形态）、rev 门控锁（同形态 v6 信封不合格）、零接受 / 无拒绝 / gate 错误（accepted op 破坏 JSON 被 C2 拦）/ bounded 路由 / 无变化输出 五类反例）；`splitters.py` 字节级；`py_compile` 全绿。
- **四归档 replay 回归全部判定一致**：0707 PASS 400；0708 exit 1 且唯一 mismatch = 已知 quantum4 RT9 评估器非确定性（与基线逐行一致）；v5 campaign PASS 400；v6 campaign PASS 400（含 200 行基线拷贝 FR）。部分接受政策对 pre-v7 信封硬门控不生效，字节级复现成立。

### 未验证 / 待观察

- v7 live 效果：部分接受实际触发率（v6 里 op-rejected kept-context 仅 2.2% 水位）、forward 分歧步的链式后果、mathlean2 型负例 live 发生率、repair 与部分接受的交互（被选 attempt 仍按 accept rate 决出，可能选中 repair 的部分集）。待 v7 dev20 campaign（val40 完成后，等用户指令）。

**§226 附记：§223 遥测标签疑点排查结果（已解决，非 bug）**。protein1 RT4 bwd 的 `repair.repair_errors` 记 "no valid HybridPatch JSON envelope" 而 repair_raw 可解析——排查（零 API，直读归档行 + 当前 extractor 重放）：`original_raw` 长度为 **0**（主调用即 provider 空响应）→ 主尝试提取失败、trigger=invalid_json；按代码语义 `repair_errors=a0["errors"]` 存的是**主尝试的错误**（喂给 repair prompt 的拒因），从不描述 repair 输出。repair_raw（12752B）解析正常、被选用（used=True）、被 C2 格式 gate 拦截（`format_regression:structure.pdb`，failure_reason=validation_gate）→ kept-context，与 §223 C2 叙事完全一致。无 extractor 漂移、无 catch-all 误标；纯字段名语义误读。已在 runner 该字段写入处加注释消歧；schema 不动（保持归档兼容）。

## 227. exp_20260711_hybridv6val40：v6 val40 单臂验证（结果复核 PASS 800/800；Δ+0.137 / 披露口径 +0.042；空响应机制修正为 thinking 中途服务端流截断）

> 记录日期：2026-07-11。用户指令：val20 扩到 40 样本、8 key × 5 并发。**val40 = val20 + unused_reserve20**（封存 test20 不动；材料化与曝光披露见 `exp_20260711_hybridv6val40/val40_materialization.json`：val20 有一次 2026-07-03 旧候选曝光，unused_reserve20 对 HP 全新，84 个 clean_candidate 仍未动）。HP 单臂 × 10RT × no distractor，`minimax-m3`（thinking），40 份 task plan 从冻结基线字节级拷入；**从冻结代码快照 `_snap_val40` 执行**（指纹 == v6 dev20 campaign），与 v7 代码改动完全隔离。FR 侧从 `exp_20260710_frbaseline234` 拷贝 + 哈希校验。调度：`fr_baseline_dispatch.py --method hybridpatch`，KEY_01–08 各 5 槽 + KEY_09–11 冷备（keys 文件实为 11 个 key，非用户记忆中的 12）。

### 完整性与结果复核

- 40/40 样本 10RT 收满（800 HP 行）；7 次样本重排全部由备用 key 救回，零样本失败。
- **结果复核 PASS：800/800 backward RS 独立复现**（400 HP + 400 拷贝 FR），零 mismatch、零重复行、单一代码指纹；`preservation_violations=0` 全程。

### 配对结果（vs 冻结 FR 基线，同 task plan 天然配对）

- **冻结政策口径（headline）**：n=400，HP **0.848** vs FR **0.711**，**Δ+0.137**（t=6.21，p<0.001，d=0.31）；ECR 条件化 n=378 Δ+0.121。RS@10 **0.792 vs 0.553**；RS@1 0.970 vs 0.948。CriticalFailure@10：HP 23/360 vs FR 29/360。
- **披露口径**：FR 臂命中 ≥1 次空/短响应（<200B）的 9 样本（earncall1/geodata1/geodata4/geotrack5/jobboard3/json4/obj3d5/satellite4/transit2）剔除后 n=310，HP 0.862 vs FR 0.820，**Δ+0.042（t=1.96，边际显著）**。与 dev20（§225：+0.195→+0.030）同构：配对优势主体来自 FR 对 provider 病理的零护栏放大，干净子集残余小幅为正。
- **两半子集**：val20 半 Δ+0.129（t=4.73）、全新 unused_reserve20 半 Δ+0.146（t=4.18）——优势跨曝光/新鲜两半泛化，无曝光膨胀迹象（新鲜半反而略大）。
- **val20 纵向**（vs 2026-07-03 旧候选 val20 campaign）：kept-context 从 **97/400（24%）降到 23/800（2.9%）**；旧配对 Δ+0.103（HP RS@10 0.603）→ 本轮 val20 半 Δ+0.129 且 HP 配对均值 0.880（口径不同注意：旧为当时全 val20，本轮为同 20 样本在 v6 + 冻结基线下）。

### 遥测

- 路由份额：bounded_rewrite **66.4%**（val 集 format_conversion 占比远高于 dev20，符合预期；dev20 v6 为 58.0%）、local_patch 22.0%、bulk_patch 11.1%；kept-context 23/800（19 gate 类 + 4 双空 invalid_json）；forward 无有效修改 2/396。
- repair：attempted 115 / used 99 / success 91（14.4%）。
- 成本：HP 29.40M tokens vs FR 15.13M（1.94×，已知反向成本结构）。

### 空响应机制修正（对 §217/§225 表述的統一订正，本节最重要机制发现）

- 60 个"transport-valid but model-empty"调用逐一解剖 SSE 流：**57/57（ct=0 类）全部有 thinking 块在流、text 块从未开始、无任何收尾事件**（thinking_delta 后直接 EOF：无 content_block_stop / message_delta / message_stop）。thinking 文本中位 3.6 万字符、p75 10 万、最大 91.4 万字符。`completion_tokens=0` 是"带 usage 的收尾事件从未到达"，不是"模型没生成"。
- **机制定名：MiniMax-M3 extended thinking 阶段的服务端流截断**。客户端 watchdog（7200s）无关（时延中位 91s / 最大 1305s，远低于阈值）——服务器主动关流、HTTP 层面表现为正常 200。
- **FR 冻结基线同机制**：抽查基线 59 个 ct=0 空响应中 15 个，15/15 同形态（有 thinking、无 text、断在 thinking_delta）。§217"空响应病理"与 §225"空响应普查"的现象记载不变，机制表述以本节为准。
- **成本口径披露**：被截断流的 thinking token 无 usage 事件、未计入既有 token 统计——基线 86.0M 与本轮 29.4M 为下界，服务端实际消耗略高。
- 另 3 个 ct>0 空响应为不同亚型：流完整走完但 thinking 燃尽全部预算（2 个 finish=max_tokens、262144 tokens），同样零正文。
- 时间分布：背景截断率 ~3.5%（与 v6 dev20 campaign 3.4% 一致；FR 基线 1.3%——HP 调用更重更易中）+ **05:00–05:25 服务端事件窗**（05:0x 桶 19.4%、05:2x 12.2%；同秒 7 进程 `AlreadyLocked` 锁异常为旁证，dispatcher 按设计 requeue 全部救回）。per-key 完全均匀（8 活跃 key 各 5–9 个），排除坏 key。
- **吸收链**：60 空调用 → 44 被 repair 第二调用完全吸收（步正常提交）→ 16 落在 12 个 kept-context 步（双空或 repair 内容被拒）。12 步伤害评估：多数零伤/轻伤（json2 RT1 kept 后 1.0、quantum1 0.963 平台等）；emails5（0.709 台阶）/spreadsheet1（0.278 平台）与相邻 gate 失败纠缠；robotics1 的崩溃始于 RT6、空响应在 RT10——非肇因。

### HP 侧损失样本（表层归因，均非执行器问题）

- **crystal6（0.0 from RT4，唯一全程归零）**：RT1–3 全 1.0；RT4 起 backward 输出 CIF 触发评估器 "Star Format error" 解析失败并棘轮到底（路由混 bounded/local，损伤在链态中传承）。**C2 注册表覆盖缺口**：`.cif` 不在 lint 注册表（json/xml 系/qasm/pdb），这正是 C2 设计要拦的"格式回退"类——**候选改进 C2-ext：加 .cif lint**（pymatgen 解析器，crystal 域依赖已在 requirements）。FR 臂同样本 RS@10 也归零（RT10），但 FR 撑到更晚。
- **robotics1（0.914→0.012 from RT6）**：RT6 forward bounded_rewrite 干净执行但内容级摧毁工作区；RT6 backward bulk_patch 全 op 被拒（op_rejected、repair 未采纳）→ kept-context 把 0.012 的 forward 态锁进链，此后 4 轮平台。形态上是"forward 破坏 + backward 锁死"，且 backward 的 op 拒绝步是 v7 部分接受的目标形态（能否救回未知——被接受子集质量不可知）。
- **spreadsheet1（0.278 平台 from RT6）**：RT3 bwd 0.137 干净执行内容级损伤后 RT4–5 回满，RT6 bwd op_rejected kept（repair used 仍败）后平台。
- subtitles6 RT1 bwd context_mismatch（双空步）后 RT2 起回 1.0/0.98+——瞬态，无链毒化。

### 净判定

v6 在 40 样本 val 集（其中 20 个 HP 全新）上复现 dev20 的定量画像：**冻结政策 Δ+0.137 显著、干净披露口径 +0.042 边际为正、CF 更少、kept-context 比旧 val20 campaign 降一个量级、preservation 不变量零违例**。方法主叙事（灾难规避/优雅降级为结构性优势、干净条件下 RS 不劣）在 val 尺度成立。残余损失集中于已知类：格式毁灭性/严格格式域的内容级损伤（crystal6/robotics1/spreadsheet1，评估器可拦部分待 C2-ext）。

## 228. exp_20260711_hybridv7dev20full：hybridpatch/7 部分接受 live 首验（结果复核 PASS 400/400；5 个 partial 全部净非负、动机案例同位复发并被救回；纵向均值差由已知随机类主导、与政策无关）

> 记录日期：2026-07-11。v7 dev20 全 20 样本 × HP 单臂 × 10RT × no distractor，`minimax-m3`（thinking），live 树执行（协议 `hybridpatch/7` 已在请求中确认），与 v3/v5/v6 campaign 同冻结 task plan/seed；FR 侧从冻结基线字节拷贝 + 哈希校验。调度同 val40（8 key × 5 槽，本轮仅用 20 槽），零重排零失败，429 次调用。**结果复核 PASS 400/400**（200 HP + 200 FR backward RS 独立复现，单一代码指纹）——**v7 部分接受回放的首次实战检验：5 个 partial 提交全部经共享资格函数（`hybrid_gate.partial_acceptance_eligible`）确定性复现**。`preservation_violations=0` 全程。

### headline（vs 冻结 FR 基线）

- n=200，HP **0.888** vs FR **0.726**，**Δ+0.161**（t=6.58，p<0.001，d=0.47）；ECR 条件化 n=193 Δ+0.154。CriticalFailure@10：HP **9/180** vs FR 13/180。成本 HP 15.58M vs FR 7.08M tokens（2.2×）。
- 空响应（thinking 截断）8/429 ≈ 1.9%——背景水位，本轮无事件窗（对照 §227）。

### 部分接受 live 首验（本节核心）

- **触发 5/400（1.2%）**，kept-context 降至 6/400（1.5%；v6 为 9/400）——终局失败形态步 11 个中 5 个被政策转为部分提交。
- 3 个 backward partial 全部做了机械 kept-context 反事实（真实评估器重放），**全部净非负**：

| 步 | v7 提交 | kept 反事实 | 净效应 | 被跳 op |
|---|---|---|---|---|
| **fonteng3 RT7 bwd** | 0.933 | 0.775 | **+0.158** | 1 × overlapping_span（bulk） |
| latex2 RT7 bwd | 0.995 | 0.737 | **+0.258** | 1 × not_found |
| mathlean2 RT4 bwd | 0.982 | 0.979 | +0.003 | 1 × not_found |

- **动机案例同位复发**：fonteng3 RT7 backward 连续两个 campaign（v6/v7 不同链、不同模型输出）在同一 relay 位置出现同形态 op 冲突——v6 全有全无损失 ≈−0.16（§223），v7 live 救回 **+0.158**，与 §226 反事实扫描预测幅度一致。该位置的失败形态高度可复现（任务序列相关），部分接受正中靶心。
- **风险形态样本 mathlean2**（§219/§226 预言的唯一负例类）：live 结果 +0.003 中性偏正，未现负例。
- 2 个 forward partial（fonteng3 RT5、musicsheet2 RT9，各跳 1 op）无当步分数；musicsheet2 RT9 前后 backward 均 0.413 纹丝不动——**forward 分歧链式后果的首个（弱）证据：未观察到 post-partial 劣化**。
- 被跳 op 类型清点：3 × not_found（stale/模型自加锚点）+ 2 × overlapping_span（冲突设计）——全部是"死 op"，零误杀（资格闸其他 gate 检查零放行损伤）。

### 纵向 v6→v7（同 20 样本、同 task plan、不同随机抽样）

- 均值 0.921→0.888（−0.033）：**14/20 样本持平或改善**（protein1 +0.139、docker6 +0.043、fonteng3 +0.023、mathlean2 +0.013 等），3 个大跌样本合计 −0.80 主导差值，**逐个归因均为已知随机内容级类、零 v7 政策涉入**：
  - musicsheet2 −0.286：RT1 bwd bounded 干净执行即 0.563（重建内容损伤）+ RT8 再降；partial 在 RT9 fwd 且其前后分数不变——损伤先于政策事件。
  - starcatalog4 −0.275：RT5 断崖 0.986→0.528 后棘轮，前后零失败标记——§223"结构合法语义错列"（无参考不可拦类）随机复发；三代轨迹 v5 0.757 / v6 0.987 / v7 0.712 = 抽签本性。
  - translation4 −0.243：RT6 断崖 1.000→0.513，全程 bounded 干净执行——重建抽签类。
- 身份指标与 v6 持平（bounded 58.5% vs 58.0%、gen-byte ratio 0.603 vs 0.597、repair attempted 36 vs 41）——**纯政策消融设计成立**：prompt/执行语义未动，行为分布未漂移。

### 净判定

hybridpatch/7 达成设计目标的 live 验证：**全有全无放大被消除（5/5 partial 净非负、动机案例同位救回 +0.158、风险形态未现负例、零误杀），身份指标与失败分布不变**。v6→v7 的均值波动由已知随机内容级类主导（bounded 重建抽签 + 语义错列），与政策正交——这也再次凸显 dev20 单次抽样的方差水平，跨 campaign 均值比较须配合逐样本归因。v7 成为当前候选；val/test 路径待用户决定（val40 已被 v6 消耗，v7 若走 val 需议定集合）。

## 229. OpenCode Go Anthropic transport-v2：完整性事务边界、可审计重试与线性 raw 日志（5×2RT smoke PASS）

> 记录日期：2026-07-11。实现固定供应商 OpenCode Go：SDK base URL `https://opencode.ai/zen/go`，最终 `POST /v1/messages`，模型 `minimax-m3`；Python `anthropic==0.104.1`。MiniMax 默认/硬上限 `max_tokens=131072`，HP primary/repair、FR、Key probe 全部 adaptive thinking。MiniMax official endpoint 未进入运行路径。

### 完整性与分类

- SDK stream 只有同时观察到 `message_delta`、`message_stop` 与 final usage 才可提交；EOF thinking / EOF partial text 均为 `incomplete_stream`，partial text 只进 raw audit、不进实验行。SDK 自带 retry 关闭，项目严格最多 3 个总 attempt；401/403 等不可恢复 4xx 立即失败，408/409/429、5xx、网络/timeout/incomplete stream 才重试。
- 完整响应按正文与 `stop_reason` 分四类：text+end_turn 正常；text+max_tokens=`text_truncated`；无 text+max_tokens=`thinking_budget_exhausted`；无 text+end_turn=`model_empty`。不再用 `<200B` 或 completion token 非零判 API 成败。
- usage 口径实测成立：prompt total = `input_tokens + cache_read_input_tokens + cache_creation_input_tokens`；output 单列。11-Key 16-token canary 每次均完整 thinking-only（不是 Key 失效），1024-token canary 在约 200 total tokens 自行 end_turn 并返回 Hello，故 probe 默认 1024。

### smoke 证据

- `exp_20260711_opencode_transport_v2_smoke5`：foodmenu6/docker6/protein1/mathlean2/translation4，双方法 × 2RT。10/10 checkpoint 完成；结果复核 **PASS 20/20**；HP `preservation_violations=0`。
- API log schema v2 共 43 行。HP：23 calls，22 完整；1 次 `peer closed connection ... incomplete chunked read` 在旧词表下退出，dispatcher 换 Key 后补完。FR：20/20 最终完整；translation4 RT1 forward 的首 attempt `incomplete_stream` 被项目内部重试吸收，第二 attempt 完整结束于 `max_tokens` 且 thinking-only，分类 `thinking_budget_exhausted`。这是真实 E2E 证据：失败 attempt 被保留、partial 未提交、重试成功结果可审计。
- SDK convenience `thinking/text` event 含累计 snapshot，若逐事件原样落盘会 O(n²)：修复前 HP 23 个 transport 文件 1.12 GB（单文件最大 320 MB）；过滤 convenience snapshot、只存 protocol-shaped delta 后，FR 20 个文件 8.6 MB（最大 2.0 MB）。单 Key recorder canary 为 4.3 KB/13 records，snapshot event=0。
- tokens：HP 1,016,471；FR 723,494。诊断 paired Δ+0.192（n=10，p=0.179）不作方法结论：HP 运行中修了 retry/logging，metadata 有两个 fingerprint；正式比较必须两臂从同一最终 fingerprint 新开目录。
- 凭据扫描：305 文件、1,139,105,047 bytes，11 个 Key 值零命中。

### 可观测性

新增 `monitor_experiment.py`：checkpoint 是唯一 committed-RT 依据；最新 `.transport.jsonl` 仅用于显示当前 `RT / forward|backward / thinking|text / elapsed / idle`，不会把 partial stream 当完成。dispatcher 默认每 60 秒打印同一表。该设计解决了长 adaptive thinking 期间“外层无输出但无法判断在哪个 RT”的盲区。

## 230. transport-v3 公平性冻结：response retry 与 transient recovery 分账，跨 worker 防重复 POST

transport-v2 把所有异常压成“最多 3 个总 attempt”，无法回答中断发生在 generation 前还是 thinking/text 已开始后；dispatcher 换 Key 又可能重置进程内预算。v3 改为稳定 `semantic_call_id` 下的双预算：2 个 response slot（首次 + 一次全量重发）与 3 个 generation 前 transient failure。partial thinking/text 永不续传或进入 prompt；完整空/near-empty 按 Baseline 模型失败计分，不做 retry/repair。

新增 append-only attempt ledger 与原子 response journal。完整响应在交还 runner 前持久化，checkpoint 前崩溃可回放本地结果而不再计费；中途崩溃按是否见过 generation delta 恢复已消耗额度。API schema 升至 v3，worker 重启、dispatcher requeue 和 Key rotation 均不能刷新预算。zero-API mock 覆盖完整/缺终止链、thinking/text EOF、429/504、两类预算耗尽、空响应不重试、HP repair 边界与 journal replay；冻结规范见 `API_TRANSPORT_FROZEN_V3.md`。

真实历史回放与新 live smoke 都通过。测试 fixture 从 v2 不可变归档提取 event counts/terminal/usage（不复制正文）：foodmenu6 HP RT2 的 590 个 thinking delta 后 EOF 在 v3 下为 generation 后 `incomplete_stream`；translation4 HP text+max_tokens 为 `text_truncated`；translation4 FR 的 thinking EOF→第二 POST thinking-only max_tokens 恰好耗尽 2 个 response slot，最终为 `thinking_budget_exhausted`，无第三次请求。

`exp_20260712_transport_v3_smoke5`（5 samples×2RT×双臂）10/10 完成、result verification PASS 20/20、preservation 0。41 semantic calls 中：40 个首 slot 完成；protein1 HP RT1 backward 首 attempt generation 后 incomplete，full replay POST 成功；translation4 HP RT1 forward 完整 `max_tokens`、output_tokens=131072、text=0，被保留为模型失败且无 retry/repair。所有 FR calls 首 slot 完成。41 journals / 41 semantic IDs，transient=0，密钥扫描命中 0。诊断 RS Δ+0.107（n=10，p=0.299）只作 transport smoke，不作方法结论。

## 231. exp_20260712_hybridv7val40_transportv3：val40 双臂 transport-v3 campaign（结果复核 PASS 780/780；38 样本 HP≈FR 打平 Δ+0.008；FR-v3 较冻结基线 +0.089——旧 headline 优势主体确认为传输层病理放大）

> 记录日期：2026-07-12。用户指令重跑 val40（接受 v6 曝光，披露照旧）、10 并发。40 样本 × 10RT × **HP/FR 双臂均在 transport-v3 下新跑**（`opencode_anthropic_sdk/3`，adaptive thinking，max_tokens 131072；依 `API_TRANSPORT_FROZEN_V3.md` §7 旧 transport FR 不再作主要对照），task plan 从 `exp_20260710_frbaseline234` 字节拷入（seed 42，与 v6-val40 同 plan 纵向可比）。调度 `fr_baseline_dispatch.py`：KEY_01–10 各 1 槽 + KEY_11 备用，先 HP 后 FR。HP 协议 `hybridpatch/7`。只读归档未编辑。

### 完整性、失败样本与结果复核

- **json4**（双臂，停 RT2）与 **earncall1**（FR 臂，停 RT6）各经 3 把 key 全败（ledger：json4 2 retryable + 1 fatal terminal；earncall1 4 retryable）——按 v3 规范 §4 属基础设施失败（score=null，不算模型 0 分），**规范分析口径 = 其余 38 个完整配对样本**。已提交部分行按纪律保留在磁盘并通过 replay。
- **结果复核 PASS：780/780 backward RS 从 raw 响应独立复现**（含 11 个 partial 提交经 `partial_acceptance_eligible` 确定性重演、两失败样本的已提交行），零 mismatch、`preservation_violations=0` 全程。
- 工具修复（run 后、不影响归档数据）：`utils_env.load_sample` 补显式 `encoding="utf-8"`——无 `PYTHONUTF8=1` 时 Windows 默认 GBK 曾使结果复核中途崩溃，且存在"GBK 不报错但解码错→污染 replay"的隐患。
- 规范工件：`analysis/val40v3_38sample_paired.md`（38 样本规范数字）；`analysis/comparison.md` 为全目录标准工件（含两失败样本部分行，聚合略有差异）。

### headline（n=380，同 task plan、两臂同传输）

- 配对 backward：HP **0.825** vs FR **0.817**，Δ**+0.008**（t=0.65，p=0.51，**不显著**）；ECR 条件化 Δ−0.001；披露口径（剔 2 个 FR 病理样本 landmarks3/satellite4）Δ**−0.010**（p=0.38）。
- RS@1/5/10：HP 0.975/0.831/0.724 vs FR 0.941/0.822/0.705；CriticalFailure@10 **两臂完全相同 21/342**；成本 HP 26.88M vs FR 16.09M tokens（**1.67×**，此前约 2× 有所收窄）。
- FR 病理在 v3 下大幅收敛：38 样本仅 2 个样本出现空/短行（landmarks3 RT10 fwd `thinking_budget_exhausted`；satellite4 RT3 fwd 空 + 3 短行）——对照 v6-val40 时代 9 个病理样本。

### 机制发现（本节核心）：FR-v3 vs 冻结基线同样本 +0.089

- 同 38 样本、同 task plan 逐步配对：**FR-v3 0.817 vs 冻结 FR 基线 0.728，Δ+0.089（t=5.18，p=3.7e-07）**；HP 纵向 v6→v7v3 为 0.848→0.825（Δ−0.022，t=−1.70，p=0.09，不显著；传输与抽样混杂）。
- 解读：transport-v3 的 response-slot 重试（`incomplete_stream` 后一次全量重发）吸收了 §227 定名的"服务端 thinking 中途流截断"——FR 此前对它零护栏、整链毒化；HP 本就靠 repair 二次调用吸收同源病理，故 v3 无增益。**v6-era 同 38 样本 Δ+0.119 → v3 下 +0.008：旧 headline 优势的主体是传输层病理放大，与 §225（+0.195→+0.030）/§227（+0.137→+0.042）披露口径预测定量一致。**
- **方法叙事修正（结论级）**：在传输完整性得到工程保障的条件下，HP 与 FR 在 val 尺度 RS 统计不可区分，且 HP 贵 1.67×。"灾难规避/优雅降级"作为结构性优势，其可兑现价值取决于运行环境的传输可靠性；CF 两臂相同（21/342）说明干净传输下 FR 的断崖率也回落。

### v7 partial acceptance 在 val 尺度（11/760 触发，dev20 为 5/400）

- 6 个 backward 有分事件 ≥0.94（emails5 RT3 0.944 / mathlean5 RT1 0.998 / quantum1 RT7 0.965 / screenplay4 RT3 0.963 / subtitles6 RT4 0.977），5 个 forward 事件无当步分。
- 唯一低分 **spreadsheet1 RT5 0.296（跳 8 op）**：v6 同样本 RT6 塌到 0.278、两代终态同为 ~0.25——任务位相关复发类，初判非政策损伤；**机械 kept-context 反事实待做**（与 forward 事件链审计同列 open item）。
- kept-context 14/760（1.8%）；零误杀迹象；repair attempted 44 / used 27 / success 27。

### 纵向大变动样本（v7v3 vs v6-val40；传输+抽样混杂，非纯政策对比）

- 跌：obj3d2 −0.500（RT6 起全零；bounded_rewrite 干净执行、评估器解析正常，vertex/face accuracy 0.0=内容级损毁）、treebank4 −0.477（RT6 起全零；bulk_patch+repair used，completeness 0.0 而逐 token quality 1.0=内容大面积丢失，repair 极性观察项）、filesystem3 −0.303（RT5 bounded 重建 path_coverage 0.368）、spreadsheet6 −0.231。
- 升：robotics1 +0.529（v6 的 RT6 锁死未复发）、subtitles6 +0.290、emails5 +0.181。
- 三个塌方样本**均无 partial 事件涉入**；形态全部为已知内容级重建/损伤类（评估器解析正常），**不是 C2-lint 注册表缺口类**——修正本轮实时分析中的初步猜测（曾疑 .obj/.conllu lint 缺口）。crystal6 本轮 0.98+ 全程干净（v6-val40 曾全零），其 C2-ext 候选证据仍只有 v6 一例。

### open items

- spreadsheet1 RT5 partial 的机械反事实 + 5 个 forward partial 的链式审计（sweep 机制已有）。
- obj3d2/treebank4/filesystem3 内容级损毁的逐步根因（是否重建抽签 vs 可拦截形态）——未定，候选深挖。
- 方法层面：干净传输下 HP≈FR 且更贵——方法定位/冻结决策需用户重议（残余可辩护主张：CF 不劣、preservation 不变量、病理环境韧性、以及 §223/§228 dev20 尺度的正 Δ）。

## 232. FR+Official：保留原冻结 FR，新增官方 API 部分重跑派生基线

> 记录日期：2026-07-16。该工作只读取既有归档、建立来源清单并重算统计；没有调用 API，没有修改原冻结 FR，也没有重新运行结果复核。

### 组成与固定选择规则

- 版本名固定为 **FR+Official**，范围与原冻结基线一致，为 233 样本 × 10RT。
- 采用 87 条完整 MiniMax 官方非流式 API 轨迹：83 条来自 `exp_20260713_frbaseline_v2_rerun`，另用 `exp_20260713_problem15_official` 中 4 条完整且第一来源缺失的轨迹补齐（crystal6/filesystem3/obj3d2/robotics1）；其余 146 条样本轨迹回退原冻结 FR。
- 整样本选源，禁止按 RT 拼接、按分数选源；选中的官方 task plan 均与原冻结 FR 字节一致。逐样本来源与哈希见 `Baseline/FR+Official/source_manifest.csv`。

### 异常比率与整体曲线

异常定义固定为：异常终止/截断（finish_reason 为 null/abort/length/max_tokens 或对应截断分类）、空返回（strip 后为空或模型空返回分类）、接近空返回（非空且 UTF-8 正文 `<200 bytes`）；“任一异常”为三者并集，各类可重叠。

| 范围 | 任一异常 | 异常终止/截断 | 空返回 | 接近空返回 |
|---|---:|---:|---:|---:|
| **FR+Official（233）** | **87/233（37.34%）** | 86/233（36.91%） | 80/233（34.33%） | 37/233（15.88%） |
| 被选官方轨迹（87） | **85/87（97.70%）** | 85/87（97.70%） | 79/87（90.80%） | 35/87（40.23%） |
| 原冻结 FR（同 233） | 71/233（30.47%） | 68/233（29.18%） | 57/233（24.46%） | 20/233（8.58%） |

官方轨迹池是按旧链异常/问题样本预筛后形成的，故其 97.70% 不能外推为 MiniMax 官方 API 背景异常率。FR+Official 的可报告对象是这一个派生版本本身。整体 RS@1/5/10 从原冻结 FR 的 0.925/0.682/0.558 变为 **0.735/0.550/0.508**；CriticalFailure@10 从 167/2097 变为 **162/2097**。

### V7 val40 替换敏感性

- 原 canonical38：HP 0.825 vs FR-v3 0.817，Δ+0.008（p=0.514）。
- FR+Official strict38：在同 38 样本中替换 16 条完整官方 FR，HP **0.825** vs FR+Official **0.564**，Δ**+0.262**（p=2.54e-28，d=0.62）；RS@1/5/10 为 HP 0.975/0.831/0.724 vs FR+Official 0.686/0.555/0.519。
- maximal39 另纳入有完整官方 FR 的 earncall1：HP 0.829 vs FR+Official 0.551，Δ+0.279；json4 因 HP 不完整仍排除。

若论文把 FullRewrite baseline 的处理流程定义为 MiniMax 官方 API、非流式、
一次性完整响应照单接受，则 `Δ+0.262` 是现有论文协议对齐结果，应在该论文口径下
报告。与此同时，strict38 仅替换 16 条完整官方 FR 链，其余沿用冻结 FR，且官方
重跑池经事后筛选；因此必须把它标为混合来源的现有估计，不能单独解释成 38 个
样本全部同条件重跑得到的无偏方法效应。回答同 transport-v3 方法效应时仍使用
`Δ+0.008`。完整工件见 `Baseline/FR+Official/README.md` 与
`val40_comparison.md`；构建入口为 `Baseline/build_fr_plus_official.py`。

## 233. HP_V8 supplement40：metadata 锁是高并发本地故障；完整诊断中 HP 的长链优势扩大但高度异质

### 现象与修复

40-worker 首次启动在零结果提交前因 `portalocker.AlreadyLocked` 全局停止。原子写出的
`campaign_stop.json` 本可无锁读取，但旧实现让每个 semantic call 的只读检查也争抢全局
metadata writer lock；13 Keys×4 worker 使这条非方法路径成为系统瓶颈。唯一修复是无锁读取
stop latch、真正 writer 使用有界重试锁。40 进程故障注入和完整零 API replay 通过后，新
campaign 保持相同 task plans、模型、方法、prompt、protocol、executor、gate、evaluator、
scoring 与 transport-v4 语义，从新 clean commit 重跑；旧启动 0 结果且不拼接。

### 完整性与结果

`exp_20260719_hybridv8_transportv4_supplement40_lockfix` 完成 1,600/1,600 行和 80 个 RT10
checkpoint，独立复算 PASS 800/800 backward，preservation 0/797 applicable + 3 N/A。
12 次 generation-started incomplete stream 均在同一 semantic call 的第二 response slot
恢复，故没有 infrastructure-incomplete sample。

固定 n=40 的 paired delta 从 RT1 `+0.051086` 增至 RT10 `+0.263959`；RT10 HP/FR 为
`0.837339/0.573381`，但 sample SD=`0.402060`，且仍有 10 个 HP losses 与 20 个 HP
CriticalFailure。六条大胜贡献约一半 RT10 总 delta，说明主要现象是部分任务上 FR 长链更快
坍塌，不是 HP 在所有样本稳定保持。六个与 prior10 相同 task-plan 的 ID 也出现明显跨链
波动，因此 50-chain pooled 和去重 44-ID 都只能是描述性视图。

### 协议与成本边界

HP route bounded/local/bulk/DSL=`579/148/54/16`；repair attempted/used/success=`104/88/78`；
final protocol failure=`20/800`。soft burden `49/800` 与 prior10 的比例近似，且始终非阻塞，
不能把得分差异归因于 burden 阈值。已知 HP/FR usage 为 33.062M/25.796M tokens，费用
USD 26.581893/21.104516；12 个未知 final usage attempt 使账单只能给区间。

### 结论边界

所有 44 个唯一 ID 都已曝光，两 campaign 的 commit、日期、并发与模型随机链不同。允许的
主张是：完整固定诊断范围内 HP 的 RT10 均值较高，且 applicable preservation violations=0；
不允许外推未见集、普遍优越、统计显著性，也不把 score 变化因果归于 metadata lockfix。
完整 RT1–RT10 表与脱敏证据见
`HP_V8/analysis/exp_20260719_hybridv8_transportv4_supplement40_lockfix.md`。

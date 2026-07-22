# HybridPatch 设计总览

状态：`HP_V3`–`HP_V7` 冻结设计总览；当前 active draft 生成协议为
`hybridpatch/8`。本文描述方法语义、版本演进和证据边界，不替代各实验的
canonical record。

## 1. 方法定位与当前结论边界

HybridPatch 用“模型声明写入范围，确定性执行器应用修改”替代每一步都让模型
全文重写。它的核心目标是降低长程 relay 中未要求内容被静默改坏、单次失败污染
后续整链，以及输出截断导致工作区清空的风险。

HybridPatch **不是纯补丁方法**。`bounded_rewrite` 是正式路径：当任务确实要求
整文件生成、格式转换或大面积改写时，模型可以重写声明过的文件。因此必须同时
报告 route share、生成字节占比和额外调用成本，不能只用“Patch”名称暗示所有
内容都由源文档复制。

当前可辩护的设计级结论是：

- 未声明源块由执行器复制，`preservation_violations` 是可独立复核的字节级不变量。
- JSON/schema/op/route/gate 失败显式记录；没有静默 FullRewrite fallback。
- 失败终局要么保留输入上下文，要么在 `hybridpatch/7` 的严格条件下提交可证明
  良定义的 accepted-op 子集。
- 协议版本由归档信封自身门控，旧归档继续按原语义 replay。

这不等于“HybridPatch 已在所有条件下优于 FullRewrite”。当前同 transport-v3
的 canonical val40 严格 38 样本结果为 HP `0.825`、FR `0.817`、
`Δ=+0.008`、`p=0.514`，统计不可区分，且 HP token 成本为 FR 的 `1.67×`。
若论文把 FullRewrite baseline 的处理流程定义为 MiniMax 官方 API、非流式、
一次性完整响应照单接受，则论文协议对应的现有 strict38 结果为
HP `0.825`、FR+Official `0.564`、`Δ=+0.262`。该值必须同时披露混合来源边界：
strict38 只有 16 条 FR 链来自完整官方重跑，其余沿用冻结 FR，替换池且为事后预筛。
因此 `+0.008` 回答同 transport 方法效应，`+0.262` 回答现有论文 baseline
处理政策口径；二者不可互换。

HP_V8 的 10 样本 transport-v4 诊断 exact RT10 为 HP `0.745`、FR `0.696`、
delta `+0.049`，但样本全部已曝光且差值高度异质。它证明 V8/transport-v4 的
完整运行与审计链，不替代上述 strict38 same-transport 方法效应，也不证明提示精简
导致性能提升。

## 2. 端到端管线

```text
文档字节
  │
  ├─ 词法分类 → default 或 block_movement profile
  │    └─ 仅 block_movement 按需调用 split_struct2 coarse index
  │
  ├─ build_hybrid_prompt
  │    ├─ editable context + 权威 target/readonly 名称
  │    ├─ default: local/bulk/bounded
  │    └─ block_movement: local/bulk/DSL/bounded + coarse index
  │
  ├─ LLM → JSON envelope + 可选 [FILE BODIES]
  │
  ├─ extract_hybrid_json / validate_hybrid_envelope
  │
  ├─ apply_hybrid → output context + HybridExecLog
  │
  ├─ validate_hybrid_output
  │    ├─ 成功提交
  │    ├─ 至多一次 semantic repair 后重新判定
  │    ├─ v7 op-rejection-only partial acceptance
  │    └─ kept-context
  │
  └─ domain evaluator → RS → JSONL/checkpoint → verifier/records
```

Runner 在进入 HybridPatch 前把 distractor/read-only 文件从 editable context 中
移除，并把文件名交给 gate。执行器只产出匹配 target pattern 的文件；模型不能
通过在信封里声明某文件可写来扩大 runner 给定的真实写入范围。

## 3. 协议信封与文件体传输

当前生成端输出一个 `hybridpatch/8` 信封：

```json
{
  "protocol": "hybridpatch/8",
  "plan": {
    "task_family": "format_conversion",
    "edit_footprint": "whole_file_change"
  },
  "action": {
    "route": "bounded_rewrite",
    "files": [
      {"file": "report.json", "content": "@body:report.json"}
    ]
  }
}
```

V8 `plan` 恰好只有 `task_family` 与 `edit_footprint`；后者必须与四选一
`action.route` 一致。可编辑、target 和 readonly 文件由 runner 掌握，不要求模型在
plan 中重复授权。执行语义由 `action.route` 决定，而不是由自然语言计划猜测。

长文本、含反斜杠或引号的 `content`、`new_text`、`old_text`、
`anchor_text` 和 `text` 不应嵌套进 JSON 字符串。模型在字段中写
`@body:<name>`，再在 JSON 后输出：

````text
[FILE BODIES]
```report.json
<literal bytes represented as UTF-8 text>
```
````

变长围栏承载字面内容，避免 YARA、LaTeX、正则等反斜杠密集格式被第二层 JSON
转义破坏。短文本仍可内联；旧归档的内联内容继续兼容。`hybridpatch/4+`
会归一化 body-ref 名称周围的意外空白，但真正缺失的引用仍显式拒绝。

## 4. 四条 action 路径

| 路径 | 适用任务 | 核心操作 | 主要约束 |
|---|---|---|---|
| `local_patch` | 少量、精确、位置明确的编辑 | `replace`、`delete`、`insert` | 锚点必须唯一或给出 `occurrence`；v3+ 按步骤输入快照定位 |
| `bulk_patch` | 多处相同字面修改、整行删除 | `replace_all`、`delete_lines_containing` | 禁止正则；v5+ 按步骤输入快照解析全部匹配与 expected count |
| `dsl_rules` | 已有 struct2 块的复制或分发 | `copy_blocks`、`distribute_blocks` | 只用于块搬运，不是通用转换语言；未指派块从 v2 起留在源文件 |
| `bounded_rewrite` | 整文件生成、格式转换、大面积内容变化 | 声明文件及完整内容 | 只能写已声明且匹配 target 的文件；不是少量编辑失败后的兜底 |

路由原则是按编辑足迹选择：多数行不变时优先 local/bulk；多数内容确实变化时
才使用 bounded rewrite。该规则减少身份稀释，但不能以牺牲正确性为代价强迫模型
手写不可靠的精确字面 patch。

`dsl_rules` 的机器上限为：

```text
DSL_MAX_RULES = 16
DSL_MAX_EXPLICIT_IDS = 64
DSL_MAX_EXPANDED_ACTIONS = 128
```

超限属于 route violation，不能通过截断规则静默执行。

## 5. 确定性执行与 preservation invariant

执行器先把输入文件拆成带稳定 block id 的字节块。local/bulk 修改只替换被接受
op 覆盖的 span；其余块继续从源字节表拼回。DSL 显式记录 included、consumed 和
discarded block；bounded rewrite 只生成信封中声明的完整文件。

核心计数为：

```text
preservation_violations =
  未被标记为 edited、但执行后字节不同于源 block 的数量
```

正式归档要求它恒为 `0`。verifier 会从保存的模型响应重新解析信封、重跑执行器、
重做提交判定和 RS，不能只相信运行时记录的最终分数。

这个不变量的保证范围必须准确理解：

- 它保证**未声明源块**不会被执行器悄悄改写。
- 它不保证模型声明的修改语义正确。
- 它不保证 bounded rewrite 内部保留了原文件的所有事实；该文件整体已进入声明
  写入范围。
- 它不替代 evaluator、格式 gate 或人工失败归因。

输出 gate 还检查：

- target 文件是否完整，是否出现额外或空文件；
- read-only 文件是否被输出；
- forward 步是否发生有效修改；
- BEGIN/END 和常见括号的粗粒度结构是否回退；
- route violation 和 preservation violation；
- `hybridpatch/6+` 的格式健康非回退检查。

当前格式 lint 注册表覆盖 JSON、XML/KML/GPX/SVG、OpenQASM 和 PDB。已有输入
不可解析时不凭空要求输出可解析；输入可解析的已改文件不得回退，新建的可 lint
文件必须可解析。可选解析器缺失时该项是 no-signal，而不是假 PASS。

## 6. `hybridpatch/2`–`hybridpatch/8` 演进

所有语义都由信封内的 `protocol` 经 `rev_of()` 选择。生成端默认 v8，提取和
执行端接受 v1–v8；schema 会拒绝未知或缺失的 protocol。`rev_of()` 的 v1
fallback 只是防御性旧语义默认值，不会绕过 schema gate。

| 版本 | 变化 | 解决的问题 |
|---|---|---|
| `/1` 基线 | 严格 block-local 匹配；四路径和显式失败的原始实现 | 建立方法骨架，但 prompt 暗示的文件级能力与执行器不一致 |
| `/2` | 空白容错匹配阶梯；local 可跨 struct2 block 做文件级匹配；`distribute_blocks` 未指派块留在原位 | 减少模型为执行器内部 block 边界付出的协议负担 |
| `/3` | local op 全部按步骤输入快照解析 occurrence/唯一性；重叠 span 拒绝；一次性从右向左应用；runner repair 扩展到 op/route/gate 失败 | 消除前序 op 导致后序 occurrence 漂移和本可修失败没有 repair 机会的问题 |
| `/4` | bulk 字面匹配提升到文件级；body-ref 名称空白归一；空白容错改为先 raw token、再 unescape 变体；prompt 增加编辑足迹路由压力 | 修复跨块 bulk、悬空 body-ref 和 `\ref`/`\rho` 被误解释的问题，并降低不必要 bounded rewrite |
| `/5` | bulk 的所有匹配和 expected count 按步骤输入快照解析；跨 op 冲突显式拒绝；同行 delete 去重；统一右向左应用 | 防止前序 op 新插入文本被后序 op 再次匹配，保证 accepted op 集可独立解释 |
| `/6` | bulk 包含型冲突消解：delete span 完全包含 replace span 时由 delete 吞并；新增 reference-free format-health gate | 修复等价顺序操作被误判重叠，并拦截“格式从可解析退化为不可解析”的内容损伤 |
| `/7` | executor 和 gate 与 v6 字节语义相同；新增 runner/verifier 层 partial-acceptance 提交政策 | 避免一个死 op 让同一步其他安全 accepted op 全部丢失 |
| `/8` | 按需 coarse index/DSL；default 保留 local/bulk/bounded，block profile 保留四路径；plan 只含 task family 与 edit footprint；repair 去除无关上下文；负担 P95 改为软 telemetry | 减少无用提示与显式协议负担，同时不关闭有效路径、不按长度拒绝正确输出 |

文件体 `[FILE BODIES]` 传输是在 v2 前的 P0 修复中引入，之后各版本沿用；它不是
单独的新 route。每次版本升级都保留旧分支，使冻结归档继续按原 protocol 重放。

## 7. Repair、kept-context 与 partial acceptance

### 7.1 一次 semantic repair

每个 HP 步有一个 primary semantic call。只有非空响应已经出现 JSON、fence、
`protocol`、`ops` 等明确协议信号，并发生可描述的 JSON/schema/op/route/gate
失败时，才允许一次 `hybridpatch_repair`。

以下输出按模型失败处理，不触发 HP repair：

- 完整空响应或 thinking-only；
- refusal；
- 没有任何协议信号的普通说明文字；
- transport 已耗尽而没有可提交响应。

repair 是独立 semantic call。runner 用确定性的 attempt key 在 primary 与 repair
之间选择更完整、gate 更好、op accept rate 更高的尝试。FullRewrite 没有 semantic
repair；transport 层对 incomplete stream 的重发不等于方法 repair。

### 7.2 kept-context

若选中尝试没有可提交输出，runner 将该步 editable input 原样带到下一步，并记录
`hybridpatch_protocol_failure_kept_context`。这是显式失败，不是假装成功，也不会
调用 FullRewrite 替代。

### 7.3 v7+ partial acceptance

v7 和继承该政策的 v8 只在 repair 已经用完、终局失败**仅由 op rejection 导致**时考虑提交执行器已经
应用的 accepted-op 子集。必须同时满足：

1. 信封是 `hybridpatch/7` 或 `hybridpatch/8`；
2. route 是 `local_patch` 或 `bulk_patch`；
3. 至少一个 op accepted，且至少一个 op rejected；
4. 没有 route violation；
5. partial output 与输入不同；
6. partial output 通过 target、read-only、有效修改、粗结构、格式健康和
   preservation 等全部其他 gate。

local 的 v3 快照语义和 bulk 的 v5 快照语义使 accepted op 不依赖被拒 op 的中间
副作用，因此子集结果是确定的。runner 与 verifier 共用
`partial_acceptance_eligible()`，旧 protocol 被硬门控为不合格。

提交时记录 `partial_acceptance=true` 和 `partial_skipped_ops`。当前证据量仍有限：
V7 dev20 为 `5/400` 步，V7 val40 canonical 归档为 `11/760` 个 HP 步，V8
10 样本诊断为 `1/200`；这支持机制可运行和 replay，不足以证明所有任务类型上的
普遍无害性。

## 8. Transport 边界

HybridPatch 方法层只处理“完整模型响应交给应用后如何解析、执行和提交”。
连接、流完整性、重发预算、journal 和 provider 错误属于 transport 层。两层必须
分开计数，否则会把工程可靠性提升误报成方法能力。

### 8.1 冻结 OpenCode transport-v3（历史正式比较）

V7 历史正式同传输比较使用 `opencode_anthropic_sdk/3`：

- `message_delta`、`message_stop` 和 final usage 缺一不可；
- partial thinking/text 只进 raw audit，不进入上下文、repair prompt 或评分；
- 每个 `semantic_call_id` 最多 2 个 response slot；
- generation 前 transient failure 最多 3 次；
- attempt ledger 跨 worker/Key 保存预算；
- 完整响应先原子写 response journal，checkpoint 前崩溃可本地 replay，避免重复 POST；
- 完整空、thinking-only、refusal 不做 transport retry；
- transport 最终耗尽是基础设施缺失，`score=null`，不能记为模型 0 分。

正式 HP/FR 比较必须使用相同 transport revision、兼容 fingerprint、同一新
`out_dir` 和同一 task plan。旧 transport 的 FR 不能作为同传输 primary control。

### 8.2 活动 OpenCode transport-v4

新实验使用 `opencode_anthropic_sdk/4`，不改方法层语义。它把 HTTP 200
`Streaming response failed`、缺 `message_stop`、缺终结 usage 与 block 未正常
闭合统一为 retryable `incomplete_stream`，并在普通 status-code 分类前判断。

每个 exact semantic call 仍严格限制为 2 个 generation response slot 和 3 个
pre-generation transient failure。基础设施耗尽后的人工恢复在同一逻辑 root 下创建
新的 `gNNN` semantic call，并强绑定 parent、request fingerprint 与连续 attempt
index；不在旧 semantic ID 内清零预算。paired campaign 只有在 sample outcome、
run metadata、API row 和 attempt ledger 四证一致时才隔离单 sample，其他完整性错误
仍全局停止。活动规范见 `transport/docs/API_TRANSPORT_V4.md`；v3 文档和归档保持冻结。

### 8.3 MiniMax 官方非流式路线

`minimax_official_nonstream/1` 是独立、互斥的诊断/基线来源。它按 baseline
对齐政策接受完整 HTTP 200，包括 `finish_reason=abort`，不实现 v3 的 ledger 和
response journal。官方与 OpenCode revision 禁止混入同一实验目录。

FR+Official 把完整官方轨迹与冻结 FR 组合成 post-hoc 敏感性视图；它不是
same-transport method effect，也不覆盖 canonical V7 val40 结论。

### 8.4 代码边界

API/transport 语义只在 `transport/` 中开发并分配新 revision。验证后，需运行的
版本快照按原字节同步并记录 fingerprint；冻结的 `HP_V3`–`HP_V7` 不接受后续回灌。
新方法语义必须复制为下一 `HP_Vx`，而不是修改旧 protocol 归档。

## 9. 指标、canonical scope 与证据入口

主结果单位是每轮 backward Reconstruction Score（RS）。常用汇总包括：

- RS@1、RS@5、RS@10；
- 同 task plan 的逐步 paired mean delta、t、p、Cohen d；
- CriticalFailure：相邻 backward RS 跌幅达到 `theta=0.10` 或归零；
- route share、bounded rewrite share、generated byte ratio；
- repair attempted/used/success、kept-context、partial acceptance；
- input/output/total token、延迟；
- `preservation_violations`。

基础设施未完成行保持 `null`，不能填 0。只有预先声明的完整配对范围进入
canonical statistic；partial 历史仍保留供核查。

本仓库的证据入口为：

- [全局实验索引](EXPERIMENT_INDEX.md)：所有已登记实验的 lifecycle、claim use、
  canonical scope 和 verification。
- 每个实验的 `report.md`：GitHub 和外部模型直接阅读的 canonical human report。
- 每个实验的 `summary.json`：唯一 canonical machine scope。
- 每个实验的 `casebook.jsonl`：配对胜负、partial、kept-context、严重失败与
  排除案例的确定性诊断选集。
- `experiment.yaml > canonical_report.source_report_ref`：原始人类分析来源；
  位于 ignored archive 时，其清理后快照嵌入 `report.md`。
- [FINDINGS](FINDINGS.md)：机制归因、反事实和已知例外；它不能静默覆盖 record
  声明的样本范围。
- `HP_Vx/VERSION.md`：协议来源、指纹和冻结验收。

当前主要证据：

| 主题 | canonical / source evidence |
|---|---|
| v3 正式 dev20 | [report](../HP_V3/records/exp_20260708_hybridv3dev20full/report.md)；唯一 quantum4 replay 差异已披露 |
| v4 机制 smoke | [report](../HP_V4/records/exp_20260709_hybridv4smoke/report.md) |
| v5 dev20 | [report](../HP_V5/records/exp_20260709_hybridv5dev20full/report.md) |
| v6 dev20 / val40 | [dev20](../HP_V6/records/exp_20260710_hybridv6dev20full/report.md)；[val40](../HP_V6/records/exp_20260711_hybridv6val40/report.md) |
| v7 partial acceptance dev20 | [report](../HP_V7/records/exp_20260711_hybridv7dev20full/report.md) |
| v7 same-transport canonical38 | [human report](../HP_V7/records/exp_20260712_hybridv7val40_transportv3/report.md)；[machine summary](../HP_V7/records/exp_20260712_hybridv7val40_transportv3/summary.json)；[casebook](../HP_V7/records/exp_20260712_hybridv7val40_transportv3/casebook.jsonl)；[verification](../HP_V7/records/exp_20260712_hybridv7val40_transportv3/verification.txt) |
| v8 transport-v4 10 样本诊断 | [sanitized summary](../HP_V8/analysis/exp_20260718_hybridv8_transportv4_snapshotfix_paired10.md)；生成 record 私有封存以避免改写冻结 catalog hash |
| 冻结 FR / FR+Official | [Baseline index](../Baseline/EXPERIMENTS.md) |
| transport-v3 规范与 smoke | [frozen spec](../transport/docs/API_TRANSPORT_FROZEN_V3.md)；FINDINGS §230 |
| transport-v4 活动规范 | [active spec](../transport/docs/API_TRANSPORT_V4.md)；新实验必须使用独立 out_dir |

`analysis/comparison.md` 不自动等于 canonical。V7 val40 的普通 comparison 包含
两个基础设施失败样本的 partial rows；正式口径是 38 个完整配对样本的专用报告和
生成 record。

## 10. 已知限制

1. **同可靠 transport 下没有 RS 优势证据**：canonical38 为 `Δ=+0.008`
   且不显著。论文采用官方非流式一次性 baseline 政策时应另报
   FR+Official `Δ=+0.262`，并附 16/38 官方链、混合来源和事后预筛披露。
2. **成本更高**：HP primary、可选 repair、较长协议输出共同增加 token；
   canonical38 为 FR 的 `1.67×`，V8 10 样本诊断的已知 usage 为 `1.27×`。
3. **bounded rewrite 仍占多数**：V8 10 样本诊断中声明 route
   bounded rewrite `124/200`（62.0%）；另有 1 个 model-empty 无合法 route。方法更准确的
   定位是“有受约束写入和失败门的混合编辑”，不是纯局部 patch。
4. **preservation invariant 不保证声明区域正确**：模型可在合法写入范围内产生
   事实丢失、语义错列或格式虽可解析但内容错误。
5. **gate 覆盖有限**：当前 lint 注册表不覆盖 `.cif`、OBJ、CoNLL-U 等全部格式；
   也没有 reference-free 的通用语义正确性判定。
6. **partial acceptance 样本量小**：目前只支持“在观察到的 local/bulk
   op-rejection-only 事件中可确定 replay”；forward partial 的长期链式影响和少数
   低分事件仍需反事实审计。
7. **模型与抽样范围有限**：主要正式链为 MiniMax-M3、seed 42；V7 val40 使用
   已被 V6 运行过的 val20+reserve20，不能替代新的 sealed test、第二 seed 或
   第二模型稳健性实验。
8. **评估器与环境敏感性存在**：V3 quantum4 RT9 存储分数 `0.985`、重放
   `1.000`，属于已披露的解析器环境差异。
9. **历史 provenance 不完美**：部分历史运行没有记录精确 Git commit 和结束时间，
   records 使用归档内逐文件 fingerprint 并显式保留 `null`，不从当前 HEAD 猜测。
10. **raw 归档仍需保持私有**：record 保存位置、哈希和清理状态；未清理的完整
    prompt、模型响应和网络日志不属于普通 Git 代码提交。

## 11. 实现映射与变更纪律

冻结语义位于各 `HP_Vx/src/`；当前 active draft 语义参考 `HP_V8/src/`：

| 文件 | 职责 |
|---|---|
| `hybrid_schema.py` | protocol/route/schema、body-ref、R-ENUM、`rev_of` |
| `hybrid_index.py` | 文件和 struct2 block 索引 |
| `hybrid_prompt.py` | prompt、repair prompt、JSON 与 `[FILE BODIES]` 提取 |
| `hybrid_executor.py` | 四路径确定性执行、快照 span、`HybridExecLog` |
| `hybrid_gate.py` | 输出 gate、格式健康、`partial_acceptance_eligible` |
| `experiment_runner.py` | primary/repair 选择、commit policy、JSONL/checkpoint |
| `verify_anchorpatch.py` | 从 raw response 重演解析、执行、partial policy 和 RS |
| `analyze.py` | RS@k、配对统计、失败与路由遥测 |

新 HP_V8+ 实验完成且确认 worker 停止后，从仓库根运行：

```bash
export PYTHONUTF8=1
# 实验前已从 docs/experiment_plans/TEMPLATE.md 创建并审阅：
# docs/experiment_plans/exp_YYYYMMDD_SLUG.md
python ./tools/process_experiment.py prepare \
  --experiment ./HP_V8/exp_YYYYMMDD_SLUG \
  --confirm-stopped

# 审核 analysis/record_review.yaml 后
python ./tools/process_experiment.py finalize \
  --experiment ./HP_V8/exp_YYYYMMDD_SLUG
```

`prepare` 运行该版 verifier/analyze 并只提取机械事实；canonical scope、排除、
claim 与 paper reporting 必须在 review YAML 中显式审核。`finalize` 校验 input
digest 后原子注册 catalog、生成九件套并运行两种 validator。全过程不调用 API，
不覆盖 raw JSONL、checkpoint、task plan、prompt、evaluator 或 scoring。任何
新执行语义都必须进入新的 protocol 和新的 `HP_Vx`；旧版本的价值正是能够继续
重放自己的历史归档。

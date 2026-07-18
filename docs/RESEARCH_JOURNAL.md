# HybridPatch 研究实录

> **这是什么**：一份活文档（living document），按时间记录 HybridPatch 的设计哲学、实现思路、遇到的每一个 bug 和每一轮实验数据。新 campaign 跑完后按 §6 的模板追加。
> **记录规则**：本文档只写通过 `verify_anchorpatch.py` 结果复核（从 raw LLM 响应独立重算每个 RS）的数字；复核 FAIL 的数据必须注明原因与处置，不得作为方法证据引用。

---

## 1. 问题与设计哲学

### 1.1 源问题

DELEGATE-52 基准揭示的现象：让 LLM 对专业文档做往返中继编辑（forward 编辑 → backward 撤销，链式 10+ 轮），**全局重写（FullRewrite）会把没有要求改动的内容一起改坏**——文档被静默污染，重建分数（RS）逐轮衰减甚至断崖。

### 1.2 为什么选补丁作为入口

补丁天然适合限制写入范围：模型只声明要改什么，其余内容不经过模型的"手"。这是对抗静默污染的结构性手段，而非提示工程手段。

### 1.3 旧补丁路线的教训（AnchorPatch v1/v2/C1–C70）

前身 AnchorPatch 经历了 v1 → v2 → v2.1 → v2.2 → C1–C70 共七十多个候选迭代，最终结论：**逐块、逐槽、逐操作枚举的大协议本身成为模型的认知负担**。模型要同时完成任务理解、全局规划、协议编写、多文件协调、格式维护——这个负担在不少任务里接近甚至超过全文重写，RS 上 AP≈FR 统计不可区分。协议救不了协议。

### 1.4 四条一级设计原则

由上述教训直接推出：

| 原则 | 含义 |
|---|---|
| **减负优先** | 任何补丁协议都必须减少模型负担；大枚举协议退出主路径 |
| **写入有界** | 模型只能修改声明过的文件和区域；应保留内容由执行器复制 |
| **路径显式** | 不同任务形态走不同路径；有界重写是一条显式路径，不是耻辱的 fallback |
| **失败显式** | 无效不修改、空输出、漏文件、只读被改，都直接记失败——禁止任何静默兜底 |

### 1.5 唯一研究主张

**整体上优于 FullRewrite**，在同一 seeded 任务序列上配对比较。不做分域挑选，不做 cherry-picking；输就是输，赢要能被独立重算复现。

---

## 2. 方法设计与实现

### 2.1 三层管线

```
文档(bytes) → splitters.split_struct2 → Block 列表(block_id = "<filename>:<seq>")
            → hybrid_prompt.build_hybrid_prompt(全文 + 文件/块索引 + 4 路径 schema + 规则)
            → LLM 输出信封 JSON(+可选 [FILE BODIES] 围栏原文块) → extract_hybrid_json
            → hybrid_executor.apply_hybrid(确定性执行 + HybridExecLog 遥测)
            → {filename: content} → 领域评估器 → RS
```

### 2.2 信封与 4 条 action 路径

模型输出一个信封：`{protocol, plan, action}`。`plan` 是计划单（task_family / writable_files / readonly_files / target_files / obligations），`action` 是**一条**路径的最小结构——模型只输出当前路径需要的东西，不输出超量协议。

| 任务形态 | 路径 | 说明 |
|---|---|---|
| 少量精确编辑 | `local_patch` | old_text/anchor_text 文件级匹配，须唯一或给 occurrence |
| 大量同型修改 | `bulk_patch` | 字面 replace_all / delete_lines_containing，禁正则 |
| 块搬运/分发 | `dsl_rules` | 仅块 copy/distribute，受 R-ENUM 上限，非通用转换 |
| 整文件生成/转换 | `bounded_rewrite` | 声明可写文件，内容走 `content` 或 `@body:` 引用 |

（原设计稿是 5 条路径——"规则组装/受限转换程序"独立成路；实现时并入 `dsl_rules`+`bounded_rewrite`，通用转换一律走有界重写。）

### 2.3 文件体传输（@body: + [FILE BODIES]）

含反斜杠/引号/多行的文本**不嵌 JSON 字符串**（那是二次转义地狱，见 §4.1），而是在字段里放 `@body:<name>` 哨兵，在 JSON 之后的 `[FILE BODIES]` 段用变长围栏原文块承载。写侧（content/new_text）与匹配侧（old_text/anchor_text/text）都支持。

### 2.4 执行层：确定性 + 字节保留

执行器按信封确定性地复制、替换、搬运、组装。**核心不变量：`preservation_violations` 恒为 0**——未被任何 op 声明的块若被改动即执行器 bug。这是 HybridPatch 唯一稳健的字节级声明，任何执行器改动不得破坏。

### 2.5 验证层：失败显式

reference-free 输出闸（`hybrid_gate`）：目标文件齐全、只读不动、有效修改审计等。任何 op 拒绝 / route violation / gate 失败 → 该步**保持原上下文**（kept-context）并显式打失败标记；**禁止静默 FullRewrite fallback**。宁可显式 no-op，不做暗改。

### 2.6 repair：单次、有界、可重放

主调用失败后至多一次 repair 调用，携带具体错误；两次尝试用确定性 key 择优。选择规则版本化，结果复核 replay 时重演同一规则——repair 不是黑箱。

### 2.7 版本门控哲学

执行器语义改动必须走新 protocol rev（`rev_of` 按**信封自带的 protocol 字段**分支），旧归档在新代码下必须**字节级复现**。这使得：旧实验永远可重放审计、语义演化有明确的断代边界、"当前代码能否复现旧数据"成为每次改动的硬回归。

| 协议 rev | 语义 |
|---|---|
| `hybridpatch/1` | 严格块局部匹配（初版，冻结） |
| `hybridpatch/2` | E1 空白无关匹配阶梯 + E2 文件级跨块匹配 + E3 distribute 局部覆盖（冻结） |
| `hybridpatch/3` | v2 全部语义 + local_patch 快照定位：occurrence/唯一性按步骤输入文档解析、span 互不重叠校验、右到左一次性应用、同位 insert 按 op 序组合（当前生成默认） |

### 2.8 结果复核

`verify_anchorpatch.py` 从存储的 raw 响应出发，独立重放整条 relay（含 repair 择优规则），重算每个 backward RS 与存储值逐一比对；非零退出表示结果不一致，需调查。**任何数字先完成复核再引用**——这条纪律在本项目里不止一次拦下了"看起来能用其实不能用"的数据（见 §5.2）。冻结归档只引用既有复核记录，不因文档更新或派生分析重复运行脚本。

---

## 3. 基础设施要点

- **模型**：minimax-m3 经 OpenCode Go Anthropic 兼容 `/messages` 端点；`MINIMAX_THINKING=1` 开 extended thinking（budget 不设人为上限）；SSE 流式调用（见 §4.4）。
- **实验循环**：AP 与 FR 共享同一 seeded forward 序列（task_plan 落盘复用）→ 同任务配对比较；每往返原子提交（JSONL + checkpoint）→ 幂等断点续跑；runner `main()` 按样本隔离故障（见 §4.5）。
- **遥测**：每步落 route / protocol_rev / op 接受率 / 拒因明细 / gate 错误 / repair 记录 / 字节来源比（copied vs generated）/ preservation_violations。归因分析全靠它。

---

## 4. Bug 史（现象 → 根因 → 修复 → 教训）

按发现顺序。每一条都改变了设计或纪律。

### 4.1 P0：JSON 转义摧毁 backslash-dense 内容

- **现象**：malware（YARA 规则）、latex 等反斜杠密集域，补丁内容嵌 JSON 字符串后二次转义损坏，op 匹配失败或写出坏内容。
- **根因**：把原文当 JSON 字符串运输，转义层数随嵌套增长。
- **修复**：`[FILE BODIES]` 围栏传输 + `@body:` 哨兵——原文零转义，JSON 里只放引用。
- **教训**：**运输层永远不要让载荷经过第二次编码**。协议设计首先要为"最难运输的内容"设计。

### 4.2 执行器 vs prompt 契约错位（33% kept-context）

- **现象**：早期 dev 轮 33% 的步 kept-context no-op，op_accept_rate 0.783。
- **根因**：prompt 教模型"文件级定位、空白宽容"，执行器实际"块级定位、字节严格"——模型按契约 A 写，执行器按契约 B 判。
- **修复**：`hybridpatch/2` 执行器优先对齐——E1 空白无关匹配阶梯（可见字符仍精确）、E2 文件级跨块匹配（span 映射回重叠块）、E3 distribute 未指派块留在源文件。
- **教训**：**协议契约只有一份，以执行器为准**；prompt 描述的行为必须是执行器真实实现的行为。

### 4.3 match-侧 body-ref 未解析

- **现象**：malware6 RT4 forward ecr=0，op 拒因 `not_found matches:0`，但 old_text 是 `@body:op4_old`。
- **根因**：§4.1 的传输修复只在**写侧**解析 `@body:`，匹配侧（old_text/anchor_text）把哨兵当字面文本去 find。
- **修复**：匹配侧同样解析；引用悬空拒因改为 `body_ref_not_found`（不再误报 not_found）。
- **教训**：新机制引入的**接缝**要枚举全部消费点；一个字段族（"文本"）在协议里出现几次，解析就要覆盖几次。

### 4.4 thinking + 非流式 + Cloudflare 120s = 524 结构性死亡

- **现象**：thinking 开启后长调用反复 HTTP 524，重试 10 次全部无效，两臂进程先后死亡（exp_20260704_hybridv2think2 因此不完整）。
- **根因**：非流式请求的首字节要等全部生成完成；Cloudflare 在源站前有 120s Proxy Read Timeout——单次生成稳定超 120s 时，**每次重试都撞同一堵墙**，客户端 watchdog/重试预算全部无关。
- **修复**：minimax 路径改 SSE 流式（`stream:true`，逐事件重组完整消息）；字节持续流动使 CF 读超时不再触发，socket timeout 顺带变为块间停滞守卫。
- **教训**：**分辨"瞬态错误"与"结构性错误"**——前者值得重试，后者重试只是烧钱。修错误前先确认它属于哪类。

### 4.5 runner 无样本隔离

- **现象**：上述 524 死亡发生在第 3 个样本时，第 4、5 个样本从未开始。
- **修复**：`main()` 对单样本 relay 包 try/except，失败记录后继续下一样本；checkpoint 幂等续跑兜底。
- **教训**：**故障爆炸半径要与故障单元一致**——一个样本的死不该赔上整个 campaign。

### 4.6 protocol_version 遥测标签硬编码

- **现象**：44 行 envelope 实际全部声明 `hybridpatch/2`，遥测却全标 `hybridpatch/1`。
- **根因**：runner 写遥测时硬编码了标签，没读执行器返回的真实 `protocol_rev`。
- **修复**：从 exec log 读真实执行 rev。
- **教训**：**遥测必须来自执行现场**，不能来自书写者的假设——否则归因分析在错误的地图上进行。

### 4.7 occurrence 索引漂移（→ hybridpatch/3）

- **现象**：exp_20260706_hybridthink5 中 docker6 两步共 5 个 op 拒于 `occurrence_out_of_range`，且 docker6 是唯一败给 FR 的样本。
- **根因**：任务要求"把 4 处 X 全改成 Y"，模型按**原文档**编号发 occurrence=1..4；执行器逐 op 对**变异中**的文档解析——每替换一处剩余匹配数递减，occ=3 时只剩 2 处 → 拒绝。更隐蔽的是被接受的 op 也发生了无害错位（恰好替换文本相同才没造成伤害）。模型的理解（快照编号）是自然语义，执行器行为是 footgun。
- **修复**：`hybridpatch/3` 快照语义——所有 op 的匹配一律按步骤输入文档解析，span 互不重叠校验后一次性应用；prompt 同时加路由引导（同 old_text 多处替换本该走 `bulk_patch replace_all`）。
- **教训**：**声明式协议的引用系统必须锚定在双方共同可见的状态上**（模型看到的 prompt 快照），而不是单方内部的中间状态。

### 4.8 repair 触发盲区

- **现象**：think5 的 9 个失败步中 6 个（全部 op_rejected/gate 类）`repair_attempted=False`。
- **根因**：repair 触发条件只有 invalid_json/schema；且旧 repair prompt 说 "Output ONLY the corrected JSON"，实际禁止了 `[FILE BODIES]` 补发。
- **修复**：任何失败类都触发（op 拒绝/route violation/空输出/gate 失败——均为可精确描述的错误）；repair prompt 携带 [TASK] + [EDITABLE DOCUMENTS] grounding，明确允许 bodies。
- **教训**：**给"可精确描述的失败"以修复机会**是廉价的；但 repair 必须有界（单次）且可重放（确定性择优），否则会稀释"单次调用≈FR"的公平性。

### 4.9 模型侧失败形态（非执行器 bug，但要设计承接）

think5 归因中剩余的失败：声明了 `@body:X` 却完全不写 [FILE BODIES]（协议违规）；max_tokens 64000 被 thinking + 大文档烧满导致截断；引用不存在的 stale old_text；歧义 delete 不带 occurrence。前两类可被 §4.8 的扩展 repair 承接；后两类是模型能力边界，执行器正确拒绝即是正确行为。

### 4.10 Anthropic 流完整性与 SDK 事件日志

- **现象**：旧手写 SSE 在 EOF 时直接返回，thinking-only 或 partial text 均可能被当作成功；transport smoke 又观察到 `peer closed connection ... incomplete chunked read` 未命中内部重试，以及 SDK convenience `thinking/text` 事件携带累计 snapshot，使单个 raw 日志膨胀到 320 MB。
- **根因**：完成判据只看已收文本，没有要求 Anthropic 的 `message_delta + message_stop + final usage`；重试词表漏了 httpx `RemoteProtocolError` 文案；SDK convenience event 的 `snapshot` 是累计值，逐 delta 保存形成 O(n²) 写放大。
- **修复**：OpenCode Go 路径切换到 `anthropic==0.104.1` 的 `messages.stream()`，项目层严格执行 3 个总 attempt；缺任一终止证据即 `incomplete_stream` 并丢弃 partial text；补全 chunked-read 重试分类；raw 日志仅保存 protocol-shaped delta。实测修复后 FR 20 个 call 的 transport 日志总计 8.6 MB（最大 2.0 MB），而修复前 HP smoke 为 1.12 GB。
- **教训**：**HTTP 200 与收到若干 token 都不等于完成**；流式协议必须以终止事件定义事务边界。可观测性也必须保持线性复杂度，否则审计本身会成为运行风险。

---

## 5. 实验轨迹

### 5.1 p0smoke（exp_20260704_hybridp0smoke）——转义修复后基线

`hybridpatch/1`，无 thinking，max_tokens 20000。结果复核 **PASS**（100 backward RS）。

| RS@10 | HybridPatch | FullRewrite |
|---|---|---|
| malware6 | 0.951 | 0.892 |
| latex2 | 0.685 | 0.372 |
| mathlean2 | 0.876 | 0.862 |
| foodmenu6 | **0.434** | 0.819 |
| docker6 | **0.647** | 0.922 |

转义 bug 确认修复（malware6 翻正），但 foodmenu6/docker6 大输，整体均值 AP 0.719 vs FR 0.773——**此阶段 HybridPatch 整体是输的**。

### 5.2 think2（exp_20260704_hybridv2think2）——诊断归档，数据不可作方法证据

`hybridpatch/2` + thinking 首次尝试。三重问题：两臂死于 524（§4.4）致 campaign 不完整；run 中改码致单目录混两个 code fingerprint；结果复核 **FAIL**（2 行 hybrid 预存漂移——run 后落地的 match-侧 body-ref 修复改变了 replay 结果，属可解释漂移而非造假）。**处置**：定性为诊断归档；产出了 §4.4/§4.5/§4.6 三个修复。
**教训**：campaign 进行中绝不改代码；一个目录一个代码版本。

### 5.3 think5（exp_20260706_hybridthink5）——首个完整验证的胜利

`hybridpatch/2` + thinking + SSE 流式 + 样本隔离，max_tokens 64000，5 样本 × 10RT × 双臂，单一 code fingerprint。结果复核 **PASS**（100/100 backward RS）。

**配对统计**：backward RS AP **0.946** vs FR **0.571**，Δ**+0.375**（n=50，t=6.36，p<0.001，Cohen d=0.90）；ECR 条件化 Δ+0.340。CriticalFailure@10：AP 2/45 vs FR 4/45。

| RS@10 | HybridPatch | FullRewrite |
|---|---|---|
| malware6 | 0.990 | 1.000 |
| latex2 | 0.741 | 0.424 |
| mathlean2 | 0.900 | 0.140 |
| foodmenu6 | **0.983** | 0.025 |
| docker6 | 0.684 | **0.945** |

- **foodmenu6 从 0.434 翻到 0.983**（p0 阶段最大败点翻盘）；docker6 仍败，且其全部失败步正是 §4.7 的 occurrence bug。
- 路由份额：bounded_rewrite 52%、local_patch 38%、bulk_patch 7%、失败 3%。
- 成本：AP tokens 2.39M vs FR 1.18M（约 2×）。
- `preservation_violations` 全程 0。
- 9 个 kept-context 步的完整归因见 §4.7–§4.9 与 FINDINGS §215。
- 纵向对比注意：与 p0smoke 同 seed 同 task plan，但混杂 thinking/max_tokens/协议三因素，单样本差异不可单独归因。

### 5.4 v3–v6 dev 迭代与 FR 规范基线（2026-07-07 → 07-11）—— journal 简版从略，详录在 FINDINGS

- `hybridpatch/3`（快照 local + 扩展 repair）→ `/4`（bulk 文件级 + body-ref 归一 + ws 两趟）→ `/5`（快照 bulk + P1 路由压强）→ `/6`（C1 包含吞并 + C2 格式健康 gate）四轮 dev 迭代与三个 dev20 campaign（0707/0708 v3、0709 v5、0710 v6）：FINDINGS §216–§223。
- v6 dev20 HP 臂：结果复核 PASS 200/200，20 样本均值 0.9207，v5 四大回归全部修回、身份指标保持（bounded 58.0%）。
- **FR 规范基线冻结**（`exp_20260710_frbaseline234`，FINDINGS §224/§225）：233 样本 × 10RT 一次性无 rewind，结果复核 PASS 2334/2334，$87.00/86.0M tokens；python1 弃用（上游打包遗漏实锤）。自此 HP 迭代只跑单臂、配对一律引用基线。dev20 配对 Δ+0.195（披露口径 +0.030）。

### 5.5 val40（exp_20260711_hybridv6val40）—— v6 在 40 样本 val 集复现 dev20 定量画像

`hybridpatch/6`（冻结快照 `_snap_val40` 执行）+ thinking，uncapped，40 样本（val20+unused_reserve20，封存 test20 不动）× 10RT × HP 单臂，FR 侧字节拷贝冻结基线；8 key × 5 槽 + 3 冷备。结果复核 **PASS**（800/800 backward RS，零 mismatch，单指纹）。

**配对统计**：backward RS HP **0.848** vs FR **0.711**，Δ**+0.137**（n=400，t=6.21，p<0.001，d=0.31）；披露口径（剔 9 个 FR 空响应命中样本）Δ**+0.042**（n=310，t=1.96）；两半子集 val20 +0.129 / 全新 unused20 +0.146——跨曝光/新鲜泛化。RS@10 0.792 vs 0.553；CF@10 23/360 vs 29/360。

- **机制修正（本轮最重要发现，FINDINGS §227）**："空响应"实为 **MiniMax-M3 服务端在 thinking 阶段中途截断流**（57/57 有 thinking 无 text 无收尾事件；FR 基线抽查 15/15 同机制）；token 统计为下界。背景截断率 ~3.5% + 05:00–05:25 服务端事件窗（峰值 19.4%）。60 空调用 → 44 被 repair 吸收、16 落 12 个 kept-context 步。
- 路由份额：bounded 66.4% / local 22.0% / bulk 11.1%；kept-context **2.9%**（2026-07-03 旧 val20 campaign：24%）；repair used 99/800；成本 HP 29.4M vs FR 15.1M tokens（1.94×）；`preservation_violations` 全程 0。
- HP 侧损失（全部内容级、执行器无罪）：crystal6 CIF STAR 解析崩塌 RT4→（**C2 注册表缺口：`.cif` 未注册 lint——候选 C2-ext**）、robotics1 RT6 forward 内容摧毁 + backward op 拒绝锁死、spreadsheet1 0.278 平台。
- 纵向声明：与 dev20 同构的两层结论（冻结政策优势主体 = FR 对 provider 病理零护栏；干净子集小幅为正）；val 集 format_conversion 密度更高，bounded 份额相应上移。

### 5.6 v7dev20full（exp_20260711_hybridv7dev20full）—— 部分接受 live 首验：5/5 净非负、动机案例同位救回

`hybridpatch/7`（部分接受提交政策，设计证明 FINDINGS §226）+ thinking，uncapped，dev20 × 10RT × HP 单臂，FR 侧拷贝冻结基线；live 树执行、单指纹。结果复核 **PASS**（400/400，含 5 个 partial 提交经共享资格函数确定性复现——v7 回放首战）。

**配对统计**：backward RS HP **0.888** vs FR **0.726**，Δ**+0.161**（n=200，t=6.58，p<0.001，d=0.47）；CF@10 9/180 vs 13/180；成本 15.58M vs 7.08M tokens。

- **partial acceptance 触发 5/400**（kept-context 6/400，v6 为 9）：3 个 backward 反事实全部净非负——fonteng3 RT7 **+0.158**（动机案例在同一 relay 位置跨 campaign 复发、live 救回，幅度与 §226 扫描预测一致）、latex2 RT7 +0.258、mathlean2 RT4 +0.003（风险形态未现负例）；2 个 forward partial 无观察到 post-partial 劣化；被跳 op 全部为死 op（3 not_found + 2 overlapping_span），零误杀。
- 纵向 v6→v7 均值 0.921→0.888：14/20 持平或改善（protein1 +0.139）；3 个大跌（musicsheet2/starcatalog4/translation4）逐个归因为已知随机内容级类（干净执行断崖、损伤位置无任何政策事件）——政策正交。身份指标不变（bounded 58.5%、gen ratio 0.603）——纯政策消融成立。
- `preservation_violations` 全程 0；空响应（thinking 截断，§227 机制）8/429=1.9% 背景水位。
- 详录 FINDINGS §228。**v7 为当前候选**；后续路径（val/test 集选择或方法冻结）待用户。

### 5.7 OpenCode transport-v2 smoke（exp_20260711_opencode_transport_v2_smoke5）—— SDK 完整性/重试/遥测 E2E 通过

`minimax-m3`，OpenCode Go + Anthropic SDK，adaptive thinking，`max_tokens=131072`，5 样本 × 2RT × 双臂、同 seed/task plan、no distractor。结果复核 **PASS 20/20** backward RS；10/10 checkpoint 到 RT2，`preservation_violations=0`。

- 43 个 API call：HP 23（22 完整 + 1 个 chunked-read 中断后由 dispatcher 换 Key 补完），FR 20（20 个最终完整；其中 1 个 `incomplete_stream` 在**同一调用内**重试成功）。
- 完整但无文本的 FR `max_tokens` 响应被明确标成 `thinking_budget_exhausted`；HP 的文本截断响应标成 `text_truncated` 并由应用层 repair 处理。两者均未按字节长度/token 非零误判为正常正文。
- tokens：HP 1,016,471；FR 723,494。诊断 RS 均值 HP 0.917 vs FR 0.725（n=10，p=0.179），**不作方法结论**：HP 臂运行中修了 retry/logging，run metadata 有 2 个 fingerprint，本 campaign 定位仅为 transport smoke。
- Key canary：11/11 在 `adaptive + max_tokens=16` 下得到完整 thinking-only `max_tokens`（证明 Key/流/usage 有效）；随后 `max_tokens=1024` 的付费 canary 得到真实 Hello/end_turn，仅 204 total tokens，因此正式 probe 默认改为 1024。
- 密钥审计扫描 305 个产物文件、1.139 GB，Key 值命中 0。详细 transport 分类与日志复杂度修复见 §4.10 / FINDINGS §229。

### 5.8 transport-v3 公平性冻结与 E2E smoke

在 v2 live smoke 之后，不再把“连接异常重试”和“生成已经开始后的整次重发”合并计数。v3 以 semantic call 为核算单位，冻结为 2 个 response slot + 3 个 generation 前 transient failure；完整空响应按论文 Baseline 的模型失败语义保留，HP 也不再用 repair 掩盖空/拒答/非协议文本。为防止 worker/Key 轮换造成隐形额外采样，attempt ledger 和 complete-response journal 进入正式运行路径。

先完成 zero-API state-machine、持久化恢复和应用层边界测试，并将 v2 三类真实异常日志脱敏压缩为固定 event fixture；不把旧 v2 结果重新解释为 v3 证据。随后新建 `exp_20260712_transport_v3_smoke5`，HP/FR 同用 revision 3、5 samples×2RT、seed42、no distractor。10/10 tasks 完成，result verification PASS 20/20，41 calls 全部有 journal；live 命中 1 次 generation 后 incomplete→一次 full replay 成功，以及 1 次完整 thinking-only max_tokens→模型失败且无 retry/repair。详规见 `transport/docs/API_TRANSPORT_FROZEN_V3.md`，实现与数字见 FINDINGS §230。

### 5.9 val40 transport-v3（exp_20260712_hybridv7val40_transportv3）—— 传输修好后 val 尺度 HP≈FR，旧优势确认为传输病理放大

`hybridpatch/7` + `fullrewrite` 双臂均在 `opencode_anthropic_sdk/3` 下新跑（依 v3 规范旧 FR 不再作主对照），`minimax-m3` adaptive thinking，40 样本 × 10RT、seed42 冻结 task plan、no distractor，10 slot（KEY_01–10 各 1）+ KEY_11 备用。json4（双臂）与 earncall1（FR）为 transport 预算耗尽的基础设施失败（score=null），规范口径 = 38 完整配对样本。结果复核 **PASS 780/780**（含 11 个 partial 提交确定性重演）。

**配对统计**：Δ+0.008（HP 0.825 vs FR 0.817，n=380，t=0.65，p=0.51，不显著）；ECR Δ−0.001；披露口径（剔 landmarks3/satellite4 两个 FR 病理样本）Δ−0.010。RS@10 0.724 vs 0.705；CF@10 两臂相同 21/342；tokens 26.9M vs 16.1M（1.67×）；`preservation_violations=0`。

- **核心发现**：同 38 样本 FR-v3 vs 冻结基线 **+0.089（p=3.7e-07）**——v3 的 response-slot 重试吸收了 §227 的服务端 thinking 流截断，FR 的零护栏放大被工程性修复；HP 纵向 v6→v7v3 −0.022（n.s.）。v6-era 同集合 Δ+0.119 → +0.008：**方法叙事修正为"优势可兑现性取决于传输可靠性"**（FINDINGS §231）。
- v7 partial 在 val 尺度：11 触发、6 个有分 backward ≥0.94、唯一低分 spreadsheet1 RT5 0.296（v6 同区塌 0.278，复发类，反事实待做）、零误杀迹象。
- 纵向大跌 obj3d2/treebank4/filesystem3 均为干净执行的内容级损毁（评估器解析正常、非 lint 缺口类、无 partial 涉入）；crystal6 本轮全程干净。
- 运维：结果复核曾因缺 `PYTHONUTF8=1` 死于 GBK 解码，`utils_env.load_sample` 已补显式 utf-8（run 后工具修复，不影响归档）。

### 5.10 FR+Official 派生基线（既有官方重跑归档的只读组合）

原 233 样本冻结 FR 保持不变；另新增 **FR+Official**。固定选择规则得到 87 条完整 MiniMax 官方非流式 API 轨迹（83 条 baseline-v2 + 4 条 problem15 补位）和 146 条原冻结 FR 回退轨迹；不拼接 RT、不按分数选源，task plan 字节一致。本次没有新 API 调用，也没有重新运行结果复核。

样本级异常率为：任一异常 **87/233（37.34%）**、异常终止/截断 86/233（36.91%）、空返回 80/233（34.33%）、接近空返回（非空且 `<200` UTF-8 bytes）37/233（15.88%）。官方子集的 85/87 异常率来自预筛问题池，不能当作官方 API 背景率。

整体 FR+Official RS@1/5/10 = 0.735/0.550/0.508。若论文把 FullRewrite baseline
定义为 MiniMax 官方 API、非流式、一次性完整响应照单接受，V7 val40 strict38
应报告 HP 0.825 vs FR+Official 0.564，Δ+0.262（p=2.54e-28，d=0.62），作为
现有论文协议对齐结果。其 strict38 只有 16 条完整官方 FR 链，且替换池事后预筛，
因此必须同步披露混合来源边界；同 transport-v3 方法效应仍由 Δ+0.008 回答。
定义、来源清单和逐样本表见 `Baseline/FR+Official/`，结论详见 FINDINGS §232。

### 5.11 HP_V8 soft-budget smoke 与 paired10 stop（2026-07-17）——链路可审计，方法结论未形成

`hybridpatch/8` 在提交 `e6abad4449f7c2536c914725586fb44094546d76`
上使用 MiniMax-M3、transport-v3、seed42、distractor-on 做了先 smoke 后 paired10 的
预注册诊断。smoke 为 2 个已曝光样本 × 2RT × 双臂；paired10 为用户固定的 10 个
已曝光样本 × 10RT × 双臂。两个阶段使用同一 task-plan/config 身份。

**smoke 完整性**：16/16 行提交，17 calls（含一次 HP repair），verifier PASS 8/8，
preservation 0。exact RT2 为 HP 0.500 vs FR 0.998，delta -0.498（n=2）；HP/FR
tokens 426,989/316,543，费用 USD 0.388963/0.276923。HP 8/8 均选择 bounded，
repair 1/8，soft burden 0/8。因此它证明链路、repair 个例、记录和重放，不证明
local/bulk/DSL、软超限或效果优势。独立完整性审阅与正式 prepare/finalize 均 PASS；
record/catalog/index 快照仅收入私有 ZIP，避免提交时重写冻结版本 overlay。

`obj3d2` HP RT2 的 0 分不是单一归因：全局声明 OBJ 数组触发 evaluator 的局部
group-layout 敏感性，夸大几何损伤；同时生成文件确有 texcoord 数量和 face-corner UV
错配。treebank4 的唯一 repair 则是缺 JSON closing fence，body 保持相同但需要完整
二次响应，额外 15,874 tokens / 约 USD 0.01519。两例详见 smoke 私有归档中的
`analysis/failure_analysis.md` 与 HP_V8 版本卡。

**paired10 停止**：在 192/400 行后，musicsheet2 HP RT2 backward 遇到 provider
`Streaming response failed`，冻结 ledger 将调用写为 terminal `call_failed`，
dispatcher fail-fast 回收全体。verifier 仍 PASS 96/96，preservation 0，行/checkpoint/
API locator 完整。KEY_11 不能解除 terminal；无进展 resume 未执行。formal analyzer
拒绝 n=1/10 endpoint，因此没有 10 样本 RS@10/20-edit-step 结果。部分 usage 和失败
统计仅作为 `failed_informative` 诊断，见版本卡与 failure summary，不进入方法结论。

这轮的结论边界是：性能优先的软 burden 实现没有因阈值阻断已观察输出，且完整路径
在 prompt/schema 层存在；但真实 smoke 未覆盖三条非 bounded 路径，主 campaign 又不
完整，故唯一可靠 live 声明仍是 chain integrity、可重放 provenance 与 preservation=0。

### 5.12 HP_V8 transport-v4 snapshotfix paired10（2026-07-18）——断电续未提交后缀，完整诊断 endpoint

提交 `bc4a39f7dd1476848927d45bcf1840a74677e6be` 上的 snapshotfix smoke 通过后，
同提交 paired10 使用 MiniMax-M3、transport-v4、seed42、distractor-on 和固定 task plans。
主机在 362/400 行时断电；7 个完整样本保持不动，只从 filesystem3、musicsheet2、
satellite4 的 checkpoint 后续跑。三条 host-loss attempt 已开始生成，按 response-slot
语义显式闭合；旧 raw 和 committed prefix 不改写。satellite4 g000 二次不完整后先按样本
隔离，再用唯一 g001 完成。最终 400/400、20 个 RT10 checkpoint、10 个 latest finished；
verifier PASS 200/200，strict inspector 无错误，preservation 0/199 applicable + 1 N/A。

**配对统计**：exact backward RT10 HP **0.745178** vs FR **0.695854**，delta
**+0.049324**（n=10，sample SD 0.344047，4 正/4 负/2 平）；CF@0.10 为 4/90 vs
8/90。HP routes bounded/local/bulk/DSL=124/54/14/7，另 1 个空响应无 route；repair
32 次、成功采用 21 次；protocol kept-context 11/200；soft burden 12/200；所有超限
均继续执行。

已知 final usage 为 HP 8.448M tokens / USD 7.036488、FR 6.661M / USD 5.571257；
10 个已生成但中断的 attempt 无 final usage，精确账单未知。样本全已曝光且差值异质；
该结果只作 diagnostic，不证明 HP 普遍优于 FR，也不把随机 provider 得分变化归因于
snapshot fix。恢复机制、raw/ledger hash 和私有归档见 HP_V8 脱敏诊断报告与版本卡。

---

## 6. 追加约定（怎么继续写这份文档）

每个新 campaign 跑完后，在 §5 追加一小节，模板：

```markdown
### 5.N <slug>（exp_<YYYYMMDD>_<slug>）—— 一句话定性

协议/模型/条件。结果复核 **PASS|FAIL**（数量；FAIL 必须写原因与处置）。

**配对统计**：…（Δ / n / t / d）
| RS@10 | HybridPatch | FullRewrite |（分样本表）

- 关键变化与归因（新 bug 记入 §4 编号条目，此处引用）
- 路由份额 / 成本 / preservation_violations
- 与上一轮的纵向对比及混杂因素声明
```

新 bug 记入 §4（现象/根因/修复/教训四段式），协议语义变化更新 §2.7 的版本表。数字先过结果复核，详细流水交叉引用 `docs/FINDINGS.md` 的编号条目。

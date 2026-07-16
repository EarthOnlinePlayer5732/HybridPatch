# 03 · 当前实验结果预览

> **论文报告必须并列区分两个口径。**
>
> 1. 同 transport-v3 的受控方法效应：V7 strict38 为 `Δ=+0.008`。
> 2. 如果论文把 FullRewrite baseline 的处理流程定义为 **MiniMax 官方 API、非流式、一次性完整响应照单接受**，则论文协议对应的现有结果是 **FR+Official strict38，`Δ=+0.262`**。
>
> 第二项是论文处理协议对齐结果，但现有构建只在 strict38 中替换了 16 条完整官方 FR 链，其余链沿用冻结 FR，因此仍须同步披露“混合来源、事后预筛”的效度限制；不能把它误写成 38 个样本全部完成官方 API 同条件重跑。

## 1. 实验设置

模型为 MiniMax-M3 adaptive thinking。HybridPatch/7 与 FullRewrite 均使用冻结的 OpenCode transport revision `opencode_anthropic_sdk/3`，共享 seed 42 task plan，10RT，no distractor。

val40 共计划 40 个样本。`json4` 双臂停在 RT2，`earncall1` FR 停在 RT6，均为 transport 预算耗尽的基础设施失败；分数保持 `null`。canonical scope 排除这两个样本，固定为 **38 个完整配对样本、380 对 backward RS**。已提交 partial histories 仍保留在归档中。

## 2. 同 transport-v3 受控对照：V7 strict38

| 口径 | HybridPatch | FullRewrite | Δ (HP−FR) | 统计 |
|---|---:|---:|---:|---:|
| strict38，380 对 backward | **0.825442** | **0.816957** | **+0.008484** | t=0.654，p=0.514，Cohen d=0.034 |

- 结论：差异不显著；clean transport 下现有 val 证据不支持“HybridPatch 整体 RS 显著优于 FullRewrite”的强主张。
- RS@1/5/10：HP 0.975/0.831/0.724，FR 0.941/0.822/0.705。
- CriticalFailure@10：两臂完全相同，均为 **21/342**。
- 成本：HP 26.88M tokens，FR 16.09M，HP 为 **1.67×**。
- `preservation_violations=0`；这是 HybridPatch 仍然成立的确定性字节级不变量。
- 全归档结果复核 **PASS 780/780**，包括两个排除样本的已提交 partial histories，以及 11 个 V7 partial-acceptance 事件的确定性重演。

同 38 样本中，FR-v3 相比原冻结 FR 提高 **+0.089**（p=3.7e-07）。这说明早期 headline 优势的主体来自不完整 thinking 流被 FR 零护栏放大；transport-v3 的完整性判断与 response-slot 重发显著修复了 FR。完整解释见 [`FINDINGS.md` §231](../FINDINGS.md) 与 [`RESEARCH_JOURNAL.md` §5.9](../RESEARCH_JOURNAL.md)。

## 3. HybridPatch 内部遥测

| 指标 | V7 strict38 |
|---|---:|
| bounded_rewrite | 473/760（62.2%） |
| local_patch | 186/760（24.5%） |
| bulk_patch | 93/760（12.2%） |
| protocol-failure kept route | 8/760（1.1%） |
| partial acceptance | 11/760（1.4%） |
| kept-context commit | 14/760（1.8%） |
| repair | attempted 44；used/success 27 |

bounded_rewrite 仍占多数，因此论文不能把 HybridPatch 描述为“从不重写”。更准确的表述是：模型选择四条显式路径之一，执行器限制写入范围、复制未声明来源字节，并让失败与部分接受可审计。

## 4. 效度与叙事边界

1. strict38 排除了两个基础设施失败样本；排除规则在实验结束前由 transport 完整性语义机械决定，不把缺失行记为模型 0 分。
2. 当前证据只有一个模型、一个 seed，且 val40 包含已曝光 val20 与此前未用于 HP 的 unused-reserve20；第二 seed、第二模型和 sealed test 尚未执行。
3. HP 比 FR 贵 1.67×。任何结构性安全收益都必须与成本和 clean-transport RS 打平同时披露。
4. V7 partial acceptance 未观察到误杀信号，但 `spreadsheet1` RT5 的 kept-context 反事实及 5 个 forward partial 的链式影响仍是离线 open item。
5. 可保留的结论是 preservation 不变量、失败显式、病理环境韧性和 dev20 阶段的正向证据；这些不能替代 strict38 的主统计结论。

## 5. 论文非流式一次性处理口径：FR+Official

如果论文将 FullRewrite baseline 明确定义为“MiniMax 官方 API、非流式、一次性完整响应照单接受”的处理流程，则本节是论文协议对应的结果入口，strict38 应报告 **`Δ=+0.262`**。FR+Official 保持 233 样本范围，用完整 MiniMax 官方 API 轨迹替换其中 87 个样本：83 个来自 `frbaseline_v2_rerun`，4 个来自 `problem15_official`；其余 146 个样本继续引用原冻结 FR。组合不拼接 RT，也不按分数选源。

按样本计，FR+Official 中任一异常为 **87/233（37.34%）**；异常终止/截断 **86/233（36.91%）**、空返回 **80/233（34.33%）**、近空返回 **37/233（15.88%）**。官方重跑池按历史异常/问题样本预筛，因此这些比例不能解释为 MiniMax 官方 API 的背景异常率。

strict38 敏感性口径：

| 口径 | HybridPatch | FR+Official | Δ | 统计 |
|---|---:|---:|---:|---:|
| 论文协议对齐 strict38 | 0.825442 | 0.563780 | **+0.261662** | p=2.54e-28，Cohen d=0.62 |

论文中可以把 `+0.262` 表述为“按所声明的官方非流式一次性 baseline 处理政策得到的现有协议对齐结果”。同时必须披露：strict38 仅 16 条 FR 链实际来自完整官方重跑，其余沿用冻结 FR，且替换池经过事后问题样本预筛。因此它不是全样本同条件重跑得到的无偏方法效应；`+0.008` 仍是回答“同 transport-v3 下方法本身差异”的受控结果。

完整定义与来源见 [FR+Official 说明](../../Baseline/FR+Official/README.md)、[val40 敏感性对比](../../Baseline/FR+Official/val40_comparison.md) 和 [`FINDINGS.md` §232](../FINDINGS.md)。

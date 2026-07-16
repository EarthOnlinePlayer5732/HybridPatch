# FR+Official

`FR+Official` 是在原冻结 FR 之外新增的派生基线版本，不覆盖、重写或删除
`exp_20260710_frbaseline234`。

本目录采用 manifest 逻辑视图，不复制源 JSONL；`source_manifest.csv` 为每个
样本指定唯一源文件及 SHA-256。这样既能复现组合，又不会制造第三份可被误改的
实验归档。

## 组成

- 总范围：233 个样本 × 10RT。
- 87 个样本采用完整的 MiniMax 官方非流式 API 轨迹：83 个来自
  `exp_20260713_frbaseline_v2_rerun`，另有 4 个在该目录无完整轨迹、由
  `exp_20260713_problem15_official` 补齐（`crystal6`、`filesystem3`、
  `obj3d2`、`robotics1`）。
- 其余 146 个样本保留原冻结 FR。任何样本都只选择一条完整 10RT 轨迹；
  不按 RT 拼接，也不按分数选择来源。
- 所有被选官方轨迹的 task plan 与原冻结基线逐字节一致。
- 官方链配置与论文 baseline 对齐口径：MiniMax 官方
  `/v1/chat/completions` 非流式接口，transport revision
  `minimax_official_nonstream/1`，`minimax-m3` adaptive thinking，
  `max_tokens=131072`，seed 42，10RT，no distractor。

这个版本用于披露“按论文 baseline 的官方 API 一次性语义”对结果的影响。
它仍是混合来源：只有已有完整官方重跑的样本被替换，不能表述成 233 个
样本全部经官方 API 重跑。

## 论文报告口径

如果论文将 FullRewrite baseline 的处理流程定义为 **MiniMax 官方 API、非流式、
一次性完整响应照单接受**，则现有 strict38 论文协议对齐结果为：

| 口径 | HybridPatch | FR+Official | Δ HP−FR | p | Cohen d |
|---|---:|---:|---:|---:|---:|
| strict38 paper protocol | 0.825442 | 0.563780 | **+0.261662** | 2.54e-28 | 0.615 |

论文可按该处理政策报告 `+0.262`。同时必须披露：strict38 仅 16 条
FullRewrite 链实际来自完整官方非流式重跑，其余沿用冻结 FR；官方替换池来自
事后异常/问题样本预筛。因此这是现有论文协议对齐结果，不是 38 个样本全部
官方同条件重跑得到的无偏方法效应。回答同 transport-v3 下的方法效应时，
对应结果仍为 `+0.008`。

## 异常定义与样本率

- 异常终止/截断：`finish_reason` 为 null、`abort`、`length` 或
  `max_tokens`，或分类为相应截断类型。
- 空返回：`raw_llm_response.strip()` 为空，或分类为模型空返回。
- 接近空返回：非空，但 UTF-8 正文少于 200 bytes。
- “任一异常”是上述三类的并集；各分类可以重叠。

| 范围 | 任一异常 | 异常终止/截断 | 空返回 | 接近空返回 |
|---|---:|---:|---:|---:|
| **FR+Official（233）** | **87/233 (37.34%)** | 86/233 (36.91%) | 80/233 (34.33%) | 37/233 (15.88%) |
| 被选官方轨迹（87） | **85/87 (97.70%)** | 85/87 (97.70%) | 79/87 (90.80%) | 35/87 (40.23%) |
| 原冻结 FR（同 233） | 71/233 (30.47%) | 68/233 (29.18%) | 57/233 (24.46%) | 20/233 (8.58%) |

官方重跑池原本就是按旧链异常/问题样本筛选的，因此 87 个官方轨迹的异常率
不能解释为 MiniMax 官方 API 的总体背景率。FR+Official 的总体样本异常率是
本派生版本可报告的数字。

## 整体曲线

| 版本 | RS@1 | RS@5 | RS@10 | CriticalFailure@10 | tokens |
|---|---:|---:|---:|---:|---:|
| 原冻结 FR | 0.925 | 0.682 | 0.558 | 167/2097 | 85,874,973 |
| **FR+Official** | **0.735** | **0.550** | **0.508** | **162/2097** | **71,420,901** |

逐样本来源与哈希见 `source_manifest.csv`；异常明细见
`anomaly_by_sample.csv`；机器可读汇总见 `summary.json`。

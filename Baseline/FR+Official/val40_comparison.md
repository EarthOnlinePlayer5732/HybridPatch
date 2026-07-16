# V7 val40 × FR+Official

如果论文把 FullRewrite baseline 定义为 MiniMax 官方 API、非流式、一次性完整
响应照单接受，则本文件的 strict38 `Δ=+0.262` 是现有论文协议对齐结果。
它不修改原 val40 归档。官方重跑样本按旧异常/问题筛选，且 strict38 仅
16 条 FR 链来自完整官方重跑，因此必须同时披露混合来源与选择偏差。
同 transport-v3 的受控方法效应仍由 `Δ=+0.008` 回答。

## 配对结果

| 口径 | n | HP | FR | delta | p | d | HP CF | FR CF | HP/FR tokens |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 原 canonical38 | 380 | 0.825 | 0.817 | 0.008 | 0.514 | 0.03 | 21/342 | 21/342 | 1.67x |
| FR+Official strict38 | 380 | 0.825 | 0.564 | 0.262 | 2.54e-28 | 0.62 | 21/342 | 26/342 | 2.38x |
| FR+Official maximal39 | 390 | 0.829 | 0.551 | 0.279 | 3.26e-31 | 0.64 | 21/351 | 28/351 | 2.48x |

- strict38 保持原 canonical 38 样本范围，替换 16
  个完整官方 FR 轨迹。
- maximal39 额外纳入 `earncall1`：其旧 FR-v3 只到 RT6，而官方轨迹完整
  10RT；`json4` 仍因 HP 侧不完整而排除。
- strict38 RS@1/5/10：HP 0.975/0.831/0.724
  vs FR+Official 0.686/0.555/0.519。
- strict38 ECR-only：n=339，delta=0.210，
  p=1.48e-19。

## 被替换样本

| sample | official source | HP mean | old FR mean | FR+Official mean | official-old |
|---|---|---:|---:|---:|---:|
| crystal6 | official_problem15_fill | 0.299 | 0.300 | 0.136 | -0.164 |
| earncall1 | official_baseline_v2 | 0.970 | 0.880 | 0.047 | -0.832 |
| emails5 | official_baseline_v2 | 0.872 | 0.785 | 0.189 | -0.596 |
| filesystem3 | official_problem15_fill | 0.660 | 0.986 | 0.000 | -0.986 |
| geodata1 | official_baseline_v2 | 0.909 | 0.810 | 0.100 | -0.710 |
| geodata4 | official_baseline_v2 | 1.000 | 1.000 | 0.000 | -1.000 |
| geotrack5 | official_baseline_v2 | 0.560 | 0.555 | 0.099 | -0.456 |
| geotrack6 | official_baseline_v2 | 0.900 | 0.965 | 0.192 | -0.773 |
| jobboard3 | official_baseline_v2 | 0.251 | 0.395 | 0.000 | -0.395 |
| landmarks3 | official_baseline_v2 | 0.865 | 0.900 | 0.000 | -0.900 |
| obj3d2 | official_problem15_fill | 0.500 | 0.229 | 0.100 | -0.129 |
| obj3d5 | official_baseline_v2 | 0.759 | 0.674 | 0.000 | -0.674 |
| quantum1 | official_baseline_v2 | 0.972 | 0.986 | 0.138 | -0.848 |
| robotics1 | official_problem15_fill | 1.000 | 1.000 | 0.070 | -0.930 |
| satellite4 | official_baseline_v2 | 0.918 | 0.200 | 0.000 | -0.200 |
| screenplay5 | official_baseline_v2 | 0.993 | 0.978 | 0.848 | -0.130 |
| treebank4 | official_baseline_v2 | 0.500 | 0.735 | 0.004 | -0.731 |

论文采用上述非流式一次性 baseline 处理流程时报告 `delta=+0.262`；但必须与
“16/38 官方链、其余冻结 FR、替换池事后预筛”的效度披露绑定，不能单独解释为
全样本同条件重跑得到的无偏方法效应。

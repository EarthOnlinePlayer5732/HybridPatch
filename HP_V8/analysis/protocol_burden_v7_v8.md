# HP_V8 零 API 协议与提示负担报告

本报告只做确定性离线重算与提示重放；未调用任何模型或 provider。

## 数据口径

- 来源：`HP_V7/exp_20260711_hybridv7dev20full`。
- 总行数：400；最终提交 HybridPatch：394；kept-context：6。
- 成功子集包含 5 个 V7 partial acceptance；repair 尝试共 36 步。
- 成功定义：`bdpatch.actual_method == "hybridpatch"`，并逐条断言 chosen envelope 为 `hybridpatch/7`。
- route：bounded_rewrite=233，local_patch=116，bulk_patch=44，dsl_rules=1。
- 分位数：nearest-rank，`sorted_values[ceil(p*n)-1]`。

## V7 成功信封分布与经验软阈值

| 指标 | n | p50 | p90 | p95 | p99 | max | V8 经验阈值 | 超阈值行数 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `local_op_count` | 116 | 3 | 12 | 31 | 66 | 92 | 31 | 4 |
| `bulk_op_count` | 44 | 5 | 23 | 30 | 32 | 32 | 30 | 1 |
| `anchor_bytes` | 160 | 531 | 3490 | 4080 | 16153 | 17512 | 4080 | 8 |
| `envelope_bytes` | 394 | 766 | 1937 | 2898 | 8366 | 20109 | 2898 | 19 |
| `explicit_block_id_count` | 1 | 39 | 39 | 39 | 39 | 39 | 39 | 0 |

五项经验阈值的并集标记了 25/394 （6.35%）个历史成功信封。阈值现在只用于离线分析和 telemetry；超阈值信封仍完整执行，不触发 schema 拒绝、repair、截断或强制换路。

## 零 API 路径兼容审计

本节只检查历史 V7 chosen route 是否出现在同一步反事实 V8 prompt profile 的 allowed routes 中；它不判断路径语义等效，不预测模型在 V8 下会选择哪条路径，也不是效果实验。

- V8 profile 分布：default=184，block_movement=216。
- default allowed routes：local_patch, bulk_patch, bounded_rewrite；block_movement allowed routes：local_patch, bulk_patch, dsl_rules, bounded_rewrite。
- 399/400 步有可解析 chosen route；1 步 route unavailable，单列且不进入不兼容分母。
- 不兼容：1/399 （0.25%）；按全部 400 步为 0.25%。
- 成功提交子集：1/394 不兼容（0.25%）。
- 不兼容 route：{"dsl_rules": 1}；方向：{"forward": 1}。

### 按 matched term 的不兼容计数

同一步可命中多个 term，因此下表行数可重叠，不能相加作为不兼容总数。

| family | matched term | role | row hits | occurrences |
|---|---|---|---:|---:|
| default | <none> | - | 1 | 1 |

### 代表性不兼容案例

每个 matched term 选取稳定排序后的首个案例；完整 400 步逐步结果保存在 JSON 报告的 `path_compatibility_audit.step_results`。

| matched term | sample | RT | direction | profile | V7 chosen route | V8 allowed routes |
|---|---|---:|---|---|---|---|
| default:<none> | makefile4 | 2 | forward | default | dsl_rules | local_patch, bulk_patch, bounded_rewrite |

Route unavailable 案例：satellite6 RT9 backward，profile=default，actual_method=hybridpatch_protocol_failure_kept_context。

## V7/V8 提示字符数

| 比较 | n | V7 mean | V7 p50 | V7 p95 | V8 mean | V8 p50 | V8 p95 | 总变化 | 相对变化 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| primary 全量配对 | 400 | 27459.715 | 23122 | 45873 | 20662.02 | 16353 | 42083 | -2719078 | -24.755% |
| actual V7 repair vs counterfactual V8 repair | 36 | 20009.222 | 19513 | 29540 | 16044.667 | 15100 | 30144 | -142724 | -19.814% |

V7 长度来自归档中的实际相对 `*.request.json`；V8 primary 使用同一 400 步的任务、editable、readonly 和 target 上下文离线重建。repair 比较只覆盖 V7 实际发生 repair 的步骤。

## 局限

- Prompt and envelope character counts are not tokenizer-measured tokens.
- The source is one frozen dev20 campaign: 20 samples, one model, one seed, and sequential round trips; observations are not independent.
- The burden distribution selects finally chosen and committed envelopes, so it has survivorship bias and cannot establish that high burden causes protocol failure.
- Chosen attempts mix primary and repair outputs; the distribution is not a distribution of all model responses.
- Routes are highly imbalanced and DSL has only one successful envelope, so its 39-ID ceiling is provisional rather than statistically stable.
- Canonical envelope size excludes sidecar FILE BODIES and therefore is not total completion size.
- The empirical P95 burden thresholds are soft analysis and telemetry markers only; crossing them does not reject, truncate, repair, or reroute an envelope.
- The path-compatibility audit only checks whether an archived V7 chosen route is listed by the counterfactual V8 prompt profile; it does not establish route equivalence or predict the route a model would choose under V8.
- The zero-API replay cannot demonstrate score retention, token reduction, or protocol-failure-rate improvement; those require a controlled API experiment.

# HP_V8 零 API 协议与提示负担报告

本报告只做确定性离线重算与提示重放；未调用任何模型或 provider。

## 数据口径

- 来源：`HP_V7/exp_20260711_hybridv7dev20full`。
- 总行数：400；最终提交 HybridPatch：394；kept-context：6。
- 成功子集包含 5 个 V7 partial acceptance；repair 尝试共 36 步。
- 成功定义：`bdpatch.actual_method == "hybridpatch"`，并逐条断言 chosen envelope 为 `hybridpatch/7`。
- route：bounded_rewrite=233，local_patch=116，bulk_patch=44，dsl_rules=1。
- 分位数：nearest-rank，`sorted_values[ceil(p*n)-1]`。

## V7 成功信封分布与冻结阈值

| 指标 | n | p50 | p90 | p95 | p99 | max | V8 上限 | 超限行数 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `local_op_count` | 116 | 3 | 12 | 31 | 66 | 92 | 31 | 4 |
| `bulk_op_count` | 44 | 5 | 23 | 30 | 32 | 32 | 30 | 1 |
| `anchor_bytes` | 160 | 531 | 3490 | 4080 | 16153 | 17512 | 4080 | 8 |
| `envelope_bytes` | 394 | 766 | 1937 | 2898 | 8366 | 20109 | 2898 | 19 |
| `explicit_block_id_count` | 1 | 39 | 39 | 39 | 39 | 39 | 39 | 0 |

五项阈值的并集会要求 25/394 （6.35%）个历史成功形态合并重复操作或改用更合适路径；阈值等值放行，只有严格大于才拒绝。

## V7/V8 提示字符数

| 比较 | n | V7 mean | V7 p50 | V7 p95 | V8 mean | V8 p50 | V8 p95 | 总变化 | 相对变化 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| primary 全量配对 | 400 | 27459.715 | 23122 | 45873 | 20079.9 | 15917 | 41005 | -2951926 | -26.875% |
| actual V7 repair vs counterfactual V8 repair | 36 | 20009.222 | 19513 | 29540 | 13119.139 | 13958 | 20817 | -248043 | -34.435% |

V7 长度来自归档中的实际相对 `*.request.json`；V8 primary 使用同一 400 步的任务、editable、readonly 和 target 上下文离线重建。repair 比较只覆盖 V7 实际发生 repair 的步骤。

## 局限

- Prompt and envelope character counts are not tokenizer-measured tokens.
- The source is one frozen dev20 campaign: 20 samples, one model, one seed, and sequential round trips; observations are not independent.
- The burden distribution selects finally chosen and committed envelopes, so it has survivorship bias and cannot establish that high burden causes protocol failure.
- Chosen attempts mix primary and repair outputs; the distribution is not a distribution of all model responses.
- Routes are highly imbalanced and DSL has only one successful envelope, so its 39-ID ceiling is provisional rather than statistically stable.
- Canonical envelope size excludes sidecar FILE BODIES and therefore is not total completion size.
- The zero-API replay cannot demonstrate score retention, token reduction, or protocol-failure-rate improvement; those require a controlled API experiment.

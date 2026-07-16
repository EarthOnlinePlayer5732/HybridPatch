# Transport-v3 × HybridPatch/7：Claude Code 交接说明

> [!WARNING]
> **HISTORICAL SNAPSHOT — DO NOT TREAT “下一步实验口径” AS A PENDING TASK.** 本文保存 transport-v3 冻结时的实现交接；随后 V7 val40 双臂实验已按 strict38 收官。当前实验状态见 [EXPERIMENT_INDEX.md](../../docs/EXPERIMENT_INDEX.md)，结果解释见 [FINDINGS.md](../../docs/FINDINGS.md) §231–§232。

这份文档可以直接转发给 Claude Code。当前主工作区已经把经过 live smoke 验证的 OpenCode Go transport-v3 精确合并到冻结的 HybridPatch/7；没有回退或改写 V7 executor、gate、schema、partial-acceptance 语义。

## 先读与不可变约束

开始工作前依次阅读：

1. `AGENTS.md`
2. `CLAUDE.md`
3. `docs/API_TRANSPORT_FROZEN_V3.md`
4. 本文

不可变约束：

- 唯一 provider 是 OpenCode Go，不接 MiniMax official endpoint。
- Anthropic SDK 固定 `anthropic==0.104.1`，`base_url=https://opencode.ai/zen/go`，最终 `POST /v1/messages`。
- 模型 `minimax-m3`，所有 HP primary/repair、FR、Key probe 都是 adaptive thinking。
- `max_tokens` 默认及硬上限为 `131072`；Key probe 默认 `1024`。
- 正式 transport 名仍是 `anthropic_sdk_v2`（配置兼容名），实现 revision 必须是 `opencode_anthropic_sdk/3`。
- 不实现 partial continuation，不把中断流的 thinking/text 放入下一次 prompt。
- 新 v3 实验必须使用全新 `out_dir`；不得续跑或混合 v2 目录。

## 为什么改

旧 transport-v2 把所有异常统一压成“最多 3 个总 attempt”，无法区分：

- generation 之前的 429/504/连接失败；
- thinking/text 已经开始后的流中断；
- 协议完整但模型没有 text 的真实 empty response。

dispatcher 换 Key 或 worker 重启还可能刷新进程内 retry 预算，造成额外抽样和不公平计费。v3 把传输完整性、重发预算和模型失败明确拆开，并将预算持久化到运行目录。

## 完整性状态机

一次响应只有同时满足下列条件才可提交：

- `message_start` 已收到；
- 每个 started content block 都 stopped；
- `message_delta` 已收到；
- `message_stop` 已收到；
- final usage 中 `input_tokens`、`output_tokens` 可读；
- `stop_reason` 非空；
- 没有 SDK/HTTP/error event。

缺任一项都是 `incomplete_stream`。partial thinking/text 只保留在 raw audit，绝不进入 context、repair 或评分。

## 双预算规则

预算单位是稳定的 `semantic_call_id = method/sample/rt/direction/call_kind`：

| 预算 | 上限 | 何时消耗 |
|---|---:|---|
| response slot | 2 | 完整响应；或已见 generation delta 后中断 |
| transient failure | 3 | 首个 generation delta 前、已确认结束的 408/409/429/5xx/DNS/TLS/connect/timeout |

response slot 2 表示“首次生成 + 最多一次完整新 POST”，不是续传。两个 slot 都中断后停止；第三次 POST 被禁止。

预算或 fatal transport 耗尽时，runner 在提交该步 JSONL 行之前终止，terminal evidence 保存在 `api_calls.jsonl` / ledger；覆盖率和分析应把未提交步骤视为基础设施 `score=null`，而不是模型 0 分。当前实现不会伪造一条 synthetic score row。

watchdog 超时时旧 POST 可能仍在飞，代码会关闭 SDK client 并记 `watchdog_ambiguous_inflight`，不发重叠请求。

## 空响应与 repair

完整响应分类：

- text + `end_turn`：正常；
- text + `max_tokens`：`text_truncated`，不 transport retry，交给应用层验证；
- 无 text + `max_tokens`：`thinking_budget_exhausted`；
- 无 text + `end_turn`：`model_empty`；
- refusal/非协议纯文本：模型行为失败。

HP 只有在响应非空、存在明确 patch 协议信号、但 JSON/schema/op/route/gate 失败时，才允许一次 `hybridpatch_repair`。完整空、thinking-only、refusal、普通说明文字都不 repair。FR 始终不做 semantic repair。V7 partial acceptance 仍在这次 repair 机会之后按原规则执行。

## 崩溃恢复与产物

关键产物：

- `api_calls.jsonl`：schema `anchorpatch.api_call/3`；
- `api_attempt_ledger.jsonl`：attempt start/progress/end、预算归属和 terminal state；
- `api_journal/<hash>.response.json`：完整响应的原子 journal；
- `api_raw/...transport.jsonl`：脱敏 protocol-shaped stream audit；
- `run_metadata.jsonl`：revision、SDK、base URL、thinking、max_tokens、fingerprint。

完整响应先写 journal，再返回 runner。checkpoint 前 worker 崩溃时，新 worker 回放 journal，`provider_called=false`、`response_replayed=true`，不会重新计费。generation 中途崩溃则从 ledger 恢复已经消耗的预算；dispatcher requeue/换 Key 不能刷新额度。

## 主要代码位置

- `src/model_openai.py`：Anthropic stream 状态机、双预算、watchdog、防重叠 POST。
- `src/run_meta.py`：semantic IDs、attempt ledger、response journal、API schema v3。
- `src/experiment_runner.py`：empty/non-protocol 不 repair；V7 主逻辑保持不变。
- `src/fr_baseline_dispatch.py`、`src/launch_pipeline.py`：稳定 `worker_launch_id` 注入。
- `src/monitor_experiment.py`：显示 `R<used>/2 I<used>/3`。
- `src/probe_fr_keys.py`：逐 Key 显示完整性、stop reason、预算和 tokens。
- `src/test_model_openai.py`：zero-API transport/ledger/repair 回归。
- `src/test_fixtures/opencode_transport_v2_anomalies.json`：从真实 v2 异常归档提取的脱敏 event fixture。

## 已完成验证

- transport/ledger/repair tests：26/26 PASS；
- HybridPatch/7 executor/gate tests：49/49 PASS；
- splitter：10 个真实文档、0 coverage violation；
- 11-Key live Hello：11/11 完整 `end_turn`，合计 2,209 tokens；
- `exp_20260712_transport_v3_smoke5`：5 samples × 2RT × HP/FR，10/10 tasks，result verification 20/20，preservation 0；
- 41 semantic calls / 42 HTTP attempts / 41 journals；密钥与认证头扫描 0 命中。

该 smoke 的逻辑 artifact id 为 `exp_20260712_transport_v3_smoke5`。原始只读归档位于仓库外的私有历史工作区，**不随本仓库分发**；不得根据本文猜测本机绝对路径，也不得在任何同名旧目录续跑或覆盖。

live smoke 命中的关键案例：

- protein1 HP RT1 backward：attempt1 generation 后 `incomplete_stream`，消耗 slot1；一次 full replay POST 完整成功，最终 R2/2。
- translation4 HP RT1 forward：完整 thinking-only `max_tokens`，output_tokens=131072、text=0；记 `thinking_budget_exhausted` 模型失败，不 retry、不 repair。

历史 fixture 还覆盖：foodmenu6 thinking 590 delta 后 EOF、translation4 text+max_tokens，以及 FR thinking EOF 后第二 POST thinking-only max_tokens。

## 验证命令（PowerShell，仓库根执行）

```powershell
$env:PYTHONUTF8 = "1"; python src/test_model_openai.py
$env:PYTHONUTF8 = "1"; python src/test_hybrid_executor.py
$env:PYTHONUTF8 = "1"; python src/splitters.py
python src/probe_fr_keys.py --max_tokens 1024
python src/monitor_experiment.py --dir exp_<slug> --methods hybridpatch fullrewrite --watch
python src/verify_anchorpatch.py --dir exp_<slug>
```

Key probe 和正式实验会产生费用；前三项是 zero-API。

## 下一步实验口径

- 不必立刻重跑 234 样本 FR。
- 正式比较使用哪个集合，就为该集合冻结一份 transport-v3 FR；同条件的后续 HP 版本可复用它。
- 新 HP-v3 不能把旧 transport 的 FR 作为主要公平对照。旧 FR 只作历史/敏感性分析。
- 建议先跑同 task plans/seed 的 dev20 FR-v3，再决定 val/test；只有要替换 234 全量冻结基线时才重跑全部 FR。
- 任何正式数字先过 `verify_anchorpatch.py`，并同时报告 response retry、transient failure、journal replay、empty response、repair 和总 token。

## 禁止事项

- 不把 full replay 写成“续传成功”。
- 不按 `<200B`、output token 非零或“出现 thinking”判断传输成功。
- 不把完整 empty response 当基础设施异常重跑。
- 不让 HP repair 掩盖空响应或 refusal。
- 不删除/覆盖旧实验行、checkpoint、raw log 或 journal。
- 不在同一 `out_dir` 混用不同 transport revision 或 fingerprint。

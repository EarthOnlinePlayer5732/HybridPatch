# OpenCode Go MiniMax-M3 transport-v3 冻结规范

状态：**冻结**。本文件是新实验的规范来源；历史 transport-v2 产物只用于追溯，不得与 v3 混跑或直接配对比较。

## 1. 固定运行面

- 唯一 provider：OpenCode Go。不得切到 MiniMax official provider。
- Python SDK：`anthropic==0.104.1`。
- `base_url=https://opencode.ai/zen/go`，最终请求 `POST /v1/messages`。
- 模型：`minimax-m3`。
- HP primary、HP repair、FR primary、Key probe 全部发送 `thinking={"type":"adaptive"}`。
- MiniMax-M3 默认及硬上限 `max_tokens=131072`；显式 `1..131072` 原样使用，`None/0` 映射为 `131072`，超限立即报配置错误。Key probe 默认显式使用 `1024`。
- Anthropic SDK 自带 retry 固定为 `0`，全部 retry 由项目记录和控制。
- 正式实验只允许 `OPENCODE_TRANSPORT=anthropic_sdk_v2`；实现 revision 为 `opencode_anthropic_sdk/3`。`urllib_v1` 仅诊断，不得用于正式结果。

## 2. 一次响应何时完成

只有同时满足下列条件，响应才可提交：

1. 收到 `message_start`；
2. 每个已开始的 content block 都收到对应 `content_block_stop`（零 content block 的完整空消息允许）；
3. 收到 `message_delta`；
4. 收到 `message_stop`；
5. `get_final_message()` 给出 final usage，且 `input_tokens`、`output_tokens` 可读；
6. `stop_reason` 非空；
7. 流中没有 error event 或 SDK/HTTP 异常。

EOF、缺终止事件、block 不平衡或缺 final usage 一律为 `incomplete_stream`。该 attempt 中的 partial thinking/text 只留在 raw audit，绝不进入上下文、repair prompt 或评分。

## 3. 两类预算，不混为一个 retry 数

每个 `semantic_call_id` 有两套独立且跨进程持久化的预算：

| 预算 | 上限 | 消耗条件 |
|---|---:|---|
| response slot | 2 | 首次完整响应；或已经看到 `thinking_delta` / `text_delta` / `input_json_delta` 后流中断 |
| transient failure | 3 | 在首个 generation delta 之前，已确认结束的 408/409/429/5xx、DNS/TLS/connect/timeout/断连 |

因此一个语义调用最多只有“首次生成 + 1 次完整重发”。429/504 等发生在生成开始前时不占 response slot，但会占 transient failure；达到 3 次即基础设施失败。

每次 retry 都是新的完整 `POST /v1/messages`，不具备传输层续传语义。项目不实现 `continuation_request`，不把 partial text 或 thinking 携带到下一请求。

本地 wall-clock watchdog 超时是特殊情况：旧 POST 可能仍在服务端运行，不能证明连接已经终止。实现会主动关闭 SDK client，并把它记为 `watchdog_ambiguous_inflight` 的终止性基础设施失败；不得同时再发一个重叠 POST。

## 4. 完整响应的模型语义

- 有 text + `end_turn`：正常。
- 有 text + `max_tokens` 或 context-window 截断：`text_truncated`，进入应用层解析/验证；不做 transport retry。
- 无 text + `max_tokens`：`thinking_budget_exhausted`，属于 Baseline `empty response` 模型失败。
- 无 text + `end_turn`：`model_empty`，属于 Baseline `empty response` 模型失败。
- refusal 或非协议纯文本：模型行为失败，不是 transport failure。

完整空/近空响应不做 transport retry，也不让 dispatcher 换 Key 重跑；它按实际模型输出进入主评分。transport 最终耗尽则记基础设施失败：现行 runner 在提交该步实验行前终止，API call/ledger 保存 terminal evidence；覆盖率与分析把这个缺失步骤解释为 `score=null`，不得算作模型得分 0，也不得从其他正常任务推断模型“没能力完成”。

## 5. HP repair 与 FR

- FR 不做 semantic repair。
- HP 只有在非空响应已经出现明确 patch 协议信号，并发生 JSON/schema/op/route/gate/truncated-patch 失败时，才允许一次 `hybridpatch_repair`。
- 完整空响应、thinking-only、refusal、普通说明文字不触发 HP repair。
- repair 是新的 `semantic_call_id`，自身同样拥有上述 transport 预算，并继续使用 adaptive thinking。
- V7 partial-acceptance 语义保持不变；本规范只改变 transport、空响应和 repair 入口判定。

## 6. 崩溃恢复与防重复计费

- `step_id = method/sample/rt/direction`；`semantic_call_id = step_id/call_kind`；每次子进程启动另记 `worker_launch_id`。
- `api_attempt_ledger.jsonl` 追加记录 attempt start、首个 generation progress、attempt end、预算归属和最终状态。
- 完整响应在返回 runner 前先原子写入 `api_journal/<semantic-hash>.response.json`。worker 在 checkpoint 提交前崩溃时，下次启动回放该响应并重新执行本地解析/评分，不再调用 provider。
- 若 worker 在 attempt 中途死亡，下一进程从 ledger 恢复已消耗预算：见过 generation delta 算 response slot；此前死亡保守算 transient failure。
- fatal 或预算耗尽状态同样持久化；dispatcher requeue、换 Key 或重启不能刷新同一个 semantic call 的额度。

## 7. 日志、token 与实验隔离

- API call schema：`anchorpatch.api_call/3`；attempt ledger schema：`anchorpatch.api_attempt/3`。
- 每条记录包含 `step_id`、`semantic_call_id`、`worker_launch_id`、`provider_called`、`response_replayed`、两套预算、HTTP attempt 数、stream 完整性、终止原因和原始 usage/cache 字段。
- prompt total = `input_tokens + cache_read_input_tokens + cache_creation_input_tokens`；completion = `output_tokens`；total = prompt total + completion。不得伪造单独的 `thinking_tokens`。
- raw 日志不得包含 API Key 或完整认证头。
- v3 拒绝在 v2 或无 revision 的旧输出目录断点续跑。HP 与 FR 的正式比较必须都从同一 v3 fingerprint、新 `out_dir` 开始。

## 8. 计分与报告口径

- Baseline-compatible 主口径：完整空/近空响应保留并惩罚，不做恢复；报告 `empty_response`、`truncated_attempt` 等行为类型。
- 系统可靠性口径：另报 transport failure、response retry、transient retry、journal replay、semantic repair 发生率及包含 repair 的总 token。
- 不能把“连接中断后全量重发成功”写成“续传成功”；日志用词固定为 `transport retry` / `full replay POST`。
- 任何 v3 实验开始前先通过 zero-API transport tests；live Key probe 只验证认证、完整流和 usage，不作为模型能力结论。

## 9. 冻结时验证记录

- zero-API transport/ledger/repair tests：26/26 PASS，其中包含从 transport-v2 真实归档提取的 thinking EOF、text max_tokens、thinking-only max_tokens event fixture。
- V7 executor：49/49 PASS；splitter：10 个真实文档、0 coverage violation。
- 11-Key live Hello：11/11 完整 `end_turn`，均 R1/2、I0/3；合计 2,209 tokens。
- `exp_20260712_transport_v3_smoke5`：5 个代表样本×2RT×HP/FR，10/10 tasks、result verification 20/20、preservation 0。live 命中一次 generation 后 incomplete→一次 full replay 成功，以及一次完整 thinking-only max_tokens→无 retry/repair 的模型失败。

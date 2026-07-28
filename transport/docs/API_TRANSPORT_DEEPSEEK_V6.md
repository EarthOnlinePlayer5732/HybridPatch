# OpenCode DeepSeek OpenAI-compatible transport-v6

状态：`opencode_openai_compatible/6` 的新实验规范。它只用于新的 DeepSeek-V4-Flash
实验目录；在零 API 回归与正式启动门完成前不得据此启动付费实验。`/4` 与 `/5`
保留各自原始身份和证据，只读审计，不得原地 resume、改写或混合成 `/6`。

## 请求与完成语义

- endpoint：`https://opencode.ai/zen/go/v1/chat/completions`
- model：`deepseek-v4-flash`
- transport：`openai_sdk_stream`
- request：`stream=true`、`stream_options.include_usage=true`、
  `reasoning_effort=high`
- OpenAI SDK 内建 retry 关闭；wrapper 的 502/503、`Retry-After` 与有限预算规则
  沿用 `/5`

`/6` 不改变 `/5` 的完整流判定。成功 attempt 仍必须同时满足：

1. 唯一 choice 上存在非空、非纯空白的 `finish_reason`；
2. terminal usage 的 `prompt_tokens`、`completion_tokens`、`total_tokens` 均为
   非 bool、非负整数，且 `total_tokens = prompt_tokens + completion_tokens`；
3. 终结序列单调，finish 后不出现新 choice、第二份 usage 或 SDK/HTTP 中断。

usage 可以与 finish 位于同一 choice chunk，也可以位于随后独立的 usage-only
chunk；finish 后仅允许 `choices=[]、usage=None` 的尾随元数据。缺失、提前、重复或
畸形 usage，空白 finish，finish 后新 choice，以及 partial EOF/exception 仍分类为
retryable `incomplete_stream`，丢弃该 attempt 的全部 partial 正文并全量重发。

## Compact critical sidecar

`/4`、`/5` 的 critical sidecar 为每个 SDK chunk 写一条完整
`sdk_stream_event`，并在每行重复调用 identity。长 reasoning stream 因此产生数万行、
数十 MiB 的单调用 sidecar；成功后 `_raw_stream_events` 又被写为第二份
`.sse.jsonl`。`/6` 将 critical evidence 改为 `anchorpatch.transport_event/2`，空间和
检查成本按 HTTP attempt 数增长，而不是按 chunk 数增长。

每个 sidecar 的顺序为：

```text
transport_header
  attempt_start
  stream_checkpoint(first_chunk)?
  stream_checkpoint(generation_started)?
  stream_checkpoint(finish_seen)?
  stream_checkpoint(usage_seen)?
  stream_summary
  attempt_end
  ...下一 attempt...
```

- `transport_header` 只出现一次，绑定 revision、call ID、worker launch/PID、sample、
  method、RT 与 direction；后续行不重复这组 identity。
- 四种 checkpoint 均至多出现一次，`stream_event_count` 不得回退；成功、失败和开放
  attempt 都必须按上表语义顺序前进。供应商的 finish 后新增 choice、usage 提前等
  终结序列违例由 summary terminal flags/counters 与失败 attempt 保存。checkpoint 为
  异常退出或 dispatcher parent loss 保留
  “是否已开流、是否已生成、是否已 finish、是否已获得 usage”的最小 durable witness。
- `stream_summary` 保存 chunk 数、canonical JSON byte 数、长度前缀增量 SHA-256、
  text/reasoning/tool delta 的计数与 UTF-8 byte 数、finish、usage 和完整终结状态。
- `attempt_end.attempt` 继续保存 retry budget、HTTP status、stream state 及完整脱敏
  error body/message，并与 terminal API row 中的 `transport_attempts` 逐字段一致。

canonical chunk digest 使用 `sort_keys=True`、紧凑 separators 的 UTF-8 JSON；每个
chunk 先向 SHA-256 输入其 8-byte big-endian 长度，再输入 canonical bytes，避免相邻
JSON 串联歧义。digest 用于绑定 recorder 实际观察到的流，不承担保存或恢复模型正文。

API terminal row 继续用历史字段 `raw_sse_saved_path` 指向 critical
`.transport.jsonl`，并在 `/6` 下额外绑定：

- `transport_sidecar_sha256`
- `transport_sidecar_size_bytes`
- `transport_sidecar_record_count`

字段名 `raw_sse_saved_path` 是兼容保留；`/6` 不生成独立 `.sse.jsonl`。最终 assistant
正文、request、重建 response、terminal usage 与脱敏 provider 错误仍按原路径保存；
逐 chunk reasoning/text 原文不属于 full campaign 的 critical sidecar。

## Inspector 与 recovery

- `/6` inspector 必须验证 header linkage、sidecar SHA/size/record count、连续 attempt
  index、checkpoint 单调性、summary/attempt/API row 一致性，以及成功 attempt 的非零
  stream event count。
- 有 `generation_started` checkpoint 的 open attempt 按已开始生成处理；没有该
  checkpoint 的 open attempt 不得伪装为已有 partial generation。parent-loss recovery
  只从首个未提交 RT 继续，绝不重发已提交 step。
- 仅有 header 的崩溃前缀表示 POST 尚未开始；已有完整 terminal summary、但尚未来得及
  写 `attempt_end` 的前缀必须分类为 `complete_unpublished_response`，不得降级成普通
  open attempt 后重抽。
- `/4`、`/5` reader 保留 linear `sdk_stream_event` 只读兼容。新 writer、普通 resume
  与 recovery identity 仅为 `/6`；旧 experiment 不得通过改 manifest 或 authorization
  升级。
- 对完整 raw chunks 的一次性诊断如确有需要，应放在独立 `transport/exp_*`，不得把
  full-chunk logging 重新设为 full234 默认 critical evidence。

## 零 API 回归门

必须覆盖：

1. 两种合法 terminal usage 形态及所有 `/5` malformed/partial/retry 反例；
2. 大量 synthetic chunks 下 sidecar 行数保持常数级，不含模型 chunk 正文，不生成
   `.sse.jsonl`；
3. generation 前 503 免费预算、generation 后失败与 502 消耗预算；
4. 502/503 完整脱敏 body/message 同时存在于 attempt/API row；
5. sidecar 写入或 fsync 失败仍 fail-closed；
6. `/6` inspector/recovery 的 header、checkpoint、summary、digest 与 parent-loss 正负例；
7. `/4`、`/5` 冻结 fixture 的只读审计兼容。

本 revision 的实现与文档阶段未调用 API；任何新的正式实验仍需新的 out_dir、计划、
clean commit、零 API 验证和用户授权。

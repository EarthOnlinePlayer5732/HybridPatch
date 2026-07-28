# OpenCode DeepSeek OpenAI-compatible transport-v5

状态：`opencode_openai_compatible/5` 的活动规范。它只用于新的
DeepSeek-V4-Flash 实验目录；`/3` 与 `/4` 目录保持原身份、原证据和原完成判定，
不得原地 resume 或改写成 `/5`。

## 请求身份

- endpoint：`https://opencode.ai/zen/go/v1/chat/completions`
- model：`deepseek-v4-flash`
- transport：`openai_sdk_stream`
- request：`stream=true`、`stream_options.include_usage=true`、
  `reasoning_effort=high`
- OpenAI SDK 内建 retry 关闭；wrapper retry 与 502/503 预算规则沿用 `/4`

## 完整流判定

`/5` 必须同时观察到：

1. 唯一 choice 上非空、非纯空白字符串 `finish_reason`；
2. 完整 terminal usage，其中 `prompt_tokens`、`completion_tokens`、
   `total_tokens` 均为非 bool、非负整数，且
   `total_tokens = prompt_tokens + completion_tokens`；
3. 单调终结序列，无 finish 后的新 choice、第二份 usage 或 SDK/HTTP 中断。

允许两种等价的 terminal usage 形态：

```text
... → choice(finish_reason, usage) → choices=[] / usage=None metadata*
... → choice(finish_reason, usage=None) → choices=[] / valid usage
```

第一种是 2026-07-28 OpenCode Zen Go 的实测形态；第二种保留 OpenAI 标准
usage-only 形态。finish 后只允许零个或多个 `choices=[]、usage=None` 的非生成
元数据块。

以下情况仍分类为 retryable `incomplete_stream`，丢弃该 attempt 的全部 partial
正文并按既有预算全量重发：无 finish、完全缺 usage、提前 usage、重复 usage、
空或畸形 token accounting、finish 后出现新 choice、partial EOF 或 SDK exception。

## 兼容与证据

- `/5` 只改变 terminal chunk 形态兼容，不改变 prompt、HP/FR、evaluator、计分、
  token/cost schema、retry budget 或 Dispatcher 隔离政策。
- `final_usage_seen=true`、`terminal_sequence_valid=true` 和
  `stream_complete=true` 仍是成功 attempt 的必要条件。
- raw linear stream events、terminal API row、transport sidecar、脱敏 502/503
  body/message 与 result/checkpoint linkage 继续完整保存。
- Dispatcher 可只读审计冻结 `/4` stream 目录；新启动、普通 resume 与 recovery
  identity 固定为 `/5`，不能混合 revision。

## 回归门

零 API 测试必须覆盖：两种合法 terminal usage 形态、尾随空元数据、重复/提前/
缺失/畸形 usage、缺失或空白 finish、partial EOF、502/503 retry budget，以及
冻结 `/4` 只读审计兼容。

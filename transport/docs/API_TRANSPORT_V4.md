# OpenCode MiniMax Anthropic transport-v4 规范

状态：`opencode_anthropic_sdk/4` 的活动规范。它只适用于使用该 revision
创建的新实验目录。`API_TRANSPORT_FROZEN_V3.md` 及其历史实验语义保持冻结，
不得用本文件反向解释或续跑 transport-v3 归档。

## 1. 边界

transport-v4 只改变 MiniMax-M3 经 OpenCode Go、Anthropic SDK 流式调用的传输、
账本与 paired campaign 故障隔离。它不改变 HybridPatch 协议、提示、执行器、
validation gate、partial acceptance、preservation、no-op、FullRewrite、evaluator 或
scoring。

传输重试与 HybridPatch repair 是两类不同操作：

- `transport_initial` / `transport_retry` 对同一个 exact semantic call 使用完全相同的
  prompt、模型和生成参数；request fingerprint 不同则在 POST 前拒绝。
- `hybridpatch_repair` 是应用层在收到完整模型内容后，因合格的协议/执行错误而构造
  新提示产生的另一 semantic call；它有自己的传输预算。
- `call_kind` 描述方法语义调用；`attempt_kind` 描述该调用内的传输尝试，二者不能
  混用。

## 2. 完整流判定与错误分类

以下任一条件先于普通 HTTP 状态码判断，统一分类为
`error_type=incomplete_stream` 且可传输重试：

1. 内部 `IncompleteStreamError`；
2. Anthropic `APIStatusError` 的 `status_code=200`，且错误消息含
   `Streaming response failed`；
3. 已观察流事件但没有 `message_stop`；
4. 终结 `message_delta` 没有最终 output usage，或 SDK final message 没有完整
   input/output usage；
5. content block 未按状态机正常闭合，包括重复 start、未知 block 的 delta/stop、
   stop-before-start 或未 stop。

HTTP 200 不得提前归入 fatal。完成链必须按单调状态机出现
`message_start → content blocks → message_delta(final usage) → message_stop`；重复、
逆序或终结后新增事件均是不完整流。SDK final usage 和非空 stop reason 也必须存在。
部分 thinking/text 只写 raw audit，不交给方法层、repair 或评分。

## 3. R2/I3 传输预算

每个 exact semantic call 固定：

- 最多 2 个生成型 response slot：首次生成和一次完整重发；
- 最多 3 个生成开始前 transient failure；
- 总 HTTP attempt 不固定为 2。

观察到 `thinking_delta`、`text_delta`、`input_json_delta` 或其他实际生成 delta 后
中断，消耗一个 response slot。`signature_delta` 不单独证明生成开始。没有生成
delta 的 retryable 网络/服务错误消耗一次 transient failure。完整成功响应即使正文
为空也消耗 response slot，但空/拒答/thinking-only 属于完整模型结果，不做传输重试。

预算由 `attempt_start`、`generation_progress`、`attempt_end` 事件重新计算；
`attempt_budget` 和 terminal counter 只是必须与事件一致的校验值，不能覆盖事件事实。
JSONL 损坏、异版本行、fingerprint 漂移、事件乱序、重复 attempt 或 counter 漂移均
fail closed，禁止继续 POST。

## 4. Semantic lineage 与恢复

为同时满足“每 semantic call 严格 R2/I3”和“基础设施耗尽后可人工恢复”，v4 将
逻辑步骤 lineage 与 exact semantic call 分开：

```text
semantic_root_id = method/sample/rtNN/direction/call_kind
semantic_call_id = semantic_root_id/g000
semantic_call_id = semantic_root_id/g001  # 经审计恢复的新 semantic call
```

自动传输重试只发生在同一 `gNNN` 内，预算不重置。样本级恢复必须由 dispatcher
精确授权失败的 parent call 和下一个连续 generation；授权在 worker metadata 与环境
中同时绑定 `semantic_call_id`、`generation_index`、`request_fingerprint`、
`next_attempt_index` 四个值，且只消费一次。它创建新的 exact semantic call，保持同一
request fingerprint，并使 lineage 内 `attempt_index` 从历史最大值加一。每个
generation 自身仍严格 R2/I3。旧式在同一个 semantic ID 内写 `transport_resume` 并
清零预算的记录不被 v4 接受。

ledger、journal 与 API row 保存：`semantic_root_id`、`semantic_call_id`、
`generation_index`、`parent_semantic_call_id`、`request_fingerprint` 和全 lineage
递增的 `attempt_index`。lineage 必须 generation 连续、parent 唯一、fingerprint
相同、至多一个 committed response。每个 semantic root 使用非阻塞独占文件锁，
从 preflight 持有到 raw capture、response journal、attempt terminal 和 API terminal
row 全部落盘；重复 worker 无法发出第二个 POST。若 recorder 在 journal 后、ledger
commit 前被中断，下一次在同 fingerprint 下补写 ledger terminal 并本地 replay；若
ledger 已 committed 而 journal 缺失，或 journal/ledger 身份不一致，则 fail closed、
零 POST。该本地 reconciliation 不把已退出 formal worker 的半提交 API evidence 变成
sample-local failure：若 response 已 committed 但 worker 在 API terminal row 前退出，
仍按“未捕获 runner / ledger 无法映射”全局停止。

恢复不删除旧 failure、raw、ledger、checkpoint 或结果。代码 commit、clean tree、
transport revision、模型、seed、task plan、方法顺序或请求 fingerprint 改变时，禁止
在旧正式 campaign 续跑，必须使用新实验编号。

## 5. HybridPatch repair 资格

完整响应交给方法层后，仅以下应用错误可使用既有的一次 HP repair：有 HybridPatch
协议信号的 invalid JSON、schema、plan/route、local/bulk 字段、anchor match、
validation gate 或 format-health 错误。

以下情况不调用 HP repair：无协议信号普通文本、拒答、完整空响应、thinking-only
且 max tokens、模型正常输出但任务语义错误、evaluator 或本地代码异常、以及任何
尚无完整模型响应的 transport failure。repair 是新 `call_kind`，独立建立自己的
`semantic_root_id/g000` 与 R2/I3。

## 6. Paired campaign 故障隔离

只有四类 append-only 证据一致且明确耗尽 R2/I3 时，worker 非零退出才可隔离为
`infrastructure_incomplete`：sample outcome、run metadata、API call row 和 attempt
ledger。该 sample 保留缺失/null 终点，不写 0 分；其他 sample 继续运行。

以下任一条件仍立即全局停止：

- `preservation_violations > 0`；
- Git commit 或 tree state 漂移；
- 重复结果行或半提交 round trip；
- API/attempt ledger 不能映射到 sample、method、RT、direction 和 worker；
- 非重试 fatal 请求错误、未捕获 runner/本地/evaluator 异常；
- 共享执行器、数据或评测完整性错误。

多个 provider error 本身不触发模糊的自动全停。恢复只启动 latest outcome 为
`infrastructure_incomplete` 的 sample；完成 sample 不创建 worker，因此其已提交
forward/backward 不会再次 POST。若恢复后仍有 sample 不完整，campaign 仍是
`failed_informative`，正式 endpoint 保持 null。

dispatcher 对活动 worker 只容忍其精确 provenance 对应 generation 的一个合法尾部
写入窗口；worker 退出或 active mapping 不匹配后，缺 terminal、半写 attempt 或
跨文件 terminal 不一致立即视为全局完整性错误。provider 返回后、evaluator 前以及
result commit 前再次校验 stop latch、active worker、Git/tree 与 task-plan identity；
relay rows/checkpoint commit 与 stop latch 共用 ordering lock，stop 先落盘时不得提交
结果。dispatcher 在改变 active-worker set 前必须先证明旧 sample lease 已释放并完成
metadata/exit 审计；若 terminate 后 lease 仍被持有，则保留原 active set。全局停止时
最后才清空安全回收的 active set，避免把活 worker 人为变成 authorization drift。

## 7. Schema 与兼容性

- API call：`anchorpatch.api_call/4`
- attempt ledger：`anchorpatch.api_attempt/4`
- response journal：`anchorpatch.api_response_journal/4`
- run metadata：HP_V8 与 transport preflight 使用
  `anchorpatch.run_metadata/3`，并在 POST 前绑定 commit/tree、代码指纹、revision、
  resume policy 与四字段恢复授权
- sample outcome：`anchorpatch.sample_outcome/1`
- transport revision：`opencode_anthropic_sdk/4`
- resume policy：`exact_payload_new_semantic_call/1`

V1–V8 HybridPatch envelope replay 与 transport schema 是正交边界。旧 v1–v8 信封
继续按各自方法语义解析；transport-v3 实验目录不得混入 v4 row，也不得用 v4
dispatcher 直接续跑。

## 8. 验证门

正式 API 前至少通过：HTTP 200 incomplete 分类、delta/no-delta 预算、缺 stop、缺
terminal usage、非法 block 序列、R2/I3 事件重算、fingerprint/ledger fail-closed、
多 worker 隔离、preservation 全局停止、只恢复 incomplete sample、已提交 RT
零 POST、V1–V8 replay，以及 clean commit/tree 检查。

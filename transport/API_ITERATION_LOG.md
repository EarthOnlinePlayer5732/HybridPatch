# HybridPatch API / transport 迭代日志

本目录是传输层源码主战场。方法版本的冻结源码位于 `HP_Vx/src/`；API 修改只在这里演进，验收后拷贝同步到当前活跃 HP 版本，并记录双方指纹。冻结版本永不回灌。

`transport/src/` 保存当前传输与调度相关的七个运行时文件及测试资产：`model_openai.py`、`run_meta.py`、`experiment_runner.py`、`fr_baseline_dispatch.py`、`probe_fr_keys.py`、`monitor_experiment.py`、`launch_pipeline.py`，另含 `test_model_openai.py` 与脱敏 fixture。方法五件套、domains、data 与 prompts 不在本目录重复维护；完整零 API 回归应在同步后的活跃 `HP_Vx` 运行。

## Revision 时间线

### SSE 流式化（hybridpatch/2 时期）

- 动机：thinking + 非流式请求在 Cloudflare 120 秒边界触发结构性 524，盲重试会重复撞同一边界。
- 改动：改用 SSE 流式接收，持续传递字节；runner 增加按样本故障隔离。
- 结论：结构性超时与瞬态失败必须分开处理；收到部分 token 不等于响应完整。

### transport-v2（历史诊断线）

- 动机：V6 val40 发现 MiniMax-M3 会在 thinking 阶段中途断流；旧实现把不完整流提交为空响应。
- 改动：使用 Anthropic SDK `messages.stream()`；完成条件要求终止事件与 final usage 齐全；不完整流全量重发；raw event 由累计 snapshot 改为线性记录；新增只读 monitor。
- 证据：`exp_20260711_opencode_transport_v2_smoke5`，5 样本×双臂×2RT，结果复核历史记录 PASS 20/20。该档是诊断证据，不作方法结论；重构后位于 `_attic/`。
- 局限：所有异常仍混在单一 retry 数；换 Key 或重启会刷新额度，可能形成隐形额外采样。

### `opencode_anthropic_sdk/3`（OpenCode transport-v3，冻结）

- 固定 provider：OpenCode Go，Anthropic SDK，MiniMax-M3 adaptive thinking。
- 完整性：`message_start`、所有 block stop、`message_delta`、`message_stop`、final usage 与非空 stop reason 全部齐全才可提交。
- 双预算：每个 semantic call 最多 2 个 response slot；生成前 transient failure 最多 3 次；预算由 ledger 跨进程持久化。
- 崩溃恢复：attempt ledger + 原子 response journal；完整响应可本地回放，避免重复 POST/计费。
- 模型语义：完整空、thinking-only、refusal 与非协议文本不做 transport retry；HP repair 仅对非空且已有协议信号的应用层失败开放一次。
- 冻结证据：零 API 26/26、真实异常 fixture 回放、11-Key canary、双臂 smoke 结果复核 PASS 20/20。规范见 `docs/API_TRANSPORT_FROZEN_V3.md`。

### `opencode_anthropic_sdk/4`（OpenCode transport-v4，活动）

- 动机：MiniMax Anthropic SDK 会把 HTTP 200 的 `Streaming response failed` 暴露为 `APIStatusError`；v3 先看 status code，可能把不完整流当 fatal。旧 paired dispatcher 又会因单个 provider retry exhaustion 全局 fail-fast。
- 分类：HTTP 200 streaming failure、缺 `message_stop`、缺终结事件 usage 和 block 状态机未正常闭合统一为 retryable `incomplete_stream`；该判断先于普通 status code。
- 双预算：每个 exact semantic call 仍严格 R2/I3；预算按 delta/attempt 事件重算，累计字段只作一致性断言。
- 恢复：逻辑 root 下使用 `g000`、`g001` 等新的 exact semantic call；恢复 generation 与 parent、fingerprint、连续 attempt index 强绑定，不在同一 ID 内重置预算。已提交 response journal 只本地 replay。
- 隔离：只有 outcome、metadata、API row、attempt ledger 四证一致且明确耗尽 R2/I3，才把单 worker 标为 `infrastructure_incomplete`；其他 sample 继续。preservation、Git 漂移、重复/半提交、ledger 错配和本地/evaluator/shared-integrity 异常仍全局停止。
- 兼容：只用于新 out_dir；v3 规范和归档保持冻结，不直接续跑或拼接。活动规范见 `docs/API_TRANSPORT_V4.md`。
- 同步指纹：`transport/src/model_openai.py` 与 `HP_V8/src/model_openai.py` 原字节
  SHA-256 均为 `cdfe85e9f48b81d16ff85857d51b97db09b559877675456708838959ff15fd93`。
- 零 API 验证：HP 集成/故障注入 83/83、transport-core 47/47、V1–V8 envelope
  matrix 72/72 均 PASS；含锁存后 transport backoff 零 POST、sample isolation、
  preservation global stop、audited resume 和 committed RT 零 POST。首次 API 前仍需
  独立计划审阅、clean commit、dry-run/runtime preflight 与 Key probe。
- 首轮 v4 paired10 的 live inspector 在 API terminal 与 result 相隔 88 ms 的发布窗口
  组合了旧 API snapshot 与新 result，保守触发全局停止；最终 API/ledger/journal/result
  全部存在。dispatcher 现按 result→API→attempt/journal 的逆 publication 顺序缓存证据，
  保留全部严格校验。确定性并发测试冻结该顺序，且全局 inspection error 仍在终止 worker
  前落 durable stop latch；旧 campaign 保持 `failed_informative`，新提交使用新实验编号。

### `opencode_openai_compatible/1`（DeepSeek V4 Flash OpenCode 线）

- provider：OpenCode Zen；endpoint
  `https://opencode.ai/zen/v1/chat/completions`；model `deepseek-v4-flash`。
- 请求：Python OpenAI SDK non-stream，SDK 内建 retry 关闭，由 wrapper 提供最多 3 次可见、
  有界的 retry；每次 attempt 写入返回 metadata。
- reasoning：正式 campaign 强制 `reasoning_effort=high`。runner、run metadata、API ledger、
  raw request 和 result row 全链记录；base URL 或 reasoning 不匹配时在 provider POST 前拒绝。
- 计费：按 OpenCode Zen 的 USD DeepSeek V4 Flash 费率单独计算，不复用 DeepSeek 官方 CNY
  路线或 MiniMax 费率。
- 调度：`deepseek_capacity15` 固定单 Key、15 worker、15 sample、RT2；
  `deepseek_full234` 使用探针成功的至少两个物理唯一 Key，每 Key 15 worker，稳定 FIFO 即时
  补位，禁止 wave/batch barrier。
- 隔离：DeepSeek non-stream 不伪造 MiniMax stream attempt ledger/journal；专用 inspector
  继续严格检查 campaign identity、worker authorization、raw request、API/result linkage、
  checkpoint、preservation 和完整性。
- 首次付费调用前仍需 capacity/full 两份计划的独立审阅、clean commit、unified zero-API
  preflight、单 Key probe；全量另需 capacity PASS 和全部候选 Key probe。

### `opencode_openai_compatible/2`（DeepSeek V4 Flash OpenCode Go 端点纠正）

- `/1` 错配到非 Go 的 `https://opencode.ai/zen/v1/chat/completions`，真实探针返回 HTTP 401
  `CreditsError: Insufficient balance`，不能用于判断 Go 渠道 Key 存活。
- `/2` 固定用户确认的 `https://opencode.ai/zen/go/v1/chat/completions`；model、OpenAI SDK
  non-stream、wrapper retry 上限和 `reasoning_effort=high` 均不变。
- 纠正后从顶层 `.env.frkeys` 读取 3 个标签，正式 wrapper 探针 3/3 HTTP 200、完整终止，
  每个 Key 均只有 1 次 attempt、0 retry。
- capacity15 暴露 sustained-load 503 后，用户明确将 full234 调度改为 3 个存活 Key、
  每 Key 10 worker；capacity15 仍保留单 Key 15 worker 的诊断配置。

### `opencode_openai_compatible/3`（OpenCode Go HTTP 503 免费重试）

- OpenCode Go 返回 HTTP 503 时持续重试，不消耗 `max_retries` 的有限额度。
- 每次 503 仍保留实际 attempt、状态码与等待记录，并标记
  `retry_budget_consumed=false`；其他错误仍遵循原有重试额度。

### `opencode_openai_compatible/4`（DeepSeek stream 与完整错误证据）

- 请求改为 OpenAI Chat Completions stream，并发送
  `stream_options={"include_usage": true}`。只有非空 `finish_reason` 后出现
  usage-only terminal chunk，且三项 token 计数为一致的非负整数，才设
  `stream_complete=true`；EOF、SDK 断流、乱序/空 usage 或缺任一终结证据均分类为
  `incomplete_stream`，丢弃 partial output 后全量重发。
- 每个 chunk 以线性 `sdk_stream_event` 保存；content、reasoning 与 tool delta
  分别标记。reasoning-only 的完整响应仍是可审计 `model_empty`，不是 transport
  truncation。
- HTTP/SDK exception 的脱敏完整 body 与 message 写入 attempt metadata；formal
  recorder 另写每 call 的 `transport.jsonl` sidecar。终端 API row 保留同一
  `transport_attempts`，sidecar 写失败不丢失终端错误内容。
- 生成前 503 仍标记 `retry_budget_consumed=false`，并使用带 per-worker spread
  的指数退避；一旦流已产生 generation delta，后续 503/断流按
  `incomplete_stream` 消耗有限预算，禁止把部分生成当免费 retry。
  502 与其他 retryable 5xx 消耗有限预算。
- `Retry-After` 同时从 header、结构化 body 和 message 提取，最多接受 300 秒；
  不再把 provider 明示的 60 秒截成 30 秒。
- 正式 `deepseek_full234` 的配置语义同时修正为 RT10；`/3` RT2 non-stream
  campaign 仅按历史身份审计，不能与 `/4` 混目录或原地 resume。
- 单 Key 短提示 bounded diagnostic：c10 与 c15 两轮合计 25/25 完整 HTTP 200，
  `/4`、high reasoning 与 terminal usage 全部一致，0 retry、0 个 502/503；这验证
  stream 请求形状和完成链，不代表长 HP/FR 请求的 sustained-load 结论。

### `minimax_official_nonstream/1`（官方非流式线，在用）

- 动机：为 FR baseline 忠实翻译上游 DELEGATE-52 的官方非流式 + 盲异常重试语义，与 OpenCode v3 公平性策略分线。
- 启用：仅由进程环境 `MINIMAX_TRANSPORT=official_nonstream` 选择；不得写入 `.env`，禁止与 OpenCode revision 混目录。
- 请求：MiniMax 官方 OpenAI-compatible `chat.completions.create()`，adaptive thinking，`reasoning_split=true`。
- 接受政策：完整 HTTP 200 一律照单接受，包括 `finish=length`、`finish=abort`、空 content；只有 SDK/HTTP 异常进入盲重试。429 进入配额等待，不消耗盲重试次数。
- 关键发现：OpenCode 上的 thinking 中途断流病理，在官方端点表现为完整 200 + `finish_reason=abort`；属于 MiniMax 服务端生成病理，不是 OpenCode 网关独有问题。
- 证据：`transport/exp_20260713_problem15_official` 诊断档；`Baseline/exp_20260713_frbaseline_v2_rerun` 官方重跑源档。两者均保留现状，不再续跑。规范见 `docs/API_TRANSPORT_MINIMAX_OFFICIAL_V1.md`。
- 派生基线：新增 `FR+Official`，不替换原冻结 FR。233 个样本由 87 条完整官方链（83 条 `frbaseline_v2_rerun` + 4 条 `problem15_official` 补位）与 146 条原冻结 FR 链离线组成；不调用 API、不拼接未完成 RT。
- 异常披露（样本级，类别可重叠）：任一异常 87/233（37.34%）、截断/异常终止 86/233（36.91%）、空返回 80/233（34.33%）、非空正文小于 200 UTF-8 bytes 的接近空返回 37/233（15.88%）。被选官方子集分别为 85/87、85/87、79/87、35/87；该子集经过异常/问题样本预筛，不能当作官方 API 背景异常率。详见 [`Baseline/FR+Official/README.md`](../Baseline/FR+Official/README.md) 与 [`val40_comparison.md`](../Baseline/FR+Official/val40_comparison.md)。

## 同步与运行规则

1. API 修改只在 `transport/` 发生，并新增独立 revision；不得复用旧 revision 改语义。
2. 先在隔离 fixture/测试中验证，再原字节拷贝到当前活跃 `HP_Vx/src/`；同步记录源/目标 SHA-1。
3. 从活跃 `HP_Vx` 根运行完整测试和实验。`transport/` 不是完整方法运行根。
4. OpenCode v4、冻结 v3 与 official nonstream 各线并存且互斥，不合并、不混目录。
5. 顶层 `.env` / `.env.frkeys` 不复制进 `transport/` 或 HP 版本。调用方显式注入环境；多 Key 工具显式传 `--keys_file ../.env.frkeys`。

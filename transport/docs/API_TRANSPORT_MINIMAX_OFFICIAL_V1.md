# MiniMax 官方非流式传输 v1（`minimax_official_nonstream/1`）

状态：**在用（诊断/基线修订用途）**。本文档记录 2026-07-13 引入的 MiniMax 官方 API 传输路径的全部改动、语义与实测发现。OpenCode Go transport-v3 的冻结规范（`API_TRANSPORT_FROZEN_V3.md`）**不受影响**——两条传输路线并存、互斥、各有独立 revision。

## 1. 动机

- 上游 DELEGATE-52 baseline 的 API 语义是：**非流式** `chat.completions.create()` + 盲 `except Exception` 重试 + 完整 HTTP 200 无论内容照单接受（含空响应、`finish_reason=length` 截断）。
- 我们的旧流式管道曾把"服务端 thinking 中途流截断"提交成空响应（v2 时代）或用 response-slot 重发救回（v3）——两者都不是 baseline 行为。FR 作为对照方法须严格对齐 baseline，故为其建立官方端点 + 非流式 + 盲重试的忠实翻译。
- 用户持有 MiniMax 官方订阅（单 Key，5 小时窗 + 周配额）。

## 2. 固定运行面

| 项 | 值 |
|---|---|
| 选择方式 | 环境变量 `MINIMAX_TRANSPORT=official_nonstream`（缺省 `opencode`，走冻结 v3 路线） |
| 端点 | `https://api.minimaxi.com/v1/chat/completions`（OpenAI SDK，`max_retries=0`） |
| 密钥 | `MINIMAX_API_KEY`（放根 `.env`；⚠️ `MINIMAX_TRANSPORT` 本身**不得**写进 `.env`，只按进程设置，防止静默切换全部后续实验） |
| 模型名 | 请求体用官方大小写 `MiniMax-M3`（本仓库别名 `minimax-m3` 自动映射） |
| thinking | `extra_body: {"thinking": {"type": "adaptive"}, "reasoning_split": true}`——`reasoning_split` 使 thinking 走 `reasoning_details` 字段，`message.content` 只含正文（否则 `<think>` 标签会混入 `raw_llm_response`） |
| max tokens | `max_completion_tokens`，沿用研究政策上限 `131072`（`None/0` 映射为上限） |
| temperature | adaptive thinking 下强制 `1.0`（与 OpenCode 路线一致） |
| 超时 | `max(调用方 timeout, MINIMAX_HARD_TIMEOUT)`（默认 1800s；实验建议 env 设 7200） |
| revision 门禁 | `minimax_runtime_config` 如实上报 `minimax_official_nonstream/1`；run_meta 拒绝在不同 revision 的 out_dir 里续跑/混跑 |

## 3. Baseline 对齐语义（判定表）

| 情形 | 处置 | 与 baseline 的对应 |
|---|---|---|
| SDK 抛任何异常（连接/超时/5xx/解析…） | 盲重试：sleep 4s 后整请求重发，`max_retries`（默认 3）耗尽即失败 | 上游 `except Exception` 同款 |
| HTTP 429 / RateLimitError | **不占盲重试次数**：进入限额等待循环（见 §5） | 调度层暂停，非语义变化 |
| 完整 200 + 正常正文（`finish=stop`） | 正常，分类 `normal` | 照单接受 |
| 完整 200 + `finish=length` | 分类 `text_truncated`（有文）/ `thinking_budget_exhausted`（无文），**照单接受、不重试** | 上游不看 finish_reason，同样接受 |
| 完整 200 + `finish=abort`（服务端生成中止） | 分类 `aborted_partial_text`（有文）/ `model_empty`（无文），**照单接受、不重试** | 完整 200，上游同样接受 |
| 完整 200 + `message.content` 字段整个缺失 | 视为空响应（content=""），**不再报错崩溃**（修复见 §6-F2） | 上游 SDK 属性访问得 None，等价空 |
| 空/近空响应 | 不触发 transport retry，不触发 HP repair（HP repair 入口判定沿用 v3 规范：须非空且有协议信号） | baseline "empty response = 模型失败" |

不实现：attempt ledger、response journal、跨进程预算持久化（baseline 本无这些）；`api_calls.jsonl` 照常全量记录每次调用。断点续跑仍靠 runner checkpoint；worker 在调用中途崩溃可能造成单次调用重复计费（baseline 同样如此）。

## 4. `finish_reason="abort"`：§227 病理的官方端点形态（本路线最重要发现）

- OpenCode 上表现为"thinking 中途流截断（EOF 无收尾事件）"的服务端病理，在官方非流式端点上表现为**完整 HTTP 200 + `finish_reason:"abort"`**，两种子形态：thinking-only（无 content 或 content 空）与部分正文（生成到一半被服务端掐断）。
- **结论：病理在 MiniMax 服务端，不在 OpenCode 网关**。且官方形态是完整响应——非流式盲重试救不了（不进 except），transport-v3 的 response-slot 重发也不适用（只重发不完整流）。严格 baseline 语义下 FR 对此完全暴露。
- 实测（`exp_20260713_problem15_official` FR 臂，15 个问题样本、154 行）：**abort 24 次（15.6%）、9/15 样本命中**（crystal6×7、robotics1×8）。注意问题样本集有选择偏差，不代表全量背景率。
- 链后果实录：earncall1 / satellite4 均为 RT1 forward abort 0 字节 → baseline 语义照单入链 → 工作区清空 → 后续全链 `context_mismatch`/0（§217 weather1 型整链清空的官方端点复现）。

## 5. 5h / 周限额预案

- 撞 429（`RateLimitError` 或 `status_code==429`）：等待 `MINIMAX_QUOTA_WAIT_SECONDS`（默认 600s）后重探，至多 `MINIMAX_QUOTA_MAX_WAITS`（默认 36）次 ≈ 6 小时，覆盖一个完整 5h 窗；每次等待打 stderr 日志并计入 `quota_wait_count` 遥测。**等待不消耗盲重试次数**——这是调度而非语义，等价于晚点启动实验。
- **周限额场景**：6 小时等待预算耗尽仍 429 → 该调用失败 → 样本 abort（隔离，不拖死进程）→ 人工停工，配额恢复后**原命令重启**即从 checkpoint 续跑。

## 6. 代码改动清单（全部在 `src/model_openai.py`，除注明外）

| # | 改动 | 说明 |
|---|---|---|
| F1 | 官方传输路径 | 常量/`_minimax_transport()`/`_minimax_official_client()`/generate() 路由：官方走"非 minimax 分支"的 baseline 盲重试环 + 官方请求体；OpenCode 流式状态机分支不受影响 |
| F2 | 缺失 content 修复 | 官方响应 thinking-only abort 时可整个不带 `content` 键（SDK `to_dict()` 剔除未设字段）→ 原样本级崩溃（"provider response missing choices[0].message.content"）改为按空响应接受。**非官方路径校验保持原样** |
| F3 | abort 分类 | `finish_reason=="abort"` → `aborted_partial_text` / `model_empty`（§4）；分类纯披露，不改接受行为 |
| F4 | 限额等待 | §5；重试环从 for 改 while，非官方路径行为逐语义等价 |
| F5 | metadata | `minimax_runtime_config` 官方分支（provider/transport/revision/base_url/reasoning_split）；结果字典的 provider/transport/stream_complete/classification/input+output tokens 等字段官方语义化 |
| F6 | runner 面板（`experiment_runner.py`，显示层） | 方法列按实际方法标 `HP=`/`FR=`、参照列改名 `FRbase=`、分数/Δ/DROP 百分制、错误行显示有效分 `0.0`（错误原因留在 `FAIL:` 标志）；JSONL 存储与计分语义零变化 |
| F7 | （前置修复）`utils_env.load_sample` | 显式 `encoding="utf-8"`——无 `PYTHONUTF8=1` 时 Windows GBK 曾致结果复核流程崩溃，且有静默误解码风险 |

**已知缺口**：官方路径 `api_calls.jsonl` 的 `total_usd` 字段未正确落值（记 0）；token 数（prompt/completion/input/output）准确，费用暂以 token 手算（参考价 $0.30/$1.20 每百万，官方订阅实际计价以后台为准）。待修。

## 7. 验证记录

- 零 API：`test_model_openai.py` **33/33 PASS**（26 项 OpenCode v3 回归不动 + 7 项官方新用例：路由/请求体、空+length 分类、盲重试计数、缺 content 的 abort 不崩、abort 带文分类、限额等待不烧重试、runtime config 双态）。
- live 历史记录：`exp_20260713_problem15_official`（15 个问题样本诊断；F2 修复前有样本被崩溃中止）。现有源档已冻结、不续跑；`FR+Official` 只采用其中 4 条完整补位链，不拼接 partial。官方 Key 消耗至 2026-07-13：196 调用，input 567,271 / output 1,120,250 tokens（thinking 计入 output），≈$1.5。

## 8. 使用模板（Git Bash，从仓库根开始）

```bash
# 仅在已按版本纪律创建新的可写 HP_V8（或后续版本）后运行
cd ./HP_V8
export PYTHONUTF8=1
export MINIMAX_TRANSPORT=official_nonstream

# 探针（1 次 Hello）
python -c '<见 probe 一节或直接用 generate 一行式>'

# 实验 worker（示例）
export MINIMAX_HARD_TIMEOUT=7200
python ./src/experiment_runner.py --sample SAMPLE_ID_1 SAMPLE_ID_2 --methods fullrewrite --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 131072 --out_dir exp_DATE_SLUG --notes "<用途>"
```

- 单 Key 并发 = 同时开的 worker 进程数（当前约定 3）。
- 新实验必须新 `out_dir`；revision 门禁会拒绝官方/OpenCode 混目录。

## 9. 官方路线 campaign 与 `FR+Official`

- `exp_20260713_problem15_official`：问题样本诊断（HP+FR），abort 病理首测；其中 4 条完整 FR 链作为 `FR+Official` 的补充来源。
- `exp_20260713_frbaseline_v2_rerun`：冻结基线的官方非流式重跑源档；85 个预筛样本中取得 83 条完整 FR 链。未完成链不做 RT 拼接，也不再续跑。

### 9.1 新增派生基线：`FR+Official`

`FR+Official` 是在原冻结 FR 之外新增的派生版本，原冻结 FR 保留不变。它只离线选择完整链：优先采用 `frbaseline_v2_rerun` 的 83 条完整官方链，再用 `problem15_official` 的 4 条完整链补位，其余 146 条沿用原冻结 FR，共 233 个样本。组装过程不调用 API，也不重掷、拼接或按得分挑选 RT。

| 样本级指标 | `FR+Official` 全集（233） | 被选官方子集（87） |
|---|---:|---:|
| 任一异常（并集） | 87/233 = **37.34%** | 85/87 = **97.70%** |
| 截断/异常终止 | 86/233 = **36.91%** | 85/87 = **97.70%** |
| 空返回 | 80/233 = **34.33%** | 79/87 = **90.80%** |
| 接近空返回（非空且正文 `<200` UTF-8 bytes） | 37/233 = **15.88%** | 35/87 = **40.23%** |

“任一异常”是截断/异常终止、空返回、接近空返回三类的样本级并集，各类别可以重叠。这里的 87 条官方链来自异常/问题样本预筛池，因此 **85/87 不能解释为 MiniMax 官方 API 的背景异常率**；它只描述 `FR+Official` 所选官方子集。完整来源、判定规则与逐样本清单见 [`Baseline/FR+Official/README.md`](../../Baseline/FR+Official/README.md)，val40 替换敏感性对比见 [`val40_comparison.md`](../../Baseline/FR+Official/val40_comparison.md)。

# HP_V9 简化正式实验运行路径

## 边界

新实验的唯一入口是 `src/run_campaign.py`。旧的
`paired_campaign_dispatch.py`、`campaign_recovery_runtime.py` 和
`authorize_ledger_lock_recovery.py` 仅保留用于读取历史实验和旧证据；新运行路径不导入它们。

科学实现只有一份：`relay_core.py`。旧 `experiment_runner.py` 的 legacy I/O wrapper 与
新 `run_sample.py` 都调用该模块；prompt、HybridPatch 单次 repair、FullRewrite、executor、
gate、evaluator、row schema、seeded relay 和模型请求参数没有复制为第二套实现。

## 唯一运行命令

指定样本：

```bash
PYTHONUTF8=1 python -u -B ./src/run_campaign.py --samples accounting1 json2 treebank4 --methods hybridpatch fullrewrite --round-trips 10 --keys-file ../.env.frkeys --key-labels KEY_1 KEY_2 KEY_3 --slots-per-key 10 --out-dir ./exp_ID
```

全量：

```bash
PYTHONUTF8=1 python -u -B ./src/run_campaign.py --all --methods hybridpatch fullrewrite --round-trips 10 --keys-file ../.env.frkeys --key-labels KEY_1 KEY_2 KEY_3 --slots-per-key 10 --out-dir ./exp_ID
```

命令中没有 `new`、`resume`、campaign role、recovery authorization、parent-loss
transaction 或 worker-confirmation 选项。

## Resume 语义

- 新 `out_dir`：写一次不可变 `run.json` 和 task plans，然后开始调度。
- 已有同科学配置目录：读取 result/checkpoint/call journal，只继续未提交方法与 RT。
- 模型、endpoint、method order、RT、seed、distractor、task plan、代码 commit、协议、
  executor 或 transport 不同：在调用 API 前拒绝。
- Key 文件、Key 值、Key label 集合、slots-per-key 和日志详细度可以变化。
- 完整 `success.json` 存在：直接 replay，不重新 POST。
- replay 前验证 sample/method/RT/direction/call kind、request SHA、`/6` terminal usage、
  finish reason、stream completion 和 transport attempt；不完整 journal 不能冒充成功。
- 调用中死亡且没有完整 `success.json`：下一次运行允许重新 POST。
- result 已有完整 forward/backward pair、checkpoint 落后：sample worker 只推进 checkpoint。
- result 的 target、initial state、edit instruction、state/rid chain 必须逐 RT 对应不可变 task plan。
- checkpoint 超前、重复 row、单边 row、跳 RT 或 JSON 损坏：仅隔离该 sample。

## 文件所有权

| 路径 | 唯一写入者 |
|---|---|
| `run.json` | 首次 dispatcher，写一次 |
| `task_plans/<sample>.json` | 首次 dispatcher，写一次 |
| `dispatch.jsonl` | 当前持有 campaign lock 的 dispatcher |
| `locks/<sample>.lock` | 对应 sample worker 持有 OS lock；文件内容无状态语义 |
| `samples/<sample>/**` | 当前持有该 sample lock 的唯一 worker |
| `reports/**` | campaign 结束后的 verifier |

运行时不产生共享 API ledger、attempt ledger、sample outcome ledger 或 metadata event
ledger。全局 API 视图由 verifier 从各 sample 的 `calls/` 确定性扫描得到。

## 锁

只有两种锁：

1. `out_dir/.campaign.lock`：阻止两个 dispatcher 调度同一目录；
2. `out_dir/locks/<sample>.lock`：阻止同一样本同时存在两个 worker。

两者都是进程持有的 OS lock，进程退出后由操作系统释放。没有 metadata lock、commit
ordering lock、stop-publication lock、共享 ledger lock 或 recovery registry lock。

## 失败分类

| 事件 | sample 状态 | campaign 行为 |
|---|---|---|
| 502/503/timeout/reset/incomplete stream | 由既有 `model_openai` 预算内重试 | 不改变其他 sample |
| retry 耗尽 | `api_incomplete` | 当前命令不无限 worker 级重试；下次同命令入队 |
| 401/403/402/明确 quota | 当前 sample 重入队，Key 在内存 quarantine | 健康 Key 接管；全失效则 `incomplete` |
| evaluator 异常 | `evaluator_failed` | 不补 0，其他 sample 继续 |
| preservation violation | `preservation_invalid` | 停止该 sample 后续 RT，其他 sample 继续 |
| 未知 Python 异常/本地 evidence 损坏 | `worker_failed` | 保存 traceback，其他 sample 继续 |
| SIGINT/SIGTERM/SIGBREAK | sample 回到 `pending`（能优雅收尾时） | 停止补位、terminate/kill、campaign `interrupted` |

普通局部错误不会写 campaign-wide fatal 或 stop latch。

## API journal

每个 semantic call 的固定目录为：

```text
samples/<sample>/<method>/calls/rtNN/<forward|backward>/<primary|repair>/
```

成功响应通过临时文件加 `os.replace` 发布为 `success.json`，包含请求身份、完整模型响应、
usage、latency、response classification、transport attempts 和 compact `/6` events。失败写入
`failures/<id>.json`。recorder 只包装现有 `model_openai.generate`，不实现第二套 HTTP retry。
worker 的 stdout/stderr 直接追加到自己的 `worker.log`，因此 import、run.json、task-plan 或
sample-lock bootstrap 失败也保留 traceback。

## 结束检查

dispatcher 在所有自己启动的 worker 结束后总会生成：

```text
reports/quick_summary.json
```

它检查状态、method checkpoint、row 数、RT pair、call journal 和失败分布。完整只读审计：

```bash
PYTHONUTF8=1 python -u -B ./src/verify_campaign.py --dir ./exp_ID --full
```

完整审计逐 step 绑定固定 journal path、call ID/kind、response、usage、finish、transport attempts
和 result row；同时按 task plan 重放 target/prompt、evaluator、score 与最终 checkpoint，报告
孤立 journal、preservation 和 HP/FR paired endpoint。它只写 `reports/verification.json`；不会
修改 runtime evidence、调用 API、回滚或补跑。quick summary 对单 sample/method 的合法 JSON
坏字段作局部 error，仍汇总其他样本；中断时不会读取仍由 external worker 持锁的 evidence。

## 禁止组件检查

以下 6 个 active runtime 文件必须通过源码扫描，确认没有导入旧 forensic dispatcher/recovery，
也不包含 active worker set、campaign stop、metadata WAL、recovery receipt 或运行中 inspector：

```text
relay_core.py
run_campaign.py
run_sample.py
simple_runtime_io.py
simple_api_recorder.py
verify_campaign.py
```

## 新组件设计记录

| 组件 | 实际故障 | 验收来源 | 为什么不能只靠最终检查 | 新共享状态 |
|---|---|---|---|---|
| `relay_core.py` | 两入口复制科学语义会漂移 | 新旧 scientific core 一致性 | prompt/row 必须在运行前一致 | 无 |
| `run_campaign.py` | Key slot、局部失败和 signal 需要调度 | 场景 1/2/4/10–14 | 最终检查不能启动/停止进程 | 仅单写者 `dispatch.jsonl` |
| `run_sample.py` | sample relay 需要唯一 owner | 场景 3–6/9/15–18 | 最终检查不能提交 RT | 仅 sample-local 文件 |
| `simple_runtime_io.py` | pair/checkpoint crash window | 场景 14/17/18/22 | 需要在下一次 POST 前决定 resume | 两种 OS lock，无共享写 |
| `simple_api_recorder.py` | 成功响应已落盘但 RT 未提交 | 场景 7–9/15/16 | 必须在 POST 前 replay | 仅 per-call 文件 |
| `verify_campaign.py` | 跨 sample 汇总不应污染热路径 | 场景 19/20 | 这是任务结束后的统一检查 | 仅 reports 输出 |

## 安全声明

本次实现和验收只运行本地零 API 测试；没有 Key probe、provider POST 或付费实验。历史
`exp_*` 目录保持只读，没有恢复、删除、移动或改写旧证据。

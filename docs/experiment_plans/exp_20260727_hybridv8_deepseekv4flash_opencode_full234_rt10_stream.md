# 实验计划：`exp_20260727_hybridv8_deepseekv4flash_opencode_full234_rt10_stream`

## 1. 身份与状态

- owner：`HP_V8 / transport`
- 实验级别：`正式主实验`
- 计划状态：`draft`
- 创建时间与时区：`2026-07-27 Asia/Singapore`
- 预期 claim role：`canonical`
- 预期 lifecycle：`complete | failed_informative`
- 约束：本计划只准备未来运行；当前代码修复阶段不启动 full234 API 实验。
- 当前审阅结论：代码与回归 blockers 已闭合；本计划仍保持 `draft / PAUSED`，
  只因本轮任务明确不启动新的 full234 API 实验。

## 2. 可证伪问题与配置纠正

- 问题：OpenCode Zen `deepseek-v4-flash` 能否以 3 个 Key、每 Key 10 个
  work-conserving FIFO 槽，完成精确 234 sample 的 HP_V8 对 FullRewrite、RT10
  paired campaign？
- 相对错误 RT2 档的两项主要实验配置纠正是：`num_round_trips: 2 -> 10`；
  `opencode_openai_compatible/3` non-stream ->
  `opencode_openai_compatible/4` stream。
- 另有 transport evidence/retry 与 Dispatcher lifecycle hardening；这些改动只修复
  传输完整性、失败隔离和错误全局中止，不改变 HP/FR 方法、prompt 或 evaluator 分数定义。
- HP 方法仍为 `hybridpatch/8`，对照仍为 `fullrewrite`；dataset、seed42、
  distractor、task-plan 生成、domain evaluator 与 scoring 不变。
- 任一 campaign 不完整只作 `failed_informative`，不把较小 n 冒充 full234。

## 3. 代码、协议与传输

- 计划运行 Git commit：启动前由 clean commit 与 immutable manifest 记录。
- endpoint：`https://opencode.ai/zen/go/v1/chat/completions`
- model：`deepseek-v4-flash`
- transport：`openai_sdk_stream`
- transport revision：`opencode_openai_compatible/4`
- request：`stream=true`，`stream_options.include_usage=true`，
  `reasoning_effort=high`，`max_completion_tokens=20000`
- 完成门：非空 finish reason 之后必须出现 usage-only terminal chunk，且
  `prompt_tokens`、`completion_tokens`、`total_tokens` 为一致的非负整数；
  partial、乱序或空 usage stream 不提交并全量重发。
- retry：生成前 503 不消耗有限 retry budget；502、生成后断流与其他 retryable
  failure 消耗预算；生成前 503 使用带 per-worker spread 的指数退避，provider
  `Retry-After` 优先，上限 300 秒。
- evidence：raw request、linear stream events、脱敏 error body/message、API terminal
  row、result/checkpoint linkage、run metadata 与 preservation latch。终端 exception
  只保留脱敏 cause，不把原始 provider body 继续链接进 worker traceback。

## 4. 数据与比较设计

- dataset：`HP_V8/data/samples_delegate52`
- split：精确 234-sample inventory；canonical sample ID-list SHA-256：
  `a3e4f063f324e201082d7481a6bcd46615fb4d63579fbed3e7def90b1cfb7b6a`；
  canonical `{sample_id: sample.json SHA-256}` map SHA-256：
  `c4017f9d8062aa3dc96b6c2f28b0ed6f51727b783b973b89e7f32208e665443a`。
- round trips：`10`
- methods：`hybridpatch`, `fullrewrite`
- seed：`42`
- distractor：on
- task plan：每 sample 10 个 seeded forward state，SHA-256 固定。
- 配对单位：sample 的 exact backward RT10 endpoint；RT1–RT10 轨迹作描述性结果。
- 缺失政策：不补 0、不插补、不修改已提交 RT；基础设施/评估器不完整保持 null。
- exposure / contamination：234/234 sample 已有 FullRewrite provider exposure，部分
  sample 有历史 HP/developer exposure；启动前复核并同步根目录与 HP_V8 的
  `data/CONTAMINATION_REGISTRY.json`。
- claim 限制：本实验即使完整也不声称 provider-unseen、method-unseen 或
  developer-unseen；`canonical` 仅表示本配置下完整、严格配对的主结果身份。

## 5. 队列与运行命令

- 目标 `out_dir`：
  `HP_V8/exp_dsv4f_hpfr_full234_rt10_stream_c10`
- Key 标签：`KEY_1 KEY_2 KEY_3`；真实 Key 只从外部 `.env.frkeys` 注入。
- 每 Key 上限：10；总上限：30。
- policy：`per_key_work_conserving_v1`；worker 结束并完成审计后同 Key 立即 FIFO
  补位，不等待 wave/batch。
- 基本 semantic call 下界：`234 × 2 methods × 10 RT × 2 directions = 9,360`；
  HybridPatch repair 与 transport retry 另计。
- 预算上限：`TBD`。
- 预计时长：`TBD`。
- launch gate：收到下一次明确启动指令，并在启动时冻结 clean Git commit、
  manifest 与 live Key 身份；本轮不启动。

```bash
cd /f/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_deepseek_opencode_20260726/HP_V8
PYTHONUTF8=1 python -B ./src/paired_campaign_dispatch.py \
  --out_dir ./exp_dsv4f_hpfr_full234_rt10_stream_c10 \
  --campaign_role deepseek_full234 \
  --num_round_trips 10 --seed 42 --slots_per_key 10 \
  --keys_file /f/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_clean/.env.frkeys \
  --key_labels KEY_1 KEY_2 KEY_3 \
  --notes "DeepSeek V4 Flash OpenCode full234 RT10 stream c10"
```

旧 RT2 `out_dir` 禁止用于这条命令。用户明确要求不执行 zero-API experiment
preflight 或 dispatcher dry-run；代码级 unit/integration regression 不属于实验 dry-run。

## 6. 停止与恢复

- preservation、Git/task-plan/manifest drift、永久 API/result/attempt linkage 损坏、
  worker authorization/PID/phase barrier 损坏与未知 worker fatal：全局 fail-closed。
- 有完整 terminal evidence 的 transport exhaustion 与 evaluator incomplete：
  sample-local，队列继续补位，campaign 最终标记 incomplete。
- operator stop：先撤销 active authorization、终止 worker、核对完整 lease scope；
  lease 未释放时不得关闭 running metadata。
- 普通 resume：有完整 sample-local terminal evidence 的 transport exhaustion 或
  audited interruption，可在同一 RT10 + `/4` manifest identity 下按常规
  `--resume --resume_reason ... --confirm_workers_stopped` 断点续跑；已提交 RT
  不重复 POST，evaluator incomplete 保持 terminal null。
- recovery authorization：只有 durable global stop、跨 commit 恢复或其他需要迁移
  frozen identity 的事故边界才使用；必须绑定原 manifest、旧/新 Git identity 与
  incident evidence。
- `dispatcher_process_lost` 使用专用恢复模式并独占 `.paired_dispatch.lock`；恢复计划
  必须覆盖每个 active worker 的 stop/launch/metadata/API/attempt/stream-sidecar
  生命周期。pending transaction 可幂等重试，且连续 parent loss 不丢失此前尚未重跑
  的 sample scope。首个 worker 仅完成登记、尚未启动而无 worker stop 时，仅允许在
  全部 worker 都是 registered-prelaunch、全部 sample lease 空闲且零 execution/API
  evidence 的情况下合成恢复 stop。迟到启动的旧 worker 不得重新锁存已恢复 campaign；
  连续 parent loss 中本次已 terminal 的 sample 必须从累计 resume scope 移除。
  emergency stop publication 与 recovery 的 snapshot→pending→archive 全事务必须由
  `.campaign_stop_publication.lock` 串行化。
- 任何 resume/recovery 都禁止把 `/3` RT2 evidence 混入 `/4` RT10 目录，也禁止在
  旧 RT2 `out_dir` 原地升级。

# 实验计划：`exp_20260727_dsv4f_hpfr_full234_rt10_stream_c10`

## 1. 身份与状态

- owner：`HP_V8`
- 实验级别：`正式主实验`
- 计划状态：`approved / READY`
- 创建时间与时区：`2026-07-27 Asia/Singapore`
- 预期 claim role：`canonical`
- 预期 lifecycle：`complete | failed_informative`
- plan author：`Codex primary agent`
- plan reviewer：`experiment_plan_review`（独立只读审阅）
- 启动授权：用户于 `2026-07-28 Asia/Singapore` 明确要求开始 full234 实验。
- 当前审阅结论：独立计划审阅为 `GO WITH FIXES`；owner、实验目录身份和
  contamination provenance、分析口径与运维证据路径修复已闭合，允许从最终
  clean commit 启动。

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
- runtime：`Python 3.11.9`、`openai 2.9.0`；正式 manifest 记录精确 Git commit、
  tree state、source fingerprint、campaign config 和 transport identity。
- prompt / executor / gate identity：HP 固定为 `hybridpatch/8`，对照固定为
  `fullrewrite`；每个 sample 的 `.task_plan.json` 及其 SHA-256、sample inventory
  hash、运行源码 fingerprint 均由首个 POST 前写入的 immutable manifest 绑定。
  启动后不得回写这些 identity。

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
  sample 有历史 HP/developer exposure。绝对 provider-unseen 为 0；本实验不得作
  method-unseen、developer-unseen 或 holdout 声明。
- registry provenance：tracked `data/CONTAMINATION_REGISTRY.json` 与
  `HP_V8/data` junction 所指
  `F:/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_clean/data/CONTAMINATION_REGISTRY.json`
  在启动前 SHA-256 均为
  `c2e622f27e712c5cb3fbba06ae0a34d944e4d889d7658933d3b95f0ee222f305`。
  它们是同一历史快照的两个路径，不是两份独立同步证据；该 2026-06-25 registry
  不能代表后续完整 exposure history。
- additive exposure overlay：`docs/active_log.md` 的 2026-07-19 exposure audit、
  `HP_V8/VERSION.md` 的 method-unseen 预注册以及冻结 FullRewrite record 共同证明
  234/234 已发生真实 provider call。本实验不引入新 sample ID；顶层 data 保持只读，
  本计划、正式 manifest 和 API ledger 构成本轮只增 exposure overlay。
- 当前可复算 overlay：
  `HP_V8/analysis/20260719_hybridv8_transportv4_mixed_confirmation100/selection.json`
  SHA-256 为
  `26da1c3d27a1eb83d70af444197afde7a7f20ade20c6e232f2d6abe605d3d654`；
  它把 `strict_any_provider_call` 绑定到 234 个 sample 及逐 sample evidence，
  evidence manifest SHA-256 为
  `da6d58211475269ec3c0d2de0bdd9aeca52d80772e5cd9eaa38592bf4767447e`。
  其中 actual method/API scan 为 61 个 sample，另有 source/developer exposure；
  这些层次不得合并成“234 个都见过 HP”的错误表述。
- claim 限制：本实验即使完整也不声称 provider-unseen、method-unseen 或
  developer-unseen；`canonical` 仅表示本配置下完整、严格配对的主结果身份。

## 5. 队列与运行命令

- 目标 `out_dir`：
  `HP_V8/exp_20260727_dsv4f_hpfr_full234_rt10_stream_c10`
- Key 标签：`KEY_1 KEY_2 KEY_3`；真实 Key 只从外部 `.env.frkeys` 注入。
- 每 Key 上限：10；总上限：30。
- policy：`per_key_work_conserving_v1`；worker 结束并完成审计后同 Key 立即 FIFO
  补位，不等待 wave/batch。
- 基本 semantic call 下界：`234 × 2 methods × 10 RT × 2 directions = 9,360`；
  HybridPatch repair 与 transport retry 另计。
- 预算上限：用户已明确授权完整 9,360 基础 semantic-call campaign；未指定额外
  dollar hard cap，实际 token、retry 与费用按 ledger 记录。
- 预计时长：provider-dependent，不预先声称固定时长。
- launch gate：用户启动指令已收到；从本计划与 Before Experiment 条目所在的最终
  clean Git commit 启动，并由 immutable manifest 冻结 live Key 标签与运行身份。

```bash
cd /f/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_deepseek_opencode_20260726/HP_V8
ops_dir=/f/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_deepseek_opencode_20260726_ops/exp_20260727_dsv4f_hpfr_full234_rt10_stream_c10
mkdir -p "$ops_dir"
nohup env PYTHONUTF8=1 PYTHONUNBUFFERED=1 python -u -B ./src/paired_campaign_dispatch.py \
  --out_dir ./exp_20260727_dsv4f_hpfr_full234_rt10_stream_c10 \
  --campaign_role deepseek_full234 \
  --num_round_trips 10 --seed 42 --slots_per_key 10 \
  --keys_file /f/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_clean/.env.frkeys \
  --key_labels KEY_1 KEY_2 KEY_3 \
  --notes "DeepSeek V4 Flash OpenCode full234 RT10 stream c10" \
  > "$ops_dir/dispatcher.console.log" 2>&1 &
printf '%s\n' "$!" > "$ops_dir/dispatcher.pid"
```

旧 RT2 `out_dir` 禁止用于这条命令。用户明确要求不执行 zero-API experiment
preflight 或 dispatcher dry-run；代码级 unit/integration regression 不属于实验 dry-run。
本次复用已完成的 HP 189/189、transport 59/59、两轮无 P0/P1 安全复审和最近
25/25 streamed live diagnostic；不重复 Key probe。3 个 Key 标签仅检查为非空，
不得在日志、plan 或终端输出真实值。

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

普通 resume 只允许在只读审计确认 dispatcher 和全部 worker 已停止后执行：

```bash
cd /f/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_deepseek_opencode_20260726/HP_V8
PYTHONUTF8=1 python -B ./src/paired_campaign_dispatch.py \
  --out_dir ./exp_20260727_dsv4f_hpfr_full234_rt10_stream_c10 \
  --campaign_role deepseek_full234 \
  --num_round_trips 10 --seed 42 --slots_per_key 10 \
  --keys_file /f/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_clean/.env.frkeys \
  --key_labels KEY_1 KEY_2 KEY_3 \
  --notes "DeepSeek V4 Flash OpenCode full234 RT10 stream c10" \
  --resume --resume_reason "<audited exact reason>" --confirm_workers_stopped
```

## 7. 预注册指标、排除与判定

- 主指标：234 个固定 sample 的 backward RT10 `RS` 配对差
  `hybridpatch - fullrewrite`。sample-level paired bootstrap 固定 10,000 次、
  seed42、nearest-rank percentile 95% CI；同时报告 exact two-sided sign test，
  ties tolerance 固定 `1e-12`。
- 辅助指标：RT1–RT10 配对均值轨迹、RT10 W/L/T、CriticalFailure 转移、
  median/分位数、context kept/route/partial acceptance telemetry、
  token/latency/retry/费用。CriticalFailure 沿用冻结定义：相邻 backward RS
  下降 `>= 0.10`，或从正值坍塌为 0。
- 完整性与安全指标：234/234 sample scope；每个方法每个 sample 为
  `10 RT × 2 directions = 20` 个 committed result rows；preservation violation
  必须为 0；manifest/task-plan/API/result/attempt linkage 必须完整。
- 不作 score-based 或事后 sample 排除。`model_empty` 是模型结果而不是 transport
  retry 条件，保留在方法结果；有 durable evidence 的 infrastructure/evaluator
  incomplete 保持 null。
- 若 234 固定 scope 未完整，不发布 canonical fixed-n 结论；只允许明确标注的
  complete-pair supporting view。
- 判定：fixed-n 完整且 RT10 配对差置信区间整体高于 0 为支持 HP；整体低于或等于
  0 为不支持/反向；跨 0 为不确定。任何 campaign-level integrity/preservation
  failure 或缺失均为 `failed_informative`，不以较小 n 替代。
- representative cases 仅按预注册规则描述：绝对 RT10 配对差最大者与
  CriticalFailure 状态发生转移者；不用于改变主结论。

## 8. 监控、证据路径与归档

- canonical `out_dir` 必须由 dispatcher 自身从不存在状态创建；后台运维文件不得
  预先放入该目录。dispatcher stdout/stderr 与 PID 放在仓库和实验目录之外：
  `F:/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_deepseek_opencode_20260726_ops/exp_20260727_dsv4f_hpfr_full234_rt10_stream_c10/dispatcher.console.log`
  与同目录 `dispatcher.pid`。
- durable 证据：`dispatch_manifest.json`、`active_worker_set.json`、
  `dispatch_log.jsonl`、`dispatch_logs/`、`api_calls.jsonl`、
  `api_attempt_ledger.jsonl`、`run_metadata.jsonl`、`sample_outcomes.jsonl`、
  两方法 JSONL/checkpoint、`campaign_stop.json`、emergency stop 与
  preservation latch。
- 只读快照命令：

```bash
cd /f/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_deepseek_opencode_20260726/HP_V8
PYTHONUTF8=1 python -B ./src/monitor_experiment.py \
  --dir ./exp_20260727_dsv4f_hpfr_full234_rt10_stream_c10 \
  --methods hybridpatch fullrewrite --target_round_trips 10
```

- 启动后前 10 分钟每 2 分钟检查，随后每 10 分钟检查。监控只读，不调用 API、
  不启动/重启/恢复实验、不改证据；进程停止且不完整时立即报告精确 stop condition
  和证据位置。
- 主完成度以 checkpoint/committed rows 为准；API terminal row 可先于 RT commit，
  不能单独作为完成百分比。DeepSeek retry 读取 transport attempt 的 HTTP status、
  body 分类、`retry_budget_consumed`、`retry_budget_attempt_index`。
- 完成后先做 postprocess/records-only 验证，再将原始目录归档到私有
  `hybridpatch_private_archives`，Tier-1 records 写入 `HP_V8/records/`；公开 release
  不包含 credentials、raw API body 或 `exp_*` 原始目录。

# 实验计划：`exp_20260728_dsv4f_hpfr_full234_rt10_stream_c10_v5`

## 身份与目标

- owner：`HP_V8`
- role：正式 `deepseek_full234`
- lifecycle：`complete | failed_informative`
- scope：精确 234 sample，`hybridpatch/8` 对 `fullrewrite`
- grid：seed42、distractor-on、共享 task plan、RT10、forward/backward
- 启动授权：用户要求在修正 stream terminal usage 兼容后重新开始全量实验

本实验替代失败的
`exp_20260727_dsv4f_hpfr_full234_rt10_stream_c10`。旧目录固定为
`opencode_openai_compatible/4` 的 failed-informative transport evidence，
不修改、不 resume、不拼接。

## Provider 与 transport

- endpoint：`https://opencode.ai/zen/go/v1/chat/completions`
- model：`deepseek-v4-flash`
- transport：`openai_sdk_stream`
- revision：`opencode_openai_compatible/5`
- request：`stream=true`、`stream_options.include_usage=true`、
  `reasoning_effort=high`、`max_completion_tokens=20000`

`/5` 的成功 attempt 必须同时具有：

1. 唯一 choice 上非空、非纯空白字符串 `finish_reason`；
2. 完整一致的 terminal usage；
3. 单调终结序列和 `stream_complete=true`。

terminal usage 可以与 finish 位于同一 choice chunk，也可以位于随后的标准
usage-only chunk。同块 usage 后只允许 `choices=[]、usage=None` 的非生成元数据。
缺失、提前、重复或畸形 usage，无/空白 finish，finish 后新 choice，以及 partial
EOF/SDK exception 都分类为 `incomplete_stream`，丢弃 partial 并按既有预算全量重发。

502、503、`Retry-After`、脱敏错误证据与有限 retry 规则沿用 `/4`：
生成前 503 不消耗有限 retry budget；502、生成后断流及其他 retryable failure
消耗预算。方法、prompt、evaluator、scoring 和 token/cost schema 不变。

## 调度与命令

- Key 标签：`KEY_1 KEY_2 KEY_3`，只从外部 `.env.frkeys` 注入
- slots：每 Key 10，总上限 30
- policy：`per_key_work_conserving_v1`，同 Key FIFO 即时补位
- out_dir：
  `HP_V8/exp_20260728_dsv4f_hpfr_full234_rt10_stream_c10_v5`
- ops：
  `F:/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_deepseek_opencode_20260726_ops/exp_20260728_dsv4f_hpfr_full234_rt10_stream_c10_v5`

```bash
cd /f/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_deepseek_opencode_20260726/HP_V8
ops_dir=/f/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_deepseek_opencode_20260726_ops/exp_20260728_dsv4f_hpfr_full234_rt10_stream_c10_v5
mkdir -p "$ops_dir"
nohup env PYTHONUTF8=1 PYTHONUNBUFFERED=1 python -u -B ./src/paired_campaign_dispatch.py \
  --out_dir ./exp_20260728_dsv4f_hpfr_full234_rt10_stream_c10_v5 \
  --campaign_role deepseek_full234 \
  --num_round_trips 10 --seed 42 --slots_per_key 10 \
  --keys_file /f/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_clean/.env.frkeys \
  --key_labels KEY_1 KEY_2 KEY_3 \
  --notes "DeepSeek V4 Flash OpenCode full234 RT10 stream c10 transport-v5" \
  > "$ops_dir/dispatcher.console.log" 2>&1 &
printf '%s\n' "$!" > "$ops_dir/dispatcher.pid"
```

用户已明确免除 dispatcher dry-run、统一 zero-API preflight 和重复 Key probe。
启动前只要求相关代码级回归通过、tree clean、commit 固定、目标 out_dir 不存在、
3 个 Key 标签非空且没有旧 dispatcher/worker 存活。

## 完整性、停止与报告

- canonical 完整性：234/234 sample；每方法每 sample 10 RT，forward/backward
  API/result/checkpoint linkage 完整；preservation=0。
- infrastructure/evaluator incomplete 保持 null，不补 0、不替换 sample。
- preservation、Git/task-plan/manifest drift、重复/半提交、永久 ledger/linkage
  损坏、授权屏障损坏或未知 worker fatal 才全局 fail-closed。
- 未达到 234/234 时只作 `failed_informative`，不以 complete subset 替代 fixed-n
  主结论。
- 启动后前 10 分钟每 2 分钟只读检查，随后每 10 分钟检查。监控不调用 API、
  不启动/重启/恢复/中止实验，不修改任何 evidence。
- 监控重点：active/per-Key running/pending、sample outcomes、HP/FR committed rows
  与各 RT、HTTP/502/503/retry、terminal usage mode、partial/model_empty、
  Go URL、high reasoning、stop/preservation。

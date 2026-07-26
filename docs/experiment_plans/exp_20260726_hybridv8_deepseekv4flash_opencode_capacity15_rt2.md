# 实验计划：`exp_20260726_hybridv8_deepseekv4flash_opencode_capacity15_rt2`

## 1. 身份与状态

- owner：`HP_V8 / transport`
- 实验级别：付费容量 smoke
- 计划状态：`reviewed`
- 创建时间与时区：`2026-07-26 Asia/Singapore`
- 计划作者：Codex 主 Agent
- 计划审阅子代理：`deepseek_plan_review`
- 审阅结论：`GO WITH FIXES`；用户随后明确要求跳过 zero-API/dry-run 手续，
  直接以容量实验暴露实现或 provider 问题。
- 预期 claim role：`diagnostic_only`
- 预期 lifecycle：`complete | failed_informative`

## 2. 可证伪问题与决策规则

- 问题：OpenCode Zen 的 `deepseek-v4-flash` 是否能由单个 Key 稳定承载
  15 个同时授权的 HP_V8 paired worker，并在所有实际请求中保留
  `reasoning_effort=high`？
- 假设：15 个 worker 均完成 2 RT 的 HybridPatch 与 FullRewrite，不出现
  401/402/403、bounded retry 后仍为 terminal 的 429/5xx/timeout、共享完整性
  错误或 preservation violation。
- 正向门：初始 `queue_refill` 和 active-worker 证据均为 15；15/15 sample
  完成；每一条 API ledger、raw request 与 result row 均审计为 OpenCode Zen、
  `deepseek-v4-flash`、`reasoning_effort=high`；preservation=0。
- 负向门：任一 worker 因 provider/Key/transport 终态失败，或未达到完整
  15/15，即停止，不进入全量实验。
- 说明：这里验证的是 15 个并发 worker 的承载能力；worker 内请求是顺序的，
  不把“15 worker”夸称为 15 个 HTTP socket 在每一时刻都同时 in-flight。

## 3. 固定代码与配置

- 计划运行 Git commit：包含本计划的 clean commit，由 immutable dispatch
  manifest 和 `run_metadata.jsonl` 记录。
- 方法：`HP_V8 hybridpatch/8` 与 `fullrewrite`；不改 prompt、executor、gate、
  evaluator、scoring 或 preservation 语义。
- transport revision：`opencode_openai_compatible/1`
- provider / endpoint：OpenCode Zen，
  `https://opencode.ai/zen/v1/chat/completions`
- model：`deepseek-v4-flash`
- API：OpenAI-compatible non-stream；SDK 内建 retry 关闭，wrapper 最多
  3 个可见 HTTP attempts。
- max tokens：`20000`
- reasoning effort：固定 `high`，同时审计 SDK 参数、raw request、API ledger、
  run metadata 与 result row。
- seed：`42`
- distractor：on
- methods：每个 sample 的 `hybridpatch` 与 `fullrewrite`，首发顺序按 sample
  交替以降低方法顺序偏差。
- stop policy：preservation、Git/task-plan drift、重复/半提交或无法映射的 API
  evidence、共享账本错误全部全局 fail closed。

## 4. 样本与比较设计

- dataset：`HP_V8/data/samples_delegate52`
- round trips：`2`
- 配对单位：同 sample、同 seed task plan、同 RT、同 direction。
- 选择规则：每个 `sample_type` 先取
  `basic_state_num_tokens + distractor_context.num_tokens` 最大的 sample，再按该
  值降序取前 15；这是固定的 worst-context 容量压力集合，不用于方法质量推断。
- 固定顺序：
  `earncall1 latex6 screenplay4 dbschema1 circuit4 json4 vector2 fonteng1
  treebank1 genealogy6 jobboard6 python2 obj3d4 mathlean3 molecule2`
- 缺失政策：不补 0；任何未完成项保持 null，并使容量门失败。
- exposure：这些数据及方法已有历史曝光；本实验仅诊断 provider/transport
  兼容性与容量。

## 5. 命令、Key 与预算

- out dir：`HP_V8/exp_20260726_hybridv8_deepseekv4flash_opencode_capacity15_rt2`
- Key：仅 `KEY_1`；值仅从原工作区 `.env.frkeys` 注入，不复制、不打印。
- 并发：单 Key 15 worker，`slots_per_key=15`，稳定 FIFO。
- 预算上限：USD 3；预计主调用下界 120 次（repair 另计但仍受预算与审计约束）。

```bash
cd /f/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_deepseek_opencode_20260726
python ./tools/preflight_experiment.py \
  --experiment ./HP_V8/exp_20260726_hybridv8_deepseekv4flash_opencode_capacity15_rt2 \
  --plan ./docs/experiment_plans/exp_20260726_hybridv8_deepseekv4flash_opencode_capacity15_rt2.md \
  --evaluator-jobs 8 -- \
  --campaign_role deepseek_capacity15 \
  --samples earncall1 latex6 screenplay4 dbschema1 circuit4 json4 vector2 fonteng1 treebank1 genealogy6 jobboard6 python2 obj3d4 mathlean3 molecule2 \
  --num_round_trips 2 --seed 42 --slots_per_key 15 \
  --keys_file /f/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_clean/.env.frkeys \
  --key_labels KEY_1 \
  --notes "DeepSeek V4 Flash OpenCode single-Key capacity15 RT2"

cd HP_V8
python -B ./src/probe_deepseek_opencode_keys.py \
  --keys_file /f/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_clean/.env.frkeys \
  --key_labels KEY_1
python -B ./src/paired_campaign_dispatch.py \
  --out_dir ./exp_20260726_hybridv8_deepseekv4flash_opencode_capacity15_rt2 \
  --campaign_role deepseek_capacity15 \
  --samples earncall1 latex6 screenplay4 dbschema1 circuit4 json4 vector2 fonteng1 treebank1 genealogy6 jobboard6 python2 obj3d4 mathlean3 molecule2 \
  --num_round_trips 2 --seed 42 --slots_per_key 15 \
  --keys_file /f/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_clean/.env.frkeys \
  --key_labels KEY_1 \
  --notes "DeepSeek V4 Flash OpenCode single-Key capacity15 RT2"
```

## 6. Preflight、监控与保留

- [x] 独立计划审阅为 `GO WITH FIXES`，阻塞意见已披露；用户明确授权直接实跑。
- [ ] clean commit / clean tree。
- [x] 用户明确取消 unified zero-API preflight 与 dispatcher dry-run。
- [x] 容量实验自身兼作 `KEY_1` 存活及并发探测，不另发前置 probe。
- [ ] 实验目录不含 `.env*`。
- 监控：每 30 秒检查 running/pending/completed、Key 槽位、最新 API 终态、
  preservation stop latch、worker console log 与 run metadata；进程结束后做 strict
  complete inspection。
- 原始 API/request/response/console/ledger 只保存在私有工作区，发布时必须
  credential/privacy scan。
- resume：本容量门默认不恢复；失败保留为 `failed_informative`，另开新编号。

## 7. 审阅记录

- verdict：`GO WITH FIXES`
- observed facts：OpenCode Zen/high 主链、capacity15 固定网格与 per-Key FIFO 已闭合。
- confounds and contamination：容量实验仅作 transport/provider 诊断。
- missing reproducibility fields：缺独立 selection manifest；固定样本列表已写入计划和代码。
- metric and exclusion risks：不完整不得转成较小 n 结论。
- required fixes：reviewer 要求 full 启动硬绑定 capacity/probe、实现预算硬门并统一
  DeepSeek provider failure 语义；用户选择让实跑先暴露问题。
- evidence locations：`HP_V8/src/paired_campaign_dispatch.py`,
  `HP_V8/src/model_openai.py`, `HP_V8/src/run_meta.py`。
- 主 Agent 最终 GO/NO-GO：`GO`，依据用户在审阅结论披露后作出的直接实跑指令。

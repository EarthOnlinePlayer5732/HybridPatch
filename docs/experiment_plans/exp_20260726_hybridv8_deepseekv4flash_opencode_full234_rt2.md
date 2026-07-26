# 实验计划：`exp_20260726_hybridv8_deepseekv4flash_opencode_full234_rt2`

## 1. 身份与状态

- owner：`HP_V8 / transport`
- 实验级别：正式 provider-compatibility paired campaign
- 计划状态：`reviewed`
- 创建时间与时区：`2026-07-26 Asia/Singapore`
- 计划作者：Codex 主 Agent
- 计划审阅子代理：`deepseek_plan_review`
- 审阅结论：`GO WITH FIXES`；用户明确取消 zero-API/dry-run 手续并要求让实跑
  暴露问题。全量仍遵守用户原始条件：capacity 完整通过且至少两个 Key 存活。
- 预期 claim role：`supporting`
- 预期 lifecycle：`complete | failed_informative`

## 2. 可证伪问题与启动门

- 问题：在单 Key 15-worker 容量门通过后，OpenCode Zen 的多个存活 Key 能否以
  每 Key 15 个槽、work-conserving FIFO 完成全部 234 sample 的 HP_V8 对
  FullRewrite、2 RT paired campaign，并保持 DeepSeek high reasoning 的完整审计？
- 启动门：
  1. capacity15 实验 15/15 complete 且 strict inspection PASS；
  2. 对 `KEY_1..KEY_4` 逐个最小 probe，至少两个 Key 存活；
  3. 只把 probe 成功的物理唯一 Key 加入最终命令。
- 正向结论：完整 234/234 paired sample、每臂每 sample 2 RT、无插补、
  preservation=0，且运行时/provider/reasoning 证据一致。
- 负向或不确定：任何不完整 campaign 仅作 supporting/diagnostic evidence，
  不改成较小 n 的“全量”；Key/transport 终态失败保留 null 和原始证据。

## 3. 固定代码与配置

- 计划运行 Git commit：包含本计划的 clean commit，由 immutable dispatch
  manifest 和 `run_metadata.jsonl` 记录。
- 方法：`HP_V8 hybridpatch/8` 与 `fullrewrite`；方法语义冻结。
- provider / endpoint：OpenCode Zen Go，
  `https://opencode.ai/zen/go/v1/chat/completions`
- model：`deepseek-v4-flash`
- transport revision：`opencode_openai_compatible/2`
- API：OpenAI-compatible non-stream；SDK retry=0，wrapper 最多 3 个可见 attempts。
- max tokens：`20000`
- reasoning effort：固定 `high`，审计 SDK 参数、raw request、API ledger、
  run metadata 与 result row。
- seed：`42`
- distractor：on
- task plan：每 sample 由 seed42 预生成、SHA-256 固定。
- repair：HybridPatch 仅沿既有一次应用层 repair 规则；FullRewrite 不 repair。
- stop：preservation、Git/task-plan drift、重复/半提交、不可映射 evidence 或
  shared-integrity 错误全局 fail closed。

## 4. 数据与比较设计

- dataset：`HP_V8/data/samples_delegate52`
- split：当前精确 234-sample inventory；dispatcher canonical ID-list SHA-256
  `a3e4f063f324e201082d7481a6bcd46615fb4d63579fbed3e7def90b1cfb7b6a`，
  `{sample_id: sample.json SHA-256}` manifest SHA-256
  `c4017f9d8062aa3dc96b6c2f28b0ed6f51727b783b973b89e7f32208e665443a`。
- round trips：`2`
- methods：`hybridpatch`, `fullrewrite`
- 比较：同 sample、同 task plan、同 RT、同 direction 的完整配对。
- 方法顺序：assignment 内 HP-first 与 FR-first 交替；Key 内稳定 FIFO。
- exposure：234/234 已有 FullRewrite provider exposure，部分 sample 有历史
  HP/developer exposure；因此不声称 provider-unseen 或 method-unseen。
- 排除：无预注册排除。
- 缺失：不补 0，不跨 Key 重放已提交步骤；不完整保持 null，campaign 标为
  incomplete。

## 5. 队列、并发、Key 与预算

- out dir：`HP_V8/exp_20260726_hybridv8_deepseekv4flash_opencode_full234_rt2`
- 候选标签：`KEY_1 KEY_2 KEY_3 KEY_4`；最终只使用 probe 成功者，至少 2 个。
- 每 Key `slots_per_key=15`。若 4 个均存活，最大 60 个并发 worker，初始
  pending 174；若 3 个存活则最大 45、pending 189；若 2 个则最大 30、
  pending 204。
- dispatcher：`per_key_work_conserving_v1`。每个 Key 有稳定 FIFO；该 Key 任一
  worker 结束并完成合并审计后立即补 1 个，不等待其他 Key 或整批清空。
- 预算上限：USD 20。基本调用下界 1,872 次；repair 另计。达到预算或已知 usage
  gate 时停止，不以不完整结果替代全量。

```bash
cd /f/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_deepseek_opencode_20260726/HP_V8
python -B ./src/probe_deepseek_opencode_keys.py \
  --keys_file /f/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_clean/.env.frkeys \
  --key_labels KEY_1 KEY_2 KEY_3 KEY_4

# 下列标签在实际启动时替换为本次 probe 的全部 ALIVE 标签；至少两个。
python -B ./src/paired_campaign_dispatch.py \
  --out_dir ./exp_20260726_hybridv8_deepseekv4flash_opencode_full234_rt2 \
  --campaign_role deepseek_full234 \
  --num_round_trips 2 --seed 42 --slots_per_key 15 \
  --keys_file /f/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_clean/.env.frkeys \
  --key_labels KEY_1 KEY_2 KEY_3 KEY_4 \
  --notes "DeepSeek V4 Flash OpenCode full234 RT2 work-conserving"
```

最终 live 命令前必须用相同的存活标签重新执行 unified zero-API preflight：

```bash
cd /f/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_deepseek_opencode_20260726
python ./tools/preflight_experiment.py \
  --experiment ./HP_V8/exp_20260726_hybridv8_deepseekv4flash_opencode_full234_rt2 \
  --plan ./docs/experiment_plans/exp_20260726_hybridv8_deepseekv4flash_opencode_full234_rt2.md \
  --evaluator-jobs 8 -- \
  --campaign_role deepseek_full234 \
  --num_round_trips 2 --seed 42 --slots_per_key 15 \
  --keys_file /f/Code/AnchorPatch_goal_promptgen_20260624/hybridpatch_clean/.env.frkeys \
  --key_labels KEY_1 KEY_2 KEY_3 KEY_4 \
  --notes "DeepSeek V4 Flash OpenCode full234 RT2 work-conserving"
```

## 6. 监控、失败隔离与保留

- 每 30 秒检查每 Key running/pending、全局 completed/incomplete、slot release 与
  refill、API retry/HTTP 状态、reasoning audit、preservation latch 和 worker exit。
- work-conserving 验收：只要某 Key pending>0 且没有审计/启动屏障，该 Key 的
  running 应在补位周期后恢复到 15；禁止 cross-key wave barrier。
- DeepSeek bounded retry 后的 terminal provider failure 按当前实现全局停止；
  不复用 MiniMax R2/I3 `infrastructure_incomplete` 语义，也不自动换 Key 重发。
- evaluator incomplete 只隔离对应 sample，保持 null；共享完整性或 preservation
  仍全局停止。
- resume 只可在独立恢复授权下从首个未提交 RT 继续；已提交 forward/backward
  禁止重复 POST。
- raw request/response/API ledger/console log 为私有证据；发布前做 credential 和
  privacy scan，实验目录禁止 `.env*`。

## 7. Preflight 与审阅

- [ ] capacity15 strict PASS。
- [x] 独立计划审阅为 `GO WITH FIXES`；意见已向用户披露。
- [ ] clean commit / clean tree。
- [ ] 全部候选 Key probe，至少两个存活且标签已固定。
- [x] 用户明确取消 unified zero-API preflight、dry-run 和额外手续。
- [ ] 实验目录中没有 `.env*`。

审阅记录：

- verdict：`GO WITH FIXES`
- observed facts：待定
- confounds and contamination：已知历史 provider/method/developer exposure，不能作
  unseen claim。
- missing reproducibility fields：probe 尚无机器可绑定 receipt；本轮由主 Agent 读取
  每个标签的即时输出并只传入 ALIVE 标签。
- metric and exclusion risks：terminal provider failure 全局停止；不完整保持 null。
- required fixes：reviewer 建议代码硬绑定 capacity/probe 和预算门；用户明确选择先实跑。
- evidence locations：`HP_V8/src/paired_campaign_dispatch.py`,
  `HP_V8/src/probe_deepseek_opencode_keys.py`。
- 主 Agent 最终 GO/NO-GO：capacity 完整 PASS 且至少两个 Key probe ALIVE 后 `GO`。

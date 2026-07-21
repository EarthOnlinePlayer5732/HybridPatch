# 实验计划：`exp_20260721_hybridv8_transportv4_remaining134_hp_then_fr`

## 身份、范围与研究问题

- owner：`HP_V8`
- level / claim role：正式补充进阶实验 / `supporting_phase_ordered_remaining_scope`
- 用户授权：从当前 inventory 的 234 个样本中排除上一轮 mixed confirmation100 计划里的全部 100 个样本（包括其中 2 个未完成样本），只运行集合差的 134 个样本；是否 134/134 runtime-runnable 以本轮 preflight receipt 为准。
- 研究问题：在 MiniMax-M3、transport-v4、相同 seed/task plan/distractor 下，HP_V8 与 FullRewrite 在剩余 134 个样本的 RT1–RT10 表现、失败、协议与成本如何？
- 可证伪假设：HP_V8 在完整配对样本上的 RT10 backward RS 高于 FullRewrite，且 preservation violations 为 0。任何缺失 endpoint 保持 null，不补 0，也不替换样本。
- 计划变化：相对 mixed confirmation100，只改变样本范围、Key 数和用户指定的全局方法阶段顺序。HP_V8 方法、prompt、`hybridpatch/8`、executor、validation gate、FullRewrite、transport-v4、evaluator、scoring、seed、distractor 与 10RT 全部冻结。
- 结论边界：全局先 HP、后 FR 造成方法与运行时间/服务状态的系统性混杂。本实验可作为同任务计划的补充描述证据，但不能替代方法顺序平衡的 canonical 因果比较。

## API 前冻结范围

- 全集：`HP_V8/data/samples_delegate52` 中 234 个带 `sample.json` 的目录，按名称排序。
- 排除源：`HP_V8/analysis/20260719_hybridv8_transportv4_mixed_confirmation100/selection.json`；固定 SHA-256 `26da1c3d27a1eb83d70af444197afde7a7f20ade20c6e232f2d6abe605d3d654`；排除 100 个 unique sample，不因其中 2 个未完成而重新纳入。
- 剩余范围：134 个 unique sample，与排除集交集为 0、并集为 234；canonical sample-ID list SHA-256 `835297a349489dd6c69224ad17ae5a4b767cc3da7bc0baef4b6c2f55c2b99ad4`。
- 剩余 134 个 `sample.json` 内容清单 SHA-256：`d5c88f7d976a1fb6f3b401a57e8124410e82ed699e57434f1ab4200c596b6f18`。
- 排除 ID list SHA-256：`c293b5d14008d1dfee17e21787c0f71a327cccde9110518d048a95369bace52b`。
- dispatcher 的 `remaining134` role 是唯一范围权威：不接受 `--samples`，运行时重新核对上述 selection 文件、集合算术、数量和样本内容摘要，并把完整 ID 与 task-plan SHA 写入 manifest。
- 本轮没有 reserve 或结果后替换；任一 evaluator-incomplete 样本在本 campaign 内终止，其后续 FR 不启动。
- exposure 边界：冻结 FullRewrite baseline 已调用 234/234，本轮不是 provider-unseen；remaining134 中也包含既有 HP/method exposure。顶层 `data/` 继续冻结，不回写历史 registry；首次 HP POST 后，以本计划、committed exclusion source 和 immutable campaign manifest 作为本轮 134-sample exposure overlay。
- exposure 构成：21 个来自 mixed confirmation 的 HP method/developer-unseen reserve；69 个来自 historical HP/method-exposed reserve；另 44 个是最近 paired10/supplement40 实际 HP campaign 的 unique sample。因此按既有口径是 21 method-unseen、113 HP/method-exposed，且 134/134 均已有 FullRewrite provider exposure。

## 固定运行配置

- methods：阶段 1 仅 `hybridpatch`；134 个样本全部达到 finished/evaluator-incomplete 终态且无 infrastructure-incomplete 后，写入持久 `method_phase_complete` 屏障；阶段 2 才允许 `fullrewrite`。
- FR 启动门：每个 FR refill 在创建子进程前都必须验证同一 commit、完整 134-sample scope、preservation=0 的唯一 HP 阶段屏障。没有屏障时 provider POST 为 0。
- model/provider：MiniMax-M3 / OpenCode Go；adaptive thinking；max tokens 131072。
- transport：`opencode_anthropic_sdk/4`；每个语义调用的 R2/I3 transport 预算和独立 HP repair 预算不变。
- seed / RT / distractor：42 / 10 / on；两方法对同一样本使用同一冻结 task plan。
- concurrency：14 个物理唯一 Key，每 Key 最大 4 个 worker；每个方法阶段最大并发 56。每 Key 使用稳定 FIFO 并在槽位释放后立即补位，不等待波次清空。
- 队列分配：8 个 Key 各 10 个样本、6 个 Key 各 9 个样本；每阶段 134 个 worker，共 268 个 worker invocation。
- phase recovery：HP infrastructure-incomplete 时不启动 FR；恢复只补该阶段未提交步骤。HP evaluator-incomplete 保持 null 并从 FR eligible scope 排除。FR 恢复不重发已提交 HP 或 FR round trip。
- identity：首次 POST 前 clean commit/tree；manifest、run metadata 和 phase barrier 固定 commit、code fingerprint、transport revision、样本、seed、task plans 和命令身份。

## 指标与缺失政策

原始 0–1 数据不修改；派生报告乘 100，差值为百分点。报告：

1. 固定完整配对 scope 的 RT1–RT10 HP、FR、差值与 n；若不是 134，明确标注 complete-pair n 和计划 n=134 的缺失。
2. RT10 逐样本 HP、FR、差值；W/L/T、CriticalFailure@0.10、均值、中位数、IQR、MAD 和差值集中度。
3. HP route、prompt profile、repair、protocol failure、partial acceptance、soft burden telemetry 和 preservation。
4. method/call_kind token、known USD、缺 final usage attempt；known USD 不是 provider billing statement。
5. 结果行/checkpoint/API ledger/journal 完整性、重复或半提交 RT、incomplete sample 与私有归档 SHA-256。

evaluator 输出的显式 error row 继续按冻结 scoring 处理并单列。provider/transport 或 evaluator 导致的未提交步骤不写结果行、不补 0。只允许事先定义的 complete-pair 派生视图，不得把缺失样本描述为完成的固定 n=134。

## 停止、隔离与恢复

- sample-local：严格证据闭合的 provider/transport exhaustion 记 `infrastructure_incomplete`；同阶段其他 worker 继续，阶段结束后 dispatcher 返回 incomplete。evaluator exception 记 `evaluator_incomplete`，取消该 sample 后续阶段并继续队列。
- campaign-global：preservation violation；Git/tree/task-plan/phase-barrier drift；重复或半提交结果；API ledger 无法映射；未捕获 runner/local/evaluator 异常；共享 executor/data/evaluator integrity 错误。
- resume：同一实验只能在代码和 transport 身份未变时直接恢复；只启动 current phase 的 pristine pending、经 stale invocation/lease/task-plan/checkpoint/atomic-prefix 审计的断电中断样本，或严格授权 infrastructure-incomplete；已提交 HP/FR forward/backward 均不产生新 provider POST。开放/损坏且无法闭合的 transport lineage 仍 fail closed。

## 预算与开跑门

- primary edit steps：5,360（134×10 RT×2 directions×2 methods），另有实际 HP repair 与 transport retries。
- 预算来源：`HP_V8/exp_20260719_hybridv8_transportv4_mixed_confirmation100/analysis/confirmation100.json` 的 planned100 campaign 已知 HP/FR usage 为 USD `71.0466198 + 55.3962333 = 126.4428531`（98 个完整 pair，另 2 个部分运行）。按 planned sample 数线性投影 `126.4428531 × 134 / 100 = 169.4334232`，故预期 known cost 约 USD 170、总体规划上限 USD 220；14 Key 平均预期约 USD 12.10/Key，建议每 Key 至少保留约 USD 20 可用额度。该投影包含 survivorship/部分运行偏差，失败且无 final usage 的 attempt 可能不在 known cost 中。
- 不因软协议负担阈值停止；preservation 和完整性停止条件仍强制。

首次正式 POST 前：

- [ ] 本计划、dispatcher、测试和 VERSION/active log 形成 clean commit 并推送当前 PR 分支。
- [x] 独立只读实验计划审阅最终 verdict=`GO`；首次 `GO WITH FIXES` 指出的断电 prefix resume、21/113 exposure、复现命令和 phase evaluator 测试均已闭合。
- [ ] `tools/preflight_experiment.py` PASS：版本回归、remaining134 dispatcher dry-run、134/134 runtime evaluator。
- [ ] 14/14 Key 独立最小完整流 probe PASS；只记录 label/状态，不输出凭据。
- [ ] 最终命令、out_dir、commit/tree、selection SHA、scope SHA、task plans、模型和 transport-v4 复核。

## 正式运行命令

从 `HP_V8/` 使用 Git Bash：

```bash
PYTHONUTF8=1 OPENCODE_TRANSPORT=anthropic_sdk_v2 MINIMAX_TRANSPORT=opencode \
MINIMAX_HARD_TIMEOUT=7200 python -u src/paired_campaign_dispatch.py \
  --campaign_role remaining134 \
  --out_dir exp_20260721_hybridv8_transportv4_remaining134_hp_then_fr \
  --num_round_trips 10 --seed 42 --slots_per_key 4 \
  --keys_file ../.env.frkeys \
  --key_labels KEY_01 KEY_02 KEY_03 KEY_04 KEY_05 KEY_06 KEY_07 \
    KEY_08 KEY_09 KEY_10 KEY_11 KEY_12 KEY_13 KEY_14 \
  --notes 'HP_V8 transport-v4 remaining134 global HP then FR'
```

## Preflight、Key probe 与恢复命令

从仓库根使用 Git Bash 运行统一零 API preflight；`--` 后不重复传 `--out_dir` 或 `--dry_run`：

```bash
PYTHONUTF8=1 python ./tools/preflight_experiment.py \
  --experiment ./HP_V8/exp_20260721_hybridv8_transportv4_remaining134_hp_then_fr \
  --plan ./docs/experiment_plans/exp_20260721_hybridv8_transportv4_remaining134_hp_then_fr.md \
  --evaluator-jobs 8 -- \
  --campaign_role remaining134 --num_round_trips 10 --seed 42 \
  --slots_per_key 4 --keys_file ../.env.frkeys \
  --key_labels KEY_01 KEY_02 KEY_03 KEY_04 KEY_05 KEY_06 KEY_07 \
    KEY_08 KEY_09 KEY_10 KEY_11 KEY_12 KEY_13 KEY_14 \
  --notes 'HP_V8 transport-v4 remaining134 global HP then FR'
```

receipt 固定保存到
`HP_V8/exp_20260721_hybridv8_transportv4_remaining134_hp_then_fr/preflight/preflight_receipt.json`。
随后运行 14-Key 最小完整流 probe，预期标签恰为 `KEY_01`–`KEY_14`：

```bash
set -o pipefail
python -B ./HP_V8/src/probe_fr_keys.py --keys_file ./.env.frkeys \
  --timeout 180 --max_tokens 1024 \
  | tee ./HP_V8/exp_20260721_hybridv8_transportv4_remaining134_hp_then_fr/preflight/key_probe.log
```

若 dispatcher 因样本级 infrastructure-incomplete 或主机中断返回 incomplete，确认全部旧 worker
已停止且代码/transport 身份不变后，从 `HP_V8/` 使用同一正式命令并追加：

```bash
--resume \
--resume_reason 'resume incomplete current method phase; workers audited stopped' \
--confirm_workers_stopped
```

resume dry-run/preflight 使用相同追加参数；不得删除旧 attempt、checkpoint、outcome 或 dispatch log。

## 实验后

确认 worker 全停后，从仓库根执行：

```bash
python ./tools/postprocess_experiment.py closeout \
  --experiment ./HP_V8/exp_20260721_hybridv8_transportv4_remaining134_hp_then_fr \
  --archive ../hybridpatch_private_archives/exp_20260721_hybridv8_transportv4_remaining134_hp_then_fr_raw.tgz \
  --confirm-stopped
```

随后进行结果完整性审计、结果与失败分析、结论边界审阅、人工 `record_review.yaml`、finalize、
validators、凭据扫描与私有 raw 归档。evaluator-incomplete、infrastructure-incomplete 和模型/
协议失败必须分开报告。更新 active log、research journal、适用 findings、VERSION 和 PR；不根据
结果修改 HP_V8，不自动运行被排除的 100 个样本。

## 计划审阅

独立只读子代理最终 verdict=`GO`，remaining fixes=无。复核确认 234−100=134 与四个 SHA、
14-Key 队列、HP→FR 唯一屏障、每次 FR `Popen` 前复核、audited interrupted prefix resume、
committed RT 零 POST、HP evaluator-incomplete 只跳过其 FR、21/113 exposure 和 supporting-only
claim boundary均闭合。正式 API 仍须等待 clean commit、统一 preflight 134/134 PASS、14/14 Key
probe 和最终命令复核。

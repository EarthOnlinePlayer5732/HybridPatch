# 实验记录规范

本仓库采用“Git 中的小型审计记录 + Git 外的原始归档”两层长期保存，并把底层传输调试日志视为临时或取证材料。

实验从计划、preflight、运行、独立审阅到 finalize 的完整要求见
[`标准实验流程.md`](./标准实验流程.md)。本文重点定义实验结束后的 record 结构、
生成方式、日志分层和校验边界。

## `canonical` 到底指什么

同一实验可能同时有普通分析、严格子集分析、历史报告和敏感性分析。`canonical` 表示其中唯一可作为正式结论入口的口径，不表示“最新”“分数最高”或“文件名最正式”。

本仓库区分三个入口：

- `report.md`：canonical human report，即 GitHub 和外部审阅模型直接阅读的自包含报告。
- `summary.json`：唯一 canonical machine scope，即可被程序引用的权威样本范围、指标和排除政策。
- `experiment.yaml > canonical_report.source_report_ref`：生成 `report.md` 时采用的原始人类分析来源。若它位于被忽略的 `exp_*/analysis/`，`report.md` 会嵌入清理后的快照；若该实验没有独立来源报告，则为 `null`。

例如 V7 val40 的 canonical scope 是 38 个完整配对样本；包含两个基础设施失败样本 partial rows 的普通 `analysis/comparison.md` 仍保留，但只能用于诊断。

## 每个重要实验保存什么

每个 owner（`HP_Vx`、`Baseline`、`transport`）都有：

```text
EXPERIMENTS.md
records/
  <experiment_id>/
    experiment.yaml
    report.md
    summary.json
    samples.csv
    casebook.jsonl
    failure_stats.json
    representative_failures.jsonl
    verification.txt
    raw_manifest.json
```

各文件职责如下：

| 文件 | 内容 |
|---|---|
| `experiment.yaml` | 代码、配置、模型、数据、命令、时间、scope、来源报告和已知缺口 |
| `report.md` | canonical 人类报告：结果、口径、解释边界、下一步问题和清理后的来源报告快照 |
| `summary.json` | canonical machine scope、汇总指标、配对比较、可选 paper reporting 和 verification 摘要 |
| `samples.csv` | 完整计划网格；未提交行显式为 `missing`、RS 留空 |
| `casebook.jsonl` | 配对胜负、近似平局、partial、kept-context、严重失败和排除案例的确定性选集 |
| `failure_stats.json` | 失败阶段、标签、方法和 canonical exclusion 统计 |
| `representative_failures.jsonl` | 清理后的代表性失败事实与 raw logical reference |
| `verification.txt` | 既有 verifier 日志或文档证据的固定字段摘要 |
| `raw_manifest.json` | 原始目录位置、内容树哈希、大小、保留等级和清理状态 |

生成文件不保存完整 prompt、完整文档、原始模型正文、HTTP header、Cookie、Key、账户标识或本地绝对路径。`casebook.jsonl` 的 `raw_ref` 只是逻辑定位符，不包含原始正文。

## 生命周期、证据角色和 claim 使用

三个字段必须分开：

- `lifecycle_status`：`complete`、`incomplete`、`failed_informative` 或 `superseded`。
- `evidence_role`：例如 `primary`、`baseline`、`supporting`、`diagnostic`、`transport`、`sensitivity`。
- `claim_eligibility`：`canonical`、`supporting`、`diagnostic_only` 或 `none`。

正式排除基础设施失败样本不会自动令实验变成 incomplete。缺失值必须保持 `null`/空单元格，不能按模型 0 分处理。诊断实验可以完整且有用，但不能自动成为方法 headline。

## 原始日志的保存层级

| 层级 | 保存内容 | 位置 |
|---|---|---|
| 一级 | 配置、summary、逐行指标、失败统计、复核摘要 | Git 仓库的 `records/` |
| 二级 | 清理后的请求、完整模型响应、必要执行结果 | 私有压缩产物或对象存储 |
| 三级 | HTTP 头、连接、心跳、重复 retry/debug 输出 | 临时保存；无取证价值后删除 |

现有 `exp_*` 目录继续被 Git 忽略并保持私有。当前 record 对这些目录建立 manifest，并把位于其中的 canonical source Markdown 以清理后快照嵌入 `report.md`；不会复制 prompt、完整文档或模型 response。`archive_status: not_compressed` 或未完成 credential scan 表示二级归档尚未物理创建或重新清理，不能误称已经上传。

## 每次实验跑完后怎么处理

原始实验结束不等于已经可以提交。收尾顺序固定为：

新 HP_V8+ 实验必须已经存在
`docs/experiment_plans/<experiment_id>.md`；`prepare` 会检查计划文件中的实验编号和
owner，并记录其 SHA-256。计划在 `prepare` 后发生变化时，`finalize` 会拒绝继续。

1. 冻结原始 `exp_*` 输出，不覆盖 checkpoint、JSONL、raw response 或 task plan。
2. 对新建版本只需激活一次。例如创建 `HP_V8` 后，从仓库根运行：

```bash
export PYTHONUTF8=1
python ./tools/process_experiment.py activate --owner HP_V8
```

V8 的 runner/调度器必须在 `run_metadata.jsonl` 记录
`run_git_commit`（40 位 commit）、`git_tree_state`（`clean`/`dirty`）以及明确的
`started_at` / `finished_at` ISO 时间；`prepare` 不会再像历史 record 那样接受这些字段缺失。
因此应在正式付费实验前先把这些字段及完成标记加入 V8 runner 并做零 API
preflight。

3. 确认没有 worker 继续写入后，运行第一阶段：

```bash
python ./tools/process_experiment.py prepare \
  --experiment ./HP_V8/exp_YYYYMMDD_SLUG \
  --confirm-stopped
```

`prepare` 会从 `cwd=HP_V8` 实际运行该版本的 `verify_anchorpatch.py` 和
`analyze.py`，拒绝重复行、混合 fingerprint、冲突 metadata、冻结版本和
实验目录内 `.env*`，并生成：

```text
analysis/verification.log
analysis/analysis.log
analysis/comparison.md
analysis/experiment_results.csv
analysis/derived_facts.json
analysis/process_state.json
analysis/record_review.yaml
```

其中 `derived_facts.json` 只保存可机械证明的事实；`record_review.yaml` 专门保留
不能安全猜测的研究判断，如 purpose、canonical scope、排除理由、claim role、
source report、known exceptions 和可选 paper reporting。它还必须记录
`review_provenance`：主审阅者、审阅时间、审阅方式，以及独立子代理的角色、结论和
摘要；若付费 smoke 未创建独立审阅，必须明确写出例外理由。

4. 审核并填写 `analysis/record_review.yaml`，把影响方法设计的观察写入
`FINDINGS.md` / `RESEARCH_JOURNAL.md`。所有 review confirmation 必须显式为
`true`；不得留下 `REVIEW_REQUIRED`、`TODO` 或无理由排除。实验计划必须以
`pre_registered_experiment_plan` 角色保留在 `source_reports` 中。
5. 运行第二阶段：

```bash
python ./tools/process_experiment.py finalize \
  --experiment ./HP_V8/exp_YYYYMMDD_SLUG
```

`finalize` 会复核 raw input digest，确保 prepare 后结果未变化；根据已审核 scope
自动计算 canonical row 数，原子加入 catalog，再调用底层
`finalize_experiment.py` 计算 raw tree hash、生成九个 record 文件、刷新索引，并
依次执行本地完整校验和干净 clone `--records-only` 校验。失败时恢复 catalog 和
生成 overlay，不留下半更新状态。

新的大体量 paired campaign 推荐使用
[`postprocess_experiment.py`](../tools/postprocess_experiment.py) 的 `closeout` 入口。
它在 raw tree hash 前运行 `review-check` 和临时 record build；权威凭据/tree 扫描与
原生 tgz 并行生成 seal，finalize 通过 `--sealed-manifest` 复用结果。
只需保留私有 source-only record 时，`--private-record-bundle` 会在 validators PASS 后
打包 prospective generated state，再恢复进入命令前的 catalog/index/records。详见
[`实验后处理优化.md`](./实验后处理优化.md)。

该版本所有实验收尾且无 worker 后，冻结 active 版本，之后才能激活 V9：

```bash
python ./tools/process_experiment.py freeze \
  --owner HP_V8 \
  --confirm-no-running-experiments
```

若仍有 `prepared_for_human_review` 的实验，freeze 会拒绝。

整个流程不会调用 API，也不会自动上传原始产物。`finalize_experiment.py` 仍保留
为底层维护/历史 record 重建入口；新 V8+ 实验应优先使用
`process_experiment.py prepare/finalize`。

## 新增迭代版本时怎么做

以新 `HP_V8` 为例：

1. 按项目冻结规则复制并开发新版本，实验使用新的 `exp_<YYYYMMDD>_<slug>`。
2. 在 `tools/version_states.json` 中通过上述 `activate` 命令把新版本设为唯一
   active；冻结 `HP_V3`–`HP_V7` 永远不能重新激活。
3. 实验完成后保留原始产物，并使用上节 `prepare → review → finalize`。
4. 底层生成器若要单独诊断，可运行：

```bash
export PYTHONUTF8=1
python ./tools/build_experiment_records.py
python ./tools/validate_experiment_records.py
python ./tools/validate_experiment_records.py --records-only
```

5. 检查该版本的 `EXPERIMENTS.md` 和全局 `docs/EXPERIMENT_INDEX.md`，再提交小型 record。

生成器不会调用 API、不会修改原始实验目录，也不会为了文档重跑冻结 verifier。历史未记录的 commit、结束时间或配置值写 `null`，不得从当前 HEAD、目录日期或 mtime 猜测。

## 日常检查

完整重建会计算所有 raw 目录内容树哈希，并在同一次只读扫描中检查顶层 `.env*` 的非空凭据值以及 Authorization、Cookie、Bearer、PEM 和长 token 形态。manifest 只记录命中数量，不记录凭据值。启发式命中可能来自 benchmark 文本，不等于已确认泄漏；只要产物仍为 `sanitization.status: raw`，就必须保持私有并在分享前另行清理。

只检查 Git 记录是否由当前 catalog 可重复生成时，可复用已保存的 tree hash 和扫描状态：

```bash
python ./tools/build_experiment_records.py --check --skip-tree-hash
python ./tools/validate_experiment_records.py
python ./tools/validate_experiment_records.py --records-only
```

`--only EXPERIMENT_ID` 只适合本地诊断；正式提交前应完整重建一次。

`--records-only` 是 GitHub 干净 clone 的审计模式：不要求 ignored raw archive、原 source report 或 verifier log 物理存在，也不重算其 hash；但仍严格重算 `samples.csv → summary/failure/casebook`、检查已存 hash 格式、record 引用、敏感信息和 Git allowlist。

校验器检查：

- YAML/JSON/JSONL/CSV 结构和固定文件 allowlist；
- 完整计划网格、主键、缺失值、RS 范围和 canonical exclusion；
- summary、tokens、RS@k、配对集合、失败统计与 CSV 重算一致；
- `report.md` 的 experiment/scope 身份与 `casebook.jsonl` 的逐字段、配对 delta；
- source/manifest 引用和 raw tree hash；
- 本地绝对路径、认证字段、常见密钥模式和 `.env*` 非空值泄漏；
- record 文件是否被 `.gitignore` 错误屏蔽。

## 事实源与生成物

- 人工事实源：`tools/experiment_records_catalog.json`
- 生成器：`tools/build_experiment_records.py`
- 校验器：`tools/validate_experiment_records.py`
- 单实验收尾：`tools/finalize_experiment.py`
- 新实验两阶段处理：`tools/process_experiment.py`
- 版本冻结/active 状态：`tools/version_states.json`
- 全局入口：`docs/EXPERIMENT_INDEX.md`
- 外部模型审阅入口：`docs/AI_REVIEW_GUIDE.md`
- 标准实验流程：`docs/标准实验流程.md`
- 实验计划模板：`docs/experiment_plans/TEMPLATE.md`

不要手改 `records/`、各 owner 的 `EXPERIMENTS.md` 或 `docs/EXPERIMENT_INDEX.md`；应修改 catalog 或生成器后统一重建。

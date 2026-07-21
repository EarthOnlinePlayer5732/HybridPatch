# 实验计划：`exp_<YYYYMMDD>_<slug>`

> 复制为 `docs/experiment_plans/<experiment_id>.md`。首次正式 API 调用后冻结设计内容；实际偏差写入 `docs/active_log.md` 和实验后的 `record_review.yaml`，不要回写本计划来改变预注册口径。

## 1. 身份与状态

- 实验编号：
- owner：`HP_V8 | Baseline | transport`
- 实验级别：`正式主实验 | 支持/诊断实验 | 付费 smoke`
- 计划状态：`draft | reviewed | ready`
- 创建时间与时区：
- 计划作者：
- 计划审阅子代理：
- 审阅结论：`GO | GO WITH FIXES | NO-GO`
- 预期 claim role：`canonical | supporting | diagnostic_only | none`
- 预期 lifecycle：`complete | failed_informative`

## 2. 迭代思路

- 建议或问题来源：
  - 例如：V7 casebook、FINDINGS §N、Web GPT-5.6 Sol Pro 审阅、代码审计。
- 来源中的原始主张：
- 转写后的可证伪研究问题：
- 假设：
- 预期机制：
- 相对上一版本或对照的唯一计划变化：
- 明确保持不变的内容：

## 3. 决策规则

- 若结果为正：
- 若结果为负：
- 若统计不确定：
- 若出现基础设施失败：
- 若发现 evaluator/transport 问题：
- 停止条件：

## 4. 代码、协议与配置

- 计划运行 Git commit：
- 计划工作树状态：`clean | dirty（说明原因）`
- 方法版本 / owner：
- protocol revision：
- prompt revision：
- executor/gate revision：
- transport revision：
- 模型名称与 provider：
- API 模式：`stream | non-stream | 其他`
- temperature / max tokens / thinking：
- repair / retry / commit policy：
- 依赖或环境版本：
- 与上一实验的配置差异：

不得记录真实 Key、Authorization、Cookie 或账户令牌。

## 5. 数据与比较设计

- dataset：
- split：
- 样本编号：
- 已曝光/污染状态：
- `data/CONTAMINATION_REGISTRY.json` 是否已更新：
- seed：
- task plan 来源：
- round trips：
- methods / arms：
- comparison policy：
- distractor 设置：
- 预注册排除及理由：
- 缺失值政策：
- 配对单位：

## 6. 指标与案例

- 主指标：
- 辅助指标：
- CriticalFailure 定义与阈值：
- preservation 指标：
- route / kept-context / partial 指标：
- token / 耗时 / 成本指标：
- 计划重点检查的样本或失败类型：
- 代表案例选择规则：

## 7. 命令与资源

- 目标 `out_dir`：
- 完整运行命令：

```bash
# Git Bash；不得写入真实凭据
```

- 并发与 worker 划分：
- dispatcher policy（大规模 paired 默认 `per_key_work_conserving_v1`）：
- 每 Key worker 上限与 FIFO 队列分配：
- Key 标签：
- 预算上限：
- 预计时长：
- stdout / stderr / dispatch log：
- 恢复与断点续跑策略：
- `infrastructure_incomplete` / `evaluator_incomplete` 的隔离、缺失值和重发政策：

## 8. Preflight

- [ ] 新版本已创建并处于 active；冻结版本未修改
- [ ] 零 API preflight receipt 为 PASS；路径和 SHA-256 已记录
- [ ] dispatcher dry-run 已核对数据、task plan、out_dir 和 manifest
- [ ] manifest 样本 runtime evaluator preflight 通过（记录执行/缓存复用数量）
- [ ] Key 最小探针通过
- [ ] 最终命令、cwd、out_dir 和环境变量复核完成
- [ ] runner 会记录 `run_git_commit`
- [ ] runner 会记录 `git_tree_state`
- [ ] runner 会记录 `started_at` / `finished_at`
- [ ] 实验目录中没有 `.env*`
- [ ] 计划审阅的 required fixes 已闭合

实际命令与退出状态：

## 9. 日志与保留

- 一级 Git record：
- 二级清理后原始归档位置：
- 计划压缩格式：
- credential / privacy scan：
- 三级临时日志删除条件：
- 未来重新评分所需内容：

## 10. 实验后预计更新

- [ ] `docs/active_log.md`
- [ ] `analysis/record_review.yaml`
- [ ] `docs/RESEARCH_JOURNAL.md`（若适用）
- [ ] `docs/FINDINGS.md`（若适用）
- [ ] `HP_Vx/VERSION.md`（若适用）
- [ ] `transport/API_ITERATION_LOG.md`（若适用）
- [ ] `docs/项目结构与迭代史.md`（若适用）
- [ ] `docs/AI_REVIEW_GUIDE.md`（若适用）
- [ ] finalize 生成 records / owner index / global index

## 11. 审阅记录

### 实验计划审阅

- verdict：
- observed facts：
- confounds and contamination：
- missing reproducibility fields：
- metric and exclusion risks：
- required fixes：
- evidence locations：

### 主 Agent 处置

- 已落实的修改：
- 未采纳建议及理由：
- 最终 GO/NO-GO：

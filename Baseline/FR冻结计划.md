# FR 基线冻结计划

> 状态：**原 FR 已冻结**（2026-07-11。范围 233 样本，python1 弃用；结果复核 PASS 2334/2334；成本 $87.00 / 86.0M tokens。最终数字见 FINDINGS §225）。2026-07-16 另增派生版本 **FR+Official**；原 FR 不替换、不覆盖。
> 制定日期：2026-07-10。决策人：用户（本文档记录已拍板的政策，执行时不再重新讨论）。

## 1. 目标

**（2026-07-10 用户扩容：从 dev20 扩为全量）** 对全部 **48 域 / 234 样本** 各跑一次 FR（10RT × no distractor），落在专用目录 `exp_<YYYYMMDD>_frbaseline234`，**跑完即永久冻结为规范基线**：此后所有 HP 协议迭代（dev/val/test 任意子集）只跑 HP 单臂，配对比较一律引用这份 FR 结果，永不重跑 FR。dev20 子集与 v6 HP 臂直接配对（同冻结 plan）。

## 2. 为什么冻结基线在方法论上成立（三个已验证前提）

1. **任务侧零漂移**：task plan（`forward_state_sequence`）、编辑指令、context shuffle 全部 seeded 冻结。已哈希核验：`data/research_splits/dev20_taskplans_20260625/` 与 v3/v5/v6 三代 campaign 目录的 plan 文件字节级一致。任何时候跑 FR 面对同一套任务。
2. **FR 代码路径与 HP 演进解耦**：FR = 官方提示模板（`prompts/domain_documents.txt`）+ `parse_context_string`，不经过 hybrid 执行器/gate/prompt。HP 从 v3 到 v6 的全部改动对 FR 行为定义零影响。
3. **忠实性已有双证据链**（FINDINGS §217 上游对齐审计，2026-07-09）：`utils_context.py`/`utils_env.py`/FR 提示模板与微软官方 DELEGATE-52 仓库字节级一致（仅行尾差异），relay 循环逐行同构。有意偏差三处且已记录：uncapped max_tokens（thinking 模型必需）、seeded 共享任务计划（配对公平性）、模型家族。

## 3. 已拍板的政策（用户决定，2026-07-10）

- **一次就一次**：FR 臂只跑一遍。不做 rewind-until-clean，不做 FR×2 重复跑（成本考虑，且清洗空响应等于美化对照组）。
- **空响应原样入档**：模型 thinking 跑飞导致 text 为空/截断时，结果照常写入、毒化链照常携带。理由：我方 API 设置/重试/超时守卫均正常（§217 审计确认空返回是 authentic baseline 行为 + provider 病理），FR 对空响应无护栏是全局重写范式的真实属性，本来就是要测量的东西；HP 的空响应韧性（repair 第二调用 + kept-context）是方法优势的一部分。
- **不重新拉取上游代码**：上游 `run_relay.py` 任务选择是全局 random、无共享任务计划，重新移植等于重新引入全部已审计偏差并丢掉 checkpoint/遥测/结果复核基建。当前 runner 的 FR 路径就是审计过的上游语义。若担心漂移，正确动作是定期对上游 HEAD 重跑对齐审计（零 API 成本）。

## 4. 必须随基线披露的统计事项

- **基线是 FR 响应分布的一次抽样**：空响应彩票的方差不小（v3 轮 6/20 样本被毒化；v5 轮两次 rewind 后仍剩 3 个）。冻结即固化这一次的运气——纵向 HP-vs-HP 对比不受影响（FR 常数项抵消），绝对 Δ 的解释强度受此限制。
- **调用预算不对称**：HP 失败步有第二次调用（repair），FR 恒一次。配对报告须声明。
- **评估器非确定性边界**：quantum4 RT9 型 stored/recomputed 预存差值属已知类（评估器双解析路径环境敏感），非造假信号。

## 5. 有效性条件（触发任何一条即需重新基线）

| 条件 | 当前值 |
|---|---|
| 模型 + 端点 | minimax-m3 @ OpenCode Go Anthropic 兼容 `/messages` |
| 采样配置 | thinking 开（`MINIMAX_THINKING=1`）、temperature 1、max_tokens uncapped（`--max_tokens 0`） |
| task plan | seed 42；dev20 用 `dev20_taskplans_20260625` 冻结版（开跑前**预拷入 out_dir**）；其余 214 样本首跑按 seed 42 生成、随基线一并冻结 |
| 评估器/数据集 | 仓库当前冻结版 |

托管模型服务端更新不受我方控制，基线须标注日期；换模型（如 deepseek）时须为该模型另建基线。

## 6. 执行步骤（启动指令下达后照此执行）

前置条件（必须全部满足才能开跑）：

1. HP 臂结果复核 PASS ✅（2026-07-10 已完成：400/400 + 200/200）；
2. 无其他 runner 进程存活（防旧自动链型撞车；runner 有重复提交守卫，但重复 API 调用烧真钱）；
3. **零 API 预检**（全量跑之前必做，缺装依赖会白烧已跑样本的钱）：
   - 48 域评估器 import 自检（逐域 `get_domain(...)` 实例化，重依赖 rdkit/pymatgen/qiskit/Bio 等缺失立即暴露）；
   - `data/research_splits/dev20_taskplans_20260625/*.task_plan.json` 预拷入新 out_dir（20 份，保 dev20 与 v3/v5/v6 逐字节同任务）；
   - Key 标签清点（每个 Key 用 5 个并发 16-token 小 probe 验活并实测目标并发，thinking 关闭；probe 费用可忽略）。

执行：用调度器 `src/fr_baseline_dispatch.py`（2026-07-10 已实现并测试；runner/model 层零改动，每子进程 env 覆盖注入自己的 Key）：

```sh
# 0) Key 就位：cp .env.frkeys.example .env.frkeys 并填入真实值（gitignore 的 .env.* 规则覆盖，永不入库）
# 1) 预检（零 API 域检查 + dev20 plan 拷贝 + 每 Key 最小探针验活）
PYTHONUTF8=1 python src/fr_baseline_dispatch.py --out_dir exp_<YYYYMMDD>_frbaseline234 --preflight --probe_concurrency 5
# 2) 正式开跑（KEY_01..KEY_10 各 5 主槽；KEY_11 为 5 槽专职备用）
PYTHONUTF8=1 python src/fr_baseline_dispatch.py --out_dir exp_<YYYYMMDD>_frbaseline234 --slots_per_key 5
```

调度器行为（对应 §6.1 规程的机械化）：`KEY_01`–`KEY_10` 各建立 `--slots_per_key` 个主槽（默认 5，可用 `--slots` 限制主槽总数）；`KEY_11` 建立同数目的专职备用槽，正常队列不使用，只对已在其他 Key 上因 429、超时或其他未完成退出而 requeue 的样本开放，并在 requeue 时优先接管。因此正常启动为 50 并发，只有故障接管期间才会使用 KEY_11（最多 5 并发），且任何 Key 都不会突破单 Key 并发上限。底层命令仍是上文 runner 命令：`--methods fullrewrite --num_round_trips 10 --skip_distractor --model minimax-m3 --max_tokens 0`，`MINIMAX_THINKING=1 MINIMAX_HARD_TIMEOUT=7200`。子进程退出后按 checkpoint 判完成，不完整则**换 Key 重排队**（默认最多 3 个不同 Key，之后标 FAILED 继续不阻塞）；断点幂等（重启调度器自动跳过已完成样本）；`dispatch_log.jsonl` 记录全部 launch/complete/requeue 事件（**只记 Key 标签**，探针输出亦对 Key 值脱敏），即 §6.1 规程 5 的溯源记录。

模态边界：本次 234 样本中没有 `image`/`audio` 样本或二进制媒体文件；`audiosyn` 的 2 个样本是纯文本 CSound `.csd` 编辑，仍走文本模型。调度器启动时有 text-only 硬门，未来若数据目录混入真正的 `image`/`audio` 样本会直接拒绝，不会调用图像/音频生成接口。

并发探针边界：`--probe_concurrency 5` 会逐 Key 启动 5 个并发小调用，11 Key 共 55 次；它只证明探针当时端点可承接目标并发，不代表 provider 对 5 并发作出长期 SLA。正式运行仍以 429/`concurrency_limit` 遥测与 checkpoint 换 Key 为准。

- 中途 429/断点用 checkpoint 幂等续跑（续跑≠rewind：只补未提交轮次，不重掷已提交的空响应）；
- 单实例运行（勿同时开两个调度器——重复守卫能保数据但重复调用烧真钱）；
- **禁止**：对已提交轮次的任何 rewind/重跑/删行。

预检实测记录（2026-07-10）：48 域评估器 import 全部 OK（234 样本全覆盖，含 rdkit/pymatgen/qiskit/Bio 等重依赖），dev20 冻结 plan 拷贝机制验证通过。

### 6.2 预算与 Key 数量测算（限额：$12/Key/5h 滚动窗，$30/Key/周）

实测锚点（v5 campaign FR 臂 committed 行）：20 样本共 **$7.86**，每样本 mean **$0.393** / median $0.348 / max $1.04（protein1）；全 234 样本的 basic_state 文档体量仅比 dev20 大 4%（mean 11.2KB vs 10.8KB，最大 earncall1 43KB）。

- **总预算**：234 × $0.393 × 1.04 ≈ **$96**；计大文档尾部与方差余量 +25% → **按 $110–130 规划**。
- **周限额约束（决定 Key 数）**：$30/Key/周 → 理论最少 4 个（$120，太紧）、建议 ≥5 个。**实际配置（2026-07-10 用户提供）：11 个 Key** → 容量 $330/周、$132/5h 聚合，均为需求的 ~3 倍，极充裕。
- **5h 窗约束（决定分摊方式）**：官方公开的是金额额度，没有公开承诺“每 Key 保证 5 并发”。本次按用户给定的运行假设允许每 Key 最多 5 个并发 runner；槽位与 Key 静态绑定，429/`concurrency_limit` 时按 checkpoint 换 Key 续跑。
- **额度风险**：正常态 50 并发会把大部分预算压进同一个 5h 窗；`KEY_01`–`KEY_10` 的负载高于 11 Key 全均摊，5/key 是激进满速档，不保证零 429；`KEY_11` 保持冷备用以接管失败样本。更稳妥的 3/key（30 主并发 + 3 备用槽）可用 `--slots_per_key 3`。
- **墙钟估算**：按原 10 并发 19–22h 线性外推，33 并发约 6–7h，55 并发约 3.5–4.5h；实际受长尾样本、thinking-runaway、429 与端点吞吐影响。
- **调度定论：每 Key 固定槽位 + 撞限换 Key 兜底（§6.1 规程 3），不采用全队轮询**——这样单 Key 并发有硬上限，Key 切换也有完整溯源。

### 6.1 多 Key 执行策略（用户提供多个 API Key，规避 5h 滚动/周限额）

机制事实（已核验源码与归档）：

- `model_openai.py` 以 `override=False` 加载 `.env` → **shell 环境变量优先于 .env**；`OPENCODE_API_KEY` 在每次调用时从 `os.environ` 读取。因此**零代码改动**即可多 Key：每个 runner 进程在命令行前缀指定自己的 Key。
- api_raw 请求归档只含 `request_messages/request_body/model/provider/base_url`，**不含任何凭据字段**——Key 切换不会泄漏进实验产物。

操作规程：

1. **Key 供给**：用户开跑前提供 N 个 Key，操作者以标签（KEY_A/KEY_B/…）管理；真实值只存在于 shell 环境或本地未提交文件，**严禁写入文档/日志/metadata/源码**（`--notes` 只记标签）。
2. **静态分摊**：`KEY_01`–`KEY_10` 固定 `--slots_per_key` 个主槽（默认 5），`KEY_11` 固定同数目的 requeue-only 备用槽；如需保守运行用 3。`--slots N` 可额外设置主槽总数，调度器会在不突破单 Key 上限的前提下均匀分配。
3. **撞限轮换**：某进程出现持续 429/quota-wait（日志 quota 等待行刷屏、jsonl 无新行、`api_quota_wait_count` 增长）时——先 **kill 该进程并确认已死**（本仓库有过双 runner 赛跑教训：重复守卫能保数据但烧重复调用），再用下一个 Key 的 env 前缀重启同一样本，checkpoint 从首个未提交 RT 续跑。
4. **政策边界不变**：Key 轮换只解决**基础设施可用性**（配额墙），不得用于重掷任何**已提交**的行——"已提交的空响应保留原样"与"配额墙换 Key 续跑"是两件事：前者是 authentic 模型行为（冻结政策核心），后者不产生也不覆盖任何行。
5. **溯源记录**：执行完在本文档收尾节补记各样本使用的 Key 标签序列（何时何样本切换）；Key 身份不影响 authenticity（同端点同模型），记录仅为 provenance 完整。

历史收尾命令（已于 2026-07-11 执行并冻结，仅保留作 provenance；不得对该冻结档重复执行）：

```sh
PYTHONUTF8=1 python src/verify_anchorpatch.py --dir exp_<YYYYMMDD>_frbaseline234   # 2340/2340 backward 结果复核
PYTHONUTF8=1 python src/analyze.py --dir exp_<YYYYMMDD>_frbaseline234 --K 10 --critical_theta 0.10
# dev20 配对分析：把本基线 fullrewrite/ 下 dev20 的 20 组 jsonl+ckpt 只读拷入
# exp_20260710_hybridv6dev20full/fullrewrite/ 后对该目录跑 analyze（HP/FR 同任务 plan，天然配对）
```

收尾动作：

1. FR 臂如出现空响应/截断事件，按事实逐条记入 FINDINGS（不修不洗，标注 RT 位置与形态）；
2. FINDINGS 新增一节记录基线冻结（引用本文档 + 结果复核/analyze 结果）；
3. 本文档状态改为 **已冻结**，附冻结日期与最终数字。

## 7. 范围与后续

- **（2026-07-10 扩容后）一次冻全量 234**：dev20/val20/test20 及任何未来子集的 FR 对照全部出自本基线，val/test 阶段不再单独建 FR 臂。注意 test 阶段若要复现 §212 的 distractor 设定，distractor FR 臂不在本基线覆盖范围（本基线全部 `--skip_distractor`），届时单独决策。
- 预算参考：见 §6.2（估算 $110–130，建议 5 Key）。
- 未来 campaign 引用方式：分析时将本目录 `fullrewrite/` 结果作为配对对照（拷贝或指向均可，保持只读），HP 新迭代跑在各自新 `--out_dir`，**HP 侧 task plan 必须从本基线目录拷贝**（保证同任务配对）。

## 8. 执行收尾记录（滚动补记）

- 2026-07-10：234 样本 FR 臂由用户以多 Key 调度器跑完，唯 python1 失败（评估层 `utils_eval` 缺失）。调查结论=上游打包遗漏 + 两层环境坑，全链证据与修复见 FINDINGS §224。
- 2026-07-10 用户决定：**python1 弃用，基线范围 = 233 样本**。python1 修复后重跑的部分提交行（RT1–4）保留在磁盘、排除出分析口径。
- 2026-07-11 冻结定稿：结果复核 PASS **2334/2334** backward（233×10 + python1 部分行 4）；committed 成本 **$87.00 / 86.0M tokens**；全量 RS@10 0.558（RS@1 0.926，长程退化近腰斩）；dev20 配对 Δ+0.195（披露口径 +0.030，见 FINDINGS §225）。dev20 FR 20 组已只读拷入 `exp_20260710_hybridv6dev20full/fullrewrite/` 完成配对分析。本基线自此只读。

## 9. 新增派生版本：FR+Official（2026-07-16）

`FR+Official` 是原冻结 FR 之外的新增版本，用于对齐论文 baseline 的 MiniMax 官方 API 设置；它不替换、不修改 `exp_20260710_frbaseline234`。该版本以 manifest 逻辑视图保存，不复制或改写源 JSONL。组成规则固定为：优先采用完整的官方 10RT 轨迹，不完整时整样本回退原冻结 FR；禁止按 RT 拼接或按分数选择来源。

- 总范围仍为 233 样本：87 个完整官方轨迹（83 个来自 `exp_20260713_frbaseline_v2_rerun`，4 个由 `exp_20260713_problem15_official` 补齐），其余 146 个保留原冻结 FR。
- 样本级异常率：任一异常 **87/233（37.34%）**；异常终止/截断 **86/233（36.91%）**；空返回 **80/233（34.33%）**；接近空返回（非空且 UTF-8 正文 `<200 bytes`）**37/233（15.88%）**。分类可重叠。
- 被选官方子集为按旧链异常/问题样本预筛后的重跑池，其 `85/87` 异常率不能解释为 MiniMax 官方 API 的总体背景率。
- 整体曲线：FR+Official RS@1/5/10 = **0.735/0.550/0.508**，CriticalFailure@10 = **162/2097**；原冻结 FR 对应为 0.925/0.682/0.558、167/2097。
- V7 val40 strict38：若论文采用 MiniMax 官方 API、非流式、一次性完整响应照单
  接受的 FullRewrite baseline 处理流程，则论文协议对齐结果为 HP **0.825** vs
  FR+Official **0.564**，Δ**+0.262**（p=2.54e-28，d=0.62）。strict38 仅
  16 条 FR 链来自完整官方重跑，其余沿用冻结 FR，且替换池事后预筛；因此必须
  同步披露混合来源边界。同 transport-v3 方法效应仍由 Δ+0.008 回答。

可复现构建入口与完整定义见 [`build_fr_plus_official.py`](./build_fr_plus_official.py)；来源清单、异常逐样本明细与 val40 对比见 [`FR+Official/README.md`](./FR+Official/README.md)。本次只做既有归档的只读组合与统计，没有新增 API 调用，也没有重新运行结果复核。

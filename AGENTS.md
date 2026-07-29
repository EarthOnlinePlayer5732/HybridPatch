# HybridPatch Agent Instructions

## 作用域与项目入口

本文件适用于 `hybridpatch_clean/` 整个工作区。用户当前对话中的明确要求优先；更深目录若新增 `AGENTS.override.md` 或 `AGENTS.md`，则更具体的文件优先。

开始任务前必须完整阅读并遵循本目录的 [`CLAUDE.md`](./CLAUDE.md)。项目架构、实验语义、冻结边界、必读文档和验证命令以该文件为准；本文件只补充终端、工具编排和运行安全规则。

## Git Bash 与 Shell 基线

- Codex Agent 的命令与 Codex 自带集成终端在本工作区统一使用 **Git Bash**。即使工具宿主由 PowerShell 启动，也必须把实际命令交给 `C:\Program Files\Git\bin\bash.exe` 执行，不直接用 PowerShell 运行项目命令。
- 本机 Git Bash 固定入口为 `C:\Program Files\Git\bin\bash.exe`。若该入口不可用，先报告阻塞，不得静默回退到 PowerShell、WSL 或其他 `bash`。
- 冻结 `HP_Vx/VERSION.md`、experiment records、历史重构报告、历史 handoff 和迭代日志可以保留当时实际使用的 PowerShell 命令作为 provenance；这些片段不是当前执行指南，不覆盖本节 Git Bash 规则，也不得仅为换 shell 而改写。
- 一个文件操作流程只使用 Git Bash。不得用 Git Bash 枚举路径后交给 PowerShell、`cmd.exe` 或其他 shell 删除、移动或覆盖。
- 环境变量使用 Bash 语法，例如 `export PYTHONUTF8=1`；单条命令可使用 `PYTHONUTF8=1 python ...`。
- 给用户的命令优先使用单行形式。必须换行时只使用 Bash 反斜杠 `\` 续行，并提醒续行提示符 `>` 表示命令仍未闭合。

## 工作目录与路径

- Git、跨版本文档与目录管理命令从 `hybridpatch_clean/` 根目录运行；版本代码、测试、verify、analyze 与实验命令从目标 `HP_Vx/` 根运行。工具调用必须显式设置绝对 `workdir`，不要依赖上一条命令留下的当前目录。
- `HP_V7` 是最新冻结可运行参考，当前没有可写 active 方法快照，顶层不再有运行入口 `src/`。在付费、长时或写入命令前，先确认已按版本纪律创建新的 `HP_V8`（或后续版本），再检查其 `src/experiment_runner.py`；只读复核旧实现时可检查 `HP_V7/src/experiment_runner.py`。
- `HP_Vx/data` 是指向共享顶层 `data/` 的 Windows junction。只允许从 Git Bash 使用 `MSYS_NO_PATHCONV=1 cmd.exe /c mklink /J` 创建 junction，并向 `cmd.exe` 传入经 `cygpath -aw` 转换的绝对路径；这是唯一的跨 shell 文件操作例外。
- Git Bash 文件操作必须引用路径，并在命令支持时用 `--` 终止选项；Windows 原生程序需要绝对路径时用 `cygpath -aw` 转换。不要手工拼接未引用的含空格、中文或反斜杠路径。
- 在 JavaScript/工具编排源码中，不把 Windows 路径直接写进会解释转义的字符串；例如 `\v`、`\t`、`\n` 可能变成控制字符。优先使用 `path.join()`、正斜杠或经过验证的双重转义。
- Python 内部路径优先由 `pathlib.Path` 或 `os.path.join` 构造；CLI 输出目录应尽早规范化为绝对路径。

## Git Bash 错误处理

- 多步 Git Bash 命令开头使用 `set -euo pipefail`，让命令、未定义变量和管道错误立即终止。
- 外部程序运行后检查退出状态，或让 `set -e` 直接终止。非零退出时不得继续打印 PASS、started、completed 等成功信息。
- 确需后台运行时显式重定向 stdout/stderr、用 `$!` 记录 PID，并通过进程状态和日志路径验证启动成功后再报告。
- 成功状态必须来自实际退出码、文件或进程证据，不能根据“命令看起来已执行”推断。
- 对预期允许失败的命令单独处理退出码。例如 `rg` 的退出码 1 表示无匹配，不应让整个并行批次误报失败。

## 长任务与输出

- 默认让长任务在前台运行，由工具的 yielded cell / wait 机制继续等待；不要优先采用后台运行、手写 PID 文件和高频轮询。
- Python 长任务使用 `python -u` 或 `export PYTHONUNBUFFERED=1`，保证重定向日志能够实时刷新。
- 单次等待不超过 30–60 秒。等待期间只在关键状态变化时更新用户，不刷屏。
- 长日志写入明确的输出文件，终端只查看末尾和关键匹配。不要把数十万字符日志直接回传到对话。
- 后台任务必须记录命令、工作目录、标准输出、标准错误和退出状态；结束后检查 stderr 与最终成功标记。
- 查询进程时先限定进程名（如 `python.exe`），再匹配命令行，避免把 `grep` 或正在执行查询的 Git Bash 自身误判为目标进程。

## 搜索与并行调用

- 文本与文件搜索优先使用 `rg` / `rg --files`。
- 需要包含隐藏缓存时显式使用 `--hidden`，并排除 `.git`；不要默认认为普通 `rg` 已覆盖 `.cache`、点目录或被 ignore 的文件。
- 多个命令只在真正独立时并行。任一命令可能正常返回“无匹配”时，先把该退出状态归一化，避免一个 Promise/子命令隐藏其他有效输出。
- 输出多个文件片段时，为每段添加文件名和行号标题。不要把无边界的切片连续打印，造成代码似乎错位或损坏的假象。
- 已知符号或报错优先定向搜索；只有定向搜索不足时才扩大到隐藏目录、父仓库或用户目录。

## 研究代码简约与完整修改

- 不要过度工程化。把复杂度用在会直接影响研究问题的部分：数据与任务计划、训练流程（若有）、模型或提示/协议设计、损失或评测设计（若有）；不要为假设中的未来需求预建框架、抽象层、插件系统或通用平台。
- 保持核心代码和主调用链简约、可读、可验证。代码组织与风格优先参考成熟、主流、高采用度的基线仓库及本项目既有实现，不追求自创范式。
- 修改前先理解入口、调用链、数据流、测试和兼容边界；实施时完成一个最小但完整的闭环。避免把同一个明确改动拆成长期堆叠的零碎补丁，能在一次变更中完整改到位的，就同步处理实现、调用方、测试、配置和文档。
- “一次改到位”不等于大范围重写。存在高不确定性、不可逆风险或需要实验验证时，仍应先用最小样本、合成输入或短路径验证，再完成正式变更。
- 减少没有真实故障、调用证据或威胁模型支撑的边界包装、安全声明、防御分支和静默兜底；同时不得削弱本项目已有的凭据保护、冻结边界、实验完整性、数据只读、传输完整性和可重放要求。

## Python 子进程与测试替身

- 从目标 `HP_Vx` 根用 `python -c` 导入该版 `src/` 中的顶层模块时，必须显式设置 `PYTHONPATH`，或使用脚本自带 bootstrap。父进程对 `sys.path` 的修改不会自动传给新的 Python 解释器。
- 创建子进程时显式传递所需的 `cwd`、UTF-8 环境、模型环境变量和模块路径；不要依赖交互 shell 的隐式状态。
- Mock `subprocess.Popen`、环境变量或全局导入器时使用最窄作用域。先完成 SciPy、NumPy、平台检测等依赖导入，避免测试替身劫持第三方库内部的系统调用。
- 测试不仅验证新 helper，还要覆盖真实入口、子进程环境和工作目录。

## 实验运行安全

- HP_V9 的**新实验**唯一入口是 `HP_V9/src/run_campaign.py`，语义见
  `HP_V9/SIMPLE_RUNTIME.md`。`paired_campaign_dispatch.py`、
  `campaign_recovery_runtime.py` 与 `authorize_ledger_lock_recovery.py` 仅用于读取历史实验；
  不得包装或导入到新 active runtime。新路径只保证 sample-local relay/checkpoint，普通
  API、evaluator、preservation 和 worker 异常只隔离该 sample，campaign 结束后再统一验证。
- `tools/preflight_experiment.py` 仍服务 legacy dispatcher。HP_V9 simplified campaign 的回归、
  evaluator smoke、计划审阅和 Key probe（若实验计划要求）必须作为启动命令之外的独立门禁；
  `run_campaign.py` 启动本身不得运行回归、历史 archive 扫描或旧 dispatcher dry-run。

- 新 API 实验开跑前，优先用 `tools/preflight_experiment.py` 一次生成零 API preflight receipt：复用未变代码的回归结果，并对正式 manifest 中的样本并行运行 runtime evaluator。dispatcher dry-run 仍每次执行，以核对本次 task plan、out_dir 和命令身份；该工具不执行 Key probe，也不启动正式 worker。
- 零 API 缓存只复用输入指纹完全一致且成功的结果。代码、Python runtime 或样本内容变化时只重跑失效部分，失败结果不得缓存为通过。Key 小探针仍是独立的真实 provider 请求，成功 receipt 不能替代 Key probe。全部通过后再做最终命令复核并开跑。
- 方法实验输出放所属 `HP_Vx/exp_*`；传输诊断放顶层 `transport/exp_*`；baseline 放顶层 `Baseline/exp_*`。从 HP 根引用后两者时使用 `../transport/...` / `../Baseline/...`。
- 新方法语义先复制最新 `HP_Vx` 为下一版本再改；冻结版永不回改。API 修改只在 `transport/` 开发，验证后仅同步到新建的可写 HP 并记录指纹，绝不回灌冻结版本。
- evaluator 的 import 成功不代表样本可运行。全量付费实验前应对样本自带 scaffold / runtime evaluator 做零 API smoke test，捕获只在 `evaluate_context()` 执行时出现的缺文件或缺模块问题。
- runner 因本地异常退出时，先判断异常发生在 API 调用前还是调用后；必须检查 `api_calls.jsonl` / `api_raw`，不能仅凭缺少 checkpoint 推断“没有花费”。
- 本地确定性错误（缺模块、路径错误、评估器异常）不得通过换 Key 连续重试。修复根因前不要重新启动同一样本。
- 断点续跑只补未提交 RT，不删除、覆盖或重掷已提交行。不得同时运行两个指向同一 `out_dir` 的调度器。
- Legacy 大规模 paired campaign 使用 dispatcher 的 per-Key work-conserving 队列：每个 Key 的
  worker 数不得超过 `--slots_per_key`，但任一槽位释放后应立即从同一 Key 的 FIFO 队列
  补位，不等待其他 Key 或同一批 worker 全部结束。manifest 必须记录 dispatch policy、
  每 Key 队列和最大并发。
- Legacy dispatcher 中，`domain.evaluate_context()` 抛出的样本级异常只有在 runner 已写入
  `evaluator_incomplete` outcome、run metadata、正式 sidecar，且能证明失败步骤未提交、未补
  0 时才允许隔离该样本并继续队列。该状态在本 campaign 中是终态，resume 不重发；普通
  本地代码异常、evaluator 证据不完整、preservation 或共享完整性错误仍全局停止。
- 分析结果前先检查样本数、每样本行数、checkpoint 和已有结果复核记录。冻结归档只读既有记录，不为文档或派生分析重复运行复核脚本；缺失样本必须显式排除并报告，不能静默按 0 分或假装完整。
- 新 HP_V8+ 实验收尾统一使用 `tools/process_experiment.py prepare → review → finalize`。`prepare` 前必须确认 worker 已停止；机械事实不得代替 canonical scope、排除理由、claim role 和 paper reporting 的显式 review。冻结 HP_V3–HP_V7 不得交给该流程重新处理。

## 标准实验流程

所有新的付费 API 实验、准备形成研究结论的 campaign，以及会被后续迭代引用的诊断实验，都必须遵循 [`docs/标准实验流程.md`](./docs/标准实验流程.md)。纯零 API 单元测试或静态检查不要求单独建立实验计划，但仍须在对应代码变更记录中保存真实验证结果。

### 实验前

1. 先确定唯一实验编号 `exp_<YYYYMMDD>_<slug>`、owner、实验级别（正式主实验、支持/诊断实验、付费 smoke）和预期 claim role。
2. 从 [`docs/experiment_plans/TEMPLATE.md`](./docs/experiment_plans/TEMPLATE.md) 创建 `docs/experiment_plans/<experiment_id>.md`。计划至少写明：迭代来源与思路、研究问题、可证伪假设、相对上一版的唯一变化、代码/配置/模型/API/transport/prompt/protocol/executor、数据划分与样本编号、seed、task plan、运行命令、指标、预注册排除、成功/失败/停止条件、预算、风险和原始日志保留级别。来自外部模型（例如 Web 审阅模型）的建议必须先转写为可验证假设，不能直接当作结论。
3. 在 `docs/active_log.md` 追加 `Before Experiment` 条目并链接计划文件。若本次实验引入新方法版本、transport revision 或新样本曝光，还必须在开跑前分别更新目标 `HP_Vx/VERSION.md`、`transport/API_ITERATION_LOG.md` 或 `data/CONTAMINATION_REGISTRY.json`。
4. 正式主实验和支持性付费实验开跑前，主 Agent 必须创建一个只读的“实验计划审阅”子代理。该子代理检查混杂因素、数据污染、比较公平性、指标、排除规则、停止条件、运行身份和复现信息，只给出 `GO`、`GO WITH FIXES` 或 `NO-GO` 及证据；由主 Agent 独占文件写入权并落实修改。付费 smoke 可由主 Agent 完成同一清单，但必须在计划中说明未单独创建子代理的理由。
5. 使用 `tools/preflight_experiment.py` 完成零 API 回归、正式 manifest dry-run 和样本 runtime evaluator 检查并保存 receipt；随后独立完成 Key 小探针与最终命令复核。指纹未变时可以复用成功的零 API 结果，不为形式完整重复昂贵检查；计划或 preflight 未闭合时不得调用正式 API。
6. 首次 API 调用前固定代码状态。V8+ runner 必须把 `run_git_commit`、`git_tree_state`、`started_at`、`finished_at`、模型与接口配置、seed、task plan 和命令身份写入运行 metadata；计划文件不得用当前 HEAD、mtime 或整理时间补猜实际运行身份。

### 实验运行中

- 同一 `out_dir` 内不得修改方法语义、prompt、transport revision、数据范围、seed、比较政策或评分口径。需要改变时停止当前实验，保留已有产物，并建立新的实验编号和计划。
- 每个已提交 round trip 必须保留结果行、checkpoint、调用/响应定位信息和必要 ledger；断点续跑只补未提交项。
- 只读监控不得改写 raw response、task plan、checkpoint 或结果行。异常必须区分模型、协议、执行器、evaluator、transport 和基础设施原因。

### 实验后

1. 确认所有 worker 已停止且原始目录不再写入，再运行 `tools/process_experiment.py prepare --confirm-stopped`；不得先看分数再改计划、样本范围或排除规则。
2. `prepare` 成功后，正式主实验至少并行创建两个只读子代理：
   - “结果完整性审计”：核对计划网格、缺失行、metadata、verification、scope、排除、敏感信息和 provenance。
   - “结果与失败分析”：分析汇总指标、逐样本结果、casebook、失败类型、成本和可定位到代码的机制。
   准备用于论文或 canonical claim 的实验再增加一个独立“结论边界审阅”子代理，专门检查统计解释、混杂因素、claim eligibility 和 paper reporting。支持/诊断实验可把后两项合并，但不得省略结果完整性审计。
3. 子代理默认只读，不直接修改 `record_review.yaml`、catalog、records、`FINDINGS.md` 或研究日志，也不得启动 API、重跑冻结实验或接触凭据。主 Agent 汇总并复核其证据，独占写入 `analysis/record_review.yaml`。
4. 每次 API 实验都在 `docs/active_log.md` 追加 `After Experiment`。重要 campaign 更新 `docs/RESEARCH_JOURNAL.md`；产生新机制、失败类型、设计判断或反例时更新 `docs/FINDINGS.md`；版本五问发生变化时更新 `HP_Vx/VERSION.md`；transport 语义或异常政策变化时更新 `transport/API_ITERATION_LOG.md`；canonical 结果或迭代状态变化时更新 `docs/项目结构与迭代史.md` 和必要的 `docs/AI_REVIEW_GUIDE.md`。没有适用变化的文档不为凑流程添加空条目。
5. review 完成后运行 `tools/process_experiment.py finalize`。`records/`、owner `EXPERIMENTS.md` 和 `docs/EXPERIMENT_INDEX.md` 由工具生成，禁止手改。实验计划应作为 `pre_registered_experiment_plan` 加入 `record_review.yaml` 的 `source_reports`。
6. 按三级保存规则处理日志：Git 中提交小型 records；清理后的请求、模型原始响应和必要执行结果进入私有压缩归档；HTTP header、心跳和重复 debug 日志只作临时材料。若二级归档尚未清理或压缩，必须在 record 中明确记录真实状态。
7. 最后运行完整 record 重建检查、本地 validator 和 `--records-only` validator，再检查 Git diff、敏感信息、计划—结果偏差和所有应更新文档。

### 固定子代理分工

- 固定的是角色、检查项和交付格式，不是长期驻留的 Agent 进程；每次实验使用新上下文创建，减少沿用主 Agent 假设造成的确认偏差。
- 子代理输出统一区分：`observed facts`、`interpretation`、`claim impact`、`required fixes`、`next experiment`，并给出文件或原始逻辑定位。禁止凭摘要猜测未读取的 raw 内容。
- 主 Agent 始终负责最终 `GO/NO-GO`、canonical scope、排除、文档写入、finalize 和对用户交付。多个 Agent 不得同时修改同一文件。

## 凭据与敏感输出

- 不直接打印、读取回显或记录 `.env*` 中的 Key 值。检查 Key 文件时只输出文件是否存在、标签、条目数、非空状态和必要的脱敏摘要。
- `.env` / `.env.frkeys` 只保留 `hybridpatch_clean/` 顶层一份，不得复制进任何 `HP_Vx`、`transport/` 或文档。HP 代码不会自动向上找到顶层 `.env`；由父进程显式注入，或使用 `python -m dotenv -f ../.env run -- ...`。
- 子进程日志、dispatch 日志、notes 和 metadata 只记录 Key 标签，不记录真实值。
- 搜索配置文件时限制输出字段；包含凭据的配置不得整文件回传到终端或对话。

## 交付前终端检查

- 核对最终 `git status` 与任务涉及文件，保留用户已有改动。
- 报告实际执行的验证、退出状态和未运行项。
- 若终端命令本身失败，先区分产品代码失败、shell/转义失败、测试桩失败和工具超时；不要把测试基础设施问题归因于项目代码。

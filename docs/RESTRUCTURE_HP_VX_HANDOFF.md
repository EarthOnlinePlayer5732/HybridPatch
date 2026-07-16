# hybridpatch_clean 版本文件夹化重构交接（HP_Vx 方案）

> [!WARNING]
> **HISTORICAL SNAPSHOT — DO NOT EXECUTE.** 本文保存的是 2026-07-13 重构执行前的交接状态；Phase 0/A/B/C 已完成，文中的“待执行”“当前 `src/`”和未收尾 campaign 均不是当前指令。当前实验状态以 [EXPERIMENT_INDEX.md](./EXPERIMENT_INDEX.md) 为准，结论解释以 [FINDINGS.md](./FINDINGS.md) 最新编号条目为准。

状态：**历史快照（当时待执行，现已完成）**。本文是给当时执行重构的 agent 的完整交接：动机、已拍板决策、目标结构、逐目录归类映射、版本重建来源与验收标准、硬约束红线、分阶段步骤与完成判据。

---

## 0. 启动指令（可直接粘贴给执行 agent）

> 阅读 `hybridpatch_clean/docs/RESTRUCTURE_HP_VX_HANDOFF.md` 并执行其中的重构任务。用户已**全量放权**：Phase 0 → A → B → C 一次跑通，无需中途请示（2026-07-13 确认：无 worker 进程在跑，两个未收尾 campaign 按 §5-4 冻结现状整体搬移）。每个 Phase 完成后 git commit 一次作里程碑回滚点（信息注明 phase 与验收摘要）。全程遵守 §5 红线：只搬移不删除、重建版源码零修改、每步验收有证据。仍需停下的唯一情形：红线冲突，或实况与本文档不符且文档没有给出既定降级路径。终验报告要写成"用户不看过程也能信任结果"的形态：每项 DoD 逐条附证据（命令 + 关键输出）。

---

## 1. 动机与现状

**现状**：`hybridpatch_clean/` 是单代码库——hybridpatch/1..7 七代协议语义全部塞在一份 `src/` 里靠 rev 门控（`hybrid_schema.py` 的 `PROTOCOL_V1..V7` + `rev_of`）区分；API/传输层改动（OpenCode transport-v3、MiniMax 官方线）与方法迭代改动混在同一批文件里；十几个 `exp_*` 归档平铺在目录根。

**痛点**（用户原话大意）：每次迭代都往同一个文件/文件夹里疯狂加东西，方法迭代和 API 迭代搅在一起，后续会形成屎山。

**目标形态**：每代方法一个自包含可运行的版本文件夹 `HP_Vx`（x=3..7，未来 8、9…），新迭代 = 复制最新版本文件夹再改；API/传输层单独拎到顶层 `transport/`；归档按归属分家。

## 2. 已拍板的四个决策（用户确认，不再讨论）

1. **HP_Vx = 完整可运行快照**：每版含全套 `src/`（方法五件套 + runner/verify/analyze + 当时的 transport 文件 + utils + domains + 测试），版本之间互不引用；旧版本文件夹冻结后永不改动。
2. **全量回溯重建 V3–V7**：不是只从当前起步。V3/V6/V7 有 git 锚点；V4/V5 无锚点，按 §4 考古流程尽力重建、按指纹验收如实定级。
3. **API/传输层拎到顶层 `transport/`**：API 开发主战场在此（自带 revision 线与迭代史文档）；活跃 HP_Vx 通过**拷贝**同步，冻结版永不回灌。两条传输线（OpenCode-v3 冻结 + `minimax_official_nonstream/1`）都保留，不合并。
4. **归档分家**：能归属某版本的 `exp_*` 挪进对应 `HP_Vx/exp_*`；FR 冻结基线类进顶层 `Baseline/`；**未跑满 10RT 的小 smoke** 进垃圾候删区 `_attic/`（只收纳不删除，最终删除由用户确认）。

## 3. 目标目录树

```
hybridpatch_clean/
├── HP_V3/ … HP_V7/            # 版本文件夹（自包含运行根），每个含：
│   ├── src/                   #   全套代码（含当时的 transport 文件、domains/、test_fixtures/）
│   ├── prompts/               #   实拷贝（小；domains 按 cwd 相对路径加载）
│   ├── data/                  #   junction → ../data（Windows：cmd /c mklink /J；junction 不可行则实拷贝）
│   ├── exp_*/                 #   该版本的实验归档（原目录名不变）
│   └── VERSION.md             #   版本卡（见 §4.3）
├── transport/                 # API/传输层主战场
│   ├── src/                   #   7 个 API 文件 + test_fixtures/（初始 = 当前最新状态）
│   ├── docs/                  #   API_TRANSPORT_FROZEN_V3.md / API_TRANSPORT_MINIMAX_OFFICIAL_V1.md /
│   │                          #   TRANSPORT_V3_CLAUDE_CODE_HANDOFF.md / Minimax_OPENAI.md
│   ├── API_ITERATION_LOG.md   #   新写：SSE 流式化 → transport-v2 → v3 冻结 → official_nonstream/1
│   │                          #   （摘自 docs/项目结构与迭代史.md §5，含各 revision 语义与去处）
│   └── exp_*/                 #   传输层诊断归档（problem15_official 等）
├── Baseline/                  # FR 规范基线：frbaseline234（冻结勿动）、frbaseline_v2_rerun、FR冻结计划.md
├── data/                      # 共享只读数据（原样不动：samples_delegate52/ hybrid_split.json research_splits/ CONTAMINATION_REGISTRY.json）
├── docs/                      # 项目级跨版本文档（迭代史/FINDINGS/JOURNAL/DESIGN/active_log/本文）
├── _attic/                    # 垃圾候删区（只进不删）：非 10RT smoke、_tmp_rawlog、根部散落 *.log
├── MIGRATION_MAP.md           # 新写：全部搬移条目的 旧路径 → 新路径 映射表
├── requirements.txt / .env / .env.frkeys
└── README.md / CLAUDE.md / AGENTS.md   # Phase B 更新指向新布局
```

**运行约定（重要变化）**：每个 `HP_Vx` 是独立运行根——命令 `cd HP_Vx` 后执行 `python src/experiment_runner.py …`。原因：代码内 `SAMPLES_ROOT = _ROOT/data/...` 是 `__file__` 相对（`_ROOT` = src 的上级 = `HP_Vx/`），prompts 是 cwd 相对；两者在"HP_Vx 自带 data junction + prompts 拷贝、cwd=HP_Vx"下同时解析成功，**源码一个字节都不用改**。transport/ 下的探针/调度脚本同理在各自运行根语义下工作（如需从 transport/ 独跑，先核实其路径解析再定运行根）。

## 4. 版本重建：来源、考古流程与验收

### 4.1 验收机制（本重构的验收基石）

每个 campaign 的 `run_metadata.jsonl` 里存有当年 `code_fingerprint`：对 `run_meta._FINGERPRINT_FILES` 清单（patch_schema / splitters / experiment_runner / hybrid_schema / hybrid_index / hybrid_prompt / hybrid_executor / hybrid_gate / model_openai / run_meta / ../requirements.txt 共 11 项）逐文件取 `sha1(文件字节)[:12]`。重建验收分三级，**逐版本记入 VERSION.md，不许谎报等级**：

- **字节级确证**：重建 src 对 11 项指纹全匹配该版 campaign 的 run_metadata。
- **行为级确证**：指纹不全匹配（或非指纹文件无锚点），但 `verify_anchorpatch.py` 对该版归档结果复核 PASS（字节级重放成立）。
- **近似重建**：以上都达不到——如实记录差异清单与来源推断；或与用户确认后放弃该版代码、只留 VERSION.md 占位（归档与文档仍在）。

注意：指纹只覆盖 11 个文件；verify/analyze/utils/domains 等非指纹文件一律取自同一 git 锚点并在 VERSION.md 披露"随锚点状态"。

### 4.2 逐版本来源表

| 版本 | 协议 | 来源锚点 | 验收对象归档 |
|---|---|---|---|
| HP_V3 | hybridpatch/3 | git tag `hybridpatch3-snapshot-20260709`（= commit db5048d2 "Publish hybridpatch_clean only"，2026-07-09 03:16） | exp_20260708_hybridv3dev20full（结果复核预期 39/40 + 1 处已知评估器非确定性，见迭代史 §3.1——**这不是失败**） |
| HP_V4 | hybridpatch/4 | ⚠️ 无 commit 锚点（07-09 03:16 → 07-12 06:27 之间 git 零提交）。考古顺序见 §4.4 | exp_20260709_hybridv4smoke（PASS 70/70） |
| HP_V5 | hybridpatch/5 | ⚠️ 同上无锚点 | exp_20260709_hybridv5dev20full（PASS 400/400） |
| HP_V6 | hybridpatch/6 | 双源交叉：git tag `hybridpatch6-snapshot-20260711`（commit 2e1e004b）与磁盘快照 `_snap_val40/src`（迭代史称其指纹 == v6 dev20 campaign，**须实测**） | exp_20260710_hybridv6dev20full（PASS 200/200）+ exp_20260711_hybridv6val40（PASS 800/800） |
| HP_V7 | hybridpatch/7 | 当前 `src/`（活跃版；相对 git tag `hybridpatch7-20260711` = commit 3f742380 增加了官方传输线 F1–F7、runner 面板等改动，已于 2026-07-13 提交，**以当前 HEAD 为准**） | exp_20260711_hybridv7dev20full（PASS 400/400 含 5 partial）+ exp_20260712_hybridv7val40_transportv3 |

### 4.3 VERSION.md 版本卡模板

每个 HP_Vx 一份，内容从 `docs/项目结构与迭代史.md` §3.x 对应小节摘录 + 重构时新增验收记录：

```markdown
# HP_Vx（hybridpatch/x）版本卡
- 五问摘要：① 为什么 ② 方向 ③ 效果（关键数字+出处 §nn）④ 新问题 ⑤ 下轮靶子
- 来源锚点：<git tag / commit / 磁盘快照 / 考古方式>
- 验收等级：字节级确证 / 行为级确证 / 近似重建（差异清单：…）
- 指纹比对明细：11 项逐文件 match/mismatch
- 本版归档：exp_…（结果复核重验结果与日期）
- 运行方式：cd HP_Vx && PYTHONUTF8=1 python src/…
```

### 4.4 V4/V5 考古流程（按序尝试，全程记录）

1. **悬空对象打捞**：`git fsck --lost-found --dangling`，对捞出的 blob 逐个算 `sha1(bytes)[:12]` 与 V4/V5 campaign 指纹匹配（11 项中先匹配 hybrid_executor / hybrid_schema / experiment_runner 这几个版本敏感文件）。
2. **定向逆推**：V4 ≈ V3 + D1（bulk 文件级匹配）+ D2（@body 哨兵空白归一）+ D3（ws 两趟化）+ P1 路由压强 prompt；V5 ≈ V6 锚点 − C1（包含吞并）− C2（格式健康 gate）。以迭代史 §3.2/§3.3 的改动描述为规格逆推，逆推结果必须过指纹或结果复核验收，**过不了就不算重建成功**。
3. **降级**：两条路都失败 → 停下向用户汇报，按 §4.1 第三级处理。

## 5. 硬约束红线（违反任何一条 = 重构失败）

1. **重建版源码零修改**：不许为适配新布局改任何 import、路径常量、一行代码——布局迁就代码（§3 运行约定），不是代码迁就布局。发现不改跑不通 → 停下汇报。
2. **只搬移不删除**：任何文件的"删除"都只是移入 `_attic/`；`_attic/` 内容的真删除只能由用户执行。
3. **归档字节不动**：exp_* 目录整体 move，内部 JSONL/checkpoint/api_raw/docs 一个字节不改；搬移后逐档跑结果复核重验，PASS 才算该档迁移完成；已知预存差值（如 v3 档 quantum4 RT9）照迭代史披露口径核对，不算 FAIL。
4. **未收尾 campaign 冻结现状搬移**：`exp_20260713_problem15_official`（截至 2026-07-13：FR 臂 11/15、HP 臂 5/15 有产出）与 `exp_20260713_frbaseline_v2_rerun`（备料完成、尚未开跑）——用户已确认无 worker 在跑并授权 Phase B 直接执行：两目录**按当前中间状态整体搬移**（内部一个字节不改），搬移后在各自目录内新增一份 `RESUME.md`（新布局下的精确续跑命令：cwd=HP_V7、新 out_dir 路径、`MINIMAX_TRANSPORT=official_nonstream` 等 env 设置、并发约定），并在 MIGRATION_MAP 标注"未收尾、待续跑"。除 RESUME.md 外不得在其中写入任何内容。
5. **`data/` 只读**、`.env` / `.env.frkeys` 不拷贝进任何版本文件夹或文档（junction 指向共享 data；密钥仅根目录一份，`load_dotenv` 从 cwd 向上找不到时须核实——见 §7 假设 4）。
6. **验收纪律**：所有验收结论必须出自真实命令输出；每阶段末汇报附证据（命令 + 关键输出）；指纹不匹配、结果复核 FAIL 一律如实上报，禁止"应该没问题"。

## 6. 归档与文件逐项归类映射（执行时照此落 MIGRATION_MAP.md）

| 现路径 | 去处 | 备注 |
|---|---|---|
| exp_20260707_hybridv3dev20 | HP_V3/ | v3 dev20 首跑（四归档 replay 回归基线之一） |
| exp_20260708_hybridv3dev20full | HP_V3/ | v3 正式档 §217 |
| exp_20260709_hybridv4smoke | HP_V4/ | 名带 smoke 但是 7 样本×10RT **完整证据档**（§219），保留 |
| exp_20260709_hybridv5dev20full | HP_V5/ | §221 |
| exp_20260710_frbaseline234 | Baseline/ | 永久冻结 FR 基线 §225，所有 HP 版本共同引用 |
| exp_20260710_hybridv6dev20full | HP_V6/ | §223 |
| exp_20260711_hybridv6val40 | HP_V6/ | §227 |
| exp_20260711_hybridv7dev20full | HP_V7/ | §228 |
| exp_20260711_opencode_transport_v2_smoke5 | 核 RT 数：跑满 10RT → transport/；未满 → _attic/ | §229 诊断档；FINDINGS 引用它，进 _attic 时在 MIGRATION_MAP 标注证据链去向 |
| exp_20260712_hybridv7val40_transportv3 | HP_V7/ | Phase 0 核实是否已收尾；未收尾则 Phase B |
| exp_20260713_problem15_official | transport/ | 官方线 abort 病理诊断档（API_TRANSPORT_MINIMAX_OFFICIAL_V1.md §4/§9 引用）；进行中 → Phase B |
| exp_20260713_frtest_robotics1 | 核 RT 数：跑满 → transport/；未满 → _attic/ | 单样本官方线诊断 |
| exp_20260713_frbaseline_v2_rerun | Baseline/ | 备料完成未开跑（85 份 task_plan + manifest）→ Phase B；跑完收尾后组装 frbaseline_v2 |
| _snap_val40/ | HP_V6 重建源之一；收编后整目录移入 HP_V6/_snapshot_provenance/ | 先实测其 src 指纹 == v6 campaign |
| _tmp_rawlog/ | _attic/ | 临时产物 |
| 根部散落 *.log（baseline_verify_* / v7regress_verify_* 等） | _attic/logs_root/ | 保守处理；MIGRATION_MAP 逐条记 |
| docs/API_TRANSPORT_FROZEN_V3.md、API_TRANSPORT_MINIMAX_OFFICIAL_V1.md、TRANSPORT_V3_CLAUDE_CODE_HANDOFF.md、Minimax_OPENAI.md | transport/docs/ | API 文档随主战场走 |
| docs/FR冻结计划.md | Baseline/ | 基线政策文档 |
| docs/ 其余（迭代史/FINDINGS/RESEARCH_JOURNAL/HYBRIDPATCH_DESIGN/active_log/本文） | 原地 docs/ | 跨版本文档 |
| src/（当前） | Phase B 定格为 HP_V7/src/ | 定格前不动（进行中 campaign 依赖此布局续跑） |
| prompts/ | 原地保留 + 拷贝进各 HP_Vx/prompts/ | — |

## 7. 执行时必须核实的假设（Phase 0 逐条出证据）

1. **进行中 campaign 实况**：两个 07-13 campaign 是否仍在跑/未收满（查 checkpoint `stopped_early`/rounds、`tasklist` 有无 python worker）。
2. **`_ROOT`/`_HERE` 解析**：确认 runner/verify/analyze/build_hybrid_split 及 transport 各脚本的路径常量全部 `__file__` 相对或 cwd 相对，且在"cwd=HP_Vx + data junction + prompts 拷贝"下闭合；**逐历史版本重复核实**（旧版代码路径写法可能不同）。
3. **junction 可行性**：`cmd /c mklink /J` 在本机无需管理员即可建目录 junction；Python `open`/`os.walk` 经 junction 正常；不可行 → 改实拷贝（23MB/版，可接受）并在 MIGRATION_MAP 注明。
4. **`.env` 加载路径**：`model_openai.py` 的 `load_dotenv` 从哪找 .env（cwd？`__file__` 上溯？）——决定 cwd=HP_Vx 时密钥是否可见；不可见则用"进程 env 显式设置"方案，不许把 .env 拷贝进版本文件夹。
5. **`_snap_val40/src` 指纹**是否真 == v6 dev20 campaign（迭代史声称，须实测）。
6. **verify/analyze 对归档新路径的兼容**：二者按 `--dir` 工作应无路径耦合，跑一档确认。
7. **git 回滚点**：已于 2026-07-13 提交两笔回滚点 commit（transport 代码与测试 / 项目文档与本交接）。Phase 0 时 `git status` 应为干净树（运行产物除外）；若发现新的未提交改动，先与用户确认处理再动工。

## 8. 分阶段执行

**Phase 0 — 预检（零改动）**：核实 §7 全部假设 + `git fsck` 考古摸底 + 各归档 run_metadata 指纹抽取建表（后续验收的对照基准）。产出：预检报告（落盘 `docs/restructure_phase0_report.md`）。除非发现红线冲突，直接进 Phase A，无需等待用户。

**Phase A — 不触碰活跃面的部分**：
1. 建 `transport/`（src 拷贝当前 7 文件 + fixtures、docs 搬移、API_ITERATION_LOG.md 撰写）。
2. 重建 HP_V3 → 验收 → 挪入其归档 → 逐档结果复核重验。
3. 同法 HP_V6（双源交叉）、HP_V4/V5（§4.4 考古，结果如实定级）。
4. `_attic/` 归类（_tmp_rawlog、根部 log、核实后的非 10RT smoke）。
5. MIGRATION_MAP.md 随做随记。
验证节奏：每建成一个 HP_Vx 立即四连——`py_compile 全过 → 该版测试脚本 PASS → 指纹比对 → 该版归档结果复核 PASS`，全绿才做下一个版本。

**Phase B — 紧接 Phase A 执行（用户已授权，无需再确认）**：
1. 当前 `src/` 定格拷贝为 `HP_V7/src/`（含 prompts/data 布线），验收四连 + v7 两档结果复核。
2. 挪 HP_V7 归档、problem15_official → transport/、frbaseline_v2_rerun → Baseline/，逐档重验（未收尾两档按 §5-4 冻结现状搬移 + 写 RESUME.md；部分产出档以现有已提交行结果复核 PASS 为准）。
3. 顶层 `src/` 整目录移入 `_attic/src_pre_restructure/`（不删，防翻车回滚）。
4. 更新 README.md / CLAUDE.md / AGENTS.md：新目录树、新运行约定（cwd=HP_Vx）、新迭代惯例（见 §9）、命令示例全部换新路径。

**Phase C — 终验**：
1. 全量结果复核清单跑一遍（每个搬移过的归档一行结论）。
2. HP_V7 零 API dry-run：`test_hybrid_executor.py` + `test_model_openai.py` + `splitters.py` 自检全 PASS。
3. MIGRATION_MAP.md 完整性核对（find 顶层残留 vs 映射表）。
4. 终验报告 + 遗留问题清单（如 V4/V5 定级、_attic 待用户清理项）。

## 9. 重构后的迭代惯例（写进新 CLAUDE.md）

- **方法迭代**：复制最新 `HP_Vx` → `HP_V(x+1)`，改动只发生在新文件夹；协议号与文件夹号对齐（hybridpatch/8 ↔ HP_V8）；旧文件夹即冻结。rev 门控代码保留（各版可重放 ≤ 自己版本的旧信封归档）。
- **API 迭代**：只发生在 `transport/`，自带 revision 号（延续 `opencode_stream/N`、`minimax_official_nonstream/N` 惯例）+ API_ITERATION_LOG.md 记录；完成后**拷贝同步进当前活跃 HP_Vx**（同步动作记录双方指纹），冻结版永不回灌。
- **实验落位**：方法实验 → 所属 `HP_Vx/exp_*`；传输诊断 → `transport/exp_*`；基线类 → `Baseline/`；命名规范沿用 `exp_<YYYYMMDD>_<slug>`。
- **归档重放责任**：每个归档由其所在版本文件夹的 verify 重放（HP_V7 因带全部 rev 门可重放 v3–v7 任意档，作兜底）。

## 10. 完成判据（DoD）

- [ ] 目录树与 §3 一致；顶层无未归类残留（对照 MIGRATION_MAP）
- [ ] HP_V3/V6/V7 达到字节级或行为级确证；V4/V5 定级如实记入 VERSION.md
- [ ] 每个搬移归档结果复核重验 PASS（已知预存差值按披露口径核对）
- [ ] 全程零删除（_attic 只收纳）、data/ 零改动、未收尾 campaign 除新增 RESUME.md 外零写入
- [ ] 每个 Phase 有对应里程碑 commit
- [ ] README / CLAUDE / AGENTS 更新完毕，含 §9 惯例
- [ ] 终验报告交付，遗留问题显式列出

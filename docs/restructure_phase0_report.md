# HP_Vx 版本文件夹化重构：Phase 0 预检报告

> [!WARNING]
> **HISTORICAL SNAPSHOT（2026-07-13）**。本文只记录重构执行前的 Phase 0 实况；Phase A/B/C 后续均已完成，文中的“可进入 Phase A”“后续续跑”不再是当前行动项。当前实验状态见 [EXPERIMENT_INDEX.md](./EXPERIMENT_INDEX.md)，当前结论见 [FINDINGS.md](./FINDINGS.md)。

日期：2026-07-13（Asia/Singapore）  
范围：`hybridpatch_clean/`，执行 `docs/RESTRUCTURE_HP_VX_HANDOFF.md` §7、§8 Phase 0  
结论：**预检通过，可进入 Phase A**。发现的唯一红线冲突已由用户明确裁决：`_snap_val40` 中的 `.env` 与 `.env.frkeys` 不进入 `HP_V6`，原字节留在版本目录之外；执行时搬到顶层 `_attic/sensitive_snapshot_env/_snap_val40/`，不读取、不复制、不删除。

## 1. 工作树与回滚点

执行：

```powershell
git status --short --branch
git log --oneline --decorate -6
git rev-parse 'hybridpatch3-snapshot-20260709^{commit}'
git rev-parse 'hybridpatch6-snapshot-20260711^{commit}'
git rev-parse 'hybridpatch7-20260711^{commit}'
```

关键输出：

```text
## main...origin/main [ahead 5]
3b322403 handoff: authorize full autonomous run through Phase B/C ...
deb10e1d docs through 2026-07-13 + HP_Vx restructure handoff ...
8f28abf2 transport: freeze opencode v3 + minimax official nonstream v1 ...
V3 db5048d2e4548fe48a87fdc996a9b617ce0cb896
V6 2e1e004b42a86ab1216726ca2c94f3717ba12cda
V7 3f7423804505597aa9a93b7ce5766bff42c4579b
```

`git status --short` 为空；没有任务开始前的未提交或未跟踪用户改动。交接列出的三笔回滚点均存在且为当前 HEAD 的直接历史。

## 2. worker 与 campaign 实况

### 2.1 无实验 worker

执行：

```powershell
$p = Get-CimInstance Win32_Process | Where-Object { $_.Name -match '^python(w)?\.exe$' }
[pscustomobject]@{
  PythonProcesses = $p.Count
  ExperimentRunner = ($p | Where-Object CommandLine -match 'experiment_runner\.py').Count
  RepoPath = ($p | Where-Object CommandLine -match 'AnchorPatch_goal_promptgen_20260624|hybridpatch_clean').Count
  Problem15 = ($p | Where-Object CommandLine -match 'exp_20260713_problem15_official').Count
  FRBaselineV2 = ($p | Where-Object CommandLine -match 'exp_20260713_frbaseline_v2_rerun').Count
  Val40V3 = ($p | Where-Object CommandLine -match 'exp_20260712_hybridv7val40_transportv3').Count
}
```

关键输出：`ExperimentRunner=0, RepoPath=0, Problem15=0, FRBaselineV2=0, Val40V3=0`。系统中另有与本仓库无关的 Python 进程，不构成搬移风险。

### 2.2 两个未收尾 campaign

对每个 checkpoint 执行等价检查：

```powershell
$ck = Get-Content -Raw <*.ckpt.json> | ConvertFrom-Json
$lines = (Get-Content <result.jsonl> | Measure-Object -Line).Lines
[pscustomobject]@{
  RT = $ck.completed_round_trips
  Lines = $lines
  AtomicMatch = ($lines -eq 2 * $ck.completed_round_trips)
  HasStoppedEarly = ($ck.PSObject.Properties.Name -contains 'stopped_early')
}
```

- `exp_20260713_problem15_official`：15 份 task plan；FR 11 个样本、HP 5 个样本有产出。FR 中 9 个样本为 10RT，`protein1=6RT`、`spreadsheet1=4RT`；HP 为 `crystal6=1RT, filesystem3=2RT, json4=4RT, robotics1=1RT, treebank4=1RT`。16/16 结果均满足 `JSONL 行数 = 2 × completed_round_trips`，无 `stopped_early`。结论：冻结现状整体搬移，新增 `RESUME.md` 后续跑。
- `exp_20260713_frbaseline_v2_rerun`：85 份 task plan + `rerun_manifest.json`；checkpoint=0、结果 JSONL=0。manifest 记录 `contaminated=85, clean_kept=148`。结论：仅备料、尚未开跑，冻结现状整体搬移，新增 `RESUME.md` 后续跑。

### 2.3 V7 val40 与 smoke 条件判断

- `exp_20260712_hybridv7val40_transportv3`：80 个结果档，共 780RT/1560 行；HP 39 个 10RT + `json4=2RT`，FR 38 个 10RT + `earncall1=6RT, json4=2RT`。既有结果复核日志为 `PASS — 780 backward RS independently reproduced`；`analysis/val40v3_38sample_paired.md` 明确以 38 个完整配对样本为 canonical 口径，文档状态已标 complete。按已收官归档迁入 `HP_V7`，保留三个基础设施 partial 的原貌。
- `exp_20260709_hybridv4smoke`：7 个 HP 样本均为 10RT，属于完整证据档，迁入 `HP_V4`。
- `exp_20260711_opencode_transport_v2_smoke5`：5 样本×双臂，每档仅 2RT，迁入 `_attic/`。
- `exp_20260713_frtest_robotics1`：单样本 FR 仅 1RT，迁入 `_attic/`。

顶层实测共有 13 个 `exp_*` 目录、25 个根部 `*.log`；均已在交接映射中找到确定去处。

## 3. 路径、data junction 与凭据

### 3.1 HP_Vx 运行根闭合

静态核查当前、V3 tag、V6 tag 的 `experiment_runner.py`、`verify_anchorpatch.py`、`analyze.py`、`build_hybrid_split.py` 与 domain：

```text
_HERE = dirname(__file__)
_ROOT = dirname(_HERE)
SAMPLES_ROOT = _ROOT/data/samples_delegate52
domain prompt = cwd 相对的 prompts/...
verify/analyze --dir = 直接使用调用方路径
```

因此每版使用 `cwd=HP_Vx`、版内 `data` junction、版内 `prompts` 实拷贝即可运行；无需修改历史源码。`verify_anchorpatch.py --dir` 对新归档路径无硬编码。`analyze.py` 会写 `<dir>/analysis`，故归档搬移验收只运行只读 verify，不刷新 analyze。

### 3.2 junction 实测

仅在 `%TEMP%\anchorpatch_junction_probe_<guid>` 执行：

```text
cmd /c mklink /J <junction> <target>  -> Junction created
Python open()                         -> junction-ok
Python os.walk()                      -> [('.', ['probe.txt'])]
清理临时探针                          -> CLEANED=True
```

结论：本机无需管理员权限即可建 junction；Python 读与遍历均正常。正式版本目录使用 `cmd /c mklink /J` 指向共享顶层 `data/`。

搬移前共享数据只读基线：

```text
files=1764
bytes=19789798
manifest_sha256=54d9c8a9314f3c0d6673ca05fb5e05732ff16e81891795a7008d61ff1f504a4e
```

聚合方式为按相对路径排序后，对 `relative_path\0size\0sha256(file)` 再取 SHA-256；Phase C 用同一算法复核。

### 3.3 `.env` 加载与红线裁决

当前、V3、V6 的 `model_openai.py` 均只查 `<运行根>/.env` 与 `<运行根>/src/.env`。移入 HP_Vx 后不会自动找到顶层 `hybridpatch_clean/.env`。`fr_baseline_dispatch.py` / `probe_fr_keys.py` 默认的 `.env.frkeys` 也会指向版内。

执行约定：凭据由父 PowerShell/调度器显式注入环境；多 Key 文件显式使用 `--keys_file ../.env.frkeys`。任何 `.env*` 均不复制入版本目录或文档。

实况发现 `_snap_val40/` 除 `src/prompts/data` 外还含 `.env`（516 B）与 `.env.frkeys`（981 B）；未读取内容。用户已于 2026-07-13 裁决“不要放进去，留在外面”。故执行时把这两份文件原字节 move 到 `_attic/sensitive_snapshot_env/_snap_val40/`，再收编其余快照；全程零复制、零删除。

## 4. 指纹真值与 Git 考古

### 4.1 metadata 字段实况

交接文档按当前 11 项指纹描述验收，但 V3–V6 历史 campaign 的 `run_metadata.jsonl` 实际仅记录 8 项：`patch_schema/splitters/experiment_runner/hybrid_schema/hybrid_index/hybrid_prompt/hybrid_executor/hybrid_gate`。`model_openai/run_meta/requirements.txt` 是后来加入的字段。

因此旧版验收口径为“历史字段 N/N + 同锚点补充项披露”，不得虚构 11/11。每版仍必须带版内 `requirements.txt`，因为各版 `run_meta.py` 从 `src/../requirements.txt` 计算该项；V3 requirements 与 HEAD 不同，V6 requirements 与 HEAD 相同。

### 4.2 目标指纹摘要

| 版本/归档 | patch | split | runner | schema | index | prompt | executor | gate |
|---|---|---|---|---|---|---|---|---|
| V3 正式档 | `b166af25b94e` | `663c467adf3d` | `202fe11c8f17` | `9ada21329c38` | `65d390993c3e` | `ee41b055d51b` | `9b16dad84d25` | `48dd3e7af480` |
| V4 | `ba8a9eb35f06` | `690b1981879f` | `291ec60aa8df` | `1f7e9ba68bb6` | `d66d13a8fb60` | `7ff76aec06b0` | `d9297cccfadf` | `3c992dd20f1f` |
| V5 | `ba8a9eb35f06` | `690b1981879f` | `291ec60aa8df` | `54abc09fbcb8` | `d66d13a8fb60` | `81668ab72e5d` | `9746659b868f` | `3c992dd20f1f` |
| V6 | `ba8a9eb35f06` | `690b1981879f` | `291ec60aa8df` | `edbd2517894a` | `d66d13a8fb60` | `81668ab72e5d` | `a4ea477217b0` | `3143bd4b3d0a` |
| V7 dev20 | `ba8a9eb35f06` | `690b1981879f` | `5dbe66187330` | `93e8b9c78f12` | `d66d13a8fb60` | `81668ab72e5d` | `79ef3e766a00` | `06299fbd4822` |

V7 val40 transport-v3 的方法字段与 V7 dev20 相同，但 `runner=dd3f706bdd51, model_openai=6ddfdafcd065, run_meta=e0d7d7861cfb, requirements=38ffe361be94`。当前 HEAD 对该档 9/11：`experiment_runner.py` 与 `model_openai.py` 因 07-13 官方传输/面板演进不匹配，其余 9 项匹配；后续以结果复核给行为级证据。

### 4.3 V3、V4、V5、V6 来源结论

- **V3**：tag 对正式档历史 8 项为 7/8，唯一差异是 runner（档 `202fe11c8f17`，tag `b00c4a6769d9`）。按 tag 取完整源码与同版 requirements，结果复核决定最终等级。
- **V4**：`git fsck --full --no-reflogs --unreachable --no-progress` 找到完整 src 子树 `ddad676708c54af4913083d6b9153e73444a2ce0`，由 2026-07-09 的根树（最早 `78fd5f164cc91607e7dc6b1230af18dcd1728ff3`）以正确路径引用。按 Git for Windows checkout filter 物化后历史字段 **8/8** 匹配。直接 `git archive` 会得到 LF 并产生伪 mismatch，重建须用 checkout filter 保留历史 CRLF 工作树字节。
- **V5**：全扫 31,190 个不可达 blob 后，核心目标 `hybrid_schema=54abc09fbcb8`、`hybrid_executor=9746659b868f` 无 raw/CRLF 命中。按交接既定路径执行 `V6 − C1 − C2` 逆推；若指纹不全匹配但结果复核 PASS，定行为级确证，否则近似重建。
- **V6**：`_snap_val40/src` 对两个 V6 campaign 的历史字段 **8/8**；snapshot 的 `model_openai/run_meta` 与 V6 tag 也匹配，即 src 补充口径 10/10。snapshot 本身无根 requirements，版内 requirements 从 V6 tag 取得并单列。

Git 考古统计：43,457 个 unreachable 对象（31,190 blob / 12,266 tree / 1 commit）；唯一 unreachable commit 是 V7 pre-amend，与 V4/V5 敏感源码无关。考古命令未使用会写 `.git/lost-found` 的 `--lost-found`。

## 5. transport 独立性边界

目标规格的 `transport/src` 七个运行时文件为 `model_openai.py, run_meta.py, experiment_runner.py, fr_baseline_dispatch.py, launch_pipeline.py, monitor_experiment.py, probe_fr_keys.py`，另带 `test_fixtures/`。

仅这七个文件不是完整实验运行根：runner 仍依赖方法五件套、utils、domains、data、prompts；dispatch/launch 固定启动同目录 runner；`test_model_openai.py` 也依赖 runner 与其他模块。依据交接 §9 的同步规则，顶层 `transport/` 定性为 API 源码主战场；完成 API 改动后拷贝同步进当前活跃 HP_Vx，从该 HP_Vx 运行完整调度和零 API 回归。`monitor_experiment.py` 可按 `--dir` 独立运行，`probe_fr_keys.py` 可在显式 key file/env 下独立运行。

这不是红线冲突：交接已规定活跃 HP 通过拷贝同步、冻结版不回灌，并要求独跑前核实路径。最终文档将明确该边界。

## 6. Phase 0 判定与 Phase A 输入

- 无 worker 与归档并发写风险。
- junction、HP_Vx 的 data/prompts/cwd 路径闭合。
- `.env*` 处理已有用户裁决，不进入任何版本目录。
- V4 有 8/8 指纹源码候选；V5 走明确降级路径。
- verify 支持新 `--dir`；analyze 因会写归档，不用于字节不变验收。
- 每版根必须额外放同版 `requirements.txt`，这是源码零修改条件下保持指纹/运行语义的必要布局项。

**停止条件检查：无剩余红线冲突，无“实况不符且文档没有降级路径”的事项。Phase A 可以开始。**

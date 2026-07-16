# HP_Vx 版本文件夹化重构终验报告

> [!WARNING]
> **HISTORICAL SNAPSHOT（2026-07-13）**。本文验证的是当日重构本身，不是当前实验状态页。文中两个 campaign “未收尾、按 RESUME 续跑”是当时事实，现均已冻结且明确不得续跑。当前范围、canonical scope 与 verification 以 [EXPERIMENT_INDEX.md](./EXPERIMENT_INDEX.md) 为准，后续实验结论以 [FINDINGS.md](./FINDINGS.md) 最新编号条目为准。

日期：2026-07-13（Asia/Singapore）

权威规格：`docs/RESTRUCTURE_HP_VX_HANDOFF.md`

范围：Phase 0 → A → B → C
最终结论：**完成，DoD 全部满足；有 4 项已披露遗留，不影响本次重构验收。**

## 1. 可信结论摘要

- 目标布局已落地：`HP_V3`–`HP_V7` 五个自包含版本根、`transport/`、`Baseline/`、共享只读 `data/`、项目活文档 `docs/`、只收纳不删除的 `_attic/` 与全量 `MIGRATION_MAP.md`。
- V3/V4/V5/V6 的历史 campaign 指纹均达到 **8/8**。旧 metadata 当时只记录 8 项，不能虚构 11/11；三项后来新增字段在各 `VERSION.md` 单独披露来源与重建值。
- V3/V4/V5/V6 定级为**字节级确证**；V7 精确定格当前 HEAD，但因归档后 runner/transport 的已提交演进，相对 dev20 为 7/8、相对 val40 为 9/11，依靠两档完整结果复核定为**行为级确证**。
- Phase C 对 13 个搬移归档再次全量重放，共覆盖 **6114** 条 backward RS：6113 条精确一致；唯一非一致项严格等于交接预披露的 V3 `quantum4 RT9 stored=0.9850 / recomputed=1.0000`，没有第二处差异。
- HP_V7 零 API 回归全绿：executor **49/49**、transport **33/33**、splitter 全部 byte-exact coverage PASS。
- `data/` 搬移前后清单哈希相同；五个 HP 的 `data` 均为指向同一共享目录的 Windows junction。
- 没有 `.env*` 进入任何 HP 或 `transport/`。`_snap_val40` 中两份凭据按用户裁决原字节留在 `_attic/sensitive_snapshot_env/_snap_val40/`，未读取、未复制、未删除。
- 两个未收尾 campaign 只增加各自 `RESUME.md`；续跑命令分别覆盖 15/15 与 85/85 样本，无遗漏、无重复。

## 2. Phase 里程碑

执行：

```powershell
git log --oneline -4
```

关键输出：

```text
17e7d317 restructure phase B: freeze HP_V7 and switch entry layout
717c1b81 restructure phase A: rebuild HP_V3 through HP_V6
1d55772c restructure phase 0: record preflight and archaeology evidence
3b322403 handoff: authorize full autonomous run through Phase B/C ...
```

- Phase 0：`1d55772c`，预检、路径/junction、worker/campaign、Git 考古与凭据冲突裁决落盘。
- Phase A：`717c1b81`，transport 初建、V3–V6 重建与四连、Baseline 与 `_attic` 第一批归类。
- Phase B：`17e7d317`，V7 定格、未收尾 campaign/RESUME、旧 `src` 收纳、入口文档切换。
- Phase C：本报告所在最终提交；提交后以 `git log -1` 为准。

## 3. DoD-1：目录树、运行根与顶层残留

执行：

```powershell
foreach ($v in 3..7) {
  $j = Get-Item "HP_V$v\data"
  $py = (Get-ChildItem -Recurse -File -Filter '*.py' "HP_V$v\src").Count
  "HP_V${v}: py=$py data_type=$($j.LinkType) target=$($j.Target)"
}
"src=$(Test-Path .\src) exp_dirs=$((Get-ChildItem -Directory -Filter 'exp_*').Count) logs=$((Get-ChildItem -File -Filter '*.log').Count)"
Select-String -Path .\MIGRATION_MAP.md -SimpleMatch '| [ ] |'
```

关键输出：

```text
HP_V3: py=76 data_type=Junction target=...\hybridpatch_clean\data
HP_V4: py=76 data_type=Junction target=...\hybridpatch_clean\data
HP_V5: py=78 data_type=Junction target=...\hybridpatch_clean\data
HP_V6: py=78 data_type=Junction target=...\hybridpatch_clean\data
HP_V7: py=82 data_type=Junction target=...\hybridpatch_clean\data
src=False exp_dirs=0 logs=0
MIGRATION pending rows: 0
```

每版均实测存在 `src/`、`prompts/`、`data` junction、`requirements.txt`、`VERSION.md`。顶层 `_snap_val40`、`_tmp_rawlog` 也已无残留；旧 `src/` 在确认与 HP_V7 的 84 个非 bytecode 文件逐字节一致后整体移入 `_attic/src_pre_restructure/`。

判定：**PASS**。

## 4. DoD-2：逐版四连验收与定级

统一命令形态（工作目录为对应 `HP_Vx`）：

```powershell
$env:PYTHONUTF8 = "1"
# py_compile 的 cfile 定向 %TEMP%，不在冻结源码内产生 pyc
python -B src\test_hybrid_executor.py
python -B src\splitters.py
python -B src\verify_anchorpatch.py --dir <该版归档>
```

| 版本 | py_compile | 版本测试 | 历史指纹 | 归档行为重放 | 最终等级 |
|---|---:|---:|---:|---|---|
| HP_V3 | 76 files | 21/21 + splitter PASS | 8/8 | 0707 PASS 400；0708 仅已知 quantum4 RT9 差异 | 字节级确证 |
| HP_V4 | 76 files | 27/27 + splitter PASS | 8/8 | PASS 70/70 | 字节级确证 |
| HP_V5 | 78 files | 32/32 + splitter PASS | 8/8 | PASS 400/400 | 字节级确证 |
| HP_V6 | 78 files | 40/40 + splitter PASS | 8/8；src 双源补充 10/10 | PASS 400/400 + 800/800 | 字节级确证 |
| HP_V7 | 82 files | 49/49 + splitter PASS | dev20 7/8；val40 9/11 | PASS 400/400 + 780/780 | 行为级确证 |

### V3 来源证据

- 正式 tag：`db5048d2`。
- campaign 完整 src tree：`30b52270f3e3b692fbee5b760c59939cc3d2adf5`；runner blob：`83e68c2a54521627db93499952a80343cce06bf0`。
- 正式 tag 的非指纹文件 + campaign 精确 runner 组装后，raw LF 8/8。`.gitattributes` 对 HP 源码设置 `-text`，防止 Git checkout 改写历史字节。

### V4 来源证据

- unreachable root tree：`78fd5f164cc91607e7dc6b1230af18dcd1728ff3`；src tree：`ddad676708c54af4913083d6b9153e73444a2ce0`。
- 按 Windows checkout filter 物化 CRLF 后 8/8；直接 raw LF 会产生伪 mismatch，因此未采用。

### V5 来源证据

- Git dangling blob 未命中核心 V5 文件后，按既定降级路径继续考古。
- 两个独立 Claude Code file-history session 找到字节相同的 V5 `hybrid_schema`、`hybrid_executor`、`hybrid_gate`、32-test 文件；三项 campaign 指纹直接命中。
- 用 V6 snapshot 完整基底覆盖这四个精确历史文件，最终 8/8、32/32、结果复核 400/400，不再是近似重建。

### V6/V7 口径

- V6 `_snap_val40/src` 对历史 8 项 8/8；`model_openai/run_meta` 与 V6 tag 交叉一致；requirements 来自同版 tag。
- V7 当前 HEAD 相对 val40 的 mismatch 仅 `experiment_runner.py` 与 `model_openai.py`；相对 dev20 仅 runner。两档用当前 verifier 完整 PASS，故证据定级为行为级，而非谎报字节级。

逐文件哈希、来源与每条命令末行见 `HP_V3/VERSION.md` … `HP_V7/VERSION.md`。

判定：**PASS**。

## 5. DoD-3：Phase C 全量结果复核清单

所有命令设置 `PYTHONUTF8=1`、使用 `python -B`；完整输出只写 `%TEMP%`，提取退出码与末行后清理。除 V3 已知例外外，退出码均为 0。

| cwd | `--dir` | 退出码 | 真实关键输出 |
|---|---|---:|---|
| HP_V3 | `exp_20260707_hybridv3dev20` | 0 | `PASS — 400 backward RS independently reproduced` |
| HP_V3 | `exp_20260708_hybridv3dev20full` | 1 | 唯一 `[hybridpatch/quantum4] MISMATCH x1: RT9 stored=0.9850 recomputed=1.0000` |
| HP_V4 | `exp_20260709_hybridv4smoke` | 0 | `PASS — 70 backward RS independently reproduced` |
| HP_V5 | `exp_20260709_hybridv5dev20full` | 0 | `PASS — 400 backward RS independently reproduced` |
| HP_V6 | `exp_20260710_hybridv6dev20full` | 0 | `PASS — 400 backward RS independently reproduced` |
| HP_V6 | `exp_20260711_hybridv6val40` | 0 | `PASS — 800 backward RS independently reproduced` |
| HP_V7 | `../Baseline/exp_20260710_frbaseline234` | 0 | `PASS — 2334 backward RS independently reproduced` |
| HP_V7 | `exp_20260711_hybridv7dev20full` | 0 | `PASS — 400 backward RS independently reproduced` |
| HP_V7 | `exp_20260712_hybridv7val40_transportv3` | 0 | `PASS — 780 backward RS independently reproduced` |
| HP_V7 | `../_attic/exp_20260711_opencode_transport_v2_smoke5` | 0 | `PASS — 20 backward RS independently reproduced` |
| HP_V7 | `../_attic/exp_20260713_frtest_robotics1` | 0 | `PASS — 1 backward RS independently reproduced` |
| HP_V7 | `../transport/exp_20260713_problem15_official` | 0 | `PASS — 109 backward RS independently reproduced` |
| HP_V7 | `../Baseline/exp_20260713_frbaseline_v2_rerun` | 0 | `PASS — 0 backward RS independently reproduced` |

合计：13 个归档，6114 条 backward RS；6113 条精确一致 + 1 条权威规格预披露差异。`frbaseline_v2_rerun` 的 0/0 只证明当前备料档没有可重放行，不构成实验结果证据。

V3 正式档的结果复核状态为 FAIL，本报告没有把它错写成普通 PASS；接受依据仅是差异样本、RT、stored/recomputed 数值全部严格等于迭代史与交接文档的唯一已知评估器非确定性。

判定：**PASS（含唯一预披露例外，无新增漂移）**。

## 6. DoD-4：归档字节、data 与凭据保护

### 6.1 归档 move 前后计数

所有目录均为同卷 `Move-Item` 整体搬移。搬移当场比较文件数与总字节数：

| 归档 | files | bytes | 搬移后 |
|---|---:|---:|---|
| V3 0707 | 3928 | 36,738,556 | exact |
| V3 0708full | 5910 | 210,353,802 | exact |
| V4 smoke | 965 | 42,175,616 | exact |
| V5 dev20 | 6484 | 234,889,045 | exact |
| FR baseline234 | 31,942 | 888,871,978 | exact |
| V6 dev20 | 2954 | 147,589,690 | exact |
| V6 val40 | 6306 | 294,738,428 | exact |
| V7 dev20 | 2871 | 142,416,115 | exact |
| V7 val40 | 12,935 | 757,909,214 | exact |
| transport-v2 smoke5 | 305 | 1,139,105,047 | exact |
| frtest robotics1 | 23 | 231,882 | exact |
| problem15 official | 1662 | 14,597,386 | exact，随后只加 RESUME |
| frbaseline-v2 prep | 86 | 34,986 | exact，随后只加 RESUME |

Phase C 再测未收尾目录：

```text
problem15: files=1663 = 1662+1; bytes=14,599,788 = 14,597,386+RESUME(2402)
frbaseline-v2: files=87 = 86+1; bytes=38,187 = 34,986+RESUME(3201)
```

因此两档相对冻结快照的唯一增量就是授权的 `RESUME.md`。

### 6.2 data 只读证据

执行算法：对 `data/` 全部文件按相对路径排序，记录 `relative_path\0size\0sha256(file)`，再对记录流取 SHA-256。

Phase 0 与 Phase C 输出完全相同：

```text
files=1764
bytes=19789798
manifest_sha256=54d9c8a9314f3c0d6673ca05fb5e05732ff16e81891795a7008d61ff1f504a4e
```

### 6.3 凭据与 snapshot

执行：

```powershell
Get-ChildItem HP_V3,HP_V4,HP_V5,HP_V6,HP_V7,transport -Recurse -Force -File |
  Where-Object Name -Like '.env*'
```

输出：`0` 个。

原 `_snap_val40` 共 1920 files / 22,862,116 B；裁决后：

- `HP_V6/_snapshot_provenance/`：1918 files / 22,860,619 B。
- `_attic/sensitive_snapshot_env/_snap_val40/.env`：516 B。
- `_attic/sensitive_snapshot_env/_snap_val40/.env.frkeys`：981 B。

三者总量精确回到原 snapshot；凭据内容从未打印、写入文档或复制。

### 6.4 只搬移不删除

- 所有规格内旧路径均在 `MIGRATION_MAP.md` 有新去处，pending 行为 0。
- 顶层 25 个历史日志全部在 `_attic/logs_root/`；`_tmp_rawlog` 在 `_attic/_tmp_rawlog/`；短 smoke 保留在 `_attic/`。
- 首次 V3 重建时生成的错误 CRLF 候选没有删除，保留在 `_attic/reconstruction_attempts/`。
- 旧顶层 `src/` 既有 HP_V7 精确副本，又整体保留于 `_attic/src_pre_restructure/`。

判定：**PASS**。

## 7. DoD-5：HP_V7 零 API dry-run

执行（cwd=`HP_V7`）：

```powershell
$env:PYTHONUTF8 = "1"
python -B src\test_hybrid_executor.py
python -B src\test_model_openai.py
python -B src\splitters.py
```

关键输出：

```text
RESULT: PASS (49 tests)
Ran 33 tests in 0.133s
OK
RESULT: PASS (all splitters byte-exact coverage)
```

`test_model_openai.py` 使用 fixture/mock；日志中的 quota wait 文本是测试分支，wall time 0.133s，没有真实等待或 API 请求。

判定：**PASS**。

## 8. DoD-6：文档、迁移表与续跑闭合

- `README.md`：新目录树、HP_V7 运行根、PowerShell 命令、dotenv 父级凭据注入、实验归属已更新。
- `CLAUDE.md`：新布局、cwd 约定、方法/API 两条迭代纪律、归档落位、冻结边界与新文档路径已更新。
- `AGENTS.md`：根目录/HP 根工作目录分工、junction、凭据、实验落位和冻结版规则已更新。
- transport 四份文档已存在于 `transport/docs/`；FR 政策已存在于 `Baseline/FR冻结计划.md`；活动文档内旧路径引用已更新。
- `MIGRATION_MAP.md`：全部映射已执行，`| [ ] |` 计数为 0。
- `transport/API_ITERATION_LOG.md`：SSE → transport-v2 → OpenCode v3 → official nonstream/1 的语义与去处齐全。

续跑覆盖自动核对：

```text
problem15 RESUME: groups=[5,5,5], total=15, unique=15, task_plan set_match=True
frbaseline-v2 RESUME: groups=[29,28,28], total=85, unique=85, manifest set_match=True
```

两份续跑说明均固定 `cwd=HP_V7`、`MINIMAX_TRANSPORT=official_nonstream`、`MINIMAX_HARD_TIMEOUT=7200`、单 Key 3 worker 并发与新相对 out_dir；未启动付费续跑。

判定：**PASS**。

## 9. DoD 总表

- [x] 目录树与目标一致；顶层无 `src/`、`exp_*`、根部日志或未映射残留。
- [x] HP_V3/V4/V5/V6 字节级确证；HP_V7 行为级确证，差异如实披露。
- [x] 每个搬移归档 Phase C 重验；唯一 V3 预存差值严格核对，无新增漂移。
- [x] 全程原有内容零删除；`data/` 清单哈希零变化；未收尾 campaign 只有 `RESUME.md` 增量。
- [x] Phase 0/A/B 已有独立 milestone commit；Phase C 由本报告提交收口。
- [x] README / CLAUDE / AGENTS / API iteration log / MIGRATION_MAP / 五份 VERSION / 两份 RESUME 完成。
- [x] 终验报告逐项给出命令、退出码或关键输出，并列明遗留。

## 10. 已披露遗留与后续动作

1. **V3 已知评估器差异**：正式档 verifier 仍按设计退出 1；唯一差异为 quantum4 RT9 0.985→1.000。若未来出现第二条 mismatch，不能沿用本例外。
2. **V7 为行为级而非字节级**：当前 HEAD 含归档后的 transport/runner 演进；两档都可完整重放，但历史 fingerprint 不全相等。
3. **两个 campaign 未收尾**：`problem15_official` 与 `frbaseline_v2_rerun` 仅冻结搬移，后续按各自 `RESUME.md` 付费续跑；本任务没有启动 API。
4. **`_attic` 待用户决定清理**：包括短 smoke、历史日志、旧入口、V3 重建候选与 snapshot 凭据。按红线，本任务未真删除任何一项。

补充运行边界：`transport/src/` 是七个运行时 API/调度文件 + 测试资产的源码主战场，不是完整方法运行根；完整回归在同步后的活跃 HP 版本运行。Windows junction 是本地文件系统对象，若工作区整体迁移或从新 clone 恢复，需要按 `README.md` 重新执行 `cmd /c mklink /J`。

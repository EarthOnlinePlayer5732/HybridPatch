# HP_V3（hybridpatch/3）版本卡

## 五问摘要

1. **为什么迭代**：上一轮 think5 暴露 occurrence 漂移与 repair 盲区。执行器按变异中间态解释 occurrence，与模型在 prompt 中看到的编号不一致；repair 也只覆盖 JSON/schema 错误，无法处理可精确描述的 op 拒绝、route violation、空输出和 gate 失败。
2. **优化方向**：local_patch 改为步骤输入快照语义；所有 span 在输入文档上定位、检查不重叠后从右向左一次性应用。repair 扩展到可描述失败，并携带任务与可编辑文档 grounding。自本版起，语义变化走新 protocol rev，旧归档保持可重放。
3. **效果**：正式 dev20 双臂档 `exp_20260708_hybridv3dev20full` 中，HP 0.910、FR 0.685，配对 Δ+0.225；但约四分之三 headline 来自 FR provider 空返回链毒化。rewind 后 Δ 收敛到 +0.141；再剔除仍随机复发的三个样本后为 +0.048。结构性收益是 HP 用 repair/kept-context 吸收失败，避免一次空返回污染后续整链。
4. **新问题**：bulk_patch 尚未采用文件级/快照匹配；`@body:` 空白与 LaTeX 反斜杠转义存在缺口；bounded_rewrite 占比 74.2%，身份稀释明显。`quantum4` RT9 有已披露的评估器环境敏感差值，stored=0.985、recomputed=1.000。
5. **下一轮靶子**：D1 bulk 文件级匹配、D2 body-ref 空白归一、D3 ws 两趟化，以及 P1 按编辑足迹施加路由压强。

历史数字出处：`docs/项目结构与迭代史.md` §3.1、§7（重构后相对路径为 `../docs/...`）。

## 来源与重建

- 协议：`hybridpatch/3`
- 正式 tag：`hybridpatch3-snapshot-20260709`，commit `db5048d2e4548fe48a87fdc996a9b617ce0cb896`
- campaign 源码树：unreachable root tree `205bfad8a0976b1b7e9c69f2cbbbea4e14649af0`，完整 src tree `30b52270f3e3b692fbee5b760c59939cc3d2adf5`
- 组装方式：非指纹文件取正式 tag 原始 LF 字节；八个历史指纹文件中，tag 已匹配七项，仅 `experiment_runner.py` 用 campaign 源码树的精确 blob `83e68c2a54521627db93499952a80343cce06bf0` 补齐。`requirements.txt` 取正式 tag。
- 布局：`prompts/` 为顶层原字节副本；`data/` 是指向共享顶层 `data/` 的 Windows junction。
- 源码未为新布局修改任何 import、路径常量或逻辑。

## 指纹验收

旧 campaign metadata 只记录 8 项，不含后来加入的 `model_openai.py`、`run_meta.py`、`requirements.txt`；因此不能虚构 11/11 campaign 结论。

| 文件 | 归档 | 重建 | 结果 |
|---|---|---|---|
| `patch_schema.py` | `b166af25b94e` | `b166af25b94e` | MATCH |
| `splitters.py` | `663c467adf3d` | `663c467adf3d` | MATCH |
| `experiment_runner.py` | `202fe11c8f17` | `202fe11c8f17` | MATCH |
| `hybrid_schema.py` | `9ada21329c38` | `9ada21329c38` | MATCH |
| `hybrid_index.py` | `65d390993c3e` | `65d390993c3e` | MATCH |
| `hybrid_prompt.py` | `ee41b055d51b` | `ee41b055d51b` | MATCH |
| `hybrid_executor.py` | `9b16dad84d25` | `9b16dad84d25` | MATCH |
| `hybrid_gate.py` | `48dd3e7af480` | `48dd3e7af480` | MATCH |

补充三项仅记录重建值，不声称 campaign 比对：`model_openai.py=c1ec8518b35a`、`run_meta.py=8d8e15fab076`、`requirements.txt=ae50444f1fc4`。

**验收等级：字节级确证（历史 metadata 8/8；其余文件按正式 tag 来源披露）。**

## 四连验收（2026-07-13）

工作目录：`hybridpatch_clean/HP_V3`，所有 Python 命令均设置 `PYTHONUTF8=1`，测试使用 `python -B`。

1. `py_compile.compile(..., cfile=%TEMP%)`：`PY_COMPILE PASS files=76`。
2. `python -B src/test_hybrid_executor.py`：`RESULT: PASS (21 tests)`；`python -B src/splitters.py`：`RESULT: PASS (all splitters byte-exact coverage)`。
3. 正式档 8 项指纹：`FINGERPRINT PASS 8/8`。
4. 归档结果复核：
   - `python -B src/verify_anchorpatch.py --dir exp_20260707_hybridv3dev20`：退出 0；结果复核 PASS，400 个 backward RS 从 raw responses 独立复现。
   - `python -B src/verify_anchorpatch.py --dir exp_20260708_hybridv3dev20full`：退出 1，且唯一 mismatch 精确为 `[hybridpatch/quantum4] MISMATCH x1: RT9 stored=0.9850 recomputed=1.0000`；没有新增差异。按权威交接与迭代史的预披露口径，这是评估器非确定性已知例外，不是归档篡改或重建失败。

归档搬移前后元数据计数/字节数一致：

- `exp_20260707_hybridv3dev20`：3928 files，36,738,556 B。
- `exp_20260708_hybridv3dev20full`：5910 files，210,353,802 B。

## 运行方式

```powershell
cd HP_V3
$env:PYTHONUTF8 = "1"
python src/experiment_runner.py ...
python -B src/verify_anchorpatch.py --dir exp_20260708_hybridv3dev20full
```

凭据必须由父进程环境显式注入；本目录不得放置 `.env` 或 `.env.frkeys`。

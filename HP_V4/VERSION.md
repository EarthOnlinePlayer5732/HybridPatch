# HP_V4（hybridpatch/4）版本卡

## 五问摘要

1. **为什么迭代**：V3 暴露 D1/D2/D3 三个执行器缺口，以及“多数行不变却走 bounded_rewrite”的身份稀释。
2. **优化方向**：bulk 文件级匹配与跨块右到左应用；`@body:` 哨兵空白归一；空白容错先匹配原 token、再做转义回退；prompt 增加按编辑足迹路由的 P1 压强。
3. **效果**：`exp_20260709_hybridv4smoke` 是 7 个靶样本×HP 单臂×10RT 的完整证据档，历史与本次结果复核均 PASS 70/70。kept-context 从同集合 V3 的 5 个降为 0，forward no-op 与 gate 失败均为 0，repair 6/6 被采用。patch 路由 39.3%→59.8%，生成字节占比 0.534→0.418；聚合 RS 0.887 vs V3 0.893，表面持平。
4. **新问题**：mathlean2 暴露 delete 语义误用和 repair 放大；foodmenu6 的跌幅来自评估器可见性彩票；fonteng3 RT7 暴露 bulk 在变异缓冲区顺序执行会改写刚插入的撤销线索。
5. **下一轮靶子**：F1 bulk 快照定位、F3 delete 语义提示；repair 采纳闸保留为观察项。

历史数字出处：`docs/项目结构与迭代史.md` §3.2、§7（重构后相对路径为 `../docs/...`）。

## 来源与重建

- 协议：`hybridpatch/4`
- 正式 commit：无。
- 考古来源：unreachable root tree `78fd5f164cc91607e7dc6b1230af18dcd1728ff3`，完整 src 子树 `ddad676708c54af4913083d6b9153e73444a2ce0`。
- 重建方式：通过 Git checkout filter 物化历史 Windows CRLF 工作树字节。直接使用 raw LF blob 会产生伪 mismatch，因此未采用。
- 完整性：`src/` 与 `requirements.txt` 取同一 unreachable 根树；`prompts/` 为当前顶层原字节副本；`data/` 为共享顶层 `data/` 的 Windows junction。
- 源码未为新布局修改任何 import、路径常量或逻辑。

## 指纹验收

历史 metadata 记录 8 项：

| 文件 | 归档 | 重建 | 结果 |
|---|---|---|---|
| `patch_schema.py` | `ba8a9eb35f06` | `ba8a9eb35f06` | MATCH |
| `splitters.py` | `690b1981879f` | `690b1981879f` | MATCH |
| `experiment_runner.py` | `291ec60aa8df` | `291ec60aa8df` | MATCH |
| `hybrid_schema.py` | `1f7e9ba68bb6` | `1f7e9ba68bb6` | MATCH |
| `hybrid_index.py` | `d66d13a8fb60` | `d66d13a8fb60` | MATCH |
| `hybrid_prompt.py` | `7ff76aec06b0` | `7ff76aec06b0` | MATCH |
| `hybrid_executor.py` | `d9297cccfadf` | `d9297cccfadf` | MATCH |
| `hybrid_gate.py` | `3c992dd20f1f` | `3c992dd20f1f` | MATCH |

补充三项仅记录重建值：`model_openai.py=4afd6dc5da79`、`run_meta.py=4f232bd71c5e`、`requirements.txt=ab1f5954f83e`。

**验收等级：字节级确证（历史 metadata 8/8；非指纹文件随同一完整考古树披露）。**

## 四连验收（2026-07-13）

工作目录：`hybridpatch_clean/HP_V4`；Python 命令设置 `PYTHONUTF8=1` 并使用 `-B`。

1. `py_compile.compile(..., cfile=%TEMP%)`：`PY_COMPILE PASS files=76`。
2. `python -B src/test_hybrid_executor.py`：`RESULT: PASS (27 tests)`；`python -B src/splitters.py`：`RESULT: PASS (all splitters byte-exact coverage)`。
3. `exp_20260709_hybridv4smoke` 指纹：`FINGERPRINT PASS 8/8`。
4. `python -B src/verify_anchorpatch.py --dir exp_20260709_hybridv4smoke`：退出 0；结果复核 PASS，70 个 backward RS 从 raw responses 独立复现。

归档搬移前后均为 965 files、42,175,616 B。

## 运行方式

```powershell
cd HP_V4
$env:PYTHONUTF8 = "1"
python src/experiment_runner.py ...
python -B src/verify_anchorpatch.py --dir exp_20260709_hybridv4smoke
```

凭据必须由父进程环境显式注入；本目录不得放置 `.env` 或 `.env.frkeys`。

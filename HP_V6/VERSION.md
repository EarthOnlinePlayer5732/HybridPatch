# HP_V6（hybridpatch/6）版本卡

## 五问摘要

1. **为什么迭代**：V5 的 circuit2、quantum4、protein1 伤亡中有三个可确定性修复；starcatalog4 类无参考语义错误承认不可修。
2. **优化方向**：C1 允许删行 span 完全包含 replace span 时按净语义吞并，仍拒绝部分重叠与 replace-replace；C2 增加 JSON/XML/KML/GPX/SVG/QASM/PDB 格式健康 gate，输入可解析时输出不得退化。
3. **效果**：dev20 HP 均值 0.9207，三代共同 16 样本由 V3 0.920→V5 0.830→V6 0.918，收复失地并保持身份增益。C2 live 仅命中 protein1 两步，零观察误报；repair 极性 20 positive / 10 neutral / 0 negative。对冻结 FR 基线，dev20 headline Δ+0.195、披露口径 Δ+0.030；val40 headline Δ+0.137、披露口径 Δ+0.042。
4. **新问题**：crystal6 暴露 `.cif` 不在 C2 注册表；robotics1 暴露 backward 全拒后的 kept-context 锁死；全有全无提交政策成为头号残留伤害。val40 还确认“空响应”是 MiniMax-M3 thinking 阶段服务端流截断，直接催生 transport-v2/v3。
5. **下一轮靶子**：V7 处理全有全无放大；C2-ext `.cif` 继续排队。

历史数字出处：`docs/项目结构与迭代史.md` §3.4、§7（重构后相对路径为 `../docs/...`）。

## 来源与重建

- 协议：`hybridpatch/6`
- Git 锚点：tag `hybridpatch6-snapshot-20260711`，commit `2e1e004b42a86ab1216726ca2c94f3717ba12cda`
- 主源码来源：磁盘快照 `_snap_val40/src`
- 双源交叉：snapshot 对两个 V6 campaign 的历史 8 项为 8/8；其 `model_openai.py`、`run_meta.py` 也与 V6 tag 的历史工作树状态匹配，src 补充口径 10/10。
- `requirements.txt`：从 V6 tag 按 Windows checkout filter 物化；snapshot 本身没有根 requirements。
- provenance：移除凭据后的原快照整体保留在 `_snapshot_provenance/`（1918 files，22,860,619 B）。
- 快照凭据：`.env`（516 B）与 `.env.frkeys`（981 B）未读取、未复制、未删除，按用户裁决原字节移至版本目录外 `_attic/sensitive_snapshot_env/_snap_val40/`。
- 布局：`prompts/` 为顶层原字节副本；`data/` 为共享顶层 `data/` 的 Windows junction。源码未为布局修改。

## 指纹验收

历史 metadata 只记录以下 8 项：

| 文件 | 归档 | 重建 | 结果 |
|---|---|---|---|
| `patch_schema.py` | `ba8a9eb35f06` | `ba8a9eb35f06` | MATCH |
| `splitters.py` | `690b1981879f` | `690b1981879f` | MATCH |
| `experiment_runner.py` | `291ec60aa8df` | `291ec60aa8df` | MATCH |
| `hybrid_schema.py` | `edbd2517894a` | `edbd2517894a` | MATCH |
| `hybrid_index.py` | `d66d13a8fb60` | `d66d13a8fb60` | MATCH |
| `hybrid_prompt.py` | `81668ab72e5d` | `81668ab72e5d` | MATCH |
| `hybrid_executor.py` | `a4ea477217b0` | `a4ea477217b0` | MATCH |
| `hybrid_gate.py` | `3143bd4b3d0a` | `3143bd4b3d0a` | MATCH |

补充三项重建值：`model_openai.py=4afd6dc5da79`、`run_meta.py=4f232bd71c5e`、`requirements.txt=a6ef722a6b0b`。前两项由 V6 tag 与 snapshot 双源交叉；requirements 只声明 tag 来源，不虚构 campaign 比对。

**验收等级：字节级确证（历史 metadata 8/8；src 双源补充 10/10；requirements 同版 tag 来源）。**

## 四连验收（2026-07-13）

工作目录：`hybridpatch_clean/HP_V6`；Python 命令设置 `PYTHONUTF8=1` 并使用 `-B`。

1. `py_compile.compile(..., cfile=%TEMP%)`：`PY_COMPILE PASS files=78`。
2. `python -B src/test_hybrid_executor.py`：`RESULT: PASS (40 tests)`；`python -B src/splitters.py`：`RESULT: PASS (all splitters byte-exact coverage)`。
3. 两个 V6 归档的历史指纹相同；实测正式字段 `FINGERPRINT PASS 8/8`。
4. 归档结果复核：
   - `python -B src/verify_anchorpatch.py --dir exp_20260710_hybridv6dev20full`：退出 0；结果复核 PASS，400 个 backward RS 从 raw responses 独立复现。交接来源表的 200 指 live HP 臂；当前目录还含冻结 FR 200 行，本次按完整目录记录为 400。
   - `python -B src/verify_anchorpatch.py --dir exp_20260711_hybridv6val40`：退出 0；结果复核 PASS，800 个 backward RS 从 raw responses 独立复现。

归档搬移前后计数/字节数一致：

- `exp_20260710_hybridv6dev20full`：2954 files，147,589,690 B。
- `exp_20260711_hybridv6val40`：6306 files，294,738,428 B。

## 运行方式

```powershell
cd HP_V6
$env:PYTHONUTF8 = "1"
python src/experiment_runner.py ...
python -B src/verify_anchorpatch.py --dir exp_20260711_hybridv6val40
```

凭据必须由父进程环境显式注入；本目录不得放置 `.env` 或 `.env.frkeys`。

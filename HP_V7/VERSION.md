# HP_V7（hybridpatch/7）版本卡

## 五问摘要

1. **为什么迭代**：V6 审计确认头号残留伤害是全有全无放大：一个装饰性、无法匹配的 op 会连坐丢弃同信封内全部合法 op。问题位于 runner 提交政策，不是执行器字节语义。
2. **优化方向**：执行器/gate 与 V6 逐字节相同；单次 repair 后若终局失败仅由 op 拒绝构成、至少一个 op 接受、无其他 gate/route 错误且输出非 no-op，则提交接受 op 的部分结果。prompt 不告知模型，保持纯政策消融。
3. **效果**：`exp_20260711_hybridv7dev20full` 对冻结 FR 基线 Δ+0.161（HP 0.888 vs FR 0.726），CF 9/180 vs 13/180。partial 触发 5/400，三个 backward partial 的机械 kept 反事实全部非负：fonteng3 +0.158、latex2 +0.258、mathlean2 +0.003；被跳 op 均为死 op。
4. **新问题**：未观察到政策回归；forward partial 链式后果、mathlean2 型内容级放行与 repair-partial 交互仍需规模数据。记账语义改变：跨版本比较应看 kept+partial 合计。
5. **下一轮**：先在同一 transport-v3 下完成 val40 双臂验证与 FR 基线重建；C2-ext `.cif`、第二 seed/模型稳健性继续排队。

历史数字出处：`docs/项目结构与迭代史.md` §3.5、§7（重构后相对路径为 `../docs/...`）。

## 来源与定格

- 协议：`hybridpatch/7`
- 方法 tag：`hybridpatch7-20260711`，commit `3f7423804505597aa9a93b7ce5766bff42c4579b`
- 本版实际来源：重构前当前 HEAD `3b322403` 的完整 `src/` 与当前 `requirements.txt`，包含 tag 后新增的 OpenCode transport-v3、MiniMax official nonstream/1、F1–F7 与 runner 显示层更新。
- 定格方式：当前 `src/` 原字节 copy；`prompts/` 原字节 copy；`data/` 为共享顶层 `data/` 的 Windows junction。源码未为新布局修改。
- API 主战场：后续传输层修改只在顶层 `transport/` 发生，通过有记录的原字节同步进入活跃版本；本卡记录的 HP_V7 源码在重构完成后冻结。

## 指纹验收

当前重建值：

| 文件 | HP_V7 当前值 |
|---|---|
| `patch_schema.py` | `ba8a9eb35f06` |
| `splitters.py` | `690b1981879f` |
| `experiment_runner.py` | `32d5746e07eb` |
| `hybrid_schema.py` | `93e8b9c78f12` |
| `hybrid_index.py` | `d66d13a8fb60` |
| `hybrid_prompt.py` | `81668ab72e5d` |
| `hybrid_executor.py` | `79ef3e766a00` |
| `hybrid_gate.py` | `06299fbd4822` |
| `model_openai.py` | `0839fc1ed6ea` |
| `run_meta.py` | `e0d7d7861cfb` |
| `requirements.txt` | `38ffe361be94` |

与历史归档的真实比对：

- `exp_20260711_hybridv7dev20full`：7/8；仅 `experiment_runner.py` 不同（archive `5dbe66187330`，当前 `32d5746e07eb`）。
- `exp_20260712_hybridv7val40_transportv3`：9/11；`experiment_runner.py`（archive `dd3f706bdd51`）与 `model_openai.py`（archive `6ddfdafcd065`）不同，其余 9 项匹配。

这些差异来自归档完成后的已提交 transport/显示层演进；不把它们改写成字节匹配。

**验收等级：行为级确证（当前 HEAD 精确定格；两档归档均由当前 verifier 完整重放 PASS）。**

## 四连验收（2026-07-13）

工作目录：`hybridpatch_clean/HP_V7`；Python 命令设置 `PYTHONUTF8=1` 并使用 `-B`。

1. `py_compile.compile(..., cfile=%TEMP%)`：`PY_COMPILE PASS files=82`。
2. `python -B src/test_hybrid_executor.py`：`RESULT: PASS (49 tests)`；`python -B src/splitters.py`：`RESULT: PASS (all splitters byte-exact coverage)`。
3. 指纹真实结果：dev20 7/8；val40 9/11，差异如上。
4. 归档结果复核：
   - `python -B src/verify_anchorpatch.py --dir exp_20260711_hybridv7dev20full`：退出 0；结果复核 PASS，400 个 backward RS 从 raw responses 独立复现。
   - `python -B src/verify_anchorpatch.py --dir exp_20260712_hybridv7val40_transportv3`：退出 0；结果复核 PASS，780 个 backward RS 从 raw responses 独立复现。该档按 canonical 38 个完整配对样本收官；json4 双臂与 earncall1 FR 的基础设施 partial 原样保留。

归档搬移前后计数/字节数一致：

- `exp_20260711_hybridv7dev20full`：2871 files，142,416,115 B。
- `exp_20260712_hybridv7val40_transportv3`：12,935 files，757,909,214 B。

## 运行方式

```powershell
cd HP_V7
$env:PYTHONUTF8 = "1"
python src/experiment_runner.py ...
python -B src/verify_anchorpatch.py --dir exp_20260712_hybridv7val40_transportv3
```

`model_openai.py` 不会向上发现顶层 `.env`。凭据必须由父进程环境显式注入，或使用 `python -m dotenv -f ..\.env run -- ...`；本目录不得放置 `.env` 或 `.env.frkeys`。

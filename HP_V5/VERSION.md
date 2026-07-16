# HP_V5（hybridpatch/5）版本卡

## 五问摘要

1. **为什么迭代**：V4 smoke 暴露 bulk 顺序变异会毁掉刚插入的撤销线索，delete 语义也需约束；同时 P1 路由压强需要在完整 dev20 上验证。
2. **优化方向**：bulk_patch 改为步骤输入快照定位；expected count 也在输入快照上计算；跨 op span 冲突显式拒绝、同行 delete 去重；prompt 增加 delete 只删除目标内容的约束。
3. **效果**：`exp_20260709_hybridv5dev20full` 历史与本次结果复核均 PASS 400/400。配对 Δ+0.035，不显著；剔除三个 FR provider 污染样本后 Δ+0.004，方法效应归零。身份指标改善：bounded 57.5%、生成字节占比 0.595、字节保留率 0.341。
4. **新问题**：路由压强放大模型手写精确字面量的短板：quantum4、protein1 触发格式灾难；circuit2 被过严的包含型重叠拒绝误伤；starcatalog4 是无参考条件下不可判的语义错列。
5. **下一轮靶子**：C1 包含型重叠消解；C2 无参考格式健康 gate。C3 路由措辞逃生口评估后缓行。

历史数字出处：`docs/项目结构与迭代史.md` §3.3、§7（重构后相对路径为 `../docs/...`）。

## 来源与重建

- 协议：`hybridpatch/5`
- 正式 commit：无。
- 初始 Git 考古：31,190 个 unreachable blob 中未命中目标 `hybrid_schema.py` 与 `hybrid_executor.py`，原计划按 V6−C1−C2 逆推。
- 精确来源：本机 Claude Code file-history 的两个独立 session 都保留了字节相同的 V5 历史文件。使用 session `5af0bca7-3572-4da7-8338-1edf069dad9e` 的 `hybrid_schema.py`、`hybrid_executor.py`、`hybrid_gate.py`、`test_hybrid_executor.py`；session `82331619-c3b0-4563-bb3d-c0d143ca8a84` 提供逐文件相同 SHA-1 的第二来源交叉。
- 完整基底：V6 磁盘快照 `src/`；只用上述四个历史文件原字节覆盖。campaign 的其余五个指纹文件与 V6 snapshot 相同。
- `requirements.txt`：随 V6 基底同版 tag 的 Windows 物化状态；V5 campaign 没有记录该字段。
- `prompts/` 为顶层原字节副本；`data/` 为共享顶层 `data/` 的 Windows junction。源码未为布局修改。

## 指纹验收

| 文件 | 归档 | 重建 | 结果 |
|---|---|---|---|
| `patch_schema.py` | `ba8a9eb35f06` | `ba8a9eb35f06` | MATCH |
| `splitters.py` | `690b1981879f` | `690b1981879f` | MATCH |
| `experiment_runner.py` | `291ec60aa8df` | `291ec60aa8df` | MATCH |
| `hybrid_schema.py` | `54abc09fbcb8` | `54abc09fbcb8` | MATCH |
| `hybrid_index.py` | `d66d13a8fb60` | `d66d13a8fb60` | MATCH |
| `hybrid_prompt.py` | `81668ab72e5d` | `81668ab72e5d` | MATCH |
| `hybrid_executor.py` | `9746659b868f` | `9746659b868f` | MATCH |
| `hybrid_gate.py` | `3c992dd20f1f` | `3c992dd20f1f` | MATCH |

历史 V5 测试文件 SHA-1 为 `dd5428509139`，实测 32 项。补充三项重建值：`model_openai.py=4afd6dc5da79`、`run_meta.py=4f232bd71c5e`、`requirements.txt=a6ef722a6b0b`。

**验收等级：字节级确证（历史 metadata 8/8；四个 V5 特有文件有双份本地历史来源；其余来源完整披露）。**

## 四连验收（2026-07-13）

工作目录：`hybridpatch_clean/HP_V5`；Python 命令设置 `PYTHONUTF8=1` 并使用 `-B`。

1. `py_compile.compile(..., cfile=%TEMP%)`：`PY_COMPILE PASS files=78`。
2. `python -B src/test_hybrid_executor.py`：`RESULT: PASS (32 tests)`；`python -B src/splitters.py`：`RESULT: PASS (all splitters byte-exact coverage)`。
3. `exp_20260709_hybridv5dev20full` 指纹：`FINGERPRINT PASS 8/8`。
4. `python -B src/verify_anchorpatch.py --dir exp_20260709_hybridv5dev20full`：退出 0；结果复核 PASS，400 个 backward RS 从 raw responses 独立复现。

归档搬移前后均为 6484 files、234,889,045 B。

## 运行方式

```powershell
cd HP_V5
$env:PYTHONUTF8 = "1"
python src/experiment_runner.py ...
python -B src/verify_anchorpatch.py --dir exp_20260709_hybridv5dev20full
```

凭据必须由父进程环境显式注入；本目录不得放置 `.env` 或 `.env.frkeys`。

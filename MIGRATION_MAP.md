# HP_Vx 重构迁移映射

本表记录重构前路径到新路径的逐项映射。`move` 表示同卷整体搬移、内部字节不改；`copy` 表示为自包含版本快照新增的原字节副本；`junction` 表示 Windows directory junction。归档验收结果见各版 `VERSION.md` 与最终报告。

状态：`[x]` 已执行；`[ ]` 待后续 Phase 执行。

## 版本源码、共享布局与 transport

| 状态 | 操作 | 旧路径/来源 | 新路径 | 说明 |
|---|---|---|---|---|
| [x] | copy | 当前 `src/model_openai.py` | `transport/src/model_openai.py` | API 主战场初始快照 |
| [x] | copy | 当前 `src/run_meta.py` | `transport/src/run_meta.py` | 同上 |
| [x] | copy | 当前 `src/experiment_runner.py` | `transport/src/experiment_runner.py` | runner 的 transport 入口/面板逻辑 |
| [x] | copy | 当前 `src/fr_baseline_dispatch.py` | `transport/src/fr_baseline_dispatch.py` | 同上 |
| [x] | copy | 当前 `src/probe_fr_keys.py` | `transport/src/probe_fr_keys.py` | 同上 |
| [x] | copy | 当前 `src/monitor_experiment.py` | `transport/src/monitor_experiment.py` | 同上 |
| [x] | copy | 当前 `src/launch_pipeline.py` | `transport/src/launch_pipeline.py` | 同上 |
| [x] | copy | 当前 `src/test_model_openai.py` | `transport/src/test_model_openai.py` | 零 API 回归源 |
| [x] | copy | 当前 `src/test_fixtures/` | `transport/src/test_fixtures/` | 脱敏真实异常 fixture |
| [x] | reconstruct/copy | V3 tag `db5048d2` + campaign src tree `30b52270...` | `HP_V3/src/`, `HP_V3/requirements.txt` | 正式 tag 完整源码；campaign runner 精确补齐；历史指纹 8/8 |
| [x] | reconstruct/copy | dangling tree `78fd5f...` / src tree `ddad676...` | `HP_V4/src/`, `HP_V4/requirements.txt` | Windows checkout filter 物化；历史指纹 8/8 |
| [x] | reconstruct | V6 snapshot 基底 + 双份 Claude file-history 精确 V5 文件 | `HP_V5/src/`, `HP_V5/requirements.txt` | 历史指纹 8/8；32/32 版本测试 |
| [x] | reconstruct/copy | V6 tag `2e1e004b` + `_snap_val40/src` | `HP_V6/src/`, `HP_V6/requirements.txt` | 双源交叉；历史指纹 8/8、src 补充 10/10 |
| [x] | copy | 当前 `src/` | `HP_V7/src/` | Phase B 定格当前 HEAD；49/49 测试、两档行为重放 PASS |
| [x] | move | 当前 `src/` | `_attic/src_pre_restructure/` | 与 HP_V7 copy 的 84 个非 bytecode 文件逐字节一致后整体搬移；保留旧入口，不删除 |
| [x] | copy | `prompts/` | `HP_V3/prompts/` … `HP_V6/prompts/` | Phase A 四版实拷贝；顶层原地保留 |
| [x] | copy | `prompts/` | `HP_V7/prompts/` | Phase B 实拷贝 |
| [x] | junction | 顶层 `data/` | `HP_V3/data` … `HP_V6/data` | `cmd /c mklink /J`，共享只读数据 |
| [x] | junction | 顶层 `data/` | `HP_V7/data` | Phase B，`cmd /c mklink /J` |

## 文档

| 状态 | 操作 | 旧路径 | 新路径 |
|---|---|---|---|
| [x] | move | `docs/API_TRANSPORT_FROZEN_V3.md` | `transport/docs/API_TRANSPORT_FROZEN_V3.md` |
| [x] | move | `docs/API_TRANSPORT_MINIMAX_OFFICIAL_V1.md` | `transport/docs/API_TRANSPORT_MINIMAX_OFFICIAL_V1.md` |
| [x] | move | `docs/TRANSPORT_V3_CLAUDE_CODE_HANDOFF.md` | `transport/docs/TRANSPORT_V3_CLAUDE_CODE_HANDOFF.md` |
| [x] | move | `docs/Minimax_OPENAI.md` | `transport/docs/Minimax_OPENAI.md` |
| [x] | move | `docs/FR冻结计划.md` | `Baseline/FR冻结计划.md` |
| [x] | keep | `docs/` 其余项目级文档 | 原路径不变；活动文档中的已搬路径引用已更新 |

## 实验归档

| 状态 | 操作 | 旧路径 | 新路径 | 说明 |
|---|---|---|---|---|
| [x] | move | `exp_20260707_hybridv3dev20/` | `HP_V3/exp_20260707_hybridv3dev20/` | V3 首跑；结果复核 PASS 400/400 |
| [x] | move | `exp_20260708_hybridv3dev20full/` | `HP_V3/exp_20260708_hybridv3dev20full/` | V3 正式档；唯一为预披露 quantum4 RT9 差异 |
| [x] | move | `exp_20260709_hybridv4smoke/` | `HP_V4/exp_20260709_hybridv4smoke/` | 7 样本均跑满 10RT；结果复核 PASS 70/70 |
| [x] | move | `exp_20260709_hybridv5dev20full/` | `HP_V5/exp_20260709_hybridv5dev20full/` | V5 正式档；结果复核 PASS 400/400 |
| [x] | move | `exp_20260710_frbaseline234/` | `Baseline/exp_20260710_frbaseline234/` | 永久冻结 FR 基线；结果复核 PASS 2334/2334 |
| [x] | move | `exp_20260710_hybridv6dev20full/` | `HP_V6/exp_20260710_hybridv6dev20full/` | V6 dev20；完整目录结果复核 PASS 400/400 |
| [x] | move | `exp_20260711_hybridv6val40/` | `HP_V6/exp_20260711_hybridv6val40/` | V6 val40；结果复核 PASS 800/800 |
| [x] | move | `exp_20260711_hybridv7dev20full/` | `HP_V7/exp_20260711_hybridv7dev20full/` | V7 dev20；结果复核 PASS 400/400 |
| [x] | move | `exp_20260711_opencode_transport_v2_smoke5/` | `_attic/exp_20260711_opencode_transport_v2_smoke5/` | 仅 2RT；结果复核 PASS 20/20；FINDINGS 证据链的新位置 |
| [x] | move | `exp_20260712_hybridv7val40_transportv3/` | `HP_V7/exp_20260712_hybridv7val40_transportv3/` | canonical 38 配对样本已收官；结果复核 PASS 780/780；保留 3 个基础设施 partial |
| [x] | move | `exp_20260713_problem15_official/` | `transport/exp_20260713_problem15_official/` | 诊断源档保留现状、不续跑；其中 4 条完整 FR 链用于 `FR+Official` |
| [x] | move | `exp_20260713_frtest_robotics1/` | `_attic/exp_20260713_frtest_robotics1/` | 仅 1RT；结果复核 PASS 1/1 |
| [x] | move | `exp_20260713_frbaseline_v2_rerun/` | `Baseline/exp_20260713_frbaseline_v2_rerun/` | 官方重跑源档保留现状、不续跑；83 条完整 FR 链用于 `FR+Official` |
| [x] | derive (additive) | 原冻结 FR + 上述两档完整官方链 | [`Baseline/FR+Official/`](Baseline/FR+Official/README.md) | 新增版本，不替换原冻结 FR；233 = 87 官方（83+4）+ 146 原冻结。任一异常 87/233（37.34%）、截断 86/233（36.91%）、空返回 80/233（34.33%）、接近空返回 37/233（15.88%）；官方子集经过预筛，不能解释为 API 背景率；val40 对比见 [`val40_comparison.md`](Baseline/FR+Official/val40_comparison.md) |

## 快照、临时产物与根部日志

| 状态 | 操作 | 旧路径 | 新路径 | 说明 |
|---|---|---|---|---|
| [x] | move | `_snap_val40/`（除 `.env*`） | `HP_V6/_snapshot_provenance/` | V6 双源证据；1918 files / 22,860,619 B |
| [x] | move | `_snap_val40/.env`, `_snap_val40/.env.frkeys` | `_attic/sensitive_snapshot_env/_snap_val40/` | 用户裁决留在版本外；不读、不复制、不删除 |
| [x] | move | `_tmp_rawlog/` | `_attic/_tmp_rawlog/` | 临时产物保留 |
| [x] | keep | 本次重建的 Windows CRLF/tag 候选 | `_attic/reconstruction_attempts/HP_V3_crlf_checkout/` | 任务过程候选只收纳不删除；最终 HP_V3 不引用这些文件 |

以下 25 个根部日志逐文件 move 到 `_attic/logs_root/`：

| 状态 | 旧路径 | 新路径 |
|---|---|---|
| [x] | `baseline_verify_20260707.log` | `_attic/logs_root/baseline_verify_20260707.log` |
| [x] | `baseline_verify_20260708.log` | `_attic/logs_root/baseline_verify_20260708.log` |
| [x] | `frbaseline_analyze.log` | `_attic/logs_root/frbaseline_analyze.log` |
| [x] | `frbaseline_verify.log` | `_attic/logs_root/frbaseline_verify.log` |
| [x] | `post_v5_verify_20260707.log` | `_attic/logs_root/post_v5_verify_20260707.log` |
| [x] | `post_v5_verify_20260708.log` | `_attic/logs_root/post_v5_verify_20260708.log` |
| [x] | `post_verify_20260707.log` | `_attic/logs_root/post_verify_20260707.log` |
| [x] | `post_verify_20260708.log` | `_attic/logs_root/post_verify_20260708.log` |
| [x] | `v5campaign_verify.log` | `_attic/logs_root/v5campaign_verify.log` |
| [x] | `v6campaign_hp_verify.log` | `_attic/logs_root/v6campaign_hp_verify.log` |
| [x] | `v6paired_analyze.log` | `_attic/logs_root/v6paired_analyze.log` |
| [x] | `v6regress_verify_0707.log` | `_attic/logs_root/v6regress_verify_0707.log` |
| [x] | `v6regress_verify_0708.log` | `_attic/logs_root/v6regress_verify_0708.log` |
| [x] | `v6regress_verify_v4smoke.log` | `_attic/logs_root/v6regress_verify_v4smoke.log` |
| [x] | `v6regress_verify_v5campaign.log` | `_attic/logs_root/v6regress_verify_v5campaign.log` |
| [x] | `v7dev20_analyze.log` | `_attic/logs_root/v7dev20_analyze.log` |
| [x] | `v7dev20_verify.log` | `_attic/logs_root/v7dev20_verify.log` |
| [x] | `v7regress_verify_20260707_hybridv3dev20.log` | `_attic/logs_root/v7regress_verify_20260707_hybridv3dev20.log` |
| [x] | `v7regress_verify_20260708_hybridv3dev20full.log` | `_attic/logs_root/v7regress_verify_20260708_hybridv3dev20full.log` |
| [x] | `v7regress_verify_20260709_hybridv5dev20full.log` | `_attic/logs_root/v7regress_verify_20260709_hybridv5dev20full.log` |
| [x] | `v7regress_verify_20260710_hybridv6dev20full.log` | `_attic/logs_root/v7regress_verify_20260710_hybridv6dev20full.log` |
| [x] | `val40_analyze.log` | `_attic/logs_root/val40_analyze.log` |
| [x] | `val40_verify.log` | `_attic/logs_root/val40_verify.log` |
| [x] | `val40v3_analyze.log` | `_attic/logs_root/val40v3_analyze.log` |
| [x] | `val40v3_verify.log` | `_attic/logs_root/val40v3_verify.log` |

[根目录](../CLAUDE.md) > **domains**

# domains 模块

## 模块职责

实现 54 个专业文档领域的**解析器（parser）+ 评估器（evaluator）**。每个领域定义如何解析特定文件格式、计算文档统计、以及将 LLM 编辑后的文档与参考文档对比打分。所有领域继承统一基类 `DomainBase`，由 `__init__.py` 在导入时自动发现并注册，对外暴露 `get_domain(name)` 工厂。

## 入口与启动

- `domains/__init__.py`：包初始化即扫描 `domain_*.py`（除 `domain_base`），导入后遍历 `DomainBase.__subclasses__()` 构建 `_DOMAIN_REGISTRY`（键为类名去掉 `Domain` 前缀并小写）。
  - `get_domain(domain_name)` → 返回领域实例；未知名抛 `ValueError`。
  - `DOMAIN_NAMES` → 全部已注册领域名列表。
  - 直接运行 `python -m domains` 打印加载耗时与领域名。

## 对外接口（DomainBase 契约）

定义于 `domain_base.py`，子类需实现/可覆盖：

| 方法 | 必须实现 | 说明 |
|------|:--------:|------|
| `parse_context(context)` | 是 | 将 `{filename: content}` 解析为领域结构化表示 |
| `compute_domain_statistics(context)` | 是 | 返回供展示的统计字典（如 Entries / Functions） |
| `evaluate_context(sample_id, generated_context, target_state)` | 是 | 对比生成文档与参考，返回含 `score`(0–1) 或 `error` 的字典 |
| `preprocess_context(context)` | 否 | 解析前归一化原始内容（修 LLM 语法怪癖） |
| `render_context_visual(context, outfile)` | 否 | 渲染为图像；覆盖时须设 `supports_visual = True` |
| `prepare_prompt(...)` | 已实现 | 用 `[[INPUT_CONTEXT]]`/`[[FILE_NAMES]]`/`[[EDITING_OPERATION]]` 占位填充模板 |
| `run_single_step_edit(...)` | 已实现 | 编辑执行总入口；`agentic-` 前缀分流到 `model_agentic`（⚠️ 该文件在本 fork 已删除，走此路径必 ImportError），否则走 `model_openai.generate`；含 wildcard/完整性校验 |

子类构造时设置：`sample_type`、`summary`、`description`、`file_format`（扩展名列表）、`domain_parser`（库名或 `"custom"`）、`category`（`code`/`science`/`creative`/`records`/`everyday`/`visual`/`audio`）。

## 关键依赖与配置

- 上游：`utils_context`（序列化/校验/统计）、`model_openai.generate`、`model_agentic.run_agentic_edit`（⚠️ 本 fork 已删除 `model_agentic.py`）。
- prompt 模板：多数领域用 `prompts/domain_documents.txt`；`python` 用 `domain_python.txt`，`fiction` 用 `domain_fiction.txt`，`image` 用 `domain_image.txt`，`audio` 用 `domain_audio.txt`。
- 领域专属第三方库（见根 `requirements.txt`）：如 `srt`(subtitles)、`python-chess`(chess)、`pymatgen`/`PyCifRW`(crystal)、`biopython`/`biotite`(protein)、`rdkit`(molecule)、`qiskit`/`openqasm3`(quantum)、`librosa`/`soundfile`(audio)、`CairoSVG`(vector) 等。

## 数据模型

- **context**：`{filename: content_str}`；二进制文件（图像）以 base64 字符串存储。
- **target_state**：`{"state_id": ..., "context": {filename: ...}, "solution_folder": ...}`；评估通常仅在 `state_id == "basic_state"`（即 backward 回到原始态）时计算分数。
- 评估器从 `{samples_folder}/{sample_id}/sample.json` 读取 `start_state` 与 `solution_folder` 还原参考文档。

## 代表性领域实现差异（非同构领域）

54 个领域多数同构（解析 → 统计 → 评分三件套），但少数领域覆盖了基类的 `prepare_prompt` / `run_single_step_edit` / `evaluate_context`，改动相关逻辑前务必先读对应文件：

| 领域 | 解析器 | 评分构成（权重） | 特殊机制 |
|------|--------|------------------|----------|
| `crystal` | PyCifRW | atom_sites 30% · bonds 20% · cell/symmetry 各 5% · aniso/angles/hbonds/metadata 各 10% | `preprocess_context` 6 步修复 LLM 的 CIF 语法（剥围栏、引号→分号字段、补引号、删空/去重 `loop_`）；**atom_recall 级联**——缺失原子按比例缩小依赖它的 bonds/angles/aniso/hbonds 分数；`render_context_visual` 用 pymatgen+ASE 画 3D 结构 |
| `fiction` | custom | `score = clip(ttcw_relative × length_penalty, 0, 1)` | **唯一用 `target_length`**：覆盖 `prepare_prompt` 填充 `[[TARGET_LENGTH]]`/`[[CURRENT_LENGTH]]`（nltk 分词）；LLM-judge 跑 14 项 TTCW（独立 prompt `prompts/domain_fiction_eval.txt`，judge 模型 `t-gpt-5.2`）；baseline TTCW 缓存进 `sample.json`；长度惩罚 [90%,110%]=1.0、偏离按 `0.75^x` 衰减；WQRM 回归模型代码已注释（避免 GPU OOM） |
| `image` | pillow | SSIM 50% · 直方图(HSV) 25% · 像素(RMSE) 25% | 评估硬编码 `samples/<id>/` 路径（非 `self.samples_folder`）；`render_context_visual` 存首图 |
| `audio` | librosa/soundfile | (mel-SSIM 50% · chroma 25% · 样本RMSE 25%) × duration_penalty | `render_context_visual` 画波形 + mel 频谱图 |

> ⚠️ **关键陷阱**：`image` 与 `audio` 领域在本开源 release 中**无法执行编辑步骤**——其覆盖的 `run_single_step_edit` 首行即 `raise NotImplementedError`（缺 `generate_image()` / `generate_audio()` provider）；二者仅评估器可用。其余 52 个文本类领域可正常运行。

## 测试与质量

无独立单测。（上游 `run_single.py` QA 工具在本 fork 已删除；执行器级验证见 `anchorpatch/test_executor.py`。）`domain_python` 通过样例自带 `testing.py::run_tests(target_state_id)` 做真实功能测试（复制到 `tmp_eval_*` 临时目录、隔离 `sys.modules`、执行后清理）。

## 常见问题 (FAQ)

- **如何新增领域？** 建 `domains/domain_<name>.py`，定义 `class Domain<Name>(DomainBase)`，构造调 `super().__init__("prompts/domain_documents.txt")` 并设元数据，实现三个必需方法。无需手动注册。
- **评估为何返回 `{}`？** 多数领域仅在 backward（`basic_state`）方向打分；forward 方向返回空字典。
- **wildcard 文件名？** `target_state.context` 的键可含 `*`/`?`；`utils_context` 的 `is_context_complete`/`validate_wildcard_context` 负责匹配校验。

## 相关文件清单

- `domain_base.py` — 抽象基类与编辑执行入口（含 agentic 分流）。
- `__init__.py` — 自动注册机制与 `get_domain` 工厂。
- 54 个 `domain_*.py`，按 `category` 分组：

| 分类 | 领域（`sample_type`） |
|------|----------------------|
| code (11) | dbschema, dns, docker, filesystem, graphviz, infra, json, makefile, malware, python, translation |
| science (11) | aviation, circuit, crystal, mathlean, molecule, protein, quantum, robotics, satellite, starcatalog, weather |
| creative (11) | audiosyn, fiction, fonteng, latex, musicsheet, obj3d, screenplay, slides, subtitles, vector, weaving |
| records (11) | accounting, calendar, edifact, emails, genealogy, geodata, geotrack, hamradio, libcatalog, spreadsheet, treebank |
| everyday (8) | chess, earncall, foodmenu, jobboard, landmarks, playlist, recipe, transit |
| visual / audio (2) | image, audio |

> 14 个领域 `supports_visual = True`（可渲染图像）：vector, weaving, treebank, starcatalog, protein, quantum, obj3d, molecule, image, geodata, graphviz, crystal, audio。

## 变更记录 (Changelog)

| 日期 | 变更 |
|------|------|
| 2026-05-30 | 初始化生成模块文档 |
| 2026-05-30 | 补扫 crystal/image/audio/fiction 全文，新增「代表性领域实现差异（非同构领域）」小节 |

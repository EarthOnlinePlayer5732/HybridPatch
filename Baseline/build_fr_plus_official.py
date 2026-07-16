"""Build the additive FR+Official baseline view and its val40 sensitivity report.

The source experiment directories remain immutable.  A complete 10-RT official
chain replaces the corresponding chain only in this logical view; incomplete
chains are never spliced with the frozen FR baseline.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import statistics
import sys
from collections import Counter
from pathlib import Path


HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
ORIGINAL_DIR = HERE / "exp_20260710_frbaseline234"
OFFICIAL_BASELINE_DIR = HERE / "exp_20260713_frbaseline_v2_rerun"
OFFICIAL_DIAGNOSTIC_DIR = ROOT / "transport" / "exp_20260713_problem15_official"
VAL40_DIR = ROOT / "HP_V7" / "exp_20260712_hybridv7val40_transportv3"
OUTPUT_DIR = HERE / "FR+Official"

sys.path.insert(0, str(ROOT / "HP_V7" / "src"))
import analyze  # noqa: E402


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def read_jsonl(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def load_method(directory: Path, method: str) -> dict[str, list[dict]]:
    result = {}
    for path in sorted((directory / method).glob("*.jsonl")):
        result[path.stem] = read_jsonl(path)
    return result


def is_complete(rows: list[dict]) -> bool:
    directions = Counter(row.get("round_trip_direction") for row in rows)
    forward = {
        row.get("round_trip_num")
        for row in rows
        if row.get("round_trip_direction") == "forward"
    }
    backward = {
        row.get("round_trip_num")
        for row in rows
        if row.get("round_trip_direction") == "backward"
    }
    return (
        len(rows) == 20
        and directions == {"forward": 10, "backward": 10}
        and forward == set(range(1, 11))
        and backward == set(range(1, 11))
    )


OFFICIAL_RUN_CONFIG = {
    "provider": "minimax_official",
    "transport": "openai_sdk_nonstream",
    "transport_revision": "minimax_official_nonstream/1",
    "model": "minimax-m3",
    "num_round_trips": 10,
    "seed": 42,
    "distractor": False,
    "effective_max_tokens": 131072,
    "thinking_mode": "adaptive",
}


def validate_official_metadata(directory: Path, samples: set[str]) -> None:
    metadata = read_jsonl(directory / "run_metadata.jsonl")
    covered = set()
    for record in metadata:
        overlap = samples & set(record.get("samples", []))
        if not overlap:
            continue
        covered.update(overlap)
        mismatches = {
            key: (record.get(key), expected)
            for key, expected in OFFICIAL_RUN_CONFIG.items()
            if record.get(key) != expected
        }
        if mismatches:
            raise RuntimeError(
                f"official run config mismatch in {directory.name}: {mismatches}"
            )
    missing = samples - covered
    if missing:
        raise RuntimeError(
            f"official samples missing from metadata in {directory.name}: "
            f"{sorted(missing)}"
        )


def flatten(mapping: dict[str, list[dict]], samples: set[str]) -> list[dict]:
    return [row for sample in sorted(samples) for row in mapping[sample]]


def scores(rows: list[dict]) -> list[float]:
    return [
        analyze.score_of(row)
        for row in analyze.backward(rows)
        if analyze.score_of(row) is not None
    ]


def mean_score(rows: list[dict]) -> float:
    values = scores(rows)
    return statistics.mean(values) if values else math.nan


def row_flags(row: dict) -> dict[str, bool]:
    raw = row.get("raw_llm_response") or ""
    classification = row.get("response_classification")
    finish = row.get("finish_reason")
    empty = not raw.strip() or classification in {
        "model_empty",
        "thinking_budget_exhausted",
    }
    near_empty = not empty and len(raw.encode("utf-8")) < 200
    abnormal_termination = finish in {
        None,
        "abort",
        "length",
        "max_tokens",
    } or classification in {
        "aborted_partial_text",
        "text_truncated",
        "thinking_budget_exhausted",
    }
    truncated_with_text = classification in {
        "aborted_partial_text",
        "text_truncated",
    }
    return {
        "abnormal_termination": abnormal_termination,
        "truncated_with_text": truncated_with_text,
        "empty": empty,
        "near_empty": near_empty,
        "small_output": empty or near_empty,
        "any_anomaly": abnormal_termination or empty or near_empty,
    }


def anomaly_summary(mapping: dict[str, list[dict]]) -> tuple[dict, list[dict]]:
    categories = list(row_flags({}))
    row_counts = Counter()
    sample_sets = {name: set() for name in categories}
    sample_rows = []
    for sample, rows in sorted(mapping.items()):
        counts = Counter()
        for row in rows:
            flags = row_flags(row)
            for name, hit in flags.items():
                if hit:
                    counts[name] += 1
                    row_counts[name] += 1
                    sample_sets[name].add(sample)
        sample_rows.append(
            {
                "sample": sample,
                "source_rows": len(rows),
                **{f"{name}_rows": counts[name] for name in categories},
                **{f"has_{name}": sample in sample_sets[name] for name in categories},
            }
        )
    total_samples = len(mapping)
    total_rows = sum(len(rows) for rows in mapping.values())
    summary = {
        "samples": total_samples,
        "rows": total_rows,
        "sample_counts": {name: len(sample_sets[name]) for name in categories},
        "sample_rates": {
            name: len(sample_sets[name]) / total_samples for name in categories
        },
        "row_counts": dict(row_counts),
        "row_rates": {name: row_counts[name] / total_rows for name in categories},
    }
    return summary, sample_rows


def result_stats(hp_rows: list[dict], fr_rows: list[dict]) -> dict:
    hp_values, fr_values, _ = analyze.paired(hp_rows, fr_rows)
    paired = analyze.paired_stats(hp_values, fr_values)
    hp_ecr, fr_ecr, _ = analyze.paired(hp_rows, fr_rows, ecr_only=True)
    return {
        "paired": paired,
        "ecr": analyze.paired_stats(hp_ecr, fr_ecr),
        "hp_rs": analyze.rs_at_k(hp_rows),
        "fr_rs": analyze.rs_at_k(fr_rows),
        "hp_cf": analyze.critical_failures(hp_rows, 0.10),
        "fr_cf": analyze.critical_failures(fr_rows, 0.10),
        "hp_tokens": analyze.tokens(hp_rows)[2],
        "fr_tokens": analyze.tokens(fr_rows)[2],
    }


def fmt(value: float | None, digits: int = 3) -> str:
    return "n/a" if value is None else f"{value:.{digits}f}"


def pct(numerator: int, denominator: int) -> str:
    return f"{numerator}/{denominator} ({100 * numerator / denominator:.2f}%)"


def write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_paired_scores(
    path: Path,
    samples: set[str],
    hp: dict[str, list[dict]],
    original_val_fr: dict[str, list[dict]],
    mixed_fr: dict[str, list[dict]],
    selected_source: dict[str, str],
) -> None:
    rows = []
    for sample in sorted(samples):
        hp_index = {
            row["round_trip_num"]: analyze.score_of(row)
            for row in analyze.backward(hp[sample])
        }
        old_index = {
            row["round_trip_num"]: analyze.score_of(row)
            for row in analyze.backward(original_val_fr[sample])
        }
        new_index = {
            row["round_trip_num"]: analyze.score_of(row)
            for row in analyze.backward(mixed_fr[sample])
        }
        for rt in range(1, 11):
            rows.append(
                {
                    "sample": sample,
                    "round_trip": rt,
                    "hp_score": hp_index.get(rt),
                    "original_fr_score": old_index.get(rt),
                    "fr_plus_official_score": new_index.get(rt),
                    "fr_plus_official_source": selected_source.get(
                        sample, "frozen_fr"
                    ),
                }
            )
    write_csv(path, rows)


def main() -> None:
    original = load_method(ORIGINAL_DIR, "fullrewrite")
    official_baseline = load_method(OFFICIAL_BASELINE_DIR, "fullrewrite")
    official_diagnostic = load_method(OFFICIAL_DIAGNOSTIC_DIR, "fullrewrite")
    val_hp = load_method(VAL40_DIR, "hybridpatch")
    val_fr = load_method(VAL40_DIR, "fullrewrite")

    rerun_manifest = json.loads(
        (OFFICIAL_BASELINE_DIR / "rerun_manifest.json").read_text(encoding="utf-8")
    )
    scope = set(rerun_manifest["contaminated"]) | set(
        rerun_manifest["clean_kept"]
    )
    if len(scope) != 233:
        raise RuntimeError(f"unexpected frozen baseline scope: {len(scope)}")

    complete_baseline = {
        sample for sample, rows in official_baseline.items() if is_complete(rows)
    }
    complete_diagnostic = {
        sample for sample, rows in official_diagnostic.items() if is_complete(rows)
    }
    selected_rows = {}
    selected_source = {}
    source_dir = {}
    for sample in sorted(scope):
        if sample in complete_baseline:
            selected_rows[sample] = official_baseline[sample]
            selected_source[sample] = "official_baseline_v2"
            source_dir[sample] = OFFICIAL_BASELINE_DIR
        elif sample in complete_diagnostic:
            selected_rows[sample] = official_diagnostic[sample]
            selected_source[sample] = "official_problem15_fill"
            source_dir[sample] = OFFICIAL_DIAGNOSTIC_DIR
        else:
            selected_rows[sample] = original[sample]
            selected_source[sample] = "frozen_fr"
            source_dir[sample] = ORIGINAL_DIR

    official_samples = {
        sample for sample, source in selected_source.items() if source != "frozen_fr"
    }
    diagnostic_fills = {
        sample
        for sample, source in selected_source.items()
        if source == "official_problem15_fill"
    }
    if len(official_samples) != 87 or diagnostic_fills != {
        "crystal6",
        "filesystem3",
        "obj3d2",
        "robotics1",
    }:
        raise RuntimeError("unexpected official source composition")
    validate_official_metadata(OFFICIAL_BASELINE_DIR, complete_baseline)
    validate_official_metadata(OFFICIAL_DIAGNOSTIC_DIR, diagnostic_fills)

    manifest_rows = []
    for sample in sorted(scope):
        selected_dir = source_dir[sample]
        result_path = selected_dir / "fullrewrite" / f"{sample}.jsonl"
        selected_plan = selected_dir / f"{sample}.task_plan.json"
        original_plan = ORIGINAL_DIR / f"{sample}.task_plan.json"
        if digest(selected_plan) != digest(original_plan):
            raise RuntimeError(f"task plan mismatch: {sample}")
        candidate_notes = []
        if sample in official_baseline and not is_complete(official_baseline[sample]):
            candidate_notes.append(
                f"official_baseline_v2 incomplete ({len(official_baseline[sample]) // 2}RT)"
            )
        if sample in official_diagnostic and not is_complete(
            official_diagnostic[sample]
        ):
            candidate_notes.append(
                f"official_problem15 incomplete ({len(official_diagnostic[sample]) // 2}RT)"
            )
        manifest_rows.append(
            {
                "sample": sample,
                "selected_source": selected_source[sample],
                "selected_result": result_path.relative_to(ROOT).as_posix(),
                "selected_rows": len(selected_rows[sample]),
                "selected_backward_rts": len(scores(selected_rows[sample])),
                "mean_backward_rs": mean_score(selected_rows[sample]),
                "result_sha256": digest(result_path),
                "task_plan_sha256": digest(original_plan),
                "note": "; ".join(candidate_notes),
            }
        )

    original_scope = {sample: original[sample] for sample in scope}
    official_only = {sample: selected_rows[sample] for sample in official_samples}
    original_anomaly, _ = anomaly_summary(original_scope)
    official_anomaly, _ = anomaly_summary(official_only)
    composite_anomaly, anomaly_rows = anomaly_summary(selected_rows)

    original_flat = flatten(original_scope, scope)
    composite_flat = flatten(selected_rows, scope)
    original_metrics = {
        "rs": analyze.rs_at_k(original_flat),
        "critical_failures": analyze.critical_failures(original_flat, 0.10),
        "tokens": analyze.tokens(original_flat),
    }
    composite_metrics = {
        "rs": analyze.rs_at_k(composite_flat),
        "critical_failures": analyze.critical_failures(composite_flat, 0.10),
        "tokens": analyze.tokens(composite_flat),
    }

    complete_hp = {sample for sample, rows in val_hp.items() if is_complete(rows)}
    complete_val_fr = {sample for sample, rows in val_fr.items() if is_complete(rows)}
    strict38 = complete_hp & complete_val_fr
    maximal39 = complete_hp & (complete_val_fr | official_samples)

    def mixed_val(samples: set[str]) -> dict[str, list[dict]]:
        result = {}
        for sample in samples:
            result[sample] = (
                selected_rows[sample]
                if sample in official_samples
                else val_fr[sample]
            )
        return result

    strict_mixed = mixed_val(strict38)
    maximal_mixed = mixed_val(maximal39)
    strict_stats = result_stats(
        flatten(val_hp, strict38), flatten(strict_mixed, strict38)
    )
    maximal_stats = result_stats(
        flatten(val_hp, maximal39), flatten(maximal_mixed, maximal39)
    )
    original_stats = result_stats(
        flatten(val_hp, strict38), flatten(val_fr, strict38)
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    write_csv(OUTPUT_DIR / "source_manifest.csv", manifest_rows)
    anomaly_by_sample = []
    anomaly_index = {row["sample"]: row for row in anomaly_rows}
    for row in manifest_rows:
        anomaly_by_sample.append(
            {
                "sample": row["sample"],
                "selected_source": row["selected_source"],
                **{
                    key: value
                    for key, value in anomaly_index[row["sample"]].items()
                    if key != "sample"
                },
            }
        )
    write_csv(OUTPUT_DIR / "anomaly_by_sample.csv", anomaly_by_sample)
    write_paired_scores(
        OUTPUT_DIR / "val40_paired_scores_strict38.csv",
        strict38,
        val_hp,
        val_fr,
        strict_mixed,
        selected_source,
    )
    write_paired_scores(
        OUTPUT_DIR / "val40_paired_scores_maximal39.csv",
        maximal39,
        val_hp,
        val_fr,
        maximal_mixed,
        selected_source,
    )

    summary = {
        "schema": "fr_plus_official/1",
        "name": "FR+Official",
        "scope_samples": len(scope),
        "official_samples": len(official_samples),
        "official_baseline_v2_samples": len(complete_baseline),
        "official_problem15_fill_samples": sorted(diagnostic_fills),
        "official_run_config": OFFICIAL_RUN_CONFIG,
        "frozen_fr_fallback_samples": len(scope - official_samples),
        "anomaly_definition": {
            "abnormal_termination": "finish_reason is null/abort/length/max_tokens or mapped truncated classification",
            "empty": "raw_llm_response.strip() is empty or mapped empty classification",
            "near_empty": "non-empty raw_llm_response shorter than 200 UTF-8 bytes",
            "any_anomaly": "union of abnormal_termination, empty, near_empty",
        },
        "original_anomaly": original_anomaly,
        "official_only_anomaly": official_anomaly,
        "fr_plus_official_anomaly": composite_anomaly,
        "original_metrics": original_metrics,
        "fr_plus_official_metrics": composite_metrics,
        "val40_original_canonical38": original_stats,
        "val40_fr_plus_official_strict38": strict_stats,
        "val40_fr_plus_official_maximal39": maximal_stats,
        "val40_strict38_replacements": sorted(strict38 & official_samples),
        "val40_maximal39_replacements": sorted(maximal39 & official_samples),
    }
    (OUTPUT_DIR / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    any_samples = composite_anomaly["sample_counts"]["any_anomaly"]
    trunc_samples = composite_anomaly["sample_counts"]["abnormal_termination"]
    empty_samples = composite_anomaly["sample_counts"]["empty"]
    near_samples = composite_anomaly["sample_counts"]["near_empty"]
    official_any = official_anomaly["sample_counts"]["any_anomaly"]
    readme = f"""# FR+Official

`FR+Official` 是在原冻结 FR 之外新增的派生基线版本，不覆盖、重写或删除
`exp_20260710_frbaseline234`。

本目录采用 manifest 逻辑视图，不复制源 JSONL；`source_manifest.csv` 为每个
样本指定唯一源文件及 SHA-256。这样既能复现组合，又不会制造第三份可被误改的
实验归档。

## 组成

- 总范围：233 个样本 × 10RT。
- 87 个样本采用完整的 MiniMax 官方非流式 API 轨迹：83 个来自
  `exp_20260713_frbaseline_v2_rerun`，另有 4 个在该目录无完整轨迹、由
  `exp_20260713_problem15_official` 补齐（`crystal6`、`filesystem3`、
  `obj3d2`、`robotics1`）。
- 其余 146 个样本保留原冻结 FR。任何样本都只选择一条完整 10RT 轨迹；
  不按 RT 拼接，也不按分数选择来源。
- 所有被选官方轨迹的 task plan 与原冻结基线逐字节一致。
- 官方链配置与论文 baseline 对齐口径：MiniMax 官方
  `/v1/chat/completions` 非流式接口，transport revision
  `minimax_official_nonstream/1`，`minimax-m3` adaptive thinking，
  `max_tokens=131072`，seed 42，10RT，no distractor。

这个版本用于披露“按论文 baseline 的官方 API 一次性语义”对结果的影响。
它仍是混合来源：只有已有完整官方重跑的样本被替换，不能表述成 233 个
样本全部经官方 API 重跑。

## 异常定义与样本率

- 异常终止/截断：`finish_reason` 为 null、`abort`、`length` 或
  `max_tokens`，或分类为相应截断类型。
- 空返回：`raw_llm_response.strip()` 为空，或分类为模型空返回。
- 接近空返回：非空，但 UTF-8 正文少于 200 bytes。
- “任一异常”是上述三类的并集；各分类可以重叠。

| 范围 | 任一异常 | 异常终止/截断 | 空返回 | 接近空返回 |
|---|---:|---:|---:|---:|
| **FR+Official（233）** | **{pct(any_samples, 233)}** | {pct(trunc_samples, 233)} | {pct(empty_samples, 233)} | {pct(near_samples, 233)} |
| 被选官方轨迹（87） | **{pct(official_any, 87)}** | {pct(official_anomaly['sample_counts']['abnormal_termination'], 87)} | {pct(official_anomaly['sample_counts']['empty'], 87)} | {pct(official_anomaly['sample_counts']['near_empty'], 87)} |
| 原冻结 FR（同 233） | {pct(original_anomaly['sample_counts']['any_anomaly'], 233)} | {pct(original_anomaly['sample_counts']['abnormal_termination'], 233)} | {pct(original_anomaly['sample_counts']['empty'], 233)} | {pct(original_anomaly['sample_counts']['near_empty'], 233)} |

官方重跑池原本就是按旧链异常/问题样本筛选的，因此 87 个官方轨迹的异常率
不能解释为 MiniMax 官方 API 的总体背景率。FR+Official 的总体样本异常率是
本派生版本可报告的数字。

## 整体曲线

| 版本 | RS@1 | RS@5 | RS@10 | CriticalFailure@10 | tokens |
|---|---:|---:|---:|---:|---:|
| 原冻结 FR | {fmt(original_metrics['rs'][1])} | {fmt(original_metrics['rs'][5])} | {fmt(original_metrics['rs'][10])} | {original_metrics['critical_failures'][0]}/{original_metrics['critical_failures'][1]} | {original_metrics['tokens'][2]:,} |
| **FR+Official** | **{fmt(composite_metrics['rs'][1])}** | **{fmt(composite_metrics['rs'][5])}** | **{fmt(composite_metrics['rs'][10])}** | **{composite_metrics['critical_failures'][0]}/{composite_metrics['critical_failures'][1]}** | **{composite_metrics['tokens'][2]:,}** |

逐样本来源与哈希见 `source_manifest.csv`；异常明细见
`anomaly_by_sample.csv`；机器可读汇总见 `summary.json`。
"""
    (OUTPUT_DIR / "README.md").write_text(readme, encoding="utf-8")

    def comparison_line(label: str, result: dict) -> str:
        p = result["paired"]
        hp_cf, fr_cf = result["hp_cf"], result["fr_cf"]
        ratio = result["hp_tokens"] / result["fr_tokens"]
        return (
            f"| {label} | {p['n']} | {fmt(p['mean_ap'])} | {fmt(p['mean_fr'])} | "
            f"{fmt(p['delta'])} | {p['p']:.3g} | {fmt(p['cohend'], 2)} | "
            f"{hp_cf[0]}/{hp_cf[1]} | {fr_cf[0]}/{fr_cf[1]} | {ratio:.2f}x |"
        )

    replacement_rows = []
    for sample in sorted(maximal39 & official_samples):
        hp_mean = mean_score(val_hp[sample])
        old_mean = mean_score(val_fr[sample])
        new_mean = mean_score(selected_rows[sample])
        replacement_rows.append(
            f"| {sample} | {selected_source[sample]} | {hp_mean:.3f} | "
            f"{old_mean:.3f} | {new_mean:.3f} | {new_mean - old_mean:+.3f} |"
        )

    val_report = f"""# V7 val40 × FR+Official

这是派生敏感性分析，不修改原 val40 归档，也不取代两臂同 transport-v3 的
canonical 结论。官方重跑样本是按旧异常/问题筛选的，存在明确选择偏差。

## 配对结果

| 口径 | n | HP | FR | delta | p | d | HP CF | FR CF | HP/FR tokens |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
{comparison_line('原 canonical38', original_stats)}
{comparison_line('FR+Official strict38', strict_stats)}
{comparison_line('FR+Official maximal39', maximal_stats)}

- strict38 保持原 canonical 38 样本范围，替换 {len(strict38 & official_samples)}
  个完整官方 FR 轨迹。
- maximal39 额外纳入 `earncall1`：其旧 FR-v3 只到 RT6，而官方轨迹完整
  10RT；`json4` 仍因 HP 侧不完整而排除。
- strict38 RS@1/5/10：HP {fmt(strict_stats['hp_rs'][1])}/{fmt(strict_stats['hp_rs'][5])}/{fmt(strict_stats['hp_rs'][10])}
  vs FR+Official {fmt(strict_stats['fr_rs'][1])}/{fmt(strict_stats['fr_rs'][5])}/{fmt(strict_stats['fr_rs'][10])}。
- strict38 ECR-only：n={strict_stats['ecr']['n']}，delta={fmt(strict_stats['ecr']['delta'])}，
  p={strict_stats['ecr']['p']:.3g}。

## 被替换样本

| sample | official source | HP mean | old FR mean | FR+Official mean | official-old |
|---|---|---:|---:|---:|---:|
{chr(10).join(replacement_rows)}

这组混合结果主要反映 baseline 端点/一次性终止政策敏感性，不能把
`delta={strict_stats['paired']['delta']:+.3f}` 单独解释为无偏的方法效应。
"""
    (OUTPUT_DIR / "val40_comparison.md").write_text(val_report, encoding="utf-8")

    print(f"Built {OUTPUT_DIR}")
    print(
        f"FR+Official anomalies: {any_samples}/233 "
        f"({100 * any_samples / 233:.2f}%)"
    )
    print(
        "val40 strict38: "
        f"HP={strict_stats['paired']['mean_ap']:.6f} "
        f"FR={strict_stats['paired']['mean_fr']:.6f} "
        f"delta={strict_stats['paired']['delta']:+.6f}"
    )


if __name__ == "__main__":
    main()

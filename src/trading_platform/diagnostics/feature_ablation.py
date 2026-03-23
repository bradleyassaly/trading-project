from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean, median
from typing import Any

from trading_platform.diagnostics.promotion_frequency import run_promotion_frequency_diagnostic


@dataclass(frozen=True)
class AblationMode:
    name: str
    signal_family: str
    equity_context_enabled: bool
    equity_context_include_volume: bool
    description: str


def _ablation_modes() -> list[AblationMode]:
    return [
        AblationMode(
            name="baseline_momentum",
            signal_family="momentum",
            equity_context_enabled=False,
            equity_context_include_volume=False,
            description="Current baseline momentum path.",
        ),
        AblationMode(
            name="momentum_with_context_features",
            signal_family="momentum",
            equity_context_enabled=True,
            equity_context_include_volume=True,
            description="Control leg with context features materialized but not consumed by the momentum signal family.",
        ),
        AblationMode(
            name="equity_context_momentum",
            signal_family="equity_context_momentum",
            equity_context_enabled=True,
            equity_context_include_volume=True,
            description="Expanded equity-only context signal path using relative return, breadth, realized volatility, and volume regime features.",
        ),
    ]


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric != numeric:
        return None
    return numeric


def _sharpe_distribution(rows: list[dict[str, Any]]) -> dict[str, Any]:
    values = [value for value in (_safe_float(row.get("portfolio_sharpe")) for row in rows) if value is not None]
    if not values:
        return {
            "count": 0,
            "mean": None,
            "median": None,
            "min": None,
            "max": None,
        }
    return {
        "count": len(values),
        "mean": mean(values),
        "median": median(values),
        "min": min(values),
        "max": max(values),
    }


def _build_mode_summary(result: dict[str, Any], mode: AblationMode) -> dict[str, Any]:
    summary = dict(result["summary"])
    summary["portfolio_sharpe_distribution"] = _sharpe_distribution(result["rows"])
    summary["signal_family"] = mode.signal_family
    summary["equity_context_enabled"] = mode.equity_context_enabled
    summary["equity_context_include_volume"] = mode.equity_context_include_volume
    return summary


def _build_per_run_comparison(mode_results: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    scenario_names = sorted(
        {
            row["scenario_name"]
            for result in mode_results.values()
            for row in result["rows"]
        }
    )
    rows: list[dict[str, Any]] = []
    for scenario_name in scenario_names:
        row: dict[str, Any] = {"scenario_name": scenario_name}
        for mode_name, result in mode_results.items():
            match = next((item for item in result["rows"] if item["scenario_name"] == scenario_name), None)
            prefix = mode_name
            row[f"{prefix}_candidate_count"] = int(match.get("candidate_count") or 0) if match else 0
            row[f"{prefix}_validation_pass_count"] = int(match.get("validation_pass_count") or 0) if match else 0
            row[f"{prefix}_promotion_candidate_count"] = int(match.get("promotion_candidate_count") or 0) if match else 0
            row[f"{prefix}_promoted_strategy_count"] = int(match.get("promoted_strategy_count") or 0) if match else 0
            row[f"{prefix}_paper_stage_reached"] = bool(match.get("paper_stage_reached")) if match else False
            row[f"{prefix}_portfolio_sharpe"] = _safe_float(match.get("portfolio_sharpe")) if match else None
        rows.append(row)
    return rows


def _recommendation(mode_summaries: dict[str, dict[str, Any]]) -> str:
    baseline = mode_summaries["baseline_momentum"]
    control = mode_summaries["momentum_with_context_features"]
    expanded = mode_summaries["equity_context_momentum"]
    baseline_promotions = int(baseline["runs_with_promoted_strategies"])
    expanded_promotions = int(expanded["runs_with_promoted_strategies"])
    baseline_sharpe = _safe_float(baseline["portfolio_sharpe_distribution"]["mean"]) or 0.0
    expanded_sharpe = _safe_float(expanded["portfolio_sharpe_distribution"]["mean"]) or 0.0
    control_matches_baseline = (
        control["runs_with_promoted_strategies"] == baseline["runs_with_promoted_strategies"]
        and control["runs_reaching_paper_stage"] == baseline["runs_reaching_paper_stage"]
    )
    if expanded_promotions > baseline_promotions and expanded_sharpe >= baseline_sharpe:
        return "promote equity-context features into broader testing because they show measurable benefit"
    if expanded_promotions >= baseline_promotions and expanded_sharpe > baseline_sharpe and control_matches_baseline:
        return "keep equity-context features as optional experimental path"
    return "keep baseline only"


def _build_comparison_payload(
    *,
    mode_results: dict[str, dict[str, Any]],
    modes: list[AblationMode],
) -> dict[str, Any]:
    mode_summaries = {
        mode.name: _build_mode_summary(mode_results[mode.name], mode)
        for mode in modes
    }
    best_mode = max(
        mode_summaries.items(),
        key=lambda item: (
            int(item[1]["runs_with_promoted_strategies"]),
            int(item[1]["runs_reaching_paper_stage"]),
            _safe_float(item[1]["portfolio_sharpe_distribution"]["mean"]) or float("-inf"),
        ),
    )[0]
    return {
        "mode_summaries": mode_summaries,
        "per_run_comparison": _build_per_run_comparison(mode_results),
        "best_mode_by_funnel_then_sharpe": best_mode,
        "decision": _recommendation(mode_summaries),
    }


def _render_markdown(
    *,
    output_root: Path,
    base_config_path: Path,
    payload: dict[str, Any],
    commands: list[str],
) -> str:
    lines = [
        "# Feature Ablation Comparison",
        "",
        "## Config",
        "",
        f"- base orchestration config: `{base_config_path}`",
        f"- output root: `{output_root}`",
        "- scenario set: `richer_ablation`",
        "",
        "## Commands Run",
        "",
    ]
    lines.extend([f"- `{command}`" for command in commands])
    lines.extend(
        [
            "",
            "## Aggregate Funnel Comparison",
            "",
            "| mode | candidates | validation_pass | promotion_candidates | promoted | portfolio | paper | sharpe_mean | sharpe_median |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for mode_name, summary in payload["comparison"]["mode_summaries"].items():
        sharpe = summary["portfolio_sharpe_distribution"]
        lines.append(
            f"| {mode_name} | {summary['runs_with_candidates']} | {summary['runs_passing_validation']} | "
            f"{summary['runs_with_promotion_candidates']} | {summary['runs_with_promoted_strategies']} | "
            f"{summary['runs_reaching_portfolio_stage']} | {summary['runs_reaching_paper_stage']} | "
            f"{sharpe['mean']} | {sharpe['median']} |"
        )
    lines.extend(
        [
            "",
            "## Per-Run Highlights",
            "",
            "| scenario | baseline promoted | control promoted | equity-context promoted | baseline sharpe | control sharpe | equity-context sharpe |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in payload["comparison"]["per_run_comparison"]:
        lines.append(
            f"| {row['scenario_name']} | {row['baseline_momentum_promoted_strategy_count']} | "
            f"{row['momentum_with_context_features_promoted_strategy_count']} | "
            f"{row['equity_context_momentum_promoted_strategy_count']} | "
            f"{row['baseline_momentum_portfolio_sharpe']} | "
            f"{row['momentum_with_context_features_portfolio_sharpe']} | "
            f"{row['equity_context_momentum_portfolio_sharpe']} |"
        )
    lines.extend(
        [
            "",
            "## Conclusion",
            "",
            f"- best mode by funnel then sharpe: `{payload['comparison']['best_mode_by_funnel_then_sharpe']}`",
            f"- decision: {payload['comparison']['decision']}",
        ]
    )
    return "\n".join(lines) + "\n"


def run_feature_ablation_comparison(
    *,
    output_root: Path,
    base_config_path: Path,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    modes = _ablation_modes()
    commands = [
        (
            f"python -m trading_platform.diagnostics.feature_ablation --output-root {output_root} "
            f"--base-config {base_config_path}"
        )
    ]
    mode_results: dict[str, dict[str, Any]] = {}
    for mode in modes:
        mode_output_root = output_root / mode.name
        commands.append(
            (
                f"python -m trading_platform.diagnostics.promotion_frequency --output-root {mode_output_root} "
                f"--base-config {base_config_path} --signal-family {mode.signal_family} "
                f"--scenario-set richer_ablation"
                + (" --equity-context-enabled" if mode.equity_context_enabled else "")
                + (" --equity-context-include-volume" if mode.equity_context_include_volume else "")
            )
        )
        mode_results[mode.name] = run_promotion_frequency_diagnostic(
            output_root=mode_output_root,
            base_config_path=base_config_path,
            signal_family=mode.signal_family,
            equity_context_enabled=mode.equity_context_enabled,
            equity_context_include_volume=mode.equity_context_include_volume,
            scenario_set_name="richer_ablation",
        )

    comparison = _build_comparison_payload(mode_results=mode_results, modes=modes)
    payload = {
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "base_config_path": str(base_config_path),
        "output_root": str(output_root),
        "modes": [
            {
                "name": mode.name,
                "signal_family": mode.signal_family,
                "equity_context_enabled": mode.equity_context_enabled,
                "equity_context_include_volume": mode.equity_context_include_volume,
                "description": mode.description,
            }
            for mode in modes
        ],
        "mode_results": mode_results,
        "comparison": comparison,
        "commands": commands,
    }
    json_path = output_root / "feature_ablation_comparison.json"
    md_path = output_root / "feature_ablation_comparison.md"
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    md_path.write_text(
        _render_markdown(
            output_root=output_root,
            base_config_path=base_config_path,
            payload=payload,
            commands=commands,
        ),
        encoding="utf-8",
    )
    return {
        "json_path": str(json_path),
        "md_path": str(md_path),
        "comparison": comparison,
        "mode_results": mode_results,
        "commands": commands,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a richer equity-only feature ablation comparison.")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("artifacts/diagnostics/equity_feature_expansion"),
        help="Directory where ablation comparison artifacts will be written.",
    )
    parser.add_argument(
        "--base-config",
        type=Path,
        default=Path("configs/orchestration_signal_promotion_test.yaml"),
        help="Existing orchestration viability config reused for all ablation modes.",
    )
    args = parser.parse_args()
    result = run_feature_ablation_comparison(
        output_root=args.output_root,
        base_config_path=args.base_config,
    )
    print(f"Ablation JSON: {result['json_path']}")
    print(f"Ablation Markdown: {result['md_path']}")
    print(json.dumps(result["comparison"], indent=2))


if __name__ == "__main__":
    main()

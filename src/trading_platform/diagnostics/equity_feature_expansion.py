from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from trading_platform.diagnostics.promotion_frequency import run_promotion_frequency_diagnostic


def _rate_delta(expanded: dict[str, Any], baseline: dict[str, Any], key: str) -> float:
    expanded_rate = float(expanded.get("rates", {}).get(key, 0.0) or 0.0)
    baseline_rate = float(baseline.get("rates", {}).get(key, 0.0) or 0.0)
    return expanded_rate - baseline_rate


def _build_comparison_summary(*, baseline: dict[str, Any], expanded: dict[str, Any]) -> dict[str, Any]:
    count_fields = [
        "total_runs_attempted",
        "runs_with_candidates",
        "runs_passing_validation",
        "runs_with_promotion_candidates",
        "runs_with_promoted_strategies",
        "runs_reaching_portfolio_stage",
        "runs_reaching_paper_stage",
    ]
    deltas = {
        field: int(expanded.get(field, 0) or 0) - int(baseline.get(field, 0) or 0)
        for field in count_fields
    }
    rate_keys = [
        "with_candidates",
        "passing_validation",
        "with_promotion_candidates",
        "with_promoted_strategies",
        "reaching_portfolio_stage",
        "reaching_paper_stage",
    ]
    rate_deltas = {
        key: _rate_delta(expanded, baseline, key)
        for key in rate_keys
    }
    improved = (
        deltas["runs_with_promoted_strategies"] > 0
        or deltas["runs_reaching_paper_stage"] > 0
        or rate_deltas["with_promoted_strategies"] > 0.0
        or rate_deltas["reaching_paper_stage"] > 0.0
    )
    return {
        "baseline": baseline,
        "expanded": expanded,
        "count_deltas": deltas,
        "rate_deltas": rate_deltas,
        "promotion_frequency_improved": improved,
        "downstream_activity_improved": (
            deltas["runs_reaching_portfolio_stage"] > 0 or deltas["runs_reaching_paper_stage"] > 0
        ),
        "conclusion": (
            "Equity-only context features improved promotable-signal frequency and downstream activity in this lightweight diagnostic."
            if improved
            else "Equity-only context features did not improve promotable-signal frequency in this lightweight diagnostic."
        ),
    }


def _render_markdown(
    *,
    output_root: Path,
    base_config_path: Path,
    payload: dict[str, Any],
) -> str:
    baseline = payload["baseline"]
    expanded = payload["expanded"]
    comparison = payload["comparison"]
    lines = [
        "# Equity Feature Expansion Comparison",
        "",
        "## Config",
        "",
        f"- base orchestration config: `{base_config_path}`",
        f"- output root: `{output_root}`",
        "- baseline signal family: `momentum`",
        "- expanded signal family: `equity_context_momentum`",
        "- expanded equity context toggles: `equity_context_enabled=true`, `equity_context_include_volume=false`",
        "",
        "## Commands Run",
        "",
    ]
    lines.extend([f"- `{command}`" for command in payload["commands"]])
    lines.extend(
        [
            "",
            "## Funnel Comparison",
            "",
            "| metric | baseline | expanded | delta |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for field in [
        "total_runs_attempted",
        "runs_with_candidates",
        "runs_passing_validation",
        "runs_with_promotion_candidates",
        "runs_with_promoted_strategies",
        "runs_reaching_portfolio_stage",
        "runs_reaching_paper_stage",
    ]:
        lines.append(
            f"| {field} | {baseline['summary'][field]} | {expanded['summary'][field]} | {comparison['count_deltas'][field]} |"
        )
    lines.extend(
        [
            "",
            "## Assessment",
            "",
            f"- promotion frequency improved: `{comparison['promotion_frequency_improved']}`",
            f"- downstream activity improved: `{comparison['downstream_activity_improved']}`",
            f"- baseline most common drop stage: `{baseline['summary']['most_common_drop_stage']}`",
            f"- expanded most common drop stage: `{expanded['summary']['most_common_drop_stage']}`",
            f"- conclusion: {comparison['conclusion']}",
            "",
            "## Artifact Paths",
            "",
            f"- baseline json: `{baseline['json_path']}`",
            f"- expanded json: `{expanded['json_path']}`",
        ]
    )
    return "\n".join(lines) + "\n"


def run_equity_feature_expansion_comparison(
    *,
    output_root: Path,
    base_config_path: Path,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    baseline_output_root = output_root / "baseline"
    expanded_output_root = output_root / "expanded"
    commands = [
        (
            f"python -m trading_platform.diagnostics.equity_feature_expansion --output-root {output_root} "
            f"--base-config {base_config_path}"
        ),
        (
            f"python -m trading_platform.diagnostics.promotion_frequency --output-root {baseline_output_root} "
            f"--base-config {base_config_path} --signal-family momentum"
        ),
        (
            f"python -m trading_platform.diagnostics.promotion_frequency --output-root {expanded_output_root} "
            f"--base-config {base_config_path} --signal-family equity_context_momentum --equity-context-enabled"
        ),
    ]
    baseline = run_promotion_frequency_diagnostic(
        output_root=baseline_output_root,
        base_config_path=base_config_path,
        signal_family="momentum",
        equity_context_enabled=False,
        equity_context_include_volume=False,
    )
    expanded = run_promotion_frequency_diagnostic(
        output_root=expanded_output_root,
        base_config_path=base_config_path,
        signal_family="equity_context_momentum",
        equity_context_enabled=True,
        equity_context_include_volume=False,
    )
    comparison = _build_comparison_summary(
        baseline=baseline["summary"],
        expanded=expanded["summary"],
    )
    payload = {
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "base_config_path": str(base_config_path),
        "output_root": str(output_root),
        "baseline": baseline,
        "expanded": expanded,
        "comparison": comparison,
        "commands": commands,
    }
    json_path = output_root / "equity_feature_expansion_comparison.json"
    md_path = output_root / "equity_feature_expansion_comparison.md"
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    md_path.write_text(
        _render_markdown(
            output_root=output_root,
            base_config_path=base_config_path,
            payload=payload,
        ),
        encoding="utf-8",
    )
    return {
        "json_path": str(json_path),
        "md_path": str(md_path),
        "baseline": baseline,
        "expanded": expanded,
        "comparison": comparison,
        "commands": commands,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare baseline and equity-context promotion-frequency diagnostics.")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("artifacts/diagnostics/equity_feature_expansion"),
        help="Directory where comparison artifacts will be written.",
    )
    parser.add_argument(
        "--base-config",
        type=Path,
        default=Path("configs/orchestration_signal_promotion_test.yaml"),
        help="Existing orchestration viability config reused for both baseline and expanded runs.",
    )
    args = parser.parse_args()
    result = run_equity_feature_expansion_comparison(
        output_root=args.output_root,
        base_config_path=args.base_config,
    )
    print(f"Comparison JSON: {result['json_path']}")
    print(f"Comparison Markdown: {result['md_path']}")
    print(json.dumps(result["comparison"], indent=2))


if __name__ == "__main__":
    main()

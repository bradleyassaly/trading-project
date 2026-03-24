from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.diagnostics.promotion_frequency import run_promotion_frequency_diagnostic


DEFAULT_SIGNAL_FAMILIES = [
    "momentum",
    "volatility_adjusted_momentum",
    "short_horizon_mean_reversion",
    "momentum_acceleration",
    "cross_sectional_relative_strength",
    "volume_shock_momentum",
]


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _sharpe_summary(rows: list[dict[str, Any]]) -> dict[str, float | int | None]:
    values = [_safe_float(row.get("portfolio_sharpe")) for row in rows]
    clean = [value for value in values if value is not None]
    if not clean:
        return {
            "count": 0,
            "mean": None,
            "median": None,
            "max": None,
            "min": None,
        }
    series = pd.Series(clean, dtype=float)
    return {
        "count": int(series.count()),
        "mean": float(series.mean()),
        "median": float(series.median()),
        "max": float(series.max()),
        "min": float(series.min()),
    }


def _build_family_summary(signal_family: str, result: dict[str, Any]) -> dict[str, Any]:
    summary = dict(result["summary"])
    rows = list(result["rows"])
    return {
        "signal_family": signal_family,
        "scenario_count": len(rows),
        "total_candidate_count": int(sum(int(row.get("candidate_count") or 0) for row in rows)),
        "validation_pass_count": int(sum(int(row.get("validation_pass_count") or 0) for row in rows)),
        "promotion_candidate_count": int(sum(int(row.get("promotion_candidate_count") or 0) for row in rows)),
        "promoted_strategy_count": int(sum(int(row.get("promoted_strategy_count") or 0) for row in rows)),
        "portfolio_selected_count": int(sum(int(row.get("portfolio_selected_strategy_count") or 0) for row in rows)),
        "paper_stage_count": int(sum(1 for row in rows if bool(row.get("paper_stage_reached")))),
        "summary": summary,
        "portfolio_sharpe_summary": _sharpe_summary(rows),
        "json_path": result["json_path"],
        "csv_path": result["csv_path"],
        "md_path": result["md_path"],
    }


def _ranking_tuple(row: dict[str, Any]) -> tuple[float, float, float, float]:
    sharpe_mean = _safe_float((row.get("portfolio_sharpe_summary") or {}).get("mean")) or float("-inf")
    summary = row.get("summary", {})
    return (
        float(summary.get("runs_with_promoted_strategies") or 0),
        float(summary.get("runs_reaching_paper_stage") or 0),
        float(summary.get("runs_passing_validation") or 0),
        sharpe_mean,
    )


def _build_comparison_payload(
    *,
    family_summaries: list[dict[str, Any]],
    commands: list[str],
    base_config_path: Path,
    output_root: Path,
    scenario_set_name: str,
) -> dict[str, Any]:
    ordered = sorted(family_summaries, key=_ranking_tuple, reverse=True)
    best = ordered[0]["signal_family"] if ordered else None
    baseline = next((row for row in ordered if row["signal_family"] == "momentum"), None)
    best_row = ordered[0] if ordered else None
    decision = "keep baseline only"
    if best_row is not None and best_row["signal_family"] != "momentum":
        baseline_rank = _ranking_tuple(baseline) if baseline else (0.0, 0.0, 0.0, float("-inf"))
        if _ranking_tuple(best_row) > baseline_rank:
            decision = "promote winning new family into broader testing"
        else:
            decision = "keep new families optional and experimental"
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "base_config_path": str(base_config_path),
        "output_root": str(output_root),
        "scenario_set_name": scenario_set_name,
        "commands": commands,
        "families": family_summaries,
        "best_family_by_funnel_then_sharpe": best,
        "decision": decision,
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Signal Family Comparison",
        "",
        "## Summary",
        "",
        f"- base config: `{payload['base_config_path']}`",
        f"- scenario set: `{payload['scenario_set_name']}`",
        f"- best family by funnel then sharpe: `{payload['best_family_by_funnel_then_sharpe']}`",
        f"- decision: `{payload['decision']}`",
        "",
        "## Commands Run",
        "",
    ]
    lines.extend(f"- `{command}`" for command in payload["commands"])
    lines.extend(
        [
            "",
            "## Family Comparison",
            "",
            "| family | candidates | validation_pass | promotion_candidates | promoted | paper_runs | sharpe_mean |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in payload["families"]:
        sharpe_mean = (row.get("portfolio_sharpe_summary") or {}).get("mean")
        lines.append(
            f"| {row['signal_family']} | {row['total_candidate_count']} | {row['validation_pass_count']} | "
            f"{row['promotion_candidate_count']} | {row['promoted_strategy_count']} | {row['paper_stage_count']} | {sharpe_mean} |"
        )
    return "\n".join(lines) + "\n"


def run_signal_family_comparison(
    *,
    output_root: Path,
    base_config_path: Path,
    signal_families: list[str],
    scenario_set_name: str = "default",
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    commands = [
        (
            f"python -m trading_platform.diagnostics.signal_family_comparison --output-root {output_root} "
            f"--base-config {base_config_path} --scenario-set {scenario_set_name} --signal-families "
            + " ".join(signal_families)
        )
    ]
    family_summaries: list[dict[str, Any]] = []
    for signal_family in signal_families:
        family_result = run_promotion_frequency_diagnostic(
            output_root=output_root / signal_family,
            base_config_path=base_config_path,
            signal_family=signal_family,
            artifact_stem="signal_promotion_frequency",
            scenario_set_name=scenario_set_name,
        )
        family_summaries.append(_build_family_summary(signal_family, family_result))
    payload = _build_comparison_payload(
        family_summaries=family_summaries,
        commands=commands,
        base_config_path=base_config_path,
        output_root=output_root,
        scenario_set_name=scenario_set_name,
    )
    json_path = output_root / "signal_family_comparison.json"
    md_path = output_root / "signal_family_comparison.md"
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    md_path.write_text(_render_markdown(payload), encoding="utf-8")
    return {
        "json_path": str(json_path),
        "md_path": str(md_path),
        "payload": payload,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare repeated promotion-frequency results across signal families.")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("artifacts/diagnostics/signal_family_comparison"),
        help="Directory where comparison artifacts will be written.",
    )
    parser.add_argument(
        "--base-config",
        type=Path,
        default=Path("configs/orchestration_signal_promotion_test.yaml"),
        help="Existing orchestration config to reuse across family comparisons.",
    )
    parser.add_argument(
        "--signal-families",
        nargs="+",
        default=DEFAULT_SIGNAL_FAMILIES,
        help="Signal families to compare.",
    )
    parser.add_argument(
        "--scenario-set",
        type=str,
        default="default",
        choices=["default", "richer_ablation"],
        help="Built-in deterministic scenario set used to generate fixtures for the comparison.",
    )
    args = parser.parse_args()
    result = run_signal_family_comparison(
        output_root=args.output_root,
        base_config_path=args.base_config,
        signal_families=list(args.signal_families),
        scenario_set_name=args.scenario_set,
    )
    print(f"Signal family comparison JSON: {result['json_path']}")
    print(f"Signal family comparison Markdown: {result['md_path']}")
    print(json.dumps(result["payload"]["families"], indent=2))


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from trading_platform.config.loader import load_strategy_portfolio_policy_config
from trading_platform.portfolio.strategy_portfolio import (
    StrategyPortfolioPolicyConfig,
    build_strategy_portfolio,
    export_strategy_portfolio_run_config,
    load_strategy_portfolio,
)


DEFAULT_WEIGHTING_MODES = [
    "equal_weight",
    "metric_weighted",
    "capped_metric_weighted",
    "inverse_count_by_signal_family",
    "score_then_cap",
]


def _write_sample_promoted_fixture(output_root: Path) -> Path:
    fixture_dir = output_root / "input_fixture"
    fixture_dir.mkdir(parents=True, exist_ok=True)
    strategies = [
        {
            "preset_name": "generated_momentum_core",
            "source_run_id": "run-momentum-core",
            "signal_family": "momentum",
            "universe": "nasdaq100",
            "status": "active",
            "ranking_metric": "portfolio_sharpe",
            "ranking_value": 1.60,
            "promotion_timestamp": "2026-03-24T00:00:00+00:00",
        },
        {
            "preset_name": "generated_momentum_satellite",
            "source_run_id": "run-momentum-satellite",
            "signal_family": "momentum",
            "universe": "sp500",
            "status": "active",
            "ranking_metric": "portfolio_sharpe",
            "ranking_value": 1.25,
            "promotion_timestamp": "2026-03-23T00:00:00+00:00",
        },
        {
            "preset_name": "generated_relative_strength",
            "source_run_id": "run-relative-strength",
            "signal_family": "cross_sectional_relative_strength",
            "universe": "nasdaq100",
            "status": "active",
            "ranking_metric": "portfolio_sharpe",
            "ranking_value": 1.18,
            "promotion_timestamp": "2026-03-22T00:00:00+00:00",
        },
        {
            "preset_name": "generated_volume_shock",
            "source_run_id": "run-volume-shock",
            "signal_family": "volume_shock_momentum",
            "universe": "nasdaq100",
            "status": "active",
            "ranking_metric": "portfolio_sharpe",
            "ranking_value": 1.05,
            "promotion_timestamp": "2026-03-21T00:00:00+00:00",
        },
        {
            "preset_name": "generated_mean_reversion",
            "source_run_id": "run-mean-reversion",
            "signal_family": "short_horizon_mean_reversion",
            "universe": "sp500",
            "status": "active",
            "ranking_metric": "portfolio_sharpe",
            "ranking_value": 0.82,
            "promotion_timestamp": "2026-03-20T00:00:00+00:00",
        },
        {
            "preset_name": "generated_duplicate_momentum",
            "source_run_id": "run-momentum-core",
            "signal_family": "momentum",
            "universe": "nasdaq100",
            "status": "active",
            "ranking_metric": "portfolio_sharpe",
            "ranking_value": 1.40,
            "promotion_timestamp": "2026-03-19T00:00:00+00:00",
        },
    ]
    payload = {"strategies": []}
    for strategy in strategies:
        preset_path = fixture_dir / f"{strategy['preset_name']}.json"
        registry_path = fixture_dir / f"{strategy['preset_name']}_registry.json"
        pipeline_path = fixture_dir / f"{strategy['preset_name']}_pipeline.yaml"
        preset_path.write_text(json.dumps({"name": strategy["preset_name"], "params": {}}, indent=2), encoding="utf-8")
        registry_path.write_text(json.dumps({"strategy_id": strategy["preset_name"]}, indent=2), encoding="utf-8")
        pipeline_path.write_text("run_name: sample\n", encoding="utf-8")
        payload["strategies"].append(
            strategy
            | {
                "generated_preset_path": str(preset_path),
                "generated_registry_path": str(registry_path),
                "generated_pipeline_config_path": str(pipeline_path),
            }
        )
    promoted_path = fixture_dir / "promoted_strategies.json"
    promoted_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return fixture_dir


def _mode_summary(
    *,
    mode: str,
    portfolio_payload: dict[str, Any],
    portfolio_paths: dict[str, Any],
    run_bundle_paths: dict[str, Any] | None,
) -> dict[str, Any]:
    summary = portfolio_payload.get("summary", {})
    selected = list(portfolio_payload.get("selected_strategies", []))
    weights = {str(row["preset_name"]): float(row.get("allocation_weight") or 0.0) for row in selected}
    weighted_metric_average = sum(
        float(row.get("allocation_weight") or 0.0) * float(row.get("selection_metric_value") or 0.0)
        for row in selected
    )
    return {
        "weighting_mode": mode,
        "selected_strategy_count": int(summary.get("total_selected_strategies") or 0),
        "selected_weights": weights,
        "max_strategy_weight": float(summary.get("max_strategy_weight") or 0.0),
        "max_family_weight": float(summary.get("max_family_weight") or 0.0),
        "effective_strategy_count": float(summary.get("effective_strategy_count") or 0.0),
        "effective_family_count": float(summary.get("effective_family_count") or 0.0),
        "family_weight_summary": dict(summary.get("signal_family_weights") or {}),
        "total_active_weight": float(summary.get("total_active_weight") or 0.0),
        "warning_count": int(summary.get("warning_count") or 0),
        "weighted_metric_average": weighted_metric_average,
        "preset_path_ready_count": int(summary.get("preset_path_ready_count") or 0),
        "pipeline_path_ready_count": int(summary.get("pipeline_path_ready_count") or 0),
        "run_bundle_exported": bool(run_bundle_paths),
        "strategy_portfolio_json_path": portfolio_paths["strategy_portfolio_json_path"],
        "strategy_portfolio_csv_path": portfolio_paths["strategy_portfolio_csv_path"],
        "run_bundle_path": run_bundle_paths["run_bundle_path"] if run_bundle_paths else None,
        "multi_strategy_config_path": run_bundle_paths["multi_strategy_config_path"] if run_bundle_paths else None,
    }


def _recommend_mode(rows: list[dict[str, Any]]) -> str | None:
    if not rows:
        return None

    def score(row: dict[str, Any]) -> tuple[float, float, float, float, float]:
        diversification = float(row.get("effective_strategy_count") or 0.0) + float(row.get("effective_family_count") or 0.0)
        return (
            float(row.get("run_bundle_exported") or 0.0),
            diversification,
            float(row.get("weighted_metric_average") or 0.0),
            -float(row.get("max_family_weight") or 0.0),
            -float(row.get("max_strategy_weight") or 0.0),
        )

    ordered = sorted(rows, key=score, reverse=True)
    return str(ordered[0]["weighting_mode"])


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Strategy Weighting Comparison",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- promoted_dir: `{payload['promoted_dir']}`",
        f"- input_kind: `{payload['input_kind']}`",
        f"- recommended_mode: `{payload['recommended_mode']}`",
        "",
        "## Commands",
        "",
    ]
    lines.extend(f"- `{command}`" for command in payload["commands"])
    lines.extend(
        [
            "",
            "## Modes",
            "",
            "| mode | selected | max_strategy | max_family | effective_strategies | effective_families | weighted_metric_avg | exportable |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in payload["rows"]:
        lines.append(
            f"| {row['weighting_mode']} | {row['selected_strategy_count']} | {row['max_strategy_weight']:.4f} | "
            f"{row['max_family_weight']:.4f} | {row['effective_strategy_count']:.4f} | {row['effective_family_count']:.4f} | "
            f"{row['weighted_metric_average']:.4f} | {'yes' if row['run_bundle_exported'] else 'no'} |"
        )
    return "\n".join(lines) + "\n"


def run_strategy_weighting_comparison(
    *,
    output_root: Path,
    promoted_dir: Path | None = None,
    policy_config_path: Path | None = None,
    weighting_modes: list[str] | None = None,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    input_kind = "provided_promoted_dir"
    if promoted_dir is None:
        promoted_dir = _write_sample_promoted_fixture(output_root)
        input_kind = "synthetic_fixture"
    policy = (
        load_strategy_portfolio_policy_config(policy_config_path)
        if policy_config_path is not None
        else StrategyPortfolioPolicyConfig(
            max_strategies=5,
            max_strategies_per_signal_family=2,
            max_weight_per_strategy=0.40,
            min_weight_per_strategy=0.05,
            metric_weight_cap_multiple=1.0,
        )
    )
    modes = weighting_modes or list(DEFAULT_WEIGHTING_MODES)
    rows: list[dict[str, Any]] = []
    for mode in modes:
        mode_policy = replace(policy, weighting_mode=mode)
        mode_output_dir = output_root / mode
        portfolio_paths = build_strategy_portfolio(
            promoted_dir=promoted_dir,
            output_dir=mode_output_dir,
            policy=mode_policy,
        )
        portfolio_payload = load_strategy_portfolio(mode_output_dir)
        run_bundle_paths = export_strategy_portfolio_run_config(
            strategy_portfolio_path=mode_output_dir,
            output_dir=mode_output_dir / "run_bundle",
        )
        rows.append(
            _mode_summary(
                mode=mode,
                portfolio_payload=portfolio_payload,
                portfolio_paths=portfolio_paths,
                run_bundle_paths=run_bundle_paths,
            )
        )
    commands = [
        (
            f"python -m trading_platform.diagnostics.strategy_weighting_comparison --output-root {output_root}"
            + (f" --promoted-dir {promoted_dir}" if input_kind == "provided_promoted_dir" else "")
            + (f" --policy-config {policy_config_path}" if policy_config_path is not None else "")
            + " --weighting-modes "
            + " ".join(modes)
        )
    ]
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "promoted_dir": str(promoted_dir),
        "policy": policy.__dict__,
        "input_kind": input_kind,
        "commands": commands,
        "rows": rows,
        "recommended_mode": _recommend_mode(rows),
    }
    json_path = output_root / "strategy_weighting_comparison.json"
    md_path = output_root / "strategy_weighting_comparison.md"
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    md_path.write_text(_render_markdown(payload), encoding="utf-8")
    return {
        "json_path": str(json_path),
        "md_path": str(md_path),
        "payload": payload,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare transparent strategy weighting modes for promoted strategies.")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("artifacts/diagnostics/strategy_weighting_comparison"),
        help="Directory where comparison artifacts will be written.",
    )
    parser.add_argument(
        "--promoted-dir",
        type=Path,
        default=None,
        help="Optional promoted-strategies directory. If omitted, a deterministic synthetic fixture is used.",
    )
    parser.add_argument(
        "--policy-config",
        type=Path,
        default=None,
        help="Optional strategy portfolio policy config to use as the base policy.",
    )
    parser.add_argument(
        "--weighting-modes",
        nargs="+",
        default=list(DEFAULT_WEIGHTING_MODES),
        help="Weighting modes to compare.",
    )
    args = parser.parse_args()
    result = run_strategy_weighting_comparison(
        output_root=args.output_root,
        promoted_dir=args.promoted_dir,
        policy_config_path=args.policy_config,
        weighting_modes=list(args.weighting_modes),
    )
    print(f"Strategy weighting comparison JSON: {result['json_path']}")
    print(f"Strategy weighting comparison Markdown: {result['md_path']}")
    print(json.dumps(result["payload"]["rows"], indent=2))


if __name__ == "__main__":
    main()

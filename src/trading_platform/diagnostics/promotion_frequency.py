from __future__ import annotations

import argparse
import json
import shutil
import warnings
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.config.loader import load_automated_orchestration_config
from trading_platform.config.loader import load_promotion_policy_config, load_strategy_validation_policy_config
from trading_platform.orchestration.pipeline_runner import run_automated_orchestration
from trading_platform.research.alpha_lab.runner import run_alpha_research
from trading_platform.research.promotion_pipeline import apply_research_promotions
from trading_platform.research.registry import build_promotion_candidates, build_research_registry
from trading_platform.research.strategy_validation import build_strategy_validation


@dataclass(frozen=True)
class ScenarioSpec:
    name: str
    symbols: tuple[str, ...]
    returns_by_symbol: dict[str, list[float]]
    volume_by_symbol: dict[str, list[float]] | None = None


def _default_scenarios() -> list[ScenarioSpec]:
    def constant(value: float, periods: int = 79) -> list[float]:
        return [value] * periods

    def alternating(a: float, b: float, periods: int = 79) -> list[float]:
        return [a if idx % 2 == 0 else b for idx in range(periods)]

    def split(first: float, second: float, periods: int = 79) -> list[float]:
        pivot = periods // 2
        return [first] * pivot + [second] * (periods - pivot)

    return [
        ScenarioSpec(
            name="strong_momentum",
            symbols=("AAPL", "MSFT", "NVDA"),
            returns_by_symbol={
                "AAPL": constant(0.010),
                "MSFT": constant(0.015),
                "NVDA": constant(0.020),
            },
        ),
        ScenarioSpec(
            name="moderate_momentum",
            symbols=("AAPL", "MSFT", "NVDA"),
            returns_by_symbol={
                "AAPL": constant(0.0010),
                "MSFT": constant(0.0020),
                "NVDA": constant(0.0030),
            },
        ),
        ScenarioSpec(
            name="mixed_dispersion",
            symbols=("AAPL", "MSFT", "NVDA", "AMZN"),
            returns_by_symbol={
                "AAPL": constant(0.0060),
                "MSFT": constant(0.0020),
                "NVDA": constant(-0.0025),
                "AMZN": constant(-0.0060),
            },
        ),
        ScenarioSpec(
            name="regime_flip",
            symbols=("AAPL", "MSFT", "NVDA"),
            returns_by_symbol={
                "AAPL": split(0.018, -0.018),
                "MSFT": split(0.010, -0.010),
                "NVDA": split(0.004, -0.004),
            },
        ),
        ScenarioSpec(
            name="alternating_noise",
            symbols=("AAPL", "MSFT", "NVDA"),
            returns_by_symbol={
                "AAPL": alternating(0.012, -0.012),
                "MSFT": alternating(-0.008, 0.008),
                "NVDA": alternating(0.006, -0.006),
            },
        ),
        ScenarioSpec(
            name="flat_market",
            symbols=("AAPL", "MSFT", "NVDA"),
            returns_by_symbol={
                "AAPL": constant(0.0),
                "MSFT": constant(0.0),
                "NVDA": constant(0.0),
            },
        ),
    ]


def _richer_ablation_scenarios() -> list[ScenarioSpec]:
    periods = 79

    def constant(value: float) -> list[float]:
        return [value] * periods

    def alternating(a: float, b: float) -> list[float]:
        return [a if idx % 2 == 0 else b for idx in range(periods)]

    def split(first: float, second: float) -> list[float]:
        pivot = periods // 2
        return [first] * pivot + [second] * (periods - pivot)

    def cyclical(base: float, amplitude: float, cycle: int) -> list[float]:
        values: list[float] = []
        for idx in range(periods):
            phase = (idx % cycle) / max(cycle - 1, 1)
            values.append(base + amplitude * (2.0 * phase - 1.0))
        return values

    def trend(start: float, stop: float) -> list[float]:
        if periods <= 1:
            return [start]
        step = (stop - start) / (periods - 1)
        return [start + step * idx for idx in range(periods)]

    def volume_constant(value: float) -> list[float]:
        return [value] * periods

    def volume_trend(start: float, stop: float) -> list[float]:
        if periods <= 1:
            return [start]
        step = (stop - start) / (periods - 1)
        return [start + step * idx for idx in range(periods)]

    return [
        ScenarioSpec(
            name="steady_trend_with_vol_penalty",
            symbols=("AAPL", "MSFT", "NVDA", "AMZN"),
            returns_by_symbol={
                "AAPL": constant(0.007),
                "MSFT": alternating(0.018, -0.004),
                "NVDA": constant(0.005),
                "AMZN": constant(-0.002),
            },
            volume_by_symbol={
                "AAPL": volume_constant(1_000_000),
                "MSFT": volume_constant(900_000),
                "NVDA": volume_constant(850_000),
                "AMZN": volume_constant(800_000),
            },
        ),
        ScenarioSpec(
            name="volume_confirmation_rotation",
            symbols=("AAPL", "MSFT", "NVDA", "AMZN"),
            returns_by_symbol={
                "AAPL": constant(0.004),
                "MSFT": constant(0.0045),
                "NVDA": constant(0.004),
                "AMZN": constant(0.0035),
            },
            volume_by_symbol={
                "AAPL": volume_trend(700_000, 1_600_000),
                "MSFT": volume_trend(1_400_000, 650_000),
                "NVDA": volume_constant(950_000),
                "AMZN": volume_constant(900_000),
            },
        ),
        ScenarioSpec(
            name="breadth_collapse_defensive_leader",
            symbols=("AAPL", "MSFT", "NVDA", "AMZN", "META"),
            returns_by_symbol={
                "AAPL": split(0.008, 0.002),
                "MSFT": split(0.006, -0.008),
                "NVDA": split(0.005, -0.012),
                "AMZN": split(0.004, -0.010),
                "META": split(0.003, -0.009),
            },
            volume_by_symbol={
                "AAPL": volume_constant(1_200_000),
                "MSFT": volume_constant(1_000_000),
                "NVDA": volume_constant(980_000),
                "AMZN": volume_constant(920_000),
                "META": volume_constant(910_000),
            },
        ),
        ScenarioSpec(
            name="noisy_leader_vs_stable_runner",
            symbols=("AAPL", "MSFT", "NVDA", "AMZN"),
            returns_by_symbol={
                "AAPL": constant(0.0055),
                "MSFT": alternating(0.020, -0.006),
                "NVDA": constant(0.0045),
                "AMZN": constant(-0.0010),
            },
            volume_by_symbol={
                "AAPL": volume_constant(1_050_000),
                "MSFT": alternating(1_900_000, 500_000),
                "NVDA": volume_constant(980_000),
                "AMZN": volume_constant(850_000),
            },
        ),
        ScenarioSpec(
            name="broad_market_chop",
            symbols=("AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOG"),
            returns_by_symbol={
                "AAPL": cyclical(0.002, 0.006, 6),
                "MSFT": cyclical(0.0015, 0.005, 5),
                "NVDA": alternating(0.012, -0.010),
                "AMZN": alternating(0.009, -0.007),
                "META": constant(0.0010),
                "GOOG": constant(-0.0005),
            },
            volume_by_symbol={
                "AAPL": volume_constant(1_100_000),
                "MSFT": volume_constant(1_050_000),
                "NVDA": alternating(1_800_000, 700_000),
                "AMZN": alternating(1_600_000, 650_000),
                "META": volume_constant(900_000),
                "GOOG": volume_constant(880_000),
            },
        ),
        ScenarioSpec(
            name="late_volume_breakout",
            symbols=("AAPL", "MSFT", "NVDA", "AMZN"),
            returns_by_symbol={
                "AAPL": split(0.001, 0.010),
                "MSFT": split(0.006, 0.002),
                "NVDA": split(0.004, 0.004),
                "AMZN": split(-0.002, 0.001),
            },
            volume_by_symbol={
                "AAPL": split(600_000, 1_700_000),
                "MSFT": volume_constant(1_000_000),
                "NVDA": volume_constant(950_000),
                "AMZN": volume_constant(870_000),
            },
        ),
        ScenarioSpec(
            name="mean_reverting_volume_decay",
            symbols=("AAPL", "MSFT", "NVDA", "AMZN"),
            returns_by_symbol={
                "AAPL": alternating(0.011, -0.007),
                "MSFT": constant(0.004),
                "NVDA": constant(0.0035),
                "AMZN": constant(-0.001),
            },
            volume_by_symbol={
                "AAPL": volume_trend(1_800_000, 550_000),
                "MSFT": volume_constant(980_000),
                "NVDA": volume_constant(920_000),
                "AMZN": volume_constant(880_000),
            },
        ),
        ScenarioSpec(
            name="narrow_leadership_with_lagging_volume",
            symbols=("AAPL", "MSFT", "NVDA", "AMZN", "META"),
            returns_by_symbol={
                "AAPL": constant(0.007),
                "MSFT": constant(0.0065),
                "NVDA": split(0.012, -0.004),
                "AMZN": constant(0.001),
                "META": constant(-0.0015),
            },
            volume_by_symbol={
                "AAPL": volume_constant(1_100_000),
                "MSFT": volume_constant(1_050_000),
                "NVDA": volume_trend(1_700_000, 650_000),
                "AMZN": volume_constant(870_000),
                "META": volume_constant(860_000),
            },
        ),
        ScenarioSpec(
            name="broad_recovery_with_stable_turnover",
            symbols=("AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOG"),
            returns_by_symbol={
                "AAPL": split(-0.006, 0.008),
                "MSFT": split(-0.004, 0.007),
                "NVDA": split(-0.008, 0.011),
                "AMZN": split(-0.005, 0.006),
                "META": split(-0.003, 0.005),
                "GOOG": split(-0.002, 0.004),
            },
            volume_by_symbol={
                "AAPL": volume_constant(1_050_000),
                "MSFT": volume_constant(1_020_000),
                "NVDA": volume_constant(1_150_000),
                "AMZN": volume_constant(980_000),
                "META": volume_constant(930_000),
                "GOOG": volume_constant(910_000),
            },
        ),
        ScenarioSpec(
            name="flat_breadth_high_vol_spikes",
            symbols=("AAPL", "MSFT", "NVDA", "AMZN"),
            returns_by_symbol={
                "AAPL": constant(0.003),
                "MSFT": constant(0.003),
                "NVDA": alternating(0.025, -0.019),
                "AMZN": alternating(0.018, -0.014),
            },
            volume_by_symbol={
                "AAPL": volume_constant(950_000),
                "MSFT": volume_constant(940_000),
                "NVDA": alternating(2_000_000, 700_000),
                "AMZN": alternating(1_700_000, 680_000),
            },
        ),
    ]


def _resolve_scenarios(scenario_set_name: str) -> list[ScenarioSpec]:
    if scenario_set_name == "default":
        return _default_scenarios()
    if scenario_set_name == "richer_ablation":
        return _richer_ablation_scenarios()
    raise ValueError(f"Unsupported scenario set: {scenario_set_name}")


def _build_feature_fixture(output_dir: Path, scenario: ScenarioSpec) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamps = pd.date_range("2024-01-01", periods=80, freq="D")
    for symbol in scenario.symbols:
        closes = [100.0]
        for daily_return in scenario.returns_by_symbol[symbol]:
            closes.append(closes[-1] * (1.0 + daily_return))
        frame = pd.DataFrame(
            {"timestamp": timestamps, "symbol": [symbol] * len(timestamps), "close": closes}
        )
        if scenario.volume_by_symbol and symbol in scenario.volume_by_symbol:
            volume_values = list(scenario.volume_by_symbol[symbol])
            if len(volume_values) == len(frame) - 1:
                volume_values = [volume_values[0], *volume_values]
            frame["volume"] = volume_values
        frame.to_parquet(output_dir / f"{symbol}.parquet", index=False)
    return output_dir


def _orchestration_config_for_run(
    *,
    base_config_path: Path,
    scenario_name: str,
    run_root: Path,
    research_root: Path,
) -> Any:
    base = load_automated_orchestration_config(base_config_path)
    orchestration_root = run_root / "orchestration_runs"
    return replace(
        base,
        run_name=f"signal_promotion_frequency_{scenario_name}",
        research_artifacts_root=str(research_root),
        output_root_dir=str(orchestration_root),
        validation_output_dir=str(orchestration_root / "validation"),
        strategy_lifecycle_path=str(run_root / "governance" / "strategy_lifecycle.json"),
        paper_state_path=str(run_root / "paper" / f"{scenario_name}_state.json"),
        notes=f"Diagnostic-only repeated promotion-frequency run for scenario {scenario_name}.",
    )


def _bool_stage_succeeded(result: Any, stage_name: str) -> bool:
    for record in result.stage_records:
        if record.stage_name == stage_name:
            return record.status == "succeeded"
    return False


def _first_failed_or_skipped_stage(result: Any) -> str | None:
    for record in result.stage_records:
        if record.status in {"failed", "skipped"} and record.stage_name in {
            "research",
            "registry",
            "validation",
            "promotion",
            "portfolio",
            "paper",
        }:
            return record.stage_name
    return None


def _run_single_scenario(
    base_output_root: Path,
    scenario: ScenarioSpec,
    base_config_path: Path,
    *,
    signal_family: str,
    equity_context_enabled: bool,
    equity_context_include_volume: bool,
) -> dict[str, Any]:
    run_root = base_output_root / scenario.name
    if run_root.exists():
        shutil.rmtree(run_root)
    features_dir = _build_feature_fixture(run_root / "features", scenario)
    research_output_dir = run_root / "research" / "run_alpha_small"
    research_root = run_root / "research"
    registry_dir = run_root / "registry"
    validation_dir = run_root / "validation"
    generated_dir = run_root / "generated_strategies"
    validation_policy = load_strategy_validation_policy_config("configs/strategy_validation_experiment.yaml")
    promotion_policy = load_promotion_policy_config("configs/promotion_experiment.yaml")

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=RuntimeWarning, message="invalid value encountered in divide")
        warnings.filterwarnings("ignore", message="An input array is constant; the correlation coefficient is not defined.")
        run_alpha_research(
            symbols=list(scenario.symbols),
            universe=None,
            feature_dir=features_dir,
            signal_family=signal_family,
            lookbacks=[1, 2],
            horizons=[1],
            min_rows=20,
            top_quantile=0.34,
            bottom_quantile=0.34,
            output_dir=research_output_dir,
            train_size=20,
            test_size=10,
            step_size=10,
            min_train_size=None,
            regime_aware_enabled=True,
            regime_min_history=1,
            equity_context_enabled=equity_context_enabled,
            equity_context_include_volume=equity_context_include_volume,
        )

    build_research_registry(artifacts_root=research_root, output_dir=registry_dir)
    validation_result = build_strategy_validation(
        artifacts_root=research_root,
        output_dir=validation_dir,
        policy=validation_policy,
    )
    candidates_result = build_promotion_candidates(artifacts_root=research_root, output_dir=registry_dir)
    promotion_result = apply_research_promotions(
        artifacts_root=research_root,
        registry_dir=registry_dir,
        validation_path=validation_dir,
        output_dir=generated_dir,
        policy=promotion_policy,
    )

    manifest = json.loads((research_output_dir / "research_run.json").read_text(encoding="utf-8"))
    candidates_payload = json.loads((registry_dir / "promotion_candidates.json").read_text(encoding="utf-8"))
    orchestration_config = _orchestration_config_for_run(
        base_config_path=base_config_path,
        scenario_name=scenario.name,
        run_root=run_root,
        research_root=research_root,
    )
    orchestration_result, orchestration_artifacts = run_automated_orchestration(orchestration_config)

    return {
        "scenario_name": scenario.name,
        "signal_family": signal_family,
        "equity_context_enabled": equity_context_enabled,
        "equity_context_include_volume": equity_context_include_volume,
        "symbol_count": len(scenario.symbols),
        "candidate_count": int(manifest.get("candidate_count") or 0),
        "promoted_signal_count": int(manifest.get("promoted_signal_count") or 0),
        "validation_pass_count": int(validation_result.get("pass_count") or 0),
        "validation_weak_count": int(validation_result.get("weak_count") or 0),
        "validation_fail_count": int(validation_result.get("fail_count") or 0),
        "promotion_candidate_count": int(candidates_result.get("eligible_count") or 0),
        "promoted_strategy_count": int(promotion_result.get("selected_count") or 0),
        "portfolio_stage_reached": _bool_stage_succeeded(orchestration_result, "portfolio"),
        "paper_stage_reached": _bool_stage_succeeded(orchestration_result, "paper"),
        "portfolio_selected_strategy_count": int(orchestration_result.outputs.get("selected_strategy_count") or 0),
        "paper_order_count": int(orchestration_result.outputs.get("paper", {}).get("paper_order_count") or 0),
        "portfolio_sharpe": manifest.get("top_metrics", {}).get("portfolio_sharpe"),
        "promotion_recommendation": manifest.get("promotion_recommendation", {}).get("recommendation"),
        "promotion_reasons": "; ".join(manifest.get("promotion_recommendation", {}).get("reasons", [])),
        "candidate_reason_summary": "; ".join(
            str(row.get("reasons", "")) for row in candidates_payload.get("rows", []) if not bool(row.get("eligible"))
        ),
        "first_drop_stage": _first_failed_or_skipped_stage(orchestration_result),
        "orchestration_run_id": orchestration_result.run_id,
        "orchestration_status": orchestration_result.status,
        "orchestration_run_json_path": str(orchestration_artifacts["orchestration_run_json_path"]),
    }


def _build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total_runs = len(rows)

    def count(predicate: Any) -> int:
        return sum(1 for row in rows if predicate(row))

    summary = {
        "total_runs_attempted": total_runs,
        "runs_with_candidates": count(lambda row: row["candidate_count"] > 0),
        "runs_passing_validation": count(lambda row: row["validation_pass_count"] > 0),
        "runs_with_promotion_candidates": count(lambda row: row["promotion_candidate_count"] > 0),
        "runs_with_promoted_strategies": count(lambda row: row["promoted_strategy_count"] > 0),
        "runs_reaching_portfolio_stage": count(lambda row: bool(row["portfolio_stage_reached"])),
        "runs_reaching_paper_stage": count(lambda row: bool(row["paper_stage_reached"])),
    }
    summary["rates"] = {
        key.replace("runs_", "").replace("total_", ""): (value / total_runs if total_runs else 0.0)
        for key, value in summary.items()
        if key != "rates"
    }
    drop_counts: dict[str, int] = {}
    for row in rows:
        stage = row.get("first_drop_stage") or "none"
        drop_counts[stage] = drop_counts.get(stage, 0) + 1
    summary["drop_stage_counts"] = drop_counts
    if drop_counts:
        summary["most_common_drop_stage"] = max(drop_counts.items(), key=lambda item: item[1])[0]
    else:
        summary["most_common_drop_stage"] = "none"
    return summary


def _render_markdown(
    *,
    output_root: Path,
    rows: list[dict[str, Any]],
    summary: dict[str, Any],
    commands: list[str],
    base_config_path: Path,
    signal_family: str,
    equity_context_enabled: bool,
    equity_context_include_volume: bool,
) -> str:
    lines = [
        "# Signal Promotion Frequency Diagnostic",
        "",
        "## Summary",
        "",
        f"- total runs attempted: `{summary['total_runs_attempted']}`",
        f"- runs with candidates: `{summary['runs_with_candidates']}`",
        f"- runs passing validation: `{summary['runs_passing_validation']}`",
        f"- runs with promotion candidates: `{summary['runs_with_promotion_candidates']}`",
        f"- runs with promoted strategies: `{summary['runs_with_promoted_strategies']}`",
        f"- runs reaching portfolio stage: `{summary['runs_reaching_portfolio_stage']}`",
        f"- runs reaching paper stage: `{summary['runs_reaching_paper_stage']}`",
        "",
        "## Bottleneck Assessment",
        "",
        f"- most common first drop stage: `{summary['most_common_drop_stage']}`",
        f"- drop-stage counts: `{json.dumps(summary['drop_stage_counts'], sort_keys=True)}`",
        "",
        "## Config And Execution Path",
        "",
        f"- base orchestration config reused: `{base_config_path}`",
        f"- output root: `{output_root}`",
        f"- signal family: `{signal_family}`",
        f"- equity context enabled: `{equity_context_enabled}`",
        f"- equity context include volume: `{equity_context_include_volume}`",
        "- repeated run path: feature fixture -> research alpha -> registry -> validation -> promotion -> orchestration",
        "",
        "## Commands Run",
        "",
    ]
    lines.extend([f"- `{command}`" for command in commands])
    lines.extend(
        [
            "",
            "## Per-Run Breakdown",
            "",
            "| run | candidates | validation_pass | promotion_candidates | promoted | portfolio | paper | portfolio_sharpe | promotion_recommendation |",
            "| --- | ---: | ---: | ---: | ---: | --- | --- | ---: | --- |",
        ]
    )
    for row in rows:
        lines.append(
            f"| {row['scenario_name']} | {row['candidate_count']} | {row['validation_pass_count']} | "
            f"{row['promotion_candidate_count']} | {row['promoted_strategy_count']} | "
            f"{'yes' if row['portfolio_stage_reached'] else 'no'} | {'yes' if row['paper_stage_reached'] else 'no'} | "
            f"{row['portfolio_sharpe']} | {row['promotion_recommendation']} |"
        )
    lines.extend(
        [
            "",
            "## Conclusion",
            "",
            (
                "Current research is sufficient for sustained end-to-end experimentation on this lightweight deterministic mini-campaign."
                if summary["runs_reaching_paper_stage"] > 0 and summary["runs_with_promoted_strategies"] >= max(1, summary["total_runs_attempted"] // 2)
                else "Current research is not yet producing promoted strategies frequently enough in this mini-campaign to support sustained end-to-end experimentation."
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def run_promotion_frequency_diagnostic(
    *,
    output_root: Path,
    base_config_path: Path,
    signal_family: str = "momentum",
    equity_context_enabled: bool = False,
    equity_context_include_volume: bool = False,
    artifact_stem: str = "signal_promotion_frequency",
    scenario_set_name: str = "default",
    scenarios: list[ScenarioSpec] | None = None,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    resolved_scenarios = scenarios or _resolve_scenarios(scenario_set_name)
    commands = [
        (
            f"python -m trading_platform.diagnostics.promotion_frequency --output-root {output_root} "
            f"--base-config {base_config_path} --signal-family {signal_family}"
            + (" --equity-context-enabled" if equity_context_enabled else "")
            + (" --equity-context-include-volume" if equity_context_include_volume else "")
            + (f" --scenario-set {scenario_set_name}" if scenario_set_name != "default" else "")
        )
    ]
    rows = [
        _run_single_scenario(
            output_root,
            scenario,
            base_config_path,
            signal_family=signal_family,
            equity_context_enabled=equity_context_enabled,
            equity_context_include_volume=equity_context_include_volume,
        )
        for scenario in resolved_scenarios
    ]
    summary = _build_summary(rows)

    generated_at = datetime.now(UTC).isoformat()
    payload = {
        "schema_version": 1,
        "generated_at": generated_at,
        "base_config_path": str(base_config_path),
        "output_root": str(output_root),
        "signal_family": signal_family,
        "equity_context_enabled": equity_context_enabled,
        "equity_context_include_volume": equity_context_include_volume,
        "scenario_set_name": scenario_set_name,
        "scenario_count": len(resolved_scenarios),
        "scenario_names": [scenario.name for scenario in resolved_scenarios],
        "summary": summary,
        "rows": rows,
        "commands": commands,
    }
    json_path = output_root / f"{artifact_stem}.json"
    csv_path = output_root / f"{artifact_stem}.csv"
    md_path = output_root / f"{artifact_stem}.md"
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    pd.DataFrame(rows).to_csv(csv_path, index=False)
    md_path.write_text(
        _render_markdown(
            output_root=output_root,
            rows=rows,
            summary=summary,
            commands=commands,
            base_config_path=base_config_path,
            signal_family=signal_family,
            equity_context_enabled=equity_context_enabled,
            equity_context_include_volume=equity_context_include_volume,
        ),
        encoding="utf-8",
    )
    return {
        "json_path": str(json_path),
        "csv_path": str(csv_path),
        "md_path": str(md_path),
        "summary": summary,
        "rows": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a lightweight repeated promotion-frequency diagnostic.")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("artifacts/diagnostics/promotion_frequency"),
        help="Directory where diagnostic artifacts will be written.",
    )
    parser.add_argument(
        "--base-config",
        type=Path,
        default=Path("configs/orchestration_signal_promotion_test.yaml"),
        help="Existing orchestration viability config to reuse per scenario.",
    )
    parser.add_argument(
        "--signal-family",
        type=str,
        default="momentum",
        choices=["momentum", "short_term_reversal", "vol_adjusted_momentum", "equity_context_momentum"],
        help="Signal family to evaluate across the repeated diagnostic scenarios.",
    )
    parser.add_argument(
        "--equity-context-enabled",
        action="store_true",
        help="Enable the equity-only context feature expansion inside the alpha research runner.",
    )
    parser.add_argument(
        "--equity-context-include-volume",
        action="store_true",
        help="Include volume-ratio context when the feature inputs contain volume.",
    )
    parser.add_argument(
        "--scenario-set",
        type=str,
        default="default",
        choices=["default", "richer_ablation"],
        help="Built-in deterministic scenario set used to generate feature fixtures for the diagnostic.",
    )
    args = parser.parse_args()
    result = run_promotion_frequency_diagnostic(
        output_root=args.output_root,
        base_config_path=args.base_config,
        signal_family=args.signal_family,
        equity_context_enabled=bool(args.equity_context_enabled),
        equity_context_include_volume=bool(args.equity_context_include_volume),
        scenario_set_name=args.scenario_set,
    )
    print(f"Frequency diagnostic JSON: {result['json_path']}")
    print(f"Frequency diagnostic CSV: {result['csv_path']}")
    print(f"Frequency diagnostic Markdown: {result['md_path']}")
    print(json.dumps(result["summary"], indent=2))


if __name__ == "__main__":
    main()

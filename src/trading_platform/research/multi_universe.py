from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

from trading_platform.research.alpha_lab.runner import run_alpha_research
from trading_platform.research.experiment_tracking import (
    build_alpha_experiment_record,
    register_experiment,
)
from trading_platform.universes.registry import get_universe_symbols


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _safe_read_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


@dataclass(frozen=True)
class MultiUniverseResearchConfig:
    universes: tuple[str, ...]
    feature_dir: Path
    output_dir: Path
    signal_family: str = "momentum"
    lookbacks: tuple[int, ...] = (5, 10, 20, 60)
    horizons: tuple[int, ...] = (1, 5, 20)
    min_rows: int = 126
    top_quantile: float = 0.2
    bottom_quantile: float = 0.2
    train_size: int = 252 * 3
    test_size: int = 63
    step_size: int | None = None
    min_train_size: int | None = None
    portfolio_top_n: int = 10
    portfolio_long_quantile: float = 0.2
    portfolio_short_quantile: float = 0.2
    commission: float = 0.0
    min_price: float | None = None
    min_volume: float | None = None
    min_avg_dollar_volume: float | None = None
    max_adv_participation: float = 0.05
    max_position_pct_of_adv: float = 0.1
    max_notional_per_name: float | None = None
    slippage_bps_per_turnover: float = 0.0
    slippage_bps_per_adv: float = 10.0
    dynamic_recent_quality_window: int = 20
    dynamic_min_history: int = 5
    dynamic_downweight_mean_rank_ic: float = 0.01
    dynamic_deactivate_mean_rank_ic: float = -0.02
    regime_aware_enabled: bool = False
    regime_min_history: int = 5
    regime_underweight_mean_rank_ic: float = 0.01
    regime_exclude_mean_rank_ic: float = -0.01
    experiment_tracker_dir: Path | None = None

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["feature_dir"] = str(self.feature_dir)
        payload["output_dir"] = str(self.output_dir)
        payload["experiment_tracker_dir"] = (
            str(self.experiment_tracker_dir) if self.experiment_tracker_dir else None
        )
        return payload


def _resolve_universe_output_dir(base_dir: Path, universe: str) -> Path:
    return base_dir / universe


def _signal_key(record: dict[str, object]) -> str:
    candidate_id = record.get("candidate_id")
    if candidate_id:
        return str(candidate_id)
    parts = []
    for key in ("signal_family", "lookback", "window", "threshold", "feature_a", "feature_b", "horizon"):
        value = record.get(key)
        if value is None or value == "":
            continue
        if pd.isna(value):
            continue
        parts.append(f"{key}={value}")
    return "|".join(parts)


def _build_overlap_rows(promoted_by_universe: dict[str, pd.DataFrame]) -> pd.DataFrame:
    columns = [
        "universe_a",
        "universe_b",
        "approved_signals_a",
        "approved_signals_b",
        "overlap_count",
        "jaccard_overlap",
        "only_in_a",
        "only_in_b",
    ]
    universes = sorted(promoted_by_universe.keys())
    rows: list[dict[str, object]] = []
    for index, left_universe in enumerate(universes):
        left_keys = {
            _signal_key(record)
            for record in promoted_by_universe[left_universe].to_dict(orient="records")
        }
        for right_universe in universes[index + 1 :]:
            right_keys = {
                _signal_key(record)
                for record in promoted_by_universe[right_universe].to_dict(orient="records")
            }
            overlap = left_keys & right_keys
            union = left_keys | right_keys
            rows.append(
                {
                    "universe_a": left_universe,
                    "universe_b": right_universe,
                    "approved_signals_a": len(left_keys),
                    "approved_signals_b": len(right_keys),
                    "overlap_count": len(overlap),
                    "jaccard_overlap": float(len(overlap) / len(union)) if union else 0.0,
                    "only_in_a": json.dumps(sorted(left_keys - right_keys)),
                    "only_in_b": json.dumps(sorted(right_keys - left_keys)),
                }
            )
    return pd.DataFrame(rows, columns=columns)


def _best_metric_row(metrics_df: pd.DataFrame) -> pd.Series | None:
    if metrics_df.empty:
        return None
    ordered = metrics_df.sort_values(
        ["portfolio_sharpe", "portfolio_total_return"],
        ascending=[False, False],
        na_position="last",
    ).reset_index(drop=True)
    return ordered.iloc[0]


def build_multi_universe_comparison_report(
    *,
    output_dir: Path,
    universe_output_dirs: dict[str, Path] | None = None,
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    if universe_output_dirs is None:
        universe_output_dirs = {
            path.name: path
            for path in output_dir.iterdir()
            if path.is_dir() and (path / "promoted_signals.csv").exists()
        } if output_dir.exists() else {}

    promoted_frames: list[pd.DataFrame] = []
    portfolio_frames: list[pd.DataFrame] = []
    robustness_frames: list[pd.DataFrame] = []
    implementability_frames: list[pd.DataFrame] = []
    universe_summary_rows: list[dict[str, object]] = []
    promoted_by_universe: dict[str, pd.DataFrame] = {}

    for universe, artifact_dir in sorted(universe_output_dirs.items()):
        promoted_df = _safe_read_csv(artifact_dir / "promoted_signals.csv")
        portfolio_df = _safe_read_csv(artifact_dir / "portfolio_metrics.csv")
        robustness_df = _safe_read_csv(artifact_dir / "robustness_report.csv")
        implementability_df = _safe_read_csv(artifact_dir / "implementability_report.csv")
        signal_diagnostics = _safe_read_json(artifact_dir / "signal_diagnostics.json")

        promoted_by_universe[universe] = promoted_df.copy()
        if not promoted_df.empty:
            promoted_frames.append(promoted_df.assign(universe=universe))
        if not portfolio_df.empty:
            portfolio_frames.append(portfolio_df.assign(universe=universe))
        if not robustness_df.empty:
            robustness_frames.append(robustness_df.assign(universe=universe))
        if not implementability_df.empty:
            implementability_frames.append(implementability_df.assign(universe=universe))

        best_row = _best_metric_row(portfolio_df)
        implementability_row = implementability_df.iloc[0] if not implementability_df.empty else None
        universe_summary_rows.append(
            {
                "universe": universe,
                "symbol_count": _universe_symbol_count(universe),
                "promoted_signal_count": int(len(promoted_df)),
                "portfolio_total_return": float(best_row["portfolio_total_return"]) if best_row is not None else float("nan"),
                "portfolio_sharpe": float(best_row["portfolio_sharpe"]) if best_row is not None else float("nan"),
                "implementability_return_drag": float(implementability_row["return_drag"]) if implementability_row is not None and "return_drag" in implementability_row else float("nan"),
                "excluded_names": float(implementability_row["excluded_names"]) if implementability_row is not None and "excluded_names" in implementability_row else float("nan"),
                "regime_aware_enabled": bool(signal_diagnostics.get("regime", {}).get("enabled", False)),
            }
        )

    promoted_by_universe_df = (
        pd.concat(promoted_frames, ignore_index=True)
        if promoted_frames
        else pd.DataFrame(columns=["universe"])
    )
    portfolio_by_universe_df = (
        pd.concat(portfolio_frames, ignore_index=True)
        if portfolio_frames
        else pd.DataFrame(columns=["universe"])
    )
    robustness_by_universe_df = (
        pd.concat(robustness_frames, ignore_index=True)
        if robustness_frames
        else pd.DataFrame(columns=["universe"])
    )
    implementability_by_universe_df = (
        pd.concat(implementability_frames, ignore_index=True)
        if implementability_frames
        else pd.DataFrame(columns=["universe"])
    )
    overlap_df = _build_overlap_rows(promoted_by_universe)
    universe_summary_df = pd.DataFrame(universe_summary_rows)

    universe_signal_counts: dict[str, int] = {}
    for universe, promoted_df in promoted_by_universe.items():
        for signal in promoted_df.to_dict(orient="records"):
            key = _signal_key(signal)
            universe_signal_counts[key] = universe_signal_counts.get(key, 0) + 1
    universe_specific_signals = sorted(
        key for key, count in universe_signal_counts.items() if count == 1
    )

    performance_concentration = False
    liquidity_explains_performance = float("nan")
    if not universe_summary_df.empty and universe_summary_df["portfolio_total_return"].notna().any():
        returns = pd.to_numeric(universe_summary_df["portfolio_total_return"], errors="coerce").fillna(0.0)
        positive_total = float(returns.clip(lower=0.0).sum())
        if positive_total > 0:
            performance_concentration = bool((returns.max() / positive_total) >= 0.7)
    if (
        not universe_summary_df.empty
        and universe_summary_df["portfolio_total_return"].notna().sum() >= 2
        and universe_summary_df["implementability_return_drag"].notna().sum() >= 2
    ):
        returns_series = pd.to_numeric(
            universe_summary_df["portfolio_total_return"],
            errors="coerce",
        )
        drag_series = pd.to_numeric(
            universe_summary_df["implementability_return_drag"],
            errors="coerce",
        )
        if returns_series.nunique(dropna=True) >= 2 and drag_series.nunique(dropna=True) >= 2:
            liquidity_explains_performance = float(returns_series.corr(drag_series))

    comparison_summary_path = output_dir / "cross_universe_comparison_summary.json"
    promoted_by_universe_path = output_dir / "promoted_signals_by_universe.csv"
    portfolio_by_universe_path = output_dir / "composite_portfolio_metrics_by_universe.csv"
    robustness_by_universe_path = output_dir / "robustness_metrics_by_universe.csv"
    implementability_by_universe_path = output_dir / "implementability_metrics_by_universe.csv"
    overlap_path = output_dir / "approved_signal_overlap.csv"
    universe_summary_path = output_dir / "universe_summary.csv"

    promoted_by_universe_df.to_csv(promoted_by_universe_path, index=False)
    portfolio_by_universe_df.to_csv(portfolio_by_universe_path, index=False)
    robustness_by_universe_df.to_csv(robustness_by_universe_path, index=False)
    implementability_by_universe_df.to_csv(implementability_by_universe_path, index=False)
    overlap_df.to_csv(overlap_path, index=False)
    universe_summary_df.to_csv(universe_summary_path, index=False)

    comparison_summary_path.write_text(
        json.dumps(
            {
                "universes_evaluated": sorted(universe_output_dirs.keys()),
                "performance_concentrated_in_one_universe": performance_concentration,
                "universe_specific_signals": universe_specific_signals,
                "liquidity_performance_correlation": liquidity_explains_performance,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    return {
        "promoted_signals_by_universe_path": str(promoted_by_universe_path),
        "composite_portfolio_metrics_by_universe_path": str(portfolio_by_universe_path),
        "robustness_metrics_by_universe_path": str(robustness_by_universe_path),
        "implementability_metrics_by_universe_path": str(implementability_by_universe_path),
        "approved_signal_overlap_path": str(overlap_path),
        "universe_summary_path": str(universe_summary_path),
        "cross_universe_comparison_summary_path": str(comparison_summary_path),
    }


def run_multi_universe_alpha_research(
    *,
    config: MultiUniverseResearchConfig,
) -> dict[str, str]:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    universe_output_dirs: dict[str, Path] = {}
    tracker_dir = config.experiment_tracker_dir
    for universe in config.universes:
        symbols = get_universe_symbols(universe)
        universe_output_dir = _resolve_universe_output_dir(config.output_dir, universe)
        universe_output_dirs[universe] = universe_output_dir
        run_alpha_research(
            symbols=symbols,
            universe=None,
            feature_dir=config.feature_dir,
            signal_family=config.signal_family,
            lookbacks=list(config.lookbacks),
            horizons=list(config.horizons),
            min_rows=config.min_rows,
            top_quantile=config.top_quantile,
            bottom_quantile=config.bottom_quantile,
            output_dir=universe_output_dir,
            train_size=config.train_size,
            test_size=config.test_size,
            step_size=config.step_size,
            min_train_size=config.min_train_size,
            portfolio_top_n=config.portfolio_top_n,
            portfolio_long_quantile=config.portfolio_long_quantile,
            portfolio_short_quantile=config.portfolio_short_quantile,
            commission=config.commission,
            min_price=config.min_price,
            min_volume=config.min_volume,
            min_avg_dollar_volume=config.min_avg_dollar_volume,
            max_adv_participation=config.max_adv_participation,
            max_position_pct_of_adv=config.max_position_pct_of_adv,
            max_notional_per_name=config.max_notional_per_name,
            slippage_bps_per_turnover=config.slippage_bps_per_turnover,
            slippage_bps_per_adv=config.slippage_bps_per_adv,
            dynamic_recent_quality_window=config.dynamic_recent_quality_window,
            dynamic_min_history=config.dynamic_min_history,
            dynamic_downweight_mean_rank_ic=config.dynamic_downweight_mean_rank_ic,
            dynamic_deactivate_mean_rank_ic=config.dynamic_deactivate_mean_rank_ic,
            regime_aware_enabled=config.regime_aware_enabled,
            regime_min_history=config.regime_min_history,
            regime_underweight_mean_rank_ic=config.regime_underweight_mean_rank_ic,
            regime_exclude_mean_rank_ic=config.regime_exclude_mean_rank_ic,
        )
        if tracker_dir is not None:
            register_experiment(
                build_alpha_experiment_record(universe_output_dir),
                tracker_dir=tracker_dir,
            )

    comparison_paths = build_multi_universe_comparison_report(
        output_dir=config.output_dir,
        universe_output_dirs=universe_output_dirs,
    )
    config_path = config.output_dir / "multi_universe_research_config.json"
    config_path.write_text(
        json.dumps(config.to_dict(), indent=2, default=str),
        encoding="utf-8",
    )
    return {
        "multi_universe_output_dir": str(config.output_dir),
        "config_path": str(config_path),
        **comparison_paths,
    }


def _universe_symbol_count(universe: str) -> int:
    try:
        return int(len(get_universe_symbols(universe))) if universe else 0
    except ValueError:
        return 0

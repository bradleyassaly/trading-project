from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.research.alpha_lab.composite import (
    DEFAULT_COMPOSITE_CONFIG,
    build_composite_scores,
    evaluate_composite_scores,
    select_low_redundancy_signals,
)
from trading_platform.research.alpha_lab.composite_portfolio import (
    DEFAULT_COMPOSITE_PORTFOLIO_CONFIG,
    CompositePortfolioConfig,
    build_regime_performance_report,
    build_robustness_report,
    run_composite_portfolio_backtest,
    run_stress_tests,
)
from trading_platform.research.alpha_lab.folds import build_walk_forward_folds
from trading_platform.research.alpha_lab.labels import add_forward_return_labels
from trading_platform.research.alpha_lab.metrics import (
    compute_cross_sectional_daily_metrics,
    evaluate_cross_sectional_signal,
)
from trading_platform.research.alpha_lab.promotion import (
    DEFAULT_PROMOTION_THRESHOLDS,
    apply_promotion_rules,
)
from trading_platform.research.alpha_lab.signals import build_signal


def _load_symbol_feature_data(feature_dir: Path, symbol: str) -> pd.DataFrame:
    parquet_path = feature_dir / f"{symbol}.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(f"Feature file not found for {symbol}: {parquet_path}")

    df = pd.read_parquet(parquet_path)

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp").reset_index(drop=True)

    if "symbol" not in df.columns:
        df["symbol"] = symbol

    return df


def _resolve_symbols(symbols: list[str] | None, universe: str | None) -> list[str]:
    if symbols:
        return sorted(set(symbols))

    if universe:
        universe_path = Path("config/universes") / f"{universe}.txt"
        if universe_path.exists():
            return [line.strip() for line in universe_path.read_text().splitlines() if line.strip()]
        raise ValueError(
            f"Universe '{universe}' was provided, but no resolver is wired yet."
        )

    raise ValueError("Provide either --symbols or --universe.")


def _slice_fold(
    df: pd.DataFrame,
    *,
    test_start: pd.Timestamp,
    test_end: pd.Timestamp,
) -> pd.DataFrame:
    mask = (df["timestamp"] >= test_start) & (df["timestamp"] <= test_end)
    return df.loc[mask].copy()


def _build_shared_folds(
    symbol_data: dict[str, pd.DataFrame],
    *,
    train_size: int,
    test_size: int,
    step_size: int | None,
    min_train_size: int | None,
) -> list:
    if not symbol_data:
        return []

    timestamps = pd.Series(
        sorted(
            {
                timestamp
                for df in symbol_data.values()
                for timestamp in pd.to_datetime(df["timestamp"]).tolist()
            }
        )
    )

    return build_walk_forward_folds(
        timestamps,
        train_size=train_size,
        test_size=test_size,
        step_size=step_size,
        min_train_size=min_train_size,
    )


def _candidate_key(signal_family: str, lookback: int, horizon: int) -> tuple[str, int, int]:
    return signal_family, lookback, horizon


def _safe_series_corr(left: pd.Series, right: pd.Series) -> float:
    joined = pd.concat([left, right], axis=1).dropna()
    if len(joined) < 2:
        return float("nan")

    left_series = joined.iloc[:, 0]
    right_series = joined.iloc[:, 1]
    left_unique = left_series.nunique()
    right_unique = right_series.nunique()
    if left_unique == 1 and right_unique == 1:
        return 1.0 if left_series.iloc[0] == right_series.iloc[0] else float("nan")
    if left_unique == 1 or right_unique == 1:
        return float("nan")

    corr = left_series.corr(right_series)
    if pd.notna(corr):
        return float(corr)

    return float("nan")


def _compute_redundancy_diagnostics(
    leaderboard_df: pd.DataFrame,
    *,
    daily_metrics_by_candidate: dict[tuple[str, int, int], pd.DataFrame],
    score_panel_by_candidate: dict[tuple[str, int, int], pd.DataFrame],
) -> pd.DataFrame:
    columns = [
        "signal_family_a",
        "lookback_a",
        "horizon_a",
        "signal_family_b",
        "lookback_b",
        "horizon_b",
        "overlap_dates",
        "overlap_scores",
        "performance_corr",
        "rank_ic_corr",
        "score_corr",
    ]
    promoted = leaderboard_df.loc[leaderboard_df["promotion_status"] == "promote"].copy()
    if len(promoted) < 2:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, float | int | str]] = []
    for _, left_row in promoted.iterrows():
        for _, right_row in promoted.iterrows():
            left_key = _candidate_key(
                str(left_row["signal_family"]),
                int(left_row["lookback"]),
                int(left_row["horizon"]),
            )
            right_key = _candidate_key(
                str(right_row["signal_family"]),
                int(right_row["lookback"]),
                int(right_row["horizon"]),
            )
            if left_key >= right_key:
                continue

            left_daily = daily_metrics_by_candidate.get(
                left_key,
                pd.DataFrame(columns=["timestamp", "long_short_spread", "spearman_ic"]),
            ).rename(
                columns={
                    "long_short_spread": "long_short_spread_a",
                    "spearman_ic": "spearman_ic_a",
                }
            )
            right_daily = daily_metrics_by_candidate.get(
                right_key,
                pd.DataFrame(columns=["timestamp", "long_short_spread", "spearman_ic"]),
            ).rename(
                columns={
                    "long_short_spread": "long_short_spread_b",
                    "spearman_ic": "spearman_ic_b",
                }
            )
            daily_overlap = left_daily.merge(right_daily, on="timestamp", how="inner")

            left_scores = score_panel_by_candidate.get(
                left_key,
                pd.DataFrame(columns=["timestamp", "symbol", "signal"]),
            ).rename(columns={"signal": "signal_a"})
            right_scores = score_panel_by_candidate.get(
                right_key,
                pd.DataFrame(columns=["timestamp", "symbol", "signal"]),
            ).rename(columns={"signal": "signal_b"})
            score_overlap = left_scores.merge(
                right_scores,
                on=["timestamp", "symbol"],
                how="inner",
            )

            performance_corr = _safe_series_corr(
                daily_overlap["long_short_spread_a"],
                daily_overlap["long_short_spread_b"],
            )
            rank_ic_corr = _safe_series_corr(
                daily_overlap["spearman_ic_a"],
                daily_overlap["spearman_ic_b"],
            )
            score_corr = _safe_series_corr(score_overlap["signal_a"], score_overlap["signal_b"])

            rows.append(
                {
                    "signal_family_a": left_key[0],
                    "lookback_a": left_key[1],
                    "horizon_a": left_key[2],
                    "signal_family_b": right_key[0],
                    "lookback_b": right_key[1],
                    "horizon_b": right_key[2],
                    "overlap_dates": int(len(daily_overlap)),
                    "overlap_scores": int(len(score_overlap)),
                    "performance_corr": performance_corr,
                    "rank_ic_corr": rank_ic_corr,
                    "score_corr": score_corr,
                }
            )

    return pd.DataFrame(rows, columns=columns)


def run_alpha_research(
    *,
    symbols: list[str] | None,
    universe: str | None,
    feature_dir: Path,
    signal_family: str,
    lookbacks: list[int],
    horizons: list[int],
    min_rows: int,
    top_quantile: float,
    bottom_quantile: float,
    output_dir: Path,
    train_size: int = 252 * 3,
    test_size: int = 63,
    step_size: int | None = None,
    min_train_size: int | None = None,
    portfolio_top_n: int = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.top_n,
    portfolio_long_quantile: float = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.long_quantile,
    portfolio_short_quantile: float = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.short_quantile,
    commission: float = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.commission,
    min_price: float | None = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.min_price,
    min_volume: float | None = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.min_volume,
    min_avg_dollar_volume: float | None = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.min_avg_dollar_volume,
    max_adv_participation: float = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.max_adv_participation,
    max_position_pct_of_adv: float = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.max_position_pct_of_adv,
    max_notional_per_name: float | None = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.max_notional_per_name,
    slippage_bps_per_turnover: float = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.slippage_bps_per_turnover,
    slippage_bps_per_adv: float = DEFAULT_COMPOSITE_PORTFOLIO_CONFIG.slippage_bps_per_adv,
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)

    resolved_symbols = _resolve_symbols(symbols, universe)
    symbol_data: dict[str, pd.DataFrame] = {}

    for symbol in resolved_symbols:
        try:
            df = _load_symbol_feature_data(feature_dir, symbol)
        except FileNotFoundError:
            continue

        if len(df) < min_rows:
            continue

        if "timestamp" not in df.columns:
            raise ValueError(f"{symbol} feature data must include a 'timestamp' column.")

        symbol_data[symbol] = add_forward_return_labels(df, horizons=horizons)

    folds = _build_shared_folds(
        symbol_data,
        train_size=train_size,
        test_size=test_size,
        step_size=step_size,
        min_train_size=min_train_size,
    )

    signal_cache: dict[tuple[str, int], pd.Series] = {}
    detailed_rows: list[dict] = []
    daily_metrics_by_candidate: dict[tuple[str, int, int], list[pd.DataFrame]] = {}
    score_panel_by_candidate: dict[tuple[str, int, int], list[pd.DataFrame]] = {}
    label_panel_by_horizon: dict[int, list[pd.DataFrame]] = {}

    for lookback in lookbacks:
        for symbol, df in symbol_data.items():
            signal_cache[(symbol, lookback)] = build_signal(
                df,
                signal_family=signal_family,
                lookback=lookback,
            )

        for horizon in horizons:
            label_col = f"fwd_return_{horizon}d"

            for fold in folds:
                fold_frames: list[pd.DataFrame] = []

                for symbol, df in symbol_data.items():
                    test_df = _slice_fold(
                        df.assign(_signal=signal_cache[(symbol, lookback)]),
                        test_start=fold.test_start,
                        test_end=fold.test_end,
                    )

                    if test_df.empty:
                        continue

                    fold_frames.append(
                        test_df[["timestamp", "symbol", "_signal", label_col]].rename(
                            columns={"_signal": "signal", label_col: "forward_return"}
                        )
                    )

                if not fold_frames:
                    continue

                fold_panel = pd.concat(fold_frames, ignore_index=True)
                candidate_key = _candidate_key(signal_family, lookback, horizon)
                metrics = evaluate_cross_sectional_signal(
                    fold_panel,
                    top_quantile=top_quantile,
                    bottom_quantile=bottom_quantile,
                )
                daily_metrics = compute_cross_sectional_daily_metrics(
                    fold_panel,
                    top_quantile=top_quantile,
                    bottom_quantile=bottom_quantile,
                )

                if not daily_metrics.empty:
                    daily_metrics_by_candidate.setdefault(candidate_key, []).append(daily_metrics)

                score_panel = fold_panel[["timestamp", "symbol", "signal"]].dropna().copy()
                if not score_panel.empty:
                    score_panel_by_candidate.setdefault(candidate_key, []).append(score_panel)
                label_panel = fold_panel[["timestamp", "symbol", "forward_return"]].dropna().copy()
                if not label_panel.empty:
                    label_panel_by_horizon.setdefault(horizon, []).append(label_panel)

                detailed_rows.append(
                    {
                        "signal_family": signal_family,
                        "lookback": lookback,
                        "horizon": horizon,
                        "fold_id": fold.fold_id,
                        "train_start": fold.train_start,
                        "train_end": fold.train_end,
                        "test_start": fold.test_start,
                        "test_end": fold.test_end,
                        **metrics,
                    }
                )

    detailed_columns = [
        "signal_family",
        "lookback",
        "horizon",
        "fold_id",
        "train_start",
        "train_end",
        "test_start",
        "test_end",
        "dates_evaluated",
        "symbols_evaluated",
        "n_obs",
        "pearson_ic",
        "spearman_ic",
        "hit_rate",
        "long_short_spread",
        "quantile_spread",
        "turnover",
    ]
    detailed_df = pd.DataFrame(detailed_rows, columns=detailed_columns)

    if detailed_df.empty:
        leaderboard_df = pd.DataFrame(
            columns=[
                "signal_family",
                "lookback",
                "horizon",
                "symbols_tested",
                "folds_tested",
                "mean_dates_evaluated",
                "mean_pearson_ic",
                "mean_spearman_ic",
                "mean_hit_rate",
                "mean_long_short_spread",
                "mean_quantile_spread",
                "mean_turnover",
                "worst_fold_spearman_ic",
                "total_obs",
                "rejection_reason",
                "promotion_status",
            ]
        )
    else:
        leaderboard_df = (
            detailed_df.groupby(["signal_family", "lookback", "horizon"], as_index=False)
            .agg(
                symbols_tested=("symbols_evaluated", "max"),
                folds_tested=("fold_id", "nunique"),
                mean_dates_evaluated=("dates_evaluated", "mean"),
                mean_pearson_ic=("pearson_ic", "mean"),
                mean_spearman_ic=("spearman_ic", "mean"),
                mean_hit_rate=("hit_rate", "mean"),
                mean_long_short_spread=("long_short_spread", "mean"),
                mean_quantile_spread=("quantile_spread", "mean"),
                mean_turnover=("turnover", "mean"),
                worst_fold_spearman_ic=("spearman_ic", "min"),
                total_obs=("n_obs", "sum"),
            )
            .sort_values(
                ["mean_spearman_ic", "mean_quantile_spread"],
                ascending=[False, False],
            )
            .reset_index(drop=True)
        )
        leaderboard_df = apply_promotion_rules(leaderboard_df)

    combined_daily_metrics = {
        key: pd.concat(frames, ignore_index=True).sort_values("timestamp").reset_index(drop=True)
        for key, frames in daily_metrics_by_candidate.items()
        if frames
    }
    combined_score_panels = {
        key: pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["timestamp", "symbol"])
        .sort_values(["timestamp", "symbol"])
        .reset_index(drop=True)
        for key, frames in score_panel_by_candidate.items()
        if frames
    }
    combined_label_panels = {
        horizon: pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["timestamp", "symbol"])
        .sort_values(["timestamp", "symbol"])
        .reset_index(drop=True)
        for horizon, frames in label_panel_by_horizon.items()
        if frames
    }
    redundancy_df = _compute_redundancy_diagnostics(
        leaderboard_df,
        daily_metrics_by_candidate=combined_daily_metrics,
        score_panel_by_candidate=combined_score_panels,
    )
    promoted_signals_df = leaderboard_df.loc[
        leaderboard_df["promotion_status"] == "promote"
    ].reset_index(drop=True)
    composite_score_frames: list[pd.DataFrame] = []
    composite_diagnostics: dict[str, object] = {
        "config": DEFAULT_COMPOSITE_CONFIG.to_dict(),
        "horizons": {},
    }
    for horizon in sorted(promoted_signals_df["horizon"].unique().tolist()) if not promoted_signals_df.empty else []:
        selected_signals_df, excluded_rows = select_low_redundancy_signals(
            promoted_signals_df,
            redundancy_df,
            horizon=int(horizon),
            redundancy_corr_threshold=DEFAULT_COMPOSITE_CONFIG.redundancy_corr_threshold,
        )
        composite_diagnostics["horizons"][str(int(horizon))] = {
            "selected_signals": selected_signals_df[
                ["signal_family", "lookback", "horizon"]
            ].to_dict(orient="records"),
            "excluded_signals": excluded_rows,
        }
        for weighting_scheme in DEFAULT_COMPOSITE_CONFIG.weighting_schemes:
            composite_scores = build_composite_scores(
                selected_signals_df,
                score_panel_by_candidate=combined_score_panels,
                weighting_scheme=weighting_scheme,
                quality_metric=DEFAULT_COMPOSITE_CONFIG.quality_metric,
            )
            if composite_scores.empty:
                continue
            composite_score_frames.append(composite_scores)

    composite_scores_df = (
        pd.concat(composite_score_frames, ignore_index=True)
        if composite_score_frames
        else pd.DataFrame(
            columns=[
                "timestamp",
                "symbol",
                "horizon",
                "weighting_scheme",
                "composite_score",
                "component_count",
                "selected_signal_count",
            ]
        )
    )
    composite_leaderboard_df = evaluate_composite_scores(
        composite_scores_df,
        label_panel_by_horizon=combined_label_panels,
        folds=folds,
        top_quantile=top_quantile,
        bottom_quantile=bottom_quantile,
    )
    portfolio_config = CompositePortfolioConfig(
        top_n=portfolio_top_n,
        long_quantile=portfolio_long_quantile,
        short_quantile=portfolio_short_quantile,
        commission=commission,
        min_price=min_price,
        min_volume=min_volume,
        min_avg_dollar_volume=min_avg_dollar_volume,
        max_adv_participation=max_adv_participation,
        max_position_pct_of_adv=max_position_pct_of_adv,
        max_notional_per_name=max_notional_per_name,
        slippage_bps_per_turnover=slippage_bps_per_turnover,
        slippage_bps_per_adv=slippage_bps_per_adv,
    )
    (
        composite_portfolio_returns_df,
        composite_portfolio_metrics_df,
        composite_portfolio_weights_df,
        composite_portfolio_diagnostics,
    ) = run_composite_portfolio_backtest(
        composite_scores_df,
        symbol_data=symbol_data,
        config=portfolio_config,
    )
    asset_returns_matrix = (
        composite_portfolio_diagnostics.get("asset_returns_matrix")
        if isinstance(composite_portfolio_diagnostics, dict)
        else None
    )
    robustness_report_df = build_robustness_report(
        composite_portfolio_returns_df,
        composite_portfolio_weights_df,
        folds=folds,
    )
    regime_performance_df = build_regime_performance_report(
        composite_portfolio_returns_df,
        asset_returns=asset_returns_matrix if isinstance(asset_returns_matrix, pd.DataFrame) else pd.DataFrame(),
    )
    stress_test_results_df = run_stress_tests(
        composite_scores_df,
        symbol_data=symbol_data,
        config=portfolio_config,
    )
    implementability_report_df = composite_portfolio_diagnostics.get(
        "implementability_report",
        pd.DataFrame(),
    )
    liquidity_filtered_portfolio_metrics_df = composite_portfolio_diagnostics.get(
        "liquidity_filtered_portfolio_metrics",
        pd.DataFrame(),
    )
    capacity_scenarios_df = composite_portfolio_diagnostics.get(
        "capacity_scenarios",
        pd.DataFrame(),
    )

    detailed_path_csv = output_dir / "fold_results.csv"
    leaderboard_path_csv = output_dir / "leaderboard.csv"
    detailed_path_parquet = output_dir / "fold_results.parquet"
    leaderboard_path_parquet = output_dir / "leaderboard.parquet"
    promoted_signals_path_csv = output_dir / "promoted_signals.csv"
    promoted_signals_path_parquet = output_dir / "promoted_signals.parquet"
    redundancy_report_path_csv = output_dir / "redundancy_report.csv"
    redundancy_report_path_parquet = output_dir / "redundancy_report.parquet"
    redundancy_path_csv = output_dir / "redundancy_diagnostics.csv"
    redundancy_path_parquet = output_dir / "redundancy_diagnostics.parquet"
    composite_scores_path_csv = output_dir / "composite_scores.csv"
    composite_scores_path_parquet = output_dir / "composite_scores.parquet"
    composite_leaderboard_path_csv = output_dir / "composite_leaderboard.csv"
    composite_leaderboard_path_parquet = output_dir / "composite_leaderboard.parquet"
    composite_diagnostics_path = output_dir / "composite_diagnostics.json"
    portfolio_returns_path_csv = output_dir / "portfolio_returns.csv"
    portfolio_returns_path_parquet = output_dir / "portfolio_returns.parquet"
    portfolio_metrics_path_csv = output_dir / "portfolio_metrics.csv"
    portfolio_metrics_path_parquet = output_dir / "portfolio_metrics.parquet"
    portfolio_weights_path_csv = output_dir / "portfolio_weights.csv"
    portfolio_weights_path_parquet = output_dir / "portfolio_weights.parquet"
    portfolio_diagnostics_path = output_dir / "portfolio_diagnostics.json"
    robustness_report_path_csv = output_dir / "robustness_report.csv"
    robustness_report_path_parquet = output_dir / "robustness_report.parquet"
    regime_performance_path_csv = output_dir / "regime_performance.csv"
    regime_performance_path_parquet = output_dir / "regime_performance.parquet"
    stress_test_results_path_csv = output_dir / "stress_test_results.csv"
    stress_test_results_path_parquet = output_dir / "stress_test_results.parquet"
    implementability_report_path_csv = output_dir / "implementability_report.csv"
    implementability_report_path_parquet = output_dir / "implementability_report.parquet"
    liquidity_filtered_metrics_path_csv = output_dir / "liquidity_filtered_portfolio_metrics.csv"
    liquidity_filtered_metrics_path_parquet = output_dir / "liquidity_filtered_portfolio_metrics.parquet"
    capacity_scenarios_path_csv = output_dir / "capacity_scenarios.csv"
    capacity_scenarios_path_parquet = output_dir / "capacity_scenarios.parquet"
    diagnostics_path = output_dir / "signal_diagnostics.json"

    detailed_df.to_csv(detailed_path_csv, index=False)
    leaderboard_df.to_csv(leaderboard_path_csv, index=False)
    promoted_signals_df.to_csv(promoted_signals_path_csv, index=False)
    redundancy_df.to_csv(redundancy_report_path_csv, index=False)
    redundancy_df.to_csv(redundancy_path_csv, index=False)
    composite_scores_df.to_csv(composite_scores_path_csv, index=False)
    composite_leaderboard_df.to_csv(composite_leaderboard_path_csv, index=False)
    composite_portfolio_returns_df.to_csv(portfolio_returns_path_csv, index=False)
    composite_portfolio_metrics_df.to_csv(portfolio_metrics_path_csv, index=False)
    composite_portfolio_weights_df.to_csv(portfolio_weights_path_csv, index=False)
    robustness_report_df.to_csv(robustness_report_path_csv, index=False)
    regime_performance_df.to_csv(regime_performance_path_csv, index=False)
    stress_test_results_df.to_csv(stress_test_results_path_csv, index=False)
    implementability_report_df.to_csv(implementability_report_path_csv, index=False)
    liquidity_filtered_portfolio_metrics_df.to_csv(liquidity_filtered_metrics_path_csv, index=False)
    capacity_scenarios_df.to_csv(capacity_scenarios_path_csv, index=False)
    detailed_df.to_parquet(detailed_path_parquet, index=False)
    leaderboard_df.to_parquet(leaderboard_path_parquet, index=False)
    promoted_signals_df.to_parquet(promoted_signals_path_parquet, index=False)
    redundancy_df.to_parquet(redundancy_report_path_parquet, index=False)
    redundancy_df.to_parquet(redundancy_path_parquet, index=False)
    composite_scores_df.to_parquet(composite_scores_path_parquet, index=False)
    composite_leaderboard_df.to_parquet(composite_leaderboard_path_parquet, index=False)
    composite_portfolio_returns_df.to_parquet(portfolio_returns_path_parquet, index=False)
    composite_portfolio_metrics_df.to_parquet(portfolio_metrics_path_parquet, index=False)
    composite_portfolio_weights_df.to_parquet(portfolio_weights_path_parquet, index=False)
    robustness_report_df.to_parquet(robustness_report_path_parquet, index=False)
    regime_performance_df.to_parquet(regime_performance_path_parquet, index=False)
    stress_test_results_df.to_parquet(stress_test_results_path_parquet, index=False)
    implementability_report_df.to_parquet(implementability_report_path_parquet, index=False)
    liquidity_filtered_portfolio_metrics_df.to_parquet(liquidity_filtered_metrics_path_parquet, index=False)
    capacity_scenarios_df.to_parquet(capacity_scenarios_path_parquet, index=False)
    composite_diagnostics_path.write_text(json.dumps(composite_diagnostics, indent=2, default=str))
    portfolio_diagnostics_payload: dict[str, object] = {}
    for key, value in composite_portfolio_diagnostics.items():
        if key == "asset_returns_matrix":
            continue
        if isinstance(value, pd.DataFrame):
            portfolio_diagnostics_payload[key] = value.to_dict(orient="records")
        else:
            portfolio_diagnostics_payload[key] = value
    portfolio_diagnostics_path.write_text(
        json.dumps(
            portfolio_diagnostics_payload,
            indent=2,
            default=str,
        )
    )

    diagnostics = {
        "symbols_requested": resolved_symbols,
        "signal_family": signal_family,
        "lookbacks": lookbacks,
        "horizons": horizons,
        "min_rows": min_rows,
        "feature_dir": str(feature_dir),
        "evaluation_mode": "cross_sectional_long_short",
        "train_size": train_size,
        "test_size": test_size,
        "step_size": step_size,
        "min_train_size": min_train_size,
        "promotion_rules": DEFAULT_PROMOTION_THRESHOLDS.to_dict(),
        "composite_portfolio": portfolio_config.to_dict(),
    }
    diagnostics_path.write_text(json.dumps(diagnostics, indent=2, default=str))

    return {
        "leaderboard_path": str(leaderboard_path_csv),
        "fold_results_path": str(detailed_path_csv),
        "promoted_signals_path": str(promoted_signals_path_csv),
        "redundancy_report_path": str(redundancy_report_path_csv),
        "redundancy_path": str(redundancy_path_csv),
        "composite_scores_path": str(composite_scores_path_csv),
        "composite_leaderboard_path": str(composite_leaderboard_path_csv),
        "composite_diagnostics_path": str(composite_diagnostics_path),
        "portfolio_returns_path": str(portfolio_returns_path_csv),
        "portfolio_metrics_path": str(portfolio_metrics_path_csv),
        "portfolio_weights_path": str(portfolio_weights_path_csv),
        "portfolio_diagnostics_path": str(portfolio_diagnostics_path),
        "robustness_report_path": str(robustness_report_path_csv),
        "regime_performance_path": str(regime_performance_path_csv),
        "stress_test_results_path": str(stress_test_results_path_csv),
        "implementability_report_path": str(implementability_report_path_csv),
        "liquidity_filtered_portfolio_metrics_path": str(liquidity_filtered_metrics_path_csv),
        "capacity_scenarios_path": str(capacity_scenarios_path_csv),
    }

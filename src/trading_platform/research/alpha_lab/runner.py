from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.research.alpha_lab.folds import build_walk_forward_folds
from trading_platform.research.alpha_lab.labels import add_forward_return_labels
from trading_platform.research.alpha_lab.metrics import evaluate_cross_sectional_signal
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
                metrics = evaluate_cross_sectional_signal(
                    fold_panel,
                    top_quantile=top_quantile,
                    bottom_quantile=bottom_quantile,
                )

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
                "total_obs",
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
                total_obs=("n_obs", "sum"),
            )
            .sort_values(
                ["mean_spearman_ic", "mean_quantile_spread"],
                ascending=[False, False],
            )
            .reset_index(drop=True)
        )

        leaderboard_df["promotion_status"] = (
            (leaderboard_df["mean_spearman_ic"] > 0.02)
            & (leaderboard_df["symbols_tested"] >= 2)
            & (leaderboard_df["total_obs"] >= 100)
        ).map({True: "promote", False: "reject"})

    detailed_path_csv = output_dir / "fold_results.csv"
    leaderboard_path_csv = output_dir / "leaderboard.csv"
    detailed_path_parquet = output_dir / "fold_results.parquet"
    leaderboard_path_parquet = output_dir / "leaderboard.parquet"
    diagnostics_path = output_dir / "signal_diagnostics.json"

    detailed_df.to_csv(detailed_path_csv, index=False)
    leaderboard_df.to_csv(leaderboard_path_csv, index=False)
    detailed_df.to_parquet(detailed_path_parquet, index=False)
    leaderboard_df.to_parquet(leaderboard_path_parquet, index=False)

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
    }
    diagnostics_path.write_text(json.dumps(diagnostics, indent=2, default=str))

    return {
        "leaderboard_path": str(leaderboard_path_csv),
        "fold_results_path": str(detailed_path_csv),
    }

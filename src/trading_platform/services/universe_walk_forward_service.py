from __future__ import annotations

from dataclasses import replace

import pandas as pd

from trading_platform.config.models import (
    BacktestConfig,
    UniverseWalkForwardConfig,
    UniverseWalkForwardResult,
    WalkForwardConfig,
)
from trading_platform.services.portfolio_service import run_portfolio_construction
from trading_platform.services.walk_forward_service import run_walk_forward_evaluation


def _clone_backtest_config_for_symbol(
    base_config: BacktestConfig,
    symbol: str,
) -> BacktestConfig:
    return replace(base_config, symbol=symbol)


def _clone_walk_forward_config_for_symbol(
    base_config: WalkForwardConfig,
    symbol: str,
) -> WalkForwardConfig:
    return replace(base_config, symbol=symbol)

def _extract_placeholder_score_panel(
    fold_results_df: pd.DataFrame,
    symbol: str,
) -> pd.DataFrame:
    """
    Temporary score extraction for the first end-to-end implementation.

    This uses fold-level test metrics as rebalance-date scores.
    The final implementation should replace this with true per-date,
    per-symbol OOS signal values from the test windows.
    """
    if fold_results_df.empty:
        return pd.DataFrame(columns=["timestamp", "symbol", "score"])

    timestamp_col = "test_start" if "test_start" in fold_results_df.columns else None
    if timestamp_col is None:
        raise ValueError("Expected fold results to include 'test_start'.")

    if "test_sharpe_ratio" in fold_results_df.columns:
        score_col = "test_sharpe_ratio"
    elif "test_return_pct" in fold_results_df.columns:
        score_col = "test_return_pct"
    else:
        raise ValueError(
            "Expected fold results to include either 'test_sharpe_ratio' or 'test_return_pct'."
        )

    scores_df = fold_results_df[[timestamp_col, score_col]].copy()
    scores_df = scores_df.rename(
        columns={
            timestamp_col: "timestamp",
            score_col: "score",
        }
    )
    scores_df["symbol"] = symbol

    output_cols = ["timestamp", "symbol", "score"]
    for col in ("fold", "selected_fast", "selected_slow", "selected_lookback"):
        if col in fold_results_df.columns:
            scores_df[col] = fold_results_df[col].values
            output_cols.append(col)

    return scores_df[output_cols]


def _extract_placeholder_forward_returns(
    fold_results_df: pd.DataFrame,
    symbol: str,
) -> pd.DataFrame:
    """
    Temporary realized-return extraction.

    First-pass implementation maps test_return_pct to a rebalance-date forward_return.
    Final implementation should use actual per-symbol realized forward returns aligned
    to each OOS rebalance timestamp.
    """
    if fold_results_df.empty:
        return pd.DataFrame(columns=["timestamp", "symbol", "forward_return"])

    if "test_start" not in fold_results_df.columns or "test_return_pct" not in fold_results_df.columns:
        raise ValueError(
            "Expected fold results to include 'test_start' and 'test_return_pct'."
        )

    returns_df = fold_results_df[["test_start", "test_return_pct"]].copy()
    returns_df = returns_df.rename(
        columns={
            "test_start": "timestamp",
            "test_return_pct": "forward_return",
        }
    )
    returns_df["symbol"] = symbol
    returns_df["forward_return"] = pd.to_numeric(
        returns_df["forward_return"],
        errors="coerce",
    ).fillna(0.0) / 100.0

    return returns_df[["timestamp", "symbol", "forward_return"]]


def _summarize_universe_results(
    fold_results_df: pd.DataFrame,
    oos_scores_df: pd.DataFrame,
    portfolio_summary: dict,
    config: UniverseWalkForwardConfig,
) -> dict:
    summary = {
        "n_symbols": len(config.universe.symbols),
        "symbols": list(config.universe.symbols),
        "n_fold_rows": int(len(fold_results_df)),
        "n_score_rows": int(len(oos_scores_df)),
        "portfolio_summary": portfolio_summary,
    }

    if "fold" in fold_results_df.columns and not fold_results_df.empty:
        summary["n_folds"] = int(fold_results_df["fold"].nunique())

    for col in ("test_return_pct", "test_sharpe_ratio", "test_max_drawdown_pct"):
        if col in fold_results_df.columns and not fold_results_df.empty:
            values = pd.to_numeric(fold_results_df[col], errors="coerce").dropna()
            if not values.empty:
                summary[f"mean_{col}"] = float(values.mean())

    return summary


def run_universe_walk_forward_research(
    config: UniverseWalkForwardConfig,
) -> UniverseWalkForwardResult:
    all_fold_frames: list[pd.DataFrame] = []
    all_score_frames: list[pd.DataFrame] = []
    all_return_frames: list[pd.DataFrame] = []

    for symbol in config.universe.symbols:
        symbol_backtest_config = _clone_backtest_config_for_symbol(
            config.backtest,
            symbol=symbol,
        )
        symbol_walk_forward_config = _clone_walk_forward_config_for_symbol(
            config.walk_forward,
            symbol=symbol,
        )

        wf_result = run_walk_forward_evaluation(
            feature_path=config.feature_path,
            backtest_config=symbol_backtest_config,
            walk_forward_config=symbol_walk_forward_config,
        )

        symbol_fold_df = wf_result.results_df.copy()
        symbol_fold_df["symbol"] = symbol
        all_fold_frames.append(symbol_fold_df)

        symbol_scores_df = _extract_placeholder_score_panel(
            symbol_fold_df,
            symbol=symbol,
        )
        all_score_frames.append(symbol_scores_df)

        symbol_forward_returns_df = _extract_placeholder_forward_returns(
            symbol_fold_df,
            symbol=symbol,
        )
        all_return_frames.append(symbol_forward_returns_df)

    fold_results_df = (
        pd.concat(all_fold_frames, ignore_index=True)
        if all_fold_frames
        else pd.DataFrame()
    )
    oos_scores_df = (
        pd.concat(all_score_frames, ignore_index=True)
        if all_score_frames
        else pd.DataFrame(columns=["timestamp", "symbol", "score"])
    )
    forward_returns_df = (
        pd.concat(all_return_frames, ignore_index=True)
        if all_return_frames
        else pd.DataFrame(columns=["timestamp", "symbol", "forward_return"])
    )

    portfolio_result = run_portfolio_construction(
        scores_df=oos_scores_df,
        forward_returns_df=forward_returns_df,
        portfolio_config=config.portfolio,
    )

    summary = _summarize_universe_results(
        fold_results_df=fold_results_df,
        oos_scores_df=oos_scores_df,
        portfolio_summary=portfolio_result.summary,
        config=config,
    )

    return UniverseWalkForwardResult(
        fold_results_df=fold_results_df,
        oos_scores_df=oos_scores_df,
        portfolio_result=portfolio_result,
        summary=summary,
    )
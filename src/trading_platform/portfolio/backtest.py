from __future__ import annotations

import pandas as pd

from trading_platform.execution.models import ExecutionConfig
from trading_platform.execution.service import estimate_backtest_transaction_cost_bps


def backtest_portfolio_from_weights(
    weights_df: pd.DataFrame,
    forward_returns_df: pd.DataFrame,
    transaction_cost_bps: float = 0.0,
    execution_config: ExecutionConfig | None = None,
) -> pd.DataFrame:
    """
    Minimal portfolio backtest from target weights and realized forward returns.

    weights_df columns:
        timestamp, symbol, weight

    forward_returns_df columns:
        timestamp, symbol, forward_return
    """
    if weights_df.empty:
        return pd.DataFrame(
            columns=["timestamp", "gross_return", "turnover", "cost", "net_return"]
        )

    required_weight_cols = {"timestamp", "symbol", "weight"}
    weight_missing = required_weight_cols - set(weights_df.columns)
    if weight_missing:
        raise ValueError(f"weights_df missing required columns: {sorted(weight_missing)}")

    required_return_cols = {"timestamp", "symbol", "forward_return"}
    return_missing = required_return_cols - set(forward_returns_df.columns)
    if return_missing:
        raise ValueError(
            f"forward_returns_df missing required columns: {sorted(return_missing)}"
        )

    merged = weights_df.merge(
        forward_returns_df[["timestamp", "symbol", "forward_return"]],
        on=["timestamp", "symbol"],
        how="left",
    )
    merged["forward_return"] = pd.to_numeric(
        merged["forward_return"],
        errors="coerce",
    ).fillna(0.0)

    gross_returns_df = (
        merged.assign(weighted_return=merged["weight"] * merged["forward_return"])
        .groupby("timestamp", as_index=False)["weighted_return"]
        .sum()
        .rename(columns={"weighted_return": "gross_return"})
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    turnover_records: list[dict] = []
    prev_weights: pd.Series | None = None

    for timestamp, group in weights_df.groupby("timestamp", sort=True):
        current_weights = group.set_index("symbol")["weight"].sort_index()

        if prev_weights is None:
            turnover = current_weights.abs().sum()
        else:
            aligned = pd.concat(
                [prev_weights.rename("prev"), current_weights.rename("curr")],
                axis=1,
            ).fillna(0.0)
            turnover = (aligned["curr"] - aligned["prev"]).abs().sum()

        turnover_records.append(
            {
                "timestamp": timestamp,
                "turnover": float(turnover),
            }
        )
        prev_weights = current_weights

    turnover_df = pd.DataFrame(turnover_records)

    result = gross_returns_df.merge(turnover_df, on="timestamp", how="left")
    result["turnover"] = result["turnover"].fillna(0.0)
    effective_transaction_cost_bps = (
        estimate_backtest_transaction_cost_bps(execution_config)
        if execution_config is not None
        else transaction_cost_bps
    )
    result["cost"] = result["turnover"] * effective_transaction_cost_bps / 10000.0
    result["net_return"] = result["gross_return"] - result["cost"]
    result["transaction_cost_bps"] = effective_transaction_cost_bps

    return result

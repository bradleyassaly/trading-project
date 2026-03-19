from __future__ import annotations

import pandas as pd


def build_top_n_portfolio_weights(
    scores_df: pd.DataFrame,
    top_n: int,
    max_weight: float | None = None,
) -> pd.DataFrame:
    """
    Build equal-weight top-N portfolios by timestamp from a score panel.

    Expected columns:
        timestamp, symbol, score
    """
    if scores_df.empty:
        return pd.DataFrame(columns=["timestamp", "symbol", "weight"])

    required_cols = {"timestamp", "symbol", "score"}
    missing = required_cols - set(scores_df.columns)
    if missing:
        raise ValueError(f"scores_df missing required columns: {sorted(missing)}")

    if top_n <= 0:
        raise ValueError("top_n must be positive")

    output_frames: list[pd.DataFrame] = []

    for timestamp, group in scores_df.groupby("timestamp", sort=True):
        ranked = group.sort_values(["score", "symbol"], ascending=[False, True]).head(top_n).copy()
        if ranked.empty:
            continue

        ranked["weight"] = 1.0 / len(ranked)

        if max_weight is not None:
            ranked["weight"] = ranked["weight"].clip(upper=max_weight)
            weight_sum = ranked["weight"].sum()
            if weight_sum > 0:
                ranked["weight"] = ranked["weight"] / weight_sum

        output_frames.append(ranked[["timestamp", "symbol", "weight"]])

    if not output_frames:
        return pd.DataFrame(columns=["timestamp", "symbol", "weight"])

    return pd.concat(output_frames, ignore_index=True)
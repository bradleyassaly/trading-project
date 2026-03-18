from __future__ import annotations

import pandas as pd

from trading_platform.construction.constraints import (
    apply_min_score_filter,
    cap_and_redistribute_weights,
)
from trading_platform.construction.group_constraints import (
    enforce_max_group_weight,
    enforce_max_names_per_group,
)
from trading_platform.construction.selectors import select_top_n
from trading_platform.risk.sizing import equal_weight_target_weights, inverse_vol_target_weights


def build_top_n_portfolio_weights(
    scores: pd.DataFrame,
    asset_returns: pd.DataFrame,
    *,
    top_n: int,
    weighting_scheme: str = "equal",
    vol_window: int = 20,
    periods_per_year: int = 252,
    min_score: float | None = None,
    max_weight: float | None = None,
    symbol_groups: pd.Series | None = None,
    max_names_per_group: int | None = None,
    max_group_weight: float | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    selection = select_top_n(scores, n=top_n)

    selection = apply_min_score_filter(
        scores=scores,
        selection=selection,
        min_score=min_score,
    )

    if symbol_groups is not None:
        selection = enforce_max_names_per_group(
            selection=selection,
            scores=scores,
            symbol_groups=symbol_groups,
            max_names_per_group=max_names_per_group,
        )

    if weighting_scheme == "equal":
        target_weights = equal_weight_target_weights(selection)
    elif weighting_scheme == "inverse_vol":
        target_weights = inverse_vol_target_weights(
            selection=selection,
            asset_returns=asset_returns,
            vol_window=vol_window,
            periods_per_year=periods_per_year,
        )
    else:
        raise ValueError(f"Unsupported weighting_scheme: {weighting_scheme}")

    target_weights = cap_and_redistribute_weights(
        target_weights,
        max_weight=max_weight,
    )

    if symbol_groups is not None:
        target_weights = enforce_max_group_weight(
            target_weights,
            symbol_groups=symbol_groups,
            max_group_weight=max_group_weight,
        )

    return selection, target_weights
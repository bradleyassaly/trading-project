from __future__ import annotations

import pandas as pd


def select_top_n(scores: pd.DataFrame, n: int) -> pd.DataFrame:
    if n <= 0:
        raise ValueError(f"n must be positive, got {n}")

    scores = scores.copy()

    def _select_row(row: pd.Series) -> pd.Series:
        valid = row.dropna()
        selected = pd.Series(0.0, index=row.index, dtype=float)

        if valid.empty:
            return selected

        top = valid.nlargest(n).index
        selected.loc[top] = 1.0
        return selected

    return scores.apply(_select_row, axis=1)
from __future__ import annotations

import pandas as pd


def apply_min_score_filter(
    scores: pd.DataFrame,
    selection: pd.DataFrame,
    *,
    min_score: float | None = None,
) -> pd.DataFrame:
    if min_score is None:
        return selection.fillna(0.0).astype(float)

    scores = scores.copy()
    selection = selection.fillna(0.0).astype(float)

    filtered = selection.where(scores >= min_score, other=0.0)
    return filtered.fillna(0.0).astype(float)


def cap_and_redistribute_weights(
    weights: pd.DataFrame,
    *,
    max_weight: float | None = None,
    max_iterations: int = 20,
    tolerance: float = 1e-12,
) -> pd.DataFrame:
    """
    Cap weights row-by-row and redistribute excess proportionally
    across uncapped positive weights.
    """
    if max_weight is None:
        return weights.fillna(0.0).astype(float)

    if max_weight <= 0 or max_weight > 1:
        raise ValueError(f"max_weight must be in (0, 1], got {max_weight}")

    weights = weights.fillna(0.0).astype(float).copy()
    capped_rows: list[pd.Series] = []

    for _, row in weights.iterrows():
        w = row.copy()

        total = w.sum()
        if total > 0:
            w = w / total
        else:
            capped_rows.append(w)
            continue

        for _ in range(max_iterations):
            over = w > max_weight + tolerance
            if not over.any():
                break

            excess = float((w[over] - max_weight).sum())
            w.loc[over] = max_weight

            under = (w > 0) & (w < max_weight - tolerance)
            under_sum = float(w.loc[under].sum())

            if excess <= tolerance or under_sum <= tolerance:
                break

            w.loc[under] = w.loc[under] + excess * (w.loc[under] / under_sum)

        w = w.clip(lower=0.0, upper=max_weight)
        total = w.sum()
        if total > 0:
            w = w / total

        capped_rows.append(w)

    out = pd.DataFrame(capped_rows, index=weights.index, columns=weights.columns)
    return out.fillna(0.0).astype(float)
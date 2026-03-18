from __future__ import annotations

import pandas as pd


def enforce_max_names_per_group(
    selection: pd.DataFrame,
    scores: pd.DataFrame,
    symbol_groups: pd.Series,
    *,
    max_names_per_group: int | None = None,
) -> pd.DataFrame:
    if max_names_per_group is None:
        return selection.fillna(0.0).astype(float)

    if max_names_per_group <= 0:
        raise ValueError(
            f"max_names_per_group must be positive, got {max_names_per_group}"
        )

    selection = selection.fillna(0.0).astype(float)
    scores = scores.copy()
    symbol_groups = symbol_groups.copy()

    constrained_rows: list[pd.Series] = []

    for dt, sel_row in selection.iterrows():
        score_row = scores.loc[dt]
        out = pd.Series(0.0, index=selection.columns, dtype=float)

        selected_symbols = sel_row[sel_row > 0].index.tolist()
        if not selected_symbols:
            constrained_rows.append(out)
            continue

        selected_df = pd.DataFrame(
            {
                "symbol": selected_symbols,
                "score": [score_row.get(sym) for sym in selected_symbols],
                "group": [symbol_groups.get(sym, "UNGROUPED") for sym in selected_symbols],
            }
        )

        selected_df = selected_df.sort_values(
            by=["group", "score", "symbol"],
            ascending=[True, False, True],
            na_position="last",
        )

        kept_symbols: list[str] = []
        for _, grp_df in selected_df.groupby("group", sort=True):
            kept_symbols.extend(grp_df.head(max_names_per_group)["symbol"].tolist())

        out.loc[kept_symbols] = 1.0
        constrained_rows.append(out)

    return pd.DataFrame(
        constrained_rows,
        index=selection.index,
        columns=selection.columns,
    ).fillna(0.0).astype(float)


def enforce_max_group_weight(
    weights: pd.DataFrame,
    symbol_groups: pd.Series,
    *,
    max_group_weight: float | None = None,
    max_iterations: int = 20,
    tolerance: float = 1e-12,
) -> pd.DataFrame:
    if max_group_weight is None:
        return weights.fillna(0.0).astype(float)

    if max_group_weight <= 0 or max_group_weight > 1:
        raise ValueError(
            f"max_group_weight must be in (0, 1], got {max_group_weight}"
        )

    weights = weights.fillna(0.0).astype(float).copy()
    symbol_groups = symbol_groups.copy()

    constrained_rows: list[pd.Series] = []

    for _, row in weights.iterrows():
        w = row.copy()

        total = float(w.sum())
        if total > 0:
            w = w / total
        else:
            constrained_rows.append(w)
            continue

        for _ in range(max_iterations):
            group_weight = w.groupby(symbol_groups).sum()
            violating_groups = group_weight[group_weight > max_group_weight + tolerance]

            if violating_groups.empty:
                break

            excess_total = 0.0

            for group_name, group_total in violating_groups.items():
                group_symbols = symbol_groups[symbol_groups == group_name].index
                group_symbols = [s for s in group_symbols if s in w.index]

                if not group_symbols:
                    continue

                current_group_weights = w.loc[group_symbols]
                if float(current_group_weights.sum()) <= 0:
                    continue

                scale = max_group_weight / float(current_group_weights.sum())
                new_group_weights = current_group_weights * scale
                excess_total += float(current_group_weights.sum() - new_group_weights.sum())
                w.loc[group_symbols] = new_group_weights

            eligible = []
            violating_group_names = set(violating_groups.index.tolist())
            for sym in w.index:
                group_name = symbol_groups.get(sym, "UNGROUPED")
                if group_name not in violating_group_names and w.loc[sym] > 0:
                    eligible.append(sym)

            eligible_sum = float(w.loc[eligible].sum()) if eligible else 0.0
            if excess_total <= tolerance or eligible_sum <= tolerance:
                break

            w.loc[eligible] = w.loc[eligible] + excess_total * (w.loc[eligible] / eligible_sum)

        total = float(w.sum())
        if total > 0:
            w = w / total

        constrained_rows.append(w)

    return pd.DataFrame(
        constrained_rows,
        index=weights.index,
        columns=weights.columns,
    ).fillna(0.0).astype(float)
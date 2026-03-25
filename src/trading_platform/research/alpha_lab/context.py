from __future__ import annotations

import pandas as pd

from trading_platform.research.alpha_lab.metrics import compute_cross_sectional_daily_metrics


SUB_UNIVERSE_COLUMNS = ("sub_universe_id", "sub_universe", "sub_universe_label")
BENCHMARK_CONTEXT_COLUMNS = ("benchmark_context_label", "benchmark_context")


def build_sub_universe_membership_by_symbol_date(
    symbol_data: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    columns = [
        "timestamp",
        "symbol",
        "sub_universe_id",
        "context_source",
        "context_status",
    ]
    rows: list[dict[str, object]] = []
    for symbol, df in symbol_data.items():
        working = df.sort_values("timestamp").copy()
        explicit_column = next((name for name in SUB_UNIVERSE_COLUMNS if name in working.columns), None)
        if explicit_column is not None:
            for _, row in working.loc[working[explicit_column].notna()].iterrows():
                label = str(row.get(explicit_column) or "").strip()
                if not label:
                    continue
                rows.append(
                    {
                        "timestamp": row["timestamp"],
                        "symbol": symbol,
                        "sub_universe_id": label,
                        "context_source": explicit_column,
                        "context_status": "confirmed",
                    }
                )
            continue
        boolean_columns = [name for name in working.columns if name.startswith("sub_universe_")]
        for column in boolean_columns:
            label = column.removeprefix("sub_universe_") or column
            active = working.loc[pd.to_numeric(working[column], errors="coerce").fillna(0.0) > 0.0, ["timestamp"]].copy()
            for _, row in active.iterrows():
                rows.append(
                    {
                        "timestamp": row["timestamp"],
                        "symbol": symbol,
                        "sub_universe_id": label,
                        "context_source": column,
                        "context_status": "confirmed",
                    }
                )
    return pd.DataFrame(rows, columns=columns)


def build_benchmark_context_by_symbol_date(
    symbol_data: dict[str, pd.DataFrame],
    *,
    lookbacks: list[int],
) -> dict[int, pd.DataFrame]:
    columns = [
        "timestamp",
        "symbol",
        "benchmark_context_label",
        "relative_return",
        "market_return",
        "breadth_impulse",
        "context_source",
        "context_status",
    ]
    results: dict[int, pd.DataFrame] = {}
    for lookback in sorted(set(lookbacks)):
        rows: list[dict[str, object]] = []
        for symbol, df in symbol_data.items():
            working = df.sort_values("timestamp").copy()
            explicit_column = next((name for name in BENCHMARK_CONTEXT_COLUMNS if name in working.columns), None)
            if explicit_column is not None:
                for _, row in working.loc[working[explicit_column].notna()].iterrows():
                    label = str(row.get(explicit_column) or "").strip()
                    if not label:
                        continue
                    rows.append(
                        {
                            "timestamp": row["timestamp"],
                            "symbol": symbol,
                            "benchmark_context_label": label,
                            "relative_return": row.get(f"relative_return_{lookback}"),
                            "market_return": row.get(f"market_return_{lookback}"),
                            "breadth_impulse": row.get(f"breadth_impulse_{lookback}"),
                            "context_source": explicit_column,
                            "context_status": "confirmed",
                        }
                    )
                continue

            relative_col = f"relative_return_{lookback}"
            market_col = f"market_return_{lookback}"
            breadth_col = f"breadth_impulse_{lookback}"
            if relative_col not in working.columns or market_col not in working.columns:
                continue
            relative = pd.to_numeric(working[relative_col], errors="coerce")
            market = pd.to_numeric(working[market_col], errors="coerce")
            breadth = pd.to_numeric(working[breadth_col], errors="coerce") if breadth_col in working.columns else pd.Series(index=working.index, dtype=float)
            for index, row in working.iterrows():
                relative_value = relative.loc[index]
                market_value = market.loc[index]
                if pd.isna(relative_value) or pd.isna(market_value):
                    continue
                breadth_value = breadth.loc[index] if index in breadth.index else float("nan")
                market_label = "risk_on" if market_value >= 0.0 else "risk_off"
                breadth_label = "broad" if pd.notna(breadth_value) and breadth_value >= 0.0 else "narrow"
                relative_label = "outperform" if relative_value >= 0.0 else "lagging"
                rows.append(
                    {
                        "timestamp": row["timestamp"],
                        "symbol": symbol,
                        "benchmark_context_label": f"{market_label}_{relative_label}_{breadth_label}",
                        "relative_return": relative_value,
                        "market_return": market_value,
                        "breadth_impulse": breadth_value,
                        "context_source": "equity_context_features",
                        "context_status": "derived",
                    }
                )
        results[lookback] = pd.DataFrame(rows, columns=columns)
    return results


def compute_signal_performance_by_sub_universe(
    selected_signals_df: pd.DataFrame,
    *,
    candidate_panels_by_candidate: dict[tuple[str, int, int], pd.DataFrame],
    sub_universe_membership_df: pd.DataFrame,
    horizon: int,
    top_quantile: float,
    bottom_quantile: float,
) -> pd.DataFrame:
    columns = [
        "candidate_id",
        "signal_family",
        "lookback",
        "horizon",
        "sub_universe_id",
        "dates_evaluated",
        "sample_size",
        "coverage_ratio",
        "mean_spearman_ic",
        "mean_long_short_spread",
        "context_source",
        "context_status",
    ]
    selected = selected_signals_df.loc[selected_signals_df["horizon"] == horizon].copy()
    if selected.empty or sub_universe_membership_df.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, object]] = []
    for _, row in selected.iterrows():
        candidate_key = (str(row["signal_family"]), int(row["lookback"]), int(row["horizon"]))
        panel = candidate_panels_by_candidate.get(candidate_key, pd.DataFrame())
        if panel.empty:
            continue
        merged = panel.merge(sub_universe_membership_df, on=["timestamp", "symbol"], how="inner")
        if merged.empty:
            continue
        overall_dates = max(int(panel["timestamp"].nunique()), 1)
        for (sub_universe_id, context_source, context_status), group in merged.groupby(
            ["sub_universe_id", "context_source", "context_status"],
            sort=False,
        ):
            daily_metrics = compute_cross_sectional_daily_metrics(
                group,
                top_quantile=top_quantile,
                bottom_quantile=bottom_quantile,
            )
            if daily_metrics.empty:
                continue
            rows.append(
                {
                    "candidate_id": f"{candidate_key[0]}|{candidate_key[1]}|{candidate_key[2]}",
                    "signal_family": candidate_key[0],
                    "lookback": candidate_key[1],
                    "horizon": candidate_key[2],
                    "sub_universe_id": sub_universe_id,
                    "dates_evaluated": int(daily_metrics["timestamp"].nunique()),
                    "sample_size": int(len(group)),
                    "coverage_ratio": float(daily_metrics["timestamp"].nunique()) / float(overall_dates),
                    "mean_spearman_ic": float(daily_metrics["spearman_ic"].mean()),
                    "mean_long_short_spread": float(daily_metrics["long_short_spread"].mean()),
                    "context_source": context_source,
                    "context_status": context_status,
                }
            )
    return pd.DataFrame(rows, columns=columns)


def compute_signal_performance_by_benchmark_context(
    selected_signals_df: pd.DataFrame,
    *,
    candidate_panels_by_candidate: dict[tuple[str, int, int], pd.DataFrame],
    benchmark_context_by_lookback: dict[int, pd.DataFrame],
    horizon: int,
    top_quantile: float,
    bottom_quantile: float,
) -> pd.DataFrame:
    columns = [
        "candidate_id",
        "signal_family",
        "lookback",
        "horizon",
        "benchmark_context_label",
        "dates_evaluated",
        "sample_size",
        "coverage_ratio",
        "mean_spearman_ic",
        "mean_long_short_spread",
        "mean_relative_return",
        "mean_market_return",
        "context_source",
        "context_status",
    ]
    selected = selected_signals_df.loc[selected_signals_df["horizon"] == horizon].copy()
    if selected.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, object]] = []
    for _, row in selected.iterrows():
        candidate_key = (str(row["signal_family"]), int(row["lookback"]), int(row["horizon"]))
        panel = candidate_panels_by_candidate.get(candidate_key, pd.DataFrame())
        context_df = benchmark_context_by_lookback.get(int(row["lookback"]), pd.DataFrame())
        if panel.empty or context_df.empty:
            continue
        merged = panel.merge(context_df, on=["timestamp", "symbol"], how="inner")
        if merged.empty:
            continue
        overall_dates = max(int(panel["timestamp"].nunique()), 1)
        for (context_label, context_source, context_status), group in merged.groupby(
            ["benchmark_context_label", "context_source", "context_status"],
            sort=False,
        ):
            daily_metrics = compute_cross_sectional_daily_metrics(
                group,
                top_quantile=top_quantile,
                bottom_quantile=bottom_quantile,
            )
            if daily_metrics.empty:
                continue
            rows.append(
                {
                    "candidate_id": f"{candidate_key[0]}|{candidate_key[1]}|{candidate_key[2]}",
                    "signal_family": candidate_key[0],
                    "lookback": candidate_key[1],
                    "horizon": candidate_key[2],
                    "benchmark_context_label": context_label,
                    "dates_evaluated": int(daily_metrics["timestamp"].nunique()),
                    "sample_size": int(len(group)),
                    "coverage_ratio": float(daily_metrics["timestamp"].nunique()) / float(overall_dates),
                    "mean_spearman_ic": float(daily_metrics["spearman_ic"].mean()),
                    "mean_long_short_spread": float(daily_metrics["long_short_spread"].mean()),
                    "mean_relative_return": float(pd.to_numeric(group["relative_return"], errors="coerce").mean()),
                    "mean_market_return": float(pd.to_numeric(group["market_return"], errors="coerce").mean()),
                    "context_source": context_source,
                    "context_status": context_status,
                }
            )
    return pd.DataFrame(rows, columns=columns)

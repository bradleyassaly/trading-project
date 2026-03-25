from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.research.alpha_lab.composite import candidate_id
from trading_platform.research.alpha_lab.metrics import compute_cross_sectional_daily_metrics


SUB_UNIVERSE_COLUMNS = ("sub_universe_id", "sub_universe", "sub_universe_label")
BENCHMARK_CONTEXT_COLUMNS = ("benchmark_context_label", "benchmark_context")


def _normalize_symbol(symbol: str) -> str:
    return str(symbol).strip().upper()


def _derive_benchmark_context_label(
    *,
    relative_value: float,
    market_value: float,
    breadth_value: float | None,
) -> str:
    market_label = "risk_on" if market_value >= 0.0 else "risk_off"
    breadth_label = (
        "broad" if breadth_value is not None and pd.notna(breadth_value) and breadth_value >= 0.0 else "narrow"
    )
    relative_label = "outperform" if relative_value >= 0.0 else "lagging"
    return f"{market_label}_{relative_label}_{breadth_label}"


def _load_context_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        return pd.DataFrame(payload if isinstance(payload, list) else payload.get("rows", []))
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.DataFrame()


def _load_sub_universe_lookup(context_artifact_dir: Path | None) -> dict[str, str]:
    if context_artifact_dir is None or not context_artifact_dir.exists():
        return {}

    candidates = [
        context_artifact_dir / "sub_universe_snapshot.csv",
        context_artifact_dir / "sub_universe_snapshot.parquet",
        context_artifact_dir / "universe_enrichment.csv",
        context_artifact_dir / "universe_enrichment.parquet",
    ]
    for candidate in candidates:
        frame = _load_context_table(candidate)
        if frame.empty or "symbol" not in frame.columns:
            continue
        label_column = next(
            (
                name
                for name in ("sub_universe_id", "sub_universe", "sub_universe_label")
                if name in frame.columns
            ),
            None,
        )
        if label_column is None:
            continue
        subset = frame.loc[frame[label_column].notna(), ["symbol", label_column]].copy()
        if subset.empty:
            continue
        subset["symbol"] = subset["symbol"].astype(str).str.upper()
        subset[label_column] = subset[label_column].astype(str).str.strip()
        subset = subset.loc[subset[label_column] != ""].drop_duplicates(subset=["symbol"], keep="last")
        return dict(zip(subset["symbol"], subset[label_column], strict=False))
    return {}


def enrich_symbol_data_with_explicit_context(
    symbol_data: dict[str, pd.DataFrame],
    *,
    lookbacks: list[int],
    context_artifact_dir: Path | None = None,
) -> tuple[dict[str, pd.DataFrame], dict[str, object]]:
    sub_universe_lookup = _load_sub_universe_lookup(context_artifact_dir)
    enriched: dict[str, pd.DataFrame] = {}
    summary_rows: list[dict[str, object]] = []

    for symbol, frame in symbol_data.items():
        working = frame.sort_values("timestamp").copy()
        symbol_key = _normalize_symbol(symbol)

        explicit_sub_universe_column = next(
            (name for name in SUB_UNIVERSE_COLUMNS if name in working.columns and working[name].notna().any()),
            None,
        )
        sub_universe_source = explicit_sub_universe_column
        sub_universe_status = "confirmed" if explicit_sub_universe_column is not None else "unavailable"
        if explicit_sub_universe_column is None:
            fallback_sub_universe = sub_universe_lookup.get(symbol_key)
            if fallback_sub_universe:
                working["sub_universe_id"] = fallback_sub_universe
                working["sub_universe"] = fallback_sub_universe
                working["sub_universe_label"] = fallback_sub_universe
                sub_universe_source = "context_artifact"
                sub_universe_status = "confirmed"
        working["sub_universe_context_source"] = sub_universe_source
        working["sub_universe_context_status"] = sub_universe_status

        explicit_benchmark_labels = 0
        derived_benchmark_labels = 0
        for lookback in sorted(set(lookbacks)):
            specific_columns = (
                f"benchmark_context_label_{lookback}",
                f"benchmark_context_{lookback}",
            )
            explicit_benchmark_column = next(
                (
                    name
                    for name in specific_columns + BENCHMARK_CONTEXT_COLUMNS
                    if name in working.columns and working[name].notna().any()
                ),
                None,
            )
            if explicit_benchmark_column is not None:
                working[f"benchmark_context_source_{lookback}"] = explicit_benchmark_column
                working[f"benchmark_context_status_{lookback}"] = "confirmed"
                explicit_benchmark_labels += int(working[explicit_benchmark_column].notna().sum())
                if explicit_benchmark_column in specific_columns:
                    explicit_series = working[explicit_benchmark_column]
                else:
                    explicit_series = working[explicit_benchmark_column]
                if f"benchmark_context_label_{lookback}" not in working.columns:
                    working[f"benchmark_context_label_{lookback}"] = explicit_series
                continue

            relative_col = f"relative_return_{lookback}"
            market_col = f"market_return_{lookback}"
            breadth_col = f"breadth_impulse_{lookback}"
            if relative_col not in working.columns or market_col not in working.columns:
                working[f"benchmark_context_source_{lookback}"] = None
                working[f"benchmark_context_status_{lookback}"] = "unavailable"
                continue

            relative = pd.to_numeric(working[relative_col], errors="coerce")
            market = pd.to_numeric(working[market_col], errors="coerce")
            breadth = (
                pd.to_numeric(working[breadth_col], errors="coerce")
                if breadth_col in working.columns
                else pd.Series(index=working.index, dtype=float)
            )
            labels = []
            for index in working.index:
                relative_value = relative.loc[index]
                market_value = market.loc[index]
                if pd.isna(relative_value) or pd.isna(market_value):
                    labels.append(None)
                    continue
                breadth_value = breadth.loc[index] if index in breadth.index else None
                labels.append(
                    _derive_benchmark_context_label(
                        relative_value=float(relative_value),
                        market_value=float(market_value),
                        breadth_value=(float(breadth_value) if pd.notna(breadth_value) else None),
                    )
                )
            working[f"benchmark_context_label_{lookback}"] = labels
            working[f"benchmark_context_source_{lookback}"] = "equity_context_features"
            working[f"benchmark_context_status_{lookback}"] = "derived"
            derived_benchmark_labels += sum(1 for label in labels if label)

        if len(set(lookbacks)) == 1:
            only_lookback = sorted(set(lookbacks))[0]
            if "benchmark_context_label" not in working.columns:
                working["benchmark_context_label"] = working.get(f"benchmark_context_label_{only_lookback}")
            if "benchmark_context" not in working.columns:
                working["benchmark_context"] = working.get(f"benchmark_context_label_{only_lookback}")

        summary_rows.append(
            {
                "symbol": symbol_key,
                "explicit_sub_universe": bool(sub_universe_source and sub_universe_source != "context_artifact"),
                "persisted_sub_universe": bool(
                    next((name for name in SUB_UNIVERSE_COLUMNS if name in working.columns and working[name].notna().any()), None)
                ),
                "sub_universe_context_source": sub_universe_source,
                "sub_universe_context_status": sub_universe_status,
                "explicit_benchmark_label_count": explicit_benchmark_labels,
                "derived_benchmark_label_count": derived_benchmark_labels,
                "available_benchmark_lookbacks": [
                    lookback
                    for lookback in sorted(set(lookbacks))
                    if f"benchmark_context_label_{lookback}" in working.columns
                    and working[f"benchmark_context_label_{lookback}"].notna().any()
                ],
            }
        )
        enriched[symbol] = working

    return enriched, {
        "symbols": summary_rows,
        "context_artifact_dir": str(context_artifact_dir) if context_artifact_dir is not None else None,
        "symbols_with_sub_universe_labels": sum(1 for row in summary_rows if row["persisted_sub_universe"]),
        "symbols_with_explicit_benchmark_labels": sum(
            1 for row in summary_rows if int(row["explicit_benchmark_label_count"]) > 0
        ),
        "symbols_with_derived_benchmark_labels": sum(
            1 for row in summary_rows if int(row["derived_benchmark_label_count"]) > 0
        ),
    }


def write_context_feature_panels(
    symbol_data: dict[str, pd.DataFrame],
    *,
    output_dir: Path,
    coverage_summary: dict[str, object],
) -> dict[str, str]:
    context_dir = output_dir / "context_features"
    context_dir.mkdir(parents=True, exist_ok=True)
    for symbol, frame in symbol_data.items():
        frame.to_parquet(context_dir / f"{symbol}.parquet", index=False)
    summary_path = output_dir / "research_context_coverage.json"
    summary_path.write_text(json.dumps(coverage_summary, indent=2, default=str), encoding="utf-8")
    return {
        "context_features_dir": str(context_dir),
        "research_context_coverage_path": str(summary_path),
    }


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
            explicit_column = next(
                (
                    name
                    for name in (
                        f"benchmark_context_label_{lookback}",
                        f"benchmark_context_{lookback}",
                        *BENCHMARK_CONTEXT_COLUMNS,
                    )
                    if name in working.columns
                ),
                None,
            )
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
                rows.append(
                    {
                        "timestamp": row["timestamp"],
                        "symbol": symbol,
                        "benchmark_context_label": _derive_benchmark_context_label(
                            relative_value=float(relative_value),
                            market_value=float(market_value),
                            breadth_value=(float(breadth_value) if pd.notna(breadth_value) else None),
                        ),
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
    candidate_panels_by_candidate: dict[str, pd.DataFrame],
    sub_universe_membership_df: pd.DataFrame,
    horizon: int,
    top_quantile: float,
    bottom_quantile: float,
) -> pd.DataFrame:
    columns = [
        "candidate_id",
        "signal_family",
        "signal_variant",
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
        signal_candidate_id = str(
            row.get("candidate_id")
            or candidate_id(
                str(row["signal_family"]),
                int(row["lookback"]),
                int(row["horizon"]),
                str(row.get("signal_variant") or "base"),
            )
        )
        panel = candidate_panels_by_candidate.get(signal_candidate_id, pd.DataFrame())
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
                    "candidate_id": signal_candidate_id,
                    "signal_family": str(row["signal_family"]),
                    "signal_variant": str(row.get("signal_variant") or "base"),
                    "lookback": int(row["lookback"]),
                    "horizon": int(row["horizon"]),
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
    candidate_panels_by_candidate: dict[str, pd.DataFrame],
    benchmark_context_by_lookback: dict[int, pd.DataFrame],
    horizon: int,
    top_quantile: float,
    bottom_quantile: float,
) -> pd.DataFrame:
    columns = [
        "candidate_id",
        "signal_family",
        "signal_variant",
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
        signal_candidate_id = str(
            row.get("candidate_id")
            or candidate_id(
                str(row["signal_family"]),
                int(row["lookback"]),
                int(row["horizon"]),
                str(row.get("signal_variant") or "base"),
            )
        )
        panel = candidate_panels_by_candidate.get(signal_candidate_id, pd.DataFrame())
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
                    "candidate_id": signal_candidate_id,
                    "signal_family": str(row["signal_family"]),
                    "signal_variant": str(row.get("signal_variant") or "base"),
                    "lookback": int(row["lookback"]),
                    "horizon": int(row["horizon"]),
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

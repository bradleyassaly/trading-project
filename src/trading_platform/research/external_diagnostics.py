from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.integrations.alphalens_adapter import (
    build_clean_alphalens_factor_data,
    write_alphalens_artifacts,
)
from trading_platform.integrations.quantstats_adapter import write_quantstats_report
from trading_platform.reference.classification_service import build_symbol_group_map
from trading_platform.research.alpha_lab.data_loading import load_symbol_feature_data
from trading_platform.research.alpha_lab.signals import build_signal


def _read_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _parse_variant_parameters(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return dict(value)
    if value in (None, "", "{}"):
        return {}
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _select_alphalens_candidate(leaderboard_df: pd.DataFrame) -> dict[str, Any]:
    if leaderboard_df.empty:
        raise ValueError("leaderboard_df is empty")
    ranked = leaderboard_df.copy()
    ranked["promotion_rank_metric"] = pd.to_numeric(
        ranked.get("runtime_adjusted_mean_spearman_ic", ranked.get("mean_spearman_ic")),
        errors="coerce",
    )
    ranked = ranked.sort_values("promotion_rank_metric", ascending=False).reset_index(drop=True)
    return ranked.iloc[0].to_dict()


def _build_research_factor_inputs(
    *,
    feature_dir: Path,
    symbols: list[str],
    candidate_row: dict[str, Any],
    signal_composition_preset: str,
    enable_context_confirmations: bool | None,
    enable_relative_features: bool | None,
    enable_flow_confirmations: bool | None,
) -> tuple[pd.Series, pd.DataFrame]:
    factor_rows: list[pd.DataFrame] = []
    pricing_frames: list[pd.DataFrame] = []
    for symbol in symbols:
        try:
            feature_df = load_symbol_feature_data(feature_dir, symbol)
        except FileNotFoundError:
            continue
        if feature_df.empty or "timestamp" not in feature_df.columns or "close" not in feature_df.columns:
            continue
        timestamps = pd.to_datetime(feature_df["timestamp"], errors="coerce")
        working = feature_df.copy()
        working["timestamp"] = timestamps
        working = working.dropna(subset=["timestamp"]).sort_values("timestamp")
        signal = build_signal(
            working,
            signal_family=str(candidate_row.get("signal_family") or ""),
            lookback=int(candidate_row.get("lookback") or 0),
            signal_variant=str(candidate_row.get("signal_variant") or "base"),
            variant_params=_parse_variant_parameters(candidate_row.get("variant_parameters_json")),
            signal_composition_preset=signal_composition_preset,
            enable_context_confirmations=enable_context_confirmations,
            enable_relative_features=enable_relative_features,
            enable_flow_confirmations=enable_flow_confirmations,
        )
        factor_rows.append(
            pd.DataFrame(
                {
                    "date": working["timestamp"],
                    "asset": symbol,
                    "factor": pd.to_numeric(signal, errors="coerce"),
                }
            )
        )
        pricing_frames.append(
            working[["timestamp", "close"]].rename(columns={"timestamp": "date", "close": symbol}).set_index("date")
        )
    if not factor_rows:
        raise ValueError("No factor rows could be built for Alphalens diagnostics")
    factor_frame = pd.concat(factor_rows, ignore_index=True).dropna(subset=["date", "asset", "factor"])
    factor_series = factor_frame.set_index(["date", "asset"])["factor"].sort_index()
    pricing_frame = pd.concat(pricing_frames, axis=1).sort_index()
    return factor_series, pricing_frame


def maybe_run_alphalens_diagnostics(
    *,
    enabled: bool,
    feature_dir: Path,
    leaderboard_path: Path,
    output_dir: Path,
    symbols: list[str],
    signal_composition_preset: str,
    enable_context_confirmations: bool | None,
    enable_relative_features: bool | None,
    enable_flow_confirmations: bool | None,
    classification_path: Path | None = None,
    groupby_field: str | None = None,
    package_override=None,
) -> dict[str, str]:
    if not enabled:
        return {}
    leaderboard_df = _read_csv_if_exists(leaderboard_path)
    if leaderboard_df.empty:
        return {}
    candidate_row = _select_alphalens_candidate(leaderboard_df)
    factor_series, pricing_frame = _build_research_factor_inputs(
        feature_dir=feature_dir,
        symbols=symbols,
        candidate_row=candidate_row,
        signal_composition_preset=signal_composition_preset,
        enable_context_confirmations=enable_context_confirmations,
        enable_relative_features=enable_relative_features,
        enable_flow_confirmations=enable_flow_confirmations,
    )
    groupby = None
    if classification_path is not None and groupby_field:
        groupby = build_symbol_group_map(security_master_path=classification_path, level=groupby_field)
    factor_data = build_clean_alphalens_factor_data(
        factor_series=factor_series,
        pricing_frame=pricing_frame,
        groupby=groupby,
        package_override=package_override,
    )
    paths = write_alphalens_artifacts(
        factor_data=factor_data,
        output_dir=output_dir,
        package_override=package_override,
    )
    return {
        "alphalens_factor_data_path": str(paths.factor_data_path),
        "alphalens_ic_summary_path": str(paths.ic_summary_path),
        "alphalens_quantile_returns_path": str(paths.quantile_returns_path),
        "alphalens_turnover_path": str(paths.turnover_path),
        "alphalens_group_summary_path": str(paths.group_summary_path),
        "alphalens_metadata_path": str(paths.metadata_path),
    }


def maybe_run_quantstats_report(
    *,
    enabled: bool,
    returns_csv_path: Path | None,
    output_dir: Path,
    title: str,
    benchmark_col: str | None = None,
    returns_col_candidates: tuple[str, ...] = ("portfolio_return", "strategy_return_net", "return"),
    package_override=None,
) -> dict[str, str]:
    if not enabled or returns_csv_path is None or not returns_csv_path.exists():
        return {}
    frame = pd.read_csv(returns_csv_path)
    if frame.empty:
        return {}
    timestamp_col = "timestamp" if "timestamp" in frame.columns else ("date" if "date" in frame.columns else None)
    if timestamp_col is not None:
        frame[timestamp_col] = pd.to_datetime(frame[timestamp_col], errors="coerce")
        frame = frame.dropna(subset=[timestamp_col]).set_index(timestamp_col)
    returns_col = next((column for column in returns_col_candidates if column in frame.columns), None)
    if returns_col is None:
        return {}
    returns = pd.to_numeric(frame[returns_col], errors="coerce").dropna()
    benchmark = None
    if benchmark_col and benchmark_col in frame.columns:
        benchmark = pd.to_numeric(frame[benchmark_col], errors="coerce").dropna()
    bundle = write_quantstats_report(
        returns=returns,
        benchmark=benchmark,
        output_dir=output_dir,
        title=title,
        package_override=package_override,
    )
    return {
        "quantstats_metrics_path": str(bundle.metrics_json_path),
        "quantstats_summary_path": str(bundle.summary_csv_path),
        "quantstats_tearsheet_path": str(bundle.tearsheet_html_path) if bundle.tearsheet_html_path is not None else "",
    }

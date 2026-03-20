from __future__ import annotations

import json
from argparse import Namespace
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd

from trading_platform.cli.common import resolve_symbols as resolve_cli_symbols
from trading_platform.research.alpha_lab.composite import (
    DEFAULT_COMPOSITE_CONFIG,
    select_low_redundancy_signals,
)
from trading_platform.research.alpha_lab.data_loading import load_symbol_feature_data
from trading_platform.research.alpha_lab.folds import build_walk_forward_folds
from trading_platform.research.alpha_lab.generation import (
    CROSS_SECTIONAL_TRANSFORMS,
    SignalGenerationConfig,
    apply_cross_sectional_transform,
    build_generated_signal,
    generate_candidate_signals,
)
from trading_platform.research.alpha_lab.labels import add_forward_return_labels
from trading_platform.research.alpha_lab.metrics import (
    compute_cross_sectional_daily_metrics,
    evaluate_cross_sectional_signal,
)
from trading_platform.research.alpha_lab.promotion import (
    DEFAULT_PROMOTION_THRESHOLDS,
    apply_promotion_rules,
)
from trading_platform.research.approved_model_state import write_approved_model_state


@dataclass(frozen=True)
class SignalSearchSpace:
    signal_family: str
    lookbacks: tuple[int, ...]
    horizons: tuple[int, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class AutomatedAlphaResearchConfig:
    symbols: list[str] | None
    universe: str | None
    feature_dir: Path
    output_dir: Path
    search_spaces: tuple[SignalSearchSpace, ...] = ()
    generation_config: SignalGenerationConfig | None = None
    min_rows: int = 126
    top_quantile: float = 0.2
    bottom_quantile: float = 0.2
    train_size: int = 252 * 3
    test_size: int = 63
    step_size: int | None = None
    min_train_size: int | None = None
    schedule_frequency: str = "manual"
    force: bool = False
    stale_after_days: int | None = None
    max_iterations: int | None = 1

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["feature_dir"] = str(self.feature_dir)
        payload["output_dir"] = str(self.output_dir)
        return payload


REGISTRY_COLUMNS = [
    "candidate_id",
    "signal_name",
    "signal_family",
    "parameters_json",
    "lookback",
    "window",
    "threshold",
    "feature_a",
    "feature_b",
    "horizon",
    "symbols_tested",
    "folds_tested",
    "mean_dates_evaluated",
    "mean_pearson_ic",
    "mean_spearman_ic",
    "mean_hit_rate",
    "mean_long_short_spread",
    "mean_quantile_spread",
    "mean_turnover",
    "worst_fold_spearman_ic",
    "total_obs",
    "rejection_reason",
    "promotion_status",
    "evaluation_status",
    "last_evaluated_at",
]

HISTORY_COLUMNS = [
    "run_id",
    "candidate_id",
    "signal_name",
    "signal_family",
    "parameters_json",
    "lookback",
    "window",
    "threshold",
    "feature_a",
    "feature_b",
    "horizon",
    "evaluation_status",
    "promotion_status",
    "last_evaluated_at",
]

THRESHOLD_DISTANCE_COLUMNS = [
    "distance_mean_rank_ic",
    "distance_folds_tested",
    "distance_dates_evaluated",
    "distance_turnover_headroom",
    "distance_worst_fold_rank_ic",
    "distance_total_obs",
    "distance_symbols_tested",
]


def _feature_column_sets(symbol_data: dict[str, pd.DataFrame]) -> dict[str, set[str]]:
    return {
        symbol: set(df.columns)
        for symbol, df in symbol_data.items()
    }


def _has_benchmark_inputs(columns: set[str]) -> bool:
    benchmark_columns = {
        "market_return",
        "benchmark_return",
    }
    return bool(benchmark_columns & columns) or any(
        column.startswith("market_return_") or column.startswith("benchmark_return_")
        for column in columns
    )


def _has_sector_inputs(columns: set[str]) -> bool:
    return any(
        any(column.startswith(prefix) for column in columns)
        for prefix in (
            "sector_mean_return_",
            "sector_momentum_",
            "group_momentum_",
            "industry_momentum_",
            "benchmark_momentum_",
            "sector_return_",
            "group_return_",
            "industry_return_",
        )
    ) or bool({"sector", "group", "industry", "sector_mean_return"} & columns)


def _has_volume_inputs(columns: set[str]) -> bool:
    return bool({"volume", "dollar_volume", "avg_dollar_volume_20"} & columns)


def _feature_combo_availability(columns: set[str], config: SignalGenerationConfig) -> tuple[int, int]:
    available_pairs = 0
    for feature_a, feature_b in config.combo_pairs:
        if {feature_a, feature_b}.issubset(columns):
            available_pairs += 1
    return available_pairs, len(config.combo_pairs)


def build_signal_family_summary(
    leaderboard_df: pd.DataFrame,
    *,
    universe: str | None,
) -> pd.DataFrame:
    columns = [
        "universe",
        "signal_family",
        "candidate_count",
        "mean_spearman_ic",
        "top_spearman_ic",
        "promotion_count",
        "rejection_reason_counts",
    ]
    if leaderboard_df.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, object]] = []
    for signal_family, family_df in leaderboard_df.groupby("signal_family", dropna=False):
        rejection_counts: dict[str, int] = {}
        for reason_string in family_df["rejection_reason"].fillna(""):
            if not reason_string or reason_string == "none":
                continue
            for reason in str(reason_string).split(";"):
                rejection_counts[reason] = rejection_counts.get(reason, 0) + 1
        rows.append(
            {
                "universe": universe or "custom",
                "signal_family": signal_family,
                "candidate_count": int(len(family_df)),
                "mean_spearman_ic": float(pd.to_numeric(family_df["mean_spearman_ic"], errors="coerce").mean()),
                "top_spearman_ic": float(pd.to_numeric(family_df["mean_spearman_ic"], errors="coerce").max()),
                "promotion_count": int((family_df["promotion_status"] == "promote").sum()),
                "rejection_reason_counts": json.dumps(rejection_counts, sort_keys=True),
            }
        )
    return pd.DataFrame(rows, columns=columns).sort_values("signal_family").reset_index(drop=True)


def build_skipped_candidates_report(
    generation_config: SignalGenerationConfig | None,
    *,
    feature_columns: set[str],
    universe: str | None,
) -> pd.DataFrame:
    columns = ["universe", "signal_family", "skip_reason", "skipped_candidate_count"]
    if generation_config is None:
        return pd.DataFrame(columns=columns)

    horizon_count = len(generation_config.horizons)
    lookback_count = len(generation_config.lookbacks)
    vol_window_count = len(generation_config.vol_windows)
    rows: list[dict[str, object]] = []

    if "sector_relative_momentum" in generation_config.signal_families and not _has_sector_inputs(feature_columns):
        rows.append(
            {
                "universe": universe or "custom",
                "signal_family": "sector_relative_momentum",
                "skip_reason": "missing_sector_or_group_inputs",
                "skipped_candidate_count": lookback_count * horizon_count,
            }
        )

    if "volume_surprise" in generation_config.signal_families and not _has_volume_inputs(feature_columns):
        rows.append(
            {
                "universe": universe or "custom",
                "signal_family": "volume_surprise",
                "skip_reason": "missing_volume_inputs",
                "skipped_candidate_count": vol_window_count * horizon_count,
            }
        )

    if "interaction_reversal_volume_spike" in generation_config.signal_families and not _has_volume_inputs(feature_columns):
        rows.append(
            {
                "universe": universe or "custom",
                "signal_family": "interaction_reversal_volume_spike",
                "skip_reason": "missing_volume_inputs",
                "skipped_candidate_count": lookback_count * horizon_count,
            }
        )

    if "feature_combo" in generation_config.signal_families:
        for feature_a, feature_b in generation_config.combo_pairs:
            if not {feature_a, feature_b}.issubset(feature_columns):
                rows.append(
                    {
                        "universe": universe or "custom",
                        "signal_family": "feature_combo",
                        "skip_reason": f"missing_combo_inputs:{feature_a},{feature_b}",
                        "skipped_candidate_count": len(generation_config.combo_thresholds) * horizon_count,
                    }
                )

    return pd.DataFrame(rows, columns=columns)


def build_feature_availability_report(
    symbol_data: dict[str, pd.DataFrame],
    *,
    generation_config: SignalGenerationConfig | None,
    universe: str | None,
) -> pd.DataFrame:
    report_columns = [
        "scope",
        "universe",
        "symbols_total",
        "benchmark_data_availability_rate",
        "sector_group_mapping_availability_rate",
        "volume_availability_rate",
        "required_rolling_feature_availability_rate",
        "skipped_candidate_count",
    ]
    if not symbol_data:
        empty_row = {
            "universe": universe or "custom",
            "symbols_total": 0,
            "benchmark_data_availability_rate": 0.0,
            "sector_group_mapping_availability_rate": 0.0,
            "volume_availability_rate": 0.0,
            "required_rolling_feature_availability_rate": 0.0,
            "skipped_candidate_count": 0,
        }
        return pd.DataFrame(
            [
                {"scope": "universe", **empty_row},
                {"scope": "overall", **empty_row},
            ],
            columns=report_columns,
        )

    column_sets = _feature_column_sets(symbol_data)
    symbol_count = len(column_sets)
    benchmark_rate = sum(_has_benchmark_inputs(feature_cols) for feature_cols in column_sets.values()) / symbol_count
    sector_rate = sum(_has_sector_inputs(feature_cols) for feature_cols in column_sets.values()) / symbol_count
    volume_rate = sum(_has_volume_inputs(feature_cols) for feature_cols in column_sets.values()) / symbol_count
    if generation_config is None:
        rolling_rate = 0.0
    else:
        available_pairs = []
        for feature_cols in column_sets.values():
            available_pair_count, total_pair_count = _feature_combo_availability(feature_cols, generation_config)
            available_pairs.append(1.0 if total_pair_count == 0 else available_pair_count / total_pair_count)
        rolling_rate = float(sum(available_pairs) / symbol_count) if available_pairs else 0.0

    aggregate_feature_columns = set().union(*column_sets.values())
    skipped_candidates_report = build_skipped_candidates_report(
        generation_config,
        feature_columns=aggregate_feature_columns,
        universe=universe,
    )
    skipped_candidate_count = int(skipped_candidates_report["skipped_candidate_count"].sum()) if not skipped_candidates_report.empty else 0
    row = {
        "universe": universe or "custom",
        "symbols_total": symbol_count,
        "benchmark_data_availability_rate": benchmark_rate,
        "sector_group_mapping_availability_rate": sector_rate,
        "volume_availability_rate": volume_rate,
        "required_rolling_feature_availability_rate": rolling_rate,
        "skipped_candidate_count": skipped_candidate_count,
    }
    return pd.DataFrame(
        [
            {"scope": "universe", **row},
            {"scope": "overall", **row},
        ],
        columns=report_columns,
    )


def build_fallback_usage_report(
    symbol_data: dict[str, pd.DataFrame],
    *,
    generation_config: SignalGenerationConfig | None,
    universe: str | None,
) -> pd.DataFrame:
    columns = ["universe", "signal_family", "fallback_type", "usage_count", "symbols_impacted"]
    if not symbol_data or generation_config is None:
        return pd.DataFrame(columns=columns)

    column_sets = _feature_column_sets(symbol_data)
    lookback_horizon_count = len(generation_config.lookbacks) * len(generation_config.horizons)
    rows: list[dict[str, object]] = []

    benchmark_missing_symbols = [symbol for symbol, columns in column_sets.items() if not _has_benchmark_inputs(columns)]
    if "market_residual_momentum" in generation_config.signal_families and benchmark_missing_symbols:
        rows.append(
            {
                "universe": universe or "custom",
                "signal_family": "market_residual_momentum",
                "fallback_type": "trailing_mean_proxy",
                "usage_count": len(benchmark_missing_symbols) * lookback_horizon_count,
                "symbols_impacted": len(benchmark_missing_symbols),
            }
        )

    dollar_volume_symbols = [symbol for symbol, columns in column_sets.items() if "dollar_volume" in columns and "volume" not in columns]
    if {"volume_surprise", "interaction_reversal_volume_spike"} & set(generation_config.signal_families) and dollar_volume_symbols:
        for signal_family in sorted({"volume_surprise", "interaction_reversal_volume_spike"} & set(generation_config.signal_families)):
            multiplier = len(generation_config.vol_windows) * len(generation_config.horizons) if signal_family == "volume_surprise" else lookback_horizon_count
            rows.append(
                {
                    "universe": universe or "custom",
                    "signal_family": signal_family,
                    "fallback_type": "dollar_volume_proxy",
                    "usage_count": len(dollar_volume_symbols) * multiplier,
                    "symbols_impacted": len(dollar_volume_symbols),
                }
            )

    skipped_candidates_report = build_skipped_candidates_report(
        generation_config,
        feature_columns=set().union(*column_sets.values()),
        universe=universe,
    )
    if not skipped_candidates_report.empty:
        for _, row in skipped_candidates_report.iterrows():
            rows.append(
                {
                    "universe": row["universe"],
                    "signal_family": row["signal_family"],
                    "fallback_type": f"generation_degraded:{row['skip_reason']}",
                    "usage_count": int(row["skipped_candidate_count"]),
                    "symbols_impacted": 0,
                }
            )

    return pd.DataFrame(rows, columns=columns)


def _add_promotion_threshold_distances(
    df: pd.DataFrame,
    *,
    universe: str | None,
) -> pd.DataFrame:
    if df.empty:
        result = df.copy()
        result["universe"] = pd.Series(dtype="object")
        for column in THRESHOLD_DISTANCE_COLUMNS:
            result[column] = pd.Series(dtype="float64")
        return result

    result = df.copy()
    result["universe"] = universe or "custom"
    result["distance_mean_rank_ic"] = (
        pd.to_numeric(result["mean_spearman_ic"], errors="coerce")
        - DEFAULT_PROMOTION_THRESHOLDS.min_mean_spearman_ic
    )
    result["distance_folds_tested"] = (
        pd.to_numeric(result["folds_tested"], errors="coerce")
        - DEFAULT_PROMOTION_THRESHOLDS.min_folds_tested
    )
    result["distance_dates_evaluated"] = (
        pd.to_numeric(result["mean_dates_evaluated"], errors="coerce")
        - DEFAULT_PROMOTION_THRESHOLDS.min_mean_dates_evaluated
    )
    result["distance_turnover_headroom"] = (
        DEFAULT_PROMOTION_THRESHOLDS.max_mean_turnover
        - pd.to_numeric(result["mean_turnover"], errors="coerce")
    )
    result["distance_worst_fold_rank_ic"] = (
        pd.to_numeric(result["worst_fold_spearman_ic"], errors="coerce")
        - DEFAULT_PROMOTION_THRESHOLDS.min_worst_fold_spearman_ic
    )
    result["distance_total_obs"] = (
        pd.to_numeric(result["total_obs"], errors="coerce")
        - DEFAULT_PROMOTION_THRESHOLDS.min_total_obs
    )
    result["distance_symbols_tested"] = (
        pd.to_numeric(result["symbols_tested"], errors="coerce")
        - DEFAULT_PROMOTION_THRESHOLDS.min_symbols_tested
    )
    return result


def build_top_rejected_signals_report(
    leaderboard_df: pd.DataFrame,
    *,
    universe: str | None,
) -> pd.DataFrame:
    columns = [
        "candidate_id",
        "universe",
        "mean_spearman_ic",
        "folds_tested",
        "mean_turnover",
        "rejection_reason",
        "distance_mean_rank_ic",
        "distance_folds_tested",
        "distance_dates_evaluated",
        "distance_turnover_headroom",
        "distance_worst_fold_rank_ic",
        "distance_total_obs",
        "distance_symbols_tested",
    ]
    if leaderboard_df.empty:
        return pd.DataFrame(columns=columns)

    rejected_df = leaderboard_df.loc[leaderboard_df["promotion_status"] != "promote"].copy()
    if rejected_df.empty:
        return pd.DataFrame(columns=columns)

    rejected_df = _add_promotion_threshold_distances(
        rejected_df,
        universe=universe,
    )
    if "mean_long_short_spread" not in rejected_df.columns:
        rejected_df["mean_long_short_spread"] = pd.Series(dtype="float64")
    rejected_df = rejected_df.sort_values(
        ["mean_spearman_ic", "mean_long_short_spread", "folds_tested"],
        ascending=[False, False, False],
    ).reset_index(drop=True)
    return rejected_df[columns]


def build_near_miss_signals_report(
    leaderboard_df: pd.DataFrame,
    *,
    universe: str | None,
) -> pd.DataFrame:
    columns = [
        "candidate_id",
        "universe",
        "mean_spearman_ic",
        "folds_tested",
        "mean_turnover",
        "rejection_reason",
        *THRESHOLD_DISTANCE_COLUMNS,
        "failing_threshold_count",
        "promotion_gap_score",
    ]
    if leaderboard_df.empty:
        return pd.DataFrame(columns=columns)

    rejected_df = build_top_rejected_signals_report(
        leaderboard_df,
        universe=universe,
    )
    if rejected_df.empty:
        return pd.DataFrame(columns=columns)

    scale_by_column = {
        "distance_mean_rank_ic": max(abs(DEFAULT_PROMOTION_THRESHOLDS.min_mean_spearman_ic), 1e-9),
        "distance_folds_tested": float(DEFAULT_PROMOTION_THRESHOLDS.min_folds_tested),
        "distance_dates_evaluated": DEFAULT_PROMOTION_THRESHOLDS.min_mean_dates_evaluated,
        "distance_turnover_headroom": DEFAULT_PROMOTION_THRESHOLDS.max_mean_turnover,
        "distance_worst_fold_rank_ic": max(abs(DEFAULT_PROMOTION_THRESHOLDS.min_worst_fold_spearman_ic), 1e-9),
        "distance_total_obs": DEFAULT_PROMOTION_THRESHOLDS.min_total_obs,
        "distance_symbols_tested": DEFAULT_PROMOTION_THRESHOLDS.min_symbols_tested,
    }
    near_miss_df = rejected_df.copy()
    negative_distance_mask = near_miss_df[THRESHOLD_DISTANCE_COLUMNS].lt(0.0)
    near_miss_df["failing_threshold_count"] = negative_distance_mask.sum(axis=1)
    normalized_shortfalls = []
    for column in THRESHOLD_DISTANCE_COLUMNS:
        shortfall = (-pd.to_numeric(near_miss_df[column], errors="coerce")).clip(lower=0.0)
        normalized_shortfalls.append(shortfall / scale_by_column[column])
    normalized_shortfall_df = pd.concat(normalized_shortfalls, axis=1)
    near_miss_df["promotion_gap_score"] = normalized_shortfall_df.mean(axis=1)
    near_miss_df = near_miss_df.sort_values(
        ["failing_threshold_count", "promotion_gap_score", "mean_spearman_ic"],
        ascending=[True, True, False],
    ).reset_index(drop=True)
    return near_miss_df[columns]


def build_promotion_threshold_diagnostics(
    leaderboard_df: pd.DataFrame,
    *,
    universe: str | None,
) -> dict[str, object]:
    labeled_df = _add_promotion_threshold_distances(
        leaderboard_df,
        universe=universe,
    )
    mean_spearman_ic = pd.to_numeric(
        labeled_df.get("mean_spearman_ic", pd.Series(dtype="float64")),
        errors="coerce",
    )
    mean_turnover = pd.to_numeric(
        labeled_df.get("mean_turnover", pd.Series(dtype="float64")),
        errors="coerce",
    )
    rejection_reason_counts: dict[str, int] = {}
    for reason_string in labeled_df.get("rejection_reason", pd.Series(dtype="object")).fillna(""):
        if not reason_string or reason_string == "none":
            continue
        for reason in str(reason_string).split(";"):
            rejection_reason_counts[reason] = rejection_reason_counts.get(reason, 0) + 1

    near_miss_df = build_near_miss_signals_report(
        leaderboard_df,
        universe=universe,
    )
    return {
        "universe": universe or "custom",
        "promotion_thresholds": DEFAULT_PROMOTION_THRESHOLDS.to_dict(),
        "candidate_pool_summary": {
            "total_candidates": int(len(labeled_df)),
            "promoted_candidates": int((labeled_df.get("promotion_status", pd.Series(dtype="object")) == "promote").sum()),
            "rejected_candidates": int((labeled_df.get("promotion_status", pd.Series(dtype="object")) != "promote").sum()),
            "near_miss_candidates": int(len(near_miss_df)),
        },
        "mean_rank_ic_distribution": mean_spearman_ic.describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9]).to_dict(),
        "turnover_distribution": mean_turnover.describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9]).to_dict(),
        "rejection_reason_counts": rejection_reason_counts,
    }


def generate_candidate_configs(
    search_spaces: Iterable[SignalSearchSpace] | None = None,
    *,
    generation_config: SignalGenerationConfig | None = None,
    feature_columns: list[str] | None = None,
) -> pd.DataFrame:
    if generation_config is not None:
        return generate_candidate_signals(
            generation_config,
            feature_columns=feature_columns,
        )

    rows: list[dict[str, object]] = []
    for space in search_spaces or ():
        for lookback in space.lookbacks:
            for horizon in space.horizons:
                params = {"lookback": int(lookback)}
                rows.append(
                    {
                        "candidate_id": f"{space.signal_family}|horizon={int(horizon)}|lookback={int(lookback)}",
                        "signal_name": space.signal_family,
                        "signal_family": space.signal_family,
                        "parameters_json": json.dumps(params, sort_keys=True),
                        "lookback": int(lookback),
                        "window": pd.NA,
                        "threshold": pd.NA,
                        "feature_a": pd.NA,
                        "feature_b": pd.NA,
                        "horizon": int(horizon),
                    }
                )
    return pd.DataFrame(rows).drop_duplicates(subset=["candidate_id"]).reset_index(drop=True)


def load_research_registry(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=REGISTRY_COLUMNS)
    return pd.read_csv(path)


def load_research_history(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=HISTORY_COLUMNS)
    return pd.read_csv(path)


def select_untested_candidates(
    candidates_df: pd.DataFrame,
    registry_df: pd.DataFrame,
) -> pd.DataFrame:
    return select_candidates_for_evaluation(
        candidates_df,
        registry_df,
        stale_after_days=None,
    )


def select_candidates_for_evaluation(
    candidates_df: pd.DataFrame,
    registry_df: pd.DataFrame,
    *,
    stale_after_days: int | None,
    now: datetime | None = None,
) -> pd.DataFrame:
    if candidates_df.empty:
        return candidates_df.copy()
    if registry_df.empty or "candidate_id" not in registry_df.columns:
        return candidates_df.copy()

    completed = registry_df.loc[registry_df["evaluation_status"] == "completed"].copy()
    tested = set(completed["candidate_id"].tolist())
    pending_mask = ~candidates_df["candidate_id"].isin(tested)

    if stale_after_days is None:
        return candidates_df.loc[pending_mask].reset_index(drop=True)

    completed["last_evaluated_at"] = pd.to_datetime(
        completed.get("last_evaluated_at"),
        errors="coerce",
        utc=True,
    )
    cutoff = pd.Timestamp(now or datetime.now(UTC)) - pd.Timedelta(days=int(stale_after_days))
    stale_ids = set(
        completed.loc[
            completed["last_evaluated_at"].notna()
            & (completed["last_evaluated_at"] <= cutoff),
            "candidate_id",
        ].tolist()
    )
    return candidates_df.loc[
        pending_mask | candidates_df["candidate_id"].isin(stale_ids)
    ].reset_index(drop=True)


def should_run_scheduled_loop(
    metadata_path: Path,
    *,
    schedule_frequency: str,
    force: bool = False,
    now: datetime | None = None,
) -> bool:
    if force or schedule_frequency == "manual":
        return True
    if not metadata_path.exists():
        return True

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    next_run_at = metadata.get("next_run_at")
    if not next_run_at:
        return True

    current_time = now or datetime.now(UTC)
    return current_time >= datetime.fromisoformat(next_run_at)


def _next_run_at(now: datetime, schedule_frequency: str) -> str | None:
    if schedule_frequency == "manual":
        return None
    if schedule_frequency == "daily":
        return (now + timedelta(days=1)).isoformat()
    if schedule_frequency == "weekly":
        return (now + timedelta(days=7)).isoformat()
    raise ValueError(f"Unsupported schedule frequency: {schedule_frequency}")


def _load_symbol_feature_data(feature_dir: Path, symbol: str) -> pd.DataFrame:
    return load_symbol_feature_data(feature_dir, symbol)


def _resolve_symbols(symbols: list[str] | None, universe: str | None) -> list[str]:
    args = Namespace(symbols=symbols, universe=universe)
    return resolve_cli_symbols(args)


def _build_shared_folds(
    symbol_data: dict[str, pd.DataFrame],
    *,
    train_size: int,
    test_size: int,
    step_size: int | None,
    min_train_size: int | None,
) -> list:
    if not symbol_data:
        return []
    timestamps = pd.Series(
        sorted(
            {
                timestamp
                for df in symbol_data.values()
                for timestamp in pd.to_datetime(df["timestamp"]).tolist()
            }
        )
    )
    return build_walk_forward_folds(
        timestamps,
        train_size=train_size,
        test_size=test_size,
        step_size=step_size,
        min_train_size=min_train_size,
    )


def _slice_fold(
    df: pd.DataFrame,
    *,
    test_start: pd.Timestamp,
    test_end: pd.Timestamp,
) -> pd.DataFrame:
    mask = (df["timestamp"] >= test_start) & (df["timestamp"] <= test_end)
    return df.loc[mask].copy()


def _safe_series_corr(left: pd.Series, right: pd.Series) -> float:
    joined = pd.concat([left, right], axis=1).dropna()
    if len(joined) < 2:
        return float("nan")
    left_series = joined.iloc[:, 0]
    right_series = joined.iloc[:, 1]
    if left_series.nunique() == 1 and right_series.nunique() == 1:
        return 1.0 if left_series.iloc[0] == right_series.iloc[0] else float("nan")
    if left_series.nunique() == 1 or right_series.nunique() == 1:
        return float("nan")
    corr = left_series.corr(right_series)
    return float(corr) if pd.notna(corr) else float("nan")


def _optional_int(value: object) -> int | None:
    if pd.isna(value):
        return None
    return int(value)


def _candidate_dir(base_dir: Path, candidate_key: str) -> Path:
    return base_dir / "candidate_details" / candidate_key.replace("|", "__")


def _persist_candidate_details(
    base_dir: Path,
    *,
    candidate_key: str,
    fold_results_df: pd.DataFrame,
    daily_metrics_df: pd.DataFrame,
    score_panel_df: pd.DataFrame,
) -> None:
    candidate_dir = _candidate_dir(base_dir, candidate_key)
    candidate_dir.mkdir(parents=True, exist_ok=True)
    fold_results_df.to_parquet(candidate_dir / "fold_results.parquet", index=False)
    daily_metrics_df.to_parquet(candidate_dir / "daily_metrics.parquet", index=False)
    score_panel_df.to_parquet(candidate_dir / "score_panel.parquet", index=False)


def _load_candidate_details(
    base_dir: Path,
    candidate_key: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    candidate_dir = _candidate_dir(base_dir, candidate_key)
    daily_metrics_path = candidate_dir / "daily_metrics.parquet"
    score_panel_path = candidate_dir / "score_panel.parquet"
    daily_metrics_df = pd.read_parquet(daily_metrics_path) if daily_metrics_path.exists() else pd.DataFrame()
    score_panel_df = pd.read_parquet(score_panel_path) if score_panel_path.exists() else pd.DataFrame()
    return daily_metrics_df, score_panel_df


def evaluate_candidate_signal(
    candidate_row: pd.Series,
    *,
    symbol_data: dict[str, pd.DataFrame],
    folds: list,
    top_quantile: float,
    bottom_quantile: float,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    signal_name = str(candidate_row["signal_name"])
    signal_family = str(candidate_row["signal_family"])
    lookback = int(candidate_row["lookback"]) if pd.notna(candidate_row.get("lookback")) else pd.NA
    window = int(candidate_row["window"]) if pd.notna(candidate_row.get("window")) else pd.NA
    threshold = float(candidate_row["threshold"]) if pd.notna(candidate_row.get("threshold")) else pd.NA
    feature_a = candidate_row.get("feature_a", pd.NA)
    feature_b = candidate_row.get("feature_b", pd.NA)
    horizon = int(candidate_row["horizon"])
    label_col = f"fwd_return_{horizon}d"

    fold_rows: list[dict[str, object]] = []
    daily_metrics_frames: list[pd.DataFrame] = []
    score_frames: list[pd.DataFrame] = []
    for fold in folds:
        fold_frames: list[pd.DataFrame] = []
        for symbol, df in symbol_data.items():
            signal = build_generated_signal(df, candidate_row)
            test_df = _slice_fold(
                df.assign(_signal=signal),
                test_start=fold.test_start,
                test_end=fold.test_end,
            )
            if test_df.empty:
                continue
            fold_frames.append(
                test_df[["timestamp", "symbol", "_signal", label_col]].rename(
                    columns={"_signal": "signal", label_col: "forward_return"}
                )
            )
        if not fold_frames:
            continue
        fold_panel = pd.concat(fold_frames, ignore_index=True)
        transform_method = CROSS_SECTIONAL_TRANSFORMS.get(signal_name)
        if transform_method is not None:
            fold_panel = apply_cross_sectional_transform(
                fold_panel,
                method=transform_method,
            )
        metrics = evaluate_cross_sectional_signal(
            fold_panel,
            top_quantile=top_quantile,
            bottom_quantile=bottom_quantile,
        )
        daily_metrics = compute_cross_sectional_daily_metrics(
            fold_panel,
            top_quantile=top_quantile,
            bottom_quantile=bottom_quantile,
        )
        if not daily_metrics.empty:
            daily_metrics_frames.append(daily_metrics)
        score_panel = fold_panel[["timestamp", "symbol", "signal"]].dropna().copy()
        if not score_panel.empty:
            score_frames.append(score_panel)
        fold_rows.append(
            {
                "candidate_id": str(candidate_row["candidate_id"]),
                "signal_name": signal_name,
                "signal_family": signal_family,
                "lookback": lookback,
                "window": window,
                "threshold": threshold,
                "feature_a": feature_a,
                "feature_b": feature_b,
                "horizon": horizon,
                "fold_id": fold.fold_id,
                "train_start": fold.train_start,
                "train_end": fold.train_end,
                "test_start": fold.test_start,
                "test_end": fold.test_end,
                **metrics,
            }
        )

    fold_results_df = pd.DataFrame(
        fold_rows,
        columns=[
            "candidate_id",
            "signal_name",
            "signal_family",
            "lookback",
            "window",
            "threshold",
            "feature_a",
            "feature_b",
            "horizon",
            "fold_id",
            "train_start",
            "train_end",
            "test_start",
            "test_end",
            "dates_evaluated",
            "symbols_evaluated",
            "n_obs",
            "pearson_ic",
            "spearman_ic",
            "hit_rate",
            "long_short_spread",
            "quantile_spread",
            "turnover",
        ],
    )
    daily_metrics_df = (
        pd.concat(daily_metrics_frames, ignore_index=True).sort_values("timestamp").reset_index(drop=True)
        if daily_metrics_frames
        else pd.DataFrame(columns=["timestamp", "pearson_ic", "spearman_ic", "hit_rate", "long_short_spread", "quantile_spread"])
    )
    score_panel_df = (
        pd.concat(score_frames, ignore_index=True)
        .drop_duplicates(subset=["timestamp", "symbol"])
        .sort_values(["timestamp", "symbol"])
        .reset_index(drop=True)
        if score_frames
        else pd.DataFrame(columns=["timestamp", "symbol", "signal"])
    )
    if fold_results_df.empty:
        leaderboard_df = pd.DataFrame(columns=REGISTRY_COLUMNS)
    else:
        leaderboard_df = (
            fold_results_df.groupby(
                [
                    "candidate_id",
                    "signal_name",
                    "signal_family",
                    "lookback",
                    "window",
                    "threshold",
                    "feature_a",
                    "feature_b",
                    "horizon",
                ],
                as_index=False,
                dropna=False,
            )
            .agg(
                symbols_tested=("symbols_evaluated", "max"),
                folds_tested=("fold_id", "nunique"),
                mean_dates_evaluated=("dates_evaluated", "mean"),
                mean_pearson_ic=("pearson_ic", "mean"),
                mean_spearman_ic=("spearman_ic", "mean"),
                mean_hit_rate=("hit_rate", "mean"),
                mean_long_short_spread=("long_short_spread", "mean"),
                mean_quantile_spread=("quantile_spread", "mean"),
                mean_turnover=("turnover", "mean"),
                worst_fold_spearman_ic=("spearman_ic", "min"),
                total_obs=("n_obs", "sum"),
            )
            .reset_index(drop=True)
        )
        leaderboard_df = apply_promotion_rules(leaderboard_df)
        leaderboard_df["parameters_json"] = candidate_row["parameters_json"]
        leaderboard_df["evaluation_status"] = "completed"
        leaderboard_df["last_evaluated_at"] = datetime.now(UTC).isoformat()
        leaderboard_df = leaderboard_df[REGISTRY_COLUMNS]
    return fold_results_df, daily_metrics_df, score_panel_df, leaderboard_df


def _compute_redundancy_diagnostics(
    leaderboard_df: pd.DataFrame,
    *,
    daily_metrics_by_candidate: dict[str, pd.DataFrame],
    score_panel_by_candidate: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    columns = [
        "candidate_id_a",
        "signal_family_a",
        "lookback_a",
        "horizon_a",
        "candidate_id_b",
        "signal_family_b",
        "lookback_b",
        "horizon_b",
        "overlap_dates",
        "overlap_scores",
        "performance_corr",
        "rank_ic_corr",
        "score_corr",
    ]
    promoted = leaderboard_df.loc[leaderboard_df["promotion_status"] == "promote"].copy()
    if len(promoted) < 2:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, object]] = []
    for _, left_row in promoted.iterrows():
        for _, right_row in promoted.iterrows():
            left_key = str(left_row["candidate_id"])
            right_key = str(right_row["candidate_id"])
            if left_key >= right_key:
                continue
            left_daily = daily_metrics_by_candidate.get(left_key, pd.DataFrame()).rename(
                columns={
                    "long_short_spread": "long_short_spread_a",
                    "spearman_ic": "spearman_ic_a",
                }
            )
            right_daily = daily_metrics_by_candidate.get(right_key, pd.DataFrame()).rename(
                columns={
                    "long_short_spread": "long_short_spread_b",
                    "spearman_ic": "spearman_ic_b",
                }
            )
            daily_overlap = left_daily.merge(right_daily, on="timestamp", how="inner")
            left_scores = score_panel_by_candidate.get(left_key, pd.DataFrame()).rename(columns={"signal": "signal_a"})
            right_scores = score_panel_by_candidate.get(right_key, pd.DataFrame()).rename(columns={"signal": "signal_b"})
            score_overlap = left_scores.merge(right_scores, on=["timestamp", "symbol"], how="inner")
            rows.append(
                {
                    "candidate_id_a": left_key,
                    "signal_family_a": str(left_row["signal_family"]),
                    "lookback_a": _optional_int(left_row["lookback"]),
                    "horizon_a": int(left_row["horizon"]),
                    "candidate_id_b": right_key,
                    "signal_family_b": str(right_row["signal_family"]),
                    "lookback_b": _optional_int(right_row["lookback"]),
                    "horizon_b": int(right_row["horizon"]),
                    "overlap_dates": int(len(daily_overlap)),
                    "overlap_scores": int(len(score_overlap)),
                    "performance_corr": _safe_series_corr(
                        daily_overlap.get("long_short_spread_a", pd.Series(dtype="float64")),
                        daily_overlap.get("long_short_spread_b", pd.Series(dtype="float64")),
                    ),
                    "rank_ic_corr": _safe_series_corr(
                        daily_overlap.get("spearman_ic_a", pd.Series(dtype="float64")),
                        daily_overlap.get("spearman_ic_b", pd.Series(dtype="float64")),
                    ),
                    "score_corr": _safe_series_corr(
                        score_overlap.get("signal_a", pd.Series(dtype="float64")),
                        score_overlap.get("signal_b", pd.Series(dtype="float64")),
                    ),
                }
            )
    return pd.DataFrame(rows, columns=columns)


def aggregate_research_registry(
    registry_df: pd.DataFrame,
    *,
    output_dir: Path,
    universe: str | None = None,
) -> dict[str, str]:
    leaderboard_df = registry_df.copy()
    if leaderboard_df.empty:
        leaderboard_df = pd.DataFrame(columns=REGISTRY_COLUMNS)
    else:
        promoted = apply_promotion_rules(
            leaderboard_df[
                [
                    "candidate_id",
                    "signal_name",
                    "signal_family",
                    "parameters_json",
                    "lookback",
                    "window",
                    "threshold",
                    "feature_a",
                    "feature_b",
                    "horizon",
                    "symbols_tested",
                    "folds_tested",
                    "mean_dates_evaluated",
                    "mean_pearson_ic",
                    "mean_spearman_ic",
                    "mean_hit_rate",
                    "mean_long_short_spread",
                    "mean_quantile_spread",
                    "mean_turnover",
                    "worst_fold_spearman_ic",
                    "total_obs",
                ]
            ].copy()
        )
        metadata_df = leaderboard_df[
            ["candidate_id", "last_evaluated_at", "evaluation_status"]
        ].drop_duplicates(subset=["candidate_id"], keep="last")
        leaderboard_df = promoted.merge(
            metadata_df,
            on="candidate_id",
            how="left",
        )
        leaderboard_df = leaderboard_df[REGISTRY_COLUMNS]

    daily_metrics_by_candidate: dict[str, pd.DataFrame] = {}
    score_panel_by_candidate: dict[str, pd.DataFrame] = {}
    for candidate_key in leaderboard_df.get("candidate_id", pd.Series(dtype="object")).tolist():
        daily_metrics_df, score_panel_df = _load_candidate_details(output_dir, str(candidate_key))
        if not daily_metrics_df.empty:
            daily_metrics_by_candidate[str(candidate_key)] = daily_metrics_df
        if not score_panel_df.empty:
            score_panel_by_candidate[str(candidate_key)] = score_panel_df

    redundancy_df = _compute_redundancy_diagnostics(
        leaderboard_df,
        daily_metrics_by_candidate=daily_metrics_by_candidate,
        score_panel_by_candidate=score_panel_by_candidate,
    )
    promoted_signals_df = leaderboard_df.loc[leaderboard_df["promotion_status"] == "promote"].reset_index(drop=True)
    top_rejected_signals_df = build_top_rejected_signals_report(
        leaderboard_df,
        universe=universe,
    )
    near_miss_signals_df = build_near_miss_signals_report(
        leaderboard_df,
        universe=universe,
    )
    promotion_threshold_diagnostics = build_promotion_threshold_diagnostics(
        leaderboard_df,
        universe=universe,
    )
    composite_inputs: dict[str, object] = {"horizons": {}}
    for horizon in sorted(promoted_signals_df["horizon"].dropna().unique().tolist()) if not promoted_signals_df.empty else []:
        selected_df, excluded_rows = select_low_redundancy_signals(
            promoted_signals_df,
            redundancy_df,
            horizon=int(horizon),
            redundancy_corr_threshold=DEFAULT_COMPOSITE_CONFIG.redundancy_corr_threshold,
        )
        composite_inputs["horizons"][str(int(horizon))] = {
            "selected_signals": selected_df[
                ["candidate_id", "signal_name", "signal_family", "lookback", "window", "threshold", "feature_a", "feature_b", "horizon"]
            ].to_dict(orient="records"),
            "excluded_signals": excluded_rows,
        }

    leaderboard_path = output_dir / "leaderboard.csv"
    promoted_path = output_dir / "promoted_signals.csv"
    rejected_path = output_dir / "rejected_signals.csv"
    top_rejected_path = output_dir / "top_rejected_signals.csv"
    near_miss_path = output_dir / "near_miss_signals.csv"
    redundancy_path = output_dir / "redundancy_report.csv"
    composite_inputs_path = output_dir / "composite_inputs.json"
    promotion_threshold_diagnostics_path = output_dir / "promotion_threshold_diagnostics.json"
    leaderboard_df.to_csv(leaderboard_path, index=False)
    promoted_signals_df.to_csv(promoted_path, index=False)
    leaderboard_df.loc[leaderboard_df["promotion_status"] != "promote"].to_csv(rejected_path, index=False)
    top_rejected_signals_df.to_csv(top_rejected_path, index=False)
    near_miss_signals_df.to_csv(near_miss_path, index=False)
    redundancy_df.to_csv(redundancy_path, index=False)
    composite_inputs_path.write_text(json.dumps(composite_inputs, indent=2, default=str), encoding="utf-8")
    promotion_threshold_diagnostics_path.write_text(
        json.dumps(promotion_threshold_diagnostics, indent=2, default=str),
        encoding="utf-8",
    )
    approved_model_state_paths = write_approved_model_state(artifact_dir=output_dir)
    return {
        "leaderboard_path": str(leaderboard_path),
        "promoted_signals_path": str(promoted_path),
        "rejected_signals_path": str(rejected_path),
        "top_rejected_signals_path": str(top_rejected_path),
        "near_miss_signals_path": str(near_miss_path),
        "redundancy_report_path": str(redundancy_path),
        "composite_inputs_path": str(composite_inputs_path),
        "promotion_threshold_diagnostics_path": str(promotion_threshold_diagnostics_path),
        **approved_model_state_paths,
    }


def _run_automated_alpha_research_iteration(
    *,
    config: AutomatedAlphaResearchConfig,
) -> dict[str, str]:
    output_dir = config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    registry_path = output_dir / "research_registry.csv"
    signal_registry_path = output_dir / "signal_registry.csv"
    history_path = output_dir / "research_history.csv"
    schedule_path = output_dir / "research_schedule.json"

    if not should_run_scheduled_loop(
        schedule_path,
        schedule_frequency=config.schedule_frequency,
        force=config.force,
    ):
        return {
            "status": "skipped",
            "registry_path": str(registry_path),
            "history_path": str(history_path),
            "schedule_path": str(schedule_path),
        }

    resolved_symbols = _resolve_symbols(config.symbols, config.universe)
    feature_columns: list[str] = []
    candidate_feature_columns: list[str] | None = None
    for symbol in resolved_symbols:
        try:
            df = _load_symbol_feature_data(config.feature_dir, symbol)
        except FileNotFoundError:
            continue
        feature_columns = [column for column in df.columns if column not in {"timestamp", "symbol"}]
        candidate_feature_columns = feature_columns
        break

    candidates_df = generate_candidate_configs(
        config.search_spaces,
        generation_config=config.generation_config,
        feature_columns=candidate_feature_columns,
    )
    registry_df = load_research_registry(registry_path)
    history_df = load_research_history(history_path)
    pending_candidates_df = select_candidates_for_evaluation(
        candidates_df,
        registry_df,
        stale_after_days=config.stale_after_days,
    )

    unique_horizons = sorted(
        set(candidates_df.get("horizon", pd.Series(dtype="int64")).dropna().astype(int).tolist())
    )
    symbol_data: dict[str, pd.DataFrame] = {}
    if not pending_candidates_df.empty:
        for symbol in resolved_symbols:
            try:
                df = _load_symbol_feature_data(config.feature_dir, symbol)
            except FileNotFoundError:
                continue
            if len(df) < config.min_rows:
                continue
            symbol_data[symbol] = add_forward_return_labels(df, horizons=unique_horizons)

    folds = _build_shared_folds(
        symbol_data,
        train_size=config.train_size,
        test_size=config.test_size,
        step_size=config.step_size,
        min_train_size=config.min_train_size,
    ) if symbol_data else []

    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    registry_updates: list[pd.DataFrame] = []
    history_rows: list[dict[str, object]] = []
    for _, candidate_row in pending_candidates_df.iterrows():
        fold_results_df, daily_metrics_df, score_panel_df, leaderboard_df = evaluate_candidate_signal(
            candidate_row,
            symbol_data=symbol_data,
            folds=folds,
            top_quantile=config.top_quantile,
            bottom_quantile=config.bottom_quantile,
        )
        if leaderboard_df.empty:
            row = {
                "candidate_id": candidate_row["candidate_id"],
                "signal_name": candidate_row["signal_name"],
                "signal_family": candidate_row["signal_family"],
                "parameters_json": candidate_row["parameters_json"],
                "lookback": int(candidate_row["lookback"]) if pd.notna(candidate_row["lookback"]) else pd.NA,
                "window": int(candidate_row["window"]) if pd.notna(candidate_row["window"]) else pd.NA,
                "threshold": float(candidate_row["threshold"]) if pd.notna(candidate_row["threshold"]) else pd.NA,
                "feature_a": candidate_row["feature_a"],
                "feature_b": candidate_row["feature_b"],
                "horizon": int(candidate_row["horizon"]),
                "symbols_tested": 0.0,
                "folds_tested": 0.0,
                "mean_dates_evaluated": 0.0,
                "mean_pearson_ic": float("nan"),
                "mean_spearman_ic": float("nan"),
                "mean_hit_rate": float("nan"),
                "mean_long_short_spread": float("nan"),
                "mean_quantile_spread": float("nan"),
                "mean_turnover": float("nan"),
                "worst_fold_spearman_ic": float("nan"),
                "total_obs": 0.0,
                "rejection_reason": "no_observations",
                "promotion_status": "reject",
                "evaluation_status": "completed",
                "last_evaluated_at": datetime.now(UTC).isoformat(),
            }
            leaderboard_df = pd.DataFrame([row], columns=REGISTRY_COLUMNS)
        _persist_candidate_details(
            output_dir,
            candidate_key=str(candidate_row["candidate_id"]),
            fold_results_df=fold_results_df,
            daily_metrics_df=daily_metrics_df,
            score_panel_df=score_panel_df,
        )
        registry_updates.append(leaderboard_df)
        history_rows.append(
            {
                "run_id": run_id,
                "candidate_id": str(candidate_row["candidate_id"]),
                "signal_name": str(candidate_row["signal_name"]),
                "signal_family": str(candidate_row["signal_family"]),
                "parameters_json": str(candidate_row["parameters_json"]),
                "lookback": int(candidate_row["lookback"]) if pd.notna(candidate_row["lookback"]) else pd.NA,
                "window": int(candidate_row["window"]) if pd.notna(candidate_row["window"]) else pd.NA,
                "threshold": float(candidate_row["threshold"]) if pd.notna(candidate_row["threshold"]) else pd.NA,
                "feature_a": candidate_row["feature_a"],
                "feature_b": candidate_row["feature_b"],
                "horizon": int(candidate_row["horizon"]),
                "evaluation_status": str(leaderboard_df["evaluation_status"].iloc[0]),
                "promotion_status": str(leaderboard_df["promotion_status"].iloc[0]),
                "last_evaluated_at": str(leaderboard_df["last_evaluated_at"].iloc[0]),
            }
        )

    if registry_updates:
        updated_registry = pd.concat([registry_df, *registry_updates], ignore_index=True)
    else:
        updated_registry = registry_df.copy()
    if not updated_registry.empty:
        updated_registry = (
            updated_registry.sort_values("last_evaluated_at")
            .drop_duplicates(subset=["candidate_id"], keep="last")
            .reset_index(drop=True)
        )
    updated_registry = updated_registry.reindex(columns=REGISTRY_COLUMNS)
    updated_registry.to_csv(registry_path, index=False)
    updated_registry.to_csv(signal_registry_path, index=False)

    if history_rows:
        updated_history = pd.concat([history_df, pd.DataFrame(history_rows, columns=HISTORY_COLUMNS)], ignore_index=True)
    else:
        updated_history = history_df.reindex(columns=HISTORY_COLUMNS)
    updated_history.to_csv(history_path, index=False)

    signal_family_summary_df = build_signal_family_summary(
        updated_registry,
        universe=config.universe,
    )
    feature_availability_report_df = build_feature_availability_report(
        symbol_data if symbol_data else {
            symbol: _load_symbol_feature_data(config.feature_dir, symbol)
            for symbol in resolved_symbols
            if (config.feature_dir / f"{symbol}.parquet").exists()
        },
        generation_config=config.generation_config,
        universe=config.universe,
    )
    aggregate_feature_columns = set(candidate_feature_columns or [])
    skipped_candidates_report_df = build_skipped_candidates_report(
        config.generation_config,
        feature_columns=aggregate_feature_columns,
        universe=config.universe,
    )
    fallback_usage_report_df = build_fallback_usage_report(
        symbol_data if symbol_data else {
            symbol: _load_symbol_feature_data(config.feature_dir, symbol)
            for symbol in resolved_symbols
            if (config.feature_dir / f"{symbol}.parquet").exists()
        },
        generation_config=config.generation_config,
        universe=config.universe,
    )

    signal_family_summary_path = output_dir / "signal_family_summary.csv"
    feature_availability_report_path = output_dir / "feature_availability_report.csv"
    skipped_candidates_report_path = output_dir / "skipped_candidates_report.csv"
    fallback_usage_report_path = output_dir / "fallback_usage_report.csv"
    signal_family_summary_df.to_csv(signal_family_summary_path, index=False)
    feature_availability_report_df.to_csv(feature_availability_report_path, index=False)
    skipped_candidates_report_df.to_csv(skipped_candidates_report_path, index=False)
    fallback_usage_report_df.to_csv(fallback_usage_report_path, index=False)

    aggregate_paths = aggregate_research_registry(
        updated_registry,
        output_dir=output_dir,
        universe=config.universe,
    )
    now = datetime.now(UTC)
    schedule_payload = {
        "last_run_at": now.isoformat(),
        "next_run_at": _next_run_at(now, config.schedule_frequency),
        "schedule_frequency": config.schedule_frequency,
        "run_id": run_id,
        "candidates_generated": int(len(candidates_df)),
        "candidates_evaluated": int(len(pending_candidates_df)),
    }
    schedule_path.write_text(json.dumps(schedule_payload, indent=2), encoding="utf-8")
    config_path = output_dir / "research_loop_config.json"
    config_path.write_text(json.dumps(config.to_dict(), indent=2), encoding="utf-8")

    return {
        "status": "completed",
        "registry_path": str(registry_path),
        "signal_registry_path": str(signal_registry_path),
        "history_path": str(history_path),
        "schedule_path": str(schedule_path),
        "config_path": str(config_path),
        "signal_family_summary_path": str(signal_family_summary_path),
        "feature_availability_report_path": str(feature_availability_report_path),
        "skipped_candidates_report_path": str(skipped_candidates_report_path),
        "fallback_usage_report_path": str(fallback_usage_report_path),
        **aggregate_paths,
    }


def run_automated_alpha_research_loop(
    *,
    config: AutomatedAlphaResearchConfig,
) -> dict[str, str | int]:
    iteration = 0
    last_result: dict[str, str | int] = {}

    while config.max_iterations is None or iteration < config.max_iterations:
        iteration += 1
        iteration_result = _run_automated_alpha_research_iteration(config=config)
        last_result = dict(iteration_result)
        last_result["iterations_completed"] = iteration

        if iteration_result.get("status") == "skipped":
            break

    if not last_result:
        return {"status": "skipped", "iterations_completed": 0}

    return last_result

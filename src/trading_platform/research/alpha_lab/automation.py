from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd

from trading_platform.research.alpha_lab.composite import (
    DEFAULT_COMPOSITE_CONFIG,
    candidate_id,
    select_low_redundancy_signals,
)
from trading_platform.research.alpha_lab.folds import build_walk_forward_folds
from trading_platform.research.alpha_lab.labels import add_forward_return_labels
from trading_platform.research.alpha_lab.metrics import (
    compute_cross_sectional_daily_metrics,
    evaluate_cross_sectional_signal,
)
from trading_platform.research.alpha_lab.promotion import apply_promotion_rules
from trading_platform.research.alpha_lab.signals import build_signal


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
    search_spaces: tuple[SignalSearchSpace, ...]
    min_rows: int = 126
    top_quantile: float = 0.2
    bottom_quantile: float = 0.2
    train_size: int = 252 * 3
    test_size: int = 63
    step_size: int | None = None
    min_train_size: int | None = None
    schedule_frequency: str = "manual"
    force: bool = False

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["feature_dir"] = str(self.feature_dir)
        payload["output_dir"] = str(self.output_dir)
        return payload


REGISTRY_COLUMNS = [
    "candidate_id",
    "signal_family",
    "lookback",
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
    "signal_family",
    "lookback",
    "horizon",
    "evaluation_status",
    "promotion_status",
    "last_evaluated_at",
]


def generate_candidate_configs(
    search_spaces: Iterable[SignalSearchSpace],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for space in search_spaces:
        for lookback in space.lookbacks:
            for horizon in space.horizons:
                rows.append(
                    {
                        "candidate_id": candidate_id(space.signal_family, int(lookback), int(horizon)),
                        "signal_family": space.signal_family,
                        "lookback": int(lookback),
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
    if candidates_df.empty:
        return candidates_df.copy()
    if registry_df.empty or "candidate_id" not in registry_df.columns:
        return candidates_df.copy()

    tested = set(registry_df.loc[registry_df["evaluation_status"] == "completed", "candidate_id"])
    return candidates_df.loc[~candidates_df["candidate_id"].isin(tested)].reset_index(drop=True)


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
    parquet_path = feature_dir / f"{symbol}.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(f"Feature file not found for {symbol}: {parquet_path}")

    df = pd.read_parquet(parquet_path)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp").reset_index(drop=True)
    if "symbol" not in df.columns:
        df["symbol"] = symbol
    return df


def _resolve_symbols(symbols: list[str] | None, universe: str | None) -> list[str]:
    if symbols:
        return sorted(set(symbols))
    if universe:
        universe_path = Path("config/universes") / f"{universe}.txt"
        if universe_path.exists():
            return [line.strip() for line in universe_path.read_text().splitlines() if line.strip()]
        raise ValueError(f"Universe '{universe}' was provided, but no resolver is wired yet.")
    raise ValueError("Provide either --symbols or --universe.")


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
    signal_family = str(candidate_row["signal_family"])
    lookback = int(candidate_row["lookback"])
    horizon = int(candidate_row["horizon"])
    label_col = f"fwd_return_{horizon}d"

    fold_rows: list[dict[str, object]] = []
    daily_metrics_frames: list[pd.DataFrame] = []
    score_frames: list[pd.DataFrame] = []
    for fold in folds:
        fold_frames: list[pd.DataFrame] = []
        for symbol, df in symbol_data.items():
            signal = build_signal(df, signal_family=signal_family, lookback=lookback)
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
                "signal_family": signal_family,
                "lookback": lookback,
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
            "signal_family",
            "lookback",
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
            fold_results_df.groupby(["signal_family", "lookback", "horizon"], as_index=False)
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
        leaderboard_df["candidate_id"] = candidate_row["candidate_id"]
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
        "signal_family_a",
        "lookback_a",
        "horizon_a",
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
                    "signal_family_a": str(left_row["signal_family"]),
                    "lookback_a": int(left_row["lookback"]),
                    "horizon_a": int(left_row["horizon"]),
                    "signal_family_b": str(right_row["signal_family"]),
                    "lookback_b": int(right_row["lookback"]),
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
) -> dict[str, str]:
    leaderboard_df = registry_df.copy()
    if leaderboard_df.empty:
        leaderboard_df = pd.DataFrame(columns=REGISTRY_COLUMNS)
    else:
        promoted = apply_promotion_rules(
            leaderboard_df[
                [
                    "signal_family",
                    "lookback",
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
        leaderboard_df = leaderboard_df[
            ["candidate_id", "last_evaluated_at", "evaluation_status"]
        ]
        leaderboard_df = pd.concat(
            [promoted.reset_index(drop=True), leaderboard_df.reset_index(drop=True)],
            axis=1,
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
    composite_inputs: dict[str, object] = {"horizons": {}}
    for horizon in sorted(promoted_signals_df["horizon"].dropna().unique().tolist()) if not promoted_signals_df.empty else []:
        selected_df, excluded_rows = select_low_redundancy_signals(
            promoted_signals_df,
            redundancy_df,
            horizon=int(horizon),
            redundancy_corr_threshold=DEFAULT_COMPOSITE_CONFIG.redundancy_corr_threshold,
        )
        composite_inputs["horizons"][str(int(horizon))] = {
            "selected_signals": selected_df[["candidate_id", "signal_family", "lookback", "horizon"]].to_dict(orient="records"),
            "excluded_signals": excluded_rows,
        }

    leaderboard_path = output_dir / "leaderboard.csv"
    promoted_path = output_dir / "promoted_signals.csv"
    redundancy_path = output_dir / "redundancy_report.csv"
    composite_inputs_path = output_dir / "composite_inputs.json"
    leaderboard_df.to_csv(leaderboard_path, index=False)
    promoted_signals_df.to_csv(promoted_path, index=False)
    redundancy_df.to_csv(redundancy_path, index=False)
    composite_inputs_path.write_text(json.dumps(composite_inputs, indent=2, default=str), encoding="utf-8")
    return {
        "leaderboard_path": str(leaderboard_path),
        "promoted_signals_path": str(promoted_path),
        "redundancy_report_path": str(redundancy_path),
        "composite_inputs_path": str(composite_inputs_path),
    }


def run_automated_alpha_research_loop(
    *,
    config: AutomatedAlphaResearchConfig,
) -> dict[str, str]:
    output_dir = config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    registry_path = output_dir / "research_registry.csv"
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

    candidates_df = generate_candidate_configs(config.search_spaces)
    registry_df = load_research_registry(registry_path)
    history_df = load_research_history(history_path)
    pending_candidates_df = select_untested_candidates(candidates_df, registry_df)

    resolved_symbols = _resolve_symbols(config.symbols, config.universe)
    unique_horizons = sorted(
        {
            int(horizon)
            for search_space in config.search_spaces
            for horizon in search_space.horizons
        }
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
                "signal_family": candidate_row["signal_family"],
                "lookback": int(candidate_row["lookback"]),
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
                "signal_family": str(candidate_row["signal_family"]),
                "lookback": int(candidate_row["lookback"]),
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

    if history_rows:
        updated_history = pd.concat([history_df, pd.DataFrame(history_rows, columns=HISTORY_COLUMNS)], ignore_index=True)
    else:
        updated_history = history_df.reindex(columns=HISTORY_COLUMNS)
    updated_history.to_csv(history_path, index=False)

    aggregate_paths = aggregate_research_registry(updated_registry, output_dir=output_dir)
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
        "history_path": str(history_path),
        "schedule_path": str(schedule_path),
        "config_path": str(config_path),
        **aggregate_paths,
    }

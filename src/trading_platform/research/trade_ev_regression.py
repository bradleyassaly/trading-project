from __future__ import annotations

import json
import pickle
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from trading_platform.reporting.ev_lifecycle import aggregate_replay_ev_lifecycle
from trading_platform.research.trade_ev import _normalize_candidate_scores, build_trade_ev_candidate_market_features

REGRESSION_FEATURE_COLUMNS = [
    "score_entry",
    "score_percentile_entry",
    "expected_horizon_days",
    "recent_return_3d",
    "recent_return_5d",
    "recent_return_10d",
    "recent_vol_20d",
]

REGRESSION_PREDICTION_COLUMNS = [
    "trade_id",
    "entry_date",
    "exit_date",
    "symbol",
    "strategy_id",
    "signal_family",
    "score_entry",
    "score_percentile_entry",
    "expected_horizon_days",
    "recent_return_3d",
    "recent_return_5d",
    "recent_return_10d",
    "recent_vol_20d",
    "predicted_ev",
    "ev_confidence",
    "ev_confidence_multiplier",
    "residual_std_bucket",
    "residual_std_global",
    "residual_std_final",
    "sample_size_used",
    "residual_std_confidence",
    "magnitude_confidence",
    "model_performance_confidence",
    "combined_confidence",
    "ev_score_before_confidence",
    "ev_score_after_confidence",
    "residual_std_used",
    "confidence_source",
    "realized_return",
    "prediction_available",
    "prediction_reason",
    "training_sample_count",
]


def _safe_float(value: Any) -> float:
    try:
        if value is None or pd.isna(value):
            return 0.0
    except TypeError:
        if value is None:
            return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _read_csv_frame(path: str | Path) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(csv_path)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame()


def _normalize_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    frame = pd.DataFrame(rows)
    if frame.empty:
        return []
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def _safe_corr(left: pd.Series, right: pd.Series, *, method: str = "pearson") -> float:
    if len(left.index) < 2 or len(right.index) < 2:
        return 0.0
    value = left.corr(right, method=method)
    if value is None or pd.isna(value):
        return 0.0
    return float(value)


def _confidence_from_std(std_value: float, *, residual_eps: float) -> float:
    return 1.0 / (float(residual_eps) + max(float(std_value), 0.0))


def _normalize_range(value: float, *, min_value: float, max_value: float, default: float = 0.5) -> float:
    if max_value > min_value:
        return float(min(1.0, max(0.0, (float(value) - min_value) / (max_value - min_value))))
    return float(default)


def _empirical_percentile(sorted_values: list[float], value: float) -> float:
    if not sorted_values:
        return 0.5
    position = int(np.searchsorted(np.array(sorted_values, dtype=float), float(value), side="right"))
    return float(position / len(sorted_values))


def _summarize_regression_rows(rows: list[dict[str, Any]], *, expected_horizon_days: int) -> dict[str, Any]:
    return {
        "row_count": int(len(rows)),
        "signal_family_count": int(len({str(row.get('signal_family') or 'unknown') for row in rows})),
        "average_realized_return": float(
            pd.to_numeric(pd.Series([row.get("realized_return", 0.0) for row in rows]), errors="coerce")
            .fillna(0.0)
            .mean()
        )
        if rows
        else 0.0,
        "expected_horizon_days": int(expected_horizon_days),
    }


def _build_trade_ev_regression_rows(
    *,
    lifecycle_frame: pd.DataFrame,
    expected_horizon_days: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if lifecycle_frame.empty:
        return [], _summarize_regression_rows([], expected_horizon_days=expected_horizon_days)
    market_feature_cache: dict[str, pd.DataFrame] = {}
    rows: list[dict[str, Any]] = []
    for row in lifecycle_frame.astype(object).where(pd.notna(lifecycle_frame), None).to_dict(orient="records"):
        symbol = str(row.get("symbol") or "")
        entry_date = str(row.get("entry_date") or "")
        exit_date = str(row.get("exit_date") or row.get("date") or "")
        if not symbol or not entry_date or not exit_date:
            continue
        market_features = build_trade_ev_candidate_market_features(
            symbol=symbol,
            as_of_date=entry_date,
            frame_cache=market_feature_cache,
        )
        rows.append(
            {
                "trade_id": row.get("trade_id"),
                "entry_date": entry_date,
                "exit_date": exit_date,
                "symbol": symbol,
                "strategy_id": row.get("strategy_id"),
                "signal_family": str(row.get("signal_family") or "unknown"),
                "score_entry": _safe_float(row.get("score_entry")),
                "score_percentile_entry": _safe_float(row.get("score_percentile_entry")),
                "expected_horizon_days": int(expected_horizon_days),
                "recent_return_3d": _safe_float(market_features.get("recent_return_3d")),
                "recent_return_5d": _safe_float(market_features.get("recent_return_5d")),
                "recent_return_10d": _safe_float(market_features.get("recent_return_10d")),
                "recent_vol_20d": _safe_float(market_features.get("recent_vol_20d")),
                "realized_return": _safe_float(row.get("realized_return")),
            }
        )
    return _normalize_records(rows), _summarize_regression_rows(rows, expected_horizon_days=expected_horizon_days)


def build_trade_ev_regression_dataset(
    *,
    replay_root: str | Path,
    expected_horizon_days: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    root = Path(replay_root)
    lifecycle_path = root / "replay_trade_ev_lifecycle.csv"
    lifecycle_frame = _read_csv_frame(lifecycle_path)
    if lifecycle_frame.empty:
        lifecycle_rows, _ = aggregate_replay_ev_lifecycle(replay_root=root)
        lifecycle_frame = pd.DataFrame(lifecycle_rows)
    if lifecycle_frame.empty:
        return [], {
            "row_count": 0,
            "signal_family_count": 0,
            "average_realized_return": 0.0,
            "expected_horizon_days": int(expected_horizon_days),
        }
    return _build_trade_ev_regression_rows(
        lifecycle_frame=lifecycle_frame,
        expected_horizon_days=expected_horizon_days,
    )


def build_trade_ev_regression_history_dataset(
    *,
    history_root: str | Path | None,
    as_of_date: str,
    expected_horizon_days: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    root = Path(history_root) if history_root else None
    if root is None or not root.exists():
        summary = _summarize_regression_rows([], expected_horizon_days=expected_horizon_days)
        summary["cutoff_date"] = str(as_of_date)
        return [], summary
    cutoff = pd.Timestamp(as_of_date).date()
    lifecycle_rows: list[dict[str, Any]] = []
    for day_dir in sorted(item for item in root.iterdir() if item.is_dir()):
        try:
            day_value = pd.Timestamp(day_dir.name).date()
        except (TypeError, ValueError):
            continue
        if day_value >= cutoff:
            continue
        frame = _read_csv_frame(day_dir / "paper" / "trade_ev_lifecycle.csv")
        if frame.empty:
            continue
        lifecycle_rows.extend(frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records"))
    dataset_rows, summary = _build_trade_ev_regression_rows(
        lifecycle_frame=pd.DataFrame(lifecycle_rows),
        expected_horizon_days=expected_horizon_days,
    )
    summary["cutoff_date"] = str(as_of_date)
    return dataset_rows, summary


def _design_matrix(frame: pd.DataFrame, *, signal_families: list[str]) -> np.ndarray:
    numeric = frame.reindex(columns=REGRESSION_FEATURE_COLUMNS).apply(pd.to_numeric, errors="coerce").fillna(0.0)
    parts = [numeric.to_numpy(dtype=float)]
    signal_series = frame.get("signal_family", pd.Series(["unknown"] * len(frame.index), index=frame.index)).astype(str)
    for family in signal_families:
        parts.append((signal_series == family).astype(float).to_numpy(dtype=float).reshape(-1, 1))
    return np.hstack(parts) if parts else np.empty((len(frame.index), 0))


def _score_bucket(score_percentile: float) -> str:
    if score_percentile >= 0.8:
        return "q5"
    if score_percentile >= 0.6:
        return "q4"
    if score_percentile >= 0.4:
        return "q3"
    if score_percentile >= 0.2:
        return "q2"
    return "q1"


def train_trade_ev_regression_model(
    *,
    training_rows: list[dict[str, Any]],
    min_training_samples: int = 20,
    ridge_alpha: float = 1.0,
    confidence_min_samples_per_bucket: int = 20,
    confidence_shrinkage_enabled: bool = True,
    confidence_recent_performance_window: int = 50,
) -> dict[str, Any]:
    if len(training_rows) < int(min_training_samples):
        return {
            "model_type": "regression",
            "training_available": False,
            "training_sample_count": int(len(training_rows)),
            "warnings": ["insufficient_lifecycle_history_for_regression_ev"],
        }
    frame = pd.DataFrame(training_rows)
    signal_families = sorted({str(value or "unknown") for value in frame.get("signal_family", [])})
    x_raw = _design_matrix(frame, signal_families=signal_families)
    means = x_raw.mean(axis=0)
    stds = x_raw.std(axis=0, ddof=0)
    stds = np.where(stds <= 0.0, 1.0, stds)
    x = (x_raw - means) / stds
    x_with_intercept = np.column_stack([np.ones(len(frame.index)), x])
    y = pd.to_numeric(frame["realized_return"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    penalty = np.eye(x_with_intercept.shape[1]) * float(ridge_alpha)
    penalty[0, 0] = 0.0
    beta = np.linalg.solve(x_with_intercept.T @ x_with_intercept + penalty, x_with_intercept.T @ y)
    fitted = beta[0] + (x @ beta[1:])
    residuals = y - fitted
    global_residual_std = float(np.std(residuals, ddof=0)) if len(residuals) else 0.0
    abs_fitted = np.abs(fitted)
    residual_eps = 1e-6
    confidence_groups: dict[str, dict[str, Any]] = {}
    group_keys = [
        f"{str(row.get('signal_family') or 'unknown')}|{_score_bucket(_safe_float(row.get('score_percentile_entry')))}"
        for row in training_rows
    ]
    entry_dates = pd.to_datetime(frame.get("entry_date"), errors="coerce")
    exit_dates = pd.to_datetime(frame.get("exit_date"), errors="coerce")
    group_frame = pd.DataFrame(
        {
            "group_key": group_keys,
            "residual": residuals,
            "predicted": fitted,
            "realized": y,
            "entry_date": entry_dates,
            "exit_date": exit_dates,
        }
    )
    for row in (
        group_frame.groupby("group_key", dropna=False)
        .agg(sample_count=("residual", "count"), residual_std=("residual", "std"))
        .reset_index()
        .to_dict(orient="records")
    ):
        group_key = str(row["group_key"])
        group_subset = group_frame[group_frame["group_key"] == group_key].sort_values(
            ["exit_date", "entry_date"],
            kind="stable",
        )
        recent_subset = group_subset.tail(max(int(confidence_recent_performance_window), 2))
        confidence_groups[group_key] = {
            "sample_count": int(row["sample_count"]),
            "residual_std": _safe_float(row.get("residual_std")),
            "recent_rank_correlation": _safe_corr(
                pd.Series(recent_subset["predicted"], dtype=float),
                pd.Series(recent_subset["realized"], dtype=float),
                method="spearman",
            ),
        }
    ordered_frame = group_frame.sort_values(["exit_date", "entry_date"], kind="stable")
    recent_global_subset = ordered_frame.tail(max(int(confidence_recent_performance_window), 2))
    global_recent_rank_correlation = _safe_corr(
        pd.Series(recent_global_subset["predicted"], dtype=float),
        pd.Series(recent_global_subset["realized"], dtype=float),
        method="spearman",
    )
    confidence_raw_values = [
        _confidence_from_std(global_residual_std, residual_eps=residual_eps),
        *[
            _confidence_from_std(_safe_float(group.get("residual_std")), residual_eps=residual_eps)
            for group in confidence_groups.values()
            if int(group.get("sample_count", 0) or 0) > 0
        ],
    ]
    confidence_raw_min = float(min(confidence_raw_values)) if confidence_raw_values else 0.0
    confidence_raw_max = float(max(confidence_raw_values)) if confidence_raw_values else 0.0
    feature_names = REGRESSION_FEATURE_COLUMNS + [f"signal_family={family}" for family in signal_families]
    return {
        "model_type": "regression",
        "training_available": True,
        "training_sample_count": int(len(training_rows)),
        "ridge_alpha": float(ridge_alpha),
        "feature_names": feature_names,
        "signal_families": signal_families,
        "feature_means": [float(value) for value in means],
        "feature_stds": [float(value) for value in stds],
        "intercept": float(beta[0]),
        "coefficients": [float(value) for value in beta[1:]],
        "global_residual_std": global_residual_std,
        "global_recent_rank_correlation": global_recent_rank_correlation,
        "residual_eps": residual_eps,
        "confidence_groups": confidence_groups,
        "confidence_group_min_samples": int(confidence_min_samples_per_bucket),
        "confidence_shrinkage_enabled": bool(confidence_shrinkage_enabled),
        "confidence_raw_min": confidence_raw_min,
        "confidence_raw_max": confidence_raw_max,
        "abs_predicted_ev_sorted": [float(value) for value in sorted(abs_fitted.tolist())],
        "warnings": [],
    }


def predict_trade_ev_regression(
    *,
    model: dict[str, Any],
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not rows:
        return []
    frame = pd.DataFrame(rows)
    if not bool(model.get("training_available", False)):
        prediction_rows = [
            {
                **row,
                "predicted_ev": None,
                "prediction_available": False,
                "prediction_reason": "insufficient_history",
                "training_sample_count": int(model.get("training_sample_count", 0) or 0),
            }
            for row in rows
        ]
        return _normalize_records(prediction_rows)
    x_raw = _design_matrix(frame, signal_families=list(model.get("signal_families") or []))
    means = np.array(model.get("feature_means") or [0.0] * x_raw.shape[1], dtype=float)
    stds = np.array(model.get("feature_stds") or [1.0] * x_raw.shape[1], dtype=float)
    stds = np.where(stds <= 0.0, 1.0, stds)
    x = (x_raw - means) / stds
    coefficients = np.array(model.get("coefficients") or [0.0] * x.shape[1], dtype=float)
    predicted = float(model.get("intercept", 0.0)) + (x @ coefficients)
    prediction_rows = []
    for index, row in enumerate(rows):
        prediction_rows.append(
            {
                **row,
                "predicted_ev": float(predicted[index]),
                "prediction_available": True,
                "prediction_reason": "regression_model",
                "training_sample_count": int(model.get("training_sample_count", 0) or 0),
            }
        )
    return _normalize_records(prediction_rows)


def _prediction_confidence(
    *,
    model: dict[str, Any],
    row: dict[str, Any],
    use_confidence_weighting: bool,
    confidence_scale: float,
    confidence_clip_min: float,
    confidence_clip_max: float,
    confidence_min_samples_per_bucket: int,
    confidence_shrinkage_enabled: bool,
    confidence_component_residual_std_weight: float,
    confidence_component_magnitude_weight: float,
    confidence_component_model_performance_weight: float,
) -> dict[str, Any]:
    if not use_confidence_weighting or not bool(model.get("training_available", False)):
        return {
            "ev_confidence": 1.0,
            "ev_confidence_multiplier": 1.0,
            "residual_std_bucket": _safe_float(model.get("global_residual_std")),
            "residual_std_global": _safe_float(model.get("global_residual_std")),
            "residual_std_final": _safe_float(model.get("global_residual_std")),
            "sample_size_used": 0,
            "residual_std_confidence": 1.0,
            "magnitude_confidence": 1.0,
            "model_performance_confidence": 1.0,
            "combined_confidence": 1.0,
            "residual_std_used": _safe_float(model.get("global_residual_std")),
            "confidence_source": "disabled",
        }
    signal_family = str(row.get("signal_family") or "unknown")
    score_bucket = _score_bucket(_safe_float(row.get("score_percentile_entry")))
    group_key = f"{signal_family}|{score_bucket}"
    group_stats = dict((model.get("confidence_groups") or {}).get(group_key) or {})
    sample_count = int(group_stats.get("sample_count", 0) or 0)
    min_samples = int(
        confidence_min_samples_per_bucket
        or model.get("confidence_group_min_samples", 20)
        or 20
    )
    residual_std_bucket = _safe_float(group_stats.get("residual_std"))
    residual_std_global = _safe_float(model.get("global_residual_std"))
    if confidence_shrinkage_enabled:
        shrink_weight = float(min(1.0, sample_count / max(min_samples, 1)))
        residual_std = (shrink_weight * residual_std_bucket) + ((1.0 - shrink_weight) * residual_std_global)
        confidence_source = "shrunk_bucket"
    elif sample_count >= min_samples:
        residual_std = residual_std_bucket
        confidence_source = "group"
    else:
        residual_std = residual_std_global
        confidence_source = "global"
    residual_eps = _safe_float(model.get("residual_eps")) or 1e-6
    raw_confidence = _confidence_from_std(residual_std, residual_eps=residual_eps)
    raw_min = _safe_float(model.get("confidence_raw_min"))
    raw_max = _safe_float(model.get("confidence_raw_max"))
    residual_std_confidence = _normalize_range(raw_confidence, min_value=raw_min, max_value=raw_max, default=1.0)
    magnitude_confidence = _empirical_percentile(
        list(model.get("abs_predicted_ev_sorted") or []),
        abs(_safe_float(row.get("predicted_ev"))),
    )
    recent_rank_correlation = _safe_float(group_stats.get("recent_rank_correlation"))
    if sample_count < min_samples and confidence_shrinkage_enabled:
        correlation_weight = float(min(1.0, sample_count / max(min_samples, 1)))
        recent_rank_correlation = (
            correlation_weight * recent_rank_correlation
            + (1.0 - correlation_weight) * _safe_float(model.get("global_recent_rank_correlation"))
        )
    elif sample_count < min_samples:
        recent_rank_correlation = _safe_float(model.get("global_recent_rank_correlation"))
    model_performance_confidence = float(min(1.0, max(0.0, (recent_rank_correlation + 1.0) / 2.0)))
    component_weights = {
        "residual_std": max(0.0, float(confidence_component_residual_std_weight)),
        "magnitude": max(0.0, float(confidence_component_magnitude_weight)),
        "model_performance": max(0.0, float(confidence_component_model_performance_weight)),
    }
    total_component_weight = float(sum(component_weights.values()))
    if total_component_weight <= 0.0:
        normalized_confidence = residual_std_confidence
    else:
        normalized_confidence = float(
            (
                component_weights["residual_std"] * residual_std_confidence
                + component_weights["magnitude"] * magnitude_confidence
                + component_weights["model_performance"] * model_performance_confidence
            )
            / total_component_weight
        )
    multiplier = 1.0 + (float(confidence_scale) * (normalized_confidence - 0.5))
    multiplier = float(min(float(confidence_clip_max), max(float(confidence_clip_min), multiplier)))
    return {
        "ev_confidence": normalized_confidence,
        "ev_confidence_multiplier": multiplier,
        "residual_std_bucket": residual_std_bucket,
        "residual_std_global": residual_std_global,
        "residual_std_final": residual_std,
        "sample_size_used": sample_count,
        "residual_std_confidence": residual_std_confidence,
        "magnitude_confidence": magnitude_confidence,
        "model_performance_confidence": model_performance_confidence,
        "combined_confidence": normalized_confidence,
        "residual_std_used": residual_std,
        "confidence_source": confidence_source,
    }


def score_trade_ev_regression_candidates(
    *,
    model: dict[str, Any],
    candidate_rows: list[dict[str, Any]],
    use_confidence_weighting: bool = False,
    confidence_scale: float = 1.0,
    confidence_clip_min: float = 0.5,
    confidence_clip_max: float = 1.5,
    confidence_min_samples_per_bucket: int = 20,
    confidence_shrinkage_enabled: bool = True,
    confidence_component_residual_std_weight: float = 1.0,
    confidence_component_magnitude_weight: float = 0.0,
    confidence_component_model_performance_weight: float = 0.0,
    score_clip_min: float | None = None,
    score_clip_max: float | None = None,
    normalize_scores: bool = False,
    normalization_method: str = "zscore",
    normalize_within: str = "all_candidates",
    use_normalized_score_for_weighting: bool = True,
) -> list[dict[str, Any]]:
    regression_rows = [
        {
            "symbol": row.get("symbol"),
            "strategy_id": row.get("strategy_id"),
            "signal_family": str(row.get("signal_family") or "unknown"),
            "score_entry": _safe_float(row.get("signal_score")),
            "score_percentile_entry": _safe_float(row.get("score_percentile")),
            "expected_horizon_days": int(_safe_float(row.get("expected_horizon_days"))),
            "recent_return_3d": _safe_float(row.get("recent_return_3d")),
            "recent_return_5d": _safe_float(row.get("recent_return_5d")),
            "recent_return_10d": _safe_float(row.get("recent_return_10d")),
            "recent_vol_20d": _safe_float(row.get("recent_vol_20d")),
            "estimated_execution_cost_pct": _safe_float(row.get("estimated_execution_cost_pct")),
            "weight_delta": _safe_float(row.get("weight_delta")),
        }
        for row in candidate_rows
    ]
    predicted_rows = predict_trade_ev_regression(model=model, rows=regression_rows)
    frame = pd.DataFrame(predicted_rows)
    if not frame.empty:
        frame = _normalize_candidate_scores(
            frame=frame,
            raw_score_column="predicted_ev",
            normalize_scores=normalize_scores,
            normalization_method=normalization_method,
            normalize_within=normalize_within,
            score_clip_min=score_clip_min,
            score_clip_max=score_clip_max,
        )
    output_rows: list[dict[str, Any]] = []
    for index, row in enumerate(predicted_rows):
        normalized_row = frame.iloc[index] if not frame.empty else None
        raw_score = _safe_float(row.get("predicted_ev"))
        normalized_score = float(normalized_row["normalized_ev_score"]) if normalized_row is not None else raw_score
        score_post_clip = float(normalized_row["ev_score_post_clip"]) if normalized_row is not None else raw_score
        confidence = _prediction_confidence(
            model=model,
            row=row,
            use_confidence_weighting=use_confidence_weighting,
            confidence_scale=confidence_scale,
            confidence_clip_min=confidence_clip_min,
            confidence_clip_max=confidence_clip_max,
            confidence_min_samples_per_bucket=confidence_min_samples_per_bucket,
            confidence_shrinkage_enabled=confidence_shrinkage_enabled,
            confidence_component_residual_std_weight=confidence_component_residual_std_weight,
            confidence_component_magnitude_weight=confidence_component_magnitude_weight,
            confidence_component_model_performance_weight=confidence_component_model_performance_weight,
        )
        score_before_confidence = score_post_clip if use_normalized_score_for_weighting else raw_score
        score_after_confidence = float(score_before_confidence * float(confidence["ev_confidence_multiplier"]))
        output_rows.append(
            {
                **row,
                "expected_gross_return": raw_score + _safe_float(row.get("estimated_execution_cost_pct")),
                "expected_net_return": raw_score,
                "expected_cost": _safe_float(row.get("estimated_execution_cost_pct")),
                "regression_raw_ev_score": raw_score,
                "regression_normalized_ev_score": normalized_score,
                "regression_ev_score_post_clip": score_post_clip,
                "ev_confidence": confidence["ev_confidence"],
                "ev_confidence_multiplier": confidence["ev_confidence_multiplier"],
                "residual_std_bucket": confidence["residual_std_bucket"],
                "residual_std_global": confidence["residual_std_global"],
                "residual_std_final": confidence["residual_std_final"],
                "sample_size_used": confidence["sample_size_used"],
                "residual_std_confidence": confidence["residual_std_confidence"],
                "magnitude_confidence": confidence["magnitude_confidence"],
                "model_performance_confidence": confidence["model_performance_confidence"],
                "combined_confidence": confidence["combined_confidence"],
                "residual_std_used": confidence["residual_std_used"],
                "confidence_source": confidence["confidence_source"],
                "ev_score_before_confidence": score_before_confidence,
                "ev_score_after_confidence": score_after_confidence,
                "raw_ev_score": raw_score,
                "normalized_ev_score": normalized_score,
                "ev_score_pre_clip": float(normalized_row["ev_score_pre_clip"]) if normalized_row is not None else raw_score,
                "ev_score_post_clip": score_post_clip,
                "ev_score_clipped": bool(normalized_row["ev_score_clipped"]) if normalized_row is not None else False,
                "ev_weighting_score": score_after_confidence,
                "normalization_method": str(normalized_row["normalization_method"]) if normalized_row is not None else "disabled",
                "normalize_within": str(normalized_row["normalize_within"]) if normalized_row is not None else "all_candidates",
                "candidate_count_for_normalization": int(
                    normalized_row["candidate_count_for_normalization"] if normalized_row is not None else len(predicted_rows)
                ),
            }
        )
    return _normalize_records(output_rows)


def evaluate_trade_ev_regression_predictions(
    *,
    prediction_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not prediction_rows:
        return [], {
            "prediction_count": 0,
            "correlation": 0.0,
            "rank_correlation": 0.0,
            "bucket_spread": 0.0,
            "bucket_rows": [],
        }
    frame = pd.DataFrame(prediction_rows)
    frame["predicted_ev"] = pd.to_numeric(frame.get("predicted_ev"), errors="coerce")
    frame["realized_return"] = pd.to_numeric(frame.get("realized_return"), errors="coerce")
    frame = frame.dropna(subset=["predicted_ev", "realized_return"]).copy()
    if frame.empty:
        return [], {
            "prediction_count": 0,
            "correlation": 0.0,
            "rank_correlation": 0.0,
            "bucket_spread": 0.0,
            "bucket_rows": [],
        }
    if frame["predicted_ev"].nunique(dropna=False) <= 1:
        frame["bucket"] = "b1"
    else:
        rank = frame["predicted_ev"].rank(method="first")
        bucket_count = min(10, len(frame.index))
        buckets = pd.qcut(rank, q=bucket_count, labels=False, duplicates="drop")
        frame["bucket"] = buckets.fillna(0).astype(int).map(lambda value: f"b{int(value) + 1}")
    grouped = (
        frame.groupby("bucket", dropna=False)
        .agg(
            trade_count=("trade_id", "count"),
            avg_predicted_ev=("predicted_ev", "mean"),
            avg_realized_return=("realized_return", "mean"),
        )
        .reset_index()
        .sort_values("bucket", kind="stable")
    )
    bucket_rows = grouped.astype(object).where(pd.notna(grouped), None).to_dict(orient="records")
    top_bucket = grouped.iloc[-1] if not grouped.empty else None
    bottom_bucket = grouped.iloc[0] if not grouped.empty else None
    summary = {
        "prediction_count": int(len(frame.index)),
        "correlation": float(frame["predicted_ev"].corr(frame["realized_return"])) if len(frame.index) >= 2 else 0.0,
        "rank_correlation": float(frame["predicted_ev"].corr(frame["realized_return"], method="spearman")) if len(frame.index) >= 2 else 0.0,
        "bucket_spread": (
            float(top_bucket["avg_realized_return"] - bottom_bucket["avg_realized_return"])
            if top_bucket is not None and bottom_bucket is not None
            else 0.0
        ),
        "bucket_rows": bucket_rows,
    }
    for column in REGRESSION_PREDICTION_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    return _normalize_records(frame[REGRESSION_PREDICTION_COLUMNS].to_dict(orient="records")), summary


def build_trade_ev_confidence_summary(
    *,
    prediction_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not prediction_rows:
        return [], {
            "prediction_count": 0,
            "confidence_row_count": 0,
            "avg_ev_confidence": 1.0,
            "avg_ev_confidence_multiplier": 1.0,
            "confidence_absolute_error_correlation": 0.0,
            "confidence_realized_return_correlation": 0.0,
        }
    frame = pd.DataFrame(prediction_rows).copy()
    frame["predicted_ev"] = pd.to_numeric(frame.get("predicted_ev"), errors="coerce")
    frame["realized_return"] = pd.to_numeric(frame.get("realized_return"), errors="coerce")
    frame["ev_confidence"] = pd.to_numeric(frame.get("ev_confidence"), errors="coerce").fillna(1.0)
    frame["ev_confidence_multiplier"] = pd.to_numeric(frame.get("ev_confidence_multiplier"), errors="coerce").fillna(1.0)
    frame = frame.dropna(subset=["predicted_ev", "realized_return"]).copy()
    if frame.empty:
        return [], {
            "prediction_count": 0,
            "confidence_row_count": 0,
            "avg_ev_confidence": 1.0,
            "avg_ev_confidence_multiplier": 1.0,
            "confidence_absolute_error_correlation": 0.0,
            "confidence_realized_return_correlation": 0.0,
        }
    frame["residual"] = frame["realized_return"] - frame["predicted_ev"]
    frame["absolute_error"] = frame["residual"].abs()
    absolute_error_corr = _safe_corr(frame["ev_confidence"], frame["absolute_error"], method="spearman")
    realized_return_corr = _safe_corr(frame["ev_confidence"], frame["realized_return"], method="spearman")
    summary = {
        "prediction_count": int(len(frame.index)),
        "confidence_row_count": int(len(frame.index)),
        "avg_ev_confidence": float(frame["ev_confidence"].mean()),
        "avg_ev_confidence_multiplier": float(frame["ev_confidence_multiplier"].mean()),
        "confidence_absolute_error_correlation": absolute_error_corr,
        "confidence_realized_return_correlation": realized_return_corr,
    }
    columns = REGRESSION_PREDICTION_COLUMNS + ["residual", "absolute_error"]
    return _normalize_records(frame[columns].to_dict(orient="records")), summary


def build_trade_ev_confidence_bucket_analysis(
    *,
    confidence_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not confidence_rows:
        return [], {
            "bucket_count": 0,
            "top_vs_bottom_realized_return_spread": 0.0,
        }
    frame = pd.DataFrame(confidence_rows).copy()
    frame["ev_confidence"] = pd.to_numeric(frame.get("ev_confidence"), errors="coerce")
    frame["realized_return"] = pd.to_numeric(frame.get("realized_return"), errors="coerce")
    frame["predicted_ev"] = pd.to_numeric(frame.get("predicted_ev"), errors="coerce")
    frame = frame.dropna(subset=["ev_confidence", "realized_return"]).copy()
    if frame.empty:
        return [], {
            "bucket_count": 0,
            "top_vs_bottom_realized_return_spread": 0.0,
        }
    bucket_count = min(5, len(frame.index))
    if frame["ev_confidence"].nunique(dropna=False) <= 1:
        frame["confidence_bucket"] = "q1"
    else:
        rank = frame["ev_confidence"].rank(method="first")
        buckets = pd.qcut(rank, q=bucket_count, labels=False, duplicates="drop")
        frame["confidence_bucket"] = buckets.fillna(0).astype(int).map(lambda value: f"q{int(value) + 1}")
    frame["positive_realized_return"] = (frame["realized_return"] > 0.0).astype(float)
    grouped = (
        frame.groupby("confidence_bucket", dropna=False)
        .agg(
            trade_count=("realized_return", "count"),
            avg_confidence=("ev_confidence", "mean"),
            avg_predicted_ev=("predicted_ev", "mean"),
            avg_realized_return=("realized_return", "mean"),
            hit_rate=("positive_realized_return", "mean"),
            pnl_contribution=("realized_return", "sum"),
        )
        .reset_index()
        .sort_values("confidence_bucket", kind="stable")
    )
    rows = _normalize_records(grouped.to_dict(orient="records"))
    top_bucket = grouped.iloc[-1] if not grouped.empty else None
    bottom_bucket = grouped.iloc[0] if not grouped.empty else None
    summary = {
        "bucket_count": int(len(grouped.index)),
        "top_vs_bottom_realized_return_spread": (
            float(top_bucket["avg_realized_return"] - bottom_bucket["avg_realized_return"])
            if top_bucket is not None and bottom_bucket is not None
            else 0.0
        ),
    }
    return rows, summary


def _build_execution_confidence_rows(*, replay_root: str | Path) -> list[dict[str, Any]]:
    root = Path(replay_root)
    lifecycle_frame = _read_csv_frame(root / "replay_trade_ev_lifecycle.csv")
    if lifecycle_frame.empty:
        lifecycle_rows, _ = aggregate_replay_ev_lifecycle(replay_root=root)
        lifecycle_frame = pd.DataFrame(lifecycle_rows)
    if lifecycle_frame.empty:
        return []
    lifecycle_lookup: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in lifecycle_frame.astype(object).where(pd.notna(lifecycle_frame), None).to_dict(orient="records"):
        key = (
            str(row.get("entry_date") or ""),
            str(row.get("symbol") or ""),
            str(row.get("strategy_id") or ""),
        )
        lifecycle_lookup.setdefault(key, []).append(dict(row))
    rows: list[dict[str, Any]] = []
    for day_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        candidate_frame = _read_csv_frame(day_dir / "paper" / "trade_candidate_dataset.csv")
        if candidate_frame.empty:
            continue
        candidate_frame = candidate_frame[
            candidate_frame.get("candidate_outcome", pd.Series(dtype=str)).astype(str) == "executed"
        ].copy()
        if candidate_frame.empty:
            continue
        for candidate_row in candidate_frame.astype(object).where(pd.notna(candidate_frame), None).to_dict(orient="records"):
            lookup_key = (
                str(candidate_row.get("date") or ""),
                str(candidate_row.get("symbol") or ""),
                str(candidate_row.get("strategy_id") or ""),
            )
            lifecycle_rows = lifecycle_lookup.get(lookup_key) or lifecycle_lookup.get(
                (lookup_key[0], lookup_key[1], "")
            ) or []
            if not lifecycle_rows:
                continue
            lifecycle_row = lifecycle_rows.pop(0)
            predicted_ev = candidate_row.get("regression_raw_ev_score")
            if predicted_ev is None:
                predicted_ev = candidate_row.get("raw_ev_score", candidate_row.get("expected_net_return"))
            rows.append(
                {
                    "trade_id": lifecycle_row.get("trade_id"),
                    "entry_date": lifecycle_row.get("entry_date"),
                    "exit_date": lifecycle_row.get("exit_date"),
                    "symbol": candidate_row.get("symbol"),
                    "strategy_id": candidate_row.get("strategy_id"),
                    "signal_family": candidate_row.get("signal_family"),
                    "score_entry": candidate_row.get("signal_score"),
                    "score_percentile_entry": candidate_row.get("score_percentile"),
                    "expected_horizon_days": candidate_row.get("expected_horizon_days"),
                    "recent_return_3d": candidate_row.get("recent_return_3d"),
                    "recent_return_5d": candidate_row.get("recent_return_5d"),
                    "recent_return_10d": candidate_row.get("recent_return_10d"),
                    "recent_vol_20d": candidate_row.get("recent_vol_20d"),
                    "predicted_ev": _safe_float(predicted_ev),
                    "ev_confidence": _safe_float(candidate_row.get("ev_confidence")),
                    "ev_confidence_multiplier": (
                        1.0
                        if candidate_row.get("ev_confidence_multiplier") is None
                        else _safe_float(candidate_row.get("ev_confidence_multiplier"))
                    ),
                    "residual_std_bucket": _safe_float(candidate_row.get("residual_std_bucket")),
                    "residual_std_global": _safe_float(candidate_row.get("residual_std_global")),
                    "residual_std_final": _safe_float(candidate_row.get("residual_std_final")),
                    "sample_size_used": int(_safe_float(candidate_row.get("sample_size_used"))),
                    "residual_std_confidence": _safe_float(candidate_row.get("residual_std_confidence")),
                    "magnitude_confidence": _safe_float(candidate_row.get("magnitude_confidence")),
                    "model_performance_confidence": _safe_float(candidate_row.get("model_performance_confidence")),
                    "combined_confidence": _safe_float(candidate_row.get("combined_confidence")),
                    "ev_score_before_confidence": _safe_float(candidate_row.get("ev_score_before_confidence")),
                    "ev_score_after_confidence": _safe_float(candidate_row.get("ev_score_after_confidence")),
                    "residual_std_used": _safe_float(candidate_row.get("residual_std_final")),
                    "confidence_source": candidate_row.get("confidence_source"),
                    "realized_return": _safe_float(lifecycle_row.get("realized_return")),
                    "prediction_available": True,
                    "prediction_reason": "execution_candidate_join",
                    "training_sample_count": int(_safe_float(candidate_row.get("ev_training_sample_count"))),
                }
            )
    return _normalize_records(rows)


def save_trade_ev_regression_model(*, model: dict[str, Any], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        pickle.dump(model, handle)
    return path


def load_trade_ev_regression_model(path: str | Path) -> dict[str, Any]:
    with Path(path).open("rb") as handle:
        return pickle.load(handle)


def run_replay_trade_ev_regression(
    *,
    replay_root: str | Path,
    model_output_path: str | Path,
    expected_horizon_days: int,
    min_training_samples: int = 20,
    ridge_alpha: float = 1.0,
    use_confidence_weighting: bool = False,
    confidence_scale: float = 1.0,
    confidence_clip_min: float = 0.5,
    confidence_clip_max: float = 1.5,
    confidence_min_samples_per_bucket: int = 20,
    confidence_shrinkage_enabled: bool = True,
    confidence_component_residual_std_weight: float = 1.0,
    confidence_component_magnitude_weight: float = 0.0,
    confidence_component_model_performance_weight: float = 0.0,
) -> dict[str, Any]:
    dataset_rows, dataset_summary = build_trade_ev_regression_dataset(
        replay_root=replay_root,
        expected_horizon_days=expected_horizon_days,
    )
    ordered_rows = sorted(
        dataset_rows,
        key=lambda row: (
            str(row.get("entry_date") or ""),
            str(row.get("trade_id") or ""),
        ),
    )
    walkforward_predictions: list[dict[str, Any]] = []
    for row in ordered_rows:
        current_entry_date = pd.Timestamp(str(row.get("entry_date"))).date()
        training_rows = [
            candidate
            for candidate in ordered_rows
            if pd.Timestamp(str(candidate.get("exit_date"))).date() < current_entry_date
        ]
        model = train_trade_ev_regression_model(
            training_rows=training_rows,
            min_training_samples=min_training_samples,
            ridge_alpha=ridge_alpha,
            confidence_min_samples_per_bucket=confidence_min_samples_per_bucket,
            confidence_shrinkage_enabled=confidence_shrinkage_enabled,
        )
        predicted_rows = score_trade_ev_regression_candidates(
            model=model,
            candidate_rows=[
                {
                    "symbol": row.get("symbol"),
                    "strategy_id": row.get("strategy_id"),
                    "signal_family": row.get("signal_family"),
                    "signal_score": row.get("score_entry"),
                    "score_percentile": row.get("score_percentile_entry"),
                    "expected_horizon_days": row.get("expected_horizon_days"),
                    "recent_return_3d": row.get("recent_return_3d"),
                    "recent_return_5d": row.get("recent_return_5d"),
                    "recent_return_10d": row.get("recent_return_10d"),
                    "recent_vol_20d": row.get("recent_vol_20d"),
                    "estimated_execution_cost_pct": 0.0,
                    "weight_delta": 0.0,
                }
            ],
            use_confidence_weighting=use_confidence_weighting,
            confidence_scale=confidence_scale,
            confidence_clip_min=confidence_clip_min,
            confidence_clip_max=confidence_clip_max,
            confidence_min_samples_per_bucket=confidence_min_samples_per_bucket,
            confidence_shrinkage_enabled=confidence_shrinkage_enabled,
            confidence_component_residual_std_weight=confidence_component_residual_std_weight,
            confidence_component_magnitude_weight=confidence_component_magnitude_weight,
            confidence_component_model_performance_weight=confidence_component_model_performance_weight,
            normalize_scores=False,
            use_normalized_score_for_weighting=False,
        )
        predicted_rows = [{**dict(predicted_rows[0]), **row}] if predicted_rows else []
        walkforward_predictions.extend(predicted_rows)
    prediction_rows, evaluation_summary = evaluate_trade_ev_regression_predictions(
        prediction_rows=walkforward_predictions,
    )
    execution_confidence_rows = _build_execution_confidence_rows(replay_root=replay_root)
    confidence_input_rows = execution_confidence_rows or prediction_rows
    confidence_rows, confidence_summary = build_trade_ev_confidence_summary(
        prediction_rows=confidence_input_rows,
    )
    confidence_bucket_rows, confidence_bucket_summary = build_trade_ev_confidence_bucket_analysis(
        confidence_rows=confidence_rows,
    )
    final_model = train_trade_ev_regression_model(
        training_rows=ordered_rows,
        min_training_samples=min_training_samples,
        ridge_alpha=ridge_alpha,
        confidence_min_samples_per_bucket=confidence_min_samples_per_bucket,
        confidence_shrinkage_enabled=confidence_shrinkage_enabled,
    )
    model_path = save_trade_ev_regression_model(model=final_model, output_path=model_output_path)
    root = Path(replay_root)
    predictions_path = root / "replay_trade_ev_regression_predictions.csv"
    bucket_path = root / "replay_trade_ev_regression_buckets.csv"
    confidence_path = root / "replay_trade_ev_confidence.csv"
    confidence_bucket_path = root / "replay_ev_confidence_bucket_analysis.csv"
    summary_path = root / "replay_ev_regression_summary.json"
    confidence_summary_path = root / "replay_ev_confidence_summary.json"
    pd.DataFrame(prediction_rows, columns=REGRESSION_PREDICTION_COLUMNS).to_csv(predictions_path, index=False)
    pd.DataFrame(evaluation_summary.get("bucket_rows") or []).to_csv(bucket_path, index=False)
    pd.DataFrame(confidence_rows).to_csv(confidence_path, index=False)
    pd.DataFrame(confidence_bucket_rows).to_csv(confidence_bucket_path, index=False)
    summary = {
        **dataset_summary,
        **{key: value for key, value in evaluation_summary.items() if key != "bucket_rows"},
        **confidence_summary,
        **confidence_bucket_summary,
        "confidence_source": (
            "execution_candidate_join" if execution_confidence_rows else "walkforward_regression_predictions"
        ),
        "execution_confidence_row_count": int(len(execution_confidence_rows)),
        "model_path": str(model_path),
        "model_training_sample_count": int(final_model.get("training_sample_count", 0) or 0),
        "model_training_available": bool(final_model.get("training_available", False)),
        "model_type": "regression",
    }
    summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    confidence_summary_path.write_text(json.dumps(confidence_summary, indent=2, default=str), encoding="utf-8")
    return {
        "prediction_rows": prediction_rows,
        "bucket_rows": list(evaluation_summary.get("bucket_rows") or []),
        "confidence_rows": confidence_rows,
        "confidence_bucket_rows": confidence_bucket_rows,
        "summary": summary,
        "artifact_paths": {
            "replay_trade_ev_regression_predictions_path": predictions_path,
            "replay_trade_ev_regression_buckets_path": bucket_path,
            "replay_trade_ev_confidence_path": confidence_path,
            "replay_ev_confidence_bucket_analysis_path": confidence_bucket_path,
            "replay_ev_regression_summary_path": summary_path,
            "replay_ev_confidence_summary_path": confidence_summary_path,
            "ev_regression_model_path": model_path,
        },
    }

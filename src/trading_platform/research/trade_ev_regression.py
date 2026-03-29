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


def train_trade_ev_regression_model(
    *,
    training_rows: list[dict[str, Any]],
    min_training_samples: int = 20,
    ridge_alpha: float = 1.0,
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


def score_trade_ev_regression_candidates(
    *,
    model: dict[str, Any],
    candidate_rows: list[dict[str, Any]],
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
        output_rows.append(
            {
                **row,
                "expected_gross_return": raw_score + _safe_float(row.get("estimated_execution_cost_pct")),
                "expected_net_return": raw_score,
                "expected_cost": _safe_float(row.get("estimated_execution_cost_pct")),
                "regression_raw_ev_score": raw_score,
                "regression_normalized_ev_score": normalized_score,
                "regression_ev_score_post_clip": score_post_clip,
                "raw_ev_score": raw_score,
                "normalized_ev_score": normalized_score,
                "ev_score_pre_clip": float(normalized_row["ev_score_pre_clip"]) if normalized_row is not None else raw_score,
                "ev_score_post_clip": score_post_clip,
                "ev_score_clipped": bool(normalized_row["ev_score_clipped"]) if normalized_row is not None else False,
                "ev_weighting_score": score_post_clip if use_normalized_score_for_weighting else raw_score,
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
    return _normalize_records(frame[REGRESSION_PREDICTION_COLUMNS].to_dict(orient="records")), summary


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
        )
        predicted_rows = predict_trade_ev_regression(model=model, rows=[row])
        walkforward_predictions.extend(predicted_rows)
    prediction_rows, evaluation_summary = evaluate_trade_ev_regression_predictions(
        prediction_rows=walkforward_predictions,
    )
    final_model = train_trade_ev_regression_model(
        training_rows=ordered_rows,
        min_training_samples=min_training_samples,
        ridge_alpha=ridge_alpha,
    )
    model_path = save_trade_ev_regression_model(model=final_model, output_path=model_output_path)
    root = Path(replay_root)
    predictions_path = root / "replay_trade_ev_regression_predictions.csv"
    bucket_path = root / "replay_trade_ev_regression_buckets.csv"
    summary_path = root / "replay_ev_regression_summary.json"
    pd.DataFrame(prediction_rows, columns=REGRESSION_PREDICTION_COLUMNS).to_csv(predictions_path, index=False)
    pd.DataFrame(evaluation_summary.get("bucket_rows") or []).to_csv(bucket_path, index=False)
    summary = {
        **dataset_summary,
        **{key: value for key, value in evaluation_summary.items() if key != "bucket_rows"},
        "model_path": str(model_path),
        "model_training_sample_count": int(final_model.get("training_sample_count", 0) or 0),
        "model_training_available": bool(final_model.get("training_available", False)),
        "model_type": "regression",
    }
    summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    return {
        "prediction_rows": prediction_rows,
        "bucket_rows": list(evaluation_summary.get("bucket_rows") or []),
        "summary": summary,
        "artifact_paths": {
            "replay_trade_ev_regression_predictions_path": predictions_path,
            "replay_trade_ev_regression_buckets_path": bucket_path,
            "replay_ev_regression_summary_path": summary_path,
            "ev_regression_model_path": model_path,
        },
    }

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

RELIABILITY_NUMERIC_FEATURE_COLUMNS = [
    "predicted_return",
    "score_entry",
    "score_percentile_entry",
    "recent_return_3d",
    "recent_return_5d",
    "recent_return_10d",
    "recent_vol_20d",
    "candidate_rank_pct",
    "signal_dispersion",
    "recent_model_success_rate",
]

RELIABILITY_ROW_COLUMNS = [
    "trade_id",
    "entry_date",
    "exit_date",
    "symbol",
    "strategy_id",
    "signal_family",
    "score_entry",
    "score_percentile_entry",
    "score_bucket",
    "predicted_return",
    "realized_return",
    "realized_minus_predicted",
    "ev_success",
    "recent_return_3d",
    "recent_return_5d",
    "recent_return_10d",
    "recent_vol_20d",
    "candidate_rank_pct",
    "signal_dispersion",
    "day_of_week",
    "recent_model_success_rate",
    "ev_reliability",
    "ev_reliability_multiplier",
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


def _sigmoid(values: np.ndarray) -> np.ndarray:
    clipped = np.clip(values, -30.0, 30.0)
    return 1.0 / (1.0 + np.exp(-clipped))


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


def _all_replay_day_dirs(history_root: str | Path) -> list[Path]:
    root = Path(history_root)
    if not root.exists():
        return []
    rows: list[Path] = []
    for path in sorted(item for item in root.iterdir() if item.is_dir()):
        try:
            pd.Timestamp(path.name).date()
        except (TypeError, ValueError):
            continue
        rows.append(path)
    return rows


def _historical_day_dirs(history_root: str | Path, *, as_of_date: str) -> list[Path]:
    cutoff = pd.Timestamp(as_of_date).date()
    return [path for path in _all_replay_day_dirs(history_root) if pd.Timestamp(path.name).date() < cutoff]


def _execution_candidate_frame(history_root: str | Path, *, entry_date: str) -> pd.DataFrame:
    return _read_csv_frame(Path(history_root) / entry_date / "paper" / "trade_candidate_dataset.csv")


def _matching_candidate_row(
    *,
    candidate_frame: pd.DataFrame,
    entry_date: str,
    symbol: str,
    strategy_id: str,
) -> dict[str, Any] | None:
    if candidate_frame.empty:
        return None
    frame = candidate_frame.copy()
    if "candidate_outcome" in frame.columns:
        frame = frame[frame["candidate_outcome"].astype(str) == "executed"].copy()
    if frame.empty:
        return None
    date_series = frame.get("date", pd.Series([""] * len(frame.index), index=frame.index)).astype(str)
    symbol_series = frame.get("symbol", pd.Series([""] * len(frame.index), index=frame.index)).astype(str)
    strategy_series = frame.get("strategy_id", pd.Series([""] * len(frame.index), index=frame.index)).fillna("").astype(str)
    subset = frame[(date_series == str(entry_date)) & (symbol_series == str(symbol)) & (strategy_series == str(strategy_id))]
    if subset.empty and strategy_id:
        subset = frame[(date_series == str(entry_date)) & (symbol_series == str(symbol))]
    if subset.empty:
        return None
    return dict(subset.astype(object).where(pd.notna(subset), None).iloc[0].to_dict())


def _candidate_day_stats(candidate_frame: pd.DataFrame) -> dict[str, Any]:
    if candidate_frame.empty:
        return {"candidate_count": 0, "signal_dispersion": 0.0}
    scores = pd.to_numeric(candidate_frame.get("signal_score"), errors="coerce")
    candidate_count = int(len(candidate_frame.index))
    signal_dispersion = float(scores.std(ddof=0)) if scores.notna().any() else 0.0
    return {
        "candidate_count": candidate_count,
        "signal_dispersion": signal_dispersion,
    }


def _candidate_rank_pct(candidate_row: dict[str, Any], *, candidate_frame: pd.DataFrame) -> float:
    candidate_count = max(int(len(candidate_frame.index)), 1)
    score_rank = _safe_float(candidate_row.get("score_rank"))
    if score_rank > 0.0 and candidate_count > 1:
        return float(1.0 - ((score_rank - 1.0) / max(candidate_count - 1.0, 1.0)))
    score_percentile = candidate_row.get("score_percentile")
    if score_percentile is not None:
        return float(_safe_float(score_percentile))
    return 0.5


def _predicted_return_from_candidate(candidate_row: dict[str, Any]) -> float | None:
    for key in ("regression_raw_ev_score", "raw_ev_score", "expected_net_return"):
        value = candidate_row.get(key)
        if value is None:
            continue
        try:
            if pd.isna(value):
                continue
        except TypeError:
            pass
        return float(value)
    return None


def _ev_success(predicted_return: float, realized_return: float) -> int:
    if predicted_return > 0.0 and realized_return > 0.0:
        return 1
    if predicted_return < 0.0 and realized_return < 0.0:
        return 1
    return 0


def _attach_recent_success_rates(
    rows: list[dict[str, Any]],
    *,
    recent_window: int,
) -> list[dict[str, Any]]:
    if not rows:
        return []
    ordered = sorted(
        [dict(row) for row in rows],
        key=lambda row: (
            str(row.get("exit_date") or ""),
            str(row.get("entry_date") or ""),
            str(row.get("trade_id") or ""),
        ),
    )
    history: list[int] = []
    group_history: dict[str, list[int]] = {}
    output: list[dict[str, Any]] = []
    for row in ordered:
        group_key = f"{str(row.get('signal_family') or 'unknown')}|{str(row.get('score_bucket') or 'q1')}"
        global_recent = history[-max(int(recent_window), 1) :]
        group_recent = group_history.get(group_key, [])[-max(int(recent_window), 1) :]
        if group_recent:
            recent_rate = float(sum(group_recent) / len(group_recent))
        elif global_recent:
            recent_rate = float(sum(global_recent) / len(global_recent))
        else:
            recent_rate = 0.5
        row["recent_model_success_rate"] = recent_rate
        output.append(row)
        success = int(row.get("ev_success", 0) or 0)
        history.append(success)
        group_history.setdefault(group_key, []).append(success)
    return output


def build_trade_ev_reliability_history_dataset(
    *,
    history_root: str | Path | None,
    as_of_date: str,
    recent_window: int = 20,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    root = Path(history_root) if history_root else None
    if root is None or not root.exists():
        return [], {
            "row_count": 0,
            "labeled_row_count": 0,
            "positive_label_rate": 0.0,
            "cutoff_date": str(as_of_date),
        }
    lifecycle_rows: list[dict[str, Any]] = []
    for day_dir in _historical_day_dirs(root, as_of_date=as_of_date):
        frame = _read_csv_frame(day_dir / "paper" / "trade_ev_lifecycle.csv")
        if frame.empty:
            continue
        lifecycle_rows.extend(frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records"))
    candidate_frame_cache: dict[str, pd.DataFrame] = {}
    joined_rows: list[dict[str, Any]] = []
    for lifecycle_row in lifecycle_rows:
        entry_date = str(lifecycle_row.get("entry_date") or "")
        symbol = str(lifecycle_row.get("symbol") or "")
        strategy_id = str(lifecycle_row.get("strategy_id") or "")
        if not entry_date or not symbol:
            continue
        if entry_date not in candidate_frame_cache:
            candidate_frame_cache[entry_date] = _execution_candidate_frame(root, entry_date=entry_date)
        candidate_frame = candidate_frame_cache[entry_date]
        candidate_row = _matching_candidate_row(
            candidate_frame=candidate_frame,
            entry_date=entry_date,
            symbol=symbol,
            strategy_id=strategy_id,
        )
        if candidate_row is None:
            continue
        predicted_return = _predicted_return_from_candidate(candidate_row)
        if predicted_return is None:
            continue
        realized_return = _safe_float(lifecycle_row.get("realized_return"))
        stats = _candidate_day_stats(candidate_frame)
        score_percentile = _safe_float(candidate_row.get("score_percentile"))
        joined_rows.append(
            {
                "trade_id": lifecycle_row.get("trade_id"),
                "entry_date": entry_date,
                "exit_date": str(lifecycle_row.get("exit_date") or lifecycle_row.get("date") or ""),
                "symbol": symbol,
                "strategy_id": strategy_id,
                "signal_family": str(candidate_row.get("signal_family") or lifecycle_row.get("signal_family") or "unknown"),
                "score_entry": _safe_float(candidate_row.get("signal_score")),
                "score_percentile_entry": score_percentile,
                "score_bucket": _score_bucket(score_percentile),
                "predicted_return": float(predicted_return),
                "realized_return": realized_return,
                "realized_minus_predicted": float(realized_return - float(predicted_return)),
                "ev_success": int(_ev_success(float(predicted_return), realized_return)),
                "recent_return_3d": _safe_float(candidate_row.get("recent_return_3d")),
                "recent_return_5d": _safe_float(candidate_row.get("recent_return_5d")),
                "recent_return_10d": _safe_float(candidate_row.get("recent_return_10d")),
                "recent_vol_20d": _safe_float(candidate_row.get("recent_vol_20d")),
                "candidate_rank_pct": _candidate_rank_pct(candidate_row, candidate_frame=candidate_frame),
                "signal_dispersion": float(stats.get("signal_dispersion", 0.0)),
                "day_of_week": int(pd.Timestamp(entry_date).dayofweek),
            }
        )
    joined_rows = _attach_recent_success_rates(joined_rows, recent_window=recent_window)
    frame = pd.DataFrame(joined_rows)
    return _normalize_records(joined_rows), {
        "row_count": int(len(joined_rows)),
        "labeled_row_count": int(len(joined_rows)),
        "positive_label_rate": float(pd.to_numeric(frame.get("ev_success"), errors="coerce").fillna(0.0).mean())
        if not frame.empty
        else 0.0,
        "cutoff_date": str(as_of_date),
    }


def _design_matrix(frame: pd.DataFrame, *, signal_families: list[str], score_buckets: list[str], weekdays: list[int]) -> np.ndarray:
    numeric = frame.reindex(columns=RELIABILITY_NUMERIC_FEATURE_COLUMNS).apply(pd.to_numeric, errors="coerce").fillna(0.0)
    parts = [numeric.to_numpy(dtype=float)]
    signal_series = frame.get("signal_family", pd.Series(["unknown"] * len(frame.index), index=frame.index)).astype(str)
    for family in signal_families:
        parts.append((signal_series == family).astype(float).to_numpy(dtype=float).reshape(-1, 1))
    bucket_series = frame.get("score_bucket", pd.Series(["q1"] * len(frame.index), index=frame.index)).astype(str)
    for bucket in score_buckets:
        parts.append((bucket_series == bucket).astype(float).to_numpy(dtype=float).reshape(-1, 1))
    weekday_series = pd.to_numeric(frame.get("day_of_week"), errors="coerce").fillna(0).astype(int)
    for weekday in weekdays:
        parts.append((weekday_series == int(weekday)).astype(float).to_numpy(dtype=float).reshape(-1, 1))
    return np.hstack(parts) if parts else np.empty((len(frame.index), 0))


def train_trade_ev_reliability_model(
    *,
    training_rows: list[dict[str, Any]],
    min_training_samples: int = 20,
    ridge_alpha: float = 1.0,
    max_iter: int = 400,
    learning_rate: float = 0.1,
    recent_window: int = 20,
) -> dict[str, Any]:
    if len(training_rows) < int(min_training_samples):
        return {
            "model_type": "reliability_logistic",
            "training_available": False,
            "training_sample_count": int(len(training_rows)),
            "warnings": ["insufficient_history_for_reliability_model"],
        }
    frame = pd.DataFrame(training_rows).copy()
    signal_families = sorted({str(value or "unknown") for value in frame.get("signal_family", [])})
    score_buckets = sorted({str(value or "q1") for value in frame.get("score_bucket", [])})
    weekdays = sorted({int(_safe_float(value)) for value in frame.get("day_of_week", [])})
    x_raw = _design_matrix(frame, signal_families=signal_families, score_buckets=score_buckets, weekdays=weekdays)
    means = x_raw.mean(axis=0)
    stds = x_raw.std(axis=0, ddof=0)
    stds = np.where(stds <= 0.0, 1.0, stds)
    x = (x_raw - means) / stds
    y = pd.to_numeric(frame["ev_success"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    intercept = 0.0
    coefficients = np.zeros(x.shape[1], dtype=float)
    sample_count = max(len(frame.index), 1)
    for _ in range(max(int(max_iter), 1)):
        logits = intercept + (x @ coefficients)
        probabilities = _sigmoid(logits)
        error = probabilities - y
        intercept -= float(learning_rate) * float(error.mean())
        gradient = (x.T @ error) / sample_count + (float(ridge_alpha) * coefficients / sample_count)
        coefficients -= float(learning_rate) * gradient
    fitted = _sigmoid(intercept + (x @ coefficients))
    frame["predicted_probability"] = fitted
    global_recent_success_rate = float(y[-max(int(recent_window), 1) :].mean()) if len(y) else 0.5
    group_recent_success_rates: dict[str, dict[str, Any]] = {}
    for row in (
        frame.groupby(["signal_family", "score_bucket"], dropna=False)
        .agg(sample_count=("ev_success", "count"), success_rate=("ev_success", "mean"))
        .reset_index()
        .to_dict(orient="records")
    ):
        group_key = f"{str(row.get('signal_family') or 'unknown')}|{str(row.get('score_bucket') or 'q1')}"
        group_recent_success_rates[group_key] = {
            "sample_count": int(row.get("sample_count", 0) or 0),
            "success_rate": _safe_float(row.get("success_rate")),
        }
    return {
        "model_type": "reliability_logistic",
        "training_available": True,
        "training_sample_count": int(len(training_rows)),
        "feature_means": [float(value) for value in means],
        "feature_stds": [float(value) for value in stds],
        "intercept": float(intercept),
        "coefficients": [float(value) for value in coefficients],
        "signal_families": signal_families,
        "score_buckets": score_buckets,
        "weekdays": weekdays,
        "global_recent_success_rate": global_recent_success_rate,
        "group_recent_success_rates": group_recent_success_rates,
        "recent_window": int(recent_window),
        "warnings": [],
    }


def _current_recent_success_rate(model: dict[str, Any], *, signal_family: str, score_bucket: str) -> float:
    global_rate = _safe_float(model.get("global_recent_success_rate"))
    group_key = f"{signal_family}|{score_bucket}"
    group_stats = dict((model.get("group_recent_success_rates") or {}).get(group_key) or {})
    if not group_stats:
        return global_rate if global_rate > 0.0 else 0.5
    group_rate = _safe_float(group_stats.get("success_rate"))
    sample_count = int(group_stats.get("sample_count", 0) or 0)
    recent_window = max(int(model.get("recent_window", 20) or 20), 1)
    weight = float(min(1.0, sample_count / recent_window))
    blended = (weight * group_rate) + ((1.0 - weight) * global_rate)
    return blended if blended > 0.0 else 0.5


def _build_current_candidate_rows(
    *,
    candidate_rows: list[dict[str, Any]],
    model: dict[str, Any],
) -> list[dict[str, Any]]:
    if not candidate_rows:
        return []
    frame = pd.DataFrame(candidate_rows).copy()
    signal_scores = pd.to_numeric(frame.get("signal_score"), errors="coerce")
    signal_dispersion = float(signal_scores.std(ddof=0)) if signal_scores.notna().any() else 0.0
    candidate_count = max(len(frame.index), 1)
    rows: list[dict[str, Any]] = []
    for raw in frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records"):
        score_percentile = _safe_float(raw.get("score_percentile"))
        signal_family = str(raw.get("signal_family") or "unknown")
        score_bucket = _score_bucket(score_percentile)
        score_rank = _safe_float(raw.get("score_rank"))
        if score_rank > 0.0 and candidate_count > 1:
            candidate_rank_pct = float(1.0 - ((score_rank - 1.0) / max(candidate_count - 1.0, 1.0)))
        else:
            candidate_rank_pct = score_percentile if score_percentile > 0.0 else 0.5
        predicted_return = raw.get("regression_raw_ev_score", raw.get("raw_ev_score"))
        if predicted_return is None:
            predicted_return = raw.get("expected_net_return")
        rows.append(
            {
                "date": raw.get("date"),
                "symbol": raw.get("symbol"),
                "strategy_id": raw.get("strategy_id"),
                "signal_family": signal_family,
                "score_entry": _safe_float(raw.get("signal_score")),
                "score_percentile_entry": score_percentile,
                "score_bucket": score_bucket,
                "predicted_return": _safe_float(predicted_return),
                "recent_return_3d": _safe_float(raw.get("recent_return_3d")),
                "recent_return_5d": _safe_float(raw.get("recent_return_5d")),
                "recent_return_10d": _safe_float(raw.get("recent_return_10d")),
                "recent_vol_20d": _safe_float(raw.get("recent_vol_20d")),
                "candidate_rank_pct": candidate_rank_pct,
                "signal_dispersion": signal_dispersion,
                "day_of_week": int(pd.Timestamp(str(raw.get("date") or pd.Timestamp.utcnow().date())).dayofweek),
                "recent_model_success_rate": _current_recent_success_rate(
                    model,
                    signal_family=signal_family,
                    score_bucket=score_bucket,
                ),
            }
        )
    return rows


def score_trade_ev_reliability_candidates(
    *,
    model: dict[str, Any],
    candidate_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not candidate_rows:
        return []
    current_rows = _build_current_candidate_rows(candidate_rows=candidate_rows, model=model)
    if not bool(model.get("training_available", False)):
        return _normalize_records(
            [
                {
                    **row,
                    "ev_reliability": 0.5,
                    "ev_reliability_multiplier": 0.5,
                    "prediction_available": False,
                    "prediction_reason": "insufficient_history",
                    "training_sample_count": int(model.get("training_sample_count", 0) or 0),
                }
                for row in current_rows
            ]
        )
    frame = pd.DataFrame(current_rows)
    x_raw = _design_matrix(
        frame,
        signal_families=list(model.get("signal_families") or []),
        score_buckets=list(model.get("score_buckets") or []),
        weekdays=list(model.get("weekdays") or []),
    )
    means = np.array(model.get("feature_means") or [0.0] * x_raw.shape[1], dtype=float)
    stds = np.array(model.get("feature_stds") or [1.0] * x_raw.shape[1], dtype=float)
    stds = np.where(stds <= 0.0, 1.0, stds)
    x = (x_raw - means) / stds
    coefficients = np.array(model.get("coefficients") or [0.0] * x.shape[1], dtype=float)
    probabilities = _sigmoid(float(model.get("intercept", 0.0)) + (x @ coefficients))
    rows: list[dict[str, Any]] = []
    for index, row in enumerate(current_rows):
        probability = float(probabilities[index])
        rows.append(
            {
                **row,
                "ev_reliability": probability,
                "ev_reliability_multiplier": probability,
                "prediction_available": True,
                "prediction_reason": "reliability_logistic_model",
                "training_sample_count": int(model.get("training_sample_count", 0) or 0),
            }
        )
    return _normalize_records(rows)


def _bucket_label(series: pd.Series, bucket_count: int) -> pd.Series:
    filled = pd.to_numeric(series, errors="coerce").fillna(0.0)
    if filled.empty:
        return pd.Series(dtype=object)
    if filled.nunique(dropna=False) <= 1:
        return pd.Series(["q1"] * len(filled), index=filled.index)
    rank = filled.rank(method="first")
    buckets = pd.qcut(rank, q=min(bucket_count, len(filled)), labels=False, duplicates="drop")
    return buckets.fillna(0).astype(int).map(lambda value: f"q{int(value) + 1}")


def _build_execution_reliability_rows(*, replay_root: str | Path) -> list[dict[str, Any]]:
    root = Path(replay_root)
    lifecycle_path = root / "replay_trade_ev_lifecycle.csv"
    lifecycle_frame = _read_csv_frame(lifecycle_path)
    if lifecycle_frame.empty:
        from trading_platform.reporting.ev_lifecycle import aggregate_replay_ev_lifecycle

        lifecycle_rows, _ = aggregate_replay_ev_lifecycle(replay_root=root)
        lifecycle_frame = pd.DataFrame(lifecycle_rows)
    if lifecycle_frame.empty:
        return []
    candidate_cache: dict[str, pd.DataFrame] = {}
    rows: list[dict[str, Any]] = []
    for lifecycle_row in lifecycle_frame.astype(object).where(pd.notna(lifecycle_frame), None).to_dict(orient="records"):
        entry_date = str(lifecycle_row.get("entry_date") or "")
        symbol = str(lifecycle_row.get("symbol") or "")
        strategy_id = str(lifecycle_row.get("strategy_id") or "")
        if not entry_date or not symbol:
            continue
        if entry_date not in candidate_cache:
            candidate_cache[entry_date] = _execution_candidate_frame(root, entry_date=entry_date)
        candidate_frame = candidate_cache[entry_date]
        candidate_row = _matching_candidate_row(
            candidate_frame=candidate_frame,
            entry_date=entry_date,
            symbol=symbol,
            strategy_id=strategy_id,
        )
        if candidate_row is None:
            continue
        predicted_return = _predicted_return_from_candidate(candidate_row)
        if predicted_return is None:
            continue
        realized_return = _safe_float(lifecycle_row.get("realized_return"))
        reliability = candidate_row.get("ev_reliability")
        if reliability is None:
            continue
        rows.append(
            {
                "trade_id": lifecycle_row.get("trade_id"),
                "entry_date": entry_date,
                "exit_date": lifecycle_row.get("exit_date"),
                "symbol": symbol,
                "strategy_id": strategy_id,
                "signal_family": str(candidate_row.get("signal_family") or lifecycle_row.get("signal_family") or "unknown"),
                "predicted_return": float(predicted_return),
                "realized_return": realized_return,
                "realized_minus_predicted": float(realized_return - float(predicted_return)),
                "ev_success": int(_ev_success(float(predicted_return), realized_return)),
                "ev_reliability": _safe_float(reliability),
                "ev_reliability_multiplier": _safe_float(candidate_row.get("ev_reliability_multiplier")),
                "prediction_available": True,
                "prediction_reason": "execution_candidate_join",
                "training_sample_count": int(_safe_float(candidate_row.get("reliability_training_sample_count"))),
            }
        )
    return _normalize_records(rows)


def build_trade_ev_reliability_analysis(
    *,
    reliability_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not reliability_rows:
        return [], {
            "row_count": 0,
            "avg_ev_reliability": 0.0,
            "reliability_realized_return_correlation": 0.0,
            "reliability_success_correlation": 0.0,
            "calibration_error": 0.0,
            "top_vs_bottom_realized_return_spread": 0.0,
            "top_vs_bottom_hit_rate_spread": 0.0,
            "bucket_count": 0,
        }
    frame = pd.DataFrame(reliability_rows).copy()
    frame["ev_reliability"] = pd.to_numeric(frame.get("ev_reliability"), errors="coerce")
    frame["realized_return"] = pd.to_numeric(frame.get("realized_return"), errors="coerce")
    frame["predicted_return"] = pd.to_numeric(frame.get("predicted_return"), errors="coerce")
    frame["ev_success"] = pd.to_numeric(frame.get("ev_success"), errors="coerce")
    frame = frame.dropna(subset=["ev_reliability", "realized_return", "ev_success"]).copy()
    if frame.empty:
        return [], {
            "row_count": 0,
            "avg_ev_reliability": 0.0,
            "reliability_realized_return_correlation": 0.0,
            "reliability_success_correlation": 0.0,
            "calibration_error": 0.0,
            "top_vs_bottom_realized_return_spread": 0.0,
            "top_vs_bottom_hit_rate_spread": 0.0,
            "bucket_count": 0,
        }
    frame["reliability_bucket"] = _bucket_label(frame["ev_reliability"], 5)
    grouped = (
        frame.groupby("reliability_bucket", dropna=False)
        .agg(
            trade_count=("trade_id", "count"),
            avg_reliability=("ev_reliability", "mean"),
            avg_predicted_return=("predicted_return", "mean"),
            avg_realized_return=("realized_return", "mean"),
            hit_rate=("ev_success", "mean"),
            pnl_contribution=("realized_return", "sum"),
        )
        .reset_index()
        .sort_values("reliability_bucket", kind="stable")
    )
    rows = _normalize_records(grouped.to_dict(orient="records"))
    top_bucket = grouped.iloc[-1] if not grouped.empty else None
    bottom_bucket = grouped.iloc[0] if not grouped.empty else None
    summary = {
        "row_count": int(len(frame.index)),
        "avg_ev_reliability": float(frame["ev_reliability"].mean()),
        "reliability_realized_return_correlation": _safe_corr(
            frame["ev_reliability"],
            frame["realized_return"],
            method="spearman",
        ),
        "reliability_success_correlation": _safe_corr(
            frame["ev_reliability"],
            frame["ev_success"],
            method="spearman",
        ),
        "calibration_error": float((frame["ev_reliability"] - frame["ev_success"]).abs().mean()),
        "top_vs_bottom_realized_return_spread": (
            float(top_bucket["avg_realized_return"] - bottom_bucket["avg_realized_return"])
            if top_bucket is not None and bottom_bucket is not None
            else 0.0
        ),
        "top_vs_bottom_hit_rate_spread": (
            float(top_bucket["hit_rate"] - bottom_bucket["hit_rate"])
            if top_bucket is not None and bottom_bucket is not None
            else 0.0
        ),
        "bucket_count": int(len(grouped.index)),
    }
    return rows, summary


def run_replay_trade_ev_reliability(
    *,
    replay_root: str | Path,
) -> dict[str, Any]:
    root = Path(replay_root)
    reliability_rows = _build_execution_reliability_rows(replay_root=root)
    bucket_rows, summary = build_trade_ev_reliability_analysis(reliability_rows=reliability_rows)
    rows_path = root / "replay_trade_ev_reliability.csv"
    analysis_path = root / "replay_ev_reliability_analysis.csv"
    summary_path = root / "replay_ev_reliability_summary.json"
    pd.DataFrame(reliability_rows, columns=RELIABILITY_ROW_COLUMNS).to_csv(rows_path, index=False)
    pd.DataFrame(bucket_rows).to_csv(analysis_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    return {
        "rows": reliability_rows,
        "bucket_rows": bucket_rows,
        "summary": summary,
        "artifact_paths": {
            "replay_trade_ev_reliability_path": rows_path,
            "replay_ev_reliability_analysis_path": analysis_path,
            "replay_ev_reliability_summary_path": summary_path,
        },
    }

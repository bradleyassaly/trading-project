from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

RELIABILITY_NUMERIC_FEATURE_COLUMNS = [
    "predicted_return",
    "ev_weighting_score_entry",
    "target_weight_entry",
    "expected_horizon_days",
    "estimated_execution_cost_pct",
    "score_entry",
    "score_percentile_entry",
    "recent_return_3d",
    "recent_return_5d",
    "recent_return_10d",
    "recent_vol_20d",
    "candidate_rank_pct",
    "predicted_return_rank_pct",
    "signal_dispersion",
    "recent_model_hit_rate",
    "recent_symbol_trade_frequency",
    "recent_symbol_turnover",
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
    "ev_weighting_score_entry",
    "target_weight_entry",
    "expected_horizon_days",
    "estimated_execution_cost_pct",
    "recent_return_3d",
    "recent_return_5d",
    "recent_return_10d",
    "recent_vol_20d",
    "candidate_rank_pct",
    "predicted_return_rank_pct",
    "signal_dispersion",
    "day_of_week",
    "recent_model_hit_rate",
    "recent_symbol_trade_frequency",
    "recent_symbol_turnover",
    "realized_return_after_costs",
    "realized_minus_predicted_after_costs",
    "sign_success",
    "positive_net_realized_return",
    "top_bucket_realized_return",
    "positive_realized_minus_cost_hurdle",
    "reliability_target_value",
    "ev_reliability",
    "ev_reliability_rank_pct",
    "ev_reliability_multiplier",
    "reliability_target_type",
    "reliability_usage_mode",
    "prediction_available",
    "prediction_reason",
    "training_sample_count",
    "weight_delta",
    "reliability_turnover_delta",
    "reliability_cost_drag_delta",
    "was_reliability_promoted",
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


def _predicted_return_rank_pct(candidate_row: dict[str, Any], *, candidate_frame: pd.DataFrame) -> float:
    if candidate_frame.empty:
        return 0.5
    frame = candidate_frame.copy()
    frame["predicted_proxy"] = frame.apply(
        lambda row: _predicted_return_from_candidate(dict(row.astype(object).where(pd.notna(row), None))),
        axis=1,
    )
    frame["predicted_proxy"] = pd.to_numeric(frame["predicted_proxy"], errors="coerce")
    frame = frame.dropna(subset=["predicted_proxy"]).copy()
    if frame.empty:
        return 0.5
    frame = frame.sort_values(["predicted_proxy", "symbol"], ascending=[False, True], kind="stable")
    ordered_symbols = list(frame["symbol"].astype(str))
    try:
        rank = ordered_symbols.index(str(candidate_row.get("symbol"))) + 1
    except ValueError:
        return 0.5
    total = max(len(ordered_symbols), 1)
    if total <= 1:
        return 1.0
    return float(1.0 - ((rank - 1.0) / (total - 1.0)))


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


def _recent_symbol_activity(
    rows: list[dict[str, Any]],
    *,
    symbol: str,
    entry_date: str,
    recent_window: int,
) -> tuple[float, float]:
    cutoff = pd.Timestamp(entry_date)
    matches = [
        row
        for row in rows
        if str(row.get("symbol") or "") == str(symbol)
        and pd.Timestamp(str(row.get("entry_date") or row.get("date") or entry_date)) < cutoff
    ]
    matches = matches[-max(int(recent_window), 1) :]
    frequency = float(len(matches) / max(int(recent_window), 1)) if matches else 0.0
    turnover = float(sum(abs(_safe_float(row.get("target_weight_entry"))) for row in matches))
    return frequency, turnover


def _recent_family_hit_rate(
    rows: list[dict[str, Any]],
    *,
    signal_family: str,
    score_bucket: str,
    entry_date: str,
    recent_window: int,
) -> float:
    cutoff = pd.Timestamp(entry_date)
    grouped = [
        _safe_float(row.get("reliability_target_value"))
        for row in rows
        if str(row.get("signal_family") or "unknown") == str(signal_family)
        and str(row.get("score_bucket") or "q1") == str(score_bucket)
        and pd.Timestamp(str(row.get("entry_date") or row.get("date") or entry_date)) < cutoff
    ]
    fallback = [
        _safe_float(row.get("reliability_target_value"))
        for row in rows
        if pd.Timestamp(str(row.get("entry_date") or row.get("date") or entry_date)) < cutoff
    ]
    recent = grouped[-max(int(recent_window), 1) :] if grouped else fallback[-max(int(recent_window), 1) :]
    return float(sum(recent) / len(recent)) if recent else 0.5


def _apply_target_columns(
    frame: pd.DataFrame,
    *,
    top_percentile: float,
    hurdle: float,
) -> pd.DataFrame:
    updated = frame.copy()
    if updated.empty:
        for column in (
            "sign_success",
            "positive_net_realized_return",
            "top_bucket_realized_return",
            "positive_realized_minus_cost_hurdle",
            "top_bucket_realized_return_threshold",
        ):
            updated[column] = pd.Series(dtype=float if "threshold" in column else int)
        return updated
    realized = pd.to_numeric(
        updated.get("realized_return_after_costs", pd.Series(index=updated.index, dtype=float)),
        errors="coerce",
    ).fillna(0.0)
    predicted = pd.to_numeric(
        updated.get("predicted_return", pd.Series(index=updated.index, dtype=float)),
        errors="coerce",
    ).fillna(0.0)
    updated["sign_success"] = (((predicted > 0.0) & (realized > 0.0)) | ((predicted < 0.0) & (realized < 0.0))).astype(int)
    updated["positive_net_realized_return"] = (realized > 0.0).astype(int)
    threshold = float(realized.quantile(min(max(float(top_percentile), 0.0), 1.0))) if len(realized.index) else 0.0
    updated["top_bucket_realized_return"] = (realized >= threshold).astype(int)
    updated["positive_realized_minus_cost_hurdle"] = (realized >= float(hurdle)).astype(int)
    updated["top_bucket_realized_return_threshold"] = threshold
    return updated


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
    target_type: str = "sign_success",
    top_percentile: float = 0.8,
    hurdle: float = 0.0,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    root = Path(history_root) if history_root else None
    if root is None or not root.exists():
        return [], {
            "row_count": 0,
            "labeled_row_count": 0,
            "positive_label_rate": 0.0,
            "cutoff_date": str(as_of_date),
            "target_type": str(target_type),
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
                "ev_weighting_score_entry": _safe_float(candidate_row.get("ev_weighting_score")),
                "target_weight_entry": _safe_float(
                    candidate_row.get("requested_target_weight", candidate_row.get("target_weight"))
                ),
                "expected_horizon_days": int(_safe_float(candidate_row.get("expected_horizon_days")) or 5),
                "estimated_execution_cost_pct": _safe_float(candidate_row.get("estimated_execution_cost_pct")),
                "recent_return_3d": _safe_float(candidate_row.get("recent_return_3d")),
                "recent_return_5d": _safe_float(candidate_row.get("recent_return_5d")),
                "recent_return_10d": _safe_float(candidate_row.get("recent_return_10d")),
                "recent_vol_20d": _safe_float(candidate_row.get("recent_vol_20d")),
                "candidate_rank_pct": _candidate_rank_pct(candidate_row, candidate_frame=candidate_frame),
                "predicted_return_rank_pct": _predicted_return_rank_pct(
                    candidate_row,
                    candidate_frame=candidate_frame,
                ),
                "signal_dispersion": float(stats.get("signal_dispersion", 0.0)),
                "day_of_week": int(pd.Timestamp(entry_date).dayofweek),
                "realized_return_after_costs": realized_return,
                "realized_minus_predicted_after_costs": float(realized_return - float(predicted_return)),
            }
        )
    frame = _apply_target_columns(pd.DataFrame(joined_rows), top_percentile=top_percentile, hurdle=hurdle)
    rows = frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")
    enriched_rows: list[dict[str, Any]] = []
    for row in rows:
        row = dict(row)
        trade_frequency, symbol_turnover = _recent_symbol_activity(
            enriched_rows,
            symbol=str(row.get("symbol") or ""),
            entry_date=str(row.get("entry_date") or ""),
            recent_window=recent_window,
        )
        row["recent_symbol_trade_frequency"] = trade_frequency
        row["recent_symbol_turnover"] = symbol_turnover
        row["recent_model_hit_rate"] = _recent_family_hit_rate(
            enriched_rows,
            signal_family=str(row.get("signal_family") or "unknown"),
            score_bucket=str(row.get("score_bucket") or "q1"),
            entry_date=str(row.get("entry_date") or ""),
            recent_window=recent_window,
        )
        row["reliability_target_value"] = int(_safe_float(row.get(str(target_type))))
        enriched_rows.append(row)
    frame = pd.DataFrame(enriched_rows)
    return _normalize_records(enriched_rows), {
        "row_count": int(len(enriched_rows)),
        "labeled_row_count": int(len(enriched_rows)),
        "positive_label_rate": float(pd.to_numeric(frame.get("reliability_target_value"), errors="coerce").fillna(0.0).mean())
        if not frame.empty
        else 0.0,
        "cutoff_date": str(as_of_date),
        "target_type": str(target_type),
        "top_bucket_realized_return_threshold": float(
            frame.get("top_bucket_realized_return_threshold", pd.Series([0.0])).iloc[0] or 0.0
        )
        if not frame.empty
        else 0.0,
        "hurdle": float(hurdle),
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
    target_type: str = "sign_success",
) -> dict[str, Any]:
    if len(training_rows) < int(min_training_samples):
        return {
            "model_type": "reliability_logistic",
            "training_available": False,
            "training_sample_count": int(len(training_rows)),
            "warnings": ["insufficient_history_for_reliability_model"],
            "target_type": str(target_type),
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
    y = pd.to_numeric(frame["reliability_target_value"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
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
        "recent_window": int(recent_window),
        "target_type": str(target_type),
        "warnings": [],
    }


def _build_current_candidate_rows(
    *,
    candidate_rows: list[dict[str, Any]],
    training_rows: list[dict[str, Any]],
    recent_window: int,
) -> list[dict[str, Any]]:
    if not candidate_rows:
        return []
    frame = pd.DataFrame(candidate_rows).copy()
    signal_scores = pd.to_numeric(frame.get("signal_score"), errors="coerce")
    signal_dispersion = float(signal_scores.std(ddof=0)) if signal_scores.notna().any() else 0.0
    candidate_count = max(len(frame.index), 1)
    predicted_series = pd.to_numeric(
        frame.get("regression_raw_ev_score", frame.get("raw_ev_score", frame.get("expected_net_return"))),
        errors="coerce",
    )
    prediction_ranks = predicted_series.rank(method="first", ascending=False)
    rows: list[dict[str, Any]] = []
    for index, raw in enumerate(frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")):
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
        predicted_rank = float(prediction_ranks.iloc[index]) if len(prediction_ranks.index) > index else 1.0
        predicted_rank_pct = (
            float(1.0 - ((predicted_rank - 1.0) / max(candidate_count - 1.0, 1.0))) if candidate_count > 1 else 1.0
        )
        trade_frequency, symbol_turnover = _recent_symbol_activity(
            training_rows,
            symbol=str(raw.get("symbol") or ""),
            entry_date=str(raw.get("date") or pd.Timestamp.utcnow().date()),
            recent_window=recent_window,
        )
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
                "ev_weighting_score_entry": _safe_float(raw.get("ev_weighting_score")),
                "target_weight_entry": _safe_float(raw.get("requested_target_weight", raw.get("target_weight"))),
                "expected_horizon_days": int(_safe_float(raw.get("expected_horizon_days")) or 5),
                "estimated_execution_cost_pct": _safe_float(raw.get("estimated_execution_cost_pct")),
                "recent_return_3d": _safe_float(raw.get("recent_return_3d")),
                "recent_return_5d": _safe_float(raw.get("recent_return_5d")),
                "recent_return_10d": _safe_float(raw.get("recent_return_10d")),
                "recent_vol_20d": _safe_float(raw.get("recent_vol_20d")),
                "candidate_rank_pct": candidate_rank_pct,
                "predicted_return_rank_pct": predicted_rank_pct,
                "signal_dispersion": signal_dispersion,
                "day_of_week": int(pd.Timestamp(str(raw.get("date") or pd.Timestamp.utcnow().date())).dayofweek),
                "recent_model_hit_rate": _recent_family_hit_rate(
                    training_rows,
                    signal_family=signal_family,
                    score_bucket=score_bucket,
                    entry_date=str(raw.get("date") or pd.Timestamp.utcnow().date()),
                    recent_window=recent_window,
                ),
                "recent_symbol_trade_frequency": trade_frequency,
                "recent_symbol_turnover": symbol_turnover,
            }
        )
    return rows


def score_trade_ev_reliability_candidates(
    *,
    model: dict[str, Any],
    candidate_rows: list[dict[str, Any]],
    training_rows: list[dict[str, Any]] | None = None,
    usage_mode: str = "weighting_only",
    threshold: float = 0.5,
    weight_multiplier_min: float = 0.75,
    weight_multiplier_max: float = 1.25,
    neutral_band: float = 0.05,
    max_promoted_trades_per_day: int | None = None,
    recent_window: int = 20,
    target_type: str = "sign_success",
) -> list[dict[str, Any]]:
    if not candidate_rows:
        return []
    current_rows = _build_current_candidate_rows(
        candidate_rows=candidate_rows,
        training_rows=list(training_rows or []),
        recent_window=recent_window,
    )
    if not bool(model.get("training_available", False)):
        return _normalize_records(
            [
                {
                    **row,
                    "ev_reliability": 0.5,
                    "ev_reliability_rank_pct": 0.5,
                    "ev_reliability_multiplier": 1.0,
                    "prediction_available": False,
                    "prediction_reason": "insufficient_history",
                    "training_sample_count": int(model.get("training_sample_count", 0) or 0),
                    "reliability_target_type": str(target_type),
                    "reliability_usage_mode": str(usage_mode),
                    "was_reliability_promoted": False,
                    "was_filtered_by_reliability": False,
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
    probabilities = pd.Series(_sigmoid(float(model.get("intercept", 0.0)) + (x @ coefficients)), index=frame.index)
    rank_pct = _reliability_rank_pct(probabilities)
    promoted_indices: set[int] = set()
    if max_promoted_trades_per_day is not None and int(max_promoted_trades_per_day) >= 0:
        promoted_indices = set(
            probabilities.sort_values(ascending=False).head(int(max_promoted_trades_per_day)).index.to_list()
        )
    rows: list[dict[str, Any]] = []
    for index, row in enumerate(current_rows):
        probability = float(probabilities.iloc[index])
        reliability_rank = float(rank_pct.iloc[index])
        multiplier = 1.0
        promoted = False
        filtered = False
        if usage_mode in {"weighting_only", "hybrid"} and abs(probability - 0.5) > float(neutral_band):
            multiplier = float(np.clip(1.0 + (probability - 0.5), weight_multiplier_min, weight_multiplier_max))
            promoted = multiplier > 1.0
        if max_promoted_trades_per_day is not None and promoted and index not in promoted_indices:
            multiplier = 1.0
            promoted = False
        if usage_mode == "filtering_only" and probability < float(threshold):
            filtered = True
        elif usage_mode == "reranking_only" and reliability_rank < float(threshold):
            filtered = True
        elif usage_mode == "hybrid" and reliability_rank < float(threshold):
            filtered = True
        rows.append(
            {
                **row,
                "ev_reliability": probability,
                "ev_reliability_rank_pct": reliability_rank,
                "ev_reliability_multiplier": multiplier,
                "prediction_available": True,
                "prediction_reason": "reliability_logistic_model",
                "training_sample_count": int(model.get("training_sample_count", 0) or 0),
                "reliability_target_type": str(target_type),
                "reliability_usage_mode": str(usage_mode),
                "was_reliability_promoted": promoted,
                "was_filtered_by_reliability": filtered,
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


def _reliability_rank_pct(probabilities: pd.Series) -> pd.Series:
    if probabilities.empty:
        return pd.Series(dtype=float)
    if probabilities.nunique(dropna=False) <= 1:
        return pd.Series([1.0] * len(probabilities.index), index=probabilities.index)
    ranks = probabilities.rank(method="first", ascending=False)
    if len(probabilities.index) <= 1:
        return pd.Series([1.0] * len(probabilities.index), index=probabilities.index)
    return 1.0 - ((ranks - 1.0) / max(len(probabilities.index) - 1.0, 1.0))


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
                "realized_return_after_costs": realized_return,
                "realized_minus_predicted_after_costs": float(realized_return - float(predicted_return)),
                "sign_success": int(_ev_success(float(predicted_return), realized_return)),
                "positive_net_realized_return": int(realized_return > 0.0),
                "ev_reliability": _safe_float(reliability),
                "ev_reliability_rank_pct": _safe_float(candidate_row.get("ev_reliability_rank_pct")),
                "ev_reliability_multiplier": _safe_float(candidate_row.get("ev_reliability_multiplier")),
                "estimated_execution_cost_pct": _safe_float(candidate_row.get("estimated_execution_cost_pct")),
                "weight_delta": abs(_safe_float(candidate_row.get("weight_delta"))),
                "reliability_turnover_delta": _safe_float(candidate_row.get("reliability_turnover_delta")),
                "reliability_cost_drag_delta": _safe_float(candidate_row.get("reliability_cost_drag_delta")),
                "was_reliability_promoted": bool(candidate_row.get("was_reliability_promoted", False)),
                "prediction_available": True,
                "prediction_reason": "execution_candidate_join",
                "training_sample_count": int(_safe_float(candidate_row.get("reliability_training_sample_count"))),
            }
        )
    return _normalize_records(rows)


def build_trade_ev_reliability_analysis(
    *,
    reliability_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    if not reliability_rows:
        return [], [], {
            "row_count": 0,
            "avg_ev_reliability": 0.0,
            "reliability_after_cost_correlation": 0.0,
            "reliability_success_correlation": 0.0,
            "reliability_top_vs_bottom_after_cost_spread": 0.0,
            "reliability_turnover_uplift": 0.0,
            "reliability_cost_drag_uplift": 0.0,
            "bucket_count": 0,
        }
    frame = pd.DataFrame(reliability_rows).copy()
    frame["ev_reliability"] = pd.to_numeric(frame.get("ev_reliability"), errors="coerce")
    frame["realized_return_after_costs"] = pd.to_numeric(frame.get("realized_return_after_costs"), errors="coerce")
    frame["estimated_execution_cost_pct"] = pd.to_numeric(
        frame.get("estimated_execution_cost_pct"), errors="coerce"
    ).fillna(0.0)
    frame["weight_delta"] = pd.to_numeric(frame.get("weight_delta"), errors="coerce").fillna(0.0)
    frame["reliability_turnover_delta"] = pd.to_numeric(
        frame.get("reliability_turnover_delta"), errors="coerce"
    ).fillna(0.0)
    frame["reliability_cost_drag_delta"] = pd.to_numeric(
        frame.get("reliability_cost_drag_delta"), errors="coerce"
    ).fillna(0.0)
    frame = frame.dropna(subset=["ev_reliability", "realized_return_after_costs"]).copy()
    if frame.empty:
        return [], [], {
            "row_count": 0,
            "avg_ev_reliability": 0.0,
            "reliability_after_cost_correlation": 0.0,
            "reliability_success_correlation": 0.0,
            "reliability_top_vs_bottom_after_cost_spread": 0.0,
            "reliability_turnover_uplift": 0.0,
            "reliability_cost_drag_uplift": 0.0,
            "bucket_count": 0,
        }
    frame["hit_rate_after_costs"] = (frame["realized_return_after_costs"] > 0.0).astype(float)
    frame["reliability_bucket"] = _bucket_label(frame["ev_reliability"], 5)
    grouped = (
        frame.groupby("reliability_bucket", dropna=False)
        .agg(
            trade_count=("trade_id", "count"),
            avg_reliability=("ev_reliability", "mean"),
            avg_predicted_return=("predicted_return", "mean"),
            avg_realized_return_after_costs=("realized_return_after_costs", "mean"),
            hit_rate_after_costs=("hit_rate_after_costs", "mean"),
            pnl_contribution=("realized_return_after_costs", "sum"),
            turnover_contribution=("weight_delta", "sum"),
            avg_execution_cost_pct=("estimated_execution_cost_pct", "mean"),
        )
        .reset_index()
        .sort_values("reliability_bucket", kind="stable")
    )
    turnover_rows = (
        frame.groupby("reliability_bucket", dropna=False)
        .agg(
            trade_count=("trade_id", "count"),
            turnover_contribution=("weight_delta", "sum"),
            reliability_turnover_delta=("reliability_turnover_delta", "sum"),
            reliability_cost_drag_delta=("reliability_cost_drag_delta", "sum"),
            promoted_trade_count=("was_reliability_promoted", "sum"),
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
        "reliability_after_cost_correlation": _safe_corr(
            frame["ev_reliability"],
            frame["realized_return_after_costs"],
            method="spearman",
        ),
        "reliability_success_correlation": _safe_corr(
            frame["ev_reliability"],
            frame["hit_rate_after_costs"],
            method="spearman",
        ),
        "reliability_top_vs_bottom_after_cost_spread": (
            float(top_bucket["avg_realized_return_after_costs"] - bottom_bucket["avg_realized_return_after_costs"])
            if top_bucket is not None and bottom_bucket is not None
            else 0.0
        ),
        "reliability_turnover_uplift": float(frame["reliability_turnover_delta"].sum()),
        "reliability_cost_drag_uplift": float(frame["reliability_cost_drag_delta"].sum()),
        "bucket_count": int(len(grouped.index)),
    }
    return rows, _normalize_records(turnover_rows.to_dict(orient="records")), summary


def run_replay_trade_ev_reliability(
    *,
    replay_root: str | Path,
) -> dict[str, Any]:
    root = Path(replay_root)
    reliability_rows = _build_execution_reliability_rows(replay_root=root)
    bucket_rows, turnover_rows, summary = build_trade_ev_reliability_analysis(reliability_rows=reliability_rows)
    rows_path = root / "replay_trade_ev_reliability.csv"
    analysis_path = root / "replay_ev_reliability_analysis.csv"
    economic_path = root / "replay_ev_reliability_economic_analysis.csv"
    turnover_path = root / "replay_ev_reliability_turnover_analysis.csv"
    summary_path = root / "replay_ev_reliability_summary.json"
    pd.DataFrame(reliability_rows, columns=RELIABILITY_ROW_COLUMNS).to_csv(rows_path, index=False)
    pd.DataFrame(bucket_rows).to_csv(analysis_path, index=False)
    pd.DataFrame(bucket_rows).to_csv(economic_path, index=False)
    pd.DataFrame(turnover_rows).to_csv(turnover_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    return {
        "rows": reliability_rows,
        "bucket_rows": bucket_rows,
        "turnover_rows": turnover_rows,
        "summary": summary,
        "artifact_paths": {
            "replay_trade_ev_reliability_path": rows_path,
            "replay_ev_reliability_analysis_path": analysis_path,
            "replay_ev_reliability_economic_analysis_path": economic_path,
            "replay_ev_reliability_turnover_analysis_path": turnover_path,
            "replay_ev_reliability_summary_path": summary_path,
        },
    }

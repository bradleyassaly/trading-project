from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from trading_platform.signals.loaders import load_feature_frame

TRAINING_COLUMNS = [
    "date",
    "symbol",
    "strategy_id",
    "signal_score",
    "score_rank",
    "score_percentile",
    "current_weight",
    "target_weight",
    "weight_delta",
    "action",
    "action_type",
    "current_position_held",
    "estimated_execution_cost_pct",
    "recent_return_3d",
    "recent_return_5d",
    "recent_return_10d",
    "recent_vol_20d",
    "dollar_volume",
    "forward_gross_return",
    "forward_net_return",
    "positive_net_return",
]

CANDIDATE_COLUMNS = [
    "date",
    "symbol",
    "strategy_id",
    "signal_score",
    "score_rank",
    "score_percentile",
    "current_weight",
    "target_weight",
    "weight_delta",
    "requested_target_weight",
    "requested_weight_delta",
    "adjusted_target_weight",
    "adjusted_weight_delta",
    "action",
    "action_type",
    "current_position_held",
    "estimated_execution_cost_pct",
    "recent_return_3d",
    "recent_return_5d",
    "recent_return_10d",
    "recent_vol_20d",
    "dollar_volume",
    "candidate_status",
    "candidate_outcome",
    "candidate_stage",
    "skip_reason",
    "action_reason",
    "band_decision",
    "entry_threshold",
    "exit_threshold",
    "score_band_enabled",
    "ev_gate_enabled",
    "ev_gate_mode",
    "ev_gate_decision",
    "probability_positive",
]

PREDICTION_COLUMNS = [
    "date",
    "symbol",
    "strategy_id",
    "signal_score",
    "score_rank",
    "score_percentile",
    "current_weight",
    "target_weight",
    "weight_delta",
    "action",
    "action_type",
    "current_position_held",
    "estimated_execution_cost_pct",
    "recent_return_3d",
    "recent_return_5d",
    "recent_return_10d",
    "recent_vol_20d",
    "dollar_volume",
    "expected_gross_return",
    "expected_net_return",
    "expected_cost",
    "probability_positive",
    "raw_ev_score",
    "ev_decision_score",
    "ev_gate_threshold",
    "ev_gate_decision",
    "ev_gate_mode",
    "ev_weight_multiplier",
    "ev_adjusted_target_weight",
    "ev_adjusted_weight_delta",
    "ev_model_bucket",
    "ev_training_sample_count",
    "action_reason",
]

CALIBRATION_COLUMNS = [
    "date",
    "symbol",
    "strategy_id",
    "bucket",
    "expected_gross_return",
    "expected_net_return",
    "realized_gross_return",
    "realized_net_return",
    "execution_cost",
    "probability_positive",
    "positive_realized_net_return",
    "ev_weight_multiplier",
]

LINEAR_FEATURE_COLUMNS = [
    "signal_score",
    "score_percentile",
    "score_rank_scaled",
    "current_position_held",
    "side_sign",
    "target_weight_abs",
    "weight_delta_abs",
    "estimated_execution_cost_pct",
    "recent_return_3d",
    "recent_return_5d",
    "recent_return_10d",
    "recent_vol_20d",
    "log_dollar_volume",
    "strategy_bias",
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


def _normalize_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    frame = pd.DataFrame(rows)
    if frame.empty:
        return []
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def _read_csv_frame(path: str | Path) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        return pd.DataFrame()
    if csv_path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(csv_path)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame()


def _iter_replay_day_dirs(history_root: str | Path, *, as_of_date: str) -> list[Path]:
    root = Path(history_root)
    if not root.exists():
        return []
    cutoff = pd.Timestamp(as_of_date).date()
    day_dirs: list[Path] = []
    for path in sorted(item for item in root.iterdir() if item.is_dir()):
        try:
            day_value = pd.Timestamp(path.name).date()
        except (TypeError, ValueError):
            continue
        if day_value < cutoff:
            day_dirs.append(path)
    return day_dirs


def _load_feature_snapshot_frame(symbol: str, frame_cache: dict[str, pd.DataFrame]) -> pd.DataFrame | None:
    if symbol not in frame_cache:
        try:
            frame_cache[symbol] = load_feature_frame(symbol)
        except Exception:
            return None
    frame = frame_cache[symbol].copy()
    if frame.empty or "timestamp" not in frame.columns or "close" not in frame.columns:
        return None
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=False)
    frame = frame.sort_values("timestamp", kind="stable").reset_index(drop=True)
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame["return_1d"] = frame["close"].pct_change()
    frame["recent_return_3d"] = frame["close"].pct_change(3)
    frame["recent_return_5d"] = frame["close"].pct_change(5)
    frame["recent_return_10d"] = frame["close"].pct_change(10)
    frame["recent_vol_20d"] = frame["return_1d"].rolling(20).std()
    if "volume" in frame.columns:
        frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce")
        frame["dollar_volume"] = frame["close"] * frame["volume"]
    else:
        frame["dollar_volume"] = None
    return frame


def _feature_snapshot(
    *,
    symbol: str,
    as_of_date: str,
    horizon_days: int,
    frame_cache: dict[str, pd.DataFrame],
) -> dict[str, Any] | None:
    frame = _load_feature_snapshot_frame(symbol, frame_cache)
    if frame is None:
        return None
    as_of_ts = pd.Timestamp(as_of_date)
    eligible = frame.index[frame["timestamp"].dt.date <= as_of_ts.date()].tolist()
    if not eligible:
        return None
    index = int(eligible[-1])
    future_index = index + int(horizon_days)
    if future_index >= len(frame.index):
        return None
    entry_close = _safe_float(frame.iloc[index]["close"])
    future_close = _safe_float(frame.iloc[future_index]["close"])
    if entry_close <= 0.0 or future_close <= 0.0:
        return None
    return {
        "recent_return_3d": _safe_float(frame.iloc[index].get("recent_return_3d")),
        "recent_return_5d": _safe_float(frame.iloc[index].get("recent_return_5d")),
        "recent_return_10d": _safe_float(frame.iloc[index].get("recent_return_10d")),
        "recent_vol_20d": _safe_float(frame.iloc[index].get("recent_vol_20d")),
        "dollar_volume": _safe_float(frame.iloc[index].get("dollar_volume")),
        "forward_price_return": float((future_close / entry_close) - 1.0),
    }


def build_trade_ev_candidate_market_features(
    *,
    symbol: str,
    as_of_date: str,
    frame_cache: dict[str, pd.DataFrame] | None = None,
) -> dict[str, float]:
    cache = frame_cache if frame_cache is not None else {}
    frame = _load_feature_snapshot_frame(symbol, cache)
    if frame is None:
        return {
            "recent_return_3d": 0.0,
            "recent_return_5d": 0.0,
            "recent_return_10d": 0.0,
            "recent_vol_20d": 0.0,
            "dollar_volume": 0.0,
        }
    as_of_ts = pd.Timestamp(as_of_date)
    eligible = frame.index[frame["timestamp"].dt.date <= as_of_ts.date()].tolist()
    if not eligible:
        return {
            "recent_return_3d": 0.0,
            "recent_return_5d": 0.0,
            "recent_return_10d": 0.0,
            "recent_vol_20d": 0.0,
            "dollar_volume": 0.0,
        }
    index = int(eligible[-1])
    return {
        "recent_return_3d": _safe_float(frame.iloc[index].get("recent_return_3d")),
        "recent_return_5d": _safe_float(frame.iloc[index].get("recent_return_5d")),
        "recent_return_10d": _safe_float(frame.iloc[index].get("recent_return_10d")),
        "recent_vol_20d": _safe_float(frame.iloc[index].get("recent_vol_20d")),
        "dollar_volume": _safe_float(frame.iloc[index].get("dollar_volume")),
    }


def _cost_pct_from_fills(frame: pd.DataFrame, symbol: str) -> float:
    if frame.empty or "symbol" not in frame.columns:
        return 0.0
    symbol_rows = frame[frame["symbol"].astype(str) == str(symbol)]
    if symbol_rows.empty:
        return 0.0
    gross_notional = 0.0
    if "gross_notional" in symbol_rows.columns:
        gross_notional = float(pd.to_numeric(symbol_rows["gross_notional"], errors="coerce").fillna(0.0).sum())
    if gross_notional <= 0.0 and {"quantity", "reference_price"}.issubset(symbol_rows.columns):
        gross_notional = float(
            (
                pd.to_numeric(symbol_rows["quantity"], errors="coerce").fillna(0.0).abs()
                * pd.to_numeric(symbol_rows["reference_price"], errors="coerce").fillna(0.0)
            ).sum()
        )
    total_cost = float(pd.to_numeric(symbol_rows.get("total_execution_cost"), errors="coerce").fillna(0.0).sum())
    return float(total_cost / gross_notional) if gross_notional > 0.0 else 0.0


def _action_side_sign(row: dict[str, Any]) -> float:
    weight_delta = _safe_float(row.get("requested_weight_delta", row.get("weight_delta")))
    if weight_delta > 0.0:
        return 1.0
    if weight_delta < 0.0:
        return -1.0
    action = str(row.get("action") or "").lower()
    if action == "buy":
        return 1.0
    if action == "sell":
        return -1.0
    return 0.0


def _action_type(row: dict[str, Any]) -> str:
    current_position = int(_safe_float(row.get("current_position")))
    target_position = int(_safe_float(row.get("target_position")))
    weight_delta = _safe_float(row.get("requested_weight_delta", row.get("weight_delta")))
    if current_position == 0 and target_position != 0:
        return "entry"
    if current_position != 0 and target_position == 0:
        return "exit"
    if weight_delta > 0.0:
        return "increase"
    if weight_delta < 0.0:
        return "reduction"
    return "hold"


def _score_bucket(score_percentile: float) -> str:
    if score_percentile <= 0.2:
        return "q1"
    if score_percentile <= 0.4:
        return "q2"
    if score_percentile <= 0.6:
        return "q3"
    if score_percentile <= 0.8:
        return "q4"
    return "q5"


def _bucket_label(series: pd.Series, bucket_count: int) -> pd.Series:
    filled = pd.to_numeric(series, errors="coerce").fillna(0.0)
    if filled.empty:
        return pd.Series(dtype=object)
    if filled.nunique(dropna=False) <= 1:
        return pd.Series(["b1"] * len(filled), index=filled.index)
    rank = filled.rank(method="first")
    buckets = pd.qcut(rank, q=min(bucket_count, len(filled)), labels=False, duplicates="drop")
    return buckets.fillna(0).astype(int).map(lambda value: f"b{int(value) + 1}")


def _feature_matrix(frame: pd.DataFrame, strategy_bias: dict[str, float]) -> np.ndarray:
    matrix = pd.DataFrame(index=frame.index)
    matrix["signal_score"] = pd.to_numeric(frame["signal_score"], errors="coerce").fillna(0.0)
    matrix["score_percentile"] = pd.to_numeric(frame["score_percentile"], errors="coerce").fillna(0.0)
    rank_series = pd.to_numeric(frame["score_rank"], errors="coerce").fillna(0.0)
    matrix["score_rank_scaled"] = 1.0 / (1.0 + rank_series.clip(lower=0.0))
    matrix["current_position_held"] = pd.to_numeric(frame["current_position_held"], errors="coerce").fillna(0.0)
    weight_delta = pd.to_numeric(frame["weight_delta"], errors="coerce").fillna(0.0)
    matrix["side_sign"] = np.sign(weight_delta)
    matrix["target_weight_abs"] = pd.to_numeric(frame["target_weight"], errors="coerce").fillna(0.0).abs()
    matrix["weight_delta_abs"] = weight_delta.abs()
    matrix["estimated_execution_cost_pct"] = (
        pd.to_numeric(frame["estimated_execution_cost_pct"], errors="coerce").fillna(0.0)
    )
    matrix["recent_return_3d"] = pd.to_numeric(frame["recent_return_3d"], errors="coerce").fillna(0.0)
    matrix["recent_return_5d"] = pd.to_numeric(frame["recent_return_5d"], errors="coerce").fillna(0.0)
    matrix["recent_return_10d"] = pd.to_numeric(frame["recent_return_10d"], errors="coerce").fillna(0.0)
    matrix["recent_vol_20d"] = pd.to_numeric(frame["recent_vol_20d"], errors="coerce").fillna(0.0)
    dollar_volume = pd.to_numeric(frame["dollar_volume"], errors="coerce").fillna(0.0).clip(lower=0.0)
    matrix["log_dollar_volume"] = np.log1p(dollar_volume)
    matrix["strategy_bias"] = frame["strategy_id"].map(lambda value: strategy_bias.get(str(value or ""), 0.0)).astype(float)
    return matrix[LINEAR_FEATURE_COLUMNS].to_numpy(dtype=float)


def _training_summary_from_rows(
    *,
    rows: list[dict[str, Any]],
    horizon_days: int,
    warnings: list[str],
    training_source: str,
    candidate_row_count: int = 0,
    executed_row_count: int = 0,
    skipped_row_count: int = 0,
    feature_missingness: dict[str, float] | None = None,
) -> dict[str, Any]:
    positive_rate = (
        float(pd.Series([_safe_float(row.get("positive_net_return")) for row in rows], dtype=float).mean())
        if rows
        else 0.0
    )
    return {
        "training_source": str(training_source),
        "training_sample_count": len(rows),
        "labeled_row_count": len(rows),
        "candidate_row_count": int(candidate_row_count),
        "executed_row_count": int(executed_row_count),
        "skipped_row_count": int(skipped_row_count),
        "positive_label_rate": positive_rate,
        "training_day_count": len({str(row.get("date") or "") for row in rows if row.get("date")}),
        "horizon_days": int(horizon_days),
        "training_window_start": rows[0]["date"] if rows else None,
        "training_window_end": rows[-1]["date"] if rows else None,
        "warnings": warnings,
        "target_definition": f"forward_{int(horizon_days)}d_market_return_minus_estimated_cost",
        "executed_only": training_source == "executed_trades",
        "feature_missingness": dict(feature_missingness or {}),
    }


def _build_executed_trade_ev_training_dataset(
    *,
    history_root: str | Path | None,
    as_of_date: str,
    horizon_days: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if history_root is None:
        return [], _training_summary_from_rows(
            rows=[],
            horizon_days=horizon_days,
            warnings=["missing_ev_training_root"],
            training_source="executed_trades",
        )
    frame_cache: dict[str, pd.DataFrame] = {}
    rows: list[dict[str, Any]] = []
    candidate_row_count = 0
    executed_row_count = 0
    skipped_row_count = 0
    for day_dir in _iter_replay_day_dirs(history_root, as_of_date=as_of_date):
        decision_path = day_dir / "trade_decision_log.csv"
        fills_path = day_dir / "paper" / "paper_fills.csv"
        if not decision_path.exists() or not fills_path.exists():
            continue
        decision_frame = _read_csv_frame(decision_path)
        fills_frame = _read_csv_frame(fills_path)
        if decision_frame.empty or fills_frame.empty:
            continue
        executed_symbols = set(fills_frame["symbol"].astype(str).tolist()) if "symbol" in fills_frame.columns else set()
        if not executed_symbols:
            continue
        candidate_row_count += len(decision_frame.index)
        for row in decision_frame.astype(object).where(pd.notna(decision_frame), None).to_dict(orient="records"):
            symbol = str(row.get("symbol") or "")
            if not symbol or symbol not in executed_symbols:
                skipped_row_count += 1
                continue
            executed_row_count += 1
            feature_row = _feature_snapshot(
                symbol=symbol,
                as_of_date=str(row.get("date") or day_dir.name),
                horizon_days=horizon_days,
                frame_cache=frame_cache,
            )
            if feature_row is None:
                continue
            side_sign = _action_side_sign(row)
            if side_sign == 0.0:
                continue
            cost_pct = _cost_pct_from_fills(fills_frame, symbol)
            forward_gross_return = side_sign * _safe_float(feature_row.get("forward_price_return"))
            forward_net_return = float(forward_gross_return - cost_pct)
            rows.append(
                {
                    "date": str(row.get("date") or day_dir.name),
                    "symbol": symbol,
                    "strategy_id": row.get("strategy_id"),
                    "signal_score": _safe_float(row.get("score_value", row.get("signal_score"))),
                    "score_rank": _safe_float(row.get("score_rank", row.get("rank"))),
                    "score_percentile": _safe_float(row.get("score_percentile")),
                    "current_weight": _safe_float(row.get("current_weight")),
                    "target_weight": _safe_float(row.get("target_weight")),
                    "weight_delta": _safe_float(row.get("weight_delta")),
                    "action": str(row.get("action") or ""),
                    "action_type": _action_type(row),
                    "current_position_held": int(_safe_float(row.get("current_position")) != 0.0),
                    "estimated_execution_cost_pct": cost_pct,
                    "recent_return_3d": _safe_float(feature_row.get("recent_return_3d")),
                    "recent_return_5d": _safe_float(feature_row.get("recent_return_5d")),
                    "recent_return_10d": _safe_float(feature_row.get("recent_return_10d")),
                    "recent_vol_20d": _safe_float(feature_row.get("recent_vol_20d")),
                    "dollar_volume": _safe_float(feature_row.get("dollar_volume")),
                    "forward_gross_return": forward_gross_return,
                    "forward_net_return": forward_net_return,
                    "positive_net_return": int(forward_net_return > 0.0),
                }
            )
    normalized_rows = _normalize_records(rows)
    summary = _training_summary_from_rows(
        rows=normalized_rows,
        horizon_days=horizon_days,
        warnings=[] if normalized_rows else ["insufficient_trade_history_for_ev_gate"],
        training_source="executed_trades",
        candidate_row_count=candidate_row_count,
        executed_row_count=executed_row_count,
        skipped_row_count=skipped_row_count,
    )
    return normalized_rows, summary


def _build_candidate_trade_ev_training_dataset(
    *,
    history_root: str | Path | None,
    as_of_date: str,
    horizon_days: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if history_root is None:
        return [], _training_summary_from_rows(
            rows=[],
            horizon_days=horizon_days,
            warnings=["missing_ev_training_root"],
            training_source="candidate_decisions",
        )
    frame_cache: dict[str, pd.DataFrame] = {}
    rows: list[dict[str, Any]] = []
    candidate_row_count = 0
    executed_row_count = 0
    skipped_row_count = 0
    missing_feature_counts = {
        "recent_return_3d": 0,
        "recent_return_5d": 0,
        "recent_return_10d": 0,
        "recent_vol_20d": 0,
        "dollar_volume": 0,
    }
    for day_dir in _iter_replay_day_dirs(history_root, as_of_date=as_of_date):
        candidate_path = day_dir / "paper" / "trade_candidate_dataset.csv"
        candidate_frame = _read_csv_frame(candidate_path)
        if candidate_frame.empty:
            continue
        records = candidate_frame.astype(object).where(pd.notna(candidate_frame), None).to_dict(orient="records")
        candidate_row_count += len(records)
        for row in records:
            requested_weight_delta = _safe_float(row.get("requested_weight_delta", row.get("weight_delta")))
            side_sign = _action_side_sign(row)
            if side_sign == 0.0 or abs(requested_weight_delta) <= 1e-12:
                skipped_row_count += 1
                continue
            candidate_outcome = str(row.get("candidate_outcome") or "")
            if candidate_outcome == "executed":
                executed_row_count += 1
            else:
                skipped_row_count += 1
            symbol = str(row.get("symbol") or "")
            if not symbol:
                continue
            feature_row = _feature_snapshot(
                symbol=symbol,
                as_of_date=str(row.get("date") or day_dir.name),
                horizon_days=horizon_days,
                frame_cache=frame_cache,
            )
            if feature_row is None:
                continue
            for key in missing_feature_counts:
                if row.get(key) in (None, ""):
                    missing_feature_counts[key] += 1
            cost_pct = _safe_float(row.get("estimated_execution_cost_pct"))
            forward_gross_return = side_sign * _safe_float(feature_row.get("forward_price_return"))
            forward_net_return = float(forward_gross_return - cost_pct)
            rows.append(
                {
                    "date": str(row.get("date") or day_dir.name),
                    "symbol": symbol,
                    "strategy_id": row.get("strategy_id"),
                    "signal_score": _safe_float(row.get("signal_score", row.get("score_value"))),
                    "score_rank": _safe_float(row.get("score_rank", row.get("rank"))),
                    "score_percentile": _safe_float(row.get("score_percentile")),
                    "current_weight": _safe_float(row.get("current_weight")),
                    "target_weight": _safe_float(row.get("requested_target_weight", row.get("target_weight"))),
                    "weight_delta": requested_weight_delta,
                    "action": str(row.get("action") or ""),
                    "action_type": str(row.get("action_type") or _action_type(row)),
                    "current_position_held": int(_safe_float(row.get("current_position_held")) != 0.0),
                    "estimated_execution_cost_pct": cost_pct,
                    "recent_return_3d": _safe_float(row.get("recent_return_3d", feature_row.get("recent_return_3d"))),
                    "recent_return_5d": _safe_float(row.get("recent_return_5d", feature_row.get("recent_return_5d"))),
                    "recent_return_10d": _safe_float(row.get("recent_return_10d", feature_row.get("recent_return_10d"))),
                    "recent_vol_20d": _safe_float(row.get("recent_vol_20d", feature_row.get("recent_vol_20d"))),
                    "dollar_volume": _safe_float(row.get("dollar_volume", feature_row.get("dollar_volume"))),
                    "forward_gross_return": forward_gross_return,
                    "forward_net_return": forward_net_return,
                    "positive_net_return": int(forward_net_return > 0.0),
                }
            )
    normalized_rows = _normalize_records(rows)
    total_candidates = max(candidate_row_count, 1)
    feature_missingness = {
        key: float(count / total_candidates) for key, count in missing_feature_counts.items()
    }
    summary = _training_summary_from_rows(
        rows=normalized_rows,
        horizon_days=horizon_days,
        warnings=[] if normalized_rows else ["insufficient_candidate_history_for_ev_gate"],
        training_source="candidate_decisions",
        candidate_row_count=candidate_row_count,
        executed_row_count=executed_row_count,
        skipped_row_count=skipped_row_count,
        feature_missingness=feature_missingness,
    )
    return normalized_rows, summary


def build_trade_ev_training_dataset(
    *,
    history_root: str | Path | None,
    as_of_date: str,
    horizon_days: int,
    training_source: str = "executed_trades",
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    normalized_source = str(training_source or "executed_trades").lower()
    if normalized_source == "candidate_decisions":
        return _build_candidate_trade_ev_training_dataset(
            history_root=history_root,
            as_of_date=as_of_date,
            horizon_days=horizon_days,
        )
    return _build_executed_trade_ev_training_dataset(
        history_root=history_root,
        as_of_date=as_of_date,
        horizon_days=horizon_days,
    )


def train_trade_ev_model(
    *,
    training_rows: list[dict[str, Any]],
    model_type: str,
    min_training_samples: int,
) -> dict[str, Any]:
    normalized_type = str(model_type or "bucketed_mean").lower()
    if normalized_type not in {"bucketed_mean", "bucketed_linear"}:
        raise ValueError(f"Unsupported EV model type: {model_type}")
    if len(training_rows) < int(min_training_samples):
        return {
            "model_type": normalized_type,
            "training_sample_count": len(training_rows),
            "training_available": False,
            "warnings": ["insufficient_trade_history_for_ev_gate"],
            "bucket_stats": {},
            "side_stats": {},
            "global_mean_gross_return": 0.0,
            "global_mean_net_return": 0.0,
            "global_probability_positive": 0.0,
            "strategy_bias": {},
        }
    frame = pd.DataFrame(training_rows)
    frame["score_bucket"] = (
        pd.cut(
            pd.to_numeric(frame["score_percentile"], errors="coerce").fillna(0.0),
            bins=[-0.001, 0.2, 0.4, 0.6, 0.8, 1.001],
            labels=["q1", "q2", "q3", "q4", "q5"],
        )
        .astype(str)
        .replace("nan", "q0")
    )
    frame["held_bucket"] = frame["current_position_held"].astype(int)
    frame["side_bucket"] = frame["weight_delta"].apply(lambda value: "buy" if _safe_float(value) > 0.0 else "sell")
    strategy_bias = (
        frame.groupby(frame["strategy_id"].astype(str), dropna=False)["forward_net_return"].mean().to_dict()
    )
    bucket_stats: dict[str, dict[str, Any]] = {}
    grouped = (
        frame.groupby(["held_bucket", "side_bucket", "score_bucket"], dropna=False)
        .agg(
            sample_count=("forward_net_return", "count"),
            expected_gross_return=("forward_gross_return", "mean"),
            expected_net_return=("forward_net_return", "mean"),
            probability_positive=("positive_net_return", "mean"),
            average_cost=("estimated_execution_cost_pct", "mean"),
        )
        .reset_index()
    )
    for row in grouped.to_dict(orient="records"):
        bucket_key = f"{int(row['held_bucket'])}|{row['side_bucket']}|{row['score_bucket']}"
        bucket_stats[bucket_key] = {
            "sample_count": int(row["sample_count"]),
            "expected_gross_return": _safe_float(row["expected_gross_return"]),
            "expected_net_return": _safe_float(row["expected_net_return"]),
            "probability_positive": _safe_float(row["probability_positive"]),
            "average_cost": _safe_float(row["average_cost"]),
        }
    side_stats: dict[str, dict[str, Any]] = {}
    side_grouped = (
        frame.groupby(["held_bucket", "side_bucket"], dropna=False)
        .agg(
            sample_count=("forward_net_return", "count"),
            expected_gross_return=("forward_gross_return", "mean"),
            expected_net_return=("forward_net_return", "mean"),
            probability_positive=("positive_net_return", "mean"),
            average_cost=("estimated_execution_cost_pct", "mean"),
        )
        .reset_index()
    )
    for row in side_grouped.to_dict(orient="records"):
        side_key = f"{int(row['held_bucket'])}|{row['side_bucket']}"
        side_stats[side_key] = {
            "sample_count": int(row["sample_count"]),
            "expected_gross_return": _safe_float(row["expected_gross_return"]),
            "expected_net_return": _safe_float(row["expected_net_return"]),
            "probability_positive": _safe_float(row["probability_positive"]),
            "average_cost": _safe_float(row["average_cost"]),
        }
    model: dict[str, Any] = {
        "model_type": normalized_type,
        "training_sample_count": len(training_rows),
        "training_available": True,
        "warnings": [],
        "bucket_stats": bucket_stats,
        "side_stats": side_stats,
        "global_mean_gross_return": float(pd.to_numeric(frame["forward_gross_return"], errors="coerce").mean()),
        "global_mean_net_return": float(pd.to_numeric(frame["forward_net_return"], errors="coerce").mean()),
        "global_probability_positive": float(pd.to_numeric(frame["positive_net_return"], errors="coerce").mean()),
        "strategy_bias": {str(key): float(value) for key, value in strategy_bias.items()},
    }
    if normalized_type == "bucketed_linear":
        x = _feature_matrix(frame, model["strategy_bias"])
        x_with_intercept = np.column_stack([np.ones(len(frame)), x])
        ridge_lambda = 1.0
        penalty = np.eye(x_with_intercept.shape[1]) * ridge_lambda
        penalty[0, 0] = 0.0
        y_net = pd.to_numeric(frame["forward_net_return"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
        y_gross = pd.to_numeric(frame["forward_gross_return"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
        beta_net = np.linalg.solve(x_with_intercept.T @ x_with_intercept + penalty, x_with_intercept.T @ y_net)
        beta_gross = np.linalg.solve(x_with_intercept.T @ x_with_intercept + penalty, x_with_intercept.T @ y_gross)
        model["linear_model"] = {
            "feature_columns": LINEAR_FEATURE_COLUMNS,
            "intercept_net": float(beta_net[0]),
            "coefficients_net": [float(value) for value in beta_net[1:]],
            "intercept_gross": float(beta_gross[0]),
            "coefficients_gross": [float(value) for value in beta_gross[1:]],
        }
    return model


def score_trade_ev_candidates(
    *,
    model: dict[str, Any],
    candidate_rows: list[dict[str, Any]],
    min_expected_net_return: float,
    min_probability_positive: float | None,
    risk_penalty_lambda: float,
    score_clip_min: float | None = None,
    score_clip_max: float | None = None,
    normalize_scores: bool = False,
) -> list[dict[str, Any]]:
    predictions: list[dict[str, Any]] = []
    bucket_stats = dict(model.get("bucket_stats") or {})
    side_stats = dict(model.get("side_stats") or {})
    global_gross = _safe_float(model.get("global_mean_gross_return"))
    global_net = _safe_float(model.get("global_mean_net_return"))
    global_prob = _safe_float(model.get("global_probability_positive"))
    normalized_type = str(model.get("model_type", "bucketed_mean") or "bucketed_mean").lower()
    strategy_bias = {str(key): float(value) for key, value in dict(model.get("strategy_bias") or {}).items()}
    candidate_frame = pd.DataFrame(candidate_rows)
    linear_expected_gross: list[float] = []
    linear_expected_net: list[float] = []
    if normalized_type == "bucketed_linear" and not candidate_frame.empty:
        linear_model = dict(model.get("linear_model") or {})
        x = _feature_matrix(candidate_frame, strategy_bias)
        linear_expected_net = list(
            float(linear_model.get("intercept_net", 0.0))
            + (x @ np.array(linear_model.get("coefficients_net", [0.0] * x.shape[1]), dtype=float))
        )
        linear_expected_gross = list(
            float(linear_model.get("intercept_gross", 0.0))
            + (x @ np.array(linear_model.get("coefficients_gross", [0.0] * x.shape[1]), dtype=float))
        )
    raw_scores: list[float] = []
    staged_predictions: list[dict[str, Any]] = []
    for index, row in enumerate(candidate_rows):
        score_percentile = _safe_float(row.get("score_percentile"))
        score_bucket = _score_bucket(score_percentile)
        side_bucket = "buy" if _safe_float(row.get("weight_delta")) > 0.0 else "sell"
        held_bucket = int(bool(row.get("current_position_held")))
        bucket_key = f"{held_bucket}|{side_bucket}|{score_bucket}"
        side_key = f"{held_bucket}|{side_bucket}"
        stats = dict(bucket_stats.get(bucket_key) or side_stats.get(side_key) or {})
        sample_count = int(stats.get("sample_count", 0) or 0)
        probability_positive = _safe_float(stats.get("probability_positive")) if stats else global_prob
        if normalized_type == "bucketed_linear" and linear_expected_net:
            expected_gross_return = linear_expected_gross[index]
            expected_net_return = linear_expected_net[index]
            probability_positive = float(min(1.0, max(0.0, 0.5 + (expected_net_return * 10.0))))
            bucket_name = "linear"
            sample_value = int(model.get("training_sample_count", 0) or 0)
        else:
            expected_gross_return = _safe_float(stats.get("expected_gross_return")) if stats else global_gross
            expected_net_return = _safe_float(stats.get("expected_net_return")) if stats else global_net
            bucket_name = bucket_key if stats else "global"
            sample_value = sample_count if stats else int(model.get("training_sample_count", 0) or 0)
        raw_score = float(
            expected_net_return - (_safe_float(row.get("recent_vol_20d")) * float(risk_penalty_lambda or 0.0))
        )
        raw_scores.append(raw_score)
        staged_predictions.append(
            {
                **row,
                "expected_gross_return": float(expected_gross_return),
                "expected_net_return": float(expected_net_return),
                "expected_cost": _safe_float(row.get("estimated_execution_cost_pct")),
                "probability_positive": float(probability_positive),
                "raw_ev_score": raw_score,
                "ev_model_bucket": bucket_name,
                "ev_training_sample_count": sample_value,
            }
        )
    score_series = pd.Series(raw_scores, dtype=float) if raw_scores else pd.Series(dtype=float)
    if normalize_scores and not score_series.empty and float(score_series.std(ddof=0) or 0.0) > 0.0:
        final_scores = (score_series - float(score_series.mean())) / float(score_series.std(ddof=0))
    else:
        final_scores = score_series.copy()
    if score_clip_min is not None:
        final_scores = final_scores.clip(lower=float(score_clip_min))
    if score_clip_max is not None:
        final_scores = final_scores.clip(upper=float(score_clip_max))
    for index, row in enumerate(staged_predictions):
        ev_score = float(final_scores.iloc[index]) if not final_scores.empty else float(row["raw_ev_score"])
        decision = "allow"
        if ev_score < float(min_expected_net_return):
            decision = "block"
        if min_probability_positive is not None and float(row["probability_positive"]) < float(min_probability_positive):
            decision = "block"
        predictions.append(
            {
                **row,
                "ev_decision_score": ev_score,
                "ev_gate_threshold": float(min_expected_net_return),
                "ev_gate_decision": decision,
                "action_reason": "blocked_by_ev_gate" if decision == "block" else "passed_ev_gate",
            }
        )
    return _normalize_records(predictions)


def build_trade_ev_calibration(
    *,
    prediction_rows: list[dict[str, Any]],
    bucket_count: int = 5,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not prediction_rows:
        empty_summary = {
            "trade_count": 0,
            "rank_correlation": 0.0,
            "top_vs_bottom_bucket_spread": 0.0,
            "bucket_monotonicity": False,
            "calibration_error": 0.0,
            "top_bucket_realized_net_return": 0.0,
            "bottom_bucket_realized_net_return": 0.0,
        }
        return [], empty_summary
    frame = pd.DataFrame(prediction_rows)
    frame["expected_net_return"] = pd.to_numeric(frame["expected_net_return"], errors="coerce").fillna(0.0)
    frame["expected_gross_return"] = pd.to_numeric(frame["expected_gross_return"], errors="coerce").fillna(0.0)
    frame["realized_net_return"] = pd.to_numeric(frame["realized_net_return"], errors="coerce").fillna(0.0)
    frame["realized_gross_return"] = pd.to_numeric(frame["realized_gross_return"], errors="coerce").fillna(0.0)
    frame["execution_cost"] = pd.to_numeric(frame["execution_cost"], errors="coerce").fillna(0.0)
    frame["probability_positive"] = pd.to_numeric(frame["probability_positive"], errors="coerce").fillna(0.0)
    frame["ev_weight_multiplier"] = pd.to_numeric(frame.get("ev_weight_multiplier"), errors="coerce").fillna(1.0)
    if "date" not in frame.columns:
        frame["date"] = ""
    if "symbol" not in frame.columns:
        frame["symbol"] = ""
    if "strategy_id" not in frame.columns:
        frame["strategy_id"] = ""
    frame["positive_realized_net_return"] = (frame["realized_net_return"] > 0.0).astype(int)
    frame["bucket"] = _bucket_label(frame["expected_net_return"], bucket_count)
    bucket_frame = (
        frame.groupby("bucket", dropna=False)
        .agg(
            trade_count=("symbol", "count"),
            avg_predicted_gross_return=("expected_gross_return", "mean"),
            avg_predicted_net_return=("expected_net_return", "mean"),
            avg_realized_gross_return=("realized_gross_return", "mean"),
            avg_realized_net_return=("realized_net_return", "mean"),
            realized_hit_rate=("positive_realized_net_return", "mean"),
            avg_execution_cost=("execution_cost", "mean"),
            avg_weight_multiplier=("ev_weight_multiplier", "mean"),
        )
        .reset_index()
        .sort_values("bucket", kind="stable")
    )
    rank_corr = frame["expected_net_return"].corr(frame["realized_net_return"], method="spearman")
    calibration_error = float((frame["expected_net_return"] - frame["realized_net_return"]).abs().mean())
    realized_series = bucket_frame["avg_realized_net_return"].tolist()
    monotonicity = all(earlier <= later + 1e-12 for earlier, later in zip(realized_series, realized_series[1:], strict=False))
    summary = {
        "trade_count": int(len(frame)),
        "rank_correlation": float(rank_corr) if pd.notna(rank_corr) else 0.0,
        "top_vs_bottom_bucket_spread": (
            float(bucket_frame.iloc[-1]["avg_realized_net_return"] - bucket_frame.iloc[0]["avg_realized_net_return"])
            if len(bucket_frame.index) >= 2
            else 0.0
        ),
        "bucket_monotonicity": bool(monotonicity),
        "calibration_error": calibration_error,
        "top_bucket_realized_net_return": float(bucket_frame.iloc[-1]["avg_realized_net_return"]),
        "bottom_bucket_realized_net_return": float(bucket_frame.iloc[0]["avg_realized_net_return"]),
        "bucket_rows": bucket_frame.astype(object).where(pd.notna(bucket_frame), None).to_dict(orient="records"),
    }
    calibration_rows = frame[CALIBRATION_COLUMNS].to_dict(orient="records")
    return _normalize_records(calibration_rows), summary


def evaluate_replay_trade_ev_predictions(
    *,
    replay_root: str | Path,
    horizon_days: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    root = Path(replay_root)
    prediction_rows: list[dict[str, Any]] = []
    for day_dir in sorted(item for item in root.iterdir() if item.is_dir()):
        prediction_path = day_dir / "paper" / "trade_ev_predictions.csv"
        if not prediction_path.exists():
            continue
        try:
            frame = pd.read_csv(prediction_path)
        except (pd.errors.EmptyDataError, pd.errors.ParserError):
            continue
        if frame.empty:
            continue
        prediction_rows.extend(frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records"))
    if not prediction_rows:
        return [], [], {
            "trade_count": 0,
            "rank_correlation": 0.0,
            "top_vs_bottom_bucket_spread": 0.0,
            "bucket_monotonicity": False,
            "calibration_error": 0.0,
            "top_bucket_realized_net_return": 0.0,
            "bottom_bucket_realized_net_return": 0.0,
        }
    frame_cache: dict[str, pd.DataFrame] = {}
    realized_rows: list[dict[str, Any]] = []
    for row in prediction_rows:
        symbol = str(row.get("symbol") or "")
        as_of_date = str(row.get("date") or "")
        if not symbol or not as_of_date:
            continue
        feature_row = _feature_snapshot(
            symbol=symbol,
            as_of_date=as_of_date,
            horizon_days=horizon_days,
            frame_cache=frame_cache,
        )
        if feature_row is None:
            continue
        side_sign = _action_side_sign(row)
        if side_sign == 0.0:
            continue
        realized_gross = side_sign * _safe_float(feature_row.get("forward_price_return"))
        execution_cost = _safe_float(row.get("expected_cost", row.get("estimated_execution_cost_pct")))
        realized_rows.append(
            {
                **row,
                "realized_gross_return": realized_gross,
                "realized_net_return": float(realized_gross - execution_cost),
                "execution_cost": execution_cost,
            }
        )
    calibration_rows, calibration_summary = build_trade_ev_calibration(prediction_rows=realized_rows)
    bucket_rows = list(calibration_summary.pop("bucket_rows", []))
    return _normalize_records(realized_rows), bucket_rows, calibration_summary


def write_trade_ev_artifacts(
    *,
    output_dir: str | Path,
    training_summary: dict[str, Any],
    prediction_rows: list[dict[str, Any]],
    candidate_rows: list[dict[str, Any]] | None = None,
    calibration_rows: list[dict[str, Any]] | None = None,
    calibration_summary: dict[str, Any] | None = None,
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    training_summary_path = output_path / "trade_ev_training_summary.json"
    predictions_path = output_path / "trade_ev_predictions.csv"
    decision_log_path = output_path / "ev_gate_decision_log.csv"
    candidate_dataset_path = output_path / "trade_candidate_dataset.csv"
    calibration_path = output_path / "trade_ev_calibration.csv"
    training_summary_path.write_text(json.dumps(training_summary, indent=2, default=str), encoding="utf-8")
    pd.DataFrame(prediction_rows, columns=PREDICTION_COLUMNS).to_csv(predictions_path, index=False)
    pd.DataFrame(prediction_rows, columns=PREDICTION_COLUMNS).to_csv(decision_log_path, index=False)
    pd.DataFrame(candidate_rows or [], columns=CANDIDATE_COLUMNS).to_csv(candidate_dataset_path, index=False)
    pd.DataFrame(calibration_rows or [], columns=CALIBRATION_COLUMNS).to_csv(calibration_path, index=False)
    if calibration_summary is not None:
        (output_path / "trade_ev_calibration_summary.json").write_text(
            json.dumps(calibration_summary, indent=2, default=str),
            encoding="utf-8",
        )
    return {
        "trade_ev_training_summary_path": training_summary_path,
        "trade_ev_predictions_path": predictions_path,
        "ev_gate_decision_log_path": decision_log_path,
        "trade_candidate_dataset_path": candidate_dataset_path,
        "trade_ev_calibration_path": calibration_path,
    }

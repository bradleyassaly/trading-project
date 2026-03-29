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
    "signal_family",
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

TARGET_TYPES = {"market_proxy", "realized_candidate_proxy", "realized_trade_proxy", "hybrid_proxy"}

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
    "ev_model_type_requested",
    "ev_model_type_used",
    "ev_model_fallback_reason",
    "probability_positive",
    "raw_ev_score",
    "normalized_ev_score",
    "ev_score_pre_clip",
    "ev_score_post_clip",
    "ev_score_clipped",
    "ev_weighting_score",
    "ev_confidence",
    "ev_confidence_multiplier",
    "ev_score_before_confidence",
    "ev_score_after_confidence",
    "residual_std_bucket",
    "residual_std_global",
    "residual_std_final",
    "sample_size_used",
    "residual_std_confidence",
    "magnitude_confidence",
    "model_performance_confidence",
    "combined_confidence",
    "normalization_method",
    "normalize_within",
    "candidate_count_for_normalization",
    "regression_raw_ev_score",
    "regression_normalized_ev_score",
    "regression_ev_score_post_clip",
    "was_filtered_by_confidence",
    "ev_reliability",
    "ev_reliability_rank_pct",
    "ev_reliability_multiplier",
    "reliability_target_type",
    "reliability_usage_mode",
    "ev_score_before_reliability",
    "ev_score_after_reliability",
    "was_filtered_by_reliability",
    "was_reliability_promoted",
    "reliability_turnover_delta",
    "reliability_cost_drag_delta",
]

PREDICTION_COLUMNS = [
    "date",
    "symbol",
    "strategy_id",
    "signal_family",
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
    "normalized_ev_score",
    "ev_score_pre_clip",
    "ev_score_post_clip",
    "ev_score_clipped",
    "ev_weighting_score",
    "normalization_method",
    "normalize_within",
    "candidate_count_for_normalization",
    "ev_decision_score",
    "ev_gate_threshold",
    "ev_gate_decision",
    "ev_gate_mode",
    "ev_model_type_requested",
    "ev_model_type_used",
    "ev_model_fallback_reason",
    "ev_weight_multiplier",
    "ev_adjusted_target_weight",
    "ev_adjusted_weight_delta",
    "ev_model_bucket",
    "ev_training_sample_count",
    "regression_raw_ev_score",
    "regression_normalized_ev_score",
    "regression_ev_score_post_clip",
    "ev_confidence",
    "ev_confidence_multiplier",
    "ev_score_before_confidence",
    "ev_score_after_confidence",
    "residual_std_bucket",
    "residual_std_global",
    "residual_std_final",
    "sample_size_used",
    "residual_std_confidence",
    "magnitude_confidence",
    "model_performance_confidence",
    "combined_confidence",
    "residual_std_used",
    "confidence_source",
    "was_filtered_by_confidence",
    "ev_reliability",
    "ev_reliability_rank_pct",
    "ev_reliability_multiplier",
    "reliability_target_type",
    "reliability_usage_mode",
    "ev_score_before_reliability",
    "ev_score_after_reliability",
    "was_filtered_by_reliability",
    "was_reliability_promoted",
    "reliability_turnover_delta",
    "reliability_cost_drag_delta",
    "action_reason",
]

CALIBRATION_COLUMNS = [
    "date",
    "symbol",
    "strategy_id",
    "bucket",
    "expected_gross_return",
    "expected_net_return",
    "raw_ev_score",
    "normalized_ev_score",
    "ev_score_post_clip",
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
    "score_vol_interaction",
    "score_cost_interaction",
    "weight_cost_interaction",
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


def _frame_numeric_column(frame: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column in frame.columns:
        return pd.to_numeric(frame[column], errors="coerce").fillna(default)
    return pd.Series([default] * len(frame.index), index=frame.index, dtype=float)


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


def _all_replay_day_dirs(history_root: str | Path) -> list[Path]:
    root = Path(history_root)
    if not root.exists():
        return []
    day_dirs: list[Path] = []
    for path in sorted(item for item in root.iterdir() if item.is_dir()):
        try:
            pd.Timestamp(path.name).date()
        except (TypeError, ValueError):
            continue
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


def _gross_notional_from_frame(frame: pd.DataFrame) -> float:
    if frame.empty:
        return 0.0
    if "gross_notional" in frame.columns:
        gross_notional = float(pd.to_numeric(frame["gross_notional"], errors="coerce").fillna(0.0).abs().sum())
        if gross_notional > 0.0:
            return gross_notional
    if {"quantity", "reference_price"}.issubset(frame.columns):
        return float(
            (
                pd.to_numeric(frame["quantity"], errors="coerce").fillna(0.0).abs()
                * pd.to_numeric(frame["reference_price"], errors="coerce").fillna(0.0)
            ).sum()
        )
    return 0.0


def _filter_frame_by_symbol_strategy(frame: pd.DataFrame, *, symbol: str, strategy_id: str | None) -> pd.DataFrame:
    if frame.empty:
        return frame.iloc[0:0].copy()
    result = frame
    if "symbol" in result.columns:
        result = result[result["symbol"].astype(str) == str(symbol)]
    if strategy_id and "strategy_id" in result.columns:
        result = result[result["strategy_id"].astype(str) == str(strategy_id)]
    return result.copy()


def _target_definition(target_type: str, horizon_days: int) -> str:
    normalized = str(target_type or "market_proxy").lower()
    if normalized == "hybrid_proxy":
        return (
            f"forward_{int(horizon_days)}d_hybrid_proxy_"
            "blending_market_proxy_with_realized_candidate_proxy_when_available"
        )
    if normalized == "realized_candidate_proxy":
        return (
            f"forward_{int(horizon_days)}d_realized_candidate_proxy_"
            "using_daily_symbol_net_pnl_plus_horizon_mark_for_executed_long_entries"
        )
    if normalized == "realized_trade_proxy":
        return (
            f"forward_{int(horizon_days)}d_realized_trade_proxy_not_implemented_"
            "falling_back_to_realized_candidate_proxy_or_market_proxy"
        )
    return f"forward_{int(horizon_days)}d_market_return_minus_estimated_cost"


def _market_proxy_label(
    *,
    row: dict[str, Any],
    as_of_date: str,
    horizon_days: int,
    frame_cache: dict[str, pd.DataFrame],
) -> dict[str, Any] | None:
    symbol = str(row.get("symbol") or "")
    if not symbol:
        return None
    feature_row = _feature_snapshot(
        symbol=symbol,
        as_of_date=as_of_date,
        horizon_days=horizon_days,
        frame_cache=frame_cache,
    )
    if feature_row is None:
        return None
    side_sign = _action_side_sign(row)
    if side_sign == 0.0:
        return None
    cost_pct = _safe_float(row.get("estimated_execution_cost_pct"))
    forward_gross_return = side_sign * _safe_float(feature_row.get("forward_price_return"))
    forward_net_return = float(forward_gross_return - cost_pct)
    return {
        "forward_gross_return": forward_gross_return,
        "forward_net_return": forward_net_return,
        "positive_net_return": int(forward_net_return > 0.0),
        "label_source": "market_proxy",
        "label_mode": "proxy",
        "excluded_unlabeled": False,
    }


def _realized_candidate_proxy_label(
    *,
    row: dict[str, Any],
    day_dir: Path,
    ordered_day_dirs: list[Path],
    day_index_by_date: dict[str, int],
    as_of_date: str | None,
    horizon_days: int,
    frame_cache: dict[str, pd.DataFrame],
) -> dict[str, Any] | None:
    row_date = str(row.get("date") or day_dir.name)
    symbol = str(row.get("symbol") or "")
    if not row_date or not symbol:
        return None
    current_index = day_index_by_date.get(row_date)
    if current_index is None:
        return None
    horizon_index = current_index + int(horizon_days)
    if horizon_index >= len(ordered_day_dirs):
        return None
    horizon_day_dir = ordered_day_dirs[horizon_index]
    horizon_date = str(pd.Timestamp(horizon_day_dir.name).date())
    if as_of_date is not None and pd.Timestamp(horizon_date).date() >= pd.Timestamp(as_of_date).date():
        return None

    normalized_target_type = str(row.get("action_type") or _action_type(row)).lower()
    candidate_outcome = str(row.get("candidate_outcome") or "executed").lower()
    side_sign = _action_side_sign(row)
    strategy_id = str(row.get("strategy_id") or "").strip() or None

    market_proxy = _market_proxy_label(
        row=row,
        as_of_date=row_date,
        horizon_days=horizon_days,
        frame_cache=frame_cache,
    )
    if market_proxy is None:
        return None

    can_use_realized = (
        candidate_outcome == "executed"
        and side_sign > 0.0
        and normalized_target_type in {"entry", "increase"}
    )
    if not can_use_realized:
        return {
            **market_proxy,
            "label_source": "market_proxy_fallback",
            "fallback_reason": "unsupported_candidate_type_for_realized_proxy",
        }

    fills_frame = _read_csv_frame(day_dir / "paper" / "paper_fills.csv")
    fill_rows = _filter_frame_by_symbol_strategy(fills_frame, symbol=symbol, strategy_id=strategy_id)
    gross_notional = _gross_notional_from_frame(fill_rows)
    if gross_notional <= 0.0:
        return {
            **market_proxy,
            "label_source": "market_proxy_fallback",
            "fallback_reason": "missing_entry_fill_notional_for_realized_proxy",
        }

    gross_realized_total = 0.0
    net_realized_total = 0.0
    for future_day_dir in ordered_day_dirs[current_index : horizon_index + 1]:
        symbol_frame = _read_csv_frame(future_day_dir / "paper" / "symbol_pnl_attribution.csv")
        symbol_rows = _filter_frame_by_symbol_strategy(symbol_frame, symbol=symbol, strategy_id=strategy_id)
        if symbol_rows.empty:
            continue
        gross_realized_total += float(
            pd.to_numeric(symbol_rows.get("gross_realized_pnl"), errors="coerce").fillna(0.0).sum()
        )
        net_realized_total += float(
            pd.to_numeric(symbol_rows.get("net_realized_pnl", symbol_rows.get("realized_pnl")), errors="coerce")
            .fillna(0.0)
            .sum()
        )

    horizon_symbol_frame = _read_csv_frame(horizon_day_dir / "paper" / "symbol_pnl_attribution.csv")
    horizon_symbol_rows = _filter_frame_by_symbol_strategy(horizon_symbol_frame, symbol=symbol, strategy_id=strategy_id)
    gross_unrealized = float(
        pd.to_numeric(horizon_symbol_rows.get("gross_unrealized_pnl"), errors="coerce").fillna(0.0).sum()
    )
    net_unrealized = float(
        pd.to_numeric(horizon_symbol_rows.get("net_unrealized_pnl", horizon_symbol_rows.get("unrealized_pnl")), errors="coerce")
        .fillna(0.0)
        .sum()
    )
    realized_gross_return = float((gross_realized_total + gross_unrealized) / gross_notional)
    realized_net_return = float((net_realized_total + net_unrealized) / gross_notional)
    return {
        "forward_gross_return": realized_gross_return,
        "forward_net_return": realized_net_return,
        "positive_net_return": int(realized_net_return > 0.0),
        "label_source": "realized_candidate_proxy",
        "label_mode": "realized",
        "fallback_reason": "",
        "excluded_unlabeled": False,
    }


def _resolve_target_label(
    *,
    target_type: str,
    hybrid_alpha: float,
    row: dict[str, Any],
    day_dir: Path,
    ordered_day_dirs: list[Path],
    day_index_by_date: dict[str, int],
    as_of_date: str | None,
    horizon_days: int,
    frame_cache: dict[str, pd.DataFrame],
) -> dict[str, Any] | None:
    normalized_target_type = str(target_type or "market_proxy").lower()
    market_label = _market_proxy_label(
        row=row,
        as_of_date=str(row.get("date") or day_dir.name),
        horizon_days=horizon_days,
        frame_cache=frame_cache,
    )
    if market_label is None:
        return None
    realized_label = None
    if normalized_target_type in {"realized_candidate_proxy", "realized_trade_proxy", "hybrid_proxy"}:
        realized_label = _realized_candidate_proxy_label(
            row=row,
            day_dir=day_dir,
            ordered_day_dirs=ordered_day_dirs,
            day_index_by_date=day_index_by_date,
            as_of_date=as_of_date,
            horizon_days=horizon_days,
            frame_cache=frame_cache,
        )
        if realized_label is None:
            return None
    if normalized_target_type == "hybrid_proxy":
        realized_component_available = str(realized_label.get("label_source") or "") == "realized_candidate_proxy"
        if realized_component_available:
            gross_return = (float(hybrid_alpha) * _safe_float(market_label.get("forward_gross_return"))) + (
                (1.0 - float(hybrid_alpha)) * _safe_float(realized_label.get("forward_gross_return"))
            )
            net_return = (float(hybrid_alpha) * _safe_float(market_label.get("forward_net_return"))) + (
                (1.0 - float(hybrid_alpha)) * _safe_float(realized_label.get("forward_net_return"))
            )
        else:
            gross_return = _safe_float(market_label.get("forward_gross_return"))
            net_return = _safe_float(market_label.get("forward_net_return"))
        return {
            "forward_gross_return": float(gross_return),
            "forward_net_return": float(net_return),
            "positive_net_return": int(net_return > 0.0),
            "label_source": "hybrid_proxy" if realized_component_available else "hybrid_market_fallback",
            "label_mode": "hybrid",
            "realized_component_available": bool(realized_component_available),
            "market_proxy_forward_gross_return": _safe_float(market_label.get("forward_gross_return")),
            "market_proxy_forward_net_return": _safe_float(market_label.get("forward_net_return")),
            "realized_candidate_forward_gross_return": (
                _safe_float(realized_label.get("forward_gross_return")) if realized_component_available else None
            ),
            "realized_candidate_forward_net_return": (
                _safe_float(realized_label.get("forward_net_return")) if realized_component_available else None
            ),
            "hybrid_alpha": float(hybrid_alpha),
            "fallback_reason": "" if realized_component_available else str(realized_label.get("fallback_reason") or ""),
            "excluded_unlabeled": False,
        }
    if realized_label is not None:
        return {
            **realized_label,
            "realized_component_available": str(realized_label.get("label_source") or "") == "realized_candidate_proxy",
            "market_proxy_forward_gross_return": _safe_float(market_label.get("forward_gross_return")),
            "market_proxy_forward_net_return": _safe_float(market_label.get("forward_net_return")),
            "realized_candidate_forward_gross_return": _safe_float(realized_label.get("forward_gross_return")),
            "realized_candidate_forward_net_return": _safe_float(realized_label.get("forward_net_return")),
            "hybrid_alpha": float(hybrid_alpha),
        }
    return {
        **market_label,
        "realized_component_available": False,
        "market_proxy_forward_gross_return": _safe_float(market_label.get("forward_gross_return")),
        "market_proxy_forward_net_return": _safe_float(market_label.get("forward_net_return")),
        "realized_candidate_forward_gross_return": None,
        "realized_candidate_forward_net_return": None,
        "hybrid_alpha": float(hybrid_alpha),
    }


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
    matrix["score_vol_interaction"] = matrix["score_percentile"] * matrix["recent_vol_20d"]
    matrix["score_cost_interaction"] = matrix["score_percentile"] * matrix["estimated_execution_cost_pct"]
    matrix["weight_cost_interaction"] = matrix["weight_delta_abs"] * matrix["estimated_execution_cost_pct"]
    return matrix[LINEAR_FEATURE_COLUMNS].to_numpy(dtype=float)


def _training_summary_from_rows(
    *,
    rows: list[dict[str, Any]],
    horizon_days: int,
    warnings: list[str],
    training_source: str,
    target_type: str = "market_proxy",
    hybrid_alpha: float = 0.8,
    candidate_row_count: int = 0,
    executed_row_count: int = 0,
    skipped_row_count: int = 0,
    excluded_unlabeled_row_count: int = 0,
    realized_label_count: int = 0,
    proxy_fallback_count: int = 0,
    feature_missingness: dict[str, float] | None = None,
) -> dict[str, Any]:
    positive_rate = (
        float(pd.Series([_safe_float(row.get("positive_net_return")) for row in rows], dtype=float).mean())
        if rows
        else 0.0
    )
    average_target_value = (
        float(pd.Series([_safe_float(row.get("forward_net_return")) for row in rows], dtype=float).mean())
        if rows
        else 0.0
    )
    hybrid_row_count = sum(1 for row in rows if str(row.get("label_source") or "") == "hybrid_proxy")
    market_only_fallback_row_count = sum(
        1 for row in rows if str(row.get("label_source") or "") in {"hybrid_market_fallback", "market_proxy_fallback"}
    )
    realized_component_available_count = sum(1 for row in rows if bool(row.get("realized_component_available")))
    average_market_proxy_target = (
        float(pd.Series([_safe_float(row.get("market_proxy_forward_net_return")) for row in rows], dtype=float).mean())
        if rows
        else 0.0
    )
    realized_target_values = [
        _safe_float(row.get("realized_candidate_forward_net_return"))
        for row in rows
        if row.get("realized_candidate_forward_net_return") is not None
    ]
    average_realized_proxy_target = (
        float(pd.Series(realized_target_values, dtype=float).mean()) if realized_target_values else 0.0
    )
    return {
        "training_source": str(training_source),
        "target_type": str(target_type or "market_proxy"),
        "hybrid_alpha": float(hybrid_alpha),
        "training_sample_count": len(rows),
        "labeled_row_count": len(rows),
        "candidate_row_count": int(candidate_row_count),
        "executed_row_count": int(executed_row_count),
        "skipped_row_count": int(skipped_row_count),
        "excluded_unlabeled_row_count": int(excluded_unlabeled_row_count),
        "average_label_horizon_completeness": (
            float(len(rows) / max(candidate_row_count, 1)) if candidate_row_count > 0 else 0.0
        ),
        "hybrid_row_count": int(hybrid_row_count),
        "market_only_fallback_row_count": int(market_only_fallback_row_count),
        "realized_component_available_ratio": (
            float(realized_component_available_count / len(rows)) if rows else 0.0
        ),
        "positive_label_rate": positive_rate,
        "average_target_value": average_target_value,
        "average_market_proxy_target": average_market_proxy_target,
        "average_realized_proxy_target": average_realized_proxy_target,
        "average_hybrid_target": average_target_value if str(target_type or "").lower() == "hybrid_proxy" else 0.0,
        "realized_label_count": int(realized_label_count),
        "proxy_fallback_count": int(proxy_fallback_count),
        "training_day_count": len({str(row.get("date") or "") for row in rows if row.get("date")}),
        "horizon_days": int(horizon_days),
        "training_window_start": rows[0]["date"] if rows else None,
        "training_window_end": rows[-1]["date"] if rows else None,
        "warnings": warnings,
        "target_definition": _target_definition(target_type, horizon_days),
        "executed_only": training_source == "executed_trades",
        "feature_missingness": dict(feature_missingness or {}),
    }


def _build_executed_trade_ev_training_dataset(
    *,
    history_root: str | Path | None,
    as_of_date: str,
    horizon_days: int,
    target_type: str = "market_proxy",
    hybrid_alpha: float = 0.8,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if history_root is None:
        return [], _training_summary_from_rows(
            rows=[],
            horizon_days=horizon_days,
            warnings=["missing_ev_training_root"],
            training_source="executed_trades",
            target_type=target_type,
            hybrid_alpha=hybrid_alpha,
        )
    frame_cache: dict[str, pd.DataFrame] = {}
    rows: list[dict[str, Any]] = []
    candidate_row_count = 0
    executed_row_count = 0
    skipped_row_count = 0
    excluded_unlabeled_row_count = 0
    realized_label_count = 0
    proxy_fallback_count = 0
    ordered_day_dirs = _all_replay_day_dirs(history_root)
    day_index_by_date = {str(pd.Timestamp(path.name).date()): index for index, path in enumerate(ordered_day_dirs)}
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
            label = _resolve_target_label(
                target_type=target_type,
                hybrid_alpha=hybrid_alpha,
                row={
                    **row,
                    "estimated_execution_cost_pct": _cost_pct_from_fills(fills_frame, symbol),
                    "candidate_outcome": "executed",
                },
                day_dir=day_dir,
                ordered_day_dirs=ordered_day_dirs,
                day_index_by_date=day_index_by_date,
                as_of_date=as_of_date,
                horizon_days=horizon_days,
                frame_cache=frame_cache,
            )
            if label is None:
                excluded_unlabeled_row_count += 1
                continue
            feature_row = _feature_snapshot(
                symbol=symbol,
                as_of_date=str(row.get("date") or day_dir.name),
                horizon_days=max(horizon_days, 10),
                frame_cache=frame_cache,
            )
            if feature_row is None:
                feature_row = {}
            side_sign = _action_side_sign(row)
            if side_sign == 0.0:
                continue
            if str(label.get("label_mode") or "") == "realized":
                realized_label_count += 1
            if "fallback" in str(label.get("label_source") or ""):
                proxy_fallback_count += 1
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
                    "estimated_execution_cost_pct": _cost_pct_from_fills(fills_frame, symbol),
                    "recent_return_3d": _safe_float(feature_row.get("recent_return_3d")),
                    "recent_return_5d": _safe_float(feature_row.get("recent_return_5d")),
                    "recent_return_10d": _safe_float(feature_row.get("recent_return_10d")),
                    "recent_vol_20d": _safe_float(feature_row.get("recent_vol_20d")),
                    "dollar_volume": _safe_float(feature_row.get("dollar_volume")),
                    "forward_gross_return": _safe_float(label.get("forward_gross_return")),
                    "forward_net_return": _safe_float(label.get("forward_net_return")),
                    "positive_net_return": int(_safe_float(label.get("positive_net_return"))),
                    "label_source": str(label.get("label_source") or ""),
                    "realized_component_available": bool(label.get("realized_component_available", False)),
                    "market_proxy_forward_gross_return": label.get("market_proxy_forward_gross_return"),
                    "market_proxy_forward_net_return": label.get("market_proxy_forward_net_return"),
                    "realized_candidate_forward_gross_return": label.get("realized_candidate_forward_gross_return"),
                    "realized_candidate_forward_net_return": label.get("realized_candidate_forward_net_return"),
                }
            )
    normalized_rows = _normalize_records(rows)
    summary = _training_summary_from_rows(
        rows=normalized_rows,
        horizon_days=horizon_days,
        warnings=[] if normalized_rows else ["insufficient_trade_history_for_ev_gate"],
        training_source="executed_trades",
        target_type=target_type,
        hybrid_alpha=hybrid_alpha,
        candidate_row_count=candidate_row_count,
        executed_row_count=executed_row_count,
        skipped_row_count=skipped_row_count,
        excluded_unlabeled_row_count=excluded_unlabeled_row_count,
        realized_label_count=realized_label_count,
        proxy_fallback_count=proxy_fallback_count,
    )
    return normalized_rows, summary


def _build_candidate_trade_ev_training_dataset(
    *,
    history_root: str | Path | None,
    as_of_date: str,
    horizon_days: int,
    target_type: str = "market_proxy",
    hybrid_alpha: float = 0.8,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if history_root is None:
        return [], _training_summary_from_rows(
            rows=[],
            horizon_days=horizon_days,
            warnings=["missing_ev_training_root"],
            training_source="candidate_decisions",
            target_type=target_type,
            hybrid_alpha=hybrid_alpha,
        )
    frame_cache: dict[str, pd.DataFrame] = {}
    rows: list[dict[str, Any]] = []
    candidate_row_count = 0
    executed_row_count = 0
    skipped_row_count = 0
    excluded_unlabeled_row_count = 0
    realized_label_count = 0
    proxy_fallback_count = 0
    ordered_day_dirs = _all_replay_day_dirs(history_root)
    day_index_by_date = {str(pd.Timestamp(path.name).date()): index for index, path in enumerate(ordered_day_dirs)}
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
            label = _resolve_target_label(
                target_type=target_type,
                hybrid_alpha=hybrid_alpha,
                row=row,
                day_dir=day_dir,
                ordered_day_dirs=ordered_day_dirs,
                day_index_by_date=day_index_by_date,
                as_of_date=as_of_date,
                horizon_days=horizon_days,
                frame_cache=frame_cache,
            )
            if label is None:
                excluded_unlabeled_row_count += 1
                continue
            feature_row = _feature_snapshot(
                symbol=symbol,
                as_of_date=str(row.get("date") or day_dir.name),
                horizon_days=max(horizon_days, 10),
                frame_cache=frame_cache,
            )
            if feature_row is None:
                feature_row = {}
            for key in missing_feature_counts:
                if row.get(key) in (None, ""):
                    missing_feature_counts[key] += 1
            if str(label.get("label_mode") or "") == "realized":
                realized_label_count += 1
            if "fallback" in str(label.get("label_source") or ""):
                proxy_fallback_count += 1
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
                    "estimated_execution_cost_pct": _safe_float(row.get("estimated_execution_cost_pct")),
                    "recent_return_3d": _safe_float(row.get("recent_return_3d", feature_row.get("recent_return_3d"))),
                    "recent_return_5d": _safe_float(row.get("recent_return_5d", feature_row.get("recent_return_5d"))),
                    "recent_return_10d": _safe_float(row.get("recent_return_10d", feature_row.get("recent_return_10d"))),
                    "recent_vol_20d": _safe_float(row.get("recent_vol_20d", feature_row.get("recent_vol_20d"))),
                    "dollar_volume": _safe_float(row.get("dollar_volume", feature_row.get("dollar_volume"))),
                    "forward_gross_return": _safe_float(label.get("forward_gross_return")),
                    "forward_net_return": _safe_float(label.get("forward_net_return")),
                    "positive_net_return": int(_safe_float(label.get("positive_net_return"))),
                    "label_source": str(label.get("label_source") or ""),
                    "realized_component_available": bool(label.get("realized_component_available", False)),
                    "market_proxy_forward_gross_return": label.get("market_proxy_forward_gross_return"),
                    "market_proxy_forward_net_return": label.get("market_proxy_forward_net_return"),
                    "realized_candidate_forward_gross_return": label.get("realized_candidate_forward_gross_return"),
                    "realized_candidate_forward_net_return": label.get("realized_candidate_forward_net_return"),
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
        target_type=target_type,
        hybrid_alpha=hybrid_alpha,
        candidate_row_count=candidate_row_count,
        executed_row_count=executed_row_count,
        skipped_row_count=skipped_row_count,
        excluded_unlabeled_row_count=excluded_unlabeled_row_count,
        realized_label_count=realized_label_count,
        proxy_fallback_count=proxy_fallback_count,
        feature_missingness=feature_missingness,
    )
    return normalized_rows, summary


def build_trade_ev_training_dataset(
    *,
    history_root: str | Path | None,
    as_of_date: str,
    horizon_days: int,
    training_source: str = "executed_trades",
    target_type: str = "market_proxy",
    hybrid_alpha: float = 0.8,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    normalized_source = str(training_source or "executed_trades").lower()
    normalized_target_type = str(target_type or "market_proxy").lower()
    if normalized_target_type not in TARGET_TYPES:
        raise ValueError(f"Unsupported EV target type: {target_type}")
    if normalized_source == "candidate_decisions":
        return _build_candidate_trade_ev_training_dataset(
            history_root=history_root,
            as_of_date=as_of_date,
            horizon_days=horizon_days,
            target_type=normalized_target_type,
            hybrid_alpha=hybrid_alpha,
        )
    return _build_executed_trade_ev_training_dataset(
        history_root=history_root,
        as_of_date=as_of_date,
        horizon_days=horizon_days,
        target_type=normalized_target_type,
        hybrid_alpha=hybrid_alpha,
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


def _normalization_group_series(frame: pd.DataFrame, normalize_within: str) -> pd.Series:
    mode = str(normalize_within or "all_candidates").lower()
    if mode == "by_side":
        return frame["weight_delta"].map(lambda value: "buy" if _safe_float(value) > 0.0 else "sell").astype(str)
    if mode == "by_strategy":
        return frame["strategy_id"].astype(str).replace({"": "unknown"})
    return pd.Series(["all_candidates"] * len(frame.index), index=frame.index, dtype=object)


def _normalize_candidate_scores(
    *,
    frame: pd.DataFrame,
    raw_score_column: str,
    normalize_scores: bool,
    normalization_method: str,
    normalize_within: str,
    score_clip_min: float | None,
    score_clip_max: float | None,
) -> pd.DataFrame:
    result = frame.copy()
    result["raw_ev_score"] = pd.to_numeric(result[raw_score_column], errors="coerce").fillna(0.0)
    result["normalization_method"] = str(normalization_method if normalize_scores else "disabled")
    result["normalize_within"] = str(normalize_within if normalize_scores else "all_candidates")
    result["normalized_ev_score"] = result["raw_ev_score"]
    result["candidate_count_for_normalization"] = len(result.index)
    if normalize_scores and not result.empty:
        method = str(normalization_method or "zscore").lower()
        groups = _normalization_group_series(result, normalize_within)
        normalized = pd.Series(index=result.index, dtype=float)
        counts = pd.Series(index=result.index, dtype=float)
        for _, group_frame in result.groupby(groups, dropna=False):
            series = pd.to_numeric(group_frame["raw_ev_score"], errors="coerce").fillna(0.0)
            if series.empty:
                continue
            counts.loc[group_frame.index] = float(len(series.index))
            if method == "rank_pct":
                if len(series.index) == 1:
                    normalized.loc[group_frame.index] = 0.0
                else:
                    ranks = series.rank(method="average").astype(float)
                    normalized.loc[group_frame.index] = (((ranks - 1.0) / float(len(series.index) - 1)) - 0.5).astype(
                        float
                    )
            elif method == "robust_zscore":
                median = float(series.median())
                mad = float((series - median).abs().median())
                scale = 1.4826 * mad
                if scale <= 0.0:
                    normalized.loc[group_frame.index] = 0.0
                else:
                    normalized.loc[group_frame.index] = ((series - median) / scale).astype(float)
            else:
                std = float(series.std(ddof=0) or 0.0)
                if std <= 0.0:
                    normalized.loc[group_frame.index] = 0.0
                else:
                    normalized.loc[group_frame.index] = ((series - float(series.mean())) / std).astype(float)
        result["normalized_ev_score"] = normalized.fillna(0.0)
        result["candidate_count_for_normalization"] = counts.fillna(float(len(result.index))).astype(int)
    result["ev_score_pre_clip"] = result["normalized_ev_score"] if normalize_scores else result["raw_ev_score"]
    result["ev_score_post_clip"] = result["ev_score_pre_clip"].astype(float)
    if score_clip_min is not None:
        result["ev_score_post_clip"] = result["ev_score_post_clip"].clip(lower=float(score_clip_min))
    if score_clip_max is not None:
        result["ev_score_post_clip"] = result["ev_score_post_clip"].clip(upper=float(score_clip_max))
    result["ev_score_clipped"] = (
        (result["ev_score_post_clip"] - result["ev_score_pre_clip"]).abs() > 1e-12
    ).astype(bool)
    return result


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
    normalization_method: str = "zscore",
    normalize_within: str = "all_candidates",
    use_normalized_score_for_weighting: bool = True,
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
    prediction_frame = pd.DataFrame(staged_predictions)
    if not prediction_frame.empty:
        prediction_frame = _normalize_candidate_scores(
            frame=prediction_frame,
            raw_score_column="raw_ev_score",
            normalize_scores=normalize_scores,
            normalization_method=normalization_method,
            normalize_within=normalize_within,
            score_clip_min=score_clip_min,
            score_clip_max=score_clip_max,
        )
    for index, row in enumerate(staged_predictions):
        normalized_row = prediction_frame.iloc[index] if not prediction_frame.empty else None
        raw_ev_score = float(row["raw_ev_score"])
        normalized_ev_score = (
            float(normalized_row["normalized_ev_score"]) if normalized_row is not None else raw_ev_score
        )
        ev_score_pre_clip = (
            float(normalized_row["ev_score_pre_clip"]) if normalized_row is not None else raw_ev_score
        )
        ev_score_post_clip = (
            float(normalized_row["ev_score_post_clip"]) if normalized_row is not None else raw_ev_score
        )
        ev_weighting_score = ev_score_post_clip if use_normalized_score_for_weighting else raw_ev_score
        decision = "allow"
        if raw_ev_score < float(min_expected_net_return):
            decision = "block"
        if min_probability_positive is not None and float(row["probability_positive"]) < float(min_probability_positive):
            decision = "block"
        predictions.append(
            {
                **row,
                "normalized_ev_score": normalized_ev_score,
                "ev_score_pre_clip": ev_score_pre_clip,
                "ev_score_post_clip": ev_score_post_clip,
                "ev_score_clipped": bool(normalized_row["ev_score_clipped"]) if normalized_row is not None else False,
                "ev_weighting_score": ev_weighting_score,
                "normalization_method": str(
                    normalized_row["normalization_method"] if normalized_row is not None else "disabled"
                ),
                "normalize_within": str(
                    normalized_row["normalize_within"] if normalized_row is not None else "all_candidates"
                ),
                "candidate_count_for_normalization": int(
                    normalized_row["candidate_count_for_normalization"] if normalized_row is not None else len(staged_predictions)
                ),
                "ev_decision_score": raw_ev_score,
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
            "avg_raw_ev_score": 0.0,
            "avg_normalized_ev_score": 0.0,
            "avg_ev_score_post_clip": 0.0,
        }
        return [], empty_summary
    frame = pd.DataFrame(prediction_rows)
    frame["expected_net_return"] = _frame_numeric_column(frame, "expected_net_return", 0.0)
    frame["expected_gross_return"] = _frame_numeric_column(frame, "expected_gross_return", 0.0)
    frame["realized_net_return"] = _frame_numeric_column(frame, "realized_net_return", 0.0)
    frame["realized_gross_return"] = _frame_numeric_column(frame, "realized_gross_return", 0.0)
    frame["execution_cost"] = _frame_numeric_column(frame, "execution_cost", 0.0)
    frame["probability_positive"] = _frame_numeric_column(frame, "probability_positive", 0.0)
    frame["ev_weight_multiplier"] = _frame_numeric_column(frame, "ev_weight_multiplier", 1.0)
    frame["raw_ev_score"] = _frame_numeric_column(frame, "raw_ev_score", 0.0)
    frame["normalized_ev_score"] = _frame_numeric_column(frame, "normalized_ev_score", 0.0)
    frame["ev_score_post_clip"] = _frame_numeric_column(frame, "ev_score_post_clip", 0.0)
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
            avg_raw_ev_score=("raw_ev_score", "mean"),
            avg_normalized_ev_score=("normalized_ev_score", "mean"),
            avg_ev_score_post_clip=("ev_score_post_clip", "mean"),
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
        "avg_raw_ev_score": float(frame["raw_ev_score"].mean()),
        "avg_normalized_ev_score": float(frame["normalized_ev_score"].mean()),
        "avg_ev_score_post_clip": float(frame["ev_score_post_clip"].mean()),
        "bucket_rows": bucket_frame.astype(object).where(pd.notna(bucket_frame), None).to_dict(orient="records"),
    }
    calibration_rows = frame[CALIBRATION_COLUMNS].to_dict(orient="records")
    return _normalize_records(calibration_rows), summary


def evaluate_replay_trade_ev_predictions(
    *,
    replay_root: str | Path,
    horizon_days: int,
    target_type: str = "market_proxy",
    hybrid_alpha: float = 0.8,
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
            "prediction_row_count": 0,
            "labeled_prediction_count": 0,
            "excluded_unlabeled_prediction_count": 0,
            "label_coverage_ratio": 0.0,
            "rank_correlation": 0.0,
            "top_vs_bottom_bucket_spread": 0.0,
            "bucket_monotonicity": False,
            "calibration_error": 0.0,
            "top_bucket_realized_net_return": 0.0,
            "bottom_bucket_realized_net_return": 0.0,
            "target_type": str(target_type or "market_proxy"),
            "hybrid_alpha": float(hybrid_alpha),
            "realized_component_available_ratio": 0.0,
            "hybrid_row_count": 0,
            "market_only_fallback_row_count": 0,
        }
    frame_cache: dict[str, pd.DataFrame] = {}
    ordered_day_dirs = _all_replay_day_dirs(root)
    day_index_by_date = {str(pd.Timestamp(path.name).date()): index for index, path in enumerate(ordered_day_dirs)}
    realized_rows: list[dict[str, Any]] = []
    for row in prediction_rows:
        symbol = str(row.get("symbol") or "")
        as_of_date = str(row.get("date") or "")
        if not symbol or not as_of_date:
            continue
        day_dir = root / as_of_date
        label = _resolve_target_label(
            target_type=target_type,
            hybrid_alpha=hybrid_alpha,
            row={**row, "candidate_outcome": "executed"},
            day_dir=day_dir,
            ordered_day_dirs=ordered_day_dirs,
            day_index_by_date=day_index_by_date,
            as_of_date=None,
            horizon_days=horizon_days,
            frame_cache=frame_cache,
        )
        if label is None:
            continue
        realized_rows.append(
            {
                **row,
                "realized_gross_return": _safe_float(label.get("forward_gross_return")),
                "realized_net_return": _safe_float(label.get("forward_net_return")),
                "execution_cost": float(
                    _safe_float(label.get("forward_gross_return")) - _safe_float(label.get("forward_net_return"))
                ),
                "label_source": str(label.get("label_source") or ""),
                "realized_component_available": bool(label.get("realized_component_available", False)),
                "market_proxy_forward_net_return": label.get("market_proxy_forward_net_return"),
                "realized_candidate_forward_net_return": label.get("realized_candidate_forward_net_return"),
            }
        )
    calibration_rows, calibration_summary = build_trade_ev_calibration(prediction_rows=realized_rows)
    bucket_rows = list(calibration_summary.pop("bucket_rows", []))
    calibration_summary["prediction_row_count"] = int(len(prediction_rows))
    calibration_summary["labeled_prediction_count"] = int(len(realized_rows))
    calibration_summary["excluded_unlabeled_prediction_count"] = int(len(prediction_rows) - len(realized_rows))
    calibration_summary["label_coverage_ratio"] = (
        float(len(realized_rows) / len(prediction_rows)) if prediction_rows else 0.0
    )
    calibration_summary["target_type"] = str(target_type or "market_proxy")
    calibration_summary["hybrid_alpha"] = float(hybrid_alpha)
    calibration_summary["hybrid_row_count"] = int(
        sum(1 for row in realized_rows if str(row.get("label_source") or "") == "hybrid_proxy")
    )
    calibration_summary["market_only_fallback_row_count"] = int(
        sum(
            1
            for row in realized_rows
            if str(row.get("label_source") or "") in {"hybrid_market_fallback", "market_proxy_fallback"}
        )
    )
    calibration_summary["realized_component_available_ratio"] = (
        float(sum(1 for row in realized_rows if bool(row.get("realized_component_available"))) / len(realized_rows))
        if realized_rows
        else 0.0
    )
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

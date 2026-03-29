from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.signals.loaders import load_feature_frame

TRADE_EV_LIFECYCLE_COLUMNS = [
    "trade_id",
    "date",
    "symbol",
    "strategy_id",
    "signal_source",
    "signal_family",
    "side",
    "quantity",
    "entry_date",
    "exit_date",
    "entry_reference_price",
    "exit_reference_price",
    "ev_entry",
    "score_entry",
    "score_percentile_entry",
    "ev_exit",
    "score_exit",
    "score_percentile_exit",
    "exit_reason",
    "realized_pnl",
    "gross_realized_pnl",
    "realized_return",
    "mfe_pnl",
    "mae_pnl",
    "mfe_return",
    "mae_return",
    "holding_period_days",
    "exit_efficiency",
    "ev_decay",
    "ev_alignment",
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


def _load_price_path(
    *,
    symbol: str,
    entry_date: str | None,
    exit_date: str | None,
    frame_cache: dict[str, pd.DataFrame],
) -> list[float]:
    if not symbol or not entry_date or not exit_date:
        return []
    if symbol not in frame_cache:
        try:
            frame_cache[symbol] = load_feature_frame(symbol)
        except Exception:
            return []
    frame = frame_cache[symbol].copy()
    if frame.empty or "timestamp" not in frame.columns or "close" not in frame.columns:
        return []
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=False)
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame.sort_values("timestamp", kind="stable")
    start = pd.Timestamp(entry_date).date()
    end = pd.Timestamp(exit_date).date()
    window = frame[(frame["timestamp"].dt.date >= start) & (frame["timestamp"].dt.date <= end)]
    if window.empty:
        return []
    return [float(value) for value in pd.to_numeric(window["close"], errors="coerce").dropna().tolist()]


def _bucket_label(series: pd.Series, bucket_count: int) -> pd.Series:
    filled = pd.to_numeric(series, errors="coerce").fillna(0.0)
    if filled.empty:
        return pd.Series(dtype=object)
    if filled.nunique(dropna=False) <= 1:
        return pd.Series(["b1"] * len(filled), index=filled.index)
    rank = filled.rank(method="first")
    buckets = pd.qcut(rank, q=min(bucket_count, len(filled)), labels=False, duplicates="drop")
    return buckets.fillna(0).astype(int).map(lambda value: f"b{int(value) + 1}")


def build_trade_ev_lifecycle_rows(
    *,
    trade_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    frame_cache: dict[str, pd.DataFrame] = {}
    for trade in trade_rows:
        if str(trade.get("status") or "").lower() != "closed":
            continue
        symbol = str(trade.get("symbol") or "")
        side = str(trade.get("side") or "").lower()
        quantity = abs(int(_safe_float(trade.get("quantity"))))
        entry_reference_price = _safe_float(trade.get("entry_reference_price"))
        exit_reference_price = _safe_float(trade.get("exit_reference_price"))
        entry_date = trade.get("entry_date")
        exit_date = trade.get("exit_date")
        realized_pnl = _safe_float(trade.get("realized_pnl", trade.get("net_realized_pnl")))
        gross_realized_pnl = _safe_float(trade.get("gross_realized_pnl"))
        entry_notional = float(quantity * entry_reference_price)
        price_path = _load_price_path(
            symbol=symbol,
            entry_date=str(entry_date) if entry_date else None,
            exit_date=str(exit_date) if exit_date else None,
            frame_cache=frame_cache,
        )
        if not price_path:
            price_path = [entry_reference_price, exit_reference_price] if exit_reference_price > 0.0 else [entry_reference_price]
        if side == "short":
            pnl_path = [(entry_reference_price - price) * quantity for price in price_path]
        else:
            pnl_path = [(price - entry_reference_price) * quantity for price in price_path]
        mfe_pnl = max(pnl_path) if pnl_path else 0.0
        mae_pnl = min(pnl_path) if pnl_path else 0.0
        mfe_return = (mfe_pnl / entry_notional) if entry_notional > 0.0 else 0.0
        mae_return = (mae_pnl / entry_notional) if entry_notional > 0.0 else 0.0
        ev_entry = trade.get("ev_entry")
        ev_exit = trade.get("ev_exit")
        ev_entry_value = None if ev_entry is None else float(ev_entry)
        ev_exit_value = None if ev_exit is None else float(ev_exit)
        alignment = None
        if ev_entry_value not in (None, 0.0) and realized_pnl != 0.0:
            alignment = int((ev_entry_value > 0.0 and realized_pnl > 0.0) or (ev_entry_value < 0.0 and realized_pnl < 0.0))
        rows.append(
            {
                "trade_id": trade.get("trade_id"),
                "date": exit_date,
                "symbol": symbol,
                "strategy_id": trade.get("strategy_id"),
                "signal_source": trade.get("signal_source"),
                "signal_family": trade.get("signal_family"),
                "side": side,
                "quantity": quantity,
                "entry_date": entry_date,
                "exit_date": exit_date,
                "entry_reference_price": entry_reference_price,
                "exit_reference_price": exit_reference_price,
                "ev_entry": ev_entry_value,
                "score_entry": trade.get("score_entry"),
                "score_percentile_entry": trade.get("score_percentile_entry"),
                "ev_exit": ev_exit_value,
                "score_exit": trade.get("score_exit"),
                "score_percentile_exit": trade.get("score_percentile_exit"),
                "exit_reason": trade.get("exit_reason"),
                "realized_pnl": realized_pnl,
                "gross_realized_pnl": gross_realized_pnl,
                "realized_return": (realized_pnl / entry_notional) if entry_notional > 0.0 else 0.0,
                "mfe_pnl": float(mfe_pnl),
                "mae_pnl": float(mae_pnl),
                "mfe_return": float(mfe_return),
                "mae_return": float(mae_return),
                "holding_period_days": trade.get("holding_period_days"),
                "exit_efficiency": (float(realized_pnl / mfe_pnl) if mfe_pnl > 0.0 else None),
                "ev_decay": (float(ev_entry_value - ev_exit_value) if ev_entry_value is not None and ev_exit_value is not None else None),
                "ev_alignment": alignment,
            }
        )
    return _normalize_records(rows)


def summarize_trade_ev_lifecycle(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "trade_count": 0,
            "avg_EV_entry": 0.0,
            "avg_EV_exit": 0.0,
            "avg_exit_efficiency": 0.0,
            "EV_alignment_rate": 0.0,
            "pct_trades_EV_entry_positive": 0.0,
            "pct_exits_EV_exit_negative": 0.0,
            "EV_entry_realized_return_correlation": 0.0,
            "MFE_vs_EV_entry_correlation": 0.0,
            "EV_decay_stats": {"mean": 0.0, "median": 0.0, "min": 0.0, "max": 0.0},
            "bucket_rows": [],
        }
    frame = pd.DataFrame(rows)
    for column in ("ev_entry", "ev_exit", "realized_return", "mfe_pnl", "exit_efficiency", "ev_decay", "ev_alignment"):
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce")
    bucket_frame = frame.dropna(subset=["ev_entry"]).copy()
    if not bucket_frame.empty:
        bucket_frame["bucket"] = _bucket_label(bucket_frame["ev_entry"], 5)
        grouped = (
            bucket_frame.groupby("bucket", dropna=False)
            .agg(
                trade_count=("trade_id", "count"),
                avg_EV_entry=("ev_entry", "mean"),
                avg_realized_pnl=("realized_pnl", "mean"),
                avg_realized_return=("realized_return", "mean"),
                avg_MFE=("mfe_pnl", "mean"),
                avg_MAE=("mae_pnl", "mean"),
                EV_alignment_rate=("ev_alignment", "mean"),
            )
            .reset_index()
            .sort_values("bucket", kind="stable")
        )
        bucket_rows = grouped.astype(object).where(pd.notna(grouped), None).to_dict(orient="records")
    else:
        bucket_rows = []
    decay = frame["ev_decay"].dropna()
    alignment = frame["ev_alignment"].dropna()
    ev_entry_vs_realized = frame[["ev_entry", "realized_return"]].dropna()
    ev_entry_vs_mfe = frame[["ev_entry", "mfe_pnl"]].dropna()
    return {
        "trade_count": int(len(frame.index)),
        "avg_EV_entry": float(frame["ev_entry"].dropna().mean()) if frame["ev_entry"].notna().any() else 0.0,
        "avg_EV_exit": float(frame["ev_exit"].dropna().mean()) if frame["ev_exit"].notna().any() else 0.0,
        "avg_exit_efficiency": float(frame["exit_efficiency"].dropna().mean()) if frame["exit_efficiency"].notna().any() else 0.0,
        "EV_alignment_rate": float(alignment.mean()) if not alignment.empty else 0.0,
        "pct_trades_EV_entry_positive": float((frame["ev_entry"] > 0.0).mean()) if frame["ev_entry"].notna().any() else 0.0,
        "pct_exits_EV_exit_negative": float((frame["ev_exit"] < 0.0).mean()) if frame["ev_exit"].notna().any() else 0.0,
        "EV_entry_realized_return_correlation": (
            float(ev_entry_vs_realized["ev_entry"].corr(ev_entry_vs_realized["realized_return"], method="spearman"))
            if len(ev_entry_vs_realized.index) >= 2
            else 0.0
        ),
        "MFE_vs_EV_entry_correlation": (
            float(ev_entry_vs_mfe["ev_entry"].corr(ev_entry_vs_mfe["mfe_pnl"], method="spearman"))
            if len(ev_entry_vs_mfe.index) >= 2
            else 0.0
        ),
        "EV_decay_stats": {
            "mean": float(decay.mean()) if not decay.empty else 0.0,
            "median": float(decay.median()) if not decay.empty else 0.0,
            "min": float(decay.min()) if not decay.empty else 0.0,
            "max": float(decay.max()) if not decay.empty else 0.0,
        },
        "bucket_rows": bucket_rows,
    }


def write_trade_ev_lifecycle_artifacts(
    *,
    output_dir: str | Path,
    lifecycle_rows: list[dict[str, Any]],
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    lifecycle_path = output_path / "trade_ev_lifecycle.csv"
    pd.DataFrame(lifecycle_rows, columns=TRADE_EV_LIFECYCLE_COLUMNS).to_csv(lifecycle_path, index=False)
    return {"trade_ev_lifecycle_path": lifecycle_path}


def aggregate_replay_ev_lifecycle(
    *,
    replay_root: str | Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    root = Path(replay_root)
    rows: list[dict[str, Any]] = []
    for day_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        lifecycle_path = day_dir / "paper" / "trade_ev_lifecycle.csv"
        if not lifecycle_path.exists() or lifecycle_path.stat().st_size <= 0:
            continue
        try:
            frame = pd.read_csv(lifecycle_path)
        except (pd.errors.EmptyDataError, pd.errors.ParserError):
            continue
        if frame.empty:
            continue
        rows.extend(_normalize_records(frame.to_dict(orient="records")))
    return rows, summarize_trade_ev_lifecycle(rows)


def write_replay_ev_lifecycle_artifacts(
    *,
    replay_root: str | Path,
    lifecycle_rows: list[dict[str, Any]],
    summary: dict[str, Any],
) -> dict[str, Path]:
    root = Path(replay_root)
    lifecycle_path = root / "replay_trade_ev_lifecycle.csv"
    summary_path = root / "replay_ev_lifecycle_summary.json"
    pd.DataFrame(lifecycle_rows, columns=TRADE_EV_LIFECYCLE_COLUMNS).to_csv(lifecycle_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    return {
        "replay_trade_ev_lifecycle_csv_path": lifecycle_path,
        "replay_ev_lifecycle_summary_json_path": summary_path,
    }

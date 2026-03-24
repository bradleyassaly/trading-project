from __future__ import annotations

from typing import Any

import pandas as pd

from trading_platform.paper.models import PaperExecutionPriceSnapshot


def _normalize_timestamp(value: object) -> pd.Timestamp | None:
    timestamp = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(timestamp):
        return None
    if isinstance(timestamp, pd.DatetimeIndex):
        if len(timestamp) == 0:
            return None
        timestamp = timestamp.max()
    return pd.Timestamp(timestamp).tz_convert("UTC").tz_localize(None)


def _last_frame_timestamp(frame: pd.DataFrame) -> pd.Timestamp | None:
    if frame.empty:
        return None
    if "timestamp" in frame.columns:
        normalized = pd.to_datetime(frame["timestamp"], errors="coerce", utc=True).dropna()
        if normalized.empty:
            return None
        return pd.Timestamp(normalized.max()).tz_convert("UTC").tz_localize(None)
    if isinstance(frame.index, pd.DatetimeIndex) and len(frame.index) > 0:
        return _normalize_timestamp(frame.index.max())
    return None


def _last_frame_price(frame: pd.DataFrame) -> float | None:
    if frame.empty or "close" not in frame.columns:
        return None
    close = pd.to_numeric(frame["close"], errors="coerce").dropna()
    if close.empty:
        return None
    return float(close.iloc[-1])


def build_execution_price_snapshots(
    *,
    historical_frames: dict[str, pd.DataFrame],
    final_frames: dict[str, pd.DataFrame],
    historical_source: str,
    latest_data_source: str,
    fallback_used: bool,
    latest_data_max_age_seconds: int,
) -> list[PaperExecutionPriceSnapshot]:
    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    rows: list[PaperExecutionPriceSnapshot] = []
    for symbol in sorted(final_frames):
        historical_frame = historical_frames.get(symbol, pd.DataFrame())
        final_frame = final_frames.get(symbol, pd.DataFrame())
        historical_price = _last_frame_price(historical_frame)
        final_price = _last_frame_price(final_frame)
        final_timestamp = _last_frame_timestamp(final_frame)
        age_seconds: float | None = None
        stale: bool | None = None
        if final_timestamp is not None:
            age_seconds = float(max((now - final_timestamp).total_seconds(), 0.0))
            stale = age_seconds > float(latest_data_max_age_seconds)
        latest_price = final_price if latest_data_source == "alpaca" and not fallback_used else None
        price_source_used = latest_data_source if latest_price is not None else historical_source
        rows.append(
            PaperExecutionPriceSnapshot(
                symbol=symbol,
                decision_timestamp=final_timestamp.isoformat() if final_timestamp is not None else None,
                historical_price=historical_price,
                latest_price=latest_price,
                final_price_used=final_price,
                price_source_used=price_source_used,
                fallback_used=bool(fallback_used and latest_data_source != "alpaca"),
                latest_bar_timestamp=final_timestamp.isoformat() if final_timestamp is not None else None,
                latest_bar_age_seconds=age_seconds,
                latest_data_stale=stale,
                latest_data_source=latest_data_source,
            )
        )
    return rows


def summarize_execution_price_snapshots(
    snapshots: list[PaperExecutionPriceSnapshot],
    *,
    latest_data_source: str,
    fallback_used: bool,
) -> dict[str, Any]:
    timestamps = [row.latest_bar_timestamp for row in snapshots if row.latest_bar_timestamp]
    ages = [float(row.latest_bar_age_seconds) for row in snapshots if row.latest_bar_age_seconds is not None]
    stale_values = [bool(row.latest_data_stale) for row in snapshots if row.latest_data_stale is not None]
    latest_bar_timestamp = max(timestamps) if timestamps else None
    latest_bar_age_seconds = max(ages) if ages else None
    latest_data_stale = any(stale_values) if stale_values else None
    return {
        "latest_bar_timestamp": latest_bar_timestamp,
        "latest_bar_age_seconds": latest_bar_age_seconds,
        "latest_data_stale": latest_data_stale,
        "latest_data_source": latest_data_source,
        "latest_data_fallback_used": bool(fallback_used),
        "stale_symbol_count": sum(1 for row in snapshots if row.latest_data_stale is True),
        "snapshot_symbol_count": len(snapshots),
    }

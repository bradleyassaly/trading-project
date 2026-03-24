from __future__ import annotations

import json
import logging
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd


logger = logging.getLogger(__name__)

ALPACA_DATA_BASE_URL = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets")
_ALPACA_BAR_COLUMNS = ["date", "symbol", "open", "high", "low", "close", "volume", "source"]


def _normalize_alpaca_timestamp(value: Any) -> pd.Timestamp | None:
    if value is None:
        return None
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return None
    return timestamp.tz_convert(None)


def _require_alpaca_credentials() -> tuple[str, str]:
    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")
    if not api_key or not secret_key:
        raise ValueError("Missing Alpaca credentials. Set ALPACA_API_KEY and ALPACA_SECRET_KEY.")
    return api_key, secret_key


def _request_with_retry(url: str, headers: dict[str, str], *, max_attempts: int = 3, timeout: int = 30) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            request = Request(url, headers=headers, method="GET")
            with urlopen(request, timeout=timeout) as response:  # noqa: S310
                return json.loads(response.read().decode("utf-8"))
        except Exception as exc:  # pragma: no cover - network-specific failure surface
            last_error = exc
            if attempt >= max_attempts:
                break
            sleep_seconds = float(2 ** (attempt - 1))
            logger.warning("alpaca data request failed on attempt %s/%s: %s; retrying in %.1fs", attempt, max_attempts, exc, sleep_seconds)
            time.sleep(sleep_seconds)
    if last_error is None:  # pragma: no cover
        raise RuntimeError("unknown Alpaca request failure")
    raise last_error


def fetch_alpaca_bars(
    symbols: list[str],
    start: str,
    end: str,
    timeframe: str = "1Day",
) -> pd.DataFrame:
    api_key, secret_key = _require_alpaca_credentials()
    unique_symbols = [symbol.upper() for symbol in symbols if symbol]
    if not unique_symbols:
        return pd.DataFrame(columns=_ALPACA_BAR_COLUMNS)

    rows: list[dict[str, Any]] = []
    page_token: str | None = None
    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": secret_key,
        "Accept": "application/json",
    }
    while True:
        params = {
            "symbols": ",".join(unique_symbols),
            "start": start,
            "end": end,
            "timeframe": timeframe,
            "limit": 10000,
            "sort": "asc",
        }
        if page_token:
            params["page_token"] = page_token
        url = f"{ALPACA_DATA_BASE_URL.rstrip('/')}/v2/stocks/bars?{urlencode(params)}"
        payload = _request_with_retry(url, headers)
        for symbol, bar_rows in (payload.get("bars") or {}).items():
            for bar in bar_rows or []:
                timestamp = _normalize_alpaca_timestamp(bar.get("t"))
                if timestamp is None:
                    continue
                rows.append(
                    {
                        "date": timestamp.to_pydatetime().replace(tzinfo=None),
                        "symbol": str(symbol).upper(),
                        "open": float(bar.get("o", 0.0) or 0.0),
                        "high": float(bar.get("h", 0.0) or 0.0),
                        "low": float(bar.get("l", 0.0) or 0.0),
                        "close": float(bar.get("c", 0.0) or 0.0),
                        "volume": float(bar.get("v", 0.0) or 0.0),
                        "source": "alpaca",
                    }
                )
        page_token = payload.get("next_page_token")
        if not page_token:
            break

    if not rows:
        return pd.DataFrame(columns=_ALPACA_BAR_COLUMNS)

    frame = pd.DataFrame(rows, columns=_ALPACA_BAR_COLUMNS)
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.dropna(subset=["date"]).sort_values(["symbol", "date"]).reset_index(drop=True)
    logger.info("fetched Alpaca bars for %s symbol(s) timeframe=%s rows=%s", len(unique_symbols), timeframe, len(frame.index))
    return frame


def merge_historical_with_latest(historical_df: pd.DataFrame, latest_df: pd.DataFrame) -> pd.DataFrame:
    historical = historical_df.copy()
    latest = latest_df.copy()

    if historical.empty and latest.empty:
        return pd.DataFrame()

    for frame in (historical, latest):
        if "timestamp" in frame.columns and "date" not in frame.columns:
            frame["date"] = frame["timestamp"]
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        if "symbol" in frame.columns:
            frame["symbol"] = frame["symbol"].astype(str).str.upper()

    historical = historical.dropna(subset=["date"])
    latest = latest.dropna(subset=["date"])
    overlap_keys = latest[["symbol", "date"]].drop_duplicates() if not latest.empty else pd.DataFrame(columns=["symbol", "date"])
    if not overlap_keys.empty:
        historical = historical.merge(overlap_keys.assign(_drop=True), on=["symbol", "date"], how="left")
        historical = historical[historical["_drop"].isna()].drop(columns=["_drop"])

    combined = pd.concat([historical, latest], ignore_index=True, sort=False)
    if combined.empty:
        return combined
    combined = combined.drop_duplicates(subset=["symbol", "date"], keep="last").sort_values(["symbol", "date"]).reset_index(drop=True)
    return combined

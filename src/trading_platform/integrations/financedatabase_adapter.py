from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

import pandas as pd

from trading_platform.integrations.optional_dependencies import require_dependency


def _utc_today() -> str:
    return datetime.now(UTC).date().isoformat()


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_bool(value: Any) -> bool | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = str(value).strip().lower()
    if normalized in {"true", "yes", "y", "1"}:
        return True
    if normalized in {"false", "no", "n", "0"}:
        return False
    return None


def _normalize_asset_type(value: Any) -> str | None:
    text = _safe_str(value)
    if text is None:
        return None
    normalized = text.lower()
    mapping = {
        "equities": "equity",
        "equity": "equity",
        "stocks": "equity",
        "etfs": "etf",
        "etf": "etf",
        "funds": "fund",
        "fund": "fund",
        "indices": "index",
        "index": "index",
    }
    return mapping.get(normalized, normalized)


def _extract_series(frame: pd.DataFrame, *candidates: str) -> pd.Series:
    for candidate in candidates:
        if candidate not in frame.columns:
            continue
        column = frame[candidate]
        if isinstance(column, pd.DataFrame):
            for nested_name in column.columns:
                nested_series = column[nested_name]
                if nested_series.notna().any():
                    return nested_series
            return column.iloc[:, 0]
        return column
    return pd.Series(index=frame.index, dtype="object")


@dataclass(frozen=True)
class SecurityMasterBuildResult:
    frame: pd.DataFrame
    source: str
    as_of_date: str
    requested_symbols: list[str]
    matched_symbols: list[str]
    unmatched_symbols: list[str]
    duplicate_symbol_count: int


def _normalize_financedatabase_frame(
    frame: pd.DataFrame,
    *,
    source: str,
    as_of_date: str,
) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "asset_type",
                "name",
                "sector",
                "industry_group",
                "industry",
                "category",
                "exchange",
                "country",
                "currency",
                "is_etf",
                "source",
                "as_of_date",
            ]
        )

    normalized = frame.copy()
    normalized.columns = [str(column).strip().lower() for column in normalized.columns]
    if "symbol" not in normalized.columns:
        if normalized.index.name and str(normalized.index.name).strip().lower() in {"symbol", "ticker"}:
            normalized = normalized.reset_index()
            normalized.columns = [str(column).strip().lower() for column in normalized.columns]
        elif normalized.index.dtype == "object":
            normalized = normalized.reset_index().rename(columns={"index": "symbol"})

    if "symbol" not in normalized.columns:
        raise ValueError("FinanceDatabase classification data must include a symbol column or symbol index")

    result = pd.DataFrame(
        {
            "symbol": _extract_series(normalized, "symbol", "ticker").map(_safe_str),
            "asset_type": _extract_series(normalized, "asset_type", "type", "quote_type").map(_normalize_asset_type),
            "name": _extract_series(normalized, "name", "company_name", "summary").map(_safe_str),
            "sector": _extract_series(normalized, "sector").map(_safe_str),
            "industry_group": _extract_series(normalized, "industry_group", "group", "industry_group_name").map(
                _safe_str
            ),
            "industry": _extract_series(normalized, "industry").map(_safe_str),
            "category": _extract_series(normalized, "category", "category_group").map(_safe_str),
            "exchange": _extract_series(normalized, "exchange", "market", "market_exchange").map(_safe_str),
            "country": _extract_series(normalized, "country", "locale").map(_safe_str),
            "currency": _extract_series(normalized, "currency", "currency_code").map(_safe_str),
            "is_etf": _extract_series(normalized, "is_etf", "etf").map(_safe_bool),
            "source": source,
            "as_of_date": as_of_date,
        }
    )
    result["asset_type"] = result["asset_type"].fillna("equity")
    result = result.dropna(subset=["symbol"]).drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)
    return result


def _fetch_financedatabase_frames(symbols: list[str], *, package_override=None) -> pd.DataFrame:
    financedatabase = require_dependency(
        "financedatabase",
        purpose="building reference classifications",
        package_override=package_override,
    )
    frames: list[pd.DataFrame] = []
    selectors: list[tuple[str, str]] = [
        ("equities", "equities"),
        ("etfs", "etfs"),
        ("funds", "funds"),
        ("indices", "indices"),
    ]
    for attr_name, asset_type in selectors:
        selector_cls = getattr(financedatabase, attr_name.capitalize(), None) or getattr(
            financedatabase, attr_name.upper(), None
        )
        if selector_cls is None:
            continue
        try:
            selector = selector_cls()
        except Exception:
            continue
        frame = None
        raw_data = getattr(selector, "data", None)
        if isinstance(raw_data, pd.DataFrame) and not raw_data.empty:
            working = raw_data.copy()
            if working.index.name and str(working.index.name).strip().lower() in {"symbol", "ticker"}:
                working = working.reset_index()
            elif "symbol" not in working.columns:
                working = working.reset_index().rename(columns={"index": "symbol"})
            if "symbol" in working.columns:
                normalized_symbols = {str(symbol).strip().upper() for symbol in symbols}
                frame = working[working["symbol"].astype(str).str.upper().isin(normalized_symbols)].copy()
        if frame is None:
            continue
        if isinstance(frame, pd.Series):
            frame = frame.to_frame().T
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            continue
        materialized = frame.copy()
        if "asset_type" not in materialized.columns:
            materialized["asset_type"] = asset_type
        if "is_etf" not in materialized.columns:
            materialized["is_etf"] = asset_type == "etfs"
        frames.append(materialized)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=False).reset_index(drop=False)


def build_security_master_from_financedatabase(
    *,
    symbols: list[str],
    as_of_date: str | date | None = None,
    package_override=None,
) -> SecurityMasterBuildResult:
    normalized_as_of = str(as_of_date or _utc_today())
    raw = _fetch_financedatabase_frames(symbols, package_override=package_override)
    requested_symbols = list(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()))
    duplicate_symbol_count = (
        int(raw["symbol"].astype(str).str.upper().duplicated().sum())
        if not raw.empty and "symbol" in raw.columns
        else 0
    )
    frame = _normalize_financedatabase_frame(
        raw,
        source="financedatabase",
        as_of_date=normalized_as_of,
    )
    matched_symbols = sorted(frame["symbol"].astype(str).str.upper().unique().tolist()) if not frame.empty else []
    unmatched_symbols = sorted(set(requested_symbols) - set(matched_symbols))
    return SecurityMasterBuildResult(
        frame=frame,
        source="financedatabase",
        as_of_date=normalized_as_of,
        requested_symbols=requested_symbols,
        matched_symbols=matched_symbols,
        unmatched_symbols=unmatched_symbols,
        duplicate_symbol_count=duplicate_symbol_count,
    )


def classification_group_map(frame: pd.DataFrame, *, level: str = "sector") -> dict[str, str]:
    if level not in {"sector", "industry_group", "industry", "category", "country", "exchange"}:
        raise ValueError("level must be one of: sector, industry_group, industry, category, country, exchange")
    if frame.empty:
        return {}
    working = frame.copy()
    if "symbol" not in working.columns:
        raise ValueError("classification frame must include a symbol column")
    return {
        str(row["symbol"]): str(row[level])
        for row in working[["symbol", level]].dropna().to_dict(orient="records")
        if str(row["symbol"]).strip()
    }

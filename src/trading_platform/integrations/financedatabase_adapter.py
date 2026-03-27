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


@dataclass(frozen=True)
class SecurityMasterBuildResult:
    frame: pd.DataFrame
    source: str
    as_of_date: str


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

    rename_map = {
        "ticker": "symbol",
        "type": "asset_type",
        "summary": "name",
        "company_name": "name",
        "group": "industry_group",
        "industry_group_name": "industry_group",
        "market": "exchange",
        "market_exchange": "exchange",
        "locale": "country",
        "currency_code": "currency",
        "quote_type": "asset_type",
    }
    normalized = normalized.rename(columns=rename_map)

    result = pd.DataFrame(
        {
            "symbol": normalized["symbol"].map(_safe_str),
            "asset_type": normalized.get("asset_type", pd.Series(index=normalized.index)).map(_safe_str),
            "name": normalized.get("name", pd.Series(index=normalized.index)).map(_safe_str),
            "sector": normalized.get("sector", pd.Series(index=normalized.index)).map(_safe_str),
            "industry_group": normalized.get("industry_group", pd.Series(index=normalized.index)).map(_safe_str),
            "industry": normalized.get("industry", pd.Series(index=normalized.index)).map(_safe_str),
            "category": normalized.get("category", pd.Series(index=normalized.index)).map(_safe_str),
            "exchange": normalized.get("exchange", pd.Series(index=normalized.index)).map(_safe_str),
            "country": normalized.get("country", pd.Series(index=normalized.index)).map(_safe_str),
            "currency": normalized.get("currency", pd.Series(index=normalized.index)).map(_safe_str),
            "is_etf": normalized.get("is_etf", normalized.get("etf", pd.Series(index=normalized.index))).map(
                _safe_bool
            ),
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
        for method_name in ("select", "search"):
            method = getattr(selector, method_name, None)
            if method is None:
                continue
            try:
                frame = method(symbols=symbols)
                break
            except TypeError:
                try:
                    frame = method(symbols)
                    break
                except Exception:
                    continue
            except Exception:
                continue
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
    frame = _normalize_financedatabase_frame(
        raw,
        source="financedatabase",
        as_of_date=normalized_as_of,
    )
    return SecurityMasterBuildResult(
        frame=frame,
        source="financedatabase",
        as_of_date=normalized_as_of,
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

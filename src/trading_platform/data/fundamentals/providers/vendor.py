from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

from trading_platform.data.fundamentals.models import (
    CANONICAL_FUNDAMENTAL_METRICS,
    CompanyMasterRecord,
    FilingMetadataRecord,
    FUNDAMENTAL_FILING_COLUMNS,
    FUNDAMENTAL_VALUE_COLUMNS,
    FundamentalValueRecord,
)
from trading_platform.data.fundamentals.providers.base import (
    FundamentalsProvider,
    ProviderFetchResult,
)


FMP_BASE_URL = "https://financialmodelingprep.com/stable"
FMP_SOURCE = "vendor:fmp"
FMP_STATEMENT_LIMIT = 16
FMP_TIMEOUT_SECONDS = 30
DEFAULT_FMP_CACHE_TTL_HOURS = 24.0
DEFAULT_FMP_REQUEST_DELAY_SECONDS = 0.5
DEFAULT_FMP_MAX_RETRIES = 4
DEFAULT_FMP_MAX_BACKOFF_SECONDS = 30.0

FMP_STATEMENT_FIELD_MAP: dict[str, tuple[str, ...]] = {
    "revenue": ("revenue",),
    "gross_profit": ("grossProfit",),
    "operating_income": ("operatingIncome",),
    "net_income": ("netIncome",),
    "total_assets": ("totalAssets",),
    "total_liabilities": ("totalLiabilities",),
    "shareholders_equity": ("totalStockholdersEquity", "totalEquity"),
    "cash_and_equivalents": ("cashAndCashEquivalents", "cashAndCashEquivalentsAtCarryingValue"),
    "current_assets": ("totalCurrentAssets",),
    "current_liabilities": ("totalCurrentLiabilities",),
    "long_term_debt": ("longTermDebt",),
    "operating_cash_flow": ("operatingCashFlow", "netCashProvidedByOperatingActivities"),
    "capital_expenditures": ("capitalExpenditure", "capitalExpenditureReported"),
    "free_cash_flow": ("freeCashFlow",),
    "shares_outstanding": (
        "weightedAverageShsOut",
        "weightedAverageShsOutDil",
        "commonStockSharesOutstanding",
        "shareOutstanding",
    ),
}


def _now_utc_iso() -> str:
    return datetime.now(UTC).isoformat()


def _empty_result(provider_name: str, **diagnostics: Any) -> ProviderFetchResult:
    return ProviderFetchResult(
        company_master_df=pd.DataFrame(),
        filing_metadata_df=pd.DataFrame(),
        fundamental_values_df=pd.DataFrame(),
        diagnostics={"provider": provider_name, **diagnostics},
    )


def _empty_filing_frame(provider_name: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            FilingMetadataRecord(
                symbol="",
                cik=None,
                fiscal_year=None,
                fiscal_period=None,
                period_type=None,
                period_end_date=None,
                filing_date=None,
                available_date=None,
                form_type=None,
                accession_number=None,
                source=provider_name,
            ).to_dict()
        ]
    ).iloc[0:0].copy()


def _empty_value_frame(provider_name: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            FundamentalValueRecord(
                symbol="",
                cik=None,
                fiscal_year=None,
                fiscal_period=None,
                period_type=None,
                period_end_date=None,
                filing_date=None,
                available_date=None,
                form_type=None,
                accession_number=None,
                source=provider_name,
                metric_name="",
            ).to_dict()
        ]
    ).iloc[0:0].copy()


def _load_table(path: Path) -> dict[str, pd.DataFrame]:
    if not path.exists():
        return {}
    if path.suffix.lower() == ".parquet":
        return {"fundamental_values": pd.read_parquet(path)}
    if path.suffix.lower() == ".csv":
        return {"fundamental_values": pd.read_csv(path)}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        result: dict[str, pd.DataFrame] = {}
        for key, rows in payload.items():
            if isinstance(rows, list):
                result[key] = pd.DataFrame(rows)
        if result:
            return result
    if isinstance(payload, list):
        return {"fundamental_values": pd.DataFrame(payload)}
    return {}


def _normalize_symbol_list(symbols: list[str]) -> list[str]:
    return [str(symbol).upper() for symbol in symbols]


def _coerce_float(value: Any) -> float | None:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return None if pd.isna(numeric) else float(numeric)


def _coerce_date(value: Any) -> pd.Timestamp | None:
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return None
    return pd.Timestamp(timestamp).normalize()


def _format_date(value: pd.Timestamp | None) -> str | None:
    return value.strftime("%Y-%m-%d") if value is not None else None


def _first_non_empty(mapping: dict[str, Any], field_names: tuple[str, ...]) -> Any:
    for field_name in field_names:
        if field_name in mapping and mapping[field_name] not in (None, "", []):
            return mapping[field_name]
    return None


def _extract_accession_number(statement: dict[str, Any], *, symbol: str, period_end_date: str | None, fiscal_period: str | None) -> str:
    for field_name in ("accessionNumber", "finalLink", "link"):
        field_value = statement.get(field_name)
        if isinstance(field_value, str) and field_value.strip():
            return field_value.strip()
    return f"fmp:{symbol}:{period_end_date or 'unknown'}:{fiscal_period or 'unknown'}"


def _conservative_available_date(
    *,
    statement: dict[str, Any],
    period_end_date: pd.Timestamp | None,
    period_type: str | None,
) -> tuple[pd.Timestamp | None, str]:
    accepted_date = _coerce_date(_first_non_empty(statement, ("acceptedDate",)))
    if accepted_date is not None:
        return accepted_date, "accepted_date"
    filing_date = _coerce_date(_first_non_empty(statement, ("fillingDate", "filingDate")))
    if filing_date is not None:
        return filing_date, "filing_date"
    if period_end_date is None:
        return None, "unavailable"
    lag_days = 90 if period_type == "annual" else 45
    return period_end_date + timedelta(days=lag_days), f"period_end_plus_{lag_days}d"


@dataclass(frozen=True)
class _NormalizedFMPStatement:
    filing: dict[str, Any]
    values: list[dict[str, Any]]
    available_date_method: str


class FMPClient:
    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = FMP_BASE_URL,
        timeout_seconds: int = FMP_TIMEOUT_SECONDS,
        cache_enabled: bool = True,
        cache_root: Path | None = None,
        cache_ttl_hours: float = DEFAULT_FMP_CACHE_TTL_HOURS,
        force_refresh: bool = False,
        request_delay_seconds: float = DEFAULT_FMP_REQUEST_DELAY_SECONDS,
        max_retries: int = DEFAULT_FMP_MAX_RETRIES,
        max_backoff_seconds: float = DEFAULT_FMP_MAX_BACKOFF_SECONDS,
        max_requests_per_run: int | None = None,
        sleep_fn: Any = None,
        monotonic_fn: Any = None,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.cache_enabled = cache_enabled
        self.cache_root = Path(cache_root) if cache_root is not None else None
        self.cache_ttl_hours = float(cache_ttl_hours)
        self.force_refresh = force_refresh
        self.request_delay_seconds = max(float(request_delay_seconds), 0.0)
        self.max_retries = max(int(max_retries), 0)
        self.max_backoff_seconds = max(float(max_backoff_seconds), 0.0)
        self.max_requests_per_run = max_requests_per_run
        self.sleep_fn = sleep_fn or time.sleep
        self.monotonic_fn = monotonic_fn or time.monotonic
        self._last_request_started_at: float | None = None
        self.cache_hits = 0
        self.cache_misses = 0
        self.retry_count = 0
        self.rate_limit_error_count = 0
        self.requests_made = 0

    def _cache_file_path(self, *segments: str) -> Path | None:
        if not self.cache_enabled or self.cache_root is None:
            return None
        cleaned_segments = [segment.strip("/\\") for segment in segments if str(segment).strip()]
        if not cleaned_segments:
            return None
        return self.cache_root.joinpath(*cleaned_segments).with_suffix(".json")

    def _is_cache_fresh(self, cache_path: Path | None) -> bool:
        if cache_path is None or not cache_path.exists() or self.force_refresh:
            return False
        ttl_seconds = max(self.cache_ttl_hours, 0.0) * 3600.0
        if ttl_seconds == 0.0:
            return False
        age_seconds = max(time.time() - cache_path.stat().st_mtime, 0.0)
        return age_seconds <= ttl_seconds

    def _read_cached_payload(self, cache_path: Path) -> list[dict[str, Any]]:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict) and isinstance(payload.get("payload"), list):
            return [row for row in payload["payload"] if isinstance(row, dict)]
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        return []

    def _write_cached_payload(
        self,
        *,
        cache_path: Path | None,
        endpoint: str,
        symbol: str,
        params: dict[str, Any],
        payload: list[dict[str, Any]],
    ) -> None:
        if cache_path is None:
            return
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(
            json.dumps(
                {
                    "cached_at": _now_utc_iso(),
                    "endpoint": endpoint,
                    "symbol": symbol,
                    "params": params,
                    "payload": payload,
                },
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )

    def _throttle(self) -> None:
        if self.request_delay_seconds <= 0.0:
            return
        now = self.monotonic_fn()
        if self._last_request_started_at is None:
            self._last_request_started_at = now
            return
        elapsed = now - self._last_request_started_at
        remaining = self.request_delay_seconds - elapsed
        if remaining > 0.0:
            self.sleep_fn(remaining)
        self._last_request_started_at = self.monotonic_fn()

    def _retry_delay_seconds(self, attempt_number: int) -> float:
        backoff = min((2 ** max(attempt_number - 1, 0)), self.max_backoff_seconds)
        deterministic_jitter = min(0.137 * attempt_number, 0.5)
        return min(backoff + deterministic_jitter, self.max_backoff_seconds)

    def _is_retryable_exception(self, exc: Exception) -> bool:
        if isinstance(exc, HTTPError):
            return exc.code in {429, 500, 502, 503, 504}
        return isinstance(exc, (TimeoutError, URLError))

    def _request_json(
        self,
        endpoint: str,
        params: dict[str, Any],
        *,
        symbol: str,
        cache_segments: tuple[str, ...],
    ) -> list[dict[str, Any]]:
        cache_path = self._cache_file_path(*cache_segments, symbol.upper())
        if self._is_cache_fresh(cache_path):
            self.cache_hits += 1
            return self._read_cached_payload(cache_path)
        self.cache_misses += 1
        if self.max_requests_per_run is not None and self.requests_made >= self.max_requests_per_run:
            raise RuntimeError(f"FMP request budget exhausted before fetching {endpoint} for {symbol}.")
        query_params = {
            key: value
            for key, value in {**params, "apikey": self.api_key}.items()
            if value is not None
        }
        url = f"{self.base_url}/{endpoint.lstrip('/')}?{urlencode(query_params)}"
        request = Request(url, headers={"User-Agent": "trading-platform/1.0"})
        last_error: Exception | None = None
        for attempt_number in range(1, self.max_retries + 2):
            try:
                self._throttle()
                self.requests_made += 1
                with urlopen(request, timeout=self.timeout_seconds) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                if isinstance(payload, list):
                    rows = [row for row in payload if isinstance(row, dict)]
                elif isinstance(payload, dict):
                    if isinstance(payload.get("data"), list):
                        rows = [row for row in payload["data"] if isinstance(row, dict)]
                    else:
                        rows = [payload]
                else:
                    rows = []
                self._write_cached_payload(
                    cache_path=cache_path,
                    endpoint=endpoint,
                    symbol=symbol,
                    params=params,
                    payload=rows,
                )
                return rows
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if isinstance(exc, HTTPError) and exc.code == 429:
                    self.rate_limit_error_count += 1
                if attempt_number > self.max_retries or not self._is_retryable_exception(exc):
                    break
                self.retry_count += 1
                self.sleep_fn(self._retry_delay_seconds(attempt_number))
        raise RuntimeError(f"FMP request failed for {symbol} endpoint={endpoint}: {last_error}") from last_error

    def diagnostics_summary(self) -> dict[str, Any]:
        return {
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "retry_count": self.retry_count,
            "rate_limit_error_count": self.rate_limit_error_count,
            "requests_made": self.requests_made,
        }

    def fetch_company_profile(self, symbol: str) -> dict[str, Any] | None:
        rows = self._request_json(
            "profile",
            {"symbol": symbol},
            symbol=symbol,
            cache_segments=("profile",),
        )
        return rows[0] if rows else None

    def fetch_statements(self, symbol: str, *, period: str) -> dict[str, list[dict[str, Any]]]:
        params = {"symbol": symbol, "period": period, "limit": FMP_STATEMENT_LIMIT}
        return {
            "income": self._request_json(
                "income-statement",
                params,
                symbol=symbol,
                cache_segments=("income-statement", period),
            ),
            "balance": self._request_json(
                "balance-sheet-statement",
                params,
                symbol=symbol,
                cache_segments=("balance-sheet-statement", period),
            ),
            "cash": self._request_json(
                "cash-flow-statement",
                params,
                symbol=symbol,
                cache_segments=("cash-flow-statement", period),
            ),
        }


class VendorFundamentalsProvider(FundamentalsProvider):
    provider_name = "vendor"

    def __init__(
        self,
        *,
        file_path: str | Path | None = None,
        api_key: str | None = None,
        cache_enabled: bool = True,
        cache_root: str | Path | None = None,
        cache_ttl_hours: float = DEFAULT_FMP_CACHE_TTL_HOURS,
        force_refresh: bool = False,
        request_delay_seconds: float = DEFAULT_FMP_REQUEST_DELAY_SECONDS,
        max_retries: int = DEFAULT_FMP_MAX_RETRIES,
        max_symbols_per_run: int | None = None,
        max_requests_per_run: int | None = None,
    ) -> None:
        self.file_path = Path(file_path) if file_path else None
        self.api_key = api_key or os.getenv("FMP_API_KEY")
        self.cache_enabled = cache_enabled
        self.cache_root = Path(cache_root) if cache_root else None
        self.cache_ttl_hours = cache_ttl_hours
        self.force_refresh = force_refresh
        self.request_delay_seconds = request_delay_seconds
        self.max_retries = max_retries
        self.max_symbols_per_run = max_symbols_per_run
        self.max_requests_per_run = max_requests_per_run

    def is_configured(self) -> bool:
        return bool((self.file_path and self.file_path.exists()) or self.api_key)

    def _fetch_from_file(self, *, symbols: list[str]) -> ProviderFetchResult:
        if self.file_path is None or not self.file_path.exists():
            return _empty_result(
                self.provider_name,
                configured=False,
                status="not_configured",
                message="Vendor fundamentals provider requires a vendor file path or FMP API key.",
            )

        tables = _load_table(self.file_path)
        values_df = tables.get("fundamental_values", pd.DataFrame()).copy()
        if values_df.empty:
            return _empty_result(self.provider_name, configured=True, status="empty_file", file_path=str(self.file_path))
        normalized_symbols = _normalize_symbol_list(symbols)
        values_df["symbol"] = values_df["symbol"].astype(str).str.upper()
        values_df = values_df.loc[values_df["symbol"].isin(normalized_symbols)].copy()
        if "source" not in values_df.columns:
            values_df["source"] = self.provider_name
        if "metric_name" not in values_df.columns or "metric_value" not in values_df.columns:
            metadata_columns = [column for column in FUNDAMENTAL_FILING_COLUMNS if column in values_df.columns]
            value_columns = [column for column in CANONICAL_FUNDAMENTAL_METRICS if column in values_df.columns]
            values_df = values_df.melt(
                id_vars=metadata_columns,
                value_vars=value_columns,
                var_name="metric_name",
                value_name="metric_value",
            )
            values_df = values_df.dropna(subset=["metric_value"]).reset_index(drop=True)
        if "free_cash_flow" not in set(values_df["metric_name"].astype(str)):
            operating_cash_flow = values_df.loc[values_df["metric_name"] == "operating_cash_flow"].rename(columns={"metric_value": "operating_cash_flow"})
            capital_expenditures = values_df.loc[values_df["metric_name"] == "capital_expenditures"].rename(columns={"metric_value": "capital_expenditures"})
            derived = operating_cash_flow.merge(
                capital_expenditures[
                    ["symbol", "accession_number", "period_end_date", "fiscal_year", "fiscal_period", "capital_expenditures"]
                ],
                on=["symbol", "accession_number", "period_end_date", "fiscal_year", "fiscal_period"],
                how="inner",
            )
            if not derived.empty:
                derived["metric_name"] = "free_cash_flow"
                derived["metric_value"] = (
                    pd.to_numeric(derived["operating_cash_flow"], errors="coerce")
                    - pd.to_numeric(derived["capital_expenditures"], errors="coerce")
                )
                values_df = pd.concat(
                    [
                        values_df,
                        derived[list(FUNDAMENTAL_VALUE_COLUMNS)],
                    ],
                    ignore_index=True,
                )
        values_df = values_df[[column for column in FUNDAMENTAL_VALUE_COLUMNS if column in values_df.columns]].copy()
        filing_df = values_df[list(FUNDAMENTAL_FILING_COLUMNS)].drop_duplicates().reset_index(drop=True)
        company_df = tables.get("company_master", pd.DataFrame()).copy()
        if not company_df.empty:
            company_df["symbol"] = company_df["symbol"].astype(str).str.upper()
            company_df = company_df.loc[company_df["symbol"].isin(normalized_symbols)].copy()
            if "source" not in company_df.columns:
                company_df["source"] = self.provider_name
        return ProviderFetchResult(
            company_master_df=company_df,
            filing_metadata_df=filing_df,
            fundamental_values_df=values_df.reset_index(drop=True),
            diagnostics={
                "provider": self.provider_name,
                "configured": True,
                "status": "ok",
                "mode": "file",
                "file_path": str(self.file_path),
                "cache_hits": 0,
                "cache_misses": 0,
                "symbols_fetched": sorted(set(normalized_symbols)),
                "symbols_skipped_from_cache": [],
                "retry_count": 0,
                "rate_limit_error_count": 0,
                "symbols_failed": [],
            },
        )

    def _normalize_company_master(self, *, symbol: str, profile: dict[str, Any] | None) -> dict[str, Any]:
        profile = profile or {}
        return CompanyMasterRecord(
            symbol=symbol.upper(),
            cik=str(profile.get("cik") or "").zfill(10) or None,
            company_name=profile.get("companyName") or profile.get("companyNameLong") or profile.get("name"),
            exchange=profile.get("exchangeShortName") or profile.get("exchange"),
            sector=profile.get("sector"),
            industry=profile.get("industry"),
            is_active=bool(profile.get("isActivelyTrading", True)),
            source=FMP_SOURCE,
        ).to_dict()

    def _normalize_statement_row(
        self,
        *,
        symbol: str,
        cik: str | None,
        period_hint: str,
        income_statement: dict[str, Any] | None,
        balance_statement: dict[str, Any] | None,
        cash_statement: dict[str, Any] | None,
    ) -> _NormalizedFMPStatement | None:
        merged_statement: dict[str, Any] = {}
        for payload in (income_statement, balance_statement, cash_statement):
            if payload:
                merged_statement.update(payload)
        if not merged_statement:
            return None

        period_end_date = _coerce_date(_first_non_empty(merged_statement, ("date", "calendarDate")))
        fiscal_period = str(_first_non_empty(merged_statement, ("period",)) or ("FY" if period_hint == "annual" else "Q")).upper()
        inferred_period_type = "annual" if fiscal_period == "FY" or period_hint == "annual" else "quarterly"
        fiscal_year_raw = _first_non_empty(merged_statement, ("calendarYear", "fiscalYear"))
        fiscal_year = int(str(fiscal_year_raw)) if str(fiscal_year_raw).strip().isdigit() else (period_end_date.year if period_end_date is not None else None)
        filing_date = _coerce_date(_first_non_empty(merged_statement, ("fillingDate", "filingDate")))
        available_date, available_date_method = _conservative_available_date(
            statement=merged_statement,
            period_end_date=period_end_date,
            period_type=inferred_period_type,
        )
        accession_number = _extract_accession_number(
            merged_statement,
            symbol=symbol.upper(),
            period_end_date=_format_date(period_end_date),
            fiscal_period=fiscal_period,
        )
        form_type = str(_first_non_empty(merged_statement, ("formType",)) or ("10-K" if inferred_period_type == "annual" else "10-Q"))

        filing_row: dict[str, Any] = {
            "symbol": symbol.upper(),
            "cik": cik,
            "fiscal_year": fiscal_year,
            "fiscal_period": fiscal_period,
            "period_type": inferred_period_type,
            "period_end_date": _format_date(period_end_date),
            "filing_date": _format_date(filing_date),
            "available_date": _format_date(available_date),
            "form_type": form_type,
            "accession_number": accession_number,
            "source": FMP_SOURCE,
        }
        value_rows: list[dict[str, Any]] = []
        for metric_name, field_names in FMP_STATEMENT_FIELD_MAP.items():
            metric_value = _coerce_float(_first_non_empty(merged_statement, field_names))
            if metric_value is None:
                continue
            value_rows.append(
                FundamentalValueRecord(
                    **filing_row,
                    metric_name=metric_name,
                    metric_value=metric_value,
                ).to_dict()
            )
        if not any(row["metric_name"] == "free_cash_flow" for row in value_rows):
            operating_cash_flow = next((row["metric_value"] for row in value_rows if row["metric_name"] == "operating_cash_flow"), None)
            capital_expenditures = next((row["metric_value"] for row in value_rows if row["metric_name"] == "capital_expenditures"), None)
            if operating_cash_flow is not None and capital_expenditures is not None:
                value_rows.append(
                    FundamentalValueRecord(
                        **filing_row,
                        metric_name="free_cash_flow",
                        metric_value=operating_cash_flow - capital_expenditures,
                    ).to_dict()
                )
        return _NormalizedFMPStatement(
            filing=filing_row,
            values=value_rows,
            available_date_method=available_date_method,
        )

    def _merge_statement_payloads(
        self,
        *,
        symbol: str,
        cik: str | None,
        statements_by_period: dict[str, dict[str, list[dict[str, Any]]]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
        filing_rows: list[dict[str, Any]] = []
        value_rows: list[dict[str, Any]] = []
        method_counts: dict[str, int] = {}

        for period_name, statement_group in statements_by_period.items():
            keyed_rows: dict[tuple[str | None, str | None], dict[str, dict[str, Any] | None]] = {}
            for statement_type, rows in statement_group.items():
                for row in rows:
                    period_end_date = _format_date(_coerce_date(_first_non_empty(row, ("date", "calendarDate"))))
                    fiscal_period = str(_first_non_empty(row, ("period",)) or ("FY" if period_name == "annual" else "Q")).upper()
                    keyed_rows.setdefault((period_end_date, fiscal_period), {"income": None, "balance": None, "cash": None})
                    keyed_rows[(period_end_date, fiscal_period)][statement_type] = row

            for grouped_statement in keyed_rows.values():
                normalized = self._normalize_statement_row(
                    symbol=symbol,
                    cik=cik,
                    period_hint=period_name,
                    income_statement=grouped_statement["income"],
                    balance_statement=grouped_statement["balance"],
                    cash_statement=grouped_statement["cash"],
                )
                if normalized is None:
                    continue
                filing_rows.append(normalized.filing)
                value_rows.extend(normalized.values)
                method_counts[normalized.available_date_method] = method_counts.get(normalized.available_date_method, 0) + 1

        return filing_rows, value_rows, method_counts

    def _fetch_from_fmp(self, *, symbols: list[str]) -> ProviderFetchResult:
        if not self.api_key:
            return _empty_result(
                self.provider_name,
                configured=False,
                status="api_key_missing",
                mode="fmp",
                message="Vendor fundamentals provider requires --vendor-api-key, --fundamentals-vendor-api-key, or FMP_API_KEY when no vendor file path is provided.",
            )

        cache_root = self.cache_root
        client = FMPClient(
            api_key=self.api_key,
            cache_enabled=self.cache_enabled,
            cache_root=cache_root,
            cache_ttl_hours=self.cache_ttl_hours,
            force_refresh=self.force_refresh,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            max_requests_per_run=self.max_requests_per_run,
        )
        company_rows: list[dict[str, Any]] = []
        filing_rows: list[dict[str, Any]] = []
        value_rows: list[dict[str, Any]] = []
        missing_symbols: list[str] = []
        symbol_errors: list[dict[str, str]] = []
        symbols_fetched: list[str] = []
        symbols_skipped_from_cache: list[str] = []
        skipped_due_limit: list[str] = []
        available_date_method_counts: dict[str, int] = {}

        normalized_symbols = _normalize_symbol_list(symbols)
        if self.max_symbols_per_run is not None:
            fetch_symbols = normalized_symbols[: self.max_symbols_per_run]
            skipped_due_limit = normalized_symbols[self.max_symbols_per_run :]
        else:
            fetch_symbols = normalized_symbols

        for symbol in fetch_symbols:
            requests_before = client.requests_made
            try:
                profile = client.fetch_company_profile(symbol)
                statements_by_period = {
                    "annual": client.fetch_statements(symbol, period="annual"),
                    "quarter": client.fetch_statements(symbol, period="quarter"),
                }
            except Exception as exc:
                symbol_errors.append(
                    {
                        "symbol": symbol,
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                    }
                )
                continue
            if client.requests_made == requests_before:
                symbols_skipped_from_cache.append(symbol)
            else:
                symbols_fetched.append(symbol)

            has_statement_coverage = any(
                bool(rows)
                for statement_group in statements_by_period.values()
                for rows in statement_group.values()
            )
            if not profile and not has_statement_coverage:
                missing_symbols.append(symbol)
                continue

            company_rows.append(self._normalize_company_master(symbol=symbol, profile=profile))
            filing_for_symbol, values_for_symbol, method_counts = self._merge_statement_payloads(
                symbol=symbol,
                cik=str((profile or {}).get("cik") or "").zfill(10) or None,
                statements_by_period=statements_by_period,
            )
            filing_rows.extend(filing_for_symbol)
            value_rows.extend(values_for_symbol)
            for method, count in method_counts.items():
                available_date_method_counts[method] = available_date_method_counts.get(method, 0) + count
            if not values_for_symbol:
                missing_symbols.append(symbol)

        company_df = pd.DataFrame(company_rows)
        filing_df = pd.DataFrame(filing_rows) if filing_rows else _empty_filing_frame(FMP_SOURCE)
        values_df = pd.DataFrame(value_rows) if value_rows else _empty_value_frame(FMP_SOURCE)
        if not values_df.empty:
            values_df = values_df.sort_values(
                ["symbol", "available_date", "period_end_date", "fiscal_period", "metric_name", "source"],
                na_position="last",
            ).drop_duplicates(
                subset=["symbol", "period_end_date", "fiscal_period", "fiscal_year", "accession_number", "metric_name"],
                keep="last",
            ).reset_index(drop=True)
            filing_df = values_df[list(FUNDAMENTAL_FILING_COLUMNS)].drop_duplicates().reset_index(drop=True)

        status = "ok"
        if symbol_errors and not value_rows:
            status = "error"
        elif symbol_errors:
            status = "partial_success"

        return ProviderFetchResult(
            company_master_df=company_df,
            filing_metadata_df=filing_df,
            fundamental_values_df=values_df,
            diagnostics={
                "provider": self.provider_name,
                "configured": True,
                "status": status,
                "mode": "fmp",
                "symbols_requested": len(symbols),
                "symbols_attempted": len(fetch_symbols),
                "company_count": len(company_rows),
                "filing_count": int(len(filing_df)),
                "value_count": int(len(values_df)),
                "missing_symbols": sorted(set(missing_symbols)),
                "symbols_fetched": sorted(set(symbols_fetched)),
                "symbols_skipped_from_cache": sorted(set(symbols_skipped_from_cache)),
                "symbols_failed": sorted({row["symbol"] for row in symbol_errors}),
                "skipped_due_limit": skipped_due_limit,
                "symbol_errors": symbol_errors,
                "available_date_method_counts": available_date_method_counts,
                "cache_enabled": self.cache_enabled,
                "cache_root": str(cache_root) if cache_root is not None else None,
                "cache_ttl_hours": self.cache_ttl_hours,
                "force_refresh": self.force_refresh,
                "request_delay_seconds": self.request_delay_seconds,
                "max_retries": self.max_retries,
                "max_symbols_per_run": self.max_symbols_per_run,
                "max_requests_per_run": self.max_requests_per_run,
                "message": (
                    f"FMP fetch failed for {len(symbol_errors)} symbol(s) after retries were exhausted."
                    if status == "error"
                    else None
                ),
                **client.diagnostics_summary(),
            },
        )

    def fetch(self, *, symbols: list[str]) -> ProviderFetchResult:
        if self.file_path and self.file_path.exists():
            return self._fetch_from_file(symbols=symbols)
        return self._fetch_from_fmp(symbols=symbols)

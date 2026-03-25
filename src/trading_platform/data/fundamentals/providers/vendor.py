from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

from trading_platform.data.fundamentals.models import (
    CANONICAL_FUNDAMENTAL_METRICS,
    CompanyMasterRecord,
    FilingMetadataRecord,
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
    values: dict[str, Any]
    available_date_method: str


class FMPClient:
    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = FMP_BASE_URL,
        timeout_seconds: int = FMP_TIMEOUT_SECONDS,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def _request_json(self, endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        query_params = {
            key: value
            for key, value in {**params, "apikey": self.api_key}.items()
            if value is not None
        }
        url = f"{self.base_url}/{endpoint.lstrip('/')}?{urlencode(query_params)}"
        request = Request(url, headers={"User-Agent": "trading-platform/1.0"})
        with urlopen(request, timeout=self.timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            if isinstance(payload.get("data"), list):
                return [row for row in payload["data"] if isinstance(row, dict)]
            return [payload]
        return []

    def fetch_company_profile(self, symbol: str) -> dict[str, Any] | None:
        rows = self._request_json("profile", {"symbol": symbol})
        return rows[0] if rows else None

    def fetch_statements(self, symbol: str, *, period: str) -> dict[str, list[dict[str, Any]]]:
        params = {"symbol": symbol, "period": period, "limit": FMP_STATEMENT_LIMIT}
        return {
            "income": self._request_json("income-statement", params),
            "balance": self._request_json("balance-sheet-statement", params),
            "cash": self._request_json("cash-flow-statement", params),
        }


class VendorFundamentalsProvider(FundamentalsProvider):
    provider_name = "vendor"

    def __init__(
        self,
        *,
        file_path: str | Path | None = None,
        api_key: str | None = None,
    ) -> None:
        self.file_path = Path(file_path) if file_path else None
        self.api_key = api_key or os.getenv("FMP_API_KEY")

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
        for metric in CANONICAL_FUNDAMENTAL_METRICS:
            if metric not in values_df.columns:
                values_df[metric] = pd.Series(dtype="float64")
        if "free_cash_flow" not in values_df.columns or values_df["free_cash_flow"].isna().all():
            values_df["free_cash_flow"] = (
                pd.to_numeric(values_df.get("operating_cash_flow"), errors="coerce")
                - pd.to_numeric(values_df.get("capital_expenditures"), errors="coerce")
            )
        filing_df = values_df[
            [
                "symbol",
                "cik",
                "fiscal_year",
                "fiscal_period",
                "period_type",
                "period_end_date",
                "filing_date",
                "available_date",
                "form_type",
                "accession_number",
                "source",
            ]
        ].drop_duplicates().reset_index(drop=True)
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

        row: dict[str, Any] = {
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
        for metric_name, field_names in FMP_STATEMENT_FIELD_MAP.items():
            row[metric_name] = _coerce_float(_first_non_empty(merged_statement, field_names))
        if row.get("free_cash_flow") is None:
            operating_cash_flow = row.get("operating_cash_flow")
            capital_expenditures = row.get("capital_expenditures")
            if operating_cash_flow is not None and capital_expenditures is not None:
                row["free_cash_flow"] = operating_cash_flow - capital_expenditures

        filing_row = {
            key: row[key]
            for key in (
                "symbol",
                "cik",
                "fiscal_year",
                "fiscal_period",
                "period_type",
                "period_end_date",
                "filing_date",
                "available_date",
                "form_type",
                "accession_number",
                "source",
            )
        }
        return _NormalizedFMPStatement(
            filing=filing_row,
            values=row,
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
                value_rows.append(normalized.values)
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

        client = FMPClient(api_key=self.api_key)
        company_rows: list[dict[str, Any]] = []
        filing_rows: list[dict[str, Any]] = []
        value_rows: list[dict[str, Any]] = []
        missing_symbols: list[str] = []
        symbol_errors: list[dict[str, str]] = []
        available_date_method_counts: dict[str, int] = {}

        for symbol in _normalize_symbol_list(symbols):
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
                ["symbol", "available_date", "period_end_date", "fiscal_period", "source"],
                na_position="last",
            ).drop_duplicates(
                subset=["symbol", "period_end_date", "fiscal_period", "fiscal_year", "accession_number"],
                keep="last",
            ).reset_index(drop=True)
            filing_df = values_df[
                [
                    "symbol",
                    "cik",
                    "fiscal_year",
                    "fiscal_period",
                    "period_type",
                    "period_end_date",
                    "filing_date",
                    "available_date",
                    "form_type",
                    "accession_number",
                    "source",
                ]
            ].drop_duplicates().reset_index(drop=True)

        return ProviderFetchResult(
            company_master_df=company_df,
            filing_metadata_df=filing_df,
            fundamental_values_df=values_df,
            diagnostics={
                "provider": self.provider_name,
                "configured": True,
                "status": "ok" if not symbol_errors else "partial_success",
                "mode": "fmp",
                "symbols_requested": len(symbols),
                "company_count": len(company_rows),
                "filing_count": int(len(filing_df)),
                "value_count": int(len(values_df)),
                "missing_symbols": sorted(set(missing_symbols)),
                "symbol_errors": symbol_errors,
                "available_date_method_counts": available_date_method_counts,
            },
        )

    def fetch(self, *, symbols: list[str]) -> ProviderFetchResult:
        if self.file_path and self.file_path.exists():
            return self._fetch_from_file(symbols=symbols)
        return self._fetch_from_fmp(symbols=symbols)

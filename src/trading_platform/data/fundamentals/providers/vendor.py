from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.data.fundamentals.models import CANONICAL_FUNDAMENTAL_METRICS
from trading_platform.data.fundamentals.providers.base import (
    FundamentalsProvider,
    ProviderFetchResult,
)


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


class VendorFundamentalsProvider(FundamentalsProvider):
    provider_name = "vendor"

    def __init__(
        self,
        *,
        file_path: str | Path | None = None,
        api_key: str | None = None,
    ) -> None:
        self.file_path = Path(file_path) if file_path else None
        self.api_key = api_key

    def is_configured(self) -> bool:
        return bool((self.file_path and self.file_path.exists()) or self.api_key)

    def fetch(self, *, symbols: list[str]) -> ProviderFetchResult:
        if not self.is_configured():
            return ProviderFetchResult(
                company_master_df=pd.DataFrame(),
                filing_metadata_df=pd.DataFrame(),
                fundamental_values_df=pd.DataFrame(),
                diagnostics={"provider": self.provider_name, "configured": False, "status": "not_configured"},
            )
        if self.file_path is None or not self.file_path.exists():
            return ProviderFetchResult(
                company_master_df=pd.DataFrame(),
                filing_metadata_df=pd.DataFrame(),
                fundamental_values_df=pd.DataFrame(),
                diagnostics={
                    "provider": self.provider_name,
                    "configured": True,
                    "status": "api_fetch_not_implemented",
                },
            )

        tables = _load_table(self.file_path)
        values_df = tables.get("fundamental_values", pd.DataFrame()).copy()
        if values_df.empty:
            return ProviderFetchResult(
                company_master_df=pd.DataFrame(),
                filing_metadata_df=pd.DataFrame(),
                fundamental_values_df=pd.DataFrame(),
                diagnostics={"provider": self.provider_name, "configured": True, "status": "empty_file"},
            )
        values_df["symbol"] = values_df["symbol"].astype(str).str.upper()
        values_df = values_df.loc[values_df["symbol"].isin([symbol.upper() for symbol in symbols])].copy()
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
            company_df = company_df.loc[company_df["symbol"].isin([symbol.upper() for symbol in symbols])].copy()
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
                "file_path": str(self.file_path),
            },
        )

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.data.fundamentals.models import (
    CANONICAL_FUNDAMENTAL_METRICS,
    CompanyMasterRecord,
    FilingMetadataRecord,
    FUNDAMENTAL_FILING_COLUMNS,
    FundamentalValueRecord,
)
from trading_platform.data.fundamentals.providers.base import (
    FundamentalsProvider,
    ProviderFetchResult,
)


SEC_CONCEPT_MAP: dict[str, tuple[str, ...]] = {
    "revenue": ("Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"),
    "gross_profit": ("GrossProfit",),
    "operating_income": ("OperatingIncomeLoss",),
    "net_income": ("NetIncomeLoss", "ProfitLoss"),
    "total_assets": ("Assets",),
    "total_liabilities": ("Liabilities",),
    "shareholders_equity": ("StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"),
    "cash_and_equivalents": ("CashAndCashEquivalentsAtCarryingValue",),
    "current_assets": ("AssetsCurrent",),
    "current_liabilities": ("LiabilitiesCurrent",),
    "long_term_debt": ("LongTermDebt", "LongTermDebtNoncurrent"),
    "operating_cash_flow": ("NetCashProvidedByUsedInOperatingActivities",),
    "capital_expenditures": ("PaymentsToAcquirePropertyPlantAndEquipment", "CapitalExpendituresIncurredButNotYetPaid"),
    "shares_outstanding": ("CommonStockSharesOutstanding", "EntityCommonStockSharesOutstanding"),
}


def _empty_result(provider_name: str, **diagnostics: Any) -> ProviderFetchResult:
    return ProviderFetchResult(
        company_master_df=pd.DataFrame(),
        filing_metadata_df=pd.DataFrame(),
        fundamental_values_df=pd.DataFrame(),
        diagnostics={"provider": provider_name, **diagnostics},
    )


def _load_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


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


class SECFundamentalsProvider(FundamentalsProvider):
    provider_name = "sec"

    def __init__(
        self,
        *,
        companyfacts_root: str | Path | None = None,
        submissions_root: str | Path | None = None,
    ) -> None:
        self.companyfacts_root = Path(companyfacts_root) if companyfacts_root else None
        self.submissions_root = Path(submissions_root) if submissions_root else None

    def is_configured(self) -> bool:
        return bool(
            (self.companyfacts_root and self.companyfacts_root.exists())
            or (self.submissions_root and self.submissions_root.exists())
        )

    def _resolve_path(self, root: Path | None, symbol: str, cik: str | None = None) -> Path | None:
        if root is None:
            return None
        candidates = [
            root / f"{symbol.upper()}.json",
            root / f"{str(cik or '').zfill(10)}.json",
        ]
        return next((candidate for candidate in candidates if candidate.exists()), None)

    def _normalize_company_master(self, *, symbol: str, submissions_payload: dict[str, Any]) -> dict[str, Any]:
        return CompanyMasterRecord(
            symbol=symbol.upper(),
            cik=str(submissions_payload.get("cik") or "").zfill(10) or None,
            company_name=submissions_payload.get("name"),
            exchange=(submissions_payload.get("exchanges") or [None])[0],
            sector=None,
            industry=submissions_payload.get("sicDescription"),
            is_active=True,
            source=self.provider_name,
        ).to_dict()

    def _iter_metric_entries(self, companyfacts_payload: dict[str, Any], metric_name: str) -> list[dict[str, Any]]:
        facts = companyfacts_payload.get("facts", {}).get("us-gaap", {})
        for concept in SEC_CONCEPT_MAP.get(metric_name, ()):
            concept_payload = facts.get(concept, {})
            for entries in concept_payload.get("units", {}).values():
                if isinstance(entries, list):
                    return entries
        return []

    def _normalize_values(
        self,
        *,
        symbol: str,
        cik: str | None,
        companyfacts_payload: dict[str, Any],
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        filing_rows: dict[tuple[str | None, int | None, str | None, str | None, str | None], dict[str, Any]] = {}
        value_rows: list[dict[str, Any]] = []
        for metric_name in CANONICAL_FUNDAMENTAL_METRICS:
            if metric_name == "free_cash_flow":
                continue
            for entry in self._iter_metric_entries(companyfacts_payload, metric_name):
                accession_number = entry.get("accn")
                fiscal_year = entry.get("fy")
                fiscal_period = entry.get("fp")
                period_end_date = entry.get("end")
                filing_date = entry.get("filed")
                form_type = entry.get("form")
                if not any((accession_number, filing_date, period_end_date)):
                    continue
                key = (accession_number, fiscal_year, fiscal_period, period_end_date, filing_date)
                filing_rows.setdefault(
                    key,
                    {
                        "symbol": symbol.upper(),
                        "cik": cik,
                        "fiscal_year": fiscal_year,
                        "fiscal_period": fiscal_period,
                        "period_type": "annual" if str(form_type).upper() == "10-K" else "quarterly",
                        "period_end_date": period_end_date,
                        "filing_date": filing_date,
                        "available_date": filing_date,
                        "form_type": form_type,
                        "accession_number": accession_number,
                        "source": self.provider_name,
                    },
                )
                value_rows.append(
                    FundamentalValueRecord(
                        symbol=symbol.upper(),
                        cik=cik,
                        fiscal_year=fiscal_year,
                        fiscal_period=fiscal_period,
                        period_type="annual" if str(form_type).upper() == "10-K" else "quarterly",
                        period_end_date=period_end_date,
                        filing_date=filing_date,
                        available_date=filing_date,
                        form_type=form_type,
                        accession_number=accession_number,
                        source=self.provider_name,
                        metric_name=metric_name,
                        metric_value=entry.get("val"),
                    ).to_dict()
                )

        values_df = pd.DataFrame(value_rows)
        if values_df.empty:
            return pd.DataFrame(), pd.DataFrame()
        filing_df = pd.DataFrame(filing_rows.values(), columns=FUNDAMENTAL_FILING_COLUMNS).drop_duplicates().reset_index(drop=True)
        return filing_df, values_df

    def fetch(self, *, symbols: list[str]) -> ProviderFetchResult:
        if not self.is_configured():
            return _empty_result(self.provider_name, configured=False, status="not_configured")

        company_rows: list[dict[str, Any]] = []
        filing_frames: list[pd.DataFrame] = []
        value_frames: list[pd.DataFrame] = []
        missing_symbols: list[str] = []

        for symbol in symbols:
            submissions_payload = _load_json(self._resolve_path(self.submissions_root, symbol))
            companyfacts_payload = _load_json(self._resolve_path(self.companyfacts_root, symbol, submissions_payload.get("cik")))
            if not submissions_payload and not companyfacts_payload:
                missing_symbols.append(symbol)
                continue
            cik = str(submissions_payload.get("cik") or companyfacts_payload.get("cik") or "").zfill(10) or None
            company_rows.append(self._normalize_company_master(symbol=symbol, submissions_payload=submissions_payload))
            filing_df, values_df = self._normalize_values(
                symbol=symbol,
                cik=cik,
                companyfacts_payload=companyfacts_payload,
            )
            if not filing_df.empty:
                filing_frames.append(filing_df)
            if not values_df.empty:
                value_frames.append(values_df)

        return ProviderFetchResult(
            company_master_df=pd.DataFrame(company_rows),
            filing_metadata_df=pd.concat(filing_frames, ignore_index=True) if filing_frames else _empty_filing_frame(self.provider_name),
            fundamental_values_df=pd.concat(value_frames, ignore_index=True) if value_frames else _empty_value_frame(self.provider_name),
            diagnostics={
                "provider": self.provider_name,
                "configured": True,
                "missing_symbols": missing_symbols,
                "company_count": len(company_rows),
                "filing_count": int(sum(len(frame) for frame in filing_frames)),
            },
        )

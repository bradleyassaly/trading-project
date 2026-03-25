from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


CANONICAL_FUNDAMENTAL_METRICS = (
    "revenue",
    "gross_profit",
    "operating_income",
    "net_income",
    "total_assets",
    "total_liabilities",
    "shareholders_equity",
    "cash_and_equivalents",
    "current_assets",
    "current_liabilities",
    "long_term_debt",
    "operating_cash_flow",
    "capital_expenditures",
    "free_cash_flow",
    "shares_outstanding",
)

DAILY_FUNDAMENTAL_FEATURE_COLUMNS = (
    "earnings_yield",
    "book_to_market",
    "sales_to_price",
    "roe",
    "roa",
    "gross_margin",
    "operating_margin",
    "revenue_growth_yoy",
    "net_income_growth_yoy",
    "debt_to_equity",
    "current_ratio",
    "free_cash_flow_yield",
    "accruals_proxy",
    "fundamental_value_score",
    "fundamental_quality_score",
    "fundamental_growth_score",
    "fundamental_quality_value_score",
    "sector_neutral_value_score",
    "sector_neutral_quality_score",
    "sector_neutral_growth_score",
    "sector_neutral_quality_value_score",
    "days_since_available",
)


@dataclass(frozen=True)
class CompanyMasterRecord:
    symbol: str
    cik: str | None = None
    company_name: str | None = None
    exchange: str | None = None
    sector: str | None = None
    industry: str | None = None
    is_active: bool = True
    source: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FilingMetadataRecord:
    symbol: str
    cik: str | None
    fiscal_year: int | None
    fiscal_period: str | None
    period_type: str | None
    period_end_date: str | None
    filing_date: str | None
    available_date: str | None
    form_type: str | None
    accession_number: str | None
    source: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FundamentalValueRecord:
    symbol: str
    cik: str | None
    fiscal_year: int | None
    fiscal_period: str | None
    period_type: str | None
    period_end_date: str | None
    filing_date: str | None
    available_date: str | None
    form_type: str | None
    accession_number: str | None
    source: str
    revenue: float | None = None
    gross_profit: float | None = None
    operating_income: float | None = None
    net_income: float | None = None
    total_assets: float | None = None
    total_liabilities: float | None = None
    shareholders_equity: float | None = None
    cash_and_equivalents: float | None = None
    current_assets: float | None = None
    current_liabilities: float | None = None
    long_term_debt: float | None = None
    operating_cash_flow: float | None = None
    capital_expenditures: float | None = None
    free_cash_flow: float | None = None
    shares_outstanding: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

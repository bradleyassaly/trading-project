from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from trading_platform.data.canonical import load_research_symbol_frame
from trading_platform.data.fundamentals.models import DAILY_FUNDAMENTAL_FEATURE_COLUMNS
from trading_platform.data.fundamentals.providers.base import FundamentalsProvider
from trading_platform.data.fundamentals.providers.sec import SECFundamentalsProvider
from trading_platform.data.fundamentals.providers.vendor import VendorFundamentalsProvider
from trading_platform.settings import FUNDAMENTALS_DIR


@dataclass(frozen=True)
class FundamentalsIngestionRequest:
    symbols: list[str]
    artifact_root: Path = FUNDAMENTALS_DIR
    providers: tuple[str, ...] = ("sec", "vendor")
    sec_companyfacts_root: str | None = None
    sec_submissions_root: str | None = None
    vendor_file_path: str | None = None
    vendor_api_key: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["artifact_root"] = str(self.artifact_root)
        return payload


@dataclass(frozen=True)
class FundamentalFeatureBuildRequest:
    artifact_root: Path = FUNDAMENTALS_DIR
    daily_features_path: Path | None = None
    calendar_dir: Path | None = None
    symbols: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["artifact_root"] = str(self.artifact_root)
        payload["daily_features_path"] = str(self.daily_features_path) if self.daily_features_path else None
        payload["calendar_dir"] = str(self.calendar_dir) if self.calendar_dir else None
        return payload


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = pd.to_numeric(denominator, errors="coerce").replace(0.0, np.nan)
    return pd.to_numeric(numerator, errors="coerce") / denominator


def _provider_instances(request: FundamentalsIngestionRequest) -> list[FundamentalsProvider]:
    registry: dict[str, FundamentalsProvider] = {
        "sec": SECFundamentalsProvider(
            companyfacts_root=request.sec_companyfacts_root,
            submissions_root=request.sec_submissions_root,
        ),
        "vendor": VendorFundamentalsProvider(
            file_path=request.vendor_file_path,
            api_key=request.vendor_api_key,
        ),
    }
    return [registry[name] for name in request.providers if name in registry]


def _coalesce_company_master(company_frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not company_frames:
        return pd.DataFrame(columns=["symbol", "cik", "company_name", "exchange", "sector", "industry", "is_active", "source"])
    combined = pd.concat(company_frames, ignore_index=True)
    combined["symbol"] = combined["symbol"].astype(str).str.upper()
    combined = combined.sort_values(["symbol", "source"]).drop_duplicates(subset=["symbol"], keep="first")
    return combined.reset_index(drop=True)


def _coalesce_filing_metadata(filing_frames: list[pd.DataFrame]) -> pd.DataFrame:
    columns = [
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
    if not filing_frames:
        return pd.DataFrame(columns=columns)
    combined = pd.concat(filing_frames, ignore_index=True)
    combined["symbol"] = combined["symbol"].astype(str).str.upper()
    combined["available_date"] = pd.to_datetime(combined["available_date"], errors="coerce")
    combined["filing_date"] = pd.to_datetime(combined["filing_date"], errors="coerce")
    combined["period_end_date"] = pd.to_datetime(combined["period_end_date"], errors="coerce")
    combined = combined.sort_values(["symbol", "available_date", "source"]).drop_duplicates(
        subset=["symbol", "accession_number", "period_end_date", "fiscal_year", "fiscal_period"],
        keep="first",
    )
    return combined.reset_index(drop=True)


def _coalesce_fundamental_values(value_frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not value_frames:
        return pd.DataFrame(
            columns=[
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
        )
    combined = pd.concat(value_frames, ignore_index=True)
    combined["symbol"] = combined["symbol"].astype(str).str.upper()
    combined["available_date"] = pd.to_datetime(combined["available_date"], errors="coerce")
    combined["filing_date"] = pd.to_datetime(combined["filing_date"], errors="coerce")
    combined["period_end_date"] = pd.to_datetime(combined["period_end_date"], errors="coerce")
    combined = combined.sort_values(["symbol", "available_date", "source"]).drop_duplicates(
        subset=["symbol", "accession_number", "period_end_date", "fiscal_year", "fiscal_period"],
        keep="first",
    )
    if "free_cash_flow" not in combined.columns or combined["free_cash_flow"].isna().all():
        combined["free_cash_flow"] = (
            pd.to_numeric(combined.get("operating_cash_flow"), errors="coerce")
            - pd.to_numeric(combined.get("capital_expenditures"), errors="coerce")
        )
    return combined.reset_index(drop=True)


def ingest_fundamentals(request: FundamentalsIngestionRequest) -> dict[str, str]:
    request.artifact_root.mkdir(parents=True, exist_ok=True)
    provider_diagnostics: list[dict[str, Any]] = []
    company_frames: list[pd.DataFrame] = []
    filing_frames: list[pd.DataFrame] = []
    value_frames: list[pd.DataFrame] = []

    for provider in _provider_instances(request):
        result = provider.fetch(symbols=request.symbols)
        provider_diagnostics.append(result.diagnostics)
        if not result.company_master_df.empty:
            company_frames.append(result.company_master_df)
        if not result.filing_metadata_df.empty:
            filing_frames.append(result.filing_metadata_df)
        if not result.fundamental_values_df.empty:
            value_frames.append(result.fundamental_values_df)

    company_df = _coalesce_company_master(company_frames)
    values_df = _coalesce_fundamental_values(value_frames)
    filing_df = _coalesce_filing_metadata(filing_frames)
    if filing_df.empty and not values_df.empty:
        filing_df = _coalesce_filing_metadata(
            [
                values_df[
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
                ]
            ]
        )

    company_path = request.artifact_root / "company_master.parquet"
    filing_path = request.artifact_root / "fundamental_filings.parquet"
    values_path = request.artifact_root / "fundamental_values.parquet"
    summary_path = request.artifact_root / "fundamental_summary.json"

    company_df.to_parquet(company_path, index=False)
    filing_df.to_parquet(filing_path, index=False)
    values_df.to_parquet(values_path, index=False)
    summary_path.write_text(
        json.dumps(
            {
                "request": request.to_dict(),
                "provider_diagnostics": provider_diagnostics,
                "company_count": int(len(company_df)),
                "filing_count": int(len(filing_df)),
                "value_count": int(len(values_df)),
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    return {
        "company_master_path": str(company_path),
        "fundamental_filings_path": str(filing_path),
        "fundamental_values_path": str(values_path),
        "fundamental_summary_path": str(summary_path),
    }


def _align_symbol_daily_features(
    *,
    symbol: str,
    calendar_df: pd.DataFrame,
    values_df: pd.DataFrame,
    company_master_df: pd.DataFrame,
) -> pd.DataFrame:
    symbol_values = values_df.loc[values_df["symbol"].astype(str).eq(symbol)].copy()
    if symbol_values.empty:
        base = calendar_df[["timestamp", "symbol", "close"]].copy()
        for column in DAILY_FUNDAMENTAL_FEATURE_COLUMNS:
            base[column] = pd.Series(dtype="float64")
        return base

    symbol_values = symbol_values.sort_values("available_date").copy()
    symbol_values["previous_year_revenue"] = symbol_values.groupby("fiscal_period")["revenue"].shift(1)
    symbol_values["previous_year_net_income"] = symbol_values.groupby("fiscal_period")["net_income"].shift(1)

    calendar = calendar_df[["timestamp", "symbol", "close"]].sort_values("timestamp").copy()
    merged = pd.merge_asof(
        calendar,
        symbol_values.sort_values("available_date"),
        left_on="timestamp",
        right_on="available_date",
        by="symbol",
        direction="backward",
    )
    merged["days_since_available"] = (merged["timestamp"] - merged["available_date"]).dt.days
    market_cap = pd.to_numeric(merged["close"], errors="coerce") * pd.to_numeric(merged["shares_outstanding"], errors="coerce")
    merged["earnings_yield"] = _safe_ratio(merged["net_income"], market_cap)
    merged["book_to_market"] = _safe_ratio(merged["shareholders_equity"], market_cap)
    merged["sales_to_price"] = _safe_ratio(merged["revenue"], market_cap)
    merged["roe"] = _safe_ratio(merged["net_income"], merged["shareholders_equity"])
    merged["roa"] = _safe_ratio(merged["net_income"], merged["total_assets"])
    merged["gross_margin"] = _safe_ratio(merged["gross_profit"], merged["revenue"])
    merged["operating_margin"] = _safe_ratio(merged["operating_income"], merged["revenue"])
    merged["revenue_growth_yoy"] = _safe_ratio(
        pd.to_numeric(merged["revenue"], errors="coerce") - pd.to_numeric(merged["previous_year_revenue"], errors="coerce"),
        merged["previous_year_revenue"],
    )
    merged["net_income_growth_yoy"] = _safe_ratio(
        pd.to_numeric(merged["net_income"], errors="coerce") - pd.to_numeric(merged["previous_year_net_income"], errors="coerce"),
        merged["previous_year_net_income"],
    )
    merged["debt_to_equity"] = _safe_ratio(merged["long_term_debt"], merged["shareholders_equity"])
    merged["current_ratio"] = _safe_ratio(merged["current_assets"], merged["current_liabilities"])
    merged["free_cash_flow_yield"] = _safe_ratio(merged["free_cash_flow"], market_cap)
    merged["accruals_proxy"] = _safe_ratio(
        pd.to_numeric(merged["net_income"], errors="coerce") - pd.to_numeric(merged["operating_cash_flow"], errors="coerce"),
        merged["total_assets"],
    )
    merged["fundamental_value_score"] = merged[["earnings_yield", "book_to_market", "sales_to_price", "free_cash_flow_yield"]].mean(axis=1, skipna=True)
    negative_quality_terms = -merged[["debt_to_equity", "accruals_proxy"]].apply(pd.to_numeric, errors="coerce")
    merged["fundamental_quality_score"] = pd.concat(
        [
            merged[["roe", "roa", "gross_margin", "operating_margin", "current_ratio"]],
            negative_quality_terms,
        ],
        axis=1,
    ).mean(axis=1, skipna=True)
    merged["fundamental_growth_score"] = merged[["revenue_growth_yoy", "net_income_growth_yoy"]].mean(axis=1, skipna=True)
    merged["fundamental_quality_value_score"] = merged[["fundamental_value_score", "fundamental_quality_score"]].mean(axis=1, skipna=True)
    company_row = company_master_df.loc[company_master_df["symbol"].astype(str).eq(symbol)].head(1)
    merged["sector"] = company_row["sector"].iloc[0] if not company_row.empty and "sector" in company_row.columns else None
    merged["industry"] = company_row["industry"].iloc[0] if not company_row.empty and "industry" in company_row.columns else None
    return merged


def build_daily_fundamental_features(request: FundamentalFeatureBuildRequest) -> dict[str, str]:
    artifact_root = request.artifact_root
    values_df = pd.read_parquet(artifact_root / "fundamental_values.parquet") if (artifact_root / "fundamental_values.parquet").exists() else pd.DataFrame()
    company_master_df = pd.read_parquet(artifact_root / "company_master.parquet") if (artifact_root / "company_master.parquet").exists() else pd.DataFrame()
    if not values_df.empty:
        for column in ("period_end_date", "filing_date", "available_date"):
            if column in values_df.columns:
                values_df[column] = pd.to_datetime(values_df[column], errors="coerce")
    if values_df.empty or request.calendar_dir is None:
        daily_features_df = pd.DataFrame(columns=["timestamp", "symbol", *DAILY_FUNDAMENTAL_FEATURE_COLUMNS])
    else:
        symbols = request.symbols or sorted(values_df["symbol"].dropna().astype(str).unique().tolist())
        frames: list[pd.DataFrame] = []
        for symbol in symbols:
            try:
                calendar_df = load_research_symbol_frame(request.calendar_dir, symbol)
            except FileNotFoundError:
                continue
            frames.append(
                _align_symbol_daily_features(
                    symbol=symbol,
                    calendar_df=calendar_df,
                    values_df=values_df,
                    company_master_df=company_master_df,
                )
            )
        daily_features_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["timestamp", "symbol", *DAILY_FUNDAMENTAL_FEATURE_COLUMNS])

    if not daily_features_df.empty and "sector" in daily_features_df.columns:
        for score_name, output_name in (
            ("fundamental_value_score", "sector_neutral_value_score"),
            ("fundamental_quality_score", "sector_neutral_quality_score"),
            ("fundamental_growth_score", "sector_neutral_growth_score"),
            ("fundamental_quality_value_score", "sector_neutral_quality_value_score"),
        ):
            daily_features_df[output_name] = daily_features_df.groupby(["timestamp", "sector"], dropna=False)[score_name].transform(
                lambda values: values - values.mean() if values.notna().sum() >= 1 else values
            )

    daily_features_path = request.daily_features_path or artifact_root / "daily_fundamental_features.parquet"
    coverage_path = artifact_root / "fundamental_feature_coverage.csv"
    lag_audit_path = artifact_root / "fundamental_lag_audit.csv"
    summary_path = artifact_root / "fundamental_summary.json"
    daily_features_df.to_parquet(daily_features_path, index=False)

    coverage_df = pd.DataFrame(
        [
            {
                "feature_name": column,
                "non_null_rows": int(daily_features_df[column].notna().sum()) if column in daily_features_df.columns else 0,
                "coverage_ratio": float(daily_features_df[column].notna().mean()) if column in daily_features_df.columns and len(daily_features_df) else 0.0,
            }
            for column in DAILY_FUNDAMENTAL_FEATURE_COLUMNS
        ]
    )
    coverage_df.to_csv(coverage_path, index=False)

    lag_audit_df = pd.DataFrame()
    if not values_df.empty:
        lag_audit_df = values_df[["symbol", "period_end_date", "filing_date", "available_date", "source"]].copy()
        lag_audit_df["availability_lag_days"] = (
            pd.to_datetime(lag_audit_df["available_date"], errors="coerce")
            - pd.to_datetime(lag_audit_df["period_end_date"], errors="coerce")
        ).dt.days
        lag_audit_df.to_csv(lag_audit_path, index=False)
    else:
        lag_audit_df.to_csv(lag_audit_path, index=False)

    summary_payload = {}
    if summary_path.exists():
        summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    summary_payload["daily_feature_build"] = {
        "request": request.to_dict(),
        "daily_feature_rows": int(len(daily_features_df)),
        "symbols_covered": int(daily_features_df["symbol"].nunique()) if not daily_features_df.empty else 0,
        "features": list(DAILY_FUNDAMENTAL_FEATURE_COLUMNS),
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2, default=str), encoding="utf-8")
    return {
        "daily_fundamental_features_path": str(daily_features_path),
        "fundamental_feature_coverage_path": str(coverage_path),
        "fundamental_lag_audit_path": str(lag_audit_path),
        "fundamental_summary_path": str(summary_path),
    }

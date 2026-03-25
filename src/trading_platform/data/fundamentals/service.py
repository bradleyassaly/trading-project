from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from trading_platform.data.canonical import load_research_symbol_frame
from trading_platform.data.fundamentals.models import (
    CROSS_SECTIONAL_FUNDAMENTAL_SIGNAL_BASE_COLUMNS,
    CANONICAL_FUNDAMENTAL_METRICS,
    DAILY_FUNDAMENTAL_FEATURE_COLUMNS,
    FUNDAMENTAL_FILING_COLUMNS,
    FUNDAMENTAL_VALUE_COLUMNS,
    RAW_DAILY_FUNDAMENTAL_FEATURE_COLUMNS,
)
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
    vendor_cache_enabled: bool = True
    vendor_cache_root: Path | None = None
    vendor_cache_ttl_hours: float = 24.0
    vendor_force_refresh: bool = False
    vendor_request_delay_seconds: float = 0.5
    vendor_max_retries: int = 4
    vendor_max_symbols_per_run: int | None = None
    vendor_max_requests_per_run: int | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["artifact_root"] = str(self.artifact_root)
        payload["vendor_cache_root"] = str(self.vendor_cache_root) if self.vendor_cache_root else None
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


def _winsorize_series(series: pd.Series, *, lower_quantile: float = 0.05, upper_quantile: float = 0.95) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    non_null = numeric.dropna()
    if len(non_null) < 3:
        return numeric
    lower = non_null.quantile(lower_quantile)
    upper = non_null.quantile(upper_quantile)
    return numeric.clip(lower=lower, upper=upper)


def _cross_section_rank_pct(series: pd.Series) -> pd.Series:
    winsorized = _winsorize_series(series)
    non_null = winsorized.dropna()
    if len(non_null) < 2 or non_null.nunique() <= 1:
        return pd.Series(np.nan, index=series.index, dtype=float)
    return winsorized.rank(method="average", pct=True)


def _cross_section_zscore(series: pd.Series) -> pd.Series:
    winsorized = _winsorize_series(series)
    non_null = winsorized.dropna()
    if len(non_null) < 2 or non_null.nunique() <= 1:
        return pd.Series(np.nan, index=series.index, dtype=float)
    std = non_null.std(ddof=0)
    if pd.isna(std) or std == 0.0:
        return pd.Series(np.nan, index=series.index, dtype=float)
    mean = non_null.mean()
    return (winsorized - mean) / std


def _apply_cross_sectional_fundamental_transforms(daily_features_df: pd.DataFrame) -> pd.DataFrame:
    if daily_features_df.empty:
        return daily_features_df

    transformed = daily_features_df.copy()
    for base_column in CROSS_SECTIONAL_FUNDAMENTAL_SIGNAL_BASE_COLUMNS:
        if base_column not in transformed.columns:
            transformed[f"{base_column}_rank_pct"] = pd.Series(np.nan, index=transformed.index, dtype=float)
            transformed[f"{base_column}_zscore"] = pd.Series(np.nan, index=transformed.index, dtype=float)
            continue
        transformed[f"{base_column}_rank_pct"] = transformed.groupby("timestamp", dropna=False)[base_column].transform(
            _cross_section_rank_pct
        )
        transformed[f"{base_column}_zscore"] = transformed.groupby("timestamp", dropna=False)[base_column].transform(
            _cross_section_zscore
        )
    return transformed


def _provider_instances(request: FundamentalsIngestionRequest) -> list[FundamentalsProvider]:
    registry: dict[str, FundamentalsProvider] = {
        "sec": SECFundamentalsProvider(
            companyfacts_root=request.sec_companyfacts_root,
            submissions_root=request.sec_submissions_root,
        ),
        "vendor": VendorFundamentalsProvider(
            file_path=request.vendor_file_path,
            api_key=request.vendor_api_key,
            cache_enabled=request.vendor_cache_enabled,
            cache_root=request.vendor_cache_root or (request.artifact_root / "raw_fmp"),
            cache_ttl_hours=request.vendor_cache_ttl_hours,
            force_refresh=request.vendor_force_refresh,
            request_delay_seconds=request.vendor_request_delay_seconds,
            max_retries=request.vendor_max_retries,
            max_symbols_per_run=request.vendor_max_symbols_per_run,
            max_requests_per_run=request.vendor_max_requests_per_run,
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
    def _normalize_value_frame(frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame(columns=FUNDAMENTAL_VALUE_COLUMNS)
        normalized = frame.copy()
        if "metric_name" not in normalized.columns or "metric_value" not in normalized.columns:
            metadata_columns = [column for column in FUNDAMENTAL_FILING_COLUMNS if column in normalized.columns]
            value_columns = [column for column in CANONICAL_FUNDAMENTAL_METRICS if column in normalized.columns]
            if not value_columns:
                return pd.DataFrame(columns=FUNDAMENTAL_VALUE_COLUMNS)
            normalized = normalized.melt(
                id_vars=metadata_columns,
                value_vars=value_columns,
                var_name="metric_name",
                value_name="metric_value",
            )
        normalized = normalized.dropna(subset=["metric_name"]).copy()
        normalized["metric_name"] = normalized["metric_name"].astype(str)
        normalized["metric_value"] = pd.to_numeric(normalized["metric_value"], errors="coerce")
        normalized = normalized.loc[normalized["metric_name"].isin(CANONICAL_FUNDAMENTAL_METRICS)].copy()
        normalized = normalized.loc[normalized["metric_value"].notna()].copy()
        for column in FUNDAMENTAL_VALUE_COLUMNS:
            if column not in normalized.columns:
                normalized[column] = pd.NA
        return normalized[list(FUNDAMENTAL_VALUE_COLUMNS)].copy()

    if not value_frames:
        return pd.DataFrame(columns=FUNDAMENTAL_VALUE_COLUMNS)
    combined = pd.concat([_normalize_value_frame(frame) for frame in value_frames], ignore_index=True)
    if combined.empty:
        return pd.DataFrame(columns=FUNDAMENTAL_VALUE_COLUMNS)
    combined["symbol"] = combined["symbol"].astype(str).str.upper()
    combined["available_date"] = pd.to_datetime(combined["available_date"], errors="coerce")
    combined["filing_date"] = pd.to_datetime(combined["filing_date"], errors="coerce")
    combined["period_end_date"] = pd.to_datetime(combined["period_end_date"], errors="coerce")
    combined = combined.sort_values(["symbol", "available_date", "metric_name", "source"]).drop_duplicates(
        subset=["symbol", "accession_number", "period_end_date", "fiscal_year", "fiscal_period", "metric_name"],
        keep="first",
    )
    free_cash_flow_keys = set(
        tuple(row)
        for row in combined.loc[combined["metric_name"].eq("free_cash_flow"), ["symbol", "accession_number", "period_end_date", "fiscal_year", "fiscal_period"]].itertuples(index=False, name=None)
    )
    operating_cash_flow = combined.loc[combined["metric_name"].eq("operating_cash_flow")].rename(columns={"metric_value": "operating_cash_flow"})
    capital_expenditures = combined.loc[combined["metric_name"].eq("capital_expenditures")].rename(columns={"metric_value": "capital_expenditures"})
    derived_free_cash_flow = operating_cash_flow.merge(
        capital_expenditures[
            ["symbol", "accession_number", "period_end_date", "fiscal_year", "fiscal_period", "capital_expenditures"]
        ],
        on=["symbol", "accession_number", "period_end_date", "fiscal_year", "fiscal_period"],
        how="inner",
    )
    if not derived_free_cash_flow.empty:
        derived_free_cash_flow["dedupe_key"] = list(
            zip(
                derived_free_cash_flow["symbol"],
                derived_free_cash_flow["accession_number"],
                derived_free_cash_flow["period_end_date"],
                derived_free_cash_flow["fiscal_year"],
                derived_free_cash_flow["fiscal_period"],
            )
        )
        derived_free_cash_flow = derived_free_cash_flow.loc[~derived_free_cash_flow["dedupe_key"].isin(free_cash_flow_keys)].copy()
        if not derived_free_cash_flow.empty:
            derived_free_cash_flow["metric_name"] = "free_cash_flow"
            derived_free_cash_flow["metric_value"] = (
                pd.to_numeric(derived_free_cash_flow["operating_cash_flow"], errors="coerce")
                - pd.to_numeric(derived_free_cash_flow["capital_expenditures"], errors="coerce")
            )
            combined = pd.concat(
                [
                    combined,
                    derived_free_cash_flow[list(FUNDAMENTAL_VALUE_COLUMNS)],
                ],
                ignore_index=True,
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
                values_df[list(FUNDAMENTAL_FILING_COLUMNS)]
            ]
        )

    company_path = request.artifact_root / "company_master.parquet"
    filing_path = request.artifact_root / "fundamental_filings.parquet"
    values_path = request.artifact_root / "fundamental_values.parquet"
    summary_path = request.artifact_root / "fundamental_summary.json"
    provider_errors = [
        diagnostic.get("message")
        for diagnostic in provider_diagnostics
        if diagnostic.get("status") in {"api_key_missing", "error"}
    ]
    if values_df.empty and provider_errors:
        raise RuntimeError("; ".join(str(error) for error in provider_errors if error))

    aggregate_symbols_fetched = sorted(
        {
            str(symbol)
            for diagnostic in provider_diagnostics
            for symbol in diagnostic.get("symbols_fetched", []) or []
        }
    )
    aggregate_symbols_skipped_from_cache = sorted(
        {
            str(symbol)
            for diagnostic in provider_diagnostics
            for symbol in diagnostic.get("symbols_skipped_from_cache", []) or []
        }
    )
    aggregate_symbols_failed = sorted(
        {
            str(symbol)
            for diagnostic in provider_diagnostics
            for symbol in diagnostic.get("symbols_failed", []) or []
        }
    )
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
                "metric_row_count": int(len(values_df)),
                "metrics_by_name": {
                    metric_name: int(count)
                    for metric_name, count in values_df["metric_name"].value_counts().sort_index().items()
                }
                if not values_df.empty
                else {},
                "symbols_requested": len(request.symbols),
                "symbols_with_values": int(values_df["symbol"].nunique()) if not values_df.empty else 0,
                "symbols_with_metric_coverage": sorted(values_df["symbol"].dropna().astype(str).str.upper().unique().tolist()) if not values_df.empty else [],
                "cache_hits": int(sum(int(diagnostic.get("cache_hits", 0) or 0) for diagnostic in provider_diagnostics)),
                "cache_misses": int(sum(int(diagnostic.get("cache_misses", 0) or 0) for diagnostic in provider_diagnostics)),
                "symbols_fetched": aggregate_symbols_fetched,
                "symbols_skipped_from_cache": aggregate_symbols_skipped_from_cache,
                "retry_count": int(sum(int(diagnostic.get("retry_count", 0) or 0) for diagnostic in provider_diagnostics)),
                "rate_limit_error_count": int(sum(int(diagnostic.get("rate_limit_error_count", 0) or 0) for diagnostic in provider_diagnostics)),
                "symbols_failed": aggregate_symbols_failed,
                "warnings": [
                    diagnostic.get("message")
                    for diagnostic in provider_diagnostics
                    if diagnostic.get("status") not in {None, "ok"}
                    and diagnostic.get("message")
                ],
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

    symbol_values["metric_value"] = pd.to_numeric(symbol_values["metric_value"], errors="coerce")
    symbol_values = symbol_values.sort_values(["available_date", "metric_name"]).copy()
    symbol_values = (
        symbol_values.pivot_table(
            index=list(FUNDAMENTAL_FILING_COLUMNS),
            columns="metric_name",
            values="metric_value",
            aggfunc="last",
        )
        .reset_index()
    )
    symbol_values.columns.name = None
    if "free_cash_flow" not in symbol_values.columns:
        symbol_values["free_cash_flow"] = (
            pd.to_numeric(symbol_values.get("operating_cash_flow"), errors="coerce")
            - pd.to_numeric(symbol_values.get("capital_expenditures"), errors="coerce")
        )
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
        if "metric_name" not in values_df.columns or "metric_value" not in values_df.columns:
            values_df = _coalesce_fundamental_values([values_df])
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
    daily_features_df = _apply_cross_sectional_fundamental_transforms(daily_features_df)

    if not daily_features_df.empty:
        preserved_columns = list(
            dict.fromkeys(
                [
                    column
                    for column in (
                        "timestamp",
                        "symbol",
                        "sector",
                        "industry",
                        *RAW_DAILY_FUNDAMENTAL_FEATURE_COLUMNS,
                        *DAILY_FUNDAMENTAL_FEATURE_COLUMNS,
                    )
                    if column in daily_features_df.columns
                ]
            )
        )
        daily_features_df = daily_features_df[preserved_columns].copy()

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
                "symbols_with_values": int(daily_features_df.loc[daily_features_df[column].notna(), "symbol"].nunique())
                if column in daily_features_df.columns and not daily_features_df.empty
                else 0,
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
        "feature_coverage": {
            column: float(daily_features_df[column].notna().mean())
            for column in DAILY_FUNDAMENTAL_FEATURE_COLUMNS
            if column in daily_features_df.columns and len(daily_features_df)
        },
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2, default=str), encoding="utf-8")
    return {
        "daily_fundamental_features_path": str(daily_features_path),
        "fundamental_feature_coverage_path": str(coverage_path),
        "fundamental_lag_audit_path": str(lag_audit_path),
        "fundamental_summary_path": str(summary_path),
    }

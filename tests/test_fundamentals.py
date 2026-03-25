from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from trading_platform.data.fundamentals.providers.sec import SECFundamentalsProvider
from trading_platform.data.fundamentals.providers import vendor as vendor_module
from trading_platform.data.fundamentals.providers.vendor import VendorFundamentalsProvider
from trading_platform.data.fundamentals.service import (
    FundamentalFeatureBuildRequest,
    FundamentalsIngestionRequest,
    build_daily_fundamental_features,
    ingest_fundamentals,
)
from trading_platform.research.alpha_lab.runner import run_alpha_research


class _FakeHTTPResponse:
    def __init__(self, payload: Any) -> None:
        self._payload = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._payload

    def __enter__(self) -> "_FakeHTTPResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def _install_fmp_urlopen(monkeypatch: pytest.MonkeyPatch, payloads: dict[str, Any]) -> None:
    def _fake_urlopen(request, timeout=30):  # noqa: ANN001
        full_url = request.full_url
        for expected_fragment, payload in payloads.items():
            if expected_fragment in full_url:
                return _FakeHTTPResponse(payload)
        raise AssertionError(f"Unexpected FMP URL requested during test: {full_url}")

    monkeypatch.setattr(vendor_module, "urlopen", _fake_urlopen)


def _fmp_payloads_for_symbol(symbol: str) -> dict[str, Any]:
    symbol = symbol.upper()
    return {
        f"profile?symbol={symbol}": [
            {
                "symbol": symbol,
                "companyName": f"{symbol} Inc.",
                "exchangeShortName": "NASDAQ",
                "sector": "Technology",
                "industry": "Software",
                "cik": "320193",
                "isActivelyTrading": True,
            }
        ],
        f"income-statement?symbol={symbol}&period=annual": [
            {
                "date": "2024-12-31",
                "calendarYear": "2024",
                "period": "FY",
                "revenue": 120.0,
                "grossProfit": 60.0,
                "operatingIncome": 30.0,
                "netIncome": 24.0,
                "weightedAverageShsOut": 10.0,
                "fillingDate": "2025-02-20",
                "acceptedDate": "2025-02-20 20:10:00",
                "finalLink": f"https://example.com/{symbol}/10k-2024",
            },
            {
                "date": "2023-12-31",
                "calendarYear": "2023",
                "period": "FY",
                "revenue": 100.0,
                "grossProfit": 45.0,
                "operatingIncome": 20.0,
                "netIncome": 10.0,
                "weightedAverageShsOut": 10.0,
                "fillingDate": "2024-02-15",
                "acceptedDate": "2024-02-15 18:05:00",
                "finalLink": f"https://example.com/{symbol}/10k-2023",
            },
        ],
        f"balance-sheet-statement?symbol={symbol}&period=annual": [
            {
                "date": "2024-12-31",
                "calendarYear": "2024",
                "period": "FY",
                "totalAssets": 250.0,
                "totalLiabilities": 100.0,
                "totalStockholdersEquity": 150.0,
                "cashAndCashEquivalents": 35.0,
                "totalCurrentAssets": 95.0,
                "totalCurrentLiabilities": 40.0,
                "longTermDebt": 22.0,
                "fillingDate": "2025-02-20",
                "acceptedDate": "2025-02-20 20:10:00",
                "finalLink": f"https://example.com/{symbol}/10k-2024",
            },
            {
                "date": "2023-12-31",
                "calendarYear": "2023",
                "period": "FY",
                "totalAssets": 200.0,
                "totalLiabilities": 80.0,
                "totalStockholdersEquity": 120.0,
                "cashAndCashEquivalents": 25.0,
                "totalCurrentAssets": 90.0,
                "totalCurrentLiabilities": 30.0,
                "longTermDebt": 20.0,
                "fillingDate": "2024-02-15",
                "acceptedDate": "2024-02-15 18:05:00",
                "finalLink": f"https://example.com/{symbol}/10k-2023",
            },
        ],
        f"cash-flow-statement?symbol={symbol}&period=annual": [
            {
                "date": "2024-12-31",
                "calendarYear": "2024",
                "period": "FY",
                "operatingCashFlow": 32.0,
                "capitalExpenditure": 8.0,
                "freeCashFlow": 24.0,
                "fillingDate": "2025-02-20",
                "acceptedDate": "2025-02-20 20:10:00",
                "finalLink": f"https://example.com/{symbol}/10k-2024",
            },
            {
                "date": "2023-12-31",
                "calendarYear": "2023",
                "period": "FY",
                "operatingCashFlow": 14.0,
                "capitalExpenditure": 4.0,
                "freeCashFlow": 10.0,
                "fillingDate": "2024-02-15",
                "acceptedDate": "2024-02-15 18:05:00",
                "finalLink": f"https://example.com/{symbol}/10k-2023",
            },
        ],
        f"income-statement?symbol={symbol}&period=quarter": [],
        f"balance-sheet-statement?symbol={symbol}&period=quarter": [],
        f"cash-flow-statement?symbol={symbol}&period=quarter": [],
    }


def test_vendor_provider_normalizes_symbols_and_derives_free_cash_flow(tmp_path: Path) -> None:
    vendor_path = tmp_path / "vendor_fundamentals.json"
    vendor_path.write_text(
        json.dumps(
            {
                "company_master": [
                    {"symbol": "aapl", "company_name": "Apple Inc.", "sector": "Technology"},
                ],
                "fundamental_values": [
                    {
                        "symbol": "aapl",
                        "cik": "0000320193",
                        "fiscal_year": 2024,
                        "fiscal_period": "FY",
                        "period_type": "annual",
                        "period_end_date": "2024-09-28",
                        "filing_date": "2024-11-01",
                        "available_date": "2024-11-01",
                        "form_type": "10-K",
                        "accession_number": "0000320193-24-000001",
                        "revenue": 100.0,
                        "operating_cash_flow": 30.0,
                        "capital_expenditures": 5.0,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = VendorFundamentalsProvider(file_path=vendor_path).fetch(symbols=["AAPL"])

    assert result.diagnostics["status"] == "ok"
    assert result.company_master_df.loc[0, "symbol"] == "AAPL"
    assert result.fundamental_values_df.loc[0, "symbol"] == "AAPL"
    assert result.fundamental_values_df.loc[0, "free_cash_flow"] == 25.0


def test_sec_provider_normalizes_companyfacts_into_canonical_rows(tmp_path: Path) -> None:
    companyfacts_root = tmp_path / "companyfacts"
    submissions_root = tmp_path / "submissions"
    companyfacts_root.mkdir()
    submissions_root.mkdir()

    (submissions_root / "AAPL.json").write_text(
        json.dumps(
            {
                "cik": "320193",
                "name": "Apple Inc.",
                "exchanges": ["NASDAQ"],
                "sicDescription": "Electronic Computers",
            }
        ),
        encoding="utf-8",
    )
    (companyfacts_root / "AAPL.json").write_text(
        json.dumps(
            {
                "cik": "320193",
                "facts": {
                    "us-gaap": {
                        "Revenues": {
                            "units": {
                                "USD": [
                                    {
                                        "val": 100.0,
                                        "fy": 2024,
                                        "fp": "FY",
                                        "end": "2024-09-28",
                                        "filed": "2024-11-01",
                                        "form": "10-K",
                                        "accn": "0000320193-24-000001",
                                    }
                                ]
                            }
                        },
                        "NetIncomeLoss": {
                            "units": {
                                "USD": [
                                    {
                                        "val": 22.0,
                                        "fy": 2024,
                                        "fp": "FY",
                                        "end": "2024-09-28",
                                        "filed": "2024-11-01",
                                        "form": "10-K",
                                        "accn": "0000320193-24-000001",
                                    }
                                ]
                            }
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    result = SECFundamentalsProvider(
        companyfacts_root=companyfacts_root,
        submissions_root=submissions_root,
    ).fetch(symbols=["AAPL"])

    assert result.diagnostics["configured"] is True
    assert result.company_master_df.loc[0, "company_name"] == "Apple Inc."
    assert result.company_master_df.loc[0, "exchange"] == "NASDAQ"
    assert result.fundamental_values_df.loc[0, "revenue"] == 100.0
    assert result.fundamental_values_df.loc[0, "net_income"] == 22.0
    assert result.filing_metadata_df.loc[0, "available_date"] == "2024-11-01"


def test_vendor_provider_fetches_and_normalizes_fmp_responses(monkeypatch: pytest.MonkeyPatch) -> None:
    payloads = _fmp_payloads_for_symbol("AAPL")
    _install_fmp_urlopen(monkeypatch, payloads)

    result = VendorFundamentalsProvider(api_key="test-key").fetch(symbols=["AAPL"])

    assert result.diagnostics["status"] == "ok"
    assert result.diagnostics["mode"] == "fmp"
    assert result.company_master_df.loc[0, "symbol"] == "AAPL"
    assert result.company_master_df.loc[0, "source"] == "vendor:fmp"
    assert len(result.fundamental_values_df) == 2
    latest_row = result.fundamental_values_df.sort_values("period_end_date").iloc[-1]
    assert latest_row["revenue"] == 120.0
    assert latest_row["shareholders_equity"] == 150.0
    assert latest_row["operating_cash_flow"] == 32.0
    assert latest_row["free_cash_flow"] == 24.0
    assert latest_row["available_date"] == "2025-02-20"


def test_ingest_fundamentals_raises_clear_error_when_vendor_api_key_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FMP_API_KEY", raising=False)

    with pytest.raises(RuntimeError, match="FMP_API_KEY"):
        ingest_fundamentals(
            FundamentalsIngestionRequest(
                symbols=["AAPL"],
                artifact_root=tmp_path / "fundamentals",
                providers=("vendor",),
            )
        )


def test_vendor_provider_supports_partial_symbol_coverage(monkeypatch: pytest.MonkeyPatch) -> None:
    payloads = {}
    payloads.update(_fmp_payloads_for_symbol("AAPL"))
    payloads.update(
        {
            "profile?symbol=MSFT": [],
            "income-statement?symbol=MSFT&period=annual": [],
            "balance-sheet-statement?symbol=MSFT&period=annual": [],
            "cash-flow-statement?symbol=MSFT&period=annual": [],
            "income-statement?symbol=MSFT&period=quarter": [],
            "balance-sheet-statement?symbol=MSFT&period=quarter": [],
            "cash-flow-statement?symbol=MSFT&period=quarter": [],
        }
    )
    _install_fmp_urlopen(monkeypatch, payloads)

    result = VendorFundamentalsProvider(api_key="test-key").fetch(symbols=["AAPL", "MSFT"])

    assert result.diagnostics["status"] == "ok"
    assert result.diagnostics["missing_symbols"] == ["MSFT"]
    assert sorted(result.fundamental_values_df["symbol"].unique().tolist()) == ["AAPL"]


def test_build_daily_fundamental_features_respects_available_date_and_forward_fill(tmp_path: Path) -> None:
    artifact_root = tmp_path / "fundamentals"
    calendar_dir = tmp_path / "features"
    artifact_root.mkdir()
    calendar_dir.mkdir()

    pd.DataFrame(
        [{"symbol": "AAPL", "sector": "Technology", "industry": "Hardware"}]
    ).to_parquet(artifact_root / "company_master.parquet", index=False)
    pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "cik": "0000320193",
                "fiscal_year": 2023,
                "fiscal_period": "FY",
                "period_type": "annual",
                "period_end_date": "2023-12-31",
                "filing_date": "2024-02-15",
                "available_date": "2024-02-15",
                "form_type": "10-K",
                "accession_number": "accn-2023",
                "source": "vendor",
                "revenue": 100.0,
                "gross_profit": 40.0,
                "operating_income": 20.0,
                "net_income": 10.0,
                "total_assets": 200.0,
                "total_liabilities": 80.0,
                "shareholders_equity": 120.0,
                "cash_and_equivalents": 25.0,
                "current_assets": 90.0,
                "current_liabilities": 30.0,
                "long_term_debt": 20.0,
                "operating_cash_flow": 14.0,
                "capital_expenditures": 4.0,
                "free_cash_flow": 10.0,
                "shares_outstanding": 10.0,
            },
            {
                "symbol": "AAPL",
                "cik": "0000320193",
                "fiscal_year": 2024,
                "fiscal_period": "FY",
                "period_type": "annual",
                "period_end_date": "2024-12-31",
                "filing_date": "2025-02-20",
                "available_date": "2025-02-20",
                "form_type": "10-K",
                "accession_number": "accn-2024",
                "source": "vendor",
                "revenue": 150.0,
                "gross_profit": 75.0,
                "operating_income": 36.0,
                "net_income": 24.0,
                "total_assets": 260.0,
                "total_liabilities": 90.0,
                "shareholders_equity": 170.0,
                "cash_and_equivalents": 35.0,
                "current_assets": 120.0,
                "current_liabilities": 40.0,
                "long_term_debt": 30.0,
                "operating_cash_flow": 32.0,
                "capital_expenditures": 8.0,
                "free_cash_flow": 24.0,
                "shares_outstanding": 10.0,
            },
        ]
    ).to_parquet(artifact_root / "fundamental_values.parquet", index=False)
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-02-10", periods=380, freq="D"),
            "symbol": ["AAPL"] * 380,
            "close": [20.0] * 380,
        }
    ).to_parquet(calendar_dir / "AAPL.parquet", index=False)

    result = build_daily_fundamental_features(
        FundamentalFeatureBuildRequest(
            artifact_root=artifact_root,
            calendar_dir=calendar_dir,
            symbols=["AAPL"],
        )
    )
    daily_df = pd.read_parquet(result["daily_fundamental_features_path"])

    before_first_available = daily_df.loc[daily_df["timestamp"] == pd.Timestamp("2024-02-14")].iloc[0]
    first_available = daily_df.loc[daily_df["timestamp"] == pd.Timestamp("2024-02-15")].iloc[0]
    before_second_available = daily_df.loc[daily_df["timestamp"] == pd.Timestamp("2025-02-19")].iloc[0]
    second_available = daily_df.loc[daily_df["timestamp"] == pd.Timestamp("2025-02-20")].iloc[0]

    assert pd.isna(before_first_available["earnings_yield"])
    assert first_available["earnings_yield"] == 0.05
    assert first_available["current_ratio"] == 3.0
    assert first_available["debt_to_equity"] == 20.0 / 120.0
    assert before_second_available["sales_to_price"] == 100.0 / 200.0
    assert second_available["sales_to_price"] == 150.0 / 200.0
    assert second_available["revenue_growth_yoy"] == 0.5
    assert second_available["net_income_growth_yoy"] == 1.4
    assert first_available["days_since_available"] == 0
    assert second_available["days_since_available"] == 0


def test_fmp_ingest_and_feature_build_generate_non_empty_daily_features(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_root = tmp_path / "fundamentals"
    calendar_dir = tmp_path / "features"
    calendar_dir.mkdir()

    pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-02-10", periods=380, freq="D"),
            "symbol": ["AAPL"] * 380,
            "close": [20.0] * 380,
        }
    ).to_parquet(calendar_dir / "AAPL.parquet", index=False)

    _install_fmp_urlopen(monkeypatch, _fmp_payloads_for_symbol("AAPL"))
    ingest_result = ingest_fundamentals(
        FundamentalsIngestionRequest(
            symbols=["AAPL"],
            artifact_root=artifact_root,
            providers=("vendor",),
            vendor_api_key="test-key",
        )
    )
    feature_result = build_daily_fundamental_features(
        FundamentalFeatureBuildRequest(
            artifact_root=artifact_root,
            calendar_dir=calendar_dir,
            symbols=["AAPL"],
        )
    )
    summary = json.loads(Path(ingest_result["fundamental_summary_path"]).read_text(encoding="utf-8"))
    daily_df = pd.read_parquet(feature_result["daily_fundamental_features_path"])

    assert summary["provider_diagnostics"][0]["mode"] == "fmp"
    assert not daily_df.empty
    assert daily_df["earnings_yield"].notna().any()
    assert daily_df["fundamental_quality_value_score"].notna().any()
    before_available = daily_df.loc[daily_df["timestamp"] == pd.Timestamp("2024-02-14")].iloc[0]
    on_available = daily_df.loc[daily_df["timestamp"] == pd.Timestamp("2024-02-15")].iloc[0]
    assert pd.isna(before_available["sales_to_price"])
    assert on_available["sales_to_price"] == 100.0 / 200.0


def test_ingest_and_feature_build_write_expected_artifacts(tmp_path: Path) -> None:
    vendor_path = tmp_path / "vendor_fundamentals.json"
    artifact_root = tmp_path / "fundamentals"
    calendar_dir = tmp_path / "features"
    calendar_dir.mkdir()

    vendor_path.write_text(
        json.dumps(
            {
                "company_master": [
                    {"symbol": "AAPL", "company_name": "Apple Inc.", "sector": "Technology", "industry": "Hardware"},
                ],
                "fundamental_values": [
                    {
                        "symbol": "AAPL",
                        "cik": "0000320193",
                        "fiscal_year": 2024,
                        "fiscal_period": "FY",
                        "period_type": "annual",
                        "period_end_date": "2024-09-28",
                        "filing_date": "2024-11-01",
                        "available_date": "2024-11-01",
                        "form_type": "10-K",
                        "accession_number": "0000320193-24-000001",
                        "source": "vendor",
                        "revenue": 100.0,
                        "gross_profit": 45.0,
                        "operating_income": 22.0,
                        "net_income": 15.0,
                        "total_assets": 210.0,
                        "total_liabilities": 90.0,
                        "shareholders_equity": 120.0,
                        "cash_and_equivalents": 30.0,
                        "current_assets": 85.0,
                        "current_liabilities": 35.0,
                        "long_term_debt": 25.0,
                        "operating_cash_flow": 20.0,
                        "capital_expenditures": 5.0,
                        "shares_outstanding": 10.0,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-10-28", periods=10, freq="D"),
            "symbol": ["AAPL"] * 10,
            "close": [20.0] * 10,
        }
    ).to_parquet(calendar_dir / "AAPL.parquet", index=False)

    ingest_result = ingest_fundamentals(
        FundamentalsIngestionRequest(
            symbols=["AAPL"],
            artifact_root=artifact_root,
            providers=("vendor",),
            vendor_file_path=str(vendor_path),
        )
    )
    feature_result = build_daily_fundamental_features(
        FundamentalFeatureBuildRequest(
            artifact_root=artifact_root,
            calendar_dir=calendar_dir,
            symbols=["AAPL"],
        )
    )
    summary = json.loads(Path(feature_result["fundamental_summary_path"]).read_text(encoding="utf-8"))

    assert Path(ingest_result["company_master_path"]).exists()
    assert Path(ingest_result["fundamental_filings_path"]).exists()
    assert Path(ingest_result["fundamental_values_path"]).exists()
    assert Path(feature_result["daily_fundamental_features_path"]).exists()
    assert Path(feature_result["fundamental_feature_coverage_path"]).exists()
    assert Path(feature_result["fundamental_lag_audit_path"]).exists()
    assert summary["company_count"] == 1
    assert summary["daily_feature_build"]["daily_feature_rows"] == 10


def test_run_alpha_research_integrates_fundamental_features_without_breaking_flow(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    output_dir = tmp_path / "alpha_outputs"
    fundamentals_path = tmp_path / "daily_fundamental_features.parquet"
    feature_dir.mkdir()

    timestamps = pd.date_range("2024-01-01", periods=60, freq="D")
    for symbol, drift in {"AAPL": 0.003, "MSFT": 0.001}.items():
        closes = [100.0]
        for _ in range(59):
            closes.append(closes[-1] * (1.0 + drift))
        pd.DataFrame(
            {
                "timestamp": timestamps,
                "symbol": [symbol] * len(timestamps),
                "close": closes,
            }
        ).to_parquet(feature_dir / f"{symbol}.parquet", index=False)

    daily_features = []
    for symbol, value_score, quality_score in (
        ("AAPL", 0.80, 0.65),
        ("MSFT", 0.20, 0.40),
    ):
        frame = pd.DataFrame(
            {
                "timestamp": timestamps,
                "symbol": [symbol] * len(timestamps),
                "earnings_yield": [value_score] * len(timestamps),
                "book_to_market": [value_score * 0.8] * len(timestamps),
                "sales_to_price": [value_score * 0.6] * len(timestamps),
                "roe": [quality_score] * len(timestamps),
                "roa": [quality_score * 0.9] * len(timestamps),
                "gross_margin": [quality_score * 0.7] * len(timestamps),
                "operating_margin": [quality_score * 0.5] * len(timestamps),
                "revenue_growth_yoy": [0.10 if symbol == "AAPL" else 0.02] * len(timestamps),
                "net_income_growth_yoy": [0.12 if symbol == "AAPL" else 0.03] * len(timestamps),
                "debt_to_equity": [0.2 if symbol == "AAPL" else 0.5] * len(timestamps),
                "current_ratio": [2.0 if symbol == "AAPL" else 1.2] * len(timestamps),
                "free_cash_flow_yield": [value_score * 0.5] * len(timestamps),
                "accruals_proxy": [0.05 if symbol == "AAPL" else 0.20] * len(timestamps),
                "fundamental_value_score": [value_score] * len(timestamps),
                "fundamental_quality_score": [quality_score] * len(timestamps),
                "fundamental_growth_score": [0.11 if symbol == "AAPL" else 0.025] * len(timestamps),
                "fundamental_quality_value_score": [(value_score + quality_score) / 2.0] * len(timestamps),
                "sector_neutral_value_score": [value_score - 0.5] * len(timestamps),
                "sector_neutral_quality_score": [quality_score - 0.5] * len(timestamps),
                "sector_neutral_growth_score": [0.05 if symbol == "AAPL" else -0.05] * len(timestamps),
                "sector_neutral_quality_value_score": [0.10 if symbol == "AAPL" else -0.10] * len(timestamps),
                "days_since_available": list(range(len(timestamps))),
            }
        )
        daily_features.append(frame)
    pd.concat(daily_features, ignore_index=True).to_parquet(fundamentals_path, index=False)

    result = run_alpha_research(
        symbols=["AAPL", "MSFT"],
        universe=None,
        feature_dir=feature_dir,
        signal_family="fundamental_value",
        lookbacks=[5],
        horizons=[1],
        min_rows=20,
        top_quantile=0.5,
        bottom_quantile=0.5,
        output_dir=output_dir,
        train_size=20,
        test_size=10,
        step_size=10,
        fundamentals_enabled=True,
        fundamentals_daily_features_path=fundamentals_path,
    )

    leaderboard_df = pd.read_csv(result["leaderboard_path"])
    ic_summary_df = pd.read_csv(result["fundamental_feature_ic_summary_path"])
    diagnostics = json.loads(Path(result["signal_diagnostics_path"]).read_text(encoding="utf-8"))

    assert not leaderboard_df.empty
    assert set(leaderboard_df["signal_family"]) == {"fundamental_value"}
    assert not ic_summary_df.empty
    assert "fundamental_value_score" in set(ic_summary_df["feature_name"])
    assert diagnostics["fundamentals"]["enabled"] is True
    assert diagnostics["fundamentals"]["symbols_with_features"] == 2


def test_run_alpha_research_with_generated_fmp_fundamentals_supports_fundamental_families(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_root = tmp_path / "fundamentals"
    feature_dir = tmp_path / "features"
    output_dir = tmp_path / "alpha_outputs"
    feature_dir.mkdir()

    timestamps = pd.date_range("2024-01-01", periods=120, freq="D")
    for symbol, drift in {"AAPL": 0.003, "MSFT": 0.001}.items():
        closes = [100.0]
        for _ in range(len(timestamps) - 1):
            closes.append(closes[-1] * (1.0 + drift))
        pd.DataFrame(
            {
                "timestamp": timestamps,
                "symbol": [symbol] * len(timestamps),
                "close": closes,
            }
        ).to_parquet(feature_dir / f"{symbol}.parquet", index=False)

    payloads = {}
    payloads.update(_fmp_payloads_for_symbol("AAPL"))
    msft_payloads = _fmp_payloads_for_symbol("MSFT")
    msft_payloads["income-statement?symbol=MSFT&period=annual"][0]["revenue"] = 90.0
    msft_payloads["income-statement?symbol=MSFT&period=annual"][0]["netIncome"] = 16.0
    msft_payloads["income-statement?symbol=MSFT&period=annual"][1]["revenue"] = 85.0
    msft_payloads["income-statement?symbol=MSFT&period=annual"][1]["netIncome"] = 14.0
    msft_payloads["balance-sheet-statement?symbol=MSFT&period=annual"][0]["totalStockholdersEquity"] = 110.0
    msft_payloads["balance-sheet-statement?symbol=MSFT&period=annual"][1]["totalStockholdersEquity"] = 100.0
    payloads.update(msft_payloads)
    _install_fmp_urlopen(monkeypatch, payloads)

    ingest_fundamentals(
        FundamentalsIngestionRequest(
            symbols=["AAPL", "MSFT"],
            artifact_root=artifact_root,
            providers=("vendor",),
            vendor_api_key="test-key",
        )
    )
    feature_result = build_daily_fundamental_features(
        FundamentalFeatureBuildRequest(
            artifact_root=artifact_root,
            calendar_dir=feature_dir,
            symbols=["AAPL", "MSFT"],
        )
    )

    result = run_alpha_research(
        symbols=["AAPL", "MSFT"],
        universe=None,
        feature_dir=feature_dir,
        signal_family="fundamental_quality_value",
        lookbacks=[5],
        horizons=[1],
        min_rows=30,
        top_quantile=0.5,
        bottom_quantile=0.5,
        output_dir=output_dir,
        train_size=40,
        test_size=20,
        step_size=20,
        fundamentals_enabled=True,
        fundamentals_daily_features_path=Path(feature_result["daily_fundamental_features_path"]),
    )

    leaderboard_df = pd.read_csv(result["leaderboard_path"])
    ic_summary_df = pd.read_csv(result["fundamental_feature_ic_summary_path"])

    assert not leaderboard_df.empty
    assert set(leaderboard_df["signal_family"]) == {"fundamental_quality_value"}
    assert not ic_summary_df.empty

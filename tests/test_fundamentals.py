from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.error import HTTPError

import pandas as pd
import pytest

from trading_platform.data.fundamentals.models import CANONICAL_FUNDAMENTAL_METRICS
from trading_platform.data.fundamentals.providers import sec as sec_module
from trading_platform.data.fundamentals.providers.sec import SECFundamentalsProvider
from trading_platform.data.fundamentals.providers import vendor as vendor_module
from trading_platform.data.fundamentals.providers.vendor import VendorFundamentalsProvider
from trading_platform.data.fundamentals.service import (
    FundamentalFeatureBuildRequest,
    FundamentalsIngestionRequest,
    FundamentalsSnapshotBuildRequest,
    build_sec_fundamentals_snapshot,
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


def _install_sec_urlopen(monkeypatch: pytest.MonkeyPatch, payloads: dict[str, Any], *, counter: dict[str, int] | None = None) -> None:
    def _fake_urlopen(request, timeout=30):  # noqa: ANN001
        full_url = request.full_url
        if counter is not None:
            counter["count"] = counter.get("count", 0) + 1
        for expected_fragment, payload in payloads.items():
            if expected_fragment in full_url:
                return _FakeHTTPResponse(payload)
        raise AssertionError(f"Unexpected SEC URL requested during test: {full_url}")

    monkeypatch.setattr(sec_module, "urlopen", _fake_urlopen)


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


def _sec_payloads_for_symbol(symbol: str, cik: str = "320193") -> dict[str, Any]:
    normalized_cik = str(cik).zfill(10)
    accession = "0000320193-24-000001"
    return {
        "company_tickers.json": {
            "0": {
                "cik_str": int(cik),
                "ticker": symbol.upper(),
                "title": f"{symbol.upper()} Inc.",
            }
        },
        f"submissions/CIK{normalized_cik}.json": {
            "cik": cik,
            "name": f"{symbol.upper()} Inc.",
            "tickers": [symbol.upper()],
            "exchanges": ["NASDAQ"],
            "sicDescription": "Electronic Computers",
            "filings": {
                "recent": {
                    "accessionNumber": [accession],
                    "form": ["10-K"],
                    "filingDate": ["2024-11-01"],
                    "acceptanceDateTime": ["2024-11-01T18:05:00.000Z"],
                    "reportDate": ["2024-09-28"],
                }
            },
        },
        f"companyfacts/CIK{normalized_cik}.json": {
            "cik": int(cik),
            "facts": {
                "us-gaap": {
                    "Revenues": {
                        "units": {
                            "USD": [
                                {"val": 100.0, "fy": 2024, "fp": "FY", "end": "2024-09-28", "filed": "2024-11-01", "form": "10-K", "accn": accession}
                            ]
                        }
                    },
                    "GrossProfit": {
                        "units": {
                            "USD": [
                                {"val": 45.0, "fy": 2024, "fp": "FY", "end": "2024-09-28", "filed": "2024-11-01", "form": "10-K", "accn": accession}
                            ]
                        }
                    },
                    "OperatingIncomeLoss": {
                        "units": {
                            "USD": [
                                {"val": 22.0, "fy": 2024, "fp": "FY", "end": "2024-09-28", "filed": "2024-11-01", "form": "10-K", "accn": accession}
                            ]
                        }
                    },
                    "NetIncomeLoss": {
                        "units": {
                            "USD": [
                                {"val": 15.0, "fy": 2024, "fp": "FY", "end": "2024-09-28", "filed": "2024-11-01", "form": "10-K", "accn": accession}
                            ]
                        }
                    },
                    "Assets": {
                        "units": {
                            "USD": [
                                {"val": 210.0, "fy": 2024, "fp": "FY", "end": "2024-09-28", "filed": "2024-11-01", "form": "10-K", "accn": accession}
                            ]
                        }
                    },
                    "Liabilities": {
                        "units": {
                            "USD": [
                                {"val": 90.0, "fy": 2024, "fp": "FY", "end": "2024-09-28", "filed": "2024-11-01", "form": "10-K", "accn": accession}
                            ]
                        }
                    },
                    "StockholdersEquity": {
                        "units": {
                            "USD": [
                                {"val": 120.0, "fy": 2024, "fp": "FY", "end": "2024-09-28", "filed": "2024-11-01", "form": "10-K", "accn": accession}
                            ]
                        }
                    },
                    "CashAndCashEquivalentsAtCarryingValue": {
                        "units": {
                            "USD": [
                                {"val": 30.0, "fy": 2024, "fp": "FY", "end": "2024-09-28", "filed": "2024-11-01", "form": "10-K", "accn": accession}
                            ]
                        }
                    },
                    "AssetsCurrent": {
                        "units": {
                            "USD": [
                                {"val": 85.0, "fy": 2024, "fp": "FY", "end": "2024-09-28", "filed": "2024-11-01", "form": "10-K", "accn": accession}
                            ]
                        }
                    },
                    "LiabilitiesCurrent": {
                        "units": {
                            "USD": [
                                {"val": 35.0, "fy": 2024, "fp": "FY", "end": "2024-09-28", "filed": "2024-11-01", "form": "10-K", "accn": accession}
                            ]
                        }
                    },
                    "LongTermDebt": {
                        "units": {
                            "USD": [
                                {"val": 25.0, "fy": 2024, "fp": "FY", "end": "2024-09-28", "filed": "2024-11-01", "form": "10-K", "accn": accession}
                            ]
                        }
                    },
                    "NetCashProvidedByUsedInOperatingActivities": {
                        "units": {
                            "USD": [
                                {"val": 20.0, "fy": 2024, "fp": "FY", "end": "2024-09-28", "filed": "2024-11-01", "form": "10-K", "accn": accession}
                            ]
                        }
                    },
                    "PaymentsToAcquirePropertyPlantAndEquipment": {
                        "units": {
                            "USD": [
                                {"val": 5.0, "fy": 2024, "fp": "FY", "end": "2024-09-28", "filed": "2024-11-01", "form": "10-K", "accn": accession}
                            ]
                        }
                    },
                },
                "dei": {
                    "EntityCommonStockSharesOutstanding": {
                        "units": {
                            "shares": [
                                {"val": 10.0, "fy": 2024, "fp": "FY", "end": "2024-09-28", "filed": "2024-11-01", "form": "10-K", "accn": accession}
                            ]
                        }
                    }
                },
            },
        },
    }


def _long_fundamental_values(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    metadata_columns = [
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
    metric_columns = [column for column in CANONICAL_FUNDAMENTAL_METRICS if column in frame.columns]
    return (
        frame.melt(
            id_vars=metadata_columns,
            value_vars=metric_columns,
            var_name="metric_name",
            value_name="metric_value",
        )
        .dropna(subset=["metric_value"])
        .reset_index(drop=True)
    )


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
    free_cash_flow_rows = result.fundamental_values_df.loc[result.fundamental_values_df["metric_name"] == "free_cash_flow"]
    assert not free_cash_flow_rows.empty
    assert free_cash_flow_rows.iloc[0]["metric_value"] == 25.0


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
    revenue_rows = result.fundamental_values_df.loc[result.fundamental_values_df["metric_name"] == "revenue"]
    net_income_rows = result.fundamental_values_df.loc[result.fundamental_values_df["metric_name"] == "net_income"]
    assert revenue_rows.iloc[0]["metric_value"] == 100.0
    assert net_income_rows.iloc[0]["metric_value"] == 22.0
    assert result.filing_metadata_df.loc[0, "available_date"] == "2024-11-01"


def test_sec_snapshot_build_fetches_caches_and_normalizes_from_mocked_sec_responses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_root = tmp_path / "fundamentals"
    calendar_dir = tmp_path / "features"
    calendar_dir.mkdir()
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-10-28", periods=10, freq="D"),
            "symbol": ["AAPL"] * 10,
            "close": [20.0] * 10,
        }
    ).to_parquet(calendar_dir / "AAPL.parquet", index=False)

    _install_sec_urlopen(monkeypatch, _sec_payloads_for_symbol("AAPL"))
    result = build_sec_fundamentals_snapshot(
        FundamentalsSnapshotBuildRequest(
            symbols=["AAPL"],
            artifact_root=artifact_root,
            calendar_dir=calendar_dir,
        )
    )

    values_df = pd.read_parquet(result["fundamental_values_path"])
    coverage_df = pd.read_csv(result["fundamental_feature_coverage_path"])
    summary = json.loads(Path(result["fundamental_summary_path"]).read_text(encoding="utf-8"))

    assert not values_df.empty
    assert {"metric_name", "metric_value"}.issubset(values_df.columns)
    assert "revenue" in set(values_df["metric_name"])
    assert summary["symbols_resolved_to_cik"] == ["AAPL"]
    assert summary["symbols_fetched"] == ["AAPL"]
    assert summary["metrics_by_name"]["revenue"] >= 1
    assert coverage_df.loc[coverage_df["feature_name"] == "earnings_yield", "non_null_rows"].iloc[0] > 0
    assert Path(result["raw_sec_cache_root"]).joinpath("submissions", "CIK0000320193.json").exists()
    assert Path(result["raw_sec_cache_root"]).joinpath("companyfacts", "CIK0000320193.json").exists()
    assert Path(result["sec_symbol_cik_map_path"]).exists()


def test_sec_snapshot_build_offline_rebuild_uses_cached_raw_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    artifact_root = tmp_path / "fundamentals"
    cache_root = artifact_root / "raw_sec"
    submissions_root = cache_root / "submissions"
    companyfacts_root = cache_root / "companyfacts"
    submissions_root.mkdir(parents=True)
    companyfacts_root.mkdir(parents=True)
    symbol_map_path = artifact_root / "sec_symbol_cik_map.parquet"
    pd.DataFrame(
        [{"symbol": "AAPL", "cik": "0000320193", "company_name": "AAPL Inc.", "source": "sec:ticker_map"}]
    ).to_parquet(symbol_map_path, index=False)
    payloads = _sec_payloads_for_symbol("AAPL")
    Path(submissions_root / "CIK0000320193.json").write_text(
        json.dumps(payloads["submissions/CIK0000320193.json"], indent=2),
        encoding="utf-8",
    )
    Path(companyfacts_root / "CIK0000320193.json").write_text(
        json.dumps(payloads["companyfacts/CIK0000320193.json"], indent=2),
        encoding="utf-8",
    )
    calendar_dir = tmp_path / "features"
    calendar_dir.mkdir()
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-10-28", periods=10, freq="D"),
            "symbol": ["AAPL"] * 10,
            "close": [20.0] * 10,
        }
    ).to_parquet(calendar_dir / "AAPL.parquet", index=False)

    monkeypatch.setattr(sec_module, "urlopen", lambda request, timeout=30: (_ for _ in ()).throw(AssertionError("network not allowed")))
    result = build_sec_fundamentals_snapshot(
        FundamentalsSnapshotBuildRequest(
            symbols=["AAPL"],
            artifact_root=artifact_root,
            raw_sec_cache_root=cache_root,
            symbol_cik_map_path=symbol_map_path,
            calendar_dir=calendar_dir,
            offline=True,
        )
    )
    values_df = pd.read_parquet(result["fundamental_values_path"])
    daily_df = pd.read_parquet(result["daily_fundamental_features_path"])

    assert not values_df.empty
    assert not daily_df.empty
    assert daily_df["earnings_yield"].notna().any()


def test_sec_provider_retries_http_429_and_reports_rate_limit_diagnostics(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payloads = _sec_payloads_for_symbol("AAPL")
    call_counts: dict[str, int] = {}
    sleep_calls: list[float] = []

    def _retrying_urlopen(request, timeout=30):  # noqa: ANN001
        full_url = request.full_url
        for expected_fragment, payload in payloads.items():
            if expected_fragment in full_url:
                call_counts[expected_fragment] = call_counts.get(expected_fragment, 0) + 1
                if expected_fragment == "company_tickers.json" and call_counts[expected_fragment] == 1:
                    raise HTTPError(full_url, 429, "Too Many Requests", hdrs=None, fp=None)
                return _FakeHTTPResponse(payload)
        raise AssertionError(f"Unexpected SEC URL requested during test: {full_url}")

    monkeypatch.setattr(sec_module, "urlopen", _retrying_urlopen)
    monkeypatch.setattr(sec_module.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    provider = SECFundamentalsProvider(
        cache_root=tmp_path / "raw_sec",
        symbol_cik_map_path=tmp_path / "sec_symbol_cik_map.parquet",
        request_delay_seconds=0.0,
        max_retries=2,
    )
    result = provider.fetch(symbols=["AAPL"])

    assert result.diagnostics["status"] == "ok"
    assert result.diagnostics["retry_count"] == 1
    assert result.diagnostics["rate_limit_error_count"] == 1
    assert sleep_calls
    assert not result.fundamental_values_df.empty


def test_sec_provider_reuses_fresh_cached_raw_files_without_network(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payloads = _sec_payloads_for_symbol("AAPL")
    counter = {"count": 0}
    _install_sec_urlopen(monkeypatch, payloads, counter=counter)

    provider = SECFundamentalsProvider(
        cache_root=tmp_path / "raw_sec",
        symbol_cik_map_path=tmp_path / "sec_symbol_cik_map.parquet",
        request_delay_seconds=0.0,
    )
    first = provider.fetch(symbols=["AAPL"])
    assert counter["count"] > 0
    assert first.diagnostics["cache_misses"] > 0

    counter["count"] = 0
    monkeypatch.setattr(sec_module, "urlopen", lambda request, timeout=30: (_ for _ in ()).throw(AssertionError("network not expected")))
    second = SECFundamentalsProvider(
        cache_root=tmp_path / "raw_sec",
        symbol_cik_map_path=tmp_path / "sec_symbol_cik_map.parquet",
        request_delay_seconds=0.0,
    ).fetch(symbols=["AAPL"])

    assert counter["count"] == 0
    assert second.diagnostics["cache_hits"] > 0
    assert second.diagnostics["symbols_skipped_from_cache"] == ["AAPL"]
    assert not second.fundamental_values_df.empty


def test_sec_snapshot_build_respects_max_symbols_per_run_incrementally(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_root = tmp_path / "fundamentals"
    payloads = {}
    payloads.update(_sec_payloads_for_symbol("AAPL", "320193"))
    msft_payloads = _sec_payloads_for_symbol("MSFT", "789019")
    payloads.update(msft_payloads)
    payloads["company_tickers.json"] = {
        "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
        "1": {"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft Corp."},
    }
    _install_sec_urlopen(monkeypatch, payloads)

    result = build_sec_fundamentals_snapshot(
        FundamentalsSnapshotBuildRequest(
            symbols=["AAPL", "MSFT"],
            artifact_root=artifact_root,
            build_daily_features=False,
            max_symbols_per_run=1,
        )
    )
    summary = json.loads(Path(result["fundamental_summary_path"]).read_text(encoding="utf-8"))

    assert summary["provider_diagnostics"][0]["skipped_due_limit"] == ["MSFT"]
    assert summary["symbols_fetched"] == ["AAPL"]

def test_vendor_provider_fetches_and_normalizes_fmp_responses(monkeypatch: pytest.MonkeyPatch) -> None:
    payloads = _fmp_payloads_for_symbol("AAPL")
    _install_fmp_urlopen(monkeypatch, payloads)

    result = VendorFundamentalsProvider(api_key="test-key").fetch(symbols=["AAPL"])

    assert result.diagnostics["status"] == "ok"
    assert result.diagnostics["mode"] == "fmp"
    assert result.company_master_df.loc[0, "symbol"] == "AAPL"
    assert result.company_master_df.loc[0, "source"] == "vendor:fmp"
    assert not result.fundamental_values_df.empty
    latest_rows = result.fundamental_values_df.loc[result.fundamental_values_df["period_end_date"] == "2024-12-31"]
    assert latest_rows.loc[latest_rows["metric_name"] == "revenue", "metric_value"].iloc[0] == 120.0
    assert latest_rows.loc[latest_rows["metric_name"] == "shareholders_equity", "metric_value"].iloc[0] == 150.0
    assert latest_rows.loc[latest_rows["metric_name"] == "operating_cash_flow", "metric_value"].iloc[0] == 32.0
    assert latest_rows.loc[latest_rows["metric_name"] == "free_cash_flow", "metric_value"].iloc[0] == 24.0
    assert latest_rows["available_date"].iloc[0] == "2025-02-20"


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


def test_vendor_provider_uses_fresh_cache_and_skips_network_fetch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payloads = _fmp_payloads_for_symbol("AAPL")
    request_count = {"count": 0}

    def _counting_urlopen(request, timeout=30):  # noqa: ANN001
        request_count["count"] += 1
        full_url = request.full_url
        for expected_fragment, payload in payloads.items():
            if expected_fragment in full_url:
                return _FakeHTTPResponse(payload)
        raise AssertionError(f"Unexpected FMP URL requested during test: {full_url}")

    monkeypatch.setattr(vendor_module, "urlopen", _counting_urlopen)

    cache_root = tmp_path / "raw_fmp"
    first = VendorFundamentalsProvider(
        api_key="test-key",
        cache_enabled=True,
        cache_root=cache_root,
        request_delay_seconds=0.0,
    ).fetch(symbols=["AAPL"])

    assert first.diagnostics["cache_misses"] > 0
    assert first.diagnostics["symbols_fetched"] == ["AAPL"]
    assert request_count["count"] > 0

    request_count["count"] = 0
    second = VendorFundamentalsProvider(
        api_key="test-key",
        cache_enabled=True,
        cache_root=cache_root,
        request_delay_seconds=0.0,
    ).fetch(symbols=["AAPL"])

    assert second.diagnostics["cache_hits"] > 0
    assert second.diagnostics["symbols_skipped_from_cache"] == ["AAPL"]
    assert request_count["count"] == 0


def test_vendor_provider_retries_http_429_then_succeeds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payloads = _fmp_payloads_for_symbol("AAPL")
    call_counts: dict[str, int] = {}
    sleep_calls: list[float] = []

    def _retrying_urlopen(request, timeout=30):  # noqa: ANN001
        full_url = request.full_url
        for expected_fragment, payload in payloads.items():
            if expected_fragment in full_url:
                call_counts[expected_fragment] = call_counts.get(expected_fragment, 0) + 1
                if expected_fragment == "profile?symbol=AAPL" and call_counts[expected_fragment] == 1:
                    raise HTTPError(full_url, 429, "Too Many Requests", hdrs=None, fp=None)
                return _FakeHTTPResponse(payload)
        raise AssertionError(f"Unexpected FMP URL requested during test: {full_url}")

    monkeypatch.setattr(vendor_module, "urlopen", _retrying_urlopen)
    monkeypatch.setattr(vendor_module.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    result = VendorFundamentalsProvider(
        api_key="test-key",
        cache_enabled=False,
        request_delay_seconds=0.0,
        max_retries=2,
    ).fetch(symbols=["AAPL"])

    assert result.diagnostics["status"] == "ok"
    assert result.diagnostics["retry_count"] == 1
    assert result.diagnostics["rate_limit_error_count"] == 1
    assert sleep_calls
    assert not result.fundamental_values_df.empty


def test_ingest_fundamentals_raises_when_fmp_retries_exhausted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def _always_rate_limited(request, timeout=30):  # noqa: ANN001
        raise HTTPError(request.full_url, 429, "Too Many Requests", hdrs=None, fp=None)

    monkeypatch.setattr(vendor_module, "urlopen", _always_rate_limited)
    monkeypatch.setattr(vendor_module.time, "sleep", lambda seconds: None)

    with pytest.raises(RuntimeError, match="retries were exhausted"):
        ingest_fundamentals(
            FundamentalsIngestionRequest(
                symbols=["AAPL"],
                artifact_root=tmp_path / "fundamentals",
                providers=("vendor",),
                vendor_api_key="test-key",
                vendor_cache_enabled=False,
                vendor_request_delay_seconds=0.0,
                vendor_max_retries=1,
            )
        )


def test_ingest_fundamentals_summary_reports_cache_and_retry_diagnostics(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    artifact_root = tmp_path / "fundamentals"
    calendar_dir = tmp_path / "features"
    calendar_dir.mkdir()
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-02-10", periods=30, freq="D"),
            "symbol": ["AAPL"] * 30,
            "close": [20.0] * 30,
        }
    ).to_parquet(calendar_dir / "AAPL.parquet", index=False)

    payloads = _fmp_payloads_for_symbol("AAPL")
    _install_fmp_urlopen(monkeypatch, payloads)
    ingest_result = ingest_fundamentals(
        FundamentalsIngestionRequest(
            symbols=["AAPL"],
            artifact_root=artifact_root,
            providers=("vendor",),
            vendor_api_key="test-key",
            vendor_cache_enabled=True,
            vendor_cache_root=artifact_root / "raw_fmp",
            vendor_request_delay_seconds=0.0,
            vendor_max_symbols_per_run=1,
        )
    )
    build_daily_fundamental_features(
        FundamentalFeatureBuildRequest(
            artifact_root=artifact_root,
            calendar_dir=calendar_dir,
            symbols=["AAPL"],
        )
    )
    summary = json.loads(Path(ingest_result["fundamental_summary_path"]).read_text(encoding="utf-8"))

    assert summary["cache_hits"] == 0
    assert summary["cache_misses"] > 0
    assert summary["retry_count"] == 0
    assert summary["rate_limit_error_count"] == 0
    assert summary["symbols_fetched"] == ["AAPL"]
    assert summary["symbols_skipped_from_cache"] == []
    assert summary["symbols_failed"] == []


def test_build_daily_fundamental_features_respects_available_date_and_forward_fill(tmp_path: Path) -> None:
    artifact_root = tmp_path / "fundamentals"
    calendar_dir = tmp_path / "features"
    artifact_root.mkdir()
    calendar_dir.mkdir()

    pd.DataFrame(
        [{"symbol": "AAPL", "sector": "Technology", "industry": "Hardware"}]
    ).to_parquet(artifact_root / "company_master.parquet", index=False)
    _long_fundamental_values(
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


def test_build_daily_fundamental_features_adds_cross_sectional_rank_pct_and_zscore(tmp_path: Path) -> None:
    artifact_root = tmp_path / "fundamentals"
    calendar_dir = tmp_path / "features"
    artifact_root.mkdir()
    calendar_dir.mkdir()

    pd.DataFrame(
        [
            {"symbol": "AAPL", "sector": "Technology", "industry": "Hardware"},
            {"symbol": "MSFT", "sector": "Technology", "industry": "Software"},
            {"symbol": "NVDA", "sector": "Technology", "industry": "Semiconductors"},
        ]
    ).to_parquet(artifact_root / "company_master.parquet", index=False)
    _long_fundamental_values(
        [
            {
                "symbol": symbol,
                "cik": f"0000{index}",
                "fiscal_year": 2023,
                "fiscal_period": "FY",
                "period_type": "annual",
                "period_end_date": "2023-12-31",
                "filing_date": "2024-02-15",
                "available_date": "2024-02-15",
                "form_type": "10-K",
                "accession_number": f"accn-{symbol}",
                "source": "vendor:fmp",
                "revenue": revenue,
                "gross_profit": revenue * 0.4,
                "operating_income": revenue * 0.2,
                "net_income": net_income,
                "total_assets": assets,
                "total_liabilities": liabilities,
                "shareholders_equity": equity,
                "cash_and_equivalents": 25.0 + index,
                "current_assets": 90.0 + index,
                "current_liabilities": 30.0,
                "long_term_debt": 20.0 + index,
                "operating_cash_flow": net_income + 4.0,
                "capital_expenditures": 4.0,
                "free_cash_flow": net_income,
                "shares_outstanding": 10.0,
            }
            for index, (symbol, revenue, net_income, assets, liabilities, equity) in enumerate(
                (
                    ("AAPL", 100.0, 10.0, 200.0, 80.0, 120.0),
                    ("MSFT", 120.0, 20.0, 240.0, 90.0, 150.0),
                    ("NVDA", 140.0, 30.0, 280.0, 100.0, 180.0),
                ),
                start=1,
            )
        ]
    ).to_parquet(artifact_root / "fundamental_values.parquet", index=False)
    for symbol in ("AAPL", "MSFT", "NVDA"):
        pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-02-15", periods=3, freq="D"),
                "symbol": [symbol] * 3,
                "close": [20.0] * 3,
            }
        ).to_parquet(calendar_dir / f"{symbol}.parquet", index=False)

    result = build_daily_fundamental_features(
        FundamentalFeatureBuildRequest(
            artifact_root=artifact_root,
            calendar_dir=calendar_dir,
            symbols=["AAPL", "MSFT", "NVDA"],
        )
    )
    daily_df = pd.read_parquet(result["daily_fundamental_features_path"])
    snapshot = daily_df.loc[daily_df["timestamp"] == pd.Timestamp("2024-02-15")].sort_values("symbol").reset_index(drop=True)

    assert "earnings_yield_rank_pct" in daily_df.columns
    assert "earnings_yield_zscore" in daily_df.columns
    assert snapshot["earnings_yield_rank_pct"].tolist() == [1.0 / 3.0, 2.0 / 3.0, 1.0]
    assert snapshot["fundamental_value_score_rank_pct"].nunique(dropna=True) == 3
    assert abs(snapshot["earnings_yield_zscore"].mean()) < 1e-9


def test_cross_sectional_fundamental_transforms_drop_constant_dates(tmp_path: Path) -> None:
    artifact_root = tmp_path / "fundamentals"
    calendar_dir = tmp_path / "features"
    artifact_root.mkdir()
    calendar_dir.mkdir()

    pd.DataFrame(
        [
            {"symbol": "AAPL", "sector": "Technology", "industry": "Hardware"},
            {"symbol": "MSFT", "sector": "Technology", "industry": "Software"},
        ]
    ).to_parquet(artifact_root / "company_master.parquet", index=False)
    _long_fundamental_values(
        [
            {
                "symbol": symbol,
                "cik": f"0000{index}",
                "fiscal_year": 2023,
                "fiscal_period": "FY",
                "period_type": "annual",
                "period_end_date": "2023-12-31",
                "filing_date": "2024-02-15",
                "available_date": "2024-02-15",
                "form_type": "10-K",
                "accession_number": f"accn-{symbol}",
                "source": "vendor:fmp",
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
            }
            for index, symbol in enumerate(("AAPL", "MSFT"), start=1)
        ]
    ).to_parquet(artifact_root / "fundamental_values.parquet", index=False)
    for symbol in ("AAPL", "MSFT"):
        pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-02-15", periods=2, freq="D"),
                "symbol": [symbol] * 2,
                "close": [20.0] * 2,
            }
        ).to_parquet(calendar_dir / f"{symbol}.parquet", index=False)

    result = build_daily_fundamental_features(
        FundamentalFeatureBuildRequest(
            artifact_root=artifact_root,
            calendar_dir=calendar_dir,
            symbols=["AAPL", "MSFT"],
        )
    )
    daily_df = pd.read_parquet(result["daily_fundamental_features_path"])

    assert daily_df["earnings_yield_rank_pct"].isna().all()
    assert daily_df["earnings_yield_zscore"].isna().all()


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
    values_df = pd.read_parquet(ingest_result["fundamental_values_path"])
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
    assert not values_df.empty
    assert set(values_df.columns) >= {"metric_name", "metric_value", "available_date", "period_end_date"}
    assert "revenue" in set(values_df["metric_name"])
    assert summary["metric_row_count"] == len(values_df)
    assert summary["metrics_by_name"]["revenue"] >= 1
    assert summary["symbols_with_metric_coverage"] == ["AAPL"]
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
    values_df = pd.read_parquet(ingest_result["fundamental_values_path"])

    assert Path(ingest_result["company_master_path"]).exists()
    assert Path(ingest_result["fundamental_filings_path"]).exists()
    assert Path(ingest_result["fundamental_values_path"]).exists()
    assert Path(feature_result["daily_fundamental_features_path"]).exists()
    assert Path(feature_result["fundamental_feature_coverage_path"]).exists()
    assert Path(feature_result["fundamental_lag_audit_path"]).exists()
    assert summary["company_count"] == 1
    assert not values_df.empty
    assert "metric_name" in values_df.columns
    assert "metric_value" in values_df.columns
    assert summary["daily_feature_build"]["daily_feature_rows"] == 10


def test_run_alpha_research_integrates_fundamental_features_without_breaking_flow(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    output_dir = tmp_path / "alpha_outputs"
    fundamentals_path = tmp_path / "daily_fundamental_features.parquet"
    feature_dir.mkdir()

    timestamps = pd.date_range("2024-01-01", periods=60, freq="D")
    for symbol, drift in {"AAPL": 0.003, "MSFT": 0.001, "NVDA": 0.004}.items():
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
                "earnings_yield_rank_pct": [1.0 if symbol == "AAPL" else 0.5] * len(timestamps),
                "earnings_yield_zscore": [1.0 if symbol == "AAPL" else -1.0] * len(timestamps),
                "book_to_market_rank_pct": [1.0 if symbol == "AAPL" else 0.5] * len(timestamps),
                "book_to_market_zscore": [1.0 if symbol == "AAPL" else -1.0] * len(timestamps),
                "sales_to_price_rank_pct": [1.0 if symbol == "AAPL" else 0.5] * len(timestamps),
                "sales_to_price_zscore": [1.0 if symbol == "AAPL" else -1.0] * len(timestamps),
                "roe_rank_pct": [1.0 if symbol == "AAPL" else 0.5] * len(timestamps),
                "roe_zscore": [1.0 if symbol == "AAPL" else -1.0] * len(timestamps),
                "roa_rank_pct": [1.0 if symbol == "AAPL" else 0.5] * len(timestamps),
                "roa_zscore": [1.0 if symbol == "AAPL" else -1.0] * len(timestamps),
                "gross_margin_rank_pct": [1.0 if symbol == "AAPL" else 0.5] * len(timestamps),
                "gross_margin_zscore": [1.0 if symbol == "AAPL" else -1.0] * len(timestamps),
                "operating_margin_rank_pct": [1.0 if symbol == "AAPL" else 0.5] * len(timestamps),
                "operating_margin_zscore": [1.0 if symbol == "AAPL" else -1.0] * len(timestamps),
                "revenue_growth_yoy_rank_pct": [1.0 if symbol == "AAPL" else 0.5] * len(timestamps),
                "revenue_growth_yoy_zscore": [1.0 if symbol == "AAPL" else -1.0] * len(timestamps),
                "net_income_growth_yoy_rank_pct": [1.0 if symbol == "AAPL" else 0.5] * len(timestamps),
                "net_income_growth_yoy_zscore": [1.0 if symbol == "AAPL" else -1.0] * len(timestamps),
                "debt_to_equity_rank_pct": [0.5 if symbol == "AAPL" else 1.0] * len(timestamps),
                "debt_to_equity_zscore": [-1.0 if symbol == "AAPL" else 1.0] * len(timestamps),
                "current_ratio_rank_pct": [1.0 if symbol == "AAPL" else 0.5] * len(timestamps),
                "current_ratio_zscore": [1.0 if symbol == "AAPL" else -1.0] * len(timestamps),
                "free_cash_flow_yield_rank_pct": [1.0 if symbol == "AAPL" else 0.5] * len(timestamps),
                "free_cash_flow_yield_zscore": [1.0 if symbol == "AAPL" else -1.0] * len(timestamps),
                "accruals_proxy_rank_pct": [0.5 if symbol == "AAPL" else 1.0] * len(timestamps),
                "accruals_proxy_zscore": [-1.0 if symbol == "AAPL" else 1.0] * len(timestamps),
                "fundamental_value_score_rank_pct": [1.0 if symbol == "AAPL" else 0.5] * len(timestamps),
                "fundamental_value_score_zscore": [1.0 if symbol == "AAPL" else -1.0] * len(timestamps),
                "fundamental_quality_score_rank_pct": [1.0 if symbol == "AAPL" else 0.5] * len(timestamps),
                "fundamental_quality_score_zscore": [1.0 if symbol == "AAPL" else -1.0] * len(timestamps),
                "fundamental_growth_score_rank_pct": [1.0 if symbol == "AAPL" else 0.5] * len(timestamps),
                "fundamental_growth_score_zscore": [1.0 if symbol == "AAPL" else -1.0] * len(timestamps),
                "fundamental_quality_value_score_rank_pct": [1.0 if symbol == "AAPL" else 0.5] * len(timestamps),
                "fundamental_quality_value_score_zscore": [1.0 if symbol == "AAPL" else -1.0] * len(timestamps),
                "sector_neutral_value_score_rank_pct": [1.0 if symbol == "AAPL" else 0.5] * len(timestamps),
                "sector_neutral_value_score_zscore": [1.0 if symbol == "AAPL" else -1.0] * len(timestamps),
                "sector_neutral_quality_score_rank_pct": [1.0 if symbol == "AAPL" else 0.5] * len(timestamps),
                "sector_neutral_quality_score_zscore": [1.0 if symbol == "AAPL" else -1.0] * len(timestamps),
                "sector_neutral_growth_score_rank_pct": [1.0 if symbol == "AAPL" else 0.5] * len(timestamps),
                "sector_neutral_growth_score_zscore": [1.0 if symbol == "AAPL" else -1.0] * len(timestamps),
                "sector_neutral_quality_value_score_rank_pct": [1.0 if symbol == "AAPL" else 0.5] * len(timestamps),
                "sector_neutral_quality_value_score_zscore": [1.0 if symbol == "AAPL" else -1.0] * len(timestamps),
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
    assert "fundamental_value_score_rank_pct" in set(ic_summary_df["feature_name"])
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
    for symbol, drift in {"AAPL": 0.003, "MSFT": 0.001, "NVDA": 0.004}.items():
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
    nvda_payloads = _fmp_payloads_for_symbol("NVDA")
    nvda_payloads["income-statement?symbol=NVDA&period=annual"][0]["revenue"] = 160.0
    nvda_payloads["income-statement?symbol=NVDA&period=annual"][0]["netIncome"] = 28.0
    nvda_payloads["income-statement?symbol=NVDA&period=annual"][1]["revenue"] = 120.0
    nvda_payloads["income-statement?symbol=NVDA&period=annual"][1]["netIncome"] = 18.0
    nvda_payloads["balance-sheet-statement?symbol=NVDA&period=annual"][0]["totalStockholdersEquity"] = 175.0
    nvda_payloads["balance-sheet-statement?symbol=NVDA&period=annual"][1]["totalStockholdersEquity"] = 140.0
    payloads.update(nvda_payloads)
    _install_fmp_urlopen(monkeypatch, payloads)

    ingest_fundamentals(
        FundamentalsIngestionRequest(
            symbols=["AAPL", "MSFT", "NVDA"],
            artifact_root=artifact_root,
            providers=("vendor",),
            vendor_api_key="test-key",
        )
    )
    feature_result = build_daily_fundamental_features(
        FundamentalFeatureBuildRequest(
            artifact_root=artifact_root,
            calendar_dir=feature_dir,
            symbols=["AAPL", "MSFT", "NVDA"],
        )
    )

    result = run_alpha_research(
        symbols=["AAPL", "MSFT", "NVDA"],
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
    transformed_rows = ic_summary_df.loc[ic_summary_df["feature_name"] == "fundamental_quality_value_score_rank_pct"]
    assert not transformed_rows.empty
    assert transformed_rows["spearman_ic"].notna().any()

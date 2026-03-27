from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from trading_platform.integrations.optional_dependencies import OptionalDependencyError, require_dependency
from trading_platform.integrations.quantstats_adapter import write_quantstats_report
from trading_platform.portfolio.optimizer_adapters import (
    PortfolioOptimizerPolicyConfig,
    run_optimizer_experiment,
)
from trading_platform.reference.classification_service import build_classification_artifacts, build_symbol_group_map
from trading_platform.research.backtester_validation import (
    run_vectorbt_validation_harness,
    write_vectorbt_validation_artifacts,
)
from trading_platform.research.external_diagnostics import maybe_run_alphalens_diagnostics


def test_require_dependency_raises_actionable_error(monkeypatch) -> None:
    monkeypatch.setattr("importlib.import_module", lambda package_name: (_ for _ in ()).throw(ImportError("missing")))

    with pytest.raises(OptionalDependencyError) as exc_info:
        require_dependency("quantstats", purpose="testing")

    assert "trading-platform[research_diagnostics]" in str(exc_info.value)


def test_build_classification_artifacts_normalizes_financedatabase_output(tmp_path: Path) -> None:
    class _Selector:
        def __init__(self, frame: pd.DataFrame) -> None:
            self.data = frame

    fake_package = SimpleNamespace(
        Equities=lambda: _Selector(
            pd.DataFrame(
                [
                    {
                        "symbol": "AAPL",
                        "name": "Apple Inc.",
                        "sector": "Technology",
                        "industry_group": "Hardware",
                        "exchange": "NASDAQ",
                        "country": "United States",
                        "currency": "USD",
                    }
                ]
            )
        ),
        Etfs=lambda: _Selector(
            pd.DataFrame(
                [
                    {
                        "symbol": "SPY",
                        "name": "SPDR S&P 500 ETF",
                        "category": "Large Blend",
                        "exchange": "NYSE",
                        "country": "United States",
                        "currency": "USD",
                        "is_etf": True,
                    }
                ]
            )
        ),
    )

    paths = build_classification_artifacts(
        symbols=["AAPL", "SPY"],
        output_dir=tmp_path,
        as_of_date="2026-03-27",
        package_override=fake_package,
    )

    security_master = pd.read_csv(paths["security_master_path"])
    assert {"symbol", "asset_type", "sector", "source", "as_of_date"}.issubset(security_master.columns)
    assert set(security_master["symbol"]) == {"AAPL", "SPY"}
    assert (
        build_symbol_group_map(security_master_path=paths["security_master_path"], level="sector")["AAPL"]
        == "Technology"
    )
    summary = json.loads(Path(paths["classification_summary_path"]).read_text(encoding="utf-8"))
    assert summary["point_in_time_warning"]
    assert summary["matched_symbol_count"] == 2
    assert summary["asset_type_counts"]["equity"] == 1


def test_maybe_run_alphalens_diagnostics_writes_expected_artifacts(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    feature_dir.mkdir()
    leaderboard_path = tmp_path / "leaderboard.csv"
    pd.DataFrame(
        [
            {
                "signal_family": "momentum",
                "lookback": 5,
                "signal_variant": "base",
                "variant_parameters_json": "{}",
                "mean_spearman_ic": 0.05,
            }
        ]
    ).to_csv(leaderboard_path, index=False)
    feature_frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=12, freq="B"),
            "close": [100 + i for i in range(12)],
        }
    )
    feature_frame.to_parquet(feature_dir / "AAPL.parquet")
    feature_frame.assign(close=[200 + i for i in range(12)]).to_parquet(feature_dir / "MSFT.parquet")

    fake_alphalens = SimpleNamespace(
        utils=SimpleNamespace(
            get_clean_factor_and_forward_returns=lambda **kwargs: pd.DataFrame(
                {
                    "factor": [1.0, 2.0],
                    "factor_quantile": [1, 5],
                    "1D": [0.01, 0.02],
                    "group": ["Technology", "Technology"],
                },
                index=pd.MultiIndex.from_tuples(
                    [(pd.Timestamp("2025-01-10"), "AAPL"), (pd.Timestamp("2025-01-13"), "AAPL")],
                    names=["date", "asset"],
                ),
            )
        ),
        performance=SimpleNamespace(
            factor_information_coefficient=lambda factor_data: pd.DataFrame({"1D": [0.12, 0.08]}),
            mean_return_by_quantile=lambda factor_data, by_date=False: (
                pd.DataFrame({"1D": [0.01, 0.03]}, index=pd.Index([1, 5], name="factor_quantile")),
                None,
            ),
            quantile_turnover=lambda quantiles, quantile, period: pd.Series([0.1, 0.2]),
        ),
    )

    paths = maybe_run_alphalens_diagnostics(
        enabled=True,
        feature_dir=feature_dir,
        leaderboard_path=leaderboard_path,
        output_dir=tmp_path / "alphalens",
        symbols=["AAPL", "MSFT"],
        signal_composition_preset="standard",
        enable_context_confirmations=None,
        enable_relative_features=None,
        enable_flow_confirmations=None,
        package_override=fake_alphalens,
    )

    assert Path(paths["alphalens_ic_summary_path"]).exists()
    assert Path(paths["alphalens_quantile_returns_path"]).exists()
    assert Path(paths["alphalens_turnover_path"]).exists()


def test_quantstats_adapter_writes_metrics_with_fake_package(tmp_path: Path) -> None:
    class _Reports:
        @staticmethod
        def metrics(returns, benchmark=None, mode="basic", display=False):
            return pd.DataFrame({"Strategy": [1.5, 0.12]}, index=["Sharpe", "CAGR"])

        @staticmethod
        def html(returns, benchmark=None, output=None, title=None):
            Path(output).write_text("<html>ok</html>", encoding="utf-8")

    bundle = write_quantstats_report(
        returns=pd.Series(
            [0.01, -0.005, 0.02],
            index=pd.date_range("2025-01-01", periods=3, freq="B"),
        ),
        output_dir=tmp_path,
        package_override=SimpleNamespace(reports=_Reports()),
    )

    assert bundle.metrics_json_path.exists()
    assert bundle.summary_csv_path.exists()
    assert bundle.tearsheet_html_path is not None and bundle.tearsheet_html_path.exists()
    summary = pd.read_csv(bundle.summary_csv_path)
    assert {"metric", "Strategy"}.issubset(summary.columns)
    assert set(summary["metric"]) == {"Sharpe", "CAGR"}


def test_optimizer_experiment_uses_optimizer_and_comparison() -> None:
    returns_frame = pd.DataFrame(
        {
            "AAPL": [0.01, 0.02, -0.01, 0.005],
            "MSFT": [0.015, 0.01, -0.005, 0.004],
        },
        index=pd.date_range("2025-01-01", periods=4, freq="B"),
    )

    class _ExpectedReturns:
        @staticmethod
        def mean_historical_return(returns, returns_data=True):
            return pd.Series({"AAPL": 0.1, "MSFT": 0.08})

    class _RiskModels:
        @staticmethod
        def sample_cov(returns, returns_data=True):
            return pd.DataFrame([[0.1, 0.01], [0.01, 0.08]], index=["AAPL", "MSFT"], columns=["AAPL", "MSFT"])

    class _EfficientFrontier:
        def __init__(self, expected_returns, cov_matrix):
            pass

        def min_volatility(self):
            return {"AAPL": 0.4, "MSFT": 0.6}

        def clean_weights(self):
            return {"AAPL": 0.4, "MSFT": 0.6}

        def portfolio_performance(self, verbose=False, risk_free_rate=0.0):
            return (0.09, 0.11, 0.8)

    fake_package = SimpleNamespace(
        expected_returns=_ExpectedReturns(),
        risk_models=_RiskModels(),
        efficient_frontier=SimpleNamespace(EfficientFrontier=_EfficientFrontier),
        hierarchical_portfolio=SimpleNamespace(HRPOpt=None),
    )

    result = run_optimizer_experiment(
        returns_frame=returns_frame,
        policy=PortfolioOptimizerPolicyConfig(optimizer_name="min_vol", min_history_rows=3),
        package_override=fake_package,
    )

    assert result["diagnostics"]["status"] == "optimized"
    assert pytest.approx(result["weights"]["target_weight"].sum(), rel=1e-6) == 1.0
    assert {"baseline_weight", "optimized_weight"}.issubset(result["comparison"].columns)


def test_vectorbt_validation_harness_writes_artifacts(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "trading_platform.research.backtester_validation.run_vectorbt_target_weight_scenario",
        lambda close_prices, target_weights, fees, package_override=None: SimpleNamespace(
            returns=pd.Series([0.0, 0.01, -0.005], index=close_prices.index[:3]),
            equity=pd.Series([1.0, 1.01, 1.00495], index=close_prices.index[:3]),
            trades=pd.DataFrame([{"symbol": close_prices.columns[0], "size": 1.0}]),
            turnover=0.0,
            trade_count=1,
            metrics={},
        ),
    )

    result = run_vectorbt_validation_harness()
    paths = write_vectorbt_validation_artifacts(result=result, output_dir=tmp_path)

    summary = pd.read_csv(paths["vectorbt_validation_summary_path"])
    assert not summary.empty
    assert "scenario_name" in summary.columns
    assert "status_reason" in summary.columns
    metrics = json.loads(Path(paths["vectorbt_validation_metrics_path"]).read_text(encoding="utf-8"))
    assert metrics["rows"]

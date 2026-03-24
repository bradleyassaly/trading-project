from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.paper.models import PaperTradingConfig
from trading_platform.services.target_construction_service import build_target_construction_result
from trading_platform.signals.registry import SIGNAL_REGISTRY
from trading_platform.universe_provenance.service import (
    build_universe_provenance_bundle,
    summarize_candidate_provenance,
    summarize_filter_failures,
    summarize_universe_build,
    write_universe_provenance_artifacts,
)


def test_build_universe_provenance_bundle_applies_sequential_filters() -> None:
    frames = {
        "AAPL": pd.DataFrame({"timestamp": pd.date_range("2025-01-01", periods=4), "close": [10.0, 11.0, 12.0, 13.0], "volume": [1_000_000] * 4}),
        "MSFT": pd.DataFrame({"timestamp": pd.date_range("2025-01-01", periods=4), "close": [2.0, 2.1, 2.2, 2.3], "volume": [1_000_000] * 4}),
        "NVDA": pd.DataFrame({"timestamp": pd.date_range("2025-01-01", periods=2), "close": [20.0, 21.0], "volume": [1_000_000] * 2}),
    }
    bundle = build_universe_provenance_bundle(
        symbols=["AAPL", "MSFT", "NVDA"],
        base_universe_id="demo",
        sub_universe_id="liquid_demo",
        filter_definitions=[
            {"filter_name": "min_price", "filter_type": "min_price", "threshold": 5.0},
            {"filter_name": "min_history", "filter_type": "min_feature_history", "threshold": 3},
        ],
        feature_loader=lambda symbol: frames[symbol],
    )

    assert bundle.eligible_symbols == ["AAPL"]
    memberships = {row.symbol: row for row in bundle.membership_records}
    assert memberships["AAPL"].inclusion_status == "included"
    assert memberships["MSFT"].exclusion_reason == "excluded_by_min_price"
    assert memberships["NVDA"].exclusion_reason == "excluded_by_min_feature_history"
    assert "eligible=1" in summarize_universe_build(bundle)
    assert summarize_filter_failures("MSFT", bundle) == "excluded_by_min_price"
    assert "liquid_demo" in summarize_candidate_provenance("AAPL", bundle)


def test_write_universe_provenance_artifacts_writes_expected_files(tmp_path: Path) -> None:
    bundle = build_universe_provenance_bundle(
        symbols=["AAPL"],
        base_universe_id="demo",
        sub_universe_id="demo_screened",
        filter_definitions=[{"filter_name": "include", "filter_type": "symbol_include_list", "symbols": ["AAPL"]}],
        feature_loader=lambda _symbol: pd.DataFrame({"timestamp": pd.date_range("2025-01-01", periods=3), "close": [10.0, 11.0, 12.0]}),
    )

    paths = write_universe_provenance_artifacts(bundle=bundle, output_dir=tmp_path)

    assert paths["universe_membership_csv"].exists()
    assert paths["universe_filter_results_csv"].exists()
    assert paths["universe_build_summary_json"].exists()
    assert paths["reference_data_coverage_summary_json"].exists()
    assert paths["membership_resolution_audit_csv"].exists()
    membership_df = pd.read_csv(paths["universe_membership_csv"])
    assert membership_df.iloc[0]["symbol"] == "AAPL"


def test_build_target_construction_result_integrates_universe_filters(monkeypatch) -> None:
    frames = {
        "AAPL": pd.DataFrame({"timestamp": pd.date_range("2025-01-01", periods=4), "close": [10.0, 11.0, 12.0, 13.0], "volume": [1_000_000] * 4}),
        "MSFT": pd.DataFrame({"timestamp": pd.date_range("2025-01-01", periods=4), "close": [2.0, 2.1, 2.2, 2.3], "volume": [1_000_000] * 4}),
        "NVDA": pd.DataFrame({"timestamp": pd.date_range("2025-01-01", periods=2), "close": [20.0, 21.0], "volume": [1_000_000] * 2}),
    }

    def fake_signal_fn(df: pd.DataFrame, **_: object) -> pd.DataFrame:
        out = df.copy()
        out["asset_return"] = out["close"].pct_change().fillna(0.0)
        out["score"] = pd.Series(range(1, len(out) + 1), index=out.index, dtype="float64")
        return out

    monkeypatch.setattr("trading_platform.services.target_construction_service.load_feature_frame", lambda symbol: frames[symbol])
    monkeypatch.setitem(SIGNAL_REGISTRY, "sma_cross", fake_signal_fn)

    result = build_target_construction_result(
        config=PaperTradingConfig(
            symbols=["AAPL", "MSFT", "NVDA"],
            universe_name="demo",
            strategy="sma_cross",
            top_n=1,
            universe_filters=[
                {"filter_name": "min_price", "filter_type": "min_price", "threshold": 5.0},
                {"filter_name": "min_history", "filter_type": "min_feature_history", "threshold": 3},
            ],
            sub_universe_id="demo_screened",
        )
    )

    assert result.universe_bundle is not None
    assert result.universe_bundle.eligible_symbols == ["AAPL"]
    assert result.effective_target_weights == {"AAPL": 1.0}
    candidate_rows = {row.symbol: row for row in result.decision_bundle.candidate_evaluations}
    assert candidate_rows["MSFT"].candidate_status == "filtered_out"
    assert candidate_rows["MSFT"].sub_universe_id == "demo_screened"
    assert candidate_rows["NVDA"].rejection_reason == "excluded_by_min_feature_history"

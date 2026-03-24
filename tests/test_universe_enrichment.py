from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.paper.models import PaperTradingConfig
from trading_platform.services.target_construction_service import build_target_construction_result
from trading_platform.signals.registry import SIGNAL_REGISTRY
from trading_platform.universe_provenance.service import (
    build_universe_provenance_bundle,
    summarize_benchmark_context,
    summarize_membership_resolution,
    summarize_metadata_coverage,
    summarize_symbol_enrichment,
    summarize_taxonomy_context,
    write_universe_provenance_artifacts,
)


def test_universe_enrichment_uses_point_in_time_membership_and_taxonomy(tmp_path: Path) -> None:
    membership_path = tmp_path / "membership.csv"
    membership_path.write_text(
        "base_universe_id,symbol,effective_start,effective_end\n"
        "demo,AAPL,2025-01-01,\n"
        "demo,MSFT,2024-01-01,2024-12-31\n",
        encoding="utf-8",
    )
    group_map_path = tmp_path / "groups.csv"
    group_map_path.write_text(
        "symbol,group,sector,industry\n"
        "AAPL,TECH,Technology,Consumer Electronics\n"
        "MSFT,TECH,Technology,Software\n",
        encoding="utf-8",
    )
    regime_dir = tmp_path / "regime"
    regime_dir.mkdir()
    (regime_dir / "market_regime.json").write_text(
        json.dumps({"latest": {"regime_label": "trend"}}, indent=2),
        encoding="utf-8",
    )
    frames = {
        "AAPL": pd.DataFrame({"timestamp": pd.date_range("2025-01-01", periods=25), "close": range(100, 125), "volume": [1_000_000] * 25}),
        "MSFT": pd.DataFrame({"timestamp": pd.date_range("2025-01-01", periods=25), "close": range(200, 225), "volume": [2_000_000] * 25}),
    }

    bundle = build_universe_provenance_bundle(
        symbols=["AAPL", "MSFT"],
        base_universe_id="demo",
        sub_universe_id="demo_screened",
        feature_loader=lambda symbol: frames[symbol],
        membership_history_path=str(membership_path),
        group_map_path=str(group_map_path),
        benchmark_id="equal_weight",
        market_regime_path=str(regime_dir),
    )

    membership = {row.symbol: row for row in bundle.point_in_time_membership}
    assert membership["AAPL"].membership_resolution_status == "confirmed"
    assert membership["MSFT"].membership_status == "not_member"
    enrichment = {row.symbol: row for row in bundle.enrichment_records}
    assert enrichment["AAPL"].taxonomy.sector == "Technology"
    assert enrichment["AAPL"].metadata_snapshot.regime_label == "trend"
    assert enrichment["AAPL"].benchmark_context.benchmark_resolution_status == "confirmed_synthetic"
    assert "confirmed" in summarize_membership_resolution(membership["AAPL"])
    assert "Technology" in summarize_taxonomy_context(enrichment["AAPL"].taxonomy)
    assert "relative_strength_20" in summarize_benchmark_context(enrichment["AAPL"].benchmark_context)
    assert "complete" in summarize_symbol_enrichment(enrichment["AAPL"])
    assert "complete=" in summarize_metadata_coverage(bundle)


def test_write_universe_enrichment_artifacts(tmp_path: Path) -> None:
    frame = pd.DataFrame({"timestamp": pd.date_range("2025-01-01", periods=25), "close": range(100, 125), "volume": [1_000_000] * 25})
    bundle = build_universe_provenance_bundle(
        symbols=["AAPL"],
        base_universe_id="demo",
        feature_loader=lambda _symbol: frame,
        benchmark_id="equal_weight",
    )
    paths = write_universe_provenance_artifacts(bundle=bundle, output_dir=tmp_path)
    assert paths["universe_enrichment_json"].exists()
    assert paths["universe_enrichment_csv"].exists()
    assert paths["point_in_time_membership_csv"].exists()
    assert paths["universe_enrichment_summary_json"].exists()


def test_target_construction_includes_enrichment_in_candidate_metadata(monkeypatch, tmp_path: Path) -> None:
    membership_path = tmp_path / "membership.csv"
    membership_path.write_text(
        "base_universe_id,symbol,effective_start,effective_end\n"
        "demo,AAPL,2025-01-01,\n",
        encoding="utf-8",
    )
    group_map_path = tmp_path / "groups.csv"
    group_map_path.write_text(
        "symbol,group,sector,industry\n"
        "AAPL,TECH,Technology,Consumer Electronics\n"
        "MSFT,TECH,Technology,Software\n",
        encoding="utf-8",
    )
    frames = {
        "AAPL": pd.DataFrame({"timestamp": pd.date_range("2025-01-01", periods=25), "close": range(100, 125), "volume": [1_000_000] * 25}),
        "MSFT": pd.DataFrame({"timestamp": pd.date_range("2025-01-01", periods=25), "close": range(2, 27), "volume": [1_000_000] * 25}),
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
            symbols=["AAPL", "MSFT"],
            universe_name="demo",
            strategy="sma_cross",
            top_n=1,
            universe_filters=[{"filter_name": "min_price", "filter_type": "min_price", "threshold": 50.0}],
            sub_universe_id="demo_screened",
            universe_membership_path=str(membership_path),
            group_map_path=str(group_map_path),
            benchmark="equal_weight",
        )
    )

    candidate_rows = {row.symbol: row for row in result.decision_bundle.candidate_evaluations}
    assert candidate_rows["AAPL"].selected_feature_values["sector"] == "Technology"
    assert candidate_rows["AAPL"].selected_feature_values["membership_resolution_status"] == "confirmed"
    assert candidate_rows["AAPL"].selected_feature_values["benchmark_resolution_status"] == "confirmed_synthetic"
    assert candidate_rows["MSFT"].candidate_status == "filtered_out"

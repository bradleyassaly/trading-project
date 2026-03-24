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
    assert paths["reference_data_coverage_summary_json"].exists()
    assert paths["membership_resolution_audit_csv"].exists()
    assert paths["taxonomy_resolution_audit_csv"].exists()
    assert paths["benchmark_mapping_resolution_audit_csv"].exists()


def test_universe_enrichment_prefers_versioned_reference_data(tmp_path: Path) -> None:
    reference_root = tmp_path / "reference_data"
    reference_root.mkdir()
    (reference_root / "reference_data_manifest.json").write_text(
        json.dumps(
            {
                "version": "2026.03.24",
                "datasets": {
                    "membership_history": {"version": "m1"},
                    "taxonomy_snapshots": {"version": "t1"},
                    "benchmark_mapping_snapshots": {"version": "b1"},
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (reference_root / "universe_membership_history.csv").write_text(
        "universe_id,symbol,effective_start_date,effective_end_date,membership_status,source,source_version,resolution_status,coverage_status\n"
        "demo,AAPL,2025-01-01,,member,reference_membership,m1,confirmed,confirmed\n"
        "demo,MSFT,2024-01-01,2024-12-31,member,reference_membership,m1,confirmed,confirmed\n",
        encoding="utf-8",
    )
    (reference_root / "taxonomy_snapshots.csv").write_text(
        "symbol,effective_start_date,effective_end_date,sector,industry,group,source,source_version,resolution_status,coverage_status\n"
        "AAPL,2025-01-01,,Technology,Consumer Electronics,TECH,reference_taxonomy,t1,confirmed,confirmed\n"
        "MSFT,2025-01-01,,Technology,Software,TECH,reference_taxonomy,t1,confirmed,confirmed\n",
        encoding="utf-8",
    )
    (reference_root / "benchmark_mapping_snapshots.csv").write_text(
        "symbol,effective_start_date,effective_end_date,benchmark_id,benchmark_symbol,source,source_version,resolution_status,coverage_status\n"
        "AAPL,2025-01-01,,spy_proxy,SPY,reference_benchmark,b1,confirmed,confirmed\n",
        encoding="utf-8",
    )
    frames = {
        "AAPL": pd.DataFrame({"timestamp": pd.date_range("2025-01-01", periods=25), "close": range(100, 125), "volume": [1_000_000] * 25}),
        "MSFT": pd.DataFrame({"timestamp": pd.date_range("2025-01-01", periods=25), "close": range(200, 225), "volume": [2_000_000] * 25}),
        "SPY": pd.DataFrame({"timestamp": pd.date_range("2025-01-01", periods=25), "close": range(300, 325), "volume": [5_000_000] * 25}),
    }

    bundle = build_universe_provenance_bundle(
        symbols=["AAPL", "MSFT"],
        base_universe_id="demo",
        sub_universe_id="demo_screened",
        feature_loader=lambda symbol: frames[symbol],
        reference_data_root=str(reference_root),
        benchmark_id="equal_weight",
    )

    enrichment = {row.symbol: row for row in bundle.enrichment_records}
    membership = {row.symbol: row for row in bundle.point_in_time_membership}

    assert membership["AAPL"].membership_source == "reference_membership"
    assert membership["AAPL"].membership_resolution_status == "confirmed"
    assert membership["MSFT"].membership_status == "not_member"
    assert enrichment["AAPL"].taxonomy.taxonomy_source == "reference_taxonomy"
    assert enrichment["AAPL"].benchmark_context.benchmark_id == "spy_proxy"
    assert enrichment["AAPL"].benchmark_context.benchmark_symbol == "SPY"
    assert enrichment["AAPL"].benchmark_context.benchmark_source == "reference_benchmark"
    assert bundle.reference_data_manifest is not None
    assert bundle.reference_data_manifest.version == "2026.03.24"
    assert bundle.reference_data_coverage_summary is not None
    assert bundle.reference_data_coverage_summary.confirmed_membership_count == 2


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

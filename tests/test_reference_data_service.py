from __future__ import annotations

import json
from pathlib import Path

from trading_platform.reference_data.service import (
    load_benchmark_mapping_snapshots,
    load_membership_history,
    load_reference_data_manifest,
    load_taxonomy_snapshots,
    resolve_benchmark_mapping_snapshot,
    resolve_membership_snapshot,
    resolve_taxonomy_snapshot,
    summarize_benchmark_mapping_source,
    summarize_membership_source,
    summarize_reference_data_coverage,
    summarize_taxonomy_source,
)
from trading_platform.reference_data.models import ReferenceDataCoverageSummary


def test_reference_data_service_resolves_versioned_datasets(tmp_path: Path) -> None:
    reference_root = tmp_path / "reference_data"
    reference_root.mkdir()
    (reference_root / "reference_data_manifest.json").write_text(
        json.dumps({"version": "2026.03.24", "datasets": {"membership_history": {"version": "m1"}}}, indent=2),
        encoding="utf-8",
    )
    (reference_root / "universe_membership_history.csv").write_text(
        "universe_id,symbol,effective_start_date,effective_end_date,membership_status,source,source_version,resolution_status,coverage_status\n"
        "demo,AAPL,2025-01-01,,member,maintained_membership,m1,confirmed,confirmed\n",
        encoding="utf-8",
    )
    (reference_root / "taxonomy_snapshots.csv").write_text(
        "symbol,effective_start_date,effective_end_date,sector,industry,group,source,source_version,resolution_status,coverage_status\n"
        "AAPL,2025-01-01,,Technology,Consumer Electronics,TECH,maintained_taxonomy,t1,confirmed,confirmed\n",
        encoding="utf-8",
    )
    (reference_root / "benchmark_mapping_snapshots.csv").write_text(
        "symbol,effective_start_date,effective_end_date,benchmark_id,benchmark_symbol,source,source_version,resolution_status,coverage_status\n"
        "AAPL,2025-01-01,,spy_proxy,SPY,maintained_benchmark,b1,confirmed,confirmed\n",
        encoding="utf-8",
    )

    manifest = load_reference_data_manifest(str(reference_root))
    membership_history = load_membership_history(reference_data_root=str(reference_root), universe_id="demo")
    taxonomy = load_taxonomy_snapshots(reference_data_root=str(reference_root))
    benchmark = load_benchmark_mapping_snapshots(reference_data_root=str(reference_root))

    membership_snapshot = resolve_membership_snapshot(
        symbol="AAPL",
        as_of_date="2025-03-01",
        universe_id="demo",
        membership_history=membership_history,
    )
    taxonomy_snapshot = resolve_taxonomy_snapshot(
        symbol="AAPL",
        as_of_date="2025-03-01",
        taxonomy_snapshots=taxonomy,
    )
    benchmark_snapshot = resolve_benchmark_mapping_snapshot(
        symbol="AAPL",
        as_of_date="2025-03-01",
        benchmark_mappings=benchmark,
    )

    assert manifest is not None
    assert manifest.version == "2026.03.24"
    assert membership_snapshot is not None
    assert membership_snapshot.source == "maintained_membership"
    assert taxonomy_snapshot is not None
    assert taxonomy_snapshot.sector == "Technology"
    assert benchmark_snapshot is not None
    assert benchmark_snapshot.benchmark_symbol == "SPY"
    assert "confirmed" in summarize_membership_source(membership_snapshot)
    assert "Technology" in summarize_taxonomy_source(taxonomy_snapshot)
    assert "spy_proxy" in summarize_benchmark_mapping_source(benchmark_snapshot)


def test_load_membership_history_supports_legacy_column_names(tmp_path: Path) -> None:
    path = tmp_path / "membership.csv"
    path.write_text(
        "base_universe_id,symbol,effective_start,effective_end\n"
        "demo,AAPL,2025-01-01,\n",
        encoding="utf-8",
    )

    membership_history = load_membership_history(membership_history_path=str(path), universe_id="demo")
    snapshot = resolve_membership_snapshot(
        symbol="AAPL",
        as_of_date="2025-03-01",
        universe_id="demo",
        membership_history=membership_history,
    )

    assert snapshot is not None
    assert snapshot.membership_status == "member"


def test_summarize_reference_data_coverage() -> None:
    summary = ReferenceDataCoverageSummary(
        as_of_date="2025-03-01",
        universe_id="demo",
        confirmed_membership_count=5,
        confirmed_taxonomy_count=4,
        confirmed_benchmark_mapping_count=3,
    )

    text = summarize_reference_data_coverage(summary)

    assert "membership_confirmed=5" in text
    assert "taxonomy_confirmed=4" in text
    assert "benchmark_confirmed=3" in text

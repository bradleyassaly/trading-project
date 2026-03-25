from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.config.models import FeatureConfig
from trading_platform.data.canonical import load_research_symbol_frame
from trading_platform.services.feature_service import run_feature_build_with_dirs
from trading_platform.settings import FEATURES_DIR, METADATA_DIR, NORMALIZED_DATA_DIR
from trading_platform.universe_provenance.service import (
    build_universe_provenance_bundle,
    write_universe_provenance_artifacts,
)


@dataclass(frozen=True)
class ResearchInputRefreshResult:
    status: str
    feature_dir: Path
    metadata_dir: Path
    feature_symbols_requested: list[str]
    feature_symbols_built: list[str]
    feature_failures: list[dict[str, str]]
    paths: dict[str, Path]
    summary: dict[str, Any]


@dataclass(frozen=True)
class ResearchInputRefreshRequest:
    symbols: list[str]
    feature_groups: list[str] | None = None
    universe_name: str | None = None
    sub_universe_id: str | None = None
    reference_data_root: str | None = None
    universe_membership_path: str | None = None
    taxonomy_snapshot_path: str | None = None
    benchmark_mapping_path: str | None = None
    market_regime_path: str | None = None
    group_map_path: str | None = None
    benchmark_id: str | None = None
    feature_dir: Path = FEATURES_DIR
    metadata_dir: Path = METADATA_DIR
    normalized_dir: Path = NORMALIZED_DATA_DIR
    failure_policy: str = "partial_success"

    def __post_init__(self) -> None:
        if not self.symbols:
            raise ValueError("symbols must contain at least one symbol")
        if self.failure_policy not in {"partial_success", "fail"}:
            raise ValueError("failure_policy must be one of: partial_success, fail")

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["feature_dir"] = str(self.feature_dir)
        payload["metadata_dir"] = str(self.metadata_dir)
        payload["normalized_dir"] = str(self.normalized_dir)
        return payload


def refresh_research_inputs(
    *,
    request: ResearchInputRefreshRequest | None = None,
    symbols: list[str] | None = None,
    feature_groups: list[str] | None = None,
    universe_name: str | None = None,
    sub_universe_id: str | None = None,
    reference_data_root: str | None = None,
    universe_membership_path: str | None = None,
    taxonomy_snapshot_path: str | None = None,
    benchmark_mapping_path: str | None = None,
    market_regime_path: str | None = None,
    group_map_path: str | None = None,
    benchmark_id: str | None = None,
    feature_dir: Path = FEATURES_DIR,
    metadata_dir: Path = METADATA_DIR,
    normalized_dir: Path = NORMALIZED_DATA_DIR,
    failure_policy: str = "partial_success",
) -> ResearchInputRefreshResult:
    resolved_request = request or ResearchInputRefreshRequest(
        symbols=list(symbols or []),
        feature_groups=feature_groups,
        universe_name=universe_name,
        sub_universe_id=sub_universe_id,
        reference_data_root=reference_data_root,
        universe_membership_path=universe_membership_path,
        taxonomy_snapshot_path=taxonomy_snapshot_path,
        benchmark_mapping_path=benchmark_mapping_path,
        market_regime_path=market_regime_path,
        group_map_path=group_map_path,
        benchmark_id=benchmark_id,
        feature_dir=feature_dir,
        metadata_dir=metadata_dir,
        normalized_dir=normalized_dir,
        failure_policy=failure_policy,
    )
    symbols = resolved_request.symbols
    feature_groups = resolved_request.feature_groups
    universe_name = resolved_request.universe_name
    sub_universe_id = resolved_request.sub_universe_id
    reference_data_root = resolved_request.reference_data_root
    universe_membership_path = resolved_request.universe_membership_path
    taxonomy_snapshot_path = resolved_request.taxonomy_snapshot_path
    benchmark_mapping_path = resolved_request.benchmark_mapping_path
    market_regime_path = resolved_request.market_regime_path
    group_map_path = resolved_request.group_map_path
    benchmark_id = resolved_request.benchmark_id
    feature_dir = resolved_request.feature_dir
    metadata_dir = resolved_request.metadata_dir
    normalized_dir = resolved_request.normalized_dir

    feature_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)
    metadata_artifact_dir = metadata_dir / "universe_refresh"
    metadata_artifact_dir.mkdir(parents=True, exist_ok=True)

    built_symbols: list[str] = []
    failures: list[dict[str, str]] = []
    for symbol in symbols:
        config = FeatureConfig(symbol=symbol, feature_groups=feature_groups)
        try:
            run_feature_build_with_dirs(
                config=config,
                normalized_dir=normalized_dir,
                features_dir=feature_dir,
            )
        except Exception as exc:
            failures.append(
                {
                    "symbol": symbol,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                }
            )
            continue
        built_symbols.append(symbol)

    bundle = None
    provenance_paths: dict[str, Path] = {}
    if built_symbols:
        bundle = build_universe_provenance_bundle(
            symbols=built_symbols,
            base_universe_id=universe_name,
            sub_universe_id=sub_universe_id,
            filter_definitions=[],
            feature_loader=lambda symbol: load_research_symbol_frame(feature_dir, symbol),
            reference_data_root=reference_data_root,
            group_map_path=group_map_path,
            membership_history_path=universe_membership_path,
            taxonomy_snapshot_path=taxonomy_snapshot_path,
            benchmark_mapping_path=benchmark_mapping_path,
            benchmark_id=benchmark_id,
            market_regime_path=market_regime_path,
        )
        provenance_paths = write_universe_provenance_artifacts(
            bundle=bundle,
            output_dir=metadata_artifact_dir,
            metadata_dir=metadata_dir,
        )

    status = "success"
    if failures and built_symbols:
        status = "partial_success"
    elif failures and not built_symbols:
        status = "failed"
    if failures and resolved_request.failure_policy == "fail":
        status = "failed"

    summary_payload: dict[str, Any] = {
        "status": status,
        "request": resolved_request.to_dict(),
        "feature_symbols_requested": list(symbols),
        "feature_symbols_built": built_symbols,
        "feature_failures": failures,
        "feature_dir": str(feature_dir),
        "normalized_dir": str(normalized_dir),
        "metadata_dir": str(metadata_dir),
        "metadata_artifact_dir": str(metadata_artifact_dir),
        "base_universe_id": universe_name,
        "sub_universe_id": sub_universe_id,
        "feature_file_count": len(list(feature_dir.glob("*.parquet"))),
        "metadata_sidecar_files": sorted(
            path.name
            for path in metadata_dir.iterdir()
            if path.is_file()
        ),
        "provenance_bundle_summary": bundle.summary.to_dict() if bundle is not None and bundle.summary is not None else None,
        "reference_data_coverage_summary": (
            bundle.reference_data_coverage_summary.to_dict()
            if bundle is not None and bundle.reference_data_coverage_summary is not None
            else None
        ),
    }
    summary_path = metadata_dir / "research_input_refresh_summary.json"
    manifest_path = metadata_dir / "research_input_bundle_manifest.json"
    failure_report_path = metadata_dir / "research_input_refresh_failures.csv"
    summary_path.write_text(json.dumps(summary_payload, indent=2, default=str), encoding="utf-8")
    manifest_path.write_text(
        json.dumps(
            {
                "feature_dir": str(feature_dir),
                "metadata_dir": str(metadata_dir),
                "metadata_artifact_dir": str(metadata_artifact_dir),
                "paths": {key: str(value) for key, value in provenance_paths.items()},
                "symbols": built_symbols,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    paths: dict[str, Path] = dict(provenance_paths)
    paths["research_input_refresh_summary_json"] = summary_path
    paths["research_input_bundle_manifest_json"] = manifest_path
    if failures:
        pd.DataFrame(failures).to_csv(failure_report_path, index=False)
        paths["research_input_refresh_failures_csv"] = failure_report_path

    return ResearchInputRefreshResult(
        status=status,
        feature_dir=feature_dir,
        metadata_dir=metadata_dir,
        feature_symbols_requested=list(symbols),
        feature_symbols_built=built_symbols,
        feature_failures=failures,
        paths=paths,
        summary=summary_payload,
    )

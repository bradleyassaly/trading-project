from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.reference_data.models import (
    BenchmarkMappingSnapshot,
    ReferenceDataCoverageSummary,
    ReferenceDataVersionManifest,
    TaxonomySnapshotRecord,
    UniverseMembershipSnapshot,
)


DEFAULT_REFERENCE_FILENAMES = {
    "membership_history": "universe_membership_history.csv",
    "taxonomy_snapshots": "taxonomy_snapshots.csv",
    "benchmark_mapping_snapshots": "benchmark_mapping_snapshots.csv",
    "manifest": "reference_data_manifest.json",
}


def _normalize_symbol(symbol: str) -> str:
    return str(symbol).strip().upper()


def _load_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        return pd.DataFrame(payload if isinstance(payload, list) else payload.get("rows", []))
    if suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.DataFrame()


def load_reference_data_manifest(reference_data_root: str | Path | None) -> ReferenceDataVersionManifest | None:
    if not reference_data_root:
        return None
    path = Path(reference_data_root) / DEFAULT_REFERENCE_FILENAMES["manifest"]
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return ReferenceDataVersionManifest(**payload)


def _resolve_dataset_path(
    *,
    reference_data_root: str | Path | None,
    explicit_path: str | Path | None,
    dataset_key: str,
) -> Path | None:
    if explicit_path:
        return Path(explicit_path)
    if reference_data_root:
        return Path(reference_data_root) / DEFAULT_REFERENCE_FILENAMES[dataset_key]
    return None


def load_membership_history(
    *,
    reference_data_root: str | Path | None = None,
    membership_history_path: str | Path | None = None,
    universe_id: str | None = None,
) -> pd.DataFrame:
    path = _resolve_dataset_path(
        reference_data_root=reference_data_root,
        explicit_path=membership_history_path,
        dataset_key="membership_history",
    )
    if path is None:
        return pd.DataFrame()
    frame = _load_table(path)
    if frame.empty or "symbol" not in frame.columns:
        return pd.DataFrame()
    frame = frame.copy()
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    if "base_universe_id" in frame.columns and "universe_id" not in frame.columns:
        frame["universe_id"] = frame["base_universe_id"]
    if "effective_start" in frame.columns and "effective_start_date" not in frame.columns:
        frame["effective_start_date"] = frame["effective_start"]
    if "effective_end" in frame.columns and "effective_end_date" not in frame.columns:
        frame["effective_end_date"] = frame["effective_end"]
    if "universe_id" in frame.columns and universe_id is not None:
        frame = frame[frame["universe_id"].astype(str) == str(universe_id)]
    for column in ("effective_start_date", "effective_end_date", "as_of_date"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")
    return frame


def load_taxonomy_snapshots(
    *,
    reference_data_root: str | Path | None = None,
    taxonomy_snapshot_path: str | Path | None = None,
) -> pd.DataFrame:
    path = _resolve_dataset_path(
        reference_data_root=reference_data_root,
        explicit_path=taxonomy_snapshot_path,
        dataset_key="taxonomy_snapshots",
    )
    if path is None:
        return pd.DataFrame()
    frame = _load_table(path)
    if frame.empty or "symbol" not in frame.columns:
        return pd.DataFrame()
    frame = frame.copy()
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    for column in ("effective_start_date", "effective_end_date", "as_of_date"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")
    return frame


def load_benchmark_mapping_snapshots(
    *,
    reference_data_root: str | Path | None = None,
    benchmark_mapping_path: str | Path | None = None,
) -> pd.DataFrame:
    path = _resolve_dataset_path(
        reference_data_root=reference_data_root,
        explicit_path=benchmark_mapping_path,
        dataset_key="benchmark_mapping_snapshots",
    )
    if path is None:
        return pd.DataFrame()
    frame = _load_table(path)
    if frame.empty or "symbol" not in frame.columns:
        return pd.DataFrame()
    frame = frame.copy()
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    for column in ("effective_start_date", "effective_end_date", "as_of_date"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")
    return frame


def _best_effective_match(frame: pd.DataFrame, *, symbol: str, as_of_date: str) -> pd.DataFrame:
    if frame.empty:
        return frame
    as_of_ts = pd.Timestamp(as_of_date)
    symbol_rows = frame[frame["symbol"] == _normalize_symbol(symbol)]
    if symbol_rows.empty:
        return symbol_rows
    if "effective_start_date" in symbol_rows.columns:
        if "effective_end_date" not in symbol_rows.columns:
            symbol_rows = symbol_rows.copy()
            symbol_rows["effective_end_date"] = pd.NaT
        matched = symbol_rows[
            (symbol_rows["effective_start_date"].isna() | (symbol_rows["effective_start_date"] <= as_of_ts))
            & (symbol_rows["effective_end_date"].isna() | (symbol_rows["effective_end_date"] >= as_of_ts))
        ]
        if not matched.empty:
            return matched.sort_values(["effective_start_date", "effective_end_date"], ascending=[False, True], na_position="last")
        return symbol_rows.iloc[0:0]
    if "as_of_date" in symbol_rows.columns:
        dated = symbol_rows[symbol_rows["as_of_date"].notna()].copy()
        if not dated.empty:
            dated["distance"] = (dated["as_of_date"] - as_of_ts).abs()
            return dated.sort_values(["distance", "as_of_date"])
    return symbol_rows


def resolve_membership_snapshot(
    *,
    symbol: str,
    as_of_date: str,
    universe_id: str | None,
    membership_history: pd.DataFrame,
) -> UniverseMembershipSnapshot | None:
    if membership_history.empty or "symbol" not in membership_history.columns:
        return None
    symbol_key = _normalize_symbol(symbol)
    universe_rows = membership_history
    if universe_id is not None and "universe_id" in universe_rows.columns:
        universe_rows = universe_rows[universe_rows["universe_id"].astype(str) == str(universe_id)]
    symbol_rows = universe_rows[universe_rows["symbol"] == symbol_key]
    if symbol_rows.empty:
        return None
    matched = _best_effective_match(symbol_rows, symbol=symbol, as_of_date=as_of_date)
    if matched.empty:
        row = symbol_rows.iloc[0]
        return UniverseMembershipSnapshot(
            symbol=symbol_key,
            universe_id=str(row.get("universe_id") or universe_id or ""),
            as_of_date=as_of_date,
            membership_status="not_member",
            source=str(row.get("source") or "reference_data"),
            source_version=str(row.get("source_version")) if pd.notna(row.get("source_version")) else None,
            resolution_status=str(row.get("resolution_status") or "confirmed"),
            coverage_status=str(row.get("coverage_status") or "confirmed"),
            notes=str(row.get("notes")) if pd.notna(row.get("notes")) else None,
            metadata={
                "effective_start_date": str(row.get("effective_start_date").date()) if pd.notna(row.get("effective_start_date")) else None,
                "effective_end_date": str(row.get("effective_end_date").date()) if pd.notna(row.get("effective_end_date")) else None,
            },
        )
    row = matched.iloc[0]
    return UniverseMembershipSnapshot(
        symbol=symbol_key,
        universe_id=str(row.get("universe_id") or universe_id or ""),
        as_of_date=as_of_date,
        membership_status=str(row.get("membership_status") or "member"),
        source=str(row.get("source") or "reference_data"),
        source_version=str(row.get("source_version")) if pd.notna(row.get("source_version")) else None,
        resolution_status=str(row.get("resolution_status") or "confirmed"),
        coverage_status=str(row.get("coverage_status") or "confirmed"),
        notes=str(row.get("notes")) if pd.notna(row.get("notes")) else None,
        metadata={
            "effective_start_date": str(row.get("effective_start_date").date()) if pd.notna(row.get("effective_start_date")) else None,
            "effective_end_date": str(row.get("effective_end_date").date()) if pd.notna(row.get("effective_end_date")) else None,
        },
    )


def resolve_taxonomy_snapshot(
    *,
    symbol: str,
    as_of_date: str,
    taxonomy_snapshots: pd.DataFrame,
) -> TaxonomySnapshotRecord | None:
    matched = _best_effective_match(taxonomy_snapshots, symbol=symbol, as_of_date=as_of_date)
    if matched.empty:
        return None
    row = matched.iloc[0]
    return TaxonomySnapshotRecord(
        symbol=_normalize_symbol(symbol),
        as_of_date=as_of_date,
        effective_start_date=str(row.get("effective_start_date").date()) if pd.notna(row.get("effective_start_date")) else None,
        effective_end_date=str(row.get("effective_end_date").date()) if pd.notna(row.get("effective_end_date")) else None,
        sector=str(row.get("sector")) if pd.notna(row.get("sector")) else None,
        industry=str(row.get("industry")) if pd.notna(row.get("industry")) else None,
        group=str(row.get("group")) if pd.notna(row.get("group")) else None,
        source=str(row.get("source") or "reference_data"),
        source_version=str(row.get("source_version")) if pd.notna(row.get("source_version")) else None,
        resolution_status=str(row.get("resolution_status") or "confirmed"),
        coverage_status=str(row.get("coverage_status") or "confirmed"),
        notes=str(row.get("notes")) if pd.notna(row.get("notes")) else None,
    )


def resolve_benchmark_mapping_snapshot(
    *,
    symbol: str,
    as_of_date: str,
    benchmark_mappings: pd.DataFrame,
) -> BenchmarkMappingSnapshot | None:
    matched = _best_effective_match(benchmark_mappings, symbol=symbol, as_of_date=as_of_date)
    if matched.empty:
        return None
    row = matched.iloc[0]
    return BenchmarkMappingSnapshot(
        symbol=_normalize_symbol(symbol),
        as_of_date=as_of_date,
        effective_start_date=str(row.get("effective_start_date").date()) if pd.notna(row.get("effective_start_date")) else None,
        effective_end_date=str(row.get("effective_end_date").date()) if pd.notna(row.get("effective_end_date")) else None,
        benchmark_id=str(row.get("benchmark_id")) if pd.notna(row.get("benchmark_id")) else None,
        benchmark_symbol=str(row.get("benchmark_symbol")) if pd.notna(row.get("benchmark_symbol")) else None,
        source=str(row.get("source") or "reference_data"),
        source_version=str(row.get("source_version")) if pd.notna(row.get("source_version")) else None,
        resolution_status=str(row.get("resolution_status") or "confirmed"),
        coverage_status=str(row.get("coverage_status") or "confirmed"),
        notes=str(row.get("notes")) if pd.notna(row.get("notes")) else None,
    )


def summarize_membership_source(snapshot: UniverseMembershipSnapshot | dict[str, Any] | None) -> str:
    if snapshot is None:
        return "unavailable"
    payload = snapshot.to_dict() if hasattr(snapshot, "to_dict") else dict(snapshot)
    return " | ".join(
        [
            str(payload.get("resolution_status") or "unavailable"),
            str(payload.get("source") or "unknown"),
            str(payload.get("membership_status") or "unknown"),
        ]
    )


def summarize_taxonomy_source(snapshot: TaxonomySnapshotRecord | dict[str, Any] | None) -> str:
    if snapshot is None:
        return "unavailable"
    payload = snapshot.to_dict() if hasattr(snapshot, "to_dict") else dict(snapshot)
    parts = [str(payload.get("resolution_status") or "unavailable"), str(payload.get("source") or "unknown")]
    if payload.get("sector"):
        parts.append(f"sector={payload['sector']}")
    return " | ".join(parts)


def summarize_benchmark_mapping_source(snapshot: BenchmarkMappingSnapshot | dict[str, Any] | None) -> str:
    if snapshot is None:
        return "unavailable"
    payload = snapshot.to_dict() if hasattr(snapshot, "to_dict") else dict(snapshot)
    parts = [str(payload.get("resolution_status") or "unavailable"), str(payload.get("source") or "unknown")]
    if payload.get("benchmark_id"):
        parts.append(str(payload["benchmark_id"]))
    return " | ".join(parts)


def summarize_reference_data_coverage(summary: ReferenceDataCoverageSummary | dict[str, Any] | None) -> str:
    if summary is None:
        return "no reference data coverage"
    payload = summary.to_dict() if hasattr(summary, "to_dict") else dict(summary)
    return (
        f"membership_confirmed={payload.get('confirmed_membership_count', 0)} | "
        f"taxonomy_confirmed={payload.get('confirmed_taxonomy_count', 0)} | "
        f"benchmark_confirmed={payload.get('confirmed_benchmark_mapping_count', 0)}"
    )

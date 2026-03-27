from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.integrations.financedatabase_adapter import (
    SecurityMasterBuildResult,
    build_security_master_from_financedatabase,
    classification_group_map,
)
from trading_platform.universes.registry import get_universe_symbols


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class ClassificationArtifactBundle:
    security_master_path: Path
    summary_path: Path


def resolve_classification_symbols(
    *,
    symbols: list[str] | None = None,
    universe: str | None = None,
) -> list[str]:
    selected = sum(bool(value) for value in (symbols, universe))
    if selected != 1:
        raise ValueError("exactly one of symbols or universe must be provided")
    if symbols:
        return list(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()))
    return list(dict.fromkeys(get_universe_symbols(str(universe))))


def build_classification_artifacts(
    *,
    symbols: list[str],
    output_dir: str | Path,
    as_of_date: str | None = None,
    package_override=None,
) -> dict[str, Any]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    result: SecurityMasterBuildResult = build_security_master_from_financedatabase(
        symbols=symbols,
        as_of_date=as_of_date,
        package_override=package_override,
    )
    security_master_path = output_path / "security_master.csv"
    summary_path = output_path / "classification_summary.json"
    result.frame.to_csv(security_master_path, index=False)
    frame = result.frame.copy()
    asset_type_counts = (
        frame["asset_type"].fillna("unknown").value_counts(dropna=False).to_dict()
        if "asset_type" in frame.columns
        else {}
    )
    summary_path.write_text(
        json.dumps(
            {
                "generated_at": _now_utc(),
                "source": result.source,
                "as_of_date": result.as_of_date,
                "requested_symbol_count": int(len(result.requested_symbols)),
                "symbol_count": int(len(result.frame)),
                "matched_symbol_count": int(len(result.matched_symbols)),
                "unmatched_symbol_count": int(len(result.unmatched_symbols)),
                "match_rate": float(len(result.matched_symbols) / len(result.requested_symbols))
                if result.requested_symbols
                else 0.0,
                "matched_symbols": result.matched_symbols,
                "unmatched_symbols": result.unmatched_symbols,
                "columns": list(result.frame.columns),
                "missing_sector_count": int(frame["sector"].isna().sum()) if "sector" in frame.columns else 0,
                "missing_industry_count": int(frame["industry"].isna().sum()) if "industry" in frame.columns else 0,
                "duplicate_symbol_count": int(result.duplicate_symbol_count),
                "sector_count": (
                    int(result.frame["sector"].dropna().nunique()) if "sector" in result.frame.columns else 0
                ),
                "country_count": (
                    int(result.frame["country"].dropna().nunique()) if "country" in result.frame.columns else 0
                ),
                "asset_type_counts": asset_type_counts,
                "point_in_time_warning": "FinanceDatabase classifications are reference enrichments only and are not assumed point-in-time correct.",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return {
        "security_master_path": security_master_path,
        "classification_summary_path": summary_path,
    }


def load_security_master(path_or_dir: str | Path) -> pd.DataFrame:
    path = Path(path_or_dir)
    if path.is_dir():
        path = path / "security_master.csv"
    if not path.exists():
        raise FileNotFoundError(f"Security master artifact not found: {path}")
    return pd.read_csv(path)


def build_symbol_group_map(
    *,
    security_master_path: str | Path,
    level: str = "sector",
) -> dict[str, str]:
    return classification_group_map(load_security_master(security_master_path), level=level)

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.integrations.optional_dependencies import require_dependency


@dataclass(frozen=True)
class AlphalensArtifactBundle:
    factor_data_path: Path
    ic_summary_path: Path
    quantile_returns_path: Path
    turnover_path: Path
    group_summary_path: Path
    metadata_path: Path


def build_clean_alphalens_factor_data(
    *,
    factor_series: pd.Series,
    pricing_frame: pd.DataFrame,
    periods: tuple[int, ...] = (1, 5, 10),
    quantiles: int = 5,
    groupby: dict[str, str] | None = None,
    max_loss: float = 0.35,
    package_override=None,
) -> pd.DataFrame:
    alphalens = require_dependency(
        "alphalens",
        purpose="running Alphalens diagnostics",
        package_override=package_override,
    )
    if factor_series.empty:
        raise ValueError("factor_series is empty")
    if pricing_frame.empty:
        raise ValueError("pricing_frame is empty")
    if not isinstance(factor_series.index, pd.MultiIndex) or list(factor_series.index.names)[:2] != ["date", "asset"]:
        raise ValueError("factor_series must use a MultiIndex named ['date', 'asset']")
    symbol_count = int(factor_series.index.get_level_values("asset").nunique())
    date_count = int(factor_series.index.get_level_values("date").nunique())
    if symbol_count < 2:
        raise ValueError("factor_series must contain at least 2 unique assets for cross-sectional diagnostics")
    if date_count < 5:
        raise ValueError("factor_series must contain at least 5 unique dates for Alphalens diagnostics")
    factor_data = alphalens.utils.get_clean_factor_and_forward_returns(
        factor=factor_series.sort_index(),
        prices=pricing_frame.sort_index(),
        periods=list(periods),
        quantiles=int(quantiles),
        groupby=groupby,
        max_loss=float(max_loss),
    )
    if not isinstance(factor_data, pd.DataFrame) or factor_data.empty:
        raise ValueError("Alphalens returned an empty factor_data frame")
    return factor_data


def write_alphalens_artifacts(
    *,
    factor_data: pd.DataFrame,
    output_dir: str | Path,
    metadata: dict[str, Any] | None = None,
    package_override=None,
) -> AlphalensArtifactBundle:
    alphalens = require_dependency(
        "alphalens",
        purpose="writing Alphalens diagnostics",
        package_override=package_override,
    )
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    factor_data_path = output_path / "alphalens_factor_data.parquet"
    ic_summary_path = output_path / "alphalens_ic_summary.csv"
    quantile_returns_path = output_path / "alphalens_quantile_returns.csv"
    turnover_path = output_path / "alphalens_turnover.csv"
    group_summary_path = output_path / "alphalens_group_summary.csv"
    metadata_path = output_path / "alphalens_metadata.json"

    factor_data.to_parquet(factor_data_path)

    ic = alphalens.performance.factor_information_coefficient(factor_data)
    ic_summary = pd.DataFrame(
        [
            {
                "metric": "mean",
                **{str(column): float(value) for column, value in ic.mean().to_dict().items()},
            },
            {
                "metric": "std",
                **{str(column): float(value) for column, value in ic.std(ddof=0).to_dict().items()},
            },
        ]
    )
    ic_summary.to_csv(ic_summary_path, index=False)

    mean_returns, _spread = alphalens.performance.mean_return_by_quantile(factor_data, by_date=False)
    mean_returns.reset_index().to_csv(quantile_returns_path, index=False)

    turnover_rows: list[dict[str, Any]] = []
    for period in (1, 5, 10):
        try:
            turnover_series = alphalens.performance.quantile_turnover(
                factor_data["factor_quantile"],
                quantile=1,
                period=period,
            )
        except Exception:
            continue
        turnover_rows.append(
            {
                "period": int(period),
                "mean_turnover": float(turnover_series.mean()),
                "max_turnover": float(turnover_series.max()),
            }
        )
    pd.DataFrame(turnover_rows).to_csv(turnover_path, index=False)

    group_summary = (
        factor_data.reset_index()[["asset", "group"]]
        .dropna()
        .groupby("group", as_index=False, observed=False)
        .agg(asset_count=("asset", "nunique"))
    )
    group_summary.to_csv(group_summary_path, index=False)

    metadata_path.write_text(
        json.dumps(
            {
                "row_count": int(len(factor_data)),
                "columns": list(factor_data.columns),
                "group_aware": bool("group" in factor_data.columns),
                **(metadata or {}),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return AlphalensArtifactBundle(
        factor_data_path=factor_data_path,
        ic_summary_path=ic_summary_path,
        quantile_returns_path=quantile_returns_path,
        turnover_path=turnover_path,
        group_summary_path=group_summary_path,
        metadata_path=metadata_path,
    )

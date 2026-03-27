from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from trading_platform.integrations.optional_dependencies import require_dependency


@dataclass(frozen=True)
class QuantStatsArtifactBundle:
    metrics_json_path: Path
    summary_csv_path: Path
    tearsheet_html_path: Path | None


def _normalize_return_series(returns: pd.Series) -> pd.Series:
    if returns.empty:
        raise ValueError("returns series is empty")
    normalized = pd.Series(returns).dropna().astype(float)
    if normalized.empty:
        raise ValueError("returns series is empty after dropping NaNs")
    if not isinstance(normalized.index, pd.DatetimeIndex):
        normalized.index = pd.to_datetime(normalized.index, errors="coerce")
    normalized = normalized[~normalized.index.isna()]
    if normalized.empty:
        raise ValueError("returns series has no valid datetime index values")
    if len(normalized) < 2:
        raise ValueError("returns series must contain at least 2 observations")
    return normalized.sort_index()


def _normalize_metrics_frame(metrics_frame: pd.Series | pd.DataFrame) -> pd.DataFrame:
    if isinstance(metrics_frame, pd.Series):
        frame = metrics_frame.rename("value").reset_index().rename(columns={"index": "metric"})
        return frame
    frame = pd.DataFrame(metrics_frame).copy()
    if frame.empty:
        return pd.DataFrame(columns=["metric", "value"])
    if not isinstance(frame.index, pd.RangeIndex):
        frame = frame.reset_index().rename(columns={frame.index.name or "index": "metric"})
    if "metric" not in frame.columns:
        first_column = frame.columns[0]
        frame = frame.rename(columns={first_column: "metric"})
    return frame


def write_quantstats_report(
    *,
    returns: pd.Series,
    output_dir: str | Path,
    benchmark: pd.Series | None = None,
    title: str = "Trading Platform Report",
    metadata: dict[str, object] | None = None,
    package_override=None,
) -> QuantStatsArtifactBundle:
    quantstats = require_dependency(
        "quantstats",
        purpose="generating QuantStats reports",
        package_override=package_override,
    )
    normalized_returns = _normalize_return_series(returns)
    normalized_benchmark = _normalize_return_series(benchmark) if benchmark is not None and len(benchmark) else None

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    metrics_json_path = output_path / "quantstats_metrics.json"
    summary_csv_path = output_path / "quantstats_summary.csv"
    tearsheet_html_path = output_path / "quantstats_tearsheet.html"

    metrics_frame = quantstats.reports.metrics(
        normalized_returns,
        benchmark=normalized_benchmark,
        mode="basic",
        display=False,
    )
    metrics_frame = _normalize_metrics_frame(metrics_frame)
    metrics_frame.to_csv(summary_csv_path, index=False)
    metrics_json_path.write_text(
        json.dumps(
            {
                "row_count": int(len(metrics_frame)),
                "metrics": metrics_frame.to_dict(orient="records"),
                "metadata": metadata or {},
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    try:
        quantstats.reports.html(
            normalized_returns,
            benchmark=normalized_benchmark,
            output=str(tearsheet_html_path),
            title=title,
        )
    except Exception:
        tearsheet_html_path = None

    return QuantStatsArtifactBundle(
        metrics_json_path=metrics_json_path,
        summary_csv_path=summary_csv_path,
        tearsheet_html_path=tearsheet_html_path,
    )

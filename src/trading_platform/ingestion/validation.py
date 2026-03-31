from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.ingestion.contracts import CANONICAL_MARKET_DATA_COLUMNS
from trading_platform.schemas.bars import NUMERIC_BAR_COLUMNS, REQUIRED_BAR_COLUMNS


MARKET_DATA_VALIDATION_SCHEMA_VERSION = "market_data_validation_v1"


def _normalize_examples(examples: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in examples or []:
        normalized.append({str(key): row[key] for key in sorted(row)})
    return normalized


@dataclass(frozen=True)
class MarketDataValidationIssue:
    rule: str
    severity: str
    message: str
    column: str | None = None
    example_count: int = 0
    examples: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        object.__setattr__(self, "examples", _normalize_examples(self.examples))

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule": self.rule,
            "severity": self.severity,
            "message": self.message,
            "column": self.column,
            "example_count": int(self.example_count),
            "examples": list(self.examples),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "MarketDataValidationIssue":
        data = dict(payload or {})
        return cls(
            rule=str(data["rule"]),
            severity=str(data["severity"]),
            message=str(data["message"]),
            column=str(data["column"]) if data.get("column") is not None else None,
            example_count=int(data.get("example_count", 0) or 0),
            examples=list(data.get("examples") or []),
        )


@dataclass(frozen=True)
class MarketDataValidationReport:
    row_count: int
    issue_count: int
    passed: bool
    issues: list[MarketDataValidationIssue] = field(default_factory=list)
    symbol: str | None = None
    timeframe: str | None = None
    provider: str | None = None
    asset_class: str | None = None
    schema_version: str = MARKET_DATA_VALIDATION_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "row_count": int(self.row_count),
            "issue_count": int(self.issue_count),
            "passed": bool(self.passed),
            "issues": [issue.to_dict() for issue in self.issues],
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "provider": self.provider,
            "asset_class": self.asset_class,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "MarketDataValidationReport":
        data = dict(payload or {})
        return cls(
            row_count=int(data.get("row_count", 0) or 0),
            issue_count=int(data.get("issue_count", 0) or 0),
            passed=bool(data.get("passed", False)),
            issues=[MarketDataValidationIssue.from_dict(row) for row in data.get("issues", [])],
            symbol=str(data["symbol"]) if data.get("symbol") is not None else None,
            timeframe=str(data["timeframe"]) if data.get("timeframe") is not None else None,
            provider=str(data["provider"]) if data.get("provider") is not None else None,
            asset_class=str(data["asset_class"]) if data.get("asset_class") is not None else None,
            schema_version=str(data.get("schema_version", MARKET_DATA_VALIDATION_SCHEMA_VERSION)),
        )


def _sample_rows(frame: pd.DataFrame, columns: list[str]) -> list[dict[str, Any]]:
    return frame.loc[:, columns].head(5).to_dict(orient="records")


def _build_issue(
    *,
    rule: str,
    message: str,
    severity: str = "error",
    column: str | None = None,
    frame: pd.DataFrame | None = None,
    sample_columns: list[str] | None = None,
) -> MarketDataValidationIssue:
    examples = []
    example_count = 0
    if frame is not None and not frame.empty and sample_columns:
        examples = _sample_rows(frame, sample_columns)
        example_count = int(len(frame.index))
    return MarketDataValidationIssue(
        rule=rule,
        severity=severity,
        message=message,
        column=column,
        example_count=example_count,
        examples=examples,
    )


def _validate_frame(
    df: pd.DataFrame,
    *,
    required_columns: tuple[str, ...] | list[str],
    require_timeframe_column: bool,
    symbol: str | None = None,
    timeframe: str | None = None,
    provider: str | None = None,
    asset_class: str | None = None,
) -> MarketDataValidationReport:
    issues: list[MarketDataValidationIssue] = []
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        issues.append(
            _build_issue(
                rule="missing_required_columns",
                message=f"Missing required columns: {missing}",
                frame=None,
            )
        )
        return MarketDataValidationReport(
            row_count=int(len(df.index)),
            issue_count=len(issues),
            passed=False,
            issues=issues,
            symbol=symbol,
            timeframe=timeframe,
            provider=provider,
            asset_class=asset_class,
        )

    if df.empty:
        issues.append(_build_issue(rule="empty_frame", message="Bar dataframe is empty"))
        return MarketDataValidationReport(
            row_count=0,
            issue_count=len(issues),
            passed=False,
            issues=issues,
            symbol=symbol,
            timeframe=timeframe,
            provider=provider,
            asset_class=asset_class,
        )

    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        issues.append(
            _build_issue(
                rule="timestamp_dtype",
                message="Column 'timestamp' must be datetime-like",
                column="timestamp",
            )
        )
        return MarketDataValidationReport(
            row_count=int(len(df.index)),
            issue_count=len(issues),
            passed=False,
            issues=issues,
            symbol=symbol,
            timeframe=timeframe,
            provider=provider,
            asset_class=asset_class,
        )

    for col in NUMERIC_BAR_COLUMNS:
        if not pd.api.types.is_numeric_dtype(df[col]):
            issues.append(
                _build_issue(
                    rule="numeric_dtype",
                    message=f"Column '{col}' must be numeric",
                    column=col,
                )
            )

    if require_timeframe_column and "timeframe" in df.columns:
        duplicate_keys = ["timestamp", "symbol", "timeframe"]
    else:
        duplicate_keys = ["timestamp", "symbol"]

    duplicate_rows = df.loc[df[duplicate_keys].duplicated(), duplicate_keys]
    if not duplicate_rows.empty:
        issues.append(
            _build_issue(
                rule="duplicate_timestamp_symbol",
                message=f"Duplicate rows found for keys {duplicate_keys}",
                frame=duplicate_rows,
                sample_columns=duplicate_keys,
            )
        )

    if not df["timestamp"].is_monotonic_increasing:
        issues.append(
            _build_issue(
                rule="non_monotonic_timestamps",
                message="Timestamps must be sorted in ascending order",
                column="timestamp",
            )
        )

    null_columns = [col for col in REQUIRED_BAR_COLUMNS if df[col].isnull().any()]
    for column in null_columns:
        issues.append(
            _build_issue(
                rule="null_required_values",
                message=f"Found nulls in required column '{column}'",
                column=column,
                frame=df.loc[df[column].isnull(), ["timestamp", "symbol", column]],
                sample_columns=["timestamp", "symbol", column],
            )
        )

    if "symbol" in df.columns and df["symbol"].isnull().any():
        issues.append(
            _build_issue(
                rule="null_symbol",
                message="Found nulls in 'symbol' column",
                column="symbol",
                frame=df.loc[df["symbol"].isnull(), ["timestamp", "symbol"]],
                sample_columns=["timestamp", "symbol"],
            )
        )

    negative_volume = df.loc[df["volume"] < 0, ["timestamp", "symbol", "volume"]]
    if not negative_volume.empty:
        issues.append(
            _build_issue(
                rule="negative_volume",
                message="Column 'volume' contains negative values",
                column="volume",
                frame=negative_volume,
                sample_columns=["timestamp", "symbol", "volume"],
            )
        )

    nonpositive_prices = df.loc[
        (df["open"] <= 0) | (df["high"] <= 0) | (df["low"] <= 0) | (df["close"] <= 0),
        ["timestamp", "symbol", "open", "high", "low", "close"],
    ]
    if not nonpositive_prices.empty:
        issues.append(
            _build_issue(
                rule="nonpositive_prices",
                message="Found rows with non-positive OHLC prices",
                frame=nonpositive_prices,
                sample_columns=["timestamp", "symbol", "open", "high", "low", "close"],
            )
        )

    high_below_low = df.loc[df["high"] < df["low"], ["timestamp", "symbol", "high", "low"]]
    if not high_below_low.empty:
        issues.append(
            _build_issue(
                rule="high_below_low",
                message="Found rows where high < low",
                frame=high_below_low,
                sample_columns=["timestamp", "symbol", "high", "low"],
            )
        )

    for column in ("open", "high", "low", "close"):
        outside = df.loc[
            (df[column] < df["low"]) | (df[column] > df["high"]),
            ["timestamp", "symbol", column, "low", "high"],
        ]
        if not outside.empty:
            issues.append(
                _build_issue(
                    rule="ohlc_outside_range",
                    message=f"Found rows where '{column}' is outside the [low, high] range",
                    column=column,
                    frame=outside,
                    sample_columns=["timestamp", "symbol", column, "low", "high"],
                )
            )

    return MarketDataValidationReport(
        row_count=int(len(df.index)),
        issue_count=len(issues),
        passed=not issues,
        issues=issues,
        symbol=symbol,
        timeframe=timeframe,
        provider=provider,
        asset_class=asset_class,
    )


def validate_market_data_frame(
    df: pd.DataFrame,
    *,
    symbol: str | None = None,
    timeframe: str | None = None,
    provider: str | None = None,
    asset_class: str | None = None,
) -> MarketDataValidationReport:
    return _validate_frame(
        df,
        required_columns=tuple(CANONICAL_MARKET_DATA_COLUMNS),
        require_timeframe_column=True,
        symbol=symbol,
        timeframe=timeframe,
        provider=provider,
        asset_class=asset_class,
    )


def validate_basic_bar_frame(df: pd.DataFrame) -> MarketDataValidationReport:
    return _validate_frame(
        df,
        required_columns=REQUIRED_BAR_COLUMNS,
        require_timeframe_column=False,
    )


def raise_for_validation_errors(report: MarketDataValidationReport) -> None:
    if report.passed:
        return
    primary_issue = report.issues[0] if report.issues else None
    if primary_issue is None:
        raise ValueError("Market-data validation failed")
    raise ValueError(primary_issue.message)


def write_market_data_validation_report(
    *,
    output_path: str | Path,
    report: MarketDataValidationReport,
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
    return path

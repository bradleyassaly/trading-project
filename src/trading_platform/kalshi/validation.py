from __future__ import annotations

import json
import math
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

PASS = "PASS"
WARNING = "WARNING"
FAIL = "FAIL"
_SEVERITY_ORDER = {PASS: 0, WARNING: 1, FAIL: 2}
_SYNTHETIC_MARKERS = ("SYNTH-", "synthetic")


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number):
        return None
    return number


def _safe_rate(numerator: int, denominator: int) -> float:
    return float(numerator) / float(denominator) if denominator > 0 else 0.0


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_category(value: Any) -> str:
    raw = str(value or "").strip()
    return raw if raw else "missing"


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _date_range(values: list[datetime | None]) -> dict[str, str | None]:
    clean = [value for value in values if value is not None]
    if not clean:
        return {"start": None, "end": None}
    return {
        "start": min(clean).isoformat(),
        "end": max(clean).isoformat(),
    }


def _list_parquet_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    if not path.exists():
        return []
    return sorted(item for item in path.glob("*.parquet") if item.is_file())


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _load_markets_frame(path: Path) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame()
    return pl.read_parquet(path)


def _load_resolution_frame(path: Path) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame()
    return pl.read_csv(path)


def _scan_trade_or_candle_layout(
    path: Path,
    *,
    ticker_column: str | None,
    timestamp_column: str | None,
    required_columns: tuple[str, ...],
) -> dict[str, Any]:
    files = _list_parquet_files(path)
    file_names = [item.name for item in files]
    ticker_counts: Counter[str] = Counter()
    missing_required_columns: dict[str, list[str]] = {}
    invalid_timestamp_rows = 0
    total_rows = 0
    timestamps: list[datetime | None] = []
    schema_columns: Counter[str] = Counter()

    for parquet_path in files:
        frame = pl.read_parquet(parquet_path)
        total_rows += len(frame)
        schema_columns.update(frame.columns)

        missing_columns = [column for column in required_columns if column not in frame.columns]
        if missing_columns:
            missing_required_columns[parquet_path.name] = missing_columns

        file_ticker_counts: Counter[str] = Counter()
        if ticker_column and ticker_column in frame.columns:
            for value in frame.get_column(ticker_column).to_list():
                ticker = str(value or "").strip()
                if ticker:
                    file_ticker_counts[ticker] += 1

        if not file_ticker_counts and parquet_path.stem:
            file_ticker_counts[parquet_path.stem] = len(frame)

        ticker_counts.update(file_ticker_counts)

        if timestamp_column and timestamp_column in frame.columns:
            for value in frame.get_column(timestamp_column).to_list():
                parsed = _parse_timestamp(value)
                if value is not None and str(value).strip() and parsed is None:
                    invalid_timestamp_rows += 1
                timestamps.append(parsed)

    return {
        "file_count": len(files),
        "file_names": file_names,
        "rows": total_rows,
        "ticker_counts": dict(sorted(ticker_counts.items())),
        "ticker_set": sorted(ticker_counts),
        "invalid_timestamp_rows": invalid_timestamp_rows,
        "date_range": _date_range(timestamps),
        "missing_required_columns": missing_required_columns,
        "schema_columns": dict(sorted(schema_columns.items())),
    }


def _duplicates_from_series(values: list[str]) -> list[str]:
    counts = Counter(value for value in values if value)
    return sorted(value for value, count in counts.items() if count > 1)


def _synthetic_matches(values: list[str]) -> list[str]:
    matches: set[str] = set()
    for value in values:
        text = str(value or "")
        lowered = text.lower()
        if text.startswith("SYNTH-") or "synthetic" in lowered:
            matches.add(text)
    return sorted(matches)


def _overall_status(findings: list["KalshiValidationFinding"]) -> str:
    if not findings:
        return PASS
    return max(findings, key=lambda item: _SEVERITY_ORDER[item.severity]).severity


@dataclass(frozen=True)
class KalshiValidationThresholds:
    min_resolution_coverage_warn_pct: float = 0.90
    min_resolution_coverage_fail_pct: float = 0.75
    min_trade_coverage_warn_pct: float = 0.80
    min_trade_coverage_fail_pct: float = 0.60
    min_candle_coverage_warn_pct: float = 0.80
    min_candle_coverage_fail_pct: float = 0.60
    max_duplicate_ticker_warn_rate: float = 0.0
    max_duplicate_ticker_fail_rate: float = 0.01
    max_duplicate_market_id_warn_rate: float = 0.0
    max_duplicate_market_id_fail_rate: float = 0.01
    max_invalid_timestamp_warn_rate: float = 0.01
    max_invalid_timestamp_fail_rate: float = 0.05
    allowed_missing_category_warn_rate: float = 0.05
    allowed_missing_category_fail_rate: float = 0.10
    synthetic_markers_hard_fail: bool = True


@dataclass(frozen=True)
class KalshiDataValidationConfig:
    normalized_markets_path: str = "data/kalshi/normalized/markets.parquet"
    normalized_trades_path: str = "data/kalshi/normalized/trades"
    normalized_candles_path: str = "data/kalshi/normalized/candles"
    resolution_csv_path: str = "data/kalshi/normalized/resolution.csv"
    ingest_summary_path: str = "data/kalshi/raw/ingest_summary.json"
    ingest_manifest_path: str = "data/kalshi/raw/ingest_manifest.json"
    ingest_checkpoint_path: str = "data/kalshi/raw/ingest_checkpoint.json"
    features_dir: str = "data/kalshi/features/real"
    output_dir: str = "data/kalshi/validation"
    thresholds: KalshiValidationThresholds = field(default_factory=KalshiValidationThresholds)


@dataclass(frozen=True)
class KalshiValidationFinding:
    severity: str
    code: str
    message: str
    observed: Any = None
    threshold: Any = None
    context: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class KalshiDataValidationArtifacts:
    summary_path: Path
    details_path: Path
    report_path: Path


@dataclass(frozen=True)
class KalshiDataValidationResult:
    status: str
    passed: bool
    generated_at: str
    artifacts: KalshiDataValidationArtifacts
    summary_payload: dict[str, Any]
    details_payload: dict[str, Any]


def _coverage_finding(
    *,
    code: str,
    label: str,
    coverage_pct: float,
    warn_threshold: float,
    fail_threshold: float,
    numerator: int,
    denominator: int,
) -> KalshiValidationFinding:
    severity = PASS
    threshold = warn_threshold
    if coverage_pct < fail_threshold:
        severity = FAIL
        threshold = fail_threshold
    elif coverage_pct < warn_threshold:
        severity = WARNING
    return KalshiValidationFinding(
        severity=severity,
        code=code,
        message=(
            f"{label} coverage is {coverage_pct:.1%} "
            f"({numerator} of {denominator} markets), below the "
            f"{'failure' if severity == FAIL else 'warning' if severity == WARNING else 'configured'} threshold."
            if severity != PASS
            else f"{label} coverage is {coverage_pct:.1%} ({numerator} of {denominator} markets)."
        ),
        observed=coverage_pct,
        threshold=threshold,
        context={"covered_markets": numerator, "total_markets": denominator},
    )


def _rate_finding(
    *,
    code: str,
    label: str,
    rate: float,
    warn_threshold: float,
    fail_threshold: float,
    observed_count: int,
    denominator: int,
    extras: dict[str, Any] | None = None,
) -> KalshiValidationFinding:
    severity = PASS
    threshold = warn_threshold
    if rate > fail_threshold:
        severity = FAIL
        threshold = fail_threshold
    elif rate > warn_threshold:
        severity = WARNING
    return KalshiValidationFinding(
        severity=severity,
        code=code,
        message=(
            f"{label} is {rate:.1%} ({observed_count} of {denominator}), above the "
            f"{'failure' if severity == FAIL else 'warning' if severity == WARNING else 'configured'} threshold."
            if severity != PASS
            else f"{label} is {rate:.1%} ({observed_count} of {denominator})."
        ),
        observed=rate,
        threshold=threshold,
        context={"count": observed_count, "denominator": denominator, **(extras or {})},
    )


def _synthetic_finding(matches: list[str], *, hard_fail: bool) -> KalshiValidationFinding:
    severity = FAIL if hard_fail else WARNING
    return KalshiValidationFinding(
        severity=severity,
        code="synthetic_markers_detected",
        message=(
            "Synthetic-looking tickers or paths were detected in the real-data validation inputs."
            if matches
            else "No synthetic-looking tickers or paths were detected."
        ),
        observed=len(matches),
        threshold=0,
        context={"matches": matches[:50]},
    )


def _path_payload(config: KalshiDataValidationConfig) -> dict[str, str]:
    return {
        "normalized_markets_path": config.normalized_markets_path,
        "normalized_trades_path": config.normalized_trades_path,
        "normalized_candles_path": config.normalized_candles_path,
        "resolution_csv_path": config.resolution_csv_path,
        "ingest_summary_path": config.ingest_summary_path,
        "ingest_manifest_path": config.ingest_manifest_path,
        "ingest_checkpoint_path": config.ingest_checkpoint_path,
        "features_dir": config.features_dir,
        "output_dir": config.output_dir,
    }


def _market_has_core_fields(row: dict[str, Any]) -> tuple[bool, list[str]]:
    missing: list[str] = []
    if not str(row.get("ticker") or "").strip():
        missing.append("ticker")
    if not str(row.get("status") or "").strip():
        missing.append("status")
    return len(missing) == 0, missing


def _market_time_candidate(row: dict[str, Any]) -> Any:
    return (
        row.get("close_time")
        or row.get("close_date")
        or row.get("expiration_time")
        or row.get("expiration_ts")
        or row.get("end_date")
    )


def run_kalshi_data_validation(config: KalshiDataValidationConfig | None = None) -> KalshiDataValidationResult:
    cfg = config or KalshiDataValidationConfig()
    thresholds = cfg.thresholds
    generated_at = _utc_now().isoformat()

    markets_path = Path(cfg.normalized_markets_path)
    trades_path = Path(cfg.normalized_trades_path)
    candles_path = Path(cfg.normalized_candles_path)
    resolution_path = Path(cfg.resolution_csv_path)
    ingest_summary = _load_json(Path(cfg.ingest_summary_path)) or {}
    ingest_manifest = _load_json(Path(cfg.ingest_manifest_path)) or {}
    ingest_checkpoint = _load_json(Path(cfg.ingest_checkpoint_path)) or {}
    markets_frame = _load_markets_frame(markets_path)
    resolution_frame = _load_resolution_frame(resolution_path)
    trades_layout = _scan_trade_or_candle_layout(
        trades_path,
        ticker_column="ticker",
        timestamp_column="traded_at",
        required_columns=("trade_id", "ticker", "traded_at"),
    )
    candles_layout = _scan_trade_or_candle_layout(
        candles_path,
        ticker_column="ticker",
        timestamp_column="timestamp",
        required_columns=("timestamp", "open", "high", "low", "close"),
    )

    market_rows = len(markets_frame)
    market_tickers = []
    duplicate_tickers: list[str] = []
    duplicate_market_ids: list[str] = []
    missing_category_count = 0
    invalid_market_timestamp_count = 0
    market_date_range = {"start": None, "end": None}
    category_distribution: dict[str, int] = {}
    valid_core_market_count = 0
    invalid_core_markets: list[dict[str, Any]] = []
    market_source_modes: list[str] = []

    if not markets_frame.is_empty():
        market_rows_dicts = markets_frame.to_dicts()
        if "ticker" in markets_frame.columns:
            market_tickers = [str(value or "").strip() for value in markets_frame.get_column("ticker").to_list()]
            duplicate_tickers = _duplicates_from_series(market_tickers)
        if "market_id" in markets_frame.columns:
            duplicate_market_ids = _duplicates_from_series(
                [str(value or "").strip() for value in markets_frame.get_column("market_id").to_list()]
            )
        if "category" in markets_frame.columns:
            categories = [_normalize_category(value) for value in markets_frame.get_column("category").to_list()]
            missing_category_count = sum(1 for value in categories if value == "missing")
            category_distribution = dict(sorted(Counter(categories).items()))
        if "source_mode" in markets_frame.columns:
            market_source_modes = [
                str(value or "").strip()
                for value in markets_frame.get_column("source_mode").to_list()
                if str(value or "").strip()
            ]
        for row in market_rows_dicts:
            valid_core, missing_fields = _market_has_core_fields(row)
            if valid_core:
                valid_core_market_count += 1
            else:
                invalid_core_markets.append(
                    {
                        "ticker": str(row.get("ticker") or ""),
                        "missing_fields": missing_fields,
                    }
                )
        market_time_values = [_market_time_candidate(row) for row in market_rows_dicts]
        timestamps = [_parse_timestamp(value) for value in market_time_values]
        invalid_market_timestamp_count = sum(
            1
            for raw, parsed in zip(market_time_values, timestamps, strict=False)
            if raw is not None and str(raw).strip() and parsed is None
        )
        market_date_range = _date_range(timestamps)

    market_ticker_set = set(value for value in market_tickers if value)
    resolution_ticker_set = set()
    if not resolution_frame.is_empty() and "ticker" in resolution_frame.columns:
        resolution_ticker_set = {
            str(value or "").strip()
            for value in resolution_frame.get_column("ticker").to_list()
            if str(value or "").strip()
        }

    trade_ticker_set = set(trades_layout["ticker_set"])
    candle_ticker_set = set(candles_layout["ticker_set"])
    total_trades = _safe_int(trades_layout["rows"])
    total_candles = _safe_int(candles_layout["rows"])

    resolution_coverage_pct = _safe_rate(len(market_ticker_set & resolution_ticker_set), len(market_ticker_set))
    trade_coverage_pct = _safe_rate(len(market_ticker_set & trade_ticker_set), len(market_ticker_set))
    candle_coverage_pct = _safe_rate(len(market_ticker_set & candle_ticker_set), len(market_ticker_set))
    duplicate_ticker_rate = _safe_rate(len(duplicate_tickers), len(market_ticker_set))
    duplicate_market_id_rate = _safe_rate(len(duplicate_market_ids), market_rows)
    missing_category_rate = _safe_rate(missing_category_count, market_rows)
    invalid_timestamp_count = (
        invalid_market_timestamp_count
        + _safe_int(trades_layout["invalid_timestamp_rows"])
        + _safe_int(candles_layout["invalid_timestamp_rows"])
    )
    invalid_timestamp_denominator = market_rows + total_trades + total_candles
    invalid_timestamp_rate = _safe_rate(invalid_timestamp_count, invalid_timestamp_denominator)
    market_core_validity_pct = _safe_rate(valid_core_market_count, market_rows)
    recent_market_only_dataset = bool(market_rows) and bool(market_source_modes) and all(
        mode in {"live_recent_filtered", "direct_historical_ticker"}
        for mode in market_source_modes
    ) and valid_core_market_count == market_rows

    schema_mismatches = {
        "trade_tickers_missing_from_markets": sorted(trade_ticker_set - market_ticker_set),
        "candle_tickers_missing_from_markets": sorted(candle_ticker_set - market_ticker_set),
        "resolution_tickers_missing_from_markets": sorted(resolution_ticker_set - market_ticker_set),
        "trade_files_missing_required_columns": trades_layout["missing_required_columns"],
        "candle_files_missing_required_columns": candles_layout["missing_required_columns"],
    }

    filter_diagnostics = (
        ingest_summary.get("filter_diagnostics")
        or ingest_manifest.get("filter_diagnostics")
        or {
            "total_markets_before_filters": ingest_summary.get("markets_downloaded")
            or ingest_manifest.get("markets_downloaded")
            or market_rows,
            "retained_markets": ingest_summary.get("markets_after_filters")
            or ingest_manifest.get("markets_after_filters")
            or market_rows,
            "excluded_markets_total": ingest_summary.get("markets_excluded_by_filters")
            or ingest_manifest.get("markets_excluded_by_filters")
            or 0,
            "excluded_by_category": None,
            "excluded_by_series_pattern": None,
            "excluded_by_series": None,
            "excluded_by_min_volume": None,
            "excluded_by_bracket": None,
            "excluded_no_trade_data": None,
            "excluded_missing_core_fields": None,
            "excluded_by_lookback": None,
            "effective_filter_config": ingest_summary.get("filter_config")
            or ingest_manifest.get("filter_config")
            or {},
        }
    )

    synthetic_matches = _synthetic_matches(
        market_tickers
        + trades_layout["ticker_set"]
        + candles_layout["ticker_set"]
        + [cfg.normalized_markets_path, cfg.normalized_trades_path, cfg.normalized_candles_path, cfg.features_dir]
        + list(trades_layout["file_names"])
        + list(candles_layout["file_names"])
        + [str(value) for value in (ingest_summary.get("output_layout") or {}).values()]
        + [str(value) for value in (ingest_manifest.get("output_layout") or {}).values()]
    )

    trade_finding = _coverage_finding(
        code="trade_coverage",
        label="Trade",
        coverage_pct=trade_coverage_pct,
        warn_threshold=thresholds.min_trade_coverage_warn_pct,
        fail_threshold=thresholds.min_trade_coverage_fail_pct,
        numerator=len(market_ticker_set & trade_ticker_set),
        denominator=len(market_ticker_set),
    )
    candle_finding = _coverage_finding(
        code="candle_coverage",
        label="Candle",
        coverage_pct=candle_coverage_pct,
        warn_threshold=thresholds.min_candle_coverage_warn_pct,
        fail_threshold=thresholds.min_candle_coverage_fail_pct,
        numerator=len(market_ticker_set & candle_ticker_set),
        denominator=len(market_ticker_set),
    )
    if recent_market_only_dataset:
        trade_finding = KalshiValidationFinding(
            severity=PASS,
            code="trade_coverage",
            message=(
                f"Trade coverage is {trade_coverage_pct:.1%} ({len(market_ticker_set & trade_ticker_set)} of {len(market_ticker_set)} markets), "
                "but market-only recent-ingest datasets are allowed when core market fields are valid."
            ),
            observed=trade_coverage_pct,
            threshold=0,
            context={"covered_markets": len(market_ticker_set & trade_ticker_set), "total_markets": len(market_ticker_set), "market_only_recent_dataset": True},
        )
        candle_finding = KalshiValidationFinding(
            severity=PASS,
            code="candle_coverage",
            message=(
                f"Candle coverage is {candle_coverage_pct:.1%} ({len(market_ticker_set & candle_ticker_set)} of {len(market_ticker_set)} markets), "
                "but market-only recent-ingest datasets are allowed when core market fields are valid."
            ),
            observed=candle_coverage_pct,
            threshold=0,
            context={"covered_markets": len(market_ticker_set & candle_ticker_set), "total_markets": len(market_ticker_set), "market_only_recent_dataset": True},
        )

    findings = [
        _coverage_finding(
            code="resolution_coverage",
            label="Resolution",
            coverage_pct=resolution_coverage_pct,
            warn_threshold=thresholds.min_resolution_coverage_warn_pct,
            fail_threshold=thresholds.min_resolution_coverage_fail_pct,
            numerator=len(market_ticker_set & resolution_ticker_set),
            denominator=len(market_ticker_set),
        ),
        trade_finding,
        candle_finding,
        _rate_finding(
            code="duplicate_tickers",
            label="Duplicate ticker rate",
            rate=duplicate_ticker_rate,
            warn_threshold=thresholds.max_duplicate_ticker_warn_rate,
            fail_threshold=thresholds.max_duplicate_ticker_fail_rate,
            observed_count=len(duplicate_tickers),
            denominator=len(market_ticker_set),
            extras={"duplicates": duplicate_tickers[:50]},
        ),
        _rate_finding(
            code="duplicate_market_ids",
            label="Duplicate market-id rate",
            rate=duplicate_market_id_rate,
            warn_threshold=thresholds.max_duplicate_market_id_warn_rate,
            fail_threshold=thresholds.max_duplicate_market_id_fail_rate,
            observed_count=len(duplicate_market_ids),
            denominator=market_rows,
            extras={"duplicates": duplicate_market_ids[:50]},
        ),
        _rate_finding(
            code="missing_categories",
            label="Missing-category rate",
            rate=missing_category_rate,
            warn_threshold=thresholds.allowed_missing_category_warn_rate,
            fail_threshold=thresholds.allowed_missing_category_fail_rate,
            observed_count=missing_category_count,
            denominator=market_rows,
        ),
        _rate_finding(
            code="invalid_timestamps",
            label="Invalid-timestamp rate",
            rate=invalid_timestamp_rate,
            warn_threshold=thresholds.max_invalid_timestamp_warn_rate,
            fail_threshold=thresholds.max_invalid_timestamp_fail_rate,
            observed_count=invalid_timestamp_count,
            denominator=invalid_timestamp_denominator,
        ),
        _coverage_finding(
            code="market_core_fields",
            label="Market core field validity",
            coverage_pct=market_core_validity_pct,
            warn_threshold=1.0,
            fail_threshold=0.999999,
            numerator=valid_core_market_count,
            denominator=market_rows,
        ),
    ]

    if synthetic_matches:
        findings.append(_synthetic_finding(synthetic_matches, hard_fail=thresholds.synthetic_markers_hard_fail))
    else:
        findings.append(
            KalshiValidationFinding(
                severity=PASS,
                code="synthetic_markers_detected",
                message="No synthetic-looking tickers or paths were detected in the real-data validation inputs.",
                observed=0,
                threshold=0,
                context={"matches": []},
            )
        )

    schema_issue_count = sum(
        len(values) if isinstance(values, list) else len(values)
        for values in schema_mismatches.values()
    )
    findings.append(
        KalshiValidationFinding(
            severity=FAIL if schema_issue_count else PASS,
            code="schema_consistency",
            message=(
                "Schema inconsistencies were found across normalized Kalshi layers."
                if schema_issue_count
                else "No normalized-layer schema inconsistencies were detected."
            ),
            observed=schema_issue_count,
            threshold=0,
            context=schema_mismatches,
        )
    )

    overall = _overall_status(findings)
    severity_counts = dict(
        sorted(Counter(finding.severity for finding in findings).items(), key=lambda item: _SEVERITY_ORDER[item[0]])
    )
    artifacts_dir = Path(cfg.output_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    artifacts = KalshiDataValidationArtifacts(
        summary_path=artifacts_dir / "kalshi_data_validation_summary.json",
        details_path=artifacts_dir / "kalshi_data_validation_details.json",
        report_path=artifacts_dir / "kalshi_data_validation_report.md",
    )

    summary_payload = {
        "generated_at": generated_at,
        "status": overall,
        "passed": overall != FAIL,
        "paths": _path_payload(cfg),
        "artifact_paths": {
            "summary_path": str(artifacts.summary_path),
            "details_path": str(artifacts.details_path),
            "report_path": str(artifacts.report_path),
        },
        "counts": {
            "markets": market_rows,
            "trades": total_trades,
            "candles": total_candles,
            "resolved_markets": len(resolution_ticker_set),
            "trade_files": _safe_int(trades_layout["file_count"]),
            "candle_files": _safe_int(candles_layout["file_count"]),
        },
        "coverage": {
            "resolution_pct": resolution_coverage_pct,
            "trade_pct": trade_coverage_pct,
            "candle_pct": candle_coverage_pct,
            "market_core_fields_pct": market_core_validity_pct,
        },
        "quality_rates": {
            "duplicate_ticker_rate": duplicate_ticker_rate,
            "duplicate_market_id_rate": duplicate_market_id_rate,
            "missing_category_rate": missing_category_rate,
            "invalid_timestamp_rate": invalid_timestamp_rate,
        },
        "filter_diagnostics": filter_diagnostics,
        "severity_counts": severity_counts,
        "recent_market_only_dataset": recent_market_only_dataset,
    }

    details_payload = {
        **summary_payload,
        "thresholds": asdict(thresholds),
        "findings": [asdict(finding) for finding in findings],
        "date_ranges": {
            "markets_close_time": market_date_range,
            "trades": trades_layout["date_range"],
            "candles": candles_layout["date_range"],
        },
        "category_distribution": category_distribution,
        "market_core_fields": {
            "valid_market_count": valid_core_market_count,
            "invalid_market_count": len(invalid_core_markets),
            "invalid_markets": invalid_core_markets[:50],
            "source_modes": sorted(set(market_source_modes)),
        },
        "duplicates": {
            "duplicate_tickers": duplicate_tickers,
            "duplicate_market_ids": duplicate_market_ids,
        },
        "schema_consistency": schema_mismatches,
        "ingest_metadata": {
            "summary": ingest_summary,
            "manifest": ingest_manifest,
            "checkpoint": ingest_checkpoint,
        },
        "normalized_layers": {
            "markets_columns": markets_frame.columns,
            "trades_schema_columns": trades_layout["schema_columns"],
            "candles_schema_columns": candles_layout["schema_columns"],
        },
        "synthetic_matches": synthetic_matches,
        "coverage_sets": {
            "markets_without_resolution": sorted(market_ticker_set - resolution_ticker_set),
            "markets_without_trades": sorted(market_ticker_set - trade_ticker_set),
            "markets_without_candles": sorted(market_ticker_set - candle_ticker_set),
        },
    }

    report = _build_markdown_report(
        generated_at=generated_at,
        status=overall,
        findings=findings,
        summary_payload=summary_payload,
        details_payload=details_payload,
    )

    artifacts.summary_path.write_text(json.dumps(summary_payload, indent=2, default=str), encoding="utf-8")
    artifacts.details_path.write_text(json.dumps(details_payload, indent=2, default=str), encoding="utf-8")
    artifacts.report_path.write_text(report, encoding="utf-8")

    return KalshiDataValidationResult(
        status=overall,
        passed=overall != FAIL,
        generated_at=generated_at,
        artifacts=artifacts,
        summary_payload=summary_payload,
        details_payload=details_payload,
    )


def load_kalshi_validation_summary(path: str | Path) -> dict[str, Any]:
    summary_path = Path(path)
    if not summary_path.exists():
        raise FileNotFoundError(f"Kalshi validation summary not found: {summary_path}")
    return json.loads(summary_path.read_text(encoding="utf-8"))


def require_passing_kalshi_validation(path: str | Path) -> dict[str, Any]:
    payload = load_kalshi_validation_summary(path)
    if not bool(payload.get("passed")):
        raise ValueError(
            f"Kalshi validation did not pass at {path} (status={payload.get('status', 'unknown')})."
        )
    return payload


def _build_markdown_report(
    *,
    generated_at: str,
    status: str,
    findings: list[KalshiValidationFinding],
    summary_payload: dict[str, Any],
    details_payload: dict[str, Any],
) -> str:
    counts = summary_payload["counts"]
    coverage = summary_payload["coverage"]
    quality_rates = summary_payload["quality_rates"]
    filter_diagnostics = summary_payload["filter_diagnostics"]
    lines = [
        "# Kalshi Data Validation Report",
        "",
        f"Generated: {generated_at}",
        f"Overall status: {status}",
        "",
        "## Dataset Summary",
        "",
        f"- Markets: {counts['markets']}",
        f"- Trades: {counts['trades']}",
        f"- Candles: {counts['candles']}",
        f"- Resolved markets: {counts['resolved_markets']}",
        f"- Resolution coverage: {coverage['resolution_pct']:.1%}",
        f"- Trade coverage: {coverage['trade_pct']:.1%}",
        f"- Candle coverage: {coverage['candle_pct']:.1%}",
        f"- Market core-field validity: {coverage.get('market_core_fields_pct', 0.0):.1%}",
        f"- Missing-category rate: {quality_rates['missing_category_rate']:.1%}",
        f"- Invalid-timestamp rate: {quality_rates['invalid_timestamp_rate']:.1%}",
        "",
        "## Findings",
        "",
        "| Severity | Code | Message |",
        "| --- | --- | --- |",
    ]
    for finding in findings:
        lines.append(f"| {finding.severity} | {finding.code} | {finding.message} |")

    lines += [
        "",
        "## Filter Diagnostics",
        "",
        f"- Markets before filters: {filter_diagnostics.get('total_markets_before_filters', 'n/a')}",
        f"- Markets after filters: {filter_diagnostics.get('retained_markets', 'n/a')}",
        f"- Excluded by category: {filter_diagnostics.get('excluded_by_category', 'n/a')}",
        f"- Excluded by series pattern: {filter_diagnostics.get('excluded_by_series_pattern', filter_diagnostics.get('excluded_by_series', 'n/a'))}",
        f"- Excluded by min_volume: {filter_diagnostics.get('excluded_by_min_volume', 'n/a')}",
        f"- Excluded by bracket/max_markets_per_event: {filter_diagnostics.get('excluded_by_bracket', 'n/a')}",
        f"- Excluded missing core fields: {filter_diagnostics.get('excluded_missing_core_fields', 'n/a')}",
        f"- Excluded by lookback: {filter_diagnostics.get('excluded_by_lookback', 'n/a')}",
        f"- Excluded no trade data: {filter_diagnostics.get('excluded_no_trade_data', 'n/a')}",
        f"- Effective filter config: `{json.dumps(filter_diagnostics.get('effective_filter_config', {}), sort_keys=True)}`",
        "",
        "## Category Distribution",
        "",
    ]
    category_distribution = details_payload.get("category_distribution") or {}
    if category_distribution:
        for category, count in category_distribution.items():
            lines.append(f"- {category}: {count}")
    else:
        lines.append("- none")

    date_ranges = details_payload.get("date_ranges", {})
    lines += [
        "",
        "## Date Coverage",
        "",
        f"- Markets close_time: {date_ranges.get('markets_close_time', {}).get('start') or 'n/a'} -> {date_ranges.get('markets_close_time', {}).get('end') or 'n/a'}",
        f"- Trades: {date_ranges.get('trades', {}).get('start') or 'n/a'} -> {date_ranges.get('trades', {}).get('end') or 'n/a'}",
        f"- Candles: {date_ranges.get('candles', {}).get('start') or 'n/a'} -> {date_ranges.get('candles', {}).get('end') or 'n/a'}",
        "",
        "## Actionable Notes",
        "",
    ]
    for finding in findings:
        if finding.severity == PASS:
            continue
        if finding.code == "resolution_coverage":
            lines.append("- Improve or repair `resolution.csv` generation before running resolved-market research.")
        elif finding.code == "trade_coverage":
            lines.append("- Re-run ingest with a broader market set or lower filtering pressure if trade coverage is unintentionally sparse.")
        elif finding.code == "candle_coverage":
            lines.append("- Rebuild normalized candles for retained markets before backtesting price-path assumptions.")
        elif finding.code == "duplicate_tickers":
            lines.append("- Deduplicate normalized market rows before downstream joins so market-level metrics are stable.")
        elif finding.code == "duplicate_market_ids":
            lines.append("- Investigate market-id collisions in the normalized market index before promotion or paper use.")
        elif finding.code == "invalid_timestamps":
            lines.append("- Repair timestamp parsing issues in normalized trades/candles so time-window logic remains trustworthy.")
        elif finding.code == "schema_consistency":
            lines.append("- Repair cross-layer ticker/schema mismatches before running research or paper workflows.")
        elif finding.code == "synthetic_markers_detected":
            lines.append("- Remove synthetic artifacts from real-data defaults and re-run ingest validation.")
        elif finding.code == "missing_categories":
            lines.append("- Backfill market categories so category-conditioned research and risk limits remain reliable.")
        elif finding.code == "market_core_fields":
            lines.append("- Ensure each retained market carries ticker and status before downstream research joins; category and time fields are optional metadata.")
    if not any(finding.severity != PASS for finding in findings):
        lines.append("- No blocking or warning-level issues were detected.")
    return "\n".join(lines)

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd


MARKET_REGIME_SCHEMA_VERSION = 1
_TIMESTAMP_COLUMNS = ["timestamp", "datetime", "date"]
_VALUE_COLUMNS = ["close", "adj_close", "price", "equity", "value"]


@dataclass(frozen=True)
class MarketRegimePolicyConfig:
    schema_version: int = MARKET_REGIME_SCHEMA_VERSION
    short_return_window: int = 5
    long_return_window: int = 20
    volatility_window: int = 20
    dispersion_window: int = 20
    high_volatility_threshold: float = 0.25
    low_volatility_threshold: float = 0.12
    trend_return_threshold: float = 0.03
    flat_return_threshold: float = 0.01
    confidence_floor: float = 0.20
    allow_paper_equity_curve_proxy: bool = True
    strategy_family_regime_map: dict[str, list[str]] = field(
        default_factory=lambda: {
            "momentum": ["trend", "low_vol"],
            "breakout": ["trend", "high_vol"],
            "reversal": ["mean_reversion", "low_vol"],
            "mean_reversion": ["mean_reversion", "low_vol"],
            "value": ["mean_reversion", "low_vol"],
            "volatility": ["high_vol"],
            "short_term": ["high_vol", "mean_reversion"],
        }
    )
    notes: str | None = None
    tags: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.schema_version != MARKET_REGIME_SCHEMA_VERSION:
            raise ValueError(f"Unsupported market regime schema_version: {self.schema_version}")
        for name, value in {
            "short_return_window": self.short_return_window,
            "long_return_window": self.long_return_window,
            "volatility_window": self.volatility_window,
            "dispersion_window": self.dispersion_window,
        }.items():
            if value <= 0:
                raise ValueError(f"{name} must be > 0")
        for name, value in {
            "high_volatility_threshold": self.high_volatility_threshold,
            "low_volatility_threshold": self.low_volatility_threshold,
            "trend_return_threshold": self.trend_return_threshold,
            "flat_return_threshold": self.flat_return_threshold,
            "confidence_floor": self.confidence_floor,
        }.items():
            if value < 0:
                raise ValueError(f"{name} must be >= 0")


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


def _safe_read_json(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    file_path = Path(path)
    if not file_path.exists() or file_path.is_dir():
        return {}
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _resolve_input_path(path_or_dir: str | Path, *, allow_equity_curve_proxy: bool) -> Path:
    path = Path(path_or_dir)
    if path.is_file():
        return path
    candidate_names = ["market_data.csv", "prices.csv", "price_history.csv"]
    if allow_equity_curve_proxy:
        candidate_names.append("paper_equity_curve.csv")
    for name in candidate_names:
        candidate = path / name
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"No supported regime input CSV found under {path}")


def _infer_timestamp_column(frame: pd.DataFrame) -> str:
    for column in _TIMESTAMP_COLUMNS:
        if column in frame.columns:
            return column
    raise ValueError(f"Regime input is missing a timestamp column. Expected one of: {_TIMESTAMP_COLUMNS}")


def _infer_value_column(frame: pd.DataFrame) -> str:
    for column in _VALUE_COLUMNS:
        if column in frame.columns:
            return column
    raise ValueError(f"Regime input is missing a value column. Expected one of: {_VALUE_COLUMNS}")


def _normalize_history(frame: pd.DataFrame) -> tuple[pd.DataFrame, str, str]:
    timestamp_column = _infer_timestamp_column(frame)
    value_column = _infer_value_column(frame)
    normalized = frame[[timestamp_column, value_column]].copy()
    normalized[timestamp_column] = pd.to_datetime(normalized[timestamp_column], utc=True, errors="coerce")
    normalized[value_column] = pd.to_numeric(normalized[value_column], errors="coerce")
    normalized = normalized.dropna().sort_values(timestamp_column).reset_index(drop=True)
    if normalized.empty:
        raise ValueError("Regime input contains no valid rows after normalization")
    return normalized, timestamp_column, value_column


def infer_strategy_regime_compatibility(
    *,
    signal_family: str | None,
    strategy_name: str | None = None,
    policy: MarketRegimePolicyConfig | None = None,
) -> list[str]:
    family_map = (policy.strategy_family_regime_map if policy is not None else MarketRegimePolicyConfig().strategy_family_regime_map)
    haystack = " ".join([str(signal_family or ""), str(strategy_name or "")]).lower()
    matches: list[str] = []
    for key, regimes in family_map.items():
        if key.lower() in haystack:
            for regime in regimes:
                if regime not in matches:
                    matches.append(regime)
    return matches or ["all_weather"]


def detect_market_regime(
    *,
    input_path: str | Path,
    output_dir: str | Path,
    policy: MarketRegimePolicyConfig,
) -> dict[str, Any]:
    resolved_input = _resolve_input_path(
        input_path,
        allow_equity_curve_proxy=policy.allow_paper_equity_curve_proxy,
    )
    frame = pd.read_csv(resolved_input)
    normalized, timestamp_column, value_column = _normalize_history(frame)
    series = normalized[value_column]
    returns = series.pct_change().dropna()

    short_window = min(policy.short_return_window, max(len(series) - 1, 1))
    long_window = min(policy.long_return_window, max(len(series) - 1, 1))
    vol_window = min(policy.volatility_window, max(len(returns), 1))
    dispersion_window = min(policy.dispersion_window, max(len(returns), 1))

    short_return = _safe_float((series.iloc[-1] / series.iloc[-(short_window + 1)]) - 1.0) if len(series) > short_window else None
    long_return = _safe_float((series.iloc[-1] / series.iloc[-(long_window + 1)]) - 1.0) if len(series) > long_window else None
    realized_volatility = _safe_float(returns.tail(vol_window).std(ddof=0) * (252 ** 0.5)) if not returns.empty else 0.0
    dispersion_metric = _safe_float(returns.tail(dispersion_window).abs().mean()) if not returns.empty else 0.0
    slope_proxy = _safe_float((series.iloc[-1] - series.iloc[max(len(series) - long_window - 1, 0)]) / max(long_window, 1))

    volatility_regime = "normal_vol"
    if (realized_volatility or 0.0) >= policy.high_volatility_threshold:
        volatility_regime = "high_vol"
    elif (realized_volatility or 0.0) <= policy.low_volatility_threshold:
        volatility_regime = "low_vol"

    abs_long_return = abs(long_return or 0.0)
    trend_regime = "flat"
    if abs_long_return >= policy.trend_return_threshold:
        trend_regime = "trend"
    elif abs_long_return <= policy.flat_return_threshold:
        trend_regime = "mean_reversion"

    if volatility_regime == "high_vol":
        regime_label = "high_vol"
        confidence = min(1.0, ((realized_volatility or 0.0) / max(policy.high_volatility_threshold, 1e-9)) - 1.0 + 0.5)
    elif trend_regime == "trend":
        regime_label = "trend"
        confidence = min(1.0, abs_long_return / max(policy.trend_return_threshold, 1e-9) - 0.5)
    elif trend_regime == "mean_reversion":
        regime_label = "mean_reversion"
        confidence = min(1.0, 1.0 - (abs_long_return / max(policy.flat_return_threshold, 1e-9)) * 0.5)
    else:
        regime_label = "low_vol" if volatility_regime == "low_vol" else "trend"
        confidence = min(1.0, ((policy.low_volatility_threshold - min(realized_volatility or 0.0, policy.low_volatility_threshold)) / max(policy.low_volatility_threshold, 1e-9)) + 0.25) if regime_label == "low_vol" else 0.25

    confidence = max(policy.confidence_floor, round(float(confidence), 6))
    generated_at = _now_utc()
    row = {
        "timestamp": generated_at,
        "regime_label": regime_label,
        "volatility_regime": volatility_regime,
        "trend_regime": trend_regime,
        "short_return": short_return,
        "long_return": long_return,
        "realized_volatility": realized_volatility,
        "dispersion_metric": dispersion_metric,
        "trend_slope_proxy": slope_proxy,
        "confidence_score": confidence,
        "input_path": str(resolved_input),
        "price_column": value_column,
        "timestamp_column": timestamp_column,
        "used_equity_curve_proxy": resolved_input.name == "paper_equity_curve.csv",
    }
    payload = {
        "schema_version": MARKET_REGIME_SCHEMA_VERSION,
        "generated_at": generated_at,
        "input_path": str(resolved_input),
        "policy": asdict(policy),
        "history": [row],
        "latest": row,
    }

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    json_path = output_path / "market_regime.json"
    csv_path = output_path / "market_regime.csv"
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    pd.DataFrame([row]).to_csv(csv_path, index=False)
    return {
        "market_regime_json_path": str(json_path),
        "market_regime_csv_path": str(csv_path),
        "regime_label": regime_label,
        "confidence_score": confidence,
        "latest": row,
    }


def load_market_regime(path_or_dir: str | Path) -> dict[str, Any]:
    path = Path(path_or_dir)
    if path.is_dir():
        path = path / "market_regime.json"
    payload = _safe_read_json(path)
    if not payload:
        raise FileNotFoundError(f"Market regime artifact not found or invalid: {path}")
    return payload

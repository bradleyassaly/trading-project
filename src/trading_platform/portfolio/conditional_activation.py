from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.portfolio.strategy_portfolio import load_strategy_portfolio
from trading_platform.regime.service import load_market_regime


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


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


def _safe_read_csv(path: str | Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    file_path = Path(path)
    if not file_path.exists() or file_path.is_dir():
        return pd.DataFrame()
    try:
        return pd.read_csv(file_path)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame()


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


def _write_payload(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")
    return path


@dataclass(frozen=True)
class ConditionalActivationConfig:
    evaluate_conditional_activation: bool = False
    activation_context_sources: list[str] = field(default_factory=lambda: ["regime", "benchmark_context"])
    include_inactive_conditionals_in_output: bool = True


def _normalize_activation_sources(sources: list[str] | None) -> list[str]:
    allowed_sources = {"regime", "benchmark_context", "sub_universe"}
    normalized = [str(source).strip() for source in (sources or []) if str(source).strip()]
    if not normalized:
        return ["regime", "benchmark_context"]
    return [source for source in normalized if source in allowed_sources]


def _resolve_context_path(path_or_dir: str | Path | None, filename: str) -> Path | None:
    if path_or_dir is None:
        return None
    path = Path(path_or_dir)
    if path.is_dir():
        candidate = path / filename
        return candidate if candidate.exists() else None
    return path if path.exists() else None


def _derive_dispersion_regime(dispersion_value: float | None) -> str | None:
    if dispersion_value is None:
        return None
    return "high_dispersion" if dispersion_value > 0.02 else "low_dispersion"


def _infer_risk_label(regime_payload: dict[str, Any]) -> str | None:
    latest = dict(regime_payload.get("latest") or {})
    for field in ("short_return", "long_return"):
        value = _safe_float(latest.get(field))
        if value is not None:
            return "risk_on" if value >= 0.0 else "risk_off"
    trend_regime = str(latest.get("trend_regime") or "").strip().lower()
    if trend_regime == "uptrend":
        return "risk_on"
    if trend_regime == "downtrend":
        return "risk_off"
    regime_label = str(latest.get("regime_label") or "").strip().lower()
    if regime_label in {"trend", "uptrend"}:
        return "risk_on"
    if regime_label in {"downtrend", "mean_reversion"}:
        return "risk_off"
    return None


def _load_regime_context(
    *,
    market_regime_path: str | Path | None,
    regime_labels_path: str | Path | None,
) -> dict[str, Any]:
    latest_labels = _safe_read_csv(_resolve_context_path(regime_labels_path, "regime_labels_by_date.csv"))
    if not latest_labels.empty:
        latest_row = latest_labels.sort_values("timestamp").iloc[-1].to_dict()
        regime_key = str(latest_row.get("regime_key") or "").strip() or None
        return {
            "source": str(_resolve_context_path(regime_labels_path, "regime_labels_by_date.csv")),
            "regime_key": regime_key,
            "regime_label": regime_key,
            "volatility_regime": latest_row.get("volatility_regime"),
            "trend_regime": latest_row.get("trend_regime"),
            "dispersion_regime": latest_row.get("dispersion_regime"),
            "available_labels": [label for label in [regime_key, latest_row.get("volatility_regime"), latest_row.get("trend_regime"), latest_row.get("dispersion_regime")] if label],
        }

    resolved_market_regime = _resolve_context_path(market_regime_path, "market_regime.json")
    if resolved_market_regime is None:
        return {"source": None, "available_labels": []}
    payload = load_market_regime(resolved_market_regime)
    latest = dict(payload.get("latest") or {})
    dispersion_regime = latest.get("dispersion_regime") or _derive_dispersion_regime(_safe_float(latest.get("dispersion_metric")))
    regime_key = None
    if latest.get("volatility_regime") and latest.get("trend_regime") and dispersion_regime:
        regime_key = f"{latest['volatility_regime']}|{latest['trend_regime']}|{dispersion_regime}"
    return {
        "source": str(resolved_market_regime),
        "regime_key": regime_key,
        "regime_label": latest.get("regime_label"),
        "volatility_regime": latest.get("volatility_regime"),
        "trend_regime": latest.get("trend_regime"),
        "dispersion_regime": dispersion_regime,
        "risk_label": _infer_risk_label(payload),
        "available_labels": [
            label
            for label in [
                regime_key,
                latest.get("regime_label"),
                latest.get("volatility_regime"),
                latest.get("trend_regime"),
                dispersion_regime,
            ]
            if label
        ],
    }


def _load_benchmark_context(
    *,
    metadata_dir: str | Path | None,
    regime_context: dict[str, Any],
) -> dict[str, Any]:
    path = _resolve_context_path(metadata_dir, "universe_enrichment.csv")
    frame = _safe_read_csv(path)
    if frame.empty:
        return {"source": str(path) if path else None, "benchmark_context_label": None}
    if "benchmark_context_label" in frame.columns and frame["benchmark_context_label"].notna().any():
        label = str(frame["benchmark_context_label"].dropna().astype(str).mode().iloc[0]).strip()
        return {"source": str(path), "benchmark_context_label": label}
    relative = pd.to_numeric(frame.get("relative_strength_20"), errors="coerce").dropna()
    if relative.empty:
        return {"source": str(path), "benchmark_context_label": None}
    risk_label = regime_context.get("risk_label") or "risk_on"
    relative_label = "outperform" if float(relative.median()) >= 0.0 else "lagging"
    breadth_label = "broad" if float((relative >= 0.0).mean()) >= 0.5 else "narrow"
    return {
        "source": str(path),
        "benchmark_context_label": f"{risk_label}_{relative_label}_{breadth_label}",
        "relative_strength_median": float(relative.median()),
        "breadth_positive_ratio": float((relative >= 0.0).mean()),
    }


def _load_sub_universe_context(*, metadata_dir: str | Path | None) -> dict[str, Any]:
    path = _resolve_context_path(metadata_dir, "sub_universe_snapshot.csv")
    frame = _safe_read_csv(path)
    if frame.empty:
        return {"source": str(path) if path else None, "sub_universe_ids": []}
    label_column = next((name for name in ("sub_universe_id", "sub_universe", "sub_universe_label") if name in frame.columns), None)
    if label_column is None:
        return {"source": str(path), "sub_universe_ids": []}
    ids = sorted(
        {
            str(value).strip()
            for value in frame[label_column].dropna().astype(str).tolist()
            if str(value).strip()
        }
    )
    return {"source": str(path), "sub_universe_ids": ids}


def build_activation_context(
    *,
    market_regime_path: str | Path | None = None,
    regime_labels_path: str | Path | None = None,
    metadata_dir: str | Path | None = None,
    activation_context_sources: list[str] | None = None,
) -> dict[str, Any]:
    enabled_sources = set(_normalize_activation_sources(activation_context_sources))
    regime_context = (
        _load_regime_context(
            market_regime_path=market_regime_path,
            regime_labels_path=regime_labels_path,
        )
        if "regime" in enabled_sources or "benchmark_context" in enabled_sources
        else {"source": None, "available_labels": []}
    )
    benchmark_context = (
        _load_benchmark_context(metadata_dir=metadata_dir, regime_context=regime_context)
        if "benchmark_context" in enabled_sources
        else {"source": None, "benchmark_context_label": None}
    )
    sub_universe_context = (
        _load_sub_universe_context(metadata_dir=metadata_dir)
        if "sub_universe" in enabled_sources
        else {"source": None, "sub_universe_ids": []}
    )
    return {
        "generated_at": _now_utc(),
        "activation_context_sources": sorted(enabled_sources),
        "regime": regime_context,
        "benchmark_context": benchmark_context,
        "sub_universe": sub_universe_context,
    }


def _match_condition(
    condition: dict[str, Any],
    context: dict[str, Any],
    *,
    activation_context_sources: set[str],
) -> tuple[bool, str]:
    condition_id = str(condition.get("condition_id") or "").strip()
    condition_type = str(condition.get("condition_type") or "").strip()
    if not condition_type:
        return True, "no_condition_type"
    if condition_type not in activation_context_sources:
        return False, f"context_source_disabled:{condition_type}"
    if condition_type == "regime":
        label = condition_id.split("::", 1)[1] if "::" in condition_id else condition_id
        available = set(context.get("regime", {}).get("available_labels", []))
        matched = label in available
        return matched, f"regime={label}" if matched else f"regime_mismatch:{label}"
    if condition_type == "benchmark_context":
        label = condition_id.split("::", 1)[1] if "::" in condition_id else condition_id
        current = str(context.get("benchmark_context", {}).get("benchmark_context_label") or "").strip()
        matched = bool(label) and label == current
        return matched, f"benchmark_context={label}" if matched else f"benchmark_context_mismatch:{label}"
    if condition_type == "sub_universe":
        label = condition_id.split("::", 1)[1] if "::" in condition_id else condition_id
        current_ids = set(context.get("sub_universe", {}).get("sub_universe_ids", []))
        matched = label in current_ids
        return matched, f"sub_universe={label}" if matched else f"sub_universe_mismatch:{label}"
    return False, f"unsupported_condition_type:{condition_type}"


def activate_strategy_portfolio(
    *,
    portfolio_path: str | Path,
    output_dir: str | Path,
    config: ConditionalActivationConfig | None = None,
    market_regime_path: str | Path | None = None,
    regime_labels_path: str | Path | None = None,
    metadata_dir: str | Path | None = None,
) -> dict[str, Any]:
    portfolio_payload = load_strategy_portfolio(portfolio_path)
    policy = dict(portfolio_payload.get("policy") or {})
    active_config = config or ConditionalActivationConfig(
        evaluate_conditional_activation=bool(policy.get("evaluate_conditional_activation", False)),
        activation_context_sources=list(policy.get("activation_context_sources") or ["regime", "benchmark_context"]),
        include_inactive_conditionals_in_output=bool(policy.get("include_inactive_conditionals_in_output", True)),
    )
    normalized_sources = _normalize_activation_sources(active_config.activation_context_sources)
    context = build_activation_context(
        market_regime_path=market_regime_path,
        regime_labels_path=regime_labels_path,
        metadata_dir=metadata_dir,
        activation_context_sources=normalized_sources,
    )
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    selected_rows = list(portfolio_payload.get("selected_strategies", []))
    shadow_rows = list(portfolio_payload.get("shadow_strategies", []))
    activated_rows: list[dict[str, Any]] = []
    for row in selected_rows:
        conditions = list(row.get("activation_conditions") or [])
        promotion_variant = str(row.get("promotion_variant") or "unconditional")
        if promotion_variant != "conditional" or not conditions or not active_config.evaluate_conditional_activation:
            activated_rows.append(
                {
                    **row,
                    "is_active": True if promotion_variant != "conditional" or not conditions else False,
                    "activation_state": "active" if promotion_variant != "conditional" or not conditions else row.get("activation_state"),
                    "activation_reason": "unconditional_always_active" if promotion_variant != "conditional" or not conditions else "conditional_activation_not_evaluated",
                    "matched_conditions": [] if promotion_variant != "conditional" else [],
                    "unmatched_conditions": [] if promotion_variant != "conditional" else list(conditions),
                }
            )
            continue
        matched_conditions: list[dict[str, Any]] = []
        unmatched_conditions: list[dict[str, Any]] = []
        reasons: list[str] = []
        for condition in conditions:
            matched, reason = _match_condition(
                condition,
                context,
                activation_context_sources=set(normalized_sources),
            )
            reasons.append(reason)
            if matched:
                matched_conditions.append(condition)
            else:
                unmatched_conditions.append(condition)
        is_active = not unmatched_conditions
        activated_rows.append(
            {
                **row,
                "is_active": is_active,
                "activation_state": "active" if is_active else "inactive",
                "activation_reason": "; ".join(reasons),
                "matched_conditions": matched_conditions,
                "unmatched_conditions": unmatched_conditions,
            }
        )

    if active_config.include_inactive_conditionals_in_output:
        emitted_rows = list(activated_rows)
    else:
        emitted_rows = [row for row in activated_rows if bool(row.get("is_active"))]

    active_rows = [row for row in activated_rows if bool(row.get("is_active"))]
    inactive_conditional_rows = [
        row for row in activated_rows if str(row.get("promotion_variant") or "") == "conditional" and not bool(row.get("is_active"))
    ]
    summary = {
        "total_rows": len(emitted_rows),
        "active_row_count": len(active_rows),
        "activated_unconditional_count": sum(
            1 for row in activated_rows if str(row.get("promotion_variant") or "unconditional") != "conditional" and bool(row.get("is_active"))
        ),
        "activated_conditional_count": sum(
            1 for row in activated_rows if str(row.get("promotion_variant") or "") == "conditional" and bool(row.get("is_active"))
        ),
        "inactive_conditional_count": len(inactive_conditional_rows),
        "shadow_row_count": len(shadow_rows),
    }
    payload = {
        "schema_version": 1,
        "generated_at": _now_utc(),
        "source_portfolio_path": str(Path(portfolio_path)),
        "activation_config": asdict(active_config),
        "context_snapshot": context,
        "summary": summary,
        "strategies": emitted_rows,
        "active_strategies": active_rows,
        "shadow_strategies": shadow_rows,
    }
    json_path = _write_payload(output_path / "activated_strategy_portfolio.json", payload)
    csv_path = output_path / "activated_strategy_portfolio.csv"
    pd.DataFrame(emitted_rows).to_csv(csv_path, index=False)
    return {
        "activated_strategy_portfolio_json_path": str(json_path),
        "activated_strategy_portfolio_csv_path": str(csv_path),
        "active_count": len(active_rows),
        "activated_conditional_count": summary["activated_conditional_count"],
        "inactive_conditional_count": summary["inactive_conditional_count"],
    }


def load_activated_strategy_portfolio(path_or_dir: str | Path) -> dict[str, Any]:
    path = Path(path_or_dir)
    if path.is_dir():
        path = path / "activated_strategy_portfolio.json"
    payload = _safe_read_json(path)
    if not payload:
        raise FileNotFoundError(f"Activated strategy portfolio artifact not found or invalid: {path}")
    return payload

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _safe_read_json(path_or_dir: str | Path) -> dict[str, Any]:
    path = Path(path_or_dir)
    if path.is_dir():
        path = path / "experiment_campaign_summary.json"
    if not path.exists():
        raise FileNotFoundError(f"Campaign summary artifact not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _simpler_variant_name(variants: list[dict[str, Any]]) -> str:
    def score(name: str) -> tuple[int, str]:
        lowered = name.lower()
        simple_markers = ["off", "static", "loose", "baseline"]
        complex_markers = ["on", "adaptive", "regime", "strict"]
        complexity = sum(marker in lowered for marker in complex_markers) - sum(marker in lowered for marker in simple_markers)
        return complexity, name

    return sorted((str(variant.get("variant_name")) for variant in variants), key=score)[0]


def _winner(variants: list[dict[str, Any]], metric: str, *, maximize: bool) -> list[str]:
    values = [(str(variant.get("variant_name")), _coerce_float(variant.get(metric))) for variant in variants]
    clean = [(name, value) for name, value in values if value is not None]
    if not clean:
        return []
    best = max(value for _, value in clean) if maximize else min(value for _, value in clean)
    return sorted(name for name, value in clean if value == best)


def _warning_burden(variant: dict[str, Any]) -> int:
    return int(variant.get("warning_count", 0) or 0) + int(variant.get("kill_switch_count", 0) or 0)


def _recommend_campaign(campaign_name: str, variants: list[dict[str, Any]]) -> dict[str, Any]:
    compared_variants = [str(item.get("variant_name")) for item in variants]
    baseline = _simpler_variant_name(variants)
    winner_by_return = _winner(variants, "total_return", maximize=True)
    winner_by_sharpe = _winner(variants, "sharpe", maximize=True)
    winner_by_drawdown = _winner(variants, "max_drawdown", maximize=False)
    burden_values = {str(variant.get("variant_name")): _warning_burden(variant) for variant in variants}
    min_burden = min(burden_values.values()) if burden_values else 0
    winner_by_warning = sorted(name for name, value in burden_values.items() if value == min_burden)

    by_name = {str(variant.get("variant_name")): variant for variant in variants}
    caveats: list[str] = []
    reasons: list[str] = []
    min_runs = min(int(variant.get("run_count", 0) or 0) for variant in variants) if variants else 0
    if min_runs < 2:
        caveats.append("insufficient_evidence:min_run_count_below_2")

    best_sharpe = winner_by_sharpe[0] if len(winner_by_sharpe) == 1 else None
    best_return = winner_by_return[0] if len(winner_by_return) == 1 else None
    recommended = baseline

    if best_sharpe:
        candidate = by_name[best_sharpe]
        baseline_variant = by_name[baseline]
        sharpe_diff = (_coerce_float(candidate.get("sharpe")) or 0.0) - (_coerce_float(baseline_variant.get("sharpe")) or 0.0)
        drawdown_diff = (_coerce_float(candidate.get("max_drawdown")) or 0.0) - (_coerce_float(baseline_variant.get("max_drawdown")) or 0.0)
        burden_diff = _warning_burden(candidate) - _warning_burden(baseline_variant)
        if sharpe_diff >= 0.1 and drawdown_diff <= 0.02 and burden_diff <= 1:
            recommended = best_sharpe
            reasons.append("preferred_higher_sharpe_without_material_drawdown_or_warning_penalty")
        elif sharpe_diff < 0.1:
            caveats.append("negligible_sharpe_difference")

    if recommended == baseline and best_return and best_return != baseline:
        candidate = by_name[best_return]
        baseline_variant = by_name[baseline]
        return_diff = (_coerce_float(candidate.get("total_return")) or 0.0) - (_coerce_float(baseline_variant.get("total_return")) or 0.0)
        drawdown_diff = (_coerce_float(candidate.get("max_drawdown")) or 0.0) - (_coerce_float(baseline_variant.get("max_drawdown")) or 0.0)
        burden_diff = _warning_burden(candidate) - _warning_burden(baseline_variant)
        if return_diff >= 0.01 and drawdown_diff <= 0.02 and burden_diff <= 1:
            recommended = best_return
            reasons.append("preferred_higher_return_without_material_drawdown_or_warning_penalty")
        elif abs(return_diff) < 0.01:
            caveats.append("negligible_return_difference")

    if recommended != baseline:
        candidate = by_name[recommended]
        baseline_variant = by_name[baseline]
        if _warning_burden(candidate) > _warning_burden(baseline_variant) + 1:
            recommended = baseline
            caveats.append("candidate_rejected_for_material_warning_or_kill_switch_burden")
        elif (_coerce_float(candidate.get("max_drawdown")) or 0.0) > (_coerce_float(baseline_variant.get("max_drawdown")) or 0.0) + 0.02:
            recommended = baseline
            caveats.append("candidate_rejected_for_material_drawdown_penalty")

    if not reasons:
        reasons.append("preferred_simpler_baseline_when_differences_were_negligible_or_evidence_was_weak")

    confidence = "high"
    if caveats:
        confidence = "low" if any("insufficient_evidence" in caveat for caveat in caveats) else "medium"
    elif min_runs < 3:
        confidence = "medium"

    return {
        "campaign_name": campaign_name,
        "compared_variants": compared_variants,
        "winner_by_return": winner_by_return,
        "winner_by_sharpe": winner_by_sharpe,
        "winner_by_drawdown": winner_by_drawdown,
        "winner_by_warning_burden": winner_by_warning,
        "overall_recommended_default": recommended,
        "confidence_level": confidence,
        "caveats": caveats,
        "reasons": reasons,
    }


def _read_config_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    suffix = path.suffix.lower()
    if suffix == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
    if suffix in {".yaml", ".yml"}:
        if yaml is None:
            raise ImportError("PyYAML is required for YAML config files. Install with `pip install pyyaml`.")
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        return payload or {}
    raise ValueError(f"Unsupported config file type: {suffix}")


def _write_config_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix.lower()
    if suffix == ".json":
        path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        return
    if suffix in {".yaml", ".yml"}:
        if yaml is None:
            raise ImportError("PyYAML is required for YAML config files. Install with `pip install pyyaml`.")
        path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
        return
    raise ValueError(f"Unsupported config file type: {suffix}")


def _apply_recommended_defaults(base_config: dict[str, Any], decisions: list[dict[str, Any]]) -> dict[str, Any]:
    payload = dict(base_config)
    payload["feature_flags"] = dict(payload.get("feature_flags") or {})
    payload["stages"] = dict(payload.get("stages") or {})
    payload["recommended_defaults_from_experiments"] = {
        "generated_at": _now_utc(),
        "decisions": decisions,
    }
    for decision in decisions:
        name = str(decision.get("campaign_name") or "")
        recommended = str(decision.get("overall_recommended_default") or "")
        lowered_campaign = name.lower()
        lowered_recommended = recommended.lower()
        if "regime" in lowered_campaign:
            enabled = "off" not in lowered_recommended and "baseline" not in lowered_recommended
            payload["feature_flags"]["regime"] = enabled
            payload["stages"]["regime"] = enabled
        elif "adaptive" in lowered_campaign:
            enabled = "off" not in lowered_recommended and "static" not in lowered_recommended
            payload["feature_flags"]["adaptive"] = enabled
            payload["stages"]["adaptive_allocation"] = enabled
        elif "governance" in lowered_campaign:
            payload["feature_flags"]["governance"] = True
            payload["strategy_governance_policy_config_path"] = (
                "configs/strategy_governance_strict.yaml"
                if "strict" in lowered_recommended
                else "configs/strategy_governance_loose.yaml"
            )
    return payload


def recommend_experiment_defaults(
    *,
    campaign_summary_path: str | Path,
    output_dir: str | Path,
    write_config_path: str | Path | None = None,
    base_config_path: str | Path | None = None,
) -> dict[str, Any]:
    payload = _safe_read_json(campaign_summary_path)
    variants = payload.get("variants", [])
    if not variants:
        raise ValueError("Campaign summary does not contain any variants")

    grouped: dict[str, list[dict[str, Any]]] = {}
    for variant in variants:
        grouped.setdefault(str(variant.get("experiment_name") or "unknown_campaign"), []).append(variant)

    decisions = [_recommend_campaign(campaign_name, rows) for campaign_name, rows in sorted(grouped.items())]
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    summary_payload = {
        "generated_at": _now_utc(),
        "source_campaign_summary_path": str(Path(campaign_summary_path)),
        "decision_count": len(decisions),
        "decisions": decisions,
    }
    json_path = output_path / "experiment_decision_summary.json"
    csv_path = output_path / "experiment_decision_summary.csv"
    md_path = output_path / "experiment_decision_summary.md"
    json_path.write_text(json.dumps(summary_payload, indent=2, default=str), encoding="utf-8")
    pd.DataFrame(decisions).to_csv(csv_path, index=False)
    md_lines = [
        "# Experiment Decision Summary",
        "",
        f"- Decisions: `{len(decisions)}`",
        "",
        "## Recommendations",
    ]
    for decision in decisions:
        md_lines.append(
            f"- `{decision['campaign_name']}`: recommend `{decision['overall_recommended_default']}` "
            f"(confidence=`{decision['confidence_level']}`)"
        )
        if decision["caveats"]:
            md_lines.append(f"  caveats: {', '.join(decision['caveats'])}")
        if decision["reasons"]:
            md_lines.append(f"  reasons: {', '.join(decision['reasons'])}")
    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    config_path: str | None = None
    if write_config_path is not None:
        if base_config_path is None:
            raise ValueError("base_config_path is required when write_config_path is provided")
        recommended_payload = _apply_recommended_defaults(_read_config_file(Path(base_config_path)), decisions)
        _write_config_file(Path(write_config_path), recommended_payload)
        config_path = str(write_config_path)

    return {
        "experiment_decision_summary_json_path": str(json_path),
        "experiment_decision_summary_csv_path": str(csv_path),
        "experiment_decision_summary_md_path": str(md_path),
        "recommended_config_path": config_path,
        "decisions": decisions,
    }

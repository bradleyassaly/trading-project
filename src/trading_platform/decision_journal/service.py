from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.decision_journal.models import (
    CandidateEvaluation,
    DecisionJournalBundle,
    ExecutionDecisionRecord,
    ExitDecisionRecord,
    PortfolioSelectionDecision,
    ScreenCheckResult,
    SignalBreakdown,
    SizingDecision,
    TradeDecision,
    TradeDecisionRecord,
    TradeLifecycleRecord,
)


def _decision_id(*parts: object) -> str:
    return "|".join(str(part) for part in parts if part not in (None, ""))


def _rank_map(score_map: dict[str, float | None]) -> tuple[dict[str, int], dict[str, float]]:
    valid = [(symbol, float(score)) for symbol, score in score_map.items() if score is not None]
    valid.sort(key=lambda item: (-item[1], item[0]))
    ranks: dict[str, int] = {}
    percentiles: dict[str, float] = {}
    count = len(valid)
    for index, (symbol, _score) in enumerate(valid, start=1):
        ranks[symbol] = index
        percentiles[symbol] = (count - index) / max(count - 1, 1) if count > 1 else 1.0
    return ranks, percentiles


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None




def summarize_entry_reason(record: TradeDecisionRecord | dict[str, Any]) -> str:
    payload = record.to_dict() if hasattr(record, "to_dict") else dict(record)
    if payload.get("candidate_status") == "selected":
        parts = ["selected"]
        if payload.get("final_signal_score") is not None:
            parts.append(f"score={payload['final_signal_score']}")
        if payload.get("target_weight_post_constraint") is not None:
            parts.append(f"target_weight={payload['target_weight_post_constraint']}")
        return " | ".join(parts)
    return str(payload.get("rejection_reason") or payload.get("entry_reason_summary") or "no explicit entry rationale")


def summarize_exit_reason(record: ExitDecisionRecord | dict[str, Any]) -> str:
    payload = record.to_dict() if hasattr(record, "to_dict") else dict(record)
    parts = [str(payload.get("exit_trigger_type") or "exit")]
    if payload.get("exit_reason_summary"):
        parts.append(str(payload["exit_reason_summary"]))
    return " | ".join(parts)


def summarize_selection_context(record: PortfolioSelectionDecision | dict[str, Any]) -> str:
    payload = record.to_dict() if hasattr(record, "to_dict") else dict(record)
    parts = [str(payload.get("selection_status") or "unknown")]
    if payload.get("rank") is not None:
        parts.append(f"rank={payload['rank']}")
    if payload.get("candidate_count") is not None:
        parts.append(f"candidates={payload['candidate_count']}")
    if payload.get("rejection_reason"):
        parts.append(str(payload["rejection_reason"]))
    return " | ".join(parts)


def summarize_sizing_context(record: SizingDecision | dict[str, Any]) -> str:
    payload = record.to_dict() if hasattr(record, "to_dict") else dict(record)
    parts = []
    if payload.get("target_weight_pre_constraint") is not None:
        parts.append(f"pre={payload['target_weight_pre_constraint']}")
    if payload.get("target_weight_post_constraint") is not None:
        parts.append(f"post={payload['target_weight_post_constraint']}")
    if payload.get("target_quantity") is not None:
        parts.append(f"qty={payload['target_quantity']}")
    return " | ".join(parts) or "no sizing context"


def _trade_decision_side(candidate_row: dict[str, Any]) -> str:
    candidate_action = str(candidate_row.get("action") or candidate_row.get("action_type") or "").strip().lower()
    target_weight = _safe_float(candidate_row.get("ev_adjusted_target_weight"))
    if target_weight is None:
        target_weight = _safe_float(candidate_row.get("target_weight"))
    current_weight = _safe_float(candidate_row.get("current_weight")) or 0.0
    if target_weight is not None:
        if target_weight > current_weight:
            return "BUY"
        if target_weight < current_weight:
            return "SELL"
    if candidate_action in {"entry", "increase", "buy", "long"}:
        return "BUY"
    if candidate_action in {"exit", "reduction", "sell", "short"}:
        return "SELL"
    return "HOLD"


def _trade_decision_veto_reasons(candidate_row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    for value in (
        candidate_row.get("skip_reason"),
        candidate_row.get("action_reason"),
        candidate_row.get("ev_gate_decision"),
        candidate_row.get("ev_model_fallback_reason"),
        candidate_row.get("ev_reliability_fallback_reason"),
    ):
        reason = str(value or "").strip()
        if reason and reason not in reasons:
            reasons.append(reason)
    return reasons


def _trade_decision_rationale_fields(
    *,
    candidate_row: dict[str, Any],
    vetoed: bool,
    veto_reasons: list[str],
) -> dict[str, Any]:
    rationale_labels: list[str] = []
    for value in (
        candidate_row.get("candidate_status"),
        candidate_row.get("candidate_outcome"),
        candidate_row.get("candidate_stage"),
        candidate_row.get("band_decision"),
        candidate_row.get("ev_gate_decision"),
        candidate_row.get("action_reason"),
    ):
        label = str(value or "").strip()
        if label and label not in rationale_labels:
            rationale_labels.append(label)
    for reason in veto_reasons:
        if reason not in rationale_labels:
            rationale_labels.append(reason)

    summary_parts: list[str] = []
    status = str(candidate_row.get("candidate_status") or "").strip()
    outcome = str(candidate_row.get("candidate_outcome") or "").strip()
    action_reason = str(candidate_row.get("action_reason") or "").strip()
    skip_reason = str(candidate_row.get("skip_reason") or "").strip()
    band_decision = str(candidate_row.get("band_decision") or "").strip()
    if status:
        summary_parts.append(status)
    if outcome and outcome != status:
        summary_parts.append(outcome)
    if action_reason and action_reason not in summary_parts:
        summary_parts.append(action_reason)
    if vetoed:
        for reason in veto_reasons:
            if reason and reason not in summary_parts:
                summary_parts.append(reason)
    elif skip_reason and skip_reason not in summary_parts:
        summary_parts.append(skip_reason)
    if band_decision and band_decision not in summary_parts:
        summary_parts.append(band_decision)
    rationale_summary = " | ".join(summary_parts) if summary_parts else "trade_decision_contract"

    rationale_context = {
        "candidate_status": candidate_row.get("candidate_status"),
        "candidate_outcome": candidate_row.get("candidate_outcome"),
        "candidate_stage": candidate_row.get("candidate_stage"),
        "action_reason": candidate_row.get("action_reason"),
        "skip_reason": candidate_row.get("skip_reason"),
        "band_decision": candidate_row.get("band_decision"),
        "ev_gate_decision": candidate_row.get("ev_gate_decision"),
        "signal_source": candidate_row.get("signal_source"),
        "signal_family": candidate_row.get("signal_family"),
        "veto_reason_count": len(veto_reasons),
        "has_veto": vetoed,
    }
    rationale_context = {
        str(key): value
        for key, value in rationale_context.items()
        if value not in (None, "", [])
    }

    return {
        "rationale_summary": rationale_summary,
        "rationale_labels": rationale_labels,
        "rationale_context": rationale_context,
    }


def _trade_decision_sort_key(candidate_row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(candidate_row.get("date") or candidate_row.get("timestamp") or ""),
        str(candidate_row.get("symbol") or ""),
        str(candidate_row.get("strategy_id") or ""),
        str(candidate_row.get("decision_id") or candidate_row.get("candidate_id") or ""),
    )


def _resolve_trade_decision_ev_fields(
    *,
    candidate_row: dict[str, Any],
    prediction_row: dict[str, Any],
) -> dict[str, Any]:
    predicted_return = _safe_float(candidate_row.get("predicted_return"))
    predicted_return_value_source = "candidate_row.predicted_return" if predicted_return is not None else None
    if predicted_return is None:
        predicted_return = _safe_float(prediction_row.get("expected_net_return"))
        if predicted_return is not None:
            predicted_return_value_source = "prediction_row.expected_net_return"
    if predicted_return is None:
        predicted_return = _safe_float(candidate_row.get("expected_net_return"))
        if predicted_return is not None:
            predicted_return_value_source = "candidate_row.expected_net_return"
    if predicted_return is None:
        predicted_return = _safe_float(candidate_row.get("ev_weighting_score"))
        if predicted_return is not None:
            predicted_return_value_source = "candidate_row.ev_weighting_score"
    if predicted_return is None:
        predicted_return = 0.0
        predicted_return_value_source = "default_zero"

    expected_cost = _safe_float(prediction_row.get("expected_cost"))
    expected_cost_source = "prediction_row.expected_cost" if expected_cost is not None else None
    if expected_cost is None:
        expected_cost = _safe_float(candidate_row.get("expected_cost"))
        if expected_cost is not None:
            expected_cost_source = "candidate_row.expected_cost"
    if expected_cost is None:
        expected_cost = _safe_float(candidate_row.get("estimated_execution_cost_pct"))
        if expected_cost is not None:
            expected_cost_source = "candidate_row.estimated_execution_cost_pct"
    if expected_cost is None:
        expected_cost = 0.0
        expected_cost_source = "default_zero"

    expected_value_net = _safe_float(prediction_row.get("expected_net_return"))
    expected_value_net_source = "prediction_row.expected_net_return" if expected_value_net is not None else None
    expected_value_net_derived = False
    if expected_value_net is None:
        expected_value_net = _safe_float(candidate_row.get("expected_net_return"))
        if expected_value_net is not None:
            expected_value_net_source = "candidate_row.expected_net_return"
    if expected_value_net is None:
        expected_value_net = float(predicted_return)
        expected_value_net_source = f"derived_from.{predicted_return_value_source}"
        expected_value_net_derived = True

    expected_value_gross = _safe_float(prediction_row.get("expected_gross_return"))
    expected_value_gross_source = "prediction_row.expected_gross_return" if expected_value_gross is not None else None
    expected_value_gross_derived = False
    if expected_value_gross is None:
        expected_value_gross = _safe_float(candidate_row.get("expected_gross_return"))
        if expected_value_gross is not None:
            expected_value_gross_source = "candidate_row.expected_gross_return"
    if expected_value_gross is None:
        expected_value_gross = float(expected_value_net) + float(expected_cost)
        expected_value_gross_source = "derived_from.expected_value_net_plus_expected_cost"
        expected_value_gross_derived = True

    decomposition_status = "explicit"
    if expected_value_net_derived or expected_value_gross_derived:
        decomposition_status = "derived"
    elif "default_zero" in {predicted_return_value_source, expected_cost_source}:
        decomposition_status = "partial"

    return {
        "predicted_return": float(predicted_return),
        "predicted_return_value_source": predicted_return_value_source,
        "expected_cost": float(expected_cost),
        "expected_cost_source": expected_cost_source,
        "expected_value_net": float(expected_value_net),
        "expected_value_net_source": expected_value_net_source,
        "expected_value_net_derived": expected_value_net_derived,
        "expected_value_gross": float(expected_value_gross),
        "expected_value_gross_source": expected_value_gross_source,
        "expected_value_gross_derived": expected_value_gross_derived,
        "ev_decomposition_status": decomposition_status,
    }


def _resolve_trade_decision_quality_fields(
    *,
    candidate_row: dict[str, Any],
    prediction_row: dict[str, Any],
) -> dict[str, Any]:
    probability_positive = _safe_float(candidate_row.get("probability_positive"))
    probability_positive_source = "candidate_row.probability_positive" if probability_positive is not None else None
    if probability_positive is None:
        probability_positive = _safe_float(prediction_row.get("probability_positive"))
        if probability_positive is not None:
            probability_positive_source = "prediction_row.probability_positive"

    confidence_score = _safe_float(candidate_row.get("ev_confidence"))
    confidence_score_source = "candidate_row.ev_confidence" if confidence_score is not None else None
    if confidence_score is None:
        confidence_score = _safe_float(candidate_row.get("combined_confidence"))
        if confidence_score is not None:
            confidence_score_source = "candidate_row.combined_confidence"

    reliability_score = _safe_float(candidate_row.get("ev_reliability"))
    reliability_score_source = "candidate_row.ev_reliability" if reliability_score is not None else None
    if reliability_score is None:
        reliability_score = _safe_float(candidate_row.get("reliability_calibrated_score"))
        if reliability_score is not None:
            reliability_score_source = "candidate_row.reliability_calibrated_score"

    uncertainty_score = _safe_float(candidate_row.get("residual_std_final"))
    uncertainty_score_source = "candidate_row.residual_std_final" if uncertainty_score is not None else None
    if uncertainty_score is None:
        uncertainty_score = _safe_float(candidate_row.get("residual_std_used"))
        if uncertainty_score is not None:
            uncertainty_score_source = "candidate_row.residual_std_used"

    calibration_score = _safe_float(candidate_row.get("reliability_calibrated_score"))
    calibration_score_source = "candidate_row.reliability_calibrated_score" if calibration_score is not None else None
    if calibration_score is None:
        calibration_score = _safe_float(candidate_row.get("probability_positive"))
        if calibration_score is not None:
            calibration_score_source = "candidate_row.probability_positive"
    if calibration_score is None:
        calibration_score = _safe_float(prediction_row.get("probability_positive"))
        if calibration_score is not None:
            calibration_score_source = "prediction_row.probability_positive"

    return {
        "probability_positive": probability_positive,
        "probability_positive_source": probability_positive_source,
        "confidence_score": confidence_score,
        "confidence_score_source": confidence_score_source,
        "reliability_score": reliability_score,
        "reliability_score_source": reliability_score_source,
        "uncertainty_score": uncertainty_score,
        "uncertainty_score_source": uncertainty_score_source,
        "calibration_score": calibration_score,
        "calibration_score_source": calibration_score_source,
    }


def build_trade_decision_contracts(
    *,
    candidate_rows: list[dict[str, Any]] | None,
    prediction_rows: list[dict[str, Any]] | None = None,
    schema_version: str = "trade_decision_contract_v1",
) -> list[TradeDecision]:
    rows = [dict(row) for row in candidate_rows or []]
    if not rows:
        return []
    prediction_lookup = {
        str(row.get("symbol") or ""): dict(row)
        for row in prediction_rows or []
        if str(row.get("symbol") or "")
    }
    decisions: list[TradeDecision] = []
    for candidate_row in sorted(rows, key=_trade_decision_sort_key):
        symbol = str(candidate_row.get("symbol") or "")
        strategy_id = str(candidate_row.get("strategy_id") or "unknown_strategy")
        prediction_row = dict(prediction_lookup.get(symbol) or {})
        ev_fields = _resolve_trade_decision_ev_fields(
            candidate_row=candidate_row,
            prediction_row=prediction_row,
        )
        quality_fields = _resolve_trade_decision_quality_fields(
            candidate_row=candidate_row,
            prediction_row=prediction_row,
        )
        horizon_days = int(candidate_row.get("expected_horizon_days") or prediction_row.get("horizon_days") or 1)
        side = _trade_decision_side(candidate_row)
        vetoed = str(candidate_row.get("candidate_outcome") or candidate_row.get("candidate_status") or "").lower() not in {
            "executed",
            "selected",
        }
        veto_reasons = _trade_decision_veto_reasons(candidate_row) if vetoed else []
        rationale_fields = _trade_decision_rationale_fields(
            candidate_row=candidate_row,
            vetoed=vetoed,
            veto_reasons=veto_reasons,
        )
        candidate_id = candidate_row.get("decision_id") or candidate_row.get("candidate_id")
        if candidate_id is None:
            candidate_id = _decision_id(
                schema_version,
                candidate_row.get("date") or candidate_row.get("timestamp") or "unknown_date",
                symbol,
                strategy_id,
            )
        decisions.append(
            TradeDecision(
                decision_id=str(candidate_id),
                timestamp=str(candidate_row.get("date") or candidate_row.get("timestamp") or ""),
                strategy_id=strategy_id,
                strategy_family=(
                    str(candidate_row.get("signal_family") or candidate_row.get("strategy_family") or "") or None
                ),
                candidate_id=str(candidate_row.get("candidate_id") or candidate_row.get("decision_id") or "") or None,
                instrument=symbol,
                side=side,
                horizon_days=horizon_days,
                predicted_return=float(ev_fields["predicted_return"]),
                expected_value_gross=float(ev_fields["expected_value_gross"]),
                expected_cost=float(ev_fields["expected_cost"]),
                expected_value_net=float(ev_fields["expected_value_net"]),
                probability_positive=quality_fields["probability_positive"],
                confidence_score=quality_fields["confidence_score"],
                reliability_score=quality_fields["reliability_score"],
                uncertainty_score=quality_fields["uncertainty_score"],
                calibration_score=quality_fields["calibration_score"],
                regime_label=(
                    str(candidate_row.get("regime_label") or candidate_row.get("reliability_target_type") or "")
                    or None
                ),
                sizing_signal=_safe_float(
                    candidate_row.get("ev_adjusted_target_weight", candidate_row.get("target_weight"))
                ),
                vetoed=vetoed,
                veto_reasons=veto_reasons,
                rationale_summary=rationale_fields["rationale_summary"],
                rationale_labels=rationale_fields["rationale_labels"],
                rationale_context=rationale_fields["rationale_context"],
                metadata={
                    "schema_version": schema_version,
                    "candidate_status": candidate_row.get("candidate_status"),
                    "candidate_outcome": candidate_row.get("candidate_outcome"),
                    "candidate_stage": candidate_row.get("candidate_stage"),
                    "signal_source": candidate_row.get("signal_source"),
                    "signal_family": candidate_row.get("signal_family"),
                    "predicted_return_semantics": "primary_trade_return_forecast",
                    "ev_gate_decision": candidate_row.get("ev_gate_decision"),
                    "predicted_return_source": candidate_row.get("predicted_return_source"),
                    "predicted_return_value_source": ev_fields["predicted_return_value_source"],
                    "probability_positive_source": quality_fields["probability_positive_source"],
                    "confidence_score_source": quality_fields["confidence_score_source"],
                    "reliability_score_source": quality_fields["reliability_score_source"],
                    "uncertainty_score_source": quality_fields["uncertainty_score_source"],
                    "calibration_score_source": quality_fields["calibration_score_source"],
                    "expected_cost_source": ev_fields["expected_cost_source"],
                    "expected_value_net_source": ev_fields["expected_value_net_source"],
                    "expected_value_net_derived": ev_fields["expected_value_net_derived"],
                    "expected_value_gross_source": ev_fields["expected_value_gross_source"],
                    "expected_value_gross_derived": ev_fields["expected_value_gross_derived"],
                    "ev_decomposition_status": ev_fields["ev_decomposition_status"],
                    "rationale_labels": list(rationale_fields["rationale_labels"]),
                    "rationale_context": dict(rationale_fields["rationale_context"]),
                    "veto_reason_count": len(veto_reasons),
                    "ev_model_type_requested": candidate_row.get("ev_model_type_requested"),
                    "ev_model_type_used": candidate_row.get("ev_model_type_used"),
                    "ev_model_fallback_reason": candidate_row.get("ev_model_fallback_reason"),
                    "ev_regression_prediction_available": candidate_row.get("ev_regression_prediction_available"),
                    "ev_reliability_status": candidate_row.get("ev_reliability_status"),
                    "ev_reliability_fallback_reason": candidate_row.get("ev_reliability_fallback_reason"),
                    "ev_reliability_model_fit_available": candidate_row.get("ev_reliability_model_fit_available"),
                    "was_filtered_by_confidence": candidate_row.get("was_filtered_by_confidence"),
                    "was_filtered_by_reliability": candidate_row.get("was_filtered_by_reliability"),
                },
            )
        )
    return decisions


def write_trade_decision_contract_artifacts(
    *,
    candidate_rows: list[dict[str, Any]] | None,
    output_dir: str | Path,
    prediction_rows: list[dict[str, Any]] | None = None,
    schema_version: str = "trade_decision_contract_v1",
) -> dict[str, Path]:
    decisions = build_trade_decision_contracts(
        candidate_rows=candidate_rows,
        prediction_rows=prediction_rows,
        schema_version=schema_version,
    )
    return write_trade_decision_contracts(
        decisions=decisions,
        output_dir=output_dir,
    )


def write_trade_decision_contracts(
    *,
    decisions: list[TradeDecision] | None,
    output_dir: str | Path,
) -> dict[str, Path]:
    if not decisions:
        return {}
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    json_name = "trade_decision_contracts_v1.json"
    csv_name = "trade_decision_contracts_v1.csv"
    json_path = output_path / json_name
    csv_path = output_path / csv_name
    json_path.write_text(json.dumps([row.to_dict() for row in decisions], indent=2, default=str), encoding="utf-8")
    pd.DataFrame([row.flat_dict() for row in decisions]).to_csv(csv_path, index=False)
    return {
        json_name.replace(".", "_"): json_path,
        csv_name.replace(".", "_"): csv_path,
    }


def _bundle_append(bundle: DecisionJournalBundle | None, **updates: list[Any]) -> DecisionJournalBundle:
    current = bundle or DecisionJournalBundle()
    return DecisionJournalBundle(
        candidate_evaluations=current.candidate_evaluations + list(updates.get("candidate_evaluations", [])),
        selection_decisions=current.selection_decisions + list(updates.get("selection_decisions", [])),
        sizing_decisions=current.sizing_decisions + list(updates.get("sizing_decisions", [])),
        trade_decisions=current.trade_decisions + list(updates.get("trade_decisions", [])),
        execution_decisions=current.execution_decisions + list(updates.get("execution_decisions", [])),
        exit_decisions=current.exit_decisions + list(updates.get("exit_decisions", [])),
        lifecycle_records=current.lifecycle_records + list(updates.get("lifecycle_records", [])),
        provenance_by_symbol={**current.provenance_by_symbol, **dict(updates.get("provenance_by_symbol", {}))},
    )


def build_candidate_journal_for_snapshot(
    *,
    timestamp: str,
    run_id: str | None,
    cycle_id: str | None,
    strategy_id: str | None,
    universe_id: str | None,
    base_universe_id: str | None = None,
    sub_universe_id: str | None = None,
    score_map: dict[str, float | None],
    latest_prices: dict[str, float],
    selected_weights: dict[str, float],
    scheduled_weights: dict[str, float],
    skipped_symbols: list[str] | None = None,
    skip_reasons: dict[str, str] | None = None,
    asset_return_map: dict[str, float | None] | None = None,
    selected_rejection_reasons: dict[str, str] | None = None,
    filtered_out_symbols: list[str] | None = None,
    filtered_reasons: dict[str, str] | None = None,
    universe_metadata_by_symbol: dict[str, dict[str, Any]] | None = None,
    metadata: dict[str, Any] | None = None,
) -> DecisionJournalBundle:
    skipped = set(skipped_symbols or [])
    filtered = set(filtered_out_symbols or [])
    score_map_full = dict(score_map)
    for symbol in skipped:
        score_map_full.setdefault(symbol, None)
    for symbol in filtered:
        score_map_full.setdefault(symbol, None)
    ranks, percentiles = _rank_map(score_map_full)
    selected_symbols = {symbol for symbol, weight in selected_weights.items() if abs(float(weight)) > 0.0}
    bundle = DecisionJournalBundle()
    candidate_count = len(score_map_full)
    provenance: dict[str, dict[str, Any]] = {}
    for symbol in sorted(score_map_full):
        score = score_map_full.get(symbol)
        rejection_reason = None
        status = "selected" if symbol in selected_symbols else "rejected"
        if symbol in filtered:
            rejection_reason = (filtered_reasons or {}).get(symbol, "filtered_out_before_ranking")
            status = "filtered_out"
        elif symbol in skipped:
            rejection_reason = (skip_reasons or {}).get(symbol, "symbol_skipped")
            status = "rejected"
        elif symbol not in selected_symbols:
            rejection_reason = (selected_rejection_reasons or {}).get(symbol) or "not_selected"
        feature_snapshot = {
            "latest_price": _safe_float(latest_prices.get(symbol)),
            "asset_return": _safe_float((asset_return_map or {}).get(symbol)),
        }
        if symbol in (universe_metadata_by_symbol or {}):
            for key, value in dict((universe_metadata_by_symbol or {}).get(symbol, {})).items():
                if key not in feature_snapshot:
                    feature_snapshot[key] = value
        checks = [
            ScreenCheckResult(
                "price_available",
                "pass" if feature_snapshot["latest_price"] is not None else "fail",
                feature_snapshot["latest_price"] is not None,
                value=feature_snapshot["latest_price"],
            ),
            ScreenCheckResult(
                "score_available", "pass" if score is not None else "fail", score is not None, value=score
            ),
        ]
        if symbol in filtered:
            checks.append(ScreenCheckResult("universe_eligibility", "fail", False, reason=rejection_reason))
        if symbol in skipped:
            checks.append(ScreenCheckResult("symbol_loaded", "fail", False, reason=rejection_reason))
        signal = SignalBreakdown(
            signal_name=str(strategy_id or "signal"),
            final_score=score,
            raw_components={"score": score, "asset_return": feature_snapshot["asset_return"]},
            transformed_components={
                "scheduled_target_weight": scheduled_weights.get(symbol),
                "effective_target_weight": selected_weights.get(symbol),
            },
            reason_labels=[status],
        )
        decision_id = _decision_id(run_id or cycle_id or timestamp, strategy_id or "strategy", symbol, "candidate")
        candidate = CandidateEvaluation(
            decision_id=decision_id,
            timestamp=timestamp,
            run_id=run_id,
            cycle_id=cycle_id,
            symbol=symbol,
            side="long" if (selected_weights.get(symbol, 0.0) or 0.0) >= 0 else "short",
            strategy_id=strategy_id,
            universe_id=universe_id,
            base_universe_id=base_universe_id or universe_id,
            sub_universe_id=sub_universe_id,
            candidate_status=status,
            final_signal_score=score,
            rank=ranks.get(symbol),
            rank_percentile=percentiles.get(symbol),
            rejection_reason=rejection_reason,
            selected_feature_values=feature_snapshot,
            signal_breakdown=signal,
            screening_checks=checks,
            metadata={
                **(metadata or {}),
                "selected_weight": selected_weights.get(symbol),
                "scheduled_weight": scheduled_weights.get(symbol),
            },
        )
        selection = PortfolioSelectionDecision(
            decision_id=_decision_id(run_id or cycle_id or timestamp, strategy_id or "strategy", symbol, "selection"),
            timestamp=timestamp,
            run_id=run_id,
            cycle_id=cycle_id,
            symbol=symbol,
            strategy_id=strategy_id,
            selection_status=status,
            selected=symbol in selected_symbols,
            final_signal_score=score,
            rank=ranks.get(symbol),
            rank_percentile=percentiles.get(symbol),
            candidate_count=candidate_count,
            selected_count=len(selected_symbols),
            target_weight_pre_constraint=_safe_float(scheduled_weights.get(symbol)),
            target_weight_post_constraint=_safe_float(selected_weights.get(symbol)),
            rejection_reason=rejection_reason,
            rationale_summary="selected_by_target_weight" if symbol in selected_symbols else rejection_reason,
            metadata=dict(metadata or {}),
        )
        sizing = SizingDecision(
            decision_id=_decision_id(run_id or cycle_id or timestamp, strategy_id or "strategy", symbol, "sizing"),
            timestamp=timestamp,
            run_id=run_id,
            cycle_id=cycle_id,
            symbol=symbol,
            strategy_id=strategy_id,
            side="long" if (selected_weights.get(symbol, 0.0) or 0.0) >= 0 else "short",
            target_weight_pre_constraint=_safe_float(scheduled_weights.get(symbol)),
            target_weight_post_constraint=_safe_float(selected_weights.get(symbol)),
            sizing_inputs={
                "scheduled_target_weight": scheduled_weights.get(symbol),
                "effective_target_weight": selected_weights.get(symbol),
            },
            rationale_summary="weight carried through target construction"
            if symbol in selected_symbols
            else "no target sizing assigned",
        )
        provenance[symbol] = {
            "decision_id": selection.decision_id,
            "strategy_id": strategy_id,
            "signal_score": score,
            "ranking_score": score,
            "universe_rank": ranks.get(symbol),
            "selection_status": status,
            "target_weight": _safe_float(selected_weights.get(symbol)),
            "reason": rejection_reason,
            "base_universe_id": base_universe_id or universe_id,
            "sub_universe_id": sub_universe_id,
            "signal_source": (metadata or {}).get("signal_source"),
            "signal_family": (metadata or {}).get("signal_family"),
        }
        bundle = _bundle_append(
            bundle,
            candidate_evaluations=[candidate],
            selection_decisions=[selection],
            sizing_decisions=[sizing],
            provenance_by_symbol={symbol: provenance[symbol]},
        )
    return bundle


def enrich_bundle_with_orders(
    bundle: DecisionJournalBundle | None,
    *,
    timestamp: str,
    run_id: str | None,
    cycle_id: str | None,
    strategy_id: str | None,
    universe_id: str | None,
    current_positions: dict[str, Any],
    latest_target_weights: dict[str, float],
    scheduled_target_weights: dict[str, float],
    latest_prices: dict[str, float],
    orders: list[Any],
    execution_payload: dict[str, Any] | None = None,
    reserve_cash_pct: float | None = None,
    portfolio_equity: float | None = None,
) -> DecisionJournalBundle:
    current = bundle or DecisionJournalBundle()
    trade_decisions: list[TradeDecisionRecord] = []
    sizing_updates: list[SizingDecision] = []
    execution_decisions: list[ExecutionDecisionRecord] = []
    exit_decisions: list[ExitDecisionRecord] = []
    lifecycle: list[TradeLifecycleRecord] = []
    for symbol in sorted(set(latest_target_weights) | set(getattr(current_positions, "keys", lambda: [])())):
        current_quantity = int(getattr(current_positions.get(symbol), "quantity", 0) or 0)
        target_weight = float(latest_target_weights.get(symbol, 0.0) or 0.0)
        scheduled_weight = float(scheduled_target_weights.get(symbol, 0.0) or 0.0)
        current_value = current_quantity * float(latest_prices.get(symbol, 0.0) or 0.0)
        current_weight = (current_value / float(portfolio_equity)) if portfolio_equity not in (None, 0) else None
        matched_order = next((order for order in orders if getattr(order, "symbol", None) == symbol), None)
        if matched_order is None and current_quantity == 0 and abs(target_weight) <= 1e-12:
            continue
        status = "held"
        reason = "already_held_no_rebalance_needed"
        side = None
        target_quantity = None
        if matched_order is not None:
            status = "selected" if str(getattr(matched_order, "side", "")).upper() == "BUY" else "exited"
            reason = getattr(matched_order, "reason", None) or (
                "rebalance_to_target" if status == "selected" else "rebalance_exit"
            )
            side = "long" if str(getattr(matched_order, "side", "")).upper() == "BUY" else "sell"
            target_quantity = int(getattr(matched_order, "target_quantity", 0) or 0)
        elif current_quantity > 0 and abs(target_weight) <= 1e-12:
            status = "exited"
            reason = current.provenance_by_symbol.get(symbol, {}).get("reason") or "target_weight_reduced_to_zero"
            side = "sell"
        elif current_quantity > 0:
            side = "hold"
        trade_decisions.append(
            TradeDecisionRecord(
                decision_id=_decision_id(run_id or cycle_id or timestamp, strategy_id or "strategy", symbol, "trade"),
                timestamp=timestamp,
                run_id=run_id,
                cycle_id=cycle_id,
                symbol=symbol,
                side=side,
                strategy_id=strategy_id,
                universe_id=universe_id,
                base_universe_id=current.provenance_by_symbol.get(symbol, {}).get("base_universe_id", universe_id),
                sub_universe_id=current.provenance_by_symbol.get(symbol, {}).get("sub_universe_id"),
                candidate_status=status,
                entry_reason_summary=reason if status == "selected" else None,
                rejection_reason=reason if status == "exited" else None,
                final_signal_score=current.provenance_by_symbol.get(symbol, {}).get("signal_score"),
                target_weight_pre_constraint=scheduled_weight,
                target_weight_post_constraint=target_weight,
                target_quantity=target_quantity,
                current_quantity=current_quantity,
                metadata={"current_weight": current_weight},
            )
        )
        sizing_updates.append(
            SizingDecision(
                decision_id=_decision_id(
                    run_id or cycle_id or timestamp, strategy_id or "strategy", symbol, "paper-sizing"
                ),
                timestamp=timestamp,
                run_id=run_id,
                cycle_id=cycle_id,
                symbol=symbol,
                strategy_id=strategy_id,
                side=side,
                target_weight_pre_constraint=scheduled_weight,
                target_weight_post_constraint=target_weight,
                target_quantity=target_quantity,
                current_quantity=current_quantity,
                portfolio_equity=portfolio_equity,
                investable_equity=(float(portfolio_equity) * (1.0 - float(reserve_cash_pct or 0.0)))
                if portfolio_equity is not None
                else None,
                reserve_cash_pct=reserve_cash_pct,
                sizing_inputs={"latest_price": latest_prices.get(symbol), "current_weight": current_weight},
                rationale_summary=reason,
            )
        )
        lifecycle.append(
            TradeLifecycleRecord(
                trade_id=_decision_id(run_id or cycle_id or timestamp, symbol, "trade"),
                decision_id=_decision_id(run_id or cycle_id or timestamp, strategy_id or "strategy", symbol, "trade"),
                timestamp=timestamp,
                symbol=symbol,
                strategy_id=strategy_id,
                stage="trade_decision",
                status=status,
                summary=reason,
                details={"target_weight": target_weight, "current_quantity": current_quantity},
            )
        )
        if status == "exited":
            exit_record = ExitDecisionRecord(
                decision_id=_decision_id(run_id or cycle_id or timestamp, strategy_id or "strategy", symbol, "exit"),
                timestamp=timestamp,
                run_id=run_id,
                cycle_id=cycle_id,
                symbol=symbol,
                side="sell",
                strategy_id=strategy_id,
                exit_trigger_type="rebalance",
                exit_reason_summary=reason,
                supporting_values={
                    "current_quantity": current_quantity,
                    "target_weight_post_constraint": target_weight,
                },
            )
            exit_decisions.append(exit_record)
            lifecycle.append(
                TradeLifecycleRecord(
                    trade_id=_decision_id(run_id or cycle_id or timestamp, symbol, "trade"),
                    decision_id=exit_record.decision_id,
                    timestamp=timestamp,
                    symbol=symbol,
                    strategy_id=strategy_id,
                    stage="exit_decision",
                    status="recorded",
                    summary=summarize_exit_reason(exit_record),
                )
            )

    execution_payload = execution_payload or {}
    for row in execution_payload.get("requested_orders", []):
        symbol = str(row.get("symbol") or "")
        execution_decisions.append(
            ExecutionDecisionRecord(
                decision_id=_decision_id(
                    run_id or cycle_id or timestamp, strategy_id or "strategy", symbol, "execution-request"
                ),
                timestamp=timestamp,
                run_id=run_id,
                cycle_id=cycle_id,
                symbol=symbol,
                side=row.get("side"),
                strategy_id=strategy_id,
                order_status="requested",
                requested_shares=int(row.get("requested_shares", 0) or 0),
                requested_notional=_safe_float(row.get("requested_notional")),
                target_weight=_safe_float(row.get("target_weight")),
                current_weight=_safe_float(row.get("current_weight")),
                rationale_summary="requested_for_execution",
                metadata={"provenance": row.get("provenance", {})},
            )
        )
    for row in execution_payload.get("executable_orders", []):
        symbol = str(row.get("symbol") or "")
        execution_decisions.append(
            ExecutionDecisionRecord(
                decision_id=_decision_id(
                    run_id or cycle_id or timestamp, strategy_id or "strategy", symbol, "execution"
                ),
                timestamp=timestamp,
                run_id=run_id,
                cycle_id=cycle_id,
                symbol=symbol,
                side=row.get("side"),
                strategy_id=strategy_id,
                order_status=row.get("status", "executable"),
                requested_shares=int(row.get("requested_shares", 0) or 0),
                adjusted_shares=int(row.get("adjusted_shares", 0) or 0),
                requested_notional=_safe_float(row.get("requested_notional")),
                adjusted_notional=_safe_float(row.get("adjusted_notional")),
                estimated_fill_price=_safe_float(row.get("estimated_fill_price")),
                commission=_safe_float(row.get("commission")),
                slippage_bps=_safe_float(row.get("slippage_bps")),
                rationale_summary=row.get("clipping_reason") or "executable_order",
                metadata={"provenance": row.get("provenance", {})},
            )
        )
    for row in execution_payload.get("rejected_orders", []):
        symbol = str(row.get("symbol") or "")
        execution_decisions.append(
            ExecutionDecisionRecord(
                decision_id=_decision_id(
                    run_id or cycle_id or timestamp, strategy_id or "strategy", symbol, "execution-reject"
                ),
                timestamp=timestamp,
                run_id=run_id,
                cycle_id=cycle_id,
                symbol=symbol,
                side=row.get("side"),
                strategy_id=strategy_id,
                order_status="rejected",
                requested_shares=int(row.get("requested_shares", 0) or 0),
                requested_notional=_safe_float(row.get("requested_notional")),
                rejection_reason=row.get("rejection_reason"),
                rationale_summary=row.get("rejection_reason"),
                metadata={"provenance": row.get("provenance", {})},
            )
        )
    for record in execution_decisions:
        lifecycle.append(
            TradeLifecycleRecord(
                trade_id=_decision_id(run_id or cycle_id or timestamp, record.symbol, "trade"),
                decision_id=record.decision_id,
                timestamp=timestamp,
                symbol=record.symbol,
                strategy_id=strategy_id,
                stage="execution",
                status=record.order_status,
                summary=record.rationale_summary,
            )
        )
    return _bundle_append(
        current,
        trade_decisions=trade_decisions,
        sizing_decisions=sizing_updates,
        execution_decisions=execution_decisions,
        exit_decisions=exit_decisions,
        lifecycle_records=lifecycle,
    )


def write_decision_journal_artifacts(
    *,
    bundle: DecisionJournalBundle | None,
    output_dir: str | Path,
) -> dict[str, Path]:
    if bundle is None:
        return {}
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}

    def _write_json(name: str, payload: Any) -> None:
        if payload in (None, [], {}):
            return
        path = output_path / name
        path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        paths[name.replace(".", "_")] = path

    def _write_csv(name: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        path = output_path / name
        pd.DataFrame(rows).to_csv(path, index=False)
        paths[name.replace(".", "_")] = path

    _write_json("candidate_snapshot.json", [row.to_dict() for row in bundle.candidate_evaluations])
    _write_csv("candidate_snapshot.csv", [row.flat_dict() for row in bundle.candidate_evaluations])
    _write_json("trade_decisions.json", [row.to_dict() for row in bundle.trade_decisions])
    _write_csv("trade_decisions.csv", [row.flat_dict() for row in bundle.trade_decisions])
    _write_json("execution_decisions.json", [row.to_dict() for row in bundle.execution_decisions])
    _write_csv("execution_decisions.csv", [row.flat_dict() for row in bundle.execution_decisions])
    _write_json("exit_decisions.json", [row.to_dict() for row in bundle.exit_decisions])
    _write_csv("exit_decisions.csv", [row.flat_dict() for row in bundle.exit_decisions])
    _write_json("trade_lifecycle.json", [row.to_dict() for row in bundle.lifecycle_records])
    _write_csv("trade_lifecycle.csv", [row.flat_dict() for row in bundle.lifecycle_records])
    return paths

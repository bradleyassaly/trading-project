from __future__ import annotations

import json
from dataclasses import replace
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
    score_map: dict[str, float | None],
    latest_prices: dict[str, float],
    selected_weights: dict[str, float],
    scheduled_weights: dict[str, float],
    skipped_symbols: list[str] | None = None,
    skip_reasons: dict[str, str] | None = None,
    asset_return_map: dict[str, float | None] | None = None,
    selected_rejection_reasons: dict[str, str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> DecisionJournalBundle:
    skipped = set(skipped_symbols or [])
    score_map_full = dict(score_map)
    for symbol in skipped:
        score_map_full.setdefault(symbol, None)
    ranks, percentiles = _rank_map(score_map_full)
    selected_symbols = {symbol for symbol, weight in selected_weights.items() if abs(float(weight)) > 0.0}
    bundle = DecisionJournalBundle()
    candidate_count = len(score_map_full)
    provenance: dict[str, dict[str, Any]] = {}
    for symbol in sorted(score_map_full):
        score = score_map_full.get(symbol)
        rejection_reason = None
        if symbol in skipped:
            rejection_reason = (skip_reasons or {}).get(symbol, "symbol_skipped")
        elif symbol not in selected_symbols:
            rejection_reason = (selected_rejection_reasons or {}).get(symbol) or "not_selected"
        status = "selected" if symbol in selected_symbols else "rejected"
        feature_snapshot = {
            "latest_price": _safe_float(latest_prices.get(symbol)),
            "asset_return": _safe_float((asset_return_map or {}).get(symbol)),
        }
        checks = [
            ScreenCheckResult("price_available", "pass" if feature_snapshot["latest_price"] is not None else "fail", feature_snapshot["latest_price"] is not None, value=feature_snapshot["latest_price"]),
            ScreenCheckResult("score_available", "pass" if score is not None else "fail", score is not None, value=score),
        ]
        if symbol in skipped:
            checks.append(ScreenCheckResult("symbol_loaded", "fail", False, reason=rejection_reason))
        signal = SignalBreakdown(
            signal_name=str(strategy_id or "signal"),
            final_score=score,
            raw_components={"score": score, "asset_return": feature_snapshot["asset_return"]},
            transformed_components={"scheduled_target_weight": scheduled_weights.get(symbol), "effective_target_weight": selected_weights.get(symbol)},
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
            candidate_status=status,
            final_signal_score=score,
            rank=ranks.get(symbol),
            rank_percentile=percentiles.get(symbol),
            rejection_reason=rejection_reason,
            selected_feature_values=feature_snapshot,
            signal_breakdown=signal,
            screening_checks=checks,
            metadata={**(metadata or {}), "selected_weight": selected_weights.get(symbol), "scheduled_weight": scheduled_weights.get(symbol)},
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
            sizing_inputs={"scheduled_target_weight": scheduled_weights.get(symbol), "effective_target_weight": selected_weights.get(symbol)},
            rationale_summary="weight carried through target construction" if symbol in selected_symbols else "no target sizing assigned",
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
            reason = getattr(matched_order, "reason", None) or ("rebalance_to_target" if status == "selected" else "rebalance_exit")
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
                decision_id=_decision_id(run_id or cycle_id or timestamp, strategy_id or "strategy", symbol, "paper-sizing"),
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
                investable_equity=(float(portfolio_equity) * (1.0 - float(reserve_cash_pct or 0.0))) if portfolio_equity is not None else None,
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
                supporting_values={"current_quantity": current_quantity, "target_weight_post_constraint": target_weight},
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
                decision_id=_decision_id(run_id or cycle_id or timestamp, strategy_id or "strategy", symbol, "execution-request"),
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
                decision_id=_decision_id(run_id or cycle_id or timestamp, strategy_id or "strategy", symbol, "execution"),
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
                decision_id=_decision_id(run_id or cycle_id or timestamp, strategy_id or "strategy", symbol, "execution-reject"),
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


def write_decision_journal_artifacts(*, bundle: DecisionJournalBundle | None, output_dir: str | Path) -> dict[str, Path]:
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

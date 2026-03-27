from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.paper.models import PaperPortfolioState

STRATEGY_PNL_COLUMNS = [
    "date",
    "strategy_id",
    "strategy_weight",
    "gross_exposure",
    "net_exposure",
    "position_count",
    "realized_pnl",
    "unrealized_pnl",
    "total_pnl",
    "turnover",
    "trade_count",
    "winning_trade_count",
    "closed_trade_count",
    "win_rate",
    "average_holding_period",
]
SYMBOL_PNL_COLUMNS = [
    "date",
    "symbol",
    "strategy_id",
    "side",
    "start_position",
    "end_position",
    "realized_pnl",
    "unrealized_pnl",
    "total_pnl",
    "traded_notional",
    "fill_count",
    "signal_source",
    "signal_family",
]
TRADE_PNL_COLUMNS = [
    "trade_id",
    "date",
    "symbol",
    "strategy_id",
    "signal_source",
    "signal_family",
    "side",
    "quantity",
    "entry_price",
    "exit_price",
    "realized_pnl",
    "holding_period_days",
    "attribution_method",
    "status",
    "entry_date",
    "exit_date",
]


def _safe_float(value: Any) -> float:
    try:
        if value is None or pd.isna(value):
            return 0.0
    except TypeError:
        if value is None:
            return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _normalize_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    frame = pd.DataFrame(rows)
    if frame.empty:
        return []
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def _write_csv_with_schema(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=columns).to_csv(path, index=False)


def _read_csv_records(path: str | Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    file_path = Path(path)
    if not file_path.exists():
        return []
    if file_path.stat().st_size <= 0:
        return []
    try:
        frame = pd.read_csv(file_path)
    except pd.errors.EmptyDataError:
        return []
    except pd.errors.ParserError:
        return []
    if frame.empty:
        return []
    return _normalize_records(frame.to_dict(orient="records"))


def normalize_strategy_ownership(raw: dict[str, Any] | None) -> dict[str, float]:
    weights = {
        str(strategy_id): abs(_safe_float(weight))
        for strategy_id, weight in dict(raw or {}).items()
        if str(strategy_id).strip()
    }
    total = sum(weights.values())
    if total <= 0.0:
        return {}
    return {strategy_id: float(weight / total) for strategy_id, weight in sorted(weights.items()) if weight > 0.0}


def build_symbol_strategy_provenance(
    *,
    final_target_weights: dict[str, float],
    symbol_contributions: dict[str, dict[str, float]],
    strategy_metadata: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    provenance: dict[str, dict[str, Any]] = {}
    for symbol, final_weight in sorted(final_target_weights.items()):
        contribution_map = dict(symbol_contributions.get(symbol) or {})
        matched: dict[str, float] = {}
        for strategy_id, weight in contribution_map.items():
            if final_weight == 0.0:
                continue
            same_direction = (weight >= 0.0 and final_weight >= 0.0) or (weight <= 0.0 and final_weight <= 0.0)
            if same_direction:
                matched[str(strategy_id)] = min(abs(_safe_float(weight)), abs(_safe_float(final_weight)))
        normalized = normalize_strategy_ownership(matched)
        strategy_rows: list[dict[str, Any]] = []
        signal_sources: list[str] = []
        signal_families: list[str] = []
        for strategy_id, ownership_share in normalized.items():
            meta = dict((strategy_metadata or {}).get(strategy_id) or {})
            signal_source = str(meta.get("signal_source") or "multi_strategy")
            signal_family = meta.get("signal_family")
            signal_sources.append(signal_source)
            if signal_family:
                signal_families.append(str(signal_family))
            strategy_rows.append(
                {
                    "strategy_id": strategy_id,
                    "ownership_share": float(ownership_share),
                    "signal_source": signal_source,
                    "signal_family": signal_family,
                    "sleeve_name": meta.get("sleeve_name"),
                    "preset_name": meta.get("preset_name"),
                }
            )
        provenance[symbol] = {
            "symbol": symbol,
            "target_weight": float(final_weight),
            "strategy_ownership": normalized,
            "strategy_rows": strategy_rows,
            "signal_source": signal_sources[0]
            if len(set(signal_sources)) == 1 and signal_sources
            else "multi_strategy",
            "signal_sources": sorted(set(signal_sources)),
            "signal_families": sorted(set(signal_families)),
            "attribution_method": "target_weight_proportional",
        }
    return provenance


def allocate_integer_quantities(quantity: int, ownership: dict[str, float]) -> dict[str, int]:
    normalized = normalize_strategy_ownership(ownership)
    if quantity <= 0 or not normalized:
        return {}
    raw = {strategy_id: quantity * share for strategy_id, share in normalized.items()}
    floored = {strategy_id: int(value) for strategy_id, value in raw.items()}
    remainder = int(quantity - sum(floored.values()))
    ranked = sorted(
        normalized,
        key=lambda strategy_id: (-(raw[strategy_id] - floored[strategy_id]), strategy_id),
    )
    for strategy_id in ranked[:remainder]:
        floored[strategy_id] += 1
    return {strategy_id: qty for strategy_id, qty in floored.items() if qty > 0}


def compute_strategy_unrealized_rows(
    *,
    as_of: str,
    state: PaperPortfolioState,
    equity: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    strategy_rows: list[dict[str, Any]] = []
    symbol_rows: list[dict[str, Any]] = []
    strategy_buckets: dict[str, dict[str, Any]] = {}
    for symbol, lots in sorted(state.open_lots.items()):
        position = state.positions.get(symbol)
        if position is None:
            continue
        end_price = float(position.last_price)
        total_qty = sum(int(lot.remaining_quantity) for lot in lots)
        if total_qty == 0:
            continue
        strategy_symbol_unrealized: dict[str, float] = defaultdict(float)
        strategy_symbol_qty: dict[str, int] = defaultdict(int)
        strategy_signal_source: dict[str, str | None] = {}
        strategy_signal_family: dict[str, str | None] = {}
        for lot in lots:
            remaining_qty = int(lot.remaining_quantity)
            if remaining_qty == 0:
                continue
            sign = 1.0 if remaining_qty > 0 else -1.0
            unrealized = (end_price - float(lot.entry_price)) * abs(remaining_qty) * sign
            strategy_symbol_unrealized[lot.strategy_id] += float(unrealized)
            strategy_symbol_qty[lot.strategy_id] += remaining_qty
            strategy_signal_source[lot.strategy_id] = lot.signal_source
            strategy_signal_family[lot.strategy_id] = lot.signal_family
        for strategy_id, unrealized in sorted(strategy_symbol_unrealized.items()):
            strategy_qty = int(strategy_symbol_qty[strategy_id])
            market_value = float(strategy_qty * end_price)
            gross_exposure = abs(market_value)
            bucket = strategy_buckets.setdefault(
                strategy_id,
                {
                    "date": as_of,
                    "strategy_id": strategy_id,
                    "strategy_weight": 0.0,
                    "gross_exposure": 0.0,
                    "net_exposure": 0.0,
                    "position_symbols": set(),
                    "realized_pnl": 0.0,
                    "unrealized_pnl": 0.0,
                    "turnover": 0.0,
                    "trade_count": 0,
                    "winning_trade_count": 0,
                    "closed_trade_count": 0,
                },
            )
            bucket["gross_exposure"] += gross_exposure
            bucket["net_exposure"] += market_value
            bucket["unrealized_pnl"] += float(unrealized)
            if strategy_qty != 0:
                bucket["position_symbols"].add(symbol)
            symbol_rows.append(
                {
                    "date": as_of,
                    "symbol": symbol,
                    "strategy_id": strategy_id,
                    "side": "long" if strategy_qty >= 0 else "short",
                    "start_position": None,
                    "end_position": strategy_qty,
                    "realized_pnl": 0.0,
                    "unrealized_pnl": float(unrealized),
                    "total_pnl": float(unrealized),
                    "traded_notional": 0.0,
                    "fill_count": 0,
                    "signal_source": strategy_signal_source.get(strategy_id),
                    "signal_family": strategy_signal_family.get(strategy_id),
                }
            )
    for strategy_id, bucket in sorted(strategy_buckets.items()):
        gross_exposure = float(bucket["gross_exposure"])
        bucket["strategy_weight"] = (gross_exposure / float(equity)) if equity > 0.0 else 0.0
        bucket["position_count"] = len(bucket.pop("position_symbols"))
        bucket["total_pnl"] = float(bucket["realized_pnl"] + bucket["unrealized_pnl"])
        bucket["win_rate"] = (
            float(bucket["winning_trade_count"] / bucket["closed_trade_count"])
            if bucket["closed_trade_count"] > 0
            else None
        )
        bucket["average_holding_period"] = None
        strategy_rows.append(bucket)
    return _normalize_records(strategy_rows), _normalize_records(symbol_rows)


def build_daily_attribution(
    *,
    as_of: str,
    state: PaperPortfolioState,
    equity: float,
    realized_trade_rows: list[dict[str, Any]],
    fill_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    strategy_unrealized_rows, symbol_unrealized_rows = compute_strategy_unrealized_rows(
        as_of=as_of,
        state=state,
        equity=equity,
    )
    strategy_buckets: dict[str, dict[str, Any]] = {
        str(row["strategy_id"]): dict(row) for row in strategy_unrealized_rows
    }
    symbol_buckets: dict[tuple[str, str], dict[str, Any]] = {
        (str(row["symbol"]), str(row["strategy_id"])): dict(row) for row in symbol_unrealized_rows
    }
    trade_rows = _normalize_records(realized_trade_rows)
    for fill in fill_rows:
        strategy_id = str(fill.get("strategy_id") or "").strip()
        if not strategy_id:
            continue
        bucket = strategy_buckets.setdefault(
            strategy_id,
            {
                "date": as_of,
                "strategy_id": strategy_id,
                "strategy_weight": 0.0,
                "gross_exposure": 0.0,
                "net_exposure": 0.0,
                "position_count": 0,
                "realized_pnl": 0.0,
                "unrealized_pnl": 0.0,
                "total_pnl": 0.0,
                "turnover": 0.0,
                "trade_count": 0,
                "winning_trade_count": 0,
                "closed_trade_count": 0,
                "win_rate": None,
                "average_holding_period": None,
            },
        )
        bucket["turnover"] += abs(_safe_float(fill.get("notional")))
        bucket["trade_count"] += int(fill.get("fill_count") or 1)
        symbol_key = (str(fill.get("symbol") or ""), strategy_id)
        symbol_bucket = symbol_buckets.setdefault(
            symbol_key,
            {
                "date": as_of,
                "symbol": fill.get("symbol"),
                "strategy_id": strategy_id,
                "side": fill.get("side"),
                "start_position": None,
                "end_position": None,
                "realized_pnl": 0.0,
                "unrealized_pnl": 0.0,
                "total_pnl": 0.0,
                "traded_notional": 0.0,
                "fill_count": 0,
                "signal_source": fill.get("signal_source"),
                "signal_family": fill.get("signal_family"),
            },
        )
        symbol_bucket["traded_notional"] += abs(_safe_float(fill.get("notional")))
        symbol_bucket["fill_count"] += int(fill.get("fill_count") or 1)

    for trade in trade_rows:
        strategy_id = str(trade.get("strategy_id") or "").strip()
        if not strategy_id:
            continue
        realized_pnl = _safe_float(trade.get("realized_pnl"))
        bucket = strategy_buckets.setdefault(
            strategy_id,
            {
                "date": as_of,
                "strategy_id": strategy_id,
                "strategy_weight": 0.0,
                "gross_exposure": 0.0,
                "net_exposure": 0.0,
                "position_count": 0,
                "realized_pnl": 0.0,
                "unrealized_pnl": 0.0,
                "total_pnl": 0.0,
                "turnover": 0.0,
                "trade_count": 0,
                "winning_trade_count": 0,
                "closed_trade_count": 0,
                "win_rate": None,
                "average_holding_period": None,
            },
        )
        bucket["realized_pnl"] += realized_pnl
        bucket["closed_trade_count"] += 1
        if realized_pnl > 0.0:
            bucket["winning_trade_count"] += 1
        holding_period = trade.get("holding_period_days")
        if holding_period is not None:
            current = _safe_float(bucket.get("average_holding_period"))
            count = int(bucket["closed_trade_count"])
            bucket["average_holding_period"] = (
                ((current * max(count - 1, 0)) + _safe_float(holding_period)) / count if count > 0 else None
            )
        symbol_key = (str(trade.get("symbol") or ""), strategy_id)
        symbol_bucket = symbol_buckets.setdefault(
            symbol_key,
            {
                "date": as_of,
                "symbol": trade.get("symbol"),
                "strategy_id": strategy_id,
                "side": trade.get("side"),
                "start_position": None,
                "end_position": None,
                "realized_pnl": 0.0,
                "unrealized_pnl": 0.0,
                "total_pnl": 0.0,
                "traded_notional": 0.0,
                "fill_count": 0,
                "signal_source": trade.get("signal_source"),
                "signal_family": trade.get("signal_family"),
            },
        )
        symbol_bucket["realized_pnl"] += realized_pnl

    for bucket in strategy_buckets.values():
        bucket["total_pnl"] = float(bucket["realized_pnl"] + bucket["unrealized_pnl"])
        bucket["win_rate"] = (
            float(bucket["winning_trade_count"] / bucket["closed_trade_count"])
            if bucket["closed_trade_count"] > 0
            else None
        )
    for bucket in symbol_buckets.values():
        bucket["total_pnl"] = float(bucket["realized_pnl"] + bucket["unrealized_pnl"])

    strategy_rows = sorted(strategy_buckets.values(), key=lambda row: str(row["strategy_id"]))
    symbol_rows = sorted(symbol_buckets.values(), key=lambda row: (str(row["symbol"]), str(row["strategy_id"])))
    total_realized = float(sum(_safe_float(row.get("realized_pnl")) for row in strategy_rows))
    total_unrealized = float(sum(_safe_float(row.get("unrealized_pnl")) for row in strategy_rows))
    total_pnl = float(sum(_safe_float(row.get("total_pnl")) for row in strategy_rows))
    return {
        "strategy_rows": _normalize_records(strategy_rows),
        "symbol_rows": _normalize_records(symbol_rows),
        "trade_rows": trade_rows,
        "summary": {
            "date": as_of,
            "total_realized_pnl": total_realized,
            "total_unrealized_pnl": total_unrealized,
            "total_pnl": total_pnl,
            "attribution_method": "target_weight_proportional",
        },
    }


def build_reconciliation_summary(
    *,
    strategy_rows: list[dict[str, Any]],
    symbol_rows: list[dict[str, Any]],
    portfolio_realized_pnl: float,
    portfolio_unrealized_pnl: float,
    tolerance: float = 1e-6,
) -> dict[str, Any]:
    strategy_realized = float(sum(_safe_float(row.get("realized_pnl")) for row in strategy_rows))
    strategy_unrealized = float(sum(_safe_float(row.get("unrealized_pnl")) for row in strategy_rows))
    symbol_realized = float(sum(_safe_float(row.get("realized_pnl")) for row in symbol_rows))
    symbol_unrealized = float(sum(_safe_float(row.get("unrealized_pnl")) for row in symbol_rows))
    portfolio_total = float(portfolio_realized_pnl + portfolio_unrealized_pnl)
    strategy_total = float(strategy_realized + strategy_unrealized)
    symbol_total = float(symbol_realized + symbol_unrealized)
    strategy_residual = float(portfolio_total - strategy_total)
    symbol_residual = float(portfolio_total - symbol_total)
    return {
        "portfolio_total_pnl": portfolio_total,
        "portfolio_realized_pnl": float(portfolio_realized_pnl),
        "portfolio_unrealized_pnl": float(portfolio_unrealized_pnl),
        "strategy_total_pnl": strategy_total,
        "symbol_total_pnl": symbol_total,
        "strategy_residual": strategy_residual,
        "symbol_residual": symbol_residual,
        "strategy_reconciled": abs(strategy_residual) <= tolerance,
        "symbol_reconciled": abs(symbol_residual) <= tolerance,
        "tolerance": tolerance,
    }


def _top_bottom(
    rows: list[dict[str, Any]], *, key_field: str, metric: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ordered = sorted(rows, key=lambda row: _safe_float(row.get(metric)), reverse=True)
    top = [{key_field: row.get(key_field), metric: _safe_float(row.get(metric))} for row in ordered[:5]]
    bottom = [
        {key_field: row.get(key_field), metric: _safe_float(row.get(metric))} for row in list(reversed(ordered[-5:]))
    ]
    return top, bottom


def build_attribution_summary(
    *,
    strategy_rows: list[dict[str, Any]],
    symbol_rows: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
    reconciliation: dict[str, Any],
) -> dict[str, Any]:
    top_strategies, bottom_strategies = _top_bottom(strategy_rows, key_field="strategy_id", metric="total_pnl")
    top_symbols, bottom_symbols = _top_bottom(symbol_rows, key_field="symbol", metric="total_pnl")
    turnover_ordered = sorted(strategy_rows, key=lambda row: _safe_float(row.get("turnover")), reverse=True)
    pnl_total = float(sum(_safe_float(row.get("total_pnl")) for row in strategy_rows))
    turnover_total = float(sum(_safe_float(row.get("turnover")) for row in strategy_rows))
    top_1_pnl = _safe_float(turnover_ordered[0].get("total_pnl")) if turnover_ordered else 0.0
    strategy_by_pnl = sorted(strategy_rows, key=lambda row: _safe_float(row.get("total_pnl")), reverse=True)
    top_3_pnl = float(sum(_safe_float(row.get("total_pnl")) for row in strategy_by_pnl[:3]))
    top_1_turnover = _safe_float(turnover_ordered[0].get("turnover")) if turnover_ordered else 0.0
    top_3_turnover = float(sum(_safe_float(row.get("turnover")) for row in turnover_ordered[:3]))
    closed_trade_count = len([row for row in trade_rows if row.get("exit_price") is not None])
    warnings: list[str] = []
    if not reconciliation.get("strategy_reconciled"):
        warnings.append("strategy attribution does not reconcile to portfolio total pnl within tolerance")
    if not reconciliation.get("symbol_reconciled"):
        warnings.append("symbol attribution does not reconcile to portfolio total pnl within tolerance")
    if turnover_ordered:
        leader = turnover_ordered[0]
        if turnover_total > 0.0 and (_safe_float(leader.get("turnover")) / turnover_total) > 0.75:
            warnings.append("one strategy dominates turnover")
        if pnl_total != 0.0 and abs(top_1_pnl / pnl_total) > 0.85:
            warnings.append("one strategy dominates pnl")
        if _safe_float(leader.get("turnover")) > 0.0 and abs(_safe_float(leader.get("total_pnl"))) < 1e-9:
            warnings.append("high turnover sleeve generated negligible pnl")
    return {
        "total_realized_pnl": float(sum(_safe_float(row.get("realized_pnl")) for row in strategy_rows)),
        "total_unrealized_pnl": float(sum(_safe_float(row.get("unrealized_pnl")) for row in strategy_rows)),
        "total_pnl": pnl_total,
        "top_strategies_by_total_pnl": top_strategies,
        "bottom_strategies_by_total_pnl": bottom_strategies,
        "top_symbols_by_total_pnl": top_symbols,
        "bottom_symbols_by_total_pnl": bottom_symbols,
        "highest_turnover_strategies": [
            {
                "strategy_id": row.get("strategy_id"),
                "turnover": _safe_float(row.get("turnover")),
                "total_pnl": _safe_float(row.get("total_pnl")),
            }
            for row in turnover_ordered[:5]
        ],
        "strategy_concentration_metrics": {
            "pnl_explained_top_1_strategy": (top_1_pnl / pnl_total) if pnl_total not in (0.0, -0.0) else None,
            "pnl_explained_top_3_strategies": (top_3_pnl / pnl_total) if pnl_total not in (0.0, -0.0) else None,
            "turnover_explained_top_1_strategy": (top_1_turnover / turnover_total) if turnover_total > 0.0 else None,
            "turnover_explained_top_3_strategies": (top_3_turnover / turnover_total) if turnover_total > 0.0 else None,
        },
        "closed_trade_count": closed_trade_count,
        "attribution_method": "target_weight_proportional",
        "caveats": [
            "multi-strategy ownership is attributed proportionally from final sleeve target contributions",
            "realized pnl is assigned from lot ownership when fills close previously opened lots",
            "unrealized pnl is assigned from end-of-day open lots",
        ],
        "warnings": warnings,
        "reconciliation": reconciliation,
    }


def write_pnl_attribution_artifacts(
    *,
    output_dir: str | Path,
    attribution_payload: dict[str, Any],
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    strategy_path = output_path / "strategy_pnl_attribution.csv"
    symbol_path = output_path / "symbol_pnl_attribution.csv"
    trade_path = output_path / "trade_pnl_attribution.csv"
    summary_path = output_path / "pnl_attribution_summary.json"
    _write_csv_with_schema(strategy_path, list(attribution_payload.get("strategy_rows", [])), STRATEGY_PNL_COLUMNS)
    _write_csv_with_schema(symbol_path, list(attribution_payload.get("symbol_rows", [])), SYMBOL_PNL_COLUMNS)
    _write_csv_with_schema(trade_path, list(attribution_payload.get("trade_rows", [])), TRADE_PNL_COLUMNS)
    summary_path.write_text(
        json.dumps(attribution_payload.get("summary", {}), indent=2, default=str),
        encoding="utf-8",
    )
    return {
        "strategy_pnl_attribution_path": strategy_path,
        "symbol_pnl_attribution_path": symbol_path,
        "trade_pnl_attribution_path": trade_path,
        "pnl_attribution_summary_path": summary_path,
    }


def aggregate_replay_attribution(
    *,
    replay_root: str | Path,
) -> dict[str, Any]:
    root = Path(replay_root)
    strategy_rows: list[dict[str, Any]] = []
    symbol_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    for day_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        strategy_path = day_dir / "paper" / "strategy_pnl_attribution.csv"
        symbol_path = day_dir / "paper" / "symbol_pnl_attribution.csv"
        trade_path = day_dir / "paper" / "trade_pnl_attribution.csv"
        strategy_rows.extend(_read_csv_records(strategy_path))
        symbol_rows.extend(_read_csv_records(symbol_path))
        trade_rows.extend(_read_csv_records(trade_path))
    if not strategy_rows and not symbol_rows and not trade_rows:
        return {
            "strategy_rows": [],
            "symbol_rows": [],
            "trade_rows": [],
            "summary": {},
        }
    strategy_frame = pd.DataFrame(strategy_rows)
    symbol_frame = pd.DataFrame(symbol_rows)
    replay_strategy_rows: list[dict[str, Any]] = []
    if not strategy_frame.empty:
        grouped = (
            strategy_frame.groupby("strategy_id", dropna=False)
            .agg(
                realized_pnl=("realized_pnl", "sum"),
                turnover=("turnover", "sum"),
                trade_count=("trade_count", "sum"),
                closed_trade_count=("closed_trade_count", "sum"),
                winning_trade_count=("winning_trade_count", "sum"),
                avg_strategy_weight=("strategy_weight", "mean"),
                latest_unrealized_pnl=("unrealized_pnl", "last"),
                latest_gross_exposure=("gross_exposure", "last"),
                latest_net_exposure=("net_exposure", "last"),
                latest_position_count=("position_count", "last"),
            )
            .reset_index()
        )
        for row in grouped.to_dict(orient="records"):
            row["total_pnl"] = _safe_float(row.get("realized_pnl")) + _safe_float(row.get("latest_unrealized_pnl"))
            row["win_rate"] = (
                _safe_float(row.get("winning_trade_count")) / _safe_float(row.get("closed_trade_count"))
                if _safe_float(row.get("closed_trade_count")) > 0.0
                else None
            )
            replay_strategy_rows.append(row)
    replay_symbol_rows: list[dict[str, Any]] = []
    if not symbol_frame.empty:
        if "end_position" not in symbol_frame.columns:
            symbol_frame["end_position"] = None
        grouped = (
            symbol_frame.groupby(["symbol", "strategy_id"], dropna=False)
            .agg(
                realized_pnl=("realized_pnl", "sum"),
                traded_notional=("traded_notional", "sum"),
                fill_count=("fill_count", "sum"),
                latest_unrealized_pnl=("unrealized_pnl", "last"),
                latest_end_position=("end_position", "last"),
            )
            .reset_index()
        )
        for row in grouped.to_dict(orient="records"):
            row["total_pnl"] = _safe_float(row.get("realized_pnl")) + _safe_float(row.get("latest_unrealized_pnl"))
            replay_symbol_rows.append(row)
    reconciliation = build_reconciliation_summary(
        strategy_rows=replay_strategy_rows,
        symbol_rows=replay_symbol_rows,
        portfolio_realized_pnl=float(sum(_safe_float(row.get("realized_pnl")) for row in replay_strategy_rows)),
        portfolio_unrealized_pnl=float(
            sum(_safe_float(row.get("latest_unrealized_pnl")) for row in replay_strategy_rows)
        ),
    )
    summary = build_attribution_summary(
        strategy_rows=replay_strategy_rows,
        symbol_rows=replay_symbol_rows,
        trade_rows=trade_rows,
        reconciliation=reconciliation,
    )
    return {
        "strategy_rows": _normalize_records(replay_strategy_rows),
        "symbol_rows": _normalize_records(replay_symbol_rows),
        "trade_rows": trade_rows,
        "summary": summary,
    }


def write_replay_pnl_attribution_artifacts(
    *,
    replay_root: str | Path,
    replay_payload: dict[str, Any],
) -> dict[str, Path]:
    root = Path(replay_root)
    strategy_path = root / "replay_strategy_pnl.csv"
    symbol_path = root / "replay_symbol_pnl.csv"
    trade_path = root / "replay_trade_pnl.csv"
    summary_path = root / "replay_pnl_attribution_summary.json"
    _write_csv_with_schema(strategy_path, list(replay_payload.get("strategy_rows", [])), STRATEGY_PNL_COLUMNS)
    _write_csv_with_schema(symbol_path, list(replay_payload.get("symbol_rows", [])), SYMBOL_PNL_COLUMNS)
    _write_csv_with_schema(trade_path, list(replay_payload.get("trade_rows", [])), TRADE_PNL_COLUMNS)
    summary_path.write_text(json.dumps(replay_payload.get("summary", {}), indent=2, default=str), encoding="utf-8")
    return {
        "replay_strategy_pnl_path": strategy_path,
        "replay_symbol_pnl_path": symbol_path,
        "replay_trade_pnl_path": trade_path,
        "replay_pnl_attribution_summary_path": summary_path,
    }

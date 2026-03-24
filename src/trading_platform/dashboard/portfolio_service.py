from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.dashboard.chart_service import (
    _candidate_files,
    _coerce_timestamp_series,
    _newest_path,
    _safe_read_csv,
    _safe_read_json,
    _safe_float,
    _safe_int,
    _isoformat,
    artifact_context,
    build_position_summary,
    build_chart_payload,
    load_symbol_fills,
    load_symbol_orders,
    load_symbol_provenance,
)


def _latest_path(root: Path, names: list[str]) -> Path | None:
    return _newest_path(_candidate_files(root, names))


def _equity_rows(path: Path | None) -> list[dict[str, Any]]:
    frame = _safe_read_csv(path)
    if frame.empty:
        return []
    frame, timestamp_col = _coerce_timestamp_series(frame)
    if timestamp_col is None or "equity" not in frame.columns:
        return []
    working = frame[[timestamp_col, "equity"]].copy()
    working["equity"] = pd.to_numeric(working["equity"], errors="coerce")
    working = working.dropna(subset=["equity"]).reset_index(drop=True)
    if working.empty:
        return []
    rolling_max = working["equity"].cummax().replace(0.0, pd.NA)
    working["drawdown"] = ((working["equity"] / rolling_max) - 1.0).fillna(0.0)
    return [
        {
            "ts": _isoformat(row[timestamp_col]),
            "equity": _safe_float(row["equity"]),
            "drawdown": _safe_float(row["drawdown"]),
        }
        for _, row in working.iterrows()
    ]


def _position_rows(path: Path | None) -> list[dict[str, Any]]:
    frame = _safe_read_csv(path)
    if frame.empty or "symbol" not in frame.columns:
        return []
    rows: list[dict[str, Any]] = []
    for row in frame.to_dict(orient="records"):
        qty = _safe_int(row.get("quantity") if "quantity" in row else row.get("current_qty")) or 0
        if qty == 0:
            continue
        market_value = _safe_float(row.get("market_value"))
        rows.append(
            {
                "symbol": str(row.get("symbol")).upper(),
                "qty": qty,
                "avg_price": _safe_float(row.get("avg_price")),
                "market_value": market_value,
                "side": "long" if qty > 0 else "short",
                "abs_market_value": abs(float(market_value or 0.0)),
            }
        )
    rows.sort(key=lambda item: item["abs_market_value"], reverse=True)
    for row in rows:
        row.pop("abs_market_value", None)
    return rows


def _recent_activity(root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    mappings = [
        ("order", _latest_path(root, ["paper_orders.csv", "paper_orders_history.csv", "live_dry_run_proposed_orders.csv"])),
        ("fill", _latest_path(root, ["paper_fills.csv"])),
        ("trade", _latest_path(root, ["paper_trades.csv", "trades.csv"])),
        ("execution_order", _latest_path(root, ["requested_orders.csv", "executable_orders.csv"])),
        ("execution_reject", _latest_path(root, ["rejected_orders.csv"])),
    ]
    for kind, path in mappings:
        frame = _safe_read_csv(path)
        if frame.empty:
            continue
        frame, timestamp_col = _coerce_timestamp_series(frame)
        for row in frame.tail(20).to_dict(orient="records"):
            rows.append(
                {
                    "kind": kind,
                    "ts": _isoformat(row.get(timestamp_col)) if timestamp_col else None,
                    "symbol": str(row.get("symbol") or "").upper() or None,
                    "side": row.get("side"),
                    "qty": _safe_int(row.get("quantity") if "quantity" in row else row.get("qty")),
                    "price": _safe_float(row.get("fill_price") or row.get("estimated_fill_price") or row.get("entry_price") or row.get("price")),
                    "status": row.get("status"),
                }
            )
    rows.sort(key=lambda item: str(item.get("ts") or ""), reverse=True)
    return rows[:25]


def build_portfolio_overview_payload(*, artifacts_root: str | Path) -> dict[str, Any]:
    root = Path(artifacts_root)
    summary_path = _latest_path(root, ["paper_summary.json", "live_dry_run_summary.json"])
    summary = _safe_read_json(summary_path)
    equity_path = _latest_path(root, ["paper_equity_curve.csv"])
    positions_path = _latest_path(root, ["paper_positions.csv", "live_dry_run_current_positions.csv"])
    positions = _position_rows(positions_path)
    equity_curve = _equity_rows(equity_path)
    derived_equity = equity_curve[-1]["equity"] if equity_curve else None
    trades = _all_explicit_trades(root)
    best_trades, worst_trades = _best_worst_trades(trades)
    exposure_rows = [
        {
            "symbol": row["symbol"],
            "side": row["side"],
            "market_value": row.get("market_value"),
            "weight_proxy": (
                (float(row.get("market_value") or 0.0) / float(summary.get("equity") or 1.0))
                if summary.get("equity")
                else None
            ),
        }
        for row in positions
    ]
    return {
        "summary": {
            "equity": _safe_float(summary.get("equity")) or derived_equity,
            "cash": _safe_float(summary.get("cash")),
            "gross_market_value": _safe_float(summary.get("gross_market_value")),
            "open_position_count": len(positions),
            "latest_drawdown": equity_curve[-1]["drawdown"] if equity_curve else None,
        },
        "equity_curve": [{"ts": row["ts"], "equity": row["equity"]} for row in equity_curve],
        "drawdown_curve": [{"ts": row["ts"], "drawdown": row["drawdown"]} for row in equity_curve],
        "positions": positions,
        "exposure": exposure_rows,
        "recent_activity": _recent_activity(root),
        "pnl_by_symbol": _aggregate_symbol_pnl(trades),
        "recent_realized_pnl": _pnl_by_period(trades, "D")[-10:],
        "best_trades": [_trade_table_row(row) for row in best_trades],
        "worst_trades": [_trade_table_row(row) for row in worst_trades],
        "meta": {
            "summary_source": str(summary_path) if summary_path is not None else None,
            "equity_curve_source": str(equity_path) if equity_path is not None else None,
            "positions_source": str(positions_path) if positions_path is not None else None,
        },
    }


def _trade_ledgers(root: Path) -> list[tuple[Path, dict[str, Any]]]:
    candidates = _candidate_files(root, ["paper_trades.csv", "live_trades.csv", "trades.csv"])
    return [(path, artifact_context(path, root, "trade_ledger")) for path in candidates]


def _strategy_name_from_context(context: dict[str, Any], path: Path) -> str | None:
    if context.get("strategy_id"):
        return str(context["strategy_id"])
    summary = _safe_read_json(path.parent / "paper_summary.json")
    return str(summary.get("preset_name") or summary.get("strategy") or "") or None


def _normalize_trade_frame(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, row in enumerate(frame.to_dict(orient="records"), start=1):
        entry_ts = _isoformat(row.get("entry_ts") or row.get("entry_timestamp") or row.get("opened_at") or row.get("timestamp"))
        exit_ts = _isoformat(row.get("exit_ts") or row.get("exit_timestamp") or row.get("closed_at"))
        rows.append(
            {
                "trade_id": str(row.get("trade_id") or f"T{index}"),
                "symbol": str(row.get("symbol") or "").upper() or None,
                "side": str(row.get("side") or "long").lower(),
                "qty": _safe_int(row.get("qty") or row.get("quantity") or row.get("shares")),
                "entry_ts": entry_ts,
                "entry_price": _safe_float(row.get("entry_price") or row.get("open_price") or row.get("price")),
                "exit_ts": exit_ts,
                "exit_price": _safe_float(row.get("exit_price") or row.get("close_price")),
                "realized_pnl": _safe_float(row.get("realized_pnl") or row.get("pnl") or row.get("profit_loss")) or 0.0,
                "status": str(row.get("status") or ("closed" if exit_ts else "open")).lower(),
            }
        )
    return rows


def _normalize_trade_frame_with_context(frame: pd.DataFrame, *, context: dict[str, Any], path: Path) -> list[dict[str, Any]]:
    rows = _normalize_trade_frame(frame)
    for row in rows:
        row["strategy_id"] = _strategy_name_from_context(context, path)
        row["source"] = context.get("source")
        row["run_id"] = context.get("run_id")
        row["mode"] = context.get("mode")
        row["trade_source"] = str(path)
        row["trade_source_mode"] = "explicit_ledger"
        row["hold_duration_hours"] = _holding_period_hours(row)
    return rows


def _holding_period_hours(trade: dict[str, Any]) -> float | None:
    if not trade.get("entry_ts") or not trade.get("exit_ts"):
        return None
    entry = pd.to_datetime(trade["entry_ts"], errors="coerce")
    exit_value = pd.to_datetime(trade["exit_ts"], errors="coerce")
    if pd.isna(entry) or pd.isna(exit_value):
        return None
    return float((exit_value - entry).total_seconds() / 3600.0)


def _all_explicit_trades(root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path, context in _trade_ledgers(root):
        frame = _safe_read_csv(path)
        if frame.empty:
            continue
        rows.extend(_normalize_trade_frame_with_context(frame, context=context, path=path))
    rows.sort(key=lambda row: str(row.get("entry_ts") or row.get("exit_ts") or ""), reverse=True)
    return rows


def _aggregate_symbol_pnl(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for row in trades:
        symbol = str(row.get("symbol") or "")
        if not symbol:
            continue
        entry = buckets.setdefault(symbol, {"symbol": symbol, "trade_count": 0, "closed_trade_count": 0, "cumulative_realized_pnl": 0.0, "win_rate": None})
        entry["trade_count"] += 1
        if row.get("status") == "closed":
            entry["closed_trade_count"] += 1
            entry["cumulative_realized_pnl"] += float(row.get("realized_pnl") or 0.0)
    for symbol, entry in buckets.items():
        closed_rows = [row for row in trades if row.get("symbol") == symbol and row.get("status") == "closed"]
        wins = [row for row in closed_rows if float(row.get("realized_pnl") or 0.0) > 0.0]
        entry["win_rate"] = (len(wins) / len(closed_rows)) if closed_rows else None
    return sorted(buckets.values(), key=lambda item: float(item.get("cumulative_realized_pnl") or 0.0), reverse=True)


def _pnl_by_period(trades: list[dict[str, Any]], period: str = "D") -> list[dict[str, Any]]:
    closed = [row for row in trades if row.get("status") == "closed" and row.get("exit_ts")]
    if not closed:
        return []
    frame = pd.DataFrame(closed)
    frame["exit_ts"] = pd.to_datetime(frame["exit_ts"], errors="coerce")
    frame["realized_pnl"] = pd.to_numeric(frame["realized_pnl"], errors="coerce").fillna(0.0)
    frame = frame.dropna(subset=["exit_ts"])
    if frame.empty:
        return []
    frame["exit_period"] = frame["exit_ts"].dt.tz_localize(None).dt.to_period(period)
    grouped = frame.groupby("exit_period")["realized_pnl"].sum().reset_index()
    return [{"period": str(row["exit_period"]), "realized_pnl": _safe_float(row["realized_pnl"])} for _, row in grouped.iterrows()]


def _best_worst_trades(trades: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    closed = [row for row in trades if row.get("status") == "closed"]
    ordered = sorted(closed, key=lambda row: float(row.get("realized_pnl") or 0.0), reverse=True)
    if not ordered:
        return [], []
    return ordered[:5], list(reversed(ordered[-5:]))


def _trade_table_row(row: dict[str, Any] | object) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {
            "trade_id": None,
            "symbol": None,
            "side": None,
            "realized_pnl": None,
            "entry_ts": None,
            "exit_ts": None,
            "strategy_id": None,
        }
    return {
        "trade_id": row.get("trade_id"),
        "symbol": row.get("symbol"),
        "side": row.get("side"),
        "qty": row.get("qty"),
        "realized_pnl": row.get("realized_pnl"),
        "entry_ts": row.get("entry_ts"),
        "exit_ts": row.get("exit_ts"),
        "strategy_id": row.get("strategy_id"),
        "status": row.get("status"),
    }


def _position_lookup(root: Path) -> dict[str, dict[str, Any]]:
    positions_path = _latest_path(root, ["paper_positions.csv", "live_dry_run_current_positions.csv"])
    rows = _position_rows(positions_path)
    return {str(row.get("symbol") or "").upper(): row for row in rows if row.get("symbol")}


def _latest_provenance_by_trade(root: Path, *, trades: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for symbol in sorted({str(row.get("symbol") or "").upper() for row in trades if row.get("symbol")}):
        rows, _available = load_symbol_provenance(artifacts_root=root, symbol=symbol)
        for row in rows:
            trade_id = str(row.get("trade_id") or "").strip()
            if not trade_id:
                continue
            existing = latest.get(trade_id)
            if existing is None or str(row.get("ts") or "") > str(existing.get("ts") or ""):
                latest[trade_id] = row
    return latest


def _latest_order_fill_by_trade(
    root: Path,
    *,
    trade_rows: list[dict[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    orders_by_trade: dict[str, list[dict[str, Any]]] = {}
    fills_by_trade: dict[str, list[dict[str, Any]]] = {}
    trades_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for trade in trade_rows:
        symbol = str(trade.get("symbol") or "").upper()
        if not symbol:
            continue
        trades_by_symbol.setdefault(symbol, []).append(trade)

    for symbol, trades in trades_by_symbol.items():
        fills, _fill_source, _fill_options = load_symbol_fills(artifacts_root=root, symbol=symbol)
        orders, _order_source, _order_options = load_symbol_orders(artifacts_root=root, symbol=symbol)
        for trade in trades:
            start = pd.to_datetime(trade.get("entry_ts"), errors="coerce")
            end_anchor = pd.to_datetime(trade.get("exit_ts"), errors="coerce")
            end = end_anchor if not pd.isna(end_anchor) else None
            trade_id = str(trade.get("trade_id") or "")
            if not trade_id:
                continue
            fill_matches: list[dict[str, Any]] = []
            order_matches: list[dict[str, Any]] = []
            for row in fills:
                ts = pd.to_datetime(row.get("ts"), errors="coerce")
                if pd.isna(ts):
                    continue
                if not pd.isna(start) and ts < start:
                    continue
                if end is not None and not pd.isna(end) and ts > end + pd.Timedelta(days=1):
                    continue
                fill_matches.append(row)
            for row in orders:
                ts = pd.to_datetime(row.get("ts"), errors="coerce")
                if pd.isna(ts):
                    order_matches.append(row)
                    continue
                if not pd.isna(start) and ts < start - pd.Timedelta(days=1):
                    continue
                if end is not None and not pd.isna(end) and ts > end + pd.Timedelta(days=1):
                    continue
                order_matches.append(row)
            orders_by_trade[trade_id] = sorted(order_matches, key=lambda item: str(item.get("ts") or ""))
            fills_by_trade[trade_id] = sorted(fill_matches, key=lambda item: str(item.get("ts") or ""))
    return orders_by_trade, fills_by_trade


def _trade_blotter_row(
    trade: dict[str, Any],
    *,
    provenance: dict[str, Any] | None,
    position: dict[str, Any] | None,
    orders: list[dict[str, Any]],
    fills: list[dict[str, Any]],
) -> dict[str, Any]:
    latest_fill = fills[-1] if fills else {}
    latest_order = orders[-1] if orders else {}
    status = str(trade.get("status") or latest_fill.get("status") or latest_order.get("status") or "open").lower()
    return {
        "trade_id": trade.get("trade_id"),
        "timestamp": trade.get("entry_ts") or trade.get("exit_ts"),
        "symbol": trade.get("symbol"),
        "side": trade.get("side"),
        "qty": trade.get("qty"),
        "target_weight": (provenance or {}).get("target_weight"),
        "strategy_id": trade.get("strategy_id"),
        "signal_score": (provenance or {}).get("signal_value"),
        "ranking_score": (provenance or {}).get("ranking_score"),
        "universe_rank": (provenance or {}).get("universe_rank"),
        "expected_edge": (provenance or {}).get("ranking_score"),
        "order_status": latest_order.get("status") or latest_fill.get("status") or status,
        "status": status,
        "entry_ts": trade.get("entry_ts"),
        "exit_ts": trade.get("exit_ts"),
        "entry_price": trade.get("entry_price"),
        "exit_price": trade.get("exit_price"),
        "realized_pnl": trade.get("realized_pnl"),
        "unrealized_pnl": (position or {}).get("unrealized_pnl"),
        "portfolio_qty": (position or {}).get("qty"),
        "portfolio_market_value": (position or {}).get("market_value"),
        "source": trade.get("source"),
        "run_id": trade.get("run_id"),
        "mode": trade.get("mode"),
    }


def build_trade_blotter_payload(*, artifacts_root: str | Path) -> dict[str, Any]:
    root = Path(artifacts_root)
    trades = _all_explicit_trades(root)
    provenance_by_trade = _latest_provenance_by_trade(root, trades=trades)
    positions = _position_lookup(root)
    orders_by_trade, fills_by_trade = _latest_order_fill_by_trade(root, trade_rows=trades)
    blotter_rows = [
        _trade_blotter_row(
            trade,
            provenance=provenance_by_trade.get(str(trade.get("trade_id") or "")),
            position=positions.get(str(trade.get("symbol") or "").upper()),
            orders=orders_by_trade.get(str(trade.get("trade_id") or ""), []),
            fills=fills_by_trade.get(str(trade.get("trade_id") or ""), []),
        )
        for trade in trades
    ]
    return {
        "summary": {
            "trade_count": len(blotter_rows),
            "open_trade_count": len([row for row in blotter_rows if row.get("status") != "closed"]),
            "closed_trade_count": len([row for row in blotter_rows if row.get("status") == "closed"]),
            "winning_trade_count": len([row for row in blotter_rows if float(row.get("realized_pnl") or 0.0) > 0.0]),
            "total_realized_pnl": sum(float(row.get("realized_pnl") or 0.0) for row in blotter_rows if row.get("status") == "closed"),
        },
        "trades": blotter_rows[:250],
        "meta": {
            "trade_sources": [str(path) for path, _context in _trade_ledgers(root)],
            "position_source": str(_latest_path(root, ["paper_positions.csv", "live_dry_run_current_positions.csv"]) or ""),
        },
    }


def _strategy_source_comparison(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in trades:
        key = (
            str(row.get("source") or "unknown"),
            str(row.get("run_id") or "latest"),
            str(row.get("mode") or "default"),
        )
        buckets.setdefault(key, []).append(row)
    rows: list[dict[str, Any]] = []
    for (source, run_id, mode), group in buckets.items():
        closed = [row for row in group if row.get("status") == "closed"]
        winners = [row for row in closed if float(row.get("realized_pnl") or 0.0) > 0.0]
        rows.append(
            {
                "source": source,
                "run_id": run_id,
                "mode": mode,
                "trade_count": len(group),
                "closed_trade_count": len(closed),
                "open_trade_count": len([row for row in group if row.get("status") != "closed"]),
                "cumulative_realized_pnl": sum(float(row.get("realized_pnl") or 0.0) for row in closed),
                "win_rate": (len(winners) / len(closed)) if closed else None,
            }
        )
    return sorted(rows, key=lambda row: (str(row.get("source") or ""), str(row.get("run_id") or "")), reverse=True)


def build_strategy_detail_payload(*, artifacts_root: str | Path, strategy_id: str) -> dict[str, Any]:
    root = Path(artifacts_root)
    trades: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    for path, context in _trade_ledgers(root):
        inferred_strategy_id = _strategy_name_from_context(context, path)
        if inferred_strategy_id != strategy_id:
            continue
        frame = _safe_read_csv(path)
        if frame.empty:
            continue
        normalized = _normalize_trade_frame_with_context(frame, context=context, path=path)
        trades.extend(normalized)
        sources.append(context)
    trades.sort(key=lambda row: str(row.get("entry_ts") or row.get("exit_ts") or ""), reverse=True)
    closed = [row for row in trades if row.get("status") == "closed"]
    open_trades = [row for row in trades if row.get("status") != "closed"]
    winners = [row for row in closed if float(row.get("realized_pnl") or 0.0) > 0.0]
    losers = [row for row in closed if float(row.get("realized_pnl") or 0.0) < 0.0]
    avg_win = sum(float(row["realized_pnl"]) for row in winners) / len(winners) if winners else None
    avg_loss = sum(float(row["realized_pnl"]) for row in losers) / len(losers) if losers else None
    expectancy = None
    if closed:
        expectancy = sum(float(row.get("realized_pnl") or 0.0) for row in closed) / len(closed)
    holding_periods = [value for value in (_holding_period_hours(row) for row in closed) if value is not None]
    best_trades, worst_trades = _best_worst_trades(trades)
    return {
        "strategy_id": strategy_id,
        "summary": {
            "closed_trade_count": len(closed),
            "open_trade_count": len(open_trades),
            "win_rate": (len(winners) / len(closed)) if closed else None,
            "average_win": avg_win,
            "average_loss": avg_loss,
            "expectancy": expectancy,
            "average_holding_period_hours": (sum(holding_periods) / len(holding_periods)) if holding_periods else None,
            "cumulative_realized_pnl": sum(float(row.get("realized_pnl") or 0.0) for row in closed),
            "recent_symbols": sorted({str(row.get("symbol")) for row in trades[:10] if row.get("symbol")}),
        },
        "trades": trades[:100],
        "pnl_by_symbol": _aggregate_symbol_pnl(trades),
        "recent_realized_pnl": _pnl_by_period(trades, "D")[-10:],
        "best_trades": [_trade_table_row(row) for row in best_trades],
        "worst_trades": [_trade_table_row(row) for row in worst_trades],
        "comparisons": _strategy_source_comparison(trades),
        "meta": {"sources": sources},
    }


def build_discovery_payload(*, artifacts_root: str | Path) -> dict[str, Any]:
    root = Path(artifacts_root)
    trades = _all_explicit_trades(root)
    recent_trades = trades[:15]
    symbol_buckets: dict[str, dict[str, Any]] = {}
    for trade in trades:
        symbol = str(trade.get("symbol") or "").upper()
        if not symbol:
            continue
        bucket = symbol_buckets.setdefault(
            symbol,
            {
                "symbol": symbol,
                "trade_count": 0,
                "latest_trade_id": None,
                "latest_entry_ts": None,
                "latest_strategy_id": None,
                "latest_source": None,
                "latest_run_id": None,
                "status": None,
            },
        )
        bucket["trade_count"] += 1
        if bucket["latest_entry_ts"] in (None, "") or str(trade.get("entry_ts") or "") > str(bucket["latest_entry_ts"] or ""):
            bucket["latest_entry_ts"] = trade.get("entry_ts")
            bucket["latest_trade_id"] = trade.get("trade_id")
            bucket["latest_strategy_id"] = trade.get("strategy_id")
            bucket["latest_source"] = trade.get("source")
            bucket["latest_run_id"] = trade.get("run_id")
            bucket["status"] = trade.get("status")
    recent_symbols = sorted(symbol_buckets.values(), key=lambda row: str(row.get("latest_entry_ts") or ""), reverse=True)[:15]

    strategy_buckets: dict[str, dict[str, Any]] = {}
    for trade in trades:
        strategy_id = str(trade.get("strategy_id") or "").strip()
        if not strategy_id:
            continue
        bucket = strategy_buckets.setdefault(
            strategy_id,
            {
                "strategy_id": strategy_id,
                "trade_count": 0,
                "closed_trade_count": 0,
                "latest_symbol": None,
                "latest_entry_ts": None,
                "latest_source": None,
                "latest_run_id": None,
            },
        )
        bucket["trade_count"] += 1
        if trade.get("status") == "closed":
            bucket["closed_trade_count"] += 1
        if bucket["latest_entry_ts"] in (None, "") or str(trade.get("entry_ts") or "") > str(bucket["latest_entry_ts"] or ""):
            bucket["latest_entry_ts"] = trade.get("entry_ts")
            bucket["latest_symbol"] = trade.get("symbol")
            bucket["latest_source"] = trade.get("source")
            bucket["latest_run_id"] = trade.get("run_id")
    recent_strategies = sorted(strategy_buckets.values(), key=lambda row: str(row.get("latest_entry_ts") or ""), reverse=True)[:15]

    run_context_buckets: dict[tuple[str, str, str], dict[str, Any]] = {}
    for trade in trades:
        key = (
            str(trade.get("source") or "unknown"),
            str(trade.get("run_id") or "latest"),
            str(trade.get("mode") or "default"),
        )
        bucket = run_context_buckets.setdefault(
            key,
            {
                "source": key[0],
                "run_id": key[1],
                "mode": key[2],
                "trade_count": 0,
                "strategy_count": 0,
                "symbol_count": 0,
                "latest_entry_ts": None,
            },
        )
        bucket["trade_count"] += 1
        if bucket["latest_entry_ts"] in (None, "") or str(trade.get("entry_ts") or "") > str(bucket["latest_entry_ts"] or ""):
            bucket["latest_entry_ts"] = trade.get("entry_ts")
    for bucket in run_context_buckets.values():
        matching = [
            trade for trade in trades
            if str(trade.get("source") or "unknown") == bucket["source"]
            and str(trade.get("run_id") or "latest") == bucket["run_id"]
            and str(trade.get("mode") or "default") == bucket["mode"]
        ]
        bucket["strategy_count"] = len({str(trade.get("strategy_id") or "") for trade in matching if trade.get("strategy_id")})
        bucket["symbol_count"] = len({str(trade.get("symbol") or "") for trade in matching if trade.get("symbol")})
    recent_run_contexts = sorted(run_context_buckets.values(), key=lambda row: str(row.get("latest_entry_ts") or ""), reverse=True)[:15]

    return {
        "summary": {
            "recent_symbol_count": len(recent_symbols),
            "recent_trade_count": len(recent_trades),
            "recent_strategy_count": len(recent_strategies),
            "recent_run_context_count": len(recent_run_contexts),
        },
        "recent_symbols": recent_symbols,
        "recent_trades": recent_trades,
        "recent_strategies": recent_strategies,
        "recent_run_contexts": recent_run_contexts,
    }


def build_execution_diagnostics_payload(*, artifacts_root: str | Path) -> dict[str, Any]:
    root = Path(artifacts_root)
    fills_path = _latest_path(root, ["paper_fills.csv"])
    orders_path = _latest_path(root, ["paper_orders.csv", "paper_orders_history.csv", "live_dry_run_proposed_orders.csv"])
    rejected_path = _latest_path(root, ["rejected_orders.csv"])
    fills = _safe_read_csv(fills_path)
    orders = _safe_read_csv(orders_path)
    rejected = _safe_read_csv(rejected_path)

    signal_candidates = list(root.rglob("*_signals.csv"))
    signal_frames: dict[str, pd.DataFrame] = {}
    for path in signal_candidates:
        symbol = path.name.split("_", 1)[0].upper()
        frame = _safe_read_csv(path)
        if frame.empty:
            continue
        frame, timestamp_col = _coerce_timestamp_series(frame)
        if timestamp_col is None:
            continue
        frame = frame.rename(columns={timestamp_col: "timestamp"})
        signal_frames[symbol] = frame

    latency_rows: list[dict[str, Any]] = []
    orphan_signal_count = 0
    if not fills.empty and "symbol" in fills.columns:
        fill_frame, fill_ts_col = _coerce_timestamp_series(fills)
        for row in fill_frame.to_dict(orient="records"):
            symbol = str(row.get("symbol") or "").upper()
            fill_ts = pd.to_datetime(row.get(fill_ts_col), errors="coerce") if fill_ts_col else pd.NaT
            signal_frame = signal_frames.get(symbol, pd.DataFrame())
            if signal_frame.empty or pd.isna(fill_ts):
                continue
            signal_frame = signal_frame[signal_frame["timestamp"] <= fill_ts]
            if signal_frame.empty:
                continue
            signal_row = signal_frame.iloc[-1]
            signal_price = _safe_float(signal_row.get("close"))
            fill_price = _safe_float(row.get("fill_price") or row.get("price"))
            side = str(row.get("side") or "").lower()
            slippage_bps = None
            if signal_price not in (None, 0.0) and fill_price is not None:
                raw = ((fill_price - signal_price) / signal_price) * 10000.0
                slippage_bps = raw if side == "buy" else -raw
            latency_rows.append(
                {
                    "symbol": symbol,
                    "signal_ts": _isoformat(signal_row.get("timestamp")),
                    "fill_ts": _isoformat(fill_ts),
                    "latency_seconds": float((fill_ts - signal_row["timestamp"]).total_seconds()),
                    "signal_price": signal_price,
                    "fill_price": fill_price,
                    "slippage_bps": slippage_bps,
                }
            )
        for symbol, frame in signal_frames.items():
            if fills.empty:
                orphan_signal_count += len(frame.index)
                continue
            fill_symbols = fills.get("symbol", pd.Series(dtype=object)).astype(str).str.upper()
            if symbol not in set(fill_symbols):
                orphan_signal_count += len(frame.index)

    missing_fill_count = 0
    if not orders.empty:
        order_ids = {str(row.get("client_order_id") or row.get("order_id") or "") for row in orders.to_dict(orient="records")}
        fill_ids = {str(row.get("order_id") or "") for row in fills.to_dict(orient="records")} if not fills.empty else set()
        missing_fill_count = len([order_id for order_id in order_ids if order_id and order_id not in fill_ids])

    statuses = [str(row.get("status") or "").lower() for row in orders.to_dict(orient="records")] if not orders.empty else []
    return {
        "summary": {
            "filled_order_count": int(len(fills.index)) if not fills.empty else 0,
            "canceled_order_count": len([status for status in statuses if status == "canceled"]),
            "rejected_order_count": int(len(rejected.index)) if not rejected.empty else 0,
            "missing_fill_count": missing_fill_count,
            "orphan_signal_count": orphan_signal_count,
            "average_signal_to_fill_latency_seconds": (
                sum(row["latency_seconds"] for row in latency_rows) / len(latency_rows) if latency_rows else None
            ),
            "average_slippage_bps": (
                sum(float(row["slippage_bps"]) for row in latency_rows if row.get("slippage_bps") is not None)
                / len([row for row in latency_rows if row.get("slippage_bps") is not None])
                if any(row.get("slippage_bps") is not None for row in latency_rows)
                else None
            ),
        },
        "rows": latency_rows[:50],
        "meta": {
            "fills_source": str(fills_path) if fills_path is not None else None,
            "orders_source": str(orders_path) if orders_path is not None else None,
            "rejected_source": str(rejected_path) if rejected_path is not None else None,
        },
    }


def _filter_windowed_rows(rows: list[dict[str, Any]], *, start: pd.Timestamp | None, end: pd.Timestamp | None) -> list[dict[str, Any]]:
    if start is None and end is None:
        return rows
    filtered: list[dict[str, Any]] = []
    for row in rows:
        ts = pd.to_datetime(row.get("ts"), errors="coerce")
        if pd.isna(ts):
            filtered.append(row)
            continue
        if start is not None and ts < start:
            continue
        if end is not None and ts > end:
            continue
        filtered.append(row)
    return filtered


def _indicator_snapshot(indicators: dict[str, list[dict[str, Any]]], ts: str | None) -> dict[str, float | None]:
    if not ts:
        return {}
    target = pd.to_datetime(ts, errors="coerce")
    if pd.isna(target):
        return {}
    snapshot: dict[str, float | None] = {}
    for name, rows in indicators.items():
        eligible = [row for row in rows if row.get("ts") and pd.to_datetime(row["ts"], errors="coerce") <= target]
        if eligible:
            snapshot[name] = _safe_float(eligible[-1].get("value"))
    return snapshot


def _filter_provenance_rows(
    rows: list[dict[str, Any]],
    *,
    trade_id: str | None = None,
    strategy_id: str | None = None,
    start: pd.Timestamp | None = None,
    end: pd.Timestamp | None = None,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for row in rows:
        if trade_id and row.get("trade_id") not in (None, "", trade_id):
            continue
        if strategy_id and row.get("strategy_id") not in (None, "", strategy_id):
            continue
        ts = pd.to_datetime(row.get("ts"), errors="coerce")
        if not pd.isna(ts):
            if start is not None and ts < start:
                continue
            if end is not None and ts > end:
                continue
        filtered.append(row)
    return filtered


def _build_decision_provenance_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    latest = rows[0]
    return {
        "latest": latest,
        "rows": rows[:8],
        "selection_status": latest.get("selection_status"),
        "target_weight": latest.get("target_weight"),
        "ranking_score": latest.get("ranking_score"),
        "universe_rank": latest.get("universe_rank"),
        "order_intent_summary": latest.get("order_intent_summary"),
        "constraint_hits": latest.get("constraint_hits", []),
    }


def _build_trade_lifecycle(
    *,
    trade: dict[str, Any],
    signal: dict[str, Any] | None,
    provenance_rows: list[dict[str, Any]],
    orders: list[dict[str, Any]],
    fills: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if signal:
        events.append(
            {
                "ts": signal.get("ts"),
                "kind": "signal",
                "label": signal.get("label") or signal.get("type") or "signal",
                "detail": f"price={signal.get('price')} score={signal.get('score')}",
                "status": signal.get("type"),
            }
        )
    for row in provenance_rows:
        if row.get("order_intent_summary") or row.get("selection_status") or row.get("target_weight") is not None:
            detail_parts = []
            if row.get("selection_status"):
                detail_parts.append(f"selection={row.get('selection_status')}")
            if row.get("target_weight") is not None:
                detail_parts.append(f"target_weight={row.get('target_weight')}")
            if row.get("order_intent_summary"):
                detail_parts.append(str(row.get("order_intent_summary")))
            events.append(
                {
                    "ts": row.get("ts"),
                    "kind": "decision",
                    "label": row.get("label") or row.get("signal_type") or "decision provenance",
                    "detail": " | ".join(detail_parts) or "decision context",
                    "status": row.get("selection_status") or "context",
                }
            )
    for order in orders:
        detail = f"side={order.get('side')} qty={order.get('qty')} price={order.get('price')}"
        if order.get("reason"):
            detail += f" | reason={order.get('reason')}"
        events.append(
            {
                "ts": order.get("ts"),
                "kind": "order",
                "label": order.get("status") or "order recorded",
                "detail": detail,
                "status": order.get("status"),
            }
        )
    for fill in fills:
        events.append(
            {
                "ts": fill.get("ts"),
                "kind": "fill",
                "label": fill.get("status") or "fill",
                "detail": f"side={fill.get('side')} qty={fill.get('qty')} price={fill.get('price')}",
                "status": fill.get("status"),
            }
        )
    if trade.get("exit_ts"):
        events.append(
            {
                "ts": trade.get("exit_ts"),
                "kind": "trade_close",
                "label": "trade close",
                "detail": f"exit_price={trade.get('exit_price')} realized_pnl={trade.get('realized_pnl')}",
                "status": trade.get("status"),
            }
        )
    else:
        events.append(
            {
                "ts": trade.get("entry_ts"),
                "kind": "trade_open",
                "label": "open trade",
                "detail": f"entry_price={trade.get('entry_price')} qty={trade.get('qty')}",
                "status": trade.get("status"),
            }
        )
    events.sort(key=lambda row: str(row.get("ts") or ""))
    return events


def build_trade_detail_payload(
    *,
    artifacts_root: str | Path,
    feature_dir: str | Path,
    trade_id: str,
) -> dict[str, Any]:
    root = Path(artifacts_root)
    target_trade: dict[str, Any] | None = None
    for trade in _all_explicit_trades(root):
        if trade.get("trade_id") == trade_id:
            target_trade = trade
            break
    if target_trade is None:
        return {
            "trade": None,
            "chart": {},
            "signals": [],
            "fills": [],
            "orders": [],
            "explain": {},
            "trade_summary": {},
            "portfolio_context": {},
            "execution_review": {},
            "outcome_review": {},
            "related_metadata": {},
            "provenance": {},
            "lifecycle": [],
            "comparison": {},
            "meta": {},
        }

    chart_payload = build_chart_payload(
        artifacts_root=artifacts_root,
        feature_dir=feature_dir,
        symbol=str(target_trade.get("symbol") or ""),
        timeframe="1d",
        lookback=200,
        mode=str(target_trade.get("mode")) if target_trade.get("mode") else None,
    )
    entry_ts = pd.to_datetime(target_trade.get("entry_ts"), errors="coerce")
    exit_ts = pd.to_datetime(target_trade.get("exit_ts"), errors="coerce")
    start = (entry_ts - pd.Timedelta(days=7)) if not pd.isna(entry_ts) else None
    end_anchor = exit_ts if not pd.isna(exit_ts) else entry_ts
    end = (end_anchor + pd.Timedelta(days=7)) if end_anchor is not None and not pd.isna(end_anchor) else None
    relevant_signals = _filter_windowed_rows(chart_payload.get("signals", []), start=start, end=end)
    relevant_fills = _filter_windowed_rows(chart_payload.get("fills", []), start=start, end=end)
    relevant_orders = _filter_windowed_rows(chart_payload.get("orders", []), start=start, end=end)
    provenance_rows, available_provenance_sources = load_symbol_provenance(
        artifacts_root=artifacts_root,
        symbol=str(target_trade.get("symbol") or ""),
        run_id=str(target_trade.get("run_id")) if target_trade.get("run_id") else None,
        source=str(target_trade.get("source")) if target_trade.get("source") else None,
        mode=str(target_trade.get("mode")) if target_trade.get("mode") else None,
    )
    relevant_provenance = _filter_provenance_rows(
        provenance_rows,
        trade_id=str(target_trade.get("trade_id")) if target_trade.get("trade_id") else None,
        strategy_id=str(target_trade.get("strategy_id")) if target_trade.get("strategy_id") else None,
        start=start,
        end=end,
    )
    if not relevant_provenance:
        relevant_provenance = _filter_provenance_rows(
            provenance_rows,
            strategy_id=str(target_trade.get("strategy_id")) if target_trade.get("strategy_id") else None,
            start=start,
            end=end,
        )
    nearby_signal = None
    if not pd.isna(entry_ts):
        eligible = [row for row in relevant_signals if row.get("ts") and pd.to_datetime(row["ts"], errors="coerce") <= entry_ts]
        nearby_signal = eligible[-1] if eligible else (relevant_signals[0] if relevant_signals else None)
    regime_payload = _safe_read_json(_latest_path(root, ["market_regime.json"]))
    lifecycle = _build_trade_lifecycle(
        trade=target_trade,
        signal=nearby_signal,
        provenance_rows=relevant_provenance,
        orders=relevant_orders,
        fills=relevant_fills,
    )
    related_trades = [
        row
        for row in _all_explicit_trades(root)
        if row.get("symbol") == target_trade.get("symbol") and row.get("trade_id") != trade_id
    ][:5]
    latest_price = chart_payload.get("bars", [{}])[-1].get("close") if chart_payload.get("bars") else None
    position_summary, position_source = build_position_summary(
        artifacts_root=artifacts_root,
        symbol=str(target_trade.get("symbol") or ""),
        latest_price=latest_price,
        run_id=str(target_trade.get("run_id")) if target_trade.get("run_id") else None,
        source=str(target_trade.get("source")) if target_trade.get("source") else None,
        mode=str(target_trade.get("mode")) if target_trade.get("mode") else None,
    )
    executed_qty = sum(int(row.get("qty") or 0) for row in relevant_fills)
    average_fill_price = (
        sum(float(row.get("price") or 0.0) * int(row.get("qty") or 0) for row in relevant_fills) / executed_qty
        if executed_qty > 0
        else None
    )
    candidate_set = [row for row in provenance_rows if row.get("ts") == (relevant_provenance[0].get("ts") if relevant_provenance else None)]
    trade_summary = {
        "symbol": target_trade.get("symbol"),
        "side": target_trade.get("side"),
        "status": target_trade.get("status"),
        "strategy_id": target_trade.get("strategy_id"),
        "entry_ts": target_trade.get("entry_ts"),
        "exit_ts": target_trade.get("exit_ts"),
        "qty": target_trade.get("qty"),
        "entry_price": target_trade.get("entry_price"),
        "exit_price": target_trade.get("exit_price"),
        "hold_duration_hours": target_trade.get("hold_duration_hours"),
        "realized_pnl": target_trade.get("realized_pnl"),
    }
    portfolio_context = {
        "selected_among_alternatives": (relevant_provenance[0].get("selection_included") if relevant_provenance else None),
        "selection_status": (relevant_provenance[0].get("selection_status") if relevant_provenance else None),
        "target_weight": (relevant_provenance[0].get("target_weight") if relevant_provenance else None),
        "portfolio_qty": position_summary.get("qty"),
        "portfolio_market_value": position_summary.get("market_value"),
        "unrealized_pnl": position_summary.get("unrealized_pnl"),
        "constraint_hits": (relevant_provenance[0].get("constraint_hits") if relevant_provenance else []),
        "candidate_count": len(candidate_set),
    }
    execution_review = {
        "order_count": len(relevant_orders),
        "fill_count": len(relevant_fills),
        "executed_qty": executed_qty,
        "average_fill_price": average_fill_price,
        "latest_order_status": (relevant_orders[-1].get("status") if relevant_orders else None),
        "latest_fill_status": (relevant_fills[-1].get("status") if relevant_fills else None),
        "position_source": position_source,
    }
    outcome_review = {
        "trade_status": target_trade.get("status"),
        "realized_pnl": target_trade.get("realized_pnl"),
        "unrealized_pnl": position_summary.get("unrealized_pnl"),
        "price_change": (
            float(target_trade.get("exit_price")) - float(target_trade.get("entry_price"))
            if target_trade.get("entry_price") is not None and target_trade.get("exit_price") is not None
            else None
        ),
        "holding_period_hours": target_trade.get("hold_duration_hours"),
    }
    related_metadata = {
        "run_id": target_trade.get("run_id"),
        "source": target_trade.get("source"),
        "mode": target_trade.get("mode"),
        "trade_source": target_trade.get("trade_source"),
        "trade_source_mode": target_trade.get("trade_source_mode"),
        "position_source": position_source,
    }
    return {
        "trade": target_trade,
        "chart": {
            **chart_payload,
            "bars": _filter_windowed_rows(chart_payload.get("bars", []), start=start, end=end),
            "signals": relevant_signals,
            "fills": relevant_fills,
            "orders": relevant_orders,
            "trades": [row for row in chart_payload.get("trades", []) if row.get("trade_id") == trade_id],
        },
        "signals": relevant_signals,
        "fills": relevant_fills,
        "orders": relevant_orders,
        "provenance": _build_decision_provenance_summary(relevant_provenance),
        "trade_summary": trade_summary,
        "portfolio_context": portfolio_context,
        "execution_review": execution_review,
        "outcome_review": outcome_review,
        "related_metadata": related_metadata,
        "lifecycle": lifecycle,
        "comparison": {
            "related_trades": related_trades,
            "available_chart_sources": chart_payload.get("meta", {}).get("available_chart_sources", []),
            "available_provenance_sources": available_provenance_sources,
        },
        "explain": {
            "signal": nearby_signal,
            "indicator_snapshot": _indicator_snapshot(chart_payload.get("indicators", {}), target_trade.get("entry_ts")),
            "regime": regime_payload.get("latest", {}),
            "sizing_context": {"qty": target_trade.get("qty"), "side": target_trade.get("side")},
        },
        "meta": {
            "source": target_trade.get("source"),
            "run_id": target_trade.get("run_id"),
            "mode": target_trade.get("mode"),
            "strategy_id": target_trade.get("strategy_id"),
            "trade_source": target_trade.get("trade_source"),
            "trade_source_mode": target_trade.get("trade_source_mode"),
        },
    }

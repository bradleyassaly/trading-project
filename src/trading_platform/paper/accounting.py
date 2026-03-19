from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

import pandas as pd

from trading_platform.broker.base import BrokerFill
from trading_platform.paper.models import PaperOrder, PaperPortfolioState


def _append_rows(path: str | Path, rows: list[dict], columns: list[str]) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(rows, columns=columns)
    write_header = not output_path.exists()
    df.to_csv(output_path, mode="a", header=write_header, index=False)
    return output_path


def append_fill_ledger(
    *,
    path: str | Path,
    as_of: str,
    fills: list[BrokerFill],
) -> Path:
    rows = [{"as_of": as_of, **asdict(fill)} for fill in fills]
    return _append_rows(
        path,
        rows,
        [
            "as_of",
            "symbol",
            "side",
            "quantity",
            "fill_price",
            "notional",
            "commission",
            "slippage_bps",
        ],
    )


def append_equity_ledger(
    *,
    path: str | Path,
    as_of: str,
    state: PaperPortfolioState,
) -> Path:
    rows = [
        {
            "as_of": as_of,
            "cash": float(state.cash),
            "gross_market_value": float(state.gross_market_value),
            "equity": float(state.equity),
            "position_count": int(len(state.positions)),
        }
    ]
    return _append_rows(
        path,
        rows,
        ["as_of", "cash", "gross_market_value", "equity", "position_count"],
    )


def append_positions_history(
    *,
    path: str | Path,
    as_of: str,
    state: PaperPortfolioState,
) -> Path:
    rows = [
        {
            "as_of": as_of,
            "symbol": position.symbol,
            "quantity": int(position.quantity),
            "avg_price": float(position.avg_price),
            "last_price": float(position.last_price),
            "market_value": float(position.market_value),
        }
        for position in state.positions.values()
    ]
    return _append_rows(
        path,
        rows,
        ["as_of", "symbol", "quantity", "avg_price", "last_price", "market_value"],
    )


def append_orders_history(
    *,
    path: str | Path,
    as_of: str,
    orders: list[PaperOrder],
) -> Path:
    rows = [{"as_of": as_of, **asdict(order)} for order in orders]
    return _append_rows(
        path,
        rows,
        [
            "as_of",
            "symbol",
            "side",
            "quantity",
            "reference_price",
            "target_weight",
            "current_quantity",
            "target_quantity",
            "notional",
            "reason",
        ],
    )
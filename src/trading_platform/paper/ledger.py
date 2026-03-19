from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

import pandas as pd

from trading_platform.broker.base import BrokerFill
from trading_platform.paper.models import PaperPortfolioState


def append_fills(
    *,
    path: str | Path,
    as_of: str,
    fills: list[BrokerFill],
) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows = [
        {
            "as_of": as_of,
            **asdict(fill),
        }
        for fill in fills
    ]
    df = pd.DataFrame(
        rows,
        columns=[
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

    write_header = not output_path.exists()
    df.to_csv(output_path, mode="a", header=write_header, index=False)
    return output_path


def append_equity_snapshot(
    *,
    path: str | Path,
    as_of: str,
    state: PaperPortfolioState,
) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    row = {
        "as_of": as_of,
        "cash": float(state.cash),
        "gross_market_value": float(state.gross_market_value),
        "equity": float(state.equity),
        "position_count": int(len(state.positions)),
    }
    df = pd.DataFrame(
        [row],
        columns=[
            "as_of",
            "cash",
            "gross_market_value",
            "equity",
            "position_count",
        ],
    )

    write_header = not output_path.exists()
    df.to_csv(output_path, mode="a", header=write_header, index=False)
    return output_path

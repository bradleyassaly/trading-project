from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from trading_platform.paper.accounting import (
    append_equity_ledger,
    append_fill_ledger,
    append_orders_history,
    append_positions_history,
)
from trading_platform.paper.models import PaperTradingConfig
from trading_platform.paper.service import (
    JsonPaperStateStore,
    run_paper_trading_cycle,
    write_paper_trading_artifacts,
)


@dataclass(frozen=True)
class DailyPaperTradingJobResult:
    as_of: str
    symbols: list[str]
    order_count: int
    fill_count: int
    cash: float
    equity: float
    artifact_paths: dict[str, Path]
    ledger_paths: dict[str, Path]


def run_daily_paper_trading_job(
    *,
    config: PaperTradingConfig,
    state_path: str | Path,
    output_dir: str | Path,
    auto_apply_fills: bool = False,
    refresh_data_fn: Callable[[list[str]], None] | None = None,
    build_features_fn: Callable[[list[str]], None] | None = None,
) -> DailyPaperTradingJobResult:
    symbols = list(config.symbols)

    if refresh_data_fn is not None:
        refresh_data_fn(symbols)

    if build_features_fn is not None:
        build_features_fn(symbols)

    state_store = JsonPaperStateStore(state_path)
    result = run_paper_trading_cycle(
        config=config,
        state_store=state_store,
        auto_apply_fills=auto_apply_fills,
    )

    base_output_dir = Path(output_dir)
    run_output_dir = base_output_dir / "runs" / result.as_of
    artifact_paths = write_paper_trading_artifacts(
        result=result,
        output_dir=run_output_dir,
    )

    ledger_dir = base_output_dir / "ledgers"
    ledger_paths = {
        "fills_ledger_path": append_fill_ledger(
            path=ledger_dir / "fills.csv",
            as_of=result.as_of,
            fills=result.fills,
        ),
        "equity_ledger_path": append_equity_ledger(
            path=ledger_dir / "equity_curve.csv",
            as_of=result.as_of,
            state=result.state,
        ),
        "positions_history_path": append_positions_history(
            path=ledger_dir / "positions_history.csv",
            as_of=result.as_of,
            state=result.state,
        ),
        "orders_history_path": append_orders_history(
            path=ledger_dir / "orders_history.csv",
            as_of=result.as_of,
            orders=result.orders,
        ),
    }

    return DailyPaperTradingJobResult(
        as_of=result.as_of,
        symbols=symbols,
        order_count=len(result.orders),
        fill_count=len(result.fills),
        cash=float(result.state.cash),
        equity=float(result.state.equity),
        artifact_paths=artifact_paths,
        ledger_paths=ledger_paths,
    )
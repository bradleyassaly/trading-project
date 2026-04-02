from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from trading_platform.kalshi.models import KalshiMarket, KalshiOrderBook, KalshiOrderBookLevel, KalshiTrade
from trading_platform.kalshi.paper import (
    KalshiPaperExecutionConfig,
    KalshiPaperRiskConfig,
    KalshiPaperTrader,
    KalshiPaperTradingConfig,
)
from trading_platform.kalshi.signal_registry import known_kalshi_signal_families


class FakeKalshiClient:
    def __init__(
        self,
        *,
        market: KalshiMarket,
        orderbook: KalshiOrderBook,
        trades: list[KalshiTrade],
        historical_market: dict | None = None,
    ) -> None:
        self.market = market
        self.orderbook = orderbook
        self.trades = trades
        self.historical_market = historical_market or {"ticker": market.ticker, "result": "yes"}

    def get_all_markets(self, status: str | None = None, series_ticker: str | None = None):
        return [self.market]

    def get_market(self, ticker: str):
        return self.market

    def get_orderbook(self, ticker: str, depth: int = 10):
        return self.orderbook

    def get_all_trades(self, ticker: str, min_ts: int | None = None, max_ts: int | None = None):
        return self.trades

    def get_historical_market(self, ticker: str):
        return self.historical_market


def _build_market(*, status: str = "open", close_hours: int = 24, liquidity: str = "120.0000") -> KalshiMarket:
    close_time = datetime.now(UTC) + timedelta(hours=close_hours)
    return KalshiMarket(
        ticker="KTEST-1",
        title="Will test market resolve YES?",
        subtitle=None,
        status=status,
        yes_bid="0.58",
        yes_ask="0.62",
        no_bid="0.38",
        no_ask="0.42",
        volume=250,
        open_interest=100,
        close_time=close_time.isoformat(),
        event_ticker="KTEST",
        series_ticker="KTEST",
        category="test",
        liquidity=liquidity,
        raw={},
    )


def _build_orderbook() -> KalshiOrderBook:
    return KalshiOrderBook(
        ticker="KTEST-1",
        yes_bids=[KalshiOrderBookLevel(price="0.58", quantity=50)],
        no_bids=[KalshiOrderBookLevel(price="0.38", quantity=60)],
    )


def _build_trades(*, stale: bool = False) -> list[KalshiTrade]:
    base = datetime.now(UTC) - timedelta(hours=2)
    if stale:
        base = datetime.now(UTC) - timedelta(hours=5)
    trades: list[KalshiTrade] = []
    for index in range(30):
        traded_at = base + timedelta(minutes=index * 4)
        trades.append(
            KalshiTrade(
                trade_id=f"trade-{index}",
                ticker="KTEST-1",
                side="yes" if index % 3 else "no",
                yes_price=str(0.48 + (index * 0.005)),
                no_price=str(0.52 - (index * 0.005)),
                count=5 + (index % 4),
                created_time=traded_at.isoformat(),
            )
        )
    return trades


def _build_trader(tmp_path: Path, client: FakeKalshiClient) -> KalshiPaperTrader:
    config = KalshiPaperTradingConfig(
        state_path=str(tmp_path / "state.json"),
        output_dir=str(tmp_path / "out"),
        initial_cash=500.0,
        execution=KalshiPaperExecutionConfig(
            min_recent_trades=10,
            stale_trade_seconds=7200,
            max_contracts_per_trade=10,
            max_fraction_top_level_liquidity=0.5,
            max_fraction_market_volume=0.2,
            entry_threshold=0.5,
            exit_threshold=0.5,
            max_markets_per_run=10,
        ),
        risk=KalshiPaperRiskConfig(
            max_exposure_per_market=100.0,
            max_exposure_per_category=200.0,
            max_simultaneous_positions=3,
            max_drawdown_pct=0.5,
        ),
        tracked_tickers=("KTEST-1",),
    )
    signal_families = list(known_kalshi_signal_families(informed_flow_config={}).values())
    return KalshiPaperTrader(client=client, config=config, signal_families=signal_families)


def test_execution_quote_caps_size_and_applies_penalty(tmp_path: Path) -> None:
    trader = _build_trader(
        tmp_path,
        FakeKalshiClient(market=_build_market(), orderbook=_build_orderbook(), trades=_build_trades()),
    )
    quote = trader._execution_quote_for_side(  # noqa: SLF001
        side="yes",
        action="buy",
        market=_build_market(),
        orderbook=_build_orderbook(),
        requested_quantity=20,
    )
    assert quote is not None
    assert quote.tradable_quantity == 20
    assert quote.contract_price > 0.62
    assert quote.yes_price == quote.contract_price


def test_paper_trader_writes_artifacts_and_persists_state(tmp_path: Path) -> None:
    trader = _build_trader(
        tmp_path,
        FakeKalshiClient(market=_build_market(), orderbook=_build_orderbook(), trades=_build_trades()),
    )
    summary = trader.run()

    assert summary.executed_entries == 1
    assert summary.open_positions == 1
    assert (tmp_path / "state.json").exists()
    assert (tmp_path / "out" / "kalshi_paper_positions.json").exists()
    assert (tmp_path / "out" / "kalshi_paper_trade_log.jsonl").exists()
    assert (tmp_path / "out" / "kalshi_paper_session_summary.json").exists()
    assert (tmp_path / "out" / "kalshi_paper_report.md").exists()

    state_payload = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    assert state_payload["open_positions"]["KTEST-1"]["signal_family"]

    trade_rows = [
        json.loads(line)
        for line in (tmp_path / "out" / "kalshi_paper_trade_log.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert trade_rows[0]["entry_rationale"]
    assert trade_rows[0]["execution_assumptions"]["tradable_quantity"] == 10


def test_paper_trader_rejects_stale_market(tmp_path: Path) -> None:
    trader = _build_trader(
        tmp_path,
        FakeKalshiClient(market=_build_market(), orderbook=_build_orderbook(), trades=_build_trades(stale=True)),
    )
    summary = trader.run()

    assert summary.executed_entries == 0
    assert summary.rejected_by_reason["stale_market"] == 1


def test_paper_trader_settles_existing_position(tmp_path: Path) -> None:
    client = FakeKalshiClient(market=_build_market(), orderbook=_build_orderbook(), trades=_build_trades())
    trader = _build_trader(tmp_path, client)
    state = trader.state_store.load(initial_cash=trader.config.initial_cash)

    first_summary = trader.run_once(state=state)
    assert first_summary.executed_entries == 1
    assert len(state.open_positions) == 1

    client.market = _build_market(status="settled")
    second_summary = trader.run_once(state=state)
    assert second_summary.executed_exits == 1
    assert len(state.open_positions) == 0
    closed_trade = next(trade for trade in state.trades if trade.status == "closed")
    assert closed_trade.exit_reason == "settlement"
    assert closed_trade.realized_pnl is not None

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from trading_platform.execution.realism import ExecutionConfig
from trading_platform.paper.models import PaperTradingConfig
from trading_platform.paper.service import (
    JsonPaperStateStore,
    run_paper_trading_cycle,
    write_paper_trading_artifacts,
)
from trading_platform.paper.slippage import apply_slippage, validate_slippage_config
from trading_platform.signals.registry import SIGNAL_REGISTRY


def _signal_frame(df: pd.DataFrame, **_: object) -> pd.DataFrame:
    out = df.copy()
    out["asset_return"] = out["close"].pct_change().fillna(0.0)
    out["score"] = pd.to_numeric(out["close"], errors="coerce")
    return out


def test_paper_cycle_records_fallback_and_stale_data(monkeypatch, tmp_path: Path) -> None:
    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2025-01-01", "2025-01-02"]),
                "close": [100.0, 101.0] if symbol == "AAPL" else [90.0, 91.0],
            }
        )

    monkeypatch.setattr("trading_platform.paper.service.load_feature_frame", fake_load_feature_frame)
    monkeypatch.setitem(SIGNAL_REGISTRY, "sma_cross", _signal_frame)
    monkeypatch.setattr(
        "trading_platform.services.target_construction_service.fetch_alpaca_bars",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("alpaca unavailable")),
    )

    result = run_paper_trading_cycle(
        config=PaperTradingConfig(
            symbols=["AAPL", "MSFT"],
            strategy="sma_cross",
            top_n=1,
            min_trade_dollars=1.0,
            use_alpaca_latest_data=True,
            latest_data_max_age_seconds=1,
        ),
        state_store=JsonPaperStateStore(tmp_path / "paper_state.json"),
        auto_apply_fills=False,
    )

    paper_execution = result.diagnostics["paper_execution"]
    assert paper_execution["latest_data_source"] == "yfinance"
    assert paper_execution["latest_data_fallback_used"] is True
    assert paper_execution["latest_bar_timestamp"] is not None
    assert paper_execution["latest_bar_age_seconds"] is not None
    assert paper_execution["latest_data_stale"] is True
    assert result.price_snapshots[0].fallback_used is True


def test_paper_cycle_writes_execution_price_snapshot_and_applies_slippage(
    monkeypatch,
    tmp_path: Path,
) -> None:
    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        closes = {
            "AAPL": [100.0, 101.0, 102.0],
            "MSFT": [200.0, 201.0, 202.0],
        }
        return pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"]),
                "close": closes[symbol],
            }
        )

    def fake_fetch_alpaca_bars(symbols, start, end, timeframe="1Day") -> pd.DataFrame:
        now = pd.Timestamp.now(tz="UTC").floor("min").tz_localize(None)
        rows = []
        for symbol, close in {"AAPL": 103.0, "MSFT": 205.0}.items():
            rows.append(
                {
                    "date": now,
                    "symbol": symbol,
                    "open": close - 1.0,
                    "high": close + 1.0,
                    "low": close - 2.0,
                    "close": close,
                    "volume": 1_000_000.0,
                    "source": "alpaca",
                }
            )
        return pd.DataFrame(rows)

    monkeypatch.setattr("trading_platform.paper.service.load_feature_frame", fake_load_feature_frame)
    monkeypatch.setitem(SIGNAL_REGISTRY, "sma_cross", _signal_frame)
    monkeypatch.setattr(
        "trading_platform.services.target_construction_service.fetch_alpaca_bars",
        fake_fetch_alpaca_bars,
    )

    result = run_paper_trading_cycle(
        config=PaperTradingConfig(
            symbols=["AAPL", "MSFT"],
            strategy="sma_cross",
            top_n=1,
            min_trade_dollars=1.0,
            use_alpaca_latest_data=True,
            latest_data_max_age_seconds=86_400,
            slippage_model="fixed_bps",
            slippage_buy_bps=5.0,
            slippage_sell_bps=7.0,
        ),
        state_store=JsonPaperStateStore(tmp_path / "paper_state.json"),
        auto_apply_fills=False,
    )

    assert result.price_snapshots
    assert result.diagnostics["paper_execution"]["latest_data_source"] == "alpaca"
    assert result.diagnostics["paper_execution"]["latest_data_fallback_used"] is False
    buy_orders = [order for order in result.orders if order.side == "BUY"]
    assert buy_orders
    assert buy_orders[0].expected_fill_price == pytest.approx(buy_orders[0].reference_price * 1.0005)

    paths = write_paper_trading_artifacts(result=result, output_dir=tmp_path / "artifacts")
    snapshot_df = pd.read_csv(paths["execution_price_snapshot_path"])
    assert "historical_price" in snapshot_df.columns
    assert "latest_price" in snapshot_df.columns
    assert "final_price_used" in snapshot_df.columns
    assert "price_source_used" in snapshot_df.columns
    assert "fallback_used" in snapshot_df.columns
    assert snapshot_df["price_source_used"].eq("alpaca").all()
    assert snapshot_df["fallback_used"].eq(False).all()


def test_fixed_bps_slippage_calculations_and_validation() -> None:
    base = PaperTradingConfig(symbols=["AAPL"])
    unchanged_price, unchanged_bps = apply_slippage(100.0, "BUY", base)
    assert unchanged_price == 100.0
    assert unchanged_bps == 0.0

    config = PaperTradingConfig(
        symbols=["AAPL"],
        slippage_model="fixed_bps",
        slippage_buy_bps=5.0,
        slippage_sell_bps=5.0,
    )
    buy_price, buy_bps = apply_slippage(100.0, "BUY", config)
    sell_price, sell_bps = apply_slippage(100.0, "SELL", config)
    assert buy_price == pytest.approx(100.05)
    assert sell_price == pytest.approx(99.95)
    assert buy_bps == 5.0
    assert sell_bps == 5.0

    with pytest.raises(ValueError, match="Unsupported paper slippage model"):
        validate_slippage_config(PaperTradingConfig(symbols=["AAPL"], slippage_model="bad_model"))


def test_paper_cycle_emits_typed_execution_simulation_report_for_partial_fill(
    monkeypatch,
    tmp_path: Path,
) -> None:
    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"]),
                "close": [100.0, 101.0, 102.0] if symbol == "AAPL" else [90.0, 89.0, 88.0],
            }
        )

    monkeypatch.setattr("trading_platform.paper.service.load_feature_frame", fake_load_feature_frame)
    monkeypatch.setitem(SIGNAL_REGISTRY, "sma_cross", _signal_frame)

    result = run_paper_trading_cycle(
        config=PaperTradingConfig(
            symbols=["AAPL", "MSFT"],
            strategy="sma_cross",
            top_n=1,
            min_trade_dollars=1.0,
            slippage_model="none",
        ),
        state_store=JsonPaperStateStore(tmp_path / "paper_state.json"),
        execution_config=ExecutionConfig(
            enabled=True,
            slippage_model_type="spread_plus_bps",
            half_spread_bps=2.0,
            fixed_slippage_bps=3.0,
            max_position_notional_change=500.0,
            fill_delay_seconds=60.0,
        ),
        auto_apply_fills=False,
    )

    assert result.execution_simulation_report is not None
    assert result.requested_orders
    assert result.orders
    assert result.requested_orders[0].quantity > result.orders[0].quantity
    row = result.execution_simulation_report.orders[0]
    assert row.filled_fraction < 1.0
    assert row.slippage_bps == pytest.approx(5.0)
    assert row.fill_delay_seconds == pytest.approx(60.0)
    assert type(result.execution_simulation_report).from_dict(result.execution_simulation_report.to_dict()) == result.execution_simulation_report

    paths = write_paper_trading_artifacts(result=result, output_dir=tmp_path / "artifacts")
    sim_df = pd.read_csv(paths["paper_execution_simulation_csv_path"])
    lifecycle_df = pd.read_csv(paths["order_lifecycle_records_csv_path"])
    assert "filled_fraction" in sim_df.columns
    assert "submission_delay_seconds" in sim_df.columns
    assert lifecycle_df.iloc[0]["final_status"] == "partially_filled"

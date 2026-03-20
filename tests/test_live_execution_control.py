from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.live.control import (
    LiveExecutionControlConfig,
    run_live_execution_control,
)


def _patch_simple_targets(monkeypatch, *, latest_timestamp: str = "2026-03-19", target_weight: float = 0.5) -> None:
    monkeypatch.setattr(
        "trading_platform.live.control._build_targets",
        lambda config, symbols: (
            "2026-03-19",
            pd.Timestamp(latest_timestamp),
            {"AAPL": target_weight},
            {"AAPL": target_weight},
            {"AAPL": 100.0},
            {"target_construction": {"selected_symbols": ["AAPL"]}},
        ),
    )


def test_risk_rules_block_oversized_orders(tmp_path: Path, monkeypatch) -> None:
    _patch_simple_targets(monkeypatch, target_weight=1.0)
    result = run_live_execution_control(
        config=LiveExecutionControlConfig(
            symbols=["AAPL"],
            max_order_notional=1_000.0,
            output_dir=tmp_path,
        ),
        execute=False,
    )

    blocked_df = pd.read_csv(result.artifacts["blocked_orders_report_path"])

    assert result.decision == "abort"
    assert "no_orders" in result.reason_codes
    assert not blocked_df.empty
    assert blocked_df.iloc[0]["reason_code"] == "order_notional_exceeded"


def test_kill_switch_prevents_execution(tmp_path: Path, monkeypatch) -> None:
    _patch_simple_targets(monkeypatch)
    result = run_live_execution_control(
        config=LiveExecutionControlConfig(
            symbols=["AAPL"],
            approved=True,
            kill_switch=True,
            output_dir=tmp_path,
        ),
        execute=True,
    )

    assert result.decision == "abort"
    assert "kill_switch_active" in result.reason_codes


def test_missing_approval_prevents_execution_and_falls_back_to_dry_run(tmp_path: Path, monkeypatch) -> None:
    _patch_simple_targets(monkeypatch)
    result = run_live_execution_control(
        config=LiveExecutionControlConfig(
            symbols=["AAPL"],
            output_dir=tmp_path,
        ),
        execute=True,
    )

    decision_payload = json.loads(Path(result.artifacts["live_execution_decision_path"]).read_text(encoding="utf-8"))

    assert result.decision == "dry-run"
    assert "missing_approval" in result.reason_codes
    assert decision_payload["decision"] == "dry-run"


def test_stale_data_blocks_execution(tmp_path: Path, monkeypatch) -> None:
    _patch_simple_targets(monkeypatch, latest_timestamp="2025-01-01")
    result = run_live_execution_control(
        config=LiveExecutionControlConfig(
            symbols=["AAPL"],
            approved=True,
            max_data_staleness_days=3,
            output_dir=tmp_path,
        ),
        execute=True,
    )

    assert result.decision == "abort"
    assert "stale_data" in result.reason_codes


def test_empty_portfolio_or_no_trades_aborts_cleanly(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "trading_platform.live.control._build_targets",
        lambda config, symbols: (
            "2026-03-19",
            pd.Timestamp("2026-03-19"),
            {},
            {},
            {"AAPL": 100.0},
            {"target_construction": {"selected_symbols": []}},
        ),
    )
    result = run_live_execution_control(
        config=LiveExecutionControlConfig(
            symbols=["AAPL"],
            approved=True,
            output_dir=tmp_path,
        ),
        execute=True,
    )

    pretrade_payload = json.loads(Path(result.artifacts["pretrade_risk_report_path"]).read_text(encoding="utf-8"))

    assert result.decision == "abort"
    assert "no_orders" in result.reason_codes
    assert pretrade_payload["critical_violations"]

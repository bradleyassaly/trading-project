from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import yaml

from trading_platform.kalshi.auth import KalshiConfig
from trading_platform.kalshi.paper import (
    KalshiPaperExecutionConfig,
    KalshiPaperRiskConfig,
    KalshiPaperTrader,
    KalshiPaperTradingConfig,
)
from trading_platform.kalshi.signal_registry import known_kalshi_signal_families
from trading_platform.kalshi.validation import load_kalshi_validation_summary

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _load_yaml(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    config_path = Path(path)
    if not config_path.exists():
        return {}
    with config_path.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _project_relative_path(path_str: str | None, default: str) -> Path:
    raw = path_str or default
    path = Path(raw)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _validation_summary_path(market_cfg: dict[str, Any], research_cfg: dict[str, Any], override: str | None) -> Path:
    if override:
        return Path(override)
    validation_cfg = research_cfg.get("data_validation") or market_cfg.get("data_validation") or {}
    output_dir = Path(validation_cfg.get("output_dir", "data/kalshi/validation"))
    output_dir = output_dir if output_dir.is_absolute() else PROJECT_ROOT / output_dir
    return output_dir / "kalshi_data_validation_summary.json"


def _build_client_config(config_path: str | None) -> KalshiConfig:
    yaml_config = _load_yaml(config_path)
    demo = bool(yaml_config.get("environment", {}).get("demo", False))
    auth_cfg = yaml_config.get("auth", {})
    if auth_cfg:
        config = KalshiConfig.from_mapping(
            auth_cfg,
            env_fallback=True,
            demo=demo,
            source_label=f"Kalshi auth ({config_path or 'config'})",
        )
        if config is None:
            raise ValueError("Kalshi auth is required for paper trading.")
        return config
    env_config = KalshiConfig.from_env()
    return KalshiConfig(
        api_key_id=env_config.api_key_id,
        private_key_pem=env_config.private_key_pem,
        demo=demo,
        private_key_path=env_config.private_key_path,
    )


def cmd_kalshi_paper_run(args: argparse.Namespace) -> None:
    from trading_platform.kalshi.client import KalshiClient

    market_cfg = _load_yaml(getattr(args, "config", None))
    research_cfg = _load_yaml(getattr(args, "research_config", None))

    market_ingestion_cfg = market_cfg.get("ingestion", {})
    market_risk_cfg = market_cfg.get("risk", {})
    signals_cfg = research_cfg.get("signals", {})
    informed_flow_cfg = signals_cfg.get("informed_flow", {})
    paper_cfg = research_cfg.get("paper", {})

    execution_cfg = KalshiPaperExecutionConfig(
        orderbook_depth=int(
            getattr(args, "orderbook_depth", None)
            or paper_cfg.get("execution", {}).get("orderbook_depth")
            or market_ingestion_cfg.get("orderbook_depth", 10)
        ),
        signal_lookback_hours=int(paper_cfg.get("execution", {}).get("signal_lookback_hours", 48)),
        feature_period=str(paper_cfg.get("execution", {}).get("feature_period", "1h")),
        min_recent_trades=int(paper_cfg.get("execution", {}).get("min_recent_trades", 20)),
        stale_trade_seconds=int(paper_cfg.get("execution", {}).get("stale_trade_seconds", 900)),
        min_market_volume=int(paper_cfg.get("execution", {}).get("min_market_volume", 25)),
        min_market_liquidity_dollars=float(paper_cfg.get("execution", {}).get("min_market_liquidity_dollars", 50.0)),
        max_spread=float(paper_cfg.get("execution", {}).get("max_spread", 0.08)),
        max_contracts_per_trade=int(
            paper_cfg.get("execution", {}).get(
                "max_contracts_per_trade",
                market_risk_cfg.get("max_single_trade_contracts", 10),
            )
        ),
        max_fraction_top_level_liquidity=float(
            paper_cfg.get("execution", {}).get("max_fraction_top_level_liquidity", 0.5)
        ),
        max_fraction_market_volume=float(paper_cfg.get("execution", {}).get("max_fraction_market_volume", 0.05)),
        min_confidence=float(paper_cfg.get("execution", {}).get("min_confidence", 0.10)),
        entry_threshold=float(
            getattr(args, "entry_threshold", None)
            or paper_cfg.get("execution", {}).get("entry_threshold")
            or research_cfg.get("backtest", {}).get("entry_threshold", 0.5)
        ),
        exit_threshold=float(paper_cfg.get("execution", {}).get("exit_threshold", 0.5)),
        no_entry_before_close_minutes=int(paper_cfg.get("execution", {}).get("no_entry_before_close_minutes", 60)),
        max_holding_hours=paper_cfg.get("execution", {}).get("max_holding_hours", 24.0),
        fill_penalty_factor=float(paper_cfg.get("execution", {}).get("fill_penalty_factor", 0.5)),
        max_markets_per_run=paper_cfg.get("execution", {}).get("max_markets_per_run"),
    )
    risk_cfg = KalshiPaperRiskConfig(
        max_exposure_per_market=float(paper_cfg.get("risk", {}).get("max_exposure_per_market", 100.0)),
        max_exposure_per_category=float(paper_cfg.get("risk", {}).get("max_exposure_per_category", 250.0)),
        max_simultaneous_positions=int(paper_cfg.get("risk", {}).get("max_simultaneous_positions", 5)),
        max_drawdown_pct=float(
            paper_cfg.get("risk", {}).get("max_drawdown_pct", market_risk_cfg.get("max_drawdown_pct", 0.20))
        ),
    )

    all_families = known_kalshi_signal_families(informed_flow_config=informed_flow_cfg)
    configured_family_names = tuple(signals_cfg.get("families", ())) or tuple(all_families)
    signal_families = [all_families[name] for name in configured_family_names if name in all_families]

    state_path = _project_relative_path(
        getattr(args, "state_path", None) or paper_cfg.get("state_path"),
        "artifacts/kalshi_paper/state.json",
    )
    output_dir = _project_relative_path(
        getattr(args, "output_dir", None) or paper_cfg.get("output_dir"),
        "artifacts/kalshi_paper",
    )
    state_path.parent.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    config = KalshiPaperTradingConfig(
        state_path=str(state_path),
        output_dir=str(output_dir),
        initial_cash=float(paper_cfg.get("initial_cash", 1000.0)),
        poll_interval_seconds=float(
            getattr(args, "poll_interval_seconds", None)
            or paper_cfg.get("poll_interval_seconds")
            or market_ingestion_cfg.get("orderbook_poll_interval_sec", 30.0)
        ),
        max_iterations=int(getattr(args, "max_iterations", None) or paper_cfg.get("max_iterations", 1)),
        default_market_status=str(market_ingestion_cfg.get("default_market_status", "open")),
        tracked_series=tuple(getattr(args, "tracked_series", None) or market_ingestion_cfg.get("tracked_series", ())),
        tracked_tickers=tuple(getattr(args, "tracked_tickers", None) or market_ingestion_cfg.get("tracked_tickers", ())),
        signal_family_names=tuple(family.name for family in signal_families),
        base_rate_db_path=str(_project_relative_path(paper_cfg.get("base_rate_db_path"), "data/kalshi/base_rates/base_rate_db.json")),
        execution=execution_cfg,
        risk=risk_cfg,
    )
    validation_summary_path = _validation_summary_path(
        market_cfg,
        research_cfg,
        getattr(args, "validation_summary", None),
    )

    if getattr(args, "require_validation_pass", False):
        try:
            validation_summary = load_kalshi_validation_summary(validation_summary_path)
        except FileNotFoundError:
            print(f"[ERROR] Validation summary not found: {validation_summary_path}")
            print("Run 'trading-cli data kalshi validate-dataset' first.")
            return
        if not bool(validation_summary.get("passed")):
            print(
                f"[ERROR] Kalshi dataset validation is not passing "
                f"(status={validation_summary.get('status', 'unknown')})."
            )
            print(f"Review: {validation_summary_path}")
            return

    client = KalshiClient(_build_client_config(getattr(args, "config", None)))
    trader = KalshiPaperTrader(client=client, config=config, signal_families=signal_families)

    print("Kalshi Paper Trading")
    print(f"  state path      : {config.state_path}")
    print(f"  output dir      : {config.output_dir}")
    print(f"  signal families : {', '.join(config.signal_family_names)}")
    print(f"  tracked series  : {', '.join(config.tracked_series) if config.tracked_series else 'all open markets'}")
    if config.tracked_tickers:
        print(f"  tracked tickers : {', '.join(config.tracked_tickers)}")
    print(f"  max iterations  : {config.max_iterations}")
    print(f"  poll interval   : {config.poll_interval_seconds}s")
    print(f"  entry threshold : {config.execution.entry_threshold}")
    if getattr(args, "require_validation_pass", False):
        print(f"  validation path : {validation_summary_path}")

    summary = trader.run()

    print("\nPaper session complete.")
    print(f"  Markets polled   : {summary.markets_polled}")
    print(f"  Candidate signals: {summary.candidate_signals}")
    print(f"  Entries          : {summary.executed_entries}")
    print(f"  Exits            : {summary.executed_exits}")
    print(f"  Open positions   : {summary.open_positions}")
    print(f"  Cash             : {summary.cash:.2f}")
    print(f"  Equity           : {summary.equity:.2f}")
    print(f"  Drawdown         : {summary.current_drawdown_pct:.2%}")
    if summary.halt_reason:
        print(f"  Halt reason      : {summary.halt_reason}")

    session_summary_path = Path(config.output_dir) / "kalshi_paper_session_summary.json"
    if session_summary_path.exists():
        payload = json.loads(session_summary_path.read_text(encoding="utf-8"))
        print("\nArtifacts written:")
        print(f"  Positions : {Path(config.output_dir) / 'kalshi_paper_positions.json'}")
        print(f"  Trade Log : {Path(config.output_dir) / 'kalshi_paper_trade_log.jsonl'}")
        print(f"  Summary   : {session_summary_path}")
        print(f"  Report    : {Path(config.output_dir) / 'kalshi_paper_report.md'}")
        print(f"  Rejections: {payload.get('rejected_by_reason', {})}")

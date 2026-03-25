from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from trading_platform.broker.live_models import BrokerAccount
from trading_platform.cli.commands.live_dry_run_multi_strategy import cmd_live_dry_run_multi_strategy
from trading_platform.cli.commands.refresh_research_inputs import cmd_refresh_research_inputs
from trading_platform.cli.commands.research_promote import cmd_research_promote
from trading_platform.cli.commands.strategy_portfolio_build import cmd_strategy_portfolio_build
from trading_platform.live.preview import LivePreviewResult
from trading_platform.paper.models import PaperPortfolioState, PaperTradingRunResult
from trading_platform.portfolio.strategy_portfolio import export_strategy_portfolio_run_config, load_strategy_portfolio
from trading_platform.research.alpha_lab.runner import run_alpha_research


def _write_normalized_frame(
    normalized_dir: Path,
    *,
    symbol: str,
    base_price: float,
    daily_return: float,
    periods: int = 90,
) -> None:
    closes = [base_price]
    for _ in range(periods - 1):
        closes.append(closes[-1] * (1.0 + daily_return))
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=periods, freq="D"),
            "symbol": [symbol] * periods,
            "open": [price * 0.995 for price in closes],
            "high": [price * 1.005 for price in closes],
            "low": [price * 0.99 for price in closes],
            "close": closes,
            "volume": [1_000_000 + idx * 1_000 for idx in range(periods)],
        }
    ).to_parquet(normalized_dir / f"{symbol}.parquet", index=False)


def _allocation_result():
    return SimpleNamespace(
        as_of="2025-01-04",
        combined_target_weights={"AAPL": 0.5, "MSFT": 0.3, "NVDA": 0.2},
        latest_prices={"AAPL": 150.0, "MSFT": 320.0, "NVDA": 800.0},
        sleeve_rows=[
            {"symbol": "AAPL", "sleeve_name": "canonical_a"},
            {"symbol": "MSFT", "sleeve_name": "canonical_a"},
            {"symbol": "NVDA", "sleeve_name": "canonical_a"},
        ],
        sleeve_bundles=[],
        summary={
            "enabled_sleeve_count": 1,
            "gross_exposure_before_constraints": 1.0,
            "gross_exposure_after_constraints": 1.0,
            "net_exposure_after_constraints": 1.0,
            "turnover_estimate": 0.1,
            "turnover_cap_binding": False,
            "symbols_removed_or_clipped": [],
        },
    )


def test_canonical_config_driven_workflow_smoke(
    monkeypatch,
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    normalized_dir = data_root / "normalized"
    feature_dir = data_root / "features"
    metadata_dir = data_root / "metadata"
    normalized_dir.mkdir(parents=True, exist_ok=True)

    symbol_inputs = {
        "AAPL": (100.0, 0.010),
        "MSFT": (200.0, 0.012),
        "NVDA": (300.0, 0.018),
        "AMD": (75.0, 0.016),
    }
    for symbol, (base_price, daily_return) in symbol_inputs.items():
        _write_normalized_frame(
            normalized_dir,
            symbol=symbol,
            base_price=base_price,
            daily_return=daily_return,
        )

    refresh_config_path = tmp_path / "research_input_refresh.yaml"
    refresh_config_path.write_text(
        f"""
symbols:
  - AAPL
  - MSFT
  - NVDA
  - AMD
sub_universe_id: canonical_smoke
feature_dir: {feature_dir.as_posix()}
metadata_dir: {metadata_dir.as_posix()}
normalized_dir: {normalized_dir.as_posix()}
failure_policy: fail
""".strip(),
        encoding="utf-8",
    )

    cmd_refresh_research_inputs(
        SimpleNamespace(
            config=str(refresh_config_path),
            symbols=None,
            universe=None,
            feature_groups=None,
            sub_universe_id=None,
            reference_data_root=None,
            universe_membership_path=None,
            taxonomy_snapshot_path=None,
            benchmark_mapping_path=None,
            market_regime_path=None,
            group_map_path=None,
            benchmark=None,
            failure_policy="partial_success",
            feature_dir="data/features",
            metadata_dir="data/metadata",
            normalized_dir="data/normalized",
            _cli_argv=["--config", str(refresh_config_path)],
        )
    )

    assert (feature_dir / "AAPL.parquet").exists()
    assert (metadata_dir / "sub_universe_snapshot.csv").exists()
    assert (metadata_dir / "universe_enrichment.csv").exists()
    assert (metadata_dir / "research_input_refresh_summary.json").exists()

    alpha_output_dir = tmp_path / "artifacts" / "alpha_research" / "run_smoke"
    alpha_result = run_alpha_research(
        symbols=list(symbol_inputs),
        universe=None,
        feature_dir=feature_dir,
        signal_family="momentum",
        lookbacks=[1, 2],
        horizons=[1],
        min_rows=20,
        top_quantile=0.34,
        bottom_quantile=0.34,
        output_dir=alpha_output_dir,
        train_size=20,
        test_size=10,
        step_size=10,
        equity_context_enabled=True,
    )

    assert Path(alpha_result["research_manifest_path"]).exists()
    assert Path(alpha_result["signal_performance_by_sub_universe_path"]).exists()
    assert Path(alpha_result["signal_performance_by_benchmark_context_path"]).exists()
    assert Path(alpha_result["research_context_coverage_path"]).exists()

    promoted_dir = tmp_path / "artifacts" / "promoted_strategies"
    cmd_research_promote(
        SimpleNamespace(
            artifacts_root=str(tmp_path / "artifacts" / "alpha_research"),
            registry_dir=None,
            output_dir=str(promoted_dir),
            policy_config=None,
            validation=None,
            top_n=1,
            allow_overwrite=False,
            dry_run=False,
            inactive=False,
            override_validation=False,
        )
    )

    promoted_index_path = promoted_dir / "promoted_strategies.json"
    promoted_payload = json.loads(promoted_index_path.read_text(encoding="utf-8"))
    assert promoted_payload["strategies"]
    assert promoted_payload["registry_dir"]
    assert Path(promoted_payload["promotion_candidates_path"]).exists()

    strategy_portfolio_dir = tmp_path / "artifacts" / "strategy_portfolio"
    cmd_strategy_portfolio_build(
        SimpleNamespace(
            promoted_dir=str(promoted_dir),
            policy_config=None,
            lifecycle=None,
            output_dir=str(strategy_portfolio_dir),
        )
    )

    strategy_portfolio_payload = load_strategy_portfolio(strategy_portfolio_dir)
    assert strategy_portfolio_payload["summary"]["total_selected_strategies"] >= 1

    export_dir = tmp_path / "artifacts" / "strategy_portfolio_bundle"
    export_paths = export_strategy_portfolio_run_config(
        strategy_portfolio_path=strategy_portfolio_dir,
        output_dir=export_dir,
    )
    multi_strategy_config_path = Path(export_paths["multi_strategy_config_path"])
    pipeline_config_path = Path(export_paths["pipeline_config_path"])
    run_bundle_path = Path(export_paths["run_bundle_path"])

    assert multi_strategy_config_path.exists()
    assert pipeline_config_path.exists()
    assert run_bundle_path.exists()

    paper_checks: dict[str, object] = {}

    def fake_paper_allocate(portfolio_config):
        paper_checks["sleeves"] = portfolio_config.sleeves
        assert portfolio_config.sleeves
        assert all(Path(str(sleeve.preset_path)).exists() for sleeve in portfolio_config.sleeves)
        return _allocation_result()

    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.allocate_multi_strategy_portfolio",
        fake_paper_allocate,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.write_multi_strategy_artifacts",
        lambda result, output_dir: {"allocation_summary_json_path": Path(output_dir) / "allocation_summary.json"},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.run_paper_trading_cycle_for_targets",
        lambda **kwargs: PaperTradingRunResult(
            as_of="2025-01-04",
            state=PaperPortfolioState(cash=100_000.0),
            latest_prices={"AAPL": 150.0, "MSFT": 320.0, "NVDA": 800.0},
            latest_scores={},
            latest_target_weights={"AAPL": 0.5, "MSFT": 0.3, "NVDA": 0.2},
            scheduled_target_weights={"AAPL": 0.5, "MSFT": 0.3, "NVDA": 0.2},
            orders=[],
            fills=[],
            diagnostics={},
        ),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.write_paper_trading_artifacts",
        lambda *, result, output_dir: {"summary_path": Path(output_dir) / "paper_summary.json"},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.persist_paper_run_outputs",
        lambda **kwargs: (
            {"paper_run_summary_latest_json_path": Path(kwargs["output_dir"]) / "paper_run_summary_latest.json"},
            [],
            {},
        ),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.register_experiment",
        lambda record, tracker_dir: {"experiment_registry_path": tracker_dir / "registry.csv"},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.build_paper_experiment_record",
        lambda output_dir: {},
    )

    from trading_platform.cli.commands.paper_run_multi_strategy import cmd_paper_run_multi_strategy

    paper_output_dir = tmp_path / "artifacts" / "paper_multi"
    cmd_paper_run_multi_strategy(
        SimpleNamespace(
            config=str(multi_strategy_config_path),
            execution_config=None,
            state_path=str(tmp_path / "artifacts" / "paper_multi_state.json"),
            output_dir=str(paper_output_dir),
        )
    )

    assert paper_checks["sleeves"]

    live_checks: dict[str, object] = {}

    def fake_live_allocate(portfolio_config):
        live_checks["sleeves"] = portfolio_config.sleeves
        assert portfolio_config.sleeves
        assert all(Path(str(sleeve.preset_path)).exists() for sleeve in portfolio_config.sleeves)
        return _allocation_result()

    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.allocate_multi_strategy_portfolio",
        fake_live_allocate,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.write_multi_strategy_artifacts",
        lambda result, output_dir: {"allocation_summary_json_path": Path(output_dir) / "allocation_summary.json"},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.run_live_dry_run_preview_for_targets",
        lambda **kwargs: LivePreviewResult(
            run_id="multi_strategy|2025-01-04",
            as_of="2025-01-04",
            config=kwargs["config"],
            account=BrokerAccount(account_id="acct-1", cash=100_000.0, equity=100_000.0, buying_power=100_000.0),
            positions={},
            open_orders=[],
            latest_prices=kwargs["latest_prices"],
            target_weights=kwargs["target_weights"],
            target_diagnostics=kwargs["target_diagnostics"],
            reconciliation=SimpleNamespace(orders=[], diagnostics={"investable_equity": 90_000.0}),
            adjusted_orders=[],
            order_adjustment_diagnostics={},
            execution_result=None,
            reconciliation_rows=[],
            health_checks=[],
        ),
    )

    def fake_write_live_artifacts(result):
        summary_path = Path(result.config.output_dir) / "live_dry_run_summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps({"adjusted_order_count": 0}, indent=2), encoding="utf-8")
        return {"summary_json_path": summary_path}

    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.write_live_dry_run_artifacts",
        fake_write_live_artifacts,
    )

    live_output_dir = tmp_path / "artifacts" / "live_multi"
    cmd_live_dry_run_multi_strategy(
        SimpleNamespace(
            config=str(multi_strategy_config_path),
            execution_config=None,
            broker="mock",
            output_dir=str(live_output_dir),
        )
    )

    assert live_checks["sleeves"]
    assert (live_output_dir / "live_dry_run_summary.json").exists()

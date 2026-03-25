from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from trading_platform.broker.live_models import BrokerAccount
from trading_platform.cli.commands.live_dry_run_multi_strategy import cmd_live_dry_run_multi_strategy
from trading_platform.cli.commands.pipeline_run import cmd_pipeline_run_daily
from trading_platform.cli.commands.refresh_research_inputs import cmd_refresh_research_inputs
from trading_platform.cli.commands.research_promote import cmd_research_promote
from trading_platform.cli.commands.strategy_portfolio_build import cmd_strategy_portfolio_build
from trading_platform.cli.commands.paper_run_multi_strategy import cmd_paper_run_multi_strategy
from trading_platform.config.loader import load_pipeline_run_config
from trading_platform.live.preview import LivePreviewResult
from trading_platform.orchestration.models import PipelineRunResult, PipelineStageRecord
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


def _write_daily_pipeline_config(
    *,
    multi_strategy_config_path: Path,
    output_dir: Path,
    paper_state_path: Path,
) -> Path:
    config_path = output_dir / "canonical_daily_pipeline.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        f"""
run_name: canonical_daily_bundle
schedule_type: daily
universes:
  - canonical_smoke
multi_strategy_input_path: {multi_strategy_config_path.as_posix()}
paper_state_path: {paper_state_path.as_posix()}
output_root_dir: {output_dir.as_posix()}
stages:
  portfolio_allocation: true
  paper_trading: true
  live_dry_run: true
  reporting: true
""".strip(),
        encoding="utf-8",
    )
    return config_path


def _build_canonical_exported_bundle(tmp_path: Path) -> dict[str, Path]:
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
    return {
        "feature_dir": feature_dir,
        "metadata_dir": metadata_dir,
        "alpha_output_dir": alpha_output_dir,
        "promoted_dir": promoted_dir,
        "strategy_portfolio_dir": strategy_portfolio_dir,
        "multi_strategy_config_path": Path(export_paths["multi_strategy_config_path"]),
        "pipeline_config_path": Path(export_paths["pipeline_config_path"]),
        "run_bundle_path": Path(export_paths["run_bundle_path"]),
    }


def test_canonical_config_driven_workflow_smoke(
    monkeypatch,
    tmp_path: Path,
) -> None:
    bundle = _build_canonical_exported_bundle(tmp_path)
    feature_dir = bundle["feature_dir"]
    metadata_dir = bundle["metadata_dir"]
    alpha_output_dir = bundle["alpha_output_dir"]
    promoted_dir = bundle["promoted_dir"]
    strategy_portfolio_dir = bundle["strategy_portfolio_dir"]
    multi_strategy_config_path = bundle["multi_strategy_config_path"]
    pipeline_config_path = bundle["pipeline_config_path"]
    run_bundle_path = bundle["run_bundle_path"]

    assert (feature_dir / "AAPL.parquet").exists()
    assert (metadata_dir / "sub_universe_snapshot.csv").exists()
    assert (metadata_dir / "universe_enrichment.csv").exists()
    assert (metadata_dir / "research_input_refresh_summary.json").exists()
    assert (alpha_output_dir / "research_run.json").exists()
    assert (promoted_dir / "promoted_strategies.json").exists()
    assert (strategy_portfolio_dir / "strategy_portfolio.json").exists()
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


def test_canonical_paper_multi_strategy_bundle_reuse_for_scheduled_style_runs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    bundle = _build_canonical_exported_bundle(tmp_path)
    multi_strategy_config_path = bundle["multi_strategy_config_path"]
    output_dir = tmp_path / "artifacts" / "paper_multi_scheduled"
    state_path = tmp_path / "artifacts" / "paper_multi_scheduled_state.json"
    call_counts = {"allocate": 0, "paper": 0, "persist": 0}

    def fake_allocate(portfolio_config):
        call_counts["allocate"] += 1
        assert portfolio_config.sleeves
        assert all(Path(str(sleeve.preset_path)).exists() for sleeve in portfolio_config.sleeves)
        return _allocation_result()

    def fake_run(**kwargs):
        call_counts["paper"] += 1
        return PaperTradingRunResult(
            as_of=f"2025-01-0{call_counts['paper'] + 3}",
            state=PaperPortfolioState(cash=100_000.0),
            latest_prices={"AAPL": 150.0, "MSFT": 320.0, "NVDA": 800.0},
            latest_scores={},
            latest_target_weights={"AAPL": 0.5, "MSFT": 0.3, "NVDA": 0.2},
            scheduled_target_weights={"AAPL": 0.5, "MSFT": 0.3, "NVDA": 0.2},
            orders=[],
            fills=[],
            diagnostics={},
        )

    def fake_write_paper_artifacts(*, result, output_dir):
        summary_path = Path(output_dir) / "paper_summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps({"as_of": result.as_of}, indent=2), encoding="utf-8")
        return {"summary_path": summary_path}

    def fake_persist(**kwargs):
        call_counts["persist"] += 1
        latest_path = Path(kwargs["output_dir"]) / "paper_run_summary_latest.json"
        history_path = Path(kwargs["output_dir"]) / "paper_run_summary_history.csv"
        latest_path.write_text(
            json.dumps({"iteration": call_counts["persist"], "as_of": kwargs["result"].as_of}, indent=2),
            encoding="utf-8",
        )
        history_df = (
            pd.read_csv(history_path)
            if history_path.exists()
            else pd.DataFrame(columns=["iteration", "as_of"])
        )
        history_df = pd.concat(
            [
                history_df,
                pd.DataFrame([{"iteration": call_counts["persist"], "as_of": kwargs["result"].as_of}]),
            ],
            ignore_index=True,
        )
        history_df.to_csv(history_path, index=False)
        return (
            {
                "paper_run_summary_latest_json_path": latest_path,
                "paper_run_summary_history_csv_path": history_path,
            },
            [],
            {},
        )

    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.allocate_multi_strategy_portfolio",
        fake_allocate,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.write_multi_strategy_artifacts",
        lambda result, output_dir: {"allocation_summary_json_path": Path(output_dir) / "allocation_summary.json"},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.run_paper_trading_cycle_for_targets",
        fake_run,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.write_paper_trading_artifacts",
        fake_write_paper_artifacts,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.persist_paper_run_outputs",
        fake_persist,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.register_experiment",
        lambda record, tracker_dir: {"experiment_registry_path": tracker_dir / "registry.csv"},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.build_paper_experiment_record",
        lambda output_dir: {},
    )

    args = SimpleNamespace(
        config=str(multi_strategy_config_path),
        execution_config=None,
        state_path=str(state_path),
        output_dir=str(output_dir),
    )
    cmd_paper_run_multi_strategy(args)
    cmd_paper_run_multi_strategy(args)

    assert call_counts == {"allocate": 2, "paper": 2, "persist": 2}
    latest_payload = json.loads((output_dir / "paper_run_summary_latest.json").read_text(encoding="utf-8"))
    history_df = pd.read_csv(output_dir / "paper_run_summary_history.csv")
    assert latest_payload["iteration"] == 2
    assert len(history_df.index) == 2


def test_canonical_live_multi_strategy_bundle_reuse_for_scheduled_style_runs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    bundle = _build_canonical_exported_bundle(tmp_path)
    multi_strategy_config_path = bundle["multi_strategy_config_path"]
    output_dir = tmp_path / "artifacts" / "live_multi_scheduled"
    call_counts = {"allocate": 0, "live": 0}

    def fake_allocate(portfolio_config):
        call_counts["allocate"] += 1
        assert portfolio_config.sleeves
        assert all(Path(str(sleeve.preset_path)).exists() for sleeve in portfolio_config.sleeves)
        return _allocation_result()

    def fake_preview(**kwargs):
        call_counts["live"] += 1
        return LivePreviewResult(
            run_id=f"multi_strategy|2025-01-0{call_counts['live'] + 3}",
            as_of=f"2025-01-0{call_counts['live'] + 3}",
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
        )

    def fake_write_live_artifacts(result):
        summary_path = Path(result.config.output_dir) / "live_dry_run_summary.json"
        history_path = Path(result.config.output_dir) / "live_dry_run_history.csv"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(
            json.dumps({"run_id": result.run_id, "as_of": result.as_of, "adjusted_order_count": 0}, indent=2),
            encoding="utf-8",
        )
        history_df = (
            pd.read_csv(history_path)
            if history_path.exists()
            else pd.DataFrame(columns=["run_id", "as_of"])
        )
        history_df = pd.concat(
            [
                history_df,
                pd.DataFrame([{"run_id": result.run_id, "as_of": result.as_of}]),
            ],
            ignore_index=True,
        )
        history_df.to_csv(history_path, index=False)
        return {"summary_json_path": summary_path, "history_csv_path": history_path}

    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.allocate_multi_strategy_portfolio",
        fake_allocate,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.write_multi_strategy_artifacts",
        lambda result, output_dir: {"allocation_summary_json_path": Path(output_dir) / "allocation_summary.json"},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.run_live_dry_run_preview_for_targets",
        fake_preview,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.write_live_dry_run_artifacts",
        fake_write_live_artifacts,
    )

    args = SimpleNamespace(
        config=str(multi_strategy_config_path),
        execution_config=None,
        broker="mock",
        output_dir=str(output_dir),
    )
    cmd_live_dry_run_multi_strategy(args)
    cmd_live_dry_run_multi_strategy(args)

    assert call_counts == {"allocate": 2, "live": 2}
    latest_payload = json.loads((output_dir / "live_dry_run_summary.json").read_text(encoding="utf-8"))
    history_df = pd.read_csv(output_dir / "live_dry_run_history.csv")
    assert latest_payload["run_id"].endswith("5")
    assert len(history_df.index) == 2


def test_canonical_daily_pipeline_config_reuses_exported_bundle_across_runs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    bundle = _build_canonical_exported_bundle(tmp_path)
    multi_strategy_config_path = bundle["multi_strategy_config_path"]
    daily_root = tmp_path / "artifacts" / "daily_pipeline"
    pipeline_config_path = _write_daily_pipeline_config(
        multi_strategy_config_path=multi_strategy_config_path,
        output_dir=daily_root,
        paper_state_path=tmp_path / "artifacts" / "daily_pipeline_state.json",
    )
    loaded_config = load_pipeline_run_config(pipeline_config_path)
    assert loaded_config.schedule_type == "daily"
    assert Path(str(loaded_config.multi_strategy_input_path)) == multi_strategy_config_path
    assert loaded_config.stages.paper_trading is True
    assert loaded_config.stages.live_dry_run is True

    call_count = {"runs": 0}

    def fake_run_pipeline(config):
        call_count["runs"] += 1
        assert config.schedule_type == "daily"
        assert Path(str(config.multi_strategy_input_path)) == multi_strategy_config_path
        assert config.stages.paper_trading is True
        assert config.stages.live_dry_run is True
        history_path = daily_root / "canonical_daily_history.csv"
        latest_path = daily_root / "canonical_daily_latest.json"
        history_df = (
            pd.read_csv(history_path)
            if history_path.exists()
            else pd.DataFrame(columns=["iteration", "run_name", "schedule_type", "bundle_path"])
        )
        history_df = pd.concat(
            [
                history_df,
                pd.DataFrame(
                    [
                        {
                            "iteration": call_count["runs"],
                            "run_name": config.run_name,
                            "schedule_type": config.schedule_type,
                            "bundle_path": config.multi_strategy_input_path,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )
        history_df.to_csv(history_path, index=False)
        latest_path.write_text(
            json.dumps(
                {
                    "iteration": call_count["runs"],
                    "run_name": config.run_name,
                    "schedule_type": config.schedule_type,
                    "bundle_path": config.multi_strategy_input_path,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        result = PipelineRunResult(
            run_name=config.run_name,
            schedule_type=config.schedule_type,
            started_at="2025-01-04T09:30:00+00:00",
            ended_at="2025-01-04T09:31:00+00:00",
            status="succeeded",
            run_dir=str(daily_root / f"run_{call_count['runs']}"),
            stage_records=[
                PipelineStageRecord(stage_name="portfolio_allocation", status="succeeded"),
                PipelineStageRecord(stage_name="paper_trading", status="succeeded"),
                PipelineStageRecord(stage_name="live_dry_run", status="succeeded"),
            ],
            errors=[],
            outputs={
                "multi_strategy_input_path": config.multi_strategy_input_path,
                "paper_ready": True,
                "live_ready": True,
            },
        )
        return result, {
            "canonical_daily_latest_path": latest_path,
            "canonical_daily_history_path": history_path,
        }

    monkeypatch.setattr(
        "trading_platform.cli.commands.pipeline_run.run_orchestration_pipeline",
        fake_run_pipeline,
    )

    args = SimpleNamespace(config=str(pipeline_config_path))
    cmd_pipeline_run_daily(args)
    cmd_pipeline_run_daily(args)

    latest_payload = json.loads((daily_root / "canonical_daily_latest.json").read_text(encoding="utf-8"))
    history_df = pd.read_csv(daily_root / "canonical_daily_history.csv")
    assert latest_payload["iteration"] == 2
    assert Path(str(latest_payload["bundle_path"])) == multi_strategy_config_path
    assert len(history_df.index) == 2

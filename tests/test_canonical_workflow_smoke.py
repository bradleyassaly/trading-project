from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from trading_platform.broker.live_models import BrokerAccount
from trading_platform.cli.commands.alpha_research import cmd_alpha_research
from trading_platform.cli.commands.live_dry_run_multi_strategy import cmd_live_dry_run_multi_strategy
from trading_platform.cli.commands.pipeline_run import cmd_pipeline_run_daily
from trading_platform.cli.commands.refresh_research_inputs import cmd_refresh_research_inputs
from trading_platform.cli.commands.research_promote import cmd_research_promote
from trading_platform.cli.commands.strategy_portfolio_build import cmd_strategy_portfolio_build
from trading_platform.cli.commands.strategy_portfolio_experiment_bundle import (
    cmd_strategy_portfolio_experiment_bundle,
)
from trading_platform.cli.commands.strategy_portfolio_experiment_bundle_matrix import (
    cmd_strategy_portfolio_experiment_bundle_matrix,
)
from trading_platform.cli.commands.paper_run_multi_strategy import cmd_paper_run_multi_strategy
from trading_platform.config.loader import load_pipeline_run_config
from trading_platform.live.preview import LivePreviewResult
from trading_platform.orchestration.models import PipelineRunResult, PipelineStageRecord
from trading_platform.paper.models import PaperPortfolioState, PaperTradingRunResult
from trading_platform.portfolio.strategy_portfolio import export_strategy_portfolio_run_config, load_strategy_portfolio


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
    enable_paper_trading: bool = True,
    enable_live_dry_run: bool = True,
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
  paper_trading: {"true" if enable_paper_trading else "false"}
  live_dry_run: {"true" if enable_live_dry_run else "false"}
  reporting: true
""".strip(),
        encoding="utf-8",
    )
    return config_path


def _write_alpha_research_config(
    *,
    symbols: list[str],
    feature_dir: Path,
    output_dir: Path,
    tracker_dir: Path,
    config_path: Path,
) -> Path:
    config_path.parent.mkdir(parents=True, exist_ok=True)
    symbol_block = "\n".join(f"    - {symbol}" for symbol in symbols)
    config_path.write_text(
        f"""
paths:
  feature_path: {feature_dir.as_posix()}
  output_dir: {output_dir.as_posix()}

selection:
  symbols:
{symbol_block}

signals:
  family: momentum
  lookbacks: [1, 2]
  horizons: [1]
  min_rows: 20
  equity_context_enabled: true

portfolio:
  top_quantile: 0.34
  bottom_quantile: 0.34
  train_size: 20
  test_size: 10
  step_size: 10

tracking:
  tracker_dir: {tracker_dir.as_posix()}
""".strip(),
        encoding="utf-8",
    )
    return config_path


def _write_promotion_policy_config(
    *,
    config_path: Path,
) -> Path:
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        """
schema_version: 1
metric_name: portfolio_sharpe
min_metric_threshold: 0.1
min_folds_tested: 1
min_promoted_signals: 1
require_validation_pass: false
allow_weak_validation: true
max_strategies_total: 2
max_strategies_per_group: 1
group_by: signal_family
default_status: inactive
enable_conditional_variants: true
allowed_condition_types:
  - regime
  - sub_universe
  - benchmark_context
min_condition_sample_size: 1
min_condition_improvement: 0.0
compare_condition_to_unconditional: true
notes: canonical smoke promotion policy
tags:
  - canonical_smoke
""".strip(),
        encoding="utf-8",
    )
    return config_path


def _write_strategy_portfolio_policy_config(
    *,
    config_path: Path,
) -> Path:
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        """
schema_version: 1
max_strategies: 2
max_strategies_per_signal_family: 1
max_weight_per_strategy: 0.7
min_weight_per_strategy: 0.0
selection_metric: ranking_value
weighting_mode: equal
require_active_only: false
require_promotion_eligible_only: true
deduplicate_source_runs: true
diversification_dimension: signal_family
fallback_equal_weight_mode: true
warn_on_same_family_overlap: true
output_inactive_status: inactive
notes: canonical smoke strategy portfolio policy
tags:
  - canonical_smoke
""".strip(),
        encoding="utf-8",
    )
    return config_path


def _write_bundle_experiment_config(
    *,
    bundle: dict[str, Path],
    output_dir: Path,
) -> Path:
    config_path = output_dir / "canonical_bundle_experiment.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        f"""
baseline:
  bundle_dir: {bundle['run_bundle_path'].parent.as_posix()}
  promoted_dir: {bundle['promoted_dir'].as_posix()}
  artifacts_root: {(bundle['alpha_output_dir'].parent).as_posix()}
paths:
  output_dir: {output_dir.as_posix()}
baseline_variant_name: baseline
policy_inputs:
  preset_set: policy_sensitivity_v1
""".strip(),
        encoding="utf-8",
    )
    return config_path


def _write_bundle_experiment_matrix_config(
    *,
    bundles: list[tuple[str, dict[str, Path]]],
    output_dir: Path,
) -> Path:
    case_blocks: list[str] = []
    for case_id, bundle in bundles:
        case_blocks.append(
            f"""
  - case_id: {case_id}
    label: {case_id}
    bundle_dir: {bundle['run_bundle_path'].parent.as_posix()}
    promoted_dir: {bundle['promoted_dir'].as_posix()}
    artifacts_root: {(bundle['alpha_output_dir'].parent).as_posix()}
""".rstrip()
        )
    config_path = output_dir / "canonical_bundle_experiment_matrix.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        f"""
experiment_name: policy_sensitivity_time_stability
baseline_variant_name: baseline
paths:
  output_dir: {output_dir.as_posix()}
policy_inputs:
  preset_set: policy_sensitivity_v1
cases:
{chr(10).join(case_blocks)}
""".strip(),
        encoding="utf-8",
    )
    return config_path


def _snapshot_canonical_config_manifest(
    *,
    output_dir: Path,
    stage_configs: dict[str, Path],
) -> Path:
    manifest_dir = output_dir / "config_manifest"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_entries: list[dict[str, str]] = []
    for stage_name, source_path in stage_configs.items():
        snapshot_path = manifest_dir / source_path.name
        shutil.copy2(source_path, snapshot_path)
        manifest_entries.append(
            {
                "stage": stage_name,
                "source_path": str(source_path),
                "snapshot_path": str(snapshot_path),
                "content_hash": hashlib.sha256(snapshot_path.read_bytes()).hexdigest()[:12],
            }
        )
    manifest_path = manifest_dir / "canonical_config_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_type": "canonical_config_manifest",
                "stage_count": len(manifest_entries),
                "stages": manifest_entries,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return manifest_path


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
    alpha_config_path = _write_alpha_research_config(
        symbols=list(symbol_inputs),
        feature_dir=feature_dir,
        output_dir=alpha_output_dir,
        tracker_dir=tmp_path / "artifacts" / "experiment_tracking",
        config_path=tmp_path / "alpha_research.yaml",
    )
    cmd_alpha_research(
        SimpleNamespace(
            config=str(alpha_config_path),
            symbols=None,
            universe=None,
            feature_dir="data/features",
            signal_family="momentum",
            lookbacks=[5, 10, 20, 60],
            horizons=[1, 5, 20],
            min_rows=126,
            equity_context_enabled=False,
            equity_context_include_volume=False,
            enable_ensemble=False,
            ensemble_mode="disabled",
            ensemble_weight_method="equal",
            ensemble_normalize_scores="rank_pct",
            ensemble_max_members=5,
            ensemble_max_members_per_family=None,
            ensemble_minimum_member_observations=0,
            ensemble_minimum_member_metric=None,
            top_quantile=0.2,
            bottom_quantile=0.2,
            output_dir="artifacts/alpha_research",
            train_size=756,
            test_size=63,
            step_size=None,
            min_train_size=None,
            portfolio_top_n=10,
            portfolio_long_quantile=0.2,
            portfolio_short_quantile=0.2,
            commission=0.0,
            min_price=None,
            min_volume=None,
            min_avg_dollar_volume=None,
            max_adv_participation=0.05,
            max_position_pct_of_adv=0.1,
            max_notional_per_name=None,
            slippage_bps_per_turnover=0.0,
            slippage_bps_per_adv=10.0,
            dynamic_recent_quality_window=20,
            dynamic_min_history=5,
            dynamic_downweight_mean_rank_ic=0.01,
            dynamic_deactivate_mean_rank_ic=-0.02,
            regime_aware_enabled=False,
            regime_min_history=5,
            regime_underweight_mean_rank_ic=0.01,
            regime_exclude_mean_rank_ic=-0.01,
            experiment_tracker_dir=None,
            _cli_argv=["--config", str(alpha_config_path)],
        )
    )

    alpha_result = json.loads((alpha_output_dir / "research_run.json").read_text(encoding="utf-8"))
    artifact_paths = alpha_result["artifact_paths"]
    assert (alpha_output_dir / "research_run.json").exists()
    assert Path(artifact_paths["signal_performance_by_sub_universe_path"]).exists()
    assert Path(artifact_paths["signal_performance_by_benchmark_context_path"]).exists()
    assert Path(artifact_paths["research_context_coverage_path"]).exists()

    promoted_dir = tmp_path / "artifacts" / "promoted_strategies"
    promotion_config_path = _write_promotion_policy_config(
        config_path=tmp_path / "promotion_policy.yaml",
    )
    cmd_research_promote(
        SimpleNamespace(
            artifacts_root=str(tmp_path / "artifacts" / "alpha_research"),
            registry_dir=None,
            output_dir=str(promoted_dir),
            policy_config=str(promotion_config_path),
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
    strategy_portfolio_config_path = _write_strategy_portfolio_policy_config(
        config_path=tmp_path / "strategy_portfolio_policy.yaml",
    )
    cmd_strategy_portfolio_build(
        SimpleNamespace(
            promoted_dir=str(promoted_dir),
            policy_config=str(strategy_portfolio_config_path),
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
        "refresh_config_path": refresh_config_path,
        "alpha_config_path": alpha_config_path,
        "promotion_config_path": promotion_config_path,
        "strategy_portfolio_config_path": strategy_portfolio_config_path,
        "promoted_dir": promoted_dir,
        "strategy_portfolio_dir": strategy_portfolio_dir,
        "multi_strategy_config_path": Path(export_paths["multi_strategy_config_path"]),
        "pipeline_config_path": Path(export_paths["pipeline_config_path"]),
        "run_bundle_path": Path(export_paths["run_bundle_path"]),
    }


def _build_canonical_exported_bundle_with_inputs(
    tmp_path: Path,
    *,
    symbol_inputs: dict[str, tuple[float, float]],
) -> dict[str, Path]:
    data_root = tmp_path / "data"
    normalized_dir = data_root / "normalized"
    feature_dir = data_root / "features"
    metadata_dir = data_root / "metadata"
    normalized_dir.mkdir(parents=True, exist_ok=True)

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
  - {"\n  - ".join(symbol_inputs.keys())}
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

    alpha_output_dir = tmp_path / "artifacts" / "alpha_research" / "run_smoke"
    alpha_config_path = _write_alpha_research_config(
        symbols=list(symbol_inputs),
        feature_dir=feature_dir,
        output_dir=alpha_output_dir,
        tracker_dir=tmp_path / "artifacts" / "experiment_tracking",
        config_path=tmp_path / "alpha_research.yaml",
    )
    cmd_alpha_research(
        SimpleNamespace(
            config=str(alpha_config_path),
            symbols=None,
            universe=None,
            feature_dir="data/features",
            signal_family="momentum",
            lookbacks=[5, 10, 20, 60],
            horizons=[1, 5, 20],
            min_rows=126,
            equity_context_enabled=False,
            equity_context_include_volume=False,
            enable_ensemble=False,
            ensemble_mode="disabled",
            ensemble_weight_method="equal",
            ensemble_normalize_scores="rank_pct",
            ensemble_max_members=5,
            ensemble_max_members_per_family=None,
            ensemble_minimum_member_observations=0,
            ensemble_minimum_member_metric=None,
            top_quantile=0.2,
            bottom_quantile=0.2,
            output_dir="artifacts/alpha_research",
            train_size=756,
            test_size=63,
            step_size=None,
            min_train_size=None,
            portfolio_top_n=10,
            portfolio_long_quantile=0.2,
            portfolio_short_quantile=0.2,
            commission=0.0,
            min_price=None,
            min_volume=None,
            min_avg_dollar_volume=None,
            max_adv_participation=0.05,
            max_position_pct_of_adv=0.1,
            max_notional_per_name=None,
            slippage_bps_per_turnover=0.0,
            slippage_bps_per_adv=10.0,
            dynamic_recent_quality_window=20,
            dynamic_min_history=5,
            dynamic_downweight_mean_rank_ic=0.01,
            dynamic_deactivate_mean_rank_ic=-0.02,
            regime_aware_enabled=False,
            regime_min_history=5,
            regime_underweight_mean_rank_ic=0.01,
            regime_exclude_mean_rank_ic=-0.01,
            experiment_tracker_dir=None,
            _cli_argv=["--config", str(alpha_config_path)],
        )
    )
    alpha_result = json.loads((alpha_output_dir / "research_run.json").read_text(encoding="utf-8"))

    promoted_dir = tmp_path / "artifacts" / "promoted_strategies"
    promotion_config_path = _write_promotion_policy_config(
        config_path=tmp_path / "promotion_policy.yaml",
    )
    cmd_research_promote(
        SimpleNamespace(
            artifacts_root=str(tmp_path / "artifacts" / "alpha_research"),
            registry_dir=None,
            output_dir=str(promoted_dir),
            policy_config=str(promotion_config_path),
            validation=None,
            top_n=1,
            allow_overwrite=False,
            dry_run=False,
            inactive=False,
            override_validation=False,
        )
    )

    strategy_portfolio_dir = tmp_path / "artifacts" / "strategy_portfolio"
    strategy_portfolio_config_path = _write_strategy_portfolio_policy_config(
        config_path=tmp_path / "strategy_portfolio_policy.yaml",
    )
    cmd_strategy_portfolio_build(
        SimpleNamespace(
            promoted_dir=str(promoted_dir),
            policy_config=str(strategy_portfolio_config_path),
            lifecycle=None,
            output_dir=str(strategy_portfolio_dir),
        )
    )

    export_dir = tmp_path / "artifacts" / "strategy_portfolio_bundle"
    export_paths = export_strategy_portfolio_run_config(
        strategy_portfolio_path=strategy_portfolio_dir,
        output_dir=export_dir,
    )
    return {
        "feature_dir": feature_dir,
        "metadata_dir": metadata_dir,
        "alpha_output_dir": alpha_output_dir,
        "refresh_config_path": refresh_config_path,
        "alpha_config_path": alpha_config_path,
        "promotion_config_path": promotion_config_path,
        "strategy_portfolio_config_path": strategy_portfolio_config_path,
        "promoted_dir": promoted_dir,
        "strategy_portfolio_dir": strategy_portfolio_dir,
        "multi_strategy_config_path": Path(export_paths["multi_strategy_config_path"]),
        "pipeline_config_path": Path(export_paths["pipeline_config_path"]),
        "run_bundle_path": Path(export_paths["run_bundle_path"]),
        "research_manifest_path": alpha_output_dir / "research_run.json",
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
    refresh_config_path = bundle["refresh_config_path"]
    alpha_config_path = bundle["alpha_config_path"]
    promotion_config_path = bundle["promotion_config_path"]
    strategy_portfolio_config_path = bundle["strategy_portfolio_config_path"]

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

    paper_pipeline_dir = tmp_path / "artifacts" / "paper_pipeline"
    paper_pipeline_config_path = _write_daily_pipeline_config(
        multi_strategy_config_path=multi_strategy_config_path,
        output_dir=paper_pipeline_dir,
        paper_state_path=tmp_path / "artifacts" / "paper_multi_state.json",
        enable_paper_trading=True,
        enable_live_dry_run=False,
    )
    live_pipeline_dir = tmp_path / "artifacts" / "live_pipeline"
    live_pipeline_config_path = _write_daily_pipeline_config(
        multi_strategy_config_path=multi_strategy_config_path,
        output_dir=live_pipeline_dir,
        paper_state_path=tmp_path / "artifacts" / "live_multi_state.json",
        enable_paper_trading=False,
        enable_live_dry_run=True,
    )
    config_manifest_path = _snapshot_canonical_config_manifest(
        output_dir=tmp_path / "artifacts",
        stage_configs={
            "refresh_research_inputs": refresh_config_path,
            "alpha_research": alpha_config_path,
            "promotion_policy": promotion_config_path,
            "strategy_portfolio_policy": strategy_portfolio_config_path,
            "paper_daily_pipeline": paper_pipeline_config_path,
            "live_daily_pipeline": live_pipeline_config_path,
        },
    )

    pipeline_checks: list[dict[str, object]] = []

    def fake_run_pipeline(config):
        output_root_dir = Path(str(config.output_root_dir))
        output_root_dir.mkdir(parents=True, exist_ok=True)
        artifact_paths: dict[str, str] = {}
        stage_records: list[PipelineStageRecord] = []
        outputs: dict[str, object] = {"bundle_path": str(config.multi_strategy_input_path)}
        if config.stages.paper_trading:
            paper_summary_path = output_root_dir / "paper_run_summary_latest.json"
            paper_summary_path.write_text(json.dumps({"status": "ready", "mode": "paper"}, indent=2), encoding="utf-8")
            artifact_paths["paper_run_summary_latest_json_path"] = str(paper_summary_path)
            outputs["paper_summary"] = {"status": "ready"}
            stage_records.append(PipelineStageRecord(stage_name="paper_trading", status="succeeded", duration_seconds=0.01))
        if config.stages.live_dry_run:
            live_summary_path = output_root_dir / "live_dry_run_summary.json"
            live_summary_path.write_text(json.dumps({"status": "ready", "mode": "live_dry_run"}, indent=2), encoding="utf-8")
            artifact_paths["live_dry_run_summary_json_path"] = str(live_summary_path)
            outputs["live_summary"] = {"status": "ready"}
            stage_records.append(PipelineStageRecord(stage_name="live_dry_run", status="succeeded", duration_seconds=0.01))
        pipeline_checks.append(
            {
                "schedule_type": config.schedule_type,
                "multi_strategy_input_path": str(config.multi_strategy_input_path),
                "paper_trading": config.stages.paper_trading,
                "live_dry_run": config.stages.live_dry_run,
            }
        )
        return (
            PipelineRunResult(
                run_name=config.run_name,
                schedule_type=config.schedule_type,
                started_at="2025-01-04T00:00:00Z",
                ended_at="2025-01-04T00:00:01Z",
                status="succeeded",
                run_dir=str(output_root_dir),
                stage_records=stage_records,
                errors=[],
                outputs=outputs,
            ),
            artifact_paths,
        )

    monkeypatch.setattr(
        "trading_platform.cli.commands.pipeline_run.run_orchestration_pipeline",
        fake_run_pipeline,
    )

    cmd_pipeline_run_daily(SimpleNamespace(config=str(paper_pipeline_config_path)))
    cmd_pipeline_run_daily(SimpleNamespace(config=str(live_pipeline_config_path)))

    assert pipeline_checks[0]["schedule_type"] == "daily"
    assert Path(str(pipeline_checks[0]["multi_strategy_input_path"])) == multi_strategy_config_path
    assert pipeline_checks[0]["paper_trading"] is True
    assert pipeline_checks[0]["live_dry_run"] is False
    assert pipeline_checks[1]["paper_trading"] is False
    assert pipeline_checks[1]["live_dry_run"] is True
    assert (paper_pipeline_dir / "paper_run_summary_latest.json").exists()
    assert (live_pipeline_dir / "live_dry_run_summary.json").exists()
    manifest_payload = json.loads(config_manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["manifest_type"] == "canonical_config_manifest"
    assert manifest_payload["stage_count"] == 6
    assert {entry["stage"] for entry in manifest_payload["stages"]} == {
        "refresh_research_inputs",
        "alpha_research",
        "promotion_policy",
        "strategy_portfolio_policy",
        "paper_daily_pipeline",
        "live_daily_pipeline",
    }
    assert all(Path(entry["snapshot_path"]).exists() for entry in manifest_payload["stages"])


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


def test_canonical_bundle_experiment_harness_reuses_exported_bundle(
    tmp_path: Path,
) -> None:
    bundle = _build_canonical_exported_bundle(tmp_path)
    experiment_output_dir = tmp_path / "artifacts" / "bundle_experiment"
    config_path = _write_bundle_experiment_config(
        bundle=bundle,
        output_dir=experiment_output_dir,
    )

    cmd_strategy_portfolio_experiment_bundle(
        SimpleNamespace(
            config=str(config_path),
        )
    )

    summary_path = experiment_output_dir / "experiment_summary.json"
    results_json_path = experiment_output_dir / "experiment_variant_results.json"
    results_csv_path = experiment_output_dir / "experiment_variant_results.csv"
    comparison_csv_path = experiment_output_dir / "experiment_policy_comparison.csv"
    assert summary_path.exists()
    assert results_json_path.exists()
    assert results_csv_path.exists()
    assert comparison_csv_path.exists()

    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    result_rows = summary_payload["variants"]
    assert summary_payload["baseline_variant_name"] == "baseline"
    assert summary_payload["preset_set"] == "policy_sensitivity_v1"
    assert {row["variant_name"] for row in result_rows} == {
        "baseline",
        "strict_promotion",
        "loose_promotion",
        "alternate_weighting",
        "combined_strict_weighting",
    }

    baseline_row = next(row for row in result_rows if row["variant_name"] == "baseline")
    strict_row = next(row for row in result_rows if row["variant_name"] == "strict_promotion")
    loose_row = next(row for row in result_rows if row["variant_name"] == "loose_promotion")
    metric_row = next(row for row in result_rows if row["variant_name"] == "alternate_weighting")
    combined_row = next(row for row in result_rows if row["variant_name"] == "combined_strict_weighting")

    assert baseline_row["is_baseline"] is True
    assert strict_row["promotion_rerun"] is True
    assert loose_row["promotion_rerun"] is True
    assert metric_row["portfolio_weighting_mode"] == "metric_weighted"
    assert combined_row["portfolio_weighting_mode"] == "score_then_cap"
    assert metric_row["paper_ready"] is True
    assert metric_row["live_ready"] is True
    assert "conditional_variant_count" in strict_row
    assert "max_strategy_weight" in metric_row
    assert "effective_strategy_count" in metric_row
    assert "signal_family_count" in metric_row
    assert "allocation_l1_delta_vs_baseline" in metric_row
    assert "promotion_compare_condition_to_unconditional" in strict_row
    assert "experiment_policy_comparison_csv_path" in summary_payload["paths"]

    for row in result_rows:
        assert Path(row["strategy_portfolio_json_path"]).exists()
        assert Path(row["multi_strategy_config_path"]).exists()
        assert Path(row["pipeline_config_path"]).exists()
        assert Path(row["daily_pipeline_config_path"]).exists()
        assert Path(row["run_bundle_path"]).exists()
        assert Path(row["effective_strategy_portfolio_policy_path"]).exists()
        assert Path(row["effective_promotion_policy_path"]).exists()
        assert Path(row["config_manifest_path"]).exists()
        config_manifest_payload = json.loads(Path(row["config_manifest_path"]).read_text(encoding="utf-8"))
        assert config_manifest_payload["manifest_type"] == "canonical_config_manifest"
        assert {entry["stage"] for entry in config_manifest_payload["stages"]} == {
            "promotion_policy",
            "strategy_portfolio_policy",
            "daily_pipeline_config",
        }
        assert all(Path(entry["snapshot_path"]).exists() for entry in config_manifest_payload["stages"])

    comparison_df = pd.read_csv(comparison_csv_path)
    assert set(comparison_df["variant_name"]) == {
        "baseline",
        "strict_promotion",
        "loose_promotion",
        "alternate_weighting",
        "combined_strict_weighting",
    }
    assert "paper_ready" in comparison_df.columns
    assert "live_ready" in comparison_df.columns
    assert "conditional_variant_count" in comparison_df.columns
    assert "allocation_l1_delta_vs_baseline" in comparison_df.columns
    assert "max_strategy_weight_delta" in comparison_df.columns


def test_canonical_bundle_experiment_matrix_summarizes_policy_stability_across_cases(
    tmp_path: Path,
) -> None:
    bundle_a = _build_canonical_exported_bundle_with_inputs(
        tmp_path / "case_a",
        symbol_inputs={
            "AAPL": (100.0, 0.010),
            "MSFT": (210.0, 0.011),
            "NVDA": (290.0, 0.017),
            "AMD": (80.0, 0.015),
        },
    )
    bundle_b = _build_canonical_exported_bundle_with_inputs(
        tmp_path / "case_b",
        symbol_inputs={
            "AAPL": (105.0, 0.008),
            "MSFT": (205.0, 0.014),
            "NVDA": (315.0, 0.020),
            "AMD": (78.0, 0.012),
        },
    )
    matrix_output_dir = tmp_path / "artifacts" / "bundle_experiment_matrix"
    config_path = _write_bundle_experiment_matrix_config(
        bundles=[
            ("2026-03-20", bundle_a),
            ("2026-03-21", bundle_b),
        ],
        output_dir=matrix_output_dir,
    )

    cmd_strategy_portfolio_experiment_bundle_matrix(
        SimpleNamespace(
            config=str(config_path),
        )
    )

    case_results_path = matrix_output_dir / "bundle_case_results.json"
    stability_csv_path = matrix_output_dir / "experiment_time_stability.csv"
    stability_json_path = matrix_output_dir / "experiment_time_stability.json"
    stability_summary_path = matrix_output_dir / "bundle_policy_stability_summary.json"
    assert case_results_path.exists()
    assert stability_csv_path.exists()
    assert stability_json_path.exists()
    assert stability_summary_path.exists()

    case_results_payload = json.loads(case_results_path.read_text(encoding="utf-8"))
    assert {row["case_id"] for row in case_results_payload["cases"]} == {
        "2026-03-20",
        "2026-03-21",
    }
    for case_row in case_results_payload["cases"]:
        assert {variant["variant_name"] for variant in case_row["variants"]} == {
            "baseline",
            "strict_promotion",
            "loose_promotion",
            "alternate_weighting",
            "combined_strict_weighting",
        }
        for variant in case_row["variants"]:
            assert Path(variant["config_manifest_path"]).exists()
            assert Path(variant["variant_output_dir"]).exists()
            assert Path(variant["strategy_portfolio_json_path"]).exists()
            assert Path(variant["run_bundle_path"]).exists()
            assert Path(variant["daily_pipeline_config_path"]).exists()

    stability_summary = json.loads(stability_summary_path.read_text(encoding="utf-8"))
    assert stability_summary["experiment_name"] == "policy_sensitivity_time_stability"
    assert stability_summary["preset_set"] == "policy_sensitivity_v1"
    assert stability_summary["case_count"] == 2

    variant_rows = stability_summary["variant_stability"]
    assert {row["variant_name"] for row in variant_rows} == {
        "baseline",
        "strict_promotion",
        "loose_promotion",
        "alternate_weighting",
        "combined_strict_weighting",
    }
    baseline_row = next(row for row in variant_rows if row["variant_name"] == "baseline")
    assert "allocation_l1_delta_vs_baseline_mean" in baseline_row
    assert "paper_ready_pass_count" in baseline_row
    assert "live_ready_pass_count" in baseline_row

    stability_df = pd.read_csv(stability_csv_path)
    assert set(stability_df["case_id"]) == {"2026-03-20", "2026-03-21"}
    assert "variant_name" in stability_df.columns
    assert "paper_ready" in stability_df.columns
    assert "live_ready" in stability_df.columns

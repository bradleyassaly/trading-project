"""Debug script: reproduce the automated orchestration test failure."""
from __future__ import annotations
import json, sys, os
sys.path.insert(0, 'src')
os.chdir('C:/Users/bradl/PycharmProjects/trading_platform')

from pathlib import Path
import tempfile
tmp = Path(tempfile.mkdtemp())

from trading_platform.orchestration.pipeline_runner import (
    AutomatedOrchestrationConfig, AutomatedOrchestrationStageToggles, run_automated_orchestration
)
import trading_platform.orchestration.pipeline_runner as pr

# Write policy files
for name in ['promotion', 'strategy_validation', 'strategy_portfolio', 'strategy_monitoring',
             'market_regime', 'adaptive_allocation', 'strategy_governance']:
    (tmp / f'{name}.yaml').write_text('schema_version: 1\n', encoding='utf-8')

config = AutomatedOrchestrationConfig(
    run_name='automation',
    schedule_frequency='manual',
    research_artifacts_root=str(tmp / 'research'),
    output_root_dir=str(tmp / 'runs'),
    promotion_policy_config_path=str(tmp / 'promotion.yaml'),
    strategy_validation_policy_config_path=str(tmp / 'strategy_validation.yaml'),
    strategy_portfolio_policy_config_path=str(tmp / 'strategy_portfolio.yaml'),
    strategy_monitoring_policy_config_path=str(tmp / 'strategy_monitoring.yaml'),
    market_regime_policy_config_path=str(tmp / 'market_regime.yaml'),
    adaptive_allocation_policy_config_path=str(tmp / 'adaptive_allocation.yaml'),
    strategy_governance_policy_config_path=str(tmp / 'strategy_governance.yaml'),
    strategy_lifecycle_path=str(tmp / 'strategy_lifecycle.json'),
    paper_state_path=str(tmp / 'paper_state.json'),
    market_regime_input_path=str(tmp / 'prices.csv'),
    stages=AutomatedOrchestrationStageToggles(
        research=True, registry=True, validation=True, promotion=True,
        portfolio=True, allocation=True, paper=True, monitoring=True,
        regime=True, adaptive_allocation=True, governance=True, kill_switch=True,
    ),
)

pr._now_utc = lambda: '2026-03-22T00:00:00+00:00'
pr.perf_counter = lambda: 1.0

allocation_result = type('AR', (), {
    'as_of': '2026-03-22',
    'combined_target_weights': {'AAPL': 1.0},
    'latest_prices': {'AAPL': 100.0},
    'sleeve_rows': [{'symbol': 'AAPL'}],
    'sleeve_bundles': [],
    'summary': {
        'enabled_sleeve_count': 1, 'gross_exposure_after_constraints': 1.0,
        'turnover_estimate': 0.1, 'turnover_cap_binding': False, 'symbols_removed_or_clipped': [],
        'requested_active_strategy_count': 1, 'requested_symbol_count': 1,
        'pre_validation_target_symbol_count': 1, 'usable_symbol_count': 1, 'skipped_symbol_count': 0,
        'zero_target_reason': '', 'target_drop_stage': '', 'target_drop_reason': '',
        'latest_price_source_summary': {}, 'active_strategy_count': 1,
        'generated_preset_path': '', 'signal_artifact_path': '',
        'net_exposure_after_constraints': 1.0, 'turnover_cap_binding': False,
    },
})()

pr.load_research_manifests = lambda root: [{'run_id': 'run-a'}]
pr.build_research_registry = lambda **kw: {'registry_json_path': str(Path(kw['output_dir'])/'rr.json'), 'run_count': 1}
pr.build_research_leaderboard = lambda **kw: {'leaderboard_json_path': str(Path(kw['output_dir'])/'lb.json'), 'row_count': 1}
pr.build_strategy_validation = lambda **kw: {'strategy_validation_json_path': str(Path(kw['output_dir'])/'sv.json'), 'strategy_validation_csv_path': str(Path(kw['output_dir'])/'sv.csv'), 'pass_count': 1, 'weak_count': 0, 'fail_count': 0}
pr.build_promotion_candidates = lambda **kw: ((Path(kw['output_dir'])/'pc.json').write_text(json.dumps({'rows': [{'run_id': 'run-a'}]}), encoding='utf-8'), {'promotion_candidates_json_path': str(Path(kw['output_dir'])/'pc.json'), 'eligible_count': 1})[1]
pr.apply_research_promotions = lambda **kw: {'selected_count': 1, 'dry_run': False, 'promoted_index_path': str(Path(kw['output_dir'])/'pi.json'), 'promoted_rows': [{'preset_name': 'generated_a'}]}
pr.build_strategy_portfolio = lambda **kw: {'selected_count': 1, 'warning_count': 0, 'strategy_portfolio_json_path': str(Path(kw['output_dir'])/'sp.json'), 'strategy_portfolio_csv_path': str(Path(kw['output_dir'])/'sp.csv')}
pr.load_strategy_portfolio = lambda path: {'summary': {'total_selected_strategies': 1}, 'warnings': []}
pr.export_strategy_portfolio_run_config = lambda **kw: {'multi_strategy_config_path': str(Path(kw['output_dir'])/'ms.json'), 'pipeline_config_path': str(Path(kw['output_dir'])/'pl.yaml'), 'run_bundle_path': str(Path(kw['output_dir'])/'rb.json')}
pr.allocate_multi_strategy_portfolio = lambda cfg: allocation_result
pr.write_multi_strategy_artifacts = lambda result, output_dir: {'allocation_summary_json_path': Path(output_dir)/'as.json'}
pr.run_paper_trading_cycle_for_targets = lambda **kw: type('PR', (), {'orders': [], 'as_of': '2026-03-22'})()
pr.write_paper_trading_artifacts = lambda **kw: {'paper_summary_json_path': Path(kw['output_dir'])/'ps.json'}
pr.persist_paper_run_outputs = lambda **kw: ({'paper_run_summary_latest_json_path': Path(kw['output_dir'])/'prl.json'}, [], {'current_equity': 100000.0})
pr.register_experiment = lambda *a, **kw: {'experiment_registry_path': str(tmp/'er.csv')}
pr.build_paper_experiment_record = lambda output_dir: {}
pr.build_strategy_monitoring_snapshot = lambda **kw: {'strategy_monitoring_json_path': str(Path(kw['output_dir'])/'sm.json'), 'warning_strategy_count': 1, 'deactivation_candidate_count': 1, 'kill_switch_recommendations_json_path': str(Path(kw['output_dir'])/'ks.json')}
pr.detect_market_regime = lambda **kw: {'market_regime_json_path': str(Path(kw['output_dir'])/'mr.json'), 'market_regime_csv_path': str(Path(kw['output_dir'])/'mr.csv'), 'regime_label': 'trend', 'confidence_score': 0.7, 'latest': {'regime_label': 'trend', 'confidence_score': 0.7}}
pr.build_adaptive_allocation = lambda **kw: ((Path(kw['output_dir'])/'aa.json').write_text(json.dumps({'summary': {'total_selected_strategies': 1, 'warning_count': 0}}), encoding='utf-8'), {'adaptive_allocation_json_path': str(Path(kw['output_dir'])/'aa.json'), 'adaptive_allocation_csv_path': str(Path(kw['output_dir'])/'aa.csv'), 'selected_count': 1, 'warning_count': 0, 'absolute_weight_change': 0.05})[1]
pr.export_adaptive_allocation_run_config = lambda **kw: {'multi_strategy_config_path': str(Path(kw['output_dir'])/'ams.json'), 'pipeline_config_path': str(Path(kw['output_dir'])/'ap.yaml'), 'run_bundle_path': str(Path(kw['output_dir'])/'ab.json')}
pr.apply_strategy_governance = lambda **kw: {'strategy_lifecycle_json_path': str(Path(kw['output_dir'])/'sl.json'), 'strategy_lifecycle_csv_path': str(Path(kw['output_dir'])/'sl.csv'), 'strategy_governance_summary_json_path': str(Path(kw['output_dir'])/'sg.json'), 'under_review_count': 1, 'degraded_count': 0, 'demoted_count': 0}
pr.recommend_kill_switch_actions = lambda **kw: {'kill_switch_recommendations_json_path': str(Path(kw['output_dir'])/'ks.json'), 'kill_switch_recommendations_csv_path': str(Path(kw['output_dir'])/'ks.csv'), 'recommendation_count': 1}

result, paths = run_automated_orchestration(config)
print('status:', result.status)
for rec in result.stage_records:
    print(f'  {rec.stage_name}: {rec.status}', f'-- {rec.error_message}' if rec.status == 'failed' else '')

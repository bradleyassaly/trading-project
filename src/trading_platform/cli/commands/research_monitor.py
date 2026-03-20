from __future__ import annotations

from pathlib import Path

from trading_platform.research.refresh_monitoring import (
    MonitoringConfig,
    build_monitoring_report,
)


def cmd_research_monitor(args) -> None:
    result = build_monitoring_report(
        config=MonitoringConfig(
            tracker_dir=Path(args.tracker_dir),
            output_dir=Path(args.output_dir),
            snapshot_dir=Path(args.snapshot_dir) if args.snapshot_dir else None,
            alpha_artifact_dir=Path(args.alpha_artifact_dir) if args.alpha_artifact_dir else None,
            paper_artifact_dir=Path(args.paper_artifact_dir) if args.paper_artifact_dir else None,
            recent_paper_runs=args.recent_paper_runs,
            performance_degradation_buffer=args.performance_degradation_buffer,
            turnover_spike_multiple=args.turnover_spike_multiple,
            concentration_spike_multiple=args.concentration_spike_multiple,
            signal_churn_threshold=args.signal_churn_threshold,
        )
    )
    print(f"Monitoring report: {result['monitoring_report_path']}")
    print(f"Drift alerts: {result['drift_alerts_path']}")

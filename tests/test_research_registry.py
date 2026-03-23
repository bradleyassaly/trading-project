from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pandas as pd

from trading_platform.cli.commands.research_compare_runs import cmd_research_compare_runs
from trading_platform.cli.commands.research_leaderboard import cmd_research_leaderboard
from trading_platform.cli.commands.research_promotion_candidates import cmd_research_promotion_candidates
from trading_platform.cli.commands.research_registry_build import cmd_research_registry_build
from trading_platform.research.registry import (
    build_promotion_candidates,
    build_research_leaderboard,
    build_research_registry,
    compare_research_runs,
    write_research_run_manifest,
)


def _write_research_run(
    root: Path,
    *,
    run_name: str,
    signal_family: str,
    universe: str,
    mean_spearman_ic: float,
    portfolio_sharpe: float,
    promoted_signal_count: int,
    folds_tested: int = 4,
    return_drag: float = 0.05,
    promotion_status: str = "promote",
    rejection_reason: str = "",
) -> Path:
    run_dir = root / run_name
    run_dir.mkdir(parents=True, exist_ok=True)
    leaderboard_path = run_dir / "leaderboard.csv"
    fold_results_path = run_dir / "fold_results.csv"
    promoted_path = run_dir / "promoted_signals.csv"
    portfolio_metrics_path = run_dir / "portfolio_metrics.csv"
    implementability_path = run_dir / "implementability_report.csv"
    diagnostics_path = run_dir / "signal_diagnostics.json"

    pd.DataFrame(
        [
            {
                "signal_family": signal_family,
                "lookback": 20,
                "horizon": 5,
                "mean_spearman_ic": mean_spearman_ic,
                "mean_hit_rate": 0.55,
                "mean_turnover": 0.12,
                "promotion_status": promotion_status,
                "rejection_reason": rejection_reason,
            }
        ]
    ).to_csv(leaderboard_path, index=False)
    pd.DataFrame(
        [
            {
                "fold_id": idx + 1,
                "test_start": f"2025-0{idx + 1}-01",
                "test_end": f"2025-0{idx + 1}-28",
            }
            for idx in range(folds_tested)
        ]
    ).to_csv(fold_results_path, index=False)
    pd.DataFrame([{"signal_family": signal_family}] * promoted_signal_count).to_csv(promoted_path, index=False)
    pd.DataFrame(
        [
            {
                "sharpe": portfolio_sharpe,
                "total_return": 0.18,
                "max_drawdown": -0.08,
            }
        ]
    ).to_csv(portfolio_metrics_path, index=False)
    pd.DataFrame([{"return_drag": return_drag}]).to_csv(implementability_path, index=False)
    diagnostics_path.write_text(
        json.dumps(
            {
                "evaluation_mode": "cross_sectional_long_short",
                "promotion_rules": {"min_mean_rank_ic": 0.01},
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    manifest_path = write_research_run_manifest(
        output_dir=run_dir,
        workflow_type="alpha_research",
        command="test",
        feature_dir=root / "features",
        signal_family=signal_family,
        universe=universe,
        symbols_requested=["AAPL", "MSFT", "NVDA"],
        lookbacks=[20],
        horizons=[5],
        min_rows=250,
        train_size=756,
        test_size=63,
        step_size=63,
        min_train_size=252,
        artifact_paths={
            "leaderboard_path": leaderboard_path,
            "fold_results_path": fold_results_path,
            "promoted_signals_path": promoted_path,
            "portfolio_metrics_path": portfolio_metrics_path,
            "implementability_report_path": implementability_path,
            "signal_diagnostics_path": diagnostics_path,
        },
    )
    return manifest_path


def test_manifest_generation(tmp_path: Path) -> None:
    manifest_path = _write_research_run(
        tmp_path,
        run_name="run_a",
        signal_family="momentum",
        universe="nasdaq100",
        mean_spearman_ic=0.04,
        portfolio_sharpe=1.2,
        promoted_signal_count=2,
    )

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert manifest["workflow_type"] == "alpha_research"
    assert manifest["signal_family"] == "momentum"
    assert manifest["folds_tested"] == 4
    assert manifest["promotion_recommendation"]["eligible"] is True


def test_registry_build_and_leaderboard_ranking(tmp_path: Path) -> None:
    _write_research_run(
        tmp_path,
        run_name="run_a",
        signal_family="momentum",
        universe="nasdaq100",
        mean_spearman_ic=0.04,
        portfolio_sharpe=1.2,
        promoted_signal_count=2,
    )
    _write_research_run(
        tmp_path,
        run_name="run_b",
        signal_family="value",
        universe="sp500",
        mean_spearman_ic=0.02,
        portfolio_sharpe=0.7,
        promoted_signal_count=1,
    )

    registry_result = build_research_registry(artifacts_root=tmp_path, output_dir=tmp_path / "registry")
    leaderboard_result = build_research_leaderboard(
        artifacts_root=tmp_path,
        output_dir=tmp_path / "leaderboard",
        metric="portfolio_sharpe",
        group_by="none",
        limit=10,
    )

    registry_payload = json.loads(Path(registry_result["registry_json_path"]).read_text(encoding="utf-8"))
    leaderboard_payload = json.loads(Path(leaderboard_result["leaderboard_json_path"]).read_text(encoding="utf-8"))

    assert registry_payload["summary"]["run_count"] == 2
    assert leaderboard_payload["rows"][0]["run_id"] == "run_a"
    assert leaderboard_payload["rows"][0]["rank"] == 1


def test_promotion_candidate_logic_marks_ineligible_runs(tmp_path: Path) -> None:
    _write_research_run(
        tmp_path,
        run_name="run_good",
        signal_family="momentum",
        universe="nasdaq100",
        mean_spearman_ic=0.04,
        portfolio_sharpe=1.2,
        promoted_signal_count=2,
    )
    _write_research_run(
        tmp_path,
        run_name="run_bad",
        signal_family="reversal",
        universe="sp500",
        mean_spearman_ic=0.001,
        portfolio_sharpe=0.1,
        promoted_signal_count=0,
        folds_tested=1,
        return_drag=0.4,
        promotion_status="reject",
        rejection_reason="unstable_ic",
    )

    result = build_promotion_candidates(artifacts_root=tmp_path, output_dir=tmp_path / "candidates")
    payload = json.loads(Path(result["promotion_candidates_json_path"]).read_text(encoding="utf-8"))

    assert result["eligible_count"] == 1
    assert payload["rows"][0]["run_id"] == "run_good"
    assert payload["rows"][1]["eligible"] is False
    assert "folds_tested" in payload["rows"][1]["reasons"]


def test_manifest_and_promotion_candidates_support_portfolio_sharpe_column(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_portfolio_sharpe"
    run_dir.mkdir(parents=True, exist_ok=True)
    leaderboard_path = run_dir / "leaderboard.csv"
    fold_results_path = run_dir / "fold_results.csv"
    promoted_path = run_dir / "promoted_signals.csv"
    portfolio_metrics_path = run_dir / "portfolio_metrics.csv"
    implementability_path = run_dir / "implementability_report.csv"
    diagnostics_path = run_dir / "signal_diagnostics.json"

    pd.DataFrame(
        [
            {
                "signal_family": "momentum",
                "lookback": 20,
                "horizon": 5,
                "mean_spearman_ic": 0.04,
                "mean_hit_rate": 0.55,
                "mean_turnover": 0.12,
                "promotion_status": "promote",
                "rejection_reason": "none",
            }
        ]
    ).to_csv(leaderboard_path, index=False)
    pd.DataFrame(
        [
            {"fold_id": 1, "test_start": "2025-01-01", "test_end": "2025-01-28"},
            {"fold_id": 2, "test_start": "2025-02-01", "test_end": "2025-02-28"},
            {"fold_id": 3, "test_start": "2025-03-01", "test_end": "2025-03-28"},
        ]
    ).to_csv(fold_results_path, index=False)
    pd.DataFrame([{"signal_family": "momentum"}]).to_csv(promoted_path, index=False)
    pd.DataFrame(
        [
            {
                "portfolio_sharpe": 1.25,
                "portfolio_total_return": 0.18,
                "portfolio_max_drawdown": -0.08,
            }
        ]
    ).to_csv(portfolio_metrics_path, index=False)
    pd.DataFrame([{"return_drag": 0.05}]).to_csv(implementability_path, index=False)
    diagnostics_path.write_text(json.dumps({"evaluation_mode": "cross_sectional_long_short"}, indent=2), encoding="utf-8")

    manifest_path = write_research_run_manifest(
        output_dir=run_dir,
        workflow_type="alpha_research",
        command="test",
        feature_dir=tmp_path / "features",
        signal_family="momentum",
        universe="nasdaq100",
        symbols_requested=["AAPL", "MSFT", "NVDA"],
        lookbacks=[20],
        horizons=[5],
        min_rows=250,
        train_size=756,
        test_size=63,
        step_size=63,
        min_train_size=252,
        artifact_paths={
            "leaderboard_path": leaderboard_path,
            "fold_results_path": fold_results_path,
            "promoted_signals_path": promoted_path,
            "portfolio_metrics_path": portfolio_metrics_path,
            "implementability_report_path": implementability_path,
            "signal_diagnostics_path": diagnostics_path,
        },
    )

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["top_metrics"]["portfolio_sharpe"] == 1.25
    assert manifest["top_metrics"]["rejection_reason"] is None
    assert manifest["promotion_recommendation"]["eligible"] is True

    result = build_promotion_candidates(artifacts_root=tmp_path, output_dir=tmp_path / "candidates")
    payload = json.loads(Path(result["promotion_candidates_json_path"]).read_text(encoding="utf-8"))

    assert result["eligible_count"] == 1
    assert payload["rows"][0]["portfolio_sharpe"] == 1.25
    assert "portfolio_sharpe missing" not in payload["rows"][0]["reasons"]


def test_compare_runs_writes_deterministic_artifacts(tmp_path: Path) -> None:
    _write_research_run(
        tmp_path,
        run_name="run_a",
        signal_family="momentum",
        universe="nasdaq100",
        mean_spearman_ic=0.04,
        portfolio_sharpe=1.2,
        promoted_signal_count=2,
    )
    _write_research_run(
        tmp_path,
        run_name="run_b",
        signal_family="momentum",
        universe="nasdaq100",
        mean_spearman_ic=0.03,
        portfolio_sharpe=0.9,
        promoted_signal_count=1,
    )

    result = compare_research_runs(
        artifacts_root=tmp_path,
        run_id_a="run_a",
        run_id_b="run_b",
        output_dir=tmp_path / "compare",
    )

    payload = json.loads(Path(result["comparison_json_path"]).read_text(encoding="utf-8"))
    markdown = Path(result["comparison_md_path"]).read_text(encoding="utf-8")

    assert payload["run_a"]["run_id"] == "run_a"
    assert payload["run_b"]["run_id"] == "run_b"
    assert "portfolio_sharpe" in markdown


def test_research_cli_commands_write_outputs(tmp_path: Path, capsys) -> None:
    _write_research_run(
        tmp_path,
        run_name="run_a",
        signal_family="momentum",
        universe="nasdaq100",
        mean_spearman_ic=0.04,
        portfolio_sharpe=1.2,
        promoted_signal_count=2,
    )
    _write_research_run(
        tmp_path,
        run_name="run_b",
        signal_family="value",
        universe="sp500",
        mean_spearman_ic=0.02,
        portfolio_sharpe=0.7,
        promoted_signal_count=1,
    )

    cmd_research_registry_build(Namespace(artifacts_root=str(tmp_path), output_dir=str(tmp_path / "registry")))
    cmd_research_leaderboard(
        Namespace(
            artifacts_root=str(tmp_path),
            output_dir=str(tmp_path / "leaderboard"),
            metric="portfolio_sharpe",
            group_by="none",
            limit=10,
        )
    )
    cmd_research_promotion_candidates(
        Namespace(
            artifacts_root=str(tmp_path),
            output_dir=str(tmp_path / "candidates"),
        )
    )
    cmd_research_compare_runs(
        Namespace(
            artifacts_root=str(tmp_path),
            run_id_a="run_a",
            run_id_b="run_b",
            output_dir=str(tmp_path / "compare"),
        )
    )

    captured = capsys.readouterr().out
    assert "Research registry JSON" in captured
    assert "Leaderboard JSON" in captured
    assert "Promotion candidates JSON" in captured
    assert "Research comparison JSON" in captured

from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

from trading_platform.cli.commands.strategy_governance_apply import cmd_strategy_governance_apply
from trading_platform.cli.commands.strategy_lifecycle_show import cmd_strategy_lifecycle_show
from trading_platform.cli.commands.strategy_lifecycle_update import cmd_strategy_lifecycle_update
from trading_platform.governance.strategy_lifecycle import (
    StrategyGovernancePolicyConfig,
    apply_strategy_governance,
    load_strategy_lifecycle,
)


def _write_inputs(root: Path) -> tuple[Path, Path, Path, Path]:
    promoted_dir = root / "promoted"
    promoted_dir.mkdir(parents=True, exist_ok=True)
    (promoted_dir / "promoted_strategies.json").write_text(
        json.dumps(
            {
                "strategies": [
                    {
                        "preset_name": "generated_momentum_a",
                        "source_run_id": "run-a",
                        "signal_family": "momentum",
                        "universe": "nasdaq100",
                        "status": "active",
                    },
                    {
                        "preset_name": "generated_value_b",
                        "source_run_id": "run-b",
                        "signal_family": "value",
                        "universe": "sp500",
                        "status": "inactive",
                    },
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    validation_dir = root / "validation"
    validation_dir.mkdir()
    (validation_dir / "strategy_validation.json").write_text(
        json.dumps(
            {
                "rows": [
                    {"run_id": "run-a", "signal_family": "momentum", "universe": "nasdaq100", "validation_status": "pass"},
                    {"run_id": "run-b", "signal_family": "value", "universe": "sp500", "validation_status": "weak"},
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    monitoring_dir = root / "monitoring"
    monitoring_dir.mkdir()
    (monitoring_dir / "strategy_monitoring.json").write_text(
        json.dumps(
            {
                "strategies": [
                    {
                        "preset_name": "generated_momentum_a",
                        "recommendation": "deactivate",
                        "attribution_confidence": "low",
                    }
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    adaptive_dir = root / "adaptive"
    adaptive_dir.mkdir()
    (adaptive_dir / "adaptive_allocation.json").write_text(
        json.dumps(
            {
                "strategies": [
                    {"preset_name": "generated_momentum_a", "adjusted_weight": 0.0},
                    {"preset_name": "generated_value_b", "adjusted_weight": 0.4},
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return promoted_dir, validation_dir, monitoring_dir, adaptive_dir


def test_strategy_governance_builds_lifecycle_and_demotion_logic(tmp_path: Path) -> None:
    promoted_dir, validation_dir, monitoring_dir, adaptive_dir = _write_inputs(tmp_path)
    lifecycle_file = tmp_path / "lifecycle" / "strategy_lifecycle.json"
    lifecycle_file.parent.mkdir()
    lifecycle_file.write_text(
        json.dumps(
            {
                "strategies": [
                    {
                        "strategy_id": "generated_momentum_a",
                        "preset_name": "generated_momentum_a",
                        "current_state": "degraded",
                        "transition_history": [{"to_state": "degraded"}],
                        "latest_reasons": ["old"],
                    }
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    result = apply_strategy_governance(
        promoted_dir=promoted_dir,
        strategy_validation_path=validation_dir,
        strategy_monitoring_path=monitoring_dir,
        adaptive_allocation_path=adaptive_dir,
        lifecycle_path=lifecycle_file,
        output_dir=tmp_path / "governance",
        policy=StrategyGovernancePolicyConfig(demote_after_deactivate_events=1),
    )
    payload = load_strategy_lifecycle(lifecycle_file)
    rows = {row["strategy_id"]: row for row in payload["strategies"]}

    assert result["demoted_count"] == 1
    assert rows["generated_momentum_a"]["current_state"] == "demoted"
    assert rows["run-b"]["current_state"] in {"candidate", "under_review"}


def test_strategy_lifecycle_cli_commands_write_outputs(tmp_path: Path, capsys) -> None:
    promoted_dir, validation_dir, monitoring_dir, adaptive_dir = _write_inputs(tmp_path)
    lifecycle_dir = tmp_path / "lifecycle"
    lifecycle_dir.mkdir()

    cmd_strategy_governance_apply(
        Namespace(
            promoted_dir=str(promoted_dir),
            validation=str(validation_dir),
            monitoring=str(monitoring_dir),
            adaptive_allocation=str(adaptive_dir),
            lifecycle=str(lifecycle_dir),
            policy_config=None,
            output_dir=str(tmp_path / "governance"),
            dry_run=False,
        )
    )
    cmd_strategy_lifecycle_show(Namespace(lifecycle=str(lifecycle_dir)))
    cmd_strategy_lifecycle_update(
        Namespace(
            lifecycle=str(lifecycle_dir),
            strategy_id="generated_value_b",
            state="under_review",
            reason="manual_review",
            output_path=None,
        )
    )

    captured = capsys.readouterr().out
    assert "Strategy lifecycle JSON" in captured or "Updated lifecycle" in captured
    assert (lifecycle_dir / "strategy_lifecycle.json").exists()

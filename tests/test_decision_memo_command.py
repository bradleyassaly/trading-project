from __future__ import annotations

import argparse
import json
from pathlib import Path

from trading_platform.cli.commands.decision_memo import cmd_decision_memo


def test_cmd_decision_memo_writes_markdown_and_json(tmp_path: Path) -> None:
    args = argparse.Namespace(
        preset="xsec_nasdaq100_momentum_v1_research",
        deploy_preset="xsec_nasdaq100_momentum_v1_deploy",
        output_dir=str(tmp_path),
        output_stem="validated_xsec",
    )

    cmd_decision_memo(args)

    markdown_path = tmp_path / "validated_xsec_decision_memo.md"
    json_path = tmp_path / "validated_xsec_decision_memo.json"
    assert markdown_path.exists()
    assert json_path.exists()

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["research_preset"]["name"] == "xsec_nasdaq100_momentum_v1_research"
    assert payload["deploy_preset"]["name"] == "xsec_nasdaq100_momentum_v1_deploy"
    assert "robustness_findings" in payload

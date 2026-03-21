from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from trading_platform.cli.presets import build_decision_memo_payload


def _render_markdown(payload: dict[str, object]) -> str:
    research = payload["research_preset"]
    deploy = payload["deploy_preset"]

    def _render_params(title: str, params: dict[str, object]) -> list[str]:
        lines = [f"### {title}"]
        for key, value in sorted(params.items()):
            lines.append(f"- `{key}`: `{value}`")
        return lines

    lines = [
        f"# Strategy Decision Memo: {payload['family_version']}",
        "",
        f"- Generated at: `{payload['generated_at_utc']}`",
        f"- Strategy: `{payload['strategy']}`",
        f"- Universe: `{payload['universe']}`",
        "",
        "## Selected Presets",
        f"- Research baseline: `{research['name']}`",
        f"- Deployable overlay: `{deploy['name']}`",
        "",
        "## Why These Presets",
        "### Research Baseline",
        f"- {payload['baseline_vs_overlay']['pure_topn']}",
        "### Deployable Overlay",
        f"- {payload['baseline_vs_overlay']['transition']}",
        "",
        "## Core Parameters",
    ]
    lines.extend(_render_params("Research Preset", research["params"]))
    lines.append("")
    lines.extend(_render_params("Deploy Preset", deploy["params"]))
    lines.extend(
        [
            "",
            "## Robustness Findings",
            *[f"- {item}" for item in payload["robustness_findings"]],
            "",
            "## Main Caveats",
            *[f"- {item}" for item in payload["main_caveats"]],
            "",
            "## Next Steps",
            *[f"- {item}" for item in payload["next_steps"]],
            "",
        ]
    )
    return "\n".join(lines)


def cmd_decision_memo(args) -> None:
    payload = build_decision_memo_payload(
        research_preset_name=args.preset,
        deploy_preset_name=args.deploy_preset,
    )
    payload["generated_at_utc"] = datetime.now(timezone.utc).isoformat()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = args.output_stem or payload["family_version"]
    markdown_path = output_dir / f"{stem}_decision_memo.md"
    json_path = output_dir / f"{stem}_decision_memo.json"

    markdown_path.write_text(_render_markdown(payload), encoding="utf-8")
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"Generated decision memo markdown: {markdown_path}")
    print(f"Generated decision memo JSON: {json_path}")

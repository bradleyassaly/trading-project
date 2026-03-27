from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from trading_platform.integrations.optional_dependencies import dependency_status

INTEGRATION_REGISTRY: list[dict[str, Any]] = [
    {
        "display_name": "FinanceDatabase",
        "package_name": "financedatabase",
        "config_path": "configs/classification_build.yaml",
        "artifact_paths": [
            "artifacts/reference/classifications/security_master.csv",
            "artifacts/reference/classifications/classification_summary.json",
        ],
        "purpose": "bootstrap/reference classification enrichment",
    },
    {
        "display_name": "Alphalens",
        "package_name": "alphalens",
        "config_path": "configs/alpha_research.yaml",
        "artifact_paths": [
            "artifacts/integration_validation/alpha_small/diagnostics/alphalens/alphalens_ic_summary.csv",
        ],
        "purpose": "research diagnostics",
    },
    {
        "display_name": "QuantStats",
        "package_name": "quantstats",
        "config_path": "configs/alpha_research.yaml",
        "artifact_paths": [
            "artifacts/paper/activated_portfolio_replay_validation/quantstats_validation/quantstats_summary.csv",
        ],
        "purpose": "reporting only",
    },
    {
        "display_name": "PyPortfolioOpt",
        "package_name": "pypfopt",
        "config_path": "configs/portfolio_optimizer_research.yaml",
        "artifact_paths": [
            "artifacts/portfolio_optimizer_research/optimizer_diagnostics.json",
        ],
        "purpose": "allocator research adapters",
    },
    {
        "display_name": "vectorbt",
        "package_name": "vectorbt",
        "config_path": "configs/backtester_validation.yaml",
        "artifact_paths": [
            "artifacts/validation/vectorbt/vectorbt_validation_summary.csv",
        ],
        "purpose": "benchmark validation only",
    },
]


def build_integration_health_report(*, repo_root: str | Path) -> dict[str, Any]:
    root = Path(repo_root)
    integrations: list[dict[str, Any]] = []
    for integration in INTEGRATION_REGISTRY:
        status = dependency_status(integration["package_name"])
        config_path = root / integration["config_path"]
        artifact_paths = [root / path for path in integration.get("artifact_paths", [])]
        warnings: list[str] = []
        if not status.available:
            warnings.append(f'optional package missing; install with pip install -e ".[' f'{status.extra_name}]"')
        if not config_path.exists():
            warnings.append("example config missing")
        missing_artifacts = [str(path) for path in artifact_paths if not path.exists()]
        if missing_artifacts:
            warnings.append("expected validation artifacts missing")
        integrations.append(
            {
                "display_name": integration["display_name"],
                "package_name": status.package_name,
                "purpose": integration["purpose"],
                "available": status.available,
                "version": status.version,
                "import_error": status.import_error,
                "extra_name": status.extra_name,
                "config_exists": config_path.exists(),
                "config_path": str(config_path),
                "artifact_paths": [str(path) for path in artifact_paths],
                "artifact_count_present": sum(path.exists() for path in artifact_paths),
                "artifact_count_expected": len(artifact_paths),
                "warnings": warnings,
            }
        )
    return {
        "integration_count": len(integrations),
        "available_count": sum(1 for item in integrations if item["available"]),
        "integrations": integrations,
    }


def write_integration_health_report(
    *,
    report: dict[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    json_path = output_path / "integration_health_check.json"
    md_path = output_path / "integration_health_check.md"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    lines = [
        "# Integration Health Check",
        "",
        f"- Integrations: `{report['integration_count']}`",
        f"- Available: `{report['available_count']}`",
        "",
        "## Checks",
    ]
    for item in report["integrations"]:
        status = "pass" if item["available"] else "warn"
        lines.append(
            f"- `{status}` {item['display_name']} version={item['version'] or 'missing'} "
            f"config_exists={item['config_exists']} artifacts={item['artifact_count_present']}/{item['artifact_count_expected']}"
        )
        for warning in item["warnings"]:
            lines.append(f"  - warning: {warning}")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {
        "integration_health_check_json_path": json_path,
        "integration_health_check_md_path": md_path,
    }

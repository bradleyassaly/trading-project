from __future__ import annotations

from pathlib import Path

from trading_platform.research.provider_dataset_registry import publish_shared_dataset_registry

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _resolve_path(path: str | None, default: str) -> str:
    candidate = Path(path or default)
    if candidate.is_absolute():
        return str(candidate)
    return str(PROJECT_ROOT / candidate)


def cmd_research_dataset_registry_publish(args) -> None:
    providers = list(getattr(args, "providers", None) or ["kalshi", "polymarket"])
    result = publish_shared_dataset_registry(
        project_root=PROJECT_ROOT,
        registry_path=_resolve_path(getattr(args, "registry_path", None), "data/research/dataset_registry.json"),
        kalshi_config_path=_resolve_path(getattr(args, "kalshi_config", None), "configs/kalshi.yaml"),
        polymarket_config_path=_resolve_path(getattr(args, "polymarket_config", None), "configs/polymarket.yaml"),
        include_providers=providers,
        summary_path=_resolve_path(
            getattr(args, "summary_path", None),
            "artifacts/provider_monitoring/latest_registry_summary.json",
        ),
    )
    print("Research Dataset Registry Publish")
    print(f"  providers      : {', '.join(providers)}")
    print(f"  registry path  : {result.registry_path}")
    print(f"  published      : {result.published_count}")
    if result.dataset_keys:
        print(f"  dataset keys   : {', '.join(result.dataset_keys)}")
    print(f"  summary path   : {result.summary_path}")

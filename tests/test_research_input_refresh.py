from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from trading_platform.cli.commands.refresh_research_inputs import cmd_refresh_research_inputs
from trading_platform.services.research_input_refresh_service import refresh_research_inputs


def test_refresh_research_inputs_builds_features_and_metadata_sidecars(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    normalized_dir = data_root / "normalized"
    feature_dir = data_root / "features"
    metadata_dir = data_root / "metadata"
    normalized_dir.mkdir(parents=True, exist_ok=True)

    for symbol, base_price in {"AAPL": 100.0, "MSFT": 200.0}.items():
        pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=80, freq="D"),
                "symbol": [symbol] * 80,
                "open": [base_price + i for i in range(80)],
                "high": [base_price + i + 1.0 for i in range(80)],
                "low": [base_price + i - 1.0 for i in range(80)],
                "close": [base_price + i + 0.5 for i in range(80)],
                "volume": [1_000_000 + i for i in range(80)],
            }
        ).to_parquet(normalized_dir / f"{symbol}.parquet", index=False)

    result = refresh_research_inputs(
        symbols=["AAPL", "MSFT"],
        universe_name="demo",
        sub_universe_id="demo_screened",
        feature_dir=feature_dir,
        metadata_dir=metadata_dir,
        normalized_dir=normalized_dir,
    )

    assert result.status == "success"
    assert (feature_dir / "AAPL.parquet").exists()
    assert (feature_dir / "MSFT.parquet").exists()
    assert (metadata_dir / "sub_universe_snapshot.csv").exists()
    assert (metadata_dir / "universe_enrichment.csv").exists()
    assert (metadata_dir / "research_metadata_sidecar_summary.json").exists()
    assert (metadata_dir / "research_input_refresh_summary.json").exists()
    assert (metadata_dir / "research_input_bundle_manifest.json").exists()

    summary_payload = json.loads(
        (metadata_dir / "research_input_refresh_summary.json").read_text(encoding="utf-8")
    )
    manifest_payload = json.loads(
        (metadata_dir / "research_input_bundle_manifest.json").read_text(encoding="utf-8")
    )
    sidecar_df = pd.read_csv(metadata_dir / "sub_universe_snapshot.csv")

    assert summary_payload["status"] == "success"
    assert summary_payload["feature_symbols_built"] == ["AAPL", "MSFT"]
    assert manifest_payload["feature_dir"] == str(feature_dir)
    assert manifest_payload["metadata_dir"] == str(metadata_dir)
    assert set(sidecar_df["sub_universe_id"]) == {"demo_screened"}


def test_refresh_research_inputs_reports_partial_failures(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    normalized_dir = data_root / "normalized"
    feature_dir = data_root / "features"
    metadata_dir = data_root / "metadata"
    normalized_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=80, freq="D"),
            "symbol": ["AAPL"] * 80,
            "open": [100.0 + i for i in range(80)],
            "high": [101.0 + i for i in range(80)],
            "low": [99.0 + i for i in range(80)],
            "close": [100.5 + i for i in range(80)],
            "volume": [1_000_000 + i for i in range(80)],
        }
    ).to_parquet(normalized_dir / "AAPL.parquet", index=False)

    result = refresh_research_inputs(
        symbols=["AAPL", "MSFT"],
        universe_name="demo",
        sub_universe_id="demo_screened",
        feature_dir=feature_dir,
        metadata_dir=metadata_dir,
        normalized_dir=normalized_dir,
    )

    assert result.status == "partial_success"
    assert result.feature_symbols_built == ["AAPL"]
    assert result.feature_failures[0]["symbol"] == "MSFT"
    assert (metadata_dir / "research_input_refresh_failures.csv").exists()


def test_cmd_refresh_research_inputs_prints_operator_summary(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    def fake_refresh_research_inputs(**_: object):
        return SimpleNamespace(
            status="success",
            feature_symbols_requested=["AAPL", "MSFT"],
            feature_symbols_built=["AAPL", "MSFT"],
            feature_failures=[],
            feature_dir=tmp_path / "features",
            metadata_dir=tmp_path / "metadata",
            paths={
                "research_input_refresh_summary_json": tmp_path / "metadata" / "research_input_refresh_summary.json",
                "metadata_sub_universe_snapshot_csv": tmp_path / "metadata" / "sub_universe_snapshot.csv",
            },
        )

    monkeypatch.setattr(
        "trading_platform.cli.commands.refresh_research_inputs.refresh_research_inputs",
        fake_refresh_research_inputs,
    )

    args = SimpleNamespace(
        config=None,
        symbols=["AAPL", "MSFT"],
        universe=None,
        feature_groups=["trend"],
        sub_universe_id=None,
        reference_data_root=None,
        universe_membership_path=None,
        taxonomy_snapshot_path=None,
        benchmark_mapping_path=None,
        market_regime_path=None,
        group_map_path=None,
        benchmark=None,
        failure_policy="partial_success",
        feature_dir=str(tmp_path / "features"),
        metadata_dir=str(tmp_path / "metadata"),
        normalized_dir=str(tmp_path / "normalized"),
        _cli_argv=[],
    )

    cmd_refresh_research_inputs(args)

    output = capsys.readouterr().out
    assert "Refreshed research inputs for 2/2 symbol(s)" in output
    assert "Status: success" in output
    assert "Features dir:" in output
    assert "Metadata dir:" in output
def test_cmd_refresh_research_inputs_supports_config_input(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "research_input_refresh.yaml"
    config_path.write_text(
        """
selection:
  universe: nasdaq100
outputs:
  feature_dir: custom/features
  metadata_dir: custom/metadata
  normalized_dir: custom/normalized
reference_data:
  root: artifacts/reference_data/v1
failure_handling:
  policy: fail
""".strip(),
        encoding="utf-8",
    )

    captured: dict[str, object] = {}

    def fake_refresh_research_inputs(*, request):
        captured["request"] = request
        return SimpleNamespace(
            status="success",
            feature_symbols_requested=["AAPL", "MSFT"],
            feature_symbols_built=["AAPL", "MSFT"],
            feature_failures=[],
            feature_dir=request.feature_dir,
            metadata_dir=request.metadata_dir,
            paths={},
        )

    monkeypatch.setattr(
        "trading_platform.cli.commands.refresh_research_inputs.refresh_research_inputs",
        fake_refresh_research_inputs,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.refresh_research_inputs.resolve_symbols",
        lambda args: ["AAPL", "MSFT"],
    )

    args = SimpleNamespace(
        config=str(config_path),
        symbols=None,
        universe=None,
        feature_groups=["trend"],
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
        _cli_argv=["--config", str(config_path)],
    )

    cmd_refresh_research_inputs(args)

    request = captured["request"]
    assert request.universe_name == "nasdaq100"
    assert str(request.feature_dir).endswith("custom\\features")
    assert str(request.metadata_dir).endswith("custom\\metadata")
    assert str(request.normalized_dir).endswith("custom\\normalized")
    assert request.reference_data_root == "artifacts/reference_data/v1"
    assert request.failure_policy == "fail"

"""
Tests for the FastAPI trading platform backend.

Uses httpx.AsyncClient + TestClient against the FastAPI app directly.
All artifact reads are redirected to a temporary directory so tests
run without real data on disk.
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

# Redirect artifact roots before importing the app so the reader picks them up
_TMP = tempfile.mkdtemp()
os.environ["ARTIFACTS_ROOT"] = _TMP
os.environ["DATA_ROOT"] = str(Path(_TMP) / "data")

# Import after env vars are set so artifact_reader sees them
from trading_platform.api.main import app  # noqa: E402
from trading_platform.api import artifact_reader as reader  # noqa: E402

# Point reader at the temp dir (module already loaded; update module vars)
reader.ARTIFACTS_ROOT = Path(_TMP)
reader.DATA_ROOT = Path(_TMP) / "data"

client = TestClient(app, raise_server_exceptions=True)


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def clean_tmp(tmp_path, monkeypatch):
    """
    Each test gets its own clean artifact root so state doesn't leak.
    monkeypatch redirects the reader globals for the duration of the test.
    """
    monkeypatch.setattr(reader, "ARTIFACTS_ROOT", tmp_path)
    monkeypatch.setattr(reader, "DATA_ROOT", tmp_path / "data")
    yield


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def _write_registry_entry(tmp_path: Path) -> str:
    dataset_path = tmp_path / "data" / "research" / "binance_features.parquet"
    _write_parquet(
        dataset_path,
        pd.DataFrame(
            {
                "symbol": ["BTCUSDT", "ETHUSDT"],
                "interval": ["1m", "1m"],
                "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T00:01:00Z"], utc=True),
                "feature_time": pd.to_datetime(["2024-01-01T00:01:00Z", "2024-01-01T00:02:00Z"], utc=True),
                "close": [42000.0, 2200.0],
            }
        ),
    )
    payload = {
        "generated_at": "2024-01-01T00:00:00Z",
        "entry_count": 1,
        "entries": [
            {
                "dataset_key": "binance.crypto_market_features",
                "provider": "binance",
                "asset_class": "crypto",
                "dataset_name": "crypto_market_features",
                "dataset_path": str(dataset_path),
                "storage_type": "parquet_file",
                "symbols": ["BTCUSDT", "ETHUSDT"],
                "intervals": ["1m"],
                "target_horizons": [1],
                "schema_version": "research_dataset_registry_v1",
                "latest_materialized_at": "2024-01-01T00:05:00Z",
                "latest_event_time": "2024-01-01T00:02:00Z",
                "summary_path": str(tmp_path / "artifacts" / "binance" / "summary.json"),
                "manifest_references": {"latest_sync_manifest_path": "artifacts/binance/latest_sync_manifest.json"},
                "health_references": {"health_summary_path": "artifacts/binance/health.json"},
                "metadata": {"time_semantics": {"feature_time": "feature time", "timestamp": "bar time"}, "keys": ["symbol", "interval", "timestamp"]},
            }
        ],
    }
    _write_json(tmp_path / "data" / "research" / "dataset_registry.json", payload)
    return "binance.crypto_market_features"


# ── Health ────────────────────────────────────────────────────────────────────


def test_health():
    resp = client.get("/api/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "timestamp" in data


# ── System status — no artifacts ──────────────────────────────────────────────


def test_system_status_no_artifacts():
    resp = client.get("/api/system/status")
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is True
    assert data["loop_state"] in ("running", "stopped", "trigger_pending")
    assert "active_strategy_count" in data


def test_system_status_kill_switch(tmp_path, monkeypatch):
    monkeypatch.setattr(reader, "ARTIFACTS_ROOT", tmp_path)
    control = tmp_path / "control"
    control.mkdir()
    (control / "KILL_SWITCH").touch()

    resp = client.get("/api/system/status")
    assert resp.status_code == 200
    assert resp.json()["loop_state"] == "stopped"
    assert resp.json()["kill_switch_active"] is True


def test_system_status_decision_log(tmp_path, monkeypatch):
    monkeypatch.setattr(reader, "ARTIFACTS_ROOT", tmp_path)
    _write_jsonl(
        tmp_path / "decision_journal" / "decision_log.jsonl",
        [{"timestamp": "2024-01-01T00:00:00Z", "next_run": "2024-01-01T01:00:00Z", "action": "run"}],
    )

    resp = client.get("/api/system/status")
    assert resp.status_code == 200
    data = resp.json()
    assert data["last_run_timestamp"] == "2024-01-01T00:00:00Z"
    assert data["next_scheduled_run"] == "2024-01-01T01:00:00Z"


# ── Equity curve ──────────────────────────────────────────────────────────────


def test_equity_curve_no_file():
    resp = client.get("/api/pnl/equity-curve")
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is False
    assert "data" in data


def test_equity_curve_with_data(tmp_path, monkeypatch):
    monkeypatch.setattr(reader, "ARTIFACTS_ROOT", tmp_path)
    df = pd.DataFrame({
        "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "equity": [10000.0, 10100.0, 10050.0],
        "daily_return": [0.0, 0.01, -0.005],
        "drawdown": [0.0, 0.0, -0.005],
    })
    _write_csv(tmp_path / "paper" / "paper_equity_curve.csv", df)

    resp = client.get("/api/pnl/equity-curve")
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is True
    assert len(data["data"]) == 3
    assert data["data"][0]["equity"] == 10000.0


# ── P&L summary ───────────────────────────────────────────────────────────────


def test_pnl_summary_no_file():
    resp = client.get("/api/pnl/summary")
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is False


def test_pnl_summary_with_data(tmp_path, monkeypatch):
    monkeypatch.setattr(reader, "ARTIFACTS_ROOT", tmp_path)
    df = pd.DataFrame({
        "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "equity": [10000.0, 10200.0, 10150.0],
        "daily_return": [0.0, 0.02, -0.005],
    })
    _write_csv(tmp_path / "paper" / "paper_equity_curve.csv", df)

    resp = client.get("/api/pnl/summary")
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is True
    assert "total_pnl" in data
    assert data["total_pnl"] == pytest.approx(150.0)


# ── Signal performance ────────────────────────────────────────────────────────


def test_signals_performance_no_file():
    resp = client.get("/api/signals/performance")
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is False


def test_signals_performance_with_backtest_results(tmp_path, monkeypatch):
    monkeypatch.setattr(reader, "ARTIFACTS_ROOT", tmp_path)
    df = pd.DataFrame({
        "signal_family": ["KALSHI_CALIBRATION_DRIFT", "KALSHI_VOLUME_SPIKE"],
        "n_trades": [42, 17],
        "win_rate": [0.6, 0.55],
        "mean_edge": [2.1, 1.5],
        "sharpe": [1.2, 0.8],
        "ic": [0.15, 0.09],
    })
    path = tmp_path / "kalshi_research" / "backtest" / "backtest_results.csv"
    _write_csv(path, df)

    resp = client.get("/api/signals/performance")
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is True
    assert len(data["data"]) == 2
    assert data["data"][0]["signal_family"] == "KALSHI_CALIBRATION_DRIFT"


def test_signals_performance_falls_back_to_leaderboard(tmp_path, monkeypatch):
    monkeypatch.setattr(reader, "ARTIFACTS_ROOT", tmp_path)
    df = pd.DataFrame({"signal_family": ["KALSHI_TIME_DECAY"], "ic": [0.07]})
    _write_csv(tmp_path / "kalshi_research" / "leaderboard.csv", df)

    resp = client.get("/api/signals/performance")
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is True
    assert data["source"] == "leaderboard"


# ── Signal correlation ────────────────────────────────────────────────────────


def test_signals_correlation_no_dir():
    resp = client.get("/api/signals/correlation")
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is False


def test_signals_correlation_with_parquets(tmp_path, monkeypatch):
    monkeypatch.setattr(reader, "DATA_ROOT", tmp_path / "data")
    features_dir = tmp_path / "data" / "kalshi" / "features" / "real"
    features_dir.mkdir(parents=True)

    import numpy as np

    rng = np.random.default_rng(42)
    for ticker in ["MKT-A", "MKT-B", "MKT-C"]:
        df = pd.DataFrame({
            "calibration_drift_z": rng.standard_normal(50),
            "volume_spike_z": rng.standard_normal(50),
            "tension": rng.standard_normal(50),
        })
        _write_parquet(features_dir / f"{ticker}.parquet", df)

    resp = client.get("/api/signals/correlation")
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is True
    assert "signals" in data
    assert len(data["matrix"]) == len(data["signals"])


# ── Kalshi markets ────────────────────────────────────────────────────────────


def test_kalshi_markets_no_dir():
    resp = client.get("/api/kalshi/markets")
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is False


def test_kalshi_markets_with_parquets(tmp_path, monkeypatch):
    monkeypatch.setattr(reader, "DATA_ROOT", tmp_path / "data")
    features_dir = tmp_path / "data" / "kalshi" / "features" / "real"
    features_dir.mkdir(parents=True)

    df = pd.DataFrame({
        "close": [65.0, 66.0, 67.0],
        "volume": [1000, 1100, 1200],
        "calibration_drift_z": [0.1, 0.2, 0.3],
        "volume_spike_z": [0.0, 0.5, -0.2],
    })
    _write_parquet(features_dir / "TRUMP-2024.parquet", df)

    resp = client.get("/api/kalshi/markets")
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is True
    assert len(data["data"]) == 1
    market = data["data"][0]
    assert market["ticker"] == "TRUMP-2024"
    assert market["yes_price"] == pytest.approx(67.0)
    assert "signals" in market


# ── Market history ────────────────────────────────────────────────────────────


def test_market_history_missing():
    resp = client.get("/api/kalshi/market/NO-SUCH-TICKER/history")
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is False


def test_market_history_with_parquet(tmp_path, monkeypatch):
    monkeypatch.setattr(reader, "DATA_ROOT", tmp_path / "data")
    features_dir = tmp_path / "data" / "kalshi" / "features" / "real"
    features_dir.mkdir(parents=True)

    df = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=5, freq="D"),
        "close": [50.0, 55.0, 60.0, 58.0, 62.0],
        "calibration_drift_z": [0.0, 0.1, 0.3, 0.2, 0.4],
    })
    _write_parquet(features_dir / "TEST-MARKET.parquet", df)

    resp = client.get("/api/kalshi/market/TEST-MARKET/history")
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is True
    assert data["ticker"] == "TEST-MARKET"
    assert len(data["data"]) == 5
    assert data["data"][0]["yes_price"] == pytest.approx(50.0)


def test_market_history_path_traversal_rejected(tmp_path, monkeypatch):
    """Path traversal in ticker must never expose files outside the feature dir.

    FastAPI may reject encoded slashes at the routing level (404) or our handler
    sanitises and returns available=False (200).  Either is correct — the important
    thing is it never returns 500 and never opens an arbitrary path.
    """
    monkeypatch.setattr(reader, "DATA_ROOT", tmp_path / "data")
    (tmp_path / "data" / "kalshi" / "features" / "real").mkdir(parents=True, exist_ok=True)

    resp = client.get("/api/kalshi/market/..%2F..%2Fetc%2Fpasswd/history")
    # 404 (routing rejected encoded slashes) or 200 available=False are both correct
    assert resp.status_code in (200, 404)
    if resp.status_code == 200:
        assert resp.json()["available"] is False


# ── Reasoning trades ──────────────────────────────────────────────────────────


def test_reasoning_trades_no_file():
    resp = client.get("/api/reasoning/trades")
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is False


def test_reasoning_trades_with_data(tmp_path, monkeypatch):
    monkeypatch.setattr(reader, "ARTIFACTS_ROOT", tmp_path)
    df = pd.DataFrame({
        "trade_id": ["t1", "t2"],
        "ticker": ["MKT-A", "MKT-B"],
        "signal": ["CALIBRATION_DRIFT", "VOLUME_SPIKE"],
        "score": [1.2, 0.8],
        "entry_price": [55.0, 45.0],
        "status": ["open", "closed"],
    })
    _write_csv(tmp_path / "decision_journal" / "trade_decisions.csv", df)

    resp = client.get("/api/reasoning/trades")
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is True
    assert len(data["data"]) == 2
    assert data["data"][0]["ticker"] == "MKT-A"


# ── Loop decisions ────────────────────────────────────────────────────────────


def test_loop_decisions_no_file():
    resp = client.get("/api/loop/decisions")
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is False


def test_loop_decisions_with_log(tmp_path, monkeypatch):
    monkeypatch.setattr(reader, "ARTIFACTS_ROOT", tmp_path)
    rows = [
        {"timestamp": "2024-01-01T10:00:00Z", "action": "run", "reasoning": "scheduled"},
        {"timestamp": "2024-01-01T11:00:00Z", "action": "skip", "reasoning": "no signals"},
    ]
    _write_jsonl(tmp_path / "decision_journal" / "decision_log.jsonl", rows)

    resp = client.get("/api/loop/decisions")
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is True
    assert len(data["data"]) == 2


# ── Loop control ──────────────────────────────────────────────────────────────


def test_loop_control_pause(tmp_path, monkeypatch):
    monkeypatch.setattr(reader, "ARTIFACTS_ROOT", tmp_path)
    resp = client.post("/api/loop/control", json={"action": "pause"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["success"] is True
    assert (tmp_path / "control" / "KILL_SWITCH").exists()


def test_loop_control_resume(tmp_path, monkeypatch):
    monkeypatch.setattr(reader, "ARTIFACTS_ROOT", tmp_path)
    control = tmp_path / "control"
    control.mkdir()
    (control / "KILL_SWITCH").touch()

    resp = client.post("/api/loop/control", json={"action": "resume"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["success"] is True
    assert not (control / "KILL_SWITCH").exists()


def test_loop_control_trigger_now(tmp_path, monkeypatch):
    monkeypatch.setattr(reader, "ARTIFACTS_ROOT", tmp_path)
    resp = client.post("/api/loop/control", json={"action": "trigger_now"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["success"] is True
    assert (tmp_path / "control" / "TRIGGER_NOW").exists()


def test_loop_control_unknown_action():
    resp = client.post("/api/loop/control", json={"action": "explode"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["success"] is False
    assert "Unknown action" in data["message"]


# ── Research run / status ─────────────────────────────────────────────────────


def test_research_run_returns_job_id():
    resp = client.post("/api/research/run")
    assert resp.status_code == 200
    data = resp.json()
    assert "job_id" in data
    assert len(data["job_id"]) > 0


def test_research_status_unknown_job():
    resp = client.get("/api/research/status/does-not-exist")
    assert resp.status_code == 200
    data = resp.json()
    assert data["available"] is False


def test_research_status_known_job():
    run_resp = client.post("/api/research/run")
    job_id = run_resp.json()["job_id"]

    status_resp = client.get(f"/api/research/status/{job_id}")
    assert status_resp.status_code == 200
    data = status_resp.json()
    assert data["job_id"] == job_id
    assert data["status"] in ("queued", "running", "complete", "failed")


# ── JSON safety — NaN / Inf never reach the client ───────────────────────────


def test_nan_inf_coerced_to_null_in_equity_curve(tmp_path, monkeypatch):
    monkeypatch.setattr(reader, "ARTIFACTS_ROOT", tmp_path)
    import numpy as np

    df = pd.DataFrame({
        "date": ["2024-01-01", "2024-01-02"],
        "equity": [10000.0, float("nan")],
        "daily_return": [float("inf"), -0.01],
    })
    _write_csv(tmp_path / "paper" / "paper_equity_curve.csv", df)

    resp = client.get("/api/pnl/equity-curve")
    assert resp.status_code == 200
    # Response must be valid JSON — TestClient already parses it
    data = resp.json()
    rows = data["data"]
    # NaN / Inf values must be serialized as null
    assert rows[0]["daily_return"] is None  # inf -> null
    assert rows[1]["equity"] is None  # nan -> null


# ── Never 500 ─────────────────────────────────────────────────────────────────


def test_all_get_endpoints_never_500():
    """Every GET endpoint must return 2xx even when no artifacts exist."""
    endpoints = [
        "/api/health",
        "/api/system/status",
        "/api/pnl/equity-curve",
        "/api/pnl/summary",
        "/api/signals/performance",
        "/api/signals/correlation",
        "/api/kalshi/markets",
        "/api/kalshi/market/NONEXISTENT/history",
        "/api/reasoning/trades",
        "/api/loop/decisions",
        "/api/research/status/nonexistent-job-id",
        "/api/research/datasets",
        "/api/ops/registry-summary",
        "/api/ops/provider-monitoring",
        "/api/ops/provider-health",
    ]
    for url in endpoints:
        resp = client.get(url)
        assert resp.status_code < 500, f"{url} returned {resp.status_code}"


def test_research_dataset_registry_endpoints(tmp_path, monkeypatch):
    monkeypatch.setattr(reader, "ARTIFACTS_ROOT", tmp_path / "artifacts")
    monkeypatch.setattr(reader, "DATA_ROOT", tmp_path / "data")
    dataset_key = _write_registry_entry(tmp_path)

    list_resp = client.get("/api/research/datasets")
    assert list_resp.status_code == 200
    list_payload = list_resp.json()
    assert list_payload["available"] is True
    assert list_payload["count"] == 1
    assert list_payload["data"][0]["dataset_key"] == dataset_key

    detail_resp = client.get(f"/api/research/datasets/{dataset_key}")
    assert detail_resp.status_code == 200
    detail_payload = detail_resp.json()
    assert detail_payload["available"] is True
    assert detail_payload["data"]["time_column"] == "feature_time"

    rows_resp = client.get(f"/api/research/datasets/{dataset_key}/rows?symbol=BTCUSDT&limit=1")
    assert rows_resp.status_code == 200
    rows_payload = rows_resp.json()
    assert rows_payload["available"] is True
    assert rows_payload["returned_row_count"] == 1
    assert rows_payload["data"][0]["symbol"] == "BTCUSDT"


def test_provider_monitoring_artifact_endpoints(tmp_path, monkeypatch):
    monkeypatch.setattr(reader, "ARTIFACTS_ROOT", tmp_path / "artifacts")
    monkeypatch.setattr(reader, "DATA_ROOT", tmp_path / "data")
    _write_json(
        tmp_path / "artifacts" / "provider_monitoring" / "latest_registry_summary.json",
        {"generated_at": "2024-01-01T00:00:00Z", "entry_count": 3},
    )
    _write_json(
        tmp_path / "artifacts" / "provider_monitoring" / "latest_monitoring_summary.json",
        {"generated_at": "2024-01-01T00:00:00Z", "record_count": 3, "records": [{"provider": "binance"}]},
    )
    _write_json(
        tmp_path / "artifacts" / "provider_monitoring" / "cross_provider_health_summary.json",
        {"generated_at": "2024-01-01T00:00:00Z", "overall_status": "healthy", "provider_summaries": [{"provider": "binance", "status": "healthy"}]},
    )

    registry_resp = client.get("/api/ops/registry-summary")
    assert registry_resp.status_code == 200
    assert registry_resp.json()["available"] is True
    assert registry_resp.json()["entry_count"] == 3

    monitoring_resp = client.get("/api/ops/provider-monitoring")
    assert monitoring_resp.status_code == 200
    assert monitoring_resp.json()["available"] is True
    assert monitoring_resp.json()["record_count"] == 3

    health_resp = client.get("/api/ops/provider-health")
    assert health_resp.status_code == 200
    assert health_resp.json()["available"] is True
    assert health_resp.json()["overall_status"] == "healthy"


def test_provider_detail_and_replay_preview_endpoints(tmp_path, monkeypatch):
    monkeypatch.setattr(reader, "ARTIFACTS_ROOT", tmp_path / "artifacts")
    monkeypatch.setattr(reader, "DATA_ROOT", tmp_path / "data")
    dataset_key = _write_registry_entry(tmp_path)
    _write_json(
        tmp_path / "artifacts" / "provider_monitoring" / "latest_registry_summary.json",
        {"generated_at": "2024-01-01T00:00:00Z", "entry_count": 1},
    )
    _write_json(
        tmp_path / "artifacts" / "provider_monitoring" / "latest_monitoring_summary.json",
        {
            "generated_at": "2024-01-01T00:00:00Z",
            "records": [{"provider": "binance", "dataset_key": dataset_key, "status": "healthy", "stale": False}],
        },
    )
    _write_json(
        tmp_path / "artifacts" / "provider_monitoring" / "cross_provider_health_summary.json",
        {
            "generated_at": "2024-01-01T00:00:00Z",
            "provider_summaries": [{"provider": "binance", "status": "healthy", "dataset_count": 1, "stale_dataset_count": 0}],
        },
    )

    provider_resp = client.get("/api/ops/providers/binance")
    assert provider_resp.status_code == 200
    provider_payload = provider_resp.json()
    assert provider_payload["available"] is True
    assert provider_payload["provider"] == "binance"
    assert len(provider_payload["datasets"]) == 1

    dataset_resp = client.get(f"/api/ops/datasets/{dataset_key}")
    assert dataset_resp.status_code == 200
    dataset_payload = dataset_resp.json()
    assert dataset_payload["available"] is True
    assert dataset_payload["dataset"]["dataset_key"] == dataset_key

    replay_resp = client.get("/api/research/replay/preview?provider=binance&limit=1")
    assert replay_resp.status_code == 200
    replay_payload = replay_resp.json()
    assert replay_payload["available"] is True
    assert replay_payload["returned_row_count"] == 1
    assert replay_payload["summary"]["metadata"]["alignment_mode"] == "outer_union"

    consumer_resp = client.get("/api/research/replay/consumer-preview?provider=binance&limit=1")
    assert consumer_resp.status_code == 200
    consumer_payload = consumer_resp.json()
    assert consumer_payload["available"] is True
    assert "feature_columns" in consumer_payload["summary"]

    evaluation_resp = client.get("/api/research/replay/evaluation-preview?provider=binance&limit=10")
    assert evaluation_resp.status_code == 200
    evaluation_payload = evaluation_resp.json()
    assert evaluation_payload["available"] is True
    assert "consumer_summary" in evaluation_payload
    assert "metrics" in evaluation_payload

    provider_timeline_resp = client.get("/api/ops/providers/binance/timeline")
    assert provider_timeline_resp.status_code == 200
    provider_timeline_payload = provider_timeline_resp.json()
    assert provider_timeline_payload["available"] is True
    assert provider_timeline_payload["scope_type"] == "provider"

    provider_history_resp = client.get("/api/ops/providers/binance/history-summary")
    assert provider_history_resp.status_code == 200
    provider_history_payload = provider_history_resp.json()
    assert provider_history_payload["available"] is True
    assert provider_history_payload["scope_type"] == "provider"

    dataset_timeline_resp = client.get(f"/api/ops/datasets/{dataset_key}/timeline")
    assert dataset_timeline_resp.status_code == 200
    dataset_timeline_payload = dataset_timeline_resp.json()
    assert dataset_timeline_payload["available"] is True
    assert dataset_timeline_payload["scope_type"] == "dataset"

    dataset_history_resp = client.get(f"/api/ops/datasets/{dataset_key}/history-summary")
    assert dataset_history_resp.status_code == 200
    dataset_history_payload = dataset_history_resp.json()
    assert dataset_history_payload["available"] is True
    assert dataset_history_payload["scope_type"] == "dataset"

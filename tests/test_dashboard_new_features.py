from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path

import pytest

from trading_platform.dashboard.server import create_dashboard_app


def _call_app_json(
    app, path: str, method: str = "GET", body: bytes = b""
) -> tuple[str, dict, dict]:
    captured: dict = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = dict(headers)

    path_info = path
    query_string = ""
    if "?" in path:
        path_info, query_string = path.split("?", 1)
    environ = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path_info,
        "QUERY_STRING": query_string,
        "CONTENT_LENGTH": str(len(body)),
        "wsgi.input": BytesIO(body),
    }
    response_body = b"".join(app(environ, start_response))
    return str(captured["status"]), captured["headers"], json.loads(response_body.decode("utf-8"))


def _call_app_raw(
    app, path: str, method: str = "GET", body: bytes = b""
) -> tuple[str, dict, str]:
    captured: dict = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = dict(headers)

    path_info = path
    query_string = ""
    if "?" in path:
        path_info, query_string = path.split("?", 1)
    environ = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path_info,
        "QUERY_STRING": query_string,
        "CONTENT_LENGTH": str(len(body)),
        "wsgi.input": BytesIO(body),
    }
    response_body = b"".join(app(environ, start_response))
    return str(captured["status"]), captured["headers"], response_body.decode("utf-8")


@pytest.fixture()
def app(tmp_path: Path):
    return create_dashboard_app(tmp_path)


@pytest.fixture()
def app_with_orch(tmp_path: Path):
    run_dir = tmp_path / "orchestration" / "daily" / "2026-04-01T00-00-00+00-00"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "orchestration_run.json").write_text(
        json.dumps(
            {
                "run_id": "orch-001",
                "run_name": "daily",
                "status": "succeeded",
                "started_at": "2026-04-01T00:00:00+00:00",
                "ended_at": "2026-04-01T00:10:00+00:00",
                "stage_records": [
                    {
                        "stage_name": "research",
                        "status": "succeeded",
                        "started_at": "2026-04-01T00:01:00+00:00",
                        "ended_at": "2026-04-01T00:02:00+00:00",
                        "outputs": {"research_manifest_count": 12},
                    },
                    {
                        "stage_name": "registry",
                        "status": "succeeded",
                        "started_at": "2026-04-01T00:02:00+00:00",
                        "ended_at": "2026-04-01T00:03:00+00:00",
                        "outputs": {"run_count": 5, "eligible_count": 3},
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return create_dashboard_app(tmp_path)


@pytest.fixture()
def app_with_kalshi(tmp_path: Path):
    markets_data = {
        "markets": [
            {
                "ticker": "AAPL-Q1",
                "title": "Apple Q1 Revenue",
                "yes_bid": 0.75,
                "yes_ask": 0.78,
                "volume": 5000,
                "status": "open",
                "signal_score": 0.82,
                "category": "tech",
            },
            {
                "ticker": "MSFT-Q2",
                "title": "Microsoft Q2 EPS",
                "yes_bid": 0.45,
                "yes_ask": 0.48,
                "volume": 3000,
                "status": "open",
                "signal_score": 0.55,
                "category": "tech",
            },
            {
                "ticker": "CRUDE-JUN",
                "title": "Crude Oil June",
                "yes_bid": 0.22,
                "yes_ask": 0.25,
                "volume": 1500,
                "status": "closed",
                "signal_score": 0.18,
                "category": "commodities",
            },
        ],
        "summary": {
            "count": 3,
            "avg_yes_bid": 0.473,
            "total_volume": 9500,
        },
        "generated_at": "2026-04-01T00:00:00+00:00",
    }
    (tmp_path / "kalshi_markets.json").write_text(json.dumps(markets_data, indent=2), encoding="utf-8")
    return create_dashboard_app(tmp_path)


class TestPnlStripApi:
    def test_returns_200_json(self, app):
        status, headers, data = _call_app_json(app, "/api/pnl-strip")
        assert status.startswith("200")
        assert "application/json" in headers.get("Content-Type", "")

    def test_response_shape(self, app):
        _, _, data = _call_app_json(app, "/api/pnl-strip")
        assert "open_positions" in data
        assert "loop_status" in data
        assert "generated_at" in data

    def test_graceful_on_empty_artifacts(self, app):
        _, _, data = _call_app_json(app, "/api/pnl-strip")
        assert data["loop_status"] in ("unknown", None, "")
        assert isinstance(data["open_positions"], int)


class TestControlStatusApi:
    def test_returns_200(self, app):
        status, _, data = _call_app_json(app, "/api/control/status")
        assert status.startswith("200")

    def test_response_shape(self, app):
        _, _, data = _call_app_json(app, "/api/control/status")
        assert "latest_run" in data
        assert "stage_records" in data
        assert "recent_runs" in data
        assert "control_state" in data
        assert "generated_at" in data

    def test_with_orchestration_data(self, app_with_orch):
        _, _, data = _call_app_json(app_with_orch, "/api/control/status")
        assert data["latest_run"].get("run_id") == "orch-001"
        assert isinstance(data["stage_records"], list)
        assert len(data["stage_records"]) == 2


class TestControlActionApi:
    def test_trigger_writes_file(self, tmp_path: Path):
        app = create_dashboard_app(tmp_path)
        body = json.dumps({"action": "trigger"}).encode("utf-8")
        status, _, data = _call_app_json(app, "/api/control/action", method="POST", body=body)
        assert status.startswith("200")
        assert data["ok"] is True
        assert data["action"] == "trigger"
        control_file = tmp_path / "loop_control.json"
        assert control_file.exists()
        written = json.loads(control_file.read_text(encoding="utf-8"))
        assert written["action"] == "trigger"
        assert "requested_at" in written

    def test_pause_action(self, tmp_path: Path):
        app = create_dashboard_app(tmp_path)
        body = json.dumps({"action": "pause"}).encode("utf-8")
        _, _, data = _call_app_json(app, "/api/control/action", method="POST", body=body)
        assert data["action"] == "pause"

    def test_resume_action(self, tmp_path: Path):
        app = create_dashboard_app(tmp_path)
        body = json.dumps({"action": "resume"}).encode("utf-8")
        _, _, data = _call_app_json(app, "/api/control/action", method="POST", body=body)
        assert data["action"] == "resume"

    def test_empty_body_handled(self, tmp_path: Path):
        app = create_dashboard_app(tmp_path)
        status, _, data = _call_app_json(app, "/api/control/action", method="POST", body=b"")
        assert status.startswith("200")
        assert data["ok"] is True


class TestReasoningApi:
    def test_returns_200(self, app):
        status, _, data = _call_app_json(app, "/api/reasoning/latest")
        assert status.startswith("200")

    def test_response_shape(self, app):
        _, _, data = _call_app_json(app, "/api/reasoning/latest")
        assert "decision_chain" in data
        assert "promotion_candidates" in data
        assert "generated_at" in data

    def test_decision_chain_with_orch_data(self, app_with_orch):
        _, _, data = _call_app_json(app_with_orch, "/api/reasoning/latest")
        chain = data["decision_chain"]
        assert len(chain) == 2
        assert chain[0]["stage_name"] == "research"
        assert "Scanned 12 research artifacts." in chain[0]["description"]
        assert chain[1]["stage_name"] == "registry"
        assert "5 runs" in chain[1]["description"]

    def test_duration_computed(self, app_with_orch):
        _, _, data = _call_app_json(app_with_orch, "/api/reasoning/latest")
        chain = data["decision_chain"]
        assert chain[0]["duration"] == "60.0s"

    def test_empty_artifacts_returns_empty_chain(self, app):
        _, _, data = _call_app_json(app, "/api/reasoning/latest")
        assert data["decision_chain"] == []


class TestKalshiMarketsApi:
    def test_returns_200_empty(self, app):
        status, _, data = _call_app_json(app, "/api/kalshi/markets")
        assert status.startswith("200")
        assert data["markets"] == []
        assert data["source"] == "no_data"

    def test_returns_markets_when_file_exists(self, app_with_kalshi):
        _, _, data = _call_app_json(app_with_kalshi, "/api/kalshi/markets")
        assert len(data["markets"]) == 3
        assert data["source"] == "kalshi_markets.json"
        assert data["summary"]["count"] == 3

    def test_summary_shape(self, app_with_kalshi):
        _, _, data = _call_app_json(app_with_kalshi, "/api/kalshi/markets")
        summary = data["summary"]
        assert "count" in summary


class TestControlPage:
    def test_returns_200_html(self, app):
        status, headers, body = _call_app_raw(app, "/control")
        assert status.startswith("200")
        assert "text/html" in headers.get("Content-Type", "")
        assert "Autonomous Loop Control" in body

    def test_contains_action_buttons(self, app):
        _, _, body = _call_app_raw(app, "/control")
        assert "Trigger Run" in body
        assert "Pause Loop" in body
        assert "Resume Loop" in body

    def test_contains_pnl_strip_on_home_page(self, app):
        _, _, body = _call_app_raw(app, "/")
        assert "pnl-strip" in body
        assert "/api/pnl-strip" in body

    def test_shows_recent_runs_section(self, app):
        _, _, body = _call_app_raw(app, "/control")
        assert "Recent Runs" in body

    def test_with_orch_data(self, app_with_orch):
        _, _, body = _call_app_raw(app_with_orch, "/control")
        assert "orch-001" in body


class TestReasoningPage:
    def test_returns_200_html(self, app):
        status, headers, body = _call_app_raw(app, "/reasoning")
        assert status.startswith("200")
        assert "text/html" in headers.get("Content-Type", "")
        assert "Strategy Reasoning" in body

    def test_shows_decision_chain_section(self, app):
        _, _, body = _call_app_raw(app, "/reasoning")
        assert "Decision Chain" in body

    def test_shows_stage_descriptions_with_orch_data(self, app_with_orch):
        _, _, body = _call_app_raw(app_with_orch, "/reasoning")
        assert "research" in body
        assert "Scanned" in body

    def test_shows_refresh_button(self, app):
        _, _, body = _call_app_raw(app, "/reasoning")
        assert "Refresh" in body


class TestKalshiPage:
    def test_returns_200_html_empty(self, app):
        status, headers, body = _call_app_raw(app, "/kalshi")
        assert status.startswith("200")
        assert "text/html" in headers.get("Content-Type", "")
        assert "Kalshi Markets" in body

    def test_shows_metrics_section(self, app_with_kalshi):
        _, _, body = _call_app_raw(app_with_kalshi, "/kalshi")
        assert "Market Count" in body
        assert "Avg Yes Bid" in body
        assert "Total Volume" in body

    def test_shows_market_rows(self, app_with_kalshi):
        _, _, body = _call_app_raw(app_with_kalshi, "/kalshi")
        assert "AAPL-Q1" in body
        assert "MSFT-Q2" in body
        assert "CRUDE-JUN" in body

    def test_high_prob_badge(self, app_with_kalshi):
        _, _, body = _call_app_raw(app_with_kalshi, "/kalshi")
        assert "high-prob" in body

    def test_low_prob_badge(self, app_with_kalshi):
        _, _, body = _call_app_raw(app_with_kalshi, "/kalshi")
        assert "low-prob" in body

    def test_signal_score_badges(self, app_with_kalshi):
        _, _, body = _call_app_raw(app_with_kalshi, "/kalshi")
        assert "strong buy" in body
        assert "buy" in body
        assert "avoid" in body

    def test_filter_by_status(self, app_with_kalshi):
        _, _, body = _call_app_raw(app_with_kalshi, "/kalshi?status=open")
        assert "AAPL-Q1" in body
        assert "CRUDE-JUN" not in body

    def test_filter_by_category(self, app_with_kalshi):
        _, _, body = _call_app_raw(app_with_kalshi, "/kalshi?category=commodities")
        assert "CRUDE-JUN" in body
        assert "AAPL-Q1" not in body

    def test_polling_script_present(self, app):
        _, _, body = _call_app_raw(app, "/kalshi")
        assert "/api/kalshi/markets" in body

    def test_nav_items_present(self, app):
        _, _, body = _call_app_raw(app, "/kalshi")
        assert "/control" in body
        assert "/reasoning" in body
        assert "/kalshi" in body


class TestNavItems:
    def test_all_new_nav_items_in_sidebar(self, app):
        _, _, body = _call_app_raw(app, "/")
        assert "/control" in body
        assert "/reasoning" in body
        assert "/kalshi" in body

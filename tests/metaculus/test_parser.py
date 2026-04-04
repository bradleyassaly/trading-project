"""Tests for Metaculus parser (uses mocked API responses)."""
from __future__ import annotations

import csv
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from trading_platform.metaculus.parser import MetaculusParser, MetaculusFetchResult


def _question(
    qid: int = 1,
    title: str = "Will X happen?",
    resolution: int = 1,
    n_forecasts: int = 20,
) -> dict:
    base_ts = 1700000000
    history = [
        {"t": base_ts + i * 3600, "x": 0.5 + i * 0.02}
        for i in range(n_forecasts)
    ]
    return {
        "id": qid,
        "title": title,
        "resolution": resolution,
        "resolve_time": "2026-04-01T00:00:00Z",
        "community_prediction": {"history": history},
    }


def _mock_session(questions: list[dict]):
    """Create a mock session that returns paginated questions."""
    session = MagicMock()

    def _get(url, params=None, timeout=None):
        resp = MagicMock()
        resp.status_code = 200
        if "/questions/" in url and not url.rstrip("/").split("/")[-1].isdigit():
            offset = (params or {}).get("offset", 0)
            limit = (params or {}).get("limit", 100)
            page = questions[offset:offset + limit]
            resp.json.return_value = {
                "results": page,
                "next": "http://next" if offset + limit < len(questions) else None,
            }
        else:
            # Detail endpoint
            qid = url.rstrip("/").split("/")[-1]
            for q in questions:
                if str(q["id"]) == str(qid):
                    resp.json.return_value = q
                    break
            else:
                resp.json.return_value = {}
        resp.raise_for_status = MagicMock()
        return resp

    session.get = _get
    session.headers = MagicMock()
    return session


class TestMetaculusParser:
    def test_happy_path(self, tmp_path: Path) -> None:
        questions = [_question(1), _question(2, resolution=0)]
        parser = MetaculusParser()
        parser._session = _mock_session(questions)
        parser._sleep_sec = 0

        result = parser.fetch_resolved(tmp_path, limit=10)

        assert result.questions_fetched == 2
        assert result.questions_processed == 2
        assert result.feature_files_written == 2
        assert result.resolution_records == 2

        # Feature parquets exist
        assert (tmp_path / "features" / "1.parquet").exists()
        assert (tmp_path / "features" / "2.parquet").exists()

        # Resolution CSV
        res_path = tmp_path / "resolution.csv"
        assert res_path.exists()
        with res_path.open() as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 2
        assert float(rows[0]["resolution_price"]) == 100.0  # resolution=1 → YES
        assert float(rows[1]["resolution_price"]) == 0.0    # resolution=0 → NO

    def test_skips_ambiguous_resolution(self, tmp_path: Path) -> None:
        questions = [
            _question(1, resolution=1),
            {**_question(2), "resolution": "ambiguous"},
            {**_question(3), "resolution": None},
        ]
        parser = MetaculusParser()
        parser._session = _mock_session(questions)
        parser._sleep_sec = 0

        result = parser.fetch_resolved(tmp_path, limit=10)

        assert result.questions_fetched == 3
        assert result.questions_skipped_ambiguous == 2
        assert result.questions_processed == 1

    def test_skips_few_forecasts(self, tmp_path: Path) -> None:
        questions = [_question(1, n_forecasts=3)]
        parser = MetaculusParser()
        parser._session = _mock_session(questions)
        parser._sleep_sec = 0

        result = parser.fetch_resolved(tmp_path, limit=10, min_forecasts=5)

        assert result.questions_skipped_few_forecasts == 1
        assert result.questions_processed == 0

    def test_limit_respected(self, tmp_path: Path) -> None:
        questions = [_question(i) for i in range(10)]
        parser = MetaculusParser()
        parser._session = _mock_session(questions)
        parser._sleep_sec = 0

        result = parser.fetch_resolved(tmp_path, limit=3)

        assert result.questions_fetched == 3

    def test_empty_api(self, tmp_path: Path) -> None:
        parser = MetaculusParser()
        parser._session = _mock_session([])
        parser._sleep_sec = 0

        result = parser.fetch_resolved(tmp_path, limit=10)

        assert result.questions_fetched == 0
        assert result.questions_processed == 0

    def test_no_history_skipped(self, tmp_path: Path) -> None:
        q = _question(1)
        q["community_prediction"] = {}  # no history
        parser = MetaculusParser()
        parser._session = _mock_session([q])
        parser._sleep_sec = 0

        result = parser.fetch_resolved(tmp_path, limit=10)

        # Will try detail fetch, which also returns no history
        assert result.questions_processed == 0

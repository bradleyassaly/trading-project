"""Tests for TelegramSender."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from trading_platform.ops.telegram_sender import TelegramSender


class TestTelegramSender:
    def test_send_returns_true_on_200(self):
        sender = TelegramSender(token="fake-token", chat_id="12345")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        with patch("trading_platform.ops.telegram_sender.requests.post", return_value=mock_resp):
            assert sender.send("hello") is True

    def test_send_returns_false_on_error(self):
        sender = TelegramSender(token="fake-token", chat_id="12345")
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.text = "Forbidden"
        with patch("trading_platform.ops.telegram_sender.requests.post", return_value=mock_resp):
            assert sender.send("hello") is False

    def test_send_returns_false_on_exception(self):
        sender = TelegramSender(token="fake-token", chat_id="12345")
        with patch("trading_platform.ops.telegram_sender.requests.post", side_effect=Exception("timeout")):
            assert sender.send("hello") is False

    def test_send_noop_when_not_configured(self):
        sender = TelegramSender(token="", chat_id="")
        assert sender.enabled is False
        assert sender.send("hello") is False

    def test_morning_briefing_truncates(self, tmp_path):
        sender = TelegramSender(token="fake-token", chat_id="12345")
        path = tmp_path / "briefing.md"
        path.write_text("x" * 5000, encoding="utf-8")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        with patch("trading_platform.ops.telegram_sender.requests.post", return_value=mock_resp) as mock_post:
            result = sender.send_morning_briefing(str(path))
            assert result is True
            sent_text = mock_post.call_args[1]["json"]["text"]
            assert len(sent_text) <= 4000

    def test_morning_briefing_missing_file(self, tmp_path):
        sender = TelegramSender(token="fake-token", chat_id="12345")
        assert sender.send_morning_briefing(str(tmp_path / "missing.md")) is False

    def test_send_alert_format(self):
        sender = TelegramSender(token="fake-token", chat_id="12345")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        with patch("trading_platform.ops.telegram_sender.requests.post", return_value=mock_resp) as mock_post:
            sender.send_alert(
                tier=1, wallet="0xabcdef1234567890", question="Will Fed cut rates?",
                direction="YES", amount=500.0, ewr=0.85, edge=0.30, minutes_ago=5.0,
            )
            sent_text = mock_post.call_args[1]["json"]["text"]
            assert "TIER 1 INSIDER" in sent_text
            assert "Will Fed cut rates?" in sent_text
            assert "$500" in sent_text

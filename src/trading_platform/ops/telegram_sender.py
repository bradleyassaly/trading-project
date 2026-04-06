"""
Telegram message sender for trading platform notifications.

Sends formatted messages to a Telegram chat via the Bot API.
Falls back to no-op when credentials are not configured.

Usage::

    from trading_platform.ops.telegram_sender import TelegramSender
    sender = TelegramSender()
    sender.send("Hello from trading platform")
    sender.send_alert(tier=1, wallet="0xabc...", question="Will Fed cut?",
                      direction="YES", amount=500, ewr=0.85, edge=0.30,
                      minutes_ago=5.0)
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

_MAX_MESSAGE_LENGTH = 4000


class TelegramSender:
    """Send messages to Telegram via Bot API."""

    def __init__(
        self,
        token: str | None = None,
        chat_id: str | None = None,
    ) -> None:
        self._token = token or os.environ.get("TELEGRAM_BOT_TOKEN", "")
        self._chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID", "")
        self._enabled = bool(self._token and self._chat_id)
        if not self._enabled:
            logger.info("Telegram not configured (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set)")

    @property
    def enabled(self) -> bool:
        return self._enabled

    def send(self, message: str) -> bool:
        """Send a plain text message. Returns True on success."""
        if not self._enabled:
            return False
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{self._token}/sendMessage",
                json={
                    "chat_id": self._chat_id,
                    "text": message[:_MAX_MESSAGE_LENGTH],
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": True,
                },
                timeout=10,
            )
            if resp.status_code == 200:
                return True
            logger.warning("Telegram send failed: %d %s", resp.status_code, resp.text[:100])
            return False
        except Exception as exc:
            logger.warning("Telegram send error: %s", exc)
            return False

    def send_morning_briefing(self, briefing_path: str | Path) -> bool:
        """Read a markdown briefing file and send it."""
        path = Path(briefing_path)
        if not path.exists():
            logger.warning("Briefing file not found: %s", path)
            return False
        try:
            text = path.read_text(encoding="utf-8")
        except Exception as exc:
            logger.warning("Failed to read briefing: %s", exc)
            return False

        today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        header = f"MORNING BRIEFING {today}\n\n"
        message = header + text
        if len(message) > _MAX_MESSAGE_LENGTH:
            message = message[:_MAX_MESSAGE_LENGTH - 20] + "\n\n... (truncated)"
        return self.send(message)

    def send_alert(
        self,
        tier: int,
        wallet: str,
        question: str,
        direction: str,
        amount: float,
        ewr: float,
        edge: float,
        minutes_ago: float,
    ) -> bool:
        """Send a formatted smart money alert."""
        icon = "\U0001f534" if tier == 1 else "\U0001f7e1"
        tier_label = f"TIER {tier} INSIDER" if tier == 1 else f"TIER {tier}"
        dir_arrow = "YES ^" if direction == "YES" else "NO v"
        message = (
            f"{icon} {tier_label}\n\n"
            f"Market: {question[:70]}\n"
            f"Direction: {dir_arrow}\n"
            f"Fill size: ${amount:,.0f}\n\n"
            f"Wallet: {wallet[:16]}...\n"
            f"EWR: {ewr:.1%} | Edge: {edge:.1%}\n\n"
            f"Time: {minutes_ago:.0f} min ago"
        )
        return self.send(message)

    def send_weekly_review(self, summary: str) -> bool:
        """Send a weekly review summary."""
        today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        header = f"WEEKLY REVIEW {today}\n\n"
        message = header + summary
        if len(message) > _MAX_MESSAGE_LENGTH:
            message = message[:_MAX_MESSAGE_LENGTH - 20] + "\n\n... (truncated)"
        return self.send(message)

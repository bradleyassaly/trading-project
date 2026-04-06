"""
Alert system for smart money detection.

Sends alerts via Telegram (if configured) and always prints to console.

Usage::

    from trading_platform.polymarket.alert_system import AlertSystem
    alerts = AlertSystem()
    alerts.send_mirror_alert(fill, wallet_profile, "Will Fed cut?", 0.65, 0.03)
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


class AlertSystem:
    """Send smart money alerts via Telegram and/or console."""

    def __init__(
        self,
        telegram_token: str | None = None,
        chat_id: str | None = None,
    ) -> None:
        self._token = telegram_token or os.environ.get("TELEGRAM_BOT_TOKEN")
        self._chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID")
        self._telegram_enabled = bool(self._token and self._chat_id)
        if self._telegram_enabled:
            logger.info("Telegram alerts enabled (chat_id=%s)", self._chat_id)
        else:
            logger.info("Telegram not configured, console-only alerts")

    def send_mirror_alert(
        self,
        fill: dict[str, Any],
        wallet_profile: dict[str, Any],
        question: str,
        current_price: float | None,
        spread: float | None,
        *,
        tier: int = 1,
        direction: str = "YES",
        fill_amount: float = 0.0,
    ) -> None:
        """Format and send a smart money fill alert."""
        tier_label = "TIER 1 INSIDER" if tier == 1 else "TIER 2"
        tier_icon = "[!!]" if tier == 1 else "[!]"
        dir_arrow = "YES ^" if direction == "YES" else "NO v"

        ewr = wallet_profile.get("early_win_rate", 0)
        edge = wallet_profile.get("edge", 0)
        domain = wallet_profile.get("best_domain", "other")
        uet = wallet_profile.get("uncertain_early_trades", 0)
        wallet = fill.get("maker", "")[:16]

        price_str = f"{current_price:.2f}" if current_price is not None else "n/a"
        if spread is not None:
            liq = "LIQUID" if spread < 0.05 else ("TRADEABLE" if spread < 0.15 else "ILLIQUID")
            spread_str = f"{spread:.3f} ({liq})"
        else:
            spread_str = "n/a"

        ts = fill.get("timestamp", 0)
        if ts:
            fill_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            seconds_ago = (datetime.now(tz=timezone.utc) - fill_dt).total_seconds()
            time_str = f"{seconds_ago:.0f}s ago"
        else:
            time_str = "unknown"

        # Console output
        lines = [
            f"",
            f"  {tier_icon} {tier_label}",
            f"",
            f"  Market: {question[:70]}",
            f"  Direction: {dir_arrow}",
            f"  Fill size: ${fill_amount:,.0f}",
            f"  Current price: {price_str}",
            f"  Spread: {spread_str}",
            f"",
            f"  Wallet: {wallet}...",
            f"  EWR: {ewr:.1%} | Edge: {edge:.1%} | Domain: {domain}",
            f"  Uncertain early trades: {uet}",
            f"",
            f"  Time: {time_str}",
            f"",
        ]
        print("\n".join(lines))

        # Telegram
        if self._telegram_enabled:
            tg_icon = "\U0001f534" if tier == 1 else "\U0001f7e1"
            tg_text = (
                f"{tg_icon} {tier_label}\n\n"
                f"Market: {question[:70]}\n"
                f"Direction: {dir_arrow}\n"
                f"Fill size: ${fill_amount:,.0f}\n"
                f"Current price: {price_str}\n"
                f"Spread: {spread_str}\n\n"
                f"Wallet: {wallet}...\n"
                f"EWR: {ewr:.1%} | Edge: {edge:.1%} | Domain: {domain}\n"
                f"UET: {uet}\n\n"
                f"Time: {time_str}"
            )
            self._send_telegram(tg_text)

    def _send_telegram(self, text: str) -> None:
        """Send message via Telegram Bot API."""
        try:
            import requests
            resp = requests.post(
                f"https://api.telegram.org/bot{self._token}/sendMessage",
                json={"chat_id": self._chat_id, "text": text},
                timeout=10,
            )
            if resp.status_code != 200:
                logger.warning("Telegram send failed: %d %s", resp.status_code, resp.text[:100])
        except Exception as exc:
            logger.warning("Telegram send error: %s", exc)

"""CLI: trading-cli ops send-telegram"""
from __future__ import annotations

import argparse


def cmd_ops_send_telegram(args: argparse.Namespace) -> None:
    from dotenv import load_dotenv
    load_dotenv()
    from trading_platform.ops.telegram_sender import TelegramSender

    message = getattr(args, "message", None) or "Trading Platform test message"
    sender = TelegramSender()

    if not sender.enabled:
        print("Telegram not configured. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env")
        return

    ok = sender.send(message)
    if ok:
        print(f"Message sent: {message}")
    else:
        print("Failed to send message. Check token and chat_id.")

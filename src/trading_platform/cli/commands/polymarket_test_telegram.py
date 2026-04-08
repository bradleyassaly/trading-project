"""
CLI command: trading-cli data polymarket test-telegram

Tests Telegram alert configuration by sending one example of every
alert type — base test, order book anomaly, hot market, velocity spike,
and the daily portfolio update with synthetic positions.
"""
from __future__ import annotations

import argparse


def cmd_polymarket_test_telegram(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.telegram_alerts import TelegramAlerter

    alerter = TelegramAlerter()

    if not alerter.enabled:
        print("Telegram alerts NOT configured.")
        print()
        print("Setup instructions:")
        print("  1. Message @BotFather on Telegram, create new bot, copy token")
        print("  2. Message @userinfobot to get your chat_id")
        print("  3. Add to .env file:")
        print("     TELEGRAM_BOT_TOKEN=your_bot_token")
        print("     TELEGRAM_CHAT_ID=your_chat_id")
        print("  4. Restart and run: trading-cli data polymarket test-telegram")
        return

    def _step(label: str, fn) -> None:
        print(f"  {label}...", end=" ", flush=True)
        try:
            ok = fn()
        except Exception as exc:  # noqa: BLE001
            print(f"FAILED ({exc})")
            return
        print("OK" if ok else "skipped")

    print("Sending alert sample to Telegram:")

    _step("base test", alerter.send_test)

    _step(
        "order book anomaly (critical)",
        lambda: alerter.send_order_book_anomaly(
            {
                "anomaly_type": "order_book_imbalance",
                "severity": "critical",
                "question": "Test market \u2014 Iran ceasefire by April 30?",
                "category": "geopolitics",
                "detail": "87% buy-side | $145K bids vs $21K asks",
                "imbalance": 0.87,
                "bid_usd": 145_000,
                "ask_usd": 21_000,
            },
            signal_fired=True,
            paper_trade={"signal_type": "accumulation", "confidence": 0.85, "size_usd": 1800},
        ),
    )

    _step(
        "hot market discovered",
        lambda: alerter.send_hot_market_discovered([
            {
                "question": "Test market \u2014 BTC closes above $100K by year end",
                "category": "crypto",
                "volume_24h": 4_750_000,
                "reasons": ["vol24h=$4,750,000", "ob_imbalance=78%"],
            }
        ]),
    )
    # Reset hot-market debounce so the test always fires it
    alerter._last_hot_market_alert = 0

    _step(
        "velocity spike (loud)",
        lambda: alerter.send_velocity_spike(
            question="Test market \u2014 Will rates be cut at next FOMC?",
            category="economics",
            price_change=0.18,
            velocity=0.012,
            current_price=0.74,
            minutes_elapsed=15.0,
            signal_fired=True,
            paper_trade={"size_usd": 2000, "side": "BUY"},
        ),
    )

    _step(
        "daily portfolio update",
        lambda: alerter.send_daily_update(
            positions=[
                {
                    "side": "YES",
                    "size_usd": 1800,
                    "entry_price": 0.50,
                    "current_price": 0.74,
                    "unrealized_pnl": 864.0,
                    "unrealized_pct": 48.0,
                    "question": "Test market \u2014 Iran ceasefire by April 30?",
                    "signal_type": "accumulation",
                },
                {
                    "side": "YES",
                    "size_usd": 1800,
                    "entry_price": 0.35,
                    "current_price": 0.40,
                    "unrealized_pnl": 257.0,
                    "unrealized_pct": 14.3,
                    "question": "Test market \u2014 Cavaliers vs. Grizzlies O/U 237.5",
                    "signal_type": "market_maker_flip",
                },
            ],
            stats={
                "signals_today": 12,
                "whales_today": 5,
                "markets_discovered": 3,
                "pipeline_success": True,
                "pipeline_duration": 142,
            },
        ),
    )
    # Reset daily-update debounce so the test always fires it
    alerter._last_daily_update = 0

    print("Done \u2014 check your Telegram chat for 5 messages.")

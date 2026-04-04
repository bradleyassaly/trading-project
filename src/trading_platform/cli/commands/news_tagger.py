"""
CLI commands for the economic news calendar.

Usage
-----
    trading-cli data news upcoming --days 7
    trading-cli data news label-moves --ticker KXCPI-26MAY
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def cmd_news_upcoming(args: argparse.Namespace) -> None:
    from trading_platform.signals.news_tagger import EconomicNewsCalendar

    days = int(getattr(args, "days", None) or 7)

    # Build calendar from known Kalshi series patterns
    # Generate tickers for the next 6 months across all known series
    from datetime import datetime, timezone
    now = datetime.now(tz=timezone.utc)
    months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
              "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
    series_list = ["KXCPI", "KXFED", "KXGDP", "KXJOBS", "KXPCE", "KXINFL"]

    tickers: list[str] = []
    for series in series_list:
        for month_offset in range(6):
            dt = now.replace(day=1) + __import__("datetime").timedelta(days=month_offset * 31)
            yy = dt.year % 100
            mon = months[dt.month - 1]
            tickers.append(f"{series}-{yy}{mon}")

    cal = EconomicNewsCalendar(tickers)
    events = cal.get_upcoming_events(days_ahead=days)

    print(f"Upcoming economic events (next {days} days):")
    if not events:
        print("  (none)")
    for ev in events:
        print(f"  {ev.date.strftime('%Y-%m-%d')} | {ev.name} | {ev.category} | {ev.ticker_pattern}")


def cmd_news_label_moves(args: argparse.Namespace) -> None:
    from trading_platform.signals.news_tagger import EconomicNewsCalendar
    from datetime import datetime, timezone

    ticker = args.ticker
    cal = EconomicNewsCalendar([ticker])
    now = datetime.now(tz=timezone.utc)
    label = cal.label_market_move(ticker, now)
    print(f"Ticker: {ticker}")
    print(f"Move at: {now.isoformat()}")
    print(f"Label: {label}")

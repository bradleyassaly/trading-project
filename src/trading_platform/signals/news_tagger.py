"""
Economic news calendar and market move labeler.

Parses Kalshi event tickers to build an economic calendar, then
labels price moves as ``scheduled_release``, ``pre_event``, or
``unscheduled`` based on proximity to known events.

No external API required — event dates are derived directly from
Kalshi ticker conventions (e.g. ``KXCPI-26MAY`` → CPI May 2026).

Usage::

    from trading_platform.signals.news_tagger import EconomicNewsCalendar
    cal = EconomicNewsCalendar()
    events = cal.get_upcoming_events(days_ahead=7)
    label = cal.label_market_move("KXCPI-26MAY-T0.3", move_ts)
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

# ── Kalshi series → readable event name mapping ─────────────────────────────

_SERIES_NAMES: dict[str, tuple[str, str]] = {
    "KXCPI": ("CPI Release", "economics"),
    "KXINFL": ("Inflation Report", "economics"),
    "KXFED": ("Fed Rate Decision", "economics"),
    "KXGDP": ("GDP Report", "economics"),
    "KXJOBS": ("Jobs Report", "economics"),
    "KXPCE": ("PCE Inflation Report", "economics"),
    "KXPRESAPPROVAL": ("Presidential Approval Rating", "politics"),
    "KXSENATE": ("Senate Vote", "politics"),
}

# Month abbreviation → number
_MONTH_MAP: dict[str, int] = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

# Pattern: KXCPI-26MAY, KXFED-27APR, KXGDP-26Q1
_TICKER_DATE_RE = re.compile(
    r"^([A-Z]+)-(\d{2})([A-Z]{3})(?:-|$)",
    re.IGNORECASE,
)

# Pattern for day-level tickers: KXFED-26MAR12 (March 12)
_TICKER_DATE_DAY_RE = re.compile(
    r"^([A-Z]+)-(\d{2})([A-Z]{3})(\d{2})(?:-|$)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class NewsEvent:
    name: str
    date: datetime
    category: str
    ticker_pattern: str
    series: str


class EconomicNewsCalendar:
    """Build an economic calendar from Kalshi ticker conventions."""

    def __init__(self, event_tickers: list[str] | None = None) -> None:
        self._events: list[NewsEvent] = []
        if event_tickers:
            for ticker in event_tickers:
                ev = self._parse_ticker(ticker)
                if ev:
                    self._events.append(ev)

    def add_tickers(self, tickers: list[str]) -> int:
        """Parse tickers and add to internal calendar. Returns count added."""
        added = 0
        for ticker in tickers:
            ev = self._parse_ticker(ticker)
            if ev and ev not in self._events:
                self._events.append(ev)
                added += 1
        return added

    def get_upcoming_events(self, *, days_ahead: int = 7) -> list[NewsEvent]:
        """Return events within the next ``days_ahead`` days, sorted by date."""
        now = datetime.now(tz=timezone.utc)
        cutoff = now + timedelta(days=days_ahead)
        upcoming = [e for e in self._events if now <= e.date <= cutoff]
        upcoming.sort(key=lambda e: e.date)
        return upcoming

    def get_all_events(self) -> list[NewsEvent]:
        """Return all parsed events sorted by date."""
        return sorted(self._events, key=lambda e: e.date)

    def label_market_move(
        self,
        ticker: str,
        move_timestamp: datetime,
    ) -> str:
        """Label a price move relative to known events.

        Returns:
            ``"scheduled_release"`` — move within 2h of a known event
            ``"pre_event"``         — move 24–48h before a known event
            ``"unscheduled"``       — no known catalyst nearby
        """
        # Extract series from the ticker
        series = _extract_series(ticker)
        relevant = [e for e in self._events if e.series == series] if series else self._events

        for event in relevant:
            delta = (event.date - move_timestamp).total_seconds() / 3600.0
            # Within 2h after event (or 1h before — data release window)
            if -1.0 <= delta <= 2.0:
                return "scheduled_release"
            # 24-48h before event
            if 24.0 <= delta <= 48.0:
                return "pre_event"

        return "unscheduled"

    @staticmethod
    def _parse_ticker(ticker: str) -> NewsEvent | None:
        """Parse a Kalshi event/market ticker into a NewsEvent."""
        # Try day-level first: KXFED-26MAR12
        m = _TICKER_DATE_DAY_RE.match(ticker)
        if m:
            series, yy, mon, dd = m.group(1), m.group(2), m.group(3).upper(), m.group(4)
            month = _MONTH_MAP.get(mon)
            if month:
                try:
                    year = 2000 + int(yy)
                    day = int(dd)
                    dt = datetime(year, month, day, 14, 30, tzinfo=timezone.utc)  # typical release time
                    name_tpl, category = _SERIES_NAMES.get(series.upper(), (f"{series} Event", "other"))
                    name = f"{name_tpl} {mon.title()} {day} {year}"
                    return NewsEvent(
                        name=name, date=dt, category=category,
                        ticker_pattern=f"{series}-{yy}{mon}", series=series.upper(),
                    )
                except (ValueError, OverflowError):
                    pass

        # Month-level: KXCPI-26MAY
        m = _TICKER_DATE_RE.match(ticker)
        if m:
            series, yy, mon = m.group(1), m.group(2), m.group(3).upper()
            month = _MONTH_MAP.get(mon)
            if month:
                try:
                    year = 2000 + int(yy)
                    # Default to 15th of month, 8:30 AM ET (typical release)
                    dt = datetime(year, month, 15, 12, 30, tzinfo=timezone.utc)
                    name_tpl, category = _SERIES_NAMES.get(series.upper(), (f"{series} Event", "other"))
                    name = f"{name_tpl} {mon.title()} {year}"
                    return NewsEvent(
                        name=name, date=dt, category=category,
                        ticker_pattern=f"{series}-{yy}{mon}", series=series.upper(),
                    )
                except (ValueError, OverflowError):
                    pass

        return None


def _extract_series(ticker: str) -> str | None:
    m = re.match(r"^([A-Z]+)(?=-\d)", ticker, re.IGNORECASE)
    return m.group(1).upper() if m else None

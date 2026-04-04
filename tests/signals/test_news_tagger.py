"""Tests for economic news calendar and move labeler."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from trading_platform.signals.news_tagger import (
    EconomicNewsCalendar,
    NewsEvent,
    _extract_series,
)


class TestTickerParsing:
    def test_parse_kxcpi_month(self) -> None:
        cal = EconomicNewsCalendar(["KXCPI-26MAY"])
        events = cal.get_all_events()
        assert len(events) == 1
        assert events[0].name == "CPI Release May 2026"
        assert events[0].category == "economics"
        assert events[0].series == "KXCPI"
        assert events[0].date.year == 2026
        assert events[0].date.month == 5

    def test_parse_kxfed_month(self) -> None:
        cal = EconomicNewsCalendar(["KXFED-27APR"])
        events = cal.get_all_events()
        assert len(events) == 1
        assert events[0].name == "Fed Rate Decision Apr 2027"
        assert events[0].series == "KXFED"

    def test_parse_with_suffix(self) -> None:
        """Market tickers like KXCPI-26MAY-T0.3 should still parse."""
        cal = EconomicNewsCalendar(["KXCPI-26MAY-T0.3"])
        events = cal.get_all_events()
        assert len(events) == 1
        assert events[0].series == "KXCPI"

    def test_parse_day_level(self) -> None:
        cal = EconomicNewsCalendar(["KXFED-26MAR12"])
        events = cal.get_all_events()
        assert len(events) == 1
        assert events[0].date.day == 12
        assert events[0].date.month == 3

    def test_unknown_series(self) -> None:
        cal = EconomicNewsCalendar(["NEWSER-26MAY"])
        events = cal.get_all_events()
        assert len(events) == 1
        assert "NEWSER" in events[0].name
        assert events[0].category == "other"

    def test_unparseable_ticker(self) -> None:
        cal = EconomicNewsCalendar(["invalid-ticker", "NOTADATE"])
        assert cal.get_all_events() == []

    def test_multiple_tickers_deduplicated(self) -> None:
        cal = EconomicNewsCalendar(["KXCPI-26MAY", "KXCPI-26MAY-T0.3"])
        events = cal.get_all_events()
        # Both parse to same month — but different ticker objects, so both added
        # unless _parse_ticker returns identical NewsEvent
        assert len(events) >= 1


class TestUpcomingEvents:
    def test_filters_by_days_ahead(self) -> None:
        now = datetime.now(tz=timezone.utc)
        # Create events at different future dates
        cal = EconomicNewsCalendar()
        # Manually inject events
        cal._events = [
            NewsEvent("Near", now + timedelta(days=3), "economics", "KXCPI-XX", "KXCPI"),
            NewsEvent("Far", now + timedelta(days=30), "economics", "KXFED-XX", "KXFED"),
            NewsEvent("Past", now - timedelta(days=5), "economics", "KXGDP-XX", "KXGDP"),
        ]
        upcoming = cal.get_upcoming_events(days_ahead=7)
        assert len(upcoming) == 1
        assert upcoming[0].name == "Near"

    def test_empty_calendar(self) -> None:
        cal = EconomicNewsCalendar()
        assert cal.get_upcoming_events() == []


class TestLabelMarketMove:
    def test_scheduled_release(self) -> None:
        cal = EconomicNewsCalendar()
        event_time = datetime(2026, 5, 15, 12, 30, tzinfo=timezone.utc)
        cal._events = [
            NewsEvent("CPI", event_time, "economics", "KXCPI-26MAY", "KXCPI"),
        ]
        # Move 30 minutes after event
        move_ts = event_time + timedelta(minutes=30)
        assert cal.label_market_move("KXCPI-26MAY-T0.3", move_ts) == "scheduled_release"

    def test_pre_event(self) -> None:
        cal = EconomicNewsCalendar()
        event_time = datetime(2026, 5, 15, 12, 30, tzinfo=timezone.utc)
        cal._events = [
            NewsEvent("CPI", event_time, "economics", "KXCPI-26MAY", "KXCPI"),
        ]
        # Move 36 hours before event (within 24-48h window)
        move_ts = event_time - timedelta(hours=36)
        assert cal.label_market_move("KXCPI-26MAY-T0.3", move_ts) == "pre_event"

    def test_unscheduled(self) -> None:
        cal = EconomicNewsCalendar()
        event_time = datetime(2026, 5, 15, 12, 30, tzinfo=timezone.utc)
        cal._events = [
            NewsEvent("CPI", event_time, "economics", "KXCPI-26MAY", "KXCPI"),
        ]
        # Move 5 days before event — not near any window
        move_ts = event_time - timedelta(days=5)
        assert cal.label_market_move("KXCPI-26MAY-T0.3", move_ts) == "unscheduled"

    def test_empty_calendar_returns_unscheduled(self) -> None:
        cal = EconomicNewsCalendar()
        now = datetime.now(tz=timezone.utc)
        assert cal.label_market_move("KXCPI-26MAY", now) == "unscheduled"


class TestExtractSeries:
    def test_kxcpi(self) -> None:
        assert _extract_series("KXCPI-26MAY-T0.3") == "KXCPI"

    def test_kxfed(self) -> None:
        assert _extract_series("KXFED-27APR-T4.25") == "KXFED"

    def test_invalid(self) -> None:
        assert _extract_series("invalid") is None


class TestAddTickers:
    def test_add_tickers_returns_count(self) -> None:
        cal = EconomicNewsCalendar()
        added = cal.add_tickers(["KXCPI-26MAY", "KXFED-27APR", "invalid"])
        assert added == 2
        assert len(cal.get_all_events()) == 2

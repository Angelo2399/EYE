from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from app.services.briefing_schedule_service import BriefingScheduleService


def test_resolve_schedule_returns_briefing_europe_inside_europe_window() -> None:
    service = BriefingScheduleService()

    result = service.resolve_schedule(
        current_dt=datetime(2026, 4, 1, 8, 30, 0, tzinfo=ZoneInfo("Europe/London")),
        timezone_name="Europe/London",
    )

    assert result.event_type == "briefing_europe"
    assert result.should_send is True
    assert result.scheduled_label == "briefing_europe_08:30"
    assert result.local_time == "08:30:00"


def test_resolve_schedule_returns_briefing_usa_open_inside_us_open_window() -> None:
    service = BriefingScheduleService()

    result = service.resolve_schedule(
        current_dt=datetime(2026, 4, 1, 14, 30, 0, tzinfo=ZoneInfo("Europe/London")),
        timezone_name="Europe/London",
    )

    assert result.event_type == "briefing_usa_open"
    assert result.should_send is True
    assert result.scheduled_label == "briefing_usa_open_14:30"
    assert result.local_time == "14:30:00"


def test_resolve_schedule_returns_no_send_outside_canonical_windows() -> None:
    service = BriefingScheduleService()

    result = service.resolve_schedule(
        current_dt=datetime(2026, 4, 1, 11, 0, 0, tzinfo=ZoneInfo("Europe/London")),
        timezone_name="Europe/London",
    )

    assert result.event_type is None
    assert result.should_send is False
    assert result.scheduled_label is None
    assert result.local_time == "11:00:00"


def test_resolve_schedule_returns_no_send_on_weekend() -> None:
    service = BriefingScheduleService()

    result = service.resolve_schedule(
        current_dt=datetime(2026, 4, 4, 8, 30, 0, tzinfo=ZoneInfo("Europe/London")),
        timezone_name="Europe/London",
    )

    assert result.event_type is None
    assert result.should_send is False
    assert result.scheduled_label is None
    assert result.local_time == "08:30:00"

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from app.services.asset_update_schedule_service import AssetUpdateScheduleService


def test_resolve_schedule_returns_hourly_update_when_last_send_is_outside_hour() -> None:
    service = AssetUpdateScheduleService()

    result = service.resolve_schedule(
        current_dt=datetime(2026, 4, 4, 10, 5, 0, tzinfo=ZoneInfo("Europe/London")),
        timezone_name="Europe/London",
        last_sent_at="2026-04-04T09:15:00+01:00",
        state_change_kind="follow_up",
    )

    assert result.should_send is True
    assert result.send_type == "hourly_update"
    assert result.local_time == "10:05:00"


def test_resolve_schedule_returns_immediate_alert_when_alert_has_no_same_hour_send() -> None:
    service = AssetUpdateScheduleService()

    result = service.resolve_schedule(
        current_dt=datetime(2026, 4, 4, 11, 20, 0, tzinfo=ZoneInfo("Europe/London")),
        timezone_name="Europe/London",
        last_sent_at="2026-04-04T10:05:00+01:00",
        state_change_kind="alert",
    )

    assert result.should_send is True
    assert result.send_type == "immediate_alert"
    assert result.local_time == "11:20:00"


def test_resolve_schedule_skips_duplicate_send_in_same_hour_for_hourly_update() -> None:
    service = AssetUpdateScheduleService()

    result = service.resolve_schedule(
        current_dt=datetime(2026, 4, 4, 10, 45, 0, tzinfo=ZoneInfo("Europe/London")),
        timezone_name="Europe/London",
        last_sent_at="2026-04-04T10:05:00+01:00",
        state_change_kind="follow_up",
    )

    assert result.should_send is False
    assert result.send_type is None
    assert result.local_time == "10:45:00"


def test_resolve_schedule_skips_duplicate_send_in_same_hour_for_alert() -> None:
    service = AssetUpdateScheduleService()

    result = service.resolve_schedule(
        current_dt=datetime(2026, 4, 4, 10, 50, 0, tzinfo=ZoneInfo("Europe/London")),
        timezone_name="Europe/London",
        last_sent_at="2026-04-04T10:01:00+01:00",
        state_change_kind="alert",
    )

    assert result.should_send is False
    assert result.send_type is None
    assert result.local_time == "10:50:00"

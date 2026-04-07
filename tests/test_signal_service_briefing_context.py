from __future__ import annotations

from datetime import datetime

import app.services.signal_service as signal_service_module
from app.services.signal_service import SignalService


def _build_service() -> SignalService:
    return object.__new__(SignalService)


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        fixed_utc = datetime(2026, 4, 1, 7, 15, 0, tzinfo=signal_service_module.timezone.utc)
        if tz is None:
            return fixed_utc.replace(tzinfo=None)
        return fixed_utc.astimezone(tz)


def test_build_briefing_context_returns_timezone_aware_local_and_us_open_fields(
    monkeypatch,
) -> None:
    monkeypatch.setattr(signal_service_module, "datetime", FixedDateTime)

    service = _build_service()
    context = service._build_briefing_context("Europe/London")

    assert context["timezone_name"] == "Europe/London"
    assert context["local_now_iso"] == "2026-04-01T08:15:00+01:00"
    assert context["local_date"] == "2026-04-01"
    assert context["local_time"] == "08:15:00"
    assert context["local_weekday"] == 2
    assert context["is_local_morning_window"] is True
    assert context["us_cash_open_local_time"] == "14:30"
    assert context["minutes_to_us_cash_open"] == 375

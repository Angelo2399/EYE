from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from app.services.signal_service import SignalService


def _build_service() -> SignalService:
    return object.__new__(SignalService)


class RecordingSessionService:
    def __init__(self) -> None:
        self.last_current_dt = None

    def get_session_context(self, current_dt=None):
        self.last_current_dt = current_dt
        return SimpleNamespace(
            phase="open",
            is_session_open=True,
            allow_new_trades=True,
            minutes_to_cutoff=123,
        )


class LegacySessionService:
    def get_session_context(self):
        return SimpleNamespace(
            phase="midday",
            is_session_open=True,
            allow_new_trades=True,
            minutes_to_cutoff=222,
        )


def test_get_session_context_passes_local_runtime_datetime_to_session_service() -> None:
    service = _build_service()
    service.session_service = RecordingSessionService()
    service._last_runtime_context = {
        "timezone_name": "Europe/London",
        "local_now_iso": "2026-04-01T14:35:00+01:00",
    }

    context = service._get_session_context()

    assert context.phase == "open"
    assert service.session_service.last_current_dt == datetime.fromisoformat(
        "2026-04-01T14:35:00+01:00"
    )


def test_get_session_context_falls_back_to_legacy_zero_arg_session_service() -> None:
    service = _build_service()
    service.session_service = LegacySessionService()
    service._last_runtime_context = {
        "timezone_name": "Europe/London",
        "local_now_iso": "2026-04-01T14:35:00+01:00",
    }

    context = service._get_session_context()

    assert context.phase == "midday"
    assert context.minutes_to_cutoff == 222

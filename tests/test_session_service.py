from __future__ import annotations

from datetime import datetime

import pytest

from app.services.session_service import SessionPhase, SessionService


def _dt(year: int, month: int, day: int, hour: int, minute: int) -> datetime:
    return datetime(year, month, day, hour, minute)


@pytest.mark.parametrize(
    (
        "current_dt",
        "expected_phase",
        "expected_open",
        "expected_allow_new_trades",
        "expected_minutes",
    ),
    [
        (_dt(2026, 3, 24, 15, 29), SessionPhase.pre_open, False, False, None),
        (_dt(2026, 3, 24, 15, 30), SessionPhase.open, True, True, 385),
        (_dt(2026, 3, 24, 16, 29), SessionPhase.open, True, True, 326),
        (_dt(2026, 3, 24, 16, 30), SessionPhase.midday, True, True, 325),
        (_dt(2026, 3, 24, 19, 59), SessionPhase.midday, True, True, 116),
        (_dt(2026, 3, 24, 20, 0), SessionPhase.power_hour, True, True, 115),
        (_dt(2026, 3, 24, 21, 25), SessionPhase.power_hour, True, True, 30),
        (_dt(2026, 3, 24, 21, 26), SessionPhase.power_hour, True, False, 29),
        (_dt(2026, 3, 24, 21, 54), SessionPhase.power_hour, True, False, 1),
        (_dt(2026, 3, 24, 21, 55), SessionPhase.closed, False, False, 0),
    ],
)
def test_get_session_context_classifies_intraday_phases(
    current_dt: datetime,
    expected_phase: SessionPhase,
    expected_open: bool,
    expected_allow_new_trades: bool,
    expected_minutes: int | None,
) -> None:
    service = SessionService()

    context = service.get_session_context(current_dt)

    assert context.phase == expected_phase
    assert context.is_session_open is expected_open
    assert context.allow_new_trades is expected_allow_new_trades
    assert context.minutes_to_cutoff == expected_minutes


@pytest.mark.parametrize(
    "current_dt",
    [
        _dt(2026, 3, 28, 18, 0),
        _dt(2026, 3, 29, 18, 0),
    ],
)
def test_get_session_context_returns_closed_on_weekends(
    current_dt: datetime,
) -> None:
    service = SessionService()

    context = service.get_session_context(current_dt)

    assert context.phase == SessionPhase.closed
    assert context.is_session_open is False
    assert context.allow_new_trades is False
    assert context.minutes_to_cutoff is None


def test_session_service_rejects_negative_new_trade_min_minutes() -> None:
    with pytest.raises(ValueError, match="new_trade_min_minutes must be >= 0."):
        SessionService(new_trade_min_minutes=-1)

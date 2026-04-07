from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from enum import Enum


class SessionPhase(str, Enum):
    pre_open = "pre_open"
    open = "open"
    midday = "midday"
    power_hour = "power_hour"
    closed = "closed"


@dataclass(frozen=True)
class SessionContext:
    phase: SessionPhase
    is_session_open: bool
    allow_new_trades: bool
    minutes_to_cutoff: int | None


class SessionService:
    def __init__(
        self,
        session_open: time = time(hour=15, minute=30),
        open_phase_end: time = time(hour=16, minute=30),
        midday_phase_end: time = time(hour=20, minute=0),
        session_cutoff: time = time(hour=21, minute=55),
        new_trade_min_minutes: int = 30,
    ) -> None:
        if new_trade_min_minutes < 0:
            raise ValueError("new_trade_min_minutes must be >= 0.")

        self.session_open = session_open
        self.open_phase_end = open_phase_end
        self.midday_phase_end = midday_phase_end
        self.session_cutoff = session_cutoff
        self.new_trade_min_minutes = new_trade_min_minutes

    def get_session_context(self, now: datetime | None = None) -> SessionContext:
        current_dt = now or datetime.now()

        if current_dt.weekday() >= 5:
            return SessionContext(
                phase=SessionPhase.closed,
                is_session_open=False,
                allow_new_trades=False,
                minutes_to_cutoff=None,
            )

        current_time = time(
            hour=current_dt.hour,
            minute=current_dt.minute,
            second=current_dt.second,
            microsecond=current_dt.microsecond,
        )

        if current_time < self.session_open:
            return SessionContext(
                phase=SessionPhase.pre_open,
                is_session_open=False,
                allow_new_trades=False,
                minutes_to_cutoff=None,
            )

        if current_time >= self.session_cutoff:
            return SessionContext(
                phase=SessionPhase.closed,
                is_session_open=False,
                allow_new_trades=False,
                minutes_to_cutoff=0,
            )

        if current_time < self.open_phase_end:
            phase = SessionPhase.open
        elif current_time < self.midday_phase_end:
            phase = SessionPhase.midday
        else:
            phase = SessionPhase.power_hour

        minutes_to_cutoff = self._minutes_until_cutoff(current_time)
        allow_new_trades = minutes_to_cutoff >= self.new_trade_min_minutes

        return SessionContext(
            phase=phase,
            is_session_open=True,
            allow_new_trades=allow_new_trades,
            minutes_to_cutoff=minutes_to_cutoff,
        )

    def _minutes_until_cutoff(self, current_time: time) -> int:
        current_minutes = (current_time.hour * 60) + current_time.minute
        cutoff_minutes = (self.session_cutoff.hour * 60) + self.session_cutoff.minute
        return max(cutoff_minutes - current_minutes, 0)

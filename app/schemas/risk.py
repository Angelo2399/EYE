from __future__ import annotations

from dataclasses import dataclass

from app.schemas.signal import HoldingWindow


@dataclass(frozen=True)
class RiskPlan:
    entry_min: float | None
    entry_max: float | None
    entry_window: str | None
    expected_holding: HoldingWindow | None
    hard_exit_time: str | None
    close_by_session_end: bool
    stop_loss: float | None
    take_profit_1: float | None
    take_profit_2: float | None
    risk_reward: float | None

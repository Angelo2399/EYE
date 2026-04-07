from __future__ import annotations

from dataclasses import dataclass

from app.schemas.signal import SignalAction


@dataclass(frozen=True)
class SetupScore:
    action: SignalAction
    direction: str
    score: float
    long_score: float
    short_score: float
    trend_score: float
    moving_average_score: float
    rsi_score: float
    price_position_score: float
    regime_score: float
    explanation: str

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel


class DayBias(str, Enum):
    long = "long"
    short = "short"
    neutral = "neutral"


class DayContextLabel(str, Enum):
    trend_up = "trend_up"
    trend_down = "trend_down"
    range_day = "range_day"
    volatile_day = "volatile_day"
    unclear = "unclear"


class DayContext(BaseModel):
    label: DayContextLabel
    bias: DayBias
    confidence_pct: float
    prefer_breakout: bool
    avoid_mean_reversion: bool
    explanation: str

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field

from app.schemas.day_context import DayBias, DayContextLabel
from app.schemas.market import MarketSymbol, MarketTimeframe


class SignalAction(str, Enum):
    long = "long"
    short = "short"
    wait = "wait"
    no_trade = "no_trade"


class ModelConfidence(str, Enum):
    low = "low"
    medium = "medium"
    medium_high = "medium_high"
    high = "high"


class HoldingWindow(str, Enum):
    m30 = "30m"
    h1 = "1h"
    h2 = "2h"
    half_day = "half_day"
    end_of_day = "end_of_day"


class SignalRequest(BaseModel):
    symbol: MarketSymbol = Field(..., description="Supported market symbol.")
    timeframe: MarketTimeframe = Field(
        default=MarketTimeframe.h1,
        description="Supported intraday signal timeframe.",
    )


class SignalResponse(BaseModel):
    asset: str
    action: SignalAction
    entry_min: float | None = None
    entry_max: float | None = None
    entry_window: str | None = None
    expected_holding: HoldingWindow | None = None
    hard_exit_time: str | None = None
    close_by_session_end: bool = True
    stop_loss: float | None = None
    take_profit_1: float | None = None
    take_profit_2: float | None = None
    risk_reward: float | None = None
    favorable_move_pct: float | None = Field(default=None, ge=0, le=100)
    tp1_hit_pct: float | None = Field(default=None, ge=0, le=100)
    stop_hit_first_pct: float | None = Field(default=None, ge=0, le=100)
    model_confidence_pct: float | None = Field(default=None, ge=0, le=100)
    confidence_label: ModelConfidence | None = None
    day_context_label: DayContextLabel | None = None
    day_context_bias: DayBias | None = None
    day_context_confidence_pct: float | None = Field(default=None, ge=0, le=100)
    explanation: str = ""


class StoredSignalResponse(BaseModel):
    id: int
    created_at_utc: str
    symbol: str
    timeframe: str
    asset: str
    action: str
    entry_min: float | None = None
    entry_max: float | None = None
    entry_window: str | None = None
    expected_holding: str | None = None
    hard_exit_time: str | None = None
    close_by_session_end: bool
    stop_loss: float | None = None
    take_profit_1: float | None = None
    take_profit_2: float | None = None
    risk_reward: float | None = None
    favorable_move_pct: float | None = None
    tp1_hit_pct: float | None = None
    stop_hit_first_pct: float | None = None
    model_confidence_pct: float | None = None
    confidence_label: str | None = None
    day_context_label: str | None = None
    day_context_bias: str | None = None
    day_context_confidence_pct: float | None = None
    explanation: str

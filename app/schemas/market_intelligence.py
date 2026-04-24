from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class IntelligenceSourceType(str, Enum):
    market = "market"
    macro_release = "macro_release"
    news = "news"
    speech = "speech"
    transcript = "transcript"
    internal_model = "internal_model"


class IntelligenceImportance(str, Enum):
    low = "low"
    medium = "medium"
    high = "high"
    critical = "critical"


class IntelligenceDirection(str, Enum):
    bullish = "bullish"
    bearish = "bearish"
    neutral = "neutral"
    mixed = "mixed"


class MarketBias(str, Enum):
    long = "long"
    short = "short"
    neutral = "neutral"
    no_trade = "no_trade"


class MarketEventType(str, Enum):
    macro = "macro"
    headline = "headline"
    speech = "speech"
    transcript_segment = "transcript_segment"
    price_action = "price_action"
    volatility = "volatility"
    liquidity = "liquidity"
    regime = "regime"
    internal_signal = "internal_signal"


class MarketIntelligenceItem(BaseModel):
    source: IntelligenceSourceType
    event_type: MarketEventType
    importance: IntelligenceImportance = IntelligenceImportance.medium
    direction: IntelligenceDirection = IntelligenceDirection.neutral
    title: str = ""
    summary: str = ""
    source_name: str = ""
    source_url: str | None = None
    occurred_at_utc: str | None = None
    detected_at_utc: str | None = None
    relevance_score: float = Field(default=50.0, ge=0.0, le=100.0)
    confidence_pct: float = Field(default=50.0, ge=0.0, le=100.0)
    impact_horizon_minutes: int | None = Field(default=None, ge=0)
    asset_scope: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    raw_text: str | None = None
    structured_payload: dict[str, str | float | int | bool | None] = Field(
        default_factory=dict
    )


class MarketIntelligenceSnapshot(BaseModel):
    asset: str
    symbol: str
    timeframe: str
    generated_at_utc: str
    market_bias: MarketBias = MarketBias.neutral
    bias_confidence_pct: float = Field(default=50.0, ge=0.0, le=100.0)
    session_phase: str | None = None
    regime: str | None = None
    volatility_20: float | None = None
    rsi_14: float | None = None
    distance_sma20: float | None = None
    distance_sma50: float | None = None
    key_levels: list[float] = Field(default_factory=list)
    dominant_drivers: list[str] = Field(default_factory=list)
    risk_flags: list[str] = Field(default_factory=list)
    items: list[MarketIntelligenceItem] = Field(default_factory=list)
    synthesis: str = ""


class IntelligenceIngestionResult(BaseModel):
    accepted_items: int = Field(default=0, ge=0)
    discarded_items: int = Field(default=0, ge=0)
    critical_items: int = Field(default=0, ge=0)
    latest_direction: IntelligenceDirection = IntelligenceDirection.neutral
    average_confidence_pct: float = Field(default=0.0, ge=0.0, le=100.0)
    summary: str = ""


class StructuredMarketEvent(BaseModel):
    event_id: str
    asset: str
    symbol: str
    timeframe: str | None = None
    event_type: MarketEventType
    source_type: IntelligenceSourceType
    source_name: str
    source_url: str | None = None
    direction: IntelligenceDirection = IntelligenceDirection.neutral
    urgency: IntelligenceImportance = IntelligenceImportance.medium
    confidence_pct: float = Field(default=50.0, ge=0.0, le=100.0)
    decay_minutes: int | None = Field(default=None, ge=0)
    scenario_change: bool = False
    title: str
    summary: str
    raw_text: str | None = None
    occurred_at_utc: str | None = None
    detected_at_utc: str | None = None
    tags: list[str] = Field(default_factory=list)
    market_context: dict[str, str | float | int | bool | None] = Field(
        default_factory=dict
    )
    event_score: float = Field(default=50.0)


class EventOutcomeFeedback(BaseModel):
    event_id: str
    asset: str
    symbol: str
    observed_after_5m: float | None = None
    observed_after_30m: float | None = None
    observed_after_2h: float | None = None
    session_close_outcome: float | None = None
    event_score: float = Field(default=50.0)
    notes: str = ""

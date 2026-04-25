from __future__ import annotations

from app.services.event_learning_service import EventLearningService


class FakeRepository:
    def get_learning_summary(
        self,
        *,
        symbol: str | None = None,
        lookback_days: int = 90,
        min_samples: int = 3,
    ):
        if symbol == "NDX":
            return [
                {
                    "symbol": "NDX",
                    "event_type": "headline",
                    "source_name": "FedWire",
                    "direction": "bullish",
                    "samples": 5,
                    "avg_event_score": 82.0,
                    "avg_after_5m": 0.7,
                    "avg_after_30m": 1.2,
                    "avg_after_2h": 1.7,
                    "avg_session_close": 2.2,
                }
            ]
        return []


def test_get_adaptive_weight_returns_positive_weight_for_good_history() -> None:
    service = EventLearningService(market_event_repository=FakeRepository())

    result = service.get_adaptive_weight(
        symbol="NDX",
        event_type="headline",
        source_name="FedWire",
        direction="bullish",
    )

    assert result["symbol"] == "NDX"
    assert result["event_type"] == "headline"
    assert result["source_name"] == "FedWire"
    assert result["direction"] == "bullish"
    assert result["samples"] == 5
    assert result["weight_multiplier"] > 1.0
    assert result["learning_bias"] == "positive"


def test_get_adaptive_weight_returns_neutral_fallback_when_no_history() -> None:
    service = EventLearningService(market_event_repository=FakeRepository())

    result = service.get_adaptive_weight(
        symbol="WTI",
        event_type="headline",
        source_name="UnknownSource",
        direction="bullish",
    )

    assert result["symbol"] == "WTI"
    assert result["samples"] == 0
    assert result["weight_multiplier"] == 1.0
    assert result["learning_bias"] == "neutral"


def test_apply_adaptive_weight_updates_event_score_and_market_context() -> None:
    from app.schemas.market_intelligence import (
        IntelligenceDirection,
        IntelligenceImportance,
        IntelligenceSourceType,
        MarketEventType,
        StructuredMarketEvent,
    )

    service = EventLearningService(market_event_repository=FakeRepository())

    event = StructuredMarketEvent(
        event_id="adaptive-test-001",
        asset="Nasdaq 100",
        symbol="NDX",
        timeframe="1h",
        event_type=MarketEventType.headline,
        source_type=IntelligenceSourceType.news,
        source_name="FedWire",
        direction=IntelligenceDirection.bullish,
        urgency=IntelligenceImportance.high,
        confidence_pct=80.0,
        title="Adaptive weight test",
        summary="Should increase score.",
        event_score=70.0,
        market_context={"regime": "bullish"},
    )

    weighted = service.apply_adaptive_weight(event)

    assert weighted.event_score > 70.0
    assert weighted.market_context["regime"] == "bullish"
    assert weighted.market_context["learning_weight_multiplier"] > 1.0
    assert weighted.market_context["learning_bias"] == "positive"
    assert weighted.market_context["learning_samples"] == 5
    assert weighted.market_context["learning_avg_after_30m"] == 1.2
    assert weighted.market_context["learning_avg_after_2h"] == 1.7

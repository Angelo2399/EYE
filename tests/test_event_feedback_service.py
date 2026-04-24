from __future__ import annotations

import pandas as pd

from app.schemas.market import MarketTimeframe
from app.schemas.market_intelligence import (
    IntelligenceDirection,
    IntelligenceImportance,
    IntelligenceSourceType,
    MarketEventType,
    StructuredMarketEvent,
)
from app.services.event_feedback_service import EventFeedbackService


class FakeMarketDataService:
    def get_ohlcv(self, symbol, timeframe):
        if timeframe == MarketTimeframe.m5:
            return pd.DataFrame(
                {
                    "timestamp": pd.to_datetime(
                        [
                            "2026-04-24T10:00:00Z",
                            "2026-04-24T10:05:00Z",
                            "2026-04-24T10:10:00Z",
                        ],
                        utc=True,
                    ),
                    "close": [100.0, 101.0, 102.0],
                }
            )

        if timeframe == MarketTimeframe.m30:
            return pd.DataFrame(
                {
                    "timestamp": pd.to_datetime(
                        [
                            "2026-04-24T10:00:00Z",
                            "2026-04-24T10:30:00Z",
                            "2026-04-24T11:00:00Z",
                        ],
                        utc=True,
                    ),
                    "close": [100.0, 103.0, 104.0],
                }
            )

        if timeframe == MarketTimeframe.h1:
            return pd.DataFrame(
                {
                    "timestamp": pd.to_datetime(
                        [
                            "2026-04-24T10:00:00Z",
                            "2026-04-24T12:00:00Z",
                            "2026-04-24T16:00:00Z",
                        ],
                        utc=True,
                    ),
                    "close": [100.0, 105.0, 106.0],
                }
            )

        raise AssertionError(f"Unexpected timeframe: {timeframe}")


def test_build_feedback_returns_expected_outcomes_for_bullish_event() -> None:
    service = EventFeedbackService(market_data_service=FakeMarketDataService())

    event = StructuredMarketEvent(
        event_id="feedback-test-001",
        asset="Nasdaq 100",
        symbol="NDX",
        timeframe="1h",
        event_type=MarketEventType.headline,
        source_type=IntelligenceSourceType.news,
        source_name="Test Source",
        direction=IntelligenceDirection.bullish,
        urgency=IntelligenceImportance.high,
        confidence_pct=80.0,
        title="Bullish test event",
        summary="Test feedback computation.",
        occurred_at_utc="2026-04-24T10:00:00Z",
        detected_at_utc="2026-04-24T10:00:00Z",
        event_score=75.0,
    )

    feedback = service.build_feedback(event)

    assert feedback.event_id == "feedback-test-001"
    assert feedback.observed_after_5m == 1.0
    assert feedback.observed_after_30m == 3.0
    assert feedback.observed_after_2h == 5.0
    assert feedback.session_close_outcome == 6.0
    assert feedback.event_score == 90.0

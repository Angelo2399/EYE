from __future__ import annotations

from app.services.event_feedback_runner_service import EventFeedbackRunnerService


class FakeRepository:
    def __init__(self) -> None:
        self.saved_feedback = []

    def list_events_without_feedback(self, limit: int = 50):
        return [
            {
                "event_id": "runner-test-001",
                "asset": "Nasdaq 100",
                "symbol": "NDX",
                "timeframe": "1h",
                "event_type": "headline",
                "source_type": "news",
                "source_name": "Test Source",
                "source_url": None,
                "direction": "bullish",
                "urgency": "high",
                "confidence_pct": 80.0,
                "decay_minutes": 120,
                "scenario_change": True,
                "title": "Runner test event",
                "summary": "Should generate feedback.",
                "raw_text": None,
                "occurred_at_utc": "2026-04-24T10:00:00Z",
                "detected_at_utc": "2026-04-24T10:00:00Z",
                "tags": ["test", "runner"],
                "market_context": {"regime": "bullish"},
                "event_score": 77.0,
            }
        ]

    def save_feedback(self, feedback):
        self.saved_feedback.append(feedback)
        return 1


class FakeFeedbackService:
    def build_feedback(self, event):
        from app.schemas.market_intelligence import EventOutcomeFeedback

        return EventOutcomeFeedback(
            event_id=event.event_id,
            asset=event.asset,
            symbol=event.symbol,
            observed_after_5m=1.0,
            observed_after_30m=2.0,
            observed_after_2h=3.0,
            session_close_outcome=4.0,
            event_score=88.0,
            notes="Runner test feedback.",
        )


def test_process_pending_events_saves_feedback_and_returns_counts() -> None:
    repo = FakeRepository()
    service = EventFeedbackRunnerService(
        market_event_repository=repo,
        event_feedback_service=FakeFeedbackService(),
    )

    result = service.process_pending_events(limit=10)

    assert result == {
        "processed": 1,
        "saved_feedback": 1,
        "errors": 0,
    }
    assert len(repo.saved_feedback) == 1
    assert repo.saved_feedback[0].event_id == "runner-test-001"
    assert repo.saved_feedback[0].event_score == 88.0

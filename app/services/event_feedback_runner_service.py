from __future__ import annotations

from app.repositories.market_event_repository import MarketEventRepository
from app.schemas.market_intelligence import StructuredMarketEvent
from app.services.event_feedback_service import EventFeedbackService


class EventFeedbackRunnerService:
    def __init__(
        self,
        market_event_repository: MarketEventRepository | None = None,
        event_feedback_service: EventFeedbackService | None = None,
    ) -> None:
        self.market_event_repository = (
            market_event_repository
            if market_event_repository is not None
            else MarketEventRepository()
        )
        self.event_feedback_service = (
            event_feedback_service
            if event_feedback_service is not None
            else EventFeedbackService()
        )

    def process_pending_events(self, limit: int = 50) -> dict[str, int]:
        pending_rows = self.market_event_repository.list_events_without_feedback(limit=limit)

        processed = 0
        saved_feedback = 0
        errors = 0

        for row in pending_rows:
            processed += 1

            try:
                event = StructuredMarketEvent(
                    event_id=str(row["event_id"]),
                    asset=str(row["asset"]),
                    symbol=str(row["symbol"]),
                    timeframe=row.get("timeframe"),
                    event_type=row["event_type"],
                    source_type=row["source_type"],
                    source_name=str(row.get("source_name") or ""),
                    source_url=row.get("source_url"),
                    direction=row["direction"],
                    urgency=row["urgency"],
                    confidence_pct=float(row.get("confidence_pct") or 50.0),
                    decay_minutes=row.get("decay_minutes"),
                    scenario_change=bool(row.get("scenario_change")),
                    title=str(row.get("title") or ""),
                    summary=str(row.get("summary") or ""),
                    raw_text=row.get("raw_text"),
                    occurred_at_utc=row.get("occurred_at_utc"),
                    detected_at_utc=row.get("detected_at_utc"),
                    tags=list(row.get("tags") or []),
                    market_context=dict(row.get("market_context") or {}),
                    event_score=float(row.get("event_score") or 50.0),
                )

                feedback = self.event_feedback_service.build_feedback(event)
                self.market_event_repository.save_feedback(feedback)
                saved_feedback += 1
            except Exception:
                errors += 1

        return {
            "processed": processed,
            "saved_feedback": saved_feedback,
            "errors": errors,
        }

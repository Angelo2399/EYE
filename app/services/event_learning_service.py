from __future__ import annotations

from app.repositories.market_event_repository import MarketEventRepository
from app.schemas.market_intelligence import StructuredMarketEvent


class EventLearningService:
    def __init__(
        self,
        market_event_repository: MarketEventRepository | None = None,
    ) -> None:
        self.market_event_repository = (
            market_event_repository
            if market_event_repository is not None
            else MarketEventRepository()
        )

    def get_adaptive_weight(
        self,
        *,
        symbol: str,
        event_type: str,
        source_name: str,
        direction: str,
        lookback_days: int = 90,
        min_samples: int = 3,
    ) -> dict[str, object]:
        rows = self.market_event_repository.get_learning_summary(
            symbol=symbol,
            lookback_days=lookback_days,
            min_samples=min_samples,
        )

        normalized_event_type = str(event_type).strip().lower()
        normalized_source_name = str(source_name).strip().lower()
        normalized_direction = str(direction).strip().lower()

        for row in rows:
            if (
                str(row["event_type"]).strip().lower() == normalized_event_type
                and str(row["source_name"]).strip().lower() == normalized_source_name
                and str(row["direction"]).strip().lower() == normalized_direction
            ):
                return self._build_weight_payload(row)

        return {
            "symbol": str(symbol).strip().upper(),
            "event_type": normalized_event_type,
            "source_name": source_name,
            "direction": normalized_direction,
            "samples": 0,
            "avg_event_score": 50.0,
            "avg_after_5m": 0.0,
            "avg_after_30m": 0.0,
            "avg_after_2h": 0.0,
            "avg_session_close": 0.0,
            "weight_multiplier": 1.0,
            "learning_bias": "neutral",
        }

    def apply_adaptive_weight(
        self,
        event: StructuredMarketEvent,
        *,
        lookback_days: int = 90,
        min_samples: int = 3,
    ) -> StructuredMarketEvent:
        weight = self.get_adaptive_weight(
            symbol=event.symbol,
            event_type=event.event_type.value,
            source_name=event.source_name,
            direction=event.direction.value,
            lookback_days=lookback_days,
            min_samples=min_samples,
        )

        multiplier = float(weight["weight_multiplier"])
        adjusted_score = round(
            max(0.0, min(float(event.event_score) * multiplier, 100.0)),
            1,
        )

        updated_context = dict(event.market_context)
        updated_context["learning_weight_multiplier"] = multiplier
        updated_context["learning_bias"] = str(weight["learning_bias"])
        updated_context["learning_samples"] = int(weight["samples"])
        updated_context["learning_avg_after_30m"] = float(weight["avg_after_30m"])
        updated_context["learning_avg_after_2h"] = float(weight["avg_after_2h"])

        return event.model_copy(
            update={
                "event_score": adjusted_score,
                "market_context": updated_context,
            }
        )

    def _build_weight_payload(self, row: dict[str, object]) -> dict[str, object]:
        samples = int(row.get("samples") or 0)
        avg_event_score = float(row.get("avg_event_score") or 50.0)
        avg_after_5m = float(row.get("avg_after_5m") or 0.0)
        avg_after_30m = float(row.get("avg_after_30m") or 0.0)
        avg_after_2h = float(row.get("avg_after_2h") or 0.0)
        avg_session_close = float(row.get("avg_session_close") or 0.0)
        direction = str(row.get("direction") or "").strip().lower()

        directional_edge = (avg_after_30m * 0.4) + (avg_after_2h * 0.6)
        if direction == "bearish":
            directional_edge = -directional_edge

        score_component = (avg_event_score - 50.0) / 200.0
        move_component = directional_edge / 100.0
        sample_component = min(samples, 10) * 0.01

        raw_weight = 1.0 + score_component + move_component + sample_component
        weight_multiplier = round(max(0.75, min(raw_weight, 1.35)), 3)

        if weight_multiplier > 1.03:
            learning_bias = "positive"
        elif weight_multiplier < 0.97:
            learning_bias = "negative"
        else:
            learning_bias = "neutral"

        return {
            "symbol": str(row.get("symbol") or "").strip().upper(),
            "event_type": str(row.get("event_type") or ""),
            "source_name": str(row.get("source_name") or ""),
            "direction": direction,
            "samples": samples,
            "avg_event_score": avg_event_score,
            "avg_after_5m": avg_after_5m,
            "avg_after_30m": avg_after_30m,
            "avg_after_2h": avg_after_2h,
            "avg_session_close": avg_session_close,
            "weight_multiplier": weight_multiplier,
            "learning_bias": learning_bias,
        }

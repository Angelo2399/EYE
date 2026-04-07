from __future__ import annotations

from collections.abc import Callable

from app.services.telegram_topic_message_builder import TelegramTopicMessageBuilder
from app.services.telegram_topic_routing_policy import resolve_event_destinations


EVENT_TYPE_ALIASES: dict[str, str] = {
    "briefing_europe": "briefing_europe",
    "fed_fomc_very_important": "fed_major",
    "cpi_pce_nfp_very_important": "us_macro_major",
    "oil_opec_eia_iea_supply_shock": "oil_market",
    "single_asset_specific_news": "single_asset_news",
    "bias_change_key_level_break_invalidation": "single_asset_news",
    "noise_no_useful_change_recent_duplicate": "low_signal_noise",
}

CONDITION_ALIASES: dict[str, str] = {
    "bias_change": "bias_changed",
    "scenario_change": "scenario_changed",
    "key_level_break": "key_level_broken",
    "invalidazione": "invalidation_hit",
    "news_relevant": "relevant_asset_news",
    "update_periodic": "periodic_update_60m",
}


class TelegramTopicOrchestratorService:
    def __init__(
        self,
        *,
        routing_resolver: Callable[..., object] | None = None,
        message_builder: TelegramTopicMessageBuilder | None = None,
    ) -> None:
        self.routing_resolver = routing_resolver or self._default_routing_resolver
        self.message_builder = message_builder or TelegramTopicMessageBuilder()

    def build_topic_publications(
        self,
        *,
        event_type: str,
        impacted_topics: list[str] | None = None,
        conditions: list[str] | None = None,
        context: dict[str, object] | None = None,
    ) -> dict[str, object]:
        resolved_impacted_topics = list(impacted_topics or [])
        resolved_conditions = list(conditions or [])
        resolved_context = dict(context or {})

        routing_result = self.routing_resolver(
            event_type=event_type,
            impacted_topics=resolved_impacted_topics,
            conditions=resolved_conditions,
        )

        messages = self.message_builder.build_messages(
            event_type=event_type,
            routing_result=routing_result,
            context=resolved_context,
        )

        return {
            "event_type": event_type,
            "impacted_topics": resolved_impacted_topics,
            "conditions": resolved_conditions,
            "routing_result": routing_result,
            "messages": messages,
        }

    def _default_routing_resolver(
        self,
        *,
        event_type: str,
        impacted_topics: list[str],
        conditions: list[str],
    ) -> dict[str, list[str]]:
        resolved_event_type = EVENT_TYPE_ALIASES.get(event_type, event_type)
        resolved_conditions = [
            CONDITION_ALIASES.get(condition, condition)
            for condition in conditions
        ]

        routing_result = resolve_event_destinations(
            event=resolved_event_type,
            impacted_destinations=impacted_topics,
            topic_conditions={
                topic: resolved_conditions
                for topic in impacted_topics
            },
        )

        return {
            "publish_to": [
                destination.value
                for destination in routing_result.publish_to
            ],
            "do_not_publish_to": [
                destination.value
                for destination in routing_result.do_not_publish_to
            ],
        }


__all__ = [
    "CONDITION_ALIASES",
    "EVENT_TYPE_ALIASES",
    "TelegramTopicOrchestratorService",
]

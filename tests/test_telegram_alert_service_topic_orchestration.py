from __future__ import annotations

from types import SimpleNamespace

from app.services.telegram_alert_service import TelegramAlertService


class FakeTopicOrchestratorService:
    def __init__(self) -> None:
        self.last_kwargs: dict[str, object] | None = None

    def build_topic_publications(self, **kwargs):
        self.last_kwargs = kwargs
        return {
            "event_type": kwargs["event_type"],
            "impacted_topics": kwargs["impacted_topics"],
            "conditions": kwargs["conditions"],
            "routing_result": {
                "publish_to": ["main", "ndx"],
                "do_not_publish_to": ["spx", "wti", "btc"],
            },
            "messages": {
                "main": "message for main",
                "ndx": "message for ndx",
            },
        }


def test_build_topic_alert_publications_delegates_to_orchestrator() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )
    service.topic_orchestrator_service = FakeTopicOrchestratorService()

    result = service.build_topic_alert_publications(
        event_type="fed_fomc_very_important",
        impacted_topics=["ndx"],
        conditions=["bias_change"],
        context={
            "local_time": "14:30",
            "scenario": "hawkish repricing",
            "bias": "short",
            "plausible_action": "short",
        },
    )

    assert result["event_type"] == "fed_fomc_very_important"
    assert result["routing_result"]["publish_to"] == ["main", "ndx"]
    assert result["messages"]["main"] == "message for main"
    assert result["messages"]["ndx"] == "message for ndx"

    assert service.topic_orchestrator_service.last_kwargs is not None
    assert service.topic_orchestrator_service.last_kwargs["event_type"] == (
        "fed_fomc_very_important"
    )
    assert service.topic_orchestrator_service.last_kwargs["impacted_topics"] == ["ndx"]
    assert service.topic_orchestrator_service.last_kwargs["conditions"] == [
        "bias_change"
    ]
    assert service.topic_orchestrator_service.last_kwargs["context"]["bias"] == "short"


def test_build_topic_alert_publications_uses_lazy_fallback_when_orchestrator_is_missing() -> None:
    service = object.__new__(TelegramAlertService)

    result = service.build_topic_alert_publications(
        event_type="briefing_europe",
        impacted_topics=[],
        conditions=[],
        context={
            "local_time": "08:30",
            "scenario": "opening preparation",
            "bias": "wait",
            "plausible_action": "wait",
        },
    )

    assert result["event_type"] == "briefing_europe"
    assert "routing_result" in result
    assert "messages" in result
    assert hasattr(service, "topic_orchestrator_service")

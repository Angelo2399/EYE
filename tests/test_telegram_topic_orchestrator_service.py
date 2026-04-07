from __future__ import annotations

from app.services.telegram_topic_orchestrator_service import (
    TelegramTopicOrchestratorService,
)


class FakeMessageBuilder:
    def __init__(self) -> None:
        self.last_kwargs: dict[str, object] | None = None

    def build_messages(self, **kwargs):
        self.last_kwargs = kwargs
        routing_result = kwargs["routing_result"]

        if isinstance(routing_result, dict):
            publish_to = routing_result.get("publish_to", [])
        else:
            publish_to = getattr(routing_result, "publish_to", [])

        return {destination: f"message for {destination}" for destination in publish_to}


def test_build_topic_publications_returns_routing_and_messages() -> None:
    builder = FakeMessageBuilder()

    def fake_resolver(*, event_type, impacted_topics, conditions):
        assert event_type == "briefing_europe"
        assert impacted_topics == []
        assert conditions == []
        return {
            "publish_to": ["main"],
            "do_not_publish_to": ["ndx", "spx", "wti", "btc"],
        }

    service = TelegramTopicOrchestratorService(
        routing_resolver=fake_resolver,
        message_builder=builder,
    )

    result = service.build_topic_publications(
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
    assert result["impacted_topics"] == []
    assert result["conditions"] == []
    assert result["routing_result"]["publish_to"] == ["main"]
    assert result["messages"] == {"main": "message for main"}

    assert builder.last_kwargs is not None
    assert builder.last_kwargs["event_type"] == "briefing_europe"
    assert builder.last_kwargs["context"]["local_time"] == "08:30"


def test_build_topic_publications_supports_topic_event() -> None:
    builder = FakeMessageBuilder()

    def fake_resolver(*, event_type, impacted_topics, conditions):
        assert event_type == "single_asset_specific_news"
        assert impacted_topics == ["btc"]
        assert conditions == ["scenario_change"]
        return {
            "publish_to": ["btc"],
            "do_not_publish_to": ["main", "ndx", "spx", "wti"],
        }

    service = TelegramTopicOrchestratorService(
        routing_resolver=fake_resolver,
        message_builder=builder,
    )

    result = service.build_topic_publications(
        event_type="single_asset_specific_news",
        impacted_topics=["btc"],
        conditions=["scenario_change"],
        context={
            "local_time": "14:05",
            "scenario": "breakout",
            "bias": "long",
            "plausible_action": "long",
            "destinations": {
                "btc": {
                    "asset": "Bitcoin",
                }
            },
        },
    )

    assert result["routing_result"]["publish_to"] == ["btc"]
    assert result["messages"] == {"btc": "message for btc"}


def test_build_topic_publications_returns_empty_messages_when_nothing_is_publishable() -> None:
    builder = FakeMessageBuilder()

    def fake_resolver(*, event_type, impacted_topics, conditions):
        return {
            "publish_to": [],
            "do_not_publish_to": ["main", "ndx", "spx", "wti", "btc"],
        }

    service = TelegramTopicOrchestratorService(
        routing_resolver=fake_resolver,
        message_builder=builder,
    )

    result = service.build_topic_publications(
        event_type="noise_no_useful_change_recent_duplicate",
        impacted_topics=["ndx"],
        conditions=["news_relevant"],
        context={
            "local_time": "11:00",
            "scenario": "no change",
            "bias": "wait",
            "plausible_action": "wait",
        },
    )

    assert result["messages"] == {}
    assert result["routing_result"]["publish_to"] == []

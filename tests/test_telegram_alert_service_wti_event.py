from __future__ import annotations

from types import MethodType, SimpleNamespace

from app.services.telegram_alert_service import TelegramAlertService


def _build_service() -> TelegramAlertService:
    return TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="-1001234567890",
            telegram_alerts_enabled=True,
        )
    )


def test_publish_wti_topic_alert_delegates_to_publish_topic_alerts() -> None:
    service = _build_service()

    captured: dict[str, object] = {}

    def fake_publish_topic_alerts(
        self,
        *,
        event_type,
        impacted_topics=None,
        conditions=None,
        context=None,
        destination_threads=None,
        allowed_destinations=None,
        chat_id=None,
    ):
        captured["event_type"] = event_type
        captured["impacted_topics"] = impacted_topics
        captured["conditions"] = conditions
        captured["context"] = context
        captured["destination_threads"] = destination_threads
        captured["allowed_destinations"] = allowed_destinations
        captured["chat_id"] = chat_id

        return {
            "event_type": event_type,
            "publications": {"messages": {"wti": "msg"}},
            "send_results": {"wti": {"ok": True}},
        }

    service.publish_topic_alerts = MethodType(fake_publish_topic_alerts, service)

    result = service.publish_wti_topic_alert(
        conditions=["news_relevant"],
        context={
            "local_time": "16:30",
            "scenario": "inventory-driven upside",
            "bias": "long",
            "plausible_action": "long",
        },
        destination_threads={"wti": 202},
        allowed_destinations=["wti"],
    )

    assert captured["event_type"] == "single_asset_specific_news"
    assert captured["impacted_topics"] == ["wti"]
    assert captured["conditions"] == ["news_relevant"]
    assert captured["destination_threads"] == {"wti": 202}
    assert captured["allowed_destinations"] == ["wti"]
    assert result["event_type"] == "single_asset_specific_news"
    assert result["send_results"]["wti"]["ok"] is True

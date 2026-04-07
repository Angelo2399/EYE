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


def test_publish_fed_fomc_alert_delegates_to_publish_topic_alerts() -> None:
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
            "publications": {"messages": {"main": "msg"}},
            "send_results": {"main": {"ok": True}},
        }

    service.publish_topic_alerts = MethodType(fake_publish_topic_alerts, service)

    result = service.publish_fed_fomc_alert(
        impacted_topics=["ndx"],
        conditions=["bias_change"],
        context={
            "local_time": "20:00",
            "scenario": "hawkish repricing",
            "bias": "short",
            "plausible_action": "short",
        },
        destination_threads={"ndx": 101},
        allowed_destinations=["main", "ndx"],
    )

    assert captured["event_type"] == "fed_fomc_very_important"
    assert captured["impacted_topics"] == ["ndx"]
    assert captured["conditions"] == ["bias_change"]
    assert captured["destination_threads"] == {"ndx": 101}
    assert captured["allowed_destinations"] == ["main", "ndx"]
    assert result["event_type"] == "fed_fomc_very_important"
    assert result["send_results"]["main"]["ok"] is True

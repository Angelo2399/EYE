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


def test_publish_topic_alerts_builds_and_sends_only_allowed_destinations() -> None:
    service = _build_service()

    def fake_build_topic_alert_publications(
        self,
        *,
        event_type,
        impacted_topics=None,
        conditions=None,
        context=None,
    ):
        assert event_type == "fed_fomc_very_important"
        assert impacted_topics == ["ndx"]
        assert conditions == ["bias_change"]
        return {
            "event_type": event_type,
            "routing_result": {
                "publish_to": ["main", "ndx"],
                "do_not_publish_to": ["spx", "wti", "btc"],
            },
            "messages": {
                "main": "main message",
                "ndx": "ndx message",
            },
        }

    def fake_send_topic_messages(
        self,
        *,
        publications,
        destination_threads=None,
        allowed_destinations=None,
        chat_id=None,
    ):
        assert publications["messages"]["main"] == "main message"
        assert publications["messages"]["ndx"] == "ndx message"
        assert destination_threads == {"ndx": 101}
        assert allowed_destinations == ["ndx"]
        assert chat_id is None
        return {
            "ndx": {
                "ok": True,
                "result": {"message_id": 1},
            }
        }

    service.build_topic_alert_publications = MethodType(
        fake_build_topic_alert_publications,
        service,
    )
    service.send_topic_messages = MethodType(
        fake_send_topic_messages,
        service,
    )

    result = service.publish_topic_alerts(
        event_type="fed_fomc_very_important",
        impacted_topics=["ndx"],
        conditions=["bias_change"],
        context={
            "local_time": "14:30",
            "scenario": "hawkish repricing",
            "bias": "short",
            "plausible_action": "short",
        },
        destination_threads={"ndx": 101},
        allowed_destinations=["ndx"],
    )

    assert result["event_type"] == "fed_fomc_very_important"
    assert result["publications"]["messages"]["main"] == "main message"
    assert result["publications"]["messages"]["ndx"] == "ndx message"
    assert result["send_results"]["ndx"]["ok"] is True


def test_publish_topic_alerts_returns_empty_send_results_when_nothing_is_sent() -> None:
    service = _build_service()

    def fake_build_topic_alert_publications(
        self,
        *,
        event_type,
        impacted_topics=None,
        conditions=None,
        context=None,
    ):
        return {
            "event_type": event_type,
            "routing_result": {
                "publish_to": [],
                "do_not_publish_to": ["main", "ndx", "spx", "wti", "btc"],
            },
            "messages": {},
        }

    def fake_send_topic_messages(
        self,
        *,
        publications,
        destination_threads=None,
        allowed_destinations=None,
        chat_id=None,
    ):
        assert publications["messages"] == {}
        return {}

    service.build_topic_alert_publications = MethodType(
        fake_build_topic_alert_publications,
        service,
    )
    service.send_topic_messages = MethodType(
        fake_send_topic_messages,
        service,
    )

    result = service.publish_topic_alerts(
        event_type="noise",
        impacted_topics=[],
        conditions=[],
        context={"local_time": "10:00"},
    )

    assert result["event_type"] == "noise"
    assert result["publications"]["messages"] == {}
    assert result["send_results"] == {}


def test_publish_topic_alerts_builds_operational_topic_message_end_to_end() -> None:
    service = _build_service()
    sent_calls: list[dict[str, object]] = []

    def fake_send_message(self, *, text, chat_id=None, message_thread_id=None):
        sent_calls.append(
            {
                "text": text,
                "chat_id": chat_id,
                "message_thread_id": message_thread_id,
            }
        )
        return {"ok": True, "result": {"message_id": len(sent_calls)}}

    service.send_message = MethodType(fake_send_message, service)

    result = service.publish_topic_alerts(
        event_type="single_asset_specific_news",
        impacted_topics=["ndx"],
        conditions=["scenario_change"],
        context={
            "local_time": "14:35:00",
            "scenario": "trend_up",
            "bias": "long",
            "plausible_action": "long",
            "destinations": {
                "ndx": {
                    "asset": "Nasdaq 100",
                    "timeframe": "1h",
                    "model_confidence_pct": 61.0,
                    "confidence_label": "medium",
                    "entry_min": 100.0,
                    "entry_max": 101.0,
                    "stop_loss": 99.0,
                    "take_profit_1": 102.0,
                    "take_profit_2": 103.0,
                    "risk_reward": 1.5,
                }
            },
        },
        destination_threads={"ndx": 101},
        allowed_destinations=["ndx"],
    )

    assert result["send_results"]["ndx"]["ok"] is True
    assert len(sent_calls) == 1
    assert sent_calls[0]["message_thread_id"] == 101
    assert "Timeframe: 1h" in sent_calls[0]["text"]
    assert "Confidence: medium (61.0%)" in sent_calls[0]["text"]
    assert "Entry: 100.00 - 101.00" in sent_calls[0]["text"]
    assert "Stop: 99.00" in sent_calls[0]["text"]
    assert "TP1: 102.00" in sent_calls[0]["text"]
    assert "TP2: 103.00" in sent_calls[0]["text"]
    assert "R/R: 1.50" in sent_calls[0]["text"]

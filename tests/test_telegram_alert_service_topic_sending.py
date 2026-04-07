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


def test_send_topic_messages_sends_only_allowed_destinations_with_thread_ids() -> None:
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

    result = service.send_topic_messages(
        publications={
            "messages": {
                "main": "main message",
                "ndx": "ndx message",
                "btc": "btc message",
            }
        },
        destination_threads={
            "ndx": 101,
            "btc": 202,
        },
        allowed_destinations=["ndx"],
    )

    assert list(result.keys()) == ["ndx"]
    assert sent_calls == [
        {
            "text": "ndx message",
            "chat_id": None,
            "message_thread_id": 101,
        }
    ]


def test_send_topic_messages_sends_all_messages_when_no_filter_is_provided() -> None:
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

    result = service.send_topic_messages(
        publications={
            "messages": {
                "main": "main message",
                "wti": "wti message",
            }
        },
        destination_threads={
            "wti": 303,
        },
    )

    assert list(result.keys()) == ["main", "wti"]
    assert sent_calls == [
        {
            "text": "main message",
            "chat_id": None,
            "message_thread_id": None,
        },
        {
            "text": "wti message",
            "chat_id": None,
            "message_thread_id": 303,
        },
    ]

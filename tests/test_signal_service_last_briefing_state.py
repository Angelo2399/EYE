from __future__ import annotations

from app.services.signal_service import SignalService


def _build_service() -> SignalService:
    return object.__new__(SignalService)


def test_get_last_briefing_payload_returns_none_when_missing() -> None:
    service = _build_service()

    assert service.get_last_briefing_payload() is None


def test_get_last_briefing_payload_returns_copy_of_internal_payload() -> None:
    service = _build_service()
    service._last_briefing_payload = {
        "title": "Nasdaq 100 intraday briefing",
        "symbol": "NDX",
        "timezone_name": "Europe/London",
    }

    payload = service.get_last_briefing_payload()

    assert payload == {
        "title": "Nasdaq 100 intraday briefing",
        "symbol": "NDX",
        "timezone_name": "Europe/London",
    }
    assert payload is not service._last_briefing_payload


def test_send_last_briefing_alert_uses_stored_payload_and_telegram_service() -> None:
    service = _build_service()
    service._last_briefing_payload = {
        "title": "Nasdaq 100 intraday briefing",
        "summary": "Recorded briefing payload.",
        "timezone_name": "Europe/London",
        "local_time": "08:15:00",
    }
    captured: dict[str, object] = {}

    class FakeTelegramAlertService:
        def send_briefing_payload(
            self,
            *,
            briefing_payload: dict[str, object],
            chat_id: str | None = None,
        ) -> dict:
            captured["briefing_payload"] = briefing_payload
            captured["chat_id"] = chat_id
            return {"ok": True}

    service.telegram_alert_service = FakeTelegramAlertService()

    result = service.send_last_briefing_alert(chat_id="999")

    assert result == {"ok": True}
    assert captured == {
        "briefing_payload": {
            "title": "Nasdaq 100 intraday briefing",
            "summary": "Recorded briefing payload.",
            "timezone_name": "Europe/London",
            "local_time": "08:15:00",
        },
        "chat_id": "999",
    }

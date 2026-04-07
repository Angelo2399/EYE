from __future__ import annotations

import json
from datetime import datetime, timezone
from types import MethodType
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import app.services.telegram_alert_service as telegram_alert_service_module
from app.services.telegram_alert_service import RISK_DISCLAIMER, TelegramAlertService


DISCLAIMER_TEXT = RISK_DISCLAIMER


def test_should_send_signal_alert_returns_false_when_global_flag_is_disabled() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=False,
        )
    )

    assert service.should_send_signal_alert(action="long") is False
    assert service.should_send_signal_alert(action="short") is False
    assert service.should_send_signal_alert(action="wait") is False
    assert service.should_send_signal_alert(action="no_trade") is False


def test_should_send_signal_alert_returns_true_only_for_long_and_short_when_enabled() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )

    assert service.should_send_signal_alert(action="long") is True
    assert service.should_send_signal_alert(action="short") is True
    assert service.should_send_signal_alert(action="wait") is False
    assert service.should_send_signal_alert(action="no_trade") is False


def test_send_briefing_payload_delegates_to_send_briefing_alert() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )
    captured: dict[str, object] = {}

    def fake_send_briefing_alert(
        *,
        title: str,
        summary: str,
        timezone_name: str | None = None,
        local_time: str | None = None,
        chat_id: str | None = None,
    ) -> dict:
        captured["title"] = title
        captured["summary"] = summary
        captured["timezone_name"] = timezone_name
        captured["local_time"] = local_time
        captured["chat_id"] = chat_id
        return {"ok": True}

    service.send_briefing_alert = fake_send_briefing_alert

    result = service.send_briefing_payload(
        briefing_payload={
            "title": "Nasdaq 100 intraday briefing",
            "summary": "Recorded briefing payload.",
            "timezone_name": "Europe/London",
            "local_time": "08:15:00",
        },
        chat_id="999",
    )

    assert result == {"ok": True}
    assert captured == {
        "title": "Nasdaq 100 intraday briefing",
        "summary": "Recorded briefing payload.",
        "timezone_name": "Europe/London",
        "local_time": "08:15:00",
        "chat_id": "999",
    }


def test_send_briefing_payload_appends_market_closures_section() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )
    captured: dict[str, object] = {}

    def fake_send_briefing_alert(
        *,
        title: str,
        summary: str,
        timezone_name: str | None = None,
        local_time: str | None = None,
        chat_id: str | None = None,
    ) -> dict:
        captured["summary"] = summary
        return {"ok": True}

    service.send_briefing_alert = fake_send_briefing_alert

    result = service.send_briefing_payload(
        briefing_payload={
            "title": "Nasdaq 100 intraday briefing",
            "summary": "Recorded briefing payload.",
            "timezone_name": "Europe/London",
            "local_time": "08:15:00",
            "market_closures": [
                {
                    "asset": "NDX",
                    "market_status": "closed_holiday",
                    "official_reason": "Good Friday",
                    "next_open_local": "2026-04-06T15:30:00+02:00",
                },
                {
                    "asset": "WTI",
                    "market_status": "reduced_hours",
                    "official_reason": "NYMEX WTI daily maintenance break 17:00-18:00 ET.",
                    "next_open_local": "2026-04-01T23:00:00+02:00",
                },
            ],
        }
    )

    assert result == {"ok": True}
    assert captured["summary"] == (
        "Recorded briefing payload.\n\n"
        "Closed / limited markets today:\n"
        "- NDX | closed_holiday | Good Friday | next open 2026-04-06T15:30:00+02:00\n"
        "- WTI | reduced_hours | NYMEX WTI daily maintenance break 17:00-18:00 ET. | next open 2026-04-01T23:00:00+02:00"
    )


def test_send_briefing_payload_skips_market_closures_section_when_empty() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )
    captured: dict[str, object] = {}

    def fake_send_briefing_alert(
        *,
        title: str,
        summary: str,
        timezone_name: str | None = None,
        local_time: str | None = None,
        chat_id: str | None = None,
    ) -> dict:
        captured["summary"] = summary
        return {"ok": True}

    service.send_briefing_alert = fake_send_briefing_alert

    result = service.send_briefing_payload(
        briefing_payload={
            "title": "Nasdaq 100 intraday briefing",
            "summary": "Recorded briefing payload.",
            "timezone_name": "Europe/London",
            "local_time": "08:15:00",
            "market_closures": [],
        }
    )

    assert result == {"ok": True}
    assert captured["summary"] == "Recorded briefing payload."


def test_send_message_prepends_risk_disclaimer_to_payload() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )
    captured: dict[str, object] = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self) -> bytes:
            return json.dumps({"ok": True, "result": {"message_id": 1}}).encode(
                "utf-8"
            )

    def fake_urlopen(request, timeout=15):
        captured["timeout"] = timeout
        captured["payload"] = json.loads(request.data.decode("utf-8"))
        return FakeResponse()

    original_urlopen = telegram_alert_service_module.urlopen
    telegram_alert_service_module.urlopen = fake_urlopen

    try:
        result = service.send_message(text="EYE BRIEFING\nTest body")
    finally:
        telegram_alert_service_module.urlopen = original_urlopen

    assert result["ok"] is True
    assert captured["timeout"] == 15
    assert captured["payload"]["text"] == (
        f"{DISCLAIMER_TEXT}\n\nEYE BRIEFING\nTest body"
    )


def test_send_message_does_not_duplicate_existing_risk_disclaimer() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )
    captured: dict[str, object] = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self) -> bytes:
            return json.dumps({"ok": True, "result": {"message_id": 2}}).encode(
                "utf-8"
            )

    def fake_urlopen(request, timeout=15):
        captured["payload"] = json.loads(request.data.decode("utf-8"))
        return FakeResponse()

    original_urlopen = telegram_alert_service_module.urlopen
    telegram_alert_service_module.urlopen = fake_urlopen

    try:
        service.send_message(text=f"EYE ALERT\n{DISCLAIMER_TEXT}\nBody")
    finally:
        telegram_alert_service_module.urlopen = original_urlopen

    resolved_text = str(captured["payload"]["text"])
    assert resolved_text.startswith(DISCLAIMER_TEXT)
    assert resolved_text.count(DISCLAIMER_TEXT) == 1
    assert resolved_text.endswith("EYE ALERT\n\nBody")


def test_remember_asset_state_saves_last_state_for_asset() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )

    saved = service.remember_asset_state(
        asset="ndx",
        action="long",
        scenario="breakout confirmed",
        confidence_label="high",
        entry_min=100.0,
        entry_max=101.0,
        stop_loss=99.0,
        take_profit_1=102.0,
        take_profit_2=103.0,
        risk_reward=1.5,
        sent_at="2026-04-04T18:00:00+00:00",
    )

    assert saved["asset"] == "NDX"
    assert saved["last_action"] == "long"
    assert saved["last_scenario"] == "breakout confirmed"
    assert saved["last_confidence_label"] == "high"
    assert saved["last_entry_min"] == 100.0
    assert saved["last_entry_max"] == 101.0
    assert saved["last_stop_loss"] == 99.0
    assert saved["last_take_profit_1"] == 102.0
    assert saved["last_take_profit_2"] == 103.0
    assert saved["last_risk_reward"] == 1.5
    assert saved["last_sent_at"] == "2026-04-04T18:00:00+00:00"

    restored = service.get_last_asset_state(asset="NDX")
    assert restored == saved


def test_remember_asset_state_overwrites_previous_state_for_same_asset() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )

    service.remember_asset_state(
        asset="WTI",
        action="wait",
        scenario="uncertain oil context",
        confidence_label="medium",
        sent_at="2026-04-04T18:00:00+00:00",
    )

    service.remember_asset_state(
        asset="wti",
        action="short",
        scenario="inventory build confirmed",
        confidence_label="high",
        entry_min=70.0,
        entry_max=70.4,
        stop_loss=71.1,
        take_profit_1=69.2,
        take_profit_2=68.6,
        risk_reward=1.8,
        sent_at="2026-04-04T19:00:00+00:00",
    )

    restored = service.get_last_asset_state(asset="WTI")

    assert restored is not None
    assert restored["asset"] == "WTI"
    assert restored["last_action"] == "short"
    assert restored["last_scenario"] == "inventory build confirmed"
    assert restored["last_confidence_label"] == "high"
    assert restored["last_entry_min"] == 70.0
    assert restored["last_entry_max"] == 70.4
    assert restored["last_stop_loss"] == 71.1
    assert restored["last_take_profit_1"] == 69.2
    assert restored["last_take_profit_2"] == 68.6
    assert restored["last_risk_reward"] == 1.8
    assert restored["last_sent_at"] == "2026-04-04T19:00:00+00:00"


def test_evaluate_asset_state_change_without_previous_state_returns_no_alert() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )

    result = service.evaluate_asset_state_change(
        asset="ndx",
        action="long",
        scenario="breakout confirmed",
        confidence_label="high",
    )

    assert result == {
        "has_previous_state": False,
        "change_detected": False,
        "should_alert": False,
        "change_reasons": [],
    }


def test_evaluate_asset_state_change_sets_should_alert_when_action_changes() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )
    service.remember_asset_state(
        asset="ndx",
        action="wait",
        scenario="breakout confirmed",
        confidence_label="high",
        entry_min=100.0,
        entry_max=101.0,
        stop_loss=99.0,
        take_profit_1=102.0,
        take_profit_2=103.0,
        risk_reward=1.5,
    )

    result = service.evaluate_asset_state_change(
        asset="NDX",
        action="long",
        scenario="breakout confirmed",
        confidence_label="high",
        entry_min=100.0,
        entry_max=101.0,
        stop_loss=99.0,
        take_profit_1=102.0,
        take_profit_2=103.0,
        risk_reward=1.5,
    )

    assert result["has_previous_state"] is True
    assert result["change_detected"] is True
    assert result["should_alert"] is True
    assert result["change_reasons"] == ["action_changed"]


def test_evaluate_asset_state_change_detects_levels_change_without_alert() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )
    service.remember_asset_state(
        asset="WTI",
        action="short",
        scenario="inventory build confirmed",
        confidence_label="high",
        entry_min=70.0,
        entry_max=70.4,
        stop_loss=71.1,
        take_profit_1=69.2,
        take_profit_2=68.6,
        risk_reward=1.8,
    )

    result = service.evaluate_asset_state_change(
        asset="wti",
        action="short",
        scenario="inventory build confirmed",
        confidence_label="high",
        entry_min=70.0,
        entry_max=70.5,
        stop_loss=71.1,
        take_profit_1=69.2,
        take_profit_2=68.6,
        risk_reward=1.8,
    )

    assert result["has_previous_state"] is True
    assert result["change_detected"] is True
    assert result["should_alert"] is False
    assert result["change_reasons"] == ["levels_changed"]


def test_build_asset_update_message_returns_alert_when_action_changes() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )

    message = service.build_asset_update_message(
        asset="ndx",
        asset_full_name="Nasdaq 100",
        action="long",
        scenario="breakout confirmed",
        confidence_label="high",
        market_explanation="External intelligence supports a bullish breakout.",
        entry_min=100.0,
        entry_max=101.0,
        stop_loss=99.0,
        take_profit_1=102.0,
        take_profit_2=103.0,
        risk_reward=1.5,
        previous_state={
            "last_action": "wait",
        },
        state_change={
            "has_previous_state": True,
            "change_detected": True,
            "should_alert": True,
            "change_reasons": ["action_changed"],
        },
    )

    assert message.startswith("\U0001F4CA Nasdaq 100 (NDX)")
    assert "\u2022 Signal: BUY" in message
    assert "\u2022 Scenario: breakout confirmed" in message
    assert "\u2022 Confidence: high" in message
    assert "\u2022 Reason: External intelligence supports a bullish breakout." in message
    assert "\u2022 Entry: 100.00 - 101.00" in message
    assert "\u2022 Stop loss: 99.00" in message
    assert "\u2022 Take profit: 102.00 / 103.00" in message
    assert "Why now:" not in message
    assert "Expected reward" not in message
    assert "Prima:" not in message


def test_build_asset_update_message_returns_update_when_only_levels_change() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )

    message = service.build_asset_update_message(
        asset="wti",
        asset_full_name="WTI Crude Oil",
        action="short",
        scenario="inventory build confirmed",
        confidence_label="high",
        entry_min=70.0,
        entry_max=70.5,
        stop_loss=71.1,
        take_profit_1=69.2,
        take_profit_2=68.6,
        risk_reward=1.8,
        previous_state={
            "last_action": "short",
        },
        state_change={
            "has_previous_state": True,
            "change_detected": True,
            "should_alert": False,
            "change_reasons": ["levels_changed"],
        },
    )

    assert message.startswith("\U0001F4CA WTI Crude Oil (WTI)")
    assert "\u2022 Signal: SELL" in message
    assert (
        "\u2022 Reason: No strong confirmation from current market context."
        in message
    )
    assert "\u2022 Entry: 70.00 - 70.50" in message
    assert "\u2022 Stop loss: 71.10" in message
    assert "\u2022 Take profit: 69.20 / 68.60" in message
    assert "Livelli aggiornati" not in message
    assert "Motivo:" not in message


def test_build_asset_update_message_returns_follow_up_when_no_strong_change() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )

    message = service.build_asset_update_message(
        asset="spx",
        asset_full_name="S&P 500",
        action="long",
        scenario="broad market breakout",
        confidence_label="medium",
        entry_min=5000.0,
        entry_max=5010.0,
        stop_loss=4985.0,
        take_profit_1=5030.0,
        take_profit_2=5045.0,
        risk_reward=1.7,
        previous_state={
            "last_action": "long",
        },
        state_change={
            "has_previous_state": True,
            "change_detected": False,
            "should_alert": False,
            "change_reasons": [],
        },
    )

    assert message.startswith("\U0001F4CA S&P 500 (SPX)")
    assert "\u2022 Signal: BUY" in message
    assert "\u2022 Scenario: broad market breakout" in message
    assert "\u2022 Confidence: medium" in message
    assert (
        "\u2022 Reason: No strong confirmation from current market context."
        in message
    )
    assert "Tesi invariata rispetto all'ultimo invio" not in message


def test_build_asset_update_message_returns_no_trade_message_with_na_levels() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )

    message = service.build_asset_update_message(
        asset="wti",
        action="no_trade",
        scenario="range_day",
        confidence_label="low",
        entry_min=70.0,
        entry_max=70.5,
        stop_loss=71.1,
        take_profit_1=69.2,
        take_profit_2=68.6,
    )

    assert message.startswith("\U0001F4CA WTI Crude Oil (WTI)")
    assert "\u2022 Signal: NO TRADE" in message
    assert "\u2022 Scenario: range day" in message
    assert "\u2022 Confidence: low" in message
    assert (
        "\u2022 Reason: No strong confirmation from current market context."
        in message
    )
    assert "\u2022 Entry: n/a" in message
    assert "\u2022 Stop loss: n/a" in message
    assert "\u2022 Take profit: n/a" in message


def test_build_asset_update_message_rewrites_disabled_external_intelligence_reason() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )

    message = service.build_asset_update_message(
        asset="ndx",
        action="no_trade",
        scenario="trend_up",
        confidence_label="low",
        reason="External intelligence disabled.",
    )

    assert (
        "\u2022 Reason: No confirmed macro/news edge right now."
        in message
    )


def test_publish_asset_update_first_send_without_previous_state() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )

    def fake_send_message(self, *, text, chat_id=None, message_thread_id=None):
        return {"ok": True, "date": "2026-04-04T20:00:00+00:00", "text": text}

    service.send_message = MethodType(fake_send_message, service)

    result = service.publish_asset_update(
        asset="ndx",
        asset_full_name="Nasdaq 100",
        action="long",
        scenario="breakout confirmed",
        confidence_label="high",
        market_explanation="US tech leadership remains supportive for buyers.",
        entry_min=100.0,
        entry_max=101.0,
        stop_loss=99.0,
        take_profit_1=102.0,
        take_profit_2=103.0,
        risk_reward=1.5,
    )

    assert result["previous_state"] is None
    assert result["state_change"]["has_previous_state"] is False
    assert result["state_change"]["should_alert"] is False
    assert result["message"].startswith("\U0001F4CA Nasdaq 100 (NDX)")
    assert "\u2022 Signal: BUY" in result["message"]
    assert (
        "\u2022 Reason: US tech leadership remains supportive for buyers."
        in result["message"]
    )
    assert "Why now:" not in result["message"]
    assert result["send_result"]["ok"] is True


def test_publish_asset_update_sends_alert_when_action_changes() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )
    service.remember_asset_state(
        asset="ndx",
        action="wait",
        scenario="breakout confirmed",
        confidence_label="high",
        entry_min=100.0,
        entry_max=101.0,
        stop_loss=99.0,
        take_profit_1=102.0,
        take_profit_2=103.0,
        risk_reward=1.5,
    )

    def fake_send_message(self, *, text, chat_id=None, message_thread_id=None):
        return {"ok": True, "date": "2026-04-04T20:01:00+00:00", "text": text}

    service.send_message = MethodType(fake_send_message, service)

    result = service.publish_asset_update(
        asset="NDX",
        asset_full_name="Nasdaq 100",
        action="long",
        scenario="breakout confirmed",
        confidence_label="high",
        entry_min=100.0,
        entry_max=101.0,
        stop_loss=99.0,
        take_profit_1=102.0,
        take_profit_2=103.0,
        risk_reward=1.5,
    )

    assert result["state_change"]["should_alert"] is True
    assert result["message"].startswith("\U0001F4CA Nasdaq 100 (NDX)")
    assert "\u2022 Signal: BUY" in result["message"]
    assert (
        "\u2022 Reason: No strong confirmation from current market context."
        in result["message"]
    )


def test_publish_asset_update_sends_update_when_only_levels_change() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )
    service.remember_asset_state(
        asset="WTI",
        action="short",
        scenario="inventory build confirmed",
        confidence_label="high",
        entry_min=70.0,
        entry_max=70.4,
        stop_loss=71.1,
        take_profit_1=69.2,
        take_profit_2=68.6,
        risk_reward=1.8,
    )

    def fake_send_message(self, *, text, chat_id=None, message_thread_id=None):
        return {"ok": True, "date": "2026-04-04T20:02:00+00:00", "text": text}

    service.send_message = MethodType(fake_send_message, service)

    result = service.publish_asset_update(
        asset="wti",
        asset_full_name="WTI Crude Oil",
        action="short",
        scenario="inventory build confirmed",
        confidence_label="high",
        entry_min=70.0,
        entry_max=70.5,
        stop_loss=71.1,
        take_profit_1=69.2,
        take_profit_2=68.6,
        risk_reward=1.8,
    )

    assert result["state_change"]["change_detected"] is True
    assert result["state_change"]["should_alert"] is False
    assert result["message"].startswith("\U0001F4CA WTI Crude Oil (WTI)")
    assert "\u2022 Signal: SELL" in result["message"]
    assert (
        "\u2022 Reason: No strong confirmation from current market context."
        in result["message"]
    )


def test_publish_asset_update_remembers_new_state_after_sending() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )

    def fake_send_message(self, *, text, chat_id=None, message_thread_id=None):
        return {"ok": True, "date": "2026-04-04T20:03:00+00:00", "text": text}

    service.send_message = MethodType(fake_send_message, service)

    result = service.publish_asset_update(
        asset="spx",
        asset_full_name="S&P 500",
        action="long",
        scenario="broad market breakout",
        confidence_label="medium",
        entry_min=5000.0,
        entry_max=5010.0,
        stop_loss=4985.0,
        take_profit_1=5030.0,
        take_profit_2=5045.0,
        risk_reward=1.7,
    )

    restored = service.get_last_asset_state(asset="SPX")

    assert restored is not None
    assert restored["asset"] == "SPX"
    assert restored["last_action"] == "long"
    assert restored["last_scenario"] == "broad market breakout"
    assert restored["last_confidence_label"] == "medium"
    assert restored["last_entry_min"] == 5000.0
    assert restored["last_entry_max"] == 5010.0
    assert restored["last_stop_loss"] == 4985.0
    assert restored["last_take_profit_1"] == 5030.0
    assert restored["last_take_profit_2"] == 5045.0
    assert restored["last_risk_reward"] == 1.7
    assert restored["last_sent_at"] == "2026-04-04T20:03:00+00:00"
    assert result["saved_state"] == restored


def test_publish_asset_update_saves_nested_telegram_result_date_as_last_sent_at() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )

    def fake_send_message(*, text, chat_id=None, message_thread_id=None):
        return {
            "ok": True,
            "result": {
                "message_id": 99,
                "date": 1712533740,
            },
        }

    service.send_message = fake_send_message

    result = service.publish_asset_update(
        asset="SPX",
        action="no_trade",
        scenario="trend_up",
        confidence_label="low",
        entry_min=None,
        entry_max=None,
        stop_loss=None,
        take_profit_1=None,
        take_profit_2=None,
        risk_reward=None,
    )

    assert result["saved_state"] is not None
    assert result["saved_state"]["last_sent_at"] == 1712533740


def test_publish_asset_update_blocks_hourly_update_with_unix_last_sent_at() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )
    sent_at = int(
        datetime(2026, 4, 4, 10, 5, 0, tzinfo=ZoneInfo("Europe/London")).timestamp()
    )
    service.remember_asset_state(
        asset="NDX",
        action="long",
        scenario="trend_up",
        confidence_label="medium",
        entry_min=100.0,
        entry_max=101.0,
        stop_loss=99.0,
        take_profit_1=102.0,
        take_profit_2=103.0,
        risk_reward=1.5,
        sent_at=sent_at,
    )

    def fake_now(self, timezone_name: str):
        return datetime(2026, 4, 4, 10, 45, 0, tzinfo=ZoneInfo("Europe/London"))

    def fake_send_message(self, *, text, chat_id=None, message_thread_id=None):
        raise AssertionError("send_message should not be called inside same hour")

    service._get_schedule_current_dt = MethodType(fake_now, service)
    service._utc_now_provider = lambda: datetime(
        2026, 4, 4, 12, 0, 0, tzinfo=timezone.utc
    )
    service.send_message = MethodType(fake_send_message, service)

    result = service.publish_asset_update(
        asset="NDX",
        asset_full_name="Nasdaq 100",
        action="long",
        scenario="trend_up",
        confidence_label="medium",
        market_explanation="No change in the broader setup.",
        entry_min=100.0,
        entry_max=101.0,
        stop_loss=99.0,
        take_profit_1=102.0,
        take_profit_2=103.0,
        risk_reward=1.5,
        timezone_name="Europe/London",
    )

    assert result["schedule_decision"]["should_send"] is False
    assert result["schedule_decision"]["send_type"] is None
    assert result["send_result"] is None


def test_publish_asset_update_blocks_hourly_update_inside_same_local_hour() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )
    service.remember_asset_state(
        asset="NDX",
        action="long",
        scenario="trend_up",
        confidence_label="medium",
        entry_min=100.0,
        entry_max=101.0,
        stop_loss=99.0,
        take_profit_1=102.0,
        take_profit_2=103.0,
        risk_reward=1.5,
        sent_at="2026-04-04T10:05:00+01:00",
    )

    def fake_now(self, timezone_name: str):
        return datetime(2026, 4, 4, 10, 45, 0, tzinfo=ZoneInfo("Europe/London"))

    def fake_send_message(self, *, text, chat_id=None, message_thread_id=None):
        raise AssertionError("send_message should not be called inside same hour")

    service._get_schedule_current_dt = MethodType(fake_now, service)
    service._utc_now_provider = lambda: datetime(
        2026, 4, 4, 12, 0, 0, tzinfo=timezone.utc
    )
    service.send_message = MethodType(fake_send_message, service)

    result = service.publish_asset_update(
        asset="NDX",
        asset_full_name="Nasdaq 100",
        action="long",
        scenario="trend_up",
        confidence_label="medium",
        market_explanation="No change in the broader setup.",
        entry_min=100.0,
        entry_max=101.0,
        stop_loss=99.0,
        take_profit_1=102.0,
        take_profit_2=103.0,
        risk_reward=1.5,
        timezone_name="Europe/London",
    )

    restored = service.get_last_asset_state(asset="NDX")

    assert result["schedule_decision"]["should_send"] is False
    assert result["schedule_decision"]["send_type"] is None
    assert result["send_result"] is None
    assert restored is not None
    assert restored["last_sent_at"] == "2026-04-04T10:05:00+01:00"


def test_publish_asset_update_allows_hourly_update_in_next_local_hour() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )
    service.remember_asset_state(
        asset="NDX",
        action="long",
        scenario="trend_up",
        confidence_label="medium",
        entry_min=100.0,
        entry_max=101.0,
        stop_loss=99.0,
        take_profit_1=102.0,
        take_profit_2=103.0,
        risk_reward=1.5,
        sent_at="2026-04-04T10:05:00+01:00",
    )

    def fake_now(self, timezone_name: str):
        return datetime(2026, 4, 4, 11, 5, 0, tzinfo=ZoneInfo("Europe/London"))

    def fake_send_message(self, *, text, chat_id=None, message_thread_id=None):
        return {"ok": True, "date": "2026-04-04T11:05:00+01:00", "text": text}

    service._get_schedule_current_dt = MethodType(fake_now, service)
    service._utc_now_provider = lambda: datetime(
        2026, 4, 4, 12, 0, 0, tzinfo=timezone.utc
    )
    service.send_message = MethodType(fake_send_message, service)

    result = service.publish_asset_update(
        asset="NDX",
        asset_full_name="Nasdaq 100",
        action="long",
        scenario="trend_up",
        confidence_label="medium",
        market_explanation="No change in the broader setup.",
        entry_min=100.0,
        entry_max=101.0,
        stop_loss=99.0,
        take_profit_1=102.0,
        take_profit_2=103.0,
        risk_reward=1.5,
        timezone_name="Europe/London",
    )

    restored = service.get_last_asset_state(asset="NDX")

    assert result["schedule_decision"]["should_send"] is True
    assert result["schedule_decision"]["send_type"] == "hourly_update"
    assert result["send_result"]["ok"] is True
    assert restored is not None
    assert restored["last_sent_at"] == "2026-04-04T11:05:00+01:00"


def test_publish_asset_update_allows_immediate_alert() -> None:
    service = TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="123",
            telegram_alerts_enabled=True,
        )
    )
    service.remember_asset_state(
        asset="NDX",
        action="wait",
        scenario="trend_up",
        confidence_label="medium",
        entry_min=100.0,
        entry_max=101.0,
        stop_loss=99.0,
        take_profit_1=102.0,
        take_profit_2=103.0,
        risk_reward=1.5,
        sent_at="2026-04-04T09:55:00+01:00",
    )

    def fake_now(self, timezone_name: str):
        return datetime(2026, 4, 4, 10, 2, 0, tzinfo=ZoneInfo("Europe/London"))

    def fake_send_message(self, *, text, chat_id=None, message_thread_id=None):
        return {"ok": True, "date": "2026-04-04T10:02:00+01:00", "text": text}

    service._get_schedule_current_dt = MethodType(fake_now, service)
    service._utc_now_provider = lambda: datetime(
        2026, 4, 4, 12, 0, 0, tzinfo=timezone.utc
    )
    service.send_message = MethodType(fake_send_message, service)

    result = service.publish_asset_update(
        asset="NDX",
        asset_full_name="Nasdaq 100",
        action="long",
        scenario="trend_up",
        confidence_label="medium",
        market_explanation="Momentum has flipped back in favor of buyers.",
        entry_min=100.0,
        entry_max=101.0,
        stop_loss=99.0,
        take_profit_1=102.0,
        take_profit_2=103.0,
        risk_reward=1.5,
        timezone_name="Europe/London",
    )

    assert result["schedule_decision"]["should_send"] is True
    assert result["schedule_decision"]["send_type"] == "immediate_alert"
    assert result["state_change"]["should_alert"] is True
    assert result["send_result"]["ok"] is True

from __future__ import annotations

from types import MethodType, SimpleNamespace

import pytest

from app.services.telegram_alert_service import TelegramAlertService


def _build_service() -> TelegramAlertService:
    return TelegramAlertService(
        settings=SimpleNamespace(
            telegram_bot_token="test-token",
            telegram_chat_id="-1001234567890",
            telegram_alerts_enabled=True,
        )
    )


def test_publish_asset_topic_alert_dispatches_to_ndx() -> None:
    service = _build_service()

    def fake_publish_ndx_topic_alert(
        self,
        *,
        conditions=None,
        context=None,
        destination_threads=None,
        allowed_destinations=None,
        chat_id=None,
    ):
        return {"event_type": "single_asset_specific_news", "target": "ndx"}

    service.publish_ndx_topic_alert = MethodType(fake_publish_ndx_topic_alert, service)

    result = service.publish_asset_topic_alert(
        asset_topic="ndx",
        conditions=["scenario_change"],
        context={"bias": "long"},
    )

    assert result["target"] == "ndx"


def test_publish_asset_topic_alert_dispatches_to_spx() -> None:
    service = _build_service()

    def fake_publish_spx_topic_alert(
        self,
        *,
        conditions=None,
        context=None,
        destination_threads=None,
        allowed_destinations=None,
        chat_id=None,
    ):
        return {"event_type": "single_asset_specific_news", "target": "spx"}

    service.publish_spx_topic_alert = MethodType(fake_publish_spx_topic_alert, service)

    result = service.publish_asset_topic_alert(
        asset_topic="spx",
        conditions=["scenario_change"],
        context={"bias": "long"},
    )

    assert result["target"] == "spx"


def test_publish_asset_topic_alert_dispatches_to_wti() -> None:
    service = _build_service()

    def fake_publish_wti_topic_alert(
        self,
        *,
        conditions=None,
        context=None,
        destination_threads=None,
        allowed_destinations=None,
        chat_id=None,
    ):
        return {"event_type": "single_asset_specific_news", "target": "wti"}

    service.publish_wti_topic_alert = MethodType(fake_publish_wti_topic_alert, service)

    result = service.publish_asset_topic_alert(
        asset_topic="wti",
        conditions=["news_relevant"],
        context={"bias": "long"},
    )

    assert result["target"] == "wti"


def test_publish_asset_topic_alert_raises_for_unsupported_asset() -> None:
    service = _build_service()

    with pytest.raises(ValueError, match="Unsupported asset_topic"):
        service.publish_asset_topic_alert(asset_topic="btc")

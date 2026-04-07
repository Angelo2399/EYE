from __future__ import annotations

from dataclasses import dataclass

from app.services.telegram_topic_message_builder import TelegramTopicMessageBuilder


@dataclass
class FakeRoutingResolution:
    publish_to: list[str]
    do_not_publish_to: list[str]


def test_build_messages_returns_only_publishable_destinations_from_dict() -> None:
    builder = TelegramTopicMessageBuilder()

    routing_result = {
        "publish_to": ["main", "ndx"],
        "do_not_publish_to": ["spx", "wti", "btc"],
    }
    context = {
        "local_time": "08:30",
        "scenario": "bullish continuation",
        "bias": "long",
        "plausible_action": "long",
        "destinations": {
            "main": {"asset": "EYE overview"},
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
            },
        },
    }

    messages = builder.build_messages(
        event_type="briefing_europe",
        routing_result=routing_result,
        context=context,
    )

    assert set(messages.keys()) == {"main", "ndx"}
    assert "08:30" in messages["main"]
    assert "bullish continuation" in messages["main"]
    assert "long" in messages["main"].lower()

    assert "Nasdaq 100" in messages["ndx"]
    assert "08:30" in messages["ndx"]
    assert "bullish continuation" in messages["ndx"]
    assert "Timeframe: 1h" in messages["ndx"]
    assert "Confidence: medium (61.0%)" in messages["ndx"]
    assert "Entry: 100.00 - 101.00" in messages["ndx"]
    assert "Stop: 99.00" in messages["ndx"]
    assert "TP1: 102.00" in messages["ndx"]
    assert "TP2: 103.00" in messages["ndx"]
    assert "R/R: 1.50" in messages["ndx"]


def test_build_messages_applies_destination_overrides_over_global_context() -> None:
    builder = TelegramTopicMessageBuilder()

    routing_result = {
        "publish_to": ["btc"],
        "do_not_publish_to": ["main", "ndx", "spx", "wti"],
    }
    context = {
        "local_time": "14:05",
        "scenario": "neutral consolidation",
        "bias": "wait",
        "plausible_action": "wait",
        "destinations": {
            "btc": {
                "asset": "Bitcoin",
                "scenario": "impulsive breakout",
                "bias": "long",
                "plausible_action": "long",
            }
        },
    }

    messages = builder.build_messages(
        event_type="single_asset_specific_news",
        routing_result=routing_result,
        context=context,
    )

    assert set(messages.keys()) == {"btc"}
    assert "Bitcoin" in messages["btc"]
    assert "14:05" in messages["btc"]
    assert "impulsive breakout" in messages["btc"]
    assert "long" in messages["btc"].lower()


def test_build_messages_supports_routing_resolution_object() -> None:
    builder = TelegramTopicMessageBuilder()

    routing_result = FakeRoutingResolution(
        publish_to=["wti"],
        do_not_publish_to=["main", "ndx", "spx", "btc"],
    )
    context = {
        "local_time": "16:00",
        "scenario": "supply shock risk",
        "bias": "short",
        "plausible_action": "short",
        "destinations": {
            "wti": {"asset": "WTI Crude Oil"},
        },
    }

    messages = builder.build_messages(
        event_type="oil_opec_eia_iea_supply_shock",
        routing_result=routing_result,
        context=context,
    )

    assert set(messages.keys()) == {"wti"}
    assert "WTI Crude Oil" in messages["wti"]
    assert "16:00" in messages["wti"]
    assert "supply shock risk" in messages["wti"]
    assert "short" in messages["wti"].lower()


def test_build_messages_returns_empty_dict_when_publish_to_is_empty() -> None:
    builder = TelegramTopicMessageBuilder()

    routing_result = {
        "publish_to": [],
        "do_not_publish_to": ["main", "ndx", "spx", "wti", "btc"],
    }
    context = {
        "local_time": "11:00",
        "scenario": "no useful change",
        "bias": "wait",
        "plausible_action": "wait",
    }

    messages = builder.build_messages(
        event_type="noise_no_useful_change_recent_duplicate",
        routing_result=routing_result,
        context=context,
    )

    assert messages == {}

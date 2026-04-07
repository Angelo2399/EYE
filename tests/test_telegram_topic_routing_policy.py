from __future__ import annotations

from app.services.telegram_topic_routing_policy import (
    resolve_event_destinations as _resolve_event_destinations,
)


EVENT_ALIASES = {
    "briefing_europe": "briefing_europe",
    "fed_fomc_very_important": "fed_major",
    "cpi_pce_nfp_very_important": "us_macro_major",
    "oil_opec_eia_iea_supply_shock": "oil_market",
    "single_asset_specific_news": "single_asset_news",
    "bias_change_key_level_break_invalidation": "single_asset_news",
    "noise_no_useful_change_recent_duplicate": "low_signal_noise",
}

CONDITION_ALIASES = {
    "bias_change": "bias_changed",
    "scenario_change": "scenario_changed",
    "key_level_break": "key_level_broken",
    "invalidazione": "invalidation_hit",
    "news_relevant": "relevant_asset_news",
    "update_periodic": "periodic_update_60m",
}


def resolve_event_destinations(
    *,
    event_type: str,
    impacted_topics: list[str],
    conditions: list[str],
) -> dict[str, list[str]]:
    normalized_conditions = [
        CONDITION_ALIASES.get(condition, condition)
        for condition in conditions
    ]

    result = _resolve_event_destinations(
        event=EVENT_ALIASES.get(event_type, event_type),
        impacted_destinations=impacted_topics,
        topic_conditions={
            topic: normalized_conditions
            for topic in impacted_topics
        },
    )

    return {
        "publish_to": [destination.value for destination in result.publish_to],
        "do_not_publish_to": [
            destination.value for destination in result.do_not_publish_to
        ],
    }


def test_briefing_europa_goes_only_to_main() -> None:
    result = resolve_event_destinations(
        event_type="briefing_europe",
        impacted_topics=[],
        conditions=[],
    )

    assert set(result["publish_to"]) == {"main"}
    assert "ndx" in result["do_not_publish_to"]
    assert "spx" in result["do_not_publish_to"]
    assert "wti" in result["do_not_publish_to"]
    assert "btc" in result["do_not_publish_to"]


def test_fed_very_important_goes_to_main_and_ndx_when_ndx_is_impacted_and_condition_is_met() -> None:
    result = resolve_event_destinations(
        event_type="fed_fomc_very_important",
        impacted_topics=["ndx"],
        conditions=["bias_change"],
    )

    assert "main" in result["publish_to"]
    assert "ndx" in result["publish_to"]
    assert "spx" in result["do_not_publish_to"] or "spx" not in result["publish_to"]


def test_general_macro_stays_only_on_main_when_no_topic_becomes_publishable() -> None:
    result = resolve_event_destinations(
        event_type="cpi_pce_nfp_very_important",
        impacted_topics=[],
        conditions=[],
    )

    assert set(result["publish_to"]) == {"main"}


def test_oil_event_goes_to_wti_only_when_wti_is_impacted_and_condition_is_met() -> None:
    result = resolve_event_destinations(
        event_type="oil_opec_eia_iea_supply_shock",
        impacted_topics=["wti"],
        conditions=["news_relevant"],
    )

    assert "wti" in result["publish_to"]
    assert "main" not in result["publish_to"]


def test_specific_asset_event_does_not_go_to_main() -> None:
    result = resolve_event_destinations(
        event_type="single_asset_specific_news",
        impacted_topics=["btc"],
        conditions=["scenario_change"],
    )

    assert "btc" in result["publish_to"]
    assert "main" not in result["publish_to"]


def test_impacted_topic_is_not_published_without_valid_condition() -> None:
    result = resolve_event_destinations(
        event_type="bias_change_key_level_break_invalidation",
        impacted_topics=["ndx"],
        conditions=[],
    )

    assert "ndx" not in result["publish_to"]


def test_noise_duplicate_never_publishes_anything() -> None:
    result = resolve_event_destinations(
        event_type="noise_no_useful_change_recent_duplicate",
        impacted_topics=["ndx", "spx", "wti", "btc"],
        conditions=["bias_change", "news_relevant"],
    )

    assert result["publish_to"] == []

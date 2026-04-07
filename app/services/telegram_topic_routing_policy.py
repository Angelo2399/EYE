from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable, Mapping


class TelegramRoutingEvent(str, Enum):
    briefing_europe = "briefing_europe"
    briefing_usa_open = "briefing_usa_open"
    macro_general_alert = "macro_general_alert"
    fed_major = "fed_major"
    us_macro_major = "us_macro_major"
    geopolitics_global = "geopolitics_global"
    treasury_yields_growth = "treasury_yields_growth"
    mega_cap_tech = "mega_cap_tech"
    us_broad_market = "us_broad_market"
    oil_market = "oil_market"
    btc_market = "btc_market"
    single_asset_news = "single_asset_news"
    periodic_update_60m = "periodic_update_60m"
    active_thesis_update_30m = "active_thesis_update_30m"
    low_signal_noise = "low_signal_noise"


class TelegramRoutingDestination(str, Enum):
    main = "main"
    ndx = "ndx"
    spx = "spx"
    wti = "wti"
    btc = "btc"


class TopicPublishCondition(str, Enum):
    bias_changed = "bias_changed"
    key_level_broken = "key_level_broken"
    scenario_changed = "scenario_changed"
    relevant_asset_news = "relevant_asset_news"
    invalidation_hit = "invalidation_hit"
    periodic_update_60m = "periodic_update_60m"
    active_thesis_update_30m = "active_thesis_update_30m"


CANONICAL_DESTINATIONS: tuple[TelegramRoutingDestination, ...] = (
    TelegramRoutingDestination.main,
    TelegramRoutingDestination.ndx,
    TelegramRoutingDestination.spx,
    TelegramRoutingDestination.wti,
    TelegramRoutingDestination.btc,
)

TOPIC_DESTINATIONS: tuple[TelegramRoutingDestination, ...] = (
    TelegramRoutingDestination.ndx,
    TelegramRoutingDestination.spx,
    TelegramRoutingDestination.wti,
    TelegramRoutingDestination.btc,
)

CONDITIONAL_TOPIC_PUBLISH_RULE: frozenset[TopicPublishCondition] = frozenset(
    TopicPublishCondition
)


@dataclass(frozen=True)
class EventRoutingRule:
    publish_main: bool
    eligible_topics: tuple[TelegramRoutingDestination, ...]


@dataclass(frozen=True)
class RoutingResolution:
    publish_to: tuple[TelegramRoutingDestination, ...]
    do_not_publish_to: tuple[TelegramRoutingDestination, ...]


EVENT_ROUTING_RULES: dict[TelegramRoutingEvent, EventRoutingRule] = {
    TelegramRoutingEvent.briefing_europe: EventRoutingRule(
        publish_main=True,
        eligible_topics=(),
    ),
    TelegramRoutingEvent.briefing_usa_open: EventRoutingRule(
        publish_main=True,
        eligible_topics=(),
    ),
    TelegramRoutingEvent.macro_general_alert: EventRoutingRule(
        publish_main=True,
        eligible_topics=TOPIC_DESTINATIONS,
    ),
    TelegramRoutingEvent.fed_major: EventRoutingRule(
        publish_main=True,
        eligible_topics=TOPIC_DESTINATIONS,
    ),
    TelegramRoutingEvent.us_macro_major: EventRoutingRule(
        publish_main=True,
        eligible_topics=TOPIC_DESTINATIONS,
    ),
    TelegramRoutingEvent.geopolitics_global: EventRoutingRule(
        publish_main=True,
        eligible_topics=TOPIC_DESTINATIONS,
    ),
    TelegramRoutingEvent.treasury_yields_growth: EventRoutingRule(
        publish_main=False,
        eligible_topics=(
            TelegramRoutingDestination.ndx,
            TelegramRoutingDestination.spx,
            TelegramRoutingDestination.btc,
        ),
    ),
    TelegramRoutingEvent.mega_cap_tech: EventRoutingRule(
        publish_main=False,
        eligible_topics=(
            TelegramRoutingDestination.ndx,
            TelegramRoutingDestination.spx,
        ),
    ),
    TelegramRoutingEvent.us_broad_market: EventRoutingRule(
        publish_main=False,
        eligible_topics=(
            TelegramRoutingDestination.ndx,
            TelegramRoutingDestination.spx,
            TelegramRoutingDestination.btc,
        ),
    ),
    TelegramRoutingEvent.oil_market: EventRoutingRule(
        publish_main=False,
        eligible_topics=(
            TelegramRoutingDestination.spx,
            TelegramRoutingDestination.wti,
        ),
    ),
    TelegramRoutingEvent.btc_market: EventRoutingRule(
        publish_main=False,
        eligible_topics=(TelegramRoutingDestination.btc,),
    ),
    TelegramRoutingEvent.single_asset_news: EventRoutingRule(
        publish_main=False,
        eligible_topics=TOPIC_DESTINATIONS,
    ),
    TelegramRoutingEvent.periodic_update_60m: EventRoutingRule(
        publish_main=False,
        eligible_topics=TOPIC_DESTINATIONS,
    ),
    TelegramRoutingEvent.active_thesis_update_30m: EventRoutingRule(
        publish_main=False,
        eligible_topics=TOPIC_DESTINATIONS,
    ),
    TelegramRoutingEvent.low_signal_noise: EventRoutingRule(
        publish_main=False,
        eligible_topics=(),
    ),
}


def resolve_event_destinations(
    *,
    event: TelegramRoutingEvent | str,
    impacted_destinations: (
        Iterable[TelegramRoutingDestination | str]
        | TelegramRoutingDestination
        | str
        | None
    ) = None,
    topic_conditions: Mapping[
        TelegramRoutingDestination | str,
        Iterable[TopicPublishCondition | str] | TopicPublishCondition | str,
    ]
    | None = None,
) -> RoutingResolution:
    resolved_event = _normalize_event(event)
    routing_rule = EVENT_ROUTING_RULES[resolved_event]
    resolved_impacted_destinations = _normalize_topic_destinations(impacted_destinations)
    resolved_topic_conditions = _normalize_topic_conditions(topic_conditions)

    publish_to: list[TelegramRoutingDestination] = []

    if routing_rule.publish_main:
        publish_to.append(TelegramRoutingDestination.main)

    for destination in TOPIC_DESTINATIONS:
        if destination not in routing_rule.eligible_topics:
            continue

        if destination not in resolved_impacted_destinations:
            continue

        if not _matches_cond_rule(
            destination=destination,
            topic_conditions=resolved_topic_conditions,
        ):
            continue

        publish_to.append(destination)

    publish_tuple = tuple(publish_to)
    do_not_publish_tuple = tuple(
        destination
        for destination in CANONICAL_DESTINATIONS
        if destination not in publish_tuple
    )

    return RoutingResolution(
        publish_to=publish_tuple,
        do_not_publish_to=do_not_publish_tuple,
    )


def _matches_cond_rule(
    *,
    destination: TelegramRoutingDestination,
    topic_conditions: Mapping[
        TelegramRoutingDestination,
        frozenset[TopicPublishCondition],
    ],
) -> bool:
    # `cond.` means the topic is impacted and at least one canonical publish
    # condition is present for that specific topic.
    return bool(topic_conditions.get(destination, frozenset()))


def _normalize_event(event: TelegramRoutingEvent | str) -> TelegramRoutingEvent:
    if isinstance(event, TelegramRoutingEvent):
        return event

    return TelegramRoutingEvent(str(event).strip().lower())


def _normalize_topic_destinations(
    destinations: (
        Iterable[TelegramRoutingDestination | str]
        | TelegramRoutingDestination
        | str
        | None
    ),
) -> frozenset[TelegramRoutingDestination]:
    if destinations is None:
        return frozenset()

    iterable_destinations: Iterable[TelegramRoutingDestination | str]
    if isinstance(destinations, (str, TelegramRoutingDestination)):
        iterable_destinations = (destinations,)
    else:
        iterable_destinations = destinations

    return frozenset(
        _normalize_topic_destination(destination)
        for destination in iterable_destinations
    )


def _normalize_topic_conditions(
    topic_conditions: Mapping[
        TelegramRoutingDestination | str,
        Iterable[TopicPublishCondition | str] | TopicPublishCondition | str,
    ]
    | None,
) -> dict[TelegramRoutingDestination, frozenset[TopicPublishCondition]]:
    normalized: dict[TelegramRoutingDestination, frozenset[TopicPublishCondition]] = {}

    if not topic_conditions:
        return normalized

    for destination, conditions in topic_conditions.items():
        resolved_destination = _normalize_topic_destination(destination)
        normalized_conditions = frozenset(
            _normalize_topic_condition(condition)
            for condition in _iter_topic_conditions(conditions)
        )

        normalized[resolved_destination] = frozenset(
            condition
            for condition in normalized_conditions
            if condition in CONDITIONAL_TOPIC_PUBLISH_RULE
        )

    return normalized


def _normalize_topic_destination(
    destination: TelegramRoutingDestination | str,
) -> TelegramRoutingDestination:
    if isinstance(destination, TelegramRoutingDestination):
        resolved_destination = destination
    else:
        resolved_destination = TelegramRoutingDestination(str(destination).strip().lower())

    if resolved_destination == TelegramRoutingDestination.main:
        raise ValueError("Topic destinations cannot include 'main'.")

    return resolved_destination


def _normalize_topic_condition(
    condition: TopicPublishCondition | str,
) -> TopicPublishCondition:
    if isinstance(condition, TopicPublishCondition):
        return condition

    return TopicPublishCondition(str(condition).strip().lower())


def _iter_topic_conditions(
    conditions: (
        Iterable[TopicPublishCondition | str]
        | TopicPublishCondition
        | str
        | None
    ),
) -> Iterable[TopicPublishCondition | str]:
    if conditions is None:
        return ()

    if isinstance(conditions, (str, TopicPublishCondition)):
        return (conditions,)

    return conditions


__all__ = [
    "CANONICAL_DESTINATIONS",
    "CONDITIONAL_TOPIC_PUBLISH_RULE",
    "EVENT_ROUTING_RULES",
    "EventRoutingRule",
    "RoutingResolution",
    "TOPIC_DESTINATIONS",
    "TelegramRoutingDestination",
    "TelegramRoutingEvent",
    "TopicPublishCondition",
    "resolve_event_destinations",
]

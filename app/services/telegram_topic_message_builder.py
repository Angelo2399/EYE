from __future__ import annotations

from collections.abc import Mapping

from app.services.telegram_topic_routing_policy import RoutingResolution


EVENT_TITLES: dict[str, str] = {
    "briefing_europe": "Briefing Europa",
    "briefing_usa_open": "Apertura USA",
    "macro_general_alert": "Alert generale",
    "fed_major": "Fed / FOMC",
    "us_macro_major": "Macro USA",
    "geopolitics_global": "Geopolitica globale",
    "treasury_yields_growth": "Treasury yields",
    "mega_cap_tech": "Mega-cap tech",
    "us_broad_market": "Mercato USA",
    "oil_market": "Mercato oil",
    "btc_market": "Mercato BTC",
    "single_asset_news": "News specifica asset",
    "periodic_update_60m": "Update periodico 60m",
    "active_thesis_update_30m": "Update tesi attiva 30m",
    "low_signal_noise": "Nessun aggiornamento utile",
}

DESTINATION_LABELS: dict[str, str] = {
    "main": "Mercato generale",
    "ndx": "NDX",
    "spx": "SPX",
    "wti": "WTI",
    "btc": "BTC",
}


class TelegramTopicMessageBuilder:
    def build_messages(
        self,
        *,
        event_type: str,
        routing_result: dict[str, list[str]] | RoutingResolution,
        context: dict[str, object],
    ) -> dict[str, str]:
        publish_to = self._extract_publish_to(routing_result)
        messages: dict[str, str] = {}

        for destination in publish_to:
            destination_context = self._resolve_destination_context(
                destination=destination,
                context=context,
            )
            messages[destination] = self._build_message_text(
                event_type=event_type,
                destination=destination,
                context=destination_context,
            )

        return messages

    def _extract_publish_to(
        self,
        routing_result: dict[str, list[str]] | RoutingResolution | object,
    ) -> list[str]:
        if isinstance(routing_result, RoutingResolution):
            return [destination.value for destination in routing_result.publish_to]

        if isinstance(routing_result, Mapping):
            publish_to = routing_result.get("publish_to", [])
            return [str(destination).strip().lower() for destination in publish_to]

        publish_to = getattr(routing_result, "publish_to", [])
        return [str(destination).strip().lower() for destination in publish_to]

    def _resolve_destination_context(
        self,
        *,
        destination: str,
        context: dict[str, object],
    ) -> dict[str, object]:
        resolved_context = dict(context)
        destinations_context = context.get("destinations")

        if isinstance(destinations_context, Mapping):
            destination_override = destinations_context.get(destination)

            if isinstance(destination_override, Mapping):
                resolved_context.update(destination_override)

        return resolved_context

    def _build_message_text(
        self,
        *,
        event_type: str,
        destination: str,
        context: dict[str, object],
    ) -> str:
        event_title = EVENT_TITLES.get(event_type, self._humanize_value(event_type))
        destination_label = DESTINATION_LABELS.get(
            destination,
            destination.upper(),
        )

        if destination == "main":
            title = f"EYE | {event_title}"
        else:
            title = f"EYE | {destination_label} | {event_title}"

        local_time = self._string_value(
            context,
            "local_time",
            default="n/a",
        )
        asset = self._string_value(
            context,
            "asset",
            default=destination_label,
        )
        scenario = self._string_value(
            context,
            "scenario",
            default="n/a",
        )
        bias = self._string_value(
            context,
            "bias",
            default="n/a",
        )
        plausible_action = self._string_value(
            context,
            "plausible_action",
            "action",
            default="n/a",
        )
        lines = [
            title,
            f"Topic: {destination_label}",
            f"Asset: {asset}",
            f"Ora locale: {local_time}",
            f"Scenario: {scenario}",
            f"Bias: {bias}",
            f"Azione plausibile: {plausible_action}",
        ]

        if destination != "main":
            lines.extend(self._build_operational_lines(context))

        return "\n".join(lines)

    def _build_operational_lines(
        self,
        context: Mapping[str, object],
    ) -> list[str]:
        lines: list[str] = []

        timeframe = self._string_value(
            context,
            "timeframe",
            default="",
        )
        if timeframe:
            lines.append(f"Timeframe: {timeframe}")

        confidence_line = self._build_confidence_line(context)
        if confidence_line:
            lines.append(confidence_line)

        entry_min = self._float_value(context, "entry_min")
        entry_max = self._float_value(context, "entry_max")
        if entry_min is not None and entry_max is not None:
            lines.append(f"Entry: {entry_min:.2f} - {entry_max:.2f}")
        elif entry_min is not None:
            lines.append(f"Entry: {entry_min:.2f}")
        elif entry_max is not None:
            lines.append(f"Entry: {entry_max:.2f}")

        stop_loss = self._float_value(context, "stop_loss")
        if stop_loss is not None:
            lines.append(f"Stop: {stop_loss:.2f}")

        take_profit_1 = self._float_value(context, "take_profit_1")
        if take_profit_1 is not None:
            lines.append(f"TP1: {take_profit_1:.2f}")

        take_profit_2 = self._float_value(context, "take_profit_2")
        if take_profit_2 is not None:
            lines.append(f"TP2: {take_profit_2:.2f}")

        risk_reward = self._float_value(context, "risk_reward")
        if risk_reward is not None:
            lines.append(f"R/R: {risk_reward:.2f}")

        return lines

    def _build_confidence_line(
        self,
        context: Mapping[str, object],
    ) -> str | None:
        confidence_label = self._string_value(
            context,
            "confidence_label",
            default="",
        )
        model_confidence_pct = self._float_value(context, "model_confidence_pct")

        has_label = bool(confidence_label and confidence_label.lower() != "unknown")
        has_pct = model_confidence_pct is not None and model_confidence_pct > 0.0

        if has_label and has_pct:
            return f"Confidence: {confidence_label} ({model_confidence_pct:.1f}%)"

        if has_label:
            return f"Confidence: {confidence_label}"

        if has_pct:
            return f"Confidence: {model_confidence_pct:.1f}%"

        return None

    def _string_value(
        self,
        context: Mapping[str, object],
        *keys: str,
        default: str,
    ) -> str:
        for key in keys:
            value = context.get(key)

            if value is None:
                continue

            text = str(value).strip()
            if text:
                return text

        return default

    def _float_value(
        self,
        context: Mapping[str, object],
        *keys: str,
    ) -> float | None:
        for key in keys:
            value = context.get(key)

            if value is None:
                continue

            try:
                return float(value)
            except (TypeError, ValueError):
                continue

        return None

    def _humanize_value(self, value: str) -> str:
        normalized = str(value).strip().replace("_", " ")
        if not normalized:
            return "Evento"

        return normalized.capitalize()


__all__ = [
    "DESTINATION_LABELS",
    "EVENT_TITLES",
    "TelegramTopicMessageBuilder",
]

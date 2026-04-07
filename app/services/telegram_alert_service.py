from __future__ import annotations

import json
from datetime import datetime, time, timezone
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from app.core.config import Settings, get_settings
from app.services.asset_update_schedule_service import AssetUpdateScheduleService
from app.services.telegram_topic_orchestrator_service import (
    TelegramTopicOrchestratorService,
)

RISK_DISCLAIMER = (
    "Risk disclaimer: Trading involves significant market risk. "
    "EYE provides informational support only and is not responsible "
    "for any trading losses."
)


class TelegramAlertService:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.asset_update_schedule_service = AssetUpdateScheduleService()
        self.topic_orchestrator_service = TelegramTopicOrchestratorService()
        self.quiet_hours_timezone = "Europe/Madrid"
        self.quiet_hours_start = time(21, 0)
        self.quiet_hours_end = time(8, 0)
        self._utc_now_provider = lambda: datetime.now(timezone.utc)
        self._last_asset_states: dict[str, dict[str, object]] = {}

    def is_configured(self) -> bool:
        token = self.settings.telegram_bot_token
        return bool(token and str(token).strip())

    def _normalize_asset_key(self, asset: str) -> str:
        return str(asset or "").strip().upper()

    def remember_asset_state(
        self,
        *,
        asset: str,
        action: str | None = None,
        scenario: str | None = None,
        confidence_label: str | None = None,
        entry_min: float | None = None,
        entry_max: float | None = None,
        stop_loss: float | None = None,
        take_profit_1: float | None = None,
        take_profit_2: float | None = None,
        risk_reward: float | None = None,
        sent_at: str | int | None = None,
    ) -> dict[str, object]:
        asset_key = self._normalize_asset_key(asset)

        state = {
            "asset": asset_key,
            "last_action": action,
            "last_scenario": scenario,
            "last_confidence_label": confidence_label,
            "last_entry_min": entry_min,
            "last_entry_max": entry_max,
            "last_stop_loss": stop_loss,
            "last_take_profit_1": take_profit_1,
            "last_take_profit_2": take_profit_2,
            "last_risk_reward": risk_reward,
            "last_sent_at": sent_at,
        }

        self._last_asset_states[asset_key] = state
        return dict(state)

    def get_last_asset_state(
        self,
        *,
        asset: str,
    ) -> dict[str, object] | None:
        asset_key = self._normalize_asset_key(asset)
        state = self._last_asset_states.get(asset_key)
        return dict(state) if state is not None else None

    def evaluate_asset_state_change(
        self,
        *,
        asset: str,
        action: str | None = None,
        scenario: str | None = None,
        confidence_label: str | None = None,
        entry_min: float | None = None,
        entry_max: float | None = None,
        stop_loss: float | None = None,
        take_profit_1: float | None = None,
        take_profit_2: float | None = None,
        risk_reward: float | None = None,
    ) -> dict[str, object]:
        previous_state = self.get_last_asset_state(asset=asset)
        if previous_state is None:
            return {
                "has_previous_state": False,
                "change_detected": False,
                "should_alert": False,
                "change_reasons": [],
            }

        change_reasons: list[str] = []

        if previous_state.get("last_action") != action:
            change_reasons.append("action_changed")

        if previous_state.get("last_scenario") != scenario:
            change_reasons.append("scenario_changed")

        if previous_state.get("last_confidence_label") != confidence_label:
            change_reasons.append("confidence_changed")

        levels_changed = any(
            [
                previous_state.get("last_entry_min") != entry_min,
                previous_state.get("last_entry_max") != entry_max,
                previous_state.get("last_stop_loss") != stop_loss,
                previous_state.get("last_take_profit_1") != take_profit_1,
                previous_state.get("last_take_profit_2") != take_profit_2,
                previous_state.get("last_risk_reward") != risk_reward,
            ]
        )
        if levels_changed:
            change_reasons.append("levels_changed")

        should_alert = any(
            reason in {"action_changed", "scenario_changed", "confidence_changed"}
            for reason in change_reasons
        )

        return {
            "has_previous_state": True,
            "change_detected": bool(change_reasons),
            "should_alert": should_alert,
            "change_reasons": change_reasons,
        }

    def build_asset_update_message(
        self,
        *,
        asset: str,
        asset_full_name: str | None = None,
        action: str | None = None,
        scenario: str | None = None,
        confidence_label: str | None = None,
        market_explanation: str | None = None,
        entry_min: float | None = None,
        entry_max: float | None = None,
        stop_loss: float | None = None,
        take_profit_1: float | None = None,
        take_profit_2: float | None = None,
        risk_reward: float | None = None,
        reason: str | None = None,
        previous_state: dict[str, object] | None = None,
        state_change: dict[str, object] | None = None,
    ) -> str:
        signal_text = self._format_signal_label(action)
        asset_text = self._format_asset_label(asset)
        confidence_text = self._format_confidence_label(confidence_label)
        scenario_text = self._format_scenario_label(scenario)
        reason_text = self._format_reason_text(reason or market_explanation)

        if signal_text in {"WAIT", "NO TRADE"}:
            return "\n".join(
                [
                    f"\U0001F4CA {asset_text}",
                    f"\u2022 Signal: {signal_text}",
                    f"\u2022 Scenario: {scenario_text}",
                    f"\u2022 Confidence: {confidence_text}",
                    f"\u2022 Reason: {reason_text}",
                    "\u2022 Entry: n/a",
                    "\u2022 Stop loss: n/a",
                    "\u2022 Take profit: n/a",
                ]
            )

        take_profit_text = self._format_take_profit_summary(
            take_profit_1=take_profit_1,
            take_profit_2=take_profit_2,
        )

        return "\n".join(
            [
                f"\U0001F4CA {asset_text}",
                f"\u2022 Signal: {signal_text}",
                f"\u2022 Scenario: {scenario_text}",
                f"\u2022 Confidence: {confidence_text}",
                f"\u2022 Reason: {reason_text}",
                f"\u2022 Entry: {self._format_entry_range(entry_min, entry_max)}",
                f"\u2022 Stop loss: {self._format_price(stop_loss)}",
                f"\u2022 Take profit: {take_profit_text}",
            ]
        )

    def _format_level(self, value: float | None) -> str:
        if value is None:
            return "n/a"

        return f"{float(value):.2f}"

    def _format_asset_label(self, asset: str | None) -> str:
        mapping = {
            "NDX": "Nasdaq 100 (NDX)",
            "SPX": "S&P 500 (SPX)",
            "WTI": "WTI Crude Oil (WTI)",
            "BTC": "Bitcoin (BTC)",
        }
        key = str(asset or "").strip().upper()
        return mapping.get(key, str(asset or "Asset").strip())

    def _format_signal_label(self, action: str | None) -> str:
        normalized = str(action or "").strip().lower()

        mapping = {
            "long": "BUY",
            "buy": "BUY",
            "short": "SELL",
            "sell": "SELL",
            "wait": "WAIT",
            "no_trade": "NO TRADE",
            "no trade": "NO TRADE",
        }

        return mapping.get(normalized, "WAIT")

    def _format_scenario_label(self, scenario: str | None) -> str:
        text = str(scenario or "n/a").strip().replace("_", " ")
        if not text:
            return "n/a"
        return text

    def _format_confidence_label(self, confidence_label: str | None) -> str:
        text = str(confidence_label or "n/a").strip().replace("_", " ")
        if not text:
            return "n/a"
        return text

    def _format_reason_text(self, reason: str | None) -> str:
        text = str(reason or "").strip()
        if not text:
            return "No strong confirmation from current market context."

        text = text.replace(
            "External intelligence disabled.",
            "No confirmed macro/news edge right now.",
        )
        text = text.replace(
            "External intelligence unavailable.",
            "No confirmed macro/news edge right now.",
        )
        text = text.replace(
            "External intelligence fetch failed.",
            "Macro/news input unavailable right now.",
        )
        text = text.replace("No-trade bias for now:", "")
        text = text.replace("Wait bias:", "")
        text = " ".join(text.split())

        return text

    def _format_entry_range(
        self,
        entry_min: float | None,
        entry_max: float | None,
    ) -> str:
        if entry_min is None and entry_max is None:
            return "n/a"

        if entry_min is None:
            return self._format_price(entry_max)

        if entry_max is None:
            return self._format_price(entry_min)

        return f"{self._format_price(entry_min)} - {self._format_price(entry_max)}"

    def _format_take_profit_summary(
        self,
        *,
        take_profit_1: float | None,
        take_profit_2: float | None,
    ) -> str:
        if take_profit_1 is None and take_profit_2 is None:
            return "n/a"
        if take_profit_1 is not None and take_profit_2 is not None:
            return (
                f"{self._format_price(take_profit_1)} / "
                f"{self._format_price(take_profit_2)}"
            )
        if take_profit_1 is not None:
            return self._format_price(take_profit_1)
        return self._format_price(take_profit_2)

    def _format_price(self, value: float | None) -> str:
        if value is None:
            return "n/a"
        return f"{float(value):.2f}"

    def _build_risk_reward_lines(self, risk_reward: float | None) -> list[str]:
        if risk_reward is None:
            return [
                "Risk/reward ratio: n/a",
                "Expected reward is not available.",
            ]

        ratio_text = self._format_level(risk_reward)
        return [
            f"Risk/reward ratio: {ratio_text} to 1",
            f"Expected reward is {ratio_text}x the defined risk.",
        ]

    def publish_asset_update(
        self,
        *,
        asset: str,
        asset_full_name: str | None = None,
        action: str | None = None,
        scenario: str | None = None,
        confidence_label: str | None = None,
        market_explanation: str | None = None,
        entry_min: float | None = None,
        entry_max: float | None = None,
        stop_loss: float | None = None,
        take_profit_1: float | None = None,
        take_profit_2: float | None = None,
        risk_reward: float | None = None,
        timezone_name: str | None = None,
        chat_id: str | None = None,
    ) -> dict[str, object]:
        if timezone_name and self._is_quiet_hours(timezone_name):
            return self._build_quiet_hours_result(
                timezone_name=timezone_name,
                asset=asset,
                event_type="asset_update",
            )

        previous_state = self.get_last_asset_state(asset=asset)

        state_change = self.evaluate_asset_state_change(
            asset=asset,
            action=action,
            scenario=scenario,
            confidence_label=confidence_label,
            entry_min=entry_min,
            entry_max=entry_max,
            stop_loss=stop_loss,
            take_profit_1=take_profit_1,
            take_profit_2=take_profit_2,
            risk_reward=risk_reward,
        )
        reason = str(market_explanation or "").strip()

        message = self.build_asset_update_message(
            asset=asset,
            asset_full_name=asset_full_name,
            action=action,
            scenario=scenario,
            confidence_label=confidence_label,
            market_explanation=market_explanation,
            entry_min=entry_min,
            entry_max=entry_max,
            stop_loss=stop_loss,
            take_profit_1=take_profit_1,
            take_profit_2=take_profit_2,
            risk_reward=risk_reward,
            reason=reason,
            previous_state=previous_state,
            state_change=state_change,
        )

        resolved_timezone_name = self._resolve_timezone_name(timezone_name)
        state_change_kind = (
            "alert" if bool(state_change.get("should_alert")) else "hourly_update"
        )
        schedule_service = getattr(self, "asset_update_schedule_service", None)
        if schedule_service is None:
            schedule_service = AssetUpdateScheduleService()
            self.asset_update_schedule_service = schedule_service

        schedule_decision = schedule_service.resolve_schedule(
            current_dt=self._get_schedule_current_dt(resolved_timezone_name),
            timezone_name=resolved_timezone_name,
            last_sent_at=(
                previous_state.get("last_sent_at")
                if isinstance(previous_state, dict)
                else None
            ),
            state_change_kind=state_change_kind,
        )
        resolved_schedule_decision = {
            "should_send": schedule_decision.should_send,
            "send_type": schedule_decision.send_type,
            "local_time": schedule_decision.local_time,
        }

        if not schedule_decision.should_send:
            return {
                "previous_state": previous_state,
                "state_change": state_change,
                "message": message,
                "send_result": None,
                "saved_state": previous_state,
                "schedule_decision": resolved_schedule_decision,
            }

        send_result = self.send_message(
            text=message,
            chat_id=chat_id,
        )

        saved_state = self.remember_asset_state(
            asset=asset,
            action=action,
            scenario=scenario,
            confidence_label=confidence_label,
            entry_min=entry_min,
            entry_max=entry_max,
            stop_loss=stop_loss,
            take_profit_1=take_profit_1,
            take_profit_2=take_profit_2,
            risk_reward=risk_reward,
            sent_at=self._extract_message_date(send_result),
        )

        return {
            "previous_state": previous_state,
            "state_change": state_change,
            "message": message,
            "send_result": send_result,
            "saved_state": saved_state,
            "schedule_decision": resolved_schedule_decision,
        }

    def _extract_message_date(self, send_result: object) -> str | int | None:
        if not isinstance(send_result, dict):
            return None

        direct_date = send_result.get("date")
        if direct_date is not None:
            try:
                return int(direct_date)
            except (TypeError, ValueError):
                direct_text = str(direct_date).strip()
                return direct_text or None

        nested_result = send_result.get("result")
        if isinstance(nested_result, dict):
            nested_date = nested_result.get("date")
            if nested_date is not None:
                try:
                    return int(nested_date)
                except (TypeError, ValueError):
                    nested_text = str(nested_date).strip()
                    return nested_text or None

        return None

    def _with_risk_disclaimer(self, text: str) -> str:
        message_text = str(text or "").strip()

        if RISK_DISCLAIMER in message_text:
            if message_text.startswith(RISK_DISCLAIMER):
                return message_text

            message_without_disclaimer = (
                message_text.replace(RISK_DISCLAIMER, "", 1).strip()
            )
            if not message_without_disclaimer:
                return RISK_DISCLAIMER

            return f"{RISK_DISCLAIMER}\n\n{message_without_disclaimer}"

        return f"{RISK_DISCLAIMER}\n\n{message_text}"

    def _resolve_timezone_name(self, timezone_name: str | None) -> str:
        candidate = str(timezone_name or "").strip()
        if not candidate:
            return "UTC"

        try:
            ZoneInfo(candidate)
            return candidate
        except Exception:
            return "UTC"

    def _get_schedule_current_dt(self, timezone_name: str) -> datetime:
        return datetime.now(ZoneInfo(timezone_name))

    def _get_local_now(self, timezone_name: str | None = None) -> datetime:
        resolved_timezone = str(timezone_name or self.quiet_hours_timezone)
        return self._utc_now_provider().astimezone(ZoneInfo(resolved_timezone))

    def _is_quiet_hours(self, timezone_name: str | None = None) -> bool:
        local_now = self._get_local_now(timezone_name)
        local_time = local_now.time().replace(tzinfo=None)

        return (
            local_time >= self.quiet_hours_start
            or local_time < self.quiet_hours_end
        )

    def _build_quiet_hours_result(
        self,
        *,
        timezone_name: str | None = None,
        asset: str | None = None,
        event_type: str | None = None,
    ) -> dict[str, object]:
        local_now = self._get_local_now(timezone_name)

        result = {
            "ok": False,
            "suppressed": True,
            "reason": "quiet_hours",
            "timezone_name": str(timezone_name or self.quiet_hours_timezone),
            "local_time": local_now.strftime("%H:%M:%S"),
        }

        if asset is not None:
            result["asset"] = asset

        if event_type is not None:
            result["event_type"] = event_type

        return result

    def send_message(
        self,
        *,
        text: str,
        chat_id: str | None = None,
        message_thread_id: int | None = None,
    ) -> dict:
        token = self.settings.telegram_bot_token
        if not token or not str(token).strip():
            raise ValueError("Telegram bot token not configured.")

        resolved_chat_id = None
        if chat_id and str(chat_id).strip():
            resolved_chat_id = str(chat_id).strip()
        elif self.settings.telegram_chat_id and str(self.settings.telegram_chat_id).strip():
            resolved_chat_id = str(self.settings.telegram_chat_id).strip()

        if not resolved_chat_id:
            raise ValueError("Telegram chat_id not configured.")

        if not text or not str(text).strip():
            raise ValueError("Telegram message text is required.")

        resolved_text = self._with_risk_disclaimer(str(text))
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": resolved_chat_id,
            "text": resolved_text,
        }
        if message_thread_id is not None:
            payload["message_thread_id"] = int(message_thread_id)

        request = Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "User-Agent": "EYE/1.0",
            },
            method="POST",
        )

        with urlopen(request, timeout=15) as response:
            return json.loads(response.read().decode("utf-8"))

    def should_send_signal_alert(self, *, action: str) -> bool:
        if not self.settings.telegram_alerts_enabled:
            return False

        normalized_action = str(action).strip().lower()
        return normalized_action in {"long", "short"}

    def send_signal_alert(
        self,
        *,
        asset: str,
        action: str,
        confidence_label: str,
        explanation: str,
        timeframe: str | None = None,
    ) -> dict:
        timeframe_text = f" | timeframe {timeframe}" if timeframe else ""
        message = (
            f"EYE ALERT\n"
            f"Asset: {asset}{timeframe_text}\n"
            f"Azione: {action}\n"
            f"Confidence: {confidence_label}\n"
            f"Spiegazione: {explanation}"
        )
        return self.send_message(text=message)

    def send_briefing_alert(
        self,
        *,
        title: str,
        summary: str,
        timezone_name: str | None = None,
        local_time: str | None = None,
        chat_id: str | None = None,
    ) -> dict:
        if timezone_name and self._is_quiet_hours(timezone_name):
            return self._build_quiet_hours_result(
                timezone_name=timezone_name,
                event_type="briefing",
            )

        timezone_text = f" | TZ {timezone_name}" if timezone_name else ""
        local_time_text = f" | Ora locale {local_time}" if local_time else ""

        message = (
            f"EYE BRIEFING\n"
            f"{title}{timezone_text}{local_time_text}\n"
            f"{summary}"
        )

        return self.send_message(text=message, chat_id=chat_id)

    def send_briefing_payload(
        self,
        *,
        briefing_payload: dict[str, object],
        chat_id: str | None = None,
    ) -> dict:
        payload = dict(briefing_payload or {})
        title = str(payload.get("title") or "").strip()
        summary = str(payload.get("summary") or "").strip()
        market_closures = list(payload.get("market_closures") or [])

        if not title:
            raise ValueError("Briefing payload title is required.")

        if not summary:
            raise ValueError("Briefing payload summary is required.")

        summary = self._append_market_closures_section(
            summary=summary,
            market_closures=market_closures,
        )

        timezone_name = payload.get("timezone_name")
        local_time = payload.get("local_time")

        return self.send_briefing_alert(
            title=title,
            summary=summary,
            timezone_name=(
                str(timezone_name).strip() if timezone_name is not None else None
            ),
            local_time=str(local_time).strip() if local_time is not None else None,
            chat_id=chat_id,
        )

    def _append_market_closures_section(
        self,
        *,
        summary: str,
        market_closures: list[object],
    ) -> str:
        resolved_summary = str(summary or "").strip()
        if not market_closures:
            return resolved_summary

        if "Closed / limited markets today" in resolved_summary:
            return resolved_summary

        closure_lines: list[str] = []
        for item in market_closures:
            if not isinstance(item, dict):
                continue

            asset = str(item.get("asset") or "").strip()
            market_status = str(item.get("market_status") or "").strip()
            official_reason = str(item.get("official_reason") or "").strip()
            next_open_local = item.get("next_open_local")
            next_open_text = (
                str(next_open_local).strip()
                if next_open_local is not None and str(next_open_local).strip()
                else "n/a"
            )

            if not asset:
                continue

            closure_lines.append(
                f"- {asset} | {market_status or 'n/a'} | "
                f"{official_reason or 'n/a'} | next open {next_open_text}"
            )

        if not closure_lines:
            return resolved_summary

        return "\n".join(
            [
                resolved_summary,
                "",
                "Closed / limited markets today:",
                *closure_lines,
            ]
        )

    def build_topic_alert_publications(
        self,
        *,
        event_type: str,
        impacted_topics: list[str] | None = None,
        conditions: list[str] | None = None,
        context: dict[str, object] | None = None,
    ) -> dict[str, object]:
        orchestrator = getattr(self, "topic_orchestrator_service", None)
        if orchestrator is None:
            orchestrator = TelegramTopicOrchestratorService()
            self.topic_orchestrator_service = orchestrator

        return orchestrator.build_topic_publications(
            event_type=event_type,
            impacted_topics=impacted_topics,
            conditions=conditions,
            context=context,
        )

    def send_topic_messages(
        self,
        *,
        publications: dict[str, object],
        destination_threads: dict[str, int] | None = None,
        allowed_destinations: list[str] | None = None,
        chat_id: str | None = None,
    ) -> dict[str, dict]:
        messages = dict(publications.get("messages") or {})
        thread_map = dict(destination_threads or {})
        allowed = set(allowed_destinations or [])

        results: dict[str, dict] = {}

        for destination, text in messages.items():
            if allowed and destination not in allowed:
                continue

            thread_id = thread_map.get(destination)

            results[destination] = self.send_message(
                text=str(text),
                chat_id=chat_id,
                message_thread_id=thread_id,
            )

        return results

    def publish_topic_alerts(
        self,
        *,
        event_type: str,
        impacted_topics: list[str] | None = None,
        conditions: list[str] | None = None,
        context: dict[str, object] | None = None,
        destination_threads: dict[str, int] | None = None,
        allowed_destinations: list[str] | None = None,
        chat_id: str | None = None,
    ) -> dict[str, object]:
        publications = self.build_topic_alert_publications(
            event_type=event_type,
            impacted_topics=impacted_topics,
            conditions=conditions,
            context=context,
        )

        send_results = self.send_topic_messages(
            publications=publications,
            destination_threads=destination_threads,
            allowed_destinations=allowed_destinations,
            chat_id=chat_id,
        )

        return {
            "event_type": event_type,
            "publications": publications,
            "send_results": send_results,
        }

    def publish_fed_fomc_alert(
        self,
        *,
        impacted_topics: list[str] | None = None,
        conditions: list[str] | None = None,
        context: dict[str, object] | None = None,
        destination_threads: dict[str, int] | None = None,
        allowed_destinations: list[str] | None = None,
        chat_id: str | None = None,
    ) -> dict[str, object]:
        return self.publish_topic_alerts(
            event_type="fed_fomc_very_important",
            impacted_topics=impacted_topics,
            conditions=conditions,
            context=context,
            destination_threads=destination_threads,
            allowed_destinations=allowed_destinations,
            chat_id=chat_id,
        )

    def publish_ndx_topic_alert(
        self,
        *,
        conditions: list[str] | None = None,
        context: dict[str, object] | None = None,
        destination_threads: dict[str, int] | None = None,
        allowed_destinations: list[str] | None = None,
        chat_id: str | None = None,
    ) -> dict[str, object]:
        return self.publish_topic_alerts(
            event_type="single_asset_specific_news",
            impacted_topics=["ndx"],
            conditions=conditions,
            context=context,
            destination_threads=destination_threads,
            allowed_destinations=allowed_destinations,
            chat_id=chat_id,
        )

    def publish_wti_topic_alert(
        self,
        *,
        conditions: list[str] | None = None,
        context: dict[str, object] | None = None,
        destination_threads: dict[str, int] | None = None,
        allowed_destinations: list[str] | None = None,
        chat_id: str | None = None,
    ) -> dict[str, object]:
        return self.publish_topic_alerts(
            event_type="single_asset_specific_news",
            impacted_topics=["wti"],
            conditions=conditions,
            context=context,
            destination_threads=destination_threads,
            allowed_destinations=allowed_destinations,
            chat_id=chat_id,
        )

    def publish_spx_topic_alert(
        self,
        *,
        conditions: list[str] | None = None,
        context: dict[str, object] | None = None,
        destination_threads: dict[str, int] | None = None,
        allowed_destinations: list[str] | None = None,
        chat_id: str | None = None,
    ) -> dict[str, object]:
        return self.publish_topic_alerts(
            event_type="single_asset_specific_news",
            impacted_topics=["spx"],
            conditions=conditions,
            context=context,
            destination_threads=destination_threads,
            allowed_destinations=allowed_destinations,
            chat_id=chat_id,
        )

    def publish_asset_topic_alert(
        self,
        *,
        asset_topic: str,
        conditions: list[str] | None = None,
        context: dict[str, object] | None = None,
        destination_threads: dict[str, int] | None = None,
        allowed_destinations: list[str] | None = None,
        chat_id: str | None = None,
    ) -> dict[str, object]:
        normalized_asset_topic = str(asset_topic).strip().lower()

        if normalized_asset_topic == "ndx":
            return self.publish_ndx_topic_alert(
                conditions=conditions,
                context=context,
                destination_threads=destination_threads,
                allowed_destinations=allowed_destinations,
                chat_id=chat_id,
            )

        if normalized_asset_topic == "spx":
            return self.publish_spx_topic_alert(
                conditions=conditions,
                context=context,
                destination_threads=destination_threads,
                allowed_destinations=allowed_destinations,
                chat_id=chat_id,
            )

        if normalized_asset_topic == "wti":
            return self.publish_wti_topic_alert(
                conditions=conditions,
                context=context,
                destination_threads=destination_threads,
                allowed_destinations=allowed_destinations,
                chat_id=chat_id,
            )

        raise ValueError(f"Unsupported asset_topic '{asset_topic}'.")

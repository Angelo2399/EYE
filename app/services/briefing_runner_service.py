from __future__ import annotations

import inspect
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from app.services.briefing_schedule_service import BriefingScheduleService
from app.services.signal_service import SignalService


class BriefingRunnerService:
    def __init__(
        self,
        *,
        signal_service: SignalService | None = None,
        briefing_schedule_service: BriefingScheduleService | None = None,
        now_provider=None,
    ) -> None:
        self.signal_service = signal_service or SignalService()
        self.briefing_schedule_service = (
            briefing_schedule_service or BriefingScheduleService()
        )
        self.now_provider = now_provider or (lambda: datetime.now(timezone.utc))

    def run_briefing(
        self,
        *,
        timezone_name: str | None = None,
        chat_id: str | None = None,
        current_dt: datetime | None = None,
    ) -> dict[str, object]:
        resolved_timezone = self._resolve_timezone_name(timezone_name)
        resolved_current_dt = current_dt or self.now_provider()

        uses_evaluate_schedule = hasattr(
            self.briefing_schedule_service,
            "evaluate_schedule",
        )

        if uses_evaluate_schedule:
            schedule_result = self.briefing_schedule_service.evaluate_schedule(
                timezone_name=resolved_timezone,
                current_dt=resolved_current_dt,
            )
        else:
            decision = self.briefing_schedule_service.resolve_schedule(
                current_dt=resolved_current_dt,
                timezone_name=resolved_timezone,
            )
            schedule_result = {
                "should_send": decision.should_send,
                "event_type": decision.event_type,
                "scheduled_label": decision.scheduled_label,
                "local_time": decision.local_time,
                "reason": None,
            }

        should_send = bool(schedule_result.get("should_send"))
        event_type = schedule_result.get("event_type")
        scheduled_label = schedule_result.get("scheduled_label")
        local_time = str(schedule_result.get("local_time") or "")
        reason = schedule_result.get("reason")

        if not should_send or not event_type:
            result = {
                "should_send": False,
                "event_type": None,
                "scheduled_label": None,
                "local_time": local_time,
                "send_result": None,
            }
            if uses_evaluate_schedule:
                result["sent"] = False
                result["reason"] = reason
            return result

        send_result = self._send_last_briefing_alert(
            chat_id=chat_id,
            timezone_name=resolved_timezone,
            local_time=local_time,
        )
        sent = isinstance(send_result, dict) and bool(send_result.get("ok"))

        if sent and hasattr(self.briefing_schedule_service, "mark_sent"):
            self.briefing_schedule_service.mark_sent(
                event_type=str(event_type),
                timezone_name=resolved_timezone,
                current_dt=resolved_current_dt,
            )

        result = {
            "should_send": True,
            "event_type": event_type,
            "scheduled_label": scheduled_label,
            "local_time": local_time,
            "send_result": send_result,
        }
        if uses_evaluate_schedule:
            result["sent"] = sent
            result["reason"] = reason
        return result

    def _resolve_timezone_name(self, timezone_name: str | None) -> str:
        candidate = str(timezone_name or "").strip() or "Europe/Madrid"

        try:
            ZoneInfo(candidate)
            return candidate
        except Exception:
            return "Europe/Madrid"

    def _send_last_briefing_alert(
        self,
        *,
        chat_id: str | None,
        timezone_name: str,
        local_time: str,
    ) -> dict[str, object]:
        send_method = self.signal_service.send_last_briefing_alert
        signature = inspect.signature(send_method)
        supported_parameters = signature.parameters

        kwargs: dict[str, object] = {}
        if "chat_id" in supported_parameters:
            kwargs["chat_id"] = chat_id
        if "timezone_name" in supported_parameters:
            kwargs["timezone_name"] = timezone_name
        if "local_time" in supported_parameters:
            kwargs["local_time"] = local_time

        return send_method(**kwargs)


__all__ = ["BriefingRunnerService"]

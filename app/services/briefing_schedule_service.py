from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class BriefingScheduleDecision:
    event_type: str | None
    should_send: bool
    scheduled_label: str | None
    local_time: str


class BriefingScheduleService:
    BRIEFING_EUROPE_LABEL = "briefing_europe"
    BRIEFING_US_OPEN_LABEL = "briefing_us_open"
    LEGACY_BRIEFING_USA_OPEN_LABEL = "briefing_usa_open"

    EUROPE_BRIEFING_LOCAL_TIME = time(8, 30)
    US_CASH_OPEN_ET = time(9, 30)

    SLOT_WINDOW_MINUTES = 10

    def __init__(self) -> None:
        self._sent_slot_keys: set[str] = set()

    def evaluate_schedule(
        self,
        *,
        timezone_name: str = "Europe/Madrid",
        current_dt: datetime | None = None,
    ) -> dict[str, object]:
        local_dt = self._to_local_dt(
            timezone_name=timezone_name,
            current_dt=current_dt,
        )

        if local_dt.weekday() >= 5:
            return {
                "should_send": False,
                "event_type": None,
                "scheduled_label": None,
                "local_time": local_dt.strftime("%H:%M:%S"),
                "reason": "weekend",
                "slot_key": None,
            }

        europe_target = datetime.combine(
            local_dt.date(),
            self.EUROPE_BRIEFING_LOCAL_TIME,
            tzinfo=local_dt.tzinfo,
        )
        europe_decision = self._evaluate_slot(
            local_dt=local_dt,
            timezone_name=timezone_name,
            event_type=self.BRIEFING_EUROPE_LABEL,
            scheduled_label=self.BRIEFING_EUROPE_LABEL,
            target_local_dt=europe_target,
        )
        if europe_decision["should_send"]:
            return europe_decision

        us_open_target = self._build_us_open_local_dt(
            local_dt=local_dt,
            timezone_name=timezone_name,
        )
        us_open_decision = self._evaluate_slot(
            local_dt=local_dt,
            timezone_name=timezone_name,
            event_type=self.BRIEFING_US_OPEN_LABEL,
            scheduled_label=self.BRIEFING_US_OPEN_LABEL,
            target_local_dt=us_open_target,
        )
        if us_open_decision["should_send"]:
            return us_open_decision

        return {
            "should_send": False,
            "event_type": None,
            "scheduled_label": None,
            "local_time": local_dt.strftime("%H:%M:%S"),
            "reason": "no briefing slot due now",
            "slot_key": None,
        }

    def get_due_briefing(
        self,
        *,
        timezone_name: str = "Europe/Madrid",
        current_dt: datetime | None = None,
    ) -> dict[str, object]:
        return self.evaluate_schedule(
            timezone_name=timezone_name,
            current_dt=current_dt,
        )

    def mark_sent(
        self,
        *,
        event_type: str,
        timezone_name: str = "Europe/Madrid",
        current_dt: datetime | None = None,
    ) -> dict[str, object]:
        local_dt = self._to_local_dt(
            timezone_name=timezone_name,
            current_dt=current_dt,
        )

        normalized_event = str(event_type).strip().lower()

        if normalized_event == self.BRIEFING_EUROPE_LABEL:
            target_local_dt = datetime.combine(
                local_dt.date(),
                self.EUROPE_BRIEFING_LOCAL_TIME,
                tzinfo=local_dt.tzinfo,
            )
            scheduled_label = self.BRIEFING_EUROPE_LABEL
        elif normalized_event in {
            self.BRIEFING_US_OPEN_LABEL,
            self.LEGACY_BRIEFING_USA_OPEN_LABEL,
        }:
            target_local_dt = self._build_us_open_local_dt(
                local_dt=local_dt,
                timezone_name=timezone_name,
            )
            normalized_event = self.BRIEFING_US_OPEN_LABEL
            scheduled_label = self.BRIEFING_US_OPEN_LABEL
        else:
            raise ValueError(f"Unsupported briefing event_type '{event_type}'.")

        slot_key = self._build_slot_key(
            timezone_name=timezone_name,
            scheduled_label=scheduled_label,
            target_local_dt=target_local_dt,
        )
        self._sent_slot_keys.add(slot_key)

        return {
            "event_type": normalized_event,
            "scheduled_label": scheduled_label,
            "slot_key": slot_key,
            "marked_at_local_time": local_dt.strftime("%H:%M:%S"),
        }

    def reset_sent_slots(self) -> None:
        self._sent_slot_keys.clear()

    def resolve_schedule(
        self,
        *,
        current_dt: datetime,
        timezone_name: str,
    ) -> BriefingScheduleDecision:
        decision = self.evaluate_schedule(
            timezone_name=timezone_name,
            current_dt=current_dt,
        )
        local_dt = self._to_local_dt(
            timezone_name=timezone_name,
            current_dt=current_dt,
        )

        event_type = decision.get("event_type")
        scheduled_label = None

        if event_type == self.BRIEFING_EUROPE_LABEL:
            scheduled_label = (
                f"{self.BRIEFING_EUROPE_LABEL}_"
                f"{self.EUROPE_BRIEFING_LOCAL_TIME.strftime('%H:%M')}"
            )
        elif event_type == self.BRIEFING_US_OPEN_LABEL:
            event_type = self.LEGACY_BRIEFING_USA_OPEN_LABEL
            us_open_target = self._build_us_open_local_dt(
                local_dt=local_dt,
                timezone_name=timezone_name,
            )
            scheduled_label = (
                f"{self.LEGACY_BRIEFING_USA_OPEN_LABEL}_"
                f"{us_open_target.strftime('%H:%M')}"
            )

        return BriefingScheduleDecision(
            event_type=event_type,
            should_send=bool(decision.get("should_send")),
            scheduled_label=scheduled_label,
            local_time=str(decision.get("local_time") or local_dt.strftime("%H:%M:%S")),
        )

    def _evaluate_slot(
        self,
        *,
        local_dt: datetime,
        timezone_name: str,
        event_type: str,
        scheduled_label: str,
        target_local_dt: datetime,
    ) -> dict[str, object]:
        window_end = target_local_dt + timedelta(minutes=self.SLOT_WINDOW_MINUTES)
        slot_key = self._build_slot_key(
            timezone_name=timezone_name,
            scheduled_label=scheduled_label,
            target_local_dt=target_local_dt,
        )

        if local_dt < target_local_dt:
            return {
                "should_send": False,
                "event_type": None,
                "scheduled_label": None,
                "local_time": local_dt.strftime("%H:%M:%S"),
                "reason": f"{scheduled_label} not reached yet",
                "slot_key": slot_key,
            }

        if local_dt >= window_end:
            return {
                "should_send": False,
                "event_type": None,
                "scheduled_label": None,
                "local_time": local_dt.strftime("%H:%M:%S"),
                "reason": f"{scheduled_label} window expired",
                "slot_key": slot_key,
            }

        if slot_key in self._sent_slot_keys:
            return {
                "should_send": False,
                "event_type": None,
                "scheduled_label": None,
                "local_time": local_dt.strftime("%H:%M:%S"),
                "reason": f"{scheduled_label} already sent for this slot",
                "slot_key": slot_key,
            }

        return {
            "should_send": True,
            "event_type": event_type,
            "scheduled_label": scheduled_label,
            "local_time": local_dt.strftime("%H:%M:%S"),
            "reason": f"{scheduled_label} due now",
            "slot_key": slot_key,
        }

    def _build_us_open_local_dt(
        self,
        *,
        local_dt: datetime,
        timezone_name: str,
    ) -> datetime:
        local_tz = ZoneInfo(timezone_name)
        ny_tz = ZoneInfo("America/New_York")

        ny_now = local_dt.astimezone(ny_tz)
        ny_open_dt = datetime.combine(
            ny_now.date(),
            self.US_CASH_OPEN_ET,
            tzinfo=ny_tz,
        )
        return ny_open_dt.astimezone(local_tz)

    def _build_slot_key(
        self,
        *,
        timezone_name: str,
        scheduled_label: str,
        target_local_dt: datetime,
    ) -> str:
        return (
            f"{timezone_name}|"
            f"{scheduled_label}|"
            f"{target_local_dt.date().isoformat()}|"
            f"{target_local_dt.strftime('%H:%M')}"
        )

    def _to_local_dt(
        self,
        *,
        timezone_name: str,
        current_dt: datetime | None,
    ) -> datetime:
        local_tz = ZoneInfo(timezone_name)

        if current_dt is None:
            return datetime.now(local_tz)

        if current_dt.tzinfo is None:
            current_dt = current_dt.replace(tzinfo=timezone.utc)

        return current_dt.astimezone(local_tz)


__all__ = [
    "BriefingScheduleDecision",
    "BriefingScheduleService",
]

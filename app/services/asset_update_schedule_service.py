from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class AssetUpdateScheduleDecision:
    should_send: bool
    send_type: str | None
    local_time: str


class AssetUpdateScheduleService:
    def resolve_schedule(
        self,
        *,
        current_dt: datetime,
        timezone_name: str,
        last_sent_at: str | int | datetime | None = None,
        state_change_kind: str | None = None,
    ) -> AssetUpdateScheduleDecision:
        local_tz = ZoneInfo(timezone_name)
        local_dt = self._normalize_local_dt(current_dt=current_dt, local_tz=local_tz)
        last_local_dt = self._normalize_last_sent_at(
            last_sent_at=last_sent_at,
            local_tz=local_tz,
        )

        if self._is_same_local_hour(
            current_dt=local_dt,
            last_sent_at=last_local_dt,
        ):
            return AssetUpdateScheduleDecision(
                should_send=False,
                send_type=None,
                local_time=local_dt.strftime("%H:%M:%S"),
            )

        normalized_state_change_kind = str(state_change_kind or "").strip().lower()
        send_type = (
            "immediate_alert"
            if normalized_state_change_kind == "alert"
            else "hourly_update"
        )

        return AssetUpdateScheduleDecision(
            should_send=True,
            send_type=send_type,
            local_time=local_dt.strftime("%H:%M:%S"),
        )

    def _normalize_local_dt(
        self,
        *,
        current_dt: datetime,
        local_tz: ZoneInfo,
    ) -> datetime:
        if current_dt.tzinfo is None:
            return current_dt.replace(tzinfo=local_tz)

        return current_dt.astimezone(local_tz)

    def _normalize_last_sent_at(
        self,
        *,
        last_sent_at: str | int | datetime | None,
        local_tz: ZoneInfo,
    ) -> datetime | None:
        if last_sent_at is None:
            return None

        if isinstance(last_sent_at, datetime):
            if last_sent_at.tzinfo is None:
                return last_sent_at.replace(tzinfo=local_tz)

            return last_sent_at.astimezone(local_tz)

        if isinstance(last_sent_at, int):
            return datetime.fromtimestamp(
                float(last_sent_at),
                tz=timezone.utc,
            ).astimezone(local_tz)

        candidate = str(last_sent_at).strip()
        if not candidate:
            return None

        if candidate.isdigit():
            return datetime.fromtimestamp(
                float(candidate),
                tz=timezone.utc,
            ).astimezone(local_tz)

        try:
            parsed_dt = datetime.fromisoformat(candidate)
        except ValueError:
            return None

        if parsed_dt.tzinfo is None:
            return parsed_dt.replace(tzinfo=local_tz)

        return parsed_dt.astimezone(local_tz)

    def _is_same_local_hour(
        self,
        *,
        current_dt: datetime,
        last_sent_at: datetime | None,
    ) -> bool:
        if last_sent_at is None:
            return False

        return (
            current_dt.year == last_sent_at.year
            and current_dt.month == last_sent_at.month
            and current_dt.day == last_sent_at.day
            and current_dt.hour == last_sent_at.hour
        )


__all__ = [
    "AssetUpdateScheduleDecision",
    "AssetUpdateScheduleService",
]

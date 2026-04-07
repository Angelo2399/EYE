from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Header, status
from pydantic import BaseModel

from app.api import routes_signals
from app.services.briefing_runner_service import BriefingRunnerService
from app.services.telegram_alert_service import TelegramAlertService


router = APIRouter(prefix="/briefings", tags=["briefings"])

_briefing_runner_service = BriefingRunnerService()


class RunBriefingsRequest(BaseModel):
    force_event_type: Literal["briefing_europe", "briefing_usa_open"] | None = None


def _run_briefing_runner(*, timezone_name: str | None) -> dict[str, object]:
    runner = _briefing_runner_service

    if hasattr(runner, "run"):
        return runner.run(timezone_name=timezone_name)

    if hasattr(runner, "run_once"):
        return runner.run_once(timezone_name=timezone_name)

    if hasattr(runner, "execute"):
        return runner.execute(timezone_name=timezone_name)

    if hasattr(runner, "run_briefing"):
        return runner.run_briefing(timezone_name=timezone_name)

    raise RuntimeError("BriefingRunnerService has no supported runner method.")


def _build_local_time(timezone_name: str | None) -> str:
    resolved_timezone_name = str(timezone_name or "").strip() or "UTC"

    try:
        zone = ZoneInfo(resolved_timezone_name)
    except Exception:
        zone = timezone.utc

    return datetime.now(zone).strftime("%H:%M:%S")


def _force_run_briefing_runner(
    *,
    force_event_type: str,
    timezone_name: str | None,
) -> dict[str, object]:
    payload = routes_signals.last_briefing_state
    if not payload:
        raise ValueError("No briefing payload available.")

    telegram_service = TelegramAlertService()
    send_result = telegram_service.send_briefing_payload(
        briefing_payload=payload,
    )
    return {
        "should_send": True,
        "event_type": force_event_type,
        "scheduled_label": force_event_type,
        "local_time": payload.get("local_time") or _build_local_time(timezone_name),
        "send_result": send_result,
    }


@router.post(
    "/run",
    status_code=status.HTTP_200_OK,
    summary="Run briefing runner once",
)
def run_briefings(
    payload: RunBriefingsRequest | None = None,
    x_eye_timezone: str | None = Header(default=None),
) -> dict[str, object]:
    if payload is not None and payload.force_event_type:
        try:
            result = _force_run_briefing_runner(
                force_event_type=payload.force_event_type,
                timezone_name=x_eye_timezone,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(exc),
            ) from exc
    else:
        result = _run_briefing_runner(timezone_name=x_eye_timezone)

    return {
        "should_send": bool(result.get("should_send", False)),
        "event_type": result.get("event_type"),
        "scheduled_label": result.get("scheduled_label"),
        "local_time": result.get("local_time"),
        "sent": bool(result.get("send_result")),
    }

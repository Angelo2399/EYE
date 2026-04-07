from __future__ import annotations

from app.services.briefing_runner_service import BriefingRunnerService
from app.services.briefing_schedule_service import BriefingScheduleDecision


class FakeSignalService:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def send_last_briefing_alert(self, *, chat_id: str | None = None) -> dict:
        self.calls.append({"chat_id": chat_id})
        return {"ok": True, "message_id": 1}


class FakeBriefingScheduleService:
    def __init__(self, decision: BriefingScheduleDecision) -> None:
        self.decision = decision
        self.calls: list[dict[str, object]] = []

    def resolve_schedule(self, *, current_dt, timezone_name: str):
        self.calls.append(
            {
                "current_dt": current_dt,
                "timezone_name": timezone_name,
            }
        )
        return self.decision


def test_run_briefing_sends_last_briefing_when_europe_window_is_open() -> None:
    signal_service = FakeSignalService()
    schedule_service = FakeBriefingScheduleService(
        BriefingScheduleDecision(
            event_type="briefing_europe",
            should_send=True,
            scheduled_label="briefing_europe_08:30",
            local_time="08:32:00",
        )
    )
    service = BriefingRunnerService(
        signal_service=signal_service,
        briefing_schedule_service=schedule_service,
    )

    result = service.run_briefing(
        timezone_name="Europe/Madrid",
        chat_id="123",
        current_dt="sentinel",
    )

    assert schedule_service.calls == [
        {
            "current_dt": "sentinel",
            "timezone_name": "Europe/Madrid",
        }
    ]
    assert signal_service.calls == [{"chat_id": "123"}]
    assert result == {
        "event_type": "briefing_europe",
        "should_send": True,
        "scheduled_label": "briefing_europe_08:30",
        "local_time": "08:32:00",
        "send_result": {"ok": True, "message_id": 1},
    }


def test_run_briefing_sends_last_briefing_when_us_open_window_is_open() -> None:
    signal_service = FakeSignalService()
    schedule_service = FakeBriefingScheduleService(
        BriefingScheduleDecision(
            event_type="briefing_usa_open",
            should_send=True,
            scheduled_label="briefing_usa_open_14:30",
            local_time="14:31:00",
        )
    )
    service = BriefingRunnerService(
        signal_service=signal_service,
        briefing_schedule_service=schedule_service,
    )

    result = service.run_briefing(
        timezone_name="Europe/London",
        current_dt="sentinel",
    )

    assert schedule_service.calls == [
        {
            "current_dt": "sentinel",
            "timezone_name": "Europe/London",
        }
    ]
    assert signal_service.calls == [{"chat_id": None}]
    assert result == {
        "event_type": "briefing_usa_open",
        "should_send": True,
        "scheduled_label": "briefing_usa_open_14:30",
        "local_time": "14:31:00",
        "send_result": {"ok": True, "message_id": 1},
    }


def test_run_briefing_skips_send_when_schedule_is_closed() -> None:
    signal_service = FakeSignalService()
    schedule_service = FakeBriefingScheduleService(
        BriefingScheduleDecision(
            event_type=None,
            should_send=False,
            scheduled_label=None,
            local_time="11:00:00",
        )
    )
    service = BriefingRunnerService(
        signal_service=signal_service,
        briefing_schedule_service=schedule_service,
    )

    result = service.run_briefing(
        timezone_name="Europe/Madrid",
        current_dt="sentinel",
    )

    assert signal_service.calls == []
    assert result == {
        "event_type": None,
        "should_send": False,
        "scheduled_label": None,
        "local_time": "11:00:00",
        "send_result": None,
    }

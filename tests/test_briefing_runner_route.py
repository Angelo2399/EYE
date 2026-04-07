from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_run_briefings_route_returns_runner_schedule_payload(monkeypatch) -> None:
    from app.api import routes_briefings

    observed: dict[str, object] = {}

    def fake_run_briefing(*, timezone_name=None):
        observed["timezone_name"] = timezone_name
        return {
            "should_send": False,
            "event_type": None,
            "scheduled_label": None,
            "local_time": "19:02:52",
            "send_result": None,
        }

    monkeypatch.setattr(
        routes_briefings._briefing_runner_service,
        "run_briefing",
        fake_run_briefing,
    )

    client = TestClient(app)

    response = client.post(
        "/api/v1/briefings/run",
        json={},
        headers={"X-EYE-Timezone": "Europe/Madrid"},
    )

    assert response.status_code == 200
    assert observed == {"timezone_name": "Europe/Madrid"}
    assert response.json() == {
        "should_send": False,
        "event_type": None,
        "scheduled_label": None,
        "local_time": "19:02:52",
        "sent": False,
    }


def test_run_briefings_route_forces_send_when_force_event_type_is_provided(
    monkeypatch,
) -> None:
    from app.api import routes_briefings, routes_signals

    observed: dict[str, object] = {}

    def fake_send_briefing_payload(
        self,
        *,
        briefing_payload,
        **kwargs,
    ) -> dict[str, object]:
        observed["send_called"] = True
        observed["briefing_payload"] = dict(briefing_payload)
        return {"ok": True, "result": {"message_id": 7}}

    def fake_run_briefing(*, timezone_name=None):
        raise AssertionError("run_briefing must not be called when forcing send.")

    monkeypatch.setattr(
        routes_signals,
        "last_briefing_state",
        {
            "title": "Nasdaq 100 intraday briefing",
            "summary": "Test briefing summary.",
            "timezone_name": "Europe/Madrid",
            "local_time": "08:30:00",
        },
    )
    monkeypatch.setattr(
        routes_briefings.TelegramAlertService,
        "send_briefing_payload",
        fake_send_briefing_payload,
    )
    monkeypatch.setattr(
        routes_briefings._briefing_runner_service,
        "run_briefing",
        fake_run_briefing,
    )
    monkeypatch.setattr(
        routes_briefings,
        "_build_local_time",
        lambda timezone_name: "08:30:00",
    )

    client = TestClient(app)

    response = client.post(
        "/api/v1/briefings/run",
        json={"force_event_type": "briefing_europe"},
        headers={"X-EYE-Timezone": "Europe/Madrid"},
    )

    assert response.status_code == 200
    assert observed == {
        "send_called": True,
        "briefing_payload": {
            "title": "Nasdaq 100 intraday briefing",
            "summary": "Test briefing summary.",
            "timezone_name": "Europe/Madrid",
            "local_time": "08:30:00",
        },
    }
    assert response.json() == {
        "should_send": True,
        "event_type": "briefing_europe",
        "scheduled_label": "briefing_europe",
        "local_time": "08:30:00",
        "sent": True,
    }

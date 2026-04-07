from __future__ import annotations

from fastapi.testclient import TestClient

from app.api import routes_signals
from app.main import app


client = TestClient(app)


def test_get_last_briefing_route_returns_404_when_missing() -> None:
    previous_state = routes_signals.last_briefing_state

    try:
        routes_signals.last_briefing_state = None

        response = client.get("/api/v1/signals/briefing/last")

        assert response.status_code == 404
        assert response.json() == {
            "detail": "No briefing payload available yet."
        }
    finally:
        routes_signals.last_briefing_state = previous_state


def test_get_last_briefing_route_returns_last_saved_payload() -> None:
    previous_state = routes_signals.last_briefing_state

    try:
        routes_signals.last_briefing_state = {
            "title": "Nasdaq 100 intraday briefing",
            "asset": "Nasdaq 100",
            "symbol": "NDX",
            "timeframe": "1h",
            "timezone_name": "Europe/London",
            "local_date": "2026-04-01",
            "local_time": "08:15:00",
            "session_phase": "pre_open",
            "market_bias": "long",
        }

        response = client.get("/api/v1/signals/briefing/last")

        assert response.status_code == 200
        assert response.json() == {
            "title": "Nasdaq 100 intraday briefing",
            "asset": "Nasdaq 100",
            "symbol": "NDX",
            "timeframe": "1h",
            "timezone_name": "Europe/London",
            "local_date": "2026-04-01",
            "local_time": "08:15:00",
            "session_phase": "pre_open",
            "market_bias": "long",
        }
    finally:
        routes_signals.last_briefing_state = previous_state

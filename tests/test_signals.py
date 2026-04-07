from fastapi.testclient import TestClient

from app.main import app
from app.schemas.signal import HoldingWindow, ModelConfidence, SignalAction, SignalResponse

client = TestClient(app)


def test_generate_signal_returns_signal_payload(monkeypatch) -> None:
    def fake_generate_signal(
        _payload,
        timezone_name=None,
    ) -> SignalResponse:
        return SignalResponse(
            asset="Nasdaq 100",
            action=SignalAction.short,
            entry_min=24092.66,
            entry_max=24216.34,
            entry_window="15:35-16:10",
            expected_holding=HoldingWindow.h1,
            hard_exit_time="21:55",
            close_by_session_end=True,
            stop_loss=24884.68,
            take_profit_1=23059.24,
            take_profit_2=22329.06,
            risk_reward=1.5,
            favorable_move_pct=72.8,
            tp1_hit_pct=66.8,
            stop_hit_first_pct=28.8,
            model_confidence_pct=79.0,
            confidence_label=ModelConfidence.high,
            explanation=(
                "Nasdaq 100: SHORT. Regime bearish. Score=100.0, "
                "fav move=72.8%, tp1=66.8%, stop first=28.8%, "
                "confidence=high. Entry 24092.66-24216.34, stop 24884.68, "
                "tp1 23059.24, R/R 1.50."
            ),
        )

    monkeypatch.setattr("app.api.routes_signals.generate_signal", fake_generate_signal)

    response = client.post(
        "/api/v1/signals/generate",
        json={"symbol": "NDX", "timeframe": "1d"},
    )

    assert response.status_code == 200
    payload = response.json()

    assert payload["asset"] == "Nasdaq 100"
    assert payload["action"] == "short"
    assert payload["entry_min"] == 24092.66
    assert payload["entry_max"] == 24216.34
    assert payload["entry_window"] == "15:35-16:10"
    assert payload["expected_holding"] == "1h"
    assert payload["hard_exit_time"] == "21:55"
    assert payload["close_by_session_end"] is True
    assert payload["stop_loss"] == 24884.68
    assert payload["take_profit_1"] == 23059.24
    assert payload["take_profit_2"] == 22329.06
    assert payload["risk_reward"] == 1.5
    assert payload["favorable_move_pct"] == 72.8
    assert payload["tp1_hit_pct"] == 66.8
    assert payload["stop_hit_first_pct"] == 28.8
    assert payload["model_confidence_pct"] == 79.0
    assert payload["confidence_label"] == "high"
    assert "Nasdaq 100: SHORT." in payload["explanation"]


def test_generate_signal_passes_timezone_header_to_service(monkeypatch) -> None:
    observed: dict[str, str | None] = {}

    def fake_generate_signal(
        _payload,
        timezone_name=None,
    ) -> SignalResponse:
        observed["timezone_name"] = timezone_name
        return SignalResponse(
            asset="Nasdaq 100",
            action=SignalAction.short,
            entry_min=24092.66,
            entry_max=24216.34,
            entry_window="15:35-16:10",
            expected_holding=HoldingWindow.h1,
            hard_exit_time="21:55",
            close_by_session_end=True,
            stop_loss=24884.68,
            take_profit_1=23059.24,
            take_profit_2=22329.06,
            risk_reward=1.5,
            favorable_move_pct=72.8,
            tp1_hit_pct=66.8,
            stop_hit_first_pct=28.8,
            model_confidence_pct=79.0,
            confidence_label=ModelConfidence.high,
            explanation="timezone pass-through",
        )

    monkeypatch.setattr("app.api.routes_signals.generate_signal", fake_generate_signal)

    response = client.post(
        "/api/v1/signals/generate",
        json={"symbol": "NDX", "timeframe": "1d"},
        headers={"X-EYE-Timezone": "Europe/London"},
    )

    assert response.status_code == 200
    assert observed["timezone_name"] == "Europe/London"


def test_generate_signal_returns_422_for_invalid_symbol_schema() -> None:
    response = client.post(
        "/api/v1/signals/generate",
        json={"symbol": "DJI", "timeframe": "1d"},
    )

    assert response.status_code == 422
    payload = response.json()

    assert payload["detail"]


def test_list_recent_signals_returns_rows(monkeypatch) -> None:
    def fake_list_recent_signals(limit: int) -> list[dict[str, object]]:
        assert limit == 5
        return [
            {
                "id": 1,
                "created_at_utc": "2026-03-25T12:00:00+00:00",
                "symbol": "NDX",
                "timeframe": "1d",
                "asset": "Nasdaq 100",
                "action": "short",
                "entry_min": 24092.66,
                "entry_max": 24216.34,
                "entry_window": "15:35-16:10",
                "expected_holding": "1h",
                "hard_exit_time": "21:55",
                "close_by_session_end": True,
                "stop_loss": 24884.68,
                "take_profit_1": 23059.24,
                "take_profit_2": 22329.06,
                "risk_reward": 1.5,
                "favorable_move_pct": 72.8,
                "tp1_hit_pct": 66.8,
                "stop_hit_first_pct": 28.8,
                "model_confidence_pct": 79.0,
                "confidence_label": "high",
                "explanation": "Nasdaq 100: SHORT.",
            }
        ]

    monkeypatch.setattr(
        "app.api.routes_signals.signal_repository.list_recent_signals",
        fake_list_recent_signals,
    )

    response = client.get("/api/v1/signals/recent?limit=5")

    assert response.status_code == 200
    payload = response.json()

    assert len(payload) == 1
    assert payload[0]["id"] == 1
    assert payload[0]["symbol"] == "NDX"
    assert payload[0]["timeframe"] == "1d"
    assert payload[0]["asset"] == "Nasdaq 100"
    assert payload[0]["action"] == "short"


def test_list_recent_signals_rejects_invalid_limit() -> None:
    response = client.get("/api/v1/signals/recent?limit=0")

    assert response.status_code == 422

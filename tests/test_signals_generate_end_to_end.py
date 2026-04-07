from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app
from app.schemas.day_context import DayBias, DayContextLabel
from app.schemas.signal import HoldingWindow, ModelConfidence, SignalAction, SignalResponse


def test_generate_signal_endpoint_returns_expected_intraday_payload(monkeypatch) -> None:
    from app.api import routes_signals

    def fake_generate_signal(payload, timezone_name=None):
        return SignalResponse(
            asset="Nasdaq 100",
            action=SignalAction.wait,
            entry_min=23100.0,
            entry_max=23120.0,
            entry_window="15:35-16:10",
            expected_holding=HoldingWindow.h1,
            hard_exit_time="21:55",
            close_by_session_end=True,
            stop_loss=23070.0,
            take_profit_1=23160.0,
            take_profit_2=23200.0,
            risk_reward=1.8,
            favorable_move_pct=58.0,
            tp1_hit_pct=44.0,
            stop_hit_first_pct=28.0,
            model_confidence_pct=61.0,
            confidence_label=ModelConfidence.medium,
            day_context_label=DayContextLabel.unclear,
            day_context_bias=DayBias.neutral,
            day_context_confidence_pct=45.0,
            explanation="Nasdaq 100: WAIT. Setup intraday non abbastanza pulito.",
        )

    monkeypatch.setattr(routes_signals, "generate_signal", fake_generate_signal)

    client = TestClient(app)

    response = client.post(
        "/api/v1/signals/generate",
        json={
            "symbol": "NDX",
            "timeframe": "1h",
        },
    )

    assert response.status_code == 200

    data = response.json()

    assert data["asset"] == "Nasdaq 100"
    assert data["action"] == "wait"
    assert data["entry_min"] == 23100.0
    assert data["entry_max"] == 23120.0
    assert data["entry_window"] == "15:35-16:10"
    assert data["expected_holding"] == "1h"
    assert data["hard_exit_time"] == "21:55"
    assert data["close_by_session_end"] is True
    assert data["stop_loss"] == 23070.0
    assert data["take_profit_1"] == 23160.0
    assert data["take_profit_2"] == 23200.0
    assert data["risk_reward"] == 1.8
    assert data["favorable_move_pct"] == 58.0
    assert data["tp1_hit_pct"] == 44.0
    assert data["stop_hit_first_pct"] == 28.0
    assert data["model_confidence_pct"] == 61.0
    assert data["confidence_label"] == "medium"
    assert data["day_context_label"] == "unclear"
    assert data["day_context_bias"] == "neutral"
    assert data["day_context_confidence_pct"] == 45.0
    assert "WAIT" in data["explanation"]


def test_generate_signal_endpoint_rejects_invalid_symbol() -> None:
    client = TestClient(app)

    response = client.post(
        "/api/v1/signals/generate",
        json={
            "symbol": "INVALID",
            "timeframe": "1h",
        },
    )

    assert response.status_code == 422

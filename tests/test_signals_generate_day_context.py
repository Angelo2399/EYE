from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app
from app.schemas.day_context import DayBias, DayContextLabel
from app.schemas.signal import (
    HoldingWindow,
    ModelConfidence,
    SignalAction,
    SignalResponse,
)


def test_generate_signal_route_returns_structured_day_context_fields(monkeypatch) -> None:
    expected_response = SignalResponse(
        asset="Nasdaq 100",
        action=SignalAction.long,
        entry_min=100.0,
        entry_max=101.0,
        entry_window="15:35-16:10",
        expected_holding=HoldingWindow.half_day,
        hard_exit_time="21:55",
        close_by_session_end=True,
        stop_loss=99.0,
        take_profit_1=102.0,
        take_profit_2=103.0,
        risk_reward=1.5,
        favorable_move_pct=54.0,
        tp1_hit_pct=42.0,
        stop_hit_first_pct=34.0,
        model_confidence_pct=48.0,
        confidence_label=ModelConfidence.medium,
        day_context_label=DayContextLabel.trend_up,
        day_context_bias=DayBias.long,
        day_context_confidence_pct=72.0,
        explanation="Structured day context in API response.",
    )

    def _fake_generate_signal(payload, timezone_name=None):
        return expected_response

    monkeypatch.setattr(
        "app.api.routes_signals.generate_signal",
        _fake_generate_signal,
    )

    client = TestClient(app)
    response = client.post(
        "/api/v1/signals/generate",
        json={
            "symbol": "NDX",
            "timeframe": "1h",
        },
    )

    assert response.status_code == 200

    body = response.json()

    assert body["action"] == "long"
    assert body["day_context_label"] == "trend_up"
    assert body["day_context_bias"] == "long"
    assert body["day_context_confidence_pct"] == 72.0
    assert body["explanation"] == "Structured day context in API response."

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_list_recent_signals_includes_day_context_fields(monkeypatch) -> None:
    def fake_list_recent_signals(limit: int) -> list[dict[str, object]]:
        assert limit == 5
        return [
            {
                "id": 1,
                "created_at_utc": "2026-03-27T12:00:00+00:00",
                "symbol": "NDX",
                "timeframe": "1h",
                "asset": "Nasdaq 100",
                "action": "long",
                "entry_min": 100.0,
                "entry_max": 101.0,
                "entry_window": "15:35-16:10",
                "expected_holding": "half_day",
                "hard_exit_time": "21:55",
                "close_by_session_end": True,
                "stop_loss": 99.0,
                "take_profit_1": 102.0,
                "take_profit_2": 103.0,
                "risk_reward": 1.5,
                "favorable_move_pct": 54.0,
                "tp1_hit_pct": 42.0,
                "stop_hit_first_pct": 34.0,
                "model_confidence_pct": 48.0,
                "confidence_label": "medium",
                "day_context_label": "trend_up",
                "day_context_bias": "long",
                "day_context_confidence_pct": 72.0,
                "explanation": "Explanation with day context.",
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
    assert payload[0]["day_context_label"] == "trend_up"
    assert payload[0]["day_context_bias"] == "long"
    assert payload[0]["day_context_confidence_pct"] == 72.0

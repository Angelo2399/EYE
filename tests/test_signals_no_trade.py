from fastapi.testclient import TestClient

from app.main import app
from app.schemas.signal import ModelConfidence, SignalAction, SignalResponse

client = TestClient(app)


def test_generate_signal_returns_no_trade_payload(monkeypatch) -> None:
    def fake_generate_signal(
        _payload,
        timezone_name=None,
    ) -> SignalResponse:
        return SignalResponse(
            asset="Nasdaq 100",
            action=SignalAction.no_trade,
            entry_min=None,
            entry_max=None,
            entry_window=None,
            expected_holding=None,
            hard_exit_time=None,
            close_by_session_end=True,
            stop_loss=None,
            take_profit_1=None,
            take_profit_2=None,
            risk_reward=None,
            favorable_move_pct=46.0,
            tp1_hit_pct=24.0,
            stop_hit_first_pct=30.0,
            model_confidence_pct=31.2,
            confidence_label=ModelConfidence.low,
            explanation=(
                "Nasdaq 100: WAIT. Regime sideways. "
                "Setup intraday non abbastanza pulito. "
                "Score=40.0, fav move=46.0%, confidence=low. "
                "Nessuna posizione overnight."
            ),
        )

    monkeypatch.setattr("app.api.routes_signals.generate_signal", fake_generate_signal)

    response = client.post(
        "/api/v1/signals/generate",
        json={"symbol": "NDX", "timeframe": "1h"},
    )

    assert response.status_code == 200
    payload = response.json()

    assert payload["asset"] == "Nasdaq 100"
    assert payload["action"] == "no_trade"
    assert payload["entry_min"] is None
    assert payload["entry_max"] is None
    assert payload["entry_window"] is None
    assert payload["expected_holding"] is None
    assert payload["hard_exit_time"] is None
    assert payload["close_by_session_end"] is True
    assert payload["stop_loss"] is None
    assert payload["take_profit_1"] is None
    assert payload["take_profit_2"] is None
    assert payload["risk_reward"] is None
    assert payload["favorable_move_pct"] == 46.0
    assert payload["tp1_hit_pct"] == 24.0
    assert payload["stop_hit_first_pct"] == 30.0
    assert payload["model_confidence_pct"] == 31.2
    assert payload["confidence_label"] == "low"
    assert "Nessuna posizione overnight." in payload["explanation"]

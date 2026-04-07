import pandas as pd
import pytest

from app.schemas.signal import HoldingWindow, SignalAction
from app.services.risk_service import RiskService


def _build_market_features_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=10, freq="D", tz="UTC"),
            "high": [101.0, 102.0, 103.0, 104.0, 103.0, 102.0, 101.0, 104.0, 105.0, 104.0],
            "low": [95.0, 94.0, 93.0, 94.0, 95.0, 96.0, 94.0, 93.0, 92.0, 93.0],
            "close": [98.0, 99.0, 100.0, 99.0, 100.0, 101.0, 100.0, 99.0, 100.0, 100.0],
            "atr_14": [4.0] * 10,
            "volatility_20": [0.01] * 10,
        }
    )


def test_build_risk_plan_returns_long_levels() -> None:
    service = RiskService()
    features = _build_market_features_frame()

    result = service.build_risk_plan(features, SignalAction.long)

    assert result.entry_min == 99.52
    assert result.entry_max == 100.2
    assert result.entry_window == "15:35-16:10"
    assert result.expected_holding == HoldingWindow.h1
    assert result.hard_exit_time == "21:55"
    assert result.close_by_session_end is True
    assert result.stop_loss == 92.0
    assert result.take_profit_1 == 106.93
    assert result.take_profit_2 == 110.86
    assert result.risk_reward == 0.9


def test_build_risk_plan_returns_short_levels() -> None:
    service = RiskService()
    features = _build_market_features_frame()

    result = service.build_risk_plan(features, SignalAction.short)

    assert result.entry_min == 99.8
    assert result.entry_max == 100.48
    assert result.entry_window == "15:35-16:10"
    assert result.expected_holding == HoldingWindow.h1
    assert result.hard_exit_time == "21:55"
    assert result.close_by_session_end is True
    assert result.stop_loss == 105.0
    assert result.take_profit_1 == 95.77
    assert result.take_profit_2 == 93.34
    assert result.risk_reward == 0.9


def test_build_risk_plan_returns_empty_plan_for_wait() -> None:
    service = RiskService()
    features = _build_market_features_frame()

    result = service.build_risk_plan(features, SignalAction.wait)

    assert result.entry_min is None
    assert result.entry_max is None
    assert result.entry_window is None
    assert result.expected_holding is None
    assert result.hard_exit_time is None
    assert result.close_by_session_end is True
    assert result.stop_loss is None
    assert result.take_profit_1 is None
    assert result.take_profit_2 is None
    assert result.risk_reward is None


def test_build_risk_plan_rejects_missing_columns() -> None:
    service = RiskService()
    features = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=3, freq="D", tz="UTC"),
            "close": [100.0, 101.0, 102.0],
        }
    )

    with pytest.raises(ValueError, match="Missing required risk columns"):
        service.build_risk_plan(features, SignalAction.long)


def test_build_risk_plan_rejects_invalid_atr() -> None:
    service = RiskService()
    features = _build_market_features_frame()
    features.loc[features.index[-1], "atr_14"] = 0.0

    with pytest.raises(ValueError, match="atr_14 must be available and greater than 0"):
        service.build_risk_plan(features, SignalAction.long)

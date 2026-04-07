import pandas as pd
import pytest

from app.schemas.risk import RiskPlan
from app.schemas.scoring import SetupScore
from app.schemas.signal import HoldingWindow, ModelConfidence, SignalAction
from app.services.probability_service import ProbabilityService


def _build_features_frame(regime: str = "bullish", volatility: float = 0.01) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=5, freq="h", tz="UTC"),
            "close": [100.0, 101.0, 102.0, 103.0, 104.0],
            "atr_14": [2.0, 2.0, 2.0, 2.0, 2.0],
            "volatility_20": [volatility, volatility, volatility, volatility, volatility],
            "regime": [regime] * 5,
        }
    )


def test_estimate_probabilities_for_strong_long_setup() -> None:
    service = ProbabilityService()
    features = _build_features_frame(regime="bullish", volatility=0.01)

    setup_score = SetupScore(
        action=SignalAction.long,
        direction="long",
        score=100.0,
        long_score=100.0,
        short_score=0.0,
        trend_score=25.0,
        moving_average_score=20.0,
        rsi_score=15.0,
        price_position_score=20.0,
        regime_score=20.0,
        explanation="test",
    )

    risk_plan = RiskPlan(
        entry_min=103.0,
        entry_max=104.0,
        entry_window="15:35-16:10",
        expected_holding=HoldingWindow.h1,
        hard_exit_time="21:55",
        close_by_session_end=True,
        stop_loss=100.0,
        take_profit_1=108.5,
        take_profit_2=111.5,
        risk_reward=1.5,
    )

    result = service.estimate_probabilities(features, setup_score, risk_plan)

    assert result.favorable_move_pct == 59.6
    assert result.tp1_hit_pct == 54.6
    assert result.stop_hit_first_pct == 34.0
    assert result.model_confidence_pct == 67.2
    assert result.confidence_label == ModelConfidence.medium_high


def test_estimate_probabilities_for_wait_setup() -> None:
    service = ProbabilityService()
    features = _build_features_frame(regime="sideways", volatility=0.01)

    setup_score = SetupScore(
        action=SignalAction.wait,
        direction="neutral",
        score=40.0,
        long_score=40.0,
        short_score=33.0,
        trend_score=0.0,
        moving_average_score=0.0,
        rsi_score=0.0,
        price_position_score=0.0,
        regime_score=0.0,
        explanation="test",
    )

    risk_plan = RiskPlan(
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
    )

    result = service.estimate_probabilities(features, setup_score, risk_plan)

    assert result.favorable_move_pct == 46.0
    assert result.tp1_hit_pct == 24.0
    assert result.stop_hit_first_pct == 30.0
    assert result.model_confidence_pct == 31.2
    assert result.confidence_label == ModelConfidence.low


def test_estimate_probabilities_for_high_vol_short_setup() -> None:
    service = ProbabilityService()
    features = _build_features_frame(regime="bearish_high_vol", volatility=0.01)

    setup_score = SetupScore(
        action=SignalAction.short,
        direction="short",
        score=85.0,
        long_score=5.0,
        short_score=85.0,
        trend_score=25.0,
        moving_average_score=20.0,
        rsi_score=15.0,
        price_position_score=13.0,
        regime_score=12.0,
        explanation="test",
    )

    risk_plan = RiskPlan(
        entry_min=99.0,
        entry_max=100.0,
        entry_window="15:35-16:10",
        expected_holding=HoldingWindow.m30,
        hard_exit_time="21:55",
        close_by_session_end=True,
        stop_loss=103.0,
        take_profit_1=94.0,
        take_profit_2=91.0,
        risk_reward=1.5,
    )

    result = service.estimate_probabilities(features, setup_score, risk_plan)

    assert result.favorable_move_pct == 50.3
    assert result.tp1_hit_pct == 43.3
    assert result.stop_hit_first_pct == 42.7
    assert result.model_confidence_pct == 56.1
    assert result.confidence_label == ModelConfidence.medium_high


def test_estimate_probabilities_rejects_missing_columns() -> None:
    service = ProbabilityService()
    features = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=3, freq="h", tz="UTC"),
            "close": [100.0, 101.0, 102.0],
        }
    )

    setup_score = SetupScore(
        action=SignalAction.wait,
        direction="neutral",
        score=40.0,
        long_score=40.0,
        short_score=33.0,
        trend_score=0.0,
        moving_average_score=0.0,
        rsi_score=0.0,
        price_position_score=0.0,
        regime_score=0.0,
        explanation="test",
    )

    risk_plan = RiskPlan(
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
    )

    with pytest.raises(ValueError, match="Missing required probability columns"):
        service.estimate_probabilities(features, setup_score, risk_plan)

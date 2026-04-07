import pandas as pd
import pytest

from app.schemas.scoring import SetupScore
from app.schemas.signal import SignalAction
from app.services.scoring_service import ScoringService


def test_score_setup_returns_long_when_bullish_alignment_is_strong() -> None:
    service = ScoringService()

    features = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=3, freq="h", tz="UTC"),
            "close": [110.0, 111.0, 112.0],
            "sma_20": [108.0, 109.0, 110.0],
            "sma_50": [106.0, 107.0, 108.0],
            "ema_20": [109.5, 110.2, 111.0],
            "rsi_14": [55.0, 57.0, 58.0],
            "distance_sma20": [0.010, 0.012, (112.0 / 110.0) - 1.0],
            "distance_sma50": [0.020, 0.025, (112.0 / 108.0) - 1.0],
            "regime": ["bullish", "bullish", "bullish"],
            "volatility_20": [0.010, 0.010, 0.010],
            "atr_14": [2.0, 2.0, 2.0],
        }
    )

    result = service.score_setup(features)

    assert isinstance(result, SetupScore)
    assert result.action == SignalAction.long
    assert result.direction == "long"
    assert result.score == 86.0
    assert result.long_score == 86.0
    assert result.short_score < 0.0
    assert "LONG intraday setup selected" in result.explanation


def test_score_setup_returns_short_when_bearish_alignment_is_strong() -> None:
    service = ScoringService()

    features = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=3, freq="h", tz="UTC"),
            "close": [100.0, 99.0, 98.0],
            "sma_20": [102.0, 101.0, 100.0],
            "sma_50": [104.0, 103.0, 102.0],
            "ema_20": [101.4, 100.3, 99.2],
            "rsi_14": [46.0, 44.0, 42.0],
            "distance_sma20": [-0.010, -0.012, (98.0 / 100.0) - 1.0],
            "distance_sma50": [-0.020, -0.025, (98.0 / 102.0) - 1.0],
            "regime": ["bearish", "bearish", "bearish"],
            "volatility_20": [0.010, 0.010, 0.010],
            "atr_14": [2.0, 2.0, 2.0],
        }
    )

    result = service.score_setup(features)

    assert result.action == SignalAction.short
    assert result.direction == "short"
    assert result.score == 86.0
    assert result.short_score == 86.0
    assert result.long_score < 0.0
    assert "SHORT intraday setup selected" in result.explanation


def test_score_setup_returns_wait_when_market_is_sideways() -> None:
    service = ScoringService()

    features = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=3, freq="h", tz="UTC"),
            "close": [100.0, 100.0, 100.0],
            "sma_20": [100.0, 100.0, 100.0],
            "sma_50": [100.0, 100.0, 100.0],
            "ema_20": [100.0, 100.0, 100.0],
            "rsi_14": [50.0, 50.0, 50.0],
            "distance_sma20": [0.0, 0.0, 0.0],
            "distance_sma50": [0.0, 0.0, 0.0],
            "regime": ["sideways", "sideways", "sideways"],
            "volatility_20": [0.009, 0.009, 0.009],
            "atr_14": [2.0, 2.0, 2.0],
        }
    )

    result = service.score_setup(features)

    assert result.action == SignalAction.wait
    assert result.direction == "neutral"
    assert result.score <= 10.0
    assert "No-trade bias for now" in result.explanation


def test_score_setup_returns_wait_when_setup_is_not_strong_enough() -> None:
    service = ScoringService()

    features = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=3, freq="h", tz="UTC"),
            "close": [100.0, 100.2, 100.4],
            "sma_20": [99.8, 99.9, 100.0],
            "sma_50": [99.9, 99.95, 100.0],
            "ema_20": [100.0, 100.1, 100.2],
            "rsi_14": [50.0, 50.5, 51.0],
            "distance_sma20": [0.002, 0.002, 0.004],
            "distance_sma50": [0.001, 0.002, 0.004],
            "regime": ["bullish_high_vol", "bullish_high_vol", "bullish_high_vol"],
            "volatility_20": [0.022, 0.022, 0.022],
            "atr_14": [2.0, 2.0, 2.0],
        }
    )

    result = service.score_setup(features)

    assert result.action == SignalAction.wait
    assert result.direction == "neutral"
    assert result.score < 70.0
    assert "intraday setup not selective enough" in result.explanation


def test_score_setup_rejects_missing_columns() -> None:
    service = ScoringService()

    features = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=3, freq="h", tz="UTC"),
            "close": [100.0, 101.0, 102.0],
        }
    )

    with pytest.raises(ValueError, match="Missing required scoring columns"):
        service.score_setup(features)

import pandas as pd
import pytest

from app.services.regime_service import RegimeService


def test_classify_regime_returns_bullish() -> None:
    service = RegimeService()

    features = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=80, freq="D", tz="UTC"),
            "close": [120.0] * 80,
            "sma_20": [110.0] * 80,
            "sma_50": [100.0] * 80,
            "atr_14": [2.0] * 80,
            "volatility_20": [0.01] * 80,
        }
    )

    result = service.classify_regime(features)

    assert result.iloc[-1]["trend_state"] == "bullish"
    assert result.iloc[-1]["regime"] == "bullish"
    assert bool(result.iloc[-1]["is_high_vol"]) is False


def test_classify_regime_returns_bearish_high_vol() -> None:
    service = RegimeService()

    volatility_values = [0.01] * 79 + [0.03]
    atr_values = [2.0] * 79 + [5.0]

    features = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=80, freq="D", tz="UTC"),
            "close": [90.0] * 80,
            "sma_20": [100.0] * 80,
            "sma_50": [110.0] * 80,
            "atr_14": atr_values,
            "volatility_20": volatility_values,
        }
    )

    result = service.classify_regime(features)

    assert result.iloc[-1]["trend_state"] == "bearish"
    assert bool(result.iloc[-1]["is_high_vol"]) is True
    assert result.iloc[-1]["regime"] == "bearish_high_vol"


def test_classify_regime_returns_sideways_when_not_aligned() -> None:
    service = RegimeService()

    features = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=80, freq="D", tz="UTC"),
            "close": [100.0] * 80,
            "sma_20": [105.0] * 80,
            "sma_50": [95.0] * 80,
            "atr_14": [2.0] * 80,
            "volatility_20": [0.01] * 80,
        }
    )

    result = service.classify_regime(features)

    assert result.iloc[-1]["trend_state"] == "sideways"
    assert result.iloc[-1]["regime"] == "sideways"


def test_get_current_regime_returns_last_regime_label() -> None:
    service = RegimeService()

    features = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=80, freq="D", tz="UTC"),
            "close": [120.0] * 80,
            "sma_20": [110.0] * 80,
            "sma_50": [100.0] * 80,
            "atr_14": [2.0] * 80,
            "volatility_20": [0.01] * 80,
        }
    )

    result = service.get_current_regime(features)

    assert result == "bullish"


def test_classify_regime_rejects_missing_columns() -> None:
    service = RegimeService()

    features = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=5, freq="D", tz="UTC"),
            "close": [100.0] * 5,
        }
    )

    with pytest.raises(ValueError, match="Missing required feature columns"):
        service.classify_regime(features)

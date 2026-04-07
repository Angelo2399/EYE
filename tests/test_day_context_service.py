from __future__ import annotations

import math

from app.schemas.day_context import DayBias, DayContextLabel
from app.services.day_context_service import DayContextService


def test_classify_day_context_returns_trend_up_for_clean_bullish_structure() -> None:
    service = DayContextService()

    context = service.classify_day_context(
        {
            "regime": "bullish",
            "volatility_20": 0.011,
            "rsi_14": 58.0,
            "distance_sma20": 0.012,
        }
    )

    assert context.label == DayContextLabel.trend_up
    assert context.bias == DayBias.long
    assert context.prefer_breakout is True
    assert context.avoid_mean_reversion is True


def test_classify_day_context_returns_trend_down_for_clean_bearish_structure() -> None:
    service = DayContextService()

    context = service.classify_day_context(
        {
            "regime": "bearish",
            "volatility_20": 0.011,
            "rsi_14": 41.0,
            "distance_sma20": -0.015,
        }
    )

    assert context.label == DayContextLabel.trend_down
    assert context.bias == DayBias.short
    assert context.prefer_breakout is True
    assert context.avoid_mean_reversion is True


def test_classify_day_context_returns_range_day_for_sideways_regime() -> None:
    service = DayContextService()

    context = service.classify_day_context(
        {
            "regime": "sideways",
            "volatility_20": 0.010,
            "rsi_14": 50.0,
            "distance_sma20": 0.001,
        }
    )

    assert context.label == DayContextLabel.range_day
    assert context.bias == DayBias.neutral
    assert context.prefer_breakout is False
    assert context.avoid_mean_reversion is False


def test_classify_day_context_returns_volatile_day_for_high_volatility_regime() -> None:
    service = DayContextService()

    context = service.classify_day_context(
        {
            "regime": "bullish_high_vol",
            "volatility_20": 0.020,
            "rsi_14": 56.0,
            "distance_sma20": 0.010,
        }
    )

    assert context.label == DayContextLabel.volatile_day
    assert context.bias == DayBias.neutral
    assert context.prefer_breakout is False
    assert context.avoid_mean_reversion is True


def test_classify_day_context_returns_unclear_when_inputs_are_mixed() -> None:
    service = DayContextService()

    context = service.classify_day_context(
        {
            "regime": "bullish",
            "volatility_20": 0.011,
            "rsi_14": 51.0,
            "distance_sma20": -0.004,
        }
    )

    assert context.label == DayContextLabel.unclear
    assert context.bias == DayBias.neutral
    assert context.prefer_breakout is False
    assert context.avoid_mean_reversion is False


def test_classify_day_context_handles_missing_or_nan_values_safely() -> None:
    service = DayContextService()

    context = service.classify_day_context(
        {
            "regime": "bearish",
            "volatility_20": math.nan,
            "rsi_14": math.nan,
        }
    )

    assert context.label == DayContextLabel.unclear
    assert context.bias == DayBias.neutral
    assert context.confidence_pct == 45.0

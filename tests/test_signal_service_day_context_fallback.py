from __future__ import annotations

from types import SimpleNamespace

from app.schemas.day_context import DayBias, DayContextLabel
from app.services.signal_service import SignalService


def _build_service() -> SignalService:
    return object.__new__(SignalService)


def test_get_day_context_returns_default_when_service_attribute_is_missing() -> None:
    service = _build_service()
    latest_row = {"regime": "bullish", "volatility_20": 0.01, "rsi_14": 52.0}

    context = service._get_day_context(latest_row)

    assert context.label == DayContextLabel.unclear
    assert context.bias == DayBias.neutral
    assert context.confidence_pct == 45.0
    assert context.prefer_breakout is False
    assert context.avoid_mean_reversion is False
    assert context.explanation == "Day context unavailable."


def test_get_day_context_returns_default_when_service_has_no_classifier() -> None:
    service = _build_service()
    service.day_context_service = SimpleNamespace()
    latest_row = {"regime": "bullish", "volatility_20": 0.01, "rsi_14": 52.0}

    context = service._get_day_context(latest_row)

    assert context.label == DayContextLabel.unclear
    assert context.bias == DayBias.neutral
    assert context.confidence_pct == 45.0
    assert context.prefer_breakout is False
    assert context.avoid_mean_reversion is False
    assert context.explanation == "Day context unavailable."


def test_get_day_context_returns_default_when_classifier_raises_exception() -> None:
    service = _build_service()

    def _raise_error(latest_row):
        raise RuntimeError("boom")

    service.day_context_service = SimpleNamespace(
        classify_day_context=_raise_error
    )
    latest_row = {"regime": "bearish", "volatility_20": 0.01, "rsi_14": 48.0}

    context = service._get_day_context(latest_row)

    assert context.label == DayContextLabel.unclear
    assert context.bias == DayBias.neutral
    assert context.confidence_pct == 45.0
    assert context.prefer_breakout is False
    assert context.avoid_mean_reversion is False
    assert context.explanation == "Day context unavailable."


def test_get_day_context_returns_real_context_when_service_is_available() -> None:
    service = _build_service()

    expected_context = service._build_default_day_context().model_copy(
        update={
            "label": DayContextLabel.trend_up,
            "bias": DayBias.long,
            "confidence_pct": 72.0,
            "prefer_breakout": True,
            "avoid_mean_reversion": True,
            "explanation": "Trend-up day context.",
        }
    )

    service.day_context_service = SimpleNamespace(
        classify_day_context=lambda latest_row: expected_context
    )
    latest_row = {"regime": "bullish", "volatility_20": 0.01, "rsi_14": 58.0}

    context = service._get_day_context(latest_row)

    assert context == expected_context

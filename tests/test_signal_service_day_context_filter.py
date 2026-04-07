from __future__ import annotations

from app.schemas.day_context import DayBias, DayContext, DayContextLabel
from app.schemas.scoring import SetupScore
from app.schemas.signal import SignalAction
from app.services.signal_service import SignalService


def _build_service() -> SignalService:
    return object.__new__(SignalService)


def _build_setup_score(action: SignalAction, score: float = 60.0) -> SetupScore:
    direction = "long" if action == SignalAction.long else "short" if action == SignalAction.short else "neutral"

    return SetupScore(
        action=action,
        direction=direction,
        score=score,
        long_score=62.0,
        short_score=31.0,
        trend_score=14.0,
        moving_average_score=13.0,
        rsi_score=12.0,
        price_position_score=11.0,
        regime_score=10.0,
        explanation="Original setup explanation.",
    )


def _build_day_context(
    label: DayContextLabel,
    bias: DayBias,
    confidence_pct: float,
) -> DayContext:
    return DayContext(
        label=label,
        bias=bias,
        confidence_pct=confidence_pct,
        prefer_breakout=False,
        avoid_mean_reversion=False,
        explanation="Test day context.",
    )


def test_apply_day_context_filter_keeps_wait_unchanged() -> None:
    service = _build_service()
    setup_score = _build_setup_score(action=SignalAction.wait, score=45.0)
    day_context = _build_day_context(
        label=DayContextLabel.trend_up,
        bias=DayBias.long,
        confidence_pct=80.0,
    )

    result = service._apply_day_context_filter(setup_score, day_context)

    assert result.action == SignalAction.wait
    assert result.direction == setup_score.direction
    assert result.explanation == setup_score.explanation


def test_apply_day_context_filter_keeps_no_trade_unchanged() -> None:
    service = _build_service()
    setup_score = _build_setup_score(action=SignalAction.no_trade, score=40.0)
    day_context = _build_day_context(
        label=DayContextLabel.trend_down,
        bias=DayBias.short,
        confidence_pct=80.0,
    )

    result = service._apply_day_context_filter(setup_score, day_context)

    assert result.action == SignalAction.no_trade
    assert result.direction == setup_score.direction
    assert result.explanation == setup_score.explanation


def test_apply_day_context_filter_returns_no_trade_on_volatile_day() -> None:
    service = _build_service()
    setup_score = _build_setup_score(action=SignalAction.long)
    day_context = _build_day_context(
        label=DayContextLabel.volatile_day,
        bias=DayBias.neutral,
        confidence_pct=35.0,
    )

    result = service._apply_day_context_filter(setup_score, day_context)

    assert result.action == SignalAction.no_trade
    assert result.direction == "neutral"
    assert "unstable volatility" in result.explanation


def test_apply_day_context_filter_turns_long_into_wait_when_day_bias_is_strongly_short() -> None:
    service = _build_service()
    setup_score = _build_setup_score(action=SignalAction.long)
    day_context = _build_day_context(
        label=DayContextLabel.trend_down,
        bias=DayBias.short,
        confidence_pct=72.0,
    )

    result = service._apply_day_context_filter(setup_score, day_context)

    assert result.action == SignalAction.wait
    assert result.direction == "neutral"
    assert "strongly short-biased" in result.explanation


def test_apply_day_context_filter_turns_short_into_wait_when_day_bias_is_strongly_long() -> None:
    service = _build_service()
    setup_score = _build_setup_score(action=SignalAction.short)
    day_context = _build_day_context(
        label=DayContextLabel.trend_up,
        bias=DayBias.long,
        confidence_pct=72.0,
    )

    result = service._apply_day_context_filter(setup_score, day_context)

    assert result.action == SignalAction.wait
    assert result.direction == "neutral"
    assert "strongly long-biased" in result.explanation


def test_apply_day_context_filter_keeps_long_when_short_bias_confidence_is_below_threshold() -> None:
    service = _build_service()
    setup_score = _build_setup_score(action=SignalAction.long)
    day_context = _build_day_context(
        label=DayContextLabel.trend_down,
        bias=DayBias.short,
        confidence_pct=69.9,
    )

    result = service._apply_day_context_filter(setup_score, day_context)

    assert result.action == SignalAction.long
    assert result.direction == setup_score.direction
    assert result.explanation == setup_score.explanation


def test_apply_day_context_filter_keeps_short_when_long_bias_confidence_is_below_threshold() -> None:
    service = _build_service()
    setup_score = _build_setup_score(action=SignalAction.short)
    day_context = _build_day_context(
        label=DayContextLabel.trend_up,
        bias=DayBias.long,
        confidence_pct=69.9,
    )

    result = service._apply_day_context_filter(setup_score, day_context)

    assert result.action == SignalAction.short
    assert result.direction == setup_score.direction
    assert result.explanation == setup_score.explanation

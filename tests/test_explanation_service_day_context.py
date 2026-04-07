from __future__ import annotations

from types import SimpleNamespace

from app.schemas.day_context import DayBias, DayContext, DayContextLabel
from app.schemas.signal import HoldingWindow, ModelConfidence, SignalAction
from app.services.explanation_service import ExplanationService


def _build_wait_setup_score():
    return SimpleNamespace(
        action=SignalAction.wait,
        score=45.0,
    )


def _build_long_setup_score():
    return SimpleNamespace(
        action=SignalAction.long,
        score=61.5,
    )


def _build_risk_plan():
    return SimpleNamespace(
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
    )


def _build_probability_estimate():
    return SimpleNamespace(
        favorable_move_pct=54.0,
        tp1_hit_pct=42.0,
        stop_hit_first_pct=34.0,
        confidence_label=ModelConfidence.medium,
    )


def _build_day_context() -> DayContext:
    return DayContext(
        label=DayContextLabel.trend_up,
        bias=DayBias.long,
        confidence_pct=72.0,
        prefer_breakout=True,
        avoid_mean_reversion=True,
        explanation="Trend-up day context.",
    )


def test_build_explanation_includes_day_context_for_wait() -> None:
    service = ExplanationService()

    explanation = service.build_explanation(
        asset="Nasdaq 100",
        regime="bullish",
        setup_score=_build_wait_setup_score(),
        risk_plan=_build_risk_plan(),
        probability_estimate=_build_probability_estimate(),
        day_context=_build_day_context(),
    )

    assert "WAIT" in explanation
    assert "Day context trend_up, bias=long, ctx_conf=72.0%." in explanation
    assert "Nessuna posizione overnight." in explanation


def test_build_explanation_includes_day_context_for_trade_action() -> None:
    service = ExplanationService()

    explanation = service.build_explanation(
        asset="Nasdaq 100",
        regime="bullish",
        setup_score=_build_long_setup_score(),
        risk_plan=_build_risk_plan(),
        probability_estimate=_build_probability_estimate(),
        day_context=_build_day_context(),
    )

    assert "LONG intraday." in explanation
    assert "Day context trend_up, bias=long, ctx_conf=72.0%." in explanation
    assert "Entry window 15:35-16:10" in explanation
    assert "close by session end=yes" in explanation


def test_build_explanation_remains_compatible_without_day_context() -> None:
    service = ExplanationService()

    explanation = service.build_explanation(
        asset="Nasdaq 100",
        regime="bullish",
        setup_score=_build_long_setup_score(),
        risk_plan=_build_risk_plan(),
        probability_estimate=_build_probability_estimate(),
        day_context=None,
    )

    assert "LONG intraday." in explanation
    assert "Day context" not in explanation

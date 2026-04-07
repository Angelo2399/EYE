from __future__ import annotations

from app.schemas.market_intelligence import MarketBias, MarketIntelligenceSnapshot
from app.schemas.probability import ProbabilityEstimate
from app.schemas.risk import RiskPlan
from app.schemas.scoring import SetupScore
from app.schemas.signal import HoldingWindow, ModelConfidence, SignalAction
from app.services.explanation_service import ExplanationService


def test_build_explanation_includes_external_intelligence_block_for_wti() -> None:
    service = ExplanationService()

    setup_score = SetupScore(
        action=SignalAction.long,
        direction="long",
        score=63.0,
        long_score=63.0,
        short_score=34.0,
        trend_score=12.0,
        moving_average_score=13.0,
        rsi_score=11.0,
        price_position_score=14.0,
        regime_score=13.0,
        explanation="Base setup score.",
    )

    risk_plan = RiskPlan(
        entry_min=100.0,
        entry_max=101.0,
        entry_window="15:35-16:10",
        expected_holding=HoldingWindow.h1,
        hard_exit_time="21:55",
        close_by_session_end=True,
        stop_loss=99.4,
        take_profit_1=101.6,
        take_profit_2=102.2,
        risk_reward=1.8,
    )

    probability_estimate = ProbabilityEstimate(
        favorable_move_pct=58.0,
        tp1_hit_pct=44.0,
        stop_hit_first_pct=28.0,
        model_confidence_pct=61.0,
        confidence_label=ModelConfidence.medium,
    )

    intelligence_snapshot = MarketIntelligenceSnapshot(
        asset="WTI Crude Oil",
        symbol="WTI",
        timeframe="1h",
        generated_at_utc="2026-03-31T12:00:00+00:00",
        market_bias=MarketBias.short,
        bias_confidence_pct=72.0,
        session_phase="open",
        regime="bullish",
        volatility_20=0.011,
        rsi_14=57.0,
        distance_sma20=0.002,
        distance_sma50=0.004,
        key_levels=[],
        dominant_drivers=["opec", "eia", "inventory"],
        risk_flags=[],
        items=[],
        synthesis="Oil intelligence snapshot.",
    )

    explanation = service.build_explanation(
        asset="WTI Crude Oil",
        regime="bullish",
        setup_score=setup_score,
        risk_plan=risk_plan,
        probability_estimate=probability_estimate,
        intelligence_snapshot=intelligence_snapshot,
    )

    assert "WTI Crude Oil: LONG intraday." in explanation
    assert "External intelligence bias=short, ext_conf=72.0%, drivers=opec, eia, inventory," in explanation
    assert "synthesis=Oil intelligence snapshot." in explanation
    assert "Entry 100.00-101.00" in explanation

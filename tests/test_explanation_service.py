from app.schemas.probability import ProbabilityEstimate
from app.schemas.risk import RiskPlan
from app.schemas.scoring import SetupScore
from app.schemas.signal import HoldingWindow, ModelConfidence, SignalAction
from app.services.explanation_service import ExplanationService


def test_build_explanation_for_active_short_setup() -> None:
    service = ExplanationService()

    setup_score = SetupScore(
        action=SignalAction.short,
        direction="short",
        score=100.0,
        long_score=0.0,
        short_score=100.0,
        trend_score=25.0,
        moving_average_score=20.0,
        rsi_score=15.0,
        price_position_score=20.0,
        regime_score=20.0,
        explanation="test",
    )

    risk_plan = RiskPlan(
        entry_min=24088.43,
        entry_max=24212.11,
        entry_window="15:35-16:10",
        expected_holding=HoldingWindow.h1,
        hard_exit_time="21:55",
        close_by_session_end=True,
        stop_loss=24884.68,
        take_profit_1=23048.66,
        take_profit_2=22311.16,
        risk_reward=1.5,
    )

    probability_estimate = ProbabilityEstimate(
        favorable_move_pct=72.8,
        tp1_hit_pct=66.8,
        stop_hit_first_pct=28.8,
        model_confidence_pct=79.0,
        confidence_label=ModelConfidence.high,
    )

    result = service.build_explanation(
        asset="Nasdaq 100",
        regime="bearish",
        setup_score=setup_score,
        risk_plan=risk_plan,
        probability_estimate=probability_estimate,
    )

    assert "Nasdaq 100: SHORT intraday." in result
    assert "Regime bearish." in result
    assert "Entry window 15:35-16:10" in result
    assert "holding 1h" in result
    assert "hard exit 21:55" in result
    assert "close by session end=yes" in result
    assert "Score=100.0" in result
    assert "fav move=72.8%" in result
    assert "tp1=66.8%" in result
    assert "stop first=28.8%" in result
    assert "confidence=high" in result
    assert "Entry 24088.43-24212.11" in result
    assert "stop 24884.68" in result
    assert "tp1 23048.66" in result
    assert "tp2 22311.16" in result
    assert "R/R 1.50" in result


def test_build_explanation_for_wait_setup() -> None:
    service = ExplanationService()

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

    probability_estimate = ProbabilityEstimate(
        favorable_move_pct=50.0,
        tp1_hit_pct=28.0,
        stop_hit_first_pct=26.0,
        model_confidence_pct=38.0,
        confidence_label=ModelConfidence.low,
    )

    result = service.build_explanation(
        asset="S&P 500",
        regime="sideways",
        setup_score=setup_score,
        risk_plan=risk_plan,
        probability_estimate=probability_estimate,
    )

    assert "S&P 500: WAIT." in result
    assert "Regime sideways." in result
    assert "Setup intraday non abbastanza pulito." in result
    assert "Score=40.0" in result
    assert "fav move=50.0%" in result
    assert "confidence=low" in result
    assert "Nessuna posizione overnight." in result


def test_format_external_intelligence_prefers_real_source_and_headline_driver() -> None:
    from app.schemas.market_intelligence import (
        IntelligenceDirection,
        IntelligenceImportance,
        IntelligenceSourceType,
        MarketBias,
        MarketEventType,
        MarketIntelligenceItem,
        MarketIntelligenceSnapshot,
    )
    from app.services.explanation_service import ExplanationService

    service = ExplanationService()

    snapshot = MarketIntelligenceSnapshot(
        asset="WTI Crude Oil",
        symbol="WTI",
        timeframe="1h",
        generated_at_utc="2026-05-01T18:30:00+00:00",
        market_bias=MarketBias.short,
        bias_confidence_pct=71.0,
        dominant_drivers=["macro", "central_bank", "commodity"],
        items=[
            MarketIntelligenceItem(
                source=IntelligenceSourceType.news,
                event_type=MarketEventType.headline,
                importance=IntelligenceImportance.high,
                direction=IntelligenceDirection.bearish,
                title="EIA reports larger-than-expected crude build",
                summary="Oil market reacts to bearish inventory surprise.",
                source_name="U.S. Energy Information Administration",
                relevance_score=88.0,
                confidence_pct=82.0,
            )
        ],
        synthesis="WTI Crude Oil: intelligence bias=short.",
    )

    text = service._format_external_intelligence(snapshot)

    assert (
        "drivers=U.S. Energy Information Administration: "
        "EIA reports larger-than-expected crude build"
    ) in text
    assert "drivers=macro, central_bank, commodity" not in text

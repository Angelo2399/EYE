from __future__ import annotations

from types import SimpleNamespace

from app.schemas.day_context import DayBias, DayContext, DayContextLabel
from app.schemas.market_intelligence import MarketBias, MarketIntelligenceSnapshot
from app.services.briefing_builder_service import BriefingBuilderService


def _build_day_context() -> DayContext:
    return DayContext(
        label=DayContextLabel.trend_up,
        bias=DayBias.long,
        confidence_pct=72.0,
        prefer_breakout=True,
        avoid_mean_reversion=True,
        explanation="Trend-up day context.",
    )


def _build_intelligence_snapshot() -> MarketIntelligenceSnapshot:
    return MarketIntelligenceSnapshot(
        asset="Nasdaq 100",
        symbol="NDX",
        timeframe="1h",
        generated_at_utc="2026-04-01T07:15:00+00:00",
        market_bias=MarketBias.long,
        bias_confidence_pct=67.0,
        session_phase="pre_open",
        regime="bullish",
        volatility_20=0.011,
        rsi_14=58.0,
        distance_sma20=0.003,
        distance_sma50=0.006,
        key_levels=[23000.0, 23100.0],
        dominant_drivers=["fed", "macro"],
        risk_flags=["event_risk"],
        items=[],
        synthesis="External intelligence snapshot.",
    )


def test_build_briefing_returns_structured_timezone_aware_payload() -> None:
    service = BriefingBuilderService()

    briefing = service.build_briefing(
        asset="Nasdaq 100",
        symbol="NDX",
        timeframe="1h",
        briefing_context={
            "timezone_name": "Europe/London",
            "local_date": "2026-04-01",
            "local_time": "08:15:00",
            "local_weekday": 2,
            "is_local_morning_window": True,
            "us_cash_open_local_time": "14:30",
            "minutes_to_us_cash_open": 375,
            "market_closures": [
                {
                    "asset": "NDX",
                    "market_status": "closed_holiday",
                    "official_reason": "U.S. cash equities closed for Good Friday.",
                    "next_open_local": "2026-04-06T14:30:00+01:00",
                },
                {
                    "asset": "WTI",
                    "market_status": "reduced_hours",
                    "official_reason": "NYMEX WTI daily maintenance break 17:00-18:00 ET.",
                    "next_open_local": "2026-04-01T23:00:00+01:00",
                },
            ],
        },
        session_context=SimpleNamespace(phase="pre_open"),
        day_context=_build_day_context(),
        intelligence_snapshot=_build_intelligence_snapshot(),
    )

    assert briefing["title"] == "Nasdaq 100 intraday briefing"
    assert briefing["timezone_name"] == "Europe/London"
    assert briefing["local_time"] == "08:15:00"
    assert briefing["is_local_morning_window"] is True
    assert briefing["session_phase"] == "pre_open"
    assert briefing["day_context_label"] == "trend_up"
    assert briefing["day_context_bias"] == "long"
    assert briefing["market_bias"] == "long"
    assert briefing["bias_confidence_pct"] == 67.0
    assert briefing["us_cash_open_local_time"] == "14:30"
    assert briefing["minutes_to_us_cash_open"] == 375
    assert briefing["dominant_drivers"] == ["fed", "macro"]
    assert briefing["risk_flags"] == ["event_risk"]
    assert briefing["market_closures"] == [
        {
            "asset": "NDX",
            "market_status": "closed_holiday",
            "official_reason": "U.S. cash equities closed for Good Friday.",
            "next_open_local": "2026-04-06T14:30:00+01:00",
        },
        {
            "asset": "WTI",
            "market_status": "reduced_hours",
            "official_reason": "NYMEX WTI daily maintenance break 17:00-18:00 ET.",
            "next_open_local": "2026-04-01T23:00:00+01:00",
        },
    ]
    assert "Nasdaq 100 briefing" in briefing["summary"]
    assert "Europe/London" in briefing["summary"]
    assert "market_bias=long (67.0%)" in briefing["summary"]
    assert "Closed / limited markets today" in briefing["summary"]
    assert "NDX: closed_holiday" in briefing["summary"]
    assert "Good Friday" in briefing["summary"]
    assert "WTI: reduced_hours" in briefing["summary"]
    assert "Next open: 2026-04-06T14:30:00+01:00" in briefing["summary"]

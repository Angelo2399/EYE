from __future__ import annotations

from app.schemas.market_intelligence import (
    IntelligenceDirection,
    IntelligenceImportance,
    IntelligenceSourceType,
    MarketBias,
    MarketEventType,
    MarketIntelligenceItem,
)
from app.services.market_intelligence_service import MarketIntelligenceService


def _build_item(
    *,
    title: str = "Market event",
    summary: str = "Useful intelligence.",
    source: IntelligenceSourceType = IntelligenceSourceType.news,
    event_type: MarketEventType = MarketEventType.headline,
    importance: IntelligenceImportance = IntelligenceImportance.medium,
    direction: IntelligenceDirection = IntelligenceDirection.neutral,
    source_name: str = "Reuters",
    relevance_score: float = 50.0,
    confidence_pct: float = 50.0,
    tags: list[str] | None = None,
    raw_text: str | None = None,
) -> MarketIntelligenceItem:
    return MarketIntelligenceItem(
        source=source,
        event_type=event_type,
        importance=importance,
        direction=direction,
        title=title,
        summary=summary,
        source_name=source_name,
        relevance_score=relevance_score,
        confidence_pct=confidence_pct,
        tags=tags or [],
        raw_text=raw_text,
    )


def test_summarize_ingestion_discards_empty_and_low_value_items() -> None:
    service = MarketIntelligenceService()

    items = [
        _build_item(
            title="Bullish headline",
            direction=IntelligenceDirection.bullish,
            importance=IntelligenceImportance.high,
            relevance_score=78.0,
            confidence_pct=80.0,
        ),
        _build_item(
            title="",
            summary="",
            raw_text=None,
            importance=IntelligenceImportance.medium,
            relevance_score=60.0,
        ),
        _build_item(
            title="Tiny noise",
            summary="Low-value noise.",
            importance=IntelligenceImportance.low,
            relevance_score=10.0,
            confidence_pct=20.0,
        ),
        _build_item(
            title="Bearish macro shock",
            direction=IntelligenceDirection.bearish,
            importance=IntelligenceImportance.critical,
            relevance_score=90.0,
            confidence_pct=70.0,
        ),
    ]

    result = service.summarize_ingestion(items)

    assert result.accepted_items == 2
    assert result.discarded_items == 2
    assert result.critical_items == 1
    assert result.latest_direction == IntelligenceDirection.bearish
    assert result.average_confidence_pct == 75.0
    assert "Accepted 2 items" in result.summary


def test_build_snapshot_returns_no_trade_when_regime_is_sideways() -> None:
    service = MarketIntelligenceService()

    snapshot = service.build_snapshot(
        asset="Nasdaq 100",
        symbol="NDX",
        timeframe="1h",
        items=[
            _build_item(
                title="Bullish attempt",
                direction=IntelligenceDirection.bullish,
                importance=IntelligenceImportance.high,
                relevance_score=75.0,
                confidence_pct=80.0,
                tags=["momentum"],
            )
        ],
        session_phase="midday",
        regime="sideways",
        volatility_20=0.011,
        rsi_14=50.0,
        distance_sma20=0.001,
        distance_sma50=0.002,
        key_levels=[23000.0, 23100.0],
    )

    assert snapshot.market_bias == MarketBias.no_trade
    assert snapshot.bias_confidence_pct == 72.0
    assert "sideways_regime" in snapshot.risk_flags
    assert "Nasdaq 100: intelligence bias=no_trade" in snapshot.synthesis


def test_build_snapshot_returns_no_trade_when_high_volatility_regime_is_active() -> None:
    service = MarketIntelligenceService()

    snapshot = service.build_snapshot(
        asset="S&P 500",
        symbol="SPX",
        timeframe="1h",
        items=[
            _build_item(
                title="Macro shock",
                direction=IntelligenceDirection.bearish,
                importance=IntelligenceImportance.critical,
                relevance_score=92.0,
                confidence_pct=83.0,
                tags=["macro", "volatility"],
                source_name="Fed",
                event_type=MarketEventType.macro,
            )
        ],
        session_phase="open",
        regime="bearish_high_vol",
        volatility_20=0.025,
    )

    assert snapshot.market_bias == MarketBias.no_trade
    assert snapshot.bias_confidence_pct == 78.0
    assert "high_vol_regime" in snapshot.risk_flags
    assert "elevated_volatility" in snapshot.risk_flags
    assert "critical_event_flow" in snapshot.risk_flags


def test_build_snapshot_derives_long_bias_from_bullish_flow() -> None:
    service = MarketIntelligenceService()

    snapshot = service.build_snapshot(
        asset="Nasdaq 100",
        symbol="NDX",
        timeframe="1h",
        items=[
            _build_item(
                title="Strong breakout",
                direction=IntelligenceDirection.bullish,
                importance=IntelligenceImportance.high,
                relevance_score=84.0,
                confidence_pct=82.0,
                tags=["breakout", "momentum"],
                source_name="Reuters",
                event_type=MarketEventType.headline,
            ),
            _build_item(
                title="Support holds",
                direction=IntelligenceDirection.bullish,
                importance=IntelligenceImportance.medium,
                relevance_score=72.0,
                confidence_pct=76.0,
                tags=["support", "momentum"],
                source_name="Reuters",
                event_type=MarketEventType.price_action,
            ),
            _build_item(
                title="Minor neutral note",
                direction=IntelligenceDirection.neutral,
                importance=IntelligenceImportance.low,
                relevance_score=35.0,
                confidence_pct=40.0,
                tags=["flow"],
                source_name="Desk",
                event_type=MarketEventType.internal_signal,
            ),
        ],
        session_phase="open",
        regime="bullish",
        volatility_20=0.011,
    )

    assert snapshot.market_bias == MarketBias.long
    assert snapshot.bias_confidence_pct >= 45.0
    assert "momentum" in snapshot.dominant_drivers
    assert "Reuters".lower() in snapshot.dominant_drivers or "headline" in snapshot.dominant_drivers


def test_build_snapshot_adds_conflicting_signals_when_multiple_mixed_items_exist() -> None:
    service = MarketIntelligenceService()

    snapshot = service.build_snapshot(
        asset="Nasdaq 100",
        symbol="NDX",
        timeframe="1h",
        items=[
            _build_item(
                title="Mixed speech segment 1",
                direction=IntelligenceDirection.mixed,
                importance=IntelligenceImportance.high,
                relevance_score=80.0,
                confidence_pct=74.0,
                source=IntelligenceSourceType.speech,
                event_type=MarketEventType.speech,
                source_name="Fed",
                tags=["powell", "rates"],
            ),
            _build_item(
                title="Mixed speech segment 2",
                direction=IntelligenceDirection.mixed,
                importance=IntelligenceImportance.high,
                relevance_score=79.0,
                confidence_pct=70.0,
                source=IntelligenceSourceType.transcript,
                event_type=MarketEventType.transcript_segment,
                source_name="Fed",
                tags=["powell", "inflation"],
            ),
        ],
        session_phase="power_hour",
        regime="bullish",
        volatility_20=0.012,
    )

    assert "conflicting_signals" in snapshot.risk_flags
    assert "late_session" in snapshot.risk_flags
    assert "fed" in snapshot.dominant_drivers


def test_build_snapshot_marks_session_not_tradeable_when_market_is_closed() -> None:
    service = MarketIntelligenceService()

    snapshot = service.build_snapshot(
        asset="S&P 500",
        symbol="SPX",
        timeframe="1h",
        items=[
            _build_item(
                title="After-hours note",
                direction=IntelligenceDirection.neutral,
                importance=IntelligenceImportance.medium,
                relevance_score=55.0,
                confidence_pct=60.0,
            )
        ],
        session_phase="closed",
        regime="bearish",
        volatility_20=0.010,
    )

    assert "session_not_tradeable" in snapshot.risk_flags
    assert snapshot.session_phase == "closed"

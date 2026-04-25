from __future__ import annotations

from app.schemas.market_intelligence import (
    IntelligenceDirection,
    IntelligenceImportance,
    IntelligenceSourceType,
    MarketBias,
    MarketEventType,
    MarketIntelligenceItem,
    MarketIntelligenceSnapshot,
    StructuredMarketEvent,
)
from app.schemas.news_connector import (
    ConnectorFetchMode,
    ConnectorFetchRequest,
    ConnectorFetchResult,
    ConnectorSourceKind,
    ConnectorStatus,
)
from app.services.intelligence_snapshot_service import IntelligenceSnapshotService


class FakeIngestionService:
    def __init__(
        self,
        *,
        returned_items: list[MarketIntelligenceItem],
        returned_results: list[ConnectorFetchResult],
    ) -> None:
        self.returned_items = returned_items
        self.returned_results = returned_results
        self.last_requests: list[ConnectorFetchRequest] | None = None

    def fetch_from_sources(
        self,
        requests: list[ConnectorFetchRequest],
    ) -> tuple[list[MarketIntelligenceItem], list[ConnectorFetchResult]]:
        self.last_requests = requests
        return self.returned_items, self.returned_results


class FakeMarketIntelligenceService:
    def __init__(
        self,
        *,
        returned_snapshot: MarketIntelligenceSnapshot,
        returned_structured_events: list[StructuredMarketEvent] | None = None,
    ) -> None:
        self.returned_snapshot = returned_snapshot
        self.returned_structured_events = returned_structured_events or []
        self.last_kwargs: dict[str, object] | None = None
        self.last_structured_kwargs: dict[str, object] | None = None

    def build_snapshot(self, **kwargs) -> MarketIntelligenceSnapshot:
        self.last_kwargs = kwargs
        return self.returned_snapshot

    def build_structured_events(self, **kwargs) -> list[StructuredMarketEvent]:
        self.last_structured_kwargs = kwargs
        return self.returned_structured_events


def _build_item(
    *,
    title: str,
    source_name: str,
    source: IntelligenceSourceType = IntelligenceSourceType.news,
    event_type: MarketEventType = MarketEventType.headline,
) -> MarketIntelligenceItem:
    return MarketIntelligenceItem(
        source=source,
        event_type=event_type,
        importance=IntelligenceImportance.medium,
        direction=IntelligenceDirection.neutral,
        title=title,
        summary="Useful intelligence.",
        source_name=source_name,
        relevance_score=55.0,
        confidence_pct=60.0,
        tags=["macro"],
        raw_text=title,
    )


def _build_request(
    *,
    source_kind: ConnectorSourceKind,
    source_name: str,
    fetch_mode: ConnectorFetchMode,
) -> ConnectorFetchRequest:
    return ConnectorFetchRequest(
        source_kind=source_kind,
        source_name=source_name,
        fetch_mode=fetch_mode,
        max_items=5,
    )


def _build_result(
    *,
    source_kind: ConnectorSourceKind,
    source_name: str,
    fetched_items: int,
    accepted_items: int,
) -> ConnectorFetchResult:
    return ConnectorFetchResult(
        source_kind=source_kind,
        source_name=source_name,
        status=ConnectorStatus.ok,
        fetched_items=fetched_items,
        accepted_items=accepted_items,
    )


def _build_snapshot() -> MarketIntelligenceSnapshot:
    return MarketIntelligenceSnapshot(
        asset="Nasdaq 100",
        symbol="NDX",
        timeframe="1h",
        generated_at_utc="2026-03-27T18:00:00+00:00",
        market_bias=MarketBias.long,
        bias_confidence_pct=67.0,
        session_phase="open",
        regime="bullish",
        volatility_20=0.011,
        rsi_14=58.0,
        distance_sma20=0.002,
        distance_sma50=0.004,
        key_levels=[23000.0, 23100.0],
        dominant_drivers=["fed", "macro"],
        risk_flags=[],
        items=[],
        synthesis="External intelligence snapshot.",
    )


def test_build_snapshot_from_requests_returns_snapshot_and_connector_results() -> None:
    requests = [
        _build_request(
            source_kind=ConnectorSourceKind.fed,
            source_name="Federal Reserve",
            fetch_mode=ConnectorFetchMode.rss,
        )
    ]
    returned_items = [
        _build_item(
            title="Fed statement",
            source_name="Federal Reserve",
            source=IntelligenceSourceType.macro_release,
            event_type=MarketEventType.macro,
        )
    ]
    returned_results = [
        _build_result(
            source_kind=ConnectorSourceKind.fed,
            source_name="Federal Reserve",
            fetched_items=1,
            accepted_items=1,
        )
    ]

    ingestion_service = FakeIngestionService(
        returned_items=returned_items,
        returned_results=returned_results,
    )
    market_service = FakeMarketIntelligenceService(returned_snapshot=_build_snapshot())

    service = IntelligenceSnapshotService(
        ingestion_service=ingestion_service,
        market_intelligence_service=market_service,
    )

    snapshot, connector_results = service.build_snapshot_from_requests(
        asset="Nasdaq 100",
        symbol="NDX",
        timeframe="1h",
        requests=requests,
    )

    assert ingestion_service.last_requests == requests
    assert snapshot.asset == "Nasdaq 100"
    assert snapshot.market_bias == MarketBias.long
    assert connector_results == returned_results


def test_build_snapshot_from_requests_passes_context_to_market_intelligence_service() -> None:
    returned_items = [
        _build_item(
            title="ECB decision",
            source_name="European Central Bank",
            source=IntelligenceSourceType.macro_release,
            event_type=MarketEventType.macro,
        )
    ]

    ingestion_service = FakeIngestionService(
        returned_items=returned_items,
        returned_results=[],
    )
    market_service = FakeMarketIntelligenceService(returned_snapshot=_build_snapshot())

    service = IntelligenceSnapshotService(
        ingestion_service=ingestion_service,
        market_intelligence_service=market_service,
    )

    service.build_snapshot_from_requests(
        asset="S&P 500",
        symbol="SPX",
        timeframe="1h",
        requests=[],
        session_phase="midday",
        regime="bearish",
        volatility_20=0.013,
        rsi_14=44.0,
        distance_sma20=-0.003,
        distance_sma50=-0.006,
        key_levels=[5100.0, 5150.0],
        generated_at_utc="2026-03-27T19:00:00+00:00",
    )

    assert market_service.last_kwargs is not None
    assert market_service.last_kwargs["asset"] == "S&P 500"
    assert market_service.last_kwargs["symbol"] == "SPX"
    assert market_service.last_kwargs["timeframe"] == "1h"
    assert market_service.last_kwargs["items"] == returned_items
    assert market_service.last_kwargs["session_phase"] == "midday"
    assert market_service.last_kwargs["regime"] == "bearish"
    assert market_service.last_kwargs["volatility_20"] == 0.013
    assert market_service.last_kwargs["rsi_14"] == 44.0
    assert market_service.last_kwargs["distance_sma20"] == -0.003
    assert market_service.last_kwargs["distance_sma50"] == -0.006
    assert market_service.last_kwargs["key_levels"] == [5100.0, 5150.0]
    assert market_service.last_kwargs["generated_at_utc"] == "2026-03-27T19:00:00+00:00"


def test_build_snapshot_from_requests_supports_zero_sources() -> None:
    ingestion_service = FakeIngestionService(
        returned_items=[],
        returned_results=[],
    )
    market_service = FakeMarketIntelligenceService(returned_snapshot=_build_snapshot())

    service = IntelligenceSnapshotService(
        ingestion_service=ingestion_service,
        market_intelligence_service=market_service,
    )

    snapshot, connector_results = service.build_snapshot_from_requests(
        asset="Nasdaq 100",
        symbol="NDX",
        timeframe="1h",
        requests=[],
    )

    assert ingestion_service.last_requests == []
    assert snapshot.synthesis == "External intelligence snapshot."
    assert connector_results == []
    assert market_service.last_kwargs is not None
    assert market_service.last_kwargs["items"] == []


def test_build_snapshot_from_requests_preserves_multi_source_order() -> None:
    requests = [
        _build_request(
            source_kind=ConnectorSourceKind.white_house,
            source_name="White House",
            fetch_mode=ConnectorFetchMode.html,
        ),
        _build_request(
            source_kind=ConnectorSourceKind.fed,
            source_name="Federal Reserve",
            fetch_mode=ConnectorFetchMode.rss,
        ),
        _build_request(
            source_kind=ConnectorSourceKind.ecb,
            source_name="European Central Bank",
            fetch_mode=ConnectorFetchMode.rss,
        ),
    ]

    returned_items = [
        _build_item(
            title="White House remarks",
            source_name="White House",
            source=IntelligenceSourceType.speech,
            event_type=MarketEventType.speech,
        ),
        _build_item(
            title="Fed headline",
            source_name="Federal Reserve",
            source=IntelligenceSourceType.macro_release,
            event_type=MarketEventType.macro,
        ),
        _build_item(
            title="ECB headline",
            source_name="European Central Bank",
            source=IntelligenceSourceType.macro_release,
            event_type=MarketEventType.macro,
        ),
    ]
    returned_results = [
        _build_result(
            source_kind=ConnectorSourceKind.white_house,
            source_name="White House",
            fetched_items=1,
            accepted_items=1,
        ),
        _build_result(
            source_kind=ConnectorSourceKind.fed,
            source_name="Federal Reserve",
            fetched_items=1,
            accepted_items=1,
        ),
        _build_result(
            source_kind=ConnectorSourceKind.ecb,
            source_name="European Central Bank",
            fetched_items=1,
            accepted_items=1,
        ),
    ]

    ingestion_service = FakeIngestionService(
        returned_items=returned_items,
        returned_results=returned_results,
    )
    market_service = FakeMarketIntelligenceService(returned_snapshot=_build_snapshot())

    service = IntelligenceSnapshotService(
        ingestion_service=ingestion_service,
        market_intelligence_service=market_service,
    )

    _, connector_results = service.build_snapshot_from_requests(
        asset="Nasdaq 100",
        symbol="NDX",
        timeframe="1h",
        requests=requests,
    )

    assert ingestion_service.last_requests == requests
    assert market_service.last_kwargs is not None
    assert [item.title for item in market_service.last_kwargs["items"]] == [
        "White House remarks",
        "Fed headline",
        "ECB headline",
    ]
    assert [result.source_kind for result in connector_results] == [
        ConnectorSourceKind.white_house,
        ConnectorSourceKind.fed,
        ConnectorSourceKind.ecb,
    ]


class FakeLearningService:
    def apply_adaptive_weight(self, event):
        updated_context = dict(event.market_context)
        updated_context["learning_weight_multiplier"] = 1.12
        updated_context["learning_bias"] = "positive"
        updated_context["learning_samples"] = 5

        return event.model_copy(
            update={
                "event_score": 90.0,
                "market_context": updated_context,
            }
        )


class FakeRepository:
    def __init__(self) -> None:
        self.saved_events = []

    def save_event(self, event):
        self.saved_events.append(event)
        return 1


def test_build_snapshot_from_requests_applies_learning_before_save() -> None:
    repo = FakeRepository()

    service = IntelligenceSnapshotService(
        ingestion_service=FakeIngestionService(
            returned_items=[
                MarketIntelligenceItem(
                    source=IntelligenceSourceType.news,
                    event_type=MarketEventType.headline,
                    importance=IntelligenceImportance.high,
                    direction=IntelligenceDirection.bullish,
                    title="Learning snapshot test",
                    summary="Should save learned event.",
                    source_name="FedWire",
                    relevance_score=84.0,
                    confidence_pct=79.0,
                    impact_horizon_minutes=120,
                    tags=["snapshot", "test", "ndx"],
                )
            ],
            returned_results=[],
        ),
        market_event_repository=repo,
        event_learning_service=FakeLearningService(),
    )

    snapshot, connector_results = service.build_snapshot_from_requests(
        asset="Nasdaq 100",
        symbol="NDX",
        timeframe="1h",
        requests=[],
        session_phase="open",
        regime="bullish",
        volatility_20=0.011,
    )

    assert snapshot.asset == "Nasdaq 100"
    assert connector_results == []
    assert len(repo.saved_events) == 1

    saved_event = repo.saved_events[0]
    assert saved_event.event_score == 90.0
    assert saved_event.market_context["learning_weight_multiplier"] == 1.12
    assert saved_event.market_context["learning_bias"] == "positive"
    assert saved_event.market_context["learning_samples"] == 5

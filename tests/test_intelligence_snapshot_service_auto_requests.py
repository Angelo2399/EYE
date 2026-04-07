from __future__ import annotations

from app.schemas.market import MarketSymbol
from app.schemas.market_intelligence import (
    IntelligenceDirection,
    IntelligenceImportance,
    IntelligenceSourceType,
    MarketBias,
    MarketEventType,
    MarketIntelligenceItem,
    MarketIntelligenceSnapshot,
)
from app.schemas.news_connector import (
    ConnectorFetchMode,
    ConnectorFetchRequest,
    ConnectorFetchResult,
    ConnectorSourceKind,
    ConnectorStatus,
)
from app.services.intelligence_snapshot_service import IntelligenceSnapshotService


class FakeRequestBuilderService:
    def __init__(self, returned_requests: list[ConnectorFetchRequest]) -> None:
        self.returned_requests = returned_requests
        self.last_symbol: MarketSymbol | None = None
        self.last_max_items_per_source: int | None = None

    def build_default_requests(
        self,
        *,
        symbol: MarketSymbol,
        max_items_per_source: int = 10,
    ) -> list[ConnectorFetchRequest]:
        self.last_symbol = symbol
        self.last_max_items_per_source = max_items_per_source
        return self.returned_requests


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
    def __init__(self, returned_snapshot: MarketIntelligenceSnapshot) -> None:
        self.returned_snapshot = returned_snapshot
        self.last_kwargs: dict[str, object] | None = None

    def build_snapshot(self, **kwargs) -> MarketIntelligenceSnapshot:
        self.last_kwargs = kwargs
        return self.returned_snapshot


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
        generated_at_utc="2026-03-28T10:00:00+00:00",
        market_bias=MarketBias.long,
        bias_confidence_pct=66.0,
        session_phase="open",
        regime="bullish",
        volatility_20=0.011,
        rsi_14=57.0,
        distance_sma20=0.002,
        distance_sma50=0.004,
        key_levels=[23000.0, 23100.0],
        dominant_drivers=["fed", "macro"],
        risk_flags=[],
        items=[],
        synthesis="Automatic intelligence snapshot.",
    )


def test_build_snapshot_for_symbol_calls_builder_and_uses_generated_requests_for_ndx() -> None:
    returned_requests = [
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

    builder = FakeRequestBuilderService(returned_requests=returned_requests)
    ingestion_service = FakeIngestionService(
        returned_items=returned_items,
        returned_results=returned_results,
    )
    market_service = FakeMarketIntelligenceService(returned_snapshot=_build_snapshot())

    service = IntelligenceSnapshotService(
        ingestion_service=ingestion_service,
        market_intelligence_service=market_service,
        request_builder_service=builder,
    )

    snapshot, connector_results = service.build_snapshot_for_symbol(
        asset="Nasdaq 100",
        symbol=MarketSymbol.ndx,
        timeframe="1h",
        max_items_per_source=12,
    )

    assert builder.last_symbol == MarketSymbol.ndx
    assert builder.last_max_items_per_source == 12
    assert ingestion_service.last_requests == returned_requests
    assert snapshot.asset == "Nasdaq 100"
    assert connector_results == returned_results


def test_build_snapshot_for_symbol_calls_builder_and_uses_generated_requests_for_spx() -> None:
    returned_requests = [
        _build_request(
            source_kind=ConnectorSourceKind.white_house,
            source_name="White House",
            fetch_mode=ConnectorFetchMode.html,
        )
    ]

    builder = FakeRequestBuilderService(returned_requests=returned_requests)
    ingestion_service = FakeIngestionService(
        returned_items=[],
        returned_results=[],
    )
    market_service = FakeMarketIntelligenceService(returned_snapshot=_build_snapshot())

    service = IntelligenceSnapshotService(
        ingestion_service=ingestion_service,
        market_intelligence_service=market_service,
        request_builder_service=builder,
    )

    service.build_snapshot_for_symbol(
        asset="S&P 500",
        symbol=MarketSymbol.spx,
        timeframe="1h",
        max_items_per_source=7,
    )

    assert builder.last_symbol == MarketSymbol.spx
    assert builder.last_max_items_per_source == 7
    assert ingestion_service.last_requests == returned_requests


def test_build_snapshot_for_symbol_passes_context_to_market_intelligence_service() -> None:
    returned_requests = [
        _build_request(
            source_kind=ConnectorSourceKind.fed,
            source_name="Federal Reserve",
            fetch_mode=ConnectorFetchMode.rss,
        )
    ]
    returned_items = [
        _build_item(
            title="ECB decision",
            source_name="European Central Bank",
            source=IntelligenceSourceType.macro_release,
            event_type=MarketEventType.macro,
        )
    ]

    builder = FakeRequestBuilderService(returned_requests=returned_requests)
    ingestion_service = FakeIngestionService(
        returned_items=returned_items,
        returned_results=[],
    )
    market_service = FakeMarketIntelligenceService(returned_snapshot=_build_snapshot())

    service = IntelligenceSnapshotService(
        ingestion_service=ingestion_service,
        market_intelligence_service=market_service,
        request_builder_service=builder,
    )

    service.build_snapshot_for_symbol(
        asset="S&P 500",
        symbol=MarketSymbol.spx,
        timeframe="1h",
        session_phase="midday",
        regime="bearish",
        volatility_20=0.013,
        rsi_14=44.0,
        distance_sma20=-0.003,
        distance_sma50=-0.006,
        key_levels=[5100.0, 5150.0],
        generated_at_utc="2026-03-28T11:00:00+00:00",
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
    assert market_service.last_kwargs["generated_at_utc"] == "2026-03-28T11:00:00+00:00"


def test_build_snapshot_for_symbol_calls_builder_and_uses_generated_requests_for_wti() -> None:
    returned_requests = [
        _build_request(
            source_kind=ConnectorSourceKind.opec,
            source_name="OPEC",
            fetch_mode=ConnectorFetchMode.html,
        ),
        _build_request(
            source_kind=ConnectorSourceKind.eia,
            source_name="U.S. Energy Information Administration",
            fetch_mode=ConnectorFetchMode.html,
        ),
        _build_request(
            source_kind=ConnectorSourceKind.iea,
            source_name="International Energy Agency",
            fetch_mode=ConnectorFetchMode.html,
        ),
        _build_request(
            source_kind=ConnectorSourceKind.cftc,
            source_name="U.S. Commodity Futures Trading Commission",
            fetch_mode=ConnectorFetchMode.html,
        ),
    ]

    returned_items = [
        _build_item(
            title="OPEC supply update",
            source_name="OPEC",
            source=IntelligenceSourceType.macro_release,
            event_type=MarketEventType.macro,
        )
    ]

    returned_results = [
        _build_result(
            source_kind=ConnectorSourceKind.opec,
            source_name="OPEC",
            fetched_items=1,
            accepted_items=1,
        ),
        _build_result(
            source_kind=ConnectorSourceKind.eia,
            source_name="U.S. Energy Information Administration",
            fetched_items=1,
            accepted_items=1,
        ),
        _build_result(
            source_kind=ConnectorSourceKind.iea,
            source_name="International Energy Agency",
            fetched_items=1,
            accepted_items=1,
        ),
        _build_result(
            source_kind=ConnectorSourceKind.cftc,
            source_name="U.S. Commodity Futures Trading Commission",
            fetched_items=1,
            accepted_items=1,
        ),
    ]

    builder = FakeRequestBuilderService(returned_requests=returned_requests)
    ingestion_service = FakeIngestionService(
        returned_items=returned_items,
        returned_results=returned_results,
    )
    market_service = FakeMarketIntelligenceService(returned_snapshot=_build_snapshot())

    service = IntelligenceSnapshotService(
        ingestion_service=ingestion_service,
        market_intelligence_service=market_service,
        request_builder_service=builder,
    )

    snapshot, connector_results = service.build_snapshot_for_symbol(
        asset="WTI Crude Oil",
        symbol=MarketSymbol.wti,
        timeframe="1h",
        max_items_per_source=9,
    )

    assert builder.last_symbol == MarketSymbol.wti
    assert builder.last_max_items_per_source == 9
    assert ingestion_service.last_requests == returned_requests
    assert snapshot.asset == "Nasdaq 100"
    assert connector_results == returned_results

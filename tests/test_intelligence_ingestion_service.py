from __future__ import annotations

from app.schemas.market_intelligence import (
    IntelligenceDirection,
    IntelligenceImportance,
    IntelligenceSourceType,
    MarketEventType,
    MarketIntelligenceItem,
)
from app.schemas.news_connector import (
    ConnectorFetchMode,
    ConnectorFetchRequest,
    ConnectorFetchResult,
    ConnectorSourceKind,
    ConnectorStatus,
)
from app.services.connectors.base_connector import BaseIntelligenceConnector
from app.services.intelligence_ingestion_service import IntelligenceIngestionService


class FakeConnector(BaseIntelligenceConnector):
    def __init__(
        self,
        *,
        source_name: str,
        returned_items: list[MarketIntelligenceItem],
        returned_result: ConnectorFetchResult,
    ) -> None:
        self._source_name = source_name
        self._returned_items = returned_items
        self._returned_result = returned_result
        self.last_request: ConnectorFetchRequest | None = None

    @property
    def source_name(self) -> str:
        return self._source_name

    def fetch(
        self,
        request: ConnectorFetchRequest,
    ) -> tuple[list[MarketIntelligenceItem], ConnectorFetchResult]:
        self.last_request = request
        return self._returned_items, self._returned_result


def _build_item(
    *,
    title: str,
    source_name: str,
    source_type: IntelligenceSourceType = IntelligenceSourceType.news,
    event_type: MarketEventType = MarketEventType.headline,
) -> MarketIntelligenceItem:
    return MarketIntelligenceItem(
        source=source_type,
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
    status: ConnectorStatus = ConnectorStatus.ok,
) -> ConnectorFetchResult:
    return ConnectorFetchResult(
        source_kind=source_kind,
        source_name=source_name,
        status=status,
        fetched_items=fetched_items,
        accepted_items=accepted_items,
    )


def test_list_registered_sources_returns_sorted_source_names() -> None:
    service = IntelligenceIngestionService(
        connectors={
            ConnectorSourceKind.white_house: FakeConnector(
                source_name="White House",
                returned_items=[],
                returned_result=_build_result(
                    source_kind=ConnectorSourceKind.white_house,
                    source_name="White House",
                    fetched_items=0,
                    accepted_items=0,
                ),
            ),
            ConnectorSourceKind.fed: FakeConnector(
                source_name="Federal Reserve",
                returned_items=[],
                returned_result=_build_result(
                    source_kind=ConnectorSourceKind.fed,
                    source_name="Federal Reserve",
                    fetched_items=0,
                    accepted_items=0,
                ),
            ),
            ConnectorSourceKind.ecb: FakeConnector(
                source_name="European Central Bank",
                returned_items=[],
                returned_result=_build_result(
                    source_kind=ConnectorSourceKind.ecb,
                    source_name="European Central Bank",
                    fetched_items=0,
                    accepted_items=0,
                ),
            ),
            ConnectorSourceKind.cboe: FakeConnector(
                source_name="Cboe",
                returned_items=[],
                returned_result=_build_result(
                    source_kind=ConnectorSourceKind.cboe,
                    source_name="Cboe",
                    fetched_items=0,
                    accepted_items=0,
                ),
            ),
            ConnectorSourceKind.sp_dji: FakeConnector(
                source_name="S&P Dow Jones Indices",
                returned_items=[],
                returned_result=_build_result(
                    source_kind=ConnectorSourceKind.sp_dji,
                    source_name="S&P Dow Jones Indices",
                    fetched_items=0,
                    accepted_items=0,
                ),
            ),
        }
    )

    assert service.list_registered_sources() == ["cboe", "ecb", "fed", "sp_dji", "white_house"]


def test_fetch_from_source_routes_request_to_matching_connector() -> None:
    fed_item = _build_item(
        title="Fed statement",
        source_name="Federal Reserve",
        source_type=IntelligenceSourceType.macro_release,
        event_type=MarketEventType.macro,
    )
    fed_result = _build_result(
        source_kind=ConnectorSourceKind.fed,
        source_name="Federal Reserve",
        fetched_items=1,
        accepted_items=1,
    )
    fed_connector = FakeConnector(
        source_name="Federal Reserve",
        returned_items=[fed_item],
        returned_result=fed_result,
    )

    service = IntelligenceIngestionService(
        connectors={ConnectorSourceKind.fed: fed_connector}
    )

    request = ConnectorFetchRequest(
        source_kind=ConnectorSourceKind.fed,
        source_name="Federal Reserve",
        fetch_mode=ConnectorFetchMode.rss,
        max_items=5,
        asset_scope=["NDX", "SPX"],
        tags=["macro"],
    )

    items, result = service.fetch_from_source(request)

    assert fed_connector.last_request == request
    assert len(items) == 1
    assert items[0].title == "Fed statement"
    assert result.source_kind == ConnectorSourceKind.fed
    assert result.accepted_items == 1


def test_fetch_from_source_routes_request_to_sp_dji_connector() -> None:
    sp_dji_item = _build_item(
        title="S&P 500 rebalancing effective before open",
        source_name="S&P Dow Jones Indices",
        source_type=IntelligenceSourceType.news,
        event_type=MarketEventType.headline,
    )
    sp_dji_result = _build_result(
        source_kind=ConnectorSourceKind.sp_dji,
        source_name="S&P Dow Jones Indices",
        fetched_items=1,
        accepted_items=1,
    )
    sp_dji_connector = FakeConnector(
        source_name="S&P Dow Jones Indices",
        returned_items=[sp_dji_item],
        returned_result=sp_dji_result,
    )

    service = IntelligenceIngestionService(
        connectors={ConnectorSourceKind.sp_dji: sp_dji_connector}
    )

    request = ConnectorFetchRequest(
        source_kind=ConnectorSourceKind.sp_dji,
        source_name="S&P Dow Jones Indices",
        fetch_mode=ConnectorFetchMode.html,
        max_items=5,
        asset_scope=["SPX"],
        tags=["macro", "broad_market"],
    )

    items, result = service.fetch_from_source(request)

    assert sp_dji_connector.last_request == request
    assert len(items) == 1
    assert items[0].title == "S&P 500 rebalancing effective before open"
    assert result.source_kind == ConnectorSourceKind.sp_dji
    assert result.accepted_items == 1


def test_fetch_from_source_routes_request_to_cboe_connector() -> None:
    cboe_item = _build_item(
        title="SPX Call Demand Jumps on TACO Optimism",
        source_name="Cboe",
        source_type=IntelligenceSourceType.news,
        event_type=MarketEventType.headline,
    )
    cboe_result = _build_result(
        source_kind=ConnectorSourceKind.cboe,
        source_name="Cboe",
        fetched_items=1,
        accepted_items=1,
    )
    cboe_connector = FakeConnector(
        source_name="Cboe",
        returned_items=[cboe_item],
        returned_result=cboe_result,
    )

    service = IntelligenceIngestionService(
        connectors={ConnectorSourceKind.cboe: cboe_connector}
    )

    request = ConnectorFetchRequest(
        source_kind=ConnectorSourceKind.cboe,
        source_name="Cboe",
        fetch_mode=ConnectorFetchMode.html,
        max_items=5,
        asset_scope=["SPX"],
        tags=["macro", "broad_market"],
    )

    items, result = service.fetch_from_source(request)

    assert cboe_connector.last_request == request
    assert len(items) == 1
    assert items[0].title == "SPX Call Demand Jumps on TACO Optimism"
    assert result.source_kind == ConnectorSourceKind.cboe
    assert result.accepted_items == 1


def test_fetch_from_source_raises_for_unregistered_source() -> None:
    service = IntelligenceIngestionService(connectors={})

    request = ConnectorFetchRequest(
        source_kind=ConnectorSourceKind.ecb,
        source_name="European Central Bank",
        fetch_mode=ConnectorFetchMode.rss,
    )

    try:
        service.fetch_from_source(request)
    except ValueError as exc:
        assert "source_kind='ecb'" in str(exc)
    else:
        raise AssertionError("Expected ValueError for unregistered connector.")


def test_fetch_from_sources_aggregates_items_and_results_in_request_order() -> None:
    fed_item = _build_item(
        title="Fed headline",
        source_name="Federal Reserve",
        source_type=IntelligenceSourceType.macro_release,
        event_type=MarketEventType.macro,
    )
    ecb_item = _build_item(
        title="ECB headline",
        source_name="European Central Bank",
        source_type=IntelligenceSourceType.macro_release,
        event_type=MarketEventType.macro,
    )
    white_house_item = _build_item(
        title="White House remarks",
        source_name="White House",
        source_type=IntelligenceSourceType.speech,
        event_type=MarketEventType.speech,
    )

    service = IntelligenceIngestionService(
        connectors={
            ConnectorSourceKind.fed: FakeConnector(
                source_name="Federal Reserve",
                returned_items=[fed_item],
                returned_result=_build_result(
                    source_kind=ConnectorSourceKind.fed,
                    source_name="Federal Reserve",
                    fetched_items=1,
                    accepted_items=1,
                ),
            ),
            ConnectorSourceKind.ecb: FakeConnector(
                source_name="European Central Bank",
                returned_items=[ecb_item],
                returned_result=_build_result(
                    source_kind=ConnectorSourceKind.ecb,
                    source_name="European Central Bank",
                    fetched_items=1,
                    accepted_items=1,
                ),
            ),
            ConnectorSourceKind.white_house: FakeConnector(
                source_name="White House",
                returned_items=[white_house_item],
                returned_result=_build_result(
                    source_kind=ConnectorSourceKind.white_house,
                    source_name="White House",
                    fetched_items=1,
                    accepted_items=1,
                ),
            ),
        }
    )

    requests = [
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.white_house,
            source_name="White House",
            fetch_mode=ConnectorFetchMode.html,
        ),
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.fed,
            source_name="Federal Reserve",
            fetch_mode=ConnectorFetchMode.rss,
        ),
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.ecb,
            source_name="European Central Bank",
            fetch_mode=ConnectorFetchMode.rss,
        ),
    ]

    items, results = service.fetch_from_sources(requests)

    assert [item.title for item in items] == [
        "White House remarks",
        "Fed headline",
        "ECB headline",
    ]
    assert [result.source_kind for result in results] == [
        ConnectorSourceKind.white_house,
        ConnectorSourceKind.fed,
        ConnectorSourceKind.ecb,
    ]
    assert len(results) == 3


def test_default_connectors_include_cftc_eia_iea_and_opec() -> None:
    service = IntelligenceIngestionService()

    assert service.list_registered_sources() == [
        "bea",
        "bls",
        "cboe",
        "cftc",
        "custom",
        "ecb",
        "eia",
        "fed",
        "iea",
        "nasdaq",
        "opec",
        "sec",
        "sp_dji",
        "treasury",
        "white_house",
    ]

from __future__ import annotations

from app.schemas.market_intelligence import (
    IntelligenceDirection,
    IntelligenceImportance,
    IntelligenceSourceType,
    MarketEventType,
)
from app.schemas.news_connector import (
    ConnectorFetchMode,
    ConnectorFetchRequest,
    ConnectorSourceKind,
    ConnectorStatus,
)
from app.services.connectors.iea_connector import IEAConnector


SAMPLE_IEA_HTML = """
<html>
  <body>
    <a href="/reports/oil-market-report-april-2026">Oil Market Report: tight supply and higher prices</a>
    <a href="/reports/monthly-oil-statistics-april-2026">Monthly Oil Statistics: rising stocks and weaker demand</a>
    <a href="/about/contact">Contact</a>
  </body>
</html>
"""


def test_iea_connector_fetch_parses_html_items() -> None:
    connector = IEAConnector(fetcher=lambda url: SAMPLE_IEA_HTML)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.iea,
            source_name="International Energy Agency",
            fetch_mode=ConnectorFetchMode.html,
            max_items=10,
            asset_scope=["WTI"],
            tags=["oil", "supply"],
        )
    )

    assert result.status == ConnectorStatus.ok
    assert result.fetched_items == 2
    assert result.accepted_items == 2
    assert result.next_cursor is not None
    assert len(items) == 2

    assert items[0].source == IntelligenceSourceType.macro_release
    assert items[0].event_type == MarketEventType.macro
    assert items[0].importance == IntelligenceImportance.critical
    assert items[0].direction == IntelligenceDirection.bullish
    assert items[0].asset_scope == ["WTI"]
    assert "iea" in items[0].tags
    assert "omr" in items[0].tags
    assert "oil" in items[0].tags
    assert "supply" in items[0].tags
    assert "prices" in items[0].tags
    assert items[0].source_url is not None
    assert items[0].source_url.startswith("https://")

    assert items[1].source == IntelligenceSourceType.macro_release
    assert items[1].event_type == MarketEventType.macro
    assert items[1].importance == IntelligenceImportance.high
    assert items[1].direction == IntelligenceDirection.bearish
    assert "iea" in items[1].tags
    assert "monthly_oil_statistics" in items[1].tags
    assert "stocks" in items[1].tags
    assert "demand" in items[1].tags


def test_iea_connector_rejects_non_iea_requests() -> None:
    connector = IEAConnector(fetcher=lambda url: SAMPLE_IEA_HTML)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.fed,
            source_name="Federal Reserve",
            fetch_mode=ConnectorFetchMode.html,
        )
    )

    assert items == []
    assert result.status == ConnectorStatus.failed
    assert result.errors
    assert "source_kind='iea'" in result.errors[0]


def test_iea_connector_rejects_non_html_fetch_mode() -> None:
    connector = IEAConnector(fetcher=lambda url: SAMPLE_IEA_HTML)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.iea,
            source_name="International Energy Agency",
            fetch_mode=ConnectorFetchMode.api,
        )
    )

    assert items == []
    assert result.status == ConnectorStatus.failed
    assert result.errors
    assert "fetch_mode='html'" in result.errors[0]

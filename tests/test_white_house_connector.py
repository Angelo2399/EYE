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
from app.services.connectors.white_house_connector import WhiteHouseConnector


SAMPLE_WHITE_HOUSE_HTML = """
<html>
  <body>
    <a href="/briefings-statements/2026/03/president-trump-delivers-remarks-on-the-economy/">
      President Trump Delivers Remarks on the Economy
    </a>
    <a href="/fact-sheets/2026/03/fact-sheet-president-strengthens-u-s-energy-security/">
      Fact Sheet: President Strengthens U.S. Energy Security
    </a>
    <a href="/news/">Read the Latest</a>
  </body>
</html>
"""


def test_white_house_connector_fetch_parses_html_items() -> None:
    connector = WhiteHouseConnector(fetcher=lambda url: SAMPLE_WHITE_HOUSE_HTML)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.white_house,
            source_name="White House",
            fetch_mode=ConnectorFetchMode.html,
            max_items=10,
            asset_scope=["NDX", "SPX"],
            tags=["macro"],
        )
    )

    assert result.status == ConnectorStatus.ok
    assert result.fetched_items == 2
    assert result.accepted_items == 2
    assert result.next_cursor is not None
    assert len(items) == 2

    assert items[0].source == IntelligenceSourceType.speech
    assert items[0].event_type == MarketEventType.speech
    assert items[0].importance == IntelligenceImportance.high
    assert items[0].direction == IntelligenceDirection.bullish
    assert items[0].asset_scope == ["NDX", "SPX"]
    assert "white_house" in items[0].tags
    assert "remarks" in items[0].tags
    assert "economy" in items[0].tags
    assert "macro" in items[0].tags
    assert items[0].source_url.startswith("https://www.whitehouse.gov/briefings-statements/")

    assert items[1].source == IntelligenceSourceType.news
    assert items[1].event_type == MarketEventType.headline
    assert items[1].importance == IntelligenceImportance.medium
    assert "fact_sheet" in items[1].tags
    assert "energy" in items[1].tags


def test_white_house_connector_rejects_non_white_house_requests() -> None:
    connector = WhiteHouseConnector(fetcher=lambda url: SAMPLE_WHITE_HOUSE_HTML)

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
    assert "source_kind='white_house'" in result.errors[0]


def test_white_house_connector_rejects_non_html_fetch_mode() -> None:
    connector = WhiteHouseConnector(fetcher=lambda url: SAMPLE_WHITE_HOUSE_HTML)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.white_house,
            source_name="White House",
            fetch_mode=ConnectorFetchMode.rss,
        )
    )

    assert items == []
    assert result.status == ConnectorStatus.failed
    assert result.errors
    assert "fetch_mode='html'" in result.errors[0]

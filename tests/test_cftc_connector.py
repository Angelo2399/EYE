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
from app.services.connectors.cftc_connector import CFTCConnector


SAMPLE_CFTC_HTML = """
<html>
  <body>
    WTI Crude Oil - New York Mercantile Exchange - managed money long
    Crude Oil - New York Mercantile Exchange - swap dealers short
    Gold - Commodity Exchange Inc.
  </body>
</html>
"""


def test_cftc_connector_fetch_parses_petroleum_items() -> None:
    connector = CFTCConnector(fetcher=lambda url: SAMPLE_CFTC_HTML)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.cftc,
            source_name="U.S. Commodity Futures Trading Commission",
            fetch_mode=ConnectorFetchMode.html,
            max_items=10,
            asset_scope=["WTI"],
            tags=["oil", "positioning"],
        )
    )

    assert result.status == ConnectorStatus.ok
    assert result.fetched_items == 2
    assert result.accepted_items == 2
    assert result.next_cursor is not None
    assert len(items) == 2

    assert items[0].source == IntelligenceSourceType.macro_release
    assert items[0].event_type == MarketEventType.macro
    assert items[0].importance == IntelligenceImportance.high
    assert items[0].direction == IntelligenceDirection.bullish
    assert items[0].asset_scope == ["WTI"]
    assert "cftc" in items[0].tags
    assert "cot" in items[0].tags
    assert "petroleum" in items[0].tags
    assert "wti" in items[0].tags
    assert "nymex" in items[0].tags
    assert "managed_money" in items[0].tags
    assert items[0].source_url is not None
    assert items[0].source_url.startswith("https://")

    assert items[1].source == IntelligenceSourceType.macro_release
    assert items[1].event_type == MarketEventType.macro
    assert items[1].importance == IntelligenceImportance.medium
    assert items[1].direction == IntelligenceDirection.bearish
    assert "crude_oil" in items[1].tags
    assert "nymex" in items[1].tags
    assert "swap_dealers" in items[1].tags


def test_cftc_connector_rejects_non_cftc_requests() -> None:
    connector = CFTCConnector(fetcher=lambda url: SAMPLE_CFTC_HTML)

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
    assert "source_kind='cftc'" in result.errors[0]


def test_cftc_connector_rejects_non_html_fetch_mode() -> None:
    connector = CFTCConnector(fetcher=lambda url: SAMPLE_CFTC_HTML)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.cftc,
            source_name="U.S. Commodity Futures Trading Commission",
            fetch_mode=ConnectorFetchMode.api,
        )
    )

    assert items == []
    assert result.status == ConnectorStatus.failed
    assert result.errors
    assert "fetch_mode='html'" in result.errors[0]

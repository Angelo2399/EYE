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
from app.services.connectors.eia_connector import EIAConnector


SAMPLE_EIA_HTML = """
<html>
  <body>
    <a href="/petroleum/supply/weekly/">Weekly Petroleum Status Report: rising stocks and refinery utilization</a>
    <a href="/outlooks/steo/">Short-Term Energy Outlook</a>
    <a href="/todayinenergy/">Today in Energy</a>
  </body>
</html>
"""


def test_eia_connector_fetch_parses_html_items() -> None:
    connector = EIAConnector(fetcher=lambda url: SAMPLE_EIA_HTML)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.eia,
            source_name="U.S. Energy Information Administration",
            fetch_mode=ConnectorFetchMode.html,
            max_items=10,
            asset_scope=["WTI"],
            tags=["oil", "inventory"],
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
    assert items[0].direction == IntelligenceDirection.bearish
    assert items[0].asset_scope == ["WTI"]
    assert "eia" in items[0].tags
    assert "wpsr" in items[0].tags
    assert "inventory" in items[0].tags
    assert "refinery" in items[0].tags
    assert items[0].source_url is not None
    assert items[0].source_url.startswith("https://")

    assert items[1].source == IntelligenceSourceType.macro_release
    assert items[1].event_type == MarketEventType.macro
    assert items[1].importance == IntelligenceImportance.critical
    assert "eia" in items[1].tags
    assert "steo" in items[1].tags


def test_eia_connector_rejects_non_eia_requests() -> None:
    connector = EIAConnector(fetcher=lambda url: SAMPLE_EIA_HTML)

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
    assert "source_kind='eia'" in result.errors[0]


def test_eia_connector_rejects_non_html_fetch_mode() -> None:
    connector = EIAConnector(fetcher=lambda url: SAMPLE_EIA_HTML)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.eia,
            source_name="U.S. Energy Information Administration",
            fetch_mode=ConnectorFetchMode.api,
        )
    )

    assert items == []
    assert result.status == ConnectorStatus.failed
    assert result.errors
    assert "fetch_mode='html'" in result.errors[0]

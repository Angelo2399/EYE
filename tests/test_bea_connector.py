from __future__ import annotations

from app.schemas.market_intelligence import (
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
from app.services.connectors.bea_connector import BEAConnector


SAMPLE_BEA_HTML = """
<html>
  <body>
    <h1>BEA Release Schedule</h1>
    <div>Thursday, January 29, 2026</div>
    <div>Gross Domestic Product, Fourth Quarter and Year 2025 (Advance Estimate)</div>
    <div>Friday, January 30, 2026</div>
    <div>Personal Income and Outlays, December 2025</div>
    <div>Friday, January 30, 2026</div>
    <div>Personal Consumption Expenditures (PCE) Price Index, December 2025</div>
    <div>Friday, January 30, 2026</div>
    <div>Core PCE Price Index, December 2025</div>
    <div>Thursday, February 5, 2026</div>
    <div>International Trade in Goods and Services, December 2025</div>
  </body>
</html>
"""


def test_bea_connector_fetch_parses_targeted_release_items() -> None:
    connector = BEAConnector(fetcher=lambda url: SAMPLE_BEA_HTML)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.bea,
            source_name="U.S. Bureau of Economic Analysis",
            fetch_mode=ConnectorFetchMode.html,
            max_items=10,
            asset_scope=["NDX"],
            tags=["macro", "usa"],
        )
    )

    assert result.status == ConnectorStatus.ok
    assert result.fetched_items == 5
    assert result.accepted_items == 5
    assert result.next_cursor is not None
    assert len(items) == 5

    assert items[0].source == IntelligenceSourceType.macro_release
    assert items[0].event_type == MarketEventType.macro
    assert items[0].importance == IntelligenceImportance.critical
    assert items[0].title == "GDP"
    assert "bea" in items[0].tags
    assert "gdp" in items[0].tags
    assert items[0].asset_scope == ["NDX"]

    assert items[1].title == "Personal Income and Outlays"
    assert "income" in items[1].tags
    assert "outlays" in items[1].tags
    assert "pce" in items[1].tags
    assert "core_pce" in items[1].tags

    assert items[2].title == "PCE"
    assert "pce" in items[2].tags

    assert items[3].title == "Core PCE"
    assert "core_pce" in items[3].tags

    assert items[4].title == "International Trade in Goods and Services"
    assert "trade" in items[4].tags


def test_bea_connector_rejects_non_bea_requests() -> None:
    connector = BEAConnector(fetcher=lambda url: SAMPLE_BEA_HTML)

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
    assert "source_kind='bea'" in result.errors[0]


def test_bea_connector_rejects_non_html_fetch_mode() -> None:
    connector = BEAConnector(fetcher=lambda url: SAMPLE_BEA_HTML)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.bea,
            source_name="U.S. Bureau of Economic Analysis",
            fetch_mode=ConnectorFetchMode.api,
        )
    )

    assert items == []
    assert result.status == ConnectorStatus.failed
    assert result.errors
    assert "fetch_mode='html'" in result.errors[0]

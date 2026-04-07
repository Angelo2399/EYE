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
from app.services.connectors.treasury_connector import TreasuryConnector


SAMPLE_TREASURY_HTML = """
<html>
  <body>
    <div>Treasury Announces Auction Announcement for 10-Year Notes</div>
    <div>Treasury Quarterly Refunding Statement</div>
    <div>Treasury Announces Current Quarter Borrowing Estimate</div>
    <div>Debt Management Financing Update</div>
  </body>
</html>
"""


def test_treasury_connector_fetch_parses_targeted_release_items() -> None:
    connector = TreasuryConnector(fetcher=lambda url: SAMPLE_TREASURY_HTML)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.treasury,
            source_name="U.S. Department of the Treasury",
            fetch_mode=ConnectorFetchMode.html,
            max_items=10,
            asset_scope=["NDX"],
            tags=["funding", "liquidity", "rates", "usa"],
        )
    )

    assert result.status == ConnectorStatus.ok
    assert result.fetched_items == 4
    assert result.accepted_items == 4
    assert result.next_cursor is not None
    assert len(items) == 4

    assert items[0].source == IntelligenceSourceType.news
    assert items[0].event_type == MarketEventType.headline
    assert items[0].importance == IntelligenceImportance.high
    assert items[0].title == "Auction Announcement"
    assert "treasury" in items[0].tags
    assert "auctions" in items[0].tags
    assert "funding" in items[0].tags
    assert "rates" in items[0].tags
    assert items[0].asset_scope == ["NDX"]

    assert items[1].title == "Refunding"
    assert items[1].importance == IntelligenceImportance.critical
    assert "refunding" in items[1].tags
    assert "liquidity" in items[1].tags

    assert items[2].title == "Borrowing Estimate"
    assert "borrowing" in items[2].tags

    assert items[3].title == "Debt Management Update"
    assert "funding" in items[3].tags


def test_treasury_connector_rejects_non_treasury_requests() -> None:
    connector = TreasuryConnector(fetcher=lambda url: SAMPLE_TREASURY_HTML)

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
    assert "source_kind='treasury'" in result.errors[0]


def test_treasury_connector_rejects_non_html_fetch_mode() -> None:
    connector = TreasuryConnector(fetcher=lambda url: SAMPLE_TREASURY_HTML)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.treasury,
            source_name="U.S. Department of the Treasury",
            fetch_mode=ConnectorFetchMode.api,
        )
    )

    assert items == []
    assert result.status == ConnectorStatus.failed
    assert result.errors
    assert "fetch_mode='html'" in result.errors[0]

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
from app.services.connectors.company_ir_connector import CompanyIRConnector


SAMPLE_COMPANY_IR_RSS = """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0">
  <channel>
    <title>Microsoft Investor Relations</title>
    <item>
      <title>Microsoft reports quarterly earnings and raises guidance</title>
      <link>https://www.microsoft.com/en-us/investor/earnings-release</link>
      <description>Microsoft reported strong growth and raised guidance for the next quarter.</description>
      <pubDate>Fri, 27 Mar 2026 18:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""


SAMPLE_COMPANY_IR_HTML = """
<html>
  <body>
    <a href="/investor/news/ceo-product-update">CEO outlines new product strategy</a>
    <a href="/investor/news/share-repurchase">Board authorizes share repurchase buyback</a>
    <a href="/investor/contact">Contact Investor Relations</a>
  </body>
</html>
"""


def test_company_ir_connector_fetch_parses_rss_items() -> None:
    connector = CompanyIRConnector(
        source_name="Microsoft Investor Relations",
        feed_url="https://www.microsoft.com/en-us/investor/rss.xml",
        company_ticker="MSFT",
        fetcher=lambda url: SAMPLE_COMPANY_IR_RSS,
    )

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.custom,
            source_name="Microsoft Investor Relations",
            fetch_mode=ConnectorFetchMode.rss,
            max_items=10,
            asset_scope=["NDX"],
            tags=["tech"],
        )
    )

    assert result.status == ConnectorStatus.ok
    assert result.fetched_items == 1
    assert result.accepted_items == 1
    assert result.next_cursor is not None
    assert len(items) == 1

    assert items[0].source == IntelligenceSourceType.news
    assert items[0].event_type == MarketEventType.headline
    assert items[0].importance == IntelligenceImportance.high
    assert items[0].direction == IntelligenceDirection.bullish
    assert items[0].asset_scope == ["NDX"]
    assert "company_ir" in items[0].tags
    assert "official_release" in items[0].tags
    assert "earnings" in items[0].tags
    assert "guidance" in items[0].tags
    assert "msft" in items[0].tags
    assert "tech" in items[0].tags
    assert items[0].source_url is not None
    assert items[0].occurred_at_utc is not None


def test_company_ir_connector_fetch_parses_html_items() -> None:
    connector = CompanyIRConnector(
        source_name="Example Investor Relations",
        page_url="https://example.com/investor/news",
        company_ticker="EXM",
        fetcher=lambda url: SAMPLE_COMPANY_IR_HTML,
    )

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.custom,
            source_name="Example Investor Relations",
            fetch_mode=ConnectorFetchMode.html,
            max_items=10,
            asset_scope=["NDX"],
            tags=["growth"],
        )
    )

    assert result.status == ConnectorStatus.ok
    assert result.fetched_items == 2
    assert result.accepted_items == 2
    assert len(items) == 2

    assert items[0].title == "CEO outlines new product strategy"
    assert "ceo" in items[0].tags
    assert "product" in items[0].tags
    assert "company_ir" in items[0].tags

    assert items[1].title == "Board authorizes share repurchase buyback"
    assert "buyback" in items[1].tags


def test_company_ir_connector_rejects_wrong_source_kind() -> None:
    connector = CompanyIRConnector(
        source_name="Microsoft Investor Relations",
        feed_url="https://www.microsoft.com/en-us/investor/rss.xml",
        company_ticker="MSFT",
        fetcher=lambda url: SAMPLE_COMPANY_IR_RSS,
    )

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.fed,
            source_name="Federal Reserve",
            fetch_mode=ConnectorFetchMode.rss,
        )
    )

    assert items == []
    assert result.status == ConnectorStatus.failed
    assert result.errors
    assert "source_kind='custom'" in result.errors[0]


def test_company_ir_connector_rejects_wrong_fetch_mode() -> None:
    connector = CompanyIRConnector(
        source_name="Microsoft Investor Relations",
        feed_url="https://www.microsoft.com/en-us/investor/rss.xml",
        company_ticker="MSFT",
        fetcher=lambda url: SAMPLE_COMPANY_IR_RSS,
    )

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.custom,
            source_name="Microsoft Investor Relations",
            fetch_mode=ConnectorFetchMode.api,
        )
    )

    assert items == []
    assert result.status == ConnectorStatus.failed
    assert result.errors
    assert "fetch_mode='rss' or 'html'" in result.errors[0]

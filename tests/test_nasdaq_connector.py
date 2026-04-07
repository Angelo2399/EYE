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
from app.services.connectors.nasdaq_connector import NasdaqConnector


SAMPLE_NASDAQ_RSS = """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0">
  <channel>
    <title>Nasdaq News Releases</title>
    <item>
      <title>Nasdaq Reports Strong Quarterly Earnings Growth</title>
      <link>https://ir.nasdaq.com/news-and-events/press-releases/earnings-growth</link>
      <description>Nasdaq reports strong growth and record revenue in its quarterly results.</description>
      <pubDate>Fri, 27 Mar 2026 14:00:00 GMT</pubDate>
      <category>Earnings</category>
    </item>
    <item>
      <title>Nasdaq Expands Market Structure Capabilities for Equity Index Trading</title>
      <link>https://ir.nasdaq.com/news-and-events/press-releases/market-structure</link>
      <description>Nasdaq expands market structure capabilities for index-linked products.</description>
      <pubDate>Fri, 27 Mar 2026 16:30:00 GMT</pubDate>
      <category>Market Structure</category>
    </item>
  </channel>
</rss>
"""


def test_nasdaq_connector_fetch_parses_rss_items() -> None:
    connector = NasdaqConnector(fetcher=lambda url: SAMPLE_NASDAQ_RSS)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.nasdaq,
            source_name="Nasdaq",
            fetch_mode=ConnectorFetchMode.rss,
            max_items=10,
            asset_scope=["NDX"],
            tags=["equity_index", "tech", "growth", "earnings", "market_structure"],
        )
    )

    assert result.status == ConnectorStatus.ok
    assert result.fetched_items == 2
    assert result.accepted_items == 2
    assert result.next_cursor is not None
    assert len(items) == 2

    assert items[0].source == IntelligenceSourceType.news
    assert items[0].event_type == MarketEventType.headline
    assert items[0].importance == IntelligenceImportance.high
    assert items[0].direction == IntelligenceDirection.bullish
    assert items[0].asset_scope == ["NDX"]
    assert "nasdaq" in items[0].tags
    assert "earnings" in items[0].tags
    assert "tech" in items[0].tags
    assert "growth" in items[0].tags
    assert items[0].source_url is not None
    assert items[0].occurred_at_utc is not None

    assert items[1].importance == IntelligenceImportance.high
    assert "market_structure" in items[1].tags
    assert "equity_index" in items[1].tags


def test_nasdaq_connector_rejects_non_nasdaq_requests() -> None:
    connector = NasdaqConnector(fetcher=lambda url: SAMPLE_NASDAQ_RSS)

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
    assert "source_kind='nasdaq'" in result.errors[0]


def test_nasdaq_connector_rejects_non_rss_fetch_mode() -> None:
    connector = NasdaqConnector(fetcher=lambda url: SAMPLE_NASDAQ_RSS)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.nasdaq,
            source_name="Nasdaq",
            fetch_mode=ConnectorFetchMode.api,
        )
    )

    assert items == []
    assert result.status == ConnectorStatus.failed
    assert result.errors
    assert "fetch_mode='rss'" in result.errors[0]

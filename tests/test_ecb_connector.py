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
from app.services.connectors.ecb_connector import ECBConnector


SAMPLE_ECB_RSS = """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0">
  <channel>
    <title>ECB Press Feed</title>
    <item>
      <title>Monetary policy decisions</title>
      <link>https://www.ecb.europa.eu/press/pr/date/2026/html/ecb.mp260327~abc123.en.html</link>
      <description>The Governing Council decided to keep the key ECB interest rates unchanged while inflation remains too high.</description>
      <pubDate>Fri, 27 Mar 2026 12:45:00 GMT</pubDate>
      <category>Press release</category>
    </item>
    <item>
      <title>Speech by Christine Lagarde on financial stability</title>
      <link>https://www.ecb.europa.eu/press/key/date/2026/html/ecb.sp260327~def456.en.html</link>
      <description>Remarks on financial stability, inflation and the euro area outlook.</description>
      <pubDate>Fri, 27 Mar 2026 15:00:00 GMT</pubDate>
      <category>Speech</category>
    </item>
  </channel>
</rss>
"""


def test_ecb_connector_fetch_parses_rss_items() -> None:
    connector = ECBConnector(fetcher=lambda url: SAMPLE_ECB_RSS)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.ecb,
            source_name="European Central Bank",
            fetch_mode=ConnectorFetchMode.rss,
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

    assert items[0].source == IntelligenceSourceType.macro_release
    assert items[0].event_type == MarketEventType.macro
    assert items[0].importance == IntelligenceImportance.critical
    assert items[0].direction == IntelligenceDirection.bearish
    assert items[0].asset_scope == ["NDX", "SPX"]
    assert "ecb" in items[0].tags
    assert "governing_council" in items[0].tags
    assert "rates" in items[0].tags
    assert "macro" in items[0].tags
    assert items[0].source_url.endswith(".html")
    assert items[0].occurred_at_utc is not None

    assert items[1].source == IntelligenceSourceType.speech
    assert items[1].event_type == MarketEventType.speech
    assert items[1].importance == IntelligenceImportance.high
    assert "lagarde" in items[1].tags
    assert "financial_stability" in items[1].tags


def test_ecb_connector_rejects_non_ecb_requests() -> None:
    connector = ECBConnector(fetcher=lambda url: SAMPLE_ECB_RSS)

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
    assert "source_kind='ecb'" in result.errors[0]


def test_ecb_connector_rejects_non_rss_fetch_mode() -> None:
    connector = ECBConnector(fetcher=lambda url: SAMPLE_ECB_RSS)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.ecb,
            source_name="European Central Bank",
            fetch_mode=ConnectorFetchMode.api,
        )
    )

    assert items == []
    assert result.status == ConnectorStatus.failed
    assert result.errors
    assert "fetch_mode='rss'" in result.errors[0]

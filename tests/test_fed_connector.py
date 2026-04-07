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
from app.services.connectors.fed_connector import FedConnector


SAMPLE_FED_RSS = """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0">
  <channel>
    <title>Federal Reserve Press Releases</title>
    <item>
      <title>Federal Open Market Committee issues statement</title>
      <link>https://www.federalreserve.gov/newsevents/pressreleases/monetary20260327a.htm</link>
      <description>The FOMC decided to maintain the target range and noted inflation remains elevated.</description>
      <pubDate>Fri, 27 Mar 2026 18:00:00 GMT</pubDate>
      <category>Monetary Policy</category>
    </item>
    <item>
      <title>Speech by Chair on financial stability</title>
      <link>https://www.federalreserve.gov/newsevents/speech/powell20260327a.htm</link>
      <description>Remarks on financial stability and the labor market.</description>
      <pubDate>Fri, 27 Mar 2026 20:00:00 GMT</pubDate>
      <category>Speeches</category>
    </item>
  </channel>
</rss>
"""


def test_fed_connector_fetch_parses_rss_items() -> None:
    connector = FedConnector(fetcher=lambda url: SAMPLE_FED_RSS)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.fed,
            source_name="Federal Reserve",
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
    assert "fed" in items[0].tags
    assert "fomc" in items[0].tags
    assert "macro" in items[0].tags
    assert items[0].source_url.endswith(".htm")
    assert items[0].occurred_at_utc is not None

    assert items[1].source == IntelligenceSourceType.speech
    assert items[1].event_type == MarketEventType.speech
    assert items[1].importance == IntelligenceImportance.high
    assert "financial_stability" in items[1].tags


def test_fed_connector_rejects_non_fed_requests() -> None:
    connector = FedConnector(fetcher=lambda url: SAMPLE_FED_RSS)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.ecb,
            source_name="ECB",
            fetch_mode=ConnectorFetchMode.rss,
        )
    )

    assert items == []
    assert result.status == ConnectorStatus.failed
    assert result.errors
    assert "source_kind='fed'" in result.errors[0]


def test_fed_connector_rejects_non_rss_fetch_mode() -> None:
    connector = FedConnector(fetcher=lambda url: SAMPLE_FED_RSS)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.fed,
            source_name="Federal Reserve",
            fetch_mode=ConnectorFetchMode.api,
        )
    )

    assert items == []
    assert result.status == ConnectorStatus.failed
    assert result.errors
    assert "fetch_mode='rss'" in result.errors[0]

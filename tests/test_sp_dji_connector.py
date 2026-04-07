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
from app.services.connectors.sp_dji_connector import SPDJIConnector


SAMPLE_SPDJI_HTML = """
<html>
  <body>
    <a href="/en/media-center/news-announcements/latest-news/companies-x-y-z-set-to-join-sp-500">Companies X, Y, Z set to join S&amp;P 500</a>
    <a href="/en/media-center/news-announcements/latest-news/sp-500-rebalancing-effective-before-open">S&amp;P 500 rebalancing effective before open</a>
    <a href="/en/media-center/news-announcements/latest-news/contact-us">Contact us</a>
  </body>
</html>
"""


def test_sp_dji_connector_fetch_parses_spx_relevant_items() -> None:
    connector = SPDJIConnector(fetcher=lambda url: SAMPLE_SPDJI_HTML)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.sp_dji,
            source_name="S&P Dow Jones Indices",
            fetch_mode=ConnectorFetchMode.html,
            max_items=10,
            asset_scope=["SPX"],
            tags=["macro", "broad_market"],
        )
    )

    assert result.status == ConnectorStatus.ok
    assert result.fetched_items == 2
    assert result.accepted_items == 2
    assert len(items) == 2

    first = items[0]
    second = items[1]

    assert first.source == IntelligenceSourceType.news
    assert first.event_type == MarketEventType.headline
    assert first.importance == IntelligenceImportance.high
    assert first.direction == IntelligenceDirection.bullish
    assert "sp500" in first.tags
    assert "component_change" in first.tags
    assert "SPX".lower() in [tag.lower() for tag in first.tags]

    assert second.importance == IntelligenceImportance.high
    assert second.direction == IntelligenceDirection.neutral
    assert "rebalancing" in second.tags
    assert "sp500" in second.tags


def test_sp_dji_connector_rejects_wrong_fetch_mode() -> None:
    connector = SPDJIConnector(fetcher=lambda url: SAMPLE_SPDJI_HTML)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.sp_dji,
            source_name="S&P Dow Jones Indices",
            fetch_mode=ConnectorFetchMode.rss,
            max_items=10,
            asset_scope=["SPX"],
            tags=["macro"],
        )
    )

    assert items == []
    assert result.status == ConnectorStatus.failed
    assert result.fetched_items == 0
    assert result.accepted_items == 0
    assert result.errors
    assert "only supports fetch_mode='html'" in result.errors[0]


def test_sp_dji_connector_limits_items_to_max_items() -> None:
    connector = SPDJIConnector(fetcher=lambda url: SAMPLE_SPDJI_HTML)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.sp_dji,
            source_name="S&P Dow Jones Indices",
            fetch_mode=ConnectorFetchMode.html,
            max_items=1,
            asset_scope=["SPX"],
            tags=["macro"],
        )
    )

    assert result.status == ConnectorStatus.ok
    assert len(items) == 1
    assert result.fetched_items == 1
    assert result.accepted_items == 1

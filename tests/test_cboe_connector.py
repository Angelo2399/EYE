from __future__ import annotations

from app.schemas.market_intelligence import (
    IntelligenceDirection,
    IntelligenceImportance,
)
from app.schemas.news_connector import (
    ConnectorFetchMode,
    ConnectorFetchRequest,
    ConnectorSourceKind,
    ConnectorStatus,
)
from app.services.connectors.cboe_connector import CboeConnector


SAMPLE_CBOE_HTML = """
<html>
  <body>
    <a href="/insights/posts/spx-call-demand-jumps-on-taco-optimism/">SPX Call Demand Jumps on TACO Optimism</a>
    <a href="/insights/posts/index-insights-march-2026/">Index Insights: March 2026</a>
    <a href="/about-us/contact/">Contact</a>
  </body>
</html>
"""


def test_cboe_connector_fetch_parses_spx_relevant_items() -> None:
    connector = CboeConnector(fetcher=lambda url: SAMPLE_CBOE_HTML)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.cboe,
            source_name="Cboe",
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
    assert first.importance == IntelligenceImportance.high
    assert first.direction == IntelligenceDirection.bullish
    assert "spx" in [tag.lower() for tag in first.tags]
    assert "options" in [tag.lower() for tag in first.tags]


def test_cboe_connector_rejects_wrong_fetch_mode() -> None:
    connector = CboeConnector(fetcher=lambda url: SAMPLE_CBOE_HTML)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.cboe,
            source_name="Cboe",
            fetch_mode=ConnectorFetchMode.rss,
            max_items=10,
            asset_scope=["SPX"],
            tags=["macro"],
        )
    )

    assert items == []
    assert result.status == ConnectorStatus.failed
    assert result.errors
    assert "only supports fetch_mode='html'" in result.errors[0]


def test_cboe_connector_limits_items_to_max_items() -> None:
    connector = CboeConnector(fetcher=lambda url: SAMPLE_CBOE_HTML)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.cboe,
            source_name="Cboe",
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

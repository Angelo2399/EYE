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
from app.services.connectors.bls_connector import BLSConnector


SAMPLE_BLS_HTML = """
<html>
  <body>
    <h1>Schedule of Releases</h1>
    <div>Friday, January 9, 2026</div>
    <div>08:30 AM</div>
    <div>Employment Situation for December 2025</div>
    <div>Tuesday, January 13, 2026</div>
    <div>08:30 AM</div>
    <div>Consumer Price Index for December 2025</div>
    <div>Wednesday, January 14, 2026</div>
    <div>08:30 AM</div>
    <div>Producer Price Index for November 2025</div>
    <div>Wednesday, January 7, 2026</div>
    <div>10:00 AM</div>
    <div>Job Openings and Labor Turnover Survey for November 2025</div>
  </body>
</html>
"""


def test_bls_connector_fetch_parses_targeted_release_items() -> None:
    connector = BLSConnector(fetcher=lambda url: SAMPLE_BLS_HTML)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.bls,
            source_name="U.S. Bureau of Labor Statistics",
            fetch_mode=ConnectorFetchMode.html,
            max_items=10,
            asset_scope=["NDX"],
            tags=["macro", "inflation", "usa"],
        )
    )

    assert result.status == ConnectorStatus.ok
    assert result.fetched_items == 4
    assert result.accepted_items == 4
    assert result.next_cursor is not None
    assert len(items) == 4

    assert items[0].source == IntelligenceSourceType.macro_release
    assert items[0].event_type == MarketEventType.macro
    assert items[0].importance == IntelligenceImportance.critical
    assert items[0].title == "Employment Situation"
    assert "bls" in items[0].tags
    assert "nfp" in items[0].tags
    assert "employment" in items[0].tags
    assert items[0].asset_scope == ["NDX"]

    assert items[1].title == "Consumer Price Index"
    assert "cpi" in items[1].tags
    assert "inflation" in items[1].tags

    assert items[2].title == "Producer Price Index"
    assert items[2].importance == IntelligenceImportance.high
    assert "ppi" in items[2].tags

    assert items[3].title == "Job Openings and Labor Turnover Survey"
    assert "jolts" in items[3].tags


def test_bls_connector_rejects_non_bls_requests() -> None:
    connector = BLSConnector(fetcher=lambda url: SAMPLE_BLS_HTML)

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
    assert "source_kind='bls'" in result.errors[0]


def test_bls_connector_rejects_non_html_fetch_mode() -> None:
    connector = BLSConnector(fetcher=lambda url: SAMPLE_BLS_HTML)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.bls,
            source_name="U.S. Bureau of Labor Statistics",
            fetch_mode=ConnectorFetchMode.api,
        )
    )

    assert items == []
    assert result.status == ConnectorStatus.failed
    assert result.errors
    assert "fetch_mode='html'" in result.errors[0]

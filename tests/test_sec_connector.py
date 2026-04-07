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
from app.services.connectors.sec_connector import SECConnector


SAMPLE_SEC_JSON = {
    "name": "Apple Inc.",
    "tickers": ["AAPL"],
    "filings": {
        "recent": {
            "form": ["8-K", "10-Q", "S-8"],
            "filingDate": ["2026-03-27", "2026-02-01", "2026-01-10"],
            "acceptanceDateTime": [
                "20260327160521",
                "20260201123000",
                "20260110100000",
            ],
            "accessionNumber": [
                "0000320193-26-000045",
                "0000320193-26-000012",
                "0000320193-26-000001",
            ],
            "primaryDocument": ["aapl8k.htm", "aapl10q.htm", "aapls8.htm"],
            "primaryDocDescription": [
                "Current report under Item 2.02.",
                "Quarterly report.",
                "Registration statement.",
            ],
            "items": ["2.02,9.01", "", ""],
        }
    },
}


def test_sec_connector_fetch_parses_recent_filings() -> None:
    connector = SECConnector(fetcher=lambda url: SAMPLE_SEC_JSON)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.sec,
            source_name="SEC EDGAR",
            fetch_mode=ConnectorFetchMode.api,
            max_items=10,
            asset_scope=["NDX"],
            tags=["cik:0000320193", "ticker:aapl"],
        )
    )

    assert result.status == ConnectorStatus.ok
    assert result.fetched_items == 2
    assert result.accepted_items == 2
    assert result.next_cursor is not None
    assert len(items) == 2

    assert items[0].source == IntelligenceSourceType.news
    assert items[0].event_type == MarketEventType.headline
    assert items[0].importance == IntelligenceImportance.critical
    assert items[0].title == "Apple Inc. (AAPL) filed 8-K"
    assert items[0].asset_scope == ["NDX"]
    assert "sec" in items[0].tags
    assert "edgar" in items[0].tags
    assert "filings" in items[0].tags
    assert "8k" in items[0].tags
    assert "material_event" in items[0].tags
    assert "ticker:aapl" in items[0].tags
    assert items[0].source_url is not None
    assert items[0].source_url.endswith("/aapl8k.htm")
    assert items[0].occurred_at_utc is not None
    assert items[0].structured_payload["form"] == "8-K"

    assert items[1].importance == IntelligenceImportance.medium
    assert "10q" in items[1].tags
    assert items[1].structured_payload["form"] == "10-Q"


def test_sec_connector_rejects_non_sec_requests() -> None:
    connector = SECConnector(fetcher=lambda url: SAMPLE_SEC_JSON)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.fed,
            source_name="Federal Reserve",
            fetch_mode=ConnectorFetchMode.api,
            tags=["cik:0000320193"],
        )
    )

    assert items == []
    assert result.status == ConnectorStatus.failed
    assert result.errors
    assert "source_kind='sec'" in result.errors[0]


def test_sec_connector_rejects_non_api_fetch_mode() -> None:
    connector = SECConnector(fetcher=lambda url: SAMPLE_SEC_JSON)

    items, result = connector.fetch(
        ConnectorFetchRequest(
            source_kind=ConnectorSourceKind.sec,
            source_name="SEC EDGAR",
            fetch_mode=ConnectorFetchMode.rss,
            tags=["cik:0000320193"],
        )
    )

    assert items == []
    assert result.status == ConnectorStatus.failed
    assert result.errors
    assert "fetch_mode='api'" in result.errors[0]

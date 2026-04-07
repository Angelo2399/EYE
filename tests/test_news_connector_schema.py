from __future__ import annotations

from app.schemas.news_connector import (
    ConnectorCursor,
    ConnectorFetchMode,
    ConnectorFetchRequest,
    ConnectorFetchResult,
    ConnectorSourceKind,
    ConnectorStatus,
)


def test_connector_fetch_request_builds_with_defaults() -> None:
    request = ConnectorFetchRequest(
        source_kind=ConnectorSourceKind.fed,
        source_name="Federal Reserve",
        fetch_mode=ConnectorFetchMode.rss,
    )

    assert request.source_kind == ConnectorSourceKind.fed
    assert request.fetch_mode == ConnectorFetchMode.rss
    assert request.max_items == 50
    assert request.cursor is None
    assert request.asset_scope == []
    assert request.tags == []


def test_connector_fetch_result_supports_cursor_and_status() -> None:
    result = ConnectorFetchResult(
        source_kind=ConnectorSourceKind.ecb,
        source_name="ECB",
        status=ConnectorStatus.degraded,
        fetched_items=12,
        accepted_items=8,
        next_cursor=ConnectorCursor(
            last_seen_id="abc-123",
            last_seen_timestamp_utc="2026-03-27T12:00:00+00:00",
            extra_state={"page": 2},
        ),
        warnings=["partial feed delay"],
    )

    assert result.status == ConnectorStatus.degraded
    assert result.fetched_items == 12
    assert result.accepted_items == 8
    assert result.next_cursor is not None
    assert result.next_cursor.last_seen_id == "abc-123"
    assert result.next_cursor.extra_state["page"] == 2
    assert result.warnings == ["partial feed delay"]

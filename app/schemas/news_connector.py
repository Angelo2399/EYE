from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class ConnectorStatus(str, Enum):
    ok = "ok"
    degraded = "degraded"
    failed = "failed"


class ConnectorSourceKind(str, Enum):
    sec = "sec"
    nasdaq = "nasdaq"
    cboe = "cboe"
    sp_dji = "sp_dji"
    bls = "bls"
    bea = "bea"
    treasury = "treasury"
    fed = "fed"
    ecb = "ecb"
    white_house = "white_house"
    opec = "opec"
    eia = "eia"
    iea = "iea"
    cftc = "cftc"
    manual = "manual"
    custom = "custom"


class ConnectorFetchMode(str, Enum):
    poll = "poll"
    rss = "rss"
    api = "api"
    html = "html"
    live_stream = "live_stream"


class ConnectorCursor(BaseModel):
    last_seen_id: str | None = None
    last_seen_timestamp_utc: str | None = None
    extra_state: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class ConnectorFetchRequest(BaseModel):
    source_kind: ConnectorSourceKind
    source_name: str
    fetch_mode: ConnectorFetchMode
    max_items: int = Field(default=50, ge=1, le=500)
    cursor: ConnectorCursor | None = None
    asset_scope: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)


class ConnectorFetchResult(BaseModel):
    source_kind: ConnectorSourceKind
    source_name: str
    status: ConnectorStatus = ConnectorStatus.ok
    fetched_items: int = Field(default=0, ge=0)
    accepted_items: int = Field(default=0, ge=0)
    next_cursor: ConnectorCursor | None = None
    warnings: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)

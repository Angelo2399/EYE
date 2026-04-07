from __future__ import annotations

from collections.abc import Callable
from html.parser import HTMLParser
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from app.schemas.market_intelligence import (
    IntelligenceDirection,
    IntelligenceImportance,
    IntelligenceSourceType,
    MarketEventType,
    MarketIntelligenceItem,
)
from app.schemas.news_connector import (
    ConnectorCursor,
    ConnectorFetchMode,
    ConnectorFetchRequest,
    ConnectorFetchResult,
    ConnectorSourceKind,
    ConnectorStatus,
)
from app.services.connectors.base_connector import BaseIntelligenceConnector


class _SPDJIListingParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self._current_href: str | None = None
        self._current_text_parts: list[str] = []
        self.entries: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return

        attributes = dict(attrs)
        href = (attributes.get("href") or "").strip()
        if not href:
            return

        self._current_href = urljoin(self.base_url, href)
        self._current_text_parts = []

    def handle_data(self, data: str) -> None:
        if self._current_href is None:
            return

        text = data.strip()
        if text:
            self._current_text_parts.append(text)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a":
            return

        if self._current_href is None:
            return

        title = " ".join(self._current_text_parts).strip()
        if title:
            self.entries.append(
                {
                    "title": title,
                    "url": self._current_href,
                }
            )

        self._current_href = None
        self._current_text_parts = []

    def deduplicated_entries(self) -> list[dict[str, str]]:
        seen: set[tuple[str, str]] = set()
        unique: list[dict[str, str]] = []

        for entry in self.entries:
            title = str(entry.get("title") or "").strip()
            url = str(entry.get("url") or "").strip()

            if not title or not url:
                continue

            key = (title.lower(), url.lower())
            if key in seen:
                continue

            seen.add(key)
            unique.append({"title": title, "url": url})

        return unique


class SPDJIConnector(BaseIntelligenceConnector):
    def __init__(
        self,
        *,
        page_url: str = "https://www.spglobal.com/spdji/en/media-center/news-announcements/",
        source_name: str = "S&P Dow Jones Indices",
        fetcher: Callable[[str], str] | None = None,
    ) -> None:
        self._page_url = page_url
        self._source_name = source_name
        self._fetcher = fetcher or self._default_fetcher

    @property
    def source_name(self) -> str:
        return self._source_name

    def fetch(
        self,
        request: ConnectorFetchRequest,
    ) -> tuple[list[MarketIntelligenceItem], ConnectorFetchResult]:
        try:
            self._validate_request(request)

            html_text = self._fetcher(self._page_url)
            items = self._parse_html(
                html_text=html_text,
                max_items=request.max_items,
                request_tags=request.tags,
                request_asset_scope=request.asset_scope,
            )

            next_cursor = self._build_next_cursor(items)
            result = ConnectorFetchResult(
                source_kind=ConnectorSourceKind.sp_dji,
                source_name=self.source_name,
                status=ConnectorStatus.ok,
                fetched_items=len(items),
                accepted_items=len(items),
                next_cursor=next_cursor,
                warnings=[] if items else ["No usable S&P Dow Jones Indices items were found."],
                errors=[],
            )
            return items, result

        except Exception as exc:
            result = ConnectorFetchResult(
                source_kind=ConnectorSourceKind.sp_dji,
                source_name=self.source_name,
                status=ConnectorStatus.failed,
                fetched_items=0,
                accepted_items=0,
                next_cursor=request.cursor,
                warnings=[],
                errors=[str(exc)],
            )
            return [], result

    def _validate_request(self, request: ConnectorFetchRequest) -> None:
        if request.source_kind != ConnectorSourceKind.sp_dji:
            raise ValueError("SPDJIConnector only supports source_kind='sp_dji'.")

        if request.fetch_mode != ConnectorFetchMode.html:
            raise ValueError("SPDJIConnector only supports fetch_mode='html'.")

    def _default_fetcher(self, url: str) -> str:
        http_request = Request(
            url,
            headers={
                "User-Agent": "EYE/1.0 (market-intelligence; +local-project)"
            },
        )
        with urlopen(http_request, timeout=15) as response:
            return response.read().decode("utf-8", errors="replace")

    def _parse_html(
        self,
        *,
        html_text: str,
        max_items: int,
        request_tags: list[str],
        request_asset_scope: list[str],
    ) -> list[MarketIntelligenceItem]:
        parser = _SPDJIListingParser(self._page_url)
        parser.feed(html_text)

        parsed_items: list[MarketIntelligenceItem] = []

        for entry in parser.deduplicated_entries():
            title = str(entry.get("title") or "").strip()
            url = str(entry.get("url") or "").strip()

            if not self._looks_relevant(title=title, url=url):
                continue

            tags = self._build_tags(
                title=title,
                request_tags=request_tags,
                request_asset_scope=request_asset_scope,
            )

            parsed_items.append(
                MarketIntelligenceItem(
                    source=IntelligenceSourceType.news,
                    event_type=MarketEventType.headline,
                    importance=self._infer_importance(title),
                    direction=self._infer_direction(title),
                    title=title,
                    summary=title,
                    source_name=self.source_name,
                    source_url=url,
                    relevance_score=self._infer_relevance_score(title),
                    confidence_pct=72.0,
                    tags=tags,
                    raw_text=title,
                )
            )

            if len(parsed_items) >= max_items:
                break

        return parsed_items

    def _looks_relevant(self, *, title: str, url: str) -> bool:
        text = f"{title} {url}".lower()

        positive_markers = (
            "s&p 500",
            "the 500",
            "index announcement",
            "index announcements",
            "index launches",
            "index launch",
            "rebalancing",
            "reconstitution",
            "join s&p 500",
            "set to join s&p 500",
            "removed from s&p 500",
            "licen",
            "market",
        )

        negative_markers = (
            "contact us",
            "register / login",
            "careers",
            "our culture",
            "our commitment",
        )

        if any(marker in text for marker in negative_markers):
            return False

        return any(marker in text for marker in positive_markers)

    def _build_tags(
        self,
        *,
        title: str,
        request_tags: list[str],
        request_asset_scope: list[str],
    ) -> list[str]:
        tags = {str(tag).strip() for tag in request_tags if str(tag).strip()}

        tags.update(
            {
                "usa",
                "equity_index",
                "broad_market",
                "index_provider",
                "spx",
            }
        )

        for scope in request_asset_scope:
            normalized_scope = str(scope).strip().lower()
            if normalized_scope:
                tags.add(normalized_scope)

        title_lower = title.lower()

        keyword_tags = {
            "s&p 500": "sp500",
            "the 500": "sp500",
            "index announcement": "index_announcement",
            "index announcements": "index_announcement",
            "index launch": "index_launch",
            "index launches": "index_launch",
            "rebalancing": "rebalancing",
            "reconstitution": "reconstitution",
            "join s&p 500": "component_change",
            "removed from s&p 500": "component_change",
            "licen": "licensing",
            "market": "market_structure",
        }

        for keyword, tag in keyword_tags.items():
            if keyword in title_lower:
                tags.add(tag)

        return sorted(tags)

    def _infer_importance(self, title: str) -> IntelligenceImportance:
        title_lower = title.lower()

        if any(
            marker in title_lower
            for marker in (
                "join s&p 500",
                "removed from s&p 500",
                "set to join s&p 500",
                "rebalancing",
                "reconstitution",
            )
        ):
            return IntelligenceImportance.high

        if any(
            marker in title_lower
            for marker in (
                "index announcement",
                "index launch",
                "licen",
            )
        ):
            return IntelligenceImportance.medium

        return IntelligenceImportance.low

    def _infer_direction(self, title: str) -> IntelligenceDirection:
        title_lower = title.lower()

        if "join s&p 500" in title_lower or "set to join s&p 500" in title_lower:
            return IntelligenceDirection.bullish

        if "removed from s&p 500" in title_lower:
            return IntelligenceDirection.bearish

        return IntelligenceDirection.neutral

    def _infer_relevance_score(self, title: str) -> float:
        title_lower = title.lower()

        if "s&p 500" in title_lower:
            return 92.0

        if "rebalancing" in title_lower or "reconstitution" in title_lower:
            return 88.0

        if "index announcement" in title_lower:
            return 82.0

        return 68.0

    def _build_next_cursor(
        self,
        items: list[MarketIntelligenceItem],
    ) -> ConnectorCursor | None:
        if not items:
            return None

        first_item = items[0]
        return ConnectorCursor(
            last_seen_id=first_item.title,
            last_seen_timestamp_utc=None,
        )

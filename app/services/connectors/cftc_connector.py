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


class _CFTCListingParser(HTMLParser):
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


class CFTCConnector(BaseIntelligenceConnector):
    def __init__(
        self,
        *,
        page_url: str = "https://www.cftc.gov/dea/futures/petroleum_lf.htm",
        source_name: str = "U.S. Commodity Futures Trading Commission",
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
                source_kind=ConnectorSourceKind.cftc,
                source_name=self.source_name,
                status=ConnectorStatus.ok,
                fetched_items=len(items),
                accepted_items=len(items),
                next_cursor=next_cursor,
                warnings=[] if items else ["No usable CFTC petroleum items were found."],
                errors=[],
            )
            return items, result

        except Exception as exc:
            result = ConnectorFetchResult(
                source_kind=ConnectorSourceKind.cftc,
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
        if request.source_kind != ConnectorSourceKind.cftc:
            raise ValueError("CFTCConnector only supports source_kind='cftc'.")

        if request.fetch_mode != ConnectorFetchMode.html:
            raise ValueError("CFTCConnector only supports fetch_mode='html'.")

    def _default_fetcher(self, url: str) -> str:
        http_request = Request(
            url,
            headers={"User-Agent": "EYE/1.0 (market-intelligence; +local-project)"},
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
        lines = [
            line.strip()
            for line in html_text.splitlines()
            if line.strip()
        ]

        parsed_items: list[MarketIntelligenceItem] = []
        seen_titles: set[str] = set()

        for line in lines:
            normalized = line.lower()

            if "wti" not in normalized and "crude oil" not in normalized:
                continue

            if "new york mercantile exchange" not in normalized and "nymex" not in normalized:
                continue

            title = line.strip()
            if title in seen_titles:
                continue
            seen_titles.add(title)

            direction = self._infer_direction(title=title)
            importance = self._infer_importance(title=title)
            tags = self._build_tags(title=title, request_tags=request_tags)

            parsed_items.append(
                MarketIntelligenceItem(
                    source=IntelligenceSourceType.macro_release,
                    event_type=MarketEventType.macro,
                    importance=importance,
                    direction=direction,
                    title=title,
                    summary="CFTC petroleum positioning report entry.",
                    source_name=self.source_name,
                    source_url=self._page_url,
                    occurred_at_utc=None,
                    detected_at_utc=None,
                    relevance_score=self._infer_relevance_score(importance=importance),
                    confidence_pct=self._infer_confidence_pct(
                        importance=importance,
                        direction=direction,
                    ),
                    impact_horizon_minutes=240,
                    asset_scope=request_asset_scope[:],
                    tags=tags,
                    raw_text=title,
                    structured_payload={
                        "page_url": self._page_url,
                    },
                )
            )

            if len(parsed_items) >= max_items:
                break

        return parsed_items

    def _build_next_cursor(
        self,
        items: list[MarketIntelligenceItem],
    ) -> ConnectorCursor | None:
        if not items:
            return None

        latest_item = items[-1]
        return ConnectorCursor(
            last_seen_id=latest_item.title,
            last_seen_timestamp_utc=latest_item.occurred_at_utc,
            extra_state={"source_name": self.source_name, "page_url": self._page_url},
        )

    def _infer_direction(self, *, title: str) -> IntelligenceDirection:
        text = title.lower()

        bullish_keywords = ["managed money long", "producer merchant short"]
        bearish_keywords = ["managed money short", "swap dealers short"]

        bullish_hits = sum(1 for keyword in bullish_keywords if keyword in text)
        bearish_hits = sum(1 for keyword in bearish_keywords if keyword in text)

        if bullish_hits > bearish_hits and bullish_hits > 0:
            return IntelligenceDirection.bullish
        if bearish_hits > bullish_hits and bearish_hits > 0:
            return IntelligenceDirection.bearish
        if bullish_hits > 0 and bearish_hits > 0:
            return IntelligenceDirection.mixed
        return IntelligenceDirection.neutral

    def _infer_importance(self, *, title: str) -> IntelligenceImportance:
        text = title.lower()

        if "wti" in text and "new york mercantile exchange" in text:
            return IntelligenceImportance.high
        return IntelligenceImportance.medium

    def _infer_relevance_score(self, *, importance: IntelligenceImportance) -> float:
        return {
            IntelligenceImportance.low: 35.0,
            IntelligenceImportance.medium: 58.0,
            IntelligenceImportance.high: 74.0,
            IntelligenceImportance.critical: 88.0,
        }[importance]

    def _infer_confidence_pct(
        self,
        *,
        importance: IntelligenceImportance,
        direction: IntelligenceDirection,
    ) -> float:
        base_confidence = {
            IntelligenceImportance.low: 40.0,
            IntelligenceImportance.medium: 55.0,
            IntelligenceImportance.high: 68.0,
            IntelligenceImportance.critical: 80.0,
        }[importance]

        if direction == IntelligenceDirection.mixed:
            return max(base_confidence - 10.0, 35.0)
        if direction == IntelligenceDirection.neutral:
            return max(base_confidence - 12.0, 40.0)
        return base_confidence

    def _build_tags(
        self,
        *,
        title: str,
        request_tags: list[str],
    ) -> list[str]:
        tags: set[str] = {"cftc", "cot", "petroleum"}

        text = title.lower()
        keyword_tags = {
            "wti": "wti",
            "crude oil": "crude_oil",
            "new york mercantile exchange": "nymex",
            "managed money": "managed_money",
            "producer merchant": "producer_merchant",
            "swap dealers": "swap_dealers",
        }

        for keyword, tag in keyword_tags.items():
            if keyword in text:
                tags.add(tag)

        for tag in request_tags:
            normalized = str(tag).strip().lower()
            if normalized:
                tags.add(normalized)

        return sorted(tags)

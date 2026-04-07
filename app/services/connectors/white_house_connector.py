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


class _WhiteHouseListingParser(HTMLParser):
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


class WhiteHouseConnector(BaseIntelligenceConnector):
    def __init__(
        self,
        *,
        page_url: str = "https://www.whitehouse.gov/briefings-statements/",
        source_name: str = "White House",
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
                source_kind=ConnectorSourceKind.white_house,
                source_name=self.source_name,
                status=ConnectorStatus.ok,
                fetched_items=len(items),
                accepted_items=len(items),
                next_cursor=next_cursor,
                warnings=[] if items else ["No usable White House items were found."],
                errors=[],
            )
            return items, result

        except Exception as exc:
            result = ConnectorFetchResult(
                source_kind=ConnectorSourceKind.white_house,
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
        if request.source_kind != ConnectorSourceKind.white_house:
            raise ValueError(
                "WhiteHouseConnector only supports source_kind='white_house'."
            )

        if request.fetch_mode != ConnectorFetchMode.html:
            raise ValueError("WhiteHouseConnector only supports fetch_mode='html'.")

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
        parser = _WhiteHouseListingParser(base_url=self._page_url)
        parser.feed(html_text)

        parsed_items: list[MarketIntelligenceItem] = []
        seen_urls: set[str] = set()

        for entry in parser.entries:
            title = entry["title"].strip()
            url = entry["url"].strip()

            if not self._is_candidate_entry(title=title, url=url):
                continue

            if url in seen_urls:
                continue
            seen_urls.add(url)

            event_type = self._infer_event_type(title=title, url=url)
            direction = self._infer_direction(title=title)
            importance = self._infer_importance(title=title, event_type=event_type)
            tags = self._build_tags(title=title, url=url, request_tags=request_tags)

            parsed_items.append(
                MarketIntelligenceItem(
                    source=self._infer_source_type(event_type),
                    event_type=event_type,
                    importance=importance,
                    direction=direction,
                    title=title,
                    summary="",
                    source_name=self.source_name,
                    source_url=url,
                    occurred_at_utc=None,
                    detected_at_utc=None,
                    relevance_score=self._infer_relevance_score(
                        importance=importance,
                        event_type=event_type,
                    ),
                    confidence_pct=self._infer_confidence_pct(
                        importance=importance,
                        direction=direction,
                    ),
                    impact_horizon_minutes=self._infer_impact_horizon_minutes(
                        event_type=event_type,
                        importance=importance,
                    ),
                    asset_scope=request_asset_scope[:],
                    tags=tags,
                    raw_text=title,
                    structured_payload={
                        "page_url": self._page_url,
                        "entry_url": url,
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
            last_seen_id=latest_item.source_url,
            last_seen_timestamp_utc=latest_item.occurred_at_utc,
            extra_state={"source_name": self.source_name, "page_url": self._page_url},
        )

    def _is_candidate_entry(self, *, title: str, url: str) -> bool:
        normalized_title = title.strip().lower()
        normalized_url = url.strip().lower()

        if len(normalized_title) < 8:
            return False

        blocked_titles = {
            "read the latest",
            "featured",
            "explore more videos",
            "all",
            "releases",
            "remarks",
            "research",
        }
        if normalized_title in blocked_titles:
            return False

        allowed_url_parts = (
            "/briefings-statements/",
            "/presidential-actions/",
            "/fact-sheets/",
            "/videos/",
            "/live/",
        )
        return any(part in normalized_url for part in allowed_url_parts)

    def _infer_source_type(
        self,
        event_type: MarketEventType,
    ) -> IntelligenceSourceType:
        if event_type in {MarketEventType.speech, MarketEventType.transcript_segment}:
            return IntelligenceSourceType.speech
        return IntelligenceSourceType.news

    def _infer_event_type(
        self,
        *,
        title: str,
        url: str,
    ) -> MarketEventType:
        text = f"{title} {url}".lower()

        if "transcript" in text or "q&a" in text or "questions and answers" in text:
            return MarketEventType.transcript_segment

        if any(
            keyword in text
            for keyword in [
                "remarks",
                "speech",
                "address",
                "briefing",
                "delivers remarks",
                "announcement",
            ]
        ):
            return MarketEventType.speech

        return MarketEventType.headline

    def _infer_direction(self, *, title: str) -> IntelligenceDirection:
        text = title.lower()

        bullish_keywords = [
            "growth",
            "investment",
            "jobs",
            "progress",
            "agreement",
            "strengthens",
            "support",
            "economy",
        ]
        bearish_keywords = [
            "tariff",
            "sanctions",
            "emergency",
            "crisis",
            "war",
            "threat",
            "inflation",
        ]

        bullish_hits = sum(1 for keyword in bullish_keywords if keyword in text)
        bearish_hits = sum(1 for keyword in bearish_keywords if keyword in text)

        if bullish_hits > bearish_hits and bullish_hits > 0:
            return IntelligenceDirection.bullish
        if bearish_hits > bullish_hits and bearish_hits > 0:
            return IntelligenceDirection.bearish
        if bullish_hits > 0 and bearish_hits > 0:
            return IntelligenceDirection.mixed
        return IntelligenceDirection.neutral

    def _infer_importance(
        self,
        *,
        title: str,
        event_type: MarketEventType,
    ) -> IntelligenceImportance:
        text = title.lower()

        if any(
            keyword in text
            for keyword in [
                "address to the nation",
                "state of the union",
                "executive order",
                "emergency",
                "tariff",
                "sanctions",
            ]
        ):
            return IntelligenceImportance.critical

        if event_type in {MarketEventType.speech, MarketEventType.transcript_segment}:
            return IntelligenceImportance.high

        return IntelligenceImportance.medium

    def _infer_relevance_score(
        self,
        *,
        importance: IntelligenceImportance,
        event_type: MarketEventType,
    ) -> float:
        base_score = {
            IntelligenceImportance.low: 35.0,
            IntelligenceImportance.medium: 56.0,
            IntelligenceImportance.high: 70.0,
            IntelligenceImportance.critical: 86.0,
        }[importance]

        if event_type in {MarketEventType.speech, MarketEventType.transcript_segment}:
            return min(base_score + 4.0, 100.0)
        return base_score

    def _infer_confidence_pct(
        self,
        *,
        importance: IntelligenceImportance,
        direction: IntelligenceDirection,
    ) -> float:
        base_confidence = {
            IntelligenceImportance.low: 40.0,
            IntelligenceImportance.medium: 54.0,
            IntelligenceImportance.high: 66.0,
            IntelligenceImportance.critical: 76.0,
        }[importance]

        if direction == IntelligenceDirection.mixed:
            return max(base_confidence - 10.0, 35.0)
        if direction == IntelligenceDirection.neutral:
            return max(base_confidence - 8.0, 40.0)
        return base_confidence

    def _infer_impact_horizon_minutes(
        self,
        *,
        event_type: MarketEventType,
        importance: IntelligenceImportance,
    ) -> int:
        if event_type in {MarketEventType.speech, MarketEventType.transcript_segment}:
            return 180
        if importance == IntelligenceImportance.critical:
            return 240
        if importance == IntelligenceImportance.high:
            return 120
        return 60

    def _build_tags(
        self,
        *,
        title: str,
        url: str,
        request_tags: list[str],
    ) -> list[str]:
        tags: set[str] = {"white_house"}

        normalized_url = url.lower()
        if "/briefings-statements/" in normalized_url:
            tags.add("briefings_statements")
        if "/presidential-actions/" in normalized_url:
            tags.add("presidential_actions")
        if "/fact-sheets/" in normalized_url:
            tags.add("fact_sheet")
        if "/videos/" in normalized_url or "/live/" in normalized_url:
            tags.add("video")

        text = title.lower()
        keyword_tags = {
            "president": "president",
            "trump": "president",
            "remarks": "remarks",
            "briefing": "briefing",
            "speech": "speech",
            "economy": "economy",
            "trade": "trade",
            "tariff": "tariffs",
            "sanctions": "sanctions",
            "farmers": "agriculture",
            "energy": "energy",
        }

        for keyword, tag in keyword_tags.items():
            if keyword in text:
                tags.add(tag)

        for tag in request_tags:
            normalized = str(tag).strip().lower()
            if normalized:
                tags.add(normalized)

        return sorted(tags)

from __future__ import annotations

from collections.abc import Callable
from email.utils import parsedate_to_datetime
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET

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


class NasdaqConnector(BaseIntelligenceConnector):
    def __init__(
        self,
        *,
        feed_url: str = "https://ir.nasdaq.com/rss/news-releases.xml",
        source_name: str = "Nasdaq",
        fetcher: Callable[[str], str] | None = None,
    ) -> None:
        self._feed_url = feed_url
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
            xml_text = self._fetcher(self._feed_url)
            items = self._parse_rss(
                xml_text=xml_text,
                max_items=request.max_items,
                request_tags=request.tags,
                request_asset_scope=request.asset_scope,
            )

            next_cursor = self._build_next_cursor(items)
            result = ConnectorFetchResult(
                source_kind=ConnectorSourceKind.nasdaq,
                source_name=self.source_name,
                status=ConnectorStatus.ok,
                fetched_items=len(items),
                accepted_items=len(items),
                next_cursor=next_cursor,
                warnings=[] if items else ["No Nasdaq RSS items were returned by the feed."],
                errors=[],
            )
            return items, result

        except Exception as exc:
            result = ConnectorFetchResult(
                source_kind=ConnectorSourceKind.nasdaq,
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
        if request.source_kind != ConnectorSourceKind.nasdaq:
            raise ValueError("NasdaqConnector only supports source_kind='nasdaq'.")

        if request.fetch_mode != ConnectorFetchMode.rss:
            raise ValueError("NasdaqConnector only supports fetch_mode='rss'.")

    def _default_fetcher(self, url: str) -> str:
        http_request = Request(
            url,
            headers={"User-Agent": "EYE/1.0 (market-intelligence; +local-project)"},
        )
        with urlopen(http_request, timeout=15) as response:
            return response.read().decode("utf-8", errors="replace")

    def _parse_rss(
        self,
        *,
        xml_text: str,
        max_items: int,
        request_tags: list[str],
        request_asset_scope: list[str],
    ) -> list[MarketIntelligenceItem]:
        root = ET.fromstring(xml_text)
        channel = root.find("channel")
        if channel is None:
            return []

        parsed_items: list[MarketIntelligenceItem] = []

        for item_element in channel.findall("item")[:max_items]:
            title = self._child_text(item_element, "title")
            link = self._child_text(item_element, "link")
            description = self._child_text(item_element, "description")
            pub_date_raw = self._child_text(item_element, "pubDate")
            categories = [
                (category.text or "").strip()
                for category in item_element.findall("category")
                if (category.text or "").strip()
            ]

            importance = self._infer_importance(title=title, description=description)
            direction = self._infer_direction(title=title, description=description)
            tags = self._build_tags(
                title=title,
                description=description,
                categories=categories,
                request_tags=request_tags,
            )

            parsed_items.append(
                MarketIntelligenceItem(
                    source=IntelligenceSourceType.news,
                    event_type=MarketEventType.headline,
                    importance=importance,
                    direction=direction,
                    title=title,
                    summary=description,
                    source_name=self.source_name,
                    source_url=link or None,
                    occurred_at_utc=self._parse_pub_date(pub_date_raw),
                    detected_at_utc=None,
                    relevance_score=self._infer_relevance_score(importance=importance),
                    confidence_pct=self._infer_confidence_pct(
                        importance=importance,
                        direction=direction,
                    ),
                    impact_horizon_minutes=self._infer_impact_horizon_minutes(
                        importance=importance
                    ),
                    asset_scope=request_asset_scope[:],
                    tags=tags,
                    raw_text=description or title or None,
                    structured_payload={
                        "pub_date_raw": pub_date_raw or None,
                        "link": link or None,
                        "category_count": len(categories),
                    },
                )
            )

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
            extra_state={"source_name": self.source_name},
        )

    def _child_text(self, parent: ET.Element, tag: str) -> str:
        child = parent.find(tag)
        if child is None or child.text is None:
            return ""
        return child.text.strip()

    def _parse_pub_date(self, pub_date_raw: str) -> str | None:
        if not pub_date_raw:
            return None

        try:
            return parsedate_to_datetime(pub_date_raw).isoformat()
        except Exception:
            return None

    def _infer_importance(
        self,
        *,
        title: str,
        description: str,
    ) -> IntelligenceImportance:
        text = f"{title} {description}".lower()

        if any(
            keyword in text
            for keyword in ["earnings", "quarterly results", "annual results", "market structure"]
        ):
            return IntelligenceImportance.high

        return IntelligenceImportance.medium

    def _infer_direction(
        self,
        *,
        title: str,
        description: str,
    ) -> IntelligenceDirection:
        text = f"{title} {description}".lower()

        bullish_keywords = [
            "record revenue",
            "strong growth",
            "higher revenue",
            "beat expectations",
            "expansion",
        ]
        bearish_keywords = [
            "lower revenue",
            "missed expectations",
            "decline",
            "weaker volumes",
            "restructuring",
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

    def _infer_relevance_score(self, *, importance: IntelligenceImportance) -> float:
        return {
            IntelligenceImportance.low: 35.0,
            IntelligenceImportance.medium: 60.0,
            IntelligenceImportance.high: 76.0,
            IntelligenceImportance.critical: 90.0,
        }[importance]

    def _infer_confidence_pct(
        self,
        *,
        importance: IntelligenceImportance,
        direction: IntelligenceDirection,
    ) -> float:
        base_confidence = {
            IntelligenceImportance.low: 40.0,
            IntelligenceImportance.medium: 56.0,
            IntelligenceImportance.high: 68.0,
            IntelligenceImportance.critical: 80.0,
        }[importance]

        if direction == IntelligenceDirection.mixed:
            return max(base_confidence - 10.0, 35.0)
        if direction == IntelligenceDirection.neutral:
            return max(base_confidence - 8.0, 40.0)
        return base_confidence

    def _infer_impact_horizon_minutes(self, *, importance: IntelligenceImportance) -> int:
        if importance == IntelligenceImportance.high:
            return 240
        return 120

    def _build_tags(
        self,
        *,
        title: str,
        description: str,
        categories: list[str],
        request_tags: list[str],
    ) -> list[str]:
        tags: set[str] = {"nasdaq"}

        for category in categories:
            normalized = category.strip().lower().replace(" ", "_")
            if normalized:
                tags.add(normalized)

        text = f"{title} {description}".lower()
        keyword_tags = {
            "earnings": "earnings",
            "quarterly results": "earnings",
            "annual results": "earnings",
            "market structure": "market_structure",
            "index": "equity_index",
            "equities": "equity_index",
            "technology": "tech",
            "tech": "tech",
            "growth": "growth",
        }

        for keyword, tag in keyword_tags.items():
            if keyword in text:
                tags.add(tag)

        for tag in request_tags:
            normalized = str(tag).strip().lower()
            if normalized:
                tags.add(normalized)

        return sorted(tags)

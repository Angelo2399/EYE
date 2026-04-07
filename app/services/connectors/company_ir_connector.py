from __future__ import annotations

from collections.abc import Callable
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from urllib.parse import urljoin
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


class _CompanyIRListingParser(HTMLParser):
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


class CompanyIRConnector(BaseIntelligenceConnector):
    def __init__(
        self,
        *,
        source_name: str = "Company Investor Relations",
        feed_url: str | None = None,
        page_url: str | None = None,
        company_ticker: str | None = None,
        fetcher: Callable[[str], str] | None = None,
    ) -> None:
        self._source_name = source_name
        self._feed_url = feed_url
        self._page_url = page_url
        self._company_ticker = company_ticker
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
            company_ticker = self._resolve_company_ticker(request)

            if request.fetch_mode == ConnectorFetchMode.rss:
                feed_url = self._resolve_feed_url(request)
                xml_text = self._fetcher(feed_url)
                items = self._parse_rss(
                    xml_text=xml_text,
                    max_items=request.max_items,
                    request_tags=request.tags,
                    request_asset_scope=request.asset_scope,
                    company_ticker=company_ticker,
                )
            else:
                page_url = self._resolve_page_url(request)
                html_text = self._fetcher(page_url)
                items = self._parse_html(
                    html_text=html_text,
                    max_items=request.max_items,
                    request_tags=request.tags,
                    request_asset_scope=request.asset_scope,
                    company_ticker=company_ticker,
                    page_url=page_url,
                )

            next_cursor = self._build_next_cursor(items=items, company_ticker=company_ticker)
            result = ConnectorFetchResult(
                source_kind=ConnectorSourceKind.custom,
                source_name=self.source_name,
                status=ConnectorStatus.ok,
                fetched_items=len(items),
                accepted_items=len(items),
                next_cursor=next_cursor,
                warnings=[] if items else ["No company IR items were found."],
                errors=[],
            )
            return items, result

        except Exception as exc:
            result = ConnectorFetchResult(
                source_kind=ConnectorSourceKind.custom,
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
        if request.source_kind != ConnectorSourceKind.custom:
            raise ValueError("CompanyIRConnector only supports source_kind='custom'.")

        if request.fetch_mode not in {ConnectorFetchMode.rss, ConnectorFetchMode.html}:
            raise ValueError("CompanyIRConnector only supports fetch_mode='rss' or 'html'.")

    def _resolve_feed_url(self, request: ConnectorFetchRequest) -> str:
        if self._feed_url:
            return self._feed_url

        if request.cursor is not None:
            value = request.cursor.extra_state.get("feed_url")
            if value:
                return str(value)

        raise ValueError("CompanyIRConnector requires a configured feed_url for RSS mode.")

    def _resolve_page_url(self, request: ConnectorFetchRequest) -> str:
        if self._page_url:
            return self._page_url

        if request.cursor is not None:
            value = request.cursor.extra_state.get("page_url")
            if value:
                return str(value)

        raise ValueError("CompanyIRConnector requires a configured page_url for HTML mode.")

    def _resolve_company_ticker(self, request: ConnectorFetchRequest) -> str | None:
        if self._company_ticker:
            return str(self._company_ticker).strip().upper()

        if request.cursor is not None:
            value = request.cursor.extra_state.get("company_ticker")
            if value:
                return str(value).strip().upper()

        return None

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
        company_ticker: str | None,
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

            parsed_items.append(
                MarketIntelligenceItem(
                    source=IntelligenceSourceType.news,
                    event_type=MarketEventType.headline,
                    importance=self._infer_importance(title=title, description=description),
                    direction=self._infer_direction(title=title, description=description),
                    title=title,
                    summary=description,
                    source_name=self.source_name,
                    source_url=link or None,
                    occurred_at_utc=self._parse_pub_date(pub_date_raw),
                    detected_at_utc=None,
                    relevance_score=self._infer_relevance_score(title=title, description=description),
                    confidence_pct=self._infer_confidence_pct(title=title, description=description),
                    impact_horizon_minutes=self._infer_impact_horizon_minutes(
                        title=title,
                        description=description,
                    ),
                    asset_scope=request_asset_scope[:],
                    tags=self._build_tags(
                        title=title,
                        description=description,
                        request_tags=request_tags,
                        company_ticker=company_ticker,
                    ),
                    raw_text=description or title or None,
                    structured_payload={
                        "company_ticker": company_ticker,
                        "pub_date_raw": pub_date_raw or None,
                    },
                )
            )

        return parsed_items

    def _parse_html(
        self,
        *,
        html_text: str,
        max_items: int,
        request_tags: list[str],
        request_asset_scope: list[str],
        company_ticker: str | None,
        page_url: str,
    ) -> list[MarketIntelligenceItem]:
        parser = _CompanyIRListingParser(base_url=page_url)
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

            parsed_items.append(
                MarketIntelligenceItem(
                    source=IntelligenceSourceType.news,
                    event_type=MarketEventType.headline,
                    importance=self._infer_importance(title=title, description=""),
                    direction=self._infer_direction(title=title, description=""),
                    title=title,
                    summary="Official company IR release.",
                    source_name=self.source_name,
                    source_url=url,
                    occurred_at_utc=None,
                    detected_at_utc=None,
                    relevance_score=self._infer_relevance_score(title=title, description=""),
                    confidence_pct=self._infer_confidence_pct(title=title, description=""),
                    impact_horizon_minutes=self._infer_impact_horizon_minutes(
                        title=title,
                        description="",
                    ),
                    asset_scope=request_asset_scope[:],
                    tags=self._build_tags(
                        title=title,
                        description="",
                        request_tags=request_tags,
                        company_ticker=company_ticker,
                    ),
                    raw_text=title,
                    structured_payload={
                        "company_ticker": company_ticker,
                        "page_url": page_url,
                    },
                )
            )

            if len(parsed_items) >= max_items:
                break

        return parsed_items

    def _build_next_cursor(
        self,
        *,
        items: list[MarketIntelligenceItem],
        company_ticker: str | None,
    ) -> ConnectorCursor | None:
        if not items:
            return None

        latest_item = items[-1]
        return ConnectorCursor(
            last_seen_id=latest_item.source_url,
            last_seen_timestamp_utc=latest_item.occurred_at_utc,
            extra_state={
                "source_name": self.source_name,
                "company_ticker": company_ticker,
            },
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

    def _is_candidate_entry(self, *, title: str, url: str) -> bool:
        normalized_title = title.lower()
        normalized_url = url.lower()

        if len(normalized_title) < 10:
            return False

        candidate_keywords = [
            "earnings",
            "guidance",
            "product",
            "buyback",
            "repurchase",
            "cfo",
            "ceo",
            "press",
            "news",
            "release",
            "results",
        ]

        return any(
            keyword in normalized_title or keyword in normalized_url
            for keyword in candidate_keywords
        )

    def _infer_importance(self, *, title: str, description: str) -> IntelligenceImportance:
        text = f"{title} {description}".lower()

        if any(keyword in text for keyword in ["earnings", "guidance", "buyback", "repurchase"]):
            return IntelligenceImportance.high

        if any(keyword in text for keyword in ["ceo", "cfo", "product", "results"]):
            return IntelligenceImportance.medium

        return IntelligenceImportance.medium

    def _infer_direction(self, *, title: str, description: str) -> IntelligenceDirection:
        text = f"{title} {description}".lower()

        bullish_keywords = [
            "record",
            "growth",
            "raise guidance",
            "expands",
            "launches",
            "buyback",
            "repurchase",
        ]
        bearish_keywords = [
            "lower guidance",
            "decline",
            "miss",
            "ceo transition",
            "cfo departure",
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

    def _infer_relevance_score(self, *, title: str, description: str) -> float:
        importance = self._infer_importance(title=title, description=description)
        return {
            IntelligenceImportance.low: 35.0,
            IntelligenceImportance.medium: 60.0,
            IntelligenceImportance.high: 78.0,
            IntelligenceImportance.critical: 90.0,
        }[importance]

    def _infer_confidence_pct(self, *, title: str, description: str) -> float:
        importance = self._infer_importance(title=title, description=description)
        return {
            IntelligenceImportance.low: 40.0,
            IntelligenceImportance.medium: 58.0,
            IntelligenceImportance.high: 72.0,
            IntelligenceImportance.critical: 84.0,
        }[importance]

    def _infer_impact_horizon_minutes(self, *, title: str, description: str) -> int:
        importance = self._infer_importance(title=title, description=description)
        if importance == IntelligenceImportance.high:
            return 240
        return 120

    def _build_tags(
        self,
        *,
        title: str,
        description: str,
        request_tags: list[str],
        company_ticker: str | None,
    ) -> list[str]:
        tags: set[str] = {"company_ir", "official_release"}
        text = f"{title} {description}".lower()

        keyword_tags = {
            "earnings": "earnings",
            "results": "earnings",
            "guidance": "guidance",
            "product": "product",
            "launch": "product",
            "buyback": "buyback",
            "repurchase": "buyback",
            "cfo": "cfo",
            "ceo": "ceo",
        }

        for keyword, tag in keyword_tags.items():
            if keyword in text:
                tags.add(tag)

        for tag in request_tags:
            normalized = str(tag).strip().lower()
            if normalized:
                tags.add(normalized)

        if company_ticker:
            tags.add(company_ticker.lower())

        return sorted(tags)

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


class _CboeListingParser(HTMLParser):
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


class CboeConnector(BaseIntelligenceConnector):
    def __init__(
        self,
        *,
        page_url: str = "https://www.cboe.com/insights/categories/volatility_insights/",
        source_name: str = "Cboe",
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
                source_kind=ConnectorSourceKind.cboe,
                source_name=self.source_name,
                status=ConnectorStatus.ok,
                fetched_items=len(items),
                accepted_items=len(items),
                next_cursor=next_cursor,
                warnings=[] if items else ["No usable Cboe items were found."],
                errors=[],
            )
            return items, result

        except Exception as exc:
            result = ConnectorFetchResult(
                source_kind=ConnectorSourceKind.cboe,
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
        if request.source_kind != ConnectorSourceKind.cboe:
            raise ValueError("CboeConnector only supports source_kind='cboe'.")

        if request.fetch_mode != ConnectorFetchMode.html:
            raise ValueError("CboeConnector only supports fetch_mode='html'.")

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
        parser = _CboeListingParser(self._page_url)
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
                    confidence_pct=74.0,
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
            "spx",
            "s&p 500",
            "vix",
            "volatility",
            "0dte",
            "index options",
            "index insights",
            "skew",
            "convexity",
        )

        negative_markers = (
            "contact",
            "login",
            "careers",
            "about us",
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
                "options",
                "volatility",
                "spx",
                "cboe",
            }
        )

        for scope in request_asset_scope:
            normalized_scope = str(scope).strip().lower()
            if normalized_scope:
                tags.add(normalized_scope)

        title_lower = title.lower()

        keyword_tags = {
            "spx": "spx",
            "s&p 500": "sp500",
            "vix": "vix",
            "volatility": "volatility_regime",
            "0dte": "0dte",
            "skew": "skew",
            "convexity": "convexity",
            "index options": "index_options",
        }

        for keyword, tag in keyword_tags.items():
            if keyword in title_lower:
                tags.add(tag)

        return sorted(tags)

    def _infer_importance(self, title: str) -> IntelligenceImportance:
        text = title.lower()

        if any(
            keyword in text
            for keyword in ("vix", "spx", "0dte", "volatility", "convexity", "skew")
        ):
            return IntelligenceImportance.high

        return IntelligenceImportance.medium

    def _infer_direction(self, title: str) -> IntelligenceDirection:
        text = title.lower()

        bullish_keywords = ("call demand", "rebound", "optimism", "strength")
        bearish_keywords = ("stress", "drawdown", "panic", "sell-off", "hedging")

        bullish_hits = sum(1 for keyword in bullish_keywords if keyword in text)
        bearish_hits = sum(1 for keyword in bearish_keywords if keyword in text)

        if bullish_hits > bearish_hits and bullish_hits > 0:
            return IntelligenceDirection.bullish
        if bearish_hits > bullish_hits and bearish_hits > 0:
            return IntelligenceDirection.bearish
        if bullish_hits > 0 and bearish_hits > 0:
            return IntelligenceDirection.mixed
        return IntelligenceDirection.neutral

    def _infer_relevance_score(self, title: str) -> float:
        text = title.lower()

        if "spx" in text or "s&p 500" in text:
            return 90.0
        if "vix" in text or "volatility" in text:
            return 87.0
        if "0dte" in text:
            return 84.0
        return 72.0

    def _build_next_cursor(
        self,
        items: list[MarketIntelligenceItem],
    ) -> ConnectorCursor | None:
        if not items:
            return None

        latest_item = items[-1]
        return ConnectorCursor(
            last_seen_id=latest_item.source_url or latest_item.title,
            last_seen_timestamp_utc=latest_item.occurred_at_utc,
        )

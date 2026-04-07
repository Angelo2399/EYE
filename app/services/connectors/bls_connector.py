from __future__ import annotations

import re
from collections.abc import Callable
from html import unescape
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


class BLSConnector(BaseIntelligenceConnector):
    TARGET_RELEASES: tuple[tuple[str, str, IntelligenceImportance, list[str]], ...] = (
        (
            "employment situation",
            "Employment Situation",
            IntelligenceImportance.critical,
            ["nfp", "employment"],
        ),
        (
            "consumer price index",
            "Consumer Price Index",
            IntelligenceImportance.critical,
            ["cpi", "inflation"],
        ),
        (
            "producer price index",
            "Producer Price Index",
            IntelligenceImportance.high,
            ["ppi", "inflation"],
        ),
        (
            "job openings and labor turnover",
            "Job Openings and Labor Turnover Survey",
            IntelligenceImportance.high,
            ["jolts", "employment"],
        ),
    )

    def __init__(
        self,
        *,
        page_url: str = "https://www.bls.gov/schedule/news_release/current_year.htm",
        source_name: str = "U.S. Bureau of Labor Statistics",
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
            items = self._parse_schedule(
                html_text=html_text,
                max_items=request.max_items,
                request_tags=request.tags,
                request_asset_scope=request.asset_scope,
            )

            next_cursor = self._build_next_cursor(items)
            result = ConnectorFetchResult(
                source_kind=ConnectorSourceKind.bls,
                source_name=self.source_name,
                status=ConnectorStatus.ok,
                fetched_items=len(items),
                accepted_items=len(items),
                next_cursor=next_cursor,
                warnings=[] if items else ["No targeted BLS releases were found."],
                errors=[],
            )
            return items, result

        except Exception as exc:
            result = ConnectorFetchResult(
                source_kind=ConnectorSourceKind.bls,
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
        if request.source_kind != ConnectorSourceKind.bls:
            raise ValueError("BLSConnector only supports source_kind='bls'.")

        if request.fetch_mode != ConnectorFetchMode.html:
            raise ValueError("BLSConnector only supports fetch_mode='html'.")

    def _default_fetcher(self, url: str) -> str:
        http_request = Request(
            url,
            headers={"User-Agent": "EYE/1.0 (market-intelligence; +local-project)"},
        )
        with urlopen(http_request, timeout=15) as response:
            return response.read().decode("utf-8", errors="replace")

    def _parse_schedule(
        self,
        *,
        html_text: str,
        max_items: int,
        request_tags: list[str],
        request_asset_scope: list[str],
    ) -> list[MarketIntelligenceItem]:
        text = unescape(html_text)
        compact_text = re.sub(r"<[^>]+>", "\n", text)
        lines = [
            re.sub(r"\s+", " ", line).strip()
            for line in compact_text.splitlines()
            if re.sub(r"\s+", " ", line).strip()
        ]

        parsed_items: list[MarketIntelligenceItem] = []
        seen_titles: set[str] = set()

        for keyword, display_name, importance, extra_tags in self.TARGET_RELEASES:
            for index, line in enumerate(lines):
                normalized_line = line.lower()
                if keyword not in normalized_line:
                    continue

                if display_name in seen_titles:
                    break

                context = lines[max(0, index - 2) : min(len(lines), index + 3)]
                summary = " | ".join(context)
                title = display_name

                parsed_items.append(
                    MarketIntelligenceItem(
                        source=IntelligenceSourceType.macro_release,
                        event_type=MarketEventType.macro,
                        importance=importance,
                        direction=IntelligenceDirection.neutral,
                        title=title,
                        summary=summary,
                        source_name=self.source_name,
                        source_url=self._page_url,
                        occurred_at_utc=None,
                        detected_at_utc=None,
                        relevance_score=self._infer_relevance_score(importance),
                        confidence_pct=self._infer_confidence_pct(importance),
                        impact_horizon_minutes=self._infer_impact_horizon_minutes(importance),
                        asset_scope=request_asset_scope[:],
                        tags=self._build_tags(
                            request_tags=request_tags,
                            extra_tags=extra_tags,
                        ),
                        raw_text=summary,
                        structured_payload={
                            "matched_keyword": keyword,
                            "page_url": self._page_url,
                        },
                    )
                )
                seen_titles.add(display_name)
                break

            if len(parsed_items) >= max_items:
                break

        return parsed_items[:max_items]

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

    def _infer_relevance_score(self, importance: IntelligenceImportance) -> float:
        return {
            IntelligenceImportance.low: 35.0,
            IntelligenceImportance.medium: 58.0,
            IntelligenceImportance.high: 74.0,
            IntelligenceImportance.critical: 90.0,
        }[importance]

    def _infer_confidence_pct(self, importance: IntelligenceImportance) -> float:
        return {
            IntelligenceImportance.low: 40.0,
            IntelligenceImportance.medium: 58.0,
            IntelligenceImportance.high: 70.0,
            IntelligenceImportance.critical: 82.0,
        }[importance]

    def _infer_impact_horizon_minutes(self, importance: IntelligenceImportance) -> int:
        if importance == IntelligenceImportance.critical:
            return 240
        if importance == IntelligenceImportance.high:
            return 180
        return 90

    def _build_tags(
        self,
        *,
        request_tags: list[str],
        extra_tags: list[str],
    ) -> list[str]:
        tags: set[str] = {"bls", "macro", "usa"}
        tags.update(extra_tags)

        for tag in request_tags:
            normalized = str(tag).strip().lower()
            if normalized:
                tags.add(normalized)

        return sorted(tags)

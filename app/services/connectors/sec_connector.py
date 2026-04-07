from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
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


class SECConnector(BaseIntelligenceConnector):
    def __init__(
        self,
        *,
        base_url: str = "https://data.sec.gov/submissions",
        source_name: str = "U.S. Securities and Exchange Commission",
        fetcher: Callable[[str], str | dict[str, object]] | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
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
            cik = self._resolve_cik(request)
            payload = self._fetch_json(self._build_submissions_url(cik))
            items = self._parse_recent_filings(
                payload=payload,
                cik=cik,
                max_items=request.max_items,
                request_tags=request.tags,
                request_asset_scope=request.asset_scope,
            )

            next_cursor = self._build_next_cursor(items=items, cik=cik)
            result = ConnectorFetchResult(
                source_kind=ConnectorSourceKind.sec,
                source_name=self.source_name,
                status=ConnectorStatus.ok,
                fetched_items=len(items),
                accepted_items=len(items),
                next_cursor=next_cursor,
                warnings=[] if items else ["No supported SEC filings were found."],
                errors=[],
            )
            return items, result

        except Exception as exc:
            result = ConnectorFetchResult(
                source_kind=ConnectorSourceKind.sec,
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
        if request.source_kind != ConnectorSourceKind.sec:
            raise ValueError("SECConnector only supports source_kind='sec'.")

        if request.fetch_mode != ConnectorFetchMode.api:
            raise ValueError("SECConnector only supports fetch_mode='api'.")

    def _resolve_cik(self, request: ConnectorFetchRequest) -> str:
        if request.cursor is not None:
            cursor_cik = request.cursor.extra_state.get("cik")
            if cursor_cik:
                return self._normalize_cik(str(cursor_cik))

        for tag in request.tags:
            normalized_tag = str(tag).strip()
            if normalized_tag.lower().startswith("cik:"):
                return self._normalize_cik(normalized_tag.split(":", 1)[1])

        raise ValueError("SECConnector requires a request tag formatted as 'cik:##########'.")

    def _normalize_cik(self, cik: str) -> str:
        digits_only = "".join(character for character in str(cik) if character.isdigit())
        if not digits_only:
            raise ValueError("SECConnector requires a numeric CIK.")
        return digits_only.zfill(10)

    def _build_submissions_url(self, cik: str) -> str:
        return f"{self._base_url}/CIK{cik}.json"

    def _fetch_json(self, url: str) -> dict[str, object]:
        raw_payload = self._fetcher(url)

        if isinstance(raw_payload, dict):
            return raw_payload

        return json.loads(raw_payload)

    def _default_fetcher(self, url: str) -> str:
        http_request = Request(
            url,
            headers={"User-Agent": "EYE/1.0 (market-intelligence; contact: local-project)"},
        )
        with urlopen(http_request, timeout=15) as response:
            return response.read().decode("utf-8", errors="replace")

    def _parse_recent_filings(
        self,
        *,
        payload: dict[str, object],
        cik: str,
        max_items: int,
        request_tags: list[str],
        request_asset_scope: list[str],
    ) -> list[MarketIntelligenceItem]:
        filings = payload.get("filings") or {}
        recent = filings.get("recent") if isinstance(filings, dict) else {}
        if not isinstance(recent, dict):
            return []

        forms = list(recent.get("form") or [])
        filing_dates = list(recent.get("filingDate") or [])
        accession_numbers = list(recent.get("accessionNumber") or [])
        acceptance_datetimes = list(recent.get("acceptanceDateTime") or [])
        primary_documents = list(recent.get("primaryDocument") or [])
        primary_descriptions = list(recent.get("primaryDocDescription") or [])
        item_lines = list(recent.get("items") or [])

        company_name = str(payload.get("name") or "SEC filer").strip()
        tickers = payload.get("tickers") or []
        primary_ticker = ""
        if isinstance(tickers, list) and tickers:
            primary_ticker = str(tickers[0] or "").strip().upper()

        parsed_items: list[MarketIntelligenceItem] = []

        for index, form_value in enumerate(forms):
            normalized_form = self._normalize_form(str(form_value or ""))
            if normalized_form not in {"8-K", "10-Q", "10-K", "6-K"}:
                continue

            accession_number = self._value_at(accession_numbers, index)
            filing_date = self._value_at(filing_dates, index)
            acceptance_dt = self._value_at(acceptance_datetimes, index)
            primary_document = self._value_at(primary_documents, index)
            primary_description = self._value_at(primary_descriptions, index)
            items_value = self._value_at(item_lines, index)

            importance = self._infer_importance(normalized_form)
            tags = self._build_tags(
                normalized_form=normalized_form,
                request_tags=request_tags,
            )
            source_url = self._build_filing_url(
                cik=cik,
                accession_number=accession_number,
                primary_document=primary_document,
            )
            occurred_at_utc = self._parse_timestamp(acceptance_dt) or self._parse_timestamp(
                filing_date
            )

            title = f"{company_name} filed {normalized_form}"
            if primary_ticker:
                title = f"{company_name} ({primary_ticker}) filed {normalized_form}"

            summary = self._build_summary(
                company_name=company_name,
                normalized_form=normalized_form,
                filing_date=filing_date,
                primary_description=primary_description,
                items_value=items_value,
            )

            parsed_items.append(
                MarketIntelligenceItem(
                    source=IntelligenceSourceType.news,
                    event_type=MarketEventType.headline,
                    importance=importance,
                    direction=IntelligenceDirection.neutral,
                    title=title,
                    summary=summary,
                    source_name=self.source_name,
                    source_url=source_url,
                    occurred_at_utc=occurred_at_utc,
                    detected_at_utc=None,
                    relevance_score=self._infer_relevance_score(importance=importance),
                    confidence_pct=self._infer_confidence_pct(importance=importance),
                    impact_horizon_minutes=self._infer_impact_horizon_minutes(
                        importance=importance
                    ),
                    asset_scope=request_asset_scope[:],
                    tags=tags,
                    raw_text=summary,
                    structured_payload={
                        "cik": cik,
                        "company_name": company_name,
                        "ticker": primary_ticker or None,
                        "form": normalized_form,
                        "accession_number": accession_number or None,
                        "filing_date": filing_date or None,
                        "primary_document": primary_document or None,
                        "primary_doc_description": primary_description or None,
                        "items": items_value or None,
                    },
                )
            )

            if len(parsed_items) >= max_items:
                break

        return parsed_items

    def _value_at(self, values: list[object], index: int) -> str:
        if index >= len(values):
            return ""
        return str(values[index] or "").strip()

    def _normalize_form(self, raw_form: str) -> str:
        clean_form = str(raw_form or "").strip().upper()
        if clean_form.endswith("/A"):
            clean_form = clean_form[:-2]
        return clean_form

    def _infer_importance(self, normalized_form: str) -> IntelligenceImportance:
        if normalized_form == "8-K":
            return IntelligenceImportance.critical
        if normalized_form == "6-K":
            return IntelligenceImportance.high
        return IntelligenceImportance.medium

    def _build_tags(
        self,
        *,
        normalized_form: str,
        request_tags: list[str],
    ) -> list[str]:
        tags: set[str] = {"sec", "edgar", "filings", normalized_form.lower().replace("-", "")}

        if normalized_form == "8-K":
            tags.add("material_event")

        for tag in request_tags:
            normalized_tag = str(tag).strip().lower()
            if not normalized_tag or normalized_tag.startswith("cik:"):
                continue
            tags.add(normalized_tag)

        return sorted(tags)

    def _build_filing_url(
        self,
        *,
        cik: str,
        accession_number: str,
        primary_document: str,
    ) -> str | None:
        if not accession_number:
            return None

        clean_accession = accession_number.replace("-", "")
        cik_without_padding = str(int(cik)) if cik.isdigit() else cik.lstrip("0")
        if primary_document:
            return (
                "https://www.sec.gov/Archives/edgar/data/"
                f"{cik_without_padding}/{clean_accession}/{primary_document}"
            )

        return (
            "https://www.sec.gov/Archives/edgar/data/"
            f"{cik_without_padding}/{clean_accession}/index.json"
        )

    def _build_summary(
        self,
        *,
        company_name: str,
        normalized_form: str,
        filing_date: str,
        primary_description: str,
        items_value: str,
    ) -> str:
        summary_parts = [f"{company_name} filed Form {normalized_form}"]

        if filing_date:
            summary_parts.append(f"on {filing_date}")

        summary = " ".join(summary_parts).strip() + "."

        if primary_description:
            summary = f"{summary} {primary_description.strip()}."

        if items_value:
            summary = f"{summary} Items: {items_value.strip()}."

        return summary

    def _parse_timestamp(self, raw_value: str) -> str | None:
        if not raw_value:
            return None

        normalized = raw_value.strip()
        if not normalized:
            return None

        try:
            if normalized.endswith("Z"):
                return datetime.fromisoformat(normalized.replace("Z", "+00:00")).isoformat()
            return datetime.fromisoformat(normalized).isoformat()
        except ValueError:
            pass

        for pattern in ("%Y%m%d%H%M%S", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(normalized, pattern)
                return parsed.replace(tzinfo=timezone.utc).isoformat()
            except ValueError:
                continue

        return None

    def _infer_relevance_score(self, *, importance: IntelligenceImportance) -> float:
        return {
            IntelligenceImportance.low: 35.0,
            IntelligenceImportance.medium: 60.0,
            IntelligenceImportance.high: 74.0,
            IntelligenceImportance.critical: 90.0,
        }[importance]

    def _infer_confidence_pct(self, *, importance: IntelligenceImportance) -> float:
        return {
            IntelligenceImportance.low: 42.0,
            IntelligenceImportance.medium: 58.0,
            IntelligenceImportance.high: 70.0,
            IntelligenceImportance.critical: 82.0,
        }[importance]

    def _infer_impact_horizon_minutes(self, *, importance: IntelligenceImportance) -> int:
        return {
            IntelligenceImportance.low: 60,
            IntelligenceImportance.medium: 180,
            IntelligenceImportance.high: 360,
            IntelligenceImportance.critical: 480,
        }[importance]

    def _build_next_cursor(
        self,
        *,
        items: list[MarketIntelligenceItem],
        cik: str,
    ) -> ConnectorCursor | None:
        if not items:
            return None

        latest_item = items[-1]
        return ConnectorCursor(
            last_seen_id=latest_item.source_url,
            last_seen_timestamp_utc=latest_item.occurred_at_utc,
            extra_state={
                "source_name": self.source_name,
                "cik": cik,
            },
        )

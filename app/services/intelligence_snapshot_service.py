from __future__ import annotations

from app.schemas.market import MarketSymbol
from app.schemas.market_intelligence import MarketIntelligenceSnapshot
from app.schemas.news_connector import ConnectorFetchRequest, ConnectorFetchResult
from app.services.intelligence_ingestion_service import IntelligenceIngestionService
from app.services.intelligence_request_builder_service import (
    IntelligenceRequestBuilderService,
)
from app.services.market_intelligence_service import MarketIntelligenceService


class IntelligenceSnapshotService:
    def __init__(
        self,
        ingestion_service: IntelligenceIngestionService | None = None,
        market_intelligence_service: MarketIntelligenceService | None = None,
        request_builder_service: IntelligenceRequestBuilderService | None = None,
    ) -> None:
        self.ingestion_service = (
            ingestion_service
            if ingestion_service is not None
            else IntelligenceIngestionService()
        )
        self.market_intelligence_service = (
            market_intelligence_service
            if market_intelligence_service is not None
            else MarketIntelligenceService()
        )
        self.request_builder_service = (
            request_builder_service
            if request_builder_service is not None
            else IntelligenceRequestBuilderService()
        )

    def build_snapshot_from_requests(
        self,
        *,
        asset: str,
        symbol: str,
        timeframe: str,
        requests: list[ConnectorFetchRequest],
        session_phase: str | None = None,
        regime: str | None = None,
        volatility_20: float | None = None,
        rsi_14: float | None = None,
        distance_sma20: float | None = None,
        distance_sma50: float | None = None,
        key_levels: list[float] | None = None,
        generated_at_utc: str | None = None,
    ) -> tuple[MarketIntelligenceSnapshot, list[ConnectorFetchResult]]:
        items, connector_results = self.ingestion_service.fetch_from_sources(requests)

        snapshot = self.market_intelligence_service.build_snapshot(
            asset=asset,
            symbol=symbol,
            timeframe=timeframe,
            items=items,
            session_phase=session_phase,
            regime=regime,
            volatility_20=volatility_20,
            rsi_14=rsi_14,
            distance_sma20=distance_sma20,
            distance_sma50=distance_sma50,
            key_levels=key_levels,
            generated_at_utc=generated_at_utc,
        )

        return snapshot, connector_results

    def build_snapshot_for_symbol(
        self,
        *,
        asset: str,
        symbol: MarketSymbol,
        timeframe: str,
        max_items_per_source: int = 10,
        session_phase: str | None = None,
        regime: str | None = None,
        volatility_20: float | None = None,
        rsi_14: float | None = None,
        distance_sma20: float | None = None,
        distance_sma50: float | None = None,
        key_levels: list[float] | None = None,
        generated_at_utc: str | None = None,
    ) -> tuple[MarketIntelligenceSnapshot, list[ConnectorFetchResult]]:
        requests = self.request_builder_service.build_default_requests(
            symbol=symbol,
            max_items_per_source=max_items_per_source,
        )

        return self.build_snapshot_from_requests(
            asset=asset,
            symbol=symbol.value,
            timeframe=timeframe,
            requests=requests,
            session_phase=session_phase,
            regime=regime,
            volatility_20=volatility_20,
            rsi_14=rsi_14,
            distance_sma20=distance_sma20,
            distance_sma50=distance_sma50,
            key_levels=key_levels,
            generated_at_utc=generated_at_utc,
        )

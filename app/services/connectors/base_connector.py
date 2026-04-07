from __future__ import annotations

from abc import ABC, abstractmethod

from app.schemas.market_intelligence import MarketIntelligenceItem
from app.schemas.news_connector import ConnectorFetchRequest, ConnectorFetchResult


class BaseIntelligenceConnector(ABC):
    @property
    @abstractmethod
    def source_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def fetch(
        self,
        request: ConnectorFetchRequest,
    ) -> tuple[list[MarketIntelligenceItem], ConnectorFetchResult]:
        raise NotImplementedError

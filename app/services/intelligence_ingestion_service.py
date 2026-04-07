from __future__ import annotations

from app.schemas.market_intelligence import MarketIntelligenceItem
from app.schemas.news_connector import (
    ConnectorFetchRequest,
    ConnectorFetchResult,
    ConnectorSourceKind,
)
from app.services.connectors.base_connector import BaseIntelligenceConnector
from app.services.connectors.bea_connector import BEAConnector
from app.services.connectors.bls_connector import BLSConnector
from app.services.connectors.cboe_connector import CboeConnector
from app.services.connectors.cftc_connector import CFTCConnector
from app.services.connectors.company_ir_connector import CompanyIRConnector
from app.services.connectors.ecb_connector import ECBConnector
from app.services.connectors.eia_connector import EIAConnector
from app.services.connectors.fed_connector import FedConnector
from app.services.connectors.iea_connector import IEAConnector
from app.services.connectors.nasdaq_connector import NasdaqConnector
from app.services.connectors.opec_connector import OPECConnector
from app.services.connectors.sp_dji_connector import SPDJIConnector
from app.services.connectors.sec_connector import SECConnector
from app.services.connectors.treasury_connector import TreasuryConnector
from app.services.connectors.white_house_connector import WhiteHouseConnector


class IntelligenceIngestionService:
    def __init__(
        self,
        connectors: dict[ConnectorSourceKind, BaseIntelligenceConnector] | None = None,
        custom_connectors: dict[str, BaseIntelligenceConnector] | None = None,
    ) -> None:
        if connectors is not None:
            self._connectors = connectors
            self._custom_connectors = custom_connectors or {}
        else:
            self._connectors = self._build_default_connectors()
            self._custom_connectors = self._build_default_company_ir_connectors()

    def fetch_from_source(
        self,
        request: ConnectorFetchRequest,
    ) -> tuple[list[MarketIntelligenceItem], ConnectorFetchResult]:
        if request.source_kind == ConnectorSourceKind.custom:
            custom_connector = self._custom_connectors.get(request.source_name)
            if custom_connector is not None:
                return custom_connector.fetch(request)

        connector = self._connectors.get(request.source_kind)
        if connector is None:
            raise ValueError(
                f"No connector registered for source_kind='{request.source_kind.value}'."
            )

        return connector.fetch(request)

    def fetch_from_sources(
        self,
        requests: list[ConnectorFetchRequest],
    ) -> tuple[list[MarketIntelligenceItem], list[ConnectorFetchResult]]:
        all_items: list[MarketIntelligenceItem] = []
        all_results: list[ConnectorFetchResult] = []

        for request in requests:
            items, result = self.fetch_from_source(request)
            all_items.extend(items)
            all_results.append(result)

        return all_items, all_results

    def list_registered_sources(self) -> list[str]:
        return sorted(source_kind.value for source_kind in self._connectors.keys())

    def _build_default_connectors(
        self,
    ) -> dict[ConnectorSourceKind, BaseIntelligenceConnector]:
        return {
            ConnectorSourceKind.bea: BEAConnector(),
            ConnectorSourceKind.bls: BLSConnector(),
            ConnectorSourceKind.cboe: CboeConnector(),
            ConnectorSourceKind.custom: CompanyIRConnector(),
            ConnectorSourceKind.sec: SECConnector(),
            ConnectorSourceKind.nasdaq: NasdaqConnector(),
            ConnectorSourceKind.sp_dji: SPDJIConnector(),
            ConnectorSourceKind.treasury: TreasuryConnector(),
            ConnectorSourceKind.fed: FedConnector(),
            ConnectorSourceKind.ecb: ECBConnector(),
            ConnectorSourceKind.white_house: WhiteHouseConnector(),
            ConnectorSourceKind.opec: OPECConnector(),
            ConnectorSourceKind.eia: EIAConnector(),
            ConnectorSourceKind.iea: IEAConnector(),
            ConnectorSourceKind.cftc: CFTCConnector(),
        }

    def _build_default_company_ir_connectors(
        self,
    ) -> dict[str, BaseIntelligenceConnector]:
        return {
            "Microsoft Investor Relations": CompanyIRConnector(
                source_name="Microsoft Investor Relations",
                page_url="https://www.microsoft.com/en-us/investor/default",
                company_ticker="MSFT",
            ),
            "Apple Investor Relations": CompanyIRConnector(
                source_name="Apple Investor Relations",
                page_url="https://investor.apple.com/investor-relations/",
                company_ticker="AAPL",
            ),
            "NVIDIA Investor Relations": CompanyIRConnector(
                source_name="NVIDIA Investor Relations",
                page_url="https://investor.nvidia.com/",
                company_ticker="NVDA",
            ),
            "Amazon Investor Relations": CompanyIRConnector(
                source_name="Amazon Investor Relations",
                page_url="https://ir.aboutamazon.com/",
                company_ticker="AMZN",
            ),
            "Alphabet Investor Relations": CompanyIRConnector(
                source_name="Alphabet Investor Relations",
                page_url="https://abc.xyz/investor/",
                company_ticker="GOOGL",
            ),
            "Meta Investor Relations": CompanyIRConnector(
                source_name="Meta Investor Relations",
                page_url="https://investor.atmeta.com/",
                company_ticker="META",
            ),
        }

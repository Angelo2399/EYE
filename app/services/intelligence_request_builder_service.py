from __future__ import annotations

from app.schemas.market import MarketSymbol
from app.schemas.news_connector import (
    ConnectorFetchMode,
    ConnectorFetchRequest,
    ConnectorSourceKind,
)


class IntelligenceRequestBuilderService:
    def build_default_requests(
        self,
        *,
        symbol: MarketSymbol,
        max_items_per_source: int = 10,
    ) -> list[ConnectorFetchRequest]:
        asset_scope = [symbol.value]
        tags = self._build_tags(symbol)

        if symbol == MarketSymbol.wti:
            return [
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.opec,
                    source_name="OPEC",
                    fetch_mode=ConnectorFetchMode.html,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.eia,
                    source_name="U.S. Energy Information Administration",
                    fetch_mode=ConnectorFetchMode.html,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.iea,
                    source_name="International Energy Agency",
                    fetch_mode=ConnectorFetchMode.html,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.cftc,
                    source_name="U.S. Commodity Futures Trading Commission",
                    fetch_mode=ConnectorFetchMode.html,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=tags,
                ),
            ]

        if symbol == MarketSymbol.ndx:
            sec_tags = self._build_sec_tags(symbol)
            nasdaq_tags = self._build_nasdaq_tags(symbol)
            bls_tags = self._build_bls_tags(symbol)
            bea_tags = self._build_bea_tags(symbol)
            treasury_tags = self._build_treasury_tags(symbol)
            microsoft_ir_tags = self._build_company_ir_tags(symbol)
            apple_ir_tags = self._build_company_ir_tags(symbol)
            nvidia_ir_tags = self._build_company_ir_tags(symbol)
            amazon_ir_tags = self._build_company_ir_tags(symbol)
            alphabet_ir_tags = self._build_company_ir_tags(symbol)
            meta_ir_tags = self._build_company_ir_tags(symbol)
            return [
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.sec,
                    source_name="U.S. Securities and Exchange Commission",
                    fetch_mode=ConnectorFetchMode.api,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=sec_tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.nasdaq,
                    source_name="Nasdaq",
                    fetch_mode=ConnectorFetchMode.rss,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=nasdaq_tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.bls,
                    source_name="U.S. Bureau of Labor Statistics",
                    fetch_mode=ConnectorFetchMode.html,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=bls_tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.bea,
                    source_name="U.S. Bureau of Economic Analysis",
                    fetch_mode=ConnectorFetchMode.html,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=bea_tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.treasury,
                    source_name="U.S. Department of the Treasury",
                    fetch_mode=ConnectorFetchMode.html,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=treasury_tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.custom,
                    source_name="Microsoft Investor Relations",
                    fetch_mode=ConnectorFetchMode.html,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=microsoft_ir_tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.custom,
                    source_name="Apple Investor Relations",
                    fetch_mode=ConnectorFetchMode.html,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=apple_ir_tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.custom,
                    source_name="NVIDIA Investor Relations",
                    fetch_mode=ConnectorFetchMode.html,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=nvidia_ir_tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.custom,
                    source_name="Amazon Investor Relations",
                    fetch_mode=ConnectorFetchMode.html,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=amazon_ir_tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.custom,
                    source_name="Alphabet Investor Relations",
                    fetch_mode=ConnectorFetchMode.html,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=alphabet_ir_tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.custom,
                    source_name="Meta Investor Relations",
                    fetch_mode=ConnectorFetchMode.html,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=meta_ir_tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.fed,
                    source_name="Federal Reserve",
                    fetch_mode=ConnectorFetchMode.rss,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.ecb,
                    source_name="European Central Bank",
                    fetch_mode=ConnectorFetchMode.rss,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.white_house,
                    source_name="White House",
                    fetch_mode=ConnectorFetchMode.html,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=tags,
                ),
            ]

        if symbol == MarketSymbol.spx:
            sp_dji_tags = [
                "macro",
                "central_bank",
                "policy",
                "risk",
                "equity_index",
                "usa",
                "broad_market",
                "index_provider",
                "sp500",
            ]
            cboe_tags = [
                "macro",
                "central_bank",
                "policy",
                "risk",
                "equity_index",
                "usa",
                "broad_market",
                "options",
                "volatility",
                "spx",
                "vix",
            ]
            return [
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.fed,
                    source_name="Federal Reserve",
                    fetch_mode=ConnectorFetchMode.rss,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.ecb,
                    source_name="European Central Bank",
                    fetch_mode=ConnectorFetchMode.rss,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.white_house,
                    source_name="White House",
                    fetch_mode=ConnectorFetchMode.html,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.sp_dji,
                    source_name="S&P Dow Jones Indices",
                    fetch_mode=ConnectorFetchMode.html,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=sp_dji_tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.cboe,
                    source_name="Cboe",
                    fetch_mode=ConnectorFetchMode.html,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=cboe_tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.treasury,
                    source_name="U.S. Department of the Treasury",
                    fetch_mode=ConnectorFetchMode.html,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.bls,
                    source_name="U.S. Bureau of Labor Statistics",
                    fetch_mode=ConnectorFetchMode.html,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.bea,
                    source_name="U.S. Bureau of Economic Analysis",
                    fetch_mode=ConnectorFetchMode.html,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.sec,
                    source_name="U.S. Securities and Exchange Commission",
                    fetch_mode=ConnectorFetchMode.api,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=tags,
                ),
                ConnectorFetchRequest(
                    source_kind=ConnectorSourceKind.nasdaq,
                    source_name="Nasdaq",
                    fetch_mode=ConnectorFetchMode.rss,
                    max_items=max_items_per_source,
                    asset_scope=asset_scope,
                    tags=tags,
                ),
            ]

        return [
            ConnectorFetchRequest(
                source_kind=ConnectorSourceKind.fed,
                source_name="Federal Reserve",
                fetch_mode=ConnectorFetchMode.rss,
                max_items=max_items_per_source,
                asset_scope=asset_scope,
                tags=tags,
            ),
            ConnectorFetchRequest(
                source_kind=ConnectorSourceKind.ecb,
                source_name="European Central Bank",
                fetch_mode=ConnectorFetchMode.rss,
                max_items=max_items_per_source,
                asset_scope=asset_scope,
                tags=tags,
            ),
            ConnectorFetchRequest(
                source_kind=ConnectorSourceKind.white_house,
                source_name="White House",
                fetch_mode=ConnectorFetchMode.html,
                max_items=max_items_per_source,
                asset_scope=asset_scope,
                tags=tags,
            ),
        ]

    def _build_tags(self, symbol: MarketSymbol) -> list[str]:
        base_tags = ["macro", "central_bank", "policy", "risk"]

        if symbol == MarketSymbol.ndx:
            return base_tags + ["equity_index", "growth", "tech", "usa"]

        if symbol == MarketSymbol.spx:
            return base_tags + ["equity_index", "broad_market", "usa"]

        if symbol == MarketSymbol.wti:
            return base_tags + [
                "energy",
                "oil",
                "commodity",
                "inflation",
                "geopolitics",
                "supply",
                "opec",
                "usa",
            ]

        return base_tags

    def _build_sec_tags(self, symbol: MarketSymbol) -> list[str]:
        tags = set(self._build_tags(symbol))
        tags.update(
            {
                "sec",
                "edgar",
                "filings",
                "8k",
                "10q",
                "10k",
                "material_event",
                "mega_cap",
                "tech",
            }
        )
        return sorted(tags)

    def _build_nasdaq_tags(self, symbol: MarketSymbol) -> list[str]:
        tags = set(self._build_tags(symbol))
        tags.update(
            {
                "nasdaq",
                "equity_index",
                "tech",
                "growth",
                "earnings",
                "market_structure",
            }
        )
        return sorted(tags)

    def _build_bls_tags(self, symbol: MarketSymbol) -> list[str]:
        tags = set(self._build_tags(symbol))
        tags.update(
            {
                "bls",
                "macro",
                "cpi",
                "ppi",
                "nfp",
                "employment",
                "jolts",
                "inflation",
                "usa",
            }
        )
        return sorted(tags)

    def _build_bea_tags(self, symbol: MarketSymbol) -> list[str]:
        tags = set(self._build_tags(symbol))
        tags.update(
            {
                "bea",
                "macro",
                "gdp",
                "pce",
                "core_pce",
                "income",
                "outlays",
                "trade",
                "usa",
            }
        )
        return sorted(tags)

    def _build_treasury_tags(self, symbol: MarketSymbol) -> list[str]:
        tags = set(self._build_tags(symbol))
        tags.update(
            {
                "treasury",
                "funding",
                "liquidity",
                "auctions",
                "refunding",
                "borrowing",
                "rates",
                "usa",
            }
        )
        return sorted(tags)

    def _build_company_ir_tags(self, symbol: MarketSymbol) -> list[str]:
        tags = set(self._build_tags(symbol))
        tags.update(
            {
                "company_ir",
                "official_release",
                "earnings",
                "guidance",
                "mega_cap",
                "tech",
            }
        )
        return sorted(tags)

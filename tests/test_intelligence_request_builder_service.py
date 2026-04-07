from __future__ import annotations

from app.schemas.market import MarketSymbol
from app.schemas.news_connector import ConnectorFetchMode, ConnectorSourceKind
from app.services.intelligence_request_builder_service import (
    IntelligenceRequestBuilderService,
)


def test_build_default_requests_for_ndx_returns_expected_sources_and_tags() -> None:
    service = IntelligenceRequestBuilderService()

    requests = service.build_default_requests(
        symbol=MarketSymbol.ndx,
        max_items_per_source=12,
    )

    assert len(requests) == 14

    assert [request.source_kind for request in requests] == [
        ConnectorSourceKind.sec,
        ConnectorSourceKind.nasdaq,
        ConnectorSourceKind.bls,
        ConnectorSourceKind.bea,
        ConnectorSourceKind.treasury,
        ConnectorSourceKind.custom,
        ConnectorSourceKind.custom,
        ConnectorSourceKind.custom,
        ConnectorSourceKind.custom,
        ConnectorSourceKind.custom,
        ConnectorSourceKind.custom,
        ConnectorSourceKind.fed,
        ConnectorSourceKind.ecb,
        ConnectorSourceKind.white_house,
    ]
    assert [request.fetch_mode for request in requests] == [
        ConnectorFetchMode.api,
        ConnectorFetchMode.rss,
        ConnectorFetchMode.html,
        ConnectorFetchMode.html,
        ConnectorFetchMode.html,
        ConnectorFetchMode.html,
        ConnectorFetchMode.html,
        ConnectorFetchMode.html,
        ConnectorFetchMode.html,
        ConnectorFetchMode.html,
        ConnectorFetchMode.html,
        ConnectorFetchMode.rss,
        ConnectorFetchMode.rss,
        ConnectorFetchMode.html,
    ]
    assert all(request.max_items == 12 for request in requests)
    assert all(request.asset_scope == ["NDX"] for request in requests)

    sec_request = requests[0]
    assert sec_request.source_kind == ConnectorSourceKind.sec
    assert sec_request.source_name == "U.S. Securities and Exchange Commission"
    assert "sec" in sec_request.tags
    assert "edgar" in sec_request.tags
    assert "filings" in sec_request.tags
    assert "8k" in sec_request.tags
    assert "10q" in sec_request.tags
    assert "10k" in sec_request.tags
    assert "material_event" in sec_request.tags
    assert "mega_cap" in sec_request.tags
    assert "tech" in sec_request.tags

    nasdaq_request = requests[1]
    assert nasdaq_request.source_kind == ConnectorSourceKind.nasdaq
    assert nasdaq_request.source_name == "Nasdaq"
    assert "nasdaq" in nasdaq_request.tags
    assert "equity_index" in nasdaq_request.tags
    assert "tech" in nasdaq_request.tags
    assert "growth" in nasdaq_request.tags
    assert "earnings" in nasdaq_request.tags
    assert "market_structure" in nasdaq_request.tags

    bls_request = requests[2]
    assert bls_request.source_kind == ConnectorSourceKind.bls
    assert bls_request.source_name == "U.S. Bureau of Labor Statistics"
    assert "bls" in bls_request.tags
    assert "macro" in bls_request.tags
    assert "cpi" in bls_request.tags
    assert "ppi" in bls_request.tags
    assert "nfp" in bls_request.tags
    assert "employment" in bls_request.tags
    assert "jolts" in bls_request.tags
    assert "inflation" in bls_request.tags
    assert "usa" in bls_request.tags

    bea_request = requests[3]
    assert bea_request.source_kind == ConnectorSourceKind.bea
    assert bea_request.source_name == "U.S. Bureau of Economic Analysis"
    assert "bea" in bea_request.tags
    assert "macro" in bea_request.tags
    assert "gdp" in bea_request.tags
    assert "pce" in bea_request.tags
    assert "core_pce" in bea_request.tags
    assert "income" in bea_request.tags
    assert "outlays" in bea_request.tags
    assert "trade" in bea_request.tags
    assert "usa" in bea_request.tags

    treasury_request = requests[4]
    assert treasury_request.source_kind == ConnectorSourceKind.treasury
    assert treasury_request.source_name == "U.S. Department of the Treasury"
    assert "treasury" in treasury_request.tags
    assert "funding" in treasury_request.tags
    assert "liquidity" in treasury_request.tags
    assert "auctions" in treasury_request.tags
    assert "refunding" in treasury_request.tags
    assert "borrowing" in treasury_request.tags
    assert "rates" in treasury_request.tags
    assert "usa" in treasury_request.tags

    microsoft_ir_request = requests[5]
    assert microsoft_ir_request.source_kind == ConnectorSourceKind.custom
    assert microsoft_ir_request.source_name == "Microsoft Investor Relations"
    assert "company_ir" in microsoft_ir_request.tags
    assert "official_release" in microsoft_ir_request.tags
    assert "earnings" in microsoft_ir_request.tags
    assert "guidance" in microsoft_ir_request.tags
    assert "mega_cap" in microsoft_ir_request.tags
    assert "tech" in microsoft_ir_request.tags

    apple_ir_request = requests[6]
    assert apple_ir_request.source_kind == ConnectorSourceKind.custom
    assert apple_ir_request.source_name == "Apple Investor Relations"
    assert "company_ir" in apple_ir_request.tags
    assert "official_release" in apple_ir_request.tags
    assert "earnings" in apple_ir_request.tags
    assert "guidance" in apple_ir_request.tags
    assert "mega_cap" in apple_ir_request.tags
    assert "tech" in apple_ir_request.tags

    nvidia_ir_request = requests[7]
    assert nvidia_ir_request.source_kind == ConnectorSourceKind.custom
    assert nvidia_ir_request.source_name == "NVIDIA Investor Relations"
    assert "company_ir" in nvidia_ir_request.tags
    assert "official_release" in nvidia_ir_request.tags
    assert "earnings" in nvidia_ir_request.tags
    assert "guidance" in nvidia_ir_request.tags
    assert "mega_cap" in nvidia_ir_request.tags
    assert "tech" in nvidia_ir_request.tags

    amazon_ir_request = requests[8]
    assert amazon_ir_request.source_kind == ConnectorSourceKind.custom
    assert amazon_ir_request.source_name == "Amazon Investor Relations"
    assert "company_ir" in amazon_ir_request.tags
    assert "official_release" in amazon_ir_request.tags
    assert "earnings" in amazon_ir_request.tags
    assert "guidance" in amazon_ir_request.tags
    assert "mega_cap" in amazon_ir_request.tags
    assert "tech" in amazon_ir_request.tags

    alphabet_ir_request = requests[9]
    assert alphabet_ir_request.source_kind == ConnectorSourceKind.custom
    assert alphabet_ir_request.source_name == "Alphabet Investor Relations"
    assert "company_ir" in alphabet_ir_request.tags
    assert "official_release" in alphabet_ir_request.tags
    assert "earnings" in alphabet_ir_request.tags
    assert "guidance" in alphabet_ir_request.tags
    assert "mega_cap" in alphabet_ir_request.tags
    assert "tech" in alphabet_ir_request.tags

    meta_ir_request = requests[10]
    assert meta_ir_request.source_kind == ConnectorSourceKind.custom
    assert meta_ir_request.source_name == "Meta Investor Relations"
    assert "company_ir" in meta_ir_request.tags
    assert "official_release" in meta_ir_request.tags
    assert "earnings" in meta_ir_request.tags
    assert "guidance" in meta_ir_request.tags
    assert "mega_cap" in meta_ir_request.tags
    assert "tech" in meta_ir_request.tags

    for request in requests[11:]:
        assert "macro" in request.tags
        assert "central_bank" in request.tags
        assert "policy" in request.tags
        assert "risk" in request.tags
        assert "equity_index" in request.tags
        assert "usa" in request.tags
        assert "tech" in request.tags
        assert "growth" in request.tags


def test_build_default_requests_for_spx_returns_expected_tags() -> None:
    service = IntelligenceRequestBuilderService()

    requests = service.build_default_requests(
        symbol=MarketSymbol.spx,
        max_items_per_source=7,
    )

    assert len(requests) == 10
    assert all(request.max_items == 7 for request in requests)
    assert all(request.asset_scope == ["SPX"] for request in requests)
    assert [request.source_kind for request in requests] == [
        ConnectorSourceKind.fed,
        ConnectorSourceKind.ecb,
        ConnectorSourceKind.white_house,
        ConnectorSourceKind.sp_dji,
        ConnectorSourceKind.cboe,
        ConnectorSourceKind.treasury,
        ConnectorSourceKind.bls,
        ConnectorSourceKind.bea,
        ConnectorSourceKind.sec,
        ConnectorSourceKind.nasdaq,
    ]

    for request in requests:
        assert "macro" in request.tags
        assert "central_bank" in request.tags
        assert "policy" in request.tags
        assert "risk" in request.tags
        assert "equity_index" in request.tags
        assert "usa" in request.tags
        assert "broad_market" in request.tags
        assert "tech" not in request.tags
        assert "growth" not in request.tags


def test_build_default_requests_preserves_stable_order_and_source_names() -> None:
    service = IntelligenceRequestBuilderService()

    requests = service.build_default_requests(symbol=MarketSymbol.ndx)

    assert [(request.source_kind, request.source_name) for request in requests] == [
        (ConnectorSourceKind.sec, "U.S. Securities and Exchange Commission"),
        (ConnectorSourceKind.nasdaq, "Nasdaq"),
        (ConnectorSourceKind.bls, "U.S. Bureau of Labor Statistics"),
        (ConnectorSourceKind.bea, "U.S. Bureau of Economic Analysis"),
        (ConnectorSourceKind.treasury, "U.S. Department of the Treasury"),
        (ConnectorSourceKind.custom, "Microsoft Investor Relations"),
        (ConnectorSourceKind.custom, "Apple Investor Relations"),
        (ConnectorSourceKind.custom, "NVIDIA Investor Relations"),
        (ConnectorSourceKind.custom, "Amazon Investor Relations"),
        (ConnectorSourceKind.custom, "Alphabet Investor Relations"),
        (ConnectorSourceKind.custom, "Meta Investor Relations"),
        (ConnectorSourceKind.fed, "Federal Reserve"),
        (ConnectorSourceKind.ecb, "European Central Bank"),
        (ConnectorSourceKind.white_house, "White House"),
    ]


def test_build_default_requests_uses_default_max_items_per_source() -> None:
    service = IntelligenceRequestBuilderService()

    requests = service.build_default_requests(symbol=MarketSymbol.spx)

    assert len(requests) == 10
    assert all(request.max_items == 10 for request in requests)


def test_build_default_requests_for_wti_returns_oil_specific_tags() -> None:
    service = IntelligenceRequestBuilderService()

    requests = service.build_default_requests(
        symbol=MarketSymbol.wti,
        max_items_per_source=9,
    )

    assert len(requests) == 4
    assert all(request.max_items == 9 for request in requests)
    assert all(request.asset_scope == ["WTI"] for request in requests)

    assert [request.source_kind for request in requests] == [
        ConnectorSourceKind.opec,
        ConnectorSourceKind.eia,
        ConnectorSourceKind.iea,
        ConnectorSourceKind.cftc,
    ]
    assert [request.fetch_mode for request in requests] == [
        ConnectorFetchMode.html,
        ConnectorFetchMode.html,
        ConnectorFetchMode.html,
        ConnectorFetchMode.html,
    ]
    assert [(request.source_kind, request.source_name) for request in requests] == [
        (ConnectorSourceKind.opec, "OPEC"),
        (ConnectorSourceKind.eia, "U.S. Energy Information Administration"),
        (ConnectorSourceKind.iea, "International Energy Agency"),
        (ConnectorSourceKind.cftc, "U.S. Commodity Futures Trading Commission"),
    ]

    for request in requests:
        assert "macro" in request.tags
        assert "central_bank" in request.tags
        assert "policy" in request.tags
        assert "risk" in request.tags
        assert "energy" in request.tags
        assert "oil" in request.tags
        assert "commodity" in request.tags
        assert "inflation" in request.tags
        assert "geopolitics" in request.tags
        assert "supply" in request.tags
        assert "opec" in request.tags
        assert "usa" in request.tags
        assert "equity_index" not in request.tags
        assert "growth" not in request.tags
        assert "tech" not in request.tags
        assert "broad_market" not in request.tags

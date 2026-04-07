from __future__ import annotations

import importlib
import inspect
from types import SimpleNamespace

from app.schemas.market import MarketSymbol, MarketTimeframe
from app.schemas.market_intelligence import MarketBias, MarketIntelligenceSnapshot


def _get_signal_service_class() -> type:
    module = importlib.import_module("app.services.signal_service")
    service_cls = getattr(module, "SignalService", None)
    assert inspect.isclass(service_cls), (
        "Expected app.services.signal_service.SignalService to exist."
    )
    return service_cls


def _build_signal_service_self(
    *,
    external_intelligence_enabled: bool,
    intelligence_snapshot_service=None,
):
    service_cls = _get_signal_service_class()
    service = object.__new__(service_cls)
    service.external_intelligence_enabled = external_intelligence_enabled

    if intelligence_snapshot_service is not None:
        service.intelligence_snapshot_service = intelligence_snapshot_service

    return service


def _build_session_context(phase: str = "open"):
    return SimpleNamespace(
        phase=phase,
        is_session_open=True,
        allow_new_trades=True,
        minutes_to_cutoff=None,
    )


class FakeIntelligenceSnapshotService:
    def __init__(self, returned_snapshot: MarketIntelligenceSnapshot) -> None:
        self.returned_snapshot = returned_snapshot
        self.last_kwargs: dict[str, object] | None = None

    def build_snapshot_for_symbol(self, **kwargs):
        self.last_kwargs = kwargs
        return self.returned_snapshot, []


class FailingIntelligenceSnapshotService:
    def build_snapshot_for_symbol(self, **kwargs):
        raise RuntimeError("External feed failed.")


def _build_external_snapshot() -> MarketIntelligenceSnapshot:
    return MarketIntelligenceSnapshot(
        asset="Nasdaq 100",
        symbol="NDX",
        timeframe="1h",
        generated_at_utc="2026-03-28T12:30:00+00:00",
        market_bias=MarketBias.short,
        bias_confidence_pct=71.0,
        session_phase="open",
        regime="bearish",
        volatility_20=0.013,
        rsi_14=44.0,
        distance_sma20=-0.003,
        distance_sma50=-0.006,
        key_levels=[],
        dominant_drivers=["fed", "macro"],
        risk_flags=["critical_event_flow"],
        items=[],
        synthesis="External intelligence snapshot.",
    )


def test_get_intelligence_snapshot_returns_safe_default_when_disabled() -> None:
    service = _build_signal_service_self(external_intelligence_enabled=False)

    snapshot = service._get_intelligence_snapshot(
        asset="Nasdaq 100",
        symbol=MarketSymbol.ndx,
        timeframe=MarketTimeframe.h1,
        latest_row={},
        regime="bullish",
        session_context=_build_session_context(),
    )

    assert snapshot.market_bias == MarketBias.neutral
    assert snapshot.bias_confidence_pct == 45.0
    assert snapshot.symbol == "NDX"
    assert snapshot.timeframe == "1h"
    assert snapshot.session_phase == "open"
    assert snapshot.volatility_20 == 0.0
    assert snapshot.rsi_14 == 50.0
    assert snapshot.distance_sma20 == 0.0
    assert snapshot.distance_sma50 == 0.0
    assert snapshot.synthesis == "External intelligence disabled."


def test_get_intelligence_snapshot_uses_external_service_when_enabled() -> None:
    returned_snapshot = _build_external_snapshot()
    fake_service = FakeIntelligenceSnapshotService(returned_snapshot=returned_snapshot)
    service = _build_signal_service_self(
        external_intelligence_enabled=True,
        intelligence_snapshot_service=fake_service,
    )

    snapshot = service._get_intelligence_snapshot(
        asset="Nasdaq 100",
        symbol=MarketSymbol.ndx,
        timeframe=MarketTimeframe.h1,
        latest_row={
            "volatility_20": 0.011,
            "rsi_14": 57.0,
            "distance_sma20": 0.002,
            "distance_sma50": 0.004,
        },
        regime="bullish",
        session_context=_build_session_context("midday"),
    )

    assert snapshot is returned_snapshot
    assert fake_service.last_kwargs is not None
    assert fake_service.last_kwargs["asset"] == "Nasdaq 100"
    assert fake_service.last_kwargs["symbol"] == MarketSymbol.ndx
    assert fake_service.last_kwargs["timeframe"] == "1h"
    assert fake_service.last_kwargs["session_phase"] == "midday"
    assert fake_service.last_kwargs["regime"] == "bullish"
    assert fake_service.last_kwargs["volatility_20"] == 0.011
    assert fake_service.last_kwargs["rsi_14"] == 57.0
    assert fake_service.last_kwargs["distance_sma20"] == 0.002
    assert fake_service.last_kwargs["distance_sma50"] == 0.004


def test_get_intelligence_snapshot_returns_default_when_service_missing() -> None:
    service = _build_signal_service_self(external_intelligence_enabled=True)

    snapshot = service._get_intelligence_snapshot(
        asset="S&P 500",
        symbol=MarketSymbol.spx,
        timeframe=MarketTimeframe.h1,
        latest_row={},
        regime="bearish",
        session_context=_build_session_context(),
    )

    assert snapshot.market_bias == MarketBias.neutral
    assert snapshot.bias_confidence_pct == 45.0
    assert snapshot.symbol == "SPX"
    assert snapshot.timeframe == "1h"
    assert snapshot.synthesis == "External intelligence unavailable."


def test_get_intelligence_snapshot_returns_default_when_external_fetch_fails() -> None:
    service = _build_signal_service_self(
        external_intelligence_enabled=True,
        intelligence_snapshot_service=FailingIntelligenceSnapshotService(),
    )

    snapshot = service._get_intelligence_snapshot(
        asset="Nasdaq 100",
        symbol=MarketSymbol.ndx,
        timeframe=MarketTimeframe.h1,
        latest_row={
            "volatility_20": 0.012,
            "rsi_14": 56.0,
            "distance_sma20": 0.001,
            "distance_sma50": 0.003,
        },
        regime="bullish",
        session_context=_build_session_context("open"),
    )

    assert snapshot.market_bias == MarketBias.neutral
    assert snapshot.bias_confidence_pct == 45.0
    assert snapshot.volatility_20 == 0.012
    assert snapshot.rsi_14 == 56.0
    assert snapshot.distance_sma20 == 0.001
    assert snapshot.distance_sma50 == 0.003
    assert snapshot.synthesis == "External intelligence fetch failed."

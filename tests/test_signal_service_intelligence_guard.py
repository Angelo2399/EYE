from __future__ import annotations

import importlib
import inspect
from typing import Any

from app.schemas.market import MarketSymbol, MarketTimeframe
from app.schemas.market_intelligence import MarketBias, MarketIntelligenceSnapshot
from app.schemas.scoring import SetupScore
from app.schemas.signal import SignalAction


def _get_signal_service_class() -> type:
    module = importlib.import_module("app.services.signal_service")
    service_cls = getattr(module, "SignalService", None)
    assert inspect.isclass(service_cls), (
        "Expected app.services.signal_service.SignalService to exist."
    )
    return service_cls


def _build_signal_service_self() -> Any:
    service_cls = _get_signal_service_class()
    service = object.__new__(service_cls)
    service.external_intelligence_enabled = False
    return service


def _get_signal_action_member(name: str) -> Any:
    module = importlib.import_module("app.schemas.signal")
    signal_action = getattr(module, "SignalAction")
    return getattr(signal_action, name)


def _normalize_action(value: Any) -> str:
    if hasattr(value, "value"):
        return str(value.value).lower()
    if hasattr(value, "name"):
        return str(value.name).lower()
    return str(value).lower()


def _build_setup_score(action_name: str, score: float) -> Any:
    action_value = _get_signal_action_member(action_name)
    return SetupScore(
        action=action_value,
        direction="long" if action_name == "long" else "short",
        score=score,
        long_score=score if action_name == "long" else 35.0,
        short_score=score if action_name == "short" else 35.0,
        trend_score=12.0,
        moving_average_score=11.0,
        rsi_score=10.0,
        price_position_score=9.0,
        regime_score=8.0,
        explanation="Test setup score.",
    )


def _build_snapshot(
    *,
    market_bias: MarketBias,
    bias_confidence_pct: float,
    synthesis: str = "",
) -> MarketIntelligenceSnapshot:
    return MarketIntelligenceSnapshot(
        asset="Nasdaq 100",
        symbol="NDX",
        timeframe="1h",
        generated_at_utc="2026-03-28T12:00:00+00:00",
        market_bias=market_bias,
        bias_confidence_pct=bias_confidence_pct,
        session_phase="open",
        regime="bullish",
        volatility_20=0.011,
        rsi_14=57.0,
        distance_sma20=0.002,
        distance_sma50=0.004,
        key_levels=[],
        dominant_drivers=["fed", "macro"],
        risk_flags=[],
        items=[],
        synthesis=synthesis,
    )


def test_intelligence_disabled_has_no_impact_on_setup_action() -> None:
    service = _build_signal_service_self()
    setup_score = _build_setup_score(action_name="long", score=61.0)

    default_snapshot = service._build_default_intelligence_snapshot(
        asset="Nasdaq 100",
        symbol=MarketSymbol.ndx,
        timeframe=MarketTimeframe.h1,
        regime="bullish",
        session_context=type("SessionCtx", (), {"phase": "open"})(),
        latest_row={
            "volatility_20": 0.011,
            "rsi_14": 57.0,
            "distance_sma20": 0.002,
            "distance_sma50": 0.004,
        },
        explanation="External intelligence disabled.",
    )

    result = service._apply_intelligence_guard(setup_score, default_snapshot)

    assert _normalize_action(result.action) == _normalize_action(SignalAction.long)
    assert default_snapshot.market_bias == MarketBias.neutral
    assert default_snapshot.bias_confidence_pct == 45.0
    assert default_snapshot.synthesis == "External intelligence disabled."


def test_apply_intelligence_guard_returns_wait_on_strong_opposite_bias() -> None:
    service = _build_signal_service_self()
    setup_score = _build_setup_score(action_name="long", score=63.0)
    intelligence_snapshot = _build_snapshot(
        market_bias=MarketBias.short,
        bias_confidence_pct=72.0,
        synthesis="Strong short external bias.",
    )

    result = service._apply_intelligence_guard(setup_score, intelligence_snapshot)

    assert _normalize_action(result.action) == _normalize_action(SignalAction.wait)
    assert "strongly short-biased" in result.explanation.lower()


def test_apply_intelligence_guard_returns_no_trade_on_strong_no_trade_bias() -> None:
    service = _build_signal_service_self()
    setup_score = _build_setup_score(action_name="short", score=64.0)
    intelligence_snapshot = _build_snapshot(
        market_bias=MarketBias.no_trade,
        bias_confidence_pct=78.0,
        synthesis="External context unstable.",
    )

    result = service._apply_intelligence_guard(setup_score, intelligence_snapshot)

    assert _normalize_action(result.action) == _normalize_action(SignalAction.no_trade)
    assert "external intelligence flags unstable context" in result.explanation.lower()

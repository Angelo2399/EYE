from __future__ import annotations

import importlib
import inspect
from types import SimpleNamespace

import pandas as pd

from app.schemas.day_context import DayBias, DayContext, DayContextLabel
from app.schemas.market import MarketSymbol, MarketTimeframe
from app.schemas.market_intelligence import MarketBias, MarketIntelligenceSnapshot
from app.schemas.probability import ProbabilityEstimate
from app.schemas.risk import RiskPlan
from app.schemas.scoring import SetupScore
from app.schemas.signal import (
    HoldingWindow,
    ModelConfidence,
    SignalAction,
    SignalRequest,
)


def _get_signal_service_class() -> type:
    module = importlib.import_module("app.services.signal_service")
    service_cls = getattr(module, "SignalService", None)
    assert inspect.isclass(service_cls), (
        "Expected app.services.signal_service.SignalService to exist."
    )
    return service_cls


class FakeMarketDataService:
    def get_ohlcv(self, symbol, timeframe):
        return pd.DataFrame({"close": [100.0]})


class FakeFeatureService:
    def build_features(self, market_data):
        return pd.DataFrame({"close": [100.0]})


class FakeRegimeService:
    def classify_regime(self, features):
        return pd.DataFrame(
            [
                {
                    "regime": "bullish",
                    "volatility_20": 0.011,
                    "rsi_14": 57.0,
                    "distance_sma20": 0.002,
                    "distance_sma50": 0.004,
                }
            ]
        )


class FakeScoringService:
    def __init__(self, setup_score: SetupScore) -> None:
        self.setup_score = setup_score

    def score_setup(self, features_with_regime):
        return self.setup_score


class FakeRiskService:
    def __init__(self) -> None:
        self.last_action = None

    def build_risk_plan(self, features_with_regime, action):
        self.last_action = action
        return RiskPlan(
            entry_min=100.0,
            entry_max=101.0,
            entry_window="15:35-16:10",
            expected_holding=HoldingWindow.h1,
            hard_exit_time="21:55",
            close_by_session_end=True,
            stop_loss=99.4,
            take_profit_1=101.6,
            take_profit_2=102.2,
            risk_reward=1.8,
        )


class FakeProbabilityService:
    def __init__(self) -> None:
        self.last_action = None

    def estimate_probabilities(self, features_with_regime, setup_score, risk_plan):
        self.last_action = setup_score.action
        return ProbabilityEstimate(
            favorable_move_pct=58.0,
            tp1_hit_pct=44.0,
            stop_hit_first_pct=28.0,
            model_confidence_pct=61.0,
            confidence_label=ModelConfidence.medium,
        )


class FakeExplanationService:
    def __init__(self) -> None:
        self.last_action = None
        self.last_intelligence_snapshot = None

    def build_explanation(
        self,
        *,
        asset,
        regime,
        setup_score,
        risk_plan,
        probability_estimate,
        day_context=None,
        intelligence_snapshot=None,
    ):
        self.last_action = setup_score.action
        self.last_intelligence_snapshot = intelligence_snapshot
        return (
            f"{asset}: action={setup_score.action.value}; "
            f"synthesis={getattr(intelligence_snapshot, 'synthesis', None)}"
        )


class FakeDayContextService:
    def classify_day_context(self, latest_row):
        return DayContext(
            label=DayContextLabel.unclear,
            bias=DayBias.neutral,
            confidence_pct=45.0,
            prefer_breakout=False,
            avoid_mean_reversion=False,
            explanation="Neutral day context.",
        )


class FakeSessionService:
    def get_session_context(self):
        return SimpleNamespace(
            phase="open",
            is_session_open=True,
            allow_new_trades=True,
            minutes_to_cutoff=None,
        )


class FakeIntelligenceSnapshotService:
    def __init__(self, snapshot: MarketIntelligenceSnapshot) -> None:
        self.snapshot = snapshot
        self.last_kwargs: dict[str, object] | None = None

    def build_snapshot_for_symbol(self, **kwargs):
        self.last_kwargs = kwargs
        return self.snapshot, []


class FakeSignalRepository:
    def __init__(self) -> None:
        self.saved_payload = None
        self.saved_response = None

    def save_signal(self, payload, response):
        self.saved_payload = payload
        self.saved_response = response
        return 1


def _build_setup_score(action: SignalAction) -> SetupScore:
    return SetupScore(
        action=action,
        direction="long" if action == SignalAction.long else "short",
        score=63.0,
        long_score=63.0 if action == SignalAction.long else 34.0,
        short_score=63.0 if action == SignalAction.short else 34.0,
        trend_score=12.0,
        moving_average_score=13.0,
        rsi_score=11.0,
        price_position_score=14.0,
        regime_score=13.0,
        explanation="Base setup score.",
    )


def _build_intelligence_snapshot(
    *,
    market_bias: MarketBias,
    bias_confidence_pct: float,
) -> MarketIntelligenceSnapshot:
    return MarketIntelligenceSnapshot(
        asset="Nasdaq 100",
        symbol="NDX",
        timeframe="1h",
        generated_at_utc="2026-03-28T13:00:00+00:00",
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
        synthesis="External intelligence snapshot.",
    )


def _build_service(
    *,
    setup_action: SignalAction,
    intelligence_snapshot: MarketIntelligenceSnapshot,
):
    service_cls = _get_signal_service_class()
    service = object.__new__(service_cls)

    service.market_data_service = FakeMarketDataService()
    service.feature_service = FakeFeatureService()
    service.regime_service = FakeRegimeService()
    service.scoring_service = FakeScoringService(_build_setup_score(setup_action))
    service.risk_service = FakeRiskService()
    service.probability_service = FakeProbabilityService()
    service.explanation_service = FakeExplanationService()
    service.day_context_service = FakeDayContextService()
    service.session_service = FakeSessionService()
    service.intelligence_snapshot_service = FakeIntelligenceSnapshotService(
        intelligence_snapshot
    )
    service.signal_repository = FakeSignalRepository()
    service.external_intelligence_enabled = True

    return service


def test_generate_signal_returns_wait_when_external_intelligence_is_strongly_opposite() -> None:
    service = _build_service(
        setup_action=SignalAction.long,
        intelligence_snapshot=_build_intelligence_snapshot(
            market_bias=MarketBias.short,
            bias_confidence_pct=72.0,
        ),
    )

    response = service.generate_signal(
        SignalRequest(
            symbol=MarketSymbol.ndx,
            timeframe=MarketTimeframe.h1,
        )
    )

    assert response.action == SignalAction.wait
    assert service.risk_service.last_action == SignalAction.wait
    assert service.probability_service.last_action == SignalAction.wait
    assert service.explanation_service.last_action == SignalAction.wait
    assert service.signal_repository.saved_response.action == SignalAction.wait
    assert service.intelligence_snapshot_service.last_kwargs is not None
    assert service.intelligence_snapshot_service.last_kwargs["symbol"] == MarketSymbol.ndx
    assert service.intelligence_snapshot_service.last_kwargs["timeframe"] == "1h"


def test_generate_signal_returns_no_trade_when_external_intelligence_flags_unstable_context() -> None:
    service = _build_service(
        setup_action=SignalAction.short,
        intelligence_snapshot=_build_intelligence_snapshot(
            market_bias=MarketBias.no_trade,
            bias_confidence_pct=78.0,
        ),
    )

    response = service.generate_signal(
        SignalRequest(
            symbol=MarketSymbol.ndx,
            timeframe=MarketTimeframe.h1,
        )
    )

    assert response.action == SignalAction.no_trade
    assert service.risk_service.last_action == SignalAction.no_trade
    assert service.probability_service.last_action == SignalAction.no_trade
    assert service.explanation_service.last_action == SignalAction.no_trade
    assert service.signal_repository.saved_response.action == SignalAction.no_trade
    assert service.intelligence_snapshot_service.last_kwargs is not None
    assert service.intelligence_snapshot_service.last_kwargs["regime"] == "bullish"
    assert service.intelligence_snapshot_service.last_kwargs["session_phase"] == "open"


def test_generate_signal_for_wti_returns_wait_when_external_intelligence_is_strongly_opposite() -> None:
    service = _build_service(
        setup_action=SignalAction.long,
        intelligence_snapshot=_build_intelligence_snapshot(
            market_bias=MarketBias.short,
            bias_confidence_pct=72.0,
        ),
    )

    response = service.generate_signal(
        SignalRequest(
            symbol=MarketSymbol.wti,
            timeframe=MarketTimeframe.h1,
        )
    )

    assert response.action == SignalAction.wait
    assert service.risk_service.last_action == SignalAction.wait
    assert service.probability_service.last_action == SignalAction.wait
    assert service.explanation_service.last_action == SignalAction.wait
    assert service.signal_repository.saved_payload.symbol == MarketSymbol.wti
    assert service.signal_repository.saved_response.action == SignalAction.wait
    assert "synthesis=External intelligence snapshot." in response.explanation
    assert service.explanation_service.last_intelligence_snapshot is not None
    assert service.explanation_service.last_intelligence_snapshot.market_bias == MarketBias.short
    assert service.intelligence_snapshot_service.last_kwargs is not None
    assert service.intelligence_snapshot_service.last_kwargs["symbol"] == MarketSymbol.wti
    assert service.intelligence_snapshot_service.last_kwargs["timeframe"] == "1h"


def test_generate_signal_for_wti_returns_no_trade_when_external_intelligence_flags_unstable_context() -> None:
    service = _build_service(
        setup_action=SignalAction.short,
        intelligence_snapshot=_build_intelligence_snapshot(
            market_bias=MarketBias.no_trade,
            bias_confidence_pct=78.0,
        ),
    )

    response = service.generate_signal(
        SignalRequest(
            symbol=MarketSymbol.wti,
            timeframe=MarketTimeframe.h1,
        )
    )

    assert response.action == SignalAction.no_trade
    assert service.risk_service.last_action == SignalAction.no_trade
    assert service.probability_service.last_action == SignalAction.no_trade
    assert service.explanation_service.last_action == SignalAction.no_trade
    assert service.signal_repository.saved_payload.symbol == MarketSymbol.wti
    assert service.signal_repository.saved_response.action == SignalAction.no_trade
    assert service.intelligence_snapshot_service.last_kwargs is not None
    assert service.intelligence_snapshot_service.last_kwargs["symbol"] == MarketSymbol.wti
    assert service.intelligence_snapshot_service.last_kwargs["regime"] == "bullish"
    assert service.intelligence_snapshot_service.last_kwargs["session_phase"] == "open"

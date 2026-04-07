from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from app.schemas.day_context import DayBias, DayContext, DayContextLabel
from app.schemas.market import MarketSymbol, MarketTimeframe
from app.schemas.scoring import SetupScore
from app.schemas.signal import (
    HoldingWindow,
    ModelConfidence,
    SignalAction,
    SignalRequest,
    SignalResponse,
)
from app.services.signal_service import SignalService


def _build_service() -> SignalService:
    return object.__new__(SignalService)


def _build_generate_signal_service() -> SignalService:
    service = object.__new__(SignalService)
    features_with_regime = pd.DataFrame(
        [
            {
                "regime": "bullish",
                "volatility_20": 0.010,
                "rsi_14": 55.0,
            }
        ]
    )

    risk_plan = SimpleNamespace(
        entry_min=100.0,
        entry_max=101.0,
        entry_window="15:35-16:10",
        expected_holding=HoldingWindow.h1,
        hard_exit_time="21:55",
        close_by_session_end=True,
        stop_loss=99.0,
        take_profit_1=102.0,
        take_profit_2=103.0,
        risk_reward=1.5,
    )

    probability_estimate = SimpleNamespace(
        favorable_move_pct=58.0,
        tp1_hit_pct=44.0,
        stop_hit_first_pct=28.0,
        model_confidence_pct=61.0,
        confidence_label=ModelConfidence.medium,
    )

    service.market_data_service = SimpleNamespace(
        get_ohlcv=lambda symbol, timeframe: pd.DataFrame(
            [{"open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1}]
        )
    )
    service.feature_service = SimpleNamespace(
        build_features=lambda market_data: market_data
    )
    service.regime_service = SimpleNamespace(
        classify_regime=lambda features: features_with_regime
    )
    service.scoring_service = SimpleNamespace(
        score_setup=lambda features: SetupScore(
            action=SignalAction.long,
            direction="long",
            score=72.0,
            long_score=74.0,
            short_score=28.0,
            trend_score=16.0,
            moving_average_score=14.0,
            rsi_score=12.0,
            price_position_score=15.0,
            regime_score=15.0,
            explanation="Long setup score.",
        )
    )
    service.session_service = SimpleNamespace(
        get_session_context=lambda current_dt=None: SimpleNamespace(
            phase="open",
            is_session_open=True,
            allow_new_trades=True,
            minutes_to_cutoff=None,
        )
    )
    service.day_context_service = SimpleNamespace(
        classify_day_context=lambda latest_row: _build_day_context()
    )
    service.briefing_builder_service = SimpleNamespace(
        build_briefing=lambda **kwargs: {
            "title": "Test briefing",
            "summary": "Test summary",
        }
    )
    service.risk_service = SimpleNamespace(
        build_risk_plan=lambda features, action: risk_plan
    )
    service.probability_service = SimpleNamespace(
        estimate_probabilities=lambda features, setup_score, risk_plan_arg: (
            probability_estimate
        )
    )
    service.explanation_service = SimpleNamespace(
        build_explanation=lambda **kwargs: "Structured signal explanation."
    )
    service.signal_repository = SimpleNamespace(
        save_signal=lambda payload, response: None
    )
    service.market_calendar_service = SimpleNamespace(
        get_market_status=lambda asset, timezone_name=None: {
            "is_tradable_now": True,
        }
    )
    service.telegram_alert_service = FakeTelegramAlertService(should_send=True)
    service.external_intelligence_enabled = False

    return service


def _build_day_context() -> DayContext:
    return DayContext(
        label=DayContextLabel.trend_up,
        bias=DayBias.long,
        confidence_pct=72.0,
        prefer_breakout=True,
        avoid_mean_reversion=True,
        explanation="Trend-up day context.",
    )


def _build_response(action: SignalAction) -> SignalResponse:
    return SignalResponse(
        asset="Nasdaq 100",
        action=action,
        entry_min=100.0,
        entry_max=101.0,
        entry_window="15:35-16:10",
        expected_holding=HoldingWindow.h1,
        hard_exit_time="21:55",
        close_by_session_end=True,
        stop_loss=99.0,
        take_profit_1=102.0,
        take_profit_2=103.0,
        risk_reward=1.5,
        favorable_move_pct=58.0,
        tp1_hit_pct=44.0,
        stop_hit_first_pct=28.0,
        model_confidence_pct=61.0,
        confidence_label=ModelConfidence.medium,
        explanation="Structured signal explanation.",
    )


class FakeTelegramAlertService:
    def __init__(self, should_send: bool) -> None:
        self.should_send = should_send
        self.last_action = None
        self.last_kwargs = None
        self.last_asset_update_kwargs = None

    def should_send_signal_alert(self, *, action: str) -> bool:
        self.last_action = action
        return self.should_send

    def publish_asset_topic_alert(self, **kwargs):
        self.last_kwargs = kwargs
        return {"ok": True}

    def publish_asset_update(self, **kwargs):
        self.last_asset_update_kwargs = kwargs
        return {
            "message": "ok",
            "state_change": {"change_detected": True, "should_alert": False},
        }


def test_maybe_publish_asset_topic_signal_alert_dispatches_supported_long_signal() -> None:
    service = _build_service()
    service.telegram_alert_service = FakeTelegramAlertService(should_send=True)
    service._last_runtime_context = {"local_time": "14:35:00"}

    service._maybe_publish_asset_topic_signal_alert(
        symbol=MarketSymbol.ndx,
        timeframe=MarketTimeframe.h1,
        response=_build_response(SignalAction.long),
        day_context=_build_day_context(),
        risk_plan=SimpleNamespace(
            entry_min=100.0,
            entry_max=101.0,
            stop_loss=99.0,
            take_profit_1=102.0,
            take_profit_2=103.0,
            risk_reward=1.5,
        ),
        probability_estimate=SimpleNamespace(
            model_confidence_pct=61.0,
            confidence_label=SimpleNamespace(value="medium"),
        ),
    )

    assert service.telegram_alert_service.last_action == "long"
    assert service.telegram_alert_service.last_kwargs is not None
    assert service.telegram_alert_service.last_kwargs["asset_topic"] == "ndx"
    assert service.telegram_alert_service.last_kwargs["allowed_destinations"] == [
        "ndx"
    ]
    assert service.telegram_alert_service.last_kwargs["context"]["bias"] == "long"
    assert (
        service.telegram_alert_service.last_kwargs["context"]["local_time"]
        == "14:35:00"
    )
    assert service.telegram_alert_service.last_kwargs["context"]["timeframe"] == "1h"
    assert (
        service.telegram_alert_service.last_kwargs["context"]["model_confidence_pct"]
        == 61.0
    )
    assert (
        service.telegram_alert_service.last_kwargs["context"]["confidence_label"]
        == "medium"
    )
    assert service.telegram_alert_service.last_kwargs["context"]["entry_min"] == 100.0
    assert service.telegram_alert_service.last_kwargs["context"]["entry_max"] == 101.0
    assert service.telegram_alert_service.last_kwargs["context"]["stop_loss"] == 99.0
    assert (
        service.telegram_alert_service.last_kwargs["context"]["take_profit_1"] == 102.0
    )
    assert (
        service.telegram_alert_service.last_kwargs["context"]["take_profit_2"] == 103.0
    )
    assert service.telegram_alert_service.last_kwargs["context"]["risk_reward"] == 1.5


def test_maybe_publish_asset_topic_signal_alert_skips_wait_signal() -> None:
    service = _build_service()
    service.telegram_alert_service = FakeTelegramAlertService(should_send=False)
    service._last_runtime_context = {"local_time": "14:35:00"}

    service._maybe_publish_asset_topic_signal_alert(
        symbol=MarketSymbol.ndx,
        timeframe=MarketTimeframe.h1,
        response=_build_response(SignalAction.wait),
        day_context=_build_day_context(),
    )

    assert service.telegram_alert_service.last_action == "wait"
    assert service.telegram_alert_service.last_kwargs is None


def test_maybe_publish_asset_topic_signal_alert_skips_unsupported_asset_topic() -> None:
    service = _build_service()
    service.telegram_alert_service = FakeTelegramAlertService(should_send=True)
    service._last_runtime_context = {"local_time": "14:35:00"}

    service._maybe_publish_asset_topic_signal_alert(
        symbol=MarketSymbol.btc,
        timeframe=MarketTimeframe.h1,
        response=_build_response(SignalAction.long),
        day_context=_build_day_context(),
    )

    assert service.telegram_alert_service.last_action == "long"
    assert service.telegram_alert_service.last_kwargs is None


def test_generate_signal_publishes_main_asset_update_without_breaking_response() -> None:
    service = _build_generate_signal_service()

    response = service.generate_signal(
        SignalRequest(
            symbol=MarketSymbol.ndx,
            timeframe=MarketTimeframe.h1,
        )
    )

    assert response.action == SignalAction.long
    assert service.telegram_alert_service.last_asset_update_kwargs is not None
    assert service.telegram_alert_service.last_asset_update_kwargs["asset"] == "NDX"
    assert (
        service.telegram_alert_service.last_asset_update_kwargs["asset_full_name"]
        == "Nasdaq 100"
    )
    assert service.telegram_alert_service.last_asset_update_kwargs["action"] == "long"
    assert (
        service.telegram_alert_service.last_asset_update_kwargs["scenario"]
        == "trend_up"
    )
    assert (
        service.telegram_alert_service.last_asset_update_kwargs["confidence_label"]
        == "medium"
    )
    assert (
        service.telegram_alert_service.last_asset_update_kwargs["market_explanation"]
        == "External intelligence disabled."
    )
    assert (
        service.telegram_alert_service.last_asset_update_kwargs["entry_min"] == 100.0
    )
    assert (
        service.telegram_alert_service.last_asset_update_kwargs["entry_max"] == 101.0
    )
    assert (
        service.telegram_alert_service.last_asset_update_kwargs["stop_loss"] == 99.0
    )
    assert (
        service.telegram_alert_service.last_asset_update_kwargs["take_profit_1"]
        == 102.0
    )
    assert (
        service.telegram_alert_service.last_asset_update_kwargs["take_profit_2"]
        == 103.0
    )
    assert (
        service.telegram_alert_service.last_asset_update_kwargs["risk_reward"] == 1.5
    )


def test_maybe_publish_asset_main_update_skips_when_market_not_tradable() -> None:
    service = _build_service()
    service.telegram_alert_service = FakeTelegramAlertService(should_send=True)
    service.market_calendar_service = SimpleNamespace(
        get_market_status=lambda asset, timezone_name=None: {
            "is_tradable_now": False,
        }
    )
    service._last_runtime_context = {"timezone_name": "Europe/London"}

    service._maybe_publish_asset_main_update(
        symbol=MarketSymbol.ndx,
        response=_build_response(SignalAction.long),
        day_context=_build_day_context(),
        timezone_name="Europe/London",
        risk_plan=SimpleNamespace(
            entry_min=100.0,
            entry_max=101.0,
            stop_loss=99.0,
            take_profit_1=102.0,
            take_profit_2=103.0,
            risk_reward=1.5,
        ),
        probability_estimate=SimpleNamespace(
            confidence_label=SimpleNamespace(value="medium"),
        ),
    )

    assert service.telegram_alert_service.last_asset_update_kwargs is None

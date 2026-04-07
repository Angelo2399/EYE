from __future__ import annotations

from enum import Enum
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
)
from app.services.signal_service import SignalService


def _enum_member(enum_cls: type[Enum], *preferred_values: str) -> Enum:
    normalized_candidates = {
        str(candidate).strip().lower() for candidate in preferred_values if candidate
    }

    for member in enum_cls:
        if member.name.lower() in normalized_candidates:
            return member
        if str(member.value).strip().lower() in normalized_candidates:
            return member

    return next(iter(enum_cls))


def _build_signal_request() -> SignalRequest:
    return SignalRequest(
        symbol=_enum_member(MarketSymbol, "NDX", "nasdaq_100"),
        timeframe=_enum_member(MarketTimeframe, "1h", "h1"),
    )


def _build_setup_score(action: SignalAction, score: float = 60.0) -> SetupScore:
    direction = "long" if action == SignalAction.long else "short"

    return SetupScore(
        action=action,
        direction=direction,
        score=score,
        long_score=64.0,
        short_score=29.0,
        trend_score=14.0,
        moving_average_score=13.0,
        rsi_score=12.0,
        price_position_score=11.0,
        regime_score=10.0,
        explanation="Original setup explanation.",
    )


def _build_day_context(
    label: DayContextLabel,
    bias: DayBias,
    confidence_pct: float,
) -> DayContext:
    return DayContext(
        label=label,
        bias=bias,
        confidence_pct=confidence_pct,
        prefer_breakout=False,
        avoid_mean_reversion=False,
        explanation="Test day context.",
    )


def _build_service_with_fakes(
    saved: dict[str, object],
    *,
    setup_action: SignalAction,
    day_context: DayContext,
) -> SignalService:
    service = object.__new__(SignalService)

    features_with_regime = pd.DataFrame(
        [
            {
                "regime": "bullish",
                "volatility_20": 0.010,
                "rsi_14": 52.0,
            }
        ]
    )

    risk_plan = SimpleNamespace(
        entry_min=100.0,
        entry_max=101.0,
        entry_window="15:35-16:10",
        expected_holding=_enum_member(HoldingWindow, "1h"),
        hard_exit_time="21:55",
        close_by_session_end=True,
        stop_loss=99.0,
        take_profit_1=102.0,
        take_profit_2=103.0,
        risk_reward=1.5,
    )

    probability_estimate = SimpleNamespace(
        favorable_move_pct=52.0,
        tp1_hit_pct=41.0,
        stop_hit_first_pct=35.0,
        model_confidence_pct=48.0,
        confidence_label=_enum_member(ModelConfidence, "low"),
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
        score_setup=lambda features: _build_setup_score(
            action=setup_action,
            score=60.0,
        )
    )
    service.session_service = SimpleNamespace(
        get_session_context=lambda: SimpleNamespace(
            phase="open",
            is_session_open=True,
            allow_new_trades=True,
            minutes_to_cutoff=180,
        )
    )
    service.day_context_service = SimpleNamespace(
        classify_day_context=lambda latest_row: day_context
    )

    def _build_risk_plan(features: pd.DataFrame, action: SignalAction):
        saved["risk_action"] = action
        return risk_plan

    def _estimate_probabilities(
        features: pd.DataFrame,
        setup_score: SetupScore,
        risk_plan_arg,
    ):
        saved["probability_action"] = setup_score.action
        return probability_estimate

    def _build_explanation(**kwargs) -> str:
        saved["explanation_action"] = kwargs["setup_score"].action
        return f"Explanation for {kwargs['setup_score'].action.value}."

    def _save_signal(payload: SignalRequest, response) -> None:
        saved["saved_payload"] = payload
        saved["saved_response"] = response

    service.risk_service = SimpleNamespace(build_risk_plan=_build_risk_plan)
    service.probability_service = SimpleNamespace(
        estimate_probabilities=_estimate_probabilities
    )
    service.explanation_service = SimpleNamespace(
        build_explanation=_build_explanation
    )
    service.signal_repository = SimpleNamespace(save_signal=_save_signal)

    return service


def test_generate_signal_returns_no_trade_when_day_context_is_volatile() -> None:
    saved: dict[str, object] = {}
    service = _build_service_with_fakes(
        saved,
        setup_action=SignalAction.long,
        day_context=_build_day_context(
            label=DayContextLabel.volatile_day,
            bias=DayBias.neutral,
            confidence_pct=35.0,
        ),
    )
    payload = _build_signal_request()

    response = service.generate_signal(payload)

    assert response.action == SignalAction.no_trade
    assert saved["risk_action"] == SignalAction.no_trade
    assert saved["probability_action"] == SignalAction.no_trade
    assert saved["explanation_action"] == SignalAction.no_trade
    assert saved["saved_payload"] is payload
    assert saved["saved_response"].action == SignalAction.no_trade


def test_generate_signal_turns_long_into_wait_when_day_bias_is_strongly_short() -> None:
    saved: dict[str, object] = {}
    service = _build_service_with_fakes(
        saved,
        setup_action=SignalAction.long,
        day_context=_build_day_context(
            label=DayContextLabel.trend_down,
            bias=DayBias.short,
            confidence_pct=72.0,
        ),
    )
    payload = _build_signal_request()

    response = service.generate_signal(payload)

    assert response.action == SignalAction.wait
    assert saved["risk_action"] == SignalAction.wait
    assert saved["probability_action"] == SignalAction.wait
    assert saved["explanation_action"] == SignalAction.wait
    assert saved["saved_payload"] is payload
    assert saved["saved_response"].action == SignalAction.wait


def test_generate_signal_keeps_long_when_day_context_is_not_blocking() -> None:
    saved: dict[str, object] = {}
    service = _build_service_with_fakes(
        saved,
        setup_action=SignalAction.long,
        day_context=_build_day_context(
            label=DayContextLabel.trend_up,
            bias=DayBias.long,
            confidence_pct=72.0,
        ),
    )
    payload = _build_signal_request()

    response = service.generate_signal(payload)

    assert response.action == SignalAction.long
    assert saved["risk_action"] == SignalAction.long
    assert saved["probability_action"] == SignalAction.long
    assert saved["explanation_action"] == SignalAction.long
    assert saved["saved_payload"] is payload
    assert saved["saved_response"].action == SignalAction.long

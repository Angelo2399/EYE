from __future__ import annotations

from enum import Enum
from types import SimpleNamespace
from typing import Any, get_args, get_origin

import pandas as pd

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


def _is_optional(annotation: Any) -> bool:
    origin = get_origin(annotation)
    if origin is None:
        return False
    return type(None) in get_args(annotation)


def _placeholder_from_annotation(annotation: Any) -> Any:
    if annotation is Any:
        return None

    if _is_optional(annotation):
        return None

    origin = get_origin(annotation)
    args = get_args(annotation)

    if origin in (list, tuple, set):
        return origin()
    if origin is dict:
        return {}
    if args:
        return _placeholder_from_annotation(args[0])

    if annotation is str:
        return ""
    if annotation is float:
        return 0.0
    if annotation is int:
        return 0
    if annotation is bool:
        return False
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return next(iter(annotation))

    return None


def _build_signal_request() -> SignalRequest:
    kwargs: dict[str, Any] = {
        "symbol": _enum_member(MarketSymbol, "NDX", "nasdaq_100"),
        "timeframe": _enum_member(MarketTimeframe, "1h", "h1"),
    }

    if hasattr(SignalRequest, "model_fields"):
        for field_name, field in SignalRequest.model_fields.items():
            if field_name in kwargs:
                continue
            if not field.is_required():
                continue
            kwargs[field_name] = _placeholder_from_annotation(field.annotation)

    return SignalRequest(**kwargs)


def _build_setup_score(action: SignalAction, score: float) -> SetupScore:
    return SetupScore(
        action=action,
        direction="neutral",
        score=score,
        long_score=42.0,
        short_score=38.0,
        trend_score=10.0,
        moving_average_score=8.0,
        rsi_score=7.0,
        price_position_score=9.0,
        regime_score=8.0,
        explanation="Test setup score.",
    )


def _build_service_with_fakes(saved: dict[str, Any]) -> SignalService:
    service = object.__new__(SignalService)

    features_with_regime = pd.DataFrame(
        [
            {
                "regime": "bullish",
                "volatility_20": 0.010,
                "rsi_14": 51.0,
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
            action=SignalAction.wait,
            score=45.0,
        )
    )

    def _build_risk_plan(features: pd.DataFrame, action: SignalAction):
        saved["risk_action"] = action
        return risk_plan

    def _estimate_probabilities(
        features: pd.DataFrame,
        setup_score: SetupScore,
        risk_plan_arg: Any,
    ):
        saved["probability_action"] = setup_score.action
        return probability_estimate

    def _build_explanation(**kwargs: Any) -> str:
        saved["explanation_action"] = kwargs["setup_score"].action
        return "Wait intraday explanation."

    def _save_signal(payload: SignalRequest, response: Any) -> None:
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


def test_generate_signal_keeps_wait_when_final_filter_does_not_block_trade() -> None:
    saved: dict[str, Any] = {}
    service = _build_service_with_fakes(saved)
    payload = _build_signal_request()

    response = service.generate_signal(payload)

    assert response.action == SignalAction.wait
    assert response.entry_window == "15:35-16:10"
    assert response.close_by_session_end is True
    assert response.hard_exit_time == "21:55"
    assert response.explanation == "Wait intraday explanation."

    assert saved["risk_action"] == SignalAction.wait
    assert saved["probability_action"] == SignalAction.wait
    assert saved["explanation_action"] == SignalAction.wait
    assert saved["saved_payload"] is payload
    assert saved["saved_response"].action == SignalAction.wait

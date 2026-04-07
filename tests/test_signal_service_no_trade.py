from __future__ import annotations

import pandas as pd
import pytest

from app.schemas.scoring import SetupScore
from app.schemas.signal import SignalAction
from app.services.signal_service import SignalService


def _build_service() -> SignalService:
    return object.__new__(SignalService)


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


def _build_latest_row(
    regime: str,
    volatility_20: float,
    rsi_14: float = 50.0,
) -> pd.Series:
    return pd.Series(
        {
            "regime": regime,
            "volatility_20": volatility_20,
            "rsi_14": rsi_14,
        }
    )


@pytest.mark.parametrize(
    ("regime", "volatility_20", "score"),
    [
        ("sideways", 0.010, 60.0),
        ("bullish_high_vol", 0.020, 60.0),
        ("bullish", 0.010, 44.9),
    ],
)
def test_apply_final_trade_filter_returns_no_trade_for_blocked_intraday_contexts(
    regime: str,
    volatility_20: float,
    score: float,
) -> None:
    service = _build_service()
    setup_score = _build_setup_score(action=SignalAction.wait, score=score)
    latest_row = _build_latest_row(
        regime=regime,
        volatility_20=volatility_20,
        rsi_14=52.0,
    )

    result = service._apply_final_trade_filter(setup_score, latest_row)

    assert result.action == SignalAction.no_trade
    assert result.direction == "neutral"
    assert result.score == setup_score.score
    assert "No-trade bias" in result.explanation


def test_apply_final_trade_filter_keeps_wait_on_clean_boundary_case() -> None:
    service = _build_service()
    setup_score = _build_setup_score(action=SignalAction.wait, score=45.0)
    latest_row = _build_latest_row(
        regime="bullish",
        volatility_20=0.010,
        rsi_14=52.0,
    )

    result = service._apply_final_trade_filter(setup_score, latest_row)

    assert result.action == SignalAction.wait
    assert result.direction == setup_score.direction
    assert result.score == setup_score.score
    assert result.explanation == setup_score.explanation


@pytest.mark.parametrize("regime", ["bullish_high_vol", "bearish_high_vol"])
def test_apply_final_trade_filter_keeps_wait_when_high_vol_is_below_cutoff(
    regime: str,
) -> None:
    service = _build_service()
    setup_score = _build_setup_score(action=SignalAction.wait, score=60.0)
    latest_row = _build_latest_row(
        regime=regime,
        volatility_20=0.0199,
        rsi_14=49.0,
    )

    result = service._apply_final_trade_filter(setup_score, latest_row)

    assert result.action == SignalAction.wait
    assert result.explanation == setup_score.explanation


@pytest.mark.parametrize("regime", ["bullish_high_vol", "bearish_high_vol"])
def test_apply_final_trade_filter_returns_no_trade_when_high_vol_hits_cutoff(
    regime: str,
) -> None:
    service = _build_service()
    setup_score = _build_setup_score(action=SignalAction.wait, score=60.0)
    latest_row = _build_latest_row(
        regime=regime,
        volatility_20=0.0200,
        rsi_14=49.0,
    )

    result = service._apply_final_trade_filter(setup_score, latest_row)

    assert result.action == SignalAction.no_trade
    assert "volatility too unstable" in result.explanation


def test_apply_final_trade_filter_returns_no_trade_when_wait_score_is_below_boundary() -> None:
    service = _build_service()
    setup_score = _build_setup_score(action=SignalAction.wait, score=44.9)
    latest_row = _build_latest_row(
        regime="bullish",
        volatility_20=0.010,
        rsi_14=51.0,
    )

    result = service._apply_final_trade_filter(setup_score, latest_row)

    assert result.action == SignalAction.no_trade
    assert "setup quality too weak" in result.explanation


def test_apply_final_trade_filter_keeps_wait_when_score_is_exactly_at_boundary() -> None:
    service = _build_service()
    setup_score = _build_setup_score(action=SignalAction.wait, score=45.0)
    latest_row = _build_latest_row(
        regime="bullish",
        volatility_20=0.010,
        rsi_14=51.0,
    )

    result = service._apply_final_trade_filter(setup_score, latest_row)

    assert result.action == SignalAction.wait
    assert result.explanation == setup_score.explanation

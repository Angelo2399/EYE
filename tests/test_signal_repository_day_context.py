from __future__ import annotations

from contextlib import suppress
from pathlib import Path
from uuid import uuid4

from app.repositories.signal_repository import SignalRepository
from app.schemas.day_context import DayBias, DayContextLabel
from app.schemas.market import MarketSymbol, MarketTimeframe
from app.schemas.signal import (
    HoldingWindow,
    ModelConfidence,
    SignalAction,
    SignalRequest,
    SignalResponse,
)


def _build_test_db_path() -> Path:
    artifacts_dir = Path(__file__).resolve().parent / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    return artifacts_dir / f"test_eye_day_context_{uuid4().hex}.db"


def _cleanup_db_file(db_path: Path) -> None:
    with suppress(PermissionError, FileNotFoundError):
        db_path.unlink()


def _build_request() -> SignalRequest:
    return SignalRequest(
        symbol=MarketSymbol.ndx,
        timeframe=MarketTimeframe.h1,
    )


def _build_response() -> SignalResponse:
    return SignalResponse(
        asset="Nasdaq 100",
        action=SignalAction.long,
        entry_min=100.0,
        entry_max=101.0,
        entry_window="15:35-16:10",
        expected_holding=HoldingWindow.half_day,
        hard_exit_time="21:55",
        close_by_session_end=True,
        stop_loss=99.0,
        take_profit_1=102.0,
        take_profit_2=103.0,
        risk_reward=1.5,
        favorable_move_pct=54.0,
        tp1_hit_pct=42.0,
        stop_hit_first_pct=34.0,
        model_confidence_pct=48.0,
        confidence_label=ModelConfidence.medium,
        day_context_label=DayContextLabel.trend_up,
        day_context_bias=DayBias.long,
        day_context_confidence_pct=72.0,
        explanation="Structured day context saved.",
    )


def test_save_signal_persists_day_context_fields() -> None:
    db_path = _build_test_db_path()

    try:
        repository = SignalRepository(db_path=db_path)

        signal_id = repository.save_signal(
            request=_build_request(),
            response=_build_response(),
        )
        recent = repository.list_recent_signals(limit=1)

        assert signal_id == 1
        assert len(recent) == 1
        assert recent[0]["day_context_label"] == "trend_up"
        assert recent[0]["day_context_bias"] == "long"
        assert recent[0]["day_context_confidence_pct"] == 72.0
    finally:
        _cleanup_db_file(db_path)


def test_list_recent_signals_returns_null_day_context_fields_when_absent() -> None:
    db_path = _build_test_db_path()

    try:
        repository = SignalRepository(db_path=db_path)

        response = SignalResponse(
            asset="Nasdaq 100",
            action=SignalAction.wait,
            entry_window="15:35-16:10",
            expected_holding=HoldingWindow.h1,
            hard_exit_time="21:55",
            close_by_session_end=True,
            favorable_move_pct=45.0,
            tp1_hit_pct=30.0,
            stop_hit_first_pct=40.0,
            model_confidence_pct=35.0,
            confidence_label=ModelConfidence.low,
            explanation="No structured day context.",
        )

        repository.save_signal(
            request=_build_request(),
            response=response,
        )
        recent = repository.list_recent_signals(limit=1)

        assert len(recent) == 1
        assert recent[0]["day_context_label"] is None
        assert recent[0]["day_context_bias"] is None
        assert recent[0]["day_context_confidence_pct"] is None
    finally:
        _cleanup_db_file(db_path)

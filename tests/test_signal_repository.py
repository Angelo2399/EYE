from contextlib import suppress
from pathlib import Path
from uuid import uuid4

import pytest

from app.repositories.signal_repository import SignalRepository
from app.schemas.market import MarketSymbol, MarketTimeframe
from app.schemas.signal import HoldingWindow, ModelConfidence, SignalAction, SignalRequest, SignalResponse


def _build_test_db_path() -> Path:
    artifacts_dir = Path(__file__).resolve().parent / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    return artifacts_dir / f"test_eye_{uuid4().hex}.db"


def _cleanup_db_file(db_path: Path) -> None:
    with suppress(PermissionError, FileNotFoundError):
        db_path.unlink()


def test_save_signal_and_list_recent_signals() -> None:
    db_path = _build_test_db_path()

    try:
        repository = SignalRepository(db_path=db_path)

        request = SignalRequest(
            symbol=MarketSymbol.ndx,
            timeframe=MarketTimeframe.d1,
        )
        response = SignalResponse(
            asset="Nasdaq 100",
            action=SignalAction.short,
            entry_min=24092.66,
            entry_max=24216.34,
            entry_window="15:35-16:10",
            expected_holding=HoldingWindow.h1,
            hard_exit_time="21:55",
            close_by_session_end=True,
            stop_loss=24884.68,
            take_profit_1=23059.24,
            take_profit_2=22329.06,
            risk_reward=1.5,
            favorable_move_pct=72.8,
            tp1_hit_pct=66.8,
            stop_hit_first_pct=28.8,
            model_confidence_pct=79.0,
            confidence_label=ModelConfidence.high,
            explanation=(
                "Nasdaq 100: SHORT. Regime bearish. Score=100.0, "
                "fav move=72.8%, tp1=66.8%, stop first=28.8%, "
                "confidence=high. Entry 24092.66-24216.34, stop 24884.68, "
                "tp1 23059.24, R/R 1.50."
            ),
        )

        saved_id = repository.save_signal(request, response)
        recent_signals = repository.list_recent_signals(limit=10)

        assert saved_id == 1
        assert len(recent_signals) == 1

        saved_signal = recent_signals[0]

        assert saved_signal["id"] == 1
        assert saved_signal["symbol"] == "NDX"
        assert saved_signal["timeframe"] == "1d"
        assert saved_signal["asset"] == "Nasdaq 100"
        assert saved_signal["action"] == "short"
        assert saved_signal["entry_min"] == 24092.66
        assert saved_signal["entry_max"] == 24216.34
        assert saved_signal["entry_window"] == "15:35-16:10"
        assert saved_signal["expected_holding"] == "1h"
        assert saved_signal["hard_exit_time"] == "21:55"
        assert saved_signal["close_by_session_end"] is True
        assert saved_signal["stop_loss"] == 24884.68
        assert saved_signal["take_profit_1"] == 23059.24
        assert saved_signal["take_profit_2"] == 22329.06
        assert saved_signal["risk_reward"] == 1.5
        assert saved_signal["favorable_move_pct"] == 72.8
        assert saved_signal["tp1_hit_pct"] == 66.8
        assert saved_signal["stop_hit_first_pct"] == 28.8
        assert saved_signal["model_confidence_pct"] == 79.0
        assert saved_signal["confidence_label"] == "high"
        assert "Nasdaq 100: SHORT." in saved_signal["explanation"]
        assert saved_signal["created_at_utc"]
    finally:
        _cleanup_db_file(db_path)


def test_list_recent_signals_rejects_invalid_limit() -> None:
    db_path = _build_test_db_path()

    try:
        repository = SignalRepository(db_path=db_path)

        with pytest.raises(ValueError, match="limit must be greater than 0"):
            repository.list_recent_signals(limit=0)
    finally:
        _cleanup_db_file(db_path)

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
)
from app.services.signal_service import SignalService


class RecordingBriefingBuilderService:
    def __init__(self) -> None:
        self.last_kwargs: dict[str, object] | None = None

    def build_briefing(self, **kwargs) -> dict[str, object]:
        self.last_kwargs = kwargs
        return {
            "title": "Nasdaq 100 intraday briefing",
            "asset": kwargs["asset"],
            "symbol": kwargs["symbol"],
            "timeframe": kwargs["timeframe"],
            "timezone_name": kwargs["briefing_context"]["timezone_name"],
            "session_phase": str(getattr(kwargs["session_context"], "phase", "unknown")),
            "market_closures": list(kwargs["briefing_context"].get("market_closures") or []),
            "summary": "Recorded briefing payload.",
        }


class FakeMarketCalendarService:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def get_market_status(self, *, asset, current_dt=None, timezone_name="Europe/Madrid"):
        asset_value = asset.value if hasattr(asset, "value") else str(asset)
        self.calls.append(
            {
                "asset": asset_value,
                "timezone_name": timezone_name,
            }
        )
        closures = {
            "NDX": {
                "asset": "NDX",
                "market_status": "closed_holiday",
                "official_reason": "U.S. cash equities closed for Good Friday.",
                "next_open_local": "2026-04-06T14:30:00+01:00",
            },
            "SPX": {
                "asset": "SPX",
                "market_status": "closed_session",
                "official_reason": "U.S. cash equities outside regular session hours.",
                "next_open_local": "2026-04-01T14:30:00+01:00",
            },
            "WTI": {
                "asset": "WTI",
                "market_status": "reduced_hours",
                "official_reason": "NYMEX WTI daily maintenance break 17:00-18:00 ET.",
                "next_open_local": "2026-04-01T23:00:00+01:00",
            },
        }
        return closures[asset_value]


def _build_service() -> SignalService:
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
            action=SignalAction.wait,
            direction="neutral",
            score=45.0,
            long_score=42.0,
            short_score=38.0,
            trend_score=10.0,
            moving_average_score=8.0,
            rsi_score=7.0,
            price_position_score=9.0,
            regime_score=8.0,
            explanation="Test setup score.",
        )
    )
    service.session_service = SimpleNamespace(
        get_session_context=lambda current_dt=None: SimpleNamespace(
            phase="pre_open",
            is_session_open=False,
            allow_new_trades=True,
            minutes_to_cutoff=375,
        )
    )
    service.day_context_service = SimpleNamespace(
        classify_day_context=lambda latest_row: DayContext(
            label=DayContextLabel.trend_up,
            bias=DayBias.long,
            confidence_pct=72.0,
            prefer_breakout=True,
            avoid_mean_reversion=True,
            explanation="Trend-up day context.",
        )
    )
    service.risk_service = SimpleNamespace(
        build_risk_plan=lambda features, action: SimpleNamespace(
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
    )
    service.probability_service = SimpleNamespace(
        estimate_probabilities=lambda features, setup_score, risk_plan: SimpleNamespace(
            favorable_move_pct=52.0,
            tp1_hit_pct=41.0,
            stop_hit_first_pct=35.0,
            model_confidence_pct=48.0,
            confidence_label=ModelConfidence.low,
        )
    )
    service.explanation_service = SimpleNamespace(
        build_explanation=lambda **kwargs: "Wait intraday explanation."
    )
    service.signal_repository = SimpleNamespace(
        save_signal=lambda payload, response: None
    )
    service.market_calendar_service = FakeMarketCalendarService()
    service.external_intelligence_enabled = False

    return service


def test_generate_signal_stores_last_briefing_payload() -> None:
    service = _build_service()
    service.briefing_builder_service = RecordingBriefingBuilderService()
    payload = SignalRequest(
        symbol=MarketSymbol.ndx,
        timeframe=MarketTimeframe.h1,
    )

    response = service.generate_signal(payload, timezone_name="Europe/London")

    assert response.action == SignalAction.wait
    assert service._last_briefing_payload == {
        "title": "Nasdaq 100 intraday briefing",
        "asset": "Nasdaq 100",
        "symbol": "NDX",
        "timeframe": "1h",
        "timezone_name": "Europe/London",
        "session_phase": "pre_open",
        "market_closures": [
            {
                "asset": "NDX",
                "market_status": "closed_holiday",
                "official_reason": "U.S. cash equities closed for Good Friday.",
                "next_open_local": "2026-04-06T14:30:00+01:00",
            },
            {
                "asset": "SPX",
                "market_status": "closed_session",
                "official_reason": "U.S. cash equities outside regular session hours.",
                "next_open_local": "2026-04-01T14:30:00+01:00",
            },
            {
                "asset": "WTI",
                "market_status": "reduced_hours",
                "official_reason": "NYMEX WTI daily maintenance break 17:00-18:00 ET.",
                "next_open_local": "2026-04-01T23:00:00+01:00",
            },
        ],
        "summary": "Recorded briefing payload.",
    }
    assert service.briefing_builder_service.last_kwargs is not None
    assert service.briefing_builder_service.last_kwargs["asset"] == "Nasdaq 100"
    assert service.briefing_builder_service.last_kwargs["symbol"] == "NDX"
    assert service.briefing_builder_service.last_kwargs["timeframe"] == "1h"
    assert (
        service.briefing_builder_service.last_kwargs["briefing_context"]["timezone_name"]
        == "Europe/London"
    )
    assert service.market_calendar_service.calls == [
        {"asset": "NDX", "timezone_name": "Europe/London"},
        {"asset": "SPX", "timezone_name": "Europe/London"},
        {"asset": "WTI", "timezone_name": "Europe/London"},
    ]
    market_closures = service._last_briefing_payload["market_closures"]
    assert len(market_closures) == 3
    for entry in market_closures:
        assert set(entry.keys()) == {
            "asset",
            "market_status",
            "official_reason",
            "next_open_local",
        }

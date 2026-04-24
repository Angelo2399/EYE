from __future__ import annotations

from datetime import datetime, timezone
from math import isnan

import pandas as pd

from app.schemas.market import MarketSymbol, MarketTimeframe
from app.schemas.market_intelligence import EventOutcomeFeedback, StructuredMarketEvent
from app.services.market_data_service import MarketDataService


class EventFeedbackService:
    def __init__(
        self,
        market_data_service: MarketDataService | None = None,
    ) -> None:
        self.market_data_service = (
            market_data_service if market_data_service is not None else MarketDataService()
        )

    def build_feedback(self, event: StructuredMarketEvent) -> EventOutcomeFeedback:
        symbol = self._normalize_symbol(event.symbol)
        event_dt = self._parse_event_dt(event)

        observed_after_5m = self._compute_move_pct(
            symbol=symbol,
            timeframe=MarketTimeframe.m5,
            event_dt=event_dt,
            target_dt=event_dt,
            min_target_offset_minutes=5,
        )
        observed_after_30m = self._compute_move_pct(
            symbol=symbol,
            timeframe=MarketTimeframe.m30,
            event_dt=event_dt,
            target_dt=event_dt,
            min_target_offset_minutes=30,
        )
        observed_after_2h = self._compute_move_pct(
            symbol=symbol,
            timeframe=MarketTimeframe.h1,
            event_dt=event_dt,
            target_dt=event_dt,
            min_target_offset_minutes=120,
        )
        session_close_outcome = self._compute_session_close_move_pct(
            symbol=symbol,
            event_dt=event_dt,
        )

        event_score = self._compute_feedback_score(
            direction=event.direction.value,
            observed_after_5m=observed_after_5m,
            observed_after_30m=observed_after_30m,
            observed_after_2h=observed_after_2h,
            session_close_outcome=session_close_outcome,
        )

        return EventOutcomeFeedback(
            event_id=event.event_id,
            asset=event.asset,
            symbol=event.symbol,
            observed_after_5m=observed_after_5m,
            observed_after_30m=observed_after_30m,
            observed_after_2h=observed_after_2h,
            session_close_outcome=session_close_outcome,
            event_score=event_score,
            notes="Auto-generated event feedback.",
        )

    def _compute_move_pct(
        self,
        *,
        symbol: MarketSymbol,
        timeframe: MarketTimeframe,
        event_dt: datetime,
        target_dt: datetime,
        min_target_offset_minutes: int | None = None,
    ) -> float | None:
        frame = self.market_data_service.get_ohlcv(symbol, timeframe)
        if frame.empty:
            return None

        frame = frame.copy()
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
        frame = frame.dropna(subset=["timestamp", "close"]).sort_values("timestamp")

        if frame.empty:
            return None

        anchor_row = frame[frame["timestamp"] <= event_dt].tail(1)
        if anchor_row.empty:
            return None

        anchor_close = self._safe_float(anchor_row.iloc[0]["close"])
        if anchor_close is None or anchor_close == 0:
            return None

        effective_target_dt = target_dt
        if min_target_offset_minutes is not None:
            effective_target_dt = event_dt + pd.Timedelta(minutes=min_target_offset_minutes)

        target_row = frame[frame["timestamp"] >= effective_target_dt].head(1)
        if target_row.empty:
            target_row = frame.tail(1)

        target_close = self._safe_float(target_row.iloc[0]["close"])
        if target_close is None:
            return None

        move_pct = ((target_close - anchor_close) / anchor_close) * 100.0
        return round(move_pct, 4)

    def _compute_session_close_move_pct(
        self,
        *,
        symbol: MarketSymbol,
        event_dt: datetime,
    ) -> float | None:
        frame = self.market_data_service.get_ohlcv(symbol, MarketTimeframe.h1)
        if frame.empty:
            return None

        frame = frame.copy()
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
        frame = frame.dropna(subset=["timestamp", "close"]).sort_values("timestamp")

        if frame.empty:
            return None

        same_day = frame[frame["timestamp"].dt.date == event_dt.date()]
        if same_day.empty:
            return None

        anchor_row = same_day[same_day["timestamp"] <= event_dt].tail(1)
        if anchor_row.empty:
            anchor_row = same_day.head(1)

        close_row = same_day.tail(1)

        anchor_close = self._safe_float(anchor_row.iloc[0]["close"])
        close_value = self._safe_float(close_row.iloc[0]["close"])

        if anchor_close is None or close_value is None or anchor_close == 0:
            return None

        move_pct = ((close_value - anchor_close) / anchor_close) * 100.0
        return round(move_pct, 4)

    def _compute_feedback_score(
        self,
        *,
        direction: str,
        observed_after_5m: float | None,
        observed_after_30m: float | None,
        observed_after_2h: float | None,
        session_close_outcome: float | None,
    ) -> float:
        values = [
            value
            for value in [
                observed_after_5m,
                observed_after_30m,
                observed_after_2h,
                session_close_outcome,
            ]
            if value is not None
        ]
        if not values:
            return 50.0

        weighted = (
            (observed_after_5m or 0.0) * 0.20
            + (observed_after_30m or 0.0) * 0.30
            + (observed_after_2h or 0.0) * 0.30
            + (session_close_outcome or 0.0) * 0.20
        )

        if direction == "bullish":
            raw = 50.0 + (weighted * 10.0)
        elif direction == "bearish":
            raw = 50.0 + ((-weighted) * 10.0)
        else:
            raw = 50.0

        bounded = max(0.0, min(raw, 100.0))
        snapped = min(100.0, float(int((bounded + 2.5) / 5.0) * 5))
        return round(snapped, 1)

    def _normalize_symbol(self, symbol: str) -> MarketSymbol:
        normalized = str(symbol).strip().upper()
        try:
            return MarketSymbol(normalized)
        except ValueError as exc:
            raise ValueError(f"Unsupported symbol '{symbol}' for feedback.") from exc

    def _parse_event_dt(self, event: StructuredMarketEvent) -> datetime:
        raw = event.occurred_at_utc or event.detected_at_utc
        if not raw:
            return datetime.now(timezone.utc)

        parsed = pd.to_datetime(raw, utc=True, errors="coerce")
        if pd.isna(parsed):
            return datetime.now(timezone.utc)

        return parsed.to_pydatetime()

    def _safe_float(self, value) -> float | None:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None

        if isnan(numeric):
            return None

        return numeric

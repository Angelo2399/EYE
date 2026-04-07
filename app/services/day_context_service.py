from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from app.schemas.day_context import DayBias, DayContext, DayContextLabel


class DayContextService:
    def classify_day_context(self, latest_row: Mapping[str, Any]) -> DayContext:
        regime = str(latest_row.get("regime", "")).strip().lower()
        volatility_20 = self._safe_float(latest_row.get("volatility_20"), default=0.0)
        rsi_14 = self._safe_float(latest_row.get("rsi_14"), default=50.0)
        distance_sma20 = self._safe_float(
            latest_row.get("distance_sma20"),
            default=0.0,
        )

        if regime == "sideways":
            return DayContext(
                label=DayContextLabel.range_day,
                bias=DayBias.neutral,
                confidence_pct=70.0,
                prefer_breakout=False,
                avoid_mean_reversion=False,
                explanation=(
                    "Range day context: sideways intraday regime. "
                    "Prefer patience and clean mean-reversion zones over breakout chasing."
                ),
            )

        if regime.endswith("high_vol") and volatility_20 >= 0.02:
            return DayContext(
                label=DayContextLabel.volatile_day,
                bias=DayBias.neutral,
                confidence_pct=35.0,
                prefer_breakout=False,
                avoid_mean_reversion=True,
                explanation=(
                    "Volatile day context: intraday volatility is elevated. "
                    "Prefer reduced aggression or no-trade unless the setup is exceptional."
                ),
            )

        if regime == "bullish" and rsi_14 >= 55.0 and distance_sma20 >= 0.0:
            return DayContext(
                label=DayContextLabel.trend_up,
                bias=DayBias.long,
                confidence_pct=72.0,
                prefer_breakout=True,
                avoid_mean_reversion=True,
                explanation=(
                    "Trend-up day context: bullish intraday structure with constructive momentum. "
                    "Long bias is preferred on pullbacks or clean continuation."
                ),
            )

        if regime == "bearish" and rsi_14 <= 45.0 and distance_sma20 <= 0.0:
            return DayContext(
                label=DayContextLabel.trend_down,
                bias=DayBias.short,
                confidence_pct=72.0,
                prefer_breakout=True,
                avoid_mean_reversion=True,
                explanation=(
                    "Trend-down day context: bearish intraday structure with weak momentum. "
                    "Short bias is preferred on bounces or clean continuation."
                ),
            )

        return DayContext(
            label=DayContextLabel.unclear,
            bias=DayBias.neutral,
            confidence_pct=45.0,
            prefer_breakout=False,
            avoid_mean_reversion=False,
            explanation=(
                "Unclear day context: signals are mixed and the day structure is not clean enough yet."
            ),
        )

    def _safe_float(self, value: Any, default: float) -> float:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return default

        if number != number:
            return default

        return number

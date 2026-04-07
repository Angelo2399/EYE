from __future__ import annotations

import pandas as pd

from app.schemas.scoring import SetupScore
from app.schemas.signal import SignalAction


class ScoringService:
    _REQUIRED_COLUMNS: tuple[str, ...] = (
        "timestamp",
        "close",
        "sma_20",
        "sma_50",
        "ema_20",
        "rsi_14",
        "distance_sma20",
        "distance_sma50",
        "regime",
        "volatility_20",
        "atr_14",
    )

    def score_setup(self, features_with_regime: pd.DataFrame) -> SetupScore:
        self._validate_input(features_with_regime)

        row = features_with_regime.sort_values("timestamp").iloc[-1]

        long_trend, short_trend = self._score_trend(row)
        long_ma, short_ma = self._score_moving_averages(row)
        long_rsi, short_rsi = self._score_rsi(row)
        long_price, short_price = self._score_price_position(row)
        long_regime, short_regime = self._score_regime(row)
        long_volatility, short_volatility = self._score_volatility_quality(row)

        long_score = (
            long_trend
            + long_ma
            + long_rsi
            + long_price
            + long_regime
            + long_volatility
        )
        short_score = (
            short_trend
            + short_ma
            + short_rsi
            + short_price
            + short_regime
            + short_volatility
        )

        action = self._select_action(long_score, short_score, row)
        direction = (
            action.value if action in {SignalAction.long, SignalAction.short} else "neutral"
        )
        score = max(long_score, short_score)

        explanation = self._build_explanation(
            action=action,
            long_score=long_score,
            short_score=short_score,
            regime=str(row["regime"]),
            rsi_value=float(row["rsi_14"]),
        )

        selected_components = {
            SignalAction.long: (
                long_trend,
                long_ma,
                long_rsi,
                long_price,
                long_regime + long_volatility,
            ),
            SignalAction.short: (
                short_trend,
                short_ma,
                short_rsi,
                short_price,
                short_regime + short_volatility,
            ),
        }

        trend_score, moving_average_score, rsi_score_value, price_position_score, regime_score = (
            selected_components.get(action, (0.0, 0.0, 0.0, 0.0, 0.0))
        )

        return SetupScore(
            action=action,
            direction=direction,
            score=round(score, 2),
            long_score=round(long_score, 2),
            short_score=round(short_score, 2),
            trend_score=round(trend_score, 2),
            moving_average_score=round(moving_average_score, 2),
            rsi_score=round(rsi_score_value, 2),
            price_position_score=round(price_position_score, 2),
            regime_score=round(regime_score, 2),
            explanation=explanation,
        )

    def _validate_input(self, features_with_regime: pd.DataFrame) -> None:
        if not isinstance(features_with_regime, pd.DataFrame):
            raise TypeError("features_with_regime must be a pandas DataFrame.")

        if features_with_regime.empty:
            raise ValueError("features_with_regime cannot be empty.")

        missing_columns = set(self._REQUIRED_COLUMNS) - set(features_with_regime.columns)
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Missing required scoring columns: {missing}.")

    def _score_trend(self, row: pd.Series) -> tuple[float, float]:
        close = float(row["close"])
        sma_20 = float(row["sma_20"])
        sma_50 = float(row["sma_50"])

        long_score = 0.0
        short_score = 0.0

        if close > sma_20 > sma_50:
            long_score = 24.0
        elif close > sma_20 and sma_20 >= sma_50:
            long_score = 10.0

        if close < sma_20 < sma_50:
            short_score = 24.0
        elif close < sma_20 and sma_20 <= sma_50:
            short_score = 10.0

        return long_score, short_score

    def _score_moving_averages(self, row: pd.Series) -> tuple[float, float]:
        close = float(row["close"])
        ema_20 = float(row["ema_20"])
        sma_20 = float(row["sma_20"])

        long_score = 0.0
        short_score = 0.0

        if close > ema_20 and ema_20 > sma_20:
            long_score = 18.0
        elif close > ema_20 and ema_20 >= sma_20:
            long_score = 8.0

        if close < ema_20 and ema_20 < sma_20:
            short_score = 18.0
        elif close < ema_20 and ema_20 <= sma_20:
            short_score = 8.0

        return long_score, short_score

    def _score_rsi(self, row: pd.Series) -> tuple[float, float]:
        rsi_value = float(row["rsi_14"])

        long_score = 0.0
        short_score = 0.0

        if 52.0 <= rsi_value <= 62.0:
            long_score = 14.0
        elif 49.0 <= rsi_value < 52.0:
            long_score = 5.0

        if 38.0 <= rsi_value <= 48.0:
            short_score = 14.0
        elif 48.0 < rsi_value <= 51.0:
            short_score = 5.0

        return long_score, short_score

    def _score_price_position(self, row: pd.Series) -> tuple[float, float]:
        distance_sma20 = float(row["distance_sma20"])
        distance_sma50 = float(row["distance_sma50"])

        long_score = 0.0
        short_score = 0.0

        if 0.001 <= distance_sma20 <= 0.018 and 0.003 <= distance_sma50 <= 0.035:
            long_score = 16.0
        elif 0.0 <= distance_sma20 <= 0.025:
            long_score = 6.0

        if -0.018 <= distance_sma20 <= -0.001 and -0.035 <= distance_sma50 <= -0.003:
            short_score = 16.0
        elif -0.025 <= distance_sma20 <= 0.0:
            short_score = 6.0

        return long_score, short_score

    def _score_regime(self, row: pd.Series) -> tuple[float, float]:
        regime = str(row["regime"])

        long_map = {
            "bullish": 18.0,
            "bullish_high_vol": 6.0,
            "sideways": -8.0,
            "bearish": -16.0,
            "bearish_high_vol": -20.0,
        }

        short_map = {
            "bullish": -16.0,
            "bullish_high_vol": -20.0,
            "sideways": -8.0,
            "bearish": 18.0,
            "bearish_high_vol": 6.0,
        }

        return long_map.get(regime, -10.0), short_map.get(regime, -10.0)

    def _score_volatility_quality(self, row: pd.Series) -> tuple[float, float]:
        regime = str(row["regime"])
        volatility_20 = float(row["volatility_20"]) if pd.notna(row["volatility_20"]) else 0.0

        bonus = 0.0
        penalty = 0.0

        if regime.endswith("high_vol"):
            penalty -= 6.0
        elif 0.006 <= volatility_20 <= 0.014:
            bonus += 6.0
        elif volatility_20 > 0.02:
            penalty -= 8.0

        return bonus + penalty, bonus + penalty

    def _select_action(self, long_score: float, short_score: float, row: pd.Series) -> SignalAction:
        regime = str(row["regime"])
        score_gap = abs(long_score - short_score)
        best_score = max(long_score, short_score)

        if regime == "sideways":
            return SignalAction.wait

        if best_score < 70.0:
            return SignalAction.wait

        if score_gap < 14.0:
            return SignalAction.wait

        if long_score >= 70.0 and long_score > short_score:
            return SignalAction.long

        if short_score >= 70.0 and short_score > long_score:
            return SignalAction.short

        return SignalAction.wait

    def _build_explanation(
        self,
        action: SignalAction,
        long_score: float,
        short_score: float,
        regime: str,
        rsi_value: float,
    ) -> str:
        if action == SignalAction.wait:
            return (
                f"No-trade bias for now: intraday setup not selective enough. "
                f"Long score={long_score:.1f}, short score={short_score:.1f}, "
                f"regime={regime}, RSI={rsi_value:.1f}."
            )

        return (
            f"{action.value.upper()} intraday setup selected: "
            f"long score={long_score:.1f}, short score={short_score:.1f}, "
            f"regime={regime}, RSI={rsi_value:.1f}."
        )

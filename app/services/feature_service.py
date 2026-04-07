from __future__ import annotations

import pandas as pd

from app.utils.indicators import atr, ema, rsi, sma


class FeatureService:
    _REQUIRED_COLUMNS: tuple[str, ...] = (
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    )

    def build_features(self, market_data: pd.DataFrame) -> pd.DataFrame:
        self._validate_input(market_data)

        frame = market_data.copy()
        frame = frame.sort_values("timestamp").reset_index(drop=True)

        frame["ret_1"] = frame["close"].pct_change(1)
        frame["ret_5"] = frame["close"].pct_change(5)

        frame["sma_20"] = sma(frame["close"], 20)
        frame["sma_50"] = sma(frame["close"], 50)
        frame["ema_20"] = ema(frame["close"], 20)
        frame["rsi_14"] = rsi(frame["close"], 14)
        frame["atr_14"] = atr(frame["high"], frame["low"], frame["close"], 14)

        frame["volatility_20"] = frame["ret_1"].rolling(window=20, min_periods=20).std()

        frame["distance_sma20"] = self._distance_from_reference(
            frame["close"],
            frame["sma_20"],
        )
        frame["distance_sma50"] = self._distance_from_reference(
            frame["close"],
            frame["sma_50"],
        )

        return frame

    def _validate_input(self, market_data: pd.DataFrame) -> None:
        if not isinstance(market_data, pd.DataFrame):
            raise TypeError("market_data must be a pandas DataFrame.")

        missing_columns = set(self._REQUIRED_COLUMNS) - set(market_data.columns)
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Missing required market data columns: {missing}.")

        if market_data.empty:
            raise ValueError("market_data cannot be empty.")

    def _distance_from_reference(
        self,
        price_series: pd.Series,
        reference_series: pd.Series,
    ) -> pd.Series:
        reference_series = reference_series.replace(0.0, pd.NA)
        return (price_series / reference_series) - 1.0

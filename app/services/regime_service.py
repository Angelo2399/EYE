from __future__ import annotations

import pandas as pd


class RegimeService:
    _REQUIRED_COLUMNS: tuple[str, ...] = (
        "timestamp",
        "close",
        "sma_20",
        "sma_50",
        "atr_14",
        "volatility_20",
    )

    def classify_regime(self, features: pd.DataFrame) -> pd.DataFrame:
        self._validate_input(features)

        frame = features.copy()
        frame = frame.sort_values("timestamp").reset_index(drop=True)

        frame["atr_pct"] = frame["atr_14"] / frame["close"].replace(0.0, pd.NA)
        frame["volatility_baseline_60"] = frame["volatility_20"].rolling(
            window=60,
            min_periods=20,
        ).median()
        frame["atr_pct_baseline_60"] = frame["atr_pct"].rolling(
            window=60,
            min_periods=20,
        ).median()

        frame["is_high_vol"] = (
            (frame["volatility_20"] > frame["volatility_baseline_60"] * 1.25)
            | (frame["atr_pct"] > frame["atr_pct_baseline_60"] * 1.25)
        ).fillna(False)

        frame["trend_state"] = frame.apply(self._classify_trend_state, axis=1)
        frame["regime"] = frame.apply(self._classify_regime_label, axis=1)

        return frame

    def get_current_regime(self, features: pd.DataFrame) -> str:
        classified = self.classify_regime(features)
        return str(classified.iloc[-1]["regime"])

    def _validate_input(self, features: pd.DataFrame) -> None:
        if not isinstance(features, pd.DataFrame):
            raise TypeError("features must be a pandas DataFrame.")

        if features.empty:
            raise ValueError("features cannot be empty.")

        missing_columns = set(self._REQUIRED_COLUMNS) - set(features.columns)
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Missing required feature columns: {missing}.")

    def _classify_trend_state(self, row: pd.Series) -> str:
        if pd.isna(row["close"]) or pd.isna(row["sma_20"]) or pd.isna(row["sma_50"]):
            return "sideways"

        if row["close"] > row["sma_20"] and row["sma_20"] > row["sma_50"]:
            return "bullish"

        if row["close"] < row["sma_20"] and row["sma_20"] < row["sma_50"]:
            return "bearish"

        return "sideways"

    def _classify_regime_label(self, row: pd.Series) -> str:
        trend_state = row["trend_state"]
        is_high_vol = bool(row["is_high_vol"])

        if trend_state == "bullish" and is_high_vol:
            return "bullish_high_vol"

        if trend_state == "bearish" and is_high_vol:
            return "bearish_high_vol"

        if trend_state == "bullish":
            return "bullish"

        if trend_state == "bearish":
            return "bearish"

        return "sideways"

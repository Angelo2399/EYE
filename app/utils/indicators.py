from __future__ import annotations

import numpy as np
import pandas as pd


def sma(series: pd.Series, period: int) -> pd.Series:
    """Return the simple moving average."""
    _validate_period(period)
    normalized_series = _to_numeric_series(series)

    return normalized_series.rolling(window=period, min_periods=period).mean()


def ema(series: pd.Series, period: int) -> pd.Series:
    """Return the exponential moving average."""
    _validate_period(period)
    normalized_series = _to_numeric_series(series)

    return normalized_series.ewm(span=period, adjust=False, min_periods=period).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Return the RSI using Wilder smoothing."""
    _validate_period(period)
    normalized_series = _to_numeric_series(series)

    delta = normalized_series.diff()
    gains = delta.clip(lower=0.0)
    losses = -delta.clip(upper=0.0)

    average_gain = gains.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    average_loss = losses.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()

    relative_strength = average_gain / average_loss.replace(0.0, np.nan)
    rsi_series = 100 - (100 / (1 + relative_strength))
    rsi_series = rsi_series.where(average_loss != 0.0, 100.0)

    return rsi_series.astype("float64")


def atr(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    period: int = 14,
) -> pd.Series:
    """Return the Average True Range using Wilder smoothing."""
    _validate_period(period)

    normalized_high = _to_numeric_series(high)
    normalized_low = _to_numeric_series(low)
    normalized_close = _to_numeric_series(close)

    previous_close = normalized_close.shift(1)

    true_range = pd.concat(
        [
            normalized_high - normalized_low,
            (normalized_high - previous_close).abs(),
            (normalized_low - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    return true_range.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def _to_numeric_series(series: pd.Series) -> pd.Series:
    if not isinstance(series, pd.Series):
        raise TypeError("Input must be a pandas Series.")

    return pd.to_numeric(series, errors="coerce")


def _validate_period(period: int) -> None:
    if period <= 0:
        raise ValueError("period must be greater than 0")

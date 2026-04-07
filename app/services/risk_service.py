from __future__ import annotations

import pandas as pd

from app.schemas.risk import RiskPlan
from app.schemas.signal import HoldingWindow, SignalAction


class RiskService:
    _REQUIRED_COLUMNS: tuple[str, ...] = (
        "timestamp",
        "high",
        "low",
        "close",
        "atr_14",
        "volatility_20",
    )

    def build_risk_plan(
        self,
        market_features: pd.DataFrame,
        action: SignalAction,
    ) -> RiskPlan:
        self._validate_input(market_features)

        if action in {SignalAction.wait, SignalAction.no_trade}:
            return RiskPlan(
                entry_min=None,
                entry_max=None,
                entry_window=None,
                expected_holding=None,
                hard_exit_time=None,
                close_by_session_end=True,
                stop_loss=None,
                take_profit_1=None,
                take_profit_2=None,
                risk_reward=None,
            )

        frame = market_features.sort_values("timestamp").reset_index(drop=True)
        row = frame.iloc[-1]

        close = float(row["close"])
        atr_value = float(row["atr_14"])
        volatility_20 = float(row["volatility_20"]) if pd.notna(row["volatility_20"]) else 0.0

        if pd.isna(atr_value) or atr_value <= 0:
            raise ValueError("atr_14 must be available and greater than 0.")

        lookback_frame = frame.tail(12)
        entry_window = self._build_entry_window()
        expected_holding = self._select_holding_window(volatility_20)
        hard_exit_time = "21:55"

        if action == SignalAction.long:
            return self._build_long_plan(
                close=close,
                atr_value=atr_value,
                lookback_frame=lookback_frame,
                entry_window=entry_window,
                expected_holding=expected_holding,
                hard_exit_time=hard_exit_time,
            )

        if action == SignalAction.short:
            return self._build_short_plan(
                close=close,
                atr_value=atr_value,
                lookback_frame=lookback_frame,
                entry_window=entry_window,
                expected_holding=expected_holding,
                hard_exit_time=hard_exit_time,
            )

        raise ValueError(f"Unsupported action for risk plan: {action}.")

    def _build_long_plan(
        self,
        close: float,
        atr_value: float,
        lookback_frame: pd.DataFrame,
        entry_window: str,
        expected_holding: HoldingWindow,
        hard_exit_time: str,
    ) -> RiskPlan:
        entry_min = close - (0.12 * atr_value)
        entry_max = close + (0.05 * atr_value)

        structural_stop = float(lookback_frame["low"].min())
        atr_stop = close - (0.80 * atr_value)
        stop_loss = min(structural_stop, atr_stop)

        entry_mid = (entry_min + entry_max) / 2.0
        risk_per_unit = entry_mid - stop_loss

        if risk_per_unit <= 0:
            raise ValueError("Invalid long risk structure: stop_loss is not below entry.")

        take_profit_1 = entry_mid + (0.90 * risk_per_unit)
        take_profit_2 = entry_mid + (1.40 * risk_per_unit)
        risk_reward = (take_profit_1 - entry_mid) / risk_per_unit

        return RiskPlan(
            entry_min=round(entry_min, 2),
            entry_max=round(entry_max, 2),
            entry_window=entry_window,
            expected_holding=expected_holding,
            hard_exit_time=hard_exit_time,
            close_by_session_end=True,
            stop_loss=round(stop_loss, 2),
            take_profit_1=round(take_profit_1, 2),
            take_profit_2=round(take_profit_2, 2),
            risk_reward=round(risk_reward, 2),
        )

    def _build_short_plan(
        self,
        close: float,
        atr_value: float,
        lookback_frame: pd.DataFrame,
        entry_window: str,
        expected_holding: HoldingWindow,
        hard_exit_time: str,
    ) -> RiskPlan:
        entry_min = close - (0.05 * atr_value)
        entry_max = close + (0.12 * atr_value)

        structural_stop = float(lookback_frame["high"].max())
        atr_stop = close + (0.80 * atr_value)
        stop_loss = max(structural_stop, atr_stop)

        entry_mid = (entry_min + entry_max) / 2.0
        risk_per_unit = stop_loss - entry_mid

        if risk_per_unit <= 0:
            raise ValueError("Invalid short risk structure: stop_loss is not above entry.")

        take_profit_1 = entry_mid - (0.90 * risk_per_unit)
        take_profit_2 = entry_mid - (1.40 * risk_per_unit)
        risk_reward = (entry_mid - take_profit_1) / risk_per_unit

        return RiskPlan(
            entry_min=round(entry_min, 2),
            entry_max=round(entry_max, 2),
            entry_window=entry_window,
            expected_holding=expected_holding,
            hard_exit_time=hard_exit_time,
            close_by_session_end=True,
            stop_loss=round(stop_loss, 2),
            take_profit_1=round(take_profit_1, 2),
            take_profit_2=round(take_profit_2, 2),
            risk_reward=round(risk_reward, 2),
        )

    def _build_entry_window(self) -> str:
        return "15:35-16:10"

    def _select_holding_window(self, volatility_20: float) -> HoldingWindow:
        if volatility_20 >= 0.015:
            return HoldingWindow.m30
        if volatility_20 >= 0.010:
            return HoldingWindow.h1
        if volatility_20 >= 0.007:
            return HoldingWindow.h2
        return HoldingWindow.half_day

    def _validate_input(self, market_features: pd.DataFrame) -> None:
        if not isinstance(market_features, pd.DataFrame):
            raise TypeError("market_features must be a pandas DataFrame.")

        if market_features.empty:
            raise ValueError("market_features cannot be empty.")

        missing_columns = set(self._REQUIRED_COLUMNS) - set(market_features.columns)
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Missing required risk columns: {missing}.")

from __future__ import annotations

import pandas as pd

from app.schemas.probability import ProbabilityEstimate
from app.schemas.risk import RiskPlan
from app.schemas.scoring import SetupScore
from app.schemas.signal import ModelConfidence, SignalAction


class ProbabilityService:
    _REQUIRED_COLUMNS: tuple[str, ...] = (
        "timestamp",
        "close",
        "atr_14",
        "volatility_20",
        "regime",
    )

    def estimate_probabilities(
        self,
        features_with_regime: pd.DataFrame,
        setup_score: SetupScore,
        risk_plan: RiskPlan,
    ) -> ProbabilityEstimate:
        self._validate_input(features_with_regime)

        row = features_with_regime.sort_values("timestamp").iloc[-1]
        regime = str(row["regime"])
        volatility_20 = float(row["volatility_20"]) if pd.notna(row["volatility_20"]) else 0.0

        if setup_score.action in {SignalAction.wait, SignalAction.no_trade}:
            confidence_pct = self._clamp(24.0 + (setup_score.score * 0.18), 18.0, 40.0)
            return ProbabilityEstimate(
                favorable_move_pct=46.0,
                tp1_hit_pct=24.0,
                stop_hit_first_pct=30.0,
                model_confidence_pct=round(confidence_pct, 2),
                confidence_label=self._confidence_label(confidence_pct),
            )

        score = float(setup_score.score)
        score_gap = abs(float(setup_score.long_score) - float(setup_score.short_score))
        risk_reward = float(risk_plan.risk_reward or 0.9)

        aligned_regime = self._is_regime_aligned(setup_score.action, regime)
        high_vol = regime.endswith("high_vol")
        sideways = regime == "sideways"

        favorable_move_pct = 34.0 + (score * 0.22) + (min(score_gap, 40.0) * 0.08)
        if aligned_regime:
            favorable_move_pct += 4.0
        if sideways:
            favorable_move_pct -= 8.0
        if high_vol:
            favorable_move_pct -= 6.0
        if volatility_20 > 0.020:
            favorable_move_pct -= 4.0
        favorable_move_pct -= max(risk_reward - 0.9, 0.0) * 6.0
        favorable_move_pct = self._clamp(favorable_move_pct, 38.0, 68.0)

        tp1_hit_pct = favorable_move_pct - 5.0
        if high_vol:
            tp1_hit_pct -= 2.0
        if volatility_20 > 0.020:
            tp1_hit_pct -= 2.0
        tp1_hit_pct = self._clamp(tp1_hit_pct, 20.0, 62.0)

        stop_hit_first_pct = 58.0 - (score * 0.18) - (min(score_gap, 40.0) * 0.05)
        if aligned_regime:
            stop_hit_first_pct -= 4.0
        if sideways:
            stop_hit_first_pct += 6.0
        if high_vol:
            stop_hit_first_pct += 6.0
        if volatility_20 > 0.020:
            stop_hit_first_pct += 3.0
        stop_hit_first_pct = self._clamp(stop_hit_first_pct, 22.0, 52.0)

        model_confidence_pct = 26.0 + (score * 0.34) + (min(score_gap, 40.0) * 0.08)
        if aligned_regime:
            model_confidence_pct += 4.0
        if sideways:
            model_confidence_pct -= 7.0
        if high_vol:
            model_confidence_pct -= 6.0
        if volatility_20 > 0.020:
            model_confidence_pct -= 4.0
        model_confidence_pct = self._clamp(model_confidence_pct, 20.0, 78.0)

        return ProbabilityEstimate(
            favorable_move_pct=round(favorable_move_pct, 2),
            tp1_hit_pct=round(tp1_hit_pct, 2),
            stop_hit_first_pct=round(stop_hit_first_pct, 2),
            model_confidence_pct=round(model_confidence_pct, 2),
            confidence_label=self._confidence_label(model_confidence_pct),
        )

    def _validate_input(self, features_with_regime: pd.DataFrame) -> None:
        if not isinstance(features_with_regime, pd.DataFrame):
            raise TypeError("features_with_regime must be a pandas DataFrame.")

        if features_with_regime.empty:
            raise ValueError("features_with_regime cannot be empty.")

        missing_columns = set(self._REQUIRED_COLUMNS) - set(features_with_regime.columns)
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Missing required probability columns: {missing}.")

    def _is_regime_aligned(self, action: SignalAction, regime: str) -> bool:
        if action == SignalAction.long:
            return regime.startswith("bullish")
        if action == SignalAction.short:
            return regime.startswith("bearish")
        return False

    def _confidence_label(self, confidence_pct: float) -> ModelConfidence:
        if confidence_pct < 38.0:
            return ModelConfidence.low
        if confidence_pct < 55.0:
            return ModelConfidence.medium
        if confidence_pct < 70.0:
            return ModelConfidence.medium_high
        return ModelConfidence.high

    def _clamp(self, value: float, minimum: float, maximum: float) -> float:
        return max(minimum, min(value, maximum))

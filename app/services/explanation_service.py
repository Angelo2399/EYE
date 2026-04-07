from __future__ import annotations

from app.schemas.day_context import DayContext
from app.schemas.market_intelligence import MarketIntelligenceSnapshot
from app.schemas.probability import ProbabilityEstimate
from app.schemas.risk import RiskPlan
from app.schemas.scoring import SetupScore


class ExplanationService:
    def build_explanation(
        self,
        asset: str,
        regime: str,
        setup_score: SetupScore,
        risk_plan: RiskPlan,
        probability_estimate: ProbabilityEstimate,
        day_context: DayContext | None = None,
        intelligence_snapshot: MarketIntelligenceSnapshot | None = None,
    ) -> str:
        action = setup_score.action.value.upper()
        regime_label = regime.replace("_", " ")
        holding_text = (
            risk_plan.expected_holding.value
            if risk_plan.expected_holding is not None
            else "n/a"
        )
        exit_text = risk_plan.hard_exit_time or "n/a"
        day_context_text = self._format_day_context(day_context)
        intelligence_text = self._format_external_intelligence(intelligence_snapshot)

        if setup_score.action.value in {"wait", "no_trade"}:
            return (
                f"{asset}: {action}. "
                f"Regime {regime_label}. "
                f"Setup intraday non abbastanza pulito. "
                f"{day_context_text}"
                f"{intelligence_text}"
                f"Score={setup_score.score:.1f}, "
                f"fav move={probability_estimate.favorable_move_pct:.1f}%, "
                f"confidence={probability_estimate.confidence_label.value}. "
                f"Nessuna posizione overnight."
            )

        entry_text = self._format_entry(risk_plan)
        stop_text = self._format_price(risk_plan.stop_loss)
        tp1_text = self._format_price(risk_plan.take_profit_1)
        tp2_text = self._format_price(risk_plan.take_profit_2)
        rr_text = self._format_ratio(risk_plan.risk_reward)
        entry_window_text = risk_plan.entry_window or "n/a"
        session_text = "yes" if risk_plan.close_by_session_end else "no"

        return (
            f"{asset}: {action} intraday. "
            f"Regime {regime_label}. "
            f"{day_context_text}"
            f"{intelligence_text}"
            f"Entry window {entry_window_text}, "
            f"holding {holding_text}, "
            f"hard exit {exit_text}, "
            f"close by session end={session_text}. "
            f"Entry {entry_text}, stop {stop_text}, tp1 {tp1_text}, tp2 {tp2_text}, "
            f"R/R {rr_text}. "
            f"Score={setup_score.score:.1f}, "
            f"fav move={probability_estimate.favorable_move_pct:.1f}%, "
            f"tp1={probability_estimate.tp1_hit_pct:.1f}%, "
            f"stop first={probability_estimate.stop_hit_first_pct:.1f}%, "
            f"confidence={probability_estimate.confidence_label.value}."
        )

    def _format_day_context(self, day_context: DayContext | None) -> str:
        if day_context is None:
            return ""

        return (
            f"Day context {day_context.label.value}, "
            f"bias={day_context.bias.value}, "
            f"ctx_conf={day_context.confidence_pct:.1f}%. "
        )

    def _format_external_intelligence(
        self,
        intelligence_snapshot: MarketIntelligenceSnapshot | None,
    ) -> str:
        if intelligence_snapshot is None:
            return ""

        drivers = [
            str(driver).strip()
            for driver in intelligence_snapshot.dominant_drivers[:3]
            if str(driver).strip()
        ]
        drivers_text = ", ".join(drivers) if drivers else "n/a"
        synthesis = str(intelligence_snapshot.synthesis or "").strip()

        if synthesis:
            return (
                f"External intelligence bias={intelligence_snapshot.market_bias.value}, "
                f"ext_conf={intelligence_snapshot.bias_confidence_pct:.1f}%, "
                f"drivers={drivers_text}, "
                f"synthesis={synthesis}. "
            )

        return (
            f"External intelligence bias={intelligence_snapshot.market_bias.value}, "
            f"ext_conf={intelligence_snapshot.bias_confidence_pct:.1f}%, "
            f"drivers={drivers_text}. "
        )

    def _format_entry(self, risk_plan: RiskPlan) -> str:
        if risk_plan.entry_min is None or risk_plan.entry_max is None:
            return "n/a"

        return f"{risk_plan.entry_min:.2f}-{risk_plan.entry_max:.2f}"

    def _format_price(self, value: float | None) -> str:
        if value is None:
            return "n/a"

        return f"{value:.2f}"

    def _format_ratio(self, value: float | None) -> str:
        if value is None:
            return "n/a"

        return f"{value:.2f}"
